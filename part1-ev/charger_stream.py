#!/usr/bin/env python3
"""Q4 · Q5 — Spark Structured Streaming: Kafka -> S3 Bronze / Silver / Gold + 이상 감지 Slack 알림

EC2 의 spark-master 컨테이너에서 실행한다(master 주소·패키지·S3 설정은 compose 의 spark-defaults.conf).

    docker exec -it spark-master /opt/spark/bin/spark-submit /opt/de9/part1-ev/charger_stream.py

스트리밍 쿼리
  lake    : foreachBatch 로 마이크로배치마다
              Bronze = Kafka 원본 이벤트 그대로   -> s3://<버킷>/bronze/ev-charging-sessions/dt=YYYY-MM-DD/
              Silver = 파싱·정제한 세션            -> s3://<버킷>/silver/ev-charging-sessions/dt=YYYY-MM-DD/
            (Q5) Silver 의 idleSec > 5400(90분) 인 세션 = 포트 점유 이상 -> Slack 채널로 알림
  console : 설치구역(locationGroup)별 누적 세션 수를 트리거마다 콘솔에 출력 (complete 모드)
  gold    : (Q5) 설치구역별 이상 건수 누적 집계를 트리거마다 덮어쓴다
                                                  -> s3://<버킷>/gold/ev_charging_agg/

Silver 에서 더하는 컬럼
  locationGroup        stationName 끝의 ' #숫자' 를 뗀 설치구역 이름
  totalDurationSec     totalDuration(h:mm:ss) 을 초로
  chargingDurationSec  chargingDuration(h:mm:ss) 을 초로
  idleSec              totalDurationSec - chargingDurationSec (충전이 끝난 뒤 자리를 차지한 시간)

Slack Incoming Webhook 주소는 환경변수 SLACK_WEBHOOK_URL 로 받는다(EC2 의 part1-ev/.env, git 에 올리지 않음).
비어 있으면 알림을 보내지 않고 내용만 출력한다.

Athena 등록(Q5, 콘솔 Query Editor 에서 실행):
    CREATE DATABASE IF NOT EXISTS de9_nwyu;
    CREATE EXTERNAL TABLE IF NOT EXISTS de9_nwyu.ev_charging_agg (
        locationGroup string, anomaly_count bigint)
    STORED AS PARQUET LOCATION 's3://de9-nwyu-lake/gold/ev_charging_agg/';
    SELECT * FROM de9_nwyu.ev_charging_agg ORDER BY anomaly_count DESC, locationGroup;

S3 접근은 EC2 IAM Role 로 한다. 액세스 키를 쓰지 않는다.
"""
import argparse
import json
import os
import sys
import time
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

INITIALS = "nwyu"
TOPIC = "ev-charging-sessions"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "kafka1:9092,kafka2:9092,kafka3:9092")
BUCKET = os.getenv("DE9_BUCKET", "de9-nwyu-lake")
LAKE = os.getenv("DE9_LAKE_BASE", f"s3a://{BUCKET}")
BRONZE_PATH = f"{LAKE}/bronze/ev-charging-sessions"
SILVER_PATH = f"{LAKE}/silver/ev-charging-sessions"
GOLD_PATH = f"{LAKE}/gold/ev_charging_agg"
CHECKPOINT = os.getenv("DE9_CHECKPOINT", "/opt/checkpoints/charger_stream")

IDLE_LIMIT_SEC = 5400    # 완충 후 90분 넘게 자리를 비우지 않으면 포트 점유 이상

# 메시지(JSON) 스키마를 명시한다. 추론하지 않는다
EVENT_SCHEMA = StructType([
    StructField("stationName", StringType()),
    StructField("portNumber", IntegerType()),
    StructField("portType", StringType()),
    StructField("plugType", StringType()),
    StructField("startTime", StringType()),
    StructField("endTime", StringType()),
    StructField("totalDuration", StringType()),
    StructField("chargingDuration", StringType()),
    StructField("energyKwh", DoubleType()),
    StructField("endedBy", StringType()),
])


def log(msg):
    """진행 기록은 stderr 로 보낸다(콘솔 집계표와 섞이지 않게)."""
    print(msg, file=sys.stderr, flush=True)


def hms_to_sec(col):
    """'h:mm:ss' 문자열 -> 초. 시간이 24를 넘어도 된다."""
    part = F.split(col, ":")
    return (part.getItem(0).cast("int") * 3600
            + part.getItem(1).cast("int") * 60
            + part.getItem(2).cast("int"))


def to_bronze(kafka_df):
    """Kafka 레코드를 가공 없이 그대로. dt 는 저장 폴더를 나누는 데만 쓴다."""
    return (kafka_df
            .select(F.col("key").cast("string").alias("key"),
                    F.col("value").cast("string").alias("value"),
                    "topic", "partition", "offset", "timestamp")
            .withColumn("dt", F.substring(F.get_json_object("value", "$.startTime"), 1, 10)))


def to_silver(kafka_df):
    """명시적 스키마로 파싱하고 locationGroup · 초 단위 컬럼 · idleSec 를 더한다."""
    return (kafka_df
            .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
            .select("e.*")
            .withColumn("locationGroup", F.regexp_replace("stationName", r"\s+#\d+$", ""))
            .withColumn("totalDurationSec", hms_to_sec(F.col("totalDuration")))
            .withColumn("chargingDurationSec", hms_to_sec(F.col("chargingDuration")))
            .withColumn("idleSec", F.col("totalDurationSec") - F.col("chargingDurationSec"))
            .withColumn("dt", F.substring("startTime", 1, 10)))


def send_slack(text):
    """Slack Incoming Webhook 으로 한 건 보낸다. 주소가 없으면 보내지 않는다. 주소는 출력하지 않는다."""
    url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not url:
        return False
    request = urllib.request.Request(
        url,
        data=json.dumps({"text": text}).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status == 200
    except Exception as exc:
        log(f"[slack] 전송 실패: {type(exc).__name__}")
        return False


def alert_anomalies(silver_df, batch_id):
    """Q5: 이 마이크로배치의 포트 점유 이상(idleSec > 5400)을 찾아 Slack 으로 알린다."""
    anomalies = (silver_df
                 .filter(F.col("idleSec") > IDLE_LIMIT_SEC)
                 .select("stationName", "locationGroup", "startTime", "idleSec")
                 .orderBy("startTime", "stationName")
                 .collect())
    sent = 0
    for row in anomalies:
        text = (f"[포트 점유 이상] {row['stationName']} (설치구역: {row['locationGroup']}) "
                f"/ 시작 {row['startTime']} / idleSec={row['idleSec']}")
        if send_slack(text):
            sent += 1
            time.sleep(1)      # Slack 웹훅 속도 제한(초당 1건)을 넘지 않게
        log(text)
    if anomalies:
        where = "Slack 전송" if sent == len(anomalies) else f"Slack 전송 {sent}건 (SLACK_WEBHOOK_URL 확인)"
        log(f"[alert] batch {batch_id}: 이상 {len(anomalies)}건 감지 -> {where}")
    return len(anomalies)


def write_lake(batch_df, batch_id):
    """lake 쿼리의 foreachBatch: 같은 마이크로배치를 Bronze 와 Silver 두 곳에 쓰고, 이상을 알린다."""
    batch_df.persist()
    try:
        rows = batch_df.count()
        if rows == 0:
            return
        to_bronze(batch_df).write.mode("append").partitionBy("dt").parquet(BRONZE_PATH)
        silver = to_silver(batch_df)
        silver.write.mode("append").partitionBy("dt").parquet(SILVER_PATH)
        log(f"[lake] batch {batch_id}: {rows}건 -> bronze, silver")
        alert_anomalies(silver, batch_id)
    finally:
        batch_df.unpersist()


def write_gold(agg_df, batch_id):
    """gold 쿼리의 foreachBatch: 설치구역별 이상 건수(지금까지의 누적)를 통째로 덮어쓴다."""
    (agg_df
     .coalesce(1)
     .sortWithinPartitions(F.desc("anomaly_count"), "locationGroup")
     .write.mode("overwrite").parquet(GOLD_PATH))
    total = agg_df.agg(F.sum("anomaly_count")).first()[0] or 0
    log(f"[gold] batch {batch_id}: 설치구역 {agg_df.count()}곳, 이상 {total}건 -> gold")


def main():
    parser = argparse.ArgumentParser(description="충전 세션 스트리밍 (Bronze/Silver/Gold + 실시간 집계 + 이상 알림)")
    parser.add_argument("--max-offsets", type=int, default=60,
                        help="한 트리거에 읽을 최대 메시지 수 (쌓여 있는 토픽도 여러 주기로 나눠 처리)")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--stop-when-idle", type=int, default=0,
                        help="이 초 동안 새 데이터가 없으면 종료. 0 이면 Ctrl+C 까지 계속 실행")
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"charger_stream_{INITIALS}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log(f"topic={TOPIC}  bronze={BRONZE_PATH}  silver={SILVER_PATH}  gold={GOLD_PATH}")
    log(f"checkpoint={CHECKPOINT}  Slack 알림={'켜짐' if os.getenv('SLACK_WEBHOOK_URL', '').strip() else '꺼짐(SLACK_WEBHOOK_URL 없음)'}")

    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", args.max_offsets)
                .load())
    trigger = f"{args.trigger_seconds} seconds"

    lake_query = (kafka_df.writeStream
                  .queryName("lake")
                  .foreachBatch(write_lake)
                  .option("checkpointLocation", f"{CHECKPOINT}/lake")
                  .trigger(processingTime=trigger)
                  .start())

    by_location = (to_silver(kafka_df)
                   .groupBy("locationGroup")
                   .agg(F.count("*").alias("sessions"))
                   .orderBy(F.desc("sessions"), "locationGroup"))
    console_query = (by_location.writeStream
                     .queryName("console")
                     .outputMode("complete")
                     .format("console")
                     .option("truncate", "false")      # 설치구역 이름이 잘리지 않게
                     .option("numRows", 50)
                     .option("checkpointLocation", f"{CHECKPOINT}/console")
                     .trigger(processingTime=trigger)
                     .start())

    # Q5 Gold: 이상 세션만 걸러 설치구역별 건수를 누적 집계한다
    anomaly_by_location = (to_silver(kafka_df)
                           .filter(F.col("idleSec") > IDLE_LIMIT_SEC)
                           .groupBy("locationGroup")
                           .agg(F.count("*").alias("anomaly_count")))
    gold_query = (anomaly_by_location.writeStream
                  .queryName("gold")
                  .outputMode("complete")
                  .foreachBatch(write_gold)
                  .option("checkpointLocation", f"{CHECKPOINT}/gold")
                  .trigger(processingTime=trigger)
                  .start())

    queries = [lake_query, console_query, gold_query]
    idle_since = None
    try:
        while all(q.isActive for q in queries):
            time.sleep(2)
            if args.stop_when_idle <= 0:
                continue
            started = all(q.lastProgress is not None for q in queries)
            busy = any(q.status["isDataAvailable"] or q.status["isTriggerActive"] for q in queries)
            if not started or busy:
                idle_since = None
            elif idle_since is None:
                idle_since = time.time()
            elif time.time() - idle_since >= args.stop_when_idle:
                log(f"{args.stop_when_idle}초 동안 새 데이터가 없어 종료합니다.")
                break
        for q in queries:
            if q.exception() is not None:
                raise q.exception()
    except KeyboardInterrupt:
        log("중단 요청을 받아 종료합니다.")
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
