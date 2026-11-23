#!/usr/bin/env python3
"""Q7 · Q8 — Spark 배치: Kafka product-prices -> S3 Bronze / Silver / Gold

토픽에 들어 있는 메시지를 처음부터 끝까지 한 번 읽어 처리하고 종료한다(스트리밍 아님: spark.read).
Q9 에서 Airflow 가 같은 스크립트를 주기적으로 실행하므로, 매번 덮어써서 몇 번을 돌려도 결과가 같다.

    docker exec spark-master /opt/spark/bin/spark-submit /opt/de9/part2-price/price_batch.py

  Bronze : Kafka 원본 메시지 그대로            -> s3://<버킷>/bronze/product-prices/
  Silver : 파싱, price 실수형, 값 없음·0 이하 제외 -> s3://<버킷>/silver/product-prices/
  콘솔   : 품목(item_name)·월(month)별 평균가 · 최고가 · 최저가
  Gold   : (Q8) 도시 x 품목별 전월(2016-11) 대비 당월(2016-12) 변동률과 도시 내 순위
                                                 -> s3://<버킷>/gold/grocery_price_change/
           컬럼 = market, item_name, price_nov, price_dec, change_rate, change_rank (도시 10 x 품목 10 = 100행)
  콘솔   : (Q8) 각 도시의 변동률 1위 품목 10행 (도시명 순)

Q8 의 변동률은 윈도우 함수로 계산한다. 두 달을 따로 걸러 조인하지 않는다.
  lag(price)  over (partition by market, item_name order by year, month)   -> 전월 가격
  rank()      over (partition by market order by |change_rate| desc)       -> 도시 안에서의 순위(상승·하락 무관)

S3 접근은 EC2 IAM Role 로 한다. 액세스 키를 쓰지 않는다.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

INITIALS = "nwyu"
TOPIC = "product-prices"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "kafka1:9092,kafka2:9092,kafka3:9092")
BUCKET = os.getenv("DE9_BUCKET", "de9-nwyu-lake")
LAKE = os.getenv("DE9_LAKE_BASE", f"s3a://{BUCKET}")
BRONZE_PATH = f"{LAKE}/bronze/product-prices"
SILVER_PATH = f"{LAKE}/silver/product-prices"
GOLD_PATH = f"{LAKE}/gold/grocery_price_change"

BASE_YEAR, BASE_MONTH = 2016, 12     # 기준월(당월). 전월은 2016-11

MESSAGE_SCHEMA = StructType([
    StructField("item_name", StringType()),
    StructField("market", StringType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("price", DoubleType()),
    StructField("currency", StringType()),
    StructField("unit", StringType()),
])


def read_topic(spark):
    """Kafka 토픽을 배치로 읽는다. 지금 있는 데이터(earliest~latest)만 읽고 끝난다."""
    return (spark.read.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load())


def to_bronze(kafka_df):
    """원본 메시지를 가공 없이 그대로(키·값 문자열 + Kafka 메타데이터)."""
    return kafka_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        "topic", "partition", "offset", "timestamp")


def to_silver(bronze_df):
    """JSON 을 파싱하고 price 를 실수형으로. 값이 없거나 0 이하인 레코드는 뺀다."""
    return (bronze_df
            .select(F.from_json("value", MESSAGE_SCHEMA).alias("m"))
            .select("m.*")
            .withColumn("price", F.col("price").cast("double"))
            .filter(F.col("price").isNotNull() & (F.col("price") > 0)))


def item_month_stats(silver_df):
    """품목·월별 평균가·최고가·최저가.

    평균은 십진수(decimal)로 더해서 구한다. 실수(double)를 그대로 더하면 더하는 순서에 따라
    끝자리 오차가 생겨, 평균이 정확히 20.295 인 칸(Tomatoes 11월)이 20.29 로 내려가기도 한다.
    """
    exact_price = F.col("price").cast("decimal(18,4)")
    return (silver_df
            .groupBy("item_name", "month")
            .agg(F.round(F.avg(exact_price), 2).cast("double").alias("avg_price"),
                 F.max("price").alias("max_price"),
                 F.min("price").alias("min_price"))
            .orderBy("item_name", "month"))


def price_change_rank(silver_df):
    """Q8: 도시 x 품목별 전월 대비 변동률(%)과 도시 내 순위. 윈도우 함수만 쓴다."""
    by_series = Window.partitionBy("market", "item_name").orderBy("year", "month")
    with_prev = (silver_df
                 .withColumn("prev_price", F.lag("price").over(by_series))      # 바로 앞 달 가격
                 .withColumn("prev_month", F.lag("month").over(by_series)))
    change = (with_prev
              .filter((F.col("year") == BASE_YEAR) & (F.col("month") == BASE_MONTH)
                      & (F.col("prev_month") == BASE_MONTH - 1) & F.col("prev_price").isNotNull())
              .select("market", "item_name",
                      F.col("prev_price").alias("price_nov"),
                      F.col("price").alias("price_dec"))
              .withColumn("change_rate",
                          F.round((F.col("price_dec") - F.col("price_nov")) / F.col("price_nov") * 100, 2)))
    by_market = Window.partitionBy("market").orderBy(F.abs(F.col("change_rate")).desc(), F.col("item_name"))
    by_market_rate = Window.partitionBy("market").orderBy(F.abs(F.col("change_rate")).desc())
    return (change
            .withColumn("change_rank", F.rank().over(by_market_rate))
            .withColumn("_order", F.row_number().over(by_market))
            .orderBy("market", "_order")
            .drop("_order"))


def main():
    spark = SparkSession.builder.appName(f"price_batch_{INITIALS}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    bronze = to_bronze(read_topic(spark)).cache()
    bronze_rows = bronze.count()
    bronze.write.mode("overwrite").parquet(BRONZE_PATH)
    print(f"bronze 적재: {bronze_rows}건 -> {BRONZE_PATH}/")

    silver = to_silver(bronze).cache()
    silver_rows = silver.count()
    silver.write.mode("overwrite").parquet(SILVER_PATH)
    print(f"silver 적재: {silver_rows}건 -> {SILVER_PATH}/")

    print("=== Q7 품목별 · 월별 평균/최고/최저가 ===")
    item_month_stats(silver).show(100, truncate=False)

    gold = price_change_rank(silver).cache()
    gold_rows = gold.count()
    gold.coalesce(1).write.mode("overwrite").parquet(GOLD_PATH)
    print(f"gold 적재: {gold_rows}건 -> {GOLD_PATH}/")

    print("=== Q8 도시별 변동률 1위 ===")
    (gold.filter(F.col("change_rank") == 1)
         .orderBy("market", "item_name")
         .show(100, truncate=False))

    spark.stop()


if __name__ == "__main__":
    main()
