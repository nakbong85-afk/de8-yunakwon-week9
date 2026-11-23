#!/usr/bin/env python3
"""Q2 — 충전 세션 Producer (실측 로그 replay)

data/charger_sessions.csv 를 startTime 순으로 정렬해 한 행씩, 0.05~0.2초 간격으로
Kafka 토픽 ev-charging-sessions 에 보낸다. EC2 호스트에서 실행한다.

    python3 part1-ev/charger_producer.py

유실·중복 방지 설정
  - acks=all                : 리더와 ISR 복제본이 모두 받아야 성공으로 본다 (브로커 1대가 죽어도 유실 없음)
  - enable_idempotence=True : 재시도로 같은 배치가 두 번 가도 브로커가 한 번만 저장한다 (중복 없음)
  - retries / max_in_flight_requests_per_connection<=5 : 멱등 Producer 의 전제 조건
파티션 키
  - key = stationName       : 같은 충전기의 세션은 항상 같은 파티션으로 간다 (순서 보장)

토픽에 이미 메시지가 있으면 보내지 않고 멈춘다. 같은 데이터를 두 번 보내면
이 토픽을 읽는 Q3·Q4·Q5 의 결과가 전부 달라지기 때문이다.
"""
import argparse
import csv
import json
import os
import random
import sys
import time
from collections import Counter

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092,localhost:29092,localhost:39092").split(",")
TOPIC = "ev-charging-sessions"
DEFAULT_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "charger_sessions.csv")


def load_rows(path):
    """CSV 를 읽어 startTime 순으로 정렬한다. 시각이 같으면 파일 순서를 지킨다(안정 정렬)."""
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["portNumber"] = int(r["portNumber"])
        r["energyKwh"] = float(r["energyKwh"])
    rows.sort(key=lambda r: r["startTime"])
    return rows


def topic_message_count():
    """토픽에 지금 들어 있는 메시지 수. 토픽이 없으면 None."""
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, client_id="charger-producer-nwyu-check")
    try:
        partitions = consumer.partitions_for_topic(TOPIC)
        if not partitions:
            return None
        tps = [TopicPartition(TOPIC, p) for p in sorted(partitions)]
        begin = consumer.beginning_offsets(tps)
        end = consumer.end_offsets(tps)
        return sum(end[tp] - begin[tp] for tp in tps)
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description="충전 세션 CSV 를 Kafka 로 replay")
    parser.add_argument("--csv", default=DEFAULT_CSV)
    parser.add_argument("--force", action="store_true", help="토픽에 메시지가 있어도 보낸다(중복 저장됨)")
    args = parser.parse_args()

    rows = load_rows(args.csv)
    print(f"bootstrap={','.join(BOOTSTRAP)}  topic={TOPIC}")
    print(f"CSV {len(rows)}행  startTime {rows[0]['startTime']} ~ {rows[-1]['startTime']}")

    existing = topic_message_count()
    if existing is None:
        sys.exit(f"토픽 {TOPIC} 이 없습니다. 먼저 파티션 3 · 복제 2 로 만드세요.")
    if existing > 0 and not args.force:
        sys.exit(f"토픽 {TOPIC} 에 이미 {existing}건이 있습니다. 중복 전송을 막기 위해 중단합니다. "
                 "(다시 보내려면 토픽을 지우고 같은 설정으로 다시 만드세요)")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        client_id="charger-producer-nwyu",
        acks="all",
        enable_idempotence=True,
        retries=10,
        max_in_flight_requests_per_connection=5,
        delivery_timeout_ms=120000,
        linger_ms=5,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    futures = []
    per_partition = Counter()
    try:
        for i, row in enumerate(rows, 1):
            futures.append(producer.send(TOPIC, key=row["stationName"], value=row))
            if i % 25 == 0 or i == len(rows):
                print(f"  {i:3d}/{len(rows)}  {row['startTime']}  {row['stationName']}")
            time.sleep(random.uniform(0.05, 0.2))
        # 브로커가 실제로 저장했는지(ack) 한 건씩 확인한다. 실패하면 여기서 예외가 난다
        for fut in futures:
            meta = fut.get(timeout=60)
            per_partition[meta.partition] += 1
    finally:
        producer.flush()
        producer.close()

    print(f"전송 완료: {sum(per_partition.values())}건 -> topic={TOPIC}")
    for p in sorted(per_partition):
        print(f"  partition {p}: {per_partition[p]}")


if __name__ == "__main__":
    main()
