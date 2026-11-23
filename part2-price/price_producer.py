#!/usr/bin/env python3
"""Q6 — 식료품 가격 Producer + 권역별 커스텀 파티셔너 (kafka-python)

data/india_grocery_prices.csv 200행을 토픽 product-prices 로 보낸다. EC2 호스트에서 실행한다.
토픽은 미리 파티션 3 · 복제 2 로 만들어 둔다.

    python3 part2-price/price_producer.py

메시지 키 = market(도시명). 파티션은 send() 에서 직접 지정하지 않고,
Producer 의 partitioner 설정에 넣은 RegionPartitioner 가 키(도시)를 보고 정한다.

    파티션 0  북부        Delhi · Jaipur · Lucknow · Kanpur
    파티션 1  서부·중부   Mumbai · Ahmedabad · Bhopal
    파티션 2  남부·동부   Hyderabad · Chennai · Patna
    그 밖의 도시          기본 파티셔너 규칙(키의 murmur2 해시)

토픽에 이미 메시지가 있으면 보내지 않고 멈춘다(중복 저장 방지).
"""
import argparse
import csv
import json
import os
import sys
from collections import Counter

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.partitioner.default import DefaultPartitioner

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092,localhost:29092,localhost:39092").split(",")
TOPIC = "product-prices"
DEFAULT_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "india_grocery_prices.csv")


class RegionPartitioner:
    """도시가 속한 권역으로 파티션을 정하는 커스텀 파티셔너.

    kafka-python 은 partitioner 설정에 넣은 객체를
    partitioner(직렬화된 키, 전체 파티션 목록, 사용 가능한 파티션 목록) 으로 호출한다.
    """

    REGION_PARTITION = {
        "Delhi": 0, "Jaipur": 0, "Lucknow": 0, "Kanpur": 0,       # 북부
        "Mumbai": 1, "Ahmedabad": 1, "Bhopal": 1,                 # 서부·중부
        "Hyderabad": 2, "Chennai": 2, "Patna": 2,                 # 남부·동부
    }

    def __init__(self):
        self._default = DefaultPartitioner()

    def __call__(self, key, all_partitions, available):
        city = key.decode("utf-8") if key is not None else None
        partition = self.REGION_PARTITION.get(city)
        if partition is not None and partition in all_partitions:
            return partition
        # 표에 없는 도시(또는 키 없음)는 기본 파티셔너 규칙을 따른다
        return self._default(key, all_partitions, available)


def load_rows(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["year"] = int(r["year"])
        r["month"] = int(r["month"])
        r["price"] = float(r["price"])
    return rows


def topic_message_count():
    """토픽에 지금 들어 있는 메시지 수. 토픽이 없으면 None."""
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, client_id="price-producer-nwyu-check")
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
    parser = argparse.ArgumentParser(description="식료품 가격 CSV 를 Kafka 로 전송")
    parser.add_argument("--csv", default=DEFAULT_CSV)
    parser.add_argument("--force", action="store_true", help="토픽에 메시지가 있어도 보낸다(중복 저장됨)")
    args = parser.parse_args()

    rows = load_rows(args.csv)
    existing = topic_message_count()
    if existing is None:
        sys.exit(f"토픽 {TOPIC} 이 없습니다. 먼저 파티션 3 · 복제 2 로 만드세요.")
    if existing > 0 and not args.force:
        sys.exit(f"토픽 {TOPIC} 에 이미 {existing}건이 있습니다. 중복 전송을 막기 위해 중단합니다. "
                 "(다시 보내려면 토픽을 지우고 같은 설정으로 다시 만드세요)")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        client_id="price-producer-nwyu",
        partitioner=RegionPartitioner(),          # 커스텀 파티셔너 지정
        acks="all",
        enable_idempotence=True,
        retries=10,
        max_in_flight_requests_per_connection=1,
        linger_ms=5,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    futures = []
    per_partition = Counter()
    try:
        for row in rows:
            # 파티션 번호를 넘기지 않는다. 키(market)만 주면 RegionPartitioner 가 정한다
            futures.append(producer.send(TOPIC, key=row["market"], value=row))
        for fut in futures:
            meta = fut.get(timeout=60)
            per_partition[meta.partition] += 1
    finally:
        producer.flush()     # 전송 완료 보장
        producer.close()     # 자원 정리

    print(f"전송 완료: {sum(per_partition.values())}건 -> topic={TOPIC}")
    for p in sorted(per_partition):
        print(f"  partition {p}: {per_partition[p]}")


if __name__ == "__main__":
    main()
