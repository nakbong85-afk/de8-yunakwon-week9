#!/usr/bin/env python3
"""Q10 — 컨슈머 그룹 lag 모니터

그룹 이름을 인자로 받아, 그 그룹이 커밋한 오프셋(current-offset)과
파티션의 끝 오프셋(log-end-offset)을 비교해 파티션별 lag 와 합계를 표로 출력한다.
EC2 호스트에서 실행한다.

    python3 part2-price/kafka_lag_monitor.py ev-consumer-nwyu

  current-offset : 그룹이 다음에 읽을 위치(커밋된 오프셋). 커밋이 없으면 '-'
  log-end-offset : 파티션에 다음 메시지가 쓰일 위치
  lag            : log-end-offset - current-offset (아직 읽지 않은 메시지 수)
"""
import argparse
import os
import sys

from kafka import KafkaAdminClient, KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092,localhost:29092,localhost:39092").split(",")


def collect_lag(group):
    """[(topic, partition, current, end, lag)] 를 topic, partition 순으로 돌려준다."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="kafka-lag-monitor-nwyu")
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, client_id="kafka-lag-monitor-nwyu")
    try:
        committed = admin.list_consumer_group_offsets(group)   # {TopicPartition: OffsetAndMetadata}
        if not committed:
            return []
        tps = sorted(committed, key=lambda tp: (tp.topic, tp.partition))
        end_offsets = consumer.end_offsets(tps)
        begin_offsets = consumer.beginning_offsets(tps)
        rows = []
        for tp in tps:
            current = committed[tp].offset
            end = end_offsets[tp]
            if current is None or current < 0:      # 커밋된 오프셋이 없는 파티션
                rows.append((tp.topic, tp.partition, None, end, end - begin_offsets[tp]))
            else:
                rows.append((tp.topic, tp.partition, current, end, max(end - current, 0)))
        return rows
    finally:
        consumer.close()
        admin.close()


def main():
    parser = argparse.ArgumentParser(description="컨슈머 그룹의 파티션별 lag 조회")
    parser.add_argument("group", help="컨슈머 그룹 이름 (예: ev-consumer-nwyu)")
    args = parser.parse_args()

    rows = collect_lag(args.group)
    print(f"group = {args.group}")
    if not rows:
        sys.exit("이 그룹이 커밋한 오프셋이 없습니다. 그룹 이름을 확인하세요.")

    width = max(len("topic"), max(len(r[0]) for r in rows))
    header = f"{'topic':<{width}}  {'partition':>9}  {'current-offset':>14}  {'log-end-offset':>14}  {'lag':>6}"
    print(header)
    print("-" * len(header))
    for topic, partition, current, end, lag in rows:
        current_text = "-" if current is None else str(current)
        print(f"{topic:<{width}}  {partition:>9}  {current_text:>14}  {end:>14}  {lag:>6}")
    print("-" * len(header))
    print(f"{'TOTAL LAG':<{width}}  {'':>9}  {'':>14}  {'':>14}  {sum(r[4] for r in rows):>6}")


if __name__ == "__main__":
    main()
