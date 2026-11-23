#!/usr/bin/env python3
"""Q3 — 순수 파이썬 Kafka Consumer (그룹 · 수동 오프셋 커밋 · 리밸런싱)

토픽 ev-charging-sessions 를 그룹 ev-consumer-nwyu 로 읽어 endedBy 별 건수를 센다.
인스턴스 2개를 동시에 띄우면 파티션 3개를 나눠 갖는다(리밸런싱).

    python3 part1-ev/charger_consumer.py --name c1 &
    python3 part1-ev/charger_consumer.py --name c2 &
    wait

오프셋
  - enable_auto_commit=False : 자동 커밋을 쓰지 않는다
  - 한 파티션에서 가져온 묶음을 다 처리한 뒤, 마지막으로 처리한 메시지의 다음 오프셋만 커밋한다
    (처리하지 못한 메시지의 오프셋은 올라가지 않으므로, 죽었다 살아나면 거기서부터 다시 읽는다)
  - 파티션을 빼앗기기 직전(on_partitions_revoked)에도 처리한 데까지 커밋한다

--idle-exit 초 동안 새 메시지가 없으면 집계를 출력하고 끝난다.
"""
import argparse
import json
import os
import time
from collections import Counter

from kafka import ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata, TopicPartition

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092,localhost:29092,localhost:39092").split(",")
TOPIC = "ev-charging-sessions"
GROUP_ID = "ev-consumer-nwyu"


class Listener(ConsumerRebalanceListener):
    """리밸런싱 때 할당 변화를 출력하고, 빼앗기기 전에 처리한 데까지 커밋한다."""

    def __init__(self, name, consumer, done_offsets):
        self.name = name
        self.consumer = consumer
        self.done_offsets = done_offsets   # {TopicPartition: 다음에 읽을 오프셋}
        self.assigned_history = []

    def on_partitions_revoked(self, revoked):
        if not revoked:
            return
        pending = {tp: OffsetAndMetadata(off, "", -1) for tp, off in self.done_offsets.items() if tp in revoked}
        if pending:
            try:
                self.consumer.commit(pending)
            except Exception as exc:   # 이미 세대가 바뀌었으면 커밋이 거절될 수 있다
                print(f"[{self.name}] 회수 전 커밋 실패: {exc}", flush=True)
        print(f"[{self.name}] [회수 파티션] {sorted(tp.partition for tp in revoked)}", flush=True)

    def on_partitions_assigned(self, assigned):
        parts = sorted(tp.partition for tp in assigned)
        self.assigned_history.append(parts)
        print(f"[{self.name}] [할당 파티션] {parts}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="endedBy 별 건수를 세는 그룹 컨슈머")
    parser.add_argument("--name", default=f"pid{os.getpid()}", help="출력에 붙일 인스턴스 이름")
    parser.add_argument("--idle-exit", type=float, default=30.0, help="이 초 동안 메시지가 없으면 종료")
    parser.add_argument("--delay", type=float, default=0.03, help="메시지 한 건의 처리 시간(초)")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        client_id=f"{GROUP_ID}-{args.name}",
        enable_auto_commit=False,          # 오프셋은 직접 커밋한다
        auto_offset_reset="earliest",      # 커밋된 오프셋이 없으면 처음부터
        max_poll_records=20,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    done_offsets = {}
    listener = Listener(args.name, consumer, done_offsets)
    consumer.subscribe([TOPIC], listener=listener)
    print(f"[{args.name}] group_id={GROUP_ID} 소비 시작 (topic={TOPIC})", flush=True)

    ended_by = Counter()
    per_partition = Counter()
    last_message = time.time()
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000)
            if not batch:
                if time.time() - last_message > args.idle_exit:
                    break
                continue
            for tp, records in batch.items():
                for rec in records:
                    ended_by[rec.value["endedBy"]] += 1      # 처리
                    per_partition[tp.partition] += 1
                    done_offsets[tp] = rec.offset + 1
                    time.sleep(args.delay)
                # 이 파티션에서 처리를 마친 데까지만 커밋
                consumer.commit({tp: OffsetAndMetadata(done_offsets[tp], "", -1)})
            last_message = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        total = sum(ended_by.values())
        lines = [
            f"===== consumer {args.name} (group_id={GROUP_ID}) =====",
            f"할당 파티션 변화: {listener.assigned_history}",
            f"파티션별 처리 건수: {dict(sorted(per_partition.items()))}",
            "endedBy 별 건수:",
        ]
        for key in sorted(ended_by, key=lambda k: (-ended_by[k], k)):
            lines.append(f"  {key:<22} {ended_by[key]:>4}")
        lines.append(f"  {'합계':<20} {total:>4}")
        print("\n".join(lines), flush=True)
        consumer.close(autocommit=False)


if __name__ == "__main__":
    main()
