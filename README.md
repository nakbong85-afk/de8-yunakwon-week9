https://github.com/nakbong85-afk/de8-yunakwon-week9

# 데엔 8기 유낙원 — 9주차 과제

Kafka(AWS EC2) 실시간 스트리밍 + S3 Bronze/Silver/Gold 배치 파이프라인.
모든 구성요소(Kafka 3 broker, kafka-ui, Spark standalone, Airflow)는 EC2 한 대(t3.large, Ubuntu 22.04)에서
`part1-ev/docker-compose.yaml` 하나로 실행한다. AWS 접근은 EC2 에 연결한 IAM Role `de9-ec2-nwyu` 로만 하고,
액세스 키는 어디에도 두지 않는다.

| 식별자 | 값 |
|---|---|
| 이니셜 | `nwyu` |
| Kafka 컨슈머 그룹 | `ev-consumer-nwyu` |
| Athena 데이터베이스 | `de9_nwyu` |
| Airflow DAG ID | `price_pipeline_nwyu` |
| EC2 IAM Role | `de9-ec2-nwyu` |
| S3 버킷 | `de9-nwyu-lake` |

## 구성

| 문항 | 파일 | 내용 |
|---|---|---|
| Q1 | `part1-ev/docker-compose.yaml`, `part1-ev/requirements.txt`, `part2-price/airflow/Dockerfile` | EC2 환경 |
| Q2 | `part1-ev/charger_producer.py` | 충전 세션 CSV 를 startTime 순으로 replay (acks=all, 멱등성, 키=stationName) |
| Q3 | `part1-ev/charger_consumer.py` | 그룹 컨슈머, 수동 오프셋 커밋, endedBy 별 건수 |
| Q4 · Q5 | `part1-ev/charger_stream.py` | Structured Streaming: Bronze/Silver 적재, 설치구역별 집계, 이상 감지 Slack 알림, Gold |
| Q6 | `part2-price/price_producer.py` | 가격 Producer + 권역별 커스텀 파티셔너 (kafka-python) |
| Q7 · Q8 | `part2-price/price_batch.py` | Spark 배치: Bronze/Silver, 품목·월별 집계, 윈도우 함수 변동률 순위 Gold |
| Q9 | `part2-price/dags/price_pipeline_dag.py` | SparkSubmitOperator -> Athena 등록 -> 조회(실행 ID 를 XCom 으로) |
| Q10 | `part2-price/kafka_lag_monitor.py` | 컨슈머 그룹의 파티션별 lag |

## 실행 순서 (EC2 호스트, 레포 루트 `~/de9` 에서)

```bash
# 준비
pip3 install --user -r part1-ev/requirements.txt
(cd part1-ev && docker compose up -d --build)

# Q1  토픽 생성
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
  --create --topic ev-charging-sessions --partitions 3 --replication-factor 2

# Q2  Producer (한 번만)
python3 part1-ev/charger_producer.py

# Q3  컨슈머 2개 동시 실행
python3 part1-ev/charger_consumer.py --name c1 & python3 part1-ev/charger_consumer.py --name c2 & wait

# Q4 · Q5  스트리밍 (Ctrl+C 로 종료)
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/de9/part1-ev/charger_stream.py

# Q6  토픽 생성 + Producer (한 번만)
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
  --create --topic product-prices --partitions 3 --replication-factor 2
python3 part2-price/price_producer.py

# Q7 · Q8  배치
docker exec spark-master /opt/spark/bin/spark-submit /opt/de9/part2-price/price_batch.py

# Q9  Airflow UI(8090)에서 price_pipeline_nwyu 를 켠다. 실행 전 Q4 스트리밍은 종료한다

# Q10  컨슈머를 모두 끈 뒤 오프셋 리셋, lag 조회
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:9092 \
  --group ev-consumer-nwyu --topic ev-charging-sessions --reset-offsets --to-earliest --execute
python3 part2-price/kafka_lag_monitor.py ev-consumer-nwyu
```

Slack Incoming Webhook 주소(Q5)는 EC2 의 `part1-ev/.env` 에 `SLACK_WEBHOOK_URL=...` 로만 두며 git 에 올리지 않는다.
