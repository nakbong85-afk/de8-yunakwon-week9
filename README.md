https://github.com/nakbong85-afk/de8-yunakwon-week9

# 데엔 8기 유낙원 — 9주차 과제

Kafka(AWS EC2) 실시간 스트리밍 + S3 Bronze/Silver/Gold 배치 파이프라인.
모든 구성요소(Kafka 3 broker, kafka-ui, Spark standalone, Airflow)는 EC2 한 대(t3.large, Ubuntu 22.04)에서
`part1-ev/docker-compose.yaml` 하나로 실행한다. AWS 접근은 EC2 에 연결한 IAM Role `de9-ec2-nwyu` 로만 한다.

| 식별자 | 값 |
|---|---|
| 이니셜 | `nwyu` |
| Kafka 컨슈머 그룹 | `ev-consumer-nwyu` |
| Athena 데이터베이스 | `de9_nwyu` |
| Airflow DAG ID | `price_pipeline_nwyu` |
| EC2 IAM Role | `de9-ec2-nwyu` |
| S3 버킷 | `de9-nwyu-lake` |

## 실행 순서 (EC2 호스트, 레포 루트에서)

```bash
# 0. 준비
pip3 install --user -r part1-ev/requirements.txt
cd part1-ev && docker compose up -d --build && cd ..
```
