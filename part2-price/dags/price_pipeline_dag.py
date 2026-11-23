"""Q9 — 식료품 가격 배치 파이프라인 DAG (EC2 위의 Airflow)

10분마다 아래 세 작업을 순서대로 실행한다.

  1. spark_price_batch    SparkSubmitOperator 로 price_batch.py 를 Spark 클러스터(spark://spark-master:7077)에 제출
                          (Kafka product-prices -> S3 Bronze/Silver/Gold)
  2. register_gold_table  Athena 데이터베이스 de9_nwyu 에 Gold 경로를 가리키는 외부 테이블 등록
  3. query_top1           각 도시의 변동률 1위 품목을 Athena 로 조회하고,
                          그 쿼리 실행 ID(QueryExecutionId)를 XCom(return_value)으로 남긴다
                          -> Athena 콘솔 '최근 쿼리'의 실행 ID 와 대조할 수 있다

AWS 접근은 EC2 IAM Role(de9-ec2-nwyu)로 한다. 연결에 키를 넣지 않는다.
실행이 겹치지 않게 max_active_runs=1, 지난 구간을 몰아 돌리지 않게 catchup=False.
"""
import os

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

INITIALS = "nwyu"
DAG_ID = f"price_pipeline_{INITIALS}"
REGION = "ap-northeast-2"
BUCKET = os.getenv("DE9_BUCKET", "de9-nwyu-lake")

ATHENA_DATABASE = f"de9_{INITIALS}"
ATHENA_TABLE = "grocery_price_change"
ATHENA_WORKGROUP = "primary"
ATHENA_OUTPUT = f"s3://{BUCKET}/athena-results/"
GOLD_LOCATION = f"s3://{BUCKET}/gold/grocery_price_change/"

# Airflow 이미지(part2-price/airflow/Dockerfile)에 넣어 둔 Kafka 소스 · S3(s3a) jar.
# --jars 로 넘기면 드라이버와 익스큐터 양쪽 클래스패스에 올라간다
SPARK_JARS = ",".join(f"/opt/spark-jars/{name}" for name in [
    "spark-sql-kafka-0-10_2.12-3.5.3.jar",
    "spark-token-provider-kafka-0-10_2.12-3.5.3.jar",
    "kafka-clients-3.4.1.jar",
    "commons-pool2-2.11.1.jar",
    "hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar",
])

CREATE_DATABASE_SQL = f"CREATE DATABASE IF NOT EXISTS {ATHENA_DATABASE}"
CREATE_TABLE_SQL = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE} (
    market       string,
    item_name    string,
    price_nov    double,
    price_dec    double,
    change_rate  double,
    change_rank  int
)
STORED AS PARQUET
LOCATION '{GOLD_LOCATION}'
"""
TOP1_SQL = f"""
SELECT market, item_name, price_nov, price_dec, change_rate, change_rank
FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
WHERE change_rank = 1
ORDER BY market
"""

with DAG(
    dag_id=DAG_ID,
    description="Kafka product-prices -> Spark 배치(Bronze/Silver/Gold) -> Athena 등록·조회",
    schedule="*/10 * * * *",                       # 10분 주기
    start_date=pendulum.datetime(2026, 11, 23, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["de9", INITIALS, "price"],
) as dag:

    spark_price_batch = SparkSubmitOperator(
        task_id="spark_price_batch",
        conn_id="spark_default",                   # spark://spark-master:7077 (compose 의 AIRFLOW_CONN_SPARK_DEFAULT)
        application="/opt/de9/part2-price/price_batch.py",
        name=f"price_batch_{INITIALS}_airflow",
        jars=SPARK_JARS,
        driver_memory="512m",
        executor_memory="1g",
        total_executor_cores=2,
        conf={
            # client 모드: 드라이버가 airflow 컨테이너에서 돌므로 익스큐터가 되돌아올 주소를 알려 준다
            "spark.driver.host": "airflow",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.sql.shuffle.partitions": "2",
            "spark.ui.showConsoleProgress": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
            "spark.hadoop.fs.s3a.endpoint": f"s3.{REGION}.amazonaws.com",
        },
        env_vars={"DE9_BUCKET": BUCKET},
    )

    @task(task_id="register_gold_table")
    def register_gold_table():
        """데이터베이스가 없으면 만들고, Gold 경로를 가리키는 외부 테이블을 등록한다."""
        hook = AthenaHook(aws_conn_id="aws_default", region_name=REGION)
        for sql in (CREATE_DATABASE_SQL, CREATE_TABLE_SQL):
            query_id = hook.run_query(
                query=sql,
                query_context={"Database": "default"},
                result_configuration={"OutputLocation": ATHENA_OUTPUT},
                workgroup=ATHENA_WORKGROUP,
            )
            state = hook.poll_query_status(query_id, max_polling_attempts=60, sleep_time=2)
            if state != "SUCCEEDED":
                raise RuntimeError(f"Athena DDL 실패: state={state}, query_execution_id={query_id}")
        return f"{ATHENA_DATABASE}.{ATHENA_TABLE} -> {GOLD_LOCATION}"

    # AthenaOperator 는 쿼리가 끝나면 QueryExecutionId 를 반환한다 -> XCom return_value
    query_top1 = AthenaOperator(
        task_id="query_top1",
        query=TOP1_SQL,
        database=ATHENA_DATABASE,
        output_location=ATHENA_OUTPUT,
        workgroup=ATHENA_WORKGROUP,
        aws_conn_id="aws_default",
        region_name=REGION,
        sleep_time=3,
        do_xcom_push=True,
    )

    spark_price_batch >> register_gold_table() >> query_top1
