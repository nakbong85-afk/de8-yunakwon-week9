# Spark 클러스터에 작업을 제출할 수 있는 Airflow 이미지 (Q9)
#   - Airflow 2.10.5 / Python 3.10  (Spark 이미지 apache/spark:3.5.3-java17-python3 도 Python 3.10)
#   - JDK 17 + pyspark 3.5.3  -> spark-submit (client 모드, 드라이버가 이 컨테이너에서 돈다)
#   - Kafka 소스와 S3(s3a) 용 jar 를 /opt/spark-jars 에 넣어 두고 DAG 에서 --jars 로 넘긴다
# 빌드: part1-ev/docker-compose.yaml 의 airflow 서비스가 이 폴더를 context 로 빌드한다
FROM apache/airflow:2.10.5-python3.10

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# spark-sql-kafka 와 그 의존 jar, hadoop-aws 와 AWS SDK (Spark 3.5.3 / Hadoop 3.3.4 에 맞는 버전)
RUN mkdir -p /opt/spark-jars && cd /opt/spark-jars \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && chmod 644 /opt/spark-jars/*.jar

# Airflow 메타 DB(SQLite)를 볼륨에 두기 위한 폴더
RUN mkdir -p /opt/airflow/db && chown airflow:root /opt/airflow/db

USER airflow
RUN pip install --no-cache-dir \
      "apache-airflow==2.10.5" \
      "apache-airflow-providers-apache-spark==5.0.0" \
      "apache-airflow-providers-amazon==9.2.0" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.10.txt" \
 && pip install --no-cache-dir "pyspark==3.5.3"
