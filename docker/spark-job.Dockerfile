FROM bitnami/spark:3.5

LABEL maintainer="mauricio.esquivel1806@gmail.com"
LABEL description="CryptoStream Realtime - Spark Streaming Jobs"

USER root

# Install Python dependencies for Spark jobs
COPY pyproject.toml /app/pyproject.toml
RUN pip install --no-cache-dir \
    delta-spark==3.2.0 \
    boto3>=1.28.0 \
    pydantic-settings>=2.0.0

# Copy source code
COPY src/ /app/src/

# Add Delta + Kafka + Hadoop-AWS packages
ENV SPARK_EXTRA_PACKAGES="io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4"

USER 1001

WORKDIR /app
