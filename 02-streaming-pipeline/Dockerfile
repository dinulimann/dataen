FROM python:3.11-slim-bookworm

# Spark 4 butuh Java 17+.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=python PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

# Unduh connector Kafka untuk Spark saat build, supaya container tidak perlu internet saat start.
RUN python -c "from orders_stream.job import KAFKA_PACKAGE as P; from pyspark.sql import SparkSession; SparkSession.builder.config('spark.jars.packages', P).getOrCreate().stop()"
