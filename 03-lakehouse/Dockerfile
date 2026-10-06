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

# Unduh jar Delta Lake saat build, supaya container tidak perlu internet saat dijalankan.
RUN python -c "from taxi_lakehouse.spark import build_spark; build_spark().stop()"

ENTRYPOINT ["python", "-m", "taxi_lakehouse"]
