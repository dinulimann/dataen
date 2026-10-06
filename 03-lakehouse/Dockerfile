FROM python:3.11-slim-bookworm

# Spark 4 butuh Java 17+. JRE disalin dari image Temurin resmi (multi-stage),
# jadi tidak perlu apt-get: build lebih cepat dan image lebih kecil.
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk PATH=/opt/java/openjdk/bin:$PATH \
    PYSPARK_PYTHON=python PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

# Unduh jar Delta Lake saat build, supaya container tidak perlu internet saat dijalankan.
RUN python -c "from taxi_lakehouse.spark import build_spark; build_spark().stop()"

ENTRYPOINT ["python", "-m", "taxi_lakehouse"]
