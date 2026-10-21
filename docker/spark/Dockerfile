# AEGIS runtime image: Python 3.11 + OpenJDK 17 + PySpark 3.5.9 with the
# Kafka connector pre-installed. Used for Spark jobs, Structured Streaming,
# the FastAPI backend (Spark MLlib inference), producers and tests.
FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.9
ARG SCALA_BINARY=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl tini \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

ARG PYSPARK_SHA256=ea27adc39ddac9413b8951e45aa748cbed6c785971b81386efc41938f6243d93

COPY requirements/spark.txt /tmp/requirements.txt
# The PySpark sdist is ~318 MB. On slow links it can be pre-downloaded to
# docker/spark/wheels/pyspark-<version>.tar.gz (see docker/spark/wheels/README.md);
# it is then checksum-verified and installed from there via a bind mount (not
# copied into a layer). Otherwise pip downloads it, with a long timeout + retries.
RUN --mount=type=bind,source=docker/spark/wheels,target=/tmp/wheels \
    set -eux; \
    pkg="/tmp/wheels/pyspark-${SPARK_VERSION}.tar.gz"; \
    if [ -f "$pkg" ]; then \
      echo "${PYSPARK_SHA256}  $pkg" | sha256sum -c -; \
      pip install --no-cache-dir --timeout 120 --retries 10 "$pkg"; \
    fi; \
    pip install --no-cache-dir --timeout 120 --retries 10 -r /tmp/requirements.txt

# Spark-Kafka connector and its runtime dependencies, placed in PySpark's jar
# directory so no Maven/Ivy resolution is needed at job start.
RUN set -eux; \
    JARS="$(python -c 'import os,pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    M=https://repo1.maven.org/maven2; \
    cd "$JARS"; \
    curl -fsSLO "$M/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar"; \
    curl -fsSLO "$M/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar"; \
    curl -fsSLO "$M/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSLO "$M/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    ls -la "$JARS" | grep -E "kafka|pool2"

WORKDIR /app
ENV PYTHONPATH=/app \
    AEGIS_RUNTIME=docker

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-c", "import pyspark; print('aegis-spark image, pyspark', pyspark.__version__)"]
