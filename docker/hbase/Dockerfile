# AEGIS HBase image: official Apache HBase binary, standalone mode
# (HMaster + RegionServer + embedded ZooKeeper in one JVM) plus the Thrift
# server that the Python client (happybase) talks to.
FROM eclipse-temurin:11-jre-jammy

ARG HBASE_VERSION=2.5.16
ENV HBASE_VERSION=${HBASE_VERSION} \
    HBASE_HOME=/opt/hbase \
    HBASE_HEAPSIZE=1G \
    HBASE_MANAGES_ZK=true \
    PATH=/opt/hbase/bin:$PATH

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates procps netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*

# Download from the Apache CDN (falls back to the archive for older releases)
# and verify the published SHA-512 before unpacking.
RUN set -eux; \
    f="hbase-${HBASE_VERSION}-bin.tar.gz"; \
    curl -fSL -o "/tmp/$f" "https://dlcdn.apache.org/hbase/${HBASE_VERSION}/$f" \
      || curl -fSL -o "/tmp/$f" "https://archive.apache.org/dist/hbase/${HBASE_VERSION}/$f"; \
    curl -fsSL -o /tmp/hbase.sha512 "https://downloads.apache.org/hbase/${HBASE_VERSION}/$f.sha512" \
      || curl -fsSL -o /tmp/hbase.sha512 "https://archive.apache.org/dist/hbase/${HBASE_VERSION}/$f.sha512"; \
    expected="$(sed 's/^[^:]*://' /tmp/hbase.sha512 | tr -d ' \n\r\t' | tr 'A-F' 'a-f')"; \
    actual="$(sha512sum "/tmp/$f" | cut -d' ' -f1)"; \
    [ "$expected" = "$actual" ] || { echo "SHA-512 mismatch for $f"; exit 1; }; \
    tar -xzf "/tmp/$f" -C /opt; \
    mv "/opt/hbase-${HBASE_VERSION}" "$HBASE_HOME"; \
    rm -rf "/tmp/$f" /tmp/hbase.sha512 "$HBASE_HOME/docs"; \
    mkdir -p /data/hbase /data/zookeeper "$HBASE_HOME/logs"

COPY hbase-site.xml ${HBASE_HOME}/conf/hbase-site.xml
COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

# 16010 master UI, 9090 Thrift API, 9095 Thrift info UI, 2181 ZooKeeper
EXPOSE 16010 9090 9095 2181
VOLUME ["/data"]

HEALTHCHECK --interval=15s --timeout=5s --start-period=60s --retries=10 \
  CMD nc -z localhost 9090 && curl -fs http://localhost:16010/master-status >/dev/null || exit 1

ENTRYPOINT ["/entrypoint.sh"]
