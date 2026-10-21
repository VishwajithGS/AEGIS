#!/usr/bin/env bash
# Starts standalone HBase (master + regionserver + ZooKeeper) and the Thrift
# server, then supervises both. If either process exits the container exits,
# so Docker reports the failure instead of serving a half-working database.
set -uo pipefail

LOG_DIR="${HBASE_HOME}/logs"
mkdir -p "$LOG_DIR"

shutdown() {
  echo "[aegis-hbase] stopping thrift and hbase..."
  "${HBASE_HOME}/bin/hbase-daemon.sh" stop thrift || true
  "${HBASE_HOME}/bin/stop-hbase.sh" || true
  exit 0
}
trap shutdown SIGTERM SIGINT

echo "[aegis-hbase] starting HBase ${HBASE_VERSION} in standalone mode"
"${HBASE_HOME}/bin/start-hbase.sh"

# Wait for the master UI before starting Thrift.
for i in $(seq 1 60); do
  if curl -fs http://localhost:16010/master-status >/dev/null 2>&1; then
    echo "[aegis-hbase] master is up"
    break
  fi
  sleep 2
done

"${HBASE_HOME}/bin/hbase-daemon.sh" start thrift -p 9090 --infoport 9095
echo "[aegis-hbase] thrift server started on 9090"

# Stream logs to docker logs.
tail -F "$LOG_DIR"/*.log &

while true; do
  if ! pgrep -f "org.apache.hadoop.hbase.master.HMaster" >/dev/null; then
    echo "[aegis-hbase] HMaster process exited"; exit 1
  fi
  if ! pgrep -f "org.apache.hadoop.hbase.thrift.ThriftServer" >/dev/null; then
    echo "[aegis-hbase] Thrift server exited"; exit 1
  fi
  sleep 10 &
  wait $!
done
