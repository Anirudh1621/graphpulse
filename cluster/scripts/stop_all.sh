#!/usr/bin/env bash
set -uo pipefail
source /etc/profile.d/bigdata.sh
hbase-daemon.sh stop thrift || true
stop-hbase.sh || true
for h in master worker1 worker2; do
  ssh "$h" "source /etc/profile.d/bigdata.sh; kafka-server-stop.sh" || true
done
$SPARK_HOME/sbin/stop-history-server.sh || true
stop-yarn.sh
stop-dfs.sh
