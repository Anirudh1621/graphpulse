#!/usr/bin/env bash
# Starts the whole GraphPulse cluster from master.
set -uo pipefail
source /etc/profile.d/bigdata.sh

echo "==> HDFS  (NameNode on master, DataNodes on worker1/worker2)"
start-dfs.sh
echo "==> YARN  (ResourceManager on master, NodeManagers on workers)"
start-yarn.sh
hdfs dfsadmin -safemode wait

echo "==> Spark History Server"
$SPARK_HOME/sbin/start-history-server.sh || true

echo "==> Kafka brokers (KRaft) on all nodes"
for h in master worker1 worker2; do
  ssh "$h" "source /etc/profile.d/bigdata.sh; jps | grep -q Kafka || \
    kafka-server-start.sh -daemon \$KAFKA_HOME/config/graphpulse.properties"
done

echo "==> HBase (ZooKeeper + HMaster on master, RegionServers on workers) + Thrift"
start-hbase.sh
sleep 5
hbase-daemon.sh start thrift -p 9090 || true

sleep 5
"$(dirname "${BASH_SOURCE[0]}")/status.sh"
