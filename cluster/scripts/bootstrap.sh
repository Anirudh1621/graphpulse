#!/usr/bin/env bash
# ONE-TIME initialisation, run on master after install_node.sh has run on all three PCs.
#   1. formats HDFS            2. formats Kafka KRaft storage on all brokers
#   3. starts everything       4. creates HDFS dirs, Kafka topics and HBase tables
set -euo pipefail
source /etc/profile.d/bigdata.sh
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [ ! -d /opt/bigdata/data/hadoop/namenode/current ]; then
  echo "==> formatting HDFS NameNode"
  hdfs namenode -format -force -clusterId graphpulse
fi

echo "==> formatting Kafka KRaft storage on all brokers (same cluster id everywhere)"
KAFKA_CLUSTER_ID="$(kafka-storage.sh random-uuid)"
for h in master worker1 worker2; do
  ssh "$h" "source /etc/profile.d/bigdata.sh; [ -f /opt/bigdata/data/kafka/meta.properties ] || \
    kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c \$KAFKA_HOME/config/graphpulse.properties"
done

"$REPO_DIR/cluster/scripts/start_all.sh"

echo "==> HDFS directories"
hdfs dfs -mkdir -p /spark-logs /graphpulse/raw /graphpulse/curated /graphpulse/analytics /graphpulse/lake /graphpulse/_checkpoints
hdfs dfs -chmod -R 777 /spark-logs

echo "==> Kafka topics (3 partitions, replicated on 2 brokers)"
for t in gp-events gp-insights; do
  kafka-topics.sh --bootstrap-server master:9092 --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 2
done
kafka-topics.sh --bootstrap-server master:9092 --describe

echo "==> HBase tables"
hbase shell -n "$REPO_DIR/cluster/hbase/create_tables.hbase"
echo "Bootstrap complete."
