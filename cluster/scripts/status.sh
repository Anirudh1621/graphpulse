#!/usr/bin/env bash
# Health check of every daemon on every node.
set -uo pipefail
source /etc/profile.d/bigdata.sh

declare -A EXPECT=(
  [master]="NameNode SecondaryNameNode ResourceManager HMaster HQuorumPeer ThriftServer Kafka"
  [worker1]="DataNode NodeManager HRegionServer Kafka"
  [worker2]="DataNode NodeManager HRegionServer Kafka"
)
for h in master worker1 worker2; do
  running="$(ssh "$h" "source /etc/profile.d/bigdata.sh; jps" 2>/dev/null)"
  printf '\n[%s]\n' "$h"
  for d in ${EXPECT[$h]}; do
    if grep -qw "$d" <<<"$running"; then printf '  \e[32mUP  \e[0m %s\n' "$d"; else printf '  \e[31mDOWN\e[0m %s\n' "$d"; fi
  done
done
echo
echo "HDFS live DataNodes : $(hdfs dfsadmin -report 2>/dev/null | grep -m1 'Live datanodes' || echo '?')"
echo "YARN NodeManagers   : $(yarn node -list 2>/dev/null | grep -m1 'Total Nodes' || echo '?')"
echo "Kafka brokers       : $(kafka-broker-api-versions.sh --bootstrap-server master:9092 2>/dev/null | grep -c 'id:' || echo '?')"
echo
echo "Web UIs: NameNode http://master:9870 | YARN http://master:8088 | HBase http://master:16010 | Spark history http://master:18080"
