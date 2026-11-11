#!/usr/bin/env bash
# Installs Hadoop, Spark, HBase and Kafka on ONE node and applies the GraphPulse configs.
# Run on every PC (as the 'hadoop' user, which must have sudo):
#     ./cluster/scripts/install_node.sh master     # on the master PC
#     ./cluster/scripts/install_node.sh worker1    # on worker PC 1
#     ./cluster/scripts/install_node.sh worker2    # on worker PC 2
# Tip: run it on master first, then copy ~/gp-downloads to the workers to skip re-downloading.
set -euo pipefail

ROLE="${1:-}"
case "$ROLE" in
  master)  NODE_ID=1 ;;
  worker1) NODE_ID=2 ;;
  worker2) NODE_ID=3 ;;
  *) echo "usage: $0 master|worker1|worker2"; exit 1 ;;
esac

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CFG="$REPO_DIR/cluster/config"
DL="$HOME/gp-downloads"
BD=/opt/bigdata

HADOOP_V=3.3.6
SPARK_V=3.5.1
HBASE_V=2.5.8
KAFKA_V=3.7.0
SCALA_V=2.13

echo "==> [$ROLE] system packages"
sudo apt-get update -y
sudo apt-get install -y openjdk-11-jdk python3 python3-pip python3-venv ssh rsync curl net-tools time

echo "==> [$ROLE] python packages (needed on ALL nodes: Spark executors run python here)"
PIP_FLAGS=""
if python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)'; then PIP_FLAGS="--break-system-packages"; fi
pip3 install $PIP_FLAGS --user "numpy<2" happybase kafka-python requests

echo "==> [$ROLE] downloads -> $DL"
mkdir -p "$DL"
fetch() { [ -f "$DL/$2" ] || curl -fL --retry 3 -o "$DL/$2" "$1/$2"; }
fetch https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_V hadoop-$HADOOP_V.tar.gz
fetch https://archive.apache.org/dist/spark/spark-$SPARK_V          spark-$SPARK_V-bin-hadoop3.tgz
fetch https://archive.apache.org/dist/hbase/$HBASE_V                hbase-$HBASE_V-hadoop3-bin.tar.gz
fetch https://archive.apache.org/dist/kafka/$KAFKA_V                kafka_$SCALA_V-$KAFKA_V.tgz

echo "==> [$ROLE] extract to $BD"
sudo mkdir -p $BD && sudo chown -R "$USER:$USER" $BD
mkdir -p $BD/data/{hadoop/namenode,hadoop/datanode,hadoop/tmp,zookeeper,kafka}
tar -xzf "$DL/hadoop-$HADOOP_V.tar.gz"            -C $BD
tar -xzf "$DL/spark-$SPARK_V-bin-hadoop3.tgz"      -C $BD
tar -xzf "$DL/hbase-$HBASE_V-hadoop3-bin.tar.gz"  -C $BD
tar -xzf "$DL/kafka_$SCALA_V-$KAFKA_V.tgz"         -C $BD
ln -sfn $BD/hadoop-$HADOOP_V               $BD/hadoop
ln -sfn $BD/spark-$SPARK_V-bin-hadoop3     $BD/spark
ln -sfn $BD/hbase-$HBASE_V-hadoop3         $BD/hbase
ln -sfn $BD/kafka_$SCALA_V-$KAFKA_V        $BD/kafka

echo "==> [$ROLE] environment"
sudo cp "$REPO_DIR/cluster/scripts/env.sh" /etc/profile.d/bigdata.sh
grep -q 'profile.d/bigdata.sh' ~/.bashrc || echo 'source /etc/profile.d/bigdata.sh' >> ~/.bashrc
# shellcheck disable=SC1091
source /etc/profile.d/bigdata.sh

echo "==> [$ROLE] Hadoop config"
cp "$CFG"/hadoop/*.xml "$CFG/hadoop/workers" $BD/hadoop/etc/hadoop/
sed -i "s|^# export JAVA_HOME=.*|export JAVA_HOME=$JAVA_HOME|" $BD/hadoop/etc/hadoop/hadoop-env.sh
grep -q '^export JAVA_HOME' $BD/hadoop/etc/hadoop/hadoop-env.sh || echo "export JAVA_HOME=$JAVA_HOME" >> $BD/hadoop/etc/hadoop/hadoop-env.sh

echo "==> [$ROLE] Spark config"
cp "$CFG/spark/spark-defaults.conf" "$CFG/spark/spark-env.sh" $BD/spark/conf/

echo "==> [$ROLE] HBase config"
cp "$CFG/hbase/hbase-site.xml" "$CFG/hbase/hbase-env.sh" "$CFG/hbase/regionservers" $BD/hbase/conf/

echo "==> [$ROLE] Kafka config (node.id=$NODE_ID)"
if [ "$ROLE" = master ]; then
  ROLES="broker,controller"; LISTENERS="PLAINTEXT://:9092,CONTROLLER://:9093"
else
  ROLES="broker";            LISTENERS="PLAINTEXT://:9092"
fi
sed -e "s|__NODE_ID__|$NODE_ID|" -e "s|__HOST__|$ROLE|" -e "s|__ROLES__|$ROLES|" -e "s|__LISTENERS__|$LISTENERS|" \
    "$CFG/kafka/server.properties.template" > $BD/kafka/config/graphpulse.properties

chmod +x "$REPO_DIR"/cluster/scripts/*.sh "$REPO_DIR"/scripts/*.sh 2>/dev/null || true
echo "==> [$ROLE] done. Open a new shell (or 'source /etc/profile.d/bigdata.sh')."
java -version 2>&1 | head -1
