#!/usr/bin/env bash
# Shared environment for every GraphPulse node. Installed to /etc/profile.d/bigdata.sh
export BIGDATA_HOME=/opt/bigdata
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$BIGDATA_HOME/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_CONF_DIR
export SPARK_HOME=$BIGDATA_HOME/spark
export HBASE_HOME=$BIGDATA_HOME/hbase
export KAFKA_HOME=$BIGDATA_HOME/kafka
export PYSPARK_PYTHON=python3
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$HBASE_HOME/bin:$KAFKA_HOME/bin

# GraphPulse application settings
export GP_HDFS_BASE=hdfs://master:9000/graphpulse
export GP_HBASE_HOST=master
export GP_HBASE_PORT=9090
export GP_KAFKA=master:9092,worker1:9092,worker2:9092
