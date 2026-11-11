#!/usr/bin/env bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_CONF_DIR=/opt/bigdata/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/bigdata/hadoop/etc/hadoop
export PYSPARK_PYTHON=python3
export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080"
