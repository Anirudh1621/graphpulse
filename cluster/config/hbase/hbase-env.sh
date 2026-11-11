#!/usr/bin/env bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# HBase starts/stops its own ZooKeeper on 'master'
export HBASE_MANAGES_ZK=true
# avoid SLF4J / guava clashes with the Hadoop classpath
export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true
export HBASE_HEAPSIZE=1G
