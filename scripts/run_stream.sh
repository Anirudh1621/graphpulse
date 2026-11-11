#!/usr/bin/env bash
# Starts the Spark Structured Streaming speed layer on YARN (runs until Ctrl+C).
set -euo pipefail
source /etc/profile.d/bigdata.sh
cd "$(dirname "${BASH_SOURCE[0]}")/.."
exec spark-submit --master yarn --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --py-files jobs/common.py \
  --conf spark.executor.instances=2 --conf spark.executor.memory=1g --conf spark.executor.cores=1 \
  --conf spark.executorEnv.GP_HBASE_HOST="$GP_HBASE_HOST" \
  --conf spark.executorEnv.GP_KAFKA="$GP_KAFKA" \
  jobs/stream_graph_events.py "$@"
