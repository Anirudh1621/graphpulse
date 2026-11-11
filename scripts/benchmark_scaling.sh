#!/usr/bin/env bash
# Horizontal-scaling experiment: runs the graph-analytics job (job 02) with 1 executor
# (one worker PC busy) and then with 2 executors (both worker PCs).
# Every run is recorded in HBase (gp_meta run:*) and appears on the dashboard's Cluster page.
set -euo pipefail
source /etc/profile.d/bigdata.sh
cd "$(dirname "${BASH_SOURCE[0]}")/.."
GRAPHFRAMES_PKG="${GRAPHFRAMES_PKG:-graphframes:graphframes:0.8.3-spark3.5-s_2.12}"

for n in ${EXECUTORS:-1 2}; do
  echo "================ executors = $n ================"
  /usr/bin/time -f "wall clock: %es" spark-submit --master yarn --deploy-mode client \
    --repositories https://repos.spark-packages.org --packages "$GRAPHFRAMES_PKG" \
    --py-files jobs/common.py \
    --num-executors "$n" --conf spark.dynamicAllocation.enabled=false \
    --conf spark.executorEnv.GP_HBASE_HOST="$GP_HBASE_HOST" \
    jobs/job02_graph_analytics.py 2>&1 | grep -E 'GraphPulse|wall clock|ERROR'
done
echo "Compare the runs on the dashboard -> Cluster -> Benchmark history"
