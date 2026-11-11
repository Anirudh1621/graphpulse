#!/usr/bin/env bash
# Runs the full GraphPulse batch layer on YARN:
#   01 ingest -> 02 graph analytics -> 03 recommendations (+evaluation) -> 04 publish to HBase
# Usage: scripts/run_pipeline.sh [--include-stream] [--from N]
#   --include-stream : merge follow/unfollow events captured by the streaming layer (Lambda re-computation)
#   --from N         : start at job N (1-4)
set -euo pipefail
source /etc/profile.d/bigdata.sh
cd "$(dirname "${BASH_SOURCE[0]}")/.."

INCLUDE_STREAM=""; FROM=1
while [ $# -gt 0 ]; do
  case "$1" in
    --include-stream) INCLUDE_STREAM="--include-stream" ;;
    --from) FROM="$2"; shift ;;
  esac
  shift
done

GRAPHFRAMES_PKG="${GRAPHFRAMES_PKG:-graphframes:graphframes:0.8.3-spark3.5-s_2.12}"
SUBMIT=(spark-submit --master yarn --deploy-mode client
        --repositories https://repos.spark-packages.org
        --packages "$GRAPHFRAMES_PKG"
        --py-files jobs/common.py
        --conf spark.executorEnv.GP_HBASE_HOST="$GP_HBASE_HOST"
        --conf spark.executorEnv.GP_HDFS_BASE="$GP_HDFS_BASE"
        ${SPARK_EXTRA_ARGS:-})

SNAPSHOT_DIR="${SNAPSHOT_DIR:-$HOME/graphpulse-snapshot}"
T0=$(date +%s)
[ "$FROM" -le 1 ] && "${SUBMIT[@]}" jobs/job01_ingest.py $INCLUDE_STREAM
[ "$FROM" -le 2 ] && "${SUBMIT[@]}" jobs/job02_graph_analytics.py
[ "$FROM" -le 3 ] && "${SUBMIT[@]}" jobs/job03_recommendations.py --evaluate
[ "$FROM" -le 4 ] && "${SUBMIT[@]}" jobs/job04_publish_serving.py --snapshot "$SNAPSHOT_DIR"
echo "Batch pipeline finished in $(( $(date +%s) - T0 ))s. Snapshot: $SNAPSHOT_DIR"
