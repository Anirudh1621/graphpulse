#!/usr/bin/env bash
# Downloads the SNAP Twitter social graph and loads it into HDFS.
#   twitter_combined: 81,306 users, 1,768,149 directed "follows" edges  (https://snap.stanford.edu/data/ego-Twitter.html)
#   --small : SNAP ego-Facebook (4,039 users, 88,234 edges) for quick smoke tests
set -euo pipefail
source /etc/profile.d/bigdata.sh
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mkdir -p data

if [ "${1:-}" = "--small" ]; then
  NAME=facebook_combined; URL=https://snap.stanford.edu/data/facebook_combined.txt.gz
else
  NAME=twitter_combined;  URL=https://snap.stanford.edu/data/twitter_combined.txt.gz
fi

[ -f "data/$NAME.txt" ] || { curl -fL -o "data/$NAME.txt.gz" "$URL"; gunzip -f "data/$NAME.txt.gz"; }
echo "local file: $(wc -l < "data/$NAME.txt") lines"

hdfs dfs -mkdir -p /graphpulse/raw/edges
hdfs dfs -put -f "data/$NAME.txt" /graphpulse/raw/edges/
hdfs dfs -ls -h /graphpulse/raw/edges
echo "block placement (replication across worker1/worker2):"
hdfs fsck "/graphpulse/raw/edges/$NAME.txt" -files -blocks -locations | grep -E 'blk_|Status' | head -5
