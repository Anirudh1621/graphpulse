#!/usr/bin/env bash
# Stops the GraphPulse demo cleanly on master: event producer, API, Spark jobs, then every cluster daemon.
# The data stays on disk, so scripts/demo_up.sh brings everything back.
set -u
source /etc/profile.d/bigdata.sh
cd "$(dirname "${BASH_SOURCE[0]}")/.."
YARN="$HADOOP_HOME/bin/yarn"

echo "==> event producer and API"
pkill -f 'producer/event_producer.py' && echo "  producer stopped"
pkill -f 'uvicorn app.main:app' && echo "  API stopped"

echo "==> Spark jobs on YARN"
for id in $("$YARN" application -list -appStates RUNNING,ACCEPTED 2>/dev/null | awk '$2 ~ /^GraphPulse-/ {print $1}'); do
  "$YARN" application -kill "$id" >/dev/null 2>&1 && echo "  stopped $id"
done
pkill -f 'jobs/stream_graph_events.py'

echo "==> cluster daemons"
cluster/scripts/stop_all.sh
echo "Demo stopped. All data stays on disk."
