#!/usr/bin/env bash
# Brings the whole GraphPulse demo up on master, in order, and checks every part:
#   cluster daemons -> batch results -> Kafka topics -> Spark streaming job -> API + dashboard -> event producer
# Safe to run again: anything already running is left alone. Logs go to ~/demo-logs.
# On Google Cloud it is usually started from Cloud Shell with  cluster/gcp/demo.sh start  (docs/CLOUD_SETUP.md).
set -u
source /etc/profile.d/bigdata.sh
cd "$(dirname "${BASH_SOURCE[0]}")/.."

[ "$(hostname -s)" = master ] || { echo "Run this on master."; exit 1; }
LOGS="$HOME/demo-logs"; mkdir -p "$LOGS"
RATE="${GP_DEMO_RATE:-50}"                 # baseline events per second sent by the producer
YARN="$HADOOP_HOME/bin/yarn"               # full path: on master, `yarn` can also be Node's Yarn
STREAM_APP="GraphPulse-stream-graph-events"
FAILED=0

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
ok()   { printf '  \033[32mOK\033[0m    %s\n' "$*"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$*"; FAILED=1; }

# wait_for SECONDS "what" command...  retries every 5 s until the command succeeds
wait_for() {
  local secs=$1 what=$2; shift 2
  local end=$(( $(date +%s) + secs ))
  until "$@" >/dev/null 2>&1; do
    if [ "$(date +%s)" -ge "$end" ]; then bad "$what (not ready after ${secs}s)"; return 1; fi
    sleep 5
  done
  ok "$what"
}

hbase_cmd()     { echo "$1" | timeout 90 hbase shell -n 2>/dev/null; }
api_health()    { curl -s -m 3 http://localhost:8000/api/health; }
safemode_off()  { hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF; }
two_datanodes() { hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes (2)'; }
two_nodemgrs()  { "$YARN" node -list 2>/dev/null | grep -q 'Total Nodes:2'; }
three_brokers() { [ "$(kafka-broker-api-versions.sh --bootstrap-server master:9092 2>/dev/null | grep -c '(id: ')" -ge 3 ]; }
hbase_serving() { hbase_cmd "scan 'gp_users', {LIMIT => 1}" | grep -q '1 row(s)'; }
stream_running(){ "$YARN" application -list -appStates RUNNING 2>/dev/null | grep -q "$STREAM_APP"; }
stream_queries(){ grep -q 'streaming queries running' "$LOGS/stream.log"; }
api_up()        { api_health | grep -q '"ok"'; }
api_wired()     { local h; h="$(api_health)"; grep -q '"backend": *"hbase"' <<<"$h" && grep -q '"mode": *"kafka"' <<<"$h"; }
producer_on()   { pgrep -f 'producer/event_producer.py' >/dev/null; }

step "1/6  Worker machines"
for h in worker1 worker2; do
  wait_for 180 "$h is reachable" ssh -o ConnectTimeout=5 -o BatchMode=yes "$h" true
done

step "2/6  Cluster daemons: HDFS, YARN, Spark history, Kafka, HBase"
cluster/scripts/start_all.sh > "$LOGS/start_all.log" 2>&1
ok "start scripts ran (details in $LOGS/start_all.log)"
wait_for 180 "HDFS is out of safe mode"  safemode_off
wait_for 180 "HDFS has 2 live DataNodes" two_datanodes
wait_for 180 "YARN has 2 NodeManagers"   two_nodemgrs
wait_for 180 "Kafka has 3 brokers"       three_brokers
wait_for 300 "HBase is serving tables"   hbase_serving

step "3/6  Batch results"
if hbase_cmd "get 'gp_meta', 'summary'" | grep -q '1 row(s)'; then
  ok "HBase holds the batch results"
elif hdfs dfs -test -e /graphpulse/analytics/users; then
  echo "  HBase has no results but HDFS does: publishing them again (job 04, about 2 minutes)"
  if scripts/run_pipeline.sh --from 4 > "$LOGS/job04.log" 2>&1; then ok "batch results published to HBase"
  else bad "job 04 failed: see $LOGS/job04.log"; fi
else
  bad "no batch results yet: run  nohup scripts/run_pipeline.sh > ~/pipeline.log 2>&1 &  (about 15 min), then run this again"
fi

step "4/6  Kafka topics"
for t in gp-events gp-insights; do
  kafka-topics.sh --bootstrap-server master:9092 --create --if-not-exists --topic "$t" \
    --partitions 3 --replication-factor 2 >/dev/null 2>&1
done
if [ "$(kafka-topics.sh --bootstrap-server master:9092 --list 2>/dev/null | grep -c '^gp-')" -ge 2 ]; then
  ok "gp-events and gp-insights exist"
else
  bad "the Kafka topics are missing"
fi

step "5/6  Speed layer: Spark Structured Streaming on YARN"
if stream_running; then
  ok "streaming job already running"
else
  setsid nohup scripts/run_stream.sh > "$LOGS/stream.log" 2>&1 < /dev/null &
  wait_for 300 "streaming job is RUNNING on YARN" stream_running && \
    wait_for 180 "its 5 queries have started" stream_queries
fi

step "6/6  API, dashboard and live events"
if api_up && ! api_wired; then
  echo "  the running API is not using HBase and Kafka: restarting it"
  pkill -f 'uvicorn app.main:app'; sleep 3
fi
if api_up; then
  ok "API already running"
else
  GP_BACKEND=hbase GP_LIVE=kafka setsid nohup scripts/run_api.sh > "$LOGS/api.log" 2>&1 < /dev/null &
  wait_for 300 "API answers on port 8000" api_up
fi
if api_wired; then ok "dashboard reads HBase, Live page reads Kafka"
else bad "API is not wired to HBase and Kafka: $(api_health) (log: $LOGS/api.log)"; fi

if producer_on; then
  ok "event producer already running"
elif [ ! -f data/twitter_combined.txt ]; then
  bad "data/twitter_combined.txt is missing: run scripts/download_data.sh"
else
  setsid nohup python3 producer/event_producer.py --edges data/twitter_combined.txt --rate "$RATE" \
    > "$LOGS/producer.log" 2>&1 < /dev/null &
  sleep 8
  if producer_on; then ok "event producer sends about $RATE events/s to Kafka"
  else bad "event producer stopped: see $LOGS/producer.log"; fi
fi

IP="$(curl -s -m 2 -H 'Metadata-Flavor: Google' \
  http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip 2>/dev/null)"
echo
if [ "$FAILED" -eq 0 ]; then printf '\033[1;32mDEMO READY\033[0m\n'
else printf '\033[1;31mDEMO NOT READY: see the FAIL lines above\033[0m\n'; fi
[ -n "$IP" ] && echo "MASTER IP: $IP"
echo "Logs: $LOGS    Stop everything: scripts/demo_down.sh"
exit "$FAILED"
