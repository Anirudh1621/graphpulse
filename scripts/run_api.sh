#!/usr/bin/env bash
# Starts the GraphPulse API + dashboard on master: http://master:8000
#   GP_BACKEND=hbase|snapshot|auto   (default auto: HBase, falls back to the JSON snapshot)
#   GP_LIVE=kafka|simulate|auto      (default auto: Kafka, falls back to in-process simulation)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
[ -f /etc/profile.d/bigdata.sh ] && source /etc/profile.d/bigdata.sh

if [ ! -d .venv ]; then
  python3 -m venv .venv
  .venv/bin/pip install -q -r api/requirements.txt
fi
if [ ! -f ui/dist/index.html ]; then
  if command -v npm >/dev/null; then (cd ui && npm ci && npm run build)
  else echo "ui/dist missing and npm not installed: build the UI on any PC (cd ui && npm ci && npm run build) and copy ui/dist here"; fi
fi
# prefer the real snapshot produced by job04, fall back to the bundled synthetic demo snapshot
if [ -z "${GP_SNAPSHOT_DIR:-}" ] && [ -f "$HOME/graphpulse-snapshot/users.jsonl" ]; then
  export GP_SNAPSHOT_DIR="$HOME/graphpulse-snapshot"
fi
cd api
exec ../.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
