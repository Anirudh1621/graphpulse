#!/usr/bin/env bash
# Runs the GraphPulse demo on Google Cloud from Cloud Shell (or any computer with gcloud).
#
#   demo.sh start [YOUR_IP]  start the 3 machines and everything on them (about 5 minutes)
#   demo.sh stop             stop everything, then the machines (while stopped, only the disks cost money)
#   demo.sh status           machine states, master's IP and the daemon table
#   demo.sh ip YOUR_IP       let a laptop with a new IP address open the web pages
#   demo.sh ssh              open a terminal on master
#   demo.sh prepare          after a code update: new code on all machines, dashboard rebuilt
#
# YOUR_IP is your laptop's public IP. Double-click cluster/gcp/demo_laptop.cmd on the laptop: it finds the IP
# and copies the whole start command for you. More in docs/CLOUD_SETUP.md, step 8.
set -euo pipefail

ZONE="${GP_ZONE:-$(gcloud config get-value compute/zone 2>/dev/null || true)}"
ZONE="${ZONE:-asia-south1-a}"
VMS=(master worker1 worker2)
RULE=graphpulse-web

on_master()    { gcloud compute ssh hadoop@master --zone "$ZONE" --quiet --command "$1"; }
master_state() { gcloud compute instances describe master --zone "$ZONE" --format='get(status)' 2>/dev/null || true; }
master_ip()    { gcloud compute instances describe master --zone "$ZONE" \
                   --format='get(networkInterfaces[0].accessConfigs[0].natIP)'; }

start_vms() {
  echo "==> Starting the machines (zone $ZONE)"
  gcloud compute instances start "${VMS[@]}" --zone "$ZONE"
  echo "==> Waiting for master to accept SSH"
  for _ in $(seq 1 36); do
    if on_master true >/dev/null 2>&1; then return 0; fi
    sleep 5
  done
  echo "master does not answer over SSH after 3 minutes. Try: demo.sh status"
  exit 1
}

links() {
  local ip; ip="$(master_ip)"
  cat <<EOF

MASTER IP: $ip
  Dashboard      http://$ip:8000
  YARN           http://$ip:8088
  HDFS           http://$ip:9870
  HBase          http://$ip:16010
  Spark history  http://$ip:18080
On the laptop, paste $ip into the demo_laptop window: then http://master:8000 and the links
inside the Hadoop pages work too.
EOF
}

set_ip() {
  if ! [[ "${1:-}" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]]; then
    echo "Usage: demo.sh ip YOUR_IP   (your laptop's address, like 49.43.107.12: open https://api.ipify.org)"
    exit 1
  fi
  echo "==> Letting $1 open the web pages"
  gcloud compute firewall-rules update "$RULE" --source-ranges="$1/32" --quiet
}

case "${1:-}" in
  start)
    if [ -n "${2:-}" ]; then set_ip "$2"; fi
    start_vms
    on_master "bash ~/graphpulse/scripts/demo_up.sh" || true
    links
    ;;
  stop)
    if [ "$(master_state)" = RUNNING ]; then on_master "bash ~/graphpulse/scripts/demo_down.sh" || true; fi
    echo "==> Stopping the machines"
    gcloud compute instances stop "${VMS[@]}" --zone "$ZONE"
    ;;
  status)
    gcloud compute instances list --filter="name~'^(master|worker1|worker2)$'" \
      --format='table(name,status,networkInterfaces[0].accessConfigs[0].natIP:label=EXTERNAL_IP)'
    if [ "$(master_state)" = RUNNING ]; then on_master "bash ~/graphpulse/cluster/scripts/status.sh" || true; fi
    ;;
  ip)
    set_ip "${2:-}"
    ;;
  ssh)
    exec gcloud compute ssh hadoop@master --zone "$ZONE"
    ;;
  prepare)
    start_vms
    on_master "bash -s" <<'EOF'
set -e
for h in master worker1 worker2; do
  echo "==> $h: latest code from GitHub"
  ssh -o BatchMode=yes "$h" 'set -e; cd ~/graphpulse
    if ! git diff --quiet HEAD; then git stash -q -m "local edits before demo update"; echo "  (local edits kept in git stash)"; fi
    git fetch -q origin && git reset -q --hard origin/main && git log --oneline -1
    sudo cp cluster/scripts/env.sh /etc/profile.d/bigdata.sh'
done
echo "==> master: rebuilding the dashboard (1-2 minutes)"
cd ~/graphpulse/ui && npm ci --no-audit --no-fund --loglevel=error && npm run build --silent
echo "Prepare done. Now run: ~/graphpulse/cluster/gcp/demo.sh start YOUR_IP"
EOF
    ;;
  *)
    sed -n '2,12p' "$0"
    exit 1
    ;;
esac
