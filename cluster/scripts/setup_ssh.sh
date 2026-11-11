#!/usr/bin/env bash
# Run ONCE on master as the 'hadoop' user. Sets up password-less SSH master -> all nodes
# (Hadoop/HBase start scripts and our start_all.sh rely on it).
# Prerequisite: /etc/hosts on ALL three PCs contains the cluster IPs, e.g.
#   192.168.1.10  master
#   192.168.1.11  worker1
#   192.168.1.12  worker2
# (and remove any '127.0.1.1 <hostname>' line - it breaks Hadoop RPC binding)
set -euo pipefail
[ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -b 4096 -N "" -f ~/.ssh/id_rsa
for h in master worker1 worker2; do
  echo "==> copying key to $h"
  ssh-copy-id -o StrictHostKeyChecking=accept-new "$USER@$h"
done
for h in master worker1 worker2; do ssh "$h" hostname; done
echo "Password-less SSH OK."
