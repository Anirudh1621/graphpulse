# Cluster setup: 1 master + 2 workers

Plan for about **2–3 hours** the first time. Every step is scripted. Run the commands from the repo root on the machine named in each step.

## 0. Hardware & topology

| Machine | Hostname | Daemons |
|---|---|---|
| PC 1 | `master` | HDFS NameNode + SecondaryNameNode, YARN ResourceManager, HBase HMaster + ZooKeeper + Thrift, Kafka broker 1 (KRaft controller), Spark History, GraphPulse API/UI |
| PC 2 | `worker1` | HDFS DataNode, YARN NodeManager (Spark executors), HBase RegionServer, Kafka broker 2 |
| PC 3 | `worker2` | HDFS DataNode, YARN NodeManager (Spark executors), HBase RegionServer, Kafka broker 3 |

Minimum per PC: 8 GB RAM, 4 cores, 30 GB free disk. **Ubuntu 22.04 LTS** (native, dual-boot, or a VM in *bridged* network mode). All three must be on the same LAN, ideally the same Wi-Fi/hotspot or a switch.

> With 16 GB machines, raise `yarn.nodemanager.resource.memory-mb` in `cluster/config/hadoop/yarn-site.xml` to ~10240 and `spark.executor.memory` to 4g.

## 1. On all three PCs

```bash
# a common user called 'hadoop'
sudo adduser hadoop && sudo usermod -aG sudo hadoop && su - hadoop

# hostnames
sudo hostnamectl set-hostname master      # worker1 / worker2 on the other PCs

# /etc/hosts: use your real LAN IPs (check with: ip -4 addr)
sudo nano /etc/hosts
#   192.168.1.10  master
#   192.168.1.11  worker1
#   192.168.1.12  worker2
# and DELETE the line '127.0.1.1 <hostname>' (otherwise Hadoop binds to loopback)

sudo apt-get install -y git openssh-server
git clone <your-github-repo-url> ~/graphpulse && cd ~/graphpulse
```

> If you use a phone hotspot or DHCP, IPs can change between sessions. Reserve them in the router, or re-check `/etc/hosts` before the demo.

## 2. Install the stack (all three PCs)

```bash
./cluster/scripts/install_node.sh master     # on master
./cluster/scripts/install_node.sh worker1    # on worker1
./cluster/scripts/install_node.sh worker2    # on worker2
source /etc/profile.d/bigdata.sh
```

The script installs Java 11 and Python packages, downloads Hadoop 3.3.6, Spark 3.5.1, HBase 2.5.8 and Kafka 3.7.0 into `/opt/bigdata`, then copies in our configs. To skip re-downloading on the workers, copy `~/gp-downloads/` from master first: `scp -r ~/gp-downloads worker1:~/`

## 3. Password-less SSH (master only)

```bash
./cluster/scripts/setup_ssh.sh        # asks for each node's password once
```

## 4. First-time bootstrap (master only)

```bash
./cluster/scripts/bootstrap.sh
```

This formats HDFS and Kafka storage, starts every daemon, creates the HDFS directories, creates the Kafka topics (3 partitions, replication 2) and creates the HBase tables (pre-split). Afterwards it prints a health table. Every line should say **UP**.

From then on, use `cluster/scripts/start_all.sh`, `stop_all.sh` and `status.sh`.

## 5. Verify from a browser

| UI | URL | What to check |
|---|---|---|
| HDFS NameNode | http://master:9870 | *Datanodes* tab → 2 live |
| YARN | http://master:8088 | *Nodes* → 2 active NodeManagers |
| HBase | http://master:16010 | 2 RegionServers, tables `gp_*` |
| Spark History | http://master:18080 | finished jobs |

(On Windows laptops, add the same `/etc/hosts` lines to `C:\Windows\System32\drivers\etc\hosts`.)

## 6. Load data & run the pipeline (master)

```bash
./scripts/download_data.sh            # SNAP Twitter graph -> HDFS (use --small for a quick test)
./scripts/run_pipeline.sh             # jobs 01-04 on YARN, ~5-15 min depending on hardware
./scripts/benchmark_scaling.sh        # optional: 1 vs 2 executors, shows up on the Cluster page
```

## 7. Real-time layer + dashboard (master, 3 terminals)

```bash
./scripts/run_stream.sh                                                         # terminal 1: Spark Structured Streaming
python3 producer/event_producer.py --edges data/twitter_combined.txt --rate 40   # terminal 2: event traffic
./scripts/run_api.sh                                                            # terminal 3: API + UI -> http://master:8000
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Live datanodes: 0` | `/etc/hosts` is wrong, or the `127.0.1.1` line is still there; firewall: `sudo ufw disable` |
| DataNode won't start after re-format | `rm -rf /opt/bigdata/data/hadoop/datanode/*` on the workers, then restart |
| Spark job stuck in ACCEPTED | YARN memory too small. Lower `spark.executor.memory` or raise `yarn.nodemanager.resource.memory-mb` |
| `ImportError: graphframes` | the `--packages` download needs internet on master; or set `GRAPHFRAMES_PKG` to a local jar |
| HBase RegionServer dies | check `hbase/logs/*regionserver*.log`; usually clocks out of sync: `sudo timedatectl set-ntp true` on all PCs |
| Kafka broker won't start | storage formatted with a different cluster id: `rm -rf /opt/bigdata/data/kafka/*` on all nodes, re-run the format step from `bootstrap.sh` |
| `happybase` connection refused | Thrift server down: `hbase-daemon.sh start thrift -p 9090` |
| Scripts fail with `$'\r': command not found` | Windows line endings: `sed -i 's/\r$//' cluster/scripts/*.sh scripts/*.sh` |
