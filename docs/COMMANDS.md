# Command reference (for the demo & viva)

The evaluation asks you to *execute and explain basic usage commands*. Each section below covers the commands to run live, what each one proves, and a GraphPulse-specific example.

## HDFS: distributed storage

| Command | What it does |
|---|---|
| `hdfs dfsadmin -report` | Cluster capacity + every DataNode's status. **Shows 2 live DataNodes.** |
| `hdfs dfs -ls -R /graphpulse` | List our data lake: `raw/`, `curated/`, `analytics/`, `lake/` |
| `hdfs dfs -put data/twitter_combined.txt /graphpulse/raw/edges/` | Upload a local file; it is split into 64 MB blocks |
| `hdfs dfs -du -h /graphpulse` | Space used per directory (Parquet is much smaller than the raw text) |
| `hdfs dfs -cat /graphpulse/analytics/summary/part-* \| head` | Read a file in HDFS |
| `hdfs fsck /graphpulse/raw/edges -files -blocks -locations` | **Proves replication**: each block is stored on worker1 *and* worker2 |
| `hdfs dfs -setrep 2 /path` | Change the replication factor |
| `hdfs dfsadmin -safemode get` | NameNode safe mode (read-only during startup) |

*Explain:* the NameNode on master stores only **metadata** (file → blocks → DataNodes). The data itself lives on the workers, and replication factor 2 means either worker can fail without losing data.

## YARN: resource management

| Command | What it does |
|---|---|
| `yarn node -list -all` | NodeManagers (one per worker) and their state |
| `yarn application -list` | Running applications (our Spark jobs + the streaming job) |
| `yarn application -status <appId>` | Details of one job |
| `yarn logs -applicationId <appId> \| less` | Aggregated executor logs from all workers |
| `yarn application -kill <appId>` | Stop a job |
| `yarn top` | Live cluster utilisation |

*Explain:* Spark asks the ResourceManager for **containers**. The NodeManagers on worker1/worker2 launch the executors inside them, so the computation runs on the workers, not on master.

## Spark

| Command | What it does |
|---|---|
| `spark-submit --master yarn jobs/job02_graph_analytics.py` | Submit a job to the cluster (see `scripts/run_pipeline.sh` for the full flags) |
| `pyspark --master yarn --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12` | Interactive shell on the cluster |
| `spark.read.parquet("/graphpulse/analytics/users").orderBy("pr_rank").show(10)` | (in pyspark) query the results |
| http://master:4040 | Live UI of the running job: DAG, stages, tasks per executor |
| http://master:18080 | History server for finished jobs |

Quick GraphFrames demo inside `pyspark`:
```python
from graphframes import GraphFrame
e = spark.read.parquet("/graphpulse/curated/edges"); v = spark.read.parquet("/graphpulse/curated/vertices")
g = GraphFrame(v, e)
g.inDegrees.orderBy("inDegree", ascending=False).show(5)            # most followed
g.find("(a)-[]->(b); (b)-[]->(a)").count()                           # motif: mutual follows (counted twice)
g.pageRank(resetProbability=0.15, maxIter=5).vertices.orderBy("pagerank", ascending=False).show(5)
```

## Kafka: event streaming (KRaft, 3 brokers)

| Command | What it does |
|---|---|
| `kafka-topics.sh --bootstrap-server master:9092 --list` | List topics |
| `kafka-topics.sh --bootstrap-server master:9092 --describe --topic gp-events` | **Partitions, leader broker, replicas, ISR**: shows data spread over 3 brokers |
| `kafka-console-consumer.sh --bootstrap-server master:9092 --topic gp-events --max-messages 5` | Peek at live events |
| `kafka-console-consumer.sh --bootstrap-server master:9092 --topic gp-insights` | Watch Spark's streaming output |
| `kafka-console-producer.sh --bootstrap-server master:9092 --topic gp-events` | Type an event by hand, e.g. `{"event_id":"x","type":"follow","src":1,"dst":2,"ts":1731300000000,"scenario":"manual"}` |
| `kafka-consumer-groups.sh --bootstrap-server master:9092 --list` | Consumer groups |
| `kafka-metadata-quorum.sh --bootstrap-server master:9092 describe --status` | KRaft controller quorum (replaces ZooKeeper) |

*Explain:* events are **keyed by user id**, so all events of one user go to the same partition and stay in order. Replication factor 2 means a broker can go down without losing data.

## HBase: serving layer

```bash
hbase shell
```
| Command | What it does |
|---|---|
| `status 'simple'` | HMaster + 2 RegionServers and their load |
| `list` | Tables: `gp_users`, `gp_communities`, `gp_meta`, `gp_alerts` |
| `describe 'gp_users'` | Column families `m` (metrics), `n` (adjacency), `r` (recs), `s` (stream counters) |
| `get 'gp_users', '<user id>', {COLUMNS => ['m:pagerank','m:community','m:influence']}` | Random-access read of one user in milliseconds |
| `scan 'gp_users', {LIMIT => 3, COLUMNS => ['m:influence']}` | Scan |
| `get_counter 'gp_users', '<user id>', 's:followers_delta'` | Live follower delta written by Spark Streaming |
| `count 'gp_users', INTERVAL => 20000` | Row count |
| `scan 'gp_alerts', {LIMIT => 5}` | Latest real-time threat alerts |
| `get 'gp_meta', 'summary'` | Network summary JSON |
| `list_regions 'gp_users'` | **Pre-split regions spread across worker1/worker2** |

*Explain:* HDFS is great for scanning whole datasets but bad at fetching one record. HBase gives millisecond random reads and writes by row key, which is what a dashboard and a BFS path search need.

## GraphPulse operations

| Command | What it does |
|---|---|
| `cluster/scripts/status.sh` | Health of every daemon on every node |
| `scripts/run_pipeline.sh` | Full batch layer (jobs 01→04) |
| `scripts/run_pipeline.sh --include-stream` | Re-run batch **including** events captured by the streaming layer (the Lambda merge) |
| `scripts/run_stream.sh` | Start the speed layer |
| `python3 producer/event_producer.py --edges data/twitter_combined.txt --scenario bot --duration 30` | Inject a bot attack from the CLI |
| `scripts/benchmark_scaling.sh` | 1 vs 2 executors benchmark |
| `curl -s master:8000/api/summary \| python3 -m json.tool` | Query the REST API (interactive docs at http://master:8000/docs) |
