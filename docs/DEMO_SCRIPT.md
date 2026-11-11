# Demo script (about 12 minutes) + viva preparation

## Before the evaluator arrives (30 min earlier)
1. All 3 PCs on the same network, with `/etc/hosts` correct. On master: `cluster/scripts/start_all.sh`, and every daemon should be **UP**.
2. Run `scripts/run_pipeline.sh` if the HBase tables are empty (check with `get 'gp_meta','summary'`).
3. Terminal 1: `scripts/run_stream.sh`. Terminal 2: the producer at `--rate 30`. Terminal 3: `scripts/run_api.sh`.
4. Open browser tabs: dashboard `http://master:8000`, NameNode `:9870`, YARN `:8088`, HBase `:16010`.
5. **Plan B:** if the cluster misbehaves, the dashboard still runs on a laptop (`GP_BACKEND=snapshot`, after copying `~/graphpulse-snapshot` from master). Show real results and explain what failed.

## The flow

| Time | Show | Say |
|---|---|---|
| 0:00 | Overview page | "GraphPulse is a social-network intelligence platform built on a Lambda architecture. Batch layer: Spark + GraphFrames on YARN over HDFS. Speed layer: Kafka + Spark Structured Streaming. Serving layer: HBase. Dataset: SNAP Twitter, 81k users and 1.77M follow edges." Point at the architecture strip. |
| 1:00 | **Cluster** page, then `status.sh` and `hdfs dfsadmin -report` | Three machines with the roles of each. "Data blocks are replicated on both workers." Run `hdfs fsck … -locations`. |
| 2:30 | YARN UI (`:8088`) | The streaming app is running; finished batch apps; containers on worker1/worker2. |
| 3:00 | Overview KPIs + degree chart | "The log-log degree distribution is a straight-ish heavy tail, so the graph is **scale-free**. Modularity Q > 0.3 means real community structure. Small world: most users are only 2–3 hops from the top hub." |
| 4:00 | Graph Explorer | Click a large node. "Size is PageRank, colour is the label-propagation community." Switch colour to suspicion. |
| 5:00 | User profile | Influence rank, ego network, **People You May Know** with "because you follow…". "These come from GraphFrames motif finding `(a)→(b)→(c)`, scored with Adamic-Adar." Point at the **evaluation card**: "We hid 5% of edges; our recommender beats the popularity baseline by N×." |
| 6:30 | Six Degrees | Random pair, then Find path. "This is a live bidirectional BFS over adjacency lists in HBase: N rows fetched in M ms." |
| 7:30 | Communities | Community map; click one: density, conductance. |
| 8:30 | **Live Stream** | Show events/s and the Spark windowed throughput. Click **Simulate viral surge**: within ~10 s the account tops *Rising stars*. Click **Launch bot attack**: a *critical* alert appears. "Spark detected 25+ follows in a 30-second sliding window, joined the event stream with the batch suspicion score (a stream-static join), and wrote the alert to HBase and Kafka." |
| 10:00 | Terminal: `kafka-console-consumer … gp-insights` and `hbase shell` → `scan 'gp_alerts', {LIMIT=>2}` | Proves where the data actually lives. |
| 10:30 | Profile of the viral user | "Followers (live)" now shows **+N since last batch run**. These are atomic HBase counters from the stream. "Next batch run with `--include-stream` folds these events from the HDFS data lake back into the master dataset. That is the Lambda merge." |
| 11:00 | Cluster page → benchmark chart | "Same job with 1 vs 2 executors: horizontal scaling." |
| 11:30 | GitHub repo | README, docs, scripts. |

## Likely viva questions

**Why GraphFrames and not GraphX?** GraphFrames is DataFrame-based, so it gets the Catalyst optimiser, works from Python, and adds **motif finding**, which is what powers our recommendations. Internally it uses GraphX for PageRank, connected components and label propagation.

**How does PageRank work?** An account's importance is the sum of the importance of its followers, divided by how many accounts each follower follows. With probability 0.15 a "random surfer" jumps to a random node (reset probability), which guarantees convergence. We run 15 iterations; each iteration is a distributed join plus aggregate.

**How does label propagation work?** Every node starts with its own label. In each round it adopts the most frequent label among its neighbours, and densely connected groups converge to a shared label. It costs O(edges) per iteration, so it scales well, but it is non-deterministic. We judge quality with **modularity** (Q > 0.3 is good) and **conductance** per community (lower = better separated).

**What is the clustering coefficient?** It is the fraction of a user's friend pairs who are also connected: 2T / (k(k-1)), using the triangle counts from `triangleCount()`.

**Why Adamic-Adar?** Shared contacts are evidence of a future link, but a shared celebrity says little (everyone follows them). Each shared contact b is weighted by 1/log(degree(b)). To keep the 2-hop explosion bounded, we prune each user to their 20 most influential followees.

**How did you evaluate the recommender?** We randomly hid 5% of edges, generated recommendations from the remaining 95%, then measured precision@10, recall@10 and hit-rate against the hidden edges. We compared against a popularity baseline.

**How is the suspicion score computed?** Six behaviour features (log degrees, follow ratio, reciprocity, clustering, triangles) are standardised and clustered with MLlib KMeans (k=8). Distance to the cluster centroid gives an anomaly percentile, which is weighted by how bot-like the account leans (following ≫ followers) and combined with three interpretable rules. The UI always shows *why* an account was flagged.

**What is a watermark?** It tells Spark how late an event may arrive (e.g. 30 s). Windows older than the watermark are finalised and their state is dropped, so memory stays bounded and late data is still handled correctly.

**What is the Lambda architecture?** The batch layer recomputes accurate views from the immutable master dataset (HDFS). The speed layer gives low-latency approximate updates for recent data. The serving layer (HBase) merges both. Our raw stream events are archived to HDFS (Q1) and merged in the next batch run (`--include-stream`).

**Why HBase and not just HDFS / MySQL?** We need millisecond random reads by key (a profile, adjacency lists for BFS) over millions of rows, atomic counters for streaming updates, and horizontal scaling across RegionServers. HDFS only supports sequential scans, and a single MySQL node doesn't scale out.

**Why pre-split the table?** Without pre-splitting, all writes go to one region on one RegionServer (hot-spotting). Splitting on the leading digit of the id spreads regions and load across both workers from the start.

**Why Kafka with 3 brokers?** Partitions let the stream be consumed in parallel. Replication factor 2 tolerates a broker failure. KRaft mode removes the ZooKeeper dependency (Kafka 3.x+).

**What happens if a worker dies?** HDFS keeps serving from the replica on the other worker. YARN re-runs the failed tasks on the surviving NodeManager. Kafka partitions fail over to the in-sync replica. HBase reassigns the dead RegionServer's regions (recovered from the WAL in HDFS).

**How does it scale to 100× more data?** Every heavy step is distributed (joins, Pregel-style iterations). The global windows used for ranking would become approximate quantiles. The motif fan-out is already bounded. We'd add worker nodes; nothing runs only on master except driver coordination.
