"""
GraphPulse speed layer - Spark Structured Streaming over Kafka.

Kafka 'gp-events'  (JSON: {event_id, type: follow|unfollow|like|mention, src, dst, ts(ms)})
  |
  |-- Q1 data lake      : append raw events to HDFS Parquet (partitioned by date) -> next batch run merges them
  |-- Q2 throughput     : 5 s tumbling windows, events per type                     -> Kafka 'gp-insights'
  |-- Q3 rising stars   : 2 min sliding window (10 s slide) new followers per user  -> Kafka + HBase gp_meta
  |-- Q4 threat radar   : follow bursts per account (30 s window) above a threshold,
  |                       enriched with the batch suspicion score (stream-static join) -> Kafka + HBase gp_alerts
  `-- Q5 live counters  : follower/following deltas as atomic HBase counters (gp_users s:*)

All aggregations use event-time watermarks, so late events are handled and state stays bounded.

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 jobs/stream_graph_events.py
"""
import argparse
import json
import time

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from common import HBASE_HOST, HBASE_PORT, KAFKA_BOOTSTRAP, Paths, get_spark, hdfs_exists, log

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("type", StringType()),
    StructField("src", LongType()),
    StructField("dst", LongType()),
    StructField("ts", DoubleType()),          # epoch milliseconds
    StructField("scenario", StringType()),    # baseline | viral | bot (simulator tag, for the demo)
])
INSIGHTS_TOPIC = "gp-insights"


def to_kafka(df, kind):
    """Publish a micro-batch DataFrame as JSON messages on gp-insights."""
    (df.withColumn("kind", F.lit(kind))
       .select(F.col("kind").alias("key"), F.to_json(F.struct("*")).alias("value"))
       .write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("topic", INSIGHTS_TOPIC)
       .save())


def hbase_rows(rows_iter_fn):
    """Wrap a per-partition HBase writer so it can be used with foreachPartition."""
    host, port = HBASE_HOST, HBASE_PORT

    def _run(rows):
        import happybase
        conn = happybase.Connection(host, port=port, timeout=60000)
        try:
            rows_iter_fn(conn, rows)
        finally:
            conn.close()
    return _run


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--burst-threshold", type=int, default=25, help="follows per 30 s that raise an alert")
    ap.add_argument("--starting-offsets", default="latest")
    args = ap.parse_args()

    spark = get_spark("stream-graph-events")
    spark.conf.set("spark.sql.shuffle.partitions", "6")
    ck = f"{Paths.checkpoints}/stream"

    # static batch view used to enrich alerts (stream-static join)
    if hdfs_exists(spark, Paths.users):
        profile = spark.read.parquet(Paths.users).select(
            F.col("id").alias("src"), F.col("suspicion").alias("batch_suspicion"),
            F.col("influence").alias("batch_influence"), F.col("community")).cache()
    else:
        profile = None
    log(f"static enrichment table: {'loaded' if profile is not None else 'not available'}")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", "gp-events")
           .option("startingOffsets", args.starting_offsets)
           .option("failOnDataLoss", "false")
           .option("maxOffsetsPerTrigger", 50000)
           .load())
    ev = (raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
             .select("e.*")
             .filter(F.col("src").isNotNull() & F.col("type").isNotNull())
             .withColumn("event_time", (F.col("ts") / 1000).cast("timestamp"))
             .withColumn("date", F.to_date("event_time")))

    # ---------------- Q1: immutable raw archive in the HDFS data lake
    q1 = (ev.writeStream.format("parquet").option("path", Paths.lake_events)
            .option("checkpointLocation", f"{ck}/lake").partitionBy("date")
            .trigger(processingTime="30 seconds").queryName("lake").start())

    # ---------------- Q2: throughput per event type
    thr = (ev.withWatermark("event_time", "20 seconds")
             .groupBy(F.window("event_time", "5 seconds"), "type").count())

    def q2_batch(df, _id):
        out = df.select(F.col("window.start").cast("long").alias("window_start"), "type", "count")
        to_kafka(out, "throughput")

    q2 = (thr.writeStream.outputMode("update").foreachBatch(q2_batch)
             .option("checkpointLocation", f"{ck}/throughput").trigger(processingTime="5 seconds")
             .queryName("throughput").start())

    # ---------------- Q3: rising stars (who is gaining followers right now)
    rising = (ev.filter("type = 'follow'").withWatermark("event_time", "30 seconds")
                .groupBy(F.window("event_time", "2 minutes", "10 seconds"), F.col("dst").alias("user"))
                .agg(F.count("*").alias("new_followers")))

    def q3_batch(df, _id):
        now_s = time.time()
        # newest window per user, only windows that are still "open"
        cur = (df.filter(F.col("window.end").cast("long") >= F.lit(now_s - 15))
                 .groupBy("user").agg(F.max("new_followers").alias("new_followers"))
                 .orderBy(F.desc("new_followers")).limit(10))
        top = [r.asDict() for r in cur.collect()]
        if not top:
            return
        to_kafka(spark.createDataFrame([(json.dumps(top),)], ["items"]), "rising")
        try:
            import happybase
            conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
            conn.table("gp_meta").put(b"live_rising", {b"d:json": json.dumps({"ts": now_s, "items": top}).encode()})
            conn.close()
        except Exception as exc:
            log(f"warn: HBase live_rising write failed: {exc}")

    q3 = (rising.writeStream.outputMode("update").foreachBatch(q3_batch)
                .option("checkpointLocation", f"{ck}/rising").trigger(processingTime="10 seconds")
                .queryName("rising").start())

    # ---------------- Q4: threat radar - follow-spam bursts
    bursts = (ev.filter("type = 'follow'").withWatermark("event_time", "30 seconds")
                .groupBy(F.window("event_time", "30 seconds", "10 seconds"), "src")
                .agg(F.count("*").alias("follows"), F.approx_count_distinct("dst").alias("distinct_targets"))
                .filter(F.col("follows") >= args.burst_threshold))
    threshold = args.burst_threshold

    def q4_batch(df, _id):
        alerts = df.select(F.col("window.start").cast("long").alias("window_start"), "src", "follows",
                           "distinct_targets")
        if profile is not None:
            alerts = alerts.join(F.broadcast(profile), "src", "left")
        else:
            alerts = alerts.withColumn("batch_suspicion", F.lit(None).cast("double")) \
                           .withColumn("batch_influence", F.lit(None).cast("double")) \
                           .withColumn("community", F.lit(None).cast("long"))
        alerts = alerts.withColumn("severity", F.when(
            (F.col("follows") >= 2 * threshold) | (F.coalesce("batch_suspicion", F.lit(0)) >= 50), "critical")
            .otherwise("high")).withColumn("detected_at", F.lit(time.time())).cache()
        if alerts.rdd.isEmpty():
            return
        to_kafka(alerts, "alert")

        def write(conn, rows):
            t = conn.table("gp_alerts")
            for r in rows:
                key = f"{9999999999 - int(r['window_start']):010d}:{r['src']}"   # newest first on scan
                t.put(key.encode(), {b"a:json": json.dumps(r.asDict(), default=str).encode()})
        alerts.foreachPartition(hbase_rows(write))
        alerts.unpersist()

    q4 = (bursts.writeStream.outputMode("update").foreachBatch(q4_batch)
                .option("checkpointLocation", f"{ck}/threats").trigger(processingTime="5 seconds")
                .queryName("threats").start())

    # ---------------- Q5: live follower counters in the serving layer (atomic HBase increments)
    def q5_batch(df, _id):
        sign = F.when(F.col("type") == "follow", 1).otherwise(-1)
        rel = df.filter(F.col("type").isin("follow", "unfollow")).withColumn("d", sign)
        deltas = (rel.groupBy(F.col("dst").alias("user")).agg(F.sum("d").alias("followers"))
                  .join(rel.groupBy(F.col("src").alias("user")).agg(F.sum("d").alias("following")), "user", "full")
                  .fillna(0))

        def write(conn, rows):
            t = conn.table("gp_users")
            for r in rows:
                key = str(r["user"]).encode()
                if r["followers"]:
                    t.counter_inc(key, b"s:followers_delta", int(r["followers"]))
                if r["following"]:
                    t.counter_inc(key, b"s:following_delta", int(r["following"]))
        deltas.foreachPartition(hbase_rows(write))

    q5 = (ev.writeStream.foreachBatch(q5_batch).option("checkpointLocation", f"{ck}/counters")
            .trigger(processingTime="10 seconds").queryName("counters").start())

    log("streaming queries running: lake, throughput, rising, threats, counters  (Spark UI -> Structured Streaming)")
    for q in (q1, q2, q3, q4, q5):
        log(f"  {q.name}: {q.id}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
