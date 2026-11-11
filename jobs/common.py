"""Shared helpers for the GraphPulse Spark jobs (paths, Spark session, HBase I/O, run tracking)."""
import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone

HDFS_BASE = os.environ.get("GP_HDFS_BASE", "hdfs://master:9000/graphpulse")
HBASE_HOST = os.environ.get("GP_HBASE_HOST", "master")
HBASE_PORT = int(os.environ.get("GP_HBASE_PORT", "9090"))
KAFKA_BOOTSTRAP = os.environ.get("GP_KAFKA", "master:9092,worker1:9092,worker2:9092")


class Paths:
    raw_edges = f"{HDFS_BASE}/raw/edges"
    lake_events = f"{HDFS_BASE}/lake/events"          # raw stream events archived by the speed layer
    edges = f"{HDFS_BASE}/curated/edges"
    vertices = f"{HDFS_BASE}/curated/vertices"
    users = f"{HDFS_BASE}/analytics/users"
    communities = f"{HDFS_BASE}/analytics/communities"
    community_links = f"{HDFS_BASE}/analytics/community_links"
    degree_hist = f"{HDFS_BASE}/analytics/degree_hist"
    summary = f"{HDFS_BASE}/analytics/summary"
    recs = f"{HDFS_BASE}/analytics/recommendations"
    recs_eval = f"{HDFS_BASE}/analytics/recommendations_eval"
    runs = f"{HDFS_BASE}/runs"
    checkpoints = f"{HDFS_BASE}/_checkpoints"


def get_spark(app_name):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"GraphPulse-{app_name}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def hdfs_exists(spark, path):
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(conf).exists(p)


def log(msg):
    print(f"[GraphPulse {datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# --------------------------------------------------------------------------- HBase
def _enc(v):
    if isinstance(v, bytes):
        return v
    if isinstance(v, (list, dict, tuple)):
        return json.dumps(v, separators=(",", ":"), default=str).encode()
    if isinstance(v, float):
        return repr(round(v, 8)).encode()
    return str(v).encode()


def hbase_connection():
    import happybase

    return happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=120000)


def write_hbase(df, table, row_fn, batch_size=500):
    """Distributed write: every Spark partition opens its own Thrift connection and batches puts.

    row_fn(row) -> (row_key: str, {"cf:col": value, ...})
    """
    host, port = HBASE_HOST, HBASE_PORT

    def _write_partition(rows):
        import happybase

        conn = happybase.Connection(host, port=port, timeout=120000)
        try:
            with conn.table(table).batch(batch_size=batch_size) as b:
                for r in rows:
                    key, cols = row_fn(r)
                    b.put(str(key).encode(), {c.encode(): _enc(v) for c, v in cols.items() if v is not None})
        finally:
            conn.close()

    df.foreachPartition(_write_partition)


def put_meta(name, value, conn=None):
    """Store a JSON document in gp_meta (row = name, column d:json)."""
    own = conn is None
    conn = conn or hbase_connection()
    try:
        conn.table("gp_meta").put(name.encode(), {b"d:json": _enc(value)})
    finally:
        if own:
            conn.close()


# --------------------------------------------------------------------------- run tracking
@contextmanager
def job_run(spark, job_name, **params):
    """Times a job and records it (HDFS + HBase gp_meta 'run:*') so the UI can show benchmark history."""
    sc = spark.sparkContext
    started = time.time()
    run = {"job": job_name, "params": params, "metrics": {}, "app_id": sc.applicationId,
           "started_at": datetime.now(timezone.utc).isoformat()}
    log(f"=== {job_name} started (app {sc.applicationId}) ===")
    status = "ok"
    try:
        yield run["metrics"]
    except Exception:
        status = "failed"
        raise
    finally:
        run["status"] = status
        run["duration_s"] = round(time.time() - started, 2)
        try:
            run["executors"] = max(sc._jsc.sc().getExecutorMemoryStatus().size() - 1, 0)
        except Exception:
            run["executors"] = None
        run["default_parallelism"] = sc.defaultParallelism
        log(f"=== {job_name} {status} in {run['duration_s']}s | {json.dumps(run['metrics'], default=str)} ===")
        try:
            spark.createDataFrame([(json.dumps(run, default=str),)], ["value"]) \
                .coalesce(1).write.mode("append").text(Paths.runs)
        except Exception as exc:  # never fail the job because of bookkeeping
            log(f"warn: could not write run record to HDFS: {exc}")
        try:
            put_meta(f"run:{int(started)}:{job_name}", run)
        except Exception as exc:
            log(f"warn: could not write run record to HBase: {exc}")
