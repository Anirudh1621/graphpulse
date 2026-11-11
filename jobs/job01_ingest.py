"""
Job 01 - Ingest & curate the social graph (batch layer, step 1).

  raw edge list on HDFS  (SNAP format: "<follower> <followee>" per line, '#' comments)
      + (optional) follow/unfollow events archived by the streaming layer in the HDFS data lake
  -> cleaned, de-duplicated, typed edges with a 'mutual' (reciprocal follow) flag
  -> vertex table
  stored as Parquet under /graphpulse/curated

spark-submit jobs/job01_ingest.py [--include-stream]
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import Paths, get_spark, hdfs_exists, job_run, log


def read_edge_list(spark, path):
    raw = spark.read.text(path)
    parts = F.split(F.trim(F.col("value")), r"\s+")
    lines = raw.filter((F.length(F.trim("value")) > 0) & ~F.col("value").startswith("#"))
    edges = lines.select(parts.getItem(0).cast("long").alias("src"),
                         parts.getItem(1).cast("long").alias("dst")).dropna()
    return lines, edges


def merge_stream_events(spark, edges):
    """Lambda architecture: fold the speed layer's follow/unfollow events back into the master dataset."""
    if not hdfs_exists(spark, Paths.lake_events):
        log("no streaming events in the data lake yet - skipping merge")
        return edges, 0, 0
    ev = (spark.read.parquet(Paths.lake_events)
          .filter(F.col("type").isin("follow", "unfollow"))
          .select("src", "dst", "type", "ts"))
    latest = (ev.withColumn("rn", F.row_number().over(Window.partitionBy("src", "dst").orderBy(F.desc("ts"))))
                .filter("rn = 1"))
    follows = latest.filter("type = 'follow'").select("src", "dst")
    unfollows = latest.filter("type = 'unfollow'").select("src", "dst")
    merged = edges.unionByName(follows).join(unfollows, ["src", "dst"], "left_anti")
    return merged, follows.count(), unfollows.count()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=Paths.raw_edges)
    ap.add_argument("--include-stream", action="store_true",
                    help="merge follow/unfollow events captured by the streaming layer")
    args = ap.parse_args()

    spark = get_spark("01-ingest")
    with job_run(spark, "job01_ingest", include_stream=args.include_stream) as m:
        lines, edges = read_edge_list(spark, args.input)
        m["raw_lines"] = lines.count()

        self_loops = edges.filter("src = dst").count()
        edges = edges.filter("src != dst").dropDuplicates(["src", "dst"])

        if args.include_stream:
            edges, m["stream_follows_merged"], m["stream_unfollows_applied"] = merge_stream_events(spark, edges)
            edges = edges.dropDuplicates(["src", "dst"])

        # reciprocity flag: does dst also follow src?
        rev = edges.select(F.col("dst").alias("src"), F.col("src").alias("dst")).withColumn("mutual", F.lit(True))
        edges = (edges.join(rev, ["src", "dst"], "left")
                      .withColumn("mutual", F.coalesce("mutual", F.lit(False)))
                      .repartition(24, "src")
                      .cache())

        vertices = edges.select(F.col("src").alias("id")).union(edges.select(F.col("dst").alias("id"))).distinct()

        edges.write.mode("overwrite").parquet(Paths.edges)
        vertices.write.mode("overwrite").parquet(Paths.vertices)

        m["self_loops_removed"] = self_loops
        m["edges"] = edges.count()
        if not args.include_stream:
            m["duplicates_removed"] = m["raw_lines"] - self_loops - m["edges"]
        m["vertices"] = spark.read.parquet(Paths.vertices).count()
        m["mutual_edges"] = edges.filter("mutual").count()
        log(f"curated graph: {m['vertices']:,} users, {m['edges']:,} follow edges, {m['mutual_edges']:,} mutual")


if __name__ == "__main__":
    main()
