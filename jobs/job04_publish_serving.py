"""
Job 04 - Publish batch views to the HBase serving layer (batch layer, step 4).

  gp_users        one row per user: metrics (m:), adjacency lists (n:), recommendations (r:)
  gp_communities  one row per community
  gp_meta         pre-aggregated documents for the dashboard (summary, leaderboard, viz graph, ...)

Writes are distributed: each Spark partition streams its rows to HBase over Thrift (happybase).
--snapshot DIR additionally dumps the same views as JSON on the driver (offline/demo backup for the API).

spark-submit jobs/job04_publish_serving.py [--snapshot /home/hadoop/graphpulse-snapshot]
"""
import argparse
import json
import os

from pyspark.sql import functions as F

from common import Paths, get_spark, hbase_connection, hdfs_exists, job_run, log, put_meta, write_hbase

USER_METRICS = {
    "inDegree": "in_deg", "outDegree": "out_deg", "neighbors": "neighbors", "reciprocity": "reciprocity",
    "pagerank": "pagerank", "pr_rank": "pr_rank", "influence": "influence", "influence_rank": "influence_rank",
    "community": "community", "component": "component", "triangles": "triangles", "clustering": "clustering",
    "hub_dist": "hub_dist", "hop_to_top_hub": "hop_to_top_hub", "behaviour_cluster": "behaviour_cluster",
    "anomaly_pct": "anomaly_pct", "suspicion": "suspicion", "susp_reasons": "susp_reasons",
}


def user_doc(r):
    d = {"id": r["id"]}
    for src, dst in USER_METRICS.items():
        val = r[src]
        d[dst] = json.loads(val) if dst == "hub_dist" and val else (list(val) if dst == "susp_reasons" and val else val)
    d["out"] = list(r["out"] or [])
    d["in"] = list(r["in"] or [])
    d["recs"] = [{"id": x["dst"], "score": round(x["score"], 4), "mutual": x["mutual"], "via": list(x["via"])}
                 for x in (r["recs"] or [])]
    return d


def user_row(r):
    d = user_doc(r)
    cols = {f"m:{k}": d[k] for k in USER_METRICS.values()}
    cols.update({"n:out": d["out"], "n:in": d["in"], "r:recs": d["recs"]})
    return str(r["id"]), cols


def rows_to_dicts(df):
    return [r.asDict(recursive=True) for r in df.collect()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-in", type=int, default=5000, help="cap stored follower lists")
    ap.add_argument("--viz-nodes", type=int, default=400)
    ap.add_argument("--snapshot", default=None, help="local directory for a JSON snapshot")
    ap.add_argument("--skip-hbase", action="store_true")
    args = ap.parse_args()

    spark = get_spark("04-publish-serving")
    with job_run(spark, "job04_publish_serving", **vars(args)) as m:
        users = spark.read.parquet(Paths.users)
        edges = spark.read.parquet(Paths.edges).select("src", "dst")
        recs = spark.read.parquet(Paths.recs)
        comm = spark.read.parquet(Paths.communities)
        links = spark.read.parquet(Paths.community_links)
        summary = json.loads(spark.read.text(Paths.summary).first()["value"])
        if hdfs_exists(spark, Paths.recs_eval):
            summary["recs_eval"] = json.loads(spark.read.text(Paths.recs_eval).first()["value"])

        out_l = edges.groupBy(F.col("src").alias("id")).agg(F.collect_list("dst").alias("out"))
        in_l = edges.groupBy(F.col("dst").alias("id")).agg(F.slice(F.collect_list("src"), 1, args.max_in).alias("in"))
        recs_l = recs.groupBy(F.col("src").alias("id")).agg(
            F.sort_array(F.collect_list(F.struct(F.col("score"), "dst", "mutual", "via")), asc=False).alias("recs"))
        full = users.join(out_l, "id", "left").join(in_l, "id", "left").join(recs_l, "id", "left").cache()
        m["users"] = full.count()

        # ------------------------------------------------ dashboard documents
        lb_cols = ["id", "influence", "influence_rank", "pagerank", "pr_rank", "inDegree", "outDegree",
                   "community", "clustering", "suspicion"]
        meta = {
            "summary": summary,
            "degree_hist": rows_to_dicts(spark.read.parquet(Paths.degree_hist).orderBy("degree")),
            "leaderboard": rows_to_dicts(users.orderBy("influence_rank").limit(100).select(*lb_cols)),
            "suspicious": rows_to_dicts(users.orderBy(F.desc("suspicion")).limit(100)
                                        .select(*lb_cols, "reciprocity", "anomaly_pct", "susp_reasons")),
        }
        comm_cols = ["community", "size", "size_rank", "density", "conductance", "avg_clustering",
                     "internal_edges", "cut_edges", "pagerank_mass", "top"]
        top_comm = comm.orderBy("size_rank").limit(60).select(*comm_cols)
        meta["communities_index"] = [dict(c, top=[t["id"] for t in c["top"][:5]]) for c in rows_to_dicts(top_comm)]
        top40 = [c["community"] for c in meta["communities_index"][:40]]
        meta["community_links"] = rows_to_dicts(
            links.filter(F.col("c_src").isin(top40) & F.col("c_dst").isin(top40)).orderBy(F.desc("weight")).limit(400))

        viz = users.orderBy("pr_rank").limit(args.viz_nodes).select("id", "pagerank", "influence", "community",
                                                                     "inDegree", "suspicion").cache()
        ids = viz.select(F.col("id").alias("src"))
        viz_links = edges.join(ids, "src").join(ids.select(F.col("src").alias("dst")), "dst")
        meta["viz_top"] = {"nodes": rows_to_dicts(viz), "links": rows_to_dicts(viz_links.select("src", "dst"))}
        meta["sample_edges"] = [[r["src"], r["dst"]] for r in edges.sample(fraction=min(1.0, 20000 / max(summary["edges"], 1)),
                                                                            seed=3).limit(20000).collect()]

        communities_all = comm.filter("size >= 3").select(*comm_cols)

        # ------------------------------------------------ HBase
        if not args.skip_hbase:
            log("writing gp_users (distributed, one Thrift connection per partition) ...")
            write_hbase(full.repartition(12), "gp_users", user_row)
            log("writing gp_communities ...")
            write_hbase(communities_all, "gp_communities",
                        lambda r: (str(r["community"]), {"c:json": dict(r.asDict(recursive=True),
                                                                         top=[t["id"] for t in r["top"]])}))
            conn = hbase_connection()
            try:
                for name, doc in meta.items():
                    put_meta(name, doc, conn)
            finally:
                conn.close()
            log("HBase serving layer updated")

        # ------------------------------------------------ optional JSON snapshot
        if args.snapshot:
            os.makedirs(os.path.join(args.snapshot, "meta"), exist_ok=True)
            for name, doc in meta.items():
                with open(os.path.join(args.snapshot, "meta", f"{name}.json"), "w") as f:
                    json.dump(doc, f, default=str)
            with open(os.path.join(args.snapshot, "communities.json"), "w") as f:
                json.dump([dict(c, top=[t["id"] for t in c["top"]]) for c in rows_to_dicts(communities_all)], f,
                          default=str)
            with open(os.path.join(args.snapshot, "users.jsonl"), "w") as f:
                for r in full.toLocalIterator():
                    f.write(json.dumps(user_doc(r), default=str) + "\n")
            log(f"snapshot written to {args.snapshot}")
        m["meta_documents"] = list(meta)


if __name__ == "__main__":
    main()
