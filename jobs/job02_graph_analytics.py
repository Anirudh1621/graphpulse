"""
Job 02 - Distributed graph intelligence with GraphFrames + Spark MLlib (batch layer, step 2).

Per user:
  * in/out degree, distinct neighbours, reciprocity (share of follows that are mutual)
  * PageRank + global rank, composite Influence Index (0-100)
  * weakly connected component            (GraphFrames connectedComponents)
  * community                              (GraphFrames labelPropagation)
  * triangles + local clustering coefficient (GraphFrames triangleCount)
  * hop distance to the top-K influencers  (GraphFrames shortestPaths, landmarks)
  * behaviour cluster + anomaly score      (MLlib StandardScaler + KMeans, distance-to-centroid)
  * suspicion score with human-readable reasons (anomaly score + interpretable rules)
Per community: size, internal edges, density, conductance, avg clustering, PageRank mass, top members
Global: modularity of the partition, reciprocity, transitivity, components, small-world reachability

spark-submit --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 jobs/job02_graph_analytics.py
"""
import argparse
import json
import math
import time

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import Paths, get_spark, job_run, log

ALL = Window.orderBy  # shorthand: global (un-partitioned) window, fine for ~10^5-10^6 users


def timed(metrics, name, build):
    """Run one algorithm, cache its result, and record how long it really took.

    `build` is a function that returns the result DataFrame. It is called inside the timer on purpose:
    pageRank, connectedComponents, labelPropagation, shortestPaths and the MLlib fit() calls do their
    work as soon as they are called, so a DataFrame built before the timer starts would hide that work.
    """
    t = time.time()
    df = build().cache()
    df.count()
    metrics.setdefault("algorithm_seconds", {})[name] = round(time.time() - t, 2)
    log(f"{name:<22} {metrics['algorithm_seconds'][name]:>8.2f}s")
    return df


def anomaly_scores(users, k):
    """Unsupervised behavioural clustering; distance to the assigned centroid = how unusual the account is."""
    feat = (users
            .withColumn("f_out", F.log1p("outDegree"))
            .withColumn("f_in", F.log1p("inDegree"))
            .withColumn("f_ratio", F.log1p("outDegree") - F.log1p("inDegree"))
            .withColumn("f_tri", F.log1p("triangles")))
    cols = ["f_out", "f_in", "f_ratio", "reciprocity", "clustering", "f_tri"]
    fv = VectorAssembler(inputCols=cols, outputCol="raw_features").transform(feat)
    fv = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True).fit(fv).transform(fv)
    model = KMeans(k=k, seed=42, featuresCol="features", predictionCol="behaviour_cluster", maxIter=30).fit(fv)

    centers = F.array(*[F.array(*[F.lit(float(x)) for x in c]) for c in model.clusterCenters()])
    scored = (model.transform(fv)
              .withColumn("fa", vector_to_array("features"))
              .withColumn("centers", centers)
              .withColumn("center", F.expr("element_at(centers, behaviour_cluster + 1)"))
              .withColumn("anomaly_dist", F.expr(
                  "sqrt(aggregate(zip_with(fa, center, (x, y) -> (x - y) * (x - y)), 0D, (acc, z) -> acc + z))"))
              .withColumn("anomaly_pct", F.percent_rank().over(ALL("anomaly_dist"))))

    r1 = (F.col("outDegree") >= 5 * (F.col("inDegree") + 1)) & (F.col("outDegree") >= 20)
    r2 = (F.col("reciprocity") < 0.05) & (F.col("outDegree") >= 20)
    r3 = (F.col("clustering") < 0.01) & (F.col("neighbors") >= 20)
    # Influencers are statistical outliers too - only outliers that lean bot-like (following > followers) count fully
    # lean: 0.25 for balanced/popular accounts, ramps to 1.0 when following >= 5x followers
    lean = F.least(F.lit(1.0), F.greatest(F.lit(0.25), (F.col("outDegree") / (F.col("inDegree") + 1) - 1) / 4))
    r4 = (F.col("anomaly_pct") >= 0.98) & (F.col("outDegree") >= 2 * (F.col("inDegree") + 1))
    rule_frac = (r1.cast("int") + r2.cast("int") + r3.cast("int")) / 3.0
    activity = F.least(F.lit(1.0), F.log1p("outDegree") / F.lit(math.log(101)))
    reasons = F.array(
        F.when(r1, F.lit("follows far more accounts than follow it")),
        F.when(r2, F.lit("almost nobody follows it back")),
        F.when(r3, F.lit("its contacts do not know each other")),
        F.when(r4, F.lit("statistical outlier in MLlib behaviour clustering")),
    )
    return (scored
            .withColumn("suspicion", F.round(100 * activity * (0.5 * lean * F.col("anomaly_pct") + 0.5 * rule_frac), 1))
            .withColumn("susp_reasons", F.filter(reasons, lambda x: x.isNotNull()))
            .drop("f_out", "f_in", "f_ratio", "f_tri", "raw_features", "features", "fa", "centers", "center"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pr-iter", type=int, default=15)
    ap.add_argument("--lpa-iter", type=int, default=5)
    ap.add_argument("--hubs", type=int, default=5, help="landmarks for shortest paths")
    ap.add_argument("--kmeans-k", type=int, default=8)
    ap.add_argument("--dataset-name", default="SNAP ego-Twitter follow graph")
    args = ap.parse_args()

    spark = get_spark("02-graph-analytics")
    from graphframes import GraphFrame  # provided by --packages graphframes:...

    spark.sparkContext.setCheckpointDir(f"{Paths.checkpoints}/graphframes")

    with job_run(spark, "job02_graph_analytics", **vars(args)) as m:
        v = spark.read.parquet(Paths.vertices).cache()
        e = spark.read.parquet(Paths.edges).cache()
        g = GraphFrame(v, e)
        n_users, n_edges = v.count(), e.count()
        m.update(users=n_users, edges=n_edges)
        log(f"graph loaded: {n_users:,} vertices / {n_edges:,} edges")

        # ---- degrees & reciprocity
        und = e.select("src", "dst").union(e.select(F.col("dst").alias("src"), F.col("src").alias("dst"))).distinct()
        deg = timed(m, "degrees", lambda: v.join(g.inDegrees, "id", "left").join(g.outDegrees, "id", "left")
                    .join(und.groupBy(F.col("src").alias("id")).agg(F.count("*").alias("neighbors")), "id", "left")
                    .join(e.groupBy(F.col("src").alias("id")).agg(F.sum(F.col("mutual").cast("int")).alias("mutual_out")),
                          "id", "left")
                    .fillna(0, subset=["inDegree", "outDegree", "neighbors", "mutual_out"]))

        # ---- GraphFrames algorithms
        pr = timed(m, "pagerank", lambda: g.pageRank(resetProbability=0.15, maxIter=args.pr_iter)
                   .vertices.select("id", "pagerank"))
        cc = timed(m, "connected_components", lambda: g.connectedComponents().select("id", "component"))
        lpa = timed(m, "label_propagation", lambda: g.labelPropagation(maxIter=args.lpa_iter)
                    .select("id", F.col("label").alias("community")))
        tri = timed(m, "triangle_count", lambda: g.triangleCount().select("id", F.col("count").alias("triangles")))

        hubs = [r["id"] for r in pr.orderBy(F.desc("pagerank")).limit(args.hubs).collect()]
        sp = timed(m, "shortest_paths", lambda: g.shortestPaths(landmarks=hubs).select("id", "distances"))

        # ---- assemble the user table
        users = (deg.join(pr, "id").join(cc, "id").join(lpa, "id").join(tri, "id", "left").join(sp, "id", "left")
                 .fillna(0, subset=["triangles"])
                 .withColumn("clustering", F.when(F.col("neighbors") > 1,
                                                  2.0 * F.col("triangles") / (F.col("neighbors") * (F.col("neighbors") - 1)))
                             .otherwise(0.0))
                 .withColumn("reciprocity", F.when(F.col("outDegree") > 0, F.col("mutual_out") / F.col("outDegree"))
                             .otherwise(0.0))
                 .withColumn("hop_to_top_hub", F.element_at("distances", F.lit(hubs[0]).cast("long")))
                 .withColumn("hub_dist", F.to_json("distances"))
                 .drop("distances"))

        users = (users
                 .withColumn("pr_pct", F.percent_rank().over(ALL("pagerank")))
                 .withColumn("in_pct", F.percent_rank().over(ALL("inDegree")))
                 .withColumn("influence", F.round(100 * (0.65 * F.col("pr_pct") + 0.35 * F.col("in_pct")), 2))
                 .withColumn("pr_rank", F.row_number().over(ALL(F.desc("pagerank"))))
                 .withColumn("influence_rank", F.row_number().over(ALL(F.desc("influence"), F.desc("pagerank"))))
                 .drop("pr_pct", "in_pct"))

        users = timed(m, "mllib_anomaly_kmeans", lambda: anomaly_scores(users, args.kmeans_k))
        users.write.mode("overwrite").parquet(Paths.users)
        users = spark.read.parquet(Paths.users).cache()

        # ---- community analytics
        uc = users.select("id", "community", "pagerank", "clustering", "neighbors")
        ec = (e.select("src", "dst")
              .join(uc.select(F.col("id").alias("src"), F.col("community").alias("c_src")), "src")
              .join(uc.select(F.col("id").alias("dst"), F.col("community").alias("c_dst")), "dst")
              .cache())
        internal = ec.filter("c_src = c_dst").groupBy(F.col("c_src").alias("community")) \
            .agg(F.count("*").alias("internal_edges"))
        cut_edges = ec.filter("c_src != c_dst")
        cut = (cut_edges.select(F.col("c_src").alias("community"))
               .union(cut_edges.select(F.col("c_dst").alias("community")))
               .groupBy("community").agg(F.count("*").alias("cut_edges")))
        comm = (uc.groupBy("community").agg(
                    F.count("*").alias("size"),
                    F.avg("clustering").alias("avg_clustering"),
                    F.sum("pagerank").alias("pagerank_mass"),
                    F.slice(F.sort_array(F.collect_list(F.struct("pagerank", "id")), asc=False), 1, 25).alias("top"))
                .join(internal, "community", "left").join(cut, "community", "left")
                .fillna(0, subset=["internal_edges", "cut_edges"])
                .withColumn("density", F.when(F.col("size") > 1,
                                              F.col("internal_edges") / (F.col("size") * (F.col("size") - 1))).otherwise(0.0))
                .withColumn("conductance", F.when((2 * F.col("internal_edges") + F.col("cut_edges")) > 0,
                                                  F.col("cut_edges") / (2 * F.col("internal_edges") + F.col("cut_edges")))
                            .otherwise(1.0))
                .withColumn("size_rank", F.row_number().over(ALL(F.desc("size"), F.desc("pagerank_mass")))))
        comm = timed(m, "community_stats", lambda: comm)
        comm.write.mode("overwrite").parquet(Paths.communities)
        cut_edges.groupBy("c_src", "c_dst").agg(F.count("*").alias("weight")) \
            .write.mode("overwrite").parquet(Paths.community_links)

        # ---- modularity Q of the LPA partition (undirected view)
        m_und = und.count() / 2.0
        und_c = (und.join(uc.select(F.col("id").alias("src"), F.col("community").alias("c1")), "src")
                    .join(uc.select(F.col("id").alias("dst"), F.col("community").alias("c2")), "dst")
                    .filter("c1 = c2").groupBy("c1").agg((F.count("*") / 2.0).alias("L")))
        vol = uc.groupBy("community").agg(F.sum("neighbors").alias("d"))
        q_row = (vol.join(und_c, vol.community == und_c.c1, "left").fillna(0, subset=["L"])
                 .agg(F.sum(F.col("L") / m_und - F.pow(F.col("d") / (2 * m_und), 2)).alias("Q"))).first()
        modularity = float(q_row["Q"] or 0.0)

        # ---- degree distribution
        din = users.groupBy(F.col("inDegree").alias("degree")).agg(F.count("*").alias("in_count"))
        dout = users.groupBy(F.col("outDegree").alias("degree")).agg(F.count("*").alias("out_count"))
        din.join(dout, "degree", "full").fillna(0).orderBy("degree").coalesce(1) \
            .write.mode("overwrite").parquet(Paths.degree_hist)

        # ---- global summary
        agg = users.agg(F.sum("triangles").alias("tri"), F.avg("clustering").alias("avg_cc"),
                        F.max("inDegree").alias("max_in"), F.max("outDegree").alias("max_out"),
                        F.avg("outDegree").alias("avg_out"),
                        F.sum(F.when(F.col("hop_to_top_hub").isNotNull(), 1).otherwise(0)).alias("reach_n"),
                        F.avg("hop_to_top_hub").alias("avg_hops"),
                        F.sum(F.when(F.col("suspicion") >= 50, 1).otherwise(0)).alias("suspicious")).first()
        comps = cc.groupBy("component").count()
        csum = comm.agg(F.count("*").alias("n"), F.sum(F.when(F.col("size") >= 3, 1).otherwise(0)).alias("n3"),
                        F.max("size").alias("largest")).first()
        mutual = e.filter("mutual").count()
        # say where the job really ran, so the dashboard never claims a cluster that was not used
        master = spark.sparkContext.master
        where = "on YARN" if master.startswith("yarn") else (
            "in local mode on one machine" if master.startswith("local") else f"on {master}")
        summary = {
            "dataset": args.dataset_name,
            "engine": f"Spark {spark.version} + GraphFrames {where} ({spark.sparkContext.defaultParallelism} cores)",
            "users": n_users, "edges": n_edges,
            "density": n_edges / (n_users * (n_users - 1)) if n_users > 1 else 0,
            "avg_out_degree": float(agg["avg_out"]), "max_in_degree": int(agg["max_in"]),
            "max_out_degree": int(agg["max_out"]),
            "reciprocity": mutual / n_edges if n_edges else 0,
            "triangles": int(agg["tri"] or 0) // 3, "avg_clustering": float(agg["avg_cc"] or 0),
            "components": comps.count(), "largest_component": int(comps.agg(F.max("count")).first()[0]),
            "communities": int(csum["n"]), "communities_3plus": int(csum["n3"]),
            "largest_community": int(csum["largest"]), "modularity": modularity,
            "top_hubs": hubs, "reach_top_hub_pct": 100.0 * agg["reach_n"] / n_users,
            "avg_hops_to_top_hub": float(agg["avg_hops"] or 0), "suspicious_accounts": int(agg["suspicious"]),
            "algorithm_seconds": m.get("algorithm_seconds", {}),
        }
        spark.createDataFrame([(json.dumps(summary),)], ["value"]).coalesce(1).write.mode("overwrite").text(Paths.summary)
        m.update({k: summary[k] for k in ("communities", "modularity", "avg_clustering", "triangles")})
        log(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
