"""
Job 03 - "People You May Know": link prediction with GraphFrames motif finding (batch layer, step 3).

Candidate generation : motif  (a)-[]->(b); (b)-[]->(c)   on a degree-pruned graph, minus existing follows
Scoring              : Adamic-Adar   score(a,c) = sum over intermediaries b of 1 / log(deg(b))
                       (shared contacts who are themselves "small" are stronger evidence than celebrity hubs)
Evaluation (--evaluate): hide a random 5% of edges, recommend on the remaining 95%,
                       measure precision@K / recall@K / hit-rate against a popularity baseline.

spark-submit --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 jobs/job03_recommendations.py --evaluate
"""
import argparse
import json

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import Paths, get_spark, job_run, log


def recommend(GraphFrame, users, edges, fanout, top_n):
    """Returns (src, dst, score, mutual, via) - top_n recommendations per user."""
    deg = users.select("id", (F.col("inDegree") + F.col("outDegree")).alias("deg"),
                       F.col("pagerank").alias("pr"))

    # prune: keep each user's `fanout` most influential followees -> bounds the 2-hop explosion
    pruned = (edges.join(deg.select(F.col("id").alias("dst"), "pr"), "dst")
              .withColumn("rn", F.row_number().over(Window.partitionBy("src").orderBy(F.desc("pr"))))
              .filter(F.col("rn") <= fanout).select("src", "dst"))

    g = GraphFrame(deg, pruned)
    paths = g.find("(a)-[]->(b); (b)-[]->(c)").filter("a.id != c.id")
    cand = paths.select(F.col("a.id").alias("src"), F.col("c.id").alias("dst"), F.col("b.id").alias("via"),
                        (1.0 / F.log(F.greatest(F.col("b.deg"), F.lit(2)))).alias("w"))
    cand = cand.join(edges.select("src", "dst"), ["src", "dst"], "left_anti")  # not already followed

    scored = cand.groupBy("src", "dst").agg(F.sum("w").alias("score"), F.count("*").alias("mutual"),
                                            F.slice(F.collect_list("via"), 1, 3).alias("via"))
    w = Window.partitionBy("src").orderBy(F.desc("score"), F.desc("mutual"))
    return scored.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= top_n)


def evaluate(spark, GraphFrame, users, edges, fanout, k, sample_users):
    train_test = edges.withColumn("r", F.rand(seed=7))
    test = train_test.filter("r < 0.05").select("src", "dst")
    train = train_test.filter("r >= 0.05").select("src", "dst")

    eval_users = test.select("src").distinct().sample(fraction=sample_users, seed=11).cache()
    test = test.join(eval_users, "src")
    n_eval = eval_users.count()
    log(f"evaluating on {n_eval:,} users / {test.count():,} hidden edges")

    recs = recommend(GraphFrame, users, train, fanout, k).join(eval_users, "src").select("src", "dst")

    # popularity baseline: globally most-followed accounts the user does not already follow
    popular = users.orderBy(F.desc("inDegree")).limit(k * 4).select(F.col("id").alias("dst"), "inDegree")
    base = (eval_users.crossJoin(popular).join(train, ["src", "dst"], "left_anti").filter("src != dst")
            .withColumn("rn", F.row_number().over(Window.partitionBy("src").orderBy(F.desc("inDegree"))))
            .filter(F.col("rn") <= k).select("src", "dst"))

    truth = test.groupBy("src").agg(F.count("*").alias("n_true"))

    def metrics(pred, name):
        hits = pred.join(test, ["src", "dst"]).groupBy("src").agg(F.count("*").alias("hits"))
        per_user = truth.join(hits, "src", "left").fillna(0, subset=["hits"])
        r = per_user.agg(F.avg(F.col("hits") / k).alias("p"), F.avg(F.col("hits") / F.col("n_true")).alias("r"),
                         F.avg((F.col("hits") > 0).cast("double")).alias("hr")).first()
        out = {"precision_at_k": float(r["p"] or 0), "recall_at_k": float(r["r"] or 0), "hit_rate": float(r["hr"] or 0)}
        log(f"{name:<22} P@{k}={out['precision_at_k']:.4f}  R@{k}={out['recall_at_k']:.4f}  HR={out['hit_rate']:.4f}")
        return out

    result = {"k": k, "eval_users": n_eval, "fanout": fanout,
              "adamic_adar": metrics(recs, "Adamic-Adar (motif)"),
              "popularity_baseline": metrics(base, "Popularity baseline")}
    b = result["popularity_baseline"]["precision_at_k"]
    result["lift_vs_baseline"] = result["adamic_adar"]["precision_at_k"] / b if b > 0 else None
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fanout", type=int, default=20, help="followees kept per user when generating 2-hop candidates")
    ap.add_argument("--top-n", type=int, default=10)
    ap.add_argument("--evaluate", action="store_true")
    ap.add_argument("--eval-sample", type=float, default=0.3, help="fraction of test users to evaluate")
    args = ap.parse_args()

    spark = get_spark("03-recommendations")
    from graphframes import GraphFrame

    with job_run(spark, "job03_recommendations", **vars(args)) as m:
        users = spark.read.parquet(Paths.users).select("id", "inDegree", "outDegree", "pagerank").cache()
        edges = spark.read.parquet(Paths.edges).select("src", "dst").cache()

        if args.evaluate:
            result = evaluate(spark, GraphFrame, users, edges, args.fanout, args.top_n, args.eval_sample)
            spark.createDataFrame([(json.dumps(result),)], ["value"]).coalesce(1) \
                .write.mode("overwrite").text(Paths.recs_eval)
            m["evaluation"] = result

        recs = recommend(GraphFrame, users, edges, args.fanout, args.top_n)
        recs.write.mode("overwrite").parquet(Paths.recs)
        recs = spark.read.parquet(Paths.recs)
        m["recommendations"] = recs.count()
        m["users_with_recs"] = recs.select("src").distinct().count()
        log(f"{m['recommendations']:,} recommendations for {m['users_with_recs']:,} users")


if __name__ == "__main__":
    main()
