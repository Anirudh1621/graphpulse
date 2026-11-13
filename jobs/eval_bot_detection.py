"""
Check the bot detector against planted bots (made by tools/plant_bots.py).

Run it after jobs 01-02 have processed the edge list with the planted bots. It reads job 02's user table
(Paths.users) and the bot labels, and reports:

  per kind of bot  caught = share of bots with suspicion >= 50 (the dashboard's "suspicious" line),
                   their median suspicion, and AUC against the real accounts
  all bots         AUC = the chance that a random bot scores higher than a random real account
                   (1.0 = perfect, 0.5 = a coin flip). Given for the full suspicion score and for
                   simpler signals on their own, to show what each part of the score adds.
                   P@K = share of planted bots among the K most suspicious accounts (K = number of bots)
  real accounts    how many are flagged (suspicion >= 50). Nobody knows their true labels, so these are
                   real bots or false alarms: an upper bound on the false-alarm rate.

spark-submit --py-files jobs/common.py jobs/eval_bot_detection.py --labels data/twitter_bots_labels.csv \
    [--out bot_eval.json]
"""
import argparse
import csv
import json

from common import Paths, get_spark, job_run, log

THRESHOLD = 50     # the same line job 02 uses for "suspicious_accounts"
RULE_REASONS = {   # the three rules of job 02's suspicion score, by the reason text it stores
    "follows far more accounts than follow it",
    "almost nobody follows it back",
    "its contacts do not know each other",
}


def auc(df, col):
    """Area under the ROC curve, from ranks (Mann-Whitney U); tied scores count as half right."""
    ranks = df[col].rank(method="average")
    pos = df["is_bot"]
    n_pos, n_neg = int(pos.sum()), int((~pos).sum())
    return float((ranks[pos].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--labels", required=True, help="CSV with id,kind of every planted bot")
    ap.add_argument("--out", help="also write the results to this JSON file (on the driver machine)")
    args = ap.parse_args()

    with open(args.labels) as f:
        labels = {int(r["id"]): r["kind"] for r in csv.DictReader(f)}

    spark = get_spark("eval-bot-detection")
    with job_run(spark, "eval_bot_detection", labels=args.labels) as m:
        df = (spark.read.parquet(Paths.users)
              .select("id", "suspicion", "anomaly_pct", "inDegree", "outDegree", "susp_reasons")
              .toPandas())
        df["kind"] = df["id"].map(labels).fillna("real")
        df["is_bot"] = df["kind"] != "real"
        missing = len(labels) - int(df["is_bot"].sum())
        if missing:
            log(f"warn: {missing} labelled bots are not in the user table")

        # simpler signals, to compare with the full score
        df["rules_only"] = df["susp_reasons"].apply(lambda rs: sum(r in RULE_REASONS for r in (rs if rs is not None else [])))
        df["follow_ratio"] = df["outDegree"] / (df["inDegree"] + 1)

        bots, real = df[df["is_bot"]], df[~df["is_bot"]]
        signals = {"suspicion": "suspicion", "kmeans_only": "anomaly_pct", "rules_only": "rules_only",
                   "follow_ratio": "follow_ratio"}

        def summary(b, sub):
            return {"bots": len(b), "caught": int((b["suspicion"] >= THRESHOLD).sum()),
                    "caught_pct": round(100 * float((b["suspicion"] >= THRESHOLD).mean()), 1),
                    "median_suspicion": round(float(b["suspicion"].median()), 1),
                    "auc": {name: round(auc(sub, col), 4) for name, col in signals.items()}}

        k = len(bots)
        top_k = df.nlargest(k, "suspicion")
        top_real = real.nlargest(5, "suspicion")
        res = {"threshold": THRESHOLD, "real_accounts": len(real),
               "all": summary(bots, df),
               "per_kind": {kind: summary(bots[bots["kind"] == kind], df[(df["kind"] == kind) | ~df["is_bot"]])
                            for kind in sorted(bots["kind"].unique())},
               "precision_at_k": round(float(top_k["is_bot"].mean()), 3),
               "real_flagged": int((real["suspicion"] >= THRESHOLD).sum()),
               "real_flagged_pct": round(100 * float((real["suspicion"] >= THRESHOLD).mean()), 2),
               "top_real": [{"id": int(r.id), "suspicion": float(r.suspicion), "following": int(r.outDegree),
                             "followers": int(r.inDegree)} for r in top_real.itertuples()]}
        m.update(res)

        log(f"{'kind':<8}{'bots':>5}{'caught':>8}{'median':>8}   AUC: {'score':>6}{'kmeans':>8}{'rules':>7}{'ratio':>7}")
        for kind, r in list(res["per_kind"].items()) + [("all", res["all"])]:
            a = r["auc"]
            log(f"{kind:<8}{r['bots']:>5}{r['caught_pct']:>7.0f}%{r['median_suspicion']:>8.1f}        "
                f"{a['suspicion']:>6.3f}{a['kmeans_only']:>8.3f}{a['rules_only']:>7.3f}{a['follow_ratio']:>7.3f}")
        log(f"P@{k} = {res['precision_at_k']:.3f}   real accounts flagged: {res['real_flagged']:,} "
            f"({res['real_flagged_pct']}%)")
        log("most suspicious real accounts: " + "; ".join(
            f"{t['id']} ({t['suspicion']:.0f}, follows {t['following']}, {t['followers']} followers)"
            for t in res["top_real"]))
        if args.out:
            with open(args.out, "w") as f:
                json.dump(res, f, indent=2)
            log(f"results written to {args.out}")


if __name__ == "__main__":
    main()
