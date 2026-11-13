# Checking the bot detector with planted bots

Job 02 gives every account a suspicion score from 0 to 100 (MLlib KMeans "how unusual" + three rules). No account in the SNAP data is labelled as a bot, so there is nothing to check the score against. The usual way to test such an unsupervised detector is to plant fake accounts whose behaviour you control, run the normal jobs, and count how many it finds. This page does that. The first run found a blind spot; the score was then fixed and checked again.

**Run on 13 Nov 2026:**
- **Data:** SNAP ego-Twitter (81,306 real accounts, 1,768,135 follows) plus 250 planted bots, made by `tools/plant_bots.py`. The main bot set uses seed 7 (60,271 follows). A second, fresh set uses seed 8 (58,300 follows).
- **Engine:** Spark 3.5.1 in local mode on one PC (Docker, 6 cores).
- **Jobs:** 01 and 02, then `jobs/eval_bot_detection.py`.

## The planted bots

| Kind | How it behaves | What it tests |
|---|---|---|
| spam | follows 100–500 random accounts; 2% follow back | classic follow-spam |
| fan | follows 100–500 popular accounts; 1% follow back | bots following celebrities, whose contacts know each other |
| ring | 5 groups of 10 bots that all follow each other, plus 50–300 random follows each | bots that give each other followers to look normal |
| quiet | follows only 15–40 random accounts; 2% follow back | "low and slow" bots |
| farm | follows 100–400 random accounts; 40% follow back | "follow for follow" farms: as many followers as follows, like a normal user |

## First run: results before the fix

- **Caught:** suspicion ≥ 50, the line the dashboard uses for "suspicious accounts".
- **AUC:** the chance that a random bot of that kind scores higher than a random real account (1.0 = perfect, 0.5 = guessing). It measures how well the score *ranks* bots, wherever the line is drawn.
- **Simpler signals:** the last three columns score each signal on its own, to show what each part of the suspicion score adds:
  - **KMeans only:** the `anomaly_pct` column.
  - **Rules only:** how many of the three rules an account breaks.
  - **Follow ratio:** following ÷ (followers + 1).

| Kind | Caught (≥ 50) | Median score | AUC: full score | KMeans only | Rules only | Follow ratio |
|---|---|---|---|---|---|---|
| spam | 50 / 50 (100%) | 99.8 | 1.000 | 0.998 | 1.000 | 0.999 |
| fan | 50 / 50 (100%) | 83.2 | 1.000 | 0.998 | 0.995 | 1.000 |
| ring | 50 / 50 (100%) | 82.7 | 0.998 | 0.985 | 0.995 | 0.994 |
| quiet | 38 / 50 (76%) | 70.0 | 0.991 | 0.997 | 0.877 | 0.996 |
| farm | **0 / 50 (0%)** | 35.2 | 0.981 | 0.993 | 0.984 | 0.898 |
| **all** | **188 / 250 (75%)** | 82.7 | **0.994** | 0.994 | 0.970 | 0.977 |

Real accounts flagged: 1,038 of 81,306 (1.28%). Of the 250 most suspicious accounts, 122 are planted bots (precision@250 = 0.49).

## The blind spot, and the fix

Not one follow-back farm was caught: their median score was 35, under the line of 50. Yet KMeans on its own ranks them as very unusual (AUC 0.993), so the signal was there, and the formula threw it away in two places:
- **The "lean" factor:** it shrinks the KMeans part for any account with about as many followers as follows. It is there to protect influencers, who are unusual too.
- **Only one rule fires:** the farm bots break just one of the three rules ("its contacts do not know each other").

The plain follow ratio does worst of all on them (0.898), because hiding that ratio is exactly what a farm does.

**The fix (job 02, `anomaly_scores`):** an account now leans bot-like when it follows at least as many accounts as follow it *and* its contacts do not know each other. Before, the only way to lean bot-like was to follow 5 times more accounts than follow it. The idea:
- Real people's contacts form circles, so their contacts know each other.
- Influencers' followers are strangers to each other too, but influencers have more followers than follows, so they stay protected.

**Checked before changing the job:** the score is a formula over columns that job 02 saves, so the new formula could be recomputed exactly on the saved results.
- In the real data, only 5 of 81,306 Twitter accounts have contacts who do not know each other, and all 5 are influencers. No Facebook account does.
- So the fix changes **no real account's score** in either dataset: 1,182 Twitter and 176 Facebook accounts are flagged, before and after.

## Results after the fix

Job 02 was run again with the fix, on the same bots (seed 7) and on a fresh set (seed 8):

| Kind | Before (seed 7) | After (seed 7) | After (seed 8, fresh bots) |
|---|---|---|---|
| spam | 50 / 50 | 50 / 50 | 50 / 50 |
| fan | 50 / 50 | 50 / 50 | 50 / 50 |
| ring | 50 / 50 | 50 / 50 | 50 / 50 |
| quiet | 38 / 50 | 38 / 50 | 42 / 50 |
| farm | **0 / 50** | **50 / 50** (median 66) | **50 / 50** (median 66) |
| **all caught** | 188 / 250 (75%) | **238 / 250 (95%)** | **242 / 250 (97%)** |
| AUC, full score | 0.994 | 0.997 | 0.997 |
| real accounts flagged | 1,038 | 1,038 | 947 |

The old formula would have caught 192 of the seed-8 bots, again with 0 farms. So the gain comes from the fix, not from the new sample.

## What this shows

1. **The score ranks bots well:** AUC 0.997 after the fix. It catches every spam, fan, ring and farm bot in both bot sets.
2. **Testing found a real blind spot.** On unlabelled data nobody would have noticed it: every follow-back farm passed as normal. The fix closed it without flagging a single extra real account here.
3. **Quiet bots slip under the rules, by design.** All three rules ignore accounts with fewer than 20 follows or contacts, so 8–12 of the 50 quiet bots stay under the line.
4. **The flagged real accounts may not be mistakes.** The most suspicious ones follow about 200 accounts and have 1 or 2 followers, which is exactly the spam pattern. They may be real spam accounts, or a side effect of how SNAP collected the ego networks. Without labels nobody can tell, so the 1.2–1.3% flagged is an upper bound on false alarms, not a measured rate.
5. **What each part adds:**
   - **Ranking:** after the fix, the full score ranks better than KMeans alone (0.997 vs 0.994).
   - **Reasons:** the rules add the human-readable reasons shown on the dashboard.
   - **Rules alone:** they are clearly weaker (0.970).

## Limits

- **Simple bots.** The bots are simple models. Real bots vary more and change their behaviour when they get caught.
- **Farms could adapt.** The fix relies on farm bots following strangers. A farm that follows accounts inside one community, whose members know each other, would get past it. That was not tested.
- **Other data may differ.** In these ego-network datasets almost no real account has contacts who don't know each other, so the fix cost nothing here. On a different kind of graph, for example a random crawl, it may flag more real accounts. Rerun this check there.
- **The fix was written after seeing the farm results.** The same kind of farm then tested it. The fresh seed-8 bots help, but they come from the same bot model. The main evidence that the fix is safe is that it changes no real account's score.
- **Unknown real bots.** The real accounts are counted as humans, although some are probably bots. That makes the AUCs slightly pessimistic.
- **Scores shift with the data.** The KMeans part is a percentile, so adding bots shifts everyone's score a little: 1,182 real accounts are flagged without bots, and 1,038 and 947 with the two bot sets.

## Run it again

**1. Plant the bots** (plain Python, about 10 seconds; add `--seed N` for a different sample):

```bash
python3 tools/plant_bots.py --edges data/twitter_combined.txt --out data/twitter_bots
```

This writes `data/twitter_bots.txt` (the real edges plus the bots' edges) and `data/twitter_bots_labels.csv` (the id and kind of each bot).

**2a. On one machine** (Spark in local mode; results go to a local folder, not HDFS):

```bash
export GP_HDFS_BASE=file://$PWD/lake-bots
mkdir -p lake-bots/raw/edges && cp data/twitter_bots.txt lake-bots/raw/edges/
S="spark-submit --master local[*] --driver-memory 3g --py-files jobs/common.py"
$S jobs/job01_ingest.py
$S --repositories https://repos.spark-packages.org --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
   jobs/job02_graph_analytics.py --dataset-name "Twitter + planted bots"
$S jobs/eval_bot_detection.py --labels data/twitter_bots_labels.csv --out bot_eval.json
```

**2b. On the cluster** (on master). The results go to a separate HDFS folder, so the real results are not touched. `scripts/run_pipeline.sh` cannot be used here, because it always uses `/graphpulse`.

```bash
source /etc/profile.d/bigdata.sh
export GP_HDFS_BASE=hdfs://master:9000/graphpulse-bots
hdfs dfs -mkdir -p /graphpulse-bots/raw/edges
hdfs dfs -put -f data/twitter_bots.txt /graphpulse-bots/raw/edges/
S="spark-submit --master yarn --py-files jobs/common.py --conf spark.executorEnv.GP_HDFS_BASE=$GP_HDFS_BASE"
$S jobs/job01_ingest.py
$S --repositories https://repos.spark-packages.org --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
   jobs/job02_graph_analytics.py --dataset-name "Twitter + planted bots"
$S jobs/eval_bot_detection.py --labels data/twitter_bots_labels.csv --out bot_eval.json
```

The evaluation prints a table like the ones above and writes it to `bot_eval.json`.
