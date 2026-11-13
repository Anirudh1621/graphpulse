"""
Plant fake bot accounts in a SNAP follow graph, so the bot detector (the suspicion score from job 02)
can be checked against known answers.

Real social graphs come without labels, so nobody can say which real accounts are bots. The usual way
to test an unsupervised detector is to hide accounts whose behaviour you control and see whether it
finds them. The real accounts stay unlabelled: some of them may be real bots.

Five kinds of bot (50 of each by default):
  spam   follows 100-500 random accounts; 2% of them follow back
  fan    follows 100-500 popular accounts (picked in proportion to their followers); 1% follow back
  ring   groups of 10 bots that all follow each other, to look normal, plus 50-300 random follows
  quiet  follows only 15-40 random accounts; 2% follow back ("low and slow")
  farm   follow-back farm: follows 100-400 random accounts and 40% follow back ("follow for follow"),
         so its follows-to-followers ratio looks normal

Writes:
  OUT.txt         the original edges followed by the bots' edges (same "follower followee" format)
  OUT_labels.csv  id,kind for every planted bot

Example:
  python tools/plant_bots.py --edges data/twitter_combined.txt --out data/twitter_bots
"""
import argparse
import csv
import random
import shutil

FIRST_BOT_ID = 9_100_000_000      # far above real SNAP ids, and apart from the live demo's bots (9_000_000_000+)
RING_SIZE = 10


def read_edges(path):
    edges = []
    with open(path) as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                a, b = line.split()[:2]
                edges.append((int(a), int(b)))
    return edges


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--edges", required=True, help="SNAP edge list to plant the bots in")
    ap.add_argument("--out", required=True, help="output prefix, e.g. data/twitter_bots")
    ap.add_argument("--per-kind", type=int, default=50, help="bots of each kind (ring bots: a multiple of 10)")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    edges = read_edges(args.edges)
    users = sorted({u for e in edges for u in e})
    followees = [b for _, b in edges]          # picking from this list = picking in proportion to followers
    print(f"read {len(edges):,} edges between {len(users):,} accounts")

    new_edges, labels = [], []
    next_id = FIRST_BOT_ID

    def new_bot(kind):
        nonlocal next_id
        next_id += 1
        labels.append((next_id, kind))
        return next_id

    def follow(bot, targets, follow_back):
        for t in targets:
            new_edges.append((bot, t))
            if rng.random() < follow_back:
                new_edges.append((t, bot))

    def popular(n):
        picked = set()
        while len(picked) < n:
            picked.add(rng.choice(followees))
        return list(picked)

    for _ in range(args.per_kind):
        follow(new_bot("spam"), rng.sample(users, rng.randint(100, 500)), 0.02)
    for _ in range(args.per_kind):
        follow(new_bot("fan"), popular(rng.randint(100, 500)), 0.01)
    for _ in range(max(1, args.per_kind // RING_SIZE)):
        ring = [new_bot("ring") for _ in range(RING_SIZE)]
        new_edges.extend((a, b) for a in ring for b in ring if a != b)
        for bot in ring:
            follow(bot, rng.sample(users, rng.randint(50, 300)), 0.02)
    for _ in range(args.per_kind):
        follow(new_bot("quiet"), rng.sample(users, rng.randint(15, 40)), 0.02)
    for _ in range(args.per_kind):
        follow(new_bot("farm"), rng.sample(users, rng.randint(100, 400)), 0.40)

    out_edges, out_labels = f"{args.out}.txt", f"{args.out}_labels.csv"
    shutil.copyfile(args.edges, out_edges)
    with open(out_edges, "a", newline="\n") as f:   # plain \n line ends, like the SNAP files, on any OS
        f.writelines(f"{a} {b}\n" for a, b in new_edges)
    with open(out_labels, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "kind"])
        w.writerows(labels)
    print(f"planted {len(labels)} bots with {len(new_edges):,} edges -> {out_edges}, {out_labels}")


if __name__ == "__main__":
    main()
