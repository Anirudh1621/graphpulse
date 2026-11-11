"""
Builds an OFFLINE demo snapshot (same JSON layout that job04 --snapshot produces) from a synthetic
social graph, computing every metric with plain Python. Used for UI development and as a fallback
when the cluster is not reachable. It is clearly labelled "synthetic" in the dashboard.

  python tools/make_demo_snapshot.py --users 3000 --out demo-snapshot
"""
import argparse
import json
import math
import os
import random
import time
from collections import Counter, defaultdict, deque


def generate(n, seed):
    rng = random.Random(seed)
    ids = rng.sample(range(10_000_000, 999_999_999), n)
    # power-law community sizes
    k = max(8, n // 220)
    weights = [1 / (i + 1) ** 0.8 for i in range(k)]
    comm_of = {u: rng.choices(range(k), weights)[0] for u in ids}
    members = defaultdict(list)
    for u, c in comm_of.items():
        members[c].append(u)
    indeg = Counter()
    edges = set()
    order = ids[:]
    rng.shuffle(order)
    bots = set(order[:max(5, n // 120)])
    for u in order:
        if u in bots:
            continue
        out_d = min(int(rng.lognormvariate(2.3, 0.8)) + 1, 250)
        for _ in range(out_d):
            pool = members[comm_of[u]] if rng.random() < 0.82 else ids
            cand = rng.sample(pool, min(4, len(pool)))
            v = max(cand, key=lambda x: indeg[x] + rng.random() * 3)     # preferential attachment
            if v != u and (u, v) not in edges:
                edges.add((u, v))
                indeg[v] += 1
                if rng.random() < 0.38:
                    edges.add((v, u))
                    indeg[u] += 1
    for b in bots:                                                      # follow-spam accounts
        for v in rng.sample(ids, rng.randint(120, 260)):
            if v != b:
                edges.add((b, v))
    return ids, sorted(edges), bots


def pagerank(nodes, out_adj, iters=30, d=0.85):
    n = len(nodes)
    pr = {u: 1 / n for u in nodes}
    for _ in range(iters):
        nxt = {u: (1 - d) / n for u in nodes}
        dangling = sum(pr[u] for u in nodes if not out_adj[u])
        for u in nodes:
            if out_adj[u]:
                share = d * pr[u] / len(out_adj[u])
                for v in out_adj[u]:
                    nxt[v] += share
        for u in nodes:
            nxt[u] += d * dangling / n
        pr = nxt
    return pr


def label_propagation(nodes, und, iters, rng):
    label = {u: u for u in nodes}
    order = list(nodes)
    for _ in range(iters):
        rng.shuffle(order)
        for u in order:
            if und[u]:
                c = Counter(label[v] for v in und[u])
                best = max(c.values())
                label[u] = min(lbl for lbl, x in c.items() if x == best)
    return label


def components(nodes, und):
    comp, cid = {}, 0
    for s in nodes:
        if s in comp:
            continue
        comp[s] = s
        q = deque([s])
        while q:
            u = q.popleft()
            for v in und[u]:
                if v not in comp:
                    comp[v] = s
                    q.append(v)
        cid += 1
    return comp


def bfs_dist(start, adj):
    dist, q = {start: 0}, deque([start])
    while q:
        u = q.popleft()
        for v in adj[u]:
            if v not in dist:
                dist[v] = dist[u] + 1
                q.append(v)
    return dist


def adamic_adar(nodes, out_adj, edge_set, deg, pr, fanout=20, top_n=10):
    pruned = {u: sorted(out_adj[u], key=lambda v: -pr[v])[:fanout] for u in nodes}
    recs = {}
    for a in nodes:
        score, mutual, via = defaultdict(float), Counter(), defaultdict(list)
        for b in pruned[a]:
            w = 1 / math.log(max(deg[b], 2))
            for c in pruned[b]:
                if c != a and (a, c) not in edge_set:
                    score[c] += w
                    mutual[c] += 1
                    if len(via[c]) < 3:
                        via[c].append(b)
        best = sorted(score, key=lambda c: (-score[c], -mutual[c]))[:top_n]
        recs[a] = [{"id": c, "score": round(score[c], 4), "mutual": mutual[c], "via": via[c]} for c in best]
    return recs


def pct_rank(values):
    """percent_rank like Spark: (rank-1)/(n-1)."""
    items = sorted(values.items(), key=lambda x: x[1])
    n, out, i = len(items), {}, 0
    while i < n:
        j = i
        while j + 1 < n and items[j + 1][1] == items[i][1]:
            j += 1
        for t in range(i, j + 1):
            out[items[t][0]] = i / (n - 1) if n > 1 else 0
        i = j + 1
    return out


def evaluate_recs(nodes, edges, rng, k=10):
    test = [e for e in edges if rng.random() < 0.05]
    test_set = set(test)
    train = [e for e in edges if e not in test_set]
    out_adj = defaultdict(list)
    indeg = Counter()
    for a, b in train:
        out_adj[a].append(b)
        indeg[b] += 1
    deg = Counter()
    for a, b in train:
        deg[a] += 1
        deg[b] += 1
    pr = pagerank(nodes, out_adj, 15)
    tset = set(train)
    users = list({a for a, _ in test})
    truth = defaultdict(set)
    for a, b in test:
        truth[a].add(b)
    pruned = {u: sorted(out_adj[u], key=lambda v: -pr[v])[:20] for u in nodes}
    popular = [u for u, _ in indeg.most_common(k * 4)]

    def metrics(pred_fn):
        p = r = hr = 0.0
        for u in users:
            pred = pred_fn(u)
            hits = len(set(pred) & truth[u])
            p += hits / k
            r += hits / len(truth[u])
            hr += hits > 0
        n = len(users) or 1
        return {"precision_at_k": p / n, "recall_at_k": r / n, "hit_rate": hr / n}

    def aa(u):
        score = defaultdict(float)
        for b in pruned.get(u, []):
            w = 1 / math.log(max(deg[b], 2))
            for c in pruned[b]:
                if c != u and (u, c) not in tset:
                    score[c] += w
        return sorted(score, key=lambda c: -score[c])[:k]

    def pop(u):
        return [v for v in popular if v != u and (u, v) not in tset][:k]

    res = {"k": k, "eval_users": len(users), "fanout": 20, "adamic_adar": metrics(aa),
           "popularity_baseline": metrics(pop)}
    b = res["popularity_baseline"]["precision_at_k"]
    res["lift_vs_baseline"] = res["adamic_adar"]["precision_at_k"] / b if b else None
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..", "demo-snapshot"))
    args = ap.parse_args()
    rng = random.Random(args.seed)
    timings = {}

    t = time.time()
    nodes, edges, bots = generate(args.users, args.seed)
    edge_set = set(edges)
    out_adj, in_adj, und = defaultdict(list), defaultdict(list), defaultdict(set)
    for a, b in edges:
        out_adj[a].append(b)
        in_adj[b].append(a)
        und[a].add(b)
        und[b].add(a)
    nodes = [u for u in nodes if und[u]]
    timings["generate"] = time.time() - t

    t = time.time()
    pr = pagerank(nodes, out_adj)
    timings["pagerank"] = time.time() - t
    t = time.time()
    comp = components(nodes, und)
    timings["connected_components"] = time.time() - t
    t = time.time()
    comm = label_propagation(nodes, und, 5, rng)
    timings["label_propagation"] = time.time() - t

    t = time.time()
    tri = Counter()
    for u in nodes:
        for v in und[u]:
            if v > u:
                for w in und[u] & und[v]:
                    if w > v:
                        tri[u] += 1
                        tri[v] += 1
                        tri[w] += 1
    timings["triangle_count"] = time.time() - t

    t = time.time()
    hubs = sorted(nodes, key=lambda u: -pr[u])[:5]
    hub_dist = {h: bfs_dist(h, in_adj) for h in hubs}       # reverse BFS = distance from v to hub
    timings["shortest_paths"] = time.time() - t

    users = {}
    for u in nodes:
        k_ = len(und[u])
        mutual_out = sum(1 for v in out_adj[u] if (v, u) in edge_set)
        users[u] = {"id": u, "in_deg": len(in_adj[u]), "out_deg": len(out_adj[u]), "neighbors": k_,
                    "reciprocity": mutual_out / len(out_adj[u]) if out_adj[u] else 0.0, "pagerank": pr[u],
                    "community": comm[u], "component": comp[u], "triangles": tri[u],
                    "clustering": 2 * tri[u] / (k_ * (k_ - 1)) if k_ > 1 else 0.0,
                    "hub_dist": {str(h): hub_dist[h][u] for h in hubs if u in hub_dist[h]},
                    "hop_to_top_hub": hub_dist[hubs[0]].get(u)}
    prp, inp = pct_rank({u: d["pagerank"] for u, d in users.items()}), pct_rank({u: d["in_deg"] for u, d in users.items()})
    for u, d in users.items():
        d["influence"] = round(100 * (0.65 * prp[u] + 0.35 * inp[u]), 2)
    for i, u in enumerate(sorted(users, key=lambda x: -users[x]["pagerank"])):
        users[u]["pr_rank"] = i + 1
    for i, u in enumerate(sorted(users, key=lambda x: (-users[x]["influence"], -users[x]["pagerank"]))):
        users[u]["influence_rank"] = i + 1

    # anomaly: standardized feature distance from the population mean (stand-in for the MLlib KMeans score)
    t = time.time()
    feats = {u: [math.log1p(d["out_deg"]), math.log1p(d["in_deg"]), math.log1p(d["out_deg"]) - math.log1p(d["in_deg"]),
                 d["reciprocity"], d["clustering"], math.log1p(d["triangles"])] for u, d in users.items()}
    cols = list(zip(*feats.values()))
    mu = [sum(c) / len(c) for c in cols]
    sd = [math.sqrt(sum((x - m) ** 2 for x in c) / len(c)) or 1 for c, m in zip(cols, mu)]
    dist = {u: math.sqrt(sum(((x - m) / s) ** 2 for x, m, s in zip(f, mu, sd))) for u, f in feats.items()}
    ap_ = pct_rank(dist)
    for u, d in users.items():
        r1 = d["out_deg"] >= 5 * (d["in_deg"] + 1) and d["out_deg"] >= 20
        r2 = d["reciprocity"] < 0.05 and d["out_deg"] >= 20
        r3 = d["clustering"] < 0.01 and d["neighbors"] >= 20
        lean = min(1.0, max(0.25, (d["out_deg"] / (d["in_deg"] + 1) - 1) / 4))
        r4 = ap_[u] >= 0.98 and d["out_deg"] >= 2 * (d["in_deg"] + 1)
        act = min(1.0, math.log1p(d["out_deg"]) / math.log(101))
        d["anomaly_pct"] = ap_[u]
        d["behaviour_cluster"] = int(dist[u] > 3)
        d["suspicion"] = round(100 * act * (0.5 * lean * ap_[u] + 0.5 * (r1 + r2 + r3) / 3), 1)
        d["susp_reasons"] = [s for s, f in [("follows far more accounts than follow it", r1),
                                            ("almost nobody follows it back", r2),
                                            ("its contacts do not know each other", r3),
                                            ("statistical outlier in behaviour clustering", r4)] if f]
    timings["anomaly_detection"] = time.time() - t

    t = time.time()
    deg = {u: users[u]["in_deg"] + users[u]["out_deg"] for u in users}
    recs = adamic_adar(nodes, out_adj, edge_set, deg, pr)
    timings["recommendations"] = time.time() - t
    recs_eval = evaluate_recs(nodes, edges, random.Random(7))

    for u, d in users.items():
        d["out"], d["in"], d["recs"] = out_adj[u], in_adj[u], recs[u]

    # communities
    cm = defaultdict(list)
    for u, d in users.items():
        cm[d["community"]].append(u)
    internal, cut, links = Counter(), Counter(), Counter()
    for a, b in edges:
        ca, cb = comm[a], comm[b]
        if ca == cb:
            internal[ca] += 1
        else:
            cut[ca] += 1
            cut[cb] += 1
            links[(ca, cb)] += 1
    m_und = sum(len(und[u]) for u in nodes) / 2
    L = Counter()
    for u in nodes:
        for v in und[u]:
            if comm[u] == comm[v] and u < v:
                L[comm[u]] += 1
    modularity = sum(L[c] / m_und - (sum(len(und[u]) for u in mem) / (2 * m_und)) ** 2 for c, mem in cm.items())
    comms = []
    for c, mem in cm.items():
        size = len(mem)
        top = sorted(mem, key=lambda u: -pr[u])[:25]
        comms.append({"community": c, "size": size, "internal_edges": internal[c], "cut_edges": cut[c],
                      "density": internal[c] / (size * (size - 1)) if size > 1 else 0,
                      "conductance": cut[c] / (2 * internal[c] + cut[c]) if (2 * internal[c] + cut[c]) else 1,
                      "avg_clustering": sum(users[u]["clustering"] for u in mem) / size,
                      "pagerank_mass": sum(pr[u] for u in mem), "top": top})
    comms.sort(key=lambda c: (-c["size"], -c["pagerank_mass"]))
    for i, c in enumerate(comms):
        c["size_rank"] = i + 1

    n_edges = len(edges)
    comp_sizes = Counter(comp.values())
    reach = [d["hop_to_top_hub"] for d in users.values() if d["hop_to_top_hub"] is not None]
    summary = {
        "dataset": f"Synthetic demo graph ({len(users):,} users) - offline snapshot",
        "engine": "python-local (offline)",
        "users": len(users), "edges": n_edges, "density": n_edges / (len(users) * (len(users) - 1)),
        "avg_out_degree": n_edges / len(users), "max_in_degree": max(d["in_deg"] for d in users.values()),
        "max_out_degree": max(d["out_deg"] for d in users.values()),
        "reciprocity": sum(1 for a, b in edges if (b, a) in edge_set) / n_edges,
        "triangles": sum(tri.values()) // 3,
        "avg_clustering": sum(d["clustering"] for d in users.values()) / len(users),
        "components": len(comp_sizes), "largest_component": max(comp_sizes.values()),
        "communities": len(comms), "communities_3plus": sum(1 for c in comms if c["size"] >= 3),
        "largest_community": comms[0]["size"], "modularity": modularity, "top_hubs": hubs,
        "reach_top_hub_pct": 100 * len(reach) / len(users), "avg_hops_to_top_hub": sum(reach) / len(reach),
        "suspicious_accounts": sum(1 for d in users.values() if d["suspicion"] >= 50),
        "algorithm_seconds": {k: round(v, 2) for k, v in timings.items()}, "recs_eval": recs_eval,
    }

    lb_cols = ["id", "influence", "influence_rank", "pagerank", "pr_rank", "in_deg", "out_deg", "community",
               "clustering", "suspicion"]

    def lb(d, extra=()):
        row = {k: d[k] for k in lb_cols + list(extra)}
        row["inDegree"], row["outDegree"] = row.pop("in_deg"), row.pop("out_deg")
        return row

    by_inf = sorted(users.values(), key=lambda d: d["influence_rank"])
    viz_ids = {d["id"] for d in sorted(users.values(), key=lambda d: d["pr_rank"])[:400]}
    hist = defaultdict(lambda: {"in_count": 0, "out_count": 0})
    for d in users.values():
        hist[d["in_deg"]]["in_count"] += 1
        hist[d["out_deg"]]["out_count"] += 1
    top40 = {c["community"] for c in comms[:40]}
    meta = {
        "summary": summary,
        "degree_hist": [{"degree": k, **v} for k, v in sorted(hist.items())],
        "leaderboard": [lb(d) for d in by_inf[:100]],
        "suspicious": [lb(d, ("reciprocity", "anomaly_pct", "susp_reasons"))
                       for d in sorted(users.values(), key=lambda d: -d["suspicion"])[:100]],
        "communities_index": [dict(c, top=c["top"][:5]) for c in comms[:60]],
        "community_links": [{"c_src": a, "c_dst": b, "weight": w} for (a, b), w in links.most_common()
                            if a in top40 and b in top40][:400],
        "viz_top": {"nodes": [{"id": u, "pagerank": users[u]["pagerank"], "influence": users[u]["influence"],
                               "community": users[u]["community"], "inDegree": users[u]["in_deg"],
                               "suspicion": users[u]["suspicion"]} for u in viz_ids],
                    "links": [{"src": a, "dst": b} for a, b in edges if a in viz_ids and b in viz_ids]},
        "sample_edges": [list(e) for e in random.Random(3).sample(edges, min(20000, len(edges)))],
    }

    out = os.path.abspath(args.out)
    os.makedirs(os.path.join(out, "meta"), exist_ok=True)
    for name, doc in meta.items():
        with open(os.path.join(out, "meta", f"{name}.json"), "w") as f:
            json.dump(doc, f)
    with open(os.path.join(out, "communities.json"), "w") as f:
        json.dump([c for c in comms if c["size"] >= 3], f)
    with open(os.path.join(out, "users.jsonl"), "w") as f:
        for d in users.values():
            f.write(json.dumps(d) + "\n")
    print(f"snapshot -> {out}")
    print(json.dumps({k: v for k, v in summary.items() if k not in ("algorithm_seconds", "top_hubs")}, indent=2))


if __name__ == "__main__":
    main()
