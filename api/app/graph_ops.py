"""Online graph queries executed against the serving layer (adjacency lists stored in HBase)."""
import time

USER_FIELDS = ["pagerank", "influence", "influence_rank", "community", "in_deg", "out_deg", "suspicion"]


def find_path(store, src, dst, directed=True, max_depth=6, frontier_cap=3000):
    """Bidirectional BFS: expands the smaller frontier each round, fetching adjacency rows in batches.

    directed=True  -> a chain of follows  src -> ... -> dst
    directed=False -> any connection (follow in either direction)
    """
    t0 = time.time()
    stats = {"rows_fetched": 0, "levels": 0}
    if src == dst:
        return {"path": [src], "stats": stats}
    fwd, bwd = {src: None}, {dst: None}
    f_front, b_front = [src], [dst]

    def build(meet):
        left, x = [], meet
        while x is not None:
            left.append(x)
            x = fwd[x]
        right, x = [], bwd[meet]
        while x is not None:
            right.append(x)
            x = bwd[x]
        return list(reversed(left)) + right

    for _ in range(max_depth):
        if not f_front or not b_front:
            break
        stats["levels"] += 1
        forward = len(f_front) <= len(b_front)
        front, parents, other = (f_front, fwd, bwd) if forward else (b_front, bwd, fwd)
        batch = front[:frontier_cap]
        adj = store.get_adjacency(batch)
        stats["rows_fetched"] += len(batch)
        nxt = []
        for u in batch:
            out, inn = adj.get(u, ([], []))
            if directed:
                nbrs = out if forward else inn
            else:
                nbrs = list(out) + list(inn)
            for w in nbrs:
                if w in parents:
                    continue
                parents[w] = u
                if w in other:
                    stats["ms"] = round((time.time() - t0) * 1000)
                    return {"path": build(w), "stats": stats}
                nxt.append(w)
        if forward:
            f_front = nxt
        else:
            b_front = nxt
    stats["ms"] = round((time.time() - t0) * 1000)
    return {"path": None, "stats": stats}


def ego_network(store, uid, size=40):
    """The user's most influential neighbours and the follow edges among them."""
    adj = store.get_adjacency([uid]).get(uid)
    if adj is None:
        return None
    out, inn = adj
    out_set, in_set = set(out), set(inn)
    cand = list(dict.fromkeys(list(out) + list(inn)))[:400]
    info = store.get_users(cand, USER_FIELDS)
    top = sorted(info.values(), key=lambda d: d.get("pagerank") or 0, reverse=True)[:size]
    ids = {uid} | {d["id"] for d in top}
    center = store.get_users([uid], USER_FIELDS).get(uid, {"id": uid})
    nodes = [dict(center, role="center")] + [
        dict(d, role="mutual" if d["id"] in out_set and d["id"] in in_set else
             ("following" if d["id"] in out_set else "follower")) for d in top]
    links = []
    neigh_adj = store.get_adjacency([d["id"] for d in top])
    for a, (a_out, _) in list(neigh_adj.items()) + [(uid, (out, inn))]:
        for b in a_out:
            if b in ids and b != a:
                links.append({"source": a, "target": b})
    return {"nodes": nodes, "links": links}
