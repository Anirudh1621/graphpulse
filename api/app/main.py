"""GraphPulse API - REST + WebSocket gateway over the HBase serving layer and the Kafka stream.

Run:  uvicorn app.main:app --host 0.0.0.0 --port 8000      (from the api/ directory)
Docs: http://<host>:8000/docs
"""
import logging
import math
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import config
from .cluster import cluster_status
from .graph_ops import USER_FIELDS, ego_network, find_path
from .live import LiveHub, start_live
from .store import open_store

log = logging.getLogger("graphpulse")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

state = {"store": None, "live": None}
hub = LiveHub()


@asynccontextmanager
async def lifespan(_app):
    state["store"] = open_store(config.BACKEND, config.SNAPSHOT_DIR, config.HBASE_HOST, config.HBASE_PORT, log.info)
    await hub.start()
    state["live"] = start_live(hub, state["store"], config.LIVE_MODE, config.KAFKA, log.info)
    yield


app = FastAPI(title="GraphPulse API", version="1.0", lifespan=lifespan,
              description="Social network intelligence on Hadoop, Spark/GraphFrames, Kafka and HBase")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def store():
    return state["store"]


def meta(name, default=None):
    doc = store().get_meta(name)
    return default if doc is None else doc


def public_user(d):
    """Strip the (potentially huge) adjacency lists before sending a user over the wire."""
    d = dict(d)
    out, inn = d.pop("out", None) or [], d.pop("in", None) or []
    d["out_count"], d["in_count"] = len(out), len(inn)
    stream = d.get("stream") or {}
    d["live_followers"] = (d.get("in_deg") or 0) + stream.get("followers_delta", 0)
    d["live_following"] = (d.get("out_deg") or 0) + stream.get("following_delta", 0)
    return d


# ------------------------------------------------------------------------------------ overview
@app.get("/api/health")
def health():
    live = state["live"]
    return {"ok": True, "backend": store().kind, "live": live.status() if live else {"mode": "off"},
            "ws_clients": len(hub.clients), "messages": dict(hub.stats)}


@app.get("/api/summary")
def summary():
    s = meta("summary", {})
    if not s:
        raise HTTPException(503, "batch views not published yet - run the pipeline")
    return s


@app.get("/api/degree-distribution")
def degree_distribution():
    """Log-binned in/out degree distribution (power-law check)."""
    rows = meta("degree_hist", [])
    bins = {}
    for r in rows:
        d = r["degree"]
        b = 0 if d == 0 else 2 ** int(math.log2(d))
        x = bins.setdefault(b, {"bin": b, "in_count": 0, "out_count": 0})
        x["in_count"] += r.get("in_count") or 0
        x["out_count"] += r.get("out_count") or 0
    raw = [{"degree": r["degree"], "in_count": r.get("in_count") or 0, "out_count": r.get("out_count") or 0}
           for r in rows if r["degree"] > 0]
    return {"binned": [bins[k] for k in sorted(bins)], "raw": raw}


@app.get("/api/leaderboard")
def leaderboard(limit: int = Query(50, le=100)):
    return meta("leaderboard", [])[:limit]


@app.get("/api/suspicious")
def suspicious(limit: int = Query(50, le=100)):
    return meta("suspicious", [])[:limit]


@app.get("/api/graph/top")
def graph_top():
    g = meta("viz_top", {"nodes": [], "links": []})
    return {"nodes": g["nodes"], "links": [{"source": l["src"], "target": l["dst"]} for l in g["links"]]}


@app.get("/api/communities")
def communities():
    return {"communities": meta("communities_index", []), "links": meta("community_links", [])}


@app.get("/api/communities/{cid}")
def community(cid: int):
    c = store().get_community(cid)
    if not c:
        raise HTTPException(404, "community not found")
    members = store().get_users(c.get("top", [])[:25], USER_FIELDS)
    c = dict(c)
    c["members"] = sorted(members.values(), key=lambda d: d.get("pagerank") or 0, reverse=True)
    return c


# ------------------------------------------------------------------------------------ users
@app.get("/api/users/search")
def search(q: str = Query(..., min_length=1)):
    q = q.strip()
    if not q.isdigit():
        return []
    ids = store().search(q, 10)
    return list(store().get_users(ids, USER_FIELDS).values())


@app.get("/api/users/{uid}")
def user(uid: int):
    d = store().get_user(uid)
    if not d:
        raise HTTPException(404, "user not found")
    d = public_user(d)
    recs = d.get("recs") or []
    info = store().get_users([r["id"] for r in recs], USER_FIELDS)
    d["recs"] = [dict(r, **{k: info.get(r["id"], {}).get(k) for k in ("influence", "community", "in_deg")})
                 for r in recs]
    return d


@app.get("/api/users/{uid}/ego")
def ego(uid: int, size: int = Query(40, le=80)):
    g = ego_network(store(), uid, size)
    if not g:
        raise HTTPException(404, "user not found")
    return g


@app.get("/api/path")
def path(src: int, dst: int, mode: str = Query("directed", pattern="^(directed|undirected)$")):
    res = find_path(store(), src, dst, directed=(mode == "directed"))
    if res["path"]:
        info = store().get_users(res["path"], USER_FIELDS)
        res["nodes"] = [info.get(i, {"id": i}) for i in res["path"]]
        res["hops"] = len(res["path"]) - 1
    return res


# ------------------------------------------------------------------------------------ live
@app.get("/api/live/rising")
def live_rising():
    return store().get_meta("live_rising") or {"items": []}


@app.get("/api/alerts")
def alerts(limit: int = Query(50, le=200)):
    return store().list_alerts(limit)


class SimRequest(BaseModel):
    action: str                 # viral | bot | baseline_on | baseline_off
    target: int | None = None
    duration: int = 45
    rate: float | None = None


@app.post("/api/simulate")
def simulate(req: SimRequest):
    live = state["live"]
    if not live:
        raise HTTPException(503, "live layer not running")
    try:
        return live.control(req.action, req.target, req.duration, req.rate)
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    hub.clients.add(ws)
    try:
        await ws.send_json({"kind": "hello", "mode": hub.mode, "recent": list(hub.recent)})
        while True:
            await ws.receive_text()   # keep-alive / ignore client messages
    except WebSocketDisconnect:
        pass
    finally:
        hub.clients.discard(ws)


# ------------------------------------------------------------------------------------ cluster
@app.get("/api/cluster")
def cluster():
    return cluster_status()


@app.get("/api/runs")
def runs():
    rs = store().list_runs() or []
    return sorted(rs, key=lambda r: r.get("started_at", ""), reverse=True)


# ------------------------------------------------------------------------------------ UI
if config.UI_DIST.exists():
    app.mount("/assets", StaticFiles(directory=config.UI_DIST / "assets"), name="assets")

    @app.get("/{path:path}", include_in_schema=False)
    def spa(path: str):
        f = config.UI_DIST / path
        return FileResponse(f if path and f.is_file() else config.UI_DIST / "index.html")
