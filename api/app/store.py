"""Serving-layer access. HBaseStore reads the tables written by the Spark jobs;
SnapshotStore serves the same documents from JSON files (offline mode / backup)."""
import bisect
import json
import threading
import time
from collections import defaultdict, deque
from pathlib import Path

USER_TABLE, META_TABLE, COMM_TABLE, ALERT_TABLE = "gp_users", "gp_meta", "gp_communities", "gp_alerts"


def _decode(v: bytes):
    s = v.decode()
    try:
        return json.loads(s)
    except ValueError:
        return s


def _counter(v: bytes) -> int:
    return int.from_bytes(v, "big", signed=True) if len(v) == 8 else int(_decode(v) or 0)


class Store:
    kind = "base"

    def get_meta(self, name): ...
    def get_user(self, uid): ...
    def get_users(self, ids, fields=None): ...
    def get_adjacency(self, ids): ...
    def get_community(self, cid): ...
    def search(self, prefix, limit=10): ...
    def list_runs(self): ...
    def list_alerts(self, limit=50): ...
    # used only by the in-process simulator
    def add_alert(self, alert): ...
    def inc_counter(self, uid, col, delta): ...
    def set_meta(self, name, doc): ...


# --------------------------------------------------------------------------- HBase
class HBaseStore(Store):
    kind = "hbase"

    def __init__(self, host, port):
        import happybase

        self.pool = happybase.ConnectionPool(size=8, host=host, port=port, timeout=20000)
        self._cache, self._lock = {}, threading.Lock()
        with self.pool.connection() as c:  # fail fast if HBase/Thrift is down
            tables = {t.decode() for t in c.tables()}
        missing = {USER_TABLE, META_TABLE} - tables
        if missing:
            raise RuntimeError(f"HBase tables missing: {missing}")

    @staticmethod
    def _user_from_row(uid, row):
        doc = {"id": int(uid), "stream": {}}
        for k, v in row.items():
            cf, col = k.decode().split(":", 1)
            if cf == "m":
                doc[col] = _decode(v)
            elif cf == "n":
                doc[col] = _decode(v)
            elif cf == "r" and col == "recs":
                doc["recs"] = _decode(v)
            elif cf == "s":
                doc["stream"][col] = _counter(v)
        return doc

    def get_meta(self, name, ttl=30):
        now = time.time()
        with self._lock:
            hit = self._cache.get(name)
            if hit and now - hit[0] < ttl:
                return hit[1]
        with self.pool.connection() as c:
            row = c.table(META_TABLE).row(name.encode(), columns=[b"d:json"])
        doc = _decode(row[b"d:json"]) if row else None
        with self._lock:
            self._cache[name] = (now, doc)
        return doc

    def get_user(self, uid):
        with self.pool.connection() as c:
            row = c.table(USER_TABLE).row(str(uid).encode())
        return self._user_from_row(uid, row) if row else None

    def get_users(self, ids, fields=None):
        cols = [f"m:{f}".encode() for f in fields] if fields else [b"m"]
        out = {}
        with self.pool.connection() as c:
            t = c.table(USER_TABLE)
            for i in range(0, len(ids), 500):
                for key, row in t.rows([str(x).encode() for x in ids[i:i + 500]], columns=cols):
                    out[int(key)] = self._user_from_row(key, row)
        return out

    def get_adjacency(self, ids):
        out = {}
        with self.pool.connection() as c:
            t = c.table(USER_TABLE)
            for i in range(0, len(ids), 500):
                for key, row in t.rows([str(x).encode() for x in ids[i:i + 500]], columns=[b"n:out", b"n:in"]):
                    out[int(key)] = (_decode(row.get(b"n:out", b"[]")), _decode(row.get(b"n:in", b"[]")))
        return out

    def get_community(self, cid):
        with self.pool.connection() as c:
            row = c.table(COMM_TABLE).row(str(cid).encode())
        return _decode(row[b"c:json"]) if row else None

    def search(self, prefix, limit=10):
        with self.pool.connection() as c:
            t = c.table(USER_TABLE)
            return [int(k) for k, _ in t.scan(row_prefix=prefix.encode(), columns=[b"m:influence"], limit=limit)]

    def list_runs(self):
        with self.pool.connection() as c:
            return [_decode(r[b"d:json"]) for _, r in c.table(META_TABLE).scan(row_prefix=b"run:", columns=[b"d:json"])]

    def list_alerts(self, limit=50):
        with self.pool.connection() as c:
            return [_decode(r[b"a:json"]) for _, r in c.table(ALERT_TABLE).scan(limit=limit)]

    def add_alert(self, alert):
        key = f"{9999999999 - int(alert['window_start']):010d}:{alert['src']}"
        with self.pool.connection() as c:
            c.table(ALERT_TABLE).put(key.encode(), {b"a:json": json.dumps(alert).encode()})

    def inc_counter(self, uid, col, delta):
        with self.pool.connection() as c:
            c.table(USER_TABLE).counter_inc(str(uid).encode(), f"s:{col}".encode(), int(delta))

    def set_meta(self, name, doc):
        with self.pool.connection() as c:
            c.table(META_TABLE).put(name.encode(), {b"d:json": json.dumps(doc).encode()})
        with self._lock:
            self._cache.pop(name, None)


# --------------------------------------------------------------------------- snapshot
class SnapshotStore(Store):
    kind = "snapshot"

    def __init__(self, directory: Path):
        directory = Path(directory)
        if not (directory / "users.jsonl").exists():
            raise FileNotFoundError(f"no snapshot at {directory} (run tools/make_demo_snapshot.py)")
        self.meta = {p.stem: json.loads(p.read_text()) for p in (directory / "meta").glob("*.json")}
        self.communities = {int(c["community"]): c for c in json.loads((directory / "communities.json").read_text())}
        self.users = {}
        with open(directory / "users.jsonl") as f:
            for line in f:
                d = json.loads(line)
                self.users[int(d["id"])] = d
        self.sorted_keys = sorted(str(k) for k in self.users)
        self.counters = defaultdict(lambda: defaultdict(int))
        self.alerts = deque(maxlen=500)
        self.lock = threading.Lock()

    def _with_stream(self, d):
        d = dict(d)
        d["stream"] = dict(self.counters.get(d["id"], {}))
        return d

    def get_meta(self, name):
        return self.meta.get(name)

    def get_user(self, uid):
        d = self.users.get(int(uid))
        return self._with_stream(d) if d else None

    def get_users(self, ids, fields=None):
        out = {}
        for i in ids:
            d = self.users.get(int(i))
            if d:
                out[int(i)] = {k: d.get(k) for k in (fields or d) if k not in ("out", "in", "recs")} | {"id": int(i)}
        return out

    def get_adjacency(self, ids):
        return {int(i): (self.users[int(i)].get("out", []), self.users[int(i)].get("in", []))
                for i in ids if int(i) in self.users}

    def get_community(self, cid):
        return self.communities.get(int(cid))

    def search(self, prefix, limit=10):
        i = bisect.bisect_left(self.sorted_keys, prefix)
        out = []
        while i < len(self.sorted_keys) and self.sorted_keys[i].startswith(prefix) and len(out) < limit:
            out.append(int(self.sorted_keys[i]))
            i += 1
        return out

    def list_runs(self):
        return self.meta.get("runs", [])

    def list_alerts(self, limit=50):
        with self.lock:
            return list(self.alerts)[:limit]

    def add_alert(self, alert):
        with self.lock:
            self.alerts = deque([a for a in self.alerts
                                 if not (a["src"] == alert["src"] and a["window_start"] == alert["window_start"])],
                                maxlen=500)
            self.alerts.appendleft(alert)

    def inc_counter(self, uid, col, delta):
        with self.lock:
            self.counters[int(uid)][col] += int(delta)

    def set_meta(self, name, doc):
        self.meta[name] = doc


def open_store(backend, snapshot_dir, hbase_host, hbase_port, log=print):
    if backend in ("hbase", "auto"):
        try:
            s = HBaseStore(hbase_host, hbase_port)
            log(f"store: HBase via Thrift {hbase_host}:{hbase_port}")
            return s
        except Exception as exc:
            if backend == "hbase":
                raise
            log(f"store: HBase unavailable ({exc.__class__.__name__}: {exc}) -> snapshot")
    s = SnapshotStore(snapshot_dir)
    log(f"store: snapshot {snapshot_dir} ({len(s.users):,} users)")
    return s
