"""Real-time channel: Kafka -> WebSocket fan-out, plus demo controls (inject viral / bot scenarios).

Mode "kafka"    : consumes Spark's results from 'gp-insights' (+ a sample of raw 'gp-events') and
                  produces scenario events into 'gp-events'. All analytics happen in Spark Streaming.
Mode "simulate" : no cluster available - an in-process engine generates events and computes the same
                  window aggregations (throughput / rising stars / burst alerts) so the UI still works.
"""
import asyncio
import json
import os
import sys
import threading
import time
from collections import Counter, defaultdict, deque

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "producer"))
from events import EventGenerator, encode  # noqa: E402


class LiveHub:
    def __init__(self):
        self.clients = set()
        self.loop = None
        self.queue = None
        self.recent = deque(maxlen=60)
        self.mode = "off"
        self.stats = Counter()

    async def start(self):
        self.loop = asyncio.get_running_loop()
        self.queue = asyncio.Queue(maxsize=5000)
        asyncio.create_task(self._broadcast())

    def publish(self, msg):
        """Thread-safe: may be called from Kafka / simulator threads."""
        if self.loop is None:
            return
        self.stats[msg.get("kind", "?")] += 1

        def _put():
            if not self.queue.full():
                self.queue.put_nowait(msg)
        self.loop.call_soon_threadsafe(_put)

    async def _broadcast(self):
        while True:
            msg = await self.queue.get()
            if msg.get("kind") in ("rising", "alert", "scenario"):
                self.recent.append(msg)
            data = json.dumps(msg, default=str)
            dead = []
            for ws in list(self.clients):
                try:
                    await ws.send_text(data)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                self.clients.discard(ws)


class ScenarioMixin:
    """Shared demo controls: baseline traffic on/off, viral surge, bot attack."""

    def _init_scenarios(self, gen):
        self.gen = gen
        self.baseline_rate = 25.0
        self.baseline_on = True
        self.active = []  # [(kind, target, until, rate)]
        self.lock = threading.Lock()

    def control(self, action, target=None, duration=45, rate=None):
        with self.lock:
            if action == "baseline_on":
                self.baseline_on = True
                self.baseline_rate = float(rate or self.baseline_rate)
                return {"baseline": True, "rate": self.baseline_rate}
            if action == "baseline_off":
                self.baseline_on = False
                return {"baseline": False}
            if action == "viral":
                target = int(target) if target else self.gen.pick_viral_target()
                self.active.append(("viral", target, time.time() + duration, float(rate or 6)))
            elif action == "bot":
                target = int(target) if target else self.gen.new_bot_id()
                self.active.append(("bot", target, time.time() + duration, float(rate or 4)))
            else:
                raise ValueError(f"unknown action {action}")
        msg = {"kind": "scenario", "scenario": action, "target": target, "duration": duration, "ts": time.time()}
        self.hub.publish(msg)
        return msg

    def _tick_events(self, dt):
        """Events to emit during the next `dt` seconds."""
        now, evs = time.time(), []
        with self.lock:
            self.active = [a for a in self.active if a[2] > now]
            active = list(self.active)
            base = self.baseline_rate if self.baseline_on else 0
        for _ in range(int(base * dt + 0.5)):
            evs.append(self.gen.baseline())
        for kind, target, _, rate in active:
            for _ in range(max(1, int(rate * dt + 0.5))):
                evs.append(self.gen.viral(target) if kind == "viral" else self.gen.bot(target))
        return evs

    def status(self):
        with self.lock:
            return {"mode": self.mode, "baseline": self.baseline_on, "baseline_rate": self.baseline_rate,
                    "active": [{"scenario": k, "target": t, "ends_in": round(u - time.time())}
                               for k, t, u, _ in self.active]}


# --------------------------------------------------------------------------- Kafka mode
class KafkaBridge(ScenarioMixin):
    mode = "kafka"

    def __init__(self, hub, store, bootstrap, gen):
        from kafka import KafkaConsumer, KafkaProducer

        self.hub, self.store = hub, store
        self.bootstrap = bootstrap.split(",")
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap, acks=1, linger_ms=10,
                                      key_serializer=lambda k: str(k).encode(), value_serializer=encode,
                                      request_timeout_ms=5000, max_block_ms=5000)
        # unique group per API process: every API instance sees the full stream (broadcast semantics)
        self.consumer = KafkaConsumer("gp-insights", "gp-events", bootstrap_servers=self.bootstrap,
                                      auto_offset_reset="latest", enable_auto_commit=False,
                                      group_id=f"graphpulse-api-{os.getpid()}-{int(time.time())}",
                                      consumer_timeout_ms=1000)
        self._init_scenarios(gen)
        self.baseline_on = False   # in cluster mode the CLI producer usually drives baseline traffic

    def start(self):
        threading.Thread(target=self._consume, daemon=True, name="kafka-consumer").start()
        threading.Thread(target=self._produce, daemon=True, name="kafka-producer").start()

    def _produce(self):
        while True:
            t = time.time()
            for ev in self._tick_events(0.5):
                self.producer.send("gp-events", key=ev["src"], value=ev)
            self.producer.flush()
            time.sleep(max(0, 0.5 - (time.time() - t)))

    def _consume(self):
        sec, n_in_sec, forwarded = int(time.time()), 0, 0
        while True:
            for rec in self.consumer:
                now = int(time.time())
                if now != sec:
                    self.hub.publish({"kind": "ingest_rate", "ts": sec, "events": n_in_sec})
                    sec, n_in_sec, forwarded = now, 0, 0
                try:
                    msg = json.loads(rec.value)
                except ValueError:
                    continue
                if rec.topic == "gp-events":
                    n_in_sec += 1
                    if forwarded < 6 or msg.get("scenario") != "baseline" and forwarded < 15:
                        forwarded += 1
                        self.hub.publish({"kind": "event", **msg})
                else:
                    if msg.get("kind") == "rising" and isinstance(msg.get("items"), str):
                        msg["items"] = json.loads(msg["items"])
                    self.hub.publish(msg)


# --------------------------------------------------------------------------- offline simulation
class LocalStreamEngine(ScenarioMixin):
    """Mimics the Spark Structured Streaming queries in-process (same message formats)."""
    mode = "simulate"

    def __init__(self, hub, store, gen, burst_threshold=25):
        self.hub, self.store = hub, store
        self.threshold = burst_threshold
        self.follows = deque()           # (ts, src, dst) within the last 2 minutes
        self.window_counts = defaultdict(Counter)
        self.alerted = set()
        self._init_scenarios(gen)

    def start(self):
        threading.Thread(target=self._run, daemon=True, name="local-stream").start()

    def _run(self):
        last_rise, last_window = 0.0, int(time.time() // 5) * 5
        while True:
            t = time.time()
            evs = self._tick_events(0.5)
            forwarded = 0
            for ev in evs:
                ts = ev["ts"] / 1000
                self.window_counts[int(ts // 5) * 5][ev["type"]] += 1
                if ev["type"] in ("follow", "unfollow"):
                    d = 1 if ev["type"] == "follow" else -1
                    self.store.inc_counter(ev["dst"], "followers_delta", d)
                    self.store.inc_counter(ev["src"], "following_delta", d)
                if ev["type"] == "follow":
                    self.follows.append((ts, ev["src"], ev["dst"]))
                if forwarded < 3 or (ev["scenario"] != "baseline" and forwarded < 8):
                    forwarded += 1
                    self.hub.publish({"kind": "event", **ev})
            self.hub.publish({"kind": "ingest_rate", "ts": int(t), "events": len(evs) * 2})
            while self.follows and self.follows[0][0] < t - 120:
                self.follows.popleft()

            # throughput: emit every closed 5 s window (like Q2)
            for w in sorted(k for k in self.window_counts if k + 5 <= t):
                for etype, c in self.window_counts.pop(w).items():
                    self.hub.publish({"kind": "throughput", "window_start": w, "type": etype, "count": c})
                last_window = w

            if t - last_rise >= 5:      # rising stars (like Q3)
                last_rise = t
                cnt = Counter(dst for _, _, dst in self.follows)
                items = [{"user": u, "new_followers": c} for u, c in cnt.most_common(10)]
                if items:
                    self.hub.publish({"kind": "rising", "items": items, "ts": t})
                    self.store.set_meta("live_rising", {"ts": t, "items": items})

                # follow bursts in the last 30 s (like Q4)
                recent = [(s, d) for ts, s, d in self.follows if ts >= t - 30]
                per_src = Counter(s for s, _ in recent)
                for src, c in per_src.items():
                    if c < self.threshold:
                        continue
                    wstart = int(t // 10) * 10 - 20
                    if (src, wstart) in self.alerted:
                        continue
                    self.alerted.add((src, wstart))
                    prof = (self.store.get_users([src], ["suspicion", "influence", "community"]) or {}).get(src, {})
                    alert = {"kind": "alert", "window_start": wstart, "src": src, "follows": c,
                             "distinct_targets": len({d for s, d in recent if s == src}),
                             "batch_suspicion": prof.get("suspicion"), "batch_influence": prof.get("influence"),
                             "community": prof.get("community"),
                             "severity": "critical" if c >= 2 * self.threshold or (prof.get("suspicion") or 0) >= 50
                             else "high", "detected_at": t}
                    self.store.add_alert(alert)
                    self.hub.publish(alert)
            time.sleep(max(0, 0.5 - (time.time() - t)))


def start_live(hub, store, live_mode, kafka_bootstrap, log=print):
    sample = store.get_meta("sample_edges") or []
    if not sample:
        log("live: no sample_edges in serving layer - live features disabled")
        return None
    gen = EventGenerator(sample)
    if live_mode in ("kafka", "auto"):
        try:
            bridge = KafkaBridge(hub, store, kafka_bootstrap, gen)
            bridge.start()
            hub.mode = "kafka"
            log(f"live: Kafka bridge connected to {kafka_bootstrap}")
            return bridge
        except Exception as exc:
            if live_mode == "kafka":
                raise
            log(f"live: Kafka unavailable ({exc.__class__.__name__}) -> local simulation")
    engine = LocalStreamEngine(hub, store, gen)
    engine.start()
    hub.mode = "simulate"
    return engine
