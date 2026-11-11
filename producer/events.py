"""Realistic social-event generator shared by the CLI producer and the API's demo controls.

Sampling model
  * baseline : sources are drawn from existing edge sources (active users); targets are drawn from
               existing edge targets, which is degree-proportional = preferential attachment
               ("the rich get richer", like real follow graphs).
  * viral    : a burst of follows towards one target (a post goes viral)  -> "rising star" in the stream
  * bot      : one account follows many random users very fast            -> follow-spam alert
"""
import json
import random
import time
import uuid


class EventGenerator:
    def __init__(self, edges, seed=None):
        if not edges:
            raise ValueError("EventGenerator needs at least one sample edge")
        self.rng = random.Random(seed)
        self.edges = [(int(a), int(b)) for a, b in edges]
        self.srcs = [a for a, _ in self.edges]
        self.dsts = [b for _, b in self.edges]
        self.all_ids = list(set(self.srcs) | set(self.dsts))

    @classmethod
    def from_edge_file(cls, path, limit=200_000, seed=None):
        """Reservoir-sample `limit` edges from a SNAP edge list."""
        rng = random.Random(seed)
        sample = []
        with open(path) as f:
            for i, line in enumerate(f):
                if not line.strip() or line.startswith("#"):
                    continue
                a, b = line.split()[:2]
                if len(sample) < limit:
                    sample.append((a, b))
                else:
                    j = rng.randint(0, i)
                    if j < limit:
                        sample[j] = (a, b)
        return cls(sample, seed)

    @staticmethod
    def _event(etype, src, dst, scenario):
        return {"event_id": uuid.uuid4().hex[:16], "type": etype, "src": src, "dst": dst,
                "ts": time.time() * 1000.0, "scenario": scenario}

    def baseline(self):
        r = self.rng.random()
        if r < 0.05:                                    # unfollow an existing relationship
            a, b = self.rng.choice(self.edges)
            return self._event("unfollow", a, b, "baseline")
        src, dst = self.rng.choice(self.srcs), self.rng.choice(self.dsts)
        while dst == src:
            dst = self.rng.choice(self.dsts)
        etype = "follow" if r < 0.75 else ("like" if r < 0.93 else "mention")
        return self._event(etype, src, dst, "baseline")

    def viral(self, target):
        src = self.rng.choice(self.all_ids)
        while src == target:
            src = self.rng.choice(self.all_ids)
        return self._event("follow", src, int(target), "viral")

    def bot(self, bot_id):
        dst = self.rng.choice(self.all_ids)
        while dst == bot_id:
            dst = self.rng.choice(self.all_ids)
        return self._event("follow", int(bot_id), dst, "bot")

    def pick_viral_target(self):
        """A mid-sized account (not already a celebrity) makes a more convincing 'rising star'."""
        return self.rng.choice(self.srcs)

    def new_bot_id(self):
        return 9_000_000_000 + self.rng.randint(0, 999_999)


def encode(event):
    return json.dumps(event, separators=(",", ":")).encode()
