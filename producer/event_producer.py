"""
GraphPulse event producer -> Kafka topic 'gp-events'.

Examples (run on master):
  python3 producer/event_producer.py --edges data/twitter_combined.txt --rate 40
  python3 producer/event_producer.py --edges data/twitter_combined.txt --scenario viral --duration 60
  python3 producer/event_producer.py --edges data/twitter_combined.txt --scenario bot --duration 30

Messages are keyed by source user id, so all events of one user land in the same partition (ordering per user).
"""
import argparse
import os
import sys
import time

from kafka import KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from events import EventGenerator, encode  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.environ.get("GP_KAFKA", "master:9092"))
    ap.add_argument("--topic", default="gp-events")
    ap.add_argument("--edges", required=True, help="SNAP edge list used to sample realistic users")
    ap.add_argument("--rate", type=float, default=40, help="baseline events per second")
    ap.add_argument("--scenario", choices=["baseline", "viral", "bot"], default="baseline")
    ap.add_argument("--target", type=int, help="viral target / bot id (random if omitted)")
    ap.add_argument("--burst-rate", type=float, default=15, help="scenario events per second")
    ap.add_argument("--duration", type=float, default=0, help="seconds (0 = forever)")
    args = ap.parse_args()

    gen = EventGenerator.from_edge_file(args.edges)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap.split(","), acks=1, linger_ms=20,
                             key_serializer=lambda k: str(k).encode(), value_serializer=encode)
    target = args.target or (gen.pick_viral_target() if args.scenario == "viral" else gen.new_bot_id())
    if args.scenario != "baseline":
        print(f"scenario={args.scenario} target={target}")

    start, sent, last_report = time.time(), 0, time.time()
    rate = args.rate if args.scenario == "baseline" else args.burst_rate
    try:
        while not args.duration or time.time() - start < args.duration:
            tick = time.time()
            for _ in range(max(1, int(rate))):
                if args.scenario == "viral":
                    ev = gen.viral(target)
                elif args.scenario == "bot":
                    ev = gen.bot(target)
                else:
                    ev = gen.baseline()
                producer.send(args.topic, key=ev["src"], value=ev)
                sent += 1
            producer.flush()
            if time.time() - last_report >= 5:
                print(f"{sent:,} events sent ({sent / (time.time() - start):.1f}/s)", flush=True)
                last_report = time.time()
            time.sleep(max(0.0, 1.0 - (time.time() - tick)))
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        print(f"done: {sent:,} events")


if __name__ == "__main__":
    main()
