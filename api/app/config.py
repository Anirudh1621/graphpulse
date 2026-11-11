import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Data backend: "hbase" (cluster), "snapshot" (JSON files), "auto" (try HBase, fall back to snapshot)
BACKEND = os.environ.get("GP_BACKEND", "auto")
SNAPSHOT_DIR = Path(os.environ.get("GP_SNAPSHOT_DIR", ROOT / "demo-snapshot"))

HBASE_HOST = os.environ.get("GP_HBASE_HOST", "master")
HBASE_PORT = int(os.environ.get("GP_HBASE_PORT", "9090"))

# Live stream: "kafka" (cluster), "simulate" (in-process engine), "auto"
LIVE_MODE = os.environ.get("GP_LIVE", "auto")
KAFKA = os.environ.get("GP_KAFKA", "master:9092,worker1:9092,worker2:9092")

# Cluster web endpoints (for the Cluster monitor page)
NAMENODE_URL = os.environ.get("GP_NAMENODE_URL", "http://master:9870")
YARN_URL = os.environ.get("GP_YARN_URL", "http://master:8088")
HBASE_UI_URL = os.environ.get("GP_HBASE_UI_URL", "http://master:16010")
SPARK_HISTORY_URL = os.environ.get("GP_SPARK_HISTORY_URL", "http://master:18080")

UI_DIST = ROOT / "ui" / "dist"
