"""Live health of the 3-node cluster, read from each daemon's own REST / JMX endpoints."""
import json
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from . import config

_cache = {"t": 0, "data": None}


def _get(url):
    r = requests.get(url, timeout=2.5)
    r.raise_for_status()
    return r.json()


def _hdfs():
    st = _get(f"{config.NAMENODE_URL}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState")["beans"][0]
    info = _get(f"{config.NAMENODE_URL}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo")["beans"][0]
    live = json.loads(info.get("LiveNodes") or "{}")
    return {
        "live_datanodes": st.get("NumLiveDataNodes"), "dead_datanodes": st.get("NumDeadDataNodes"),
        "capacity_total": st.get("CapacityTotal"), "capacity_used": st.get("CapacityUsed"),
        "files": st.get("FilesTotal"), "blocks": st.get("BlocksTotal"),
        "under_replicated": st.get("UnderReplicatedBlocks"), "version": info.get("SoftwareVersion"),
        "datanodes": [{"host": k.split(":")[0], "used": v.get("usedSpace"), "capacity": v.get("capacity"),
                       "blocks": v.get("numBlocks"), "last_contact_s": v.get("lastContact")}
                      for k, v in live.items()],
    }


def _yarn():
    cm = _get(f"{config.YARN_URL}/ws/v1/cluster/metrics")["clusterMetrics"]
    nodes = (_get(f"{config.YARN_URL}/ws/v1/cluster/nodes").get("nodes") or {}).get("node", [])
    apps = (_get(f"{config.YARN_URL}/ws/v1/cluster/apps?limit=50").get("apps") or {}).get("app", [])
    apps = sorted(apps, key=lambda a: a.get("startedTime", 0), reverse=True)[:12]
    return {
        "active_nodes": cm.get("activeNodes"), "apps_running": cm.get("appsRunning"),
        "apps_completed": cm.get("appsCompleted"), "total_mb": cm.get("totalMB"),
        "allocated_mb": cm.get("allocatedMB"), "total_vcores": cm.get("totalVirtualCores"),
        "allocated_vcores": cm.get("allocatedVirtualCores"), "containers": cm.get("containersAllocated"),
        "nodes": [{"host": n.get("nodeHostName"), "state": n.get("state"), "containers": n.get("numContainers"),
                   "used_mb": n.get("usedMemoryMB"), "avail_mb": n.get("availMemoryMB"),
                   "used_vcores": n.get("usedVirtualCores")} for n in nodes],
        "apps": [{"id": a.get("id"), "name": a.get("name"), "state": a.get("state"),
                  "final": a.get("finalStatus"), "elapsed_ms": a.get("elapsedTime"),
                  "allocated_mb": a.get("allocatedMB"), "progress": a.get("progress")} for a in apps],
    }


def _hbase():
    b = _get(f"{config.HBASE_UI_URL}/jmx?qry=Hadoop:service=HBase,name=Master,sub=Server")["beans"][0]
    live = [s.split(",")[0] for s in (b.get("tag.liveRegionServers") or "").split(";") if s]
    return {"region_servers": b.get("numRegionServers"), "dead_region_servers": b.get("numDeadRegionServers"),
            "live": live, "avg_load": b.get("averageLoad"), "requests": b.get("clusterRequests")}


def _kafka():
    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(bootstrap_servers=config.KAFKA.split(","), request_timeout_ms=3000)
    # kafka-python 3.x renamed the fields it returns (2.x name -> 3.x name):
    #   broker node_id -> broker_id, topic -> name, partition replicas -> replica_nodes
    try:
        try:
            cl = admin.describe_cluster()
            brokers = [{"id": b.get("broker_id", b.get("node_id")), "host": b["host"]} for b in cl.get("brokers", [])]
            controller = cl.get("controller_id")
        except AttributeError:  # older kafka-python
            brokers = [{"id": b.nodeId, "host": b.host} for b in admin._client.cluster.brokers()]
            controller = None
        topics = admin.describe_topics(["gp-events", "gp-insights"])

        def replicas(t):
            p = t["partitions"][0] if t["partitions"] else {}
            return len(p.get("replica_nodes", p.get("replicas")) or [])

        return {"brokers": sorted(brokers, key=lambda b: b["id"]), "controller": controller,
                "topics": [{"name": t.get("name", t.get("topic")), "partitions": len(t["partitions"]),
                            "replicas": replicas(t)} for t in topics]}
    finally:
        admin.close()


def _safe(fn):
    try:
        return {"ok": True, **fn()}
    except Exception as exc:
        return {"ok": False, "error": f"{exc.__class__.__name__}: {str(exc)[:160]}"}


def cluster_status():
    if _cache["data"] and time.time() - _cache["t"] < 5:
        return _cache["data"]
    with ThreadPoolExecutor(4) as ex:
        hdfs, yarn, hbase, kafka = ex.map(_safe, [_hdfs, _yarn, _hbase, _kafka])

    # role map per physical machine
    hosts = {"master": set(), "worker1": set(), "worker2": set()}
    if hdfs["ok"]:
        hosts["master"].add("NameNode")
        for d in hdfs["datanodes"]:
            hosts.setdefault(d["host"], set()).add("DataNode")
    if yarn["ok"]:
        hosts["master"].add("ResourceManager")
        for n in yarn["nodes"]:
            if n["state"] == "RUNNING":
                hosts.setdefault(n["host"], set()).add("NodeManager")
    if hbase["ok"]:
        hosts["master"].update({"HMaster", "ZooKeeper", "Thrift"})
        for h in hbase["live"]:
            hosts.setdefault(h, set()).add("RegionServer")
    if kafka["ok"]:
        for b in kafka["brokers"]:
            hosts.setdefault(b["host"], set()).add(f"Kafka broker {b['id']}")
    data = {"available": any(x["ok"] for x in (hdfs, yarn, hbase, kafka)),
            "hdfs": hdfs, "yarn": yarn, "hbase": hbase, "kafka": kafka,
            "nodes": [{"host": h, "services": sorted(s)} for h, s in hosts.items()],
            "links": {"namenode": config.NAMENODE_URL, "yarn": config.YARN_URL,
                      "hbase": config.HBASE_UI_URL, "spark_history": config.SPARK_HISTORY_URL},
            "checked_at": time.time()}
    _cache.update(t=time.time(), data=data)
    return data
