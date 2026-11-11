import { Bar as RBar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { Bar, Card, ChartTip, Kpi, Loading } from '../components/ui'
import { useApi } from '../lib/api'
import { fmt } from '../lib/util'

const EXPECTED = {
  master: ['NameNode', 'ResourceManager', 'HMaster', 'ZooKeeper', 'Thrift', 'Kafka broker 1'],
  worker1: ['DataNode', 'NodeManager', 'RegionServer', 'Kafka broker 2'],
  worker2: ['DataNode', 'NodeManager', 'RegionServer', 'Kafka broker 3'],
}

function Down({ s }) {
  return <div className="error" style={{ padding: 0 }}>unreachable: <span className="dim">{s.error}</span></div>
}

export default function Cluster() {
  const c = useApi('/api/cluster', { refresh: 5000 })
  const runs = useApi('/api/runs', { refresh: 30000 })

  const bench = (runs.data || []).filter((r) => r.job === 'job02_graph_analytics' && r.status === 'ok')
    .map((r) => ({ label: `${r.executors ?? '?'} exec · ${new Date(r.started_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`, s: r.duration_s })).reverse()

  return (
    <div className="page">
      <div className="page-head">
        <h2>Cluster</h2>
        <p>Live health of the 3-machine cluster, read from each daemon's own REST/JMX endpoints (NameNode, YARN ResourceManager, HBase Master, Kafka admin API).</p>
      </div>
      <Loading state={c}>
        {(d) => (
          <>
            {!d.available && <div className="notice">The cluster is not reachable from this API server, so the dashboard is running in <b>offline mode</b>. Start the cluster (<span className="mono">cluster/scripts/start_all.sh</span>) and run the API on the master node.</div>}
            <div className="grid g3">
              {['master', 'worker1', 'worker2'].map((h) => {
                const node = d.nodes.find((n) => n.host === h) || { services: [] }
                const extra = node.services.filter((s) => !EXPECTED[h].includes(s))
                return (
                  <Card key={h} title={<><span className={`dot ${node.services.length ? 'on' : 'off'}`} />{h}</>} sub={h === 'master' ? 'master node' : 'worker node'}>
                    <div className="chips">
                      {EXPECTED[h].map((s) => <span key={s} className={`svc ${node.services.includes(s) ? '' : 'missing'}`}>{s}</span>)}
                      {extra.map((s) => <span key={s} className="svc">{s}</span>)}
                    </div>
                  </Card>
                )
              })}
            </div>

            <div className="grid g2">
              <Card title="HDFS" sub={d.hdfs.ok ? `Hadoop ${d.hdfs.version || ''}` : ''}>
                {d.hdfs.ok ? (
                  <>
                    <div className="kpis">
                      <Kpi label="Live DataNodes" value={d.hdfs.live_datanodes} color="var(--good)" />
                      <Kpi label="Files" value={fmt.int(d.hdfs.files)} />
                      <Kpi label="Blocks" value={fmt.int(d.hdfs.blocks)} hint={`${d.hdfs.under_replicated} under-replicated`} />
                    </div>
                    <div style={{ margin: '12px 0 6px' }} className="muted">Capacity {fmt.bytes(d.hdfs.capacity_used)} / {fmt.bytes(d.hdfs.capacity_total)}</div>
                    <Bar value={d.hdfs.capacity_used} max={d.hdfs.capacity_total} />
                    <table style={{ marginTop: 10 }}><thead><tr><th>DataNode</th><th className="num">Blocks</th><th className="num">Used</th><th className="num">Heartbeat</th></tr></thead>
                      <tbody>{d.hdfs.datanodes.map((n) => <tr key={n.host}><td>{n.host}</td><td className="num">{fmt.int(n.blocks)}</td><td className="num">{fmt.bytes(n.used)}</td><td className="num">{n.last_contact_s}s</td></tr>)}</tbody></table>
                  </>
                ) : <Down s={d.hdfs} />}
              </Card>

              <Card title="YARN" sub="resource manager">
                {d.yarn.ok ? (
                  <>
                    <div className="kpis">
                      <Kpi label="NodeManagers" value={d.yarn.active_nodes} color="var(--good)" />
                      <Kpi label="Apps running" value={d.yarn.apps_running} />
                      <Kpi label="Containers" value={d.yarn.containers} />
                    </div>
                    <div style={{ margin: '12px 0 6px' }} className="muted">Memory {fmt.int(d.yarn.allocated_mb)} / {fmt.int(d.yarn.total_mb)} MB · vCores {d.yarn.allocated_vcores} / {d.yarn.total_vcores}</div>
                    <Bar value={d.yarn.allocated_mb} max={d.yarn.total_mb} />
                    <table style={{ marginTop: 10 }}><thead><tr><th>Application</th><th>State</th><th className="num">Elapsed</th></tr></thead>
                      <tbody>{d.yarn.apps.slice(0, 6).map((a) => <tr key={a.id}><td style={{ maxWidth: 260, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{a.name}</td><td><span className={`badge ${a.state === 'RUNNING' ? 'good' : a.final === 'FAILED' ? 'bad' : ''}`}>{a.state === 'FINISHED' ? a.final : a.state}</span></td><td className="num">{fmt.dur(a.elapsed_ms / 1000)}</td></tr>)}</tbody></table>
                  </>
                ) : <Down s={d.yarn} />}
              </Card>

              <Card title="HBase" sub="serving layer">
                {d.hbase.ok ? (
                  <div className="kpis">
                    <Kpi label="RegionServers" value={d.hbase.region_servers} hint={d.hbase.live.join(', ')} color="var(--good)" />
                    <Kpi label="Avg regions / server" value={fmt.num(d.hbase.avg_load, 1)} />
                    <Kpi label="Requests" value={fmt.compact(d.hbase.requests)} />
                  </div>
                ) : <Down s={d.hbase} />}
              </Card>

              <Card title="Kafka" sub="KRaft mode, no ZooKeeper">
                {d.kafka.ok ? (
                  <>
                    <div className="kpis">
                      <Kpi label="Brokers" value={d.kafka.brokers.length} hint={d.kafka.brokers.map((b) => b.host).join(', ')} color="var(--good)" />
                      <Kpi label="Controller" value={d.kafka.controller ?? '–'} />
                    </div>
                    <table style={{ marginTop: 10 }}><thead><tr><th>Topic</th><th className="num">Partitions</th><th className="num">Replicas</th></tr></thead>
                      <tbody>{d.kafka.topics.map((t) => <tr key={t.name}><td className="mono">{t.name}</td><td className="num">{t.partitions}</td><td className="num">{t.replicas}</td></tr>)}</tbody></table>
                  </>
                ) : <Down s={d.kafka} />}
              </Card>
            </div>

            <Card title="Web consoles">
              <div className="row">
                {Object.entries(d.links).map(([k, v]) => <a key={k} className="btn" href={v} target="_blank" rel="noreferrer">{k.replace('_', ' ')} ↗</a>)}
              </div>
            </Card>
          </>
        )}
      </Loading>

      <div className="grid g2">
        <Card title="Scaling benchmark" sub="job 02 duration vs executors (scripts/benchmark_scaling.sh)">
          {bench.length ? (
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={bench}>
                <CartesianGrid vertical={false} />
                <XAxis dataKey="label" />
                <YAxis />
                <Tooltip content={<ChartTip fmtValue={(v) => `${v}s`} />} />
                <RBar dataKey="s" name="duration" fill="#38bdf8" radius={[6, 6, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : <div className="empty">No benchmark runs recorded yet.</div>}
        </Card>
        <Card title="Job history" sub="recorded by every Spark job">
          <Loading state={runs} empty="No job runs recorded yet. Run scripts/run_pipeline.sh on the cluster.">
            {(rs) => (
              <div className="scroll" style={{ maxHeight: 260 }}><table>
                <thead><tr><th>Job</th><th>Status</th><th className="num">Executors</th><th className="num">Duration</th><th>Started</th></tr></thead>
                <tbody>{rs.map((r) => (
                  <tr key={r.started_at + r.job}><td className="mono">{r.job}</td><td><span className={`badge ${r.status === 'ok' ? 'good' : 'bad'}`}>{r.status}</span></td><td className="num">{r.executors ?? '–'}</td><td className="num">{fmt.dur(r.duration_s)}</td><td className="dim">{new Date(r.started_at).toLocaleString()}</td></tr>
                ))}</tbody>
              </table></div>
            )}
          </Loading>
        </Card>
      </div>
    </div>
  )
}
