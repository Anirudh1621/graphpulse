import { Bar as RBar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis, Cell } from 'recharts'
import { useNavigate } from 'react-router-dom'
import { Bar, Card, ChartTip, Kpi, Loading, UserChip } from '../components/ui'
import { useApi } from '../lib/api'
import { commColor, fmt } from '../lib/util'

function Architecture() {
  const s = (b, t) => <div className="step"><b>{b}</b><span>{t}</span></div>
  const a = <span className="arrow">→</span>
  return (
    <Card title="Lambda architecture" sub="batch + speed layers merged in the serving layer">
      <div className="arch" style={{ marginBottom: 10 }}>
        <span className="badge accent">Batch</span>
        {s('HDFS', 'raw + curated Parquet')}{a}{s('Spark on YARN', 'ETL, 2 workers')}{a}
        {s('GraphFrames', 'PageRank · LPA · triangles · motifs')}{a}{s('MLlib', 'KMeans anomaly')}{a}{s('HBase', 'pre-split serving tables')}
      </div>
      <div className="arch">
        <span className="badge good">Speed</span>
        {s('Kafka ×3 brokers', 'gp-events, RF=2')}{a}{s('Structured Streaming', 'watermarked windows')}{a}
        {s('Insights', 'rising · threats · counters')}{a}{s('HBase + Kafka', 'gp-insights')}{a}{s('FastAPI → WebSocket', 'this dashboard')}
      </div>
    </Card>
  )
}

export default function Overview() {
  const summary = useApi('/api/summary')
  const dd = useApi('/api/degree-distribution')
  const lb = useApi('/api/leaderboard?limit=10')
  const comm = useApi('/api/communities')
  const nav = useNavigate()

  return (
    <div className="page">
      <div className="page-head">
        <h2>Network Overview</h2>
        <p>{summary.data ? <>{summary.data.dataset} · computed by <b>{summary.data.engine}</b></> : 'Batch-layer results'}</p>
      </div>

      <Loading state={summary} empty="Batch views not published yet">
        {(s) => (
          <>
            <div className="kpis">
              <Kpi label="Users" value={fmt.compact(s.users)} hint={`${fmt.int(s.users)} vertices`} />
              <Kpi label="Follow edges" value={fmt.compact(s.edges)} hint={`avg out-degree ${fmt.num(s.avg_out_degree, 1)}`} />
              <Kpi label="Communities" value={fmt.int(s.communities_3plus)} hint={`label propagation · largest ${fmt.int(s.largest_community)}`} />
              <Kpi label="Modularity Q" value={fmt.num(s.modularity, 3)} hint="> 0.3 = strong community structure" color="var(--accent2)" />
              <Kpi label="Avg clustering" value={fmt.num(s.avg_clustering, 3)} hint={`${fmt.compact(s.triangles)} triangles`} />
              <Kpi label="Reciprocity" value={fmt.pct(s.reciprocity)} hint="follows that are mutual" />
              <Kpi label="Small world" value={`${fmt.num(s.avg_hops_to_top_hub, 2)} hops`} hint={`to #1 hub · ${fmt.num(s.reach_top_hub_pct, 1)}% can reach it`} color="var(--good)" />
              <Kpi label="Suspicious accounts" value={fmt.int(s.suspicious_accounts)} hint="suspicion score ≥ 50" color="var(--bad)" />
            </div>
            <Architecture />
          </>
        )}
      </Loading>

      <div className="grid g2">
        <Card title="Degree distribution" sub="log–log · heavy tail = scale-free network">
          <Loading state={dd}>
            {(d) => (
              <ResponsiveContainer width="100%" height={280}>
                <ScatterChart margin={{ top: 8, right: 12, bottom: 8, left: 0 }}>
                  <CartesianGrid />
                  <XAxis dataKey="degree" type="number" scale="log" domain={[1, 'auto']} allowDataOverflow name="degree" tickFormatter={fmt.compact} />
                  <YAxis dataKey="count" type="number" scale="log" domain={[1, 'auto']} allowDataOverflow name="users" tickFormatter={fmt.compact} />
                  <Tooltip content={<ChartTip />} />
                  <Legend />
                  <Scatter name="in-degree (followers)" data={d.raw.filter((r) => r.in_count).map((r) => ({ degree: r.degree, count: r.in_count }))} fill="#38bdf8" shape="circle" r={2} />
                  <Scatter name="out-degree (following)" data={d.raw.filter((r) => r.out_count).map((r) => ({ degree: r.degree, count: r.out_count }))} fill="#a78bfa" shape="circle" />
                </ScatterChart>
              </ResponsiveContainer>
            )}
          </Loading>
        </Card>

        <Card title="Largest communities" sub="GraphFrames label propagation">
          <Loading state={comm}>
            {(c) => (
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={c.communities.slice(0, 16)} margin={{ top: 8, right: 12, bottom: 8, left: 0 }}>
                  <CartesianGrid vertical={false} />
                  <XAxis dataKey="size_rank" tickFormatter={(r) => `C${r}`} />
                  <YAxis tickFormatter={fmt.compact} />
                  <Tooltip content={<ChartTip fmtLabel={(r) => `Community C${r}`} fmtValue={fmt.int} />} />
                  <RBar dataKey="size" name="members" radius={[6, 6, 0, 0]} onClick={() => nav('/communities')}>
                    {c.communities.slice(0, 16).map((x) => <Cell key={x.community} fill={commColor(x.community)} />)}
                  </RBar>
                </BarChart>
              </ResponsiveContainer>
            )}
          </Loading>
        </Card>
      </div>

      <div className="grid g-side">
        <Card title="Most influential users" sub="Influence = 65% PageRank percentile + 35% follower percentile">
          <Loading state={lb}>
            {(rows) => (
              <div className="scroll">
                <table>
                  <thead><tr><th>#</th><th>User</th><th>Influence</th><th className="num">PageRank</th><th className="num">Followers</th><th className="num">Clustering</th></tr></thead>
                  <tbody>
                    {rows.map((r) => (
                      <tr key={r.id} className="click" onClick={() => nav(`/user/${r.id}`)}>
                        <td className="dim">{r.influence_rank}</td>
                        <td><UserChip id={r.id} community={r.community} link={false} /></td>
                        <td style={{ minWidth: 130 }}><div className="row" style={{ flexWrap: 'nowrap' }}><Bar value={r.influence} /><b>{fmt.num(r.influence, 1)}</b></div></td>
                        <td className="num mono">{fmt.sci(r.pagerank)}</td>
                        <td className="num">{fmt.int(r.inDegree)}</td>
                        <td className="num">{fmt.num(r.clustering, 3)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Loading>
        </Card>

        <div className="grid">
          <Loading state={summary}>
            {(s) => s.recs_eval ? (
              <Card title="Recommender evaluation" sub={`offline, ${fmt.int(s.recs_eval.eval_users)} users`}>
                <div className="muted" style={{ fontSize: 12.5, marginBottom: 10 }}>
                  5% of follow edges hidden, then predicted. Adamic-Adar over GraphFrames motifs vs. "recommend the most popular".
                </div>
                {[['Adamic-Adar (ours)', s.recs_eval.adamic_adar, 'var(--accent)'], ['Popularity baseline', s.recs_eval.popularity_baseline, 'var(--dim)']].map(([n, m, c]) => (
                  <div key={n} style={{ marginBottom: 10 }}>
                    <div className="row" style={{ justifyContent: 'space-between' }}><span style={{ color: c }}>{n}</span><span className="mono">P@{s.recs_eval.k} {fmt.pct(m.precision_at_k, 2)} · HR {fmt.pct(m.hit_rate, 1)}</span></div>
                    <Bar value={m.hit_rate} max={Math.max(s.recs_eval.adamic_adar.hit_rate, 0.0001)} />
                  </div>
                ))}
                {s.recs_eval.lift_vs_baseline && <div style={{ fontSize: 26, fontWeight: 800, color: 'var(--good)' }}>{fmt.num(s.recs_eval.lift_vs_baseline, 1)}× <span style={{ fontSize: 13, fontWeight: 400 }} className="muted">precision lift</span></div>}
              </Card>
            ) : <Card title="Recommender evaluation"><div className="empty">Run job 03 with --evaluate</div></Card>}
          </Loading>
          <Loading state={summary}>
            {(s) => (
              <Card title="Algorithm runtimes" sub="distributed, seconds">
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart layout="vertical" data={Object.entries(s.algorithm_seconds || {}).map(([k, v]) => ({ k: k.replace(/_/g, ' '), v }))} margin={{ left: 30, right: 12 }}>
                    <XAxis type="number" />
                    <YAxis type="category" dataKey="k" width={110} />
                    <Tooltip content={<ChartTip fmtValue={(v) => `${v}s`} />} />
                    <RBar dataKey="v" name="seconds" fill="#a78bfa" radius={[0, 6, 6, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </Card>
            )}
          </Loading>
        </div>
      </div>
    </div>
  )
}
