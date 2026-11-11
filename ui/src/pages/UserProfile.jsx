import { useMemo, useRef } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { Bar, Card, Kpi, Loading, UserChip, useWidth } from '../components/ui'
import { useApi } from '../lib/api'
import { alias, commColor, fitOnce, fmt, suspColor } from '../lib/util'

const ROLE = { center: '#ffffff', mutual: '#34d399', following: '#38bdf8', follower: '#a78bfa' }

function Ego({ id }) {
  const ego = useApi(`/api/users/${id}/ego`)
  const [ref, width] = useWidth()
  const nav = useNavigate()
  const fg = useRef()
  const fit = useMemo(() => fitOnce(fg), [id])
  const data = useMemo(() => ego.data && {
    nodes: ego.data.nodes.map((n) => ({ ...n })), links: ego.data.links.map((l) => ({ ...l })),
  }, [ego.data])
  return (
    <Card title="Ego network" sub="most influential neighbours + edges among them">
      <div ref={ref} className="graph-wrap" style={{ height: 380 }}>
        <Loading state={ego}>
          {() => (
            <ForceGraph2D ref={fg} onEngineStop={fit} graphData={data} width={width} height={380} backgroundColor="rgba(0,0,0,0)"
              nodeId="id" nodeVal={(n) => (n.role === 'center' ? 14 : 2 + (n.influence || 0) / 20)} nodeRelSize={3}
              nodeColor={(n) => ROLE[n.role]} linkColor={() => '#94a3b833'} linkDirectionalArrowLength={2.5}
              linkDirectionalArrowRelPos={1} cooldownTicks={80}
              nodeLabel={(n) => `@${alias(n.id)} (${n.role}) · influence ${fmt.num(n.influence, 1)}`}
              onNodeClick={(n) => n.role !== 'center' && nav(`/user/${n.id}`)} />
          )}
        </Loading>
      </div>
      <div className="legend" style={{ marginTop: 10 }}>
        {Object.entries(ROLE).map(([r, c]) => <span key={r}><i style={{ background: c }} />{r}</span>)}
      </div>
    </Card>
  )
}

export default function UserProfile() {
  const { id } = useParams()
  const u = useApi(`/api/users/${id}`, { refresh: 10000 })
  const s = useApi('/api/summary')
  const users = s.data?.users

  return (
    <div className="page">
      <Loading state={u} empty="User not found">
        {(d) => {
          const fDelta = d.stream?.followers_delta || 0
          return (
            <>
              <div className="row" style={{ justifyContent: 'space-between' }}>
                <div className="row" style={{ gap: 16 }}>
                  <div className="avatar" style={{ width: 56, height: 56, fontSize: 20, background: commColor(d.community) }}>{alias(d.id).slice(0, 2).toUpperCase()}</div>
                  <div>
                    <h2 style={{ margin: 0 }}>@{alias(d.id)}</h2>
                    <div className="muted mono">user #{d.id}</div>
                  </div>
                </div>
                <div className="row">
                  <span className="badge accent">Influence rank #{fmt.int(d.influence_rank)}{users ? ` of ${fmt.compact(users)}` : ''}</span>
                  <span className="badge" style={{ color: commColor(d.community) }}>community {d.community}</span>
                  <Link className="btn" to={`/path?dst=${d.id}`}>Find path to this user</Link>
                </div>
              </div>

              <div className="kpis">
                <Kpi label="Influence index" value={fmt.num(d.influence, 1)} hint={`top ${fmt.num(100 - d.influence, 1)}%`} color="var(--accent)" />
                <Kpi label="PageRank" value={fmt.sci(d.pagerank)} hint={`rank #${fmt.int(d.pr_rank)}`} />
                <Kpi label="Followers (live)" value={fmt.int(d.live_followers)}
                  hint={fDelta ? <span style={{ color: fDelta > 0 ? 'var(--good)' : 'var(--bad)' }}>{fDelta > 0 ? '+' : ''}{fDelta} since last batch run</span> : `batch: ${fmt.int(d.in_deg)}`} />
                <Kpi label="Following (live)" value={fmt.int(d.live_following)} hint={`batch: ${fmt.int(d.out_deg)}`} />
                <Kpi label="Clustering coeff." value={fmt.num(d.clustering, 3)} hint={`${fmt.int(d.triangles)} triangles`} />
                <Kpi label="Reciprocity" value={fmt.pct(d.reciprocity)} hint="follows returned" />
                <Kpi label="Hops to #1 hub" value={d.hop_to_top_hub ?? '∞'} hint="GraphFrames shortestPaths" />
                <Kpi label="Suspicion" value={fmt.num(d.suspicion, 0)} hint={`anomaly pct ${fmt.pct(d.anomaly_pct, 0)}`} color={suspColor(d.suspicion)} />
              </div>

              <div className="grid g-side">
                <Ego id={d.id} />
                <div className="grid" style={{ alignContent: 'start' }}>
                  <Card title="Risk assessment" sub="MLlib KMeans + rules">
                    <div className="row" style={{ flexWrap: 'nowrap', marginBottom: 10 }}><Bar value={d.suspicion} red /><b style={{ color: suspColor(d.suspicion) }}>{fmt.num(d.suspicion, 0)}/100</b></div>
                    {d.susp_reasons?.length ? (
                      <ul style={{ margin: 0, paddingLeft: 18, color: 'var(--muted)', fontSize: 13, lineHeight: 1.7 }}>
                        {d.susp_reasons.map((r) => <li key={r}>{r}</li>)}
                      </ul>
                    ) : <div className="muted" style={{ fontSize: 13 }}>{d.suspicion >= 20 ? 'Statistically unusual, but not bot-like (e.g. a popular account).' : 'No suspicious behaviour patterns.'}</div>}
                  </Card>
                  <Card title="Distance to top influencers" sub="follow hops">
                    {Object.entries(d.hub_dist || {}).length ? Object.entries(d.hub_dist).map(([h, dist]) => (
                      <div key={h} className="row" style={{ justifyContent: 'space-between', marginBottom: 6 }}>
                        <UserChip id={h} showId={false} /><span className="badge">{dist} hop{dist === 1 ? '' : 's'}</span>
                      </div>
                    )) : <div className="muted" style={{ fontSize: 13 }}>Cannot reach any top hub by following links.</div>}
                  </Card>
                </div>
              </div>

              <Card title="People you may know" sub="Adamic-Adar over (a)→(b)→(c) motifs · score favours shared contacts who are not celebrities">
                {d.recs?.length ? (
                  <div className="scroll"><table>
                    <thead><tr><th>Suggested account</th><th>Score</th><th className="num">Shared contacts</th><th>Because you follow</th><th className="num">Influence</th></tr></thead>
                    <tbody>
                      {d.recs.map((r) => (
                        <tr key={r.id}>
                          <td><UserChip id={r.id} community={r.community} /></td>
                          <td style={{ minWidth: 140 }}><div className="row" style={{ flexWrap: 'nowrap' }}><Bar value={r.score} max={d.recs[0].score} /><span className="mono">{fmt.num(r.score, 2)}</span></div></td>
                          <td className="num">{r.mutual}</td>
                          <td><div className="chips">{(r.via || []).map((v) => <Link key={v} to={`/user/${v}`} className="tag">@{alias(v)}</Link>)}</div></td>
                          <td className="num">{fmt.num(r.influence, 1)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table></div>
                ) : <div className="empty">No recommendations: this account follows nobody with onward connections.</div>}
              </Card>
            </>
          )
        }}
      </Loading>
    </div>
  )
}
