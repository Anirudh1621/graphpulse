import { useMemo } from 'react'
import { CartesianGrid, Cell, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis, ZAxis } from 'recharts'
import { useNavigate } from 'react-router-dom'
import { Bar, Card, Loading, UserChip } from '../components/ui'
import { useApi } from '../lib/api'
import { useLive } from '../lib/live'
import { alias, fmt, suspColor } from '../lib/util'
import { AlertItem } from './Live'

function DotTip({ active, payload }) {
  if (!active || !payload?.length) return null
  const d = payload[0].payload
  return <div className="tooltip">@{alias(d.id)}<br />following {d.x - 1} · followers {d.y - 1}<br />suspicion <b style={{ color: suspColor(d.s) }}>{d.s}</b></div>
}

export default function Threats() {
  const sus = useApi('/api/suspicious?limit=100')
  const lb = useApi('/api/leaderboard?limit=100')
  const stored = useApi('/api/alerts?limit=50', { refresh: 15000 })
  const live = useLive()
  const nav = useNavigate()

  const points = useMemo(() => [...(sus.data || []), ...(lb.data || [])].map((d) => ({
    id: d.id, x: d.outDegree + 1, y: d.inDegree + 1, s: d.suspicion,
  })), [sus.data, lb.data])

  const alerts = useMemo(() => {
    // group by account: newest window first, keep peak burst size and number of windows flagged
    const m = new Map()
    ;[...live.alerts, ...(stored.data || [])].forEach((a) => {
      const cur = m.get(a.src)
      const windows = new Set([...(cur?.windows || []), ...(a.windows || [a.window_start])])
      const newest = !cur || a.window_start > cur.window_start ? a : cur
      m.set(a.src, { ...newest, windows: [...windows], peak: Math.max(a.peak || a.follows, cur?.peak || 0) })
    })
    return [...m.values()].sort((a, b) => b.window_start - a.window_start).slice(0, 30)
  }, [live.alerts, stored.data])

  return (
    <div className="page">
      <div className="page-head">
        <h2>Threat Radar</h2>
        <p>Two lines of defence: <b>batch</b> behavioural anomaly detection (Spark MLlib KMeans distance-to-centroid plus interpretable rules) and <b>real-time</b> follow-burst detection in Spark Streaming, enriched with the batch score through a stream–static join.</p>
      </div>

      <div className="grid g2">
        <Card title="Behaviour map" sub="following vs followers (log) · colour = suspicion">
          <Loading state={sus}>
            {() => (
              <ResponsiveContainer width="100%" height={300}>
                <ScatterChart margin={{ top: 8, right: 12, bottom: 8, left: 0 }}>
                  <CartesianGrid />
                  <XAxis dataKey="x" type="number" scale="log" domain={[1, 'auto']} name="following" tickFormatter={fmt.compact} label={{ value: 'following →', position: 'insideBottomRight', offset: -4, fill: '#5b6b8a', fontSize: 11 }} />
                  <YAxis dataKey="y" type="number" scale="log" domain={[1, 'auto']} name="followers" tickFormatter={fmt.compact} />
                  <ZAxis range={[40, 40]} />
                  <Tooltip content={<DotTip />} />
                  <Scatter data={points} onClick={(p) => nav(`/user/${p.id}`)}>
                    {points.map((p) => <Cell key={p.id} fill={suspColor(p.s)} />)}
                  </Scatter>
                </ScatterChart>
              </ResponsiveContainer>
            )}
          </Loading>
          <div className="muted" style={{ fontSize: 12 }}>Influencers sit top-left (many followers). Follow-spam accounts sit bottom-right: they follow many accounts and few follow back.</div>
        </Card>
        <Card title="Real-time alerts" sub="Spark Streaming · 30 s sliding window">
          <div className="feed" style={{ maxHeight: 340 }}>
            {alerts.length ? alerts.map((a) => <AlertItem key={a.src} a={a} />) : <div className="empty">No alerts yet. Trigger a bot attack from the Live Stream page.</div>}
          </div>
        </Card>
      </div>

      <Card title="Highest-risk accounts" sub="batch layer · suspicion score 0–100">
        <Loading state={sus}>
          {(rows) => (
            <div className="scroll"><table>
              <thead><tr><th>Account</th><th>Suspicion</th><th className="num">Following</th><th className="num">Followers</th><th className="num">Reciprocity</th><th className="num">Anomaly pct</th><th>Why</th></tr></thead>
              <tbody>
                {rows.slice(0, 40).map((r) => (
                  <tr key={r.id} className="click" onClick={() => nav(`/user/${r.id}`)}>
                    <td><UserChip id={r.id} community={r.community} link={false} /></td>
                    <td style={{ minWidth: 130 }}><div className="row" style={{ flexWrap: 'nowrap' }}><Bar value={r.suspicion} red /><b style={{ color: suspColor(r.suspicion) }}>{fmt.num(r.suspicion, 0)}</b></div></td>
                    <td className="num">{fmt.int(r.outDegree)}</td>
                    <td className="num">{fmt.int(r.inDegree)}</td>
                    <td className="num">{fmt.pct(r.reciprocity, 0)}</td>
                    <td className="num">{fmt.pct(r.anomaly_pct, 1)}</td>
                    <td><div className="chips">{(r.susp_reasons || []).map((x) => <span key={x} className="tag">{x}</span>)}</div></td>
                  </tr>
                ))}
              </tbody>
            </table></div>
          )}
        </Loading>
      </Card>
    </div>
  )
}
