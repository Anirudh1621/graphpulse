import { useMemo, useState } from 'react'
import { Area, AreaChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { Bar, Card, ChartTip, UserChip } from '../components/ui'
import { post, useApi } from '../lib/api'
import { useLive } from '../lib/live'
import { alias, fmt } from '../lib/util'

const TYPES = [['follow', '#34d399'], ['like', '#f472b6'], ['mention', '#fbbf24'], ['unfollow', '#f87171']]
const clock = (t) => new Date(t * 1000).toLocaleTimeString([], { minute: '2-digit', second: '2-digit' })

export function AlertItem({ a }) {
  return (
    <div className={`feed-item alert-item ${a.severity}`}>
      <span className={`badge ${a.severity === 'critical' ? 'bad' : 'warn'}`}>{a.severity}</span>
      <div style={{ flex: 1, minWidth: 0 }}>
        <UserChip id={a.src} community={a.community} />
        <div className="muted" style={{ fontSize: 12 }}>
          {a.peak || a.follows} follows / 30 s → {a.distinct_targets} distinct accounts
          {a.windows?.length > 1 && <> · {a.windows.length} windows</>}
          {a.batch_suspicion != null && <> · batch suspicion {fmt.num(a.batch_suspicion, 0)}</>}
          {a.batch_suspicion == null && <> · <b>new account</b> (not in batch view)</>}
        </div>
      </div>
      <span className="dim mono">{clock(a.window_start)}</span>
    </div>
  )
}

export default function Live() {
  const live = useLive()
  const health = useApi('/api/health', { refresh: 3000 })
  const [target, setTarget] = useState('')
  const [msg, setMsg] = useState(null)

  const thr = useMemo(() => Object.entries(live.windows).map(([w, c]) => ({ t: Number(w), ...c }))
    .sort((a, b) => a.t - b.t).slice(-30), [live.windows])
  const rate = live.rate.slice(-90)
  const sim = health.data?.live || {}

  const act = async (action, extra = {}) => {
    try {
      const r = await post('/api/simulate', { action, ...extra })
      setMsg(action === 'viral' ? `Viral surge started for @${alias(r.target)} (#${r.target})` :
        action === 'bot' ? `Bot #${r.target} is now mass-following accounts` : `Baseline traffic ${r.baseline ? 'on' : 'off'}`)
      health.reload()
    } catch (e) { setMsg(`⚠ ${e.message}`) }
  }
  const maxRise = live.rising[0]?.new_followers || 1

  return (
    <div className="page">
      <div className="page-head">
        <h2>Live Stream</h2>
        <p>Events flow Kafka → Spark Structured Streaming (watermarked event-time windows) → gp-insights → WebSocket. Use the controls to inject scenarios and watch Spark detect them.</p>
      </div>

      <Card title="Scenario control room" sub={sim.mode === 'kafka' ? 'events are produced into Kafka topic gp-events' : 'offline simulation engine'}>
        <div className="row">
          <button className="btn" onClick={() => act(sim.baseline ? 'baseline_off' : 'baseline_on')}>
            <span className={`dot ${sim.baseline ? 'on' : ''}`} /> Baseline traffic {sim.baseline ? 'ON' : 'OFF'}
          </button>
          <input placeholder="target id (optional)" value={target} onChange={(e) => setTarget(e.target.value.replace(/\D/g, ''))} style={{ width: 180 }} />
          <button className="btn primary" onClick={() => act('viral', { target: target ? Number(target) : null, duration: 60, rate: 6 })}>🚀 Simulate viral surge</button>
          <button className="btn danger" onClick={() => act('bot', { duration: 30, rate: 5 })}>🤖 Launch bot attack</button>
          {(sim.active || []).map((a) => <span key={a.scenario + a.target} className={`tag ${a.scenario}`}>{a.scenario} #{a.target} · {a.ends_in}s</span>)}
        </div>
        {msg && <div className="muted" style={{ marginTop: 10, fontSize: 13 }}>{msg}</div>}
      </Card>

      <div className="grid g2">
        <Card title="Ingest rate" sub="events / second seen on Kafka">
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={rate}>
              <CartesianGrid vertical={false} />
              <XAxis dataKey="t" tickFormatter={clock} minTickGap={40} />
              <YAxis />
              <Tooltip content={<ChartTip fmtLabel={clock} />} />
              <Line type="monotone" dataKey="v" name="events/s" stroke="#38bdf8" dot={false} strokeWidth={2} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        </Card>
        <Card title="Spark windowed throughput" sub="5-second tumbling windows by event type">
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={thr}>
              <CartesianGrid vertical={false} />
              <XAxis dataKey="t" tickFormatter={clock} minTickGap={40} />
              <YAxis />
              <Tooltip content={<ChartTip fmtLabel={clock} />} />
              <Legend />
              {TYPES.map(([t, c]) => <Area key={t} type="monotone" dataKey={t} stackId="1" stroke={c} fill={c + '55'} isAnimationActive={false} />)}
            </AreaChart>
          </ResponsiveContainer>
        </Card>
      </div>

      <div className="grid g3">
        <Card title="🔥 Rising stars" sub="new followers · 2-min sliding window">
          {live.rising.length ? live.rising.map((r) => (
            <div key={r.user} className="row" style={{ flexWrap: 'nowrap', marginBottom: 9 }}>
              <div style={{ flex: '0 0 55%', minWidth: 0 }}><UserChip id={r.user} showId={false} /></div>
              <Bar value={r.new_followers} max={maxRise} /><b className="mono">+{r.new_followers}</b>
            </div>
          )) : <div className="empty">Waiting for the first window…</div>}
        </Card>
        <Card title="⚠ Threat alerts" sub={`${live.alerts.length} in session`}>
          <div className="feed">
            {live.alerts.length ? live.alerts.map((a) => <AlertItem key={a.src} a={a} />) : <div className="empty">No follow-spam bursts detected. Launch a bot attack ↑</div>}
          </div>
        </Card>
        <Card title="Event feed" sub="sampled from gp-events">
          <div className="feed">
            {live.events.map((e) => (
              <div key={e.event_id} className="feed-item">
                <span className={`tag ${e.type}`}>{e.type}</span>
                {e.scenario !== 'baseline' && <span className={`tag ${e.scenario}`}>{e.scenario}</span>}
                <span className="mono" style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>@{alias(e.src)} → @{alias(e.dst)}</span>
              </div>
            ))}
          </div>
        </Card>
      </div>
    </div>
  )
}
