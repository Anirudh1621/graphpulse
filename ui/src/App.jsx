import { useState } from 'react'
import { NavLink, Route, Routes, useNavigate } from 'react-router-dom'
import { useApi, api } from './lib/api'
import { useLive } from './lib/live'
import { fmt, registerCommunities } from './lib/util'
import Overview from './pages/Overview'
import Explorer from './pages/Explorer'
import UserProfile from './pages/UserProfile'
import PathFinder from './pages/PathFinder'
import Communities from './pages/Communities'
import Live from './pages/Live'
import Threats from './pages/Threats'
import Cluster from './pages/Cluster'

const NAV = [
  ['Analytics', [['/', '◎', 'Overview'], ['/explorer', '✦', 'Graph Explorer'], ['/communities', '⬡', 'Communities'],
    ['/path', '⇢', 'Six Degrees']]],
  ['Real-time', [['/live', '◉', 'Live Stream'], ['/threats', '⚠', 'Threat Radar']]],
  ['Platform', [['/cluster', '▦', 'Cluster']]],
]

function Search() {
  const [q, setQ] = useState('')
  const [hits, setHits] = useState([])
  const nav = useNavigate()
  const go = (id) => { setQ(''); setHits([]); nav(`/user/${id}`) }
  const onChange = async (v) => {
    setQ(v)
    if (/^\d{2,}$/.test(v)) {
      try { setHits(await api(`/api/users/search?q=${v}`)) } catch { setHits([]) }
    } else setHits([])
  }
  return (
    <div style={{ position: 'relative', width: 'min(340px, 40vw)' }}>
      <input style={{ width: '100%' }} placeholder="Find user by id…" value={q} onChange={(e) => onChange(e.target.value)}
        onKeyDown={(e) => e.key === 'Enter' && /^\d+$/.test(q) && go(q)} />
      {hits.length > 0 && (
        <div className="card" style={{ position: 'absolute', top: 44, left: 0, right: 0, padding: 6, zIndex: 20 }}>
          {hits.map((h) => (
            <div key={h.id} className="feed-item" style={{ cursor: 'pointer', marginBottom: 4 }} onClick={() => go(h.id)}>
              <span className="mono">#{h.id}</span><span className="muted">influence {fmt.num(h.influence, 1)}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function TopBar() {
  const live = useLive()
  const health = useApi('/api/health', { refresh: 15000 })
  const backend = health.data?.backend
  const mode = live.mode || health.data?.live?.mode
  const last = live.rate.slice(-4)
  const eps = last.length ? last.reduce((a, b) => a + b.v, 0) / last.length : 0
  return (
    <div className="topbar">
      <Search />
      <div className="grow" />
      <span className={`badge ${backend === 'hbase' ? 'good' : 'warn'}`} title="Serving layer">
        {backend === 'hbase' ? 'HBase serving layer' : backend === 'snapshot' ? 'Snapshot (offline)' : '…'}
      </span>
      <span className={`badge ${mode === 'kafka' ? 'good' : mode === 'simulate' ? 'warn' : ''}`}>
        <span className={`dot ${live.connected ? 'on' : 'off'}`} />
        {mode === 'kafka' ? 'Kafka + Spark Streaming' : mode === 'simulate' ? 'Simulated stream' : 'stream off'}
        {live.connected && <b style={{ color: 'var(--text)' }}>{fmt.int(eps)} ev/s</b>}
      </span>
    </div>
  )
}

export default function App() {
  const comm = useApi('/api/communities')
  if (comm.data) registerCommunities(comm.data.communities)
  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-logo">G</div>
          <div><h1>GraphPulse</h1><small>Social Network Intelligence</small></div>
        </div>
        {NAV.map(([section, items]) => (
          <div key={section} className="nav">
            <div className="nav-section">{section}</div>
            {items.map(([to, ico, label]) => (
              <NavLink key={to} to={to} end={to === '/'}><span className="ico">{ico}</span>{label}</NavLink>
            ))}
          </div>
        ))}
        <div className="sidebar-foot">
          Hadoop · Spark · GraphFrames · MLlib · Kafka · HBase<br />
          CSBB 422 Big Data Analytics<br />
          <span className="dim">User aliases are generated from anonymised ids.</span>
        </div>
      </aside>
      <main className="main">
        <TopBar />
        <Routes>
          <Route path="/" element={<Overview />} />
          <Route path="/explorer" element={<Explorer />} />
          <Route path="/user/:id" element={<UserProfile />} />
          <Route path="/path" element={<PathFinder />} />
          <Route path="/communities" element={<Communities />} />
          <Route path="/live" element={<Live />} />
          <Route path="/threats" element={<Threats />} />
          <Route path="/cluster" element={<Cluster />} />
        </Routes>
      </main>
    </div>
  )
}
