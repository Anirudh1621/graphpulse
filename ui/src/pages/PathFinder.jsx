import { Fragment, useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { Card, UserChip } from '../components/ui'
import { api, useApi } from '../lib/api'
import { fmt } from '../lib/util'

export default function PathFinder() {
  const [params] = useSearchParams()
  const lb = useApi('/api/leaderboard?limit=100')
  const sus = useApi('/api/suspicious?limit=100')
  const [src, setSrc] = useState(params.get('src') || '')
  const [dst, setDst] = useState(params.get('dst') || '')
  const [mode, setMode] = useState('directed')
  const [res, setRes] = useState(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState(null)

  const pool = [...(lb.data || []), ...(sus.data || [])].map((x) => x.id)
  const random = () => {
    if (pool.length < 2) return
    const a = pool[Math.floor(Math.random() * pool.length)]
    let b = a
    while (b === a) b = pool[Math.floor(Math.random() * pool.length)]
    if (!params.get('dst')) setDst(String(a))
    setSrc(String(b))
  }
  const [auto, setAuto] = useState(false)
  useEffect(() => { if (!src && pool.length) { random(); setAuto(true) } }, [pool.length]) // eslint-disable-line

  const run = async () => {
    setBusy(true); setErr(null); setRes(null)
    try { setRes(await api(`/api/path?src=${src}&dst=${dst}&mode=${mode}`)) } catch (e) { setErr(e.message) }
    setBusy(false)
  }
  useEffect(() => { if (auto && src && dst) { setAuto(false); run() } }, [auto, src, dst]) // eslint-disable-line

  return (
    <div className="page">
      <div className="page-head">
        <h2>Six Degrees of Separation</h2>
        <p>Shortest follow-chain between any two accounts. It runs live as a <b>bidirectional BFS</b> over adjacency lists stored in HBase: the smaller frontier is expanded each round with batched multi-row reads.</p>
      </div>
      <Card>
        <div className="row">
          <input placeholder="From user id" value={src} onChange={(e) => setSrc(e.target.value.replace(/\D/g, ''))} style={{ width: 200 }} />
          <span className="muted">→</span>
          <input placeholder="To user id" value={dst} onChange={(e) => setDst(e.target.value.replace(/\D/g, ''))} style={{ width: 200 }} />
          <select value={mode} onChange={(e) => setMode(e.target.value)}>
            <option value="directed">follow chain (directed)</option>
            <option value="undirected">any connection (undirected)</option>
          </select>
          <button className="btn primary" disabled={!src || !dst || busy} onClick={run}>{busy ? 'Searching…' : 'Find path'}</button>
          <button className="btn ghost" onClick={random}>🎲 Random pair</button>
        </div>
      </Card>

      {err && <div className="error">⚠ {err}</div>}
      {res && (
        <Card title={res.path ? `${res.hops} degree${res.hops === 1 ? '' : 's'} of separation` : 'No path found'}
          sub={`${res.stats.levels} BFS levels · ${fmt.int(res.stats.rows_fetched)} HBase rows · ${res.stats.ms} ms`}>
          {res.path ? (
            <div className="path">
              {res.nodes.map((n, i) => (
                <Fragment key={n.id}>
                  {i > 0 && <div className="path-arrow">{mode === 'directed' ? '→' : '↔'}</div>}
                  <div className={`path-node ${i === 0 || i === res.nodes.length - 1 ? 'end' : ''}`}>
                    <UserChip id={n.id} community={n.community} />
                    <div className="muted" style={{ fontSize: 12, marginTop: 8 }}>
                      influence {fmt.num(n.influence, 1)} · {fmt.int(n.in_deg)} followers<br />community {n.community}
                    </div>
                  </div>
                </Fragment>
              ))}
            </div>
          ) : (
            <div className="muted">These accounts are not connected within 6 hops{mode === 'directed' ? ' along follow direction; try "any connection"' : ''}.</div>
          )}
        </Card>
      )}
    </div>
  )
}
