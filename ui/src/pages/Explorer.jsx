import { useMemo, useRef, useState } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { Link } from 'react-router-dom'
import { Card, Kpi, Loading, UserChip, useWidth } from '../components/ui'
import { useApi } from '../lib/api'
import { alias, commColor, fitOnce, fmt, suspColor } from '../lib/util'

export default function Explorer() {
  const g = useApi('/api/graph/top')
  const [colorBy, setColorBy] = useState('community')
  const [sel, setSel] = useState(null)
  const [q, setQ] = useState('')
  const [ref, width] = useWidth()
  const fg = useRef()
  const fit = useMemo(() => fitOnce(fg), [])

  const data = useMemo(() => {
    if (!g.data) return null
    const max = Math.max(...g.data.nodes.map((n) => n.pagerank))
    return {
      nodes: g.data.nodes.map((n) => ({ ...n, val: 1 + 30 * Math.sqrt(n.pagerank / max) })),
      links: g.data.links.map((l) => ({ ...l })),
    }
  }, [g.data])

  const neighbours = useMemo(() => {
    if (!sel || !data) return new Set()
    const s = new Set([sel.id])
    data.links.forEach((l) => {
      const a = l.source.id ?? l.source, b = l.target.id ?? l.target
      if (a === sel.id) s.add(b)
      if (b === sel.id) s.add(a)
    })
    return s
  }, [sel, data])

  const legend = useMemo(() => {
    if (!data) return []
    const c = {}
    data.nodes.forEach((n) => { c[n.community] = (c[n.community] || 0) + 1 })
    return Object.entries(c).sort((a, b) => b[1] - a[1]).slice(0, 8)
  }, [data])

  const color = (n) => {
    if (sel && !neighbours.has(n.id)) return '#1e293b'
    if (q && String(n.id).startsWith(q)) return '#ffffff'
    return colorBy === 'community' ? commColor(n.community) : suspColor(n.suspicion)
  }

  const focus = () => {
    const n = data?.nodes.find((x) => String(x.id).startsWith(q))
    if (n && fg.current) { setSel(n); fg.current.centerAt(n.x, n.y, 600); fg.current.zoom(3, 600) }
  }

  return (
    <div className="page">
      <div className="page-head">
        <h2>Graph Explorer</h2>
        <p>The {data?.nodes.length || 400} highest-PageRank accounts and every follow edge between them. Size = PageRank, colour = community. Click a node.</p>
      </div>
      <div className="grid g-side">
        <div ref={ref} className="graph-wrap" style={{ height: 'calc(100vh - 190px)', minHeight: 480 }}>
          <Loading state={g}>
            {() => (
              <ForceGraph2D ref={fg} graphData={data} width={width} height={Math.max(480, window.innerHeight - 190)}
                backgroundColor="rgba(0,0,0,0)" nodeId="id" nodeVal="val" nodeColor={color} nodeRelSize={3}
                nodeLabel={(n) => `@${alias(n.id)} · influence ${fmt.num(n.influence, 1)}`}
                linkColor={(l) => (sel && ((l.source.id ?? l.source) === sel.id || (l.target.id ?? l.target) === sel.id) ? '#38bdf8aa' : '#94a3b81a')}
                linkDirectionalArrowLength={sel ? 3 : 0} linkDirectionalArrowRelPos={1}
                cooldownTicks={120} onEngineStop={fit} onNodeClick={(n) => setSel(n)} onBackgroundClick={() => setSel(null)} />
            )}
          </Loading>
          <div className="graph-overlay">
            <select value={colorBy} onChange={(e) => setColorBy(e.target.value)}>
              <option value="community">Colour: community</option>
              <option value="suspicion">Colour: suspicion score</option>
            </select>
            <input placeholder="Highlight id…" value={q} onChange={(e) => setQ(e.target.value.replace(/\D/g, ''))} onKeyDown={(e) => e.key === 'Enter' && focus()} style={{ width: 160 }} />
          </div>
        </div>

        <div className="grid" style={{ alignContent: 'start' }}>
          {sel ? (
            <Card title="Selected account">
              <UserChip id={sel.id} community={sel.community} />
              <div className="kpis" style={{ gridTemplateColumns: '1fr 1fr', marginTop: 12 }}>
                <Kpi label="Influence" value={fmt.num(sel.influence, 1)} />
                <Kpi label="Followers" value={fmt.int(sel.inDegree)} />
                <Kpi label="PageRank" value={fmt.sci(sel.pagerank)} />
                <Kpi label="Suspicion" value={fmt.num(sel.suspicion, 0)} color={suspColor(sel.suspicion)} />
              </div>
              <div className="muted" style={{ fontSize: 12.5, margin: '10px 0' }}>{neighbours.size - 1} direct connections inside this view</div>
              <Link className="btn primary" to={`/user/${sel.id}`}>Open full profile →</Link>
            </Card>
          ) : (
            <Card title="How to read this"><div className="muted" style={{ fontSize: 13, lineHeight: 1.6 }}>
              Dense same-colour clusters are communities found by <b>label propagation</b>. Large nodes are
              accounts with high <b>PageRank</b>: they matter because important accounts follow them, not just because many do.
              Switch the colour to see the <b>MLlib anomaly / suspicion</b> score.
            </div></Card>
          )}
          <Card title="Communities in view">
            <div className="legend" style={{ flexDirection: 'column', gap: 6 }}>
              {legend.map(([c, n]) => <span key={c}><i style={{ background: commColor(Number(c)) }} />community {c} · {n} accounts</span>)}
            </div>
          </Card>
        </div>
      </div>
    </div>
  )
}
