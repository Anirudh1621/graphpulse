import { useMemo, useRef, useState } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { useNavigate } from 'react-router-dom'
import { Bar, Card, Kpi, Loading, UserChip, useWidth } from '../components/ui'
import { useApi } from '../lib/api'
import { commColor, fitOnce, fmt } from '../lib/util'

function Detail({ cid }) {
  const c = useApi(`/api/communities/${cid}`)
  const nav = useNavigate()
  return (
    <Loading state={c}>
      {(d) => (
        <Card title={<><span className="dot" style={{ background: commColor(d.community) }} />Community {d.community}</>} sub={`#${d.size_rank} by size`}>
          <div className="kpis" style={{ gridTemplateColumns: '1fr 1fr' }}>
            <Kpi label="Members" value={fmt.int(d.size)} />
            <Kpi label="Internal edges" value={fmt.int(d.internal_edges)} />
            <Kpi label="Density" value={fmt.num(d.density, 4)} />
            <Kpi label="Conductance" value={fmt.num(d.conductance, 3)} hint="lower = better separated" />
            <Kpi label="Avg clustering" value={fmt.num(d.avg_clustering, 3)} />
            <Kpi label="PageRank mass" value={fmt.pct(d.pagerank_mass, 2)} hint="share of total influence" />
          </div>
          <h3 style={{ marginTop: 16 }}>Key members</h3>
          <table><tbody>
            {d.members.slice(0, 12).map((m) => (
              <tr key={m.id} className="click" onClick={() => nav(`/user/${m.id}`)}>
                <td><UserChip id={m.id} community={m.community} link={false} /></td>
                <td className="num">{fmt.num(m.influence, 1)}</td>
              </tr>
            ))}
          </tbody></table>
        </Card>
      )}
    </Loading>
  )
}

export default function Communities() {
  const c = useApi('/api/communities')
  const [sel, setSel] = useState(null)
  const [ref, width] = useWidth()
  const fg = useRef()
  const fit = useMemo(() => fitOnce(fg), [])

  const graph = useMemo(() => {
    if (!c.data) return null
    const ids = new Set(c.data.communities.slice(0, 40).map((x) => x.community))
    const pair = {}
    c.data.links.forEach((l) => {
      if (!ids.has(l.c_src) || !ids.has(l.c_dst)) return
      const k = l.c_src < l.c_dst ? `${l.c_src}|${l.c_dst}` : `${l.c_dst}|${l.c_src}`
      pair[k] = (pair[k] || 0) + l.weight
    })
    const maxW = Math.max(1, ...Object.values(pair))
    return {
      nodes: c.data.communities.slice(0, 40).map((x) => ({ id: x.community, size: x.size, rank: x.size_rank, conductance: x.conductance })),
      links: Object.entries(pair).map(([k, w]) => { const [a, b] = k.split('|').map(Number); return { source: a, target: b, w: w / maxW, weight: w } }),
    }
  }, [c.data])

  return (
    <div className="page">
      <div className="page-head">
        <h2>Community Map</h2>
        <p>Each bubble is a community detected by GraphFrames label propagation. Bubble size = members, line thickness = follow edges crossing between two communities.</p>
      </div>
      <div className="grid g-side">
        <div ref={ref} className="graph-wrap" style={{ height: 520 }}>
          <Loading state={c}>
            {() => (
              <ForceGraph2D ref={fg} graphData={graph} width={width} height={520} backgroundColor="rgba(0,0,0,0)"
                onEngineStop={fit} nodeVal={(n) => 2 + Math.sqrt(n.size)} nodeRelSize={3} nodeColor={(n) => commColor(n.id)}
                linkWidth={(l) => 0.5 + 6 * l.w} linkColor={() => '#94a3b833'} cooldownTicks={100}
                nodeLabel={(n) => `C${n.rank} · ${fmt.int(n.size)} members · conductance ${fmt.num(n.conductance, 2)}`}
                nodeCanvasObjectMode={() => 'after'}
                nodeCanvasObject={(n, ctx, scale) => {
                  if (n.rank > 12) return
                  ctx.font = `${11 / scale}px sans-serif`; ctx.fillStyle = '#e2e8f0'; ctx.textAlign = 'center'
                  ctx.fillText(`C${n.rank}`, n.x, n.y + 3 / scale)
                }}
                onNodeClick={(n) => setSel(n.id)} />
            )}
          </Loading>
        </div>
        <div>{sel != null ? <Detail cid={sel} /> : <Card title="Select a community"><div className="muted" style={{ fontSize: 13 }}>Click a bubble to see its structure metrics and most influential members.</div></Card>}</div>
      </div>

      <Card title="All major communities" sub="quality metrics per community">
        <Loading state={c}>
          {(d) => (
            <div className="scroll"><table>
              <thead><tr><th>Rank</th><th>Community</th><th className="num">Members</th><th className="num">Internal edges</th><th className="num">Density</th><th>Conductance</th><th className="num">Avg clustering</th><th className="num">PageRank mass</th><th>Top members</th></tr></thead>
              <tbody>
                {d.communities.map((x) => (
                  <tr key={x.community} className="click" onClick={() => setSel(x.community)}>
                    <td className="dim">C{x.size_rank}</td>
                    <td><span className="dot" style={{ display: 'inline-block', background: commColor(x.community), marginRight: 8 }} /><span className="mono">{x.community}</span></td>
                    <td className="num">{fmt.int(x.size)}</td>
                    <td className="num">{fmt.int(x.internal_edges)}</td>
                    <td className="num">{fmt.num(x.density, 4)}</td>
                    <td style={{ minWidth: 120 }}><div className="row" style={{ flexWrap: 'nowrap' }}><Bar value={1 - x.conductance} max={1} /><span className="mono">{fmt.num(x.conductance, 2)}</span></div></td>
                    <td className="num">{fmt.num(x.avg_clustering, 3)}</td>
                    <td className="num">{fmt.pct(x.pagerank_mass, 2)}</td>
                    <td><div className="chips">{x.top.slice(0, 3).map((t) => <UserChip key={t} id={t} community={x.community} showId={false} />)}</div></td>
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
