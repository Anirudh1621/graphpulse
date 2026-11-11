import { useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { alias, commColor } from '../lib/util'

export function Card({ title, sub, children, style, className = '' }) {
  return (
    <div className={`card ${className}`} style={style}>
      {title && <h3>{title}{sub && <span className="sub">{sub}</span>}</h3>}
      {children}
    </div>
  )
}

export function Kpi({ label, value, hint, color }) {
  return (
    <div className="kpi">
      <div className="label">{label}</div>
      <div className="value" style={color ? { color } : undefined}>{value}</div>
      {hint && <div className="hint">{hint}</div>}
    </div>
  )
}

export function Bar({ value, max = 100, red }) {
  const w = Math.max(0, Math.min(100, (100 * (value || 0)) / (max || 1)))
  return <div className={`bar ${red ? 'red' : ''}`}><span style={{ width: `${w}%` }} /></div>
}

export function UserChip({ id, community, link = true, showId = true }) {
  const name = alias(id)
  const body = (
    <span className="user-chip">
      <span className="avatar" style={{ background: commColor(community) }}>{name.slice(0, 2).toUpperCase()}</span>
      <span style={{ minWidth: 0 }}>
        <span className="name">@{name}</span>
        {showId && <span className="uid"> #{id}</span>}
      </span>
    </span>
  )
  return link ? <Link to={`/user/${id}`} style={{ color: 'inherit' }}>{body}</Link> : body
}

export function Loading({ state, children, empty = 'No data' }) {
  if (state.error) return <div className="error">⚠ {state.error}</div>
  if (state.loading && !state.data) return <div className="empty">Loading…</div>
  if (!state.data || (Array.isArray(state.data) && !state.data.length)) return <div className="empty">{empty}</div>
  return children(state.data)
}

/** Tracks the width of its container so canvas graphs fit responsively. */
export function useWidth() {
  const ref = useRef(null)
  const [w, setW] = useState(600)
  useEffect(() => {
    if (!ref.current) return
    const ro = new ResizeObserver(([e]) => setW(Math.floor(e.contentRect.width)))
    ro.observe(ref.current)
    return () => ro.disconnect()
  }, [])
  return [ref, w]
}

export function ChartTip({ active, payload, label, fmtLabel, fmtValue }) {
  if (!active || !payload?.length) return null
  return (
    <div className="tooltip">
      {label != null && <div className="muted" style={{ marginBottom: 4 }}>{fmtLabel ? fmtLabel(label) : label}</div>}
      {payload.map((p) => (
        <div key={p.dataKey + p.name} style={{ color: p.color || p.fill }}>
          {p.name}: <b>{fmtValue ? fmtValue(p.value) : p.value}</b>
        </div>
      ))}
    </div>
  )
}
