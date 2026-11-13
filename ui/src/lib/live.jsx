import { createContext, useContext, useEffect, useReducer } from 'react'
import { mergeAlert } from './alerts'
import { wsUrl } from './api'

const LiveCtx = createContext(null)

const initial = {
  connected: false, mode: null, totalEvents: 0,
  rate: [],            // [{t, v}] events/s observed at the API
  windows: {},         // window_start -> {follow, like, ...}  (Spark Structured Streaming output)
  events: [], rising: [], risingTs: null, alerts: [], scenarios: [],
}

function apply(s, m) {
  switch (m.kind) {
    case 'ingest_rate': {
      const rate = [...s.rate, { t: m.ts, v: m.events }].slice(-120)
      return { ...s, rate }
    }
    case 'throughput': {
      const w = { ...(s.windows[m.window_start] || {}), [m.type]: m.count }
      const windows = { ...s.windows, [m.window_start]: w }
      const keys = Object.keys(windows).map(Number).sort((a, b) => a - b)
      if (keys.length > 36) keys.slice(0, keys.length - 36).forEach((k) => delete windows[k])
      return { ...s, windows }
    }
    case 'event':
      return { ...s, totalEvents: s.totalEvents + 1, events: [m, ...s.events].slice(0, 40) }
    case 'rising':
      return { ...s, rising: m.items || [], risingTs: m.ts || Date.now() / 1000 }
    case 'alert': {
      // one row per offending account (see mergeAlert)
      const merged = mergeAlert(s.alerts.find((a) => a.src === m.src), m)
      return { ...s, alerts: [merged, ...s.alerts.filter((a) => a.src !== m.src)].slice(0, 60) }
    }
    case 'scenario':
      return { ...s, scenarios: [m, ...s.scenarios].slice(0, 12) }
    default:
      return s
  }
}

function reducer(s, action) {
  if (action.type === 'conn') return { ...s, connected: action.value }
  const m = action.msg
  if (m.kind === 'hello') return (m.recent || []).reduce(apply, { ...s, mode: m.mode })
  return apply(s, m)
}

export function LiveProvider({ children }) {
  const [state, dispatch] = useReducer(reducer, initial)
  useEffect(() => {
    let ws, timer, closed = false
    const connect = () => {
      ws = new WebSocket(wsUrl())
      ws.onopen = () => dispatch({ type: 'conn', value: true })
      ws.onclose = () => {
        dispatch({ type: 'conn', value: false })
        if (!closed) timer = setTimeout(connect, 2000)
      }
      ws.onmessage = (e) => {
        try { dispatch({ type: 'msg', msg: JSON.parse(e.data) }) } catch { /* ignore */ }
      }
    }
    connect()
    const ping = setInterval(() => ws && ws.readyState === 1 && ws.send('ping'), 20000)
    return () => { closed = true; clearTimeout(timer); clearInterval(ping); ws && ws.close() }
  }, [])
  return <LiveCtx.Provider value={state}>{children}</LiveCtx.Provider>
}

export const useLive = () => useContext(LiveCtx)
