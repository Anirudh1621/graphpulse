import { useCallback, useEffect, useState } from 'react'

const BASE = import.meta.env.VITE_API_BASE || ''

export async function api(path, opts) {
  const r = await fetch(BASE + path, opts)
  if (!r.ok) {
    let msg = `${r.status} ${r.statusText}`
    try { msg = (await r.json()).detail || msg } catch { /* not json */ }
    throw new Error(msg)
  }
  return r.json()
}

export function post(path, body) {
  return api(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
}

/** Fetch `path` (null = skip). Returns {data, error, loading, reload}. */
export function useApi(path, { refresh } = {}) {
  const [state, setState] = useState({ data: null, error: null, loading: !!path })
  const [nonce, setNonce] = useState(0)
  const reload = useCallback(() => setNonce((n) => n + 1), [])

  useEffect(() => {
    if (!path) return
    let alive = true
    setState((s) => ({ ...s, loading: true }))
    api(path)
      .then((data) => alive && setState({ data, error: null, loading: false }))
      .catch((e) => alive && setState({ data: null, error: e.message, loading: false }))
    return () => { alive = false }
  }, [path, nonce])

  useEffect(() => {
    if (!refresh) return
    const t = setInterval(reload, refresh)
    return () => clearInterval(t)
  }, [refresh, reload])

  return { ...state, reload }
}

export function wsUrl() {
  if (BASE) return BASE.replace(/^http/, 'ws') + '/ws/live'
  return `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws/live`
}
