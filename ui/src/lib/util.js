// Display helpers. The SNAP dataset is anonymised (numeric ids only), so every account gets a
// deterministic, obviously-generated alias to make the UI readable.
const ADJ = ['swift', 'quiet', 'lunar', 'amber', 'crimson', 'silver', 'cosmic', 'velvet', 'neon', 'polar', 'misty', 'rapid',
  'golden', 'hidden', 'electric', 'frozen', 'wild', 'sonic', 'urban', 'pixel', 'stellar', 'brave', 'clever', 'jade']
const NOUN = ['otter', 'falcon', 'comet', 'maple', 'lynx', 'raven', 'tiger', 'orbit', 'harbor', 'ember', 'koala', 'nova',
  'panda', 'cipher', 'willow', 'fox', 'atlas', 'heron', 'quasar', 'bison', 'delta', 'sparrow', 'cobalt', 'drift']

function hash(n) {
  let h = 2166136261
  const s = String(n)
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619) }
  return h >>> 0
}

export function alias(id) {
  const h = hash(id)
  return `${ADJ[h % ADJ.length]}${NOUN[(h >>> 8) % NOUN.length]}${h % 97}`
}

export const PALETTE = ['#38bdf8', '#fbbf24', '#34d399', '#f472b6', '#a78bfa', '#fb923c', '#22d3ee', '#a3e635',
  '#f87171', '#60a5fa', '#e879f9', '#2dd4bf', '#fde68a', '#c084fc', '#86efac', '#fda4af']

// Largest communities get distinct palette slots (by size rank); the long tail falls back to a hash.
const RANK = new Map()
export function registerCommunities(list) {
  (list || []).forEach((c) => RANK.set(Number(c.community), c.size_rank))
}

export function commColor(c) {
  if (c == null) return '#64748b'
  const r = RANK.get(Number(c))
  return r && r <= PALETTE.length ? PALETTE[r - 1] : PALETTE[hash(c) % PALETTE.length]
}

/** Zoom a force graph to fit once, after its first layout settles. */
export function fitOnce(ref) {
  let done = false
  return () => { if (!done && ref.current) { done = true; ref.current.zoomToFit(500, 40) } }
}

export function suspColor(s) {
  if (s == null) return '#64748b'
  if (s >= 60) return '#f87171'
  if (s >= 35) return '#fbbf24'
  return '#34d399'
}

export const fmt = {
  int: (n) => (n == null ? '–' : Math.round(n).toLocaleString('en-US')),
  num: (n, d = 2) => (n == null ? '–' : Number(n).toFixed(d)),
  pct: (n, d = 1) => (n == null ? '–' : `${(n * 100).toFixed(d)}%`),
  sci: (n) => (n == null ? '–' : Number(n).toExponential(2)),
  compact: (n) => (n == null ? '–' : Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 }).format(n)),
  bytes: (b) => {
    if (b == null) return '–'
    const u = ['B', 'KB', 'MB', 'GB', 'TB']
    let i = 0
    while (b >= 1024 && i < u.length - 1) { b /= 1024; i++ }
    return `${b.toFixed(i ? 1 : 0)} ${u[i]}`
  },
  dur: (s) => (s == null ? '–' : s < 60 ? `${s.toFixed(1)}s` : `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`),
  ago: (ts) => {
    const s = Math.max(0, Date.now() / 1000 - ts)
    return s < 60 ? `${Math.round(s)}s ago` : s < 3600 ? `${Math.round(s / 60)}m ago` : `${Math.round(s / 3600)}h ago`
  },
}
