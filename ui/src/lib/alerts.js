// Spark Structured Streaming re-sends a follow-burst alert for every 30 s sliding window, and again each
// time a window's count grows, for as long as a burst lasts. mergeAlert folds them into one row per account.
// The numbers shown (follows, distinct accounts, severity) all come from the peak window, so they describe
// the same 30 seconds; window_start is the newest window, so a burst that is still going stays on top.
// Works for raw alerts (from Kafka or HBase) and for rows it has already merged.
export function mergeAlert(prev, a) {
  const follows = a.peak ?? a.follows
  const atPeak = !prev || follows > prev.peak
  return {
    ...(atPeak ? a : prev),
    peak: atPeak ? follows : prev.peak,
    windows: [...new Set([...(prev?.windows || []), ...(a.windows || [a.window_start])])],
    window_start: Math.max(a.window_start, prev?.window_start ?? a.window_start),
  }
}
