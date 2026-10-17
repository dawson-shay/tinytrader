// The console's one cache for what it reads from services. Every panel reads
// through here, so panels asking the same thing at once share one request,
// and an answer fetched a moment ago is handed out again instead of fetched.
//
// `fresh` is how long an answer may be handed out again (prices: a second or
// two, just enough to share; what a chain lists: hours). `keep: true` also
// keeps it in the browser across reloads, for what changes rarely (a chain's
// expiries and strikes), and `peek` hands that out at once, before any
// request, so a panel can lay itself out while the fresh answer comes.

/** @typedef {{ at: number, v: any }} Kept */
/** @type {Map<string, Kept>} */
const mem = new Map();
/** @type {Map<string, Promise<any>>} */
const flying = new Map();
const PREFIX = 'tt-cache:';
/** At most this many answers are kept in the browser; the oldest go first. */
const KEEP = 40;
const MEM = 300;

const stored = url => { try { const v = JSON.parse(localStorage.getItem(PREFIX + url) || 'null'); return v && typeof v.at === 'number' ? v : null; } catch (_) { return null; } };
function store(url, k) {
  try {
    localStorage.setItem(PREFIX + url, JSON.stringify(k));
    const all = [];
    for (let i = 0; i < localStorage.length; i++) { const key = localStorage.key(i); if (key?.startsWith(PREFIX)) all.push(key); }
    if (all.length > KEEP) {
      const at = key => { try { return JSON.parse(localStorage.getItem(key) || '{}').at || 0; } catch (_) { return 0; } };
      all.sort((a, b) => at(a) - at(b)).slice(0, all.length - KEEP).forEach(key => localStorage.removeItem(key));
    }
  } catch (_) { /* full, or private mode: kept in memory only */ }
}

/** A service's on-demand stream's URL: `stream('alpaca-data', 'chain', { underlying: 'SPY' })`. */
export function stream(service, name, query = {}) {
  const q = new URLSearchParams(Object.entries(query).filter(([, v]) => v != null && v !== '').map(([k, v]) => [k, String(v)]));
  return `/api/stream/${encodeURIComponent(service)}/${name}${[...q].length ? `?${q}` : ''}`;
}

/** Drops what's kept (and asked for) at URLs starting with `prefix`: after a write to them. @param {string} prefix */
export function forget(prefix) {
  for (const k of [...mem.keys()]) if (k.startsWith(prefix)) mem.delete(k);
  for (const k of [...flying.keys()]) if (k.startsWith(prefix)) flying.delete(k);
}

/** What's kept for `url`, however old (`{ at, v }`), or null: for drawing at once. */
export function peek(url) {
  const m = mem.get(url);
  if (m) return m;
  const s = stored(url);
  if (s) mem.set(url, s);
  return s;
}

/**
 * `url`'s JSON: kept if younger than `fresh` ms, else fetched (once, however
 * many ask meanwhile). A refusal throws its `error`.
 * @param {string} url
 * @param {{ fresh?: number, keep?: boolean }} [o]
 */
export async function get(url, { fresh = 1000, keep = false } = {}) {
  const k = mem.get(url) || (keep ? peek(url) : null);
  if (k && Date.now() - k.at < fresh) return k.v;
  const going = flying.get(url);
  if (going) return going;
  const p = (async () => {
    const r = await fetch(url, { cache: 'no-store' });
    const v = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(v?.error || `${r.status}`);
    const kept = { at: Date.now(), v };
    mem.delete(url); mem.set(url, kept);
    if (mem.size > MEM) mem.delete(mem.keys().next().value);
    if (keep) store(url, kept);
    return v;
  })().finally(() => flying.delete(url));
  flying.set(url, p);
  return p;
}
