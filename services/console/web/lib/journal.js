// The trading journal (services/console/src/journal.rs): entries for days,
// read through the console's shared cache, and written with a request each.
// Every write drops what the cache kept for the journal, so the next read
// shows it.
import { get, forget } from './cache.js';

const URL = '/api/journal';
/** The tags offered by default; a note can carry any other too. */
export const DEFAULT_TAGS = ['oversized', 'held to zero', 'revenge', 'apathetic', 'forcing it', 'followed plan', 'cut early'];

/** Entries whose day is in `from..to` (either end open), newest first.
 * @param {string} [from] @param {string} [to] @returns {Promise<JournalEntry[]>} */
export async function entries(from = '', to = '') {
  const q = [from && `from=${from}`, to && `to=${to}`].filter(Boolean).join('&');
  return (await get(`${URL}${q ? `?${q}` : ''}`, { fresh: 3000 })).entries || [];
}

/** @param {string} method @param {string} path @param {object} [body] */
async function write(method, path, body) {
  const r = await fetch(path, {
    method, cache: 'no-store',
    headers: body ? { 'content-type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  const v = await r.json().catch(() => ({}));
  forget(URL);
  if (!r.ok) throw new Error(v?.error || `${r.status}`);
  return v;
}

/** A new entry: `{ day, text, tags, rating, account }` (account null: all accounts). @param {object} body */
export const add = body => write('POST', URL, body);
/** Change an entry's fields. @param {string} id @param {object} body */
export const change = (id, body) => write('PUT', `${URL}/${encodeURIComponent(id)}`, body);
/** Remove an entry. @param {string} id */
export const remove = id => write('DELETE', `${URL}/${encodeURIComponent(id)}`);
