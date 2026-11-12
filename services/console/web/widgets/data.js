// Data: what the store holds, as a tree (dataset, symbol, schema), merged
// across drives. Each level shows its size and which drives hold it, and
// where on each drive. Search narrows the tree; picking a schema shows its
// days as a coverage grid, shaded by size and coloured by drive. A drive that
// isn't there shows what it held when last read, as away.
import { defineWidget } from '../lib/widgets.js';
import { catalogRoots, catalogStamp } from '../lib/fleet.js';
import { esc, bytes } from '../lib/format.js';

// Datasets merged across drives: dataset -> symbol -> schema, with bytes,
// days and the drive of each day, and each drive's path to it.
function tree() {
  const ds = new Map();
  const roots = catalogRoots();
  const symOf = (id, symbol) => {
    const set = ds.get(id) || ds.set(id, { id, syms: new Map() }).get(id);
    const key = symbol || '';
    return set.syms.get(key) || set.syms.set(key, { symbol, schemas: {} }).get(key);
  };
  const schemaOf = (sym, name) => sym.schemas[name] || (sym.schemas[name] = { bytes: 0, days: {}, where: {}, byDrive: {}, away: [], paths: [] });
  for (const r of roots) for (const [k, schemas] of Object.entries(r.present ? {} : r.held || {})) {
    // `provider/id/symbol`, `-` for none.
    const [, id, ...rest] = k.split('/'), symbol = rest.join('/');
    const sym = symOf(id, symbol === '-' ? undefined : symbol);
    for (const [name, h] of Object.entries(schemas)) {
      const t = schemaOf(sym, name);
      t.bytes += h.bytes || 0; t.byDrive[r.name] = (t.byDrive[r.name] || 0) + (h.bytes || 0);
      t.away.push({ drive: r.name, ...h });
    }
  }
  for (const r of roots) for (const d of r.datasets || []) {
    const sym = symOf(d.id, d.symbol);
    // `store: <path>` (the store's layout: `<symbol>/<schema>/` under it),
    // `files in <path>` (files as found, in that folder), or `databento batch <job>`.
    const srcs = (d.sources || []).map(src => {
      const store = src.startsWith('store:'), path = src.replace(/^(store:|files in)\s*/, '');
      let rel = path.startsWith(r.path) ? path.slice(r.path.length).replace(/^\//, '') : path;
      if (store && d.symbol && !rel.split('/').includes(d.symbol)) rel += '/' + d.symbol;
      return { drive: r.name, path: rel, store };
    });
    for (const [name, s] of Object.entries(d.schemas)) {
      const t = schemaOf(sym, name);
      t.paths.push(...srcs);
      t.bytes += s.bytes; t.byDrive[r.name] = (t.byDrive[r.name] || 0) + s.bytes;
      for (const [day, b] of Object.entries(s.days || {})) { t.days[day] = (t.days[day] || 0) + b; (t.where[day] ||= []).push(r.name); }
    }
  }
  const sum = (list, f) => list.reduce((a, x) => a + f(x), 0);
  const byDrive = list => { const o = {}; for (const x of list) for (const [k, v] of Object.entries(x)) o[k] = (o[k] || 0) + v; return o; };
  return [...ds.values()].sort((a, b) => a.id.localeCompare(b.id)).map(set => {
    const syms = [...set.syms.values()].sort((a, b) => (a.symbol || '').localeCompare(b.symbol || '')).map(sym => {
      const schemas = Object.entries(sym.schemas).sort((a, b) => b[1].bytes - a[1].bytes);
      return { ...sym, list: schemas, bytes: sum(schemas, ([, s]) => s.bytes), byDrive: byDrive(schemas.map(([, s]) => s.byDrive)) };
    });
    return { id: set.id, syms, bytes: sum(syms, s => s.bytes), byDrive: byDrive(syms.map(s => s.byDrive)) };
  });
}

// Drives not there now: what they held shows as away.
const awayDrives = () => new Set(catalogRoots().filter(r => !r.present).map(r => r.name));
const drivesOf = o => { const away = awayDrives(); return Object.entries(o).sort((a, b) => b[1] - a[1]).map(([n, b]) => `<span class="dv-drive${away.has(n) ? ' away' : ''}" title="${esc(bytes(b))} on ${esc(n)}${away.has(n) ? ' (not mounted)' : ''}">${esc(n)}${away.has(n) ? ' · away' : ''}</span>`).join(''); };
// Days held: those seen, or, on a drive that's away, as many as it held.
const dayCount = s => Math.max(Object.keys(s.days).length, ...s.away.map(a => a.days || 0));

function coverage(name, sym, s, drives) {
  const awayNote = s.away.map(a => `<p class="muted" style="margin:0;font-size:13px">On <b>${esc(a.drive)}</b>, which is not mounted: ${(a.days || 0).toLocaleString()} days, ${esc(a.first || '?')} → ${esc(a.last || '?')}, ${bytes(a.bytes)} when last read.</p>`).join('');
  const keys = Object.keys(s.days).sort(), first = keys[0], last = keys[keys.length - 1];
  if (!keys.length) return `<section class="panel dv-cov">
    <div class="d-head"><b class="mono">${esc(sym.symbol || '')} ${esc(name)}</b><span class="muted">${bytes(s.bytes)}</span></div>${awayNote}</section>`;
  const missing = [];
  for (let t = Date.parse(first + 'T12:00:00Z'); t <= Date.parse(last + 'T12:00:00Z'); t += 864e5) {
    const d = new Date(t), k = d.toISOString().slice(0, 10), wd = d.getUTCDay();
    if (wd !== 0 && wd !== 6 && !s.days[k]) missing.push(k);
  }
  // Shade by size, in four steps between the smallest and largest day.
  const sizes = Object.values(s.days), lo = Math.min(...sizes), hi = Math.max(...sizes);
  const level = b => hi === lo ? 4 : 1 + Math.min(3, Math.floor((b - lo) / (hi - lo) * 4));
  const multi = drives.length > 1;
  let cal = '';
  const start = Date.parse(first + 'T00:00:00Z') - ((new Date(first + 'T00:00:00Z').getUTCDay() + 6) % 7) * 864e5;
  for (let t = start; t <= Date.parse(last + 'T00:00:00Z') + 6 * 864e5; t += 864e5) {
    const k = new Date(t).toISOString().slice(0, 10), wd = new Date(t).getUTCDay();
    const has = s.days[k], out = k < first || k > last;
    const drive = has ? s.where[k][0] : null;
    const cls = out ? 'out' : has ? `has l${level(has)}${multi && drives.indexOf(drive) === 1 ? ' d2' : ''}` : (wd === 0 || wd === 6) ? 'weekend' : '';
    cal += `<i class="${cls}" title="${k}${has ? ` · ${bytes(has)} · ${s.where[k].join(', ')}` : cls === '' ? ' · no data' : ''}"></i>`;
  }
  const paths = s.paths.map(p => `<div><span class="dv-drive">${esc(p.drive)}</span><span class="mono">${esc(p.path)}/${p.store ? esc(name) + '/' : ''}</span></div>`).join('');
  return `<section class="panel dv-cov">
    <div class="d-head"><b class="mono">${esc(sym.symbol || '')} ${esc(name)}</b><span class="muted">${bytes(s.bytes)} · ${keys.length} days · ${first} → ${last}</span></div>
    <div class="cal-wrap"><div class="cal" aria-label="Days with data">${cal}</div></div>
    <div class="cal-legend"><span>smaller</span>${[1, 2, 3, 4].map(l => `<i class="has l${l}"></i>`).join('')}<span>larger</span>
      ${multi ? drives.map((d, i) => `<span><i class="has l4${i === 1 ? ' d2' : ''}"></i>${esc(d)}</span>`).join('') : ''}
      ${missing.length ? `<span title="${esc(missing.join(', '))}">${missing.length} weekday${missing.length === 1 ? '' : 's'} without data (holidays included)</span>` : ''}</div>
    <div class="dv-paths">${paths}</div>${awayNote}
  </section>`;
}

function dataView(my) {
  const roots = catalogRoots();
  if (!roots.length) return '<p class="muted">The store is not reachable.</p>';
  const all = tree();
  const q = my.filter.toLowerCase().split(/\s+/).filter(Boolean);
  const hit = (...parts) => q.every(w => parts.join(' ').toLowerCase().includes(w));
  let rows = '', pick = null;
  for (const set of all) {
    const syms = set.syms.map(sym => ({ sym, list: sym.list.filter(([n]) => hit(set.id, sym.symbol || '', n)) })).filter(x => x.list.length);
    if (!syms.length) continue;
    const open = q.length || my.open.has(set.id);
    rows += `<div class="dv-row dv-l0" data-toggle="${esc(set.id)}"><span class="dv-tw">${open ? '▾' : '▸'}</span><span class="dv-name">${esc(set.id)}</span><span class="dv-size">${bytes(set.bytes)}</span><span class="dv-drives">${drivesOf(set.byDrive)}</span></div>`;
    if (!open) continue;
    for (const { sym, list } of syms) {
      const k1 = set.id + '/' + (sym.symbol || '');
      const open1 = q.length || my.open.has(k1) || set.syms.length === 1;
      if (sym.symbol) rows += `<div class="dv-row dv-l1" data-toggle="${esc(k1)}"><span class="dv-tw">${open1 ? '▾' : '▸'}</span><span class="dv-name">${esc(sym.symbol)}</span><span class="dv-size">${bytes(sym.bytes)}</span><span class="dv-drives">${drivesOf(sym.byDrive)}</span></div>`;
      if (!open1 && sym.symbol) continue;
      for (const [name, s] of list) {
        const key = `${set.id}|${sym.symbol || ''}|${name}`, sel = my.pick === key;
        if (sel) { const away = awayDrives(); pick = { name, sym, s, drives: Object.keys(s.byDrive).filter(d => !away.has(d)) }; }
        rows += `<div class="dv-row dv-l2${sel ? ' sel' : ''}" data-pick="${esc(key)}"><span class="dv-tw"></span><span class="dv-name">${esc(name)}</span><span class="dv-size">${bytes(s.bytes)} · ${dayCount(s)}d</span><span class="dv-drives">${drivesOf(s.byDrive)}</span></div>`;
      }
    }
  }
  return `<input class="ds-filter" data-ds-filter placeholder="Search: XSP, SPY.OPT, cmbp-1, ALPACA…" value="${esc(my.filter)}" autocomplete="off" spellcheck="false">
    <div class="dv-tree">${rows || '<p class="muted" style="margin:8px 0">Nothing matches.</p>'}</div>
    ${pick ? coverage(pick.name, pick.sym, pick.s, pick.drives) : '<p class="muted" style="margin:0;font-size:13px">Pick a schema to see its days.</p>'}`;
}

defineWidget('data', {
  title: 'Data',
  mount(el) {
    const my = { filter: '', open: new Set(), pick: null, stamp: null };
    // Redrawn when the catalog changes or on a pick, never under typing.
    const draw = () => {
      const typing = el.contains(document.activeElement) && document.activeElement.matches('[data-ds-filter]');
      const pos = typing ? /** @type {HTMLInputElement} */ (document.activeElement).selectionStart : 0, top = el.scrollTop;
      my.stamp = catalogStamp(); el.innerHTML = dataView(my); el.scrollTop = top;
      if (typing) { const i = /** @type {HTMLInputElement} */ (el.querySelector('[data-ds-filter]')); i.focus(); i.setSelectionRange(pos, pos); }
    };
    const click = e => {
      const t = e.target.closest('[data-toggle]');
      if (t) { const k = t.dataset.toggle; my.open.has(k) ? my.open.delete(k) : my.open.add(k); draw(); return; }
      const p = e.target.closest('[data-pick]');
      if (p) { my.pick = my.pick === p.dataset.pick ? null : p.dataset.pick; draw(); }
    };
    const input = e => { if (e.target.matches('[data-ds-filter]')) { my.filter = e.target.value; draw(); } };
    el.addEventListener('click', click); el.addEventListener('input', input);
    draw();
    return {
      update() { if (catalogStamp() !== my.stamp) draw(); },
      dispose() { el.removeEventListener('click', click); el.removeEventListener('input', input); },
    };
  },
});
