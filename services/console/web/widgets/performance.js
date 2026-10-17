// Performance: one account's trading P/L (the live one until another is
// picked, kept with the panel). A day's trades as a heatmap, a cell each,
// green or red in four steps by how much it made or lost, as the Data view
// shades days by size; then a month calendar of each day's P/L and trades
// (this month, paged back with ‹). Clicking a day shows its trades.
// Today's P/L, not in the history until the day is over, is the account's
// equity against yesterday's close, as its account service reads it.
// A trade is a round trip, flat to flat in one underlying and expiry (or one
// stock), rebuilt from the account's fills by its account service; its P/L
// is before fees. A day's P/L is the change in the account's equity
// (Alpaca's portfolio history) less deposits and withdrawals, so it includes
// fees and positions still open.
// The trading journal (lib/journal.js): notes on a day, each with its time,
// text, tags, a discipline rating and the account it refers to. A day with
// notes is marked on the calendar; the notes of the picked day sit under it,
// and a list of every note (filterable by tag, account and date) below.
import { defineWidget, paint } from '../lib/widgets.js';
import { svcState, serviceFor, servicesFor } from '../lib/fleet.js';
import { esc } from '../lib/format.js';
import { occLabel } from '../lib/ticket.js';
import { get } from '../lib/cache.js';
import { publish } from '../lib/channels.js';
import { entries, add, change as changeNote, remove, DEFAULT_TAGS } from '../lib/journal.js';

const service = () => serviceFor('performance') || null;
const nyToday = () => new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
const money = v => v == null ? '—' : `${v < 0 ? '−' : v > 0 ? '+' : ''}$${Math.abs(v).toLocaleString(undefined, { maximumFractionDigits: Math.abs(v) >= 1000 ? 0 : 2 })}`;
const short = d => new Date(d + 'T12:00:00Z').toLocaleDateString(undefined, { timeZone: 'UTC', month: 'short', day: 'numeric' });
const hm = t => new Date(t).toLocaleTimeString('en-GB', { timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit' });
const tone = v => v > 0 ? 'up' : v < 0 ? 'down' : 'flat';
/** Four steps of strength, from a value's share of the largest. */
const level = (v, max) => !v || !max ? 0 : 1 + Math.min(3, Math.floor(Math.abs(v) / max * 4 - 1e-9));
/** Notes by their day. @param {JournalEntry[]} list */
const notesByDay = list => {
  const m = new Map();
  for (const e of list) (m.get(e.day) || m.set(e.day, []).get(e.day)).push(e);
  return m;
};

function accounts() {
  for (const s of servicesFor('accounts')) {
    const list = svcState(s.id).accounts?.accounts || [];
    if (list.length) return list.map(a => ({ name: a.name, label: a.label || a.name, live: a.environment === 'live' }));
  }
  return [];
}

/** The account's days, with today's so far (equity against yesterday's close). */
function dayList(my) {
  const list = my.perf?.days || [], today = nyToday();
  if (list.some(d => d.date === today)) return list;
  for (const s of servicesFor('accounts')) {
    const a = (svcState(s.id).accounts?.accounts || []).find(x => x.name === my.acct)?.account;
    if (a?.equity && a?.last_equity) return [...list, { date: today, equity: a.equity, pnl: a.equity - a.last_equity, pnl_pct: (a.equity / a.last_equity - 1) * 100 }];
  }
  return list;
}

// The day's trades: a cell each, in the order they were opened.
function tradesView(my) {
  const t = my.trades;
  if (!t) return `<p class="muted">${my.tErr ? esc(my.tErr) : 'Loading the trades…'}</p>`;
  const list = t.trades || [];
  const closed = list.filter(x => !x.open), total = closed.reduce((s, x) => s + x.pnl, 0), wins = closed.filter(x => x.pnl > 0).length;
  const max = Math.max(0, ...closed.map(x => Math.abs(x.pnl)));
  const cells = list.map((x, i) => {
    const what = [...new Set(x.contracts.map(occLabel))].join(', ');
    const tip = `${x.key} · ${hm(x.start)}–${hm(x.end)} · ${x.open ? 'open (or closing a position from before)' : money(x.pnl)} · ${x.fills} fills\n${what}`;
    return `<button type="button" class="pf-cell ${x.open ? 'open' : `${tone(x.pnl)} l${level(x.pnl, max)}`}${my.pick === i ? ' picked' : ''}" data-pf-trade="${i}" title="${esc(tip)}"><span>${x.open ? 'open' : esc(money(Math.round(x.pnl)))}</span></button>`;
  }).join('');
  const p = list[my.pick];
  const detail = p ? `<p class="pf-detail"><b>${esc(p.key)}</b> ${hm(p.start)}–${hm(p.end)} · ${p.open ? 'open' : `<b class="${tone(p.pnl)}">${esc(money(p.pnl))}</b>`} · ${p.fills} fills · <span class="mono">${esc(p.contracts.map(occLabel).join(', '))}</span></p>` : '';
  const head = `<div class="pf-head"><b>Trades · ${esc(short(t.date))}${t.date === nyToday() ? ' (today)' : ''}</b>
    <span class="muted">${list.length ? `${closed.length} closed${list.length > closed.length ? `, ${list.length - closed.length} open` : ''} · ${wins} won · <b class="${tone(total)}">${esc(money(total))}</b> realized, before fees` : 'No trades'}</span></div>`;
  return `${head}${list.length ? `<div class="pf-cells">${cells}</div>
    <div class="cal-legend"><span>loss</span>${[4, 3, 2, 1].map(l => `<i class="pf-key down l${l}"></i>`).join('')}${[1, 2, 3, 4].map(l => `<i class="pf-key up l${l}"></i>`).join('')}<span>gain</span><span><i class="pf-key open"></i>open</span></div>${detail}` : ''}`;
}

// A month of days: each day's P/L and trades, tinted by its P/L.
function monthView(my) {
  const v = my.perf; if (!v) return `<p class="muted">${my.pErr ? esc(my.pErr) : 'Loading the days…'}</p>`;
  const byDay = new Map(dayList(my).map(d => [d.date, d])), counts = v.trades || {}, notes = notesByDay(my.jMonth || []);
  const [y, m] = my.month.split('-').map(Number);
  const first = new Date(Date.UTC(y, m - 1, 1)), days = new Date(Date.UTC(y, m, 0)).getUTCDate();
  const inMonth = [...byDay.values()].filter(d => d.date.startsWith(my.month) && d.pnl != null);
  const max = Math.max(0, ...inMonth.map(d => Math.abs(d.pnl)));
  const sum = inMonth.reduce((s, d) => s + d.pnl, 0);
  const tc = Object.entries(counts).filter(([d]) => d.startsWith(my.month)).reduce((a, [, c]) => ({ closed: a.closed + c.closed, wins: a.wins + c.wins }), { closed: 0, wins: 0 });
  const today = nyToday();
  let cells = '';
  for (let i = 0; i < (first.getUTCDay() + 6) % 7; i++) cells += '<div class="pf-day blank"></div>';
  for (let d = 1; d <= days; d++) {
    const k = `${my.month}-${String(d).padStart(2, '0')}`, x = byDay.get(k), c = counts[k], wd = new Date(Date.UTC(y, m - 1, d)).getUTCDay();
    const nn = notes.get(k)?.length || 0;
    const cls = ['pf-day', wd === 0 || wd === 6 ? 'weekend' : '', x?.pnl ? `${tone(x.pnl)} l${level(x.pnl, max)}` : '', k === my.day ? 'picked' : '', k === today ? 'today' : '', nn ? 'noted' : ''].filter(Boolean).join(' ');
    cells += `<button type="button" class="${cls}" data-pf-day="${k}"${x || c || nn ? '' : ' disabled'}><span class="n">${d}</span>${x?.pnl != null ? `<b>${esc(money(Math.round(x.pnl)))}</b>` : ''}${c?.trades ? `<small>${c.trades} trade${c.trades > 1 ? 's' : ''}${c.closed ? ` · ${c.wins}/${c.closed} won` : ''}</small>` : ''}${x?.transfers ? `<small>${x.transfers > 0 ? 'deposit' : 'withdrawal'} $${Math.abs(x.transfers).toLocaleString()}</small>` : ''}${nn ? `<i class="pf-dot" title="${nn} note${nn > 1 ? 's' : ''}">${nn}</i>` : ''}</button>`;
  }
  const name = first.toLocaleDateString(undefined, { timeZone: 'UTC', month: 'long', year: 'numeric' });
  return `<div class="pf-head"><span class="pf-nav"><button type="button" class="tile-btn" data-pf-month="-1" aria-label="Previous month">‹</button><b>${esc(name)}</b><button type="button" class="tile-btn" data-pf-month="1" aria-label="Next month">›</button></span>
      <span class="muted">${inMonth.length ? `<b class="${tone(sum)}">${esc(money(sum))}</b> over ${inMonth.length} day${inMonth.length > 1 ? 's' : ''}${tc.closed ? ` · ${tc.closed} trades, ${Math.round(tc.wins / tc.closed * 100)}% won` : ''}` : 'Nothing this month'}</span></div>
    <div class="pf-week">${['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map(d => `<span>${d}</span>`).join('')}</div>
    <div class="pf-month">${cells}</div>`;
}

const acctName = e => e.account ? esc(e.account) : 'all accounts';

/** One note: its time, day, account, discipline, tags and text (escaped; its line breaks kept). */
function noteRow(e) {
  const stars = e.rating ? `<span class="pf-stars" title="Discipline ${e.rating} of 5">${'★'.repeat(e.rating)}${'☆'.repeat(5 - e.rating)}</span>` : '';
  return `<article class="pf-note-row" data-note="${esc(e.id)}">
    <div class="pf-note-head"><span class="mono muted">${esc(hm(e.at))}</span> <b>${esc(short(e.day))}</b> <span class="muted">${acctName(e)}</span> ${stars}
      <span class="pf-note-act"><button type="button" class="linkish" data-note-edit="${esc(e.id)}">Edit</button> <button type="button" class="linkish" data-note-del="${esc(e.id)}">Delete</button></span></div>
    ${e.tags.length ? `<div class="pf-tagrow">${e.tags.map(t => `<span class="pf-tag">${esc(t)}</span>`).join('')}</div>` : ''}
    <p class="pf-note-text">${esc(e.text)}</p></article>`;
}

/** The notes of the picked day. */
function noteList(my) {
  const list = (my.jMonth || []).filter(e => e.day === my.day);
  const head = `<div class="pf-head"><b>Notes · ${esc(short(my.day))}</b> <button type="button" class="linkish" data-note-new>+ Note</button></div>`;
  if (my.jErr) return `${head}<p class="muted">${esc(my.jErr)}</p>`;
  return head + (list.length ? list.map(noteRow).join('') : '<p class="muted">No notes on this day yet.</p>');
}

/** Every note in the filter's range, with its tag and account applied. */
function notesList(my) {
  const f = my.filter;
  const rows = my.jAll.filter(e => (!f.tag || e.tags.includes(f.tag)) && (!f.account || (f.account === 'all' ? !e.account : e.account === f.account)));
  return rows.length ? rows.map(noteRow).join('') : '<p class="muted">No notes match.</p>';
}

/** The form to write or change a note: its text, tags (the defaults and any added), discipline and account. */
function noteForm(my) {
  const f = my.form, tags = [...new Set([...DEFAULT_TAGS, ...f.tags])];
  return `<form class="pf-nform" data-note-form>
    <div class="pf-head"><b>${f.id ? 'Edit note' : 'New note'} · ${esc(short(my.day))}</b>${f.err ? `<span class="pf-err">${esc(f.err)}</span>` : ''}</div>
    <textarea name="text" rows="3" maxlength="8192" placeholder="What happened, your state of mind, the mistakes" aria-label="Note">${esc(f.text)}</textarea>
    <div class="pf-tags">${tags.map(t => `<button type="button" class="chip-btn" data-note-tag="${esc(t)}" aria-pressed="${f.tags.includes(t)}">${esc(t)}</button>`).join('')}
      <input class="input" name="newtag" placeholder="another tag" maxlength="32" aria-label="Another tag"></div>
    <div class="pf-nrow">
      <label>Discipline <select name="rating"><option value="">none</option>${[1, 2, 3, 4, 5].map(r => `<option value="${r}"${f.rating === r ? ' selected' : ''}>${r}</option>`).join('')}</select></label>
      <label>Account <select name="account"><option value="">All accounts</option>${accounts().map(a => `<option value="${esc(a.name)}"${f.account === a.name ? ' selected' : ''}>${esc(a.label)}</option>`).join('')}</select></label>
      <button type="submit" class="btn">${f.id ? 'Save' : 'Add note'}</button>${f.id ? '<button type="button" class="btn" data-note-cancel>Cancel</button>' : ''}
    </div></form>`;
}

/** The list's filter: tag, account and the date range (open ends are empty). */
function filterBar(my) {
  const f = my.filter, tags = [...new Set([...DEFAULT_TAGS, ...my.jAll.flatMap(e => e.tags)])].sort();
  return `<form class="pf-jfilter-bar" data-jfilter>
    <label>Tag <select name="tag"><option value="">any</option>${tags.map(t => `<option${f.tag === t ? ' selected' : ''}>${esc(t)}</option>`).join('')}</select></label>
    <label>Account <select name="account"><option value="">any</option><option value="all"${f.account === 'all' ? ' selected' : ''}>all accounts</option>${accounts().map(a => `<option value="${esc(a.name)}"${f.account === a.name ? ' selected' : ''}>${esc(a.label)}</option>`).join('')}</select></label>
    <label>From <input type="date" name="from" value="${esc(f.from)}"></label>
    <label>To <input type="date" name="to" value="${esc(f.to)}"></label>
  </form>`;
}

defineWidget('performance', {
  title: 'Performance',
  mount(el) {
    const my = { acct: '', day: '', month: nyToday().slice(0, 7), perf: /** @type {any} */ (null), trades: /** @type {any} */ (null), pErr: '', tErr: '', pick: -1, pAt: 0, tAt: 0, alive: true,
      /** The journal: this month's notes, every note in the filter's range, and the note being written or changed. */
      jMonth: /** @type {JournalEntry[]} */ ([]), jAll: /** @type {JournalEntry[]} */ ([]), jErr: '', jAt: 0, form: /** @type {any} */ (null), filter: { tag: '', account: '', from: '', to: '' } };
    try { my.acct = JSON.parse(el.dataset.args || '{}').account || ''; } catch (_) {}
    el.innerHTML = '<div class="pf"><div class="pf-top"></div><section class="pf-sec pf-trades"></section><section class="pf-sec pf-cal"></section><section class="pf-sec pf-note"><div class="pf-nform-box"></div><div class="pf-nday"></div></section><section class="pf-sec pf-jrnl"><h3 class="pf-jtitle">Journal</h3><div class="pf-jfilter"></div><div class="pf-jlist"></div></section></div>';
    const $ = s => /** @type {HTMLElement} */ (el.querySelector(s));
    const ask = (what, q) => { const svc = service(); return svc ? get(`/api/stream/${encodeURIComponent(svc)}/${what}?${q}`, { fresh: 5000 }) : Promise.reject(new Error('No service serves account performance (a `performance` stream).')); };
    const draw = () => {
      const list = accounts();
      if (!list.some(a => a.name === my.acct)) my.acct = (list.find(a => a.live) || list[0])?.name || '';
      paint($('.pf-top'), (list.length ? `<select class="acct-pick pf-acct" aria-label="Account">${list.map(a => `<option value="${esc(a.name)}"${a.name === my.acct ? ' selected' : ''}>${esc(a.label)}</option>`).join('')}</select>` : '<p class="muted">No account service is running.</p>') + '<button type="button" class="linkish" data-pf-today>Today</button>');
      paint($('.pf-nday'), noteList(my)); paint($('.pf-jlist'), notesList(my));
      if (!service()) { paint($('.pf-trades'), '<p class="muted">No service serves account performance (a <code>performance</code> stream).</p>'); return; }
      paint($('.pf-trades'), tradesView(my)); paint($('.pf-cal'), monthView(my));
    };
    async function loadPerf() {
      if (!my.acct || !service()) return;
      const acct = my.acct; my.pAt = Date.now();
      try { const v = await ask('performance', `account=${encodeURIComponent(acct)}`); if (acct !== my.acct) return; my.perf = v; my.pErr = ''; }
      catch (e) { my.pErr = `Couldn't read the account's history: ${e.message}`; }
      // The day to show first: today, else the latest with trades.
      if (!my.day) { const t = Object.keys(my.perf?.trades || {}).sort(); my.day = t.includes(nyToday()) || !t.length ? nyToday() : t[t.length - 1]; my.month = my.day.slice(0, 7); publish('day', my.day); loadTrades(); loadJournal(); }
      if (my.alive) draw();
    }
    async function loadTrades() {
      if (!my.acct || !my.day || !service()) return;
      const acct = my.acct, day = my.day; my.tAt = Date.now();
      try { const v = await ask('trades', `account=${encodeURIComponent(acct)}&date=${day}`); if (acct !== my.acct || day !== my.day) return; my.trades = v; my.tErr = ''; }
      catch (e) { my.tErr = `Couldn't read the trades: ${e.message}`; }
      if (my.alive) draw();
    }
    // The journal's notes for the month shown; the list is read as its filter says.
    async function loadJournal() {
      const month = my.month; my.jAt = Date.now();
      try { const v = await entries(`${month}-01`, `${month}-31`); if (month !== my.month) return; my.jMonth = v; my.jErr = ''; }
      catch (e) { my.jErr = `Couldn't read the journal: ${e.message}`; }
      if (my.alive) draw();
    }
    async function loadAll() {
      const f = my.filter;
      try { my.jAll = await entries(f.from, f.to); my.jErr = ''; }
      catch (e) { my.jErr = `Couldn't read the journal: ${e.message}`; }
      if (my.alive) { paint($('.pf-jfilter'), filterBar(my)); paint($('.pf-jlist'), notesList(my)); draw(); }
    }
    // The form's text, tags, discipline and account as they are on screen, before anything repaints it.
    const syncForm = () => {
      const f = $('.pf-nform'); if (!f || !my.form) return;
      my.form.text = /** @type {HTMLTextAreaElement} */ (f.querySelector('textarea')).value;
      const r = /** @type {HTMLSelectElement} */ (f.querySelector('[name=rating]')).value, a = /** @type {HTMLSelectElement} */ (f.querySelector('[name=account]')).value;
      my.form.rating = r ? +r : null; my.form.account = a || null;
    };
    const drawForm = () => { paint($('.pf-nform-box'), my.form ? noteForm(my) : ''); };
    const openForm = (e = null) => {
      syncForm();
      my.form = e
        ? { id: e.id, text: e.text, tags: [...e.tags], rating: e.rating, account: e.account, err: '' }
        : { id: null, text: '', tags: [], rating: null, account: my.acct || null, err: '' };
      drawForm();
      $('.pf-nform textarea')?.focus();
    };
    const showDay = d => { my.day = d; my.month = d.slice(0, 7); my.trades = null; my.pick = -1; publish('day', d); draw(); loadTrades(); loadJournal(); };
    const click = e => {
      const b = /** @type {HTMLElement} */ (e.target).closest('[data-pf-day], [data-pf-trade], [data-pf-month]'); if (!b) return;
      const d = /** @type {HTMLElement} */ (b).dataset;
      if (d.pfDay) showDay(d.pfDay);
      else if (d.pfTrade != null) { my.pick = my.pick === +d.pfTrade ? -1 : +d.pfTrade; draw(); }
      else if (d.pfMonth) { const [y, m] = my.month.split('-').map(Number), t = new Date(Date.UTC(y, m - 1 + +d.pfMonth, 1)); my.month = t.toISOString().slice(0, 7); draw(); loadJournal(); }
    };
    // Today's button; a note's edit, delete, and the form's tags and cancel.
    const noteClick = e => {
      const b = /** @type {HTMLElement} */ (e.target).closest('[data-pf-today], [data-note-new], [data-note-edit], [data-note-del], [data-note-tag], [data-note-cancel]'); if (!b) return;
      const d = /** @type {HTMLElement} */ (b).dataset;
      if (b.hasAttribute('data-pf-today')) showDay(nyToday());
      else if (b.hasAttribute('data-note-new')) openForm();
      else if (d.noteEdit != null) { const x = [...my.jMonth, ...my.jAll].find(n => n.id === d.noteEdit); if (x) { if (x.day !== my.day) showDay(x.day); openForm(x); } }
      else if (d.noteDel != null) {
        if (!confirm('Delete this note?')) return;
        remove(d.noteDel).then(() => { if (my.form?.id === d.noteDel) { my.form = null; drawForm(); } loadJournal(); loadAll(); })
          .catch(err => { my.jErr = `Couldn't delete the note: ${err.message}`; draw(); });
      }
      else if (d.noteTag != null) { syncForm(); const t = d.noteTag, f = my.form; f.tags = f.tags.includes(t) ? f.tags.filter(x => x !== t) : [...f.tags, t]; drawForm(); }
      else if (b.hasAttribute('data-note-cancel')) { my.form = null; drawForm(); }
    };
    // A note's form: saved, as a change or a new note on the picked day.
    const noteSubmit = e => {
      if (!/** @type {HTMLElement} */ (e.target).matches('[data-note-form]')) return;
      e.preventDefault(); syncForm();
      const f = my.form, body = { day: my.day, text: f.text, tags: f.tags, rating: f.rating, account: f.account };
      if (!f.text.trim()) { f.err = 'Write something first.'; drawForm(); return; }
      (f.id ? changeNote(f.id, body) : add(body))
        .then(() => { my.form = null; drawForm(); loadJournal(); loadAll(); })
        .catch(err => { f.err = err.message; drawForm(); });
    };
    // The list's filter; the form's new tag (Enter or leaving the box adds it).
    const noteChange = e => {
      const t = /** @type {HTMLInputElement} */ (e.target);
      if (t.name === 'newtag') { const v = t.value.trim(); if (!v) return; syncForm(); if (!my.form.tags.includes(v)) my.form.tags.push(v); drawForm(); $('.pf-nform [name=newtag]')?.focus(); return; }
      if (!t.closest('[data-jfilter]')) return;
      const bar = /** @type {HTMLElement} */ (t.closest('[data-jfilter]'));
      const v = name => /** @type {HTMLInputElement | HTMLSelectElement} */ (bar.querySelector(`[name=${name}]`)).value;
      my.filter = { tag: v('tag'), account: v('account'), from: v('from'), to: v('to') };
      loadAll();
    };
    const change = e => {
      const t = /** @type {HTMLSelectElement} */ (e.target); if (!t.classList.contains('pf-acct')) return;
      my.acct = t.value; my.perf = null; my.trades = null; my.day = ''; my.pick = -1; t.blur();
      el.dispatchEvent(new CustomEvent('tt-tab-args', { bubbles: true, detail: { args: { account: my.acct } } }));
      draw(); loadPerf();
    };
    el.addEventListener('click', click); el.addEventListener('change', change);
    el.addEventListener('click', noteClick); el.addEventListener('change', noteChange); el.addEventListener('submit', noteSubmit);
    draw(); loadPerf(); loadJournal(); loadAll();
    return {
      // Today's trades every half minute; the days every five minutes.
      update() {
        if (!my.perf && !my.pErr && Date.now() - my.pAt > 3000) loadPerf();
        else if (Date.now() - my.pAt > 300e3) loadPerf();
        if (my.day === nyToday() && Date.now() - my.tAt > 30e3) loadTrades();
        // Notes are kept on every device: read them again every half minute.
        if (Date.now() - my.jAt > 30e3 && !my.form) { loadJournal(); loadAll(); }
        draw();
      },
      dispose() { my.alive = false; el.removeEventListener('click', click); el.removeEventListener('change', change); el.removeEventListener('click', noteClick); el.removeEventListener('change', noteChange); el.removeEventListener('submit', noteSubmit); },
    };
  },
});
