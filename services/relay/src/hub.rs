//! Fan-out from one provider connection to any number of local clients.
//!
//! The provider task publishes chunks of DBN records; each live client reads
//! them from a bounded broadcast queue. A client that falls behind is warned
//! at half the queue and disconnected when it overflows, so it never slows the
//! provider connection or other clients.
//!
//! The hub also keeps the latest reference records (instrument definitions
//! and statistics) and sends them to each client when it joins, followed by
//! a `ReplayCompleted` system record, so every client knows every instrument
//! without its own provider session. Market data itself is never cached.

use std::collections::{BTreeMap, HashMap};
use std::mem::{offset_of, size_of};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
pub use tinytrader_observe::lock;

use databento::dbn::{
    Cmbp1Msg, ErrorCode, InstrumentDefMsg, Mbp1Msg, Metadata, RecordHeader, StatMsg, SystemCode,
    UNDEF_PRICE, UNDEF_TIMESTAMP, rtype,
};
use tinytrader_observe::{Events, Series};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::broadcast;

use crate::protocol::{self, Join};

/// Records as the provider sent them, in order.
pub type Chunk = Arc<[u8]>;

/// How long a client has to send its join line.
const JOIN_TIMEOUT: Duration = Duration::from_secs(5);
/// Longest join line accepted.
const JOIN_MAX: u64 = 4096;

/// What happened to a write to a client.
enum Sent {
    Ok,
    /// The client went away.
    Closed,
    /// The client stopped reading: the write didn't finish in time.
    Stalled,
}

/// How long past its stamped expiration a definition is kept (see
/// `Reference::snapshot`).
const EXPIRY_GRACE_NS: u64 = 2 * 86_400 * 1_000_000_000;

/// The latest reference records, by instrument.
///
/// An instrument id names one contract only for a day: OPRA assigns them
/// afresh each trading day, so Friday's id for a contract is another
/// contract's (or none) on Monday. A definition is therefore the truth about
/// its id and its symbol: when one arrives, any other id holding the same
/// symbol is forgotten (its definition, statistics and quote), and an id that
/// now names another contract loses what it held for the old one. Without
/// that, a contract had two ids after a weekend, and `/quotes` showed
/// whichever came last, often Friday's close (2026-11-09).
#[derive(Default)]
struct Reference {
    /// Definition bytes and expiration, by instrument id.
    definitions: BTreeMap<u32, (u64, Vec<u8>)>,
    /// The id holding each symbol's definition.
    by_symbol: HashMap<String, u32>,
    /// Statistic bytes, by instrument id and statistic type.
    statistics: BTreeMap<(u32, u16), Vec<u8>>,
}

fn u16_at(b: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(b.get(at..at + 2)?.try_into().ok()?))
}
fn u32_at(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}
fn u64_at(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

/// A definition's symbol (its `raw_symbol`), if it has one.
fn def_symbol(def: &[u8]) -> Option<&str> {
    let at = offset_of!(InstrumentDefMsg, raw_symbol);
    let raw = def.get(at..at + 71)?;
    let end = raw.iter().position(|b| *b == 0).unwrap_or(raw.len());
    std::str::from_utf8(&raw[..end])
        .ok()
        .filter(|s| !s.is_empty())
}

impl Reference {
    /// Forgets an id's definition and statistics.
    fn forget(&mut self, id: u32) {
        if let Some((_, def)) = self.definitions.remove(&id)
            && let Some(sym) = def_symbol(&def)
            && self.by_symbol.get(sym) == Some(&id)
        {
            self.by_symbol.remove(sym);
        }
        self.statistics.retain(|(i, _), _| *i != id);
    }

    /// Keep the reference records in a chunk of DBN records. Returns the ids
    /// whose quotes no longer belong to them: another id now holds their
    /// symbol, or they now name another contract.
    fn absorb(&mut self, chunk: &[u8]) -> Vec<u32> {
        let mut stale = Vec::new();
        let mut at = 0;
        while at < chunk.len() {
            let len = usize::from(chunk[at]) * 4;
            let Some(rec) = chunk.get(at..at + len).filter(|r| r.len() >= 16) else {
                return stale;
            };
            at += len;
            let Some(id) = u32_at(rec, offset_of!(RecordHeader, instrument_id)) else {
                continue;
            };
            match rec[1] {
                rtype::INSTRUMENT_DEF if len >= size_of::<InstrumentDefMsg>() => {
                    let expires = u64_at(rec, offset_of!(InstrumentDefMsg, expiration))
                        .unwrap_or(UNDEF_TIMESTAMP);
                    let sym = def_symbol(rec).map(str::to_owned);
                    // This id named another contract (an earlier day's id).
                    let was = self
                        .definitions
                        .get(&id)
                        .and_then(|(_, d)| def_symbol(d).map(str::to_owned));
                    if was.is_some() && was != sym {
                        self.forget(id);
                        stale.push(id);
                    }
                    // Another id held this symbol (the contract's earlier id).
                    if let Some(sym) = &sym
                        && let Some(&other) = self.by_symbol.get(sym)
                        && other != id
                    {
                        self.forget(other);
                        stale.push(other);
                    }
                    if let Some(sym) = sym {
                        self.by_symbol.insert(sym, id);
                    }
                    self.definitions.insert(id, (expires, rec.to_vec()));
                }
                rtype::STATISTICS if len >= size_of::<StatMsg>() => {
                    let Some(kind) = u16_at(rec, offset_of!(StatMsg, stat_type)) else {
                        continue;
                    };
                    // update_action 2 deletes a statistic.
                    if rec.get(offset_of!(StatMsg, update_action)) == Some(&2) {
                        self.statistics.remove(&(id, kind));
                    } else {
                        self.statistics.insert((id, kind), rec.to_vec());
                    }
                }
                _ => {}
            }
        }
        stale
    }

    /// Every current definition, then every statistic, dropping instruments
    /// that have expired. Databento stamps an option's `expiration` at 00:00
    /// UTC of its expiry date (20:00 New York time the evening before), so a
    /// definition is kept for two days past that stamp: through its whole
    /// expiry day and the settlement statistics that follow.
    fn snapshot(&mut self, now: u64) -> Vec<u8> {
        self.definitions.retain(|_, (expires, _)| {
            *expires == UNDEF_TIMESTAMP || expires.saturating_add(EXPIRY_GRACE_NS) > now
        });
        let defs = &self.definitions;
        self.by_symbol.retain(|_, id| defs.contains_key(id));
        self.statistics.retain(|(id, _), _| defs.contains_key(id));
        let mut out = Vec::new();
        for (_, rec) in self.definitions.values() {
            out.extend_from_slice(rec);
        }
        for rec in self.statistics.values() {
            out.extend_from_slice(rec);
        }
        out
    }

    fn len(&self) -> usize {
        self.definitions.len() + self.statistics.len()
    }

    /// One kind of statistic (`stat_type`, such as 9 for open interest) for
    /// the instruments whose symbol `keep` accepts: (symbol, quantity, the
    /// day it refers to: `ts_ref`, or the record's `ts_event` when that is
    /// undefined, in nanoseconds).
    fn stats_matching(&self, kind: u16, keep: &impl Fn(&str) -> bool) -> Vec<(String, i64, u64)> {
        self.statistics
            .iter()
            .filter(|((_, k), _)| *k == kind)
            .filter_map(|((id, _), rec)| {
                let s = crate::quotes::compact(self.symbol(*id)?);
                keep(&s).then_some(())?;
                let q = i64::from_le_bytes(
                    rec.get(offset_of!(StatMsg, quantity)..)?
                        .get(..8)?
                        .try_into()
                        .ok()?,
                );
                // OPRA leaves `ts_ref` undefined for open interest: the record's own time then.
                let ts_ref =
                    u64_at(rec, offset_of!(StatMsg, ts_ref)).filter(|t| *t != UNDEF_TIMESTAMP);
                let ts = ts_ref
                    .or_else(|| u64_at(rec, offset_of!(RecordHeader, ts_event)))
                    .unwrap_or(0);
                Some((s, q, ts))
            })
            .collect()
    }

    /// The instrument's symbol, from its definition (borrowed: no copy per record).
    fn symbol(&self, id: u32) -> Option<&str> {
        def_symbol(&self.definitions.get(&id)?.1)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Upstream {
    /// Not connected yet since start.
    Starting,
    Up,
    /// Lost at this time (ns since the epoch).
    Down(u64),
    /// Deliberately disconnected outside active hours, until this time.
    Off(u64),
}

pub struct Hub {
    tx: broadcast::Sender<Chunk>,
    /// The same stream with trade conditions interleaved, for sources that
    /// have them; otherwise identical to `tx`.
    tx_cond: broadcast::Sender<Chunk>,
    capacity: usize,
    heartbeat: Duration,
    /// A write taking longer than this means the client has stopped reading.
    write_timeout: Duration,
    /// Header for live clients; `start` is set per client.
    metadata: Metadata,
    upstream: Mutex<Upstream>,
    /// Held while publishing, so a joining client's snapshot and its live
    /// queue meet with nothing missed or repeated.
    reference: Mutex<Reference>,
    pub events: Events,
    /// Trade prices of the most active instruments, by symbol, for charts.
    /// One-minute bars of the relay's own symbols, for charts.
    pub bars: Arc<crate::bars::Bars>,
    /// The latest quote of each instrument, for pricing chains.
    pub quotes: crate::quotes::Quotes,
    /// The New York trading day the held quotes are from.
    quote_day: Mutex<Option<String>>,
    /// The latest trades and quote changes of the relay's own stock symbols, for the tape.
    pub tape: crate::tape::Tape,
    pub series: Series,
    /// Who is connected, by connection.
    roster: Mutex<BTreeMap<u64, ClientInfo>>,
    next_client: AtomicU64,
    pub clients: AtomicU64,
    pub records: AtomicU64,
    /// When the last record arrived from the provider, ns since the epoch (0: none yet).
    pub last_record_ns: AtomicU64,
    /// How far behind the provider the relay read its latest record: the
    /// time it was published less the provider's receive time (ns; 0: none yet).
    pub behind_ns: AtomicU64,
    /// The worst of those in the current minute, and that minute (since the epoch).
    behind_peak_ns: AtomicU64,
    behind_peak_minute: AtomicU64,
    pub bytes: AtomicU64,
    pub reconnects: AtomicU64,
    /// Times the provider skipped records because the relay read too slowly.
    pub upstream_skips: AtomicU64,
    pub slow_disconnects: AtomicU64,
    pub refused: AtomicU64,
    /// Provider rows that couldn't be represented or weren't for this relay.
    pub refused_rows: AtomicU64,
}

impl Hub {
    /// `capacity` chunks of up to 64 KiB each are queued per client.
    pub fn new(metadata: Metadata, capacity: usize, heartbeat: Duration) -> Self {
        let (tx, _) = broadcast::channel(capacity.max(2));
        let (tx_cond, _) = broadcast::channel(capacity.max(2));
        Self {
            tx,
            tx_cond,
            capacity: capacity.max(2),
            heartbeat,
            write_timeout: Duration::from_secs(10),
            metadata,
            upstream: Mutex::new(Upstream::Starting),
            reference: Mutex::new(Reference::default()),
            series: Series::new(900, 1000, 6),
            bars: Arc::new(crate::bars::Bars::default()),
            quotes: crate::quotes::Quotes::default(),
            quote_day: Mutex::new(None),
            tape: crate::tape::Tape::default(),
            events: Events::default(),
            clients: AtomicU64::new(0),
            roster: Mutex::new(BTreeMap::new()),
            next_client: AtomicU64::new(0),
            records: AtomicU64::new(0),
            last_record_ns: AtomicU64::new(0),
            behind_ns: AtomicU64::new(0),
            behind_peak_ns: AtomicU64::new(0),
            behind_peak_minute: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            upstream_skips: AtomicU64::new(0),
            slow_disconnects: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            refused_rows: AtomicU64::new(0),
        }
    }

    pub fn heartbeat(&self) -> Duration {
        self.heartbeat
    }

    /// The relay's stream header, the same for every client, with `start`
    /// set to where this client's stream begins.
    pub fn metadata(&self, start: u64) -> Metadata {
        let mut m = self.metadata.clone();
        m.start = start;
        m
    }

    #[cfg(test)]
    fn with_write_timeout(mut self, t: Duration) -> Self {
        self.write_timeout = t;
        self
    }

    async fn send(&self, out: &mut OwnedWriteHalf, bytes: &[u8]) -> Sent {
        match tokio::time::timeout(self.write_timeout, out.write_all(bytes)).await {
            Ok(Ok(())) => Sent::Ok,
            Ok(Err(_)) => Sent::Closed,
            Err(_) => Sent::Stalled,
        }
    }

    /// Disconnect a client that fell behind, telling it why if it can still
    /// take a record.
    ///
    /// Each drop is logged with who it was and how far behind, to tell a
    /// client that stalled briefly from one that can't keep up.
    async fn drop_slow(&self, out: &mut OwnedWriteHalf, why: &str, behind: &Behind) {
        self.slow_disconnects.fetch_add(1, Ordering::Relaxed);
        let detail = behind.describe();
        eprintln!("relay: disconnected {}: {why}; {detail}", behind.who);
        self.events.push(
            format!("disconnected {}: {why}", behind.who),
            format!("{why}; {detail}"),
        );
        let e = protocol::error(ErrorCode::SkippedRecordsAfterSlowReading, why);
        let _ = tokio::time::timeout(Duration::from_secs(1), out.write_all(&e)).await;
        let _ = tokio::time::timeout(Duration::from_secs(1), out.shutdown()).await;
    }

    /// The provider received the latest record just published at `recv_ns`:
    /// how far behind the relay is reading, in ns.
    pub fn received(&self, recv_ns: u64) -> u64 {
        let now = protocol::now_ns();
        let behind = now.saturating_sub(recv_ns);
        self.behind_ns.store(behind, Ordering::Relaxed);
        let minute = now / 60_000_000_000;
        if self.behind_peak_minute.swap(minute, Ordering::Relaxed) == minute {
            self.behind_peak_ns.fetch_max(behind, Ordering::Relaxed);
        } else {
            self.behind_peak_ns.store(behind, Ordering::Relaxed);
        }
        behind
    }

    /// The worst lag in the current or the previous minute, in ns (None: no record lately).
    pub fn behind_peak(&self) -> Option<u64> {
        let minute = protocol::now_ns() / 60_000_000_000;
        (self.behind_peak_minute.load(Ordering::Relaxed) + 1 >= minute)
            .then(|| self.behind_peak_ns.load(Ordering::Relaxed))
    }

    /// Send records to every live client, keeping any reference records.
    pub fn publish(&self, chunk: Vec<u8>, records: u64) {
        let chunk: Chunk = chunk.into();
        self.publish_pair(chunk.clone(), chunk, records);
    }

    /// Send records to live clients: `plain` to most, `with_conditions` to
    /// those that asked for trade conditions.
    pub fn publish_pair(&self, plain: Chunk, with_conditions: Chunk, records: u64) {
        self.records.fetch_add(records, Ordering::Relaxed);
        self.last_record_ns
            .store(protocol::now_ns(), Ordering::Relaxed);
        self.bytes.fetch_add(plain.len() as u64, Ordering::Relaxed);
        let mut reference = lock(&self.reference);
        let stale = reference.absorb(&plain);
        // Before this chunk's quotes, which are the new ids' own.
        if !stale.is_empty() {
            self.quotes.forget(&stale);
        }
        self.chart(&reference, &plain);
        self.quotes.absorb(&plain);
        self.tape.absorb(&plain, |id| reference.symbol(id));
        // No receivers is fine: nobody is listening right now.
        let _ = self.tx.send(plain);
        let _ = self.tx_cond.send(with_conditions);
    }

    /// Record the price of every trade in a chunk under its symbol.
    fn chart(&self, reference: &Reference, chunk: &[u8]) {
        let mut at = 0;
        while at < chunk.len() {
            let len = usize::from(chunk[at]) * 4;
            let Some(rec) = chunk.get(at..at + len).filter(|r| r.len() >= 16) else {
                return;
            };
            at += len;
            let (action, price, size) = match rec[1] {
                rtype::MBP_1 => (
                    offset_of!(Mbp1Msg, action),
                    offset_of!(Mbp1Msg, price),
                    offset_of!(Mbp1Msg, size),
                ),
                rtype::CMBP_1 => (
                    offset_of!(Cmbp1Msg, action),
                    offset_of!(Cmbp1Msg, price),
                    offset_of!(Cmbp1Msg, size),
                ),
                _ => continue,
            };
            if rec.get(action) != Some(&b'T') {
                continue;
            }
            let Some(px) = u64_at(rec, price)
                .map(|p| p as i64)
                .filter(|p| *p != UNDEF_PRICE)
            else {
                continue;
            };
            let id = u32_at(rec, offset_of!(RecordHeader, instrument_id)).unwrap_or(0);
            if let Some(symbol) = reference.symbol(id) {
                self.series.push(symbol, px as f64 / 1e9);
                let ts = u64_at(rec, offset_of!(RecordHeader, ts_event)).unwrap_or(0);
                self.bars.trade(
                    symbol,
                    ts,
                    px as f64 / 1e9,
                    u64::from(u32_at(rec, size).unwrap_or(0)),
                );
            }
        }
    }

    fn broadcast(&self, bytes: Vec<u8>) {
        let chunk: Chunk = bytes.into();
        let _ = self.tx.send(chunk.clone());
        let _ = self.tx_cond.send(chunk);
    }

    /// The latest quotes of the instruments whose compact symbols pass
    /// `keep` (the reference lock first, as when publishing).
    pub fn quotes_matching(
        &self,
        keep: impl Fn(&str) -> bool,
    ) -> Vec<(String, crate::quotes::Quote)> {
        let reference = lock(&self.reference);
        self.quotes.matching(|id| reference.symbol(id), keep)
    }

    /// A statistic of the contracts `keep` accepts (see `Reference::stats_matching`).
    pub fn stats_matching(
        &self,
        kind: u16,
        keep: impl Fn(&str) -> bool,
    ) -> Vec<(String, i64, u64)> {
        lock(&self.reference).stats_matching(kind, &keep)
    }

    /// A provider session has started. On a new trading day (New York), every
    /// quote held is from an earlier day's ids, so the quotes are dropped: a
    /// contract has none until it quotes today. A reconnect on the same day
    /// keeps them, as ids hold for the day.
    pub fn session_started(&self, now_ns: u64) {
        let day =
            tinytrader_schedule::ny_date_time(i64::try_from(now_ns / 1_000_000_000).unwrap_or(0)).0;
        let mut held = lock(&self.quote_day);
        if held.as_deref() != Some(day.as_str()) {
            if let Some(was) = held.as_deref() {
                let n = self.quotes.len();
                self.quotes.clear();
                let text = format!("a new trading day ({day}): {n} quotes from {was} dropped");
                eprintln!("relay: {text}");
                self.events.push(text.clone(), text);
            }
            *held = Some(day);
        }
    }

    /// How many of the quotes of defined instruments are older than
    /// `older_ns` at `now_ns`, of how many.
    pub fn stale_quotes(&self, now_ns: u64, older_ns: u64) -> (usize, usize) {
        let reference = lock(&self.reference);
        self.quotes
            .stale(|id| reference.symbol(id).is_some(), now_ns, older_ns)
    }

    /// Reference records held for joining clients.
    pub fn reference_len(&self) -> usize {
        lock(&self.reference).len()
    }

    pub fn upstream(&self) -> Upstream {
        *lock(&self.upstream)
    }

    /// The provider connection is up; tell clients how long it was down.
    /// The provider skipped records it couldn't deliver in time (it sends
    /// its own slow-reader error, whose code clients would mistake for the
    /// relay dropping them). Returns the relay's own marker for clients:
    /// `upstream-gap gap_from=<ns> gap_to=<ns>`, the span that is missing.
    pub fn upstream_gap(&self, from: u64, to: u64) -> Vec<u8> {
        self.upstream_skips.fetch_add(1, Ordering::Relaxed);
        let text = format!("upstream-gap gap_from={from} gap_to={to}");
        eprintln!("relay: the provider skipped records (the relay read too slowly): {text}");
        self.events.push(text.clone(), text.clone());
        protocol::system_text(&text)
    }

    /// The provider warned that the relay is reading too slowly. Not sent to
    /// clients (it is about the relay, not them); logged.
    pub fn upstream_slow(&self) {
        let text = "the provider warned that the relay is reading too slowly".to_owned();
        eprintln!("relay: {text}");
        self.events.push(text.clone(), text);
    }

    pub fn upstream_up(&self) {
        let mut u = lock(&self.upstream);
        if let Upstream::Off(_) = *u {
            self.events.push("on schedule".into(), "on schedule".into());
            self.broadcast(protocol::system_text("on-schedule"));
        }
        if let Upstream::Down(since) = *u {
            self.reconnects.fetch_add(1, Ordering::Relaxed);
            let text = format!("upstream-up gap_from={since} gap_to={}", protocol::now_ns());
            self.events.push(text.clone(), text.clone());
            self.broadcast(protocol::system_text(&text));
        }
        *u = Upstream::Up;
    }

    /// The relay is outside its active hours until `until`; tell clients once.
    pub fn off_schedule(&self, until: u64) {
        let mut u = lock(&self.upstream);
        let was_off = matches!(*u, Upstream::Off(_));
        *u = Upstream::Off(until);
        if !was_off {
            let text = format!("off-schedule until={until}");
            self.events.push(text.clone(), text.clone());
            self.broadcast(protocol::system_text(&text));
        }
    }

    /// The provider connection was lost; tell clients once.
    pub fn upstream_down(&self) {
        let mut u = lock(&self.upstream);
        if matches!(*u, Upstream::Down(_) | Upstream::Off(_)) {
            return;
        }
        let since = protocol::now_ns();
        *u = Upstream::Down(since);
        let text = format!("upstream-down since={since}");
        self.events.push(text.clone(), text.clone());
        self.broadcast(protocol::system_text(&text));
    }

    /// Refuse a client with an `ErrorMsg` and close.
    pub async fn refuse(&self, mut out: OwnedWriteHalf, code: ErrorCode, why: &str) {
        self.refused.fetch_add(1, Ordering::Relaxed);
        self.events
            .push(format!("refused: {why}"), format!("refused: {why}"));
        let _ = out.write_all(&protocol::error(code, why)).await;
        let _ = out.shutdown().await;
    }

    /// Send a client the reference records, then (when catching up) the
    /// history it asked for, then `ReplayCompleted`, then live records until
    /// it disconnects or falls behind. `start` goes in its stream header.
    ///
    /// Live records are buffered while history is sent, and market records
    /// from before `cut` (which history covers) are dropped from them.
    pub async fn serve(&self, mut out: OwnedWriteHalf, join: Serve) {
        let (mut rx, snapshot) = {
            let mut reference = lock(&self.reference);
            let rx = if join.conditions {
                self.tx_cond.subscribe()
            } else {
                self.tx.subscribe()
            };
            (rx, reference.snapshot(protocol::now_ns()))
        };
        let Ok(mut first) = protocol::header(&self.metadata(join.start)).await else {
            return;
        };
        first.extend(snapshot);
        if !matches!(self.send(&mut out, &first).await, Sent::Ok) {
            return;
        }
        let mut behind = Behind::new(join.who);
        let mut buffered: Vec<Chunk> = Vec::new();
        if let Some(mut history) = join.history {
            loop {
                tokio::select! {
                    page = history.recv() => match page {
                        Some(Ok(bytes)) => {
                            if !matches!(self.send(&mut out, &bytes).await, Sent::Ok) {
                                return;
                            }
                        }
                        Some(Err(e)) => {
                            let _ = out.write_all(&protocol::error(ErrorCode::InternalError, &e)).await;
                            return;
                        }
                        None => break,
                    },
                    got = rx.recv() => match got {
                        Ok(chunk) => buffered.push(chunk),
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            behind.skipped = n;
                            self.drop_slow(&mut out, "client fell behind while catching up", &behind).await;
                            return;
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    },
                }
            }
        }
        let mut cut = join.cut;
        let mut tail = protocol::system_code(SystemCode::ReplayCompleted);
        match self.upstream() {
            Upstream::Down(since) => tail.extend(protocol::system_text(&format!(
                "upstream-down since={since}"
            ))),
            Upstream::Off(until) => tail.extend(protocol::system_text(&format!(
                "off-schedule until={until}"
            ))),
            _ => {}
        }
        for chunk in &buffered {
            tail.extend(after_cut(chunk, &mut cut));
        }
        if !matches!(self.send(&mut out, &tail).await, Sent::Ok) {
            return;
        }
        let mut last_write = Instant::now();
        let mut warned = false;
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            let bytes: Vec<u8> = tokio::select! {
                got = rx.recv() => match got {
                    Ok(chunk) => {
                        let mut bytes = after_cut(&chunk, &mut cut);
                        let queued = rx.len();
                        behind.reading(&chunk, queued);
                        if !warned && queued > self.capacity / 2 {
                            warned = true;
                            behind.warned = Some(Instant::now());
                            bytes.extend(protocol::system_code(SystemCode::SlowReaderWarning));
                        } else if queued < self.capacity / 4 {
                            warned = false;
                            behind.warned = None;
                        }
                        bytes
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        behind.skipped = n;
                        self.drop_slow(&mut out, "client fell behind its queue", &behind).await;
                        return;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                },
                _ = tick.tick() => {
                    if last_write.elapsed() < self.heartbeat {
                        continue;
                    }
                    protocol::system_code(SystemCode::Heartbeat)
                }
            };
            if bytes.is_empty() {
                continue;
            }
            match self.send(&mut out, &bytes).await {
                Sent::Ok => last_write = Instant::now(),
                Sent::Closed => return,
                Sent::Stalled => {
                    self.drop_slow(&mut out, "client stopped reading", &behind)
                        .await;
                    return;
                }
            }
        }
    }
}

/// How far a client is behind, for the log when it's dropped.
struct Behind {
    who: String,
    joined: Instant,
    /// When it was last warned (cleared once it caught up).
    warned: Option<Instant>,
    /// The most chunks it has had waiting, and the market time of the last
    /// record it was given.
    most_queued: usize,
    last_ts: u64,
    /// Chunks it missed when the queue overflowed.
    skipped: u64,
}

impl Behind {
    fn new(who: String) -> Self {
        Self {
            who,
            joined: Instant::now(),
            warned: None,
            most_queued: 0,
            last_ts: 0,
            skipped: 0,
        }
    }

    fn reading(&mut self, chunk: &[u8], queued: usize) {
        self.most_queued = self.most_queued.max(queued);
        // Every chunk starts with a whole record: its header's ts_event.
        let at = offset_of!(RecordHeader, ts_event);
        if let Some(b) = chunk.get(at..at + 8) {
            let ts = u64::from_le_bytes(b.try_into().unwrap_or_default());
            if ts != UNDEF_TIMESTAMP {
                self.last_ts = ts;
            }
        }
    }

    fn describe(&self) -> String {
        let mut d = format!(
            "connected {:.0} s, most queued {} chunks",
            self.joined.elapsed().as_secs_f64(),
            self.most_queued
        );
        if self.last_ts > 0 {
            let lag = protocol::now_ns().saturating_sub(self.last_ts) as f64 / 1e9;
            d.push_str(&format!(", last record given {lag:.1} s old"));
        }
        if let Some(w) = self.warned {
            d.push_str(&format!(
                ", warned {:.1} s before",
                w.elapsed().as_secs_f64()
            ));
        }
        if self.skipped > 0 {
            d.push_str(&format!(", {} chunks skipped", self.skipped));
        }
        d
    }
}

/// How a client is served.
pub struct Serve {
    /// Who it is and where from, for the log.
    pub who: String,
    /// For the stream header.
    pub start: u64,
    /// Interleave trade conditions.
    pub conditions: bool,
    /// History to send before live data, as chunks of records.
    pub history: Option<tokio::sync::mpsc::Receiver<Result<Vec<u8>, String>>>,
    /// Live market records before this time are dropped (history covers
    /// them). Zero keeps everything.
    pub cut: u64,
}

/// The records of a live chunk that history doesn't already cover: market
/// records (every kind but reference and system records: definitions, symbol
/// mappings, status, system and error messages) timed before `cut` are
/// dropped until one at or after it is seen, after which `cut` is cleared.
fn after_cut(chunk: &[u8], cut: &mut u64) -> Vec<u8> {
    if *cut == 0 {
        return chunk.to_vec();
    }
    let mut out = Vec::with_capacity(chunk.len());
    let mut at = 0;
    while at < chunk.len() {
        let len = usize::from(chunk[at]) * 4;
        let Some(rec) = chunk.get(at..at + len).filter(|r| r.len() >= 16) else {
            break;
        };
        at += len;
        let market = !matches!(
            rec[1],
            rtype::INSTRUMENT_DEF
                | rtype::SYMBOL_MAPPING
                | rtype::STATUS
                | rtype::SYSTEM
                | rtype::ERROR
        );
        let ts = u64_at(rec, 8).unwrap_or(0);
        if market && *cut != 0 {
            if ts < *cut {
                continue;
            }
            *cut = 0;
        }
        out.extend_from_slice(rec);
    }
    out
}

/// Read a client's join line. On failure the client has been refused.
pub async fn read_join(stream: TcpStream, hub: &Hub) -> Option<(Join, OwnedWriteHalf)> {
    let (read, write) = stream.into_split();
    let mut line = String::new();
    let mut reader = BufReader::new(read.take(JOIN_MAX));
    let got = tokio::time::timeout(JOIN_TIMEOUT, reader.read_line(&mut line)).await;
    let why = match got {
        Err(_) => "no join request within 5 seconds".to_owned(),
        Ok(Err(e)) => format!("cannot read join request: {e}"),
        Ok(Ok(_)) if !line.ends_with('\n') => "join request must be one line of JSON".to_owned(),
        Ok(Ok(_)) => match protocol::parse_join(&line) {
            Ok(join) => return Some((join, write)),
            Err(e) => e,
        },
    };
    hub.refuse(write, ErrorCode::InvalidSubscription, &why)
        .await;
    None
}

/// A connected client, as the console shows it.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ClientInfo {
    /// The name it gave, if any.
    pub client: Option<String>,
    /// The Alpaca account it said it trades, if any.
    pub account: Option<String>,
    pub peer: String,
    /// When it joined, Unix seconds.
    pub since: f64,
    /// It asked to replay from an earlier time.
    pub replay: bool,
    pub conditions: bool,
}

/// Holds a client on the roster (and in the count) for as long as it lives.
pub struct ClientGuard<'a> {
    hub: &'a Hub,
    id: u64,
}

impl Hub {
    /// Put a client on the roster until the guard is dropped.
    pub fn enter(&self, info: ClientInfo) -> ClientGuard<'_> {
        let id = self.next_client.fetch_add(1, Ordering::Relaxed);
        lock(&self.roster).insert(id, info);
        self.clients.fetch_add(1, Ordering::Relaxed);
        ClientGuard { hub: self, id }
    }

    /// Everyone connected, oldest first.
    pub fn roster(&self) -> Vec<ClientInfo> {
        lock(&self.roster).values().cloned().collect()
    }
}

impl Drop for ClientGuard<'_> {
    fn drop(&mut self) {
        lock(&self.hub.roster).remove(&self.id);
        self.hub.clients.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use databento::dbn::decode::{AsyncDbnDecoder, DbnMetadata as _};
    use databento::dbn::{ErrorMsg, RecordRefEnum, SystemMsg};
    use tokio::net::TcpListener;

    fn metadata() -> Metadata {
        Metadata::builder()
            .dataset("TEST.DATA".to_owned())
            .schema(None)
            .start(0)
            .stype_in(None)
            .stype_out(databento::dbn::SType::InstrumentId)
            .build()
    }

    /// A hub on a local port that serves every client live.
    async fn serving(hub: Arc<Hub>) -> String {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (s, _) = l.accept().await.unwrap();
                let hub = hub.clone();
                tokio::spawn(async move {
                    if let Some((_, out)) = read_join(s, &hub).await {
                        hub.serve(
                            out,
                            Serve {
                                who: "a test client".to_owned(),
                                start: protocol::now_ns(),
                                conditions: false,
                                history: None,
                                cut: 0,
                            },
                        )
                        .await;
                    }
                });
            }
        });
        addr
    }

    async fn join(addr: &str, line: &str) -> AsyncDbnDecoder<TcpStream> {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(line.as_bytes()).await.unwrap();
        AsyncDbnDecoder::new(s).await.unwrap()
    }

    #[tokio::test]
    async fn forwards_records_and_reports_the_provider() {
        let hub = Arc::new(Hub::new(metadata(), 64, Duration::from_secs(1)));
        let addr = serving(hub.clone()).await;
        let mut dec = join(&addr, "{\"protocol\":\"tinytrader.relay/1\"}\n").await;
        assert_eq!(dec.metadata().dataset, "TEST.DATA");
        assert_eq!(dec.metadata().version, 3);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let rec = protocol::system_text("from the provider");
        hub.publish(rec, 1);
        hub.upstream_down();
        hub.upstream_up();
        let mut texts = Vec::new();
        while texts.len() < 5 {
            let r = dec.decode_record_ref().await.unwrap().unwrap();
            if let RecordRefEnum::System(m) = r.as_enum().unwrap() {
                texts.push(m.msg().unwrap().to_owned());
            }
        }
        // Nothing cached yet, so ReplayCompleted comes straight away.
        texts.remove(0);
        assert_eq!(texts[0], "from the provider");
        assert!(texts[1].starts_with("upstream-down since="));
        assert!(texts[2].starts_with("upstream-up gap_from="));
        // Then a heartbeat after a second of silence.
        assert_eq!(texts[3], "");
        assert_eq!(hub.reconnects.load(Ordering::Relaxed), 1);
    }

    fn definition(id: u32, expiration: u64, strike: i64) -> Vec<u8> {
        let rec = InstrumentDefMsg {
            hd: RecordHeader::new::<InstrumentDefMsg>(rtype::INSTRUMENT_DEF, 1, id, 0),
            expiration,
            strike_price: strike,
            ..Default::default()
        };
        databento::dbn::RecordRef::from(&rec).as_ref().to_vec()
    }

    fn statistic(id: u32, kind: u16, quantity: i64) -> Vec<u8> {
        let rec = StatMsg {
            hd: RecordHeader::new::<StatMsg>(rtype::STATISTICS, 1, id, 0),
            stat_type: kind,
            quantity,
            update_action: 1,
            ..Default::default()
        };
        databento::dbn::RecordRef::from(&rec).as_ref().to_vec()
    }

    #[tokio::test]
    async fn sends_reference_records_to_joining_clients() {
        let hub = Arc::new(Hub::new(metadata(), 64, Duration::from_secs(10)));
        let addr = serving(hub.clone()).await;
        let mut chunk = definition(7, u64::MAX, 100);
        chunk.extend(definition(8, 1, 200)); // expired long ago
        chunk.extend(definition(7, u64::MAX, 150)); // replaces the first
        chunk.extend(statistic(7, 9, 1000));
        chunk.extend(statistic(8, 9, 5)); // for the expired instrument
        hub.publish(chunk, 5);

        let mut dec = join(&addr, "{\"protocol\":\"tinytrader.relay/1\"}\n").await;
        let mut seen = Vec::new();
        loop {
            let r = dec.decode_record_ref().await.unwrap().unwrap();
            match r.as_enum().unwrap() {
                RecordRefEnum::InstrumentDef(d) => {
                    seen.push(format!("def {} {}", d.hd.instrument_id, d.strike_price))
                }
                RecordRefEnum::Stat(m) => {
                    seen.push(format!("stat {} {}", m.hd.instrument_id, m.quantity))
                }
                RecordRefEnum::System(m) if m.code().ok() == Some(SystemCode::ReplayCompleted) => {
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(seen, ["def 7 150", "stat 7 1000"]);
        assert_eq!(hub.reference_len(), 2);
    }

    #[tokio::test]
    async fn keeps_contracts_expiring_today() {
        // Databento stamps today's expiries at 00:00 UTC today, hours ago.
        let now = protocol::now_ns();
        let midnight_utc = now - now % (86_400 * 1_000_000_000);
        let hub = Arc::new(Hub::new(metadata(), 64, Duration::from_secs(10)));
        let addr = serving(hub.clone()).await;
        let mut chunk = definition(9, midnight_utc, 300); // expires today
        chunk.extend(definition(
            8,
            midnight_utc - 3 * 86_400 * 1_000_000_000,
            200,
        )); // three days ago
        hub.publish(chunk, 5);
        let mut dec = join(&addr, "{\"protocol\":\"tinytrader.relay/1\"}\n").await;
        let mut seen = Vec::new();
        loop {
            let r = dec.decode_record_ref().await.unwrap().unwrap();
            match r.as_enum().unwrap() {
                RecordRefEnum::InstrumentDef(d) => seen.push(d.hd.instrument_id),
                RecordRefEnum::System(m) if m.code().ok() == Some(SystemCode::ReplayCompleted) => {
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(seen, [9]);
    }

    #[test]
    fn the_catch_up_cut_drops_every_market_record_history_covered() {
        use databento::dbn::{RecordRef, TradeMsg};
        let at = |ts: u64| RecordHeader::new::<TradeMsg>(rtype::MBP_0, 1, 7, ts);
        let trade = |ts| {
            RecordRef::from(&TradeMsg {
                hd: at(ts),
                ..Default::default()
            })
            .as_ref()
            .to_vec()
        };
        let def = InstrumentDefMsg {
            hd: RecordHeader::new::<InstrumentDefMsg>(rtype::INSTRUMENT_DEF, 1, 7, 5),
            ..Default::default()
        };
        // A trade before the cut is dropped; a definition never is; from the
        // first record at the cut on, everything passes.
        let mut chunk = trade(10);
        chunk.extend(RecordRef::from(&def).as_ref());
        chunk.extend(trade(20));
        chunk.extend(trade(15));
        let mut cut = 20;
        let out = after_cut(&chunk, &mut cut);
        let mut expect = RecordRef::from(&def).as_ref().to_vec();
        expect.extend(trade(20));
        expect.extend(trade(15));
        assert_eq!(out, expect);
        assert_eq!(cut, 0);
    }

    #[test]
    fn charts_trades_by_symbol() {
        let hub = Hub::new(metadata(), 8, Duration::from_secs(10));
        let mut def = InstrumentDefMsg {
            hd: RecordHeader::new::<InstrumentDefMsg>(rtype::INSTRUMENT_DEF, 1, 7, 0),
            expiration: u64::MAX,
            ..Default::default()
        };
        def.raw_symbol = databento::dbn::record::str_to_c_chars("SPY 261016C00775000").unwrap();
        let trade = |action: u8, price: i64| {
            let m = Mbp1Msg {
                hd: RecordHeader::new::<Mbp1Msg>(rtype::MBP_1, 1, 7, 0),
                action: action as std::ffi::c_char,
                price,
                ..Default::default()
            };
            databento::dbn::RecordRef::from(&m).as_ref().to_vec()
        };
        let mut chunk = databento::dbn::RecordRef::from(&def).as_ref().to_vec();
        chunk.extend(trade(b'T', 1_250_000_000));
        chunk.extend(trade(b'A', 9_000_000_000)); // a quote, not charted
        hub.publish(chunk, 3);
        let recent = hub.series.snapshot();
        let points = recent["SPY 261016C00775000"].as_array().unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0][1].as_f64(), Some(1.25));
    }

    #[test]
    fn keeps_the_latest_quote_by_compact_symbol() {
        use databento::dbn::{Cmbp1Msg, ConsolidatedBidAskPair, RecordRef};
        let hub = Hub::new(metadata(), 8, Duration::from_secs(10));
        let def = |id: u32, sym: &str| {
            let mut d = InstrumentDefMsg {
                hd: RecordHeader::new::<InstrumentDefMsg>(rtype::INSTRUMENT_DEF, 1, id, 0),
                expiration: u64::MAX,
                ..Default::default()
            };
            d.raw_symbol = databento::dbn::record::str_to_c_chars(sym).unwrap();
            RecordRef::from(&d).as_ref().to_vec()
        };
        let quote = |id: u32, bid: i64, ask: i64, ts: u64| {
            let mut m = Cmbp1Msg::default_for_schema(databento::dbn::Schema::Cmbp1);
            m.hd = RecordHeader::new::<Cmbp1Msg>(rtype::CMBP_1, 1, id, ts);
            m.action = b'A' as std::ffi::c_char;
            m.ts_recv = ts;
            m.levels = [ConsolidatedBidAskPair {
                bid_px: bid,
                ask_px: ask,
                bid_sz: 5,
                ask_sz: 7,
                ..Default::default()
            }];
            RecordRef::from(&m).as_ref().to_vec()
        };
        let mut chunk = def(7, "SPY   261016C00775000");
        chunk.extend(def(8, "SPY   261023P00700000"));
        chunk.extend(quote(7, 1_200_000_000, 1_250_000_000, 1));
        chunk.extend(quote(7, 1_210_000_000, 1_240_000_000, 2)); // the latest wins
        chunk.extend(quote(8, UNDEF_PRICE, 300_000_000, 3)); // no bid
        hub.publish(chunk, 5);
        let mut got = hub.quotes_matching(|s| s.starts_with("SPY261016"));
        assert_eq!(got.len(), 1);
        let (sym, q) = got.pop().unwrap();
        assert_eq!(sym, "SPY261016C00775000");
        assert_eq!(
            (q.bid, q.ask, q.bid_size, q.ask_size, q.ts),
            (Some(1.21), Some(1.24), 5, 7, 2)
        );
        let all = hub.quotes_matching(|_| true);
        let put = all
            .iter()
            .find(|(s, _)| s == "SPY261023P00700000")
            .unwrap()
            .1;
        assert_eq!((put.bid, put.ask), (None, Some(0.3)));
        assert_eq!(hub.quotes.len(), 2);
    }

    /// A definition record with a symbol, and a `cmbp-1` quote, for the
    /// session-crossing tests.
    fn def_rec(id: u32, sym: &str) -> Vec<u8> {
        let mut d = InstrumentDefMsg {
            hd: RecordHeader::new::<InstrumentDefMsg>(rtype::INSTRUMENT_DEF, 1, id, 0),
            expiration: u64::MAX,
            ..Default::default()
        };
        d.raw_symbol = databento::dbn::record::str_to_c_chars(sym).unwrap();
        databento::dbn::RecordRef::from(&d).as_ref().to_vec()
    }

    fn quote_rec(id: u32, bid: i64, ask: i64, ts: u64) -> Vec<u8> {
        use databento::dbn::{Cmbp1Msg, ConsolidatedBidAskPair};
        let mut m = Cmbp1Msg::default_for_schema(databento::dbn::Schema::Cmbp1);
        m.hd = RecordHeader::new::<Cmbp1Msg>(rtype::CMBP_1, 1, id, ts);
        m.ts_recv = ts;
        m.levels = [ConsolidatedBidAskPair {
            bid_px: bid,
            ask_px: ask,
            bid_sz: 1,
            ask_sz: 1,
            ..Default::default()
        }];
        databento::dbn::RecordRef::from(&m).as_ref().to_vec()
    }

    const C775: &str = "SPY   261016C00775000";
    const C780: &str = "SPY   261016C00780000";

    /// Across a weekend OPRA gives a contract a new id and its old id to
    /// another contract. Each contract's quote is then only its own, today's:
    /// never the old id's Friday quote, under either symbol (2026-11-09: a
    /// share of contracts showed Friday's close all Monday).
    #[test]
    fn a_new_days_ids_never_show_the_last_days_quotes() {
        let hub = Hub::new(metadata(), 8, Duration::from_secs(10));
        // Friday: 775C is id 7, 780C id 8.
        let mut fri = def_rec(7, C775);
        fri.extend(def_rec(8, C780));
        fri.extend(quote_rec(7, 330_000_000, 340_000_000, 100));
        fri.extend(quote_rec(8, 100_000_000, 110_000_000, 100));
        hub.publish(fri, 4);
        // Monday: 775C is id 9 and quotes; id 7 is now 780C, which hasn't yet.
        let mut mon = def_rec(9, C775);
        mon.extend(def_rec(7, C780));
        mon.extend(quote_rec(9, 3_500_000_000, 3_600_000_000, 200));
        hub.publish(mon, 3);
        let got: std::collections::BTreeMap<String, crate::quotes::Quote> =
            hub.quotes_matching(|_| true).into_iter().collect();
        assert_eq!(got.len(), 1, "{got:?}");
        let q = got["SPY261016C00775000"];
        assert_eq!((q.bid, q.ts), (Some(3.5), 200), "today's, never Friday's");
        // One definition per symbol for joining clients: the old ids are gone.
        let mut snap = lock(&hub.reference);
        let defs: Vec<(u32, String)> = snap
            .definitions
            .iter()
            .map(|(id, (_, d))| (*id, def_symbol(d).unwrap().to_owned()))
            .collect();
        assert_eq!(defs, [(7, C780.to_owned()), (9, C775.to_owned())]);
        assert_eq!(snap.snapshot(0).len() % size_of::<InstrumentDefMsg>(), 0);
    }

    /// A provider session on a new New York day drops the last day's quotes;
    /// a reconnect on the same day keeps them.
    #[test]
    fn a_new_trading_day_drops_the_quotes_a_reconnect_keeps_them() {
        let hub = Hub::new(metadata(), 8, Duration::from_secs(10));
        let fri_10am = 1_793_458_800_000_000_000; // Fri 2026-11-06 10:00 New York
        hub.session_started(fri_10am);
        let mut c = def_rec(7, C775);
        c.extend(quote_rec(7, 330_000_000, 340_000_000, fri_10am));
        hub.publish(c, 2);
        hub.session_started(fri_10am + 3_600_000_000_000); // a reconnect at 11:00
        assert_eq!(hub.quotes.len(), 1);
        hub.session_started(fri_10am + 3 * 86_400_000_000_000); // Monday
        assert_eq!(hub.quotes.len(), 0);
    }

    /// The stale count: quotes of defined instruments older than the limit.
    #[test]
    fn counts_stale_quotes() {
        let hub = Hub::new(metadata(), 8, Duration::from_secs(10));
        let mut c = def_rec(7, C775);
        c.extend(def_rec(8, C780));
        c.extend(quote_rec(7, 1, 2, 1_000));
        c.extend(quote_rec(8, 1, 2, 9_000));
        c.extend(quote_rec(5, 1, 2, 1_000)); // no definition: not counted
        hub.publish(c, 5);
        assert_eq!(hub.stale_quotes(10_000, 5_000), (1, 2));
        assert_eq!(hub.stale_quotes(10_000, 10_000), (0, 2));
    }

    #[test]
    fn open_interest_by_compact_symbol() {
        use databento::dbn::RecordRef;
        let hub = Hub::new(metadata(), 8, Duration::from_secs(10));
        let mut d = InstrumentDefMsg {
            hd: RecordHeader::new::<InstrumentDefMsg>(rtype::INSTRUMENT_DEF, 1, 7, 0),
            expiration: u64::MAX,
            ..Default::default()
        };
        d.raw_symbol = databento::dbn::record::str_to_c_chars("SPY   261016C00775000").unwrap();
        let mut chunk = RecordRef::from(&d).as_ref().to_vec();
        let mut oi = StatMsg {
            hd: RecordHeader::new::<StatMsg>(rtype::STATISTICS, 1, 7, 0),
            stat_type: 9,
            quantity: 1234,
            update_action: 1,
            ..Default::default()
        };
        oi.ts_ref = 1_791_518_400_000_000_000; // 2026-10-09 New York
        chunk.extend(RecordRef::from(&oi).as_ref());
        // Another kind of statistic isn't open interest.
        chunk.extend(statistic(7, 1, 55));
        hub.publish(chunk, 3);
        let got = hub.stats_matching(9, |s| s.starts_with("SPY261016"));
        assert_eq!(
            got,
            vec![(
                "SPY261016C00775000".to_owned(),
                1234,
                1_791_518_400_000_000_000
            )]
        );
        assert!(hub.stats_matching(9, |s| s.starts_with("QQQ")).is_empty());
        // Without `ts_ref` (as OPRA sends it), the record's own time.
        let mut undated = StatMsg {
            hd: RecordHeader::new::<StatMsg>(rtype::STATISTICS, 1, 7, 1_791_800_000_000_000_000),
            stat_type: 9,
            quantity: 99,
            update_action: 1,
            ..Default::default()
        };
        undated.ts_ref = UNDEF_TIMESTAMP;
        hub.publish(RecordRef::from(&undated).as_ref().to_vec(), 1);
        assert_eq!(
            hub.stats_matching(9, |_| true)[0].2,
            1_791_800_000_000_000_000
        );
    }

    #[test]
    fn the_roster_holds_clients_while_connected() {
        let hub = Hub::new(metadata(), 8, Duration::from_secs(30));
        let join = protocol::parse_join(
            r#"{"protocol":"tinytrader.relay/1","client":"xsp-strategy","account":"paper-1"}"#,
        )
        .unwrap();
        let info = ClientInfo {
            client: join.client,
            account: join.account,
            peer: "127.0.0.1:1".into(),
            since: 0.0,
            replay: false,
            conditions: false,
        };
        {
            let _a = hub.enter(info.clone());
            let _b = hub.enter(ClientInfo {
                client: None,
                account: None,
                ..info
            });
            let r = hub.roster();
            assert_eq!(r.len(), 2);
            assert_eq!(
                (r[0].client.as_deref(), r[0].account.as_deref()),
                (Some("xsp-strategy"), Some("paper-1"))
            );
            assert_eq!(hub.clients.load(Ordering::Relaxed), 2);
        }
        assert!(hub.roster().is_empty());
        assert_eq!(hub.clients.load(Ordering::Relaxed), 0);
        assert!(
            protocol::parse_join(r#"{"protocol":"tinytrader.relay/1","client":"<script>"}"#)
                .is_err()
        );
        assert!(protocol::parse_join(r#"{"protocol":"tinytrader.relay/1","account":""}"#).is_err());
    }

    #[tokio::test]
    async fn refuses_bad_joins() {
        let hub = Arc::new(Hub::new(metadata(), 64, Duration::from_secs(10)));
        let addr = serving(hub.clone()).await;
        let mut s = TcpStream::connect(&addr).await.unwrap();
        s.write_all(b"{\"protocol\":\"other/9\"}\n").await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), std::mem::size_of::<ErrorMsg>());
        assert_eq!(hub.refused.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn disconnects_a_client_that_falls_behind() {
        let hub = Arc::new(
            Hub::new(metadata(), 8, Duration::from_secs(10))
                .with_write_timeout(Duration::from_millis(200)),
        );
        let addr = serving(hub.clone()).await;
        // Connect but never read.
        let mut s = TcpStream::connect(&addr).await.unwrap();
        s.write_all(b"{\"protocol\":\"tinytrader.relay/1\"}\n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let big = vec![0u8; 64 * 1024];
        for _ in 0..200 {
            hub.publish(big.clone(), 1);
            tokio::time::sleep(Duration::from_millis(10)).await;
            if hub.slow_disconnects.load(Ordering::Relaxed) > 0 {
                break;
            }
        }
        assert_eq!(hub.slow_disconnects.load(Ordering::Relaxed), 1);
        let _ = SystemMsg::heartbeat(0);
    }
}
