//! One symbol-day: both lanes fetched page by page into the spool, then read
//! back from it, merged, encoded and written as a stream into the day's store
//! batches (its records, its trade conditions and, for SIP, its quote
//! conditions), checked by reading them back, and handed to the store to file.
//!
//! A day counts only when both lanes reached their last page and the files
//! read back with the counts and times the pages said, trades (`T`) and quotes
//! (`M`) each against their own lane (decision 0014). A day is one file per
//! schema, as Databento's are; the receipts (every page's rows, times and
//! SHA-256) go with it to the store's `provenance/`.
//!
//! Memory doesn't grow with the day: one page a lane while fetching, and one
//! page a lane and the encoders' buffers while writing.

use std::io::Write;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use dbn::decode::{DbnMetadata, DecodeRecordRef};
use dbn::encode::{DbnEncodable, DbnMetadataEncoder, EncodeRecord};
use dbn::{Cmbp1Msg, FlagSet, Mbp1Msg, Metadata, SType, Schema, SymbolMapping, UNDEF_TIMESTAMP};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tinytrader_alpaca::{iex, sip};

use std::cell::{Cell, RefCell};
use std::sync::atomic::AtomicBool;

use crate::fetch::{Alpaca, Failure, LaneDone, LaneSpec, day_ns, lane};
use crate::spool::{LaneSpool, Pages, Parse};

/// Which of Alpaca's feeds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Feed {
    Sip,
    Iex,
}

/// The schema of trade conditions batches.
const CONDITIONS: &str = "conditions";
/// The schema of quote conditions batches (SIP).
const QUOTE_CONDITIONS: &str = "quote-conditions";

impl Feed {
    pub fn name(self) -> &'static str {
        match self {
            Self::Sip => "sip",
            Self::Iex => "iex",
        }
    }
    pub fn dataset(self) -> &'static str {
        match self {
            Self::Sip => sip::DATASET,
            Self::Iex => iex::DATASET,
        }
    }
    fn schema(self) -> Schema {
        match self {
            Self::Sip => Schema::Cmbp1,
            Self::Iex => Schema::Mbp1,
        }
    }
    /// The schemas of a day's sidecars, in batch order.
    fn sidecars(self) -> &'static [&'static str] {
        match self {
            Self::Sip => &[CONDITIONS, QUOTE_CONDITIONS],
            Self::Iex => &[CONDITIONS],
        }
    }
    /// The store batch ids of a symbol-day: its records first, then its
    /// sidecars (trade conditions, and for SIP quote conditions).
    pub fn batches(self, symbol: &str, day: &str) -> Vec<String> {
        let d = day.replace('-', "");
        let main = format!("ALPACA-{}-{symbol}-{d}", self.name().to_uppercase());
        let mut ids = vec![main.clone()];
        ids.extend(self.sidecars().iter().map(|s| format!("{main}-{s}")));
        ids
    }
}

/// What a day came to.
#[derive(Debug, Default, serde::Serialize, Deserialize)]
pub struct Outcome {
    pub trades: usize,
    pub quotes: usize,
    pub pages: usize,
    pub bytes: u64,
    /// Quotes whose conditions couldn't be held (their records say so).
    #[serde(default)]
    pub unparsed_quote_conditions: usize,
    /// What built the day: a complete spool by another encoder isn't filed.
    #[serde(default)]
    pub encoder: String,
}

/// The encoder this build writes with, as kept in a day's complete marker.
pub const ENCODER: &str = concat!(
    "tinytrader-alpaca ",
    env!("CARGO_PKG_VERSION"),
    " (quote-conditions/1)"
);

/// A day built: its batch folders (records first, then sidecars) and outcome.
#[derive(Debug)]
pub struct Built {
    pub batches: Vec<PathBuf>,
    pub outcome: Outcome,
    /// Most rows of one lane held in memory at once while it was written (one
    /// page's); 0 when taken from a complete spool.
    pub peak_rows: usize,
}

#[derive(Deserialize)]
struct Page<T> {
    #[serde(alias = "trades", alias = "quotes", default = "none")]
    rows: Option<Vec<T>>,
    #[serde(default)]
    next_page_token: Option<String>,
}
fn none<T>() -> Option<Vec<T>> {
    None
}

/// A page's rows (each parsed by `row`) and its next token.
fn page<'a, R: Deserialize<'a>, T>(
    body: &'a [u8],
    row: impl Fn(&R) -> Result<T, String>,
) -> Result<(Vec<T>, Option<String>), String> {
    let p: Page<R> = serde_json::from_slice(body).map_err(|e| format!("unreadable: {e}"))?;
    let rows = p
        .rows
        .unwrap_or_default()
        .iter()
        .map(row)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((rows, p.next_page_token.filter(|t| !t.is_empty())))
}

fn sip_trades(b: &[u8]) -> Result<(Vec<sip::Trade>, Option<String>), String> {
    page(b, sip::RawTrade::parse)
}
fn sip_quotes(b: &[u8]) -> Result<(Vec<sip::Quote>, Option<String>), String> {
    page(b, sip::RawQuote::parse)
}
fn iex_trades(b: &[u8]) -> Result<(Vec<iex::Trade>, Option<String>), String> {
    page(b, iex::RawTrade::parse)
}
fn iex_quotes(b: &[u8]) -> Result<(Vec<iex::Quote>, Option<String>), String> {
    page(b, iex::RawQuote::parse)
}

fn metadata(
    feed: Feed,
    symbol: &str,
    day: &str,
    schema: Option<Schema>,
    instrument: u32,
) -> Result<Metadata, String> {
    let start = day_ns(day).ok_or("bad day")?;
    let date = time::Date::parse(
        day,
        time::macros::format_description!("[year]-[month]-[day]"),
    )
    .map_err(|e| e.to_string())?;
    Ok(Metadata::builder()
        .dataset(feed.dataset())
        .schema(schema)
        .start(start)
        .end(NonZeroU64::new(start + 86_400 * 1_000_000_000))
        .stype_in(Some(SType::RawSymbol))
        .stype_out(SType::InstrumentId)
        .symbols(vec![symbol.to_owned()])
        .mappings(vec![SymbolMapping {
            raw_symbol: symbol.to_owned(),
            intervals: vec![dbn::MappingInterval {
                start_date: date,
                end_date: date.next_day().ok_or("no next day")?,
                symbol: instrument.to_string(),
            }],
        }])
        .build())
}

/// A zstd DBN file being written, a record at a time.
struct DbnFile {
    path: PathBuf,
    z: zstd::Encoder<'static, std::io::BufWriter<std::fs::File>>,
}

impl DbnFile {
    fn create(path: PathBuf, meta: &Metadata) -> Result<Self, String> {
        let file = std::fs::File::create(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        let mut z =
            zstd::Encoder::new(std::io::BufWriter::new(file), 3).map_err(|e| e.to_string())?;
        DbnMetadataEncoder::new(&mut z)
            .encode(meta)
            .map_err(|e| e.to_string())?;
        Ok(Self { path, z })
    }
    fn record<R: DbnEncodable>(&mut self, r: &R) -> Result<(), String> {
        dbn::encode::dbn::RecordEncoder::new(&mut self.z)
            .encode_record(r)
            .map_err(|e| format!("{}: {e}", self.path.display()))
    }
    fn raw(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.z
            .write_all(bytes)
            .map_err(|e| format!("{}: {e}", self.path.display()))
    }
    /// Finished and synced.
    fn finish(self) -> Result<(), String> {
        let buf = self.z.finish().map_err(|e| e.to_string())?;
        let file = buf.into_inner().map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())
    }
}

/// A record whose `F_LAST` is set once the next record's time is known.
trait Flagged: DbnEncodable {
    fn ts(&self) -> u64;
    fn mark_last(&mut self);
}
impl Flagged for Cmbp1Msg {
    fn ts(&self) -> u64 {
        self.hd.ts_event
    }
    fn mark_last(&mut self) {
        self.flags = FlagSet::empty().set_last();
    }
}
impl Flagged for Mbp1Msg {
    fn ts(&self) -> u64 {
        self.hd.ts_event
    }
    fn mark_last(&mut self) {
        self.flags = FlagSet::empty().set_last();
    }
}

/// The main file, one record behind: `F_LAST` is set unless the next record
/// has the same `ts_event`.
struct Main<R> {
    file: DbnFile,
    pending: Option<R>,
}

impl<R: Flagged> Main<R> {
    fn push(&mut self, rec: R) -> Result<(), String> {
        if let Some(mut p) = self.pending.take() {
            if p.ts() != rec.ts() {
                p.mark_last();
            }
            self.file.record(&p)?;
        }
        self.pending = Some(rec);
        Ok(())
    }
    fn finish(mut self) -> Result<(), String> {
        if let Some(mut p) = self.pending.take() {
            p.mark_last();
            self.file.record(&p)?;
        }
        self.file.finish()
    }
}

/// A row of either lane.
enum Either<T, Q> {
    Trade(T),
    Quote(Q),
}

/// Merge two lanes, each in time order, into tape order as they stream: by
/// time, a trade before a quote with the same time.
fn merge_lanes<T, Q>(
    trades: &mut Pages<T>,
    quotes: &mut Pages<Q>,
    tts: fn(&T) -> u64,
    qts: fn(&Q) -> u64,
    mut each: impl FnMut(Either<T, Q>) -> Result<(), String>,
) -> Result<(), String> {
    let (mut t, mut q) = (trades.next().transpose()?, quotes.next().transpose()?);
    loop {
        let take_trade = match (&t, &q) {
            (Some(a), Some(b)) => tts(a) <= qts(b),
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => return Ok(()),
        };
        if take_trade {
            let row = t.take().expect("peeked");
            let ts = tts(&row);
            t = trades.next().transpose()?;
            if t.as_ref().is_some_and(|n| tts(n) < ts) {
                return Err("trades are not in time order".into());
            }
            each(Either::Trade(row))?;
        } else {
            let row = q.take().expect("peeked");
            let ts = qts(&row);
            q = quotes.next().transpose()?;
            if q.as_ref().is_some_and(|n| qts(n) < ts) {
                return Err("quotes are not in time order".into());
            }
            each(Either::Quote(row))?;
        }
    }
}

/// What a file holds, read back: records by kind, with first and last times.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Tally {
    pub n: usize,
    pub first: Option<u64>,
    pub last: Option<u64>,
}

impl Tally {
    fn add(&mut self, t: u64) {
        self.first.get_or_insert(t);
        self.last = Some(t);
        self.n += 1;
    }
    fn of_lane(l: &LaneDone) -> Self {
        Self {
            n: l.rows,
            first: l.first,
            last: l.last,
        }
    }
}

/// The main file read back: its trades (`T`), its quotes (`M`), and anything
/// else (which must be nothing).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MainTally {
    pub trades: Tally,
    pub quotes: Tally,
    pub other: usize,
}

/// Read a main file back, streamed.
fn read_main(path: &Path) -> Result<MainTally, String> {
    let mut d = dbn::decode::dbn::Decoder::from_zstd_file(path).map_err(|e| e.to_string())?;
    let _ = d.metadata();
    let mut out = MainTally::default();
    while let Some(r) = d.decode_record_ref().map_err(|e| e.to_string())? {
        let action = r
            .get::<Cmbp1Msg>()
            .map(|m| m.action as u8)
            .or_else(|| r.get::<Mbp1Msg>().map(|m| m.action as u8));
        let t = r.header().ts_event;
        match action {
            Some(b'T') => out.trades.add(t),
            Some(b'M') => out.quotes.add(t),
            _ => out.other += 1,
        }
    }
    Ok(out)
}

/// Read a sidecar back, streamed: its records and their times.
fn read_back(path: &Path) -> Result<Tally, String> {
    let mut d = dbn::decode::dbn::Decoder::from_zstd_file(path).map_err(|e| e.to_string())?;
    let _ = d.metadata();
    let mut out = Tally::default();
    while let Some(r) = d.decode_record_ref().map_err(|e| e.to_string())? {
        out.add(r.header().ts_event);
    }
    Ok(out)
}

/// The day as written against what its pages said: trades against the trades
/// lane and quotes against the quotes lane, each by count and by first and
/// last time, so a lost trade and a duplicated quote can't cancel out; the
/// sidecars by count against theirs.
pub fn check_day(
    main: &MainTally,
    trades: &LaneDone,
    quotes: &LaneDone,
    sidecars: &[(&str, Tally)],
) -> Result<(), String> {
    let mut wrong = Vec::new();
    for (name, got, want) in [
        ("trades", main.trades, Tally::of_lane(trades)),
        ("quotes", main.quotes, Tally::of_lane(quotes)),
    ] {
        if got != want {
            wrong.push(format!(
                "{} {name} at {:?}..{:?}, the pages said {} at {:?}..{:?}",
                got.n, got.first, got.last, want.n, want.first, want.last
            ));
        }
    }
    if main.other > 0 {
        wrong.push(format!(
            "{} records that are neither trades nor quotes",
            main.other
        ));
    }
    for (schema, got) in sidecars {
        let want = if *schema == QUOTE_CONDITIONS {
            main.quotes
        } else {
            main.trades
        };
        if *got != want {
            wrong.push(format!(
                "{} {schema} records at {:?}..{:?} for {} rows at {:?}..{:?}",
                got.n, got.first, got.last, want.n, want.first, want.last
            ));
        }
    }
    if wrong.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "the written day doesn't match its pages: {}",
            wrong.join("; ")
        ))
    }
}

/// The size and SHA-256 of a file, streamed.
fn sha(path: &Path) -> Result<(u64, String), String> {
    let mut f = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut h = Sha256::new();
    let n = std::io::copy(&mut f, &mut h).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok((n, format!("{:x}", h.finalize())))
}

/// Where a day's lanes are spooled while they're fetched.
fn lanes_dir(feed: Feed, symbol: &str, day: &str, dir: &Path) -> PathBuf {
    dir.join("lanes").join(&feed.batches(symbol, day)[0])
}

/// Removes a day's spooled lanes (its pages).
pub fn clear_lanes(feed: Feed, symbol: &str, day: &str, dir: &Path) {
    let _ = std::fs::remove_dir_all(lanes_dir(feed, symbol, day, dir));
    // The folder of lanes too, once no day has any (it fails while one does).
    let _ = std::fs::remove_dir(dir.join("lanes"));
}

/// Fetch (or resume from the spool), encode and write one symbol-day into
/// `dir` as its batch folders. A page that doesn't make sense clears the
/// day's lanes, so the next attempt fetches it afresh; anything transient
/// keeps them.
pub async fn build(
    alpaca: &Alpaca,
    feed: Feed,
    symbol: &str,
    day: &str,
    dir: &Path,
) -> Result<Built, Failure> {
    let lanes = lanes_dir(feed, symbol, day, dir);
    let f = feed.name();
    let (ts, qs) = (
        LaneSpec {
            feed: f,
            symbol,
            kind: "trades",
            day,
        },
        LaneSpec {
            feed: f,
            symbol,
            kind: "quotes",
            day,
        },
    );
    let (tdir, qdir) = (lanes.join("trades"), lanes.join("quotes"));
    // Both lanes at once, sharing the pace. Both are awaited to the end (never
    // dropped mid-commit): one that fails stops the other at its next page, so
    // nothing writes to the day's spool once `build` returns.
    let stop = AtomicBool::new(false);
    let (t, q) = match feed {
        Feed::Sip => tokio::join!(
            lane(alpaca, &ts, &tdir, sip_trades, |r: &sip::Trade| r.ts, &stop),
            lane(alpaca, &qs, &qdir, sip_quotes, |r: &sip::Quote| r.ts, &stop),
        ),
        Feed::Iex => tokio::join!(
            lane(alpaca, &ts, &tdir, iex_trades, |r: &iex::Trade| r.ts, &stop),
            lane(alpaca, &qs, &qdir, iex_quotes, |r: &iex::Quote| r.ts, &stop),
        ),
    };
    // The lane that failed, rather than the one it stopped.
    let fetched = match (t, q) {
        (Ok(t), Ok(q)) => Ok((t, q)),
        (Err(Failure::Paused), Err(e)) | (Err(e), _) | (_, Err(e)) => Err(e),
    };
    let (t, q) = fetched.inspect_err(|e| {
        if matches!(e, Failure::PageIntegrity(_)) {
            clear_lanes(feed, symbol, day, dir);
        }
    })?;
    let (sym, d, out, l) = (
        symbol.to_owned(),
        day.to_owned(),
        dir.to_path_buf(),
        lanes.clone(),
    );
    let built = tokio::task::spawn_blocking(move || write_day(feed, &sym, &d, &out, &l, t, q))
        .await
        .unwrap_or_else(|e| Err(Failure::Spool(format!("writing the day: {e}"))));
    match &built {
        // Written and checked: the pages aren't needed any more.
        Ok(_) | Err(Failure::PageIntegrity(_)) => clear_lanes(feed, symbol, day, dir),
        Err(_) => {}
    }
    built
}

/// Stream a day's spooled lanes into its batch folders, read them back and
/// check them, and write their manifests and the day's complete marker.
fn write_day(
    feed: Feed,
    symbol: &str,
    day: &str,
    dir: &Path,
    lanes: &Path,
    t: LaneDone,
    q: LaneDone,
) -> Result<Built, Failure> {
    let bad = Failure::PageIntegrity;
    let disk = Failure::Spool;
    let ids = feed.batches(symbol, day);
    let d = day.replace('-', "");
    let schema = feed.schema();
    let dirs: Vec<PathBuf> = ids.iter().map(|id| dir.join(id)).collect();
    // Not complete until every batch is written and checked.
    let marker = complete_marker(feed, symbol, day, dir);
    if marker.exists() {
        std::fs::remove_file(&marker).map_err(|e| disk(e.to_string()))?;
    }
    for p in &dirs {
        // A leftover from an interrupted attempt is rebuilt from scratch.
        let _ = std::fs::remove_dir_all(p);
        std::fs::create_dir_all(p).map_err(|e| disk(format!("{}: {e}", p.display())))?;
    }
    let mut names = vec![format!(
        "alpaca-{}-{d}.{}.dbn.zst",
        feed.name(),
        schema.as_str()
    )];
    names.extend(
        feed.sidecars()
            .iter()
            .map(|s| format!("alpaca-{}-{d}.{s}.dbn.zst", feed.name())),
    );
    let paths: Vec<PathBuf> = dirs.iter().zip(&names).map(|(d, n)| d.join(n)).collect();
    let instrument = match feed {
        Feed::Sip => sip::instrument_id(symbol),
        Feed::Iex => iex::instrument_id(symbol),
    };
    let meta = metadata(feed, symbol, day, Some(schema), instrument).map_err(bad)?;
    let side_meta = metadata(feed, symbol, day, None, instrument).map_err(bad)?;
    let mut cond = DbnFile::create(paths[1].clone(), &side_meta).map_err(disk)?;
    let (tl, ql) = (
        LaneSpool::at(lanes.join("trades")),
        LaneSpool::at(lanes.join("quotes")),
    );
    // Errors while streaming: a page that doesn't read back is the pages'
    // (fetched again), a trade the layout can't hold the day's for good
    // (fetching again would only repeat it), a file that can't be written the
    // disk's (tried later). A quote whose conditions can't be held is never
    // an error: its conditions record is marked unparsed, and counted.
    let failure: RefCell<Option<Failure>> = RefCell::new(None);
    let io = |e: String| {
        failure
            .borrow_mut()
            .get_or_insert(Failure::Spool(e.clone()));
        e
    };
    let refuse = |e: String| {
        failure
            .borrow_mut()
            .get_or_insert(Failure::Encode(e.clone()));
        e
    };
    let unparsed = Cell::new(0usize);
    let peak;
    let streamed = match feed {
        Feed::Sip => {
            let mut qcond = DbnFile::create(paths[2].clone(), &side_meta).map_err(disk)?;
            let mut main = Main::<Cmbp1Msg> {
                file: DbnFile::create(paths[0].clone(), &meta).map_err(disk)?,
                pending: None,
            };
            let mut enc = sip::Encoder::new(symbol);
            let (mut tp, mut qp) = (
                Pages::new(tl, t.receipts.clone(), sip_trades as Parse<_>),
                Pages::new(ql, q.receipts.clone(), sip_quotes as Parse<_>),
            );
            let r = merge_lanes(
                &mut tp,
                &mut qp,
                |r: &sip::Trade| r.ts,
                |r: &sip::Quote| r.ts,
                |row| {
                    let row = match row {
                        Either::Trade(t) => sip::Row::Trade(t),
                        Either::Quote(q) => sip::Row::Quote(q),
                    };
                    let (rec, c) = enc.push(&row).map_err(refuse)?;
                    main.push(rec).map_err(io)?;
                    match c {
                        sip::Conditions::Trade(c) => cond
                            .raw(&c.encode().map_err(|e| refuse(e.to_string()))?)
                            .map_err(io),
                        sip::Conditions::Quote(c) => {
                            if c.unparsed {
                                unparsed.set(unparsed.get() + 1);
                            }
                            qcond
                                .raw(&c.encode().map_err(|e| refuse(e.to_string()))?)
                                .map_err(io)
                        }
                    }
                },
            )
            .and_then(|()| main.finish().map_err(io))
            .and_then(|()| qcond.finish().map_err(io));
            peak = tp.peak.max(qp.peak);
            r
        }
        Feed::Iex => {
            let mut main = Main::<Mbp1Msg> {
                file: DbnFile::create(paths[0].clone(), &meta).map_err(disk)?,
                pending: None,
            };
            let mut enc = iex::Encoder::new(symbol);
            let (mut tp, mut qp) = (
                Pages::new(tl, t.receipts.clone(), iex_trades as Parse<_>),
                Pages::new(ql, q.receipts.clone(), iex_quotes as Parse<_>),
            );
            let r = merge_lanes(
                &mut tp,
                &mut qp,
                |r: &iex::Trade| r.ts,
                |r: &iex::Quote| r.ts,
                |row| match row {
                    Either::Trade(t) => {
                        let (rec, c) = enc.trade(&t, UNDEF_TIMESTAMP);
                        let c =
                            c.map_err(|e| refuse(format!("trade {} at {}: {e}", t.id, t.ts)))?;
                        main.push(rec).map_err(io)?;
                        cond.raw(&c.encode().map_err(|e| refuse(e.to_string()))?)
                            .map_err(io)
                    }
                    Either::Quote(q) => main.push(enc.quote(&q, UNDEF_TIMESTAMP)).map_err(io),
                },
            )
            .and_then(|()| main.finish().map_err(io));
            peak = tp.peak.max(qp.peak);
            r
        }
    }
    .and_then(|()| cond.finish().map_err(io));
    if let Err(e) = streamed {
        return Err(failure.into_inner().unwrap_or(Failure::PageIntegrity(e)));
    }

    // Read back: trades and quotes each against their lane, sidecars against them.
    let main_tally = read_main(&paths[0]).map_err(bad)?;
    let mut sides = Vec::new();
    for (s, p) in feed.sidecars().iter().zip(&paths[1..]) {
        sides.push((*s, read_back(p).map_err(bad)?));
    }
    check_day(&main_tally, &t, &q, &sides).map_err(bad)?;
    let outcome = Outcome {
        trades: t.rows,
        quotes: q.rows,
        pages: t.receipts.len() + q.receipts.len(),
        bytes: 0,
        unparsed_quote_conditions: unparsed.get(),
        encoder: ENCODER.to_owned(),
    };
    let mut built = write_manifests(
        feed, symbol, day, &ids, &dirs, &names, &paths, t, q, main_tally, outcome,
    )
    .map_err(disk)?;
    built.peak_rows = peak;
    Ok(built)
}

#[allow(clippy::too_many_arguments)]
fn write_manifests(
    feed: Feed,
    symbol: &str,
    day: &str,
    ids: &[String],
    dirs: &[PathBuf],
    names: &[String],
    paths: &[PathBuf],
    t: LaneDone,
    q: LaneDone,
    main: MainTally,
    mut outcome: Outcome,
) -> Result<Built, String> {
    let schema = feed.schema();
    let mut sums = Vec::new();
    for p in paths {
        sums.push(sha(p)?);
    }
    outcome.bytes = sums.iter().map(|(n, _)| n).sum();
    let (msize, msha) = sums[0].clone();
    let request = json!({
        "provider": "alpaca", "feed": feed.name(), "symbol": symbol, "day": day,
        "window": [format!("{day}T00:00:00Z"), format!("{day}T23:59:59.999999999Z")], "limit": 10000, "sort": "asc",
    });
    let encoder = format!(
        "tinytrader-alpaca {} ({})",
        env!("CARGO_PKG_VERSION"),
        schema.as_str()
    );
    let first = [main.trades.first, main.quotes.first]
        .into_iter()
        .flatten()
        .min();
    let last = [main.trades.last, main.quotes.last]
        .into_iter()
        .flatten()
        .max();
    let receipt = json!({
        "format": "tinytrader.alpaca-history.receipts/1", "request": request, "encoder": encoder,
        "fetched_at": tinytrader_observe::unix_seconds(), "trades": outcome.trades, "quotes": outcome.quotes,
        "records": main.trades.n + main.quotes.n, "first_ns": first, "last_ns": last, "file": { "name": names[0], "size": msize, "sha256": msha },
        "pages": { "trades": t.receipts, "quotes": q.receipts },
    });
    let main_ref = json!({ "name": names[0], "size": msize, "sha256": msha });
    let binding = |s: &str| match s {
        QUOTE_CONDITIONS => json!({
            "format": "tinytrader.alpaca-history.quote-conditions-binding/1", "batch": ids[0],
            "main": main_ref, "quotes": outcome.quotes, "unparsed": outcome.unparsed_quote_conditions,
            "rule": "the nth quote conditions record belongs to the nth quote (action M) of the main file; an unparsed one (flag bit 0) holds no codes, the venue's couldn't be held",
        }),
        _ => json!({
            "format": "tinytrader.alpaca-history.conditions-binding/1", "batch": ids[0],
            "main": main_ref, "trades": outcome.trades,
            "rule": "the nth conditions record belongs to the nth trade (action T) of the main file",
        }),
    };
    for (i, dir) in dirs.iter().enumerate() {
        let (s, extra, extra_name) = if i == 0 {
            (schema.as_str(), receipt.clone(), "receipts.json")
        } else {
            let s = feed.sidecars()[i - 1];
            (s, binding(s), "binding.json")
        };
        let md = json!({
            "job_id": ids[i],
            "query": { "dataset": feed.dataset(), "schema": s, "symbols": [symbol], "stype_in": "raw_symbol",
                       "start": format!("{day}T00:00:00Z"), "end": format!("{day}T23:59:59.999999999Z"),
                       "encoding": "dbn", "compression": "zstd", "provider": "alpaca", "feed": feed.name() },
            "customizations": { "split_duration": "day", "split_symbols": false },
        });
        let (size, hash) = &sums[i];
        let mut files =
            vec![json!({ "filename": names[i], "size": size, "hash": format!("sha256:{hash}") })];
        for (name, v) in [("metadata.json", &md), (extra_name, &extra)] {
            let text = serde_json::to_vec_pretty(v).map_err(|e| e.to_string())?;
            let p = dir.join(name);
            std::fs::write(&p, &text).map_err(|e| format!("{}: {e}", p.display()))?;
            files.push(json!({ "filename": name, "size": text.len(), "hash": format!("sha256:{:x}", Sha256::digest(&text)) }));
        }
        let manifest = json!({ "job_id": ids[i], "files": files });
        std::fs::write(
            dir.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
    }
    let marker = complete_marker(feed, symbol, day, dirs[0].parent().ok_or("no spool")?);
    std::fs::write(
        &marker,
        serde_json::to_vec_pretty(&outcome).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    Ok(Built {
        batches: dirs.to_vec(),
        outcome,
        peak_rows: 0,
    })
}

/// The file that says a day's spool is complete: every batch written and checked
/// against its pages. It holds the day's [`Outcome`].
fn complete_marker(feed: Feed, symbol: &str, day: &str, dir: &Path) -> PathBuf {
    dir.join(format!("{}.complete.json", feed.batches(symbol, day)[0]))
}

/// A day's complete spool, kept from an earlier pass (the store was busy): its
/// batch folders and outcome, if the marker is there and every file still matches
/// its manifest. Filing from it files exactly what was built, without asking
/// Alpaca again, so a day whose data changed upstream since isn't refused. It reads
/// every file, streamed: call it off the async runtime. `Ok(None)` when there's no
/// complete spool (none, or damaged: a file missing or changed, or a batch
/// missing, as for a SIP day built before quote conditions), `Err` when it
/// couldn't be read (the day is tried later, the spool kept).
pub fn spooled(feed: Feed, symbol: &str, day: &str, dir: &Path) -> Result<Option<Built>, String> {
    use std::io::ErrorKind::NotFound;
    let marker = complete_marker(feed, symbol, day, dir);
    let text = match std::fs::read(&marker) {
        Ok(t) => t,
        Err(e) if e.kind() == NotFound => return Ok(None),
        Err(e) => return Err(format!("{}: {e}", marker.display())),
    };
    let Ok(outcome) = serde_json::from_slice::<Outcome>(&text) else {
        return Ok(None);
    };
    // Built by another encoder: rebuilt, not filed.
    if outcome.encoder != ENCODER {
        return Ok(None);
    }
    let dirs: Vec<PathBuf> = feed
        .batches(symbol, day)
        .into_iter()
        .map(|id| dir.join(id))
        .collect();
    // Whether a batch matches its manifest: a missing or changed file is a damaged
    // spool (`false`, rebuilt); any other error reading it is `Err` (try later).
    let intact = |d: &Path| -> Result<bool, String> {
        let read = |p: PathBuf| match std::fs::File::open(&p) {
            Ok(f) => Ok(Some(f)),
            Err(e) if e.kind() == NotFound => Ok(None),
            Err(e) => Err(format!("{}: {e}", p.display())),
        };
        let Some(mut mf) = read(d.join("manifest.json"))? else {
            return Ok(false);
        };
        let mut text = Vec::new();
        std::io::Read::read_to_end(&mut mf, &mut text)
            .map_err(|e| format!("{}: {e}", d.display()))?;
        let Ok(m) = serde_json::from_slice::<Value>(&text) else {
            return Ok(false);
        };
        let Some(files) = m["files"].as_array() else {
            return Ok(false);
        };
        for f in files {
            let (Some(name), Some(size), Some(hash)) = (
                f["filename"].as_str(),
                f["size"].as_u64(),
                f["hash"].as_str(),
            ) else {
                return Ok(false);
            };
            let Some(mut file) = read(d.join(name))? else {
                return Ok(false);
            };
            let mut h = Sha256::new();
            let n = std::io::copy(&mut file, &mut h)
                .map_err(|e| format!("{}: {e}", d.join(name).display()))?;
            if n != size || hash != format!("sha256:{:x}", h.finalize()) {
                return Ok(false);
            }
        }
        Ok(true)
    };
    for d in &dirs {
        if !intact(d)? {
            return Ok(None);
        }
    }
    Ok(Some(Built {
        batches: dirs,
        outcome,
        peak_rows: 0,
    }))
}

/// What a conditions batch says it belongs to: the main file's SHA-256, from its
/// `binding.json`.
fn bound_main(cond_dir: &Path) -> Result<String, String> {
    let b: Value = serde_json::from_slice(
        &std::fs::read(cond_dir.join("binding.json")).map_err(|e| format!("binding.json: {e}"))?,
    )
    .map_err(|e| format!("binding.json: {e}"))?;
    b["main"]["sha256"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| "binding.json names no main sha256".into())
}

/// The SHA-256 of a file the store serves, streamed: never the whole file in
/// memory.
async fn hash_served(http: &reqwest::Client, store: &str, name: &str) -> Result<String, String> {
    let mut r = http
        .get(format!("{store}/files/{name}"))
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| format!("store: {e}"))?;
    let mut h = Sha256::new();
    while let Some(chunk) = r.chunk().await.map_err(|e| format!("store: {e}"))? {
        h.update(&chunk);
    }
    Ok(format!("{:x}", h.finalize()))
}

/// Before conditions are filed alone (their main filed on an earlier pass), check
/// that they were built against the main the store holds: the binding's main
/// SHA-256 against the held day's. The store doesn't check this itself.
///
/// The held day is asked of `/coverage`, which names its files in whichever layout
/// and on every volume holding a copy, with its SHA-256 when the store has one;
/// otherwise each copy is hashed as it streams. Every copy must agree.
///
/// `Err` with `unbound` only when the held main is known and differs (the
/// provider's data changed since main was filed): nothing is filed, a person
/// decides. Anything else that stops the check (the store unreachable or busy, the
/// day not listed, its volume away, copies that disagree) is `later`: the day
/// waits with its spool kept.
pub async fn check_binding(
    http: &reqwest::Client,
    store: &str,
    feed: Feed,
    symbol: &str,
    day: &str,
    cond_dir: &Path,
) -> Result<(), FileError> {
    let want = bound_main(cond_dir).map_err(FileError::later)?;
    let r = http
        .get(format!("{store}/coverage"))
        .query(&[
            ("provider", "alpaca"),
            ("dataset", feed.dataset()),
            ("symbol", symbol),
            ("schema", feed.schema().as_str()),
            ("start", day),
            ("end", day),
        ])
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|e| FileError::later(format!("store coverage: {e}")))?;
    let cov: Value = r
        .json()
        .await
        .map_err(|e| FileError::later(format!("store coverage: {e}")))?;
    let rows: Vec<&Value> = cov["days"]
        .as_array()
        .map(|d| d.iter().filter(|r| r["day"] == day).collect())
        .unwrap_or_default();
    let [row] = rows.as_slice() else {
        let away = cov["missing_volumes"]
            .as_array()
            .filter(|m| !m.is_empty())
            .map(|m| format!("; volumes away: {}", Value::Array(m.clone())))
            .unwrap_or_default();
        return Err(FileError::later(format!(
            "the store lists {} rows for the held main of {symbol} {day}{away}",
            rows.len()
        )));
    };
    if row["conflict"] == true {
        return Err(FileError::later(format!(
            "the store's copies of {symbol} {day} differ"
        )));
    }
    // Only the day's data file in the store layout, on each volume holding it; an
    // older day folder also lists its sidecars, and a batch download names a range
    // file, neither of which is the main as filed.
    let filed = format!(
        "/{}/{symbol}/{}/{day}.dbn.zst",
        feed.dataset(),
        feed.schema().as_str()
    )
    .to_lowercase();
    let files: Vec<&str> = row["files"]
        .as_array()
        .map(|f| {
            f.iter()
                .filter_map(Value::as_str)
                .filter(|n| n.to_lowercase().ends_with(&filed))
                .collect()
        })
        .unwrap_or_default();
    if files.is_empty() {
        return Err(FileError::later(format!(
            "the store holds {symbol} {day}, but not as a filed day in its layout: {}",
            row["files"]
        )));
    }
    let mut held: Vec<(String, String)> = Vec::new();
    match row["sha256"].as_str() {
        Some(h) => held.push((files.join(", "), h.to_owned())),
        None => {
            for f in &files {
                held.push((
                    (*f).to_owned(),
                    hash_served(http, store, f)
                        .await
                        .map_err(FileError::later)?,
                ));
            }
        }
    }
    if held.iter().any(|(_, h)| *h != held[0].1) {
        return Err(FileError::later(format!(
            "the store's copies of {symbol} {day} have different hashes: {held:?}"
        )));
    }
    let (name, got) = &held[0];
    if *got == want {
        Ok(())
    } else {
        Err(FileError {
            busy: false,
            unbound: true,
            later: false,
            message: format!(
                "conditions built against main sha256 {want}, but the store holds {name} with sha256 {got}: the provider's data changed since main was filed; not filed"
            ),
        })
    }
}

/// Removes a day's spool: its batch folders, its marker and its lanes.
pub fn clear_spool(feed: Feed, symbol: &str, day: &str, dir: &Path) {
    let _ = std::fs::remove_file(complete_marker(feed, symbol, day, dir));
    for id in feed.batches(symbol, day) {
        let _ = std::fs::remove_dir_all(dir.join(id));
    }
    clear_lanes(feed, symbol, day, dir);
}

/// The newest modification time of a path and what's below it, two levels deep.
fn newest(p: &Path, depth: u32) -> Option<std::time::SystemTime> {
    let mut t = std::fs::metadata(p).and_then(|m| m.modified()).ok()?;
    if depth > 0
        && let Ok(rd) = std::fs::read_dir(p)
    {
        for e in rd.flatten() {
            if let Some(u) = newest(&e.path(), depth - 1) {
                t = t.max(u);
            }
        }
    }
    Some(t)
}

/// Why a day's spool is kept from the sweep.
pub enum Keep {
    /// Still wanted by a run that isn't finished: never swept for age (its
    /// batches may still be rebuilt if another encoder built them).
    Wanted(String),
    /// Kept for a person (conditions that don't bind): never touched.
    Untouchable(String),
}

/// A day's store batch id split: (feed name, symbol, `YYYY-MM-DD`).
pub fn parse_batch(main: &str) -> Option<(String, String, String)> {
    let rest = main.strip_prefix("ALPACA-")?;
    let (feed, rest) = rest.split_once('-')?;
    let (sym, d) = rest.rsplit_once('-')?;
    (d.len() == 8 && d.bytes().all(|b| b.is_ascii_digit())).then(|| {
        (
            feed.to_lowercase(),
            sym.to_owned(),
            format!("{}-{}-{}", &d[..4], &d[4..6], &d[6..]),
        )
    })
}

/// At startup, bound the spool: a day whose spool (batches, marker, lanes) is
/// untouched for `max_age` is removed whole; a complete spool built by
/// another encoder loses its batches and marker (its fetched pages, if any,
/// are kept: they're Alpaca's bytes, not the encoder's). `keep` says, by a
/// day's main batch id, what mustn't be: a day an unfinished run still wants
/// isn't swept for age, and one kept for a person isn't touched. Returns
/// every day swept or kept past its age, and why (`true` when swept).
pub fn sweep(
    dir: &Path,
    max_age: std::time::Duration,
    keep: impl Fn(&str) -> Option<Keep>,
) -> Vec<(String, bool, String)> {
    use std::collections::BTreeMap;
    let mut days: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    let day_of = |name: &str| -> String {
        let n = name.strip_suffix(".complete.json").unwrap_or(name);
        let n = n.strip_suffix(&format!("-{QUOTE_CONDITIONS}")).unwrap_or(n);
        n.strip_suffix(&format!("-{CONDITIONS}"))
            .unwrap_or(n)
            .to_owned()
    };
    for (base, _) in [(dir.to_path_buf(), ()), (dir.join("lanes"), ())] {
        let Ok(rd) = std::fs::read_dir(&base) else {
            continue;
        };
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name.starts_with("ALPACA-") {
                days.entry(day_of(&name)).or_default().push(e.path());
            }
        }
    }
    let now = std::time::SystemTime::now();
    let mut gone = Vec::new();
    for (day, paths) in days {
        let latest = paths.iter().filter_map(|p| newest(p, 2)).max();
        let old = latest.is_some_and(|t| now.duration_since(t).unwrap_or_default() > max_age);
        let marker = dir.join(format!("{day}.complete.json"));
        let other_encoder = std::fs::read(&marker)
            .ok()
            .and_then(|b| serde_json::from_slice::<Outcome>(&b).ok())
            .is_some_and(|o| o.encoder != ENCODER);
        let kept = keep(&day);
        if let Some(Keep::Untouchable(why)) = kept {
            if old || other_encoder {
                gone.push((day, false, format!("kept: {why}")));
            }
            continue;
        }
        if let (true, Some(Keep::Wanted(why))) = (old, &kept) {
            if other_encoder {
                for p in paths.iter().filter(|p| !p.starts_with(dir.join("lanes"))) {
                    let _ = std::fs::remove_dir_all(p).or_else(|_| std::fs::remove_file(p));
                }
                gone.push((
                    day.clone(),
                    true,
                    "its batches were built by another encoder; its pages are kept".into(),
                ));
            }
            gone.push((day, false, format!("kept past its age: {why}")));
            continue;
        }
        if old {
            for p in &paths {
                let _ = std::fs::remove_dir_all(p).or_else(|_| std::fs::remove_file(p));
            }
            gone.push((
                day,
                true,
                format!(
                    "its spool was untouched for over {} days",
                    max_age.as_secs() / 86_400
                ),
            ));
        } else if other_encoder {
            for p in paths.iter().filter(|p| !p.starts_with(dir.join("lanes"))) {
                let _ = std::fs::remove_dir_all(p).or_else(|_| std::fs::remove_file(p));
            }
            gone.push((day, true, "its batches were built by another encoder; they're built again from its pages or a new fetch".into()));
        }
    }
    let _ = std::fs::remove_dir(dir.join("lanes"));
    gone
}

/// A filing that didn't happen: the store busy past the wait (`busy`: try the day
/// again later, keeping its spool), or a refusal or error.
#[derive(Debug)]
pub struct FileError {
    pub busy: bool,
    /// Conditions that don't belong to the main the store holds.
    pub unbound: bool,
    /// Couldn't tell yet (the store unreachable or busy, the held main on a volume
    /// that's away): the day is deferred, its spool kept.
    pub later: bool,
    pub message: String,
}

impl FileError {
    fn later(message: String) -> Self {
        Self {
            busy: false,
            unbound: false,
            later: true,
            message,
        }
    }
}

impl From<String> for FileError {
    fn from(message: String) -> Self {
        Self {
            busy: false,
            unbound: false,
            later: false,
            message,
        }
    }
}

impl From<&str> for FileError {
    fn from(message: &str) -> Self {
        Self {
            busy: false,
            unbound: false,
            later: false,
            message: message.to_owned(),
        }
    }
}

/// How long a busy store (its filing queue full, 503) is waited for, asking again
/// longer apart each time.
const BUSY_WAIT: std::time::Duration = if cfg!(test) {
    std::time::Duration::from_millis(300)
} else {
    std::time::Duration::from_secs(15 * 60)
};

/// Hand a batch folder to the store: every file into its incoming area
/// (checked by size and SHA-256 on arrival), then file it. `token` is the
/// store's write token, when it requires one. A busy store (503) is waited for,
/// up to `BUSY_WAIT`; past that the error is `busy`.
pub async fn file(
    http: &reqwest::Client,
    store: &str,
    token: Option<&str>,
    batch_dir: &Path,
) -> Result<Value, FileError> {
    let write = |req: reqwest::RequestBuilder| match token {
        Some(t) => req.bearer_auth(t),
        None => req,
    };
    let id = batch_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("bad batch folder")?;
    // Only the store's explicit refusal (409, 422, with its reason) is final; a
    // network error or any other status leaves the day to be tried again, its
    // spool kept.
    let unfiled = |what: String, status: reqwest::StatusCode, body: String| {
        let message = format!("{what}: {status} {body}");
        if status == reqwest::StatusCode::CONFLICT
            || status == reqwest::StatusCode::UNPROCESSABLE_ENTITY
        {
            FileError::from(message)
        } else {
            FileError::later(message)
        }
    };
    let mut names: Vec<String> = std::fs::read_dir(batch_dir)
        .map_err(|e| e.to_string())?
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .collect();
    names.sort();
    for name in &names {
        // Hashed, then sent, as streams: never the whole file in memory.
        let path = batch_dir.join(name);
        let p = path.clone();
        let (size, hash) = tokio::task::spawn_blocking(move || sha(&p))
            .await
            .map_err(|e| e.to_string())??;
        let body = tokio::fs::File::open(&path)
            .await
            .map_err(|e| format!("{}: {e}", path.display()))?;
        let r = write(http.put(format!(
            "{store}/incoming/alpaca/{id}/{name}?size={size}&sha256={hash}"
        )))
        .header(reqwest::header::CONTENT_LENGTH, size)
        .body(reqwest::Body::from(body))
        .send()
        .await
        .map_err(|e| FileError::later(format!("store: {e}")))?;
        if !r.status().is_success() {
            let status = r.status();
            return Err(unfiled(
                format!("store didn't take {name}"),
                status,
                r.text().await.unwrap_or_default(),
            ));
        }
    }
    let until = std::time::Instant::now() + BUSY_WAIT;
    let mut wait = std::time::Duration::from_millis(if cfg!(test) { 10 } else { 5_000 });
    let r = loop {
        let r = write(http.post(format!("{store}/incorporate/alpaca/{id}")))
            .send()
            .await
            .map_err(|e| FileError::later(format!("store: {e}")))?;
        if r.status() != reqwest::StatusCode::SERVICE_UNAVAILABLE {
            break r;
        }
        if std::time::Instant::now() + wait > until {
            return Err(FileError {
                busy: true,
                unbound: false,
                later: false,
                message: format!("the store stayed busy filing {id}"),
            });
        }
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(std::time::Duration::from_secs(60));
    };
    let status = r.status();
    let v: Value = r.json().await.unwrap_or(Value::Null);
    if !status.is_success() {
        return Err(unfiled(
            format!("store didn't file {id}"),
            status,
            v.to_string(),
        ));
    }
    Ok(v)
}

#[cfg(test)]
mod filing_tests {
    use super::file;
    use axum::routing::{post, put};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A store answering incorporate 503 `busy` times, then filing.
    async fn store(busy: u32) -> String {
        let left = Arc::new(AtomicU32::new(busy));
        let app = axum::Router::new()
            .route("/incoming/alpaca/{id}/{name}", put(|| async { "ok" }))
            .route(
                "/incorporate/alpaca/{id}",
                post(move || {
                    let left = Arc::clone(&left);
                    async move {
                        if left.load(Ordering::SeqCst) > 0 {
                            left.fetch_sub(1, Ordering::SeqCst);
                            return (
                                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                r#"{"error":"busy"}"#,
                            );
                        }
                        (axum::http::StatusCode::OK, r#"{"filed":true}"#)
                    }
                }),
            );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        format!("http://{addr}")
    }

    fn batch(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("ah-filing-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("x.dbn"), b"x").unwrap();
        d
    }

    /// A busy store is waited for, and then the batch is filed.
    #[tokio::test]
    async fn a_busy_store_is_waited_for() {
        let url = store(2).await;
        let r = file(&reqwest::Client::new(), &url, None, &batch("wait")).await;
        assert!(r.is_ok(), "{r:?}");
    }

    /// One busy past the wait is a `busy` error: the day is tried again, its spool
    /// kept.
    #[tokio::test]
    async fn a_store_busy_past_the_wait_is_busy() {
        let url = store(1000).await;
        let e = file(&reqwest::Client::new(), &url, None, &batch("busy"))
            .await
            .unwrap_err();
        assert!(e.busy, "{e:?}");
    }

    /// A store answering incorporate (or, with `at_put`, the upload) with `status`.
    async fn answering(status: u16, at_put: bool) -> String {
        let code = axum::http::StatusCode::from_u16(status).unwrap();
        let ok = axum::http::StatusCode::OK;
        let app = axum::Router::new()
            .route(
                "/incoming/alpaca/{id}/{name}",
                put(move || async move { (if at_put { code } else { ok }, "{}") }),
            )
            .route(
                "/incorporate/alpaca/{id}",
                post(move || async move { (code, r#"{"error":"the reason"}"#) }),
            );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        format!("http://{addr}")
    }

    /// Only the store's explicit refusal (409, 422) is final; a network error or any
    /// other status, at the upload or the filing, is tried again with the spool kept.
    #[tokio::test]
    async fn only_an_explicit_refusal_is_final() {
        let http = reqwest::Client::new();
        for (status, at_put) in [(409, false), (422, false), (409, true)] {
            let e = file(
                &http,
                &answering(status, at_put).await,
                None,
                &batch("refused"),
            )
            .await
            .unwrap_err();
            assert!(
                !e.later && !e.busy && !e.unbound,
                "{status} {at_put}: {e:?}"
            );
            assert!(e.message.contains(&status.to_string()), "{e:?}");
        }
        for (status, at_put) in [
            (500, false),
            (502, false),
            (500, true),
            (404, false),
            (401, true),
        ] {
            let e = file(
                &http,
                &answering(status, at_put).await,
                None,
                &batch("later"),
            )
            .await
            .unwrap_err();
            assert!(e.later, "{status} {at_put}: {e:?}");
        }
        let e = file(&http, "http://127.0.0.1:9", None, &batch("no-store"))
            .await
            .unwrap_err();
        assert!(e.later, "{e:?}");
    }
}

#[cfg(test)]
mod spool_tests {
    use super::{Feed, build, check_binding, clear_spool, spooled};
    use crate::fetch::Alpaca;
    use crate::testing::{FakeAlpaca, Shape};
    use axum::routing::get;

    const DAY: &str = "2026-10-01";

    /// A fake Alpaca with one trade and one quote for `DAY`.
    async fn alpaca() -> Alpaca {
        FakeAlpaca::start(Shape::new(1, 1), Shape::new(1, 1))
            .await
            .client()
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("alpaca-spool-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn a_complete_spool_is_found_and_a_damaged_one_is_not() {
        let dir = scratch("complete");
        assert!(
            spooled(Feed::Sip, "SPY", DAY, &dir).unwrap().is_none(),
            "nothing spooled yet"
        );
        let built = build(&alpaca().await, Feed::Sip, "SPY", DAY, &dir)
            .await
            .unwrap();
        let (m, out) = (built.batches[0].clone(), built.outcome);
        let kept = spooled(Feed::Sip, "SPY", DAY, &dir)
            .unwrap()
            .expect("the built day is kept");
        assert_eq!(kept.batches, built.batches);
        let kept = kept.outcome;
        assert_eq!(
            (kept.trades, kept.quotes, kept.pages, kept.bytes),
            (out.trades, out.quotes, out.pages, out.bytes)
        );
        // A file changed since it was built: not filed from the spool.
        let data = std::fs::read_dir(&m)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.to_string_lossy().ends_with(".dbn.zst"))
            .unwrap();
        let original = std::fs::read(&data).unwrap();
        std::fs::write(&data, b"changed").unwrap();
        assert!(spooled(Feed::Sip, "SPY", DAY, &dir).unwrap().is_none());
        std::fs::write(&data, &original).unwrap();
        assert!(spooled(Feed::Sip, "SPY", DAY, &dir).unwrap().is_some());
        clear_spool(Feed::Sip, "SPY", DAY, &dir);
        assert!(spooled(Feed::Sip, "SPY", DAY, &dir).unwrap().is_none());
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            0,
            "the spool is gone"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A spool that can't be read just now is an error (the day waits), not a
    /// damaged spool to build over.
    #[tokio::test]
    async fn an_unreadable_spool_is_an_error_not_damage() {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch("unreadable");
        let m = build(&alpaca().await, Feed::Sip, "SPY", DAY, &dir)
            .await
            .unwrap()
            .batches[0]
            .clone();
        let data = std::fs::read_dir(&m)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.to_string_lossy().ends_with(".dbn.zst"))
            .unwrap();
        std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o000)).unwrap();
        let r = spooled(Feed::Sip, "SPY", DAY, &dir);
        std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(r.is_err(), "{:?}", r.map(|k| k.is_some()));
        // Missing, it's damage: rebuilt.
        std::fs::remove_file(&data).unwrap();
        assert!(spooled(Feed::Sip, "SPY", DAY, &dir).unwrap().is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Batches without their marker (a build interrupted after writing them) aren't
    /// a complete spool.
    #[tokio::test]
    async fn batches_without_their_marker_are_not_complete() {
        let dir = scratch("unmarked");
        build(&alpaca().await, Feed::Sip, "SPY", DAY, &dir)
            .await
            .unwrap();
        let marker = dir.join(format!(
            "{}.complete.json",
            Feed::Sip.batches("SPY", DAY)[0]
        ));
        std::fs::remove_file(marker).unwrap();
        assert!(spooled(Feed::Sip, "SPY", DAY, &dir).unwrap().is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A fake store: `/coverage` answers `cov` for exactly the query asked for the
    /// SIP day (400 otherwise), and `/files/{name}` serves `files`.
    async fn store(cov: serde_json::Value, files: Vec<(&'static str, Vec<u8>)>) -> String {
        use axum::extract::{Path, Query};
        use std::collections::HashMap;
        let files: std::sync::Arc<HashMap<String, Vec<u8>>> =
            std::sync::Arc::new(files.into_iter().map(|(n, b)| (n.to_owned(), b)).collect());
        let app = axum::Router::new()
            .route(
                "/coverage",
                get(move |Query(q): Query<HashMap<String, String>>| {
                    let cov = cov.clone();
                    async move {
                        let want = [
                            ("provider", "alpaca"),
                            ("dataset", "ALPACA.SIP"),
                            ("symbol", "SPY"),
                            ("schema", "cmbp-1"),
                            ("start", DAY),
                            ("end", DAY),
                        ];
                        if want
                            .iter()
                            .all(|(k, v)| q.get(*k).map(String::as_str) == Some(*v))
                            && q.len() == want.len()
                        {
                            (axum::http::StatusCode::OK, axum::Json(cov))
                        } else {
                            (
                                axum::http::StatusCode::BAD_REQUEST,
                                axum::Json(serde_json::json!({ "error": format!("{q:?}") })),
                            )
                        }
                    }
                }),
            )
            .route(
                "/files/{*name}",
                get(move |Path(name): Path<String>| {
                    let files = std::sync::Arc::clone(&files);
                    async move {
                        files
                            .get(&name)
                            .cloned()
                            .ok_or(axum::http::StatusCode::NOT_FOUND)
                    }
                }),
            );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        format!("http://{addr}")
    }

    /// Files a fake store serves: name and bytes (empty: the built main's).
    type Served = Vec<(&'static str, Vec<u8>)>;

    const A: &str = "data/Alpaca/ALPACA.SIP/SPY/cmbp-1/2026-10-01.dbn.zst";
    const B: &str = "tt03/Alpaca/ALPACA.SIP/SPY/cmbp-1/2026-10-01.dbn.zst";

    fn row(files: &[&str], sha: Option<&str>) -> serde_json::Value {
        let mut r = serde_json::json!({ "day": DAY, "files": files, "conflict": false });
        if let Some(h) = sha {
            r["sha256"] = serde_json::json!(h);
        }
        serde_json::json!({ "days": [r], "missing_volumes": [] })
    }

    fn main_file(m: &std::path::Path) -> Vec<u8> {
        let p = std::fs::read_dir(m)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.to_string_lossy().ends_with(".dbn.zst"))
            .unwrap();
        std::fs::read(p).unwrap()
    }

    fn sha(b: &[u8]) -> String {
        format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(b))
    }

    async fn check(
        cov: serde_json::Value,
        files: Served,
        name: &str,
    ) -> (Vec<u8>, Result<(), super::FileError>) {
        let dir = scratch(name);
        let built = build(&alpaca().await, Feed::Sip, "SPY", DAY, &dir)
            .await
            .unwrap();
        let (m, c) = (&built.batches[0], &built.batches[1]);
        let main = main_file(m);
        let files = files
            .into_iter()
            .map(|(n, b)| (n, if b.is_empty() { main.clone() } else { b }))
            .collect();
        let cov = serde_json::from_str(&cov.to_string().replace("MAIN_SHA", &sha(&main))).unwrap();
        let r = check_binding(
            &reqwest::Client::new(),
            &store(cov, files).await,
            Feed::Sip,
            "SPY",
            DAY,
            c,
        )
        .await;
        let _ = std::fs::remove_dir_all(dir);
        (main, r)
    }

    /// Conditions rebuilt against the same main the store holds bind to it: hashed
    /// as it streams when the store gives no hash, every copy agreeing.
    #[tokio::test]
    async fn conditions_built_against_the_held_main_bind() {
        let (_, r) = check(row(&[A], None), vec![(A, vec![])], "bound").await;
        r.unwrap();
        let (_, r) = check(
            row(&[A, B], None),
            vec![(A, vec![]), (B, vec![])],
            "bound-two",
        )
        .await;
        r.unwrap();
    }

    /// Only the filed data file is hashed: sidecars listed beside it are not.
    #[tokio::test]
    async fn only_the_filed_data_file_is_compared() {
        const SIDECAR: &str = "data/Alpaca/ALPACA.SIP/SPY/cmbp-1/manifest.json";
        let (_, r) = check(
            row(&[SIDECAR, A], None),
            vec![(A, vec![]), (SIDECAR, b"{}".to_vec())],
            "sidecar",
        )
        .await;
        r.unwrap();
    }

    /// The store's own hash is used when it has one: nothing is downloaded.
    #[tokio::test]
    async fn the_stores_hash_is_used_without_downloading() {
        let (_, r) = check(row(&[A], Some("MAIN_SHA")), vec![], "store-hash").await;
        r.unwrap();
    }

    /// The held main differs (the provider's data changed): unbound, naming both
    /// hashes.
    #[tokio::test]
    async fn conditions_against_another_main_are_unbound() {
        let (_, r) = check(
            row(&[A], None),
            vec![(A, b"another main".to_vec())],
            "unbound",
        )
        .await;
        let e = r.unwrap_err();
        assert!(e.unbound && !e.later && !e.busy, "{e:?}");
        assert!(e.message.contains(&sha(b"another main")), "{}", e.message);
    }

    /// Anything that stops the check short of a known, different main defers the
    /// day: never unbound, never filed.
    #[tokio::test]
    async fn what_cant_be_checked_is_deferred() {
        let away = serde_json::json!({ "days": [], "missing_volumes": ["archive"] });
        let disagree = (
            vec![(A, vec![]), (B, b"another copy".to_vec())],
            row(&[A, B], None),
        );
        let cases: Vec<(&str, serde_json::Value, Served)> = vec![
            ("away", away, vec![]),
            ("copies", disagree.1, disagree.0),
            (
                "conflict",
                serde_json::json!({ "days": [{ "day": DAY, "files": [A], "conflict": true }], "missing_volumes": [] }),
                vec![(A, vec![])],
            ),
            ("unserved", row(&[A], None), vec![]),
            // An older day folder's sidecar, and a batch download's range file: not
            // the main as filed.
            (
                "old-layout",
                row(
                    &[
                        "data/Alpaca/ALPACA.SIP/SPY/2026-10-01/metadata.json",
                        "data/Alpaca/ALPACA-SIP-SPY-20261001/alpaca-sip-20261001-20261002.cmbp-1.dbn.zst",
                    ],
                    None,
                ),
                vec![],
            ),
        ];
        for (name, cov, files) in cases {
            let (_, r) = check(cov, files, name).await;
            let e = r.unwrap_err();
            assert!(e.later && !e.unbound, "{name}: {e:?}");
        }
        // The store not there at all.
        let dir = scratch("no-store");
        let c = build(&alpaca().await, Feed::Sip, "SPY", DAY, &dir)
            .await
            .unwrap()
            .batches[1]
            .clone();
        let e = check_binding(
            &reqwest::Client::new(),
            "http://127.0.0.1:9",
            Feed::Sip,
            "SPY",
            DAY,
            &c,
        )
        .await
        .unwrap_err();
        assert!(e.later && !e.unbound, "{e:?}");
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[cfg(test)]
mod stream_tests {
    use super::*;
    use crate::testing::{DAY, FakeAlpaca, FakeStore, Shape, body, quote_codes, scratch};
    use serde_json::json;
    use std::time::Duration;
    use tinytrader_alpaca::quote_conditions::QuoteConditions;

    const T3: Shape = Shape::new(3, 50);
    const Q3: Shape = Shape::new(3, 70);

    fn data_file(batch: &Path) -> PathBuf {
        std::fs::read_dir(batch)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.to_string_lossy().ends_with(".dbn.zst"))
            .unwrap()
    }

    fn each_page_once(f: &FakeAlpaca) {
        for (lane, shape) in [("trades", T3), ("quotes", Q3)] {
            for p in 0..shape.pages {
                assert_eq!(f.served(lane, p), 1, "{lane} page {p}: {:?}", f.log());
            }
        }
    }

    /// A 429 in the middle of a lane, again after the pace's own retries: the
    /// lane waits and asks for that page again; nothing fetched is fetched again.
    #[tokio::test]
    async fn a_429_mid_day_resumes_without_refetching() {
        let f = FakeAlpaca::start(T3, Q3).await;
        f.fault("quotes", 1, &[429; 7]);
        let b = build(&f.client(), Feed::Sip, "SPY", DAY, &scratch("429"))
            .await
            .unwrap();
        assert_eq!(
            (b.outcome.trades, b.outcome.quotes, b.outcome.pages),
            (150, 210, 6)
        );
        each_page_once(&f);
        assert_eq!(f.log().iter().filter(|s| s.2 == 429).count(), 7);
    }

    /// Server errors past the pace's retries: the lane waits and asks again.
    /// Still failing past its waits, the day is left for later (transient),
    /// its pages kept: the next attempt starts where it stopped.
    #[tokio::test]
    async fn server_errors_are_retried_and_then_left_for_later() {
        let f = FakeAlpaca::start(T3, Q3).await;
        f.fault("trades", 1, &[500, 502, 503, 500, 500, 500, 504]);
        let dir = scratch("5xx");
        build(&f.client(), Feed::Sip, "SPY", DAY, &dir)
            .await
            .unwrap();
        each_page_once(&f);

        let g = FakeAlpaca::start(T3, Q3).await;
        // 5 tries a request, 9 requests (the first and 8 waits): the lane gives up.
        g.fault("quotes", 2, &[503; 45]);
        let dir = scratch("5xx-later");
        let e = build(&g.client(), Feed::Sip, "SPY", DAY, &dir)
            .await
            .unwrap_err();
        assert!(e.transient(), "{e}");
        let h = FakeAlpaca::start(T3, Q3).await;
        build(&h.client(), Feed::Sip, "SPY", DAY, &dir)
            .await
            .unwrap();
        assert_eq!(
            (
                h.served("quotes", 0),
                h.served("quotes", 1),
                h.served("quotes", 2)
            ),
            (0, 0, 1)
        );
        assert_eq!(
            h.log().iter().filter(|s| s.0 == "trades").count(),
            0,
            "the trades lane was done"
        );
    }

    /// The fake's tokens are `p+N/x==`, refused unless they arrive as sent.
    #[tokio::test]
    async fn page_tokens_with_special_characters_arrive_as_sent() {
        let f = FakeAlpaca::start(T3, Q3).await;
        build(&f.client(), Feed::Sip, "SPY", DAY, &scratch("tokens"))
            .await
            .unwrap();
        assert!(f.log().iter().all(|s| s.2 == 200), "{:?}", f.log());
        each_page_once(&f);
    }

    /// A page token Alpaca refuses (400) starts that lane again, from its first
    /// page; the other lane is untouched and nothing is counted twice.
    #[tokio::test]
    async fn a_refused_page_token_starts_its_lane_again() {
        let f = FakeAlpaca::start(T3, Q3).await;
        f.fault("quotes", 2, &[400]);
        let b = build(&f.client(), Feed::Sip, "SPY", DAY, &scratch("restart-lane"))
            .await
            .unwrap();
        assert_eq!((b.outcome.quotes, b.outcome.pages), (210, 6));
        assert_eq!(
            (
                f.served("quotes", 0),
                f.served("quotes", 1),
                f.served("quotes", 2)
            ),
            (2, 2, 1)
        );
        assert_eq!(
            (
                f.served("trades", 0),
                f.served("trades", 1),
                f.served("trades", 2)
            ),
            (1, 1, 1)
        );
    }

    /// Interrupted mid-lane (a crash, a restart): the next build resumes each
    /// lane from its spool, and the day is the one built in one go.
    #[tokio::test]
    async fn a_restart_mid_lane_resumes_from_the_spool() {
        let dir = scratch("restart");
        let f = FakeAlpaca::start(T3, Q3).await;
        f.hang("quotes", 2);
        // Interrupted once it has asked for the hung page (so the pages before
        // it are spooled), however slow the machine, not after a fixed time.
        {
            let client = f.client();
            let mut b = Box::pin(build(&client, Feed::Sip, "SPY", DAY, &dir));
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            // The trades lane (three pages) runs beside it: done too, then a
            // moment more for it to keep its spool.
            let mut settled = None;
            loop {
                assert!(std::time::Instant::now() < deadline, "never asked for page 2");
                if settled.is_none() && f.hung() > 0 && f.served("trades", 2) == 1 {
                    settled = Some(std::time::Instant::now() + Duration::from_millis(200));
                }
                if settled.is_some_and(|t| std::time::Instant::now() >= t) {
                    break;
                }
                tokio::select! {
                    _ = &mut b => panic!("the build finished despite the hung page"),
                    () = tokio::time::sleep(Duration::from_millis(5)) => {}
                }
            }
        }
        assert_eq!((f.served("quotes", 0), f.served("quotes", 1)), (1, 1));
        let g = FakeAlpaca::start(T3, Q3).await;
        let b = build(&g.client(), Feed::Sip, "SPY", DAY, &dir)
            .await
            .unwrap();
        assert_eq!(
            (
                g.served("quotes", 0),
                g.served("quotes", 1),
                g.served("quotes", 2)
            ),
            (0, 0, 1)
        );
        assert_eq!(g.log().iter().filter(|s| s.0 == "trades").count(), 0);
        assert!(
            !dir.join("lanes").exists(),
            "the pages go once the day is written"
        );
        let clean = build(
            &FakeAlpaca::start(T3, Q3).await.client(),
            Feed::Sip,
            "SPY",
            DAY,
            &scratch("restart-clean"),
        )
        .await
        .unwrap();
        for (x, y) in b.batches.iter().zip(&clean.batches) {
            assert_eq!(
                sha(&data_file(x)).unwrap(),
                sha(&data_file(y)).unwrap(),
                "{}",
                x.display()
            );
        }
    }

    /// Trades and quotes are each counted and time-bounded against their own
    /// lane: a lost trade and a duplicated quote don't cancel out.
    #[test]
    fn trades_and_quotes_are_checked_apart() {
        let lane = |n, first, last| LaneDone {
            receipts: vec![],
            rows: n,
            first: Some(first),
            last: Some(last),
        };
        let (t, q) = (lane(10, 1, 19), lane(10, 0, 18));
        let tally = |n, f, l| Tally {
            n,
            first: Some(f),
            last: Some(l),
        };
        let good = MainTally {
            trades: tally(10, 1, 19),
            quotes: tally(10, 0, 18),
            other: 0,
        };
        let sides = |m: &MainTally| vec![(CONDITIONS, m.trades), (QUOTE_CONDITIONS, m.quotes)];
        check_day(&good, &t, &q, &sides(&good)).unwrap();
        let swapped = MainTally {
            trades: tally(9, 1, 19),
            quotes: tally(11, 0, 18),
            other: 0,
        };
        let e = check_day(&swapped, &t, &q, &sides(&swapped)).unwrap_err();
        assert!(e.contains("9 trades") && e.contains("11 quotes"), "{e}");
        let late = MainTally {
            trades: tally(10, 1, 21),
            ..good
        };
        assert!(
            check_day(&late, &t, &q, &sides(&late))
                .unwrap_err()
                .contains("trades")
        );
        let early = MainTally {
            quotes: tally(10, 2, 18),
            ..good
        };
        assert!(
            check_day(&early, &t, &q, &sides(&early))
                .unwrap_err()
                .contains("quotes")
        );
        let short = [
            (CONDITIONS, tally(9, 1, 19)),
            (QUOTE_CONDITIONS, good.quotes),
        ];
        assert!(
            check_day(&good, &t, &q, &short)
                .unwrap_err()
                .contains("conditions")
        );
        let qshort = [
            (CONDITIONS, good.trades),
            (QUOTE_CONDITIONS, tally(10, 0, 17)),
        ];
        assert!(
            check_day(&good, &t, &q, &qshort)
                .unwrap_err()
                .contains("quote-conditions")
        );
        let other = MainTally { other: 1, ..good };
        assert!(check_day(&other, &t, &q, &sides(&other)).is_err());
    }

    /// The quote conditions sidecar: one record a quote, in the main file's
    /// order, with the quote's codes and tape, bound to the main file, and
    /// filed beside it.
    #[tokio::test]
    async fn the_quote_conditions_sidecar_round_trips() {
        let f = FakeAlpaca::start(T3, Q3).await;
        let b = build(&f.client(), Feed::Sip, "SPY", DAY, &scratch("qc"))
            .await
            .unwrap();
        assert_eq!(b.batches.len(), 3);
        let main = data_file(&b.batches[0]);
        let mut quotes = Vec::new();
        let mut d = dbn::decode::dbn::Decoder::from_zstd_file(&main).unwrap();
        while let Some(r) = d.decode_record_ref().unwrap() {
            let m = r.get::<Cmbp1Msg>().unwrap();
            if m.action as u8 == b'M' {
                quotes.push(m.hd.ts_event);
            }
        }
        let side = data_file(&b.batches[2]);
        let mut d = dbn::decode::dbn::Decoder::from_zstd_file(&side).unwrap();
        assert_eq!(
            (d.metadata().schema, d.metadata().dataset.as_str()),
            (None, "ALPACA.SIP")
        );
        let mut k = 0;
        while let Some(r) = d.decode_record_ref().unwrap() {
            let c = QuoteConditions::decode(r.as_ref()).unwrap();
            assert_eq!(
                (
                    c.quote_seq as usize,
                    c.ts_event,
                    c.codes(),
                    c.tape,
                    c.publisher_id
                ),
                (k, quotes[k], quote_codes(k), b'B', sip::PUBLISHER)
            );
            k += 1;
        }
        assert_eq!((k, quotes.len()), (210, 210));
        let binding: Value =
            serde_json::from_slice(&std::fs::read(b.batches[2].join("binding.json")).unwrap())
                .unwrap();
        assert_eq!(
            binding["format"],
            "tinytrader.alpaca-history.quote-conditions-binding/1"
        );
        assert_eq!(
            (
                binding["quotes"].as_u64(),
                binding["main"]["sha256"].as_str()
            ),
            (Some(210), Some(sha(&main).unwrap().1.as_str()))
        );
        let md: Value =
            serde_json::from_slice(&std::fs::read(b.batches[2].join("metadata.json")).unwrap())
                .unwrap();
        assert_eq!(md["query"]["schema"], QUOTE_CONDITIONS);
        let store = FakeStore::start().await;
        for batch in &b.batches {
            file(&reqwest::Client::new(), &store.url, None, batch)
                .await
                .unwrap();
        }
        let ids = Feed::Sip.batches("SPY", DAY);
        assert_eq!(store.filed(), ids.iter().cloned().collect());
        assert!(
            store
                .received(&ids[2])
                .contains_key("alpaca-sip-20261001.quote-conditions.dbn.zst")
        );
    }

    /// The day streamed from the spool is byte for byte the day encoded whole
    /// (so a main filed before this streaming still binds its sidecars).
    #[tokio::test]
    async fn the_streamed_day_is_the_day_encoded_whole() {
        let f = FakeAlpaca::start(T3, Q3).await;
        let b = build(&f.client(), Feed::Sip, "SPY", DAY, &scratch("whole"))
            .await
            .unwrap();
        let (mut trades, mut quotes) = (Vec::new(), Vec::new());
        for p in 0..3 {
            trades.extend(sip_trades(body("trades", DAY, p, T3).as_bytes()).unwrap().0);
            quotes.extend(sip_quotes(body("quotes", DAY, p, Q3).as_bytes()).unwrap().0);
        }
        let rows = sip::merge(trades, quotes).unwrap();
        let (recs, conds, qconds) = sip::encode_day("SPY", &rows).unwrap();
        let out = scratch("whole-ref");
        let id = sip::instrument_id("SPY");
        let (m, c, q) = (out.join("m"), out.join("c"), out.join("q"));
        let mut w = DbnFile::create(
            m.clone(),
            &metadata(Feed::Sip, "SPY", DAY, Some(Schema::Cmbp1), id).unwrap(),
        )
        .unwrap();
        recs.iter().for_each(|r| w.record(r).unwrap());
        w.finish().unwrap();
        let mut w = DbnFile::create(
            c.clone(),
            &metadata(Feed::Sip, "SPY", DAY, None, id).unwrap(),
        )
        .unwrap();
        conds
            .iter()
            .for_each(|r| w.raw(&r.encode().unwrap()).unwrap());
        w.finish().unwrap();
        let mut w = DbnFile::create(
            q.clone(),
            &metadata(Feed::Sip, "SPY", DAY, None, id).unwrap(),
        )
        .unwrap();
        qconds
            .iter()
            .for_each(|r| w.raw(&r.encode().unwrap()).unwrap());
        w.finish().unwrap();
        for (built, whole) in b.batches.iter().zip([m, c, q]) {
            assert_eq!(
                std::fs::read(data_file(built)).unwrap(),
                std::fs::read(whole).unwrap(),
                "{}",
                built.display()
            );
        }
    }

    /// A large day is written a page at a time: never more than one page's
    /// rows of a lane in memory, however many pages.
    #[tokio::test]
    async fn a_large_day_streams_in_bounded_memory() {
        let (t, q) = (Shape::new(4, 10_000), Shape::new(24, 10_000));
        let f = FakeAlpaca::start(t, q).await;
        let b = build(&f.client(), Feed::Sip, "SPY", DAY, &scratch("large"))
            .await
            .unwrap();
        assert_eq!(
            (b.outcome.trades, b.outcome.quotes, b.outcome.pages),
            (40_000, 240_000, 28)
        );
        assert!(b.peak_rows <= 10_000, "{} rows held at once", b.peak_rows);
    }

    /// IEX days stream the same way, with no quote conditions.
    #[tokio::test]
    async fn an_iex_day_streams_with_its_trade_conditions() {
        let f = FakeAlpaca::start(T3, Q3).await;
        let b = build(&f.client(), Feed::Iex, "SPY", DAY, &scratch("iex"))
            .await
            .unwrap();
        assert_eq!(
            (b.batches.len(), b.outcome.trades, b.outcome.quotes),
            (2, 150, 210)
        );
        assert_eq!(read_back(&data_file(&b.batches[1])).unwrap().n, 150);
        let m = read_main(&data_file(&b.batches[0])).unwrap();
        assert_eq!((m.trades.n, m.quotes.n, m.other), (150, 210, 0));
    }

    /// main's SIP encoder (`encode_day` before this branch), verbatim but for
    /// the quote type's new fields: the reference the streamed day must match
    /// byte for byte.
    fn main_encode_day(
        symbol: &str,
        rows: &[sip::Row],
    ) -> (Vec<Cmbp1Msg>, Vec<tinytrader_dbn::TradeConditions>) {
        use dbn::{ConsolidatedBidAskPair, RecordHeader, UNDEF_PRICE, rtype};
        use sip::Row;
        use tinytrader_dbn::TradeConditions;
        fn level(q: &sip::Quote) -> ConsolidatedBidAskPair {
            ConsolidatedBidAskPair {
                bid_px: q.bid.px,
                ask_px: q.ask.px,
                bid_sz: q.bid.sz,
                ask_sz: q.ask.sz,
                bid_pb: q.bid_pb,
                ask_pb: q.ask_pb,
                ..Default::default()
            }
        }
        let absent = tinytrader_alpaca::iex::Side {
            px: UNDEF_PRICE,
            sz: 0,
        };
        let instrument = sip::instrument_id(symbol);
        let mut book = sip::Quote {
            ts: 0,
            bid: absent,
            ask: absent,
            bid_pb: 0,
            ask_pb: 0,
            codes: [0; 4],
            count: 0,
            tape: 0,
            unparsed: false,
        };
        let mut records = Vec::with_capacity(rows.len());
        let mut conditions = Vec::new();
        let mut seq: u32 = 0;
        for (i, row) in rows.iter().enumerate() {
            let (publisher, ts, action, px, sz, lv) = match row {
                Row::Quote(q) => {
                    book = *q;
                    (sip::PUBLISHER, q.ts, b'M', UNDEF_PRICE, 0, level(q))
                }
                Row::Trade(t) => {
                    let (codes, count) = TradeConditions::codes_from(&t.codes).unwrap();
                    let c = TradeConditions {
                        publisher_id: t.exchange,
                        instrument_id: instrument,
                        ts_event: t.ts,
                        price: t.px,
                        trade_id: t.id,
                        size: t.sz,
                        codes,
                        count,
                        tape: t.tape,
                        update: t.update,
                        trade_seq: seq,
                    };
                    c.encode().unwrap();
                    conditions.push(c);
                    seq = seq.wrapping_add(1);
                    (t.exchange, t.ts, b'T', t.px, t.sz, level(&book))
                }
            };
            let last = rows.get(i + 1).is_none_or(|next| next.ts() != row.ts());
            let mut rec = Cmbp1Msg::default_for_schema(dbn::Schema::Cmbp1);
            rec.hd = RecordHeader::new::<Cmbp1Msg>(rtype::CMBP_1, publisher, instrument, ts);
            rec.price = px;
            rec.size = sz;
            rec.action = action as std::ffi::c_char;
            rec.side = b'N' as std::ffi::c_char;
            rec.flags = if last {
                FlagSet::empty().set_last()
            } else {
                FlagSet::empty()
            };
            rec.ts_recv = UNDEF_TIMESTAMP;
            rec.ts_in_delta = 0;
            rec.levels = [lv];
            records.push(rec);
        }
        (records, conditions)
    }

    /// Write records as a day file, as `write_day` does.
    fn reference_file<R: DbnEncodable>(
        path: &Path,
        feed: Feed,
        schema: Option<Schema>,
        id: u32,
        recs: &[R],
    ) -> Vec<u8> {
        let mut w = DbnFile::create(
            path.to_path_buf(),
            &metadata(feed, "SPY", DAY, schema, id).unwrap(),
        )
        .unwrap();
        recs.iter().for_each(|r| w.record(r).unwrap());
        w.finish().unwrap();
        std::fs::read(path).unwrap()
    }
    fn reference_sidecar(path: &Path, feed: Feed, id: u32, recs: &[Vec<u8>]) -> Vec<u8> {
        let mut w = DbnFile::create(
            path.to_path_buf(),
            &metadata(feed, "SPY", DAY, None, id).unwrap(),
        )
        .unwrap();
        recs.iter().for_each(|r| w.raw(r).unwrap());
        w.finish().unwrap();
        std::fs::read(path).unwrap()
    }

    const TT: Shape = Shape {
        ties: true,
        ..Shape::new(3, 50)
    };
    const QT: Shape = Shape {
        ties: true,
        ..Shape::new(3, 70)
    };

    /// Trades and quotes on equal times (several quotes on one time, trades on
    /// a quote's time, across page boundaries): trades come first on a tie and
    /// carry the book before the quotes of their time, `F_LAST` closes each
    /// time, the quote conditions stay aligned with the quotes, and the files
    /// are byte for byte main's encoder's (SIP) and `iex::encode_day`'s (IEX).
    #[tokio::test]
    async fn equal_times_match_mains_encoder() {
        let f = FakeAlpaca::start(TT, QT).await;
        let b = build(&f.client(), Feed::Sip, "SPY", DAY, &scratch("ties"))
            .await
            .unwrap();
        let (mut trades, mut quotes) = (Vec::new(), Vec::new());
        for p in 0..3 {
            trades.extend(sip_trades(body("trades", DAY, p, TT).as_bytes()).unwrap().0);
            quotes.extend(sip_quotes(body("quotes", DAY, p, QT).as_bytes()).unwrap().0);
        }
        let rows = sip::merge(trades, quotes).unwrap();
        let (recs, conds) = main_encode_day("SPY", &rows);
        let out = scratch("ties-ref");
        let id = sip::instrument_id("SPY");
        assert_eq!(
            std::fs::read(data_file(&b.batches[0])).unwrap(),
            reference_file(&out.join("m"), Feed::Sip, Some(Schema::Cmbp1), id, &recs)
        );
        let tc: Vec<Vec<u8>> = conds.iter().map(|c| c.encode().unwrap().to_vec()).collect();
        assert_eq!(
            std::fs::read(data_file(&b.batches[1])).unwrap(),
            reference_sidecar(&out.join("c"), Feed::Sip, id, &tc)
        );

        // And what that means, checked directly.
        // A trade carries the last quote before it in tape order, which (no
        // trade ever following a quote of its own time) is the book before
        // its time.
        let (mut book, mut quote_ts) = (dbn::UNDEF_PRICE, Vec::new());
        let mut trade_after_quote_same_time = 0;
        let mut trades_on_a_quote_time = 0;
        for (i, r) in recs.iter().enumerate() {
            let ts = r.hd.ts_event;
            let next = recs.get(i + 1).map(|n| n.hd.ts_event);
            assert_eq!(r.flags.is_last(), next != Some(ts), "F_LAST at record {i}");
            if r.action as u8 == b'M' {
                book = r.levels[0].bid_px;
                quote_ts.push(ts);
            } else {
                assert_eq!(r.levels[0].bid_px, book, "trade at record {i}");
                let prev = i.checked_sub(1).map(|p| &recs[p]);
                if prev.is_some_and(|p| p.hd.ts_event == ts && p.action as u8 == b'M') {
                    trade_after_quote_same_time += 1;
                }
                if recs[i + 1..]
                    .iter()
                    .take_while(|n| n.hd.ts_event == ts)
                    .any(|n| n.action as u8 == b'M')
                {
                    trades_on_a_quote_time += 1;
                }
            }
        }
        assert!(trades_on_a_quote_time > 0, "trades share times with quotes");
        assert_eq!(
            trade_after_quote_same_time, 0,
            "a trade never follows a quote of its own time"
        );
        assert!(
            quote_ts.windows(2).any(|w| w[0] == w[1]),
            "several quotes share a time"
        );
        let mut d = dbn::decode::dbn::Decoder::from_zstd_file(data_file(&b.batches[2])).unwrap();
        let mut k = 0;
        while let Some(r) = d.decode_record_ref().unwrap() {
            let c = QuoteConditions::decode(r.as_ref()).unwrap();
            assert_eq!(
                (c.quote_seq as usize, c.ts_event, c.codes()),
                (k, quote_ts[k], quote_codes(k))
            );
            k += 1;
        }
        assert_eq!(k, 210);

        // IEX: against iex::encode_day, untouched on this branch.
        let b = build(&f.client(), Feed::Iex, "SPY", DAY, &scratch("ties-iex"))
            .await
            .unwrap();
        let (mut trades, mut quotes) = (Vec::new(), Vec::new());
        for p in 0..3 {
            trades.extend(iex_trades(body("trades", DAY, p, TT).as_bytes()).unwrap().0);
            quotes.extend(iex_quotes(body("quotes", DAY, p, QT).as_bytes()).unwrap().0);
        }
        let (recs, conds) = iex::encode_day("SPY", &iex::merge(trades, quotes).unwrap()).unwrap();
        let id = iex::instrument_id("SPY");
        assert_eq!(
            std::fs::read(data_file(&b.batches[0])).unwrap(),
            reference_file(&out.join("im"), Feed::Iex, Some(Schema::Mbp1), id, &recs)
        );
        let tc: Vec<Vec<u8>> = conds.iter().map(|c| c.encode().unwrap().to_vec()).collect();
        assert_eq!(
            std::fs::read(data_file(&b.batches[1])).unwrap(),
            reference_sidecar(&out.join("ic"), Feed::Iex, id, &tc)
        );
    }

    /// A quote whose conditions can't be held (two-character code, no tape)
    /// is kept, its conditions record marked unparsed and counted; nothing is
    /// fetched again.
    #[tokio::test]
    async fn a_quote_whose_conditions_cant_be_held_is_marked() {
        let q = Shape {
            bad: Some(75),
            ..Q3
        };
        let f = FakeAlpaca::start(T3, q).await;
        let b = build(&f.client(), Feed::Sip, "SPY", DAY, &scratch("bad-quote"))
            .await
            .unwrap();
        assert_eq!(
            (b.outcome.quotes, b.outcome.unparsed_quote_conditions),
            (210, 1)
        );
        each_page_once(&f);
        let mut d = dbn::decode::dbn::Decoder::from_zstd_file(data_file(&b.batches[2])).unwrap();
        let mut marked = Vec::new();
        let mut k = 0;
        while let Some(r) = d.decode_record_ref().unwrap() {
            if QuoteConditions::decode(r.as_ref()).unwrap().unparsed {
                marked.push(k);
            }
            k += 1;
        }
        assert_eq!((k, marked), (210, vec![75]));
        let binding: Value =
            serde_json::from_slice(&std::fs::read(b.batches[2].join("binding.json")).unwrap())
                .unwrap();
        assert_eq!(binding["unparsed"], 1);
    }

    /// A trade the layout can't hold (five codes) fails the day as `Encode`,
    /// without fetching any page again.
    #[tokio::test]
    async fn a_trade_the_layout_cant_hold_fails_without_refetching() {
        let t = Shape {
            bad: Some(60),
            ..T3
        };
        let f = FakeAlpaca::start(t, Q3).await;
        let e = build(&f.client(), Feed::Sip, "SPY", DAY, &scratch("bad-trade"))
            .await
            .unwrap_err();
        assert!(matches!(e, Failure::Encode(_)) && !e.transient(), "{e}");
        each_page_once(&f);
    }

    /// The startup sweep: an old day's spool goes whole; a complete spool by
    /// another encoder loses its batches and marker but keeps its pages.
    #[tokio::test]
    async fn the_sweep_bounds_the_spool() {
        let dir = scratch("sweep");
        let f = FakeAlpaca::start(T3, Q3).await;
        build(&f.client(), Feed::Sip, "SPY", DAY, &dir)
            .await
            .unwrap();
        let ids = Feed::Sip.batches("SPY", DAY);
        let marker = dir.join(format!("{}.complete.json", ids[0]));
        let mut o: Value = serde_json::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
        o["encoder"] = json!("tinytrader-alpaca 0.0.1");
        std::fs::write(&marker, o.to_string()).unwrap();
        std::fs::create_dir_all(dir.join("lanes").join(&ids[0]).join("quotes")).unwrap();
        assert!(
            spooled(Feed::Sip, "SPY", DAY, &dir).unwrap().is_none(),
            "another encoder's spool isn't filed"
        );
        let gone = sweep(&dir, std::time::Duration::from_secs(3600), |_| None);
        assert_eq!(gone.len(), 1, "{gone:?}");
        assert!(!marker.exists() && ids.iter().all(|id| !dir.join(id).exists()));
        assert!(
            dir.join("lanes").join(&ids[0]).exists(),
            "the pages are kept"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
        let gone = sweep(&dir, std::time::Duration::from_millis(1), |_| None);
        assert!(gone[0].1 && gone[0].2.contains("untouched"), "{gone:?}");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0, "nothing left");
    }
}
