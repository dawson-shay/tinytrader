//! The store producer (decision 0021, section 8): the held days of one
//! dataset, symbol and schema, as one DBN stream in time order.
//!
//! One input contract with two kinds of holdings behind it:
//!
//! - [`StoreApi`]: the store service, `GET /coverage` for what each day is held
//!   in, then `GET /files/{name}` streamed, never loaded whole.
//! - [`Folder`]: a local copy, either a finished `databento-jobs` cache job
//!   (read only once its `COMPLETE` marker exists) or a folder in the store's
//!   layout (`<Provider>/<DATASET>/<symbol>/<schema>/<day>.dbn.zst`).
//!
//! The output is the stored DBN, unchanged, with three kinds of records added
//! and said so:
//!
//! - before each day's records, a `SymbolMappingMsg` for each instrument its
//!   files' metadata maps (Databento's batch files carry no definitions), with
//!   `stype_out_symbol` the raw symbol, stamped at the day's start;
//! - a day that isn't held is a `SystemMsg` `store-gap day=… reason=…`, never a
//!   silent skip;
//! - a partial day is announced by a `SystemMsg` `store-partial day=…`.
//!
//! Each file contributes only the records of the asked schema inside the day's
//! window (a batch file may hold other days; a folder, sidecars), and a file
//! whose header names another schema or symbol is left out. A day held in
//! several files is merged by [`tinytrader_dbn::Merge`].
//!
//! **Pins.** Each file's SHA-256 is computed while it streams and checked
//! where one is stated: by the holdings (a cache job's manifest, the store's
//! `/coverage` for a day in one file) or by an earlier run's pins
//! ([`StoreProducer::pinned`]), which must also name the same files. A local
//! file with a stated hash is checked before any of its records is released; a
//! store file is checked at the end of its day, so a mismatch fails the read
//! there, and a run that uses the producer must count a failed read as a
//! failed run. [`StoreProducer::pins`] lists what was read, for the manifest.
mod filter;
mod folder;
mod hashing;
mod store;

pub use folder::Folder;
pub use store::{Retry, StoreApi};

use dbn::decode::{DbnMetadata, DecodeRecordRef, DynDecoder};
use dbn::{
    Metadata, RType, RecordBuf, RecordRef, SType, Schema, SymbolMappingMsg, SystemMsg,
    VersionUpgradePolicy,
};
use filter::DayFilter;
use hashing::{Digest, Hashed};
use std::collections::{BTreeSet, VecDeque};
use std::io::{BufReader, Read};
use std::str::FromStr;
use tinytrader_dbn::{DbnSource, Merge};

/// What is read: one dataset, symbol and schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    /// The store's provider folder name: `databento`, `alpaca`.
    pub provider: String,
    /// `OPRA.PILLAR`, `ALPACA.SIP`, …
    pub dataset: String,
    /// The symbol level: `SPY.OPT`, `SPY`.
    pub symbol: String,
    /// `cbbo-1m`, `cmbp-1`, …
    pub schema: String,
}

/// One file of a held day.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRef {
    /// Its name in the holdings: a store logical name, or a path in a folder.
    pub name: String,
    /// The SHA-256 the holdings state for it, if any.
    pub sha256: Option<String>,
    /// Which layer of a [`Layered`] holdings planned it, outermost first; empty
    /// elsewhere.
    pub route: Vec<usize>,
}

/// What the holdings have for one day.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Day {
    /// Held in these files; `partial` when only some contracts or hours are.
    Held {
        /// The files, merged in this order on ties.
        files: Vec<FileRef>,
        /// Only part of the day is held.
        partial: bool,
    },
    /// Not held.
    Missing {
        /// Why, for the gap message.
        reason: String,
    },
}

/// Somewhere days are held.
pub trait Holdings: Send {
    /// Where, for the pins: `store http://…`, `job OPRA-…`, `folder /…`.
    fn describe(&self) -> String;
    /// What is held for each of `days` (`YYYY-MM-DD`), in the same order.
    ///
    /// # Errors
    /// The holdings can't be asked, or don't hold what `query` names.
    fn plan(&self, query: &Query, days: &[String]) -> Result<Vec<Day>, String>;
    /// Opens one file for streaming.
    ///
    /// # Errors
    /// It can't be opened.
    fn open(&self, file: &FileRef) -> Result<Box<dyn Read + Send>, String>;
    /// Whether files are cheap to read twice, so a stated hash is checked
    /// before the file's records are released.
    fn local(&self) -> bool {
        false
    }
}

/// Several holdings read as one: each day from the first that holds it whole, or
/// else the first that holds part of it, such as two cache jobs of consecutive
/// ranges, or a local copy before the store.
pub struct Layered {
    layers: Vec<Box<dyn Holdings>>,
}

impl Layered {
    /// `layers` in the order they're asked.
    #[must_use]
    pub fn new(layers: Vec<Box<dyn Holdings>>) -> Self {
        Self { layers }
    }
}

impl Holdings for Layered {
    fn describe(&self) -> String {
        let parts: Vec<String> = self.layers.iter().map(|h| h.describe()).collect();
        format!("first of [{}]", parts.join("; "))
    }

    fn plan(&self, query: &Query, days: &[String]) -> Result<Vec<Day>, String> {
        let plans = self
            .layers
            .iter()
            .map(|h| h.plan(query, days))
            .collect::<Result<Vec<_>, _>>()?;
        let mut out = Vec::new();
        for i in 0..days.len() {
            // The first layer holding the day whole; failing that, the first holding
            // part of it.
            let whole = plans
                .iter()
                .position(|p| matches!(&p[i], Day::Held { partial: false, .. }));
            let any = plans.iter().position(|p| matches!(&p[i], Day::Held { .. }));
            match whole.or(any) {
                Some(k) => {
                    // Each file carries the layer it comes from, for `open`.
                    let mut day = plans[k][i].clone();
                    if let Day::Held { files, .. } = &mut day {
                        for f in files {
                            f.route.insert(0, k);
                        }
                    }
                    out.push(day);
                }
                None => {
                    let reasons: Vec<String> = plans
                        .iter()
                        .filter_map(|p| match &p[i] {
                            Day::Missing { reason } => Some(reason.clone()),
                            Day::Held { .. } => None,
                        })
                        .collect();
                    out.push(Day::Missing {
                        reason: reasons.join("; "),
                    });
                }
            }
        }
        Ok(out)
    }

    fn open(&self, file: &FileRef) -> Result<Box<dyn Read + Send>, String> {
        let (&k, rest) = file
            .route
            .split_first()
            .ok_or_else(|| format!("{}: not planned here", file.name))?;
        let layer = self
            .layers
            .get(k)
            .ok_or_else(|| format!("{}: no layer {k}", file.name))?;
        layer.open(&FileRef {
            route: rest.to_vec(),
            ..file.clone()
        })
    }

    fn local(&self) -> bool {
        self.layers.iter().all(|h| h.local())
    }
}

/// One file read, for the manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilePin {
    /// Its name in the holdings.
    pub name: String,
    /// The SHA-256 of its bytes as read.
    pub sha256: String,
    /// Its size.
    pub bytes: u64,
    /// The holdings (or an earlier run's pins) stated this hash and it matched.
    pub verified: bool,
    /// Its records were read; false for a file listed with the day but left
    /// out (another schema or symbol), pinned so a rerun lists the same.
    pub used: bool,
}

/// One day of the stream, for the manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DayPin {
    /// `YYYY-MM-DD`.
    pub day: String,
    /// The files read (empty for a missing day).
    pub files: Vec<FilePin>,
    /// Only part of the day was held.
    pub partial: bool,
    /// Why it wasn't held, for a gap.
    pub missing: Option<String>,
}

type Decoder = DayFilter<DynDecoder<'static, BufReader<Hashed>>>;

enum State {
    Next,
    Records(Merge<Decoder>),
    Done,
}

/// The producer: a [`DecodeRecordRef`] over the held days, in day order.
pub struct StoreProducer {
    holdings: Box<dyn Holdings>,
    query: Query,
    schema: Schema,
    plan: Vec<(String, Day)>,
    at: usize,
    metadata: Metadata,
    state: State,
    queued: VecDeque<RecordBuf>,
    cur: Option<RecordBuf>,
    reading: Vec<(FileRef, Digest, bool)>,
    pins: Vec<DayPin>,
    /// Options expiring more than this many days after each day are left out.
    expiries_within_days: Option<u32>,
}

/// Midnight UTC of `YYYY-MM-DD`, in ns.
pub(crate) fn day_ns(day: &str) -> Option<u64> {
    let mut it = day.splitn(3, '-').map(i64::from_str);
    let (y, m, d) = (it.next()?.ok()?, it.next()?.ok()?, it.next()?.ok()?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || day.len() != 10 {
        return None;
    }
    // Days from civil (Howard Hinnant).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    u64::try_from(days).ok()?.checked_mul(86_400_000_000_000)
}

/// A `SystemMsg`, its text cut to what the record holds.
fn system(ts: u64, text: &str) -> RecordBuf {
    let mut t: String = text.chars().take(302).collect();
    loop {
        if let Ok(m) = SystemMsg::new(ts, None, &t) {
            return RecordBuf::from(m);
        }
        t.pop();
    }
}

fn fail(e: impl ToString) -> dbn::Error {
    dbn::Error::decode(e)
}

impl StoreProducer {
    /// Plans `days` (`YYYY-MM-DD`, in order) of `query` from `holdings`.
    ///
    /// # Errors
    /// The holdings can't be asked, the days aren't in order, or `query`
    /// names an unknown schema.
    pub fn new(holdings: Box<dyn Holdings>, query: Query, days: &[String]) -> Result<Self, String> {
        if days.windows(2).any(|w| w[0] >= w[1]) || days.iter().any(|d| day_ns(d).is_none()) {
            return Err("days must be YYYY-MM-DD, in order, each once".into());
        }
        let schema = Schema::from_str(&query.schema).map_err(|e| e.to_string())?;
        let plan = holdings.plan(&query, days)?;
        if plan.len() != days.len() {
            return Err(format!(
                "{}: answered for {} of {} days",
                holdings.describe(),
                plan.len(),
                days.len()
            ));
        }
        let start = days.first().and_then(|d| day_ns(d)).unwrap_or(0);
        let end = days
            .last()
            .and_then(|d| day_ns(d))
            .map(|t| t + 86_400_000_000_000);
        let mut metadata = Metadata::builder()
            .dataset(query.dataset.clone())
            .schema(Some(schema))
            .start(start)
            .stype_in(None)
            .stype_out(SType::InstrumentId)
            .symbols(vec![query.symbol.clone()])
            .build();
        metadata.end = end.and_then(std::num::NonZeroU64::new);
        Ok(Self {
            holdings,
            query,
            schema,
            plan: days.iter().cloned().zip(plan).collect(),
            at: 0,
            metadata,
            state: State::Next,
            queued: VecDeque::new(),
            cur: None,
            reading: Vec::new(),
            pins: Vec::new(),
            expiries_within_days: None,
        })
    }

    /// Leaves out options expiring more than `days` days after each day (or
    /// before it), with their symbol mappings: a session subscribed to the near
    /// expiries only. Instruments that aren't options pass.
    #[must_use]
    pub fn expiries_within_days(mut self, days: u32) -> Self {
        self.expiries_within_days = Some(days);
        self
    }

    /// As [`StoreProducer::new`], to reproduce a run: each day must be held in the
    /// same files as in `pins`, a gap then must be a gap now, and every file's
    /// hash is checked against its pin.
    ///
    /// # Errors
    /// As [`StoreProducer::new`], or a day held differently from its pin.
    pub fn pinned(
        holdings: Box<dyn Holdings>,
        query: Query,
        days: &[String],
        pins: &[DayPin],
    ) -> Result<Self, String> {
        let mut p = Self::new(holdings, query, days)?;
        for (day, plan) in &mut p.plan {
            let pin = pins
                .iter()
                .find(|x| &x.day == day)
                .ok_or_else(|| format!("{day}: not in the pins"))?;
            match (plan, &pin.missing) {
                (Day::Missing { .. }, Some(_)) => {}
                (Day::Missing { reason }, None) => {
                    return Err(format!("{day}: pinned as held, now {reason}"));
                }
                (Day::Held { .. }, Some(_)) => {
                    return Err(format!("{day}: a gap when pinned, held now"));
                }
                (Day::Held { files, partial }, None) => {
                    let mut want: Vec<&str> = pin.files.iter().map(|f| f.name.as_str()).collect();
                    let mut have: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
                    want.sort_unstable();
                    have.sort_unstable();
                    if want != have || *partial != pin.partial {
                        return Err(format!(
                            "{day}: held in {} now, pinned in {}",
                            have.join(","),
                            want.join(",")
                        ));
                    }
                    for f in files {
                        let sha = pin
                            .files
                            .iter()
                            .find(|x| x.name == f.name)
                            .map(|x| &x.sha256);
                        match (&f.sha256, sha) {
                            (Some(a), Some(b)) if !a.eq_ignore_ascii_case(b) => {
                                return Err(format!(
                                    "{}: the holdings state {a}, pinned {b}",
                                    f.name
                                ));
                            }
                            _ => f.sha256 = sha.cloned(),
                        }
                    }
                }
            }
        }
        Ok(p)
    }

    /// What each day is held in, as planned.
    pub fn plan(&self) -> &[(String, Day)] {
        &self.plan
    }

    /// The days finished so far, with the files read and their hashes.
    pub fn pins(&self) -> &[DayPin] {
        &self.pins
    }

    /// The files of the day being read, if one is: on a failed read, the file that
    /// failed is among them.
    pub fn reading(&self) -> Vec<String> {
        self.reading
            .iter()
            .map(|(f, _, _)| f.name.clone())
            .collect()
    }

    /// Where the days come from.
    pub fn describe(&self) -> String {
        self.holdings.describe()
    }

    /// Starts the next day: its announcements are queued, its files opened.
    fn start_day(&mut self) -> Result<(), String> {
        let Some((day, plan)) = self.plan.get(self.at).cloned() else {
            self.state = State::Done;
            return Ok(());
        };
        self.at += 1;
        let ts = day_ns(&day).unwrap_or(0);
        match plan {
            Day::Missing { reason } => {
                self.queued
                    .push_back(system(ts, &format!("store-gap day={day} reason={reason}")));
                self.pins.push(DayPin {
                    day,
                    files: Vec::new(),
                    partial: false,
                    missing: Some(reason),
                });
                self.state = State::Next;
            }
            Day::Held { files, partial } => {
                if partial {
                    self.queued
                        .push_back(system(ts, &format!("store-partial day={day}")));
                }
                let window = (ts, ts + 86_400_000_000_000);
                let mut sources = Vec::new();
                let mut mapped = BTreeSet::new();
                let horizon = self.expiries_within_days.map(|n| (ts, n));
                for f in &files {
                    if self.holdings.local()
                        && let Some(want) = &f.sha256
                    {
                        // Checked before any of its records is released.
                        let (got, _) = hashing::hash_all(self.holdings.open(f)?)
                            .map_err(|e| format!("{}: {e}", f.name))?;
                        if !got.eq_ignore_ascii_case(want) {
                            return Err(format!("{}: SHA-256 {got}, but {want} is stated", f.name));
                        }
                    }
                    let h = Hashed::new(self.holdings.open(f)?);
                    let digest = h.digest();
                    let dec = DynDecoder::inferred_with_buffer(
                        BufReader::new(h),
                        VersionUpgradePolicy::UpgradeToV3,
                    )
                    .map_err(|e| format!("{}: {e}", f.name))?;
                    let meta = dec.metadata();
                    // A sidecar or a file of another symbol, listed beside the day.
                    let parent = meta.stype_in == Some(SType::Parent);
                    if meta.schema.is_some_and(|x| x != self.schema)
                        || (parent
                            && !meta.symbols.is_empty()
                            && !meta.symbols.contains(&self.query.symbol))
                    {
                        drop(dec);
                        self.reading.push((f.clone(), digest, false));
                        continue;
                    }
                    let to_ns = |d: time::Date| {
                        u64::try_from(i64::from(d.to_julian_day()) - 2_440_588)
                            .map_or(0, |n| n * 86_400_000_000_000)
                    };
                    let mut denied = filter::Denied::new();
                    for m in &meta.mappings {
                        for iv in &m.intervals {
                            let (from, to) = (to_ns(iv.start_date), to_ns(iv.end_date));
                            let Ok(iid) = iv.symbol.parse::<u32>() else {
                                continue;
                            };
                            // Only the intervals of this day, from a file of several.
                            if to <= window.0
                                || from >= window.1
                                || !mapped.insert((iid, m.raw_symbol.clone()))
                            {
                                continue;
                            }
                            if !filter::admitted(&m.raw_symbol, horizon) {
                                // Over its interval only: the id may name an admitted
                                // contract in another.
                                denied.entry(iid).or_default().push((from, to));
                                continue;
                            }
                            let input = if parent {
                                &self.query.symbol
                            } else {
                                &m.raw_symbol
                            };
                            let msg = SymbolMappingMsg::new(
                                iid,
                                ts,
                                if parent {
                                    SType::Parent
                                } else {
                                    SType::RawSymbol
                                },
                                input,
                                SType::RawSymbol,
                                &m.raw_symbol,
                                from,
                                to,
                            )
                            .map_err(|e| format!("{}: {e}", f.name))?;
                            self.queued.push_back(RecordBuf::from(msg));
                        }
                    }
                    let mut filtered = DayFilter::new(dec, RType::from(self.schema), window);
                    if let Some((day, n)) = horizon {
                        filtered = filtered.horizon(day, n, denied);
                    }
                    sources.push(DbnSource::new(filtered, 0));
                    self.reading.push((f.clone(), digest, true));
                }
                self.pins.push(DayPin {
                    day,
                    files: Vec::new(),
                    partial,
                    missing: None,
                });
                self.state = State::Records(Merge::new(sources));
            }
        }
        Ok(())
    }

    /// Finishes the day being read: drains and checks each file's hash.
    fn end_day(&mut self) -> Result<(), String> {
        // Dropping the day's decoders drains their files and publishes the hashes.
        self.state = State::Next;
        let reading = std::mem::take(&mut self.reading);
        let pin = self.pins.last_mut().ok_or("no day being read")?;
        for (f, digest, used) in reading {
            let (sha256, bytes) = digest.get().map_err(|e| format!("{}: {e}", f.name))?;
            let verified = match &f.sha256 {
                Some(want) if !want.eq_ignore_ascii_case(&sha256) => {
                    return Err(format!(
                        "{}: SHA-256 {sha256}, but {want} is stated",
                        f.name
                    ));
                }
                Some(_) => true,
                None => false,
            };
            pin.files.push(FilePin {
                name: f.name,
                sha256,
                bytes,
                verified,
                used,
            });
        }
        self.state = State::Next;
        Ok(())
    }

    fn advance(&mut self) -> Result<Option<RecordBuf>, String> {
        loop {
            if let Some(r) = self.queued.pop_front() {
                return Ok(Some(r));
            }
            match &mut self.state {
                State::Done => return Ok(None),
                State::Next => self.start_day()?,
                State::Records(m) => match m.next_delivery().map_err(|e| e.to_string())? {
                    Some(d) => return Ok(Some(d.record)),
                    None => self.end_day()?,
                },
            }
        }
    }
}

impl DecodeRecordRef for StoreProducer {
    fn decode_record_ref(&mut self) -> dbn::Result<Option<RecordRef<'_>>> {
        self.cur = self.advance().map_err(fail)?;
        Ok(self.cur.as_ref().map(RecordBuf::as_rec_ref))
    }
}

impl DbnMetadata for StoreProducer {
    fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::day_ns;

    #[test]
    fn days_are_utc_midnights() {
        assert_eq!(day_ns("1970-01-01"), Some(0));
        assert_eq!(day_ns("2026-10-27"), Some(1_793_059_200_000_000_000));
        assert_eq!(day_ns("2024-02-29"), Some(1_709_164_800_000_000_000));
        assert_eq!(day_ns("2026-1x-01"), None);
    }
}
