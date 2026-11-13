//! Partial days (decision 0018): a day file that holds a list of contracts
//! for part of the day, not a provider's whole parent symbol.
//!
//! - **Coverage.** A file's contracts and time window come from its own DBN
//!   header (`stype_in = raw_symbol`, its symbols, start and end), checked
//!   against every record: each must map to a listed contract and fall inside
//!   the window. A partial day's coverage is pieces, one per file that went
//!   into it (pieces of the same window combined), recorded in its manifest
//!   entry (see the manifest module) beside the file's size and SHA-256. A day
//!   with no coverage there is complete (every contract of the parent, as a
//!   parent-symbol batch delivers it), and so is one that comes to hold every
//!   contract of the day's definitions over the whole day.
//! - **Import.** DBN that didn't come from a provider batch (a timeseries
//!   request) is uploaded to `.incoming/<id>/` with an `import.json` naming
//!   the dataset, schema, parent symbol, where it came from, and each file's
//!   size and SHA-256. Filing is as for a batch: every check before any move,
//!   moves by link, a ledger in `migrations/`, receipts in `provenance/<id>/`.
//! - **Merge.** A partial day already held, given contracts (or hours of
//!   contracts) it doesn't hold, is replaced by the two merged in timestamp
//!   order, a record at a time: written beside it, read back and checked
//!   input by input, recorded as pending, swapped in by rename, then made
//!   current. An interrupted swap is settled from the file's hash by the next
//!   scan. Contracts are never filed twice for the same hours.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use dbn::decode::{DbnMetadata as _, DecodeRecordRef as _};
use dbn::encode::{EncodeRecord as _, EncodeRecordRef as _};
use dbn::{Metadata, Record as _, RecordBuf, SType, VersionUpgradePolicy};
use serde_json::{Value, json};

use crate::ingest::{
    self, INCOMING, Ledger, Part, check_header, hash_file, place_new, safe, sync_dir, write_new,
};

pub use crate::manifest::is_partial;
#[cfg(test)]
pub use crate::manifest::{MANIFEST, read_manifest};
use crate::manifest::{read_day, set_entry, settle};
/// An import's receipt: what was uploaded, and where it came from.
pub const IMPORT: &str = "import.json";

/// Contracts held over one time window (ns, end exclusive).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Piece {
    pub symbols: BTreeSet<String>,
    pub start: u64,
    pub end: u64,
}

/// What a partial day holds: pieces, each a list of contracts over its own
/// window. A merge adds the incoming file's piece; pieces with the same
/// window are one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Coverage {
    pub pieces: Vec<Piece>,
}

impl Coverage {
    /// One list of contracts over one window, as one file's header gives.
    #[must_use]
    pub fn one(symbols: BTreeSet<String>, start: u64, end: u64) -> Self {
        Self {
            pieces: vec![Piece {
                symbols,
                start,
                end,
            }],
        }
    }

    /// Every contract held, in any piece.
    #[must_use]
    pub fn symbols(&self) -> BTreeSet<String> {
        self.pieces
            .iter()
            .flat_map(|p| p.symbols.iter().cloned())
            .collect()
    }

    /// The span of the pieces' windows.
    #[must_use]
    pub fn hull(&self) -> (u64, u64) {
        (
            self.pieces.iter().map(|p| p.start).min().unwrap_or(0),
            self.pieces.iter().map(|p| p.end).max().unwrap_or(0),
        )
    }

    /// `{ kind, stype, pieces: [{ symbols, window }] }`, with `symbols` (all
    /// of them) and `window` (the span of the pieces) for readers from
    /// before pieces: an approximation, true of no piece's hours.
    #[must_use]
    pub fn to_json(&self) -> Value {
        let pieces: Vec<Value> = self
            .pieces
            .iter()
            .map(|p| json!({ "symbols": p.symbols, "window": [p.start, p.end] }))
            .collect();
        let (start, end) = self.hull();
        json!({ "kind": "contracts", "stype": "raw_symbol", "pieces": pieces, "symbols": self.symbols(), "window": [start, end] })
    }

    /// Read `to_json`'s form, or the single `symbols` and `window` written
    /// before pieces, as one piece.
    #[must_use]
    pub fn from_json(v: &Value) -> Option<Self> {
        if v["kind"] != "contracts" {
            return None;
        }
        let piece = |p: &Value| {
            Some(Piece {
                symbols: p["symbols"]
                    .as_array()?
                    .iter()
                    .filter_map(|s| s.as_str().map(str::to_owned))
                    .collect(),
                start: p["window"][0].as_u64()?,
                end: p["window"][1].as_u64()?,
            })
        };
        let pieces = match v["pieces"].as_array() {
            Some(list) => list.iter().map(piece).collect::<Option<Vec<_>>>()?,
            None => vec![piece(v)?],
        };
        Some(Self::of(pieces))
    }

    /// These pieces, those with the same window combined, in window order.
    fn of(pieces: Vec<Piece>) -> Self {
        let mut by_window: BTreeMap<(u64, u64), BTreeSet<String>> = BTreeMap::new();
        for p in pieces {
            by_window
                .entry((p.start, p.end))
                .or_default()
                .extend(p.symbols);
        }
        Self {
            pieces: by_window
                .into_iter()
                .map(|((start, end), symbols)| Piece {
                    symbols,
                    start,
                    end,
                })
                .collect(),
        }
    }

    /// Both coverages' pieces.
    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        Self::of(self.pieces.iter().chain(&other.pieces).cloned().collect())
    }

    /// What both hold: each pair of pieces' common contracts over their
    /// common window.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        let mut pieces = Vec::new();
        for a in &self.pieces {
            for b in &other.pieces {
                let (start, end) = (a.start.max(b.start), a.end.min(b.end));
                let symbols: BTreeSet<String> =
                    a.symbols.intersection(&b.symbols).cloned().collect();
                if start < end && !symbols.is_empty() {
                    pieces.push(Piece {
                        symbols,
                        start,
                        end,
                    });
                }
            }
        }
        Self::of(pieces)
    }

    /// The contracts held in both, over hours that overlap.
    #[must_use]
    pub fn overlap(&self, other: &Self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for a in &self.pieces {
            for b in other
                .pieces
                .iter()
                .filter(|b| a.start < b.end && b.start < a.end)
            {
                out.extend(a.symbols.intersection(&b.symbols).cloned());
            }
        }
        out
    }

    /// Is `symbol` held over all of `[start, end)`?
    #[must_use]
    pub fn covers(&self, symbol: &str, start: u64, end: u64) -> bool {
        let mut windows: Vec<(u64, u64)> = self
            .pieces
            .iter()
            .filter(|p| p.symbols.contains(symbol))
            .map(|p| (p.start, p.end))
            .collect();
        windows.sort_unstable();
        let mut reached = start;
        for (s, e) in windows {
            if s > reached {
                break;
            }
            reached = reached.max(e);
        }
        reached >= end
    }
}

/// Open a day file for reading, records upgraded to the current DBN version
/// so files of different versions read (and merge) alike.
fn open(path: &Path) -> Result<dbn::decode::dbn::Decoder<impl std::io::Read>, String> {
    let mut d = dbn::decode::dbn::Decoder::from_zstd_file(path)
        .map_err(|e| format!("not a readable DBN file: {e}"))?;
    d.set_upgrade_policy(VersionUpgradePolicy::UpgradeToV3)
        .map_err(|e| e.to_string())?;
    Ok(d)
}

/// A partial day's coverage and record count, from its header, checked
/// against every record.
///
/// # Errors
/// If the file isn't a contract list (`stype_in = raw_symbol`), or a record
/// maps to no listed contract or falls outside the header's window.
pub fn inspect(path: &Path) -> Result<(Coverage, u64), String> {
    let mut d = open(path)?;
    let m = d.metadata().clone();
    if m.stype_in != Some(SType::RawSymbol) || m.symbols.is_empty() {
        return Err(
            "its header isn't a list of contracts (stype_in raw_symbol); file whole-symbol batches with /incorporate"
                .into(),
        );
    }
    let symbols: BTreeSet<String> = m.symbols.iter().cloned().collect();
    let end = m.end.ok_or("its header has no end time")?.get();
    let mut ids = HashSet::new();
    for sm in &m.mappings {
        if !symbols.contains(&sm.raw_symbol) {
            return Err(format!(
                "its header maps {} but doesn't list it",
                sm.raw_symbol
            ));
        }
        for iv in &sm.intervals {
            ids.insert(iv.symbol.parse::<u32>().map_err(|_| {
                format!(
                    "{}: mapped to {:?}, not an instrument ID",
                    sm.raw_symbol, iv.symbol
                )
            })?);
        }
    }
    let mut n = 0u64;
    while let Some(r) = d.decode_record_ref().map_err(|e| e.to_string())? {
        let id = r.header().instrument_id;
        if !ids.contains(&id) {
            return Err(format!(
                "record {n} is instrument {id}, which maps to none of its contracts"
            ));
        }
        let t = r.raw_index_ts();
        if t < m.start || t >= end {
            return Err(format!(
                "record {n} at {t} is outside its window {}..{end}",
                m.start
            ));
        }
        n += 1;
    }
    Ok((Coverage::one(symbols, m.start, end), n))
}

/// Every record of a day file, in order, and its header.
#[cfg(test)]
fn read_all(path: &Path) -> Result<(Metadata, Vec<RecordBuf>), String> {
    let mut d = open(path)?;
    let m = d.metadata().clone();
    let mut out = Vec::new();
    while let Some(r) = d.decode_record_ref().map_err(|e| e.to_string())? {
        out.push(r.to_owned());
    }
    Ok((m, out))
}

/// Instrument IDs, each to its contract.
type Ids = BTreeMap<u32, String>;

/// Each instrument ID a header maps, to its contract.
///
/// # Errors
/// If a mapping isn't to an instrument ID, or two contracts share one.
fn ids(m: &Metadata) -> Result<Ids, String> {
    let mut out = BTreeMap::new();
    for sm in &m.mappings {
        for iv in &sm.intervals {
            let id = iv.symbol.parse::<u32>().map_err(|_| {
                format!(
                    "{}: mapped to {:?}, not an instrument ID",
                    sm.raw_symbol, iv.symbol
                )
            })?;
            if let Some(other) = out.insert(id, sm.raw_symbol.clone())
                && other != sm.raw_symbol
            {
                return Err(format!(
                    "instrument {id} is both {other} and {}",
                    sm.raw_symbol
                ));
            }
        }
    }
    Ok(out)
}

/// The instrument IDs of two day files' headers, refused if one ID is two
/// different contracts across them (their records couldn't be told apart).
///
/// # Errors
/// If so, or a header can't be read.
fn same_ids(a: &Path, b: &Path) -> Result<(Ids, Ids), String> {
    let (ia, ib) = (ids(open(a)?.metadata())?, ids(open(b)?.metadata())?);
    for (id, s) in &ib {
        if let Some(t) = ia.get(id)
            && t != s
        {
            return Err(format!(
                "instrument {id} is {t} in the day held and {s} in this file; the two can't be merged"
            ));
        }
    }
    Ok((ia, ib))
}

/// A count and running SHA-256 of records, in order.
#[derive(Default)]
struct Tally {
    records: u64,
    hash: sha2::Sha256,
}

impl Tally {
    fn add(&mut self, bytes: &[u8]) {
        use sha2::Digest as _;
        self.records += 1;
        self.hash.update(bytes);
    }

    fn finish(self) -> (u64, Vec<u8>) {
        use sha2::Digest as _;
        (self.records, self.hash.finalize().to_vec())
    }
}

/// Merge partial days (the day held first) into a new file `out`, record by
/// record, in order of their index timestamps (ties: the earlier input's
/// first, each in its own order), never holding more than one record of
/// each. Then read it back, sorting each record to the input it came from
/// (by its instrument, and, for a contract in several, which input's
/// coverage holds its time), and check each input's records are there, all
/// of them, in order: by count and a running hash. Returns its record count.
///
/// # Errors
/// If the headers can't be merged, an instrument ID is two contracts, `out`
/// exists, or the file read back differs from what was written.
pub fn merge(inputs: &[(&Path, &Coverage)], out: &Path) -> Result<u64, String> {
    let mut maps: Vec<Ids> = Vec::new();
    let mut all = Ids::new();
    for (p, _) in inputs {
        let m = ids(open(p)?.metadata())?;
        for (id, s) in &m {
            if let Some(t) = all.insert(*id, s.clone())
                && t != *s
            {
                return Err(format!(
                    "instrument {id} is {t} in one input and {s} in another; they can't be merged"
                ));
            }
        }
        maps.push(m);
    }
    let mut decoders = inputs
        .iter()
        .map(|(p, _)| open(p))
        .collect::<Result<Vec<_>, _>>()?;
    let (first, rest) = decoders.split_first().ok_or("nothing to merge")?;
    let m = first
        .metadata()
        .clone()
        .merge(rest.iter().map(|d| d.metadata().clone()))
        .map_err(|e| format!("the headers can't be merged: {e}"))?;
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(out)
        .map_err(|e| format!("{}: {e}", out.display()))?;
    let mut written: Vec<Tally> = inputs.iter().map(|_| Tally::default()).collect();
    {
        let mut enc = dbn::encode::dbn::Encoder::with_zstd(std::io::BufWriter::new(&file), &m)
            .map_err(|e| e.to_string())?;
        let next = |d: &mut dbn::decode::dbn::Decoder<_>| -> Result<Option<RecordBuf>, String> {
            Ok(d.decode_record_ref()
                .map_err(|e| e.to_string())?
                .map(|r| r.to_owned()))
        };
        let mut heads = decoders
            .iter_mut()
            .map(next)
            .collect::<Result<Vec<_>, _>>()?;
        while let Some((_, pick)) = heads
            .iter()
            .enumerate()
            .filter_map(|(i, h)| Some((h.as_ref()?.raw_index_ts(), i)))
            .min()
        {
            let r = heads[pick].take().ok_or("lost a record")?;
            enc.encode_record_ref(r.as_rec_ref())
                .map_err(|e| e.to_string())?;
            written[pick].add(r.as_ref());
            heads[pick] = next(&mut decoders[pick])?;
        }
        enc.flush().map_err(|e| e.to_string())?;
    }
    file.sync_all().map_err(|e| e.to_string())?;
    if let Some(dir) = out.parent() {
        sync_dir(dir).map_err(|e| e.to_string())?;
    }
    // Read back, each record sorted to its input.
    let mut back = open(out)?;
    if back.metadata().symbols != m.symbols {
        return Err("the merged file's contracts differ from its inputs'".into());
    }
    let mut seen: Vec<Tally> = inputs.iter().map(|_| Tally::default()).collect();
    let mut last = 0;
    while let Some(r) = back.decode_record_ref().map_err(|e| e.to_string())? {
        let (id, t) = (r.header().instrument_id, r.raw_index_ts());
        if t < last {
            return Err(format!(
                "the merged file goes back in time, to {t} after {last}"
            ));
        }
        last = t;
        let Some(s) = all.get(&id) else {
            return Err(format!("the merged file has instrument {id}, of no input"));
        };
        let of: Vec<usize> = (0..inputs.len())
            .filter(|i| maps[*i].contains_key(&id))
            .collect();
        let of: Vec<usize> = match of[..] {
            [_] => of,
            _ => of
                .into_iter()
                .filter(|i| inputs[*i].1.covers(s, t, t + 1))
                .collect(),
        };
        let [input] = of[..] else {
            return Err(format!(
                "the merged file's record at {t} of {s} can't be told to one input"
            ));
        };
        seen[input].add(r.as_ref());
    }
    let total = written.iter().map(|w| w.records).sum();
    for (i, (w, s)) in written.into_iter().zip(seen).enumerate() {
        let (w, s) = (w.finish(), s.finish());
        if w != s {
            return Err(format!(
                "the merged file holds {} records of {} that read back differently ({} written)",
                s.0,
                inputs[i].0.display(),
                w.0
            ));
        }
    }
    Ok(total)
}

/// What [`holds_all`] found: the partial day's records and contracts, every
/// one of them in the complete day's file, and that file's records.
pub(crate) struct Held {
    pub records: u64,
    pub contracts: usize,
    pub total: u64,
}

/// Does `whole`, a complete day (a parent's whole chain), hold every record
/// of `held`, a partial day of coverage `cov`, and nothing else over its
/// hours? Restricted to `held`'s instruments, each over its contract's
/// pieces, `whole`'s records must be `held`'s, contract by contract, in
/// order: by count and a running SHA-256 of each record's bytes.
///
/// # Errors
/// If not (naming a contract that differs), or a file can't be read.
pub(crate) fn holds_all(held: &Path, cov: &Coverage, whole: &Path) -> Result<Held, String> {
    let mut d = open(held)?;
    let by_id = ids(d.metadata())?;
    let mut hours: BTreeMap<&str, Vec<(u64, u64)>> = BTreeMap::new();
    for p in &cov.pieces {
        for s in &p.symbols {
            hours.entry(s.as_str()).or_default().push((p.start, p.end));
        }
    }
    let mut want: BTreeMap<&str, Tally> = BTreeMap::new();
    let mut records = 0;
    while let Some(r) = d.decode_record_ref().map_err(|e| e.to_string())? {
        let id = r.header().instrument_id;
        let s = by_id
            .get(&id)
            .ok_or_else(|| format!("the day held has instrument {id}, of none of its contracts"))?;
        want.entry(s.as_str()).or_default().add(r.as_ref());
        records += 1;
    }
    let mut d = open(whole)?;
    if d.metadata().stype_in != Some(SType::Parent) {
        return Err("its header isn't a parent symbol's whole chain (stype_in parent)".into());
    }
    let mut got: BTreeMap<&str, Tally> = BTreeMap::new();
    let mut total = 0;
    while let Some(r) = d.decode_record_ref().map_err(|e| e.to_string())? {
        total += 1;
        let t = r.raw_index_ts();
        if let Some(s) = by_id.get(&r.header().instrument_id)
            && hours
                .get(s.as_str())
                .is_some_and(|w| w.iter().any(|(a, b)| *a <= t && t < *b))
        {
            got.entry(s.as_str()).or_default().add(r.as_ref());
        }
    }
    for s in want
        .keys()
        .chain(got.keys())
        .copied()
        .collect::<BTreeSet<_>>()
    {
        let (w, g) = (
            want.remove(s).unwrap_or_default().finish(),
            got.remove(s).unwrap_or_default().finish(),
        );
        if w != g {
            return Err(format!(
                "the day held has {} records of {s} over its hours and this file {}{}; nothing is superseded",
                w.0,
                g.0,
                if w.0 == g.0 { ", which differ" } else { "" }
            ));
        }
    }
    Ok(Held {
        records,
        contracts: cov.symbols().len(),
        total,
    })
}

/// Is this partial day now complete: does it hold every contract of the
/// day's definitions (the `definition` day file of the same parent, on any of
/// `roots`), each over the whole day? Returns that definition file if so.
///
/// # Errors
/// If the definitions can't be read.
fn complete_by(
    roots: &[PathBuf],
    rel: &Path,
    day: &str,
    cov: &Coverage,
) -> Result<Option<PathBuf>, String> {
    let Some(def) = roots.iter().map(|r| r.join(rel)).find(|p| p.is_file()) else {
        return Ok(None);
    };
    let from = ingest::day_ns(day).ok_or("not a day")?;
    let mut any = false;
    {
        let mut d = open(&def)?;
        while let Some(r) = d.decode_record_ref().map_err(|e| e.to_string())? {
            let Some(def_msg) = r.get::<dbn::InstrumentDefMsg>() else {
                continue;
            };
            let symbol = def_msg.raw_symbol().map_err(|e| e.to_string())?;
            if !cov.covers(symbol, from, from + 86_400_000_000_000) {
                return Ok(None);
            }
            any = true;
        }
    }
    Ok(any.then_some(def))
}

/// `YYYY-MM-DD` of an import's file name, `YYYY-MM-DD.dbn.zst`.
fn import_day(name: &str) -> Option<String> {
    let day = name.strip_suffix(".dbn.zst")?;
    (day.len() == 10 && ingest::day_ns(day).is_some() && day.as_bytes()[4] == b'-')
        .then(|| day.to_owned())
}

/// Do these contracts belong to `parent`? For options, a parent `ROOT.OPT`
/// holds OCC symbols whose root (the first six characters, padded) is `ROOT`.
fn belongs(parent: &str, symbols: &BTreeSet<String>) -> Result<(), String> {
    if !parent.ends_with(".OPT") {
        return Err(format!(
            "only option parents (ROOT.OPT) take contract lists, not {parent}"
        ));
    }
    match symbols.iter().find(|s| !of_parent(parent, s)) {
        Some(s) => Err(format!("{s:?} isn't a contract of {parent}")),
        None => Ok(()),
    }
}

/// Make a partial day complete when it holds every contract of the day's
/// definitions (of the same parent), each over the whole day: its entry
/// keeps its file's record and drops its coverage. Returns whether it did.
///
/// # Errors
/// If the definitions or the manifest can't be read, or it can't be written.
fn promote(
    dir: &Path,
    day: &str,
    roots: &[PathBuf],
    level: &Path,
    cov: &Coverage,
) -> Result<bool, String> {
    let definition = level
        .parent()
        .ok_or("no parent level")?
        .join("definition")
        .join(format!("{day}.dbn.zst"));
    let Some(def) = complete_by(roots, &definition, day, cov)? else {
        return Ok(false);
    };
    let mut entry = read_day(dir, day)?.ok_or("the day's entry vanished")?;
    if let Some(o) = entry.as_object_mut() {
        o.remove("coverage");
        o.remove("pending");
    }
    entry["completed"] = json!({
        "by": format!("every contract of {} held all day", def.display()),
        "pieces": cov.pieces.len(), "at": tinytrader_observe::unix_seconds(),
    });
    set_entry(dir, day, Some(entry))?;
    Ok(true)
}

/// What a group of filings does to one day (decision 0024, section 8b),
/// planned before anything moves: the day's file, held or where its first
/// input goes, and every input, merged into it in one write.
#[derive(Clone)]
struct Day {
    day: String,
    at: PathBuf,
    /// The partial day held: its coverage and recorded record count.
    held: Option<(Coverage, Option<u64>)>,
    inputs: Vec<Input>,
    /// What it holds once every input so far is in it.
    cov: Coverage,
    level: PathBuf,
    everywhere: Vec<PathBuf>,
}

#[derive(Clone)]
struct Input {
    /// The filing it is of, in the group.
    of: usize,
    from: PathBuf,
    cov: Coverage,
    records: u64,
    entry: Value,
}

/// Did an earlier run of import `id` file into the day at `path`? Its
/// ledger moved a file there, or merged or swapped one in (a merge whose
/// swap was undone leaves the day without the file's coverage, which the
/// caller checks).
fn filed_by(roots: &[PathBuf], id: &str, path: &Path) -> bool {
    let suffix = format!("-import-{id}.events.jsonl");
    for e in roots.iter().flat_map(|r| {
        fs::read_dir(r.join("migrations"))
            .into_iter()
            .flatten()
            .flatten()
    }) {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.strip_suffix(&suffix).is_none_or(|d| d.len() != 10) {
            continue;
        }
        let text = fs::read_to_string(e.path()).unwrap_or_default();
        for ev in text
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        {
            let to = |k: &str| ev[k].as_str().map(Path::new) == Some(path);
            let at = match ev["step"].as_str() {
                Some("moved") => to("to"),
                Some("swapped") => to("path"),
                Some("merged") => ev["inputs"][0].as_str().map(Path::new) == Some(path),
                _ => false,
            };
            if at {
                return true;
            }
        }
    }
    false
}

/// A filing of a group: an import waiting in `.incoming/<id>`, or with
/// `parent`, a provider batch bought as a list of that parent's contracts.
pub struct Filing {
    pub parts: Vec<Part>,
    pub elsewhere: Vec<PathBuf>,
    pub id: String,
    pub today: String,
    pub parent: Option<String>,
}

/// A filing whose checks passed, until it is finished.
struct Member {
    ledger: Ledger,
    receipt_path: PathBuf,
    summary: Value,
    duplicates: Vec<(String, PathBuf, PathBuf)>,
    done: Vec<Value>,
    failed: Option<String>,
}

/// File a group of imports and batches together (decision 0024, section
/// 8b), each answering for itself as if filed alone, one after the other:
/// every filing's checks run first, each against the days held and what the
/// filings before it will file; a filing refused is refused alone. Then each
/// day the group touches is written once: the day held (or the first input,
/// moved into place) and every input, merged in timestamp order.
pub fn import_group(group: &[Filing]) -> Vec<Result<Value, String>> {
    let mut days: BTreeMap<PathBuf, Day> = BTreeMap::new();
    let mut members: Vec<Result<Member, String>> = Vec::new();
    for (of, f) in group.iter().enumerate() {
        let mut mine = days.clone();
        let m = match &f.parent {
            Some(p) => batch_receipt(&f.parts, &f.id, p),
            None => Ok(()),
        }
        .and_then(|()| prepare(f, of, &mut mine));
        if m.is_ok() {
            days = mine;
        }
        members.push(m);
    }
    for d in days.into_values() {
        let of: BTreeSet<usize> = d.inputs.iter().map(|i| i.of).collect();
        if let Err(e) = file_day(d, group, &mut members) {
            for i in of {
                if let Ok(m) = &mut members[i] {
                    m.failed.get_or_insert_with(|| e.clone());
                }
            }
        }
    }
    members
        .into_iter()
        .zip(group)
        .map(|(m, f)| finish(m?, f))
        .collect()
}

/// A filing's checks: every file against its receipt and its own header,
/// and each day against what is held and what the group files before it
/// (`days`, to which its inputs are added).
#[expect(
    clippy::too_many_lines,
    reason = "one pass of checks, kept together so nothing moves before every check"
)]
fn prepare(f: &Filing, of: usize, days: &mut BTreeMap<PathBuf, Day>) -> Result<Member, String> {
    let (parts, id) = (&f.parts[..], f.id.as_str());
    let roots: Vec<&Path> = parts.iter().map(|p| p.root.as_path()).collect();
    let first = parts.first().ok_or("no part of the import was found")?;
    let holder = |name: &str| parts.iter().find(|p| p.inc.join(name).exists());
    let receipt_path = holder(IMPORT)
        .ok_or_else(|| format!("{IMPORT}: missing"))?
        .inc
        .join(IMPORT);
    let receipt: Value = serde_json::from_str(
        &fs::read_to_string(&receipt_path).map_err(|e| format!("{IMPORT}: {e}"))?,
    )
    .map_err(|e| format!("{IMPORT}: {e}"))?;
    let field = |k: &str| {
        receipt[k]
            .as_str()
            .filter(|s| safe(s))
            .ok_or_else(|| format!("{IMPORT} has no usable {k:?}"))
    };
    let (dataset, schema, parent) = (field("dataset")?, field("schema")?, field("symbol")?);
    if receipt["source"]
        .as_object()
        .is_none_or(serde_json::Map::is_empty)
    {
        return Err(format!(
            "{IMPORT} must say where the data came from (\"source\": the request, the tool and its commit, when it was fetched)"
        ));
    }
    let level = PathBuf::from(dataset).join(parent).join(schema);
    let mut everywhere: Vec<PathBuf> = roots.iter().map(|r| r.to_path_buf()).collect();
    everywhere.extend(
        f.elsewhere
            .iter()
            .filter(|e| !roots.contains(&e.as_path()))
            .cloned(),
    );

    let mut ledger = Ledger::open(
        &roots
            .iter()
            .map(|r| {
                r.join("migrations")
                    .join(format!("{}-import-{id}.events.jsonl", f.today))
            })
            .collect::<Vec<_>>(),
    )?;
    ledger.record(json!({ "step": "begin", "import": id, "dataset": dataset, "schema": schema, "symbol": parent, "parts": parts.iter().map(|p| &p.inc).collect::<Vec<_>>() }))?;

    let files = receipt["files"]
        .as_array()
        .filter(|f| !f.is_empty())
        .ok_or_else(|| format!("{IMPORT} lists no files"))?;
    let mut duplicates = Vec::new();
    let mut problems = Vec::new();
    let mut check = |f: &Value| -> Result<(), String> {
        let name = f["filename"].as_str().unwrap_or_default();
        // The day: given (a provider batch's file names say it their own way),
        // or the file's name, YYYY-MM-DD.dbn.zst.
        let day = f["day"]
            .as_str()
            .filter(|d| d.len() == 10 && ingest::day_ns(d).is_some())
            .map(str::to_owned)
            .or_else(|| import_day(name))
            .filter(|_| safe(name))
            .ok_or_else(|| format!("{name:?}: files are named YYYY-MM-DD.dbn.zst"))?;
        let want = f["hash"]
            .as_str()
            .unwrap_or_default()
            .trim_start_matches("sha256:")
            .to_ascii_lowercase();
        let part = holder(name).unwrap_or(first);
        let path = part.inc.join(name);
        let rel = level.join(format!("{day}.dbn.zst"));
        // The day where it's held, settled if a merge was interrupted.
        let mut held = None;
        let mut held_source = Value::Null;
        for r in &everywhere {
            let p = r.join(&rel);
            // An error (a volume dropping out) refuses the import, never reads
            // as "not held" (P0, 2026-11-10).
            let there = p.try_exists().map_err(|e| {
                format!(
                    "{}: {e}; not imported, since it can't be told whether this day is held there",
                    p.display()
                )
            })?;
            if there {
                let entry = settle(&r.join(&level), &day)?;
                held_source = entry.as_ref().map_or(Value::Null, |e| e["source"].clone());
                held = Some((p, entry.filter(is_partial)));
                break;
            }
        }
        let ours = |p: &Path| {
            held_source == format!("import/{id}").as_str() || filed_by(&everywhere, id, p)
        };
        let Ok(meta) = fs::metadata(&path) else {
            // Filed, or merged, by an earlier run of this import that stopped
            // after its incoming copy was gone?
            if let Some((p, _)) = &held
                && (ours(p) || hash_file(p)? == want)
            {
                return Ok(());
            }
            return Err(format!("{name}: missing"));
        };
        if f["size"].as_u64().is_some_and(|s| s != meta.len()) {
            return Err(format!(
                "{name}: {} bytes, {IMPORT} says {}",
                meta.len(),
                f["size"]
            ));
        }
        let got = hash_file(&path)?;
        if got != want {
            return Err(format!("{name}: SHA-256 {got}, {IMPORT} says {want}"));
        }
        check_header(&path, dataset, schema, &day).map_err(|e| format!("{name}: {e}"))?;
        let (cov, records) = inspect(&path).map_err(|e| format!("{name}: {e}"))?;
        belongs(parent, &cov.symbols()).map_err(|e| format!("{name}: {e}"))?;
        // Its coverage on record, so what went into a day can be rebuilt.
        ledger.record(json!({ "step": "verified", "file": path, "day": day, "bytes": meta.len(), "sha256": got, "records": records, "contracts": cov.symbols().len(), "coverage": cov.to_json() }))?;
        let entry = json!({ "file": format!("{day}.dbn.zst"), "size": meta.len(), "sha256": got, "records": records, "coverage": cov.to_json(), "source": format!("import/{id}") });
        let held_sha = match &held {
            Some((p, _)) => Some(hash_file(p)?),
            None => None,
        };
        let (at, held) = match held {
            None => (part.root.join(&rel), None),
            Some((existing, _)) if held_sha.as_deref() == Some(got.as_str()) => {
                duplicates.push((day, path, existing));
                return Ok(());
            }
            Some((existing, None)) => {
                return Err(format!(
                    "{name}: {} holds the whole day already",
                    existing.display()
                ));
            }
            // Merged by an earlier run of this import that stopped before
            // removing its incoming copy.
            Some((existing, Some(e)))
                if Coverage::from_json(&e["coverage"]).is_some_and(|h| {
                    cov.pieces
                        .iter()
                        .all(|p| p.symbols.iter().all(|s| h.covers(s, p.start, p.end)))
                }) && ours(&existing) =>
            {
                duplicates.push((day, path, existing));
                return Ok(());
            }
            Some((existing, Some(e))) => {
                let held_cov = Coverage::from_json(&e["coverage"])
                    .ok_or_else(|| format!("{name}: the held day's coverage is unreadable"))?;
                // The day held is the file its entry records.
                let size = fs::metadata(&existing).map_err(|e| e.to_string())?.len();
                let sha = held_sha.unwrap_or_default();
                if e["size"].as_u64().is_some_and(|s| s != size)
                    || e["sha256"].as_str().is_some_and(|h| h != sha)
                {
                    return Err(format!(
                        "{name}: {} is {size} bytes, SHA-256 {sha}, but its manifest records {} bytes, SHA-256 {}; nothing is merged into it until that is looked at",
                        existing.display(),
                        e["size"],
                        e["sha256"]
                    ));
                }
                (existing, Some((held_cov, e["records"].as_u64())))
            }
        };
        // The day as the group leaves it before this file: held, or filed
        // by a filing before this one.
        let key = PathBuf::from(first.root.file_name().unwrap_or_default()).join(&rel);
        let d = days.entry(key).or_insert_with(|| Day {
            day: day.clone(),
            at,
            cov: held
                .as_ref()
                .map_or(Coverage { pieces: Vec::new() }, |h| h.0.clone()),
            held,
            inputs: Vec::new(),
            level: level.clone(),
            everywhere: everywhere.clone(),
        });
        let both = d.cov.overlap(&cov);
        if let Some(s) = both.first() {
            return Err(format!(
                "{name}: {} already holds {} of these contracts over the same hours ({s}, …); only contracts not held are filed",
                d.at.display(),
                both.len()
            ));
        }
        let others = d.held.as_ref().map(|_| &d.at).into_iter();
        for other in others.chain(d.inputs.iter().map(|i| &i.from)) {
            same_ids(other, &path).map_err(|e| format!("{name}: {e}"))?;
        }
        d.cov = d.cov.union(&cov);
        d.inputs.push(Input {
            of,
            from: path,
            cov,
            records,
            entry,
        });
        Ok(())
    };
    for f in files {
        if let Err(e) = check(f) {
            problems.push(e);
        }
    }
    if !problems.is_empty() {
        ledger.record(json!({ "step": "refused", "problems": problems }))?;
        return Err(problems.join("; "));
    }
    Ok(Member {
        ledger,
        receipt_path,
        summary: json!({ "import": id, "dataset": dataset, "schema": schema, "symbol": parent }),
        duplicates,
        done: Vec::new(),
        failed: None,
    })
}

/// Write one day the group touches: its first input moved into place if
/// nothing holds it, then every other input merged into it, in one write.
fn file_day(
    d: Day,
    group: &[Filing],
    members: &mut [Result<Member, String>],
) -> Result<(), String> {
    let dir = d.at.parent().ok_or("no folder")?.to_path_buf();
    let mut log = |of: usize, ev: Value| match &mut members[of] {
        Ok(m) => m.ledger.record(ev),
        Err(_) => Ok(()),
    };
    let mut inputs = d.inputs.into_iter();
    let mut filed = Vec::new();
    let (held, held_n) = match d.held {
        Some(h) => h,
        None => {
            let i = inputs.next().ok_or("nothing to file")?;
            // The manifest first: a crash before the move leaves an entry
            // for a missing file (filed again on a retry), never a partial
            // file taken for a whole day.
            set_entry(&dir, &d.day, Some(i.entry))?;
            place_new(&i.from, &d.at)
                .map_err(|e| format!("{} -> {}: {e}", i.from.display(), d.at.display()))?;
            log(i.of, json!({ "step": "moved", "from": i.from, "to": d.at }))?;
            filed.push((i.of, "new"));
            (i.cov, Some(i.records))
        }
    };
    let rest: Vec<Input> = inputs.collect();
    let union = d.cov;
    if let Some(lead) = rest.first() {
        // Written beside the day's file, on its volume, so the swap is a rename.
        let vol_root = d
            .everywhere
            .iter()
            .find(|r| d.at.starts_with(r))
            .ok_or("the held day isn't under a provider folder")?;
        let work = vol_root.join(INCOMING).join(&group[lead.of].id);
        fs::create_dir_all(&work).map_err(|e| e.to_string())?;
        let out = work.join(format!("merged-{}.dbn.zst", d.day));
        // Left by an earlier run, whichever filing led it.
        for i in &rest {
            let id = &group[i.of].id;
            let _ = fs::remove_file(
                vol_root
                    .join(INCOMING)
                    .join(id)
                    .join(out.file_name().unwrap_or_default()),
            );
        }
        // Partial days written before sidecars move to theirs, so the
        // swap below touches only this day's.
        crate::manifest::write_days(&dir, &[])?;
        let mut all = vec![(d.at.as_path(), &held)];
        all.extend(rest.iter().map(|i| (i.from.as_path(), &i.cov)));
        let records = merge(&all, &out).inspect_err(|_| {
            let _ = fs::remove_file(&out);
        })?;
        crash("written")?;
        // Every record of each, and nothing else.
        let sum: u64 = rest.iter().map(|i| i.records).sum();
        if let Some(h) = held_n
            && h + sum != records
        {
            let _ = fs::remove_file(&out);
            return Err(format!(
                "{}: the merge holds {records} records, but the day held {h} and the files {sum}; nothing was swapped in",
                d.at.display()
            ));
        }
        let sha = hash_file(&out)?;
        let size = fs::metadata(&out).map_err(|e| e.to_string())?.len();
        let entry = read_day(&dir, &d.day)?.ok_or("the held day's entry vanished")?;
        let mut next = json!({ "file": format!("{}.dbn.zst", d.day), "size": size, "sha256": sha, "records": records, "coverage": union.to_json(), "source": format!("import/{}", group[lead.of].id) });
        // The day's condition stays with it.
        if let Some(c) = entry.get("condition").cloned() {
            next["condition"] = c;
        }
        crate::manifest::write_pending(&dir, &d.day, &next)?;
        let froms: Vec<&PathBuf> = rest.iter().map(|i| &i.from).collect();
        for i in &rest {
            let mut inputs = vec![&d.at, &i.from];
            inputs.extend(froms.iter().copied().filter(|f| **f != i.from));
            log(
                i.of,
                json!({ "step": "merged", "inputs": inputs, "output": out, "sha256": sha, "records": records }),
            )?;
        }
        crash("pending")?;
        fs::rename(&out, &d.at)
            .map_err(|e| format!("{} -> {}: {e}", out.display(), d.at.display()))?;
        sync_dir(&dir).map_err(|e| e.to_string())?;
        crash("swapped")?;
        crate::manifest::promote_pending(&dir, &d.day)?;
        for i in &rest {
            fs::remove_file(&i.from).map_err(|e| e.to_string())?;
            log(
                i.of,
                json!({ "step": "swapped", "path": d.at, "sha256": sha }),
            )?;
            filed.push((i.of, "merged"));
        }
        let _ = fs::remove_dir(&work);
    }
    let complete = promote(&dir, &d.day, &d.everywhere, &d.level, &union)?;
    for (of, how) in filed {
        if let Ok(m) = &mut members[of] {
            m.done.push(json!({ "day": d.day, "path": d.at, "filed": how, "contracts": union.symbols().len(), "pieces": union.pieces.len(), "complete": complete }));
        }
    }
    Ok(())
}

/// In tests, stop at `at` as a crash would, when told to.
pub(crate) fn crash(at: &str) -> Result<(), String> {
    #[cfg(test)]
    if tests::CRASH.with(std::cell::Cell::get) == Some(at) {
        return Err(format!("crashed {at}"));
    }
    let _ = at;
    Ok(())
}

/// A filing's last steps once its days are filed: its duplicates dropped,
/// its days' conditions, its receipts kept and its folders removed.
fn finish(mut m: Member, f: &Filing) -> Result<Value, String> {
    let (parts, id) = (&f.parts[..], f.id.as_str());
    let roots: Vec<&Path> = parts.iter().map(|p| p.root.as_path()).collect();
    if let Some(why) = m.failed {
        m.ledger
            .record(json!({ "step": "stopped", "error": why }))?;
        return Err(why);
    }
    let mut done = m.done;
    for (day, from, existing) in m.duplicates {
        fs::remove_file(&from).map_err(|e| e.to_string())?;
        m.ledger.record(
            json!({ "step": "duplicate-dropped", "file": from, "identical_to": existing }),
        )?;
        done.push(json!({ "day": day, "path": existing, "filed": "already present" }));
    }
    let receipt_path = m.receipt_path;
    let mut ledger = m.ledger;
    // The days' conditions, from a provider batch's condition.json.
    let mut dirs: Vec<PathBuf> = parts.iter().map(|p| p.inc.clone()).collect();
    dirs.extend(roots.iter().map(|r| r.join("provenance").join(id)));
    crate::conditions::record_batch(&dirs, &json!({ "days": done }), id)?;

    // The receipt goes to provenance on every volume involved.
    let text = fs::read(&receipt_path).map_err(|e| e.to_string())?;
    for root in &roots {
        let to = root.join("provenance").join(id).join(IMPORT);
        if to.exists() {
            if fs::read(&to).map_err(|e| e.to_string())? != text {
                return Err(format!(
                    "{}: provenance already holds a different {IMPORT}",
                    to.display()
                ));
            }
        } else {
            write_new(&to, &text).map_err(|e| format!("{}: {e}", to.display()))?;
        }
        ledger.record(json!({ "step": "receipt", "file": to }))?;
    }
    // Other receipts beside it (a provider batch's manifest and metadata) are
    // kept too, never over a different one.
    for part in parts {
        for e in fs::read_dir(&part.inc).into_iter().flatten().flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name == IMPORT || !name.ends_with(".json") || !safe(&name) {
                continue;
            }
            let bytes = fs::read(e.path()).map_err(|e| e.to_string())?;
            let mut kept = true;
            for root in &roots {
                let to = root.join("provenance").join(id).join(&name);
                if !to.exists() {
                    write_new(&to, &bytes).map_err(|e| format!("{}: {e}", to.display()))?;
                    ledger.record(json!({ "step": "receipt", "file": to }))?;
                } else if fs::read(&to).map_err(|e| e.to_string())? != bytes {
                    kept = false;
                }
            }
            if kept {
                fs::remove_file(e.path()).map_err(|e| e.to_string())?;
            }
        }
    }
    let mut left = Vec::new();
    for part in parts {
        let _ = fs::remove_file(part.inc.join(IMPORT));
        left.extend(
            fs::read_dir(&part.inc)
                .into_iter()
                .flatten()
                .flatten()
                .map(|e| part.inc.join(e.file_name()).display().to_string()),
        );
        let _ = fs::remove_dir(&part.inc);
        let _ = fs::remove_dir(part.root.join(INCOMING));
    }
    let mut summary = m.summary;
    summary["days"] = json!(done);
    summary["left_behind"] = json!(left);
    summary["completed_at"] = json!(tinytrader_observe::unix_seconds());
    let text = serde_json::to_string_pretty(&summary).map_err(|e| e.to_string())?;
    for root in &roots {
        let mut out = root
            .join("migrations")
            .join(format!("{}-import-{id}.json", f.today));
        if out.exists() {
            out = root.join("migrations").join(format!(
                "{}-import-{id}-{}.json",
                f.today,
                tinytrader_observe::unix_seconds() as u64
            ));
        }
        write_new(&out, text.as_bytes()).map_err(|e| format!("{}: {e}", out.display()))?;
    }
    ledger.record(json!({ "step": "complete" }))?;
    Ok(summary)
}

/// A batch's `import.json`, written from its receipts unless an earlier,
/// interrupted run wrote it.
fn batch_receipt(parts: &[Part], batch: &str, parent: &str) -> Result<(), String> {
    let read = |name: &str| -> Result<(Value, &Part), String> {
        let part = parts
            .iter()
            .find(|p| p.inc.join(name).exists())
            .ok_or_else(|| format!("{name}: missing"))?;
        let text = fs::read_to_string(part.inc.join(name)).map_err(|e| format!("{name}: {e}"))?;
        Ok((
            serde_json::from_str(&text).map_err(|e| format!("{name}: {e}"))?,
            part,
        ))
    };
    if parts.iter().any(|p| p.inc.join(IMPORT).exists()) {
        return Ok(());
    }
    let (manifest, home) = read("manifest.json")?;
    let (metadata, _) = read("metadata.json")?;
    let q = &metadata["query"];
    let (dataset, schema) = (
        q["dataset"]
            .as_str()
            .ok_or("metadata.json has no dataset")?,
        q["schema"].as_str().ok_or("metadata.json has no schema")?,
    );
    if q["encoding"] != "dbn" || q["compression"] != "zstd" {
        return Err("only DBN batches compressed with zstd are filed".into());
    }
    let c = &metadata["customizations"];
    if c["split_symbols"] == true || c["split_duration"] != "day" {
        return Err("only batches split by day, and not by symbol, are filed".into());
    }
    if q["stype_in"] != "raw_symbol" {
        return Err("only batches requested by contract (stype_in raw_symbol) are filed as partial days; file others with /incorporate without a parent".into());
    }
    let files: Vec<Value> = manifest["files"]
        .as_array()
        .ok_or("manifest.json lists no files")?
        .iter()
        .filter_map(|f| {
            let name = f["filename"].as_str()?;
            let day = ingest::databento_day(name, schema)?;
            Some(json!({ "filename": name, "day": day, "size": f["size"], "hash": f["hash"] }))
        })
        .collect();
    let receipt = json!({
        "dataset": dataset, "schema": schema, "symbol": parent,
        "source": { "provider_batch": batch, "query": q },
        "files": files,
    });
    let text = serde_json::to_string_pretty(&receipt).map_err(|e| e.to_string())?;
    write_new(&home.inc.join(IMPORT), text.as_bytes()).map_err(|e| format!("{IMPORT}: {e}"))
}

/// The part of a day's full entry that [`row`] reports, kept in memory for
/// `/coverage` (without the contracts it lists, say).
#[must_use]
pub fn reported(entry: &Value) -> Value {
    let mut out = json!({});
    for k in ["condition", "coverage", "partial", "records", "sha256"] {
        if let Some(v) = entry.get(k) {
            out[k] = v.clone();
        }
    }
    if entry.get("pending").is_some() {
        out["pending"] = json!(true);
    }
    out
}

/// What one copy of a day holds, as `/coverage` reports it.
fn row(day: &str, volume: &str, filed: bool, size: u64, entry: Option<&Value>) -> Value {
    let condition = entry
        .and_then(|m| m.get("condition"))
        .cloned()
        .unwrap_or(Value::Null);
    match entry.filter(|m| is_partial(m)) {
        None => {
            json!({ "day": day, "volume": volume, "kind": "complete", "filed": filed, "size": size, "condition": condition })
        }
        Some(m) => json!({
            "day": day, "volume": volume, "kind": "partial", "filed": filed, "size": size,
            // In pieces, however it was recorded.
            "coverage": Coverage::from_json(&m["coverage"]).map_or_else(|| m["coverage"].clone(), |c| c.to_json()),
            "records": m["records"], "sha256": m["sha256"],
            "pending": m.get("pending").is_some(), "condition": condition,
        }),
    }
}

/// Does data of `found` (a provider, or none outside every provider's
/// folder) count for `provider`? Data with no provider counts for Databento,
/// whose layout and names it has: a copy in a backup still blocks buying it
/// again.
pub(crate) fn ours(found: Option<&str>, provider: &str) -> bool {
    found.unwrap_or("databento") == provider
}

/// Symbol types whose symbols say what they hold.
const CLASSIFIED: [&str; 5] = [
    "parent",
    "raw_symbol",
    "continuous",
    "nasdaq_symbol",
    "cms_symbol",
];

/// What a holding of `symbols`, requested as `stype_in`, holds of `symbol`
/// (or, with none, of the dataset): `None` nothing, an empty set all of it,
/// otherwise those of its contracts the holding lists. Symbols that can't be
/// told apart (instrument IDs, or no type) hold all of it, to be safe.
fn holds(
    symbols: &[String],
    stype_in: Option<&str>,
    symbol: Option<&str>,
) -> Option<BTreeSet<String>> {
    if symbols.is_empty() || !stype_in.is_some_and(|t| CLASSIFIED.contains(&t)) {
        return Some(BTreeSet::new());
    }
    let all = symbols
        .iter()
        .any(|s| s.eq_ignore_ascii_case("ALL_SYMBOLS"));
    let whole = match symbol {
        Some(s) => all || symbols.iter().any(|x| x.eq_ignore_ascii_case(s)),
        None => all || symbols.len() != 1,
    };
    if whole {
        return Some(BTreeSet::new());
    }
    // A list of contracts of the parent holds those contracts.
    let contracts: BTreeSet<String> = match symbol {
        Some(s) if stype_in == Some("raw_symbol") => symbols
            .iter()
            .filter(|c| of_parent(s, c))
            .cloned()
            .collect(),
        _ => BTreeSet::new(),
    };
    (!contracts.is_empty()).then_some(contracts)
}

/// An unfiled copy of a day: complete when `contracts` is empty, otherwise
/// partial with those contracts over the part of `window` in the day.
fn held_row(
    day: &str,
    volume: &str,
    size: u64,
    contracts: &BTreeSet<String>,
    window: (u64, u64),
) -> Value {
    let partial = (!contracts.is_empty()).then(|| {
        let from = ingest::day_ns(day).unwrap_or(0);
        let held = Coverage::one(
            contracts.clone(),
            window.0.max(from),
            window.1.min(from + 86_400_000_000_000),
        );
        json!({ "coverage": held.to_json(), "records": null, "sha256": null })
    });
    row(day, volume, false, size, partial.as_ref())
}

/// Is `symbol` a contract of the option parent `parent` (`ROOT.OPT`)?
fn of_parent(parent: &str, symbol: &str) -> bool {
    parent.strip_suffix(".OPT").is_some_and(|root| {
        symbol.len() == 21
            && symbol
                .get(..6)
                .is_some_and(|r| r.trim_end().eq_ignore_ascii_case(root))
    })
}

/// Each held day of `provider`'s `dataset/symbol/schema` from `start` to
/// `end` (inclusive, `YYYY-MM-DD`), on any of `volumes` (name, path) that is
/// present: every day the catalog counts, filed or not. A day held on several
/// volumes, or in several forms, is one row: complete if any copy is,
/// otherwise the union of their contracts and windows, with `conflict` when
/// the copies differ.
///
/// # Errors
/// If a folder of the provider or the dataset can't be read, or a manifest
/// or a batch's metadata is malformed: never an answer with days left out.
#[cfg(test)]
pub fn coverage(
    volumes: &[(String, PathBuf)],
    provider: &str,
    dataset: &str,
    symbol: Option<&str>,
    schema: &str,
    start: &str,
    end: &str,
) -> Result<Vec<Value>, String> {
    let mut found = Vec::new();
    for (volume, root) in volumes.iter().filter(|(_, r)| r.is_dir()) {
        found.push((volume.as_str(), crate::catalog::places(root)?));
    }
    let index: Vec<(&str, &[crate::catalog::Place])> =
        found.iter().map(|(v, p)| (*v, p.as_slice())).collect();
    coverage_in(&index, provider, dataset, symbol, schema, start, end)
}

/// As [`coverage`], from the places a scan found on each volume (`index`),
/// with the manifest entries read with them: nothing is read from disk. Each
/// write to a schema folder reads it again into the places, so a merge into
/// a partial day shows at once.
///
/// # Errors
/// If a matching schema folder's manifest is malformed.
pub fn coverage_in(
    index: &[(&str, &[crate::catalog::Place])],
    provider: &str,
    dataset: &str,
    symbol: Option<&str>,
    schema: &str,
    start: &str,
    end: &str,
) -> Result<Vec<Value>, String> {
    let mut copies: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let wanted = |day: &str| day >= start && day <= end;
    for &(volume, places) in index {
        for place in places {
            match place {
                crate::catalog::Place::Days {
                    provider: p,
                    dataset: d,
                    symbol: s,
                    schema: k,
                    days,
                    entries,
                    files,
                    ..
                } if ours(p.as_deref(), provider)
                    && d.eq_ignore_ascii_case(dataset)
                    && s.as_deref() == symbol
                    && k == schema =>
                {
                    let entries = entries.as_ref().as_ref().map_err(Clone::clone)?;
                    for (day, size, filed) in days.iter().filter(|d| wanted(&d.0)) {
                        let entry = match entries.get(day).filter(|_| *filed) {
                            Some(e) => Some(e.as_ref().map_err(Clone::clone)?),
                            None => None,
                        };
                        let mut r = row(day, volume, *filed, *size, entry);
                        r["files"] = named(volume, files.get(day).into_iter().flatten().cloned());
                        copies.entry(day.clone()).or_default().push(r);
                    }
                }
                crate::catalog::Place::Batch {
                    provider: p,
                    batch: b,
                    dir,
                } if ours(p.as_deref(), provider) && b.dataset.eq_ignore_ascii_case(dataset) => {
                    let Some(contracts) = holds(&b.symbols, b.stype_in.as_deref(), symbol) else {
                        continue;
                    };
                    for (day, _, _, size) in b.files.iter().filter(|f| f.1 == schema) {
                        if wanted(day) {
                            let mut r = held_row(day, volume, *size, &contracts, (0, u64::MAX));
                            let names = b.names.get(&(day.clone(), schema.to_owned()));
                            let within = names
                                .into_iter()
                                .flatten()
                                .map(|n| Path::new(dir).join(n).to_string_lossy().into_owned());
                            r["files"] = named(volume, within);
                            copies.entry(day.clone()).or_default().push(r);
                        }
                    }
                }
                // Any other DBN file, by its header; one of several schemas
                // counts for each.
                crate::catalog::Place::Loose {
                    provider: p,
                    header: h,
                    size,
                    path,
                } if ours(p.as_deref(), provider)
                    && h.dataset.eq_ignore_ascii_case(dataset)
                    && h.schema.as_deref().is_none_or(|s| s == schema) =>
                {
                    let Some(contracts) = holds(&h.symbols, h.stype_in.as_deref(), symbol) else {
                        continue;
                    };
                    let days = h.days();
                    let n = days.len();
                    for (i, day) in days.into_iter().enumerate() {
                        if wanted(&day) {
                            let size = crate::catalog::share(*size, n, i);
                            let mut r = held_row(&day, volume, size, &contracts, (h.start, h.end));
                            r["files"] = named(volume, std::iter::once(path.clone()));
                            copies.entry(day).or_default().push(r);
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(copies.into_values().map(merge_copies).collect())
}

/// Files by their logical names, `<volume>/<path>`, as `GET /files` serves
/// them.
fn named(volume: &str, within: impl Iterator<Item = String>) -> Value {
    json!(within.map(|w| format!("{volume}/{w}")).collect::<Vec<_>>())
}

/// One day's row from its copies: a complete copy if there is one, otherwise
/// the union of the partial copies' contracts and windows. Its `files` are
/// that complete copy's, or every partial copy's.
fn merge_copies(copies: Vec<Value>) -> Value {
    let same = |a: &Value, b: &Value| {
        ["kind", "coverage", "size", "sha256"]
            .iter()
            .all(|k| a.get(*k) == b.get(*k))
    };
    let conflict = copies.windows(2).any(|w| !same(&w[0], &w[1]));
    let mut volumes: Vec<&str> = copies.iter().filter_map(|c| c["volume"].as_str()).collect();
    volumes.sort_unstable();
    volumes.dedup();
    let filed = copies.iter().any(|c| c["filed"] == true);
    let mut out = match copies.iter().find(|c| c["kind"] == "complete") {
        Some(c) => c.clone(),
        None => {
            let mut out = copies[0].clone();
            if conflict {
                let union = copies
                    .iter()
                    .filter_map(|c| Coverage::from_json(&c["coverage"]))
                    .reduce(|a, b| a.union(&b));
                out["coverage"] = union.map_or(Value::Null, |u| u.to_json());
                out["pending"] = json!(copies.iter().any(|c| c["pending"] == true));
                let mut files: Vec<&Value> = Vec::new();
                for f in copies
                    .iter()
                    .filter_map(|c| c["files"].as_array())
                    .flatten()
                {
                    if !files.contains(&f) {
                        files.push(f);
                    }
                }
                out["files"] = json!(files);
                for k in ["size", "sha256", "records"] {
                    out[k] = Value::Null;
                }
            }
            out
        }
    };
    out["filed"] = json!(filed);
    out["volumes"] = json!(volumes);
    out["conflict"] = json!(conflict);
    // A condition known for any copy is the day's.
    if out["condition"].is_null()
        && let Some(c) = copies
            .iter()
            .map(|c| &c["condition"])
            .find(|c| !c.is_null())
    {
        out["condition"] = c.clone();
    }
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use std::num::NonZeroU64;

    use dbn::{CbboMsg, MappingInterval, Schema, SymbolMapping};

    use super::*;

    /// File an import waiting in `.incoming/<id>` on one or more volumes.
    ///
    /// # Errors
    /// If `import.json` is missing or malformed, a file doesn't match it or its
    /// own header, or a day can't be filed: held complete, held as a different
    /// file, or held partly with some of the same contracts.
    pub fn import(
        parts: &[Part],
        elsewhere: &[PathBuf],
        id: &str,
        today: &str,
    ) -> Result<Value, String> {
        one(parts, elsewhere, id, today, None)
    }

    fn one(
        parts: &[Part],
        elsewhere: &[PathBuf],
        id: &str,
        today: &str,
        parent: Option<&str>,
    ) -> Result<Value, String> {
        let f = Filing {
            parts: parts.to_vec(),
            elsewhere: elsewhere.to_vec(),
            id: id.to_owned(),
            today: today.to_owned(),
            parent: parent.map(str::to_owned),
        };
        import_group(&[f]).pop().ok_or("nothing was filed")?
    }

    /// File a provider batch bought as a list of contracts of `parent` (such as
    /// `SPY.OPT`), waiting in `.incoming/<batch>` with the provider's
    /// `manifest.json` and `metadata.json`: as an import of its day files, each a
    /// partial day, with those receipts kept beside it.
    ///
    /// # Errors
    /// If the batch isn't a daily DBN batch requested by contract
    /// (`stype_in = raw_symbol`), or as [`import`].
    pub fn import_batch(
        parts: &[Part],
        elsewhere: &[PathBuf],
        batch: &str,
        parent: &str,
        today: &str,
    ) -> Result<Value, String> {
        one(parts, elsewhere, batch, today, Some(parent))
    }

    pub(crate) const DAY: &str = "2026-10-02";
    pub(crate) const C1: &str = "SPY   261002C00728000";
    pub(crate) const C2: &str = "SPY   261002P00728000";
    pub(crate) const C3: &str = "SPY   261002C00729000";

    thread_local! {
        /// Where a filing on this thread stops, as a crash would there.
        pub(crate) static CRASH: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
    }

    fn scratch(name: &str) -> PathBuf {
        let d =
            std::env::temp_dir().join(format!("tinytrader-partial-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    pub(crate) fn at(minute: u64) -> u64 {
        ingest::day_ns(DAY).unwrap() + (13 * 60 + 30 + minute) * 60_000_000_000
    }

    /// A partial cbbo-1m day: `contracts` (symbol, instrument ID) listed and
    /// mapped, and one record per (instrument, minute) in `records`.
    pub(crate) fn day_bytes(contracts: &[(&str, u32)], records: &[(u32, u64)]) -> Vec<u8> {
        day_window(contracts, records, (13, 24))
    }

    /// As `day_bytes`, its window the hours `(from, to)` of the day (UTC).
    fn day_window(
        contracts: &[(&str, u32)],
        records: &[(u32, u64)],
        (from, to): (u64, u64),
    ) -> Vec<u8> {
        let start = ingest::day_ns(DAY).unwrap();
        let date = time::Date::parse(
            DAY,
            time::macros::format_description!("[year]-[month]-[day]"),
        )
        .unwrap();
        let m = Metadata::builder()
            .dataset("OPRA.PILLAR")
            .schema(Some(Schema::Cbbo1M))
            .start(start + from * 3_600_000_000_000)
            .end(NonZeroU64::new(start + to * 3_600_000_000_000))
            .stype_in(Some(SType::RawSymbol))
            .stype_out(SType::InstrumentId)
            .symbols(contracts.iter().map(|(s, _)| (*s).to_owned()).collect())
            .mappings(
                contracts
                    .iter()
                    .map(|(s, id)| SymbolMapping {
                        raw_symbol: (*s).to_owned(),
                        intervals: vec![MappingInterval {
                            start_date: date,
                            end_date: date.next_day().unwrap(),
                            symbol: id.to_string(),
                        }],
                    })
                    .collect(),
            )
            .build();
        encode(&m, records.iter().map(|(id, minute)| (*id, at(*minute))))
    }

    /// A day file of header `m`, one cbbo-1m record per (instrument, time).
    fn encode(m: &Metadata, records: impl Iterator<Item = (u32, u64)>) -> Vec<u8> {
        let mut out = Vec::new();
        {
            let mut enc = dbn::encode::dbn::Encoder::with_zstd(&mut out, m).unwrap();
            for (id, t) in records {
                let mut r = CbboMsg::default_for_schema(Schema::Cbbo1M);
                r.hd.instrument_id = id;
                r.hd.ts_event = t;
                r.ts_recv = t;
                r.price = 1_000_000_000 + i64::from(id);
                enc.encode_record(&r).unwrap();
            }
            enc.flush().unwrap();
        }
        out
    }

    /// `SPY.OPT`'s whole chain on `DAY`, as a batch of the parent delivers it:
    /// one record per (instrument, time in ns).
    pub(crate) fn whole_bytes(records: &[(u32, u64)]) -> Vec<u8> {
        let start = ingest::day_ns(DAY).unwrap();
        let m = Metadata::builder()
            .dataset("OPRA.PILLAR")
            .schema(Some(Schema::Cbbo1M))
            .start(start)
            .end(NonZeroU64::new(start + 86_400_000_000_000))
            .stype_in(Some(SType::Parent))
            .stype_out(SType::InstrumentId)
            .symbols(vec!["SPY.OPT".to_owned()])
            .build();
        encode(&m, records.iter().copied())
    }

    /// An import waiting in `<root>/.incoming/<id>/`: its files and receipt.
    pub(crate) fn stage(root: &Path, id: &str, files: &[(&str, Vec<u8>)]) -> Part {
        let inc = root.join(INCOMING).join(id);
        fs::create_dir_all(&inc).unwrap();
        let mut listed = Vec::new();
        for (name, bytes) in files {
            fs::write(inc.join(name), bytes).unwrap();
            listed.push(json!({ "filename": name, "size": bytes.len(), "hash": format!("sha256:{}", hash_file(&inc.join(name)).unwrap()) }));
        }
        let receipt = json!({
            "dataset": "OPRA.PILLAR", "schema": "cbbo-1m", "symbol": "SPY.OPT",
            "source": { "api": "timeseries.get_range", "tool": "research/cbbo_fetch.py", "commit": "61f984a" },
            "files": listed,
        });
        fs::write(inc.join(IMPORT), receipt.to_string()).unwrap();
        Part {
            inc,
            root: root.to_path_buf(),
        }
    }

    fn file(root: &Path) -> PathBuf {
        root.join("OPRA.PILLAR/SPY.OPT/cbbo-1m")
            .join(format!("{DAY}.dbn.zst"))
    }

    pub(crate) fn name() -> String {
        format!("{DAY}.dbn.zst")
    }

    #[test]
    fn files_a_partial_day_and_reports_its_coverage() {
        let root = scratch("place").join("Databento");
        let bytes = day_bytes(&[(C1, 11), (C2, 12)], &[(11, 0), (12, 0), (11, 1)]);
        let part = stage(&root, "I1", &[(&name(), bytes.clone())]);
        let summary = import(std::slice::from_ref(&part), &[], "I1", "2026-10-19").unwrap();
        assert_eq!(summary["days"][0]["filed"], "new");
        assert_eq!(
            fs::read(file(&root)).unwrap(),
            bytes,
            "filed as uploaded, byte for byte"
        );
        assert!(!part.inc.exists(), "the incoming folder is gone");
        assert!(root.join("provenance/I1").join(IMPORT).exists());
        let days = coverage(
            &[("v".into(), root.parent().unwrap().to_path_buf())],
            "databento",
            "OPRA.PILLAR",
            Some("SPY.OPT"),
            "cbbo-1m",
            DAY,
            DAY,
        )
        .unwrap();
        assert_eq!(days.len(), 1);
        assert_eq!(days[0]["kind"], "partial");
        assert_eq!(days[0]["records"], 3);
        assert_eq!(days[0]["coverage"]["symbols"], json!([C1, C2]));
        // The catalog marks it partial.
        let cat = crate::catalog::scan_root(
            "v",
            root.parent().unwrap(),
            crate::catalog::Thresholds {
                warning: 100.0,
                critical: 100.0,
                floor: 100.0,
            },
        );
        let ds = cat["datasets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["symbol"] == "SPY.OPT")
            .unwrap();
        assert_eq!(ds["schemas"]["cbbo-1m"]["partial"], json!([DAY]));
        // A day with no manifest entry is complete.
        let other = root.join("OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-01.dbn.zst");
        fs::write(&other, b"x").unwrap();
        let days = coverage(
            &[("v".into(), root.parent().unwrap().to_path_buf())],
            "databento",
            "OPRA.PILLAR",
            Some("SPY.OPT"),
            "cbbo-1m",
            "2026-10-01",
            DAY,
        )
        .unwrap();
        assert_eq!(days[0]["kind"], "complete");
        // Filing it again finds it already there.
        let again = stage(&root, "I1b", &[(&name(), bytes)]);
        let s = import(&[again], &[], "I1b", "2026-10-19").unwrap();
        assert_eq!(s["days"][0]["filed"], "already present");
    }

    #[test]
    fn refuses_records_outside_its_contracts_or_window_and_moves_nothing() {
        let root = scratch("refuse").join("Databento");
        // Instrument 99 maps to no listed contract.
        let stray = day_bytes(&[(C1, 11)], &[(11, 0), (99, 1)]);
        let part = stage(&root, "I2", &[(&name(), stray)]);
        let e = import(std::slice::from_ref(&part), &[], "I2", "2026-10-19").unwrap_err();
        assert!(e.contains("maps to none of its contracts"), "{e}");
        assert!(part.inc.join(name()).exists() && !file(&root).exists());
        // A record before the window (13:00 UTC) is refused too.
        let path = root.join("early.dbn.zst");
        fs::write(
            &path,
            day_bytes_at(&[(C1, 11)], &[(11, 12 * 60 * 60_000_000_000)]),
        )
        .unwrap();
        assert!(inspect(&path).unwrap_err().contains("outside its window"));
        // A contract of another root doesn't belong to SPY.OPT.
        assert!(
            belongs(
                "SPY.OPT",
                &BTreeSet::from(["QQQ   261002C00500000".to_owned()])
            )
            .is_err()
        );
        assert!(belongs("SPY", &BTreeSet::from([C1.to_owned()])).is_err());
    }

    /// As `day_bytes`, with one record at an absolute offset from midnight.
    fn day_bytes_at(contracts: &[(&str, u32)], records: &[(u32, u64)]) -> Vec<u8> {
        let start = ingest::day_ns(DAY).unwrap();
        let bytes = day_bytes(contracts, &[]);
        let (m, _) = {
            let p = std::env::temp_dir().join(format!(
                "tinytrader-partial-at-{}.dbn.zst",
                std::process::id()
            ));
            fs::write(&p, &bytes).unwrap();
            let r = read_all(&p).unwrap();
            let _ = fs::remove_file(&p);
            r
        };
        let mut out = Vec::new();
        {
            let mut enc = dbn::encode::dbn::Encoder::with_zstd(&mut out, &m).unwrap();
            for &(id, off) in records {
                let mut r = CbboMsg::default_for_schema(Schema::Cbbo1M);
                r.hd.instrument_id = id;
                r.hd.ts_event = start + off;
                r.ts_recv = start + off;
                enc.encode_record(&r).unwrap();
            }
            enc.flush().unwrap();
        }
        out
    }

    #[test]
    fn merges_other_contracts_into_a_partial_day() {
        let root = scratch("merge").join("Databento");
        let a = day_bytes(&[(C1, 11)], &[(11, 0), (11, 2), (11, 4)]);
        import(
            &[stage(&root, "A", &[(&name(), a)])],
            &[],
            "A",
            "2026-10-19",
        )
        .unwrap();
        let b = day_bytes(&[(C2, 12), (C3, 13)], &[(12, 1), (13, 2), (12, 3)]);
        let s = import(
            &[stage(&root, "B", &[(&name(), b)])],
            &[],
            "B",
            "2026-10-19",
        )
        .unwrap();
        assert_eq!(s["days"][0]["filed"], "merged");
        assert_eq!(s["days"][0]["contracts"], 3);
        // Six records, in time order (ties: the held day's first), each check passing.
        let (cov, n) = inspect(&file(&root)).unwrap();
        assert_eq!(n, 6);
        assert_eq!(
            cov.symbols(),
            BTreeSet::from([C1.to_owned(), C2.to_owned(), C3.to_owned()])
        );
        let (_, recs) = read_all(&file(&root)).unwrap();
        let order: Vec<u32> = recs.iter().map(|r| r.header().instrument_id).collect();
        assert_eq!(order, vec![11, 12, 11, 13, 12, 11]);
        let entry = read_manifest(file(&root).parent().unwrap()).unwrap()[DAY].clone();
        assert!(entry.get("pending").is_none());
        assert_eq!(entry["sha256"], hash_file(&file(&root)).unwrap().as_str());
        assert_eq!(entry["records"], 6);
        assert!(!root.join(INCOMING).exists(), "nothing left in incoming");

        // Contracts already held are never filed twice.
        let c = day_bytes(&[(C2, 12)], &[(12, 5)]);
        let e = import(
            &[stage(&root, "C", &[(&name(), c)])],
            &[],
            "C",
            "2026-10-19",
        )
        .unwrap_err();
        assert!(e.contains("already holds 1 of these contracts"), "{e}");
    }

    #[test]
    fn refuses_a_partial_day_where_the_whole_day_is_held() {
        let root = scratch("whole").join("Databento");
        fs::create_dir_all(file(&root).parent().unwrap()).unwrap();
        fs::write(file(&root), b"whole day").unwrap();
        let a = day_bytes(&[(C1, 11)], &[(11, 0)]);
        let e = import(
            &[stage(&root, "W", &[(&name(), a)])],
            &[],
            "W",
            "2026-10-19",
        )
        .unwrap_err();
        assert!(e.contains("holds the whole day"), "{e}");
        assert_eq!(fs::read(file(&root)).unwrap(), b"whole day");
    }

    #[test]
    fn checks_the_held_day_on_another_volume() {
        let base = scratch("volumes");
        let (one, two) = (base.join("one/Databento"), base.join("two/Databento"));
        let a = day_bytes(&[(C1, 11)], &[(11, 0)]);
        import(&[stage(&one, "A", &[(&name(), a)])], &[], "A", "2026-10-19").unwrap();
        // The same contract, arriving on the other volume, is refused.
        let again = day_bytes(&[(C1, 11)], &[(11, 1)]);
        let e = import(
            &[stage(&two, "B", &[(&name(), again)])],
            std::slice::from_ref(&one),
            "B",
            "2026-10-19",
        )
        .unwrap_err();
        assert!(e.contains("already holds"), "{e}");
        // Other contracts merge into the held day, on its own volume.
        let other = day_bytes(&[(C2, 12)], &[(12, 1)]);
        let s = import(
            &[stage(&two, "C", &[(&name(), other)])],
            std::slice::from_ref(&one),
            "C",
            "2026-10-19",
        )
        .unwrap();
        assert_eq!(s["days"][0]["filed"], "merged");
        assert_eq!(inspect(&file(&one)).unwrap().1, 2);
        assert!(!file(&two).exists());
    }

    #[test]
    fn settles_an_interrupted_merge_from_the_files_hash() {
        let root = scratch("settle").join("Databento");
        let a = day_bytes(&[(C1, 11)], &[(11, 0)]);
        import(
            &[stage(&root, "A", &[(&name(), a.clone())])],
            &[],
            "A",
            "2026-10-19",
        )
        .unwrap();
        let dir = file(&root).parent().unwrap().to_path_buf();
        let held = hash_file(&file(&root)).unwrap();
        let pending = crate::manifest::write_pending;
        // Stopped after recording the pending merge, before the swap: dropped.
        pending(&dir, DAY, &json!({ "sha256": "f00d", "coverage": {} })).unwrap();
        let now = settle(&dir, DAY).unwrap().unwrap();
        assert!(now.get("pending").is_none() && now["sha256"] == held.as_str());
        assert!(crate::manifest::pending_days(&dir).is_empty());
        // Stopped after the swap: the pending entry becomes current.
        let merged = day_bytes(&[(C1, 11), (C2, 12)], &[(11, 0), (12, 1)]);
        fs::write(file(&root), &merged).unwrap();
        let sha = hash_file(&file(&root)).unwrap();
        pending(&dir, DAY, &json!({ "sha256": sha, "records": 2, "coverage": { "kind": "contracts", "symbols": [C1, C2], "window": [0, 1] } })).unwrap();
        let now = settle(&dir, DAY).unwrap().unwrap();
        assert_eq!(now["records"], 2);
        assert!(now.get("pending").is_none());
        assert_eq!(read_manifest(&dir).unwrap()[DAY]["records"], 2);
        // Neither: refused, for a person to look at.
        fs::write(file(&root), b"something else").unwrap();
        pending(&dir, DAY, &json!({ "sha256": "f00d" })).unwrap();
        assert!(settle(&dir, DAY).is_err());
        // A pending merge recorded inline, before sidecars, is settled too.
        fs::remove_file(
            dir.join(crate::manifest::SIDECARS)
                .join(format!("{DAY}.pending.json")),
        )
        .unwrap();
        let entry = json!({ "sha256": "0ld", "coverage": { "kind": "contracts", "symbols": [C1], "window": [0, 1] },
            "pending": { "sha256": hash_file(&file(&root)).unwrap(), "records": 9, "coverage": { "kind": "contracts", "symbols": [C1, C2], "window": [0, 1] } } });
        fs::write(
            dir.join(MANIFEST),
            json!({ "days": { DAY: entry } }).to_string(),
        )
        .unwrap();
        assert_eq!(settle(&dir, DAY).unwrap().unwrap()["records"], 9);
        assert!(read_manifest(&dir).unwrap()[DAY].get("pending").is_none());
    }

    #[test]
    fn files_a_contract_list_batch_as_partial_days() {
        let root = scratch("batch").join("Databento");
        let inc = root.join(INCOMING).join("OPRA-X");
        fs::create_dir_all(&inc).unwrap();
        let name = "opra-pillar-20261002.cbbo-1m.dbn.zst";
        let bytes = day_bytes(&[(C1, 11), (C2, 12)], &[(11, 0), (12, 1)]);
        fs::write(inc.join(name), &bytes).unwrap();
        let sha = hash_file(&inc.join(name)).unwrap();
        fs::write(inc.join("manifest.json"), json!({ "files": [{ "filename": name, "size": bytes.len(), "hash": format!("sha256:{sha}") }] }).to_string()).unwrap();
        let metadata = |stype: &str| {
            json!({ "query": { "dataset": "OPRA.PILLAR", "schema": "cbbo-1m", "stype_in": stype, "symbols": [C1, C2], "encoding": "dbn", "compression": "zstd" }, "customizations": { "split_duration": "day", "split_symbols": false } }).to_string()
        };
        let part = Part {
            inc: inc.clone(),
            root: root.clone(),
        };
        // A whole-symbol batch isn't filed this way.
        fs::write(inc.join("metadata.json"), metadata("parent")).unwrap();
        assert!(
            import_batch(
                std::slice::from_ref(&part),
                &[],
                "OPRA-X",
                "SPY.OPT",
                "2026-10-19"
            )
            .unwrap_err()
            .contains("raw_symbol")
        );
        fs::remove_file(inc.join("metadata.json")).unwrap();
        fs::write(inc.join("metadata.json"), metadata("raw_symbol")).unwrap();
        let condition =
            json!([{ "date": DAY, "condition": "degraded", "last_modified_date": "2026-10-04" }]);
        fs::write(inc.join("condition.json"), condition.to_string()).unwrap();
        let s = import_batch(&[part], &[], "OPRA-X", "SPY.OPT", "2026-10-19").unwrap();
        assert_eq!(s["days"][0]["day"], DAY);
        assert_eq!(fs::read(file(&root)).unwrap(), bytes);
        // The day's condition, with its coverage.
        let entry = &read_manifest(file(&root).parent().unwrap()).unwrap()[DAY];
        assert_eq!(
            (
                entry["condition"]["state"].clone(),
                entry["condition"]["source"].clone()
            ),
            (json!("degraded"), json!("batch OPRA-X"))
        );
        assert!(is_partial(entry));
        for r in [IMPORT, "manifest.json", "metadata.json", "condition.json"] {
            assert!(root.join("provenance/OPRA-X").join(r).exists(), "{r} kept");
        }
        assert!(!inc.exists());
    }

    #[test]
    fn a_day_with_only_its_condition_is_still_complete() {
        let root = scratch("condition-only").join("Databento");
        put(&file(&root), b"the whole day");
        let dir = file(&root).parent().unwrap().to_path_buf();
        let degraded = crate::conditions::Row {
            state: "degraded".into(),
            last_modified: None,
        };
        assert_eq!(
            crate::conditions::record(&dir, DAY, &degraded, "provider api").unwrap(),
            Some(Value::Null)
        );
        assert_eq!(
            crate::conditions::record(&dir, DAY, &degraded, "provider api").unwrap(),
            None,
            "unchanged"
        );
        let vol = root.parent().unwrap();
        let days = spy(&[("v", vol)], "databento").unwrap();
        assert_eq!(
            (
                days[0]["kind"].clone(),
                days[0]["condition"]["state"].clone()
            ),
            (json!("complete"), json!("degraded"))
        );
        let cat = crate::catalog::scan_root("v", vol, T);
        let s = &cat["datasets"][0]["schemas"]["cbbo-1m"];
        assert_eq!(
            (s["partial"].clone(), s["degraded"].clone()),
            (json!([]), json!([DAY]))
        );
        // Still the whole day: a partial import onto it is refused.
        let a = day_bytes(&[(C1, 11)], &[(11, 0)]);
        let e = import(
            &[stage(&root, "W2", &[(&name(), a)])],
            &[],
            "W2",
            "2026-10-19",
        )
        .unwrap_err();
        assert!(e.contains("holds the whole day"), "{e}");
        // A day without an entry is complete, its condition unknown.
        put(&dir.join("2026-10-01.dbn.zst"), b"x");
        let days = spy(&[("v", vol)], "databento").unwrap();
        assert_eq!(
            (days[0]["kind"].clone(), days[0]["condition"].clone()),
            (json!("complete"), Value::Null)
        );
    }

    #[test]
    fn a_merge_keeps_the_days_condition() {
        let root = scratch("condition-merge").join("Databento");
        let a = day_bytes(&[(C1, 11)], &[(11, 0)]);
        import(
            &[stage(&root, "A", &[(&name(), a)])],
            &[],
            "A",
            "2026-10-19",
        )
        .unwrap();
        let dir = file(&root).parent().unwrap().to_path_buf();
        let row = crate::conditions::Row {
            state: "missing".into(),
            last_modified: Some("2026-10-03".into()),
        };
        crate::conditions::record(&dir, DAY, &row, "provider api").unwrap();
        let b = day_bytes(&[(C2, 12)], &[(12, 1)]);
        import(
            &[stage(&root, "B", &[(&name(), b)])],
            &[],
            "B",
            "2026-10-19",
        )
        .unwrap();
        let entry = &read_manifest(&dir).unwrap()[DAY];
        assert_eq!(entry["coverage"]["symbols"], json!([C1, C2]));
        assert_eq!(entry["condition"]["state"], "missing");
    }

    const HOUR: u64 = 3_600_000_000_000;

    fn coverage_of(root: &Path) -> Coverage {
        let e = read_manifest(file(root).parent().unwrap()).unwrap()[DAY].clone();
        Coverage::from_json(&e["coverage"]).unwrap()
    }

    fn file_one(root: &Path, id: &str, bytes: Vec<u8>) -> Result<Value, String> {
        import(
            &[stage(root, id, &[(&name(), bytes)])],
            &[],
            id,
            "2026-10-19",
        )
    }

    #[test]
    fn keeps_each_files_hours_as_its_own_piece() {
        let root = scratch("pieces").join("Databento");
        file_one(&root, "A", day_bytes(&[(C1, 11)], &[(11, 0)])).unwrap();
        // 14:00 to 20:00 UTC, beside the held 13:00 to 24:00.
        file_one(&root, "B", day_window(&[(C2, 12)], &[(12, 60)], (14, 20))).unwrap();
        let from = ingest::day_ns(DAY).unwrap();
        let cov = coverage_of(&root);
        assert_eq!(cov.pieces.len(), 2);
        assert!(cov.covers(C1, from + 13 * HOUR, from + 24 * HOUR));
        assert!(cov.covers(C2, from + 14 * HOUR, from + 20 * HOUR));
        assert!(
            !cov.covers(C2, from + 13 * HOUR, from + 20 * HOUR),
            "not C2's hours"
        );
        // Another file of the same hours joins that piece.
        file_one(&root, "C", day_window(&[(C3, 13)], &[(13, 61)], (14, 20))).unwrap();
        let cov = coverage_of(&root);
        assert_eq!(cov.pieces.len(), 2);
        assert_eq!(
            cov.pieces[1].symbols,
            BTreeSet::from([C2.to_owned(), C3.to_owned()])
        );
        // /coverage: the pieces, and for older readers all the contracts
        // over the span of the hours.
        let vol = root.parent().unwrap();
        let row = &spy(&[("v", vol)], "databento").unwrap()[0]["coverage"];
        assert_eq!(row["pieces"].as_array().unwrap().len(), 2);
        assert_eq!(row["symbols"], json!([C1, C3, C2]));
        assert_eq!(row["window"], json!([from + 13 * HOUR, from + 24 * HOUR]));
        // The form before pieces reads as one.
        let old = json!({ "kind": "contracts", "symbols": [C1], "window": [1, 2] });
        assert_eq!(
            Coverage::from_json(&old).unwrap(),
            Coverage::one(BTreeSet::from([C1.to_owned()]), 1, 2)
        );
    }

    #[test]
    fn files_a_contract_again_for_hours_not_held() {
        let root = scratch("hours").join("Databento");
        file_one(
            &root,
            "A",
            day_window(&[(C1, 11)], &[(11, 0), (11, 100)], (13, 16)),
        )
        .unwrap();
        // Its later hours: merged, each record kept with its own input.
        file_one(
            &root,
            "B",
            day_window(&[(C1, 11)], &[(11, 200), (11, 300)], (16, 24)),
        )
        .unwrap();
        let (cov, n) = (coverage_of(&root), inspect(&file(&root)).unwrap().1);
        assert_eq!((cov.pieces.len(), n), (2, 4));
        let from = ingest::day_ns(DAY).unwrap();
        assert!(cov.covers(C1, from + 13 * HOUR, from + 24 * HOUR));
        // Hours already held are refused.
        let e = file_one(&root, "C", day_window(&[(C1, 11)], &[(11, 150)], (15, 18))).unwrap_err();
        assert!(e.contains("over the same hours"), "{e}");
    }

    #[test]
    fn refuses_a_merge_into_a_day_that_isnt_its_record() {
        let root = scratch("not-its-record").join("Databento");
        file_one(&root, "A", day_bytes(&[(C1, 11)], &[(11, 0)])).unwrap();
        // The file changed behind the store's back.
        fs::write(file(&root), day_bytes(&[(C1, 11)], &[(11, 0), (11, 1)])).unwrap();
        let e = file_one(&root, "B", day_bytes(&[(C2, 12)], &[(12, 1)])).unwrap_err();
        assert!(e.contains("its manifest records"), "{e}");
    }

    #[test]
    fn a_retry_after_its_file_was_gone_is_a_duplicate() {
        let root = scratch("retry").join("Databento");
        file_one(&root, "A", day_bytes(&[(C1, 11)], &[(11, 0)])).unwrap();
        file_one(&root, "B", day_bytes(&[(C2, 12)], &[(12, 1)])).unwrap();
        // Stopped after the merge removed its incoming file, before its
        // receipt: filed again, it is already there.
        let part = stage(&root, "B", &[(&name(), day_bytes(&[(C2, 12)], &[(12, 1)]))]);
        fs::remove_file(part.inc.join(name())).unwrap();
        import(&[part], &[], "B", "2026-10-19").unwrap();
        assert_eq!(coverage_of(&root).symbols().len(), 2);
    }

    #[test]
    fn refuses_a_merge_that_isnt_both_days_records_in_order() {
        let root = scratch("merge-checks").join("Databento");
        file_one(&root, "A", day_bytes(&[(C1, 11)], &[(11, 0)])).unwrap();
        let dir = file(&root).parent().unwrap().to_path_buf();
        // The file's records out of time order.
        let e = file_one(&root, "B", day_bytes(&[(C2, 12)], &[(12, 5), (12, 1)])).unwrap_err();
        assert!(e.contains("back in time"), "{e}");
        // Its entry records a different count.
        let mut entry = read_manifest(&dir).unwrap()[DAY].clone();
        entry["records"] = json!(99);
        set_entry(&dir, DAY, Some(entry)).unwrap();
        let e = file_one(&root, "C", day_bytes(&[(C3, 13)], &[(13, 1)])).unwrap_err();
        assert!(e.contains("the merge holds 2 records"), "{e}");
        assert_eq!(inspect(&file(&root)).unwrap().1, 1, "nothing swapped in");
    }

    #[test]
    fn refuses_two_contracts_on_one_instrument() {
        let root = scratch("one-id").join("Databento");
        file_one(&root, "A", day_bytes(&[(C1, 11)], &[(11, 0)])).unwrap();
        let e = file_one(&root, "B", day_bytes(&[(C2, 11)], &[(11, 1)])).unwrap_err();
        assert!(e.contains("instrument 11"), "{e}");
    }

    /// A definition day file of SPY.OPT listing `contracts`.
    fn definitions(root: &Path, contracts: &[&str]) {
        let start = ingest::day_ns(DAY).unwrap();
        let m = Metadata::builder()
            .dataset("OPRA.PILLAR")
            .schema(Some(Schema::Definition))
            .start(start)
            .end(NonZeroU64::new(start + 24 * HOUR))
            .stype_in(Some(SType::Parent))
            .stype_out(SType::InstrumentId)
            .symbols(vec!["SPY.OPT".into()])
            .build();
        let mut out = Vec::new();
        {
            let mut enc = dbn::encode::dbn::Encoder::with_zstd(&mut out, &m).unwrap();
            for (i, c) in contracts.iter().enumerate() {
                let mut r = dbn::InstrumentDefMsg::default();
                r.hd.instrument_id = 11 + u32::try_from(i).unwrap();
                r.hd.ts_event = start;
                r.ts_recv = start;
                r.raw_symbol = dbn::record::str_to_c_chars(c).unwrap();
                enc.encode_record(&r).unwrap();
            }
            enc.flush().unwrap();
        }
        put(
            &root.join("OPRA.PILLAR/SPY.OPT/definition").join(name()),
            &out,
        );
    }

    #[test]
    fn a_day_holding_every_contract_all_day_is_complete() {
        let root = scratch("promote").join("Databento");
        definitions(&root, &[C1, C2]);
        let s = file_one(&root, "A", day_window(&[(C1, 11)], &[(11, 0)], (0, 24))).unwrap();
        assert_eq!(s["days"][0]["complete"], false, "C2 isn't held");
        // C2 for part of the day: still partial.
        let s = file_one(&root, "B", day_window(&[(C2, 12)], &[(12, 1)], (13, 24))).unwrap();
        assert_eq!(s["days"][0]["complete"], false);
        // C2's other hours: every contract, all day.
        let s = file_one(&root, "C", day_window(&[(C2, 12)], &[], (0, 13))).unwrap();
        assert_eq!(s["days"][0]["complete"], true, "{s}");
        let entry = read_manifest(file(&root).parent().unwrap()).unwrap()[DAY].clone();
        assert!(
            !is_partial(&entry) && entry["completed"]["pieces"] == 3,
            "{entry}"
        );
        let vol = root.parent().unwrap();
        assert_eq!(
            spy(&[("v", vol)], "databento").unwrap()[0]["kind"],
            "complete"
        );
    }

    #[test]
    fn a_merge_writes_its_day_alone() {
        let root = scratch("sidecar").join("Databento");
        file_one(&root, "A", day_bytes(&[(C1, 11)], &[(11, 0)])).unwrap();
        let dir = file(&root).parent().unwrap().to_path_buf();
        let index = fs::read(dir.join(MANIFEST)).unwrap();
        assert!(
            !String::from_utf8_lossy(&index).contains(C1),
            "contracts are in the day's own file"
        );
        // Written whole, by a rename: a write is a new file.
        let ino =
            || std::os::unix::fs::MetadataExt::ino(&fs::metadata(dir.join(MANIFEST)).unwrap());
        let before = ino();
        file_one(&root, "B", day_bytes(&[(C2, 12)], &[(12, 1)])).unwrap();
        assert_eq!(ino(), before, "the index isn't written by a merge");
        let side: Value = serde_json::from_str(
            &fs::read_to_string(dir.join("manifest.d").join(format!("{DAY}.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(side["coverage"]["symbols"], json!([C1, C2]));
        // An index from before sidecars, partial days inline, is read, and
        // moves them out the first time it is written.
        let inline = json!({ "days": { DAY: side, "2026-10-01": { "coverage": { "kind": "contracts", "symbols": [C3], "window": [0, 1] },
            "pending": { "sha256": "f00d" } } } });
        fs::write(dir.join(MANIFEST), inline.to_string()).unwrap();
        fs::remove_dir_all(dir.join("manifest.d")).unwrap();
        assert_eq!(
            read_manifest(&dir).unwrap()["2026-10-01"]["coverage"]["symbols"],
            json!([C3])
        );
        crate::manifest::write_days(&dir, &[]).unwrap();
        let index = fs::read_to_string(dir.join(MANIFEST)).unwrap();
        assert!(
            !index.contains(C3) && dir.join("manifest.d/2026-10-01.json").exists(),
            "{index}"
        );
        // Its merge left pending stays pending.
        assert_eq!(crate::manifest::pending_days(&dir), ["2026-10-01"]);
        assert_eq!(
            read_manifest(&dir).unwrap()["2026-10-01"]["coverage"]["symbols"],
            json!([C3])
        );
    }

    #[test]
    fn rebuilds_merged_days_from_their_inputs() {
        let root = scratch("rebuild").join("Databento");
        file_one(&root, "A", day_bytes(&[(C1, 11)], &[(11, 0)])).unwrap();
        // A provider batch of C2, 14:00 to 20:00, filed as a partial day.
        let inc = root.join(INCOMING).join("OPRA-B");
        fs::create_dir_all(&inc).unwrap();
        let fname = "opra-pillar-20261002.cbbo-1m.dbn.zst";
        let bytes = day_window(&[(C2, 12)], &[(12, 60)], (14, 20));
        fs::write(inc.join(fname), &bytes).unwrap();
        let sha = hash_file(&inc.join(fname)).unwrap();
        fs::write(inc.join("manifest.json"), json!({ "files": [{ "filename": fname, "size": bytes.len(), "hash": format!("sha256:{sha}") }] }).to_string()).unwrap();
        let from = ingest::day_ns(DAY).unwrap();
        let q = json!({ "dataset": "OPRA.PILLAR", "schema": "cbbo-1m", "stype_in": "raw_symbol", "symbols": [C2], "encoding": "dbn", "compression": "zstd",
            "start": from + 14 * HOUR, "end": from + 20 * HOUR });
        fs::write(inc.join("metadata.json"), json!({ "query": q, "customizations": { "split_duration": "day", "split_symbols": false } }).to_string()).unwrap();
        let part = Part {
            inc,
            root: root.clone(),
        };
        import_batch(&[part], &[], "OPRA-B", "SPY.OPT", "2026-10-19").unwrap();
        let dir = file(&root).parent().unwrap().to_path_buf();
        let right = coverage_of(&root);
        // As merged before pieces: every contract over the span, and ledgers
        // that didn't record what each file held.
        let hull = || {
            let mut e = read_manifest(&dir).unwrap()[DAY].clone();
            e["coverage"] = json!({ "kind": "contracts", "symbols": [C1, C2], "window": [from + 13 * HOUR, from + 24 * HOUR] });
            set_entry(&dir, DAY, Some(e)).unwrap();
        };
        let forget = |id: &str| {
            for e in fs::read_dir(root.join("migrations")).unwrap().flatten() {
                let n = e.file_name().to_string_lossy().into_owned();
                if n.ends_with(&format!("-import-{id}.events.jsonl")) {
                    let text: Vec<String> = fs::read_to_string(e.path())
                        .unwrap()
                        .lines()
                        .map(|l| {
                            let mut v: Value = serde_json::from_str(l).unwrap();
                            v.as_object_mut().unwrap().remove("coverage");
                            v.to_string()
                        })
                        .collect();
                    fs::write(e.path(), text.join("\n")).unwrap();
                }
            }
        };
        hull();
        forget("OPRA-B");
        let roots = [root.clone()];
        let r = crate::rebuild::rebuild(&roots, std::slice::from_ref(&dir)).unwrap();
        assert_eq!(r["changed"].as_array().unwrap().len(), 1, "{r}");
        assert_eq!(coverage_of(&root), right, "B's hours from its receipt");
        let again = crate::rebuild::rebuild(&roots, std::slice::from_ref(&dir)).unwrap();
        assert_eq!(
            (again["changed"].clone(), again["unchanged"].clone()),
            (json!([]), json!(1))
        );
        // A's hours on record nowhere: left as it is, and listed.
        hull();
        forget("A");
        let r = crate::rebuild::rebuild(&roots, std::slice::from_ref(&dir)).unwrap();
        assert_eq!(r["changed"], json!([]));
        assert!(
            r["not_rebuilt"][0]["why"].as_str().unwrap().contains("A"),
            "{r}"
        );
        assert_eq!(coverage_of(&root).pieces.len(), 1);
    }

    #[test]
    fn refuses_an_import_without_its_source() {
        let root = scratch("source").join("Databento");
        let part = stage(&root, "S", &[(&name(), day_bytes(&[(C1, 11)], &[(11, 0)]))]);
        let mut r: Value =
            serde_json::from_str(&fs::read_to_string(part.inc.join(IMPORT)).unwrap()).unwrap();
        r["source"] = json!({});
        fs::write(part.inc.join(IMPORT), r.to_string()).unwrap();
        assert!(
            import(&[part], &[], "S", "2026-10-19")
                .unwrap_err()
                .contains("where the data came from")
        );
    }

    /// `coverage` of OPRA.PILLAR SPY.OPT cbbo-1m over these volumes, for a
    /// provider, from 2026-10-01 to 2026-10-09.
    fn spy(volumes: &[(&str, &Path)], provider: &str) -> Result<Vec<Value>, String> {
        let v: Vec<(String, PathBuf)> = volumes
            .iter()
            .map(|(n, p)| ((*n).to_owned(), p.to_path_buf()))
            .collect();
        coverage(
            &v,
            provider,
            "OPRA.PILLAR",
            Some("SPY.OPT"),
            "cbbo-1m",
            "2026-10-01",
            "2026-10-09",
        )
    }

    fn put(path: &Path, bytes: &[u8]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }

    const T: crate::catalog::Thresholds = crate::catalog::Thresholds {
        warning: 100.0,
        critical: 100.0,
        floor: 100.0,
    };

    #[test]
    fn keeps_providers_apart() {
        let vol = scratch("providers");
        put(
            &vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-01.dbn.zst"),
            b"a",
        );
        put(
            &vol.join("alpaca/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-02.dbn.zst"),
            b"b",
        );
        let cat = crate::catalog::scan_root("v", &vol, T);
        let found: Vec<(Value, Value, Value)> = cat["datasets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| {
                (
                    d["provider"].clone(),
                    d["filed"].clone(),
                    d["schemas"]["cbbo-1m"]["days"].clone(),
                )
            })
            .collect();
        assert_eq!(
            found,
            [
                (json!("alpaca"), json!(true), json!({ "2026-10-02": 1 })),
                (json!("databento"), json!(true), json!({ "2026-10-01": 1 })),
            ]
        );
        let days = |p: &str| -> Vec<Value> {
            spy(&[("v", &vol)], p)
                .unwrap()
                .iter()
                .map(|d| d["day"].clone())
                .collect()
        };
        assert_eq!(days("databento"), [json!("2026-10-01")]);
        assert_eq!(days("alpaca"), [json!("2026-10-02")]);
    }

    #[test]
    fn coverage_counts_batch_folders_and_the_older_layout() {
        let vol = scratch("unfiled");
        let batch = |dir: PathBuf, stype: &str, symbols: &[&str], day: &str| {
            put(&dir.join("manifest.json"), b"{}");
            let meta = json!({ "job_id": file_name_of(&dir), "query": { "dataset": "OPRA.PILLAR", "stype_in": stype, "symbols": symbols } });
            put(&dir.join("metadata.json"), meta.to_string().as_bytes());
            put(
                &dir.join(format!("opra-pillar-{day}.cbbo-1m.dbn.zst")),
                b"batch",
            );
        };
        // In Databento's folder, and outside any provider's folder.
        batch(
            vol.join("Databento/downloads/OPRA-A"),
            "parent",
            &["SPY.OPT"],
            "20261003",
        );
        batch(
            vol.join("OPRA-B"),
            "parent",
            &["QQQ.OPT", "SPY.OPT"],
            "20261005",
        );
        batch(
            vol.join("Databento/OPRA-C"),
            "raw_symbol",
            &[C1, "QQQ   261002C00500000"],
            "20261006",
        );
        batch(
            vol.join("alpaca/OPRA-D"),
            "parent",
            &["SPY.OPT"],
            "20261007",
        );
        put(
            &vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-04/x.dbn.zst"),
            b"older",
        );
        put(
            &vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-08.dbn.zst"),
            b"filed",
        );
        let days = spy(&[("v", &vol)], "databento").unwrap();
        let brief: Vec<(&str, &str, bool)> = days
            .iter()
            .map(|d| {
                (
                    d["day"].as_str().unwrap(),
                    d["kind"].as_str().unwrap(),
                    d["filed"] == true,
                )
            })
            .collect();
        assert_eq!(
            brief,
            [
                ("2026-10-03", "complete", false),
                ("2026-10-04", "complete", false),
                ("2026-10-05", "complete", false),
                ("2026-10-06", "partial", false),
                ("2026-10-08", "complete", true),
            ]
        );
        assert_eq!(
            days[3]["coverage"]["symbols"],
            json!([C1]),
            "only SPY's contracts"
        );
        assert_eq!(days[0]["volumes"], json!(["v"]));
        // The catalog counts the same days, as unfiled.
        let cat = crate::catalog::scan_root("v", &vol, T);
        let ds = cat["datasets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["provider"] == "databento" && d["symbol"] == "SPY.OPT")
            .unwrap();
        assert_eq!(ds["filed"], false);
        assert_eq!(
            ds["schemas"]["cbbo-1m"]["unfiled"],
            json!(["2026-10-04", "2026-10-03"])
        );
    }

    fn file_name_of(p: &Path) -> String {
        p.file_name().unwrap().to_string_lossy().into_owned()
    }

    #[test]
    fn merges_a_day_held_on_several_volumes() {
        let base = scratch("merge-volumes");
        let vols: Vec<PathBuf> = ["one", "two", "three"]
            .iter()
            .map(|n| base.join(n))
            .collect();
        // Different contracts of the same day, filed on two volumes apart.
        let a = day_bytes(&[(C1, 11)], &[(11, 0)]);
        import(
            &[stage(
                &vols[0].join("Databento"),
                "A",
                &[(&name(), a.clone())],
            )],
            &[],
            "A",
            "2026-10-19",
        )
        .unwrap();
        let b = day_bytes(&[(C2, 12)], &[(12, 1)]);
        import(
            &[stage(&vols[1].join("Databento"), "B", &[(&name(), b)])],
            &[],
            "B",
            "2026-10-19",
        )
        .unwrap();
        let both = spy(&[("one", &vols[0]), ("two", &vols[1])], "databento").unwrap();
        assert_eq!(both.len(), 1);
        assert_eq!(both[0]["kind"], "partial");
        assert_eq!(both[0]["coverage"]["symbols"], json!([C1, C2]));
        assert_eq!(both[0]["volumes"], json!(["one", "two"]));
        assert_eq!(both[0]["conflict"], true);
        assert_eq!(both[0]["sha256"], Value::Null, "no one copy's hash");
        // Identical copies don't conflict.
        import(
            &[stage(&vols[2].join("Databento"), "A2", &[(&name(), a)])],
            &[],
            "A2",
            "2026-10-19",
        )
        .unwrap();
        let same = spy(&[("one", &vols[0]), ("three", &vols[2])], "databento").unwrap();
        assert_eq!(
            (same[0]["conflict"].clone(), same[0]["kind"].clone()),
            (json!(false), json!("partial"))
        );
        // A complete copy wins, whichever volume is read first.
        put(&file(&vols[2].join("Databento")), b"the whole day");
        fs::remove_file(
            vols[2]
                .join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m")
                .join(MANIFEST),
        )
        .unwrap();
        let all = spy(
            &[("one", &vols[0]), ("two", &vols[1]), ("three", &vols[2])],
            "databento",
        )
        .unwrap();
        assert_eq!(all[0]["kind"], "complete");
        assert_eq!(all[0]["volumes"], json!(["one", "three", "two"]));
        assert_eq!(all[0]["conflict"], true);
    }

    #[test]
    fn fails_closed_on_a_malformed_manifest_or_an_unreadable_folder() {
        use std::os::unix::fs::PermissionsExt as _;
        let vol = scratch("closed");
        let dir = vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m");
        put(&dir.join("2026-10-01.dbn.zst"), b"a");
        put(&dir.join(MANIFEST), b"{ not json");
        assert!(
            spy(&[("v", &vol)], "databento")
                .unwrap_err()
                .contains(MANIFEST)
        );
        let cat = crate::catalog::scan_root("v", &vol, T);
        assert_eq!(cat["errors"].as_array().unwrap().len(), 1, "{cat}");
        fs::remove_file(dir.join(MANIFEST)).unwrap();
        assert_eq!(spy(&[("v", &vol)], "databento").unwrap().len(), 1);
        // A dataset folder that can't be read isn't an empty one.
        let ds = vol.join("Databento/OPRA.PILLAR");
        fs::set_permissions(&ds, fs::Permissions::from_mode(0o000)).unwrap();
        let readable = fs::read_dir(&ds).is_ok();
        let got = spy(&[("v", &vol)], "databento");
        let cat = crate::catalog::scan_root("v", &vol, T);
        fs::set_permissions(&ds, fs::Permissions::from_mode(0o755)).unwrap();
        if !readable {
            assert!(got.is_err(), "{got:?}");
            assert!(!cat["errors"].as_array().unwrap().is_empty(), "{cat}");
        }
    }

    /// A Databento batch download in `dir`: `metadata.json` from `query` and
    /// `split_symbols`, and empty files of these names.
    fn raw_batch(dir: &Path, query: &Value, split_symbols: bool, files: &[&str]) {
        put(&dir.join("manifest.json"), b"{}");
        let meta = json!({ "query": query, "customizations": { "split_symbols": split_symbols } });
        put(&dir.join("metadata.json"), meta.to_string().as_bytes());
        for f in files {
            put(&dir.join(f), b"batch");
        }
    }

    fn days_of(days: &[Value]) -> Vec<&str> {
        days.iter().map(|d| d["day"].as_str().unwrap()).collect()
    }

    #[test]
    fn counts_the_store_layout_outside_a_provider_folder_as_databento() {
        let vol = scratch("no-provider");
        put(
            &vol.join("OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-01.dbn.zst"),
            b"a",
        );
        let q = json!({ "dataset": "OPRA.PILLAR", "symbols": ["spy.opt"] });
        raw_batch(
            &vol.join("Databento/OPRA-L"),
            &q,
            false,
            &["opra-pillar-20261002.cbbo-1m.dbn.zst"],
        );
        let days = spy(&[("v", &vol)], "databento").unwrap();
        assert_eq!(
            days_of(&days),
            ["2026-10-01", "2026-10-02"],
            "symbols matched ignoring case"
        );
        assert!(spy(&[("v", &vol)], "alpaca").unwrap().is_empty());
    }

    #[test]
    fn counts_every_day_a_batch_file_or_its_query_covers() {
        let vol = scratch("ranges");
        let q = json!({ "dataset": "OPRA.PILLAR", "schema": "cbbo-1m", "symbols": ["SPY.OPT"] });
        raw_batch(
            &vol.join("Databento/OPRA-R"),
            &q,
            false,
            &["opra-pillar-20261001-20261003.cbbo-1m.dbn.zst"],
        );
        // Split by symbol: the query's days, its end excluded.
        let mut split = q.clone();
        split["start"] = json!(ingest::day_ns("2026-10-05").unwrap());
        split["end"] = json!(ingest::day_ns("2026-10-07").unwrap());
        raw_batch(
            &vol.join("Databento/OPRA-S"),
            &split,
            true,
            &["opra-pillar-20261005.cbbo-1m.SPY.OPT.dbn.zst"],
        );
        // A name that doesn't say its day: the query's, given as dates.
        let mut odd = q.clone();
        odd["start"] = json!("2026-10-08T00:00:00Z");
        // Its end excluded, as Databento means it.
        odd["end"] = json!("2026-10-10");
        raw_batch(
            &vol.join("Databento/OPRA-U"),
            &odd,
            false,
            &["spy.cbbo-1m.dbn.zst"],
        );
        let days = spy(&[("v", &vol)], "databento").unwrap();
        assert_eq!(
            days_of(&days),
            [
                "2026-10-01",
                "2026-10-02",
                "2026-10-03",
                "2026-10-05",
                "2026-10-06",
                "2026-10-08",
                "2026-10-09"
            ]
        );
        let cat = crate::catalog::scan_root("v", &vol, T);
        let ds = &cat["datasets"][0]["schemas"]["cbbo-1m"];
        assert_eq!(ds["days"].as_object().unwrap().len(), 7, "{ds}");
        assert_eq!(cat["errors"], json!([]));
        // Without the query's range, the batch is an error, not skipped.
        raw_batch(
            &vol.join("Databento/OPRA-V"),
            &q,
            true,
            &["opra-pillar-20261010.cbbo-1m.SPY.OPT.dbn.zst"],
        );
        assert!(
            spy(&[("v", &vol)], "databento")
                .unwrap_err()
                .contains("OPRA-V")
        );
        let cat = crate::catalog::scan_root("v", &vol, T);
        assert!(
            cat["errors"][0].as_str().unwrap().contains("OPRA-V"),
            "{cat}"
        );
    }

    /// A cbbo-1m DBN file of no records whose header lists `symbols`
    /// (requested as `stype`) from `first` to `last` (whole days).
    fn header_bytes(symbols: &[&str], stype: SType, first: &str, last: &str) -> Vec<u8> {
        header_of("OPRA.PILLAR", symbols, stype, first, last)
    }

    /// As `header_bytes`, of `dataset`.
    fn header_of(
        dataset: &str,
        symbols: &[&str],
        stype: SType,
        first: &str,
        last: &str,
    ) -> Vec<u8> {
        let m = Metadata::builder()
            .dataset(dataset)
            .schema(Some(Schema::Cbbo1M))
            .start(ingest::day_ns(first).unwrap())
            .end(NonZeroU64::new(
                ingest::day_ns(last).unwrap() + 86_400_000_000_000,
            ))
            .stype_in(Some(stype))
            .stype_out(SType::InstrumentId)
            .symbols(symbols.iter().map(|s| (*s).to_owned()).collect())
            .build();
        let mut out = Vec::new();
        drop(dbn::encode::dbn::Encoder::with_zstd(&mut out, &m).unwrap());
        out
    }

    /// A DBN file of no records as another program records them: no
    /// symbols, no end, and a start of 0 when `start` is `None`.
    fn recording(
        dataset: &str,
        schema: Option<Schema>,
        start: Option<&str>,
        zstd: bool,
    ) -> Vec<u8> {
        let m = Metadata::builder()
            .dataset(dataset)
            .schema(schema)
            .start(start.map_or(0, |d| ingest::day_ns(d).unwrap() + 13 * 3_600_000_000_000))
            .stype_in(Some(SType::RawSymbol))
            .stype_out(SType::InstrumentId)
            .build();
        let mut out = Vec::new();
        if zstd {
            drop(dbn::encode::dbn::Encoder::with_zstd(&mut out, &m).unwrap());
        } else {
            dbn::encode::dbn::Encoder::new(&mut out, &m).unwrap();
        }
        out
    }

    #[test]
    fn another_programs_recordings_are_listed_by_folder_and_named_day() {
        let vol = scratch("recordings");
        for sym in ["UVXY", "VXZ"] {
            let dir = vol.join("Alpaca").join(sym);
            put(
                &dir.join("2026-05-29.spot.trades.dbn.zst"),
                &recording(
                    "ALPACA.SPOT",
                    Some(Schema::Trades),
                    Some("2026-05-29"),
                    true,
                ),
            );
            put(
                &dir.join("2026-05-29.definitions.dbn.zst"),
                &recording("ALPACA.OPT", Some(Schema::Definition), None, true),
            );
            // Not datasets: another program's own ids, and an index.
            put(
                &dir.join("2026-05-11.dbn"),
                &recording("bandit-chain", None, None, false),
            );
            put(
                &dir.join("2026-05-11.spot.dbn"),
                &recording("bandit-spot", None, None, false),
            );
            put(
                &dir.join("2026-07-01.poll.idx.dbn"),
                &recording("ALPACA.POLLIDX", None, None, false),
            );
        }
        // Loose at the top of the provider's folder: no folder to name a symbol.
        put(
            &vol.join("Alpaca/2026-05-30.trades.dbn.zst"),
            &recording(
                "ALPACA.SPOT",
                Some(Schema::Trades),
                Some("2026-05-30"),
                true,
            ),
        );
        let cat = crate::catalog::scan_root("v", &vol, T);
        assert_eq!(cat["errors"], json!([]), "{cat}");
        let listed: Vec<(String, String)> = cat["datasets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| {
                (
                    d["id"].as_str().unwrap().to_owned(),
                    d["symbol"].as_str().unwrap_or("-").to_owned(),
                )
            })
            .collect();
        let pairs: Vec<(&str, &str)> = listed
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();
        assert_eq!(
            pairs,
            [
                ("ALPACA.OPT", "UVXY"),
                ("ALPACA.OPT", "VXZ"),
                ("ALPACA.SPOT", "-"),
                ("ALPACA.SPOT", "UVXY"),
                ("ALPACA.SPOT", "VXZ")
            ]
        );
        let uvxy = cat["datasets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["id"] == "ALPACA.SPOT" && d["symbol"] == "UVXY")
            .unwrap();
        assert_eq!(uvxy["provider"], "alpaca");
        // No end in the header: the day the file is named for, not until its last change.
        assert_eq!(
            uvxy["schemas"]["trades"]["days"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            ["2026-05-29"]
        );
        let opt = cat["datasets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["id"] == "ALPACA.OPT")
            .unwrap();
        // No start either: not every day since 1970.
        assert_eq!(
            opt["schemas"]["definition"]["days"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            ["2026-05-29"]
        );
        // The rest count as space used, by folder and id.
        let other: Vec<(&str, &str, u64)> = cat["other"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| {
                (
                    o["name"].as_str().unwrap(),
                    o["dataset"].as_str().unwrap(),
                    o["files"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            other,
            [
                ("Alpaca/UVXY", "ALPACA.POLLIDX", 1),
                ("Alpaca/UVXY", "bandit-chain", 1),
                ("Alpaca/UVXY", "bandit-spot", 1),
                ("Alpaca/VXZ", "ALPACA.POLLIDX", 1),
                ("Alpaca/VXZ", "bandit-chain", 1),
                ("Alpaca/VXZ", "bandit-spot", 1),
            ]
        );
        assert!(cat["other_bytes"].as_u64().unwrap() > 0);
        assert_eq!(cat["unrecognised"], json!([]));
        // Never held: a Databento purchase isn't blocked by them.
        put(
            &vol.join("backup/SPY.OPT/2026-10-02.dbn"),
            &recording("bandit-chain", Some(Schema::Cbbo1M), None, false),
        );
        assert_eq!(spy_coverage(&vol), Vec::<Value>::new());
        let cat = crate::catalog::scan_root("v", &vol, T);
        assert_eq!(cat["unrecognised"], json!(["backup"]));
        assert_eq!(
            cat["other"].as_array().unwrap().len(),
            7,
            "the backup once, as a folder"
        );
    }

    #[test]
    fn a_day_is_held_only_with_data_to_serve() {
        let vol = scratch("servable");
        let cbbo = vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m");
        // An empty day file, padding: not held.
        put(&cbbo.join("2026-10-01.dbn.zst"), b"");
        put(&cbbo.join("2026-10-02.dbn.zst"), b"abc");
        // The older layout: a day folder of data and its definitions; a
        // weekend's shell, of receipts and an archive; a day folder of only
        // the definitions.
        put(&cbbo.join("2026-10-03/definition.dbn.zst"), b"d");
        put(&cbbo.join("2026-10-03/cbbo-1m.dbn.zst"), b"data");
        put(&cbbo.join("2026-10-04/.archive/cbbo-1m.dbn.zst"), b"old");
        put(&cbbo.join("2026-10-04/manifest.json"), b"{}");
        put(&cbbo.join("2026-10-05/definition.dbn.zst"), b"d");
        // A batch download: a file of two days, each counted its share, and
        // an empty file, holding none.
        let batch = vol.join("OPRA-20261006-X");
        put(
            &batch.join("metadata.json"),
            json!({ "job_id": "OPRA-20261006-X", "query": { "dataset": "OPRA.PILLAR", "symbols": ["SPY.OPT"] } })
                .to_string()
                .as_bytes(),
        );
        put(&batch.join("manifest.json"), b"{}");
        put(
            &batch.join("opra-pillar-20261006-20261007.cbbo-1m.dbn.zst"),
            b"12345",
        );
        put(&batch.join("opra-pillar-20261008.cbbo-1m.dbn.zst"), b"");
        // Scratch output with no end in its header: its start's day only,
        // not every day until it last changed.
        let rec = recording(
            "OPRA.PILLAR",
            Some(Schema::Cbbo1M),
            Some("2026-10-09"),
            true,
        );
        put(&vol.join("tmp/scratch/combined-output.dbn.zst"), &rec);

        let days = spy_coverage(&vol);
        let at = "Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m";
        let range = "v/OPRA-20261006-X/opra-pillar-20261006-20261007.cbbo-1m.dbn.zst";
        let brief: Vec<(&str, Value, Value)> = days
            .iter()
            .map(|d| {
                (
                    d["day"].as_str().unwrap(),
                    d["size"].clone(),
                    d["files"].clone(),
                )
            })
            .collect();
        assert_eq!(
            brief,
            [
                (
                    "2026-10-02",
                    json!(3),
                    json!([format!("v/{at}/2026-10-02.dbn.zst")])
                ),
                (
                    "2026-10-03",
                    json!(5),
                    json!([
                        format!("v/{at}/2026-10-03/cbbo-1m.dbn.zst"),
                        format!("v/{at}/2026-10-03/definition.dbn.zst")
                    ])
                ),
                ("2026-10-06", json!(3), json!([range])),
                ("2026-10-07", json!(2), json!([range])),
                (
                    "2026-10-09",
                    json!(rec.len()),
                    json!(["v/tmp/scratch/combined-output.dbn.zst"])
                ),
            ]
        );

        let (cat, _, served) = crate::catalog::scan_volume("v", &vol, T, &[]);
        assert_eq!(cat["errors"], json!([]));
        let datasets = cat["datasets"].as_array().unwrap();
        let filed = datasets
            .iter()
            .find(|d| d["provider"] == "databento")
            .unwrap();
        assert_eq!(
            filed["schemas"]["cbbo-1m"]["days"],
            json!({ "2026-10-02": 3, "2026-10-03": 5 })
        );
        assert_eq!(filed["stray"], false);
        // Outside every provider's folder: the download is a dataset, the
        // scratch output is stray.
        let stray: Vec<(&Value, &Value)> = datasets
            .iter()
            .filter(|d| d["provider"].is_null())
            .map(|d| (&d["symbol"], &d["stray"]))
            .collect();
        assert_eq!(
            stray,
            [
                (&Value::Null, &json!(true)),
                (&json!("SPY.OPT"), &json!(false))
            ]
        );
        let scratch = datasets.iter().find(|d| d["stray"] == true).unwrap();
        assert_eq!(
            scratch["schemas"]["cbbo-1m"]["days"],
            json!({ "2026-10-09": rec.len() })
        );
        // `/files` lists every served file, held or not, never a hidden one.
        let listed: Vec<&str> = served.iter().map(|(p, _)| p.as_str()).collect();
        assert_eq!(
            listed,
            [
                "Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-01.dbn.zst",
                "Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-02.dbn.zst",
                "Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-03/cbbo-1m.dbn.zst",
                "Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-03/definition.dbn.zst",
                "Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-05/definition.dbn.zst",
                "OPRA-20261006-X/opra-pillar-20261006-20261007.cbbo-1m.dbn.zst",
                "OPRA-20261006-X/opra-pillar-20261008.cbbo-1m.dbn.zst",
                "tmp/scratch/combined-output.dbn.zst",
            ]
        );
        fs::remove_dir_all(&vol).unwrap();
    }

    #[test]
    fn counts_every_dbn_file_the_layouts_do_not_claim() {
        let vol = scratch("loose");
        let parent = |a: &str, b: &str| header_bytes(&["SPY.OPT"], SType::Parent, a, b);
        // A download extracted by hand: metadata.json, no manifest.
        let manual = vol.join("OPRA-M.manual-extracted");
        put(
            &manual.join("metadata.json"),
            json!({ "query": { "dataset": "OPRA.PILLAR", "schema": "cbbo-1m" } })
                .to_string()
                .as_bytes(),
        );
        put(
            &manual.join("opra-pillar-20261003.cbbo-1m.dbn.zst"),
            &parent("2026-10-03", "2026-10-03"),
        );
        // A loose file, named any way, holding two days.
        put(
            &vol.join("Databento/spy-odd-name.dbn.zst"),
            &parent("2026-10-04", "2026-10-05"),
        );
        // In a schema folder, not named for its day.
        put(
            &vol.join("Databento/OPRA.PILLAR/SPY.OPT/cbbo-1m/extra.dbn.zst"),
            &parent("2026-10-06", "2026-10-06"),
        );
        // The store layout, deeper than the catalog reads it.
        put(
            &vol.join("Databento/a/b/c/d/OPRA.PILLAR/SPY.OPT/cbbo-1m/2026-10-07.dbn.zst"),
            &parent("2026-10-07", "2026-10-07"),
        );
        // A list of contracts: partial, for those of SPY's.
        put(
            &vol.join("contracts.dbn"),
            &header_bytes(
                &[C1, "QQQ   261002C00500000"],
                SType::RawSymbol,
                "2026-10-08",
                "2026-10-08",
            ),
        );
        // Without a manifest, and no DBN: its query's days.
        let csv = vol.join("misc/deeper/still/OPRA-C");
        put(&csv.join("metadata.json"), json!({ "query": { "dataset": "OPRA.PILLAR", "schema": "cbbo-1m", "symbols": ["SPY.OPT"], "start": "2026-10-09", "end": "2026-10-10" } }).to_string().as_bytes());
        put(&csv.join("opra-pillar-20261009.cbbo-1m.csv"), b"a,b");
        // Not counted: Alpaca's provider folder.
        put(
            &vol.join("alpaca/x.dbn.zst"),
            &parent("2026-10-01", "2026-10-01"),
        );
        let days = spy_coverage(&vol);
        let brief: Vec<(&str, &str, bool)> = days
            .iter()
            .map(|d| {
                (
                    d["day"].as_str().unwrap(),
                    d["kind"].as_str().unwrap(),
                    d["filed"] == true,
                )
            })
            .collect();
        assert_eq!(
            brief,
            [
                ("2026-10-03", "complete", false),
                ("2026-10-04", "complete", false),
                ("2026-10-05", "complete", false),
                ("2026-10-06", "complete", false),
                ("2026-10-07", "complete", false),
                ("2026-10-08", "partial", false),
                ("2026-10-09", "complete", false),
            ]
        );
        assert_eq!(days[5]["coverage"]["symbols"], json!([C1]));
        let cat = crate::catalog::scan_root("v", &vol, T);
        assert_eq!(cat["errors"], json!([]));
        let ds = cat["datasets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["provider"] == "databento" && d["symbol"] == "SPY.OPT")
            .unwrap();
        assert_eq!(ds["filed"], false);
        // Outside Databento's folder (the download at the top, the one in
        // misc) they have no provider.
        let elsewhere = cat["datasets"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|d| d["provider"].is_null())
            .map(|d| d["schemas"]["cbbo-1m"]["days"].as_object().unwrap().len())
            .sum::<usize>();
        assert_eq!(elsewhere, 3, "{cat}");
        assert_eq!(
            ds["schemas"]["cbbo-1m"]["days"].as_object().unwrap().len(),
            4,
            "{ds}"
        );
        // A header that can't be read, changed just now, is being written:
        // pending, read again next scan. Older, it is an error.
        let junk = vol.join("Databento/junk.dbn.zst");
        put(&junk, b"not dbn");
        assert_eq!(spy(&[("v", &vol)], "databento").unwrap().len(), 7);
        let cat = crate::catalog::scan_root("v", &vol, T);
        assert_eq!(cat["errors"], json!([]));
        assert_eq!(cat["pending_files"], json!([junk.display().to_string()]));
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(200);
        fs::File::options()
            .write(true)
            .open(&junk)
            .unwrap()
            .set_modified(old)
            .unwrap();
        assert!(
            spy(&[("v", &vol)], "databento")
                .unwrap_err()
                .contains("junk")
        );
        let cat = crate::catalog::scan_root("v", &vol, T);
        assert!(cat["errors"][0].as_str().unwrap().contains("junk"), "{cat}");
        assert_eq!(cat["pending_files"], json!([]));
    }

    fn spy_coverage(vol: &Path) -> Vec<Value> {
        spy(&[("v", vol)], "databento").unwrap()
    }

    #[test]
    fn reads_each_header_once_while_the_file_is_unchanged() {
        let vol = scratch("header-cache");
        let file = vol.join("Databento/loose.dbn.zst");
        let bytes = header_bytes(&["SPY.OPT"], SType::Parent, "2026-10-03", "2026-10-03");
        put(&file, &bytes);
        let at = fs::metadata(&file).unwrap().modified().unwrap();
        assert_eq!(spy_coverage(&vol).len(), 1);
        // Same size and last change: the header isn't read again.
        fs::write(&file, vec![0u8; bytes.len()]).unwrap();
        fs::File::options()
            .write(true)
            .open(&file)
            .unwrap()
            .set_modified(at)
            .unwrap();
        assert_eq!(spy_coverage(&vol).len(), 1, "from the cache");
        // Changed (and not just now, so not taken as being written).
        let earlier = at - std::time::Duration::from_secs(200);
        fs::File::options()
            .write(true)
            .open(&file)
            .unwrap()
            .set_modified(earlier)
            .unwrap();
        assert!(spy(&[("v", &vol)], "databento").is_err());
    }

    #[test]
    fn data_outside_provider_folders_has_no_provider() {
        let vol = scratch("no-provider-files");
        // Databento's data in a backup, its dataset spelled another way.
        put(
            &vol.join("backups/old/spy.dbn.zst"),
            &header_of(
                "opra.pillar",
                &["SPY.OPT"],
                SType::Parent,
                "2026-10-03",
                "2026-10-03",
            ),
        );
        // A crypto recording.
        put(
            &vol.join("coinbase/btc.dbn.zst"),
            &header_of(
                "COINBASE.L2",
                &["BTC-USD"],
                SType::RawSymbol,
                "2026-10-03",
                "2026-10-03",
            ),
        );
        put(
            &vol.join("Alpaca/spy.dbn.zst"),
            &header_bytes(&["SPY.OPT"], SType::Parent, "2026-10-04", "2026-10-04"),
        );
        let cat = crate::catalog::scan_root("v", &vol, T);
        let providers: Vec<(Value, Value)> = cat["datasets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| (d["provider"].clone(), d["id"].clone()))
            .collect();
        assert_eq!(
            providers,
            [
                (Value::Null, json!("COINBASE.L2")),
                (Value::Null, json!("opra.pillar")),
                (json!("alpaca"), json!("OPRA.PILLAR")),
            ]
        );
        // The backup's copy blocks a Databento purchase; Alpaca's doesn't.
        assert_eq!(days_of(&spy_coverage(&vol)), ["2026-10-03"]);
        assert_eq!(
            days_of(&spy(&[("v", &vol)], "alpaca").unwrap()),
            ["2026-10-04"]
        );
    }

    #[test]
    fn instrument_ids_hold_every_symbol_of_the_dataset() {
        let vol = scratch("instrument-ids");
        put(
            &vol.join("Databento/ids.dbn.zst"),
            &header_bytes(&["12345"], SType::InstrumentId, "2026-10-04", "2026-10-04"),
        );
        put(
            &vol.join("Databento/qqq.dbn.zst"),
            &header_bytes(&["QQQ.OPT"], SType::Parent, "2026-10-05", "2026-10-05"),
        );
        let v = [("v".to_owned(), vol.clone())];
        let held = |symbol: &str| -> Vec<String> {
            coverage(
                &v,
                "databento",
                "OPRA.PILLAR",
                Some(symbol),
                "cbbo-1m",
                "2026-10-01",
                "2026-10-09",
            )
            .unwrap()
            .iter()
            .map(|d| {
                format!(
                    "{} {}",
                    d["day"].as_str().unwrap(),
                    d["kind"].as_str().unwrap()
                )
            })
            .collect()
        };
        assert_eq!(held("SPY.OPT"), ["2026-10-04 complete"]);
        assert_eq!(
            held("QQQ.OPT"),
            ["2026-10-04 complete", "2026-10-05 complete"]
        );
    }

    #[test]
    fn the_stores_own_folders_and_receipts_hold_nothing() {
        let vol = scratch("own-folders");
        let q = json!({ "dataset": "OPRA.PILLAR", "schema": "cbbo-1m", "symbols": ["SPY.OPT"], "start": "2026-10-01", "end": "2026-10-04" });
        let receipts = |dir: PathBuf, manifest: bool| {
            put(
                &dir.join("metadata.json"),
                json!({ "query": q }).to_string().as_bytes(),
            );
            put(&dir.join(IMPORT), b"{}");
            if manifest {
                put(&dir.join("manifest.json"), b"{}");
            }
        };
        receipts(vol.join("Databento/provenance/OPRA-P"), true);
        receipts(vol.join("Databento/provenance/OPRA-Q"), false);
        put(
            &vol.join("Databento/migrations/2026-10-19-import-x.events.jsonl"),
            b"{}",
        );
        // A download with its manifest but no data holds nothing either.
        receipts(vol.join("Databento/downloads/OPRA-E"), true);
        assert_eq!(spy_coverage(&vol), Vec::<Value>::new());
        let cat = crate::catalog::scan_root("v", &vol, T);
        assert_eq!(
            (cat["datasets"].clone(), cat["errors"].clone()),
            (json!([]), json!([]))
        );
        // Without its manifest, outside the store's folders, its query
        // counts, the end excluded; an end later in a day includes it.
        receipts(vol.join("Databento/downloads/OPRA-F"), false);
        assert_eq!(
            days_of(&spy_coverage(&vol)),
            ["2026-10-01", "2026-10-02", "2026-10-03"]
        );
        let mut late = q.clone();
        late["end"] = json!("2026-10-04T12:00:00Z");
        put(
            &vol.join("Databento/downloads/OPRA-F/metadata.json"),
            json!({ "query": late }).to_string().as_bytes(),
        );
        assert_eq!(spy_coverage(&vol).len(), 4);
    }

    #[test]
    fn reports_folders_it_cannot_read() {
        use std::os::unix::fs::PermissionsExt as _;
        let vol = scratch("unreadable-misc");
        let misc = vol.join("misc");
        fs::create_dir_all(&misc).unwrap();
        fs::set_permissions(&misc, fs::Permissions::from_mode(0o000)).unwrap();
        let readable = fs::read_dir(&misc).is_ok();
        let cat = crate::catalog::scan_root("v", &vol, T);
        fs::set_permissions(&misc, fs::Permissions::from_mode(0o755)).unwrap();
        if !readable {
            assert!(cat["errors"][0].as_str().unwrap().contains("misc"), "{cat}");
        }
    }

    /// Every file in `TT_REAL_PARTIAL_DIR` passes `inspect`, as an import
    /// would check it. Run with `--ignored`.
    #[test]
    #[ignore = "reads real files named by TT_REAL_PARTIAL_DIR"]
    fn real_partial_days_inspect() {
        let Ok(dir) = std::env::var("TT_REAL_PARTIAL_DIR") else {
            return;
        };
        let mut n = 0;
        for e in fs::read_dir(dir).unwrap().flatten() {
            if e.file_name().to_string_lossy().ends_with(".dbn.zst") {
                if let Err(err) = inspect(&e.path()) {
                    panic!("{}: {err}", e.path().display());
                }
                n += 1;
            }
        }
        eprintln!("{n} files inspected");
    }

    pub(crate) const C4: &str = "SPY   261002P00729000";

    /// A group of imports staged in `root`, each of one day file.
    fn group(root: &Path, files: &[(&str, &[u8])]) -> Vec<Filing> {
        files
            .iter()
            .map(|(id, bytes)| Filing {
                parts: vec![stage(root, id, &[(&name(), bytes.to_vec())])],
                elsewhere: Vec::new(),
                id: (*id).to_owned(),
                today: "2026-10-19".into(),
                parent: None,
            })
            .collect()
    }

    /// A held partial day (C1), and three jobs of other contracts for it.
    fn held_and_three(root: &Path) -> (Vec<u8>, [Vec<u8>; 3]) {
        let held = day_bytes(&[(C1, 11)], &[(11, 0), (11, 3), (11, 6)]);
        file_one(root, "A", held.clone()).unwrap();
        let three = [
            day_bytes(&[(C2, 12)], &[(12, 1), (12, 3), (12, 7)]),
            day_bytes(&[(C3, 13)], &[(13, 2), (13, 3)]),
            day_bytes(&[(C4, 14)], &[(14, 0), (14, 5), (14, 8), (14, 9)]),
        ];
        (held, three)
    }

    /// The day holds every record of each input, in its order, and nothing else.
    fn holds_each(root: &Path, inputs: &[&[u8]]) {
        let (_, all) = read_all(&file(root)).unwrap();
        let mut n = 0;
        for (i, bytes) in inputs.iter().enumerate() {
            let p = root.join(format!("input-{i}.dbn.zst"));
            fs::write(&p, bytes).unwrap();
            let (m, want) = read_all(&p).unwrap();
            let id = ids(&m).unwrap().into_keys().next().unwrap();
            let got: Vec<&[u8]> = all
                .iter()
                .filter(|r| r.header().instrument_id == id)
                .map(AsRef::as_ref)
                .collect();
            assert_eq!(got, want.iter().map(AsRef::as_ref).collect::<Vec<_>>());
            n += want.len();
        }
        assert_eq!(all.len(), n);
    }

    fn ledger(root: &Path, id: &str) -> Vec<Value> {
        let p = root.join(format!("migrations/2026-10-19-import-{id}.events.jsonl"));
        fs::read_to_string(p)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[test]
    fn a_group_merges_into_a_held_day_in_one_write() {
        let root = scratch("group").join("Databento");
        let (held, [b, c, d]) = held_and_three(&root);
        let g = group(&root, &[("B", &b), ("C", &c), ("D", &d)]);
        let done: Vec<Value> = import_group(&g).into_iter().map(Result::unwrap).collect();
        assert!(done.iter().all(|s| s["days"][0]["filed"] == "merged"));
        // One merge, of the day and all three, swapped in once.
        let sha = hash_file(&file(&root)).unwrap();
        let mut outputs = Vec::new();
        for (id, bytes) in [("B", &b), ("C", &c), ("D", &d)] {
            let events = ledger(&root, id);
            let step = |s: &'static str| -> Vec<&Value> {
                events.iter().filter(|e| e["step"] == s).collect()
            };
            let merged = step("merged");
            assert_eq!(merged.len(), 1, "{id}");
            assert_eq!(merged[0]["inputs"].as_array().unwrap().len(), 4);
            assert_eq!(merged[0]["sha256"], sha.as_str());
            outputs.push(merged[0]["output"].clone());
            assert_eq!(step("swapped").len(), 1);
            // Each input's SHA-256 and record count, on record.
            let v = step("verified")[0];
            let copy = root.join(format!("{id}.copy"));
            fs::write(&copy, bytes).unwrap();
            assert_eq!(v["sha256"], hash_file(&copy).unwrap().as_str());
            assert!(v["records"].is_u64());
        }
        assert!(outputs.windows(2).all(|w| w[0] == w[1]));
        let records: Vec<u64> = ["B", "C", "D"]
            .iter()
            .map(|id| {
                ledger(&root, id)
                    .iter()
                    .find(|e| e["step"] == "verified")
                    .unwrap()["records"]
                    .as_u64()
                    .unwrap()
            })
            .collect();
        assert_eq!(records, [3, 2, 4]);
        let entry = read_manifest(file(&root).parent().unwrap()).unwrap()[DAY].clone();
        assert_eq!(entry["records"], 3 + 3 + 2 + 4);
        assert_eq!(inspect(&file(&root)).unwrap().1, 12);
        holds_each(&root, &[&held, &b, &c, &d]);
        assert!(!root.join(INCOMING).exists(), "nothing left in incoming");
    }

    #[test]
    fn a_crashed_group_merge_settles_whole_and_files_again() {
        for at in ["written", "pending", "swapped"] {
            let root = scratch(&format!("crash-{at}")).join("Databento");
            let (held, [b, c, d]) = held_and_three(&root);
            let before = hash_file(&file(&root)).unwrap();
            let g = group(&root, &[("B", &b), ("C", &c), ("D", &d)]);
            CRASH.with(|c| c.set(Some(at)));
            let r = import_group(&g);
            CRASH.with(|c| c.set(None));
            assert!(
                r.iter()
                    .all(|r| r.as_ref().is_err_and(|e| e.contains("crashed"))),
                "{at}: {r:?}"
            );
            // The next scan settles the day to before the group, or after all of it.
            let dir = file(&root).parent().unwrap().to_path_buf();
            let now = settle(&dir, DAY).unwrap().unwrap();
            assert!(now.get("pending").is_none());
            assert_eq!(now["sha256"], hash_file(&file(&root)).unwrap().as_str());
            let n = inspect(&file(&root)).unwrap().1;
            if at == "swapped" {
                assert_eq!(n, 12, "{at}");
                holds_each(&root, &[&held, &b, &c, &d]);
            } else {
                assert_eq!(
                    (hash_file(&file(&root)).unwrap(), n),
                    (before.clone(), 3),
                    "{at}"
                );
            }
            // Filing again completes it.
            for r in import_group(&g) {
                r.unwrap();
            }
            assert_eq!(inspect(&file(&root)).unwrap().1, 12, "{at}");
            assert_eq!(read_manifest(&dir).unwrap()[DAY]["records"], 12);
            holds_each(&root, &[&held, &b, &c, &d]);
            assert!(
                !root.join(INCOMING).exists(),
                "{at}: nothing left in incoming"
            );
        }
    }

    #[test]
    fn a_retry_after_a_later_merge_into_its_day_is_a_duplicate() {
        let root = scratch("retry-later").join("Databento");
        let (held, [b, c, d]) = held_and_three(&root);
        let g = group(&root, &[("B", &b), ("C", &c), ("D", &d)]);
        CRASH.with(|x| x.set(Some("swapped")));
        let _ = import_group(&g);
        CRASH.with(|x| x.set(None));
        // Another filing merges into the day before they are tried again.
        let e = day_bytes(&[("SPY   261002C00730000", 15)], &[(15, 4)]);
        file_one(&root, "E", e.clone()).unwrap();
        for r in import_group(&g) {
            assert_eq!(r.unwrap()["days"][0]["filed"], "already present");
        }
        holds_each(&root, &[&held, &b, &c, &d, &e]);
        assert!(!root.join(INCOMING).exists());
    }

    #[test]
    fn a_job_refused_in_a_group_is_refused_alone() {
        let root = scratch("group-refused").join("Databento");
        let (held, [b, _, d]) = held_and_three(&root);
        // C1 over the hours held; and C3 on the held day's instrument ID.
        let again = day_bytes(&[(C1, 11)], &[(11, 1)]);
        let clash = day_bytes(&[(C3, 11)], &[(11, 2)]);
        let alone: Vec<String> = group(&root, &[("R1", &again), ("R2", &clash)])
            .into_iter()
            .map(|f| import_group(&[f]).pop().unwrap().unwrap_err())
            .collect();
        assert!(
            alone[0].contains("already holds 1 of these contracts"),
            "{}",
            alone[0]
        );
        assert!(alone[1].contains("instrument 11"), "{}", alone[1]);
        let mut g = group(&root, &[("B", &b), ("D", &d)]);
        let refused = group(&root, &[("R1", &again), ("R2", &clash)]);
        g.splice(1..1, refused);
        let r = import_group(&g);
        assert_eq!(r[1].as_ref().unwrap_err(), &alone[0]);
        assert_eq!(r[2].as_ref().unwrap_err(), &alone[1]);
        assert!(r[0].is_ok() && r[3].is_ok(), "{r:?}");
        holds_each(&root, &[&held, &b, &d]);
        // Within a group too: a job of contracts an earlier one files.
        let root = scratch("group-overlap").join("Databento");
        let (_, [b, c, _]) = held_and_three(&root);
        let r = import_group(&group(&root, &[("B", &b), ("B2", &b), ("C", &c)]));
        assert!(
            r[1].as_ref()
                .is_err_and(|e| e.contains("already holds 1 of these contracts"))
        );
        assert!(r[0].is_ok() && r[2].is_ok());
    }

    #[test]
    fn a_group_files_a_day_not_held_once() {
        let root = scratch("group-new").join("Databento");
        let (_, [b, c, d]) = held_and_three(&scratch("group-new-held").join("Databento"));
        let r = import_group(&group(&root, &[("B", &b), ("C", &c), ("D", &d)]));
        let filed: Vec<Value> = r
            .into_iter()
            .map(|r| r.unwrap()["days"][0]["filed"].clone())
            .collect();
        assert_eq!(filed, ["new", "merged", "merged"]);
        holds_each(&root, &[&b, &c, &d]);
        assert_eq!(
            read_manifest(file(&root).parent().unwrap()).unwrap()[DAY]["records"],
            9
        );
    }

    /// About 5 MB of cbbo-1m on `day`: `n` contracts from instrument `first`,
    /// a record each minute from 13:00 to 24:00 UTC, prices made up.
    fn big_day(day: &str, first: u32, n: u32) -> Vec<u8> {
        let start = ingest::day_ns(day).unwrap();
        let fmt = time::macros::format_description!("[year]-[month]-[day]");
        let date = time::Date::parse(day, fmt).unwrap();
        let contracts: Vec<(String, u32)> = (first..first + n)
            .map(|id| (format!("SPY   261002C{:08}", 100_000 + id), id))
            .collect();
        let m = Metadata::builder()
            .dataset("OPRA.PILLAR")
            .schema(Some(Schema::Cbbo1M))
            .start(start + 13 * 3_600_000_000_000)
            .end(NonZeroU64::new(start + 24 * 3_600_000_000_000))
            .stype_in(Some(SType::RawSymbol))
            .stype_out(SType::InstrumentId)
            .symbols(contracts.iter().map(|(s, _)| s.clone()).collect())
            .mappings(
                contracts
                    .iter()
                    .map(|(s, id)| SymbolMapping {
                        raw_symbol: s.clone(),
                        intervals: vec![MappingInterval {
                            start_date: date,
                            end_date: date.next_day().unwrap(),
                            symbol: id.to_string(),
                        }],
                    })
                    .collect(),
            )
            .build();
        let mut out = Vec::new();
        let mut x = u64::from(first) | 1;
        let mut enc = dbn::encode::dbn::Encoder::with_zstd(&mut out, &m).unwrap();
        for minute in 0..660 {
            for id in first..first + n {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let mut r = CbboMsg::default_for_schema(Schema::Cbbo1M);
                r.hd.instrument_id = id;
                r.hd.ts_event = start + (13 * 60 + minute) * 60_000_000_000;
                r.ts_recv = r.hd.ts_event;
                r.price = (x % 100_000) as i64 * 10_000_000;
                r.size = (x >> 20) as u32 % 500;
                r.levels[0].bid_px = r.price - ((x >> 30) % 50) as i64 * 10_000_000;
                r.levels[0].ask_px = r.price + ((x >> 36) % 50) as i64 * 10_000_000;
                r.levels[0].bid_sz = (x >> 42) as u32 % 1000;
                r.levels[0].ask_sz = (x >> 52) as u32 % 1000;
                enc.encode_record(&r).unwrap();
            }
        }
        enc.flush().unwrap();
        drop(enc);
        out
    }

    /// The filing rate of 50 jobs of about 5 MB, five into each of ten
    /// partly held days: one at a time, in groups of four (as four downloads
    /// at once deliver them), and in one group. In a temporary folder;
    /// `cargo test -p store --release filing_rate -- --ignored --nocapture`.
    #[test]
    #[ignore = "a measurement: about 1 GB written"]
    fn filing_rate() {
        let days: Vec<String> = (1..=10).map(|d| format!("2026-09-{d:02}")).collect();
        for size in [1, 4, 50] {
            let root = scratch(&format!("rate-{size}")).join("Databento");
            for (i, day) in days.iter().enumerate() {
                let held = big_day(day, 1, 280);
                let id = format!("held-{i}");
                import(
                    &[stage(&root, &id, &[(&format!("{day}.dbn.zst"), held)])],
                    &[],
                    &id,
                    "2026-10-19",
                )
                .unwrap();
            }
            let mut jobs = Vec::new();
            let mut bytes = 0;
            for (i, day) in days.iter().enumerate() {
                for j in 0..5 {
                    let b = big_day(day, 1_000 * (j + 1), 280);
                    bytes += b.len();
                    let id = format!("job-{i}-{j}");
                    jobs.push(Filing {
                        parts: vec![stage(&root, &id, &[(&format!("{day}.dbn.zst"), b)])],
                        elsewhere: Vec::new(),
                        id,
                        today: "2026-10-19".into(),
                        parent: None,
                    });
                }
            }
            let t = std::time::Instant::now();
            let mut jobs = jobs.into_iter().peekable();
            while jobs.peek().is_some() {
                let g: Vec<Filing> = jobs.by_ref().take(size).collect();
                assert!(import_group(&g).iter().all(Result::is_ok));
            }
            let secs = t.elapsed().as_secs_f64();
            eprintln!(
                "groups of {size}: 50 jobs, {:.1} MB each, in {secs:.1} s: {:.1} jobs a minute",
                bytes as f64 / 50e6,
                50.0 * 60.0 / secs
            );
            let _ = fs::remove_dir_all(root.parent().unwrap());
        }
    }
}
