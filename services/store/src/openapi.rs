//! This service's OpenAPI document, for the reads `tt` and the jobs service
//! use (decision 0021, section 5). Queries are the handlers' own types; the
//! answers, built as JSON values, are described by the types below.
#![allow(dead_code)]

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde_json::Value;
use tinytrader_openapi::Spec;

use crate::catalog::{Schema, Thresholds};
use crate::{CoverageQuery, Fresh, ListFiles};

#[derive(JsonSchema)]
struct Catalog {
    /// Seconds since the epoch: when the scan was made, and last updated in place.
    generated_at: f64,
    updated_at: Option<f64>,
    thresholds: Thresholds,
    roots: Vec<Root>,
    missing_volumes: Vec<String>,
    /// Drives found that could be approved.
    candidates: Vec<Value>,
    discover: Option<String>,
}

/// One volume.
#[derive(JsonSchema)]
struct Root {
    name: String,
    path: String,
    present: bool,
    writable: Option<bool>,
    volume: Option<Space>,
    datasets: Vec<Dataset>,
    dataset_bytes: Option<u64>,
    other: Option<Vec<Value>>,
    other_bytes: Option<u64>,
    unrecognised: Vec<String>,
    errors: Vec<String>,
    pending_files: Vec<String>,
    pending_merges: Option<Vec<Value>>,
    ignored: Option<Vec<Value>>,
    /// For a volume that isn't there: what it held when last read in full.
    held: Option<Value>,
    /// For a volume in the content-addressed format (decision 0035): its
    /// format, its id, and how many names, pending objects and conflicts
    /// its index holds.
    cas: Option<Cas>,
}

/// A content-addressed volume's index, in brief.
#[derive(JsonSchema)]
struct Cas {
    format: String,
    volume: String,
    names: u64,
    pending: u64,
    conflicts: Vec<String>,
}

#[derive(JsonSchema)]
struct Space {
    total_bytes: Option<u64>,
    available_bytes: Option<u64>,
    used_fraction: Option<f64>,
    status: SpaceStatus,
    reason: Option<String>,
}

#[derive(JsonSchema)]
#[serde(rename_all = "lowercase")]
enum SpaceStatus {
    Ok,
    Warning,
    Critical,
    Floor,
    Unknown,
}

#[derive(JsonSchema)]
struct Dataset {
    /// `null` outside every provider's folder.
    provider: Option<String>,
    id: String,
    symbol: Option<String>,
    filed: bool,
    /// Held only in DBN files outside every provider's folder and every
    /// layout: counted, but not a dataset the store keeps.
    stray: bool,
    sources: Vec<String>,
    schemas: BTreeMap<String, Schema>,
}

#[derive(JsonSchema)]
struct Coverage {
    days: Vec<Day>,
    missing_volumes: Vec<String>,
    scanned_at: f64,
    updated_at: Option<f64>,
}

/// One day held, on one or more volumes.
#[derive(JsonSchema)]
struct Day {
    day: String,
    volume: String,
    volumes: Vec<String>,
    kind: DayKind,
    filed: bool,
    size: Option<u64>,
    conflict: bool,
    condition: Option<Value>,
    records: Option<u64>,
    sha256: Option<String>,
    coverage: Option<Held>,
    pending: Option<bool>,
    /// The files that hold the day, by logical name, as `/files/{volume}/{path}`
    /// serves them.
    files: Vec<String>,
}

#[derive(JsonSchema)]
#[serde(rename_all = "lowercase")]
enum DayKind {
    Complete,
    Partial,
}

/// The contracts and hours a partial day holds.
#[derive(JsonSchema)]
struct Held {
    kind: String,
    stype: String,
    pieces: Option<Vec<Piece>>,
    symbols: Vec<String>,
    /// Nanoseconds since the epoch, each end.
    window: Vec<u64>,
}

#[derive(JsonSchema)]
struct Piece {
    symbols: Vec<String>,
    window: Vec<u64>,
}

#[derive(JsonSchema)]
struct Files {
    files: Vec<File>,
    truncated: bool,
    /// Seconds since the epoch: the scan listed from, and when a filing last
    /// updated it.
    scanned_at: f64,
    updated_at: f64,
}

#[derive(JsonSchema)]
struct File {
    /// `<volume>/<path>`.
    name: String,
    volume: String,
    path: String,
    bytes: u64,
}

#[test]
fn openapi() {
    let mut s = Spec::default();
    let (q, a) = (s.query::<Fresh>(), s.schema::<Catalog>());
    s.op(
        ("get", "/catalog"),
        "What each volume holds",
        q,
        None,
        &[("200", "the catalog", a)],
    );
    let (q, a) = (s.query::<CoverageQuery>(), s.schema::<Coverage>());
    s.op(
        ("get", "/coverage"),
        "Each day held",
        q,
        None,
        &[("200", "the days", a)],
    );
    let (q, a) = (s.query::<ListFiles>(), s.schema::<Files>());
    s.op(
        ("get", "/files"),
        "DBN files by logical name",
        q,
        None,
        &[("200", "the files", a)],
    );
    let doc = s.document("store", "See docs/api/store.md.");
    tinytrader_openapi::check("store", &doc);
}
