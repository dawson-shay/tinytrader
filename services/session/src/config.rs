//! A session's configuration (decision 0022, section 3): its days, its sources and
//! its simulated venues, read from TOML.
//!
//! ```toml
//! days = ["2026-10-26", "2026-10-27"]
//! strategy_latency_ms = 0        # the strategy's declared latency
//! ready_timeout_s = 60           # how long a step waits for the strategy's ready
//! seed = 0                       # each venue's seed derives from it and the venue's name
//! calendar = "calendar.json"     # the market calendar for the venues (optional)
//!
//! [strategy]                     # optional: the session starts the strategy itself
//! command = ["python3", "-m", "my_strategy"]
//! sandbox = "macos"              # or "off" (the default)
//! allow_read = ["/path/to/code", "/path/to/venv"]
//!
//! [[sources]]
//! name = "opra"
//! holdings = "store:http://127.0.0.1:9120"   # or "job:DIR", "folder:DIR", or a list:
//!                                            # each day from the first that holds it
//! provider = "databento"
//! dataset = "OPRA.PILLAR"
//! symbol = "SPY.OPT"
//! schema = "cbbo-1m"
//! latency_offset_ns = 0          # added to ts_event where F_BAD_TS_RECV is set
//! expiries_within_days = 30      # options expiring later are left out (optional)
//!
//! [[venues]]
//! name = "options"
//! quotes = ["opra"]              # the sources whose records it fills against
//! underlyings = ["SPY"]
//! equities = []
//! futures = []                   # futures roots allowed: "ES", "MES", ...
//! max_future_qty = 1             # contracts per opening futures order
//! max_notional = 2500            # dollars per order
//! [venues.sim]                   # the simulator's profile (order-gateway.md §10)
//! latency_ms = 5
//! fee_per_contract = "0.65"
//! ```
use serde::Deserialize;
use std::path::PathBuf;
use tinytrader_producer::{Folder, Holdings, Layered, Query, StoreApi};

/// A whole session.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The days, `YYYY-MM-DD`, in order.
    pub days: Vec<String>,
    /// The strategy's declared latency: its orders take effect this much after the
    /// step they answer.
    #[serde(default)]
    pub strategy_latency_ms: u64,
    /// How long a step waits for the strategy's `ready`, and the strategy for its
    /// first connection.
    #[serde(default = "ready_timeout")]
    pub ready_timeout_s: u64,
    /// A market calendar for the venues (`{"days": [{"date", "open", "close"}]}`,
    /// times in New York, as the deployment's market calendar serves it), given to
    /// every venue whose profile has none: holidays have no session, early closes
    /// close early. Without it, venues assume every weekday from 09:30 to 16:00.
    #[serde(default)]
    pub calendar: Option<PathBuf>,
    /// The seed of any declared randomness (decision 0022, section 6): each venue's
    /// profile gets one derived from it and the venue's name, unless it sets its own.
    #[serde(default)]
    pub seed: u64,
    /// The sources, in rank order: ties in time go to the earlier one.
    pub sources: Vec<Source>,
    /// The simulated venues, one gateway each.
    pub venues: Vec<Venue>,
    /// The strategy, when the session starts it itself.
    #[serde(default)]
    pub strategy: Option<StrategyProcess>,
}

/// A strategy the session starts, optionally sandboxed (decision 0022, section 1).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyProcess {
    /// The command and its arguments; `TT_SESSION` is set to the control address.
    pub command: Vec<String>,
    /// Its working folder.
    #[serde(default)]
    pub cwd: Option<std::path::PathBuf>,
    /// More environment.
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
    /// `off` (the default) or `macos`: under `sandbox-exec`, reading only the
    /// system's folders and `allow_read`, writing only its own scratch folder, and
    /// connecting only to the session's own addresses.
    #[serde(default)]
    pub sandbox: Sandbox,
    /// Folders a sandboxed strategy may read: its code, its interpreter. None may
    /// be or contain `/`, the user's home, held data or a `deny_read` folder.
    #[serde(default)]
    pub allow_read: Vec<std::path::PathBuf>,
    /// Folders a sandboxed strategy may never read, even inside `allow_read`: the
    /// deployment's caches and copies of held data.
    #[serde(default)]
    pub deny_read: Vec<std::path::PathBuf>,
}

/// How a started strategy is confined.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Sandbox {
    /// An ordinary process of the user running the session.
    #[default]
    Off,
    /// macOS `sandbox-exec` with a profile the session writes.
    Macos,
}

fn ready_timeout() -> u64 {
    60
}

/// One source: one dataset, symbol and schema from one holdings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    /// Its name, which venues refer to.
    pub name: String,
    /// `store:URL`, `job:DIR` (a databento-jobs cache job) or `folder:DIR` (the
    /// store's layout); or a list of them, each day read from the first that holds
    /// it.
    pub holdings: OneOrMany,
    /// The store's provider folder: `databento`, `alpaca`.
    pub provider: String,
    /// `OPRA.PILLAR`, `ALPACA.SIP`, …
    pub dataset: String,
    /// The symbol level: `SPY.OPT`, `SPY`.
    pub symbol: String,
    /// `cbbo-1m`, `cmbp-1`, …
    pub schema: String,
    /// Added to `ts_event` for records flagged `F_BAD_TS_RECV`.
    #[serde(default)]
    pub latency_offset_ns: u64,
    /// Options expiring more than this many calendar days (weekends included) after
    /// each day, or before it, are left out, mappings and records: a subscription
    /// to the near expiries.
    #[serde(default)]
    pub expiries_within_days: Option<u32>,
}

/// One holdings, or several in order.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum OneOrMany {
    /// One.
    One(String),
    /// Several, asked in order.
    Many(Vec<String>),
}

/// One simulated venue: an account at a broker, as its own gateway.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Venue {
    /// Its name, as the strategy addresses it.
    pub name: String,
    /// The sources it fills against.
    pub quotes: Vec<String>,
    /// Option underlyings allowed.
    #[serde(default)]
    pub underlyings: Vec<String>,
    /// Stocks allowed.
    #[serde(default)]
    pub equities: Vec<String>,
    /// The per-order notional cap, dollars.
    #[serde(default)]
    pub max_notional: Option<i64>,
    /// Open orders allowed at once.
    #[serde(default)]
    pub max_open: Option<usize>,
    /// Short stock sales allowed.
    #[serde(default)]
    pub allow_short: bool,
    /// Futures roots allowed (`ES`, `MES`, ...).
    #[serde(default)]
    pub futures: Vec<String>,
    /// The most futures contracts one opening order may trade (1 by default).
    #[serde(default)]
    pub max_future_qty: Option<u32>,
    /// The simulator's profile: the `--sim-script` keys (latency, fees, size cap,
    /// calendar, account).
    #[serde(default)]
    pub sim: toml::Table,
}

impl Config {
    /// Parses and checks a configuration.
    ///
    /// # Errors
    /// Bad TOML, no days or sources or venues, duplicate names, or a venue quoting
    /// a source that isn't there.
    pub fn parse(text: &str) -> Result<Self, String> {
        let c: Self = toml::from_str(text).map_err(|e| e.to_string())?;
        if c.days.is_empty() || c.sources.is_empty() || c.venues.is_empty() {
            return Err("a session needs days, sources and venues".into());
        }
        let parsed: Option<Vec<u64>> = c.days.iter().map(|d| crate::run::day_ns(d)).collect();
        match parsed {
            Some(p)
                if p.windows(2).all(|w| w[0] < w[1]) && c.days.iter().all(|d| d.len() == 10) => {}
            _ => return Err("days must be YYYY-MM-DD, in order, each once".into()),
        }
        let mut names: Vec<&str> = c.sources.iter().map(|s| s.name.as_str()).collect();
        names.sort_unstable();
        if names.windows(2).any(|w| w[0] == w[1]) {
            return Err("source names must differ".into());
        }
        let mut venues: Vec<&str> = c.venues.iter().map(|v| v.name.as_str()).collect();
        venues.sort_unstable();
        if venues.windows(2).any(|w| w[0] == w[1]) {
            return Err("venue names must differ".into());
        }
        if let Some(st) = &c.strategy {
            if st.command.is_empty() {
                return Err("strategy.command is empty".into());
            }
            if st.sandbox == Sandbox::Macos && !cfg!(target_os = "macos") {
                return Err("strategy.sandbox = \"macos\" needs macOS".into());
            }
            if st
                .allow_read
                .iter()
                .chain(&st.deny_read)
                .any(|p| !p.is_absolute())
            {
                return Err("strategy.allow_read and deny_read take absolute paths".into());
            }
        }
        for v in &c.venues {
            if v.name.is_empty()
                || !v
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                return Err(format!("venue name {:?}: letters, digits, - and _", v.name));
            }
            if v.sim.contains_key("strategy_latency_ms") {
                return Err(format!(
                    "venue {}: the strategy's latency is the session's strategy_latency_ms",
                    v.name
                ));
            }
            if let Some(bad) = v
                .futures
                .iter()
                .find(|r| order_gateway::future_multiplier(r).is_none())
            {
                return Err(format!(
                    "venue {}: {bad:?} isn't a known futures root",
                    v.name
                ));
            }
            if v.max_future_qty == Some(0) {
                return Err(format!("venue {}: max_future_qty is at least 1", v.name));
            }
            for q in &v.quotes {
                if !c.sources.iter().any(|s| &s.name == q) {
                    return Err(format!("venue {} quotes {q}, which isn't a source", v.name));
                }
            }
        }
        Ok(c)
    }
}

impl Source {
    /// The query this source asks its holdings.
    #[must_use]
    pub fn query(&self) -> Query {
        Query {
            provider: self.provider.clone(),
            dataset: self.dataset.clone(),
            symbol: self.symbol.clone(),
            schema: self.schema.clone(),
        }
    }

    /// Its holdings.
    ///
    /// # Errors
    /// An unknown kind, or a cache job that can't be read.
    pub fn holdings(&self) -> Result<Box<dyn Holdings>, String> {
        match &self.holdings {
            OneOrMany::One(h) => self.one(h),
            OneOrMany::Many(hs) if !hs.is_empty() => Ok(Box::new(Layered::new(
                hs.iter().map(|h| self.one(h)).collect::<Result<_, _>>()?,
            ))),
            OneOrMany::Many(_) => Err(format!("source {}: no holdings", self.name)),
        }
    }

    fn one(&self, holdings: &str) -> Result<Box<dyn Holdings>, String> {
        let (kind, at) = holdings.split_once(':').ok_or_else(|| {
            format!(
                "source {}: holdings is store:URL, job:DIR or folder:DIR",
                self.name
            )
        })?;
        Ok(match kind {
            "store" => Box::new(StoreApi::new(at)),
            "job" => Box::new(Folder::job(&PathBuf::from(at))?),
            "folder" => Box::new(Folder::layout(&PathBuf::from(at))),
            _ => return Err(format!("source {}: unknown holdings {kind}", self.name)),
        })
    }
}

impl Venue {
    /// The simulator's script: its profile, with the session's strategy latency.
    ///
    /// # Errors
    /// A profile that can't be written as JSON.
    pub fn script(
        &self,
        strategy_latency_ms: u64,
        calendar: Option<&serde_json::Value>,
        seed: u64,
    ) -> Result<String, String> {
        let mut sim = self.sim.clone();
        let ms = i64::try_from(strategy_latency_ms).map_err(|e| e.to_string())?;
        sim.insert("strategy_latency_ms".into(), toml::Value::Integer(ms));
        let mut json = serde_json::to_value(&sim).map_err(|e| e.to_string())?;
        if let Some(o) = json.as_object_mut() {
            if let Some(days) = calendar {
                o.entry("calendar").or_insert_with(|| days.clone());
            }
            // The venue's own seed, from the run's: each venue draws its own stream.
            o.entry("seed").or_insert_with(|| serde_json::json!(seed));
        }
        serde_json::to_string(&json).map_err(|e| e.to_string())
    }
}
