//! `tt`: tinytrader from a terminal (decisions 0021 and 0023). A front end
//! only: every command is a call to the console at `TT_CONSOLE` (its address
//! under the authenticating proxy, such as `https://<console host>`), which
//! adds its write gate and the login, and passes reads and prices to the jobs
//! service. stdout carries the answer: a table at a terminal, else JSON
//! (`--format` chooses); progress and prompts go to stderr. Exit: 0 done,
//! 1 failed, 2 usage, 3 refused (a 403, a paid purchase without a terminal,
//! or an expired quote not priced again), 128+n on signal n.

use std::io::{BufRead, IsTerminal, Write};
use std::sync::Mutex;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use serde_json::{Value, json};

#[derive(Parser)]
#[command(name = "tt", about = "tinytrader from a terminal, through TT_CONSOLE")]
struct Cli {
    /// How the answer is written on stdout (default: a table at a terminal, else JSON).
    #[arg(long, value_enum, global = true)]
    format: Option<Format>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, PartialEq, ValueEnum)]
enum Format {
    Json,
    Table,
}

#[derive(Subcommand)]
enum Command {
    /// Price a purchase, ask before anything is charged, buy it and follow its submission.
    Fetch {
        #[command(subcommand)]
        source: Source,
    },
    /// The waiting and recent quotes.
    Quotes,
    /// Show a waiting quote, ask, approve it and follow its submission.
    Approve { quote: String },
    /// Ask, then drop a waiting quote.
    Decline { quote: String },
    /// The purchases' jobs and their filing outcomes; with --follow, until
    /// those named (job IDs, or quote IDs for their jobs) are filed or failed.
    Jobs {
        #[arg(long)]
        follow: bool,
        ids: Vec<String>,
    },
    /// Any console endpoint, through the same guards: `GET|POST /api/…`, a
    /// POST's JSON body read from stdin. The answer is printed as JSON. It
    /// never approves, declines or submits a purchase (exit 3): `tt approve`
    /// and `tt decline` ask a person.
    Api {
        #[arg(value_parser = ["GET", "POST"])]
        method: String,
        path: String,
    },
    /// The store's reads, from the store at `TT_STORE` (default
    /// http://localhost:9120), not the console.
    Store {
        #[command(subcommand)]
        command: StoreCommand,
    },
}

#[derive(Subcommand)]
enum StoreCommand {
    /// The files the store serves, by name (`<volume>/<path>`), matching a
    /// pattern (`*` any run of characters, slashes included; `?` one).
    Ls {
        #[arg(default_value = "*")]
        pattern: String,
        #[arg(long)]
        limit: Option<u64>,
    },
}

#[derive(Subcommand)]
enum Source {
    /// Databento's batch API, through databento-jobs.
    Databento(Fetch),
}

/// `--symbols` is one symbol (a parent such as `SPY.OPT`, or a raw symbol);
/// `--hours` is `HH:MM-HH:MM` New York time; `--contracts` a JSON file of
/// `{"YYYY-MM-DD": ["SPY   221021C00370000", …]}`; `--write` only `store`.
#[derive(clap::Args)]
struct Fetch {
    #[arg(long)]
    dataset: String,
    #[arg(long)]
    schema: String,
    #[arg(long)]
    symbols: String,
    #[arg(long)]
    start: String,
    #[arg(long)]
    end: String,
    #[arg(long)]
    hours: Option<String>,
    #[arg(long)]
    contracts: Option<std::path::PathBuf>,
    #[arg(long, default_value = "store", value_parser = ["store"])]
    write: String,
}

/// What a person at the terminal sees and says (stderr, never stdout).
trait Term {
    fn tty(&self) -> bool;
    fn say(&mut self, line: &str);
    fn ask(&mut self, prompt: &str) -> Option<String>;
    /// A line read without echo.
    fn secret(&mut self, prompt: &str) -> Option<String>;
    /// All of stdin when it isn't a terminal (a request's body), else nothing.
    fn input(&mut self) -> Result<String, String>;
}

/// The terminal's settings while echo is off, to restore (also on a signal).
static ECHO: Mutex<Option<rustix::termios::Termios>> = Mutex::new(None);

fn echo(on: bool) {
    use rustix::termios::{LocalModes, OptionalActions::Now, tcgetattr, tcsetattr};
    let stdin = std::io::stdin();
    let mut saved = ECHO
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if on {
        saved.take().map(|t| tcsetattr(&stdin, Now, &t));
    } else if let Ok(t) = tcgetattr(&stdin) {
        let mut quiet = t.clone();
        quiet.local_modes.remove(LocalModes::ECHO);
        *saved = tcsetattr(&stdin, Now, &quiet).ok().map(|()| t);
    }
}

/// The quote being approved or submitted, to say on a signal what becomes of it.
static SUBMITTING: Mutex<Option<String>> = Mutex::new(None);

/// What a signal during a purchase leaves: the submission goes on without us.
fn interrupted(id: &str) -> String {
    format!(
        "tt: stopped waiting, but a submission of {id} that the console has begun runs to its end there; `tt jobs --follow {id}` shows how it went"
    )
}

struct Real;

impl Term for Real {
    fn tty(&self) -> bool {
        std::io::stdin().is_terminal()
    }
    fn say(&mut self, line: &str) {
        eprintln!("{line}");
    }
    fn ask(&mut self, prompt: &str) -> Option<String> {
        eprint!("{prompt} ");
        let mut line = String::new();
        let n = tokio::task::block_in_place(|| std::io::stdin().lock().read_line(&mut line));
        let got = n.ok().filter(|n| *n > 0).map(|_| line.trim().to_owned());
        if got.is_none() {
            eprintln!(); // end of input: the prompt's line ends here
        }
        got
    }
    fn input(&mut self) -> Result<String, String> {
        let mut body = String::new();
        if !self.tty() {
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut body)
                .map_err(|e| e.to_string())?;
        }
        Ok(body)
    }
    fn secret(&mut self, prompt: &str) -> Option<String> {
        echo(false);
        let line = self.ask(prompt);
        echo(true);
        eprintln!();
        line
    }
}

/// The console, and the order token once a person has given it (this run only).
struct Console {
    base: String,
    http: reqwest::Client,
    token: Mutex<Option<String>>,
    poll: Duration,
}

/// An answer: its status, its body, and whether the write gate refused it.
struct Answer {
    code: u16,
    body: Value,
    gate: bool,
}

/// A failure and its exit code: 1 failed, 3 refused.
struct Fail(u8, String);

impl From<String> for Fail {
    fn from(why: String) -> Self {
        Fail(1, why)
    }
}

fn no<T>(why: String) -> Result<T, Fail> {
    Err(Fail(1, why))
}

/// An answer that isn't a success: refused (3) when it's a 403.
fn failed(a: &Answer) -> Fail {
    Fail(if a.code == 403 { 3 } else { 1 }, why(a))
}

fn why(a: &Answer) -> String {
    let code = a.code;
    a.body["error"]
        .as_str()
        .map_or_else(|| format!("the console answered {code}"), str::to_owned)
}

const PROXY_ONLY: &str = "approvals need the console's proxy address, where your tailnet login is attached; this machine may be tagged";

const UNSEALED: &str = "TT_CONSOLE is plain http to another machine, so the order token isn't sent there: use the console's https address, or http on this machine (127.0.0.1, ::1, localhost)";

/// Whether the order token may go to `base`: over https, or to this machine.
fn private(base: &str) -> bool {
    let Ok(u) = reqwest::Url::parse(base) else {
        return false;
    };
    let host = u.host_str().unwrap_or_default();
    let ip = host.trim_matches(['[', ']']).parse::<std::net::IpAddr>();
    match u.scheme() {
        "https" => !host.is_empty(),
        "http" => host.eq_ignore_ascii_case("localhost") || ip.is_ok_and(|ip| ip.is_loopback()),
        _ => false,
    }
}

impl Console {
    async fn send(&self, post: Option<&Value>, path: &str, wait: u64) -> Result<Answer, Fail> {
        let url = format!("{}{path}", self.base);
        let mut r = post.map_or_else(|| self.http.get(&url), |b| self.http.post(&url).json(b));
        if let Some(t) = self.token.lock().unwrap().as_deref()
            && private(&self.base)
        {
            r = r.header("X-Order-Token", t);
        }
        let r = r.timeout(Duration::from_secs(wait)).send().await;
        let r = r.map_err(|e| format!("the console at {}: {}", self.base, e.without_url()))?;
        let code = r.status().as_u16();
        let gate = r
            .headers()
            .get("x-write-gate")
            .is_some_and(|v| v == "refused");
        let body = r.json().await.unwrap_or(Value::Null);
        Ok(Answer { code, body, gate })
    }

    async fn get(&self, path: &str) -> Result<Value, Fail> {
        let a = self.send(None, path, 60).await?;
        if a.code == 200 {
            Ok(a.body)
        } else {
            Err(failed(&a))
        }
    }

    /// A write through the gate. Its 401 asks for the order token, once; its
    /// 403 is never retried. With `follow`, the quote's submission progress
    /// is shown, every 50 requests, while the answer is awaited.
    async fn write(
        &self,
        term: &mut dyn Term,
        path: &str,
        body: &Value,
        follow: Option<&str>,
    ) -> Result<Answer, Fail> {
        loop {
            let call = self.send(Some(body), path, 4 * 3600);
            tokio::pin!(call);
            let mut shown = None;
            let a = loop {
                tokio::select! {
                    a = &mut call => break a?,
                    () = tokio::time::sleep(self.poll), if follow.is_some() => {
                        let Ok(q) = self.get(&format!("/api/jobs/quotes/{}", follow.unwrap_or_default())).await else { continue };
                        let (n, of) = (q["submitted"].as_u64().unwrap_or(0), q["of"].as_u64().unwrap_or(0));
                        if q["state"] == "submitting" && shown.is_none_or(|s| n >= s + 50 || n == of) {
                            term.say(&format!("submitted {n} of {of} requests"));
                            shown = Some(n);
                        }
                    }
                }
            };
            if a.gate && a.code == 401 && self.token.lock().unwrap().is_none() && term.tty() {
                if !private(&self.base) {
                    return Err(Fail(3, format!("{}, but {UNSEALED}", why(&a))));
                }
                let t = term
                    .secret("The console's order token:")
                    .unwrap_or_default();
                if t.is_empty() {
                    return no("no order token given".into());
                }
                *self.token.lock().unwrap() = Some(t);
            } else if a.gate && a.code == 403 {
                return Err(Fail(3, format!("{}: {PROXY_ONLY}", why(&a))));
            } else {
                return Ok(a);
            }
        }
    }

    /// Price a request and follow it until it's ready.
    async fn price(&self, term: &mut dyn Term, ask: &Value) -> Result<Value, Fail> {
        let a = self.write(term, "/api/jobs/price", ask, None).await?;
        if a.code != 200 && a.code != 202 {
            return Err(failed(&a));
        }
        let (mut q, mut shown) = (a.body, None);
        while q["state"] == "pricing" {
            let now = (
                q["priced"].as_u64().unwrap_or(0),
                q["of"].as_u64().unwrap_or(0),
            );
            if shown.replace(now) != Some(now) {
                term.say(&format!("pricing: {} of {} requests", now.0, now.1));
            }
            tokio::time::sleep(self.poll).await;
            q = self
                .get(&format!("/api/jobs/quotes/{}", text(&q["quote"])))
                .await?;
        }
        match q["state"].as_str() {
            Some("ready") => Ok(q),
            _ => no(format!("pricing failed: {}", text(&q["error"]))),
        }
    }
}

fn text(v: &Value) -> String {
    v.as_str().map_or_else(|| v.to_string(), str::to_owned)
}

fn now() -> u64 {
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    t.map_or(0, |d| d.as_secs())
}

/// When a ready quote stops being good: `expires_at`, or from `valid_seconds`.
fn expires(q: &Value) -> u64 {
    let left = q["valid_seconds"].as_u64().unwrap_or(0);
    q["expires_at"].as_u64().unwrap_or_else(|| now() + left)
}

/// In cents, marked ≈ when that isn't the exact value (which is what's confirmed).
fn dollars(v: &Value) -> String {
    let exact = |n: f64| {
        if (n * 100.0).round() / 100.0 == n {
            ""
        } else {
            "≈"
        }
    };
    v.as_f64()
        .map_or("not a price".into(), |n| format!("{}${n:.2}", exact(n)))
}

fn what(q: &Value) -> String {
    let (s, e) = (text(&q["start"]), text(&q["end"]));
    let days = if s == e { s } else { format!("{s} → {e}") };
    let [d, sc, sy] = ["dataset", "schema", "symbol"].map(|k| text(&q[k]));
    format!("{d} {sc} {sy}, {days}")
}

fn until(q: &Value) -> String {
    tinytrader_schedule::ny_date_time(expires(q) as i64).1 + " ET"
}

fn left(q: &Value) -> String {
    match expires(q).checked_sub(now()).filter(|s| *s > 0) {
        Some(s) => format!("{} min left (expires {})", s / 60, until(q)),
        None => "expired".into(),
    }
}

/// The quote, in summary: counts, not every note.
fn show(term: &mut dyn Term, q: &Value) {
    let n = q["requests"].as_array().map_or(0, Vec::len);
    term.say(&format!("{} ({})", what(q), text(&q["quote"])));
    let mut line = format!("{} for {n} requests", dollars(&q["cost_usd"]));
    if let (Some(c), Some(d)) = (q["contracts"].as_u64(), q["contract_days"].as_u64()) {
        line += &format!(", {c} contracts over {d} days");
    }
    term.say(&format!("{line}, {}", left(q)));
    let notes = q["notes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str);
    let (days, rest): (Vec<&str>, Vec<&str>) =
        notes.partition(|n| n.starts_with("Databento marks"));
    days.iter().for_each(|d| term.say(d));
    if let Some(first) = rest.first() {
        let n = rest.len();
        term.say(&format!(
            "{n} notes on what was skipped (first: {first}); --format json lists them all"
        ));
    }
}

fn yes(term: &mut dyn Term, prompt: &str) -> bool {
    let a = term.ask(prompt).unwrap_or_default().to_lowercase();
    a == "y" || a == "yes"
}

fn cells(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_owned()).collect()
}

/// The store's address: `TT_STORE`, by default this machine's.
fn store_base() -> String {
    std::env::var("TT_STORE").unwrap_or_else(|_| "http://localhost:9120".into())
}

/// `tt store …`: a read of the store's own API at `TT_STORE`. Exit 0, or 1
/// when the store can't answer (with why on stderr).
async fn store(base: &str, command: &StoreCommand, f: Format, out: &mut dyn Write) -> u8 {
    let base = base.trim_end_matches('/');
    let StoreCommand::Ls { pattern, limit } = command;
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .user_agent("tinytrader-tt")
        .build();
    let Ok(http) = http else {
        eprintln!("tt: no HTTP client");
        return 1;
    };
    let mut q = vec![("pattern", pattern.clone())];
    if let Some(l) = limit {
        q.push(("limit", l.to_string()));
    }
    let r = http
        .get(format!("{base}/files"))
        .query(&q)
        .timeout(Duration::from_secs(120))
        .send()
        .await;
    let r = match r {
        Ok(r) => r,
        Err(e) => {
            eprintln!("tt: the store at {base}: {}", e.without_url());
            return 1;
        }
    };
    let code = r.status().as_u16();
    let v: Value = r.json().await.unwrap_or(Value::Null);
    if code != 200 {
        eprintln!(
            "tt: the store answered {code}: {}",
            v["error"].as_str().unwrap_or("no reason given")
        );
        return 1;
    }
    let rows: Vec<Vec<String>> = std::iter::once(cells(&["NAME", "BYTES"]))
        .chain(v["files"].as_array().into_iter().flatten().map(|f| {
            vec![
                f["name"].as_str().unwrap_or_default().to_owned(),
                f["bytes"]
                    .as_u64()
                    .map_or_else(String::new, |b| b.to_string()),
            ]
        }))
        .collect();
    emit(out, f, &v, &rows);
    if v["truncated"] == true {
        eprintln!(
            "tt: only the first {} listed; give --limit or a narrower pattern",
            rows.len() - 1
        );
    }
    0
}

/// The answer on stdout: the JSON, or the rows as a table.
fn emit(out: &mut dyn Write, format: Format, v: &Value, rows: &[Vec<String>]) {
    let mut width = vec![0; rows.iter().map(Vec::len).max().unwrap_or(0)];
    for (i, c) in rows.iter().flat_map(|r| r.iter().enumerate()) {
        width[i] = width[i].max(c.chars().count());
    }
    let line = |r: &Vec<String>| {
        let padded: Vec<String> = r
            .iter()
            .zip(&width)
            .map(|(c, w)| format!("{c:<w$}"))
            .collect();
        padded.join("  ").trim_end().to_owned() + "\n"
    };
    let s = match format {
        Format::Json => format!("{v}\n"),
        Format::Table => rows.iter().map(line).collect(),
    };
    let _ = out.write_all(s.as_bytes());
}

/// A person's yes, then the purchase; an expired or changed price is offered
/// again, priced afresh and asked again, never bought unasked. `ask` is the
/// request to price again, when it's known.
async fn purchase(
    c: &Console,
    term: &mut dyn Term,
    out: &mut dyn Write,
    f: Format,
    mut q: Value,
    ask: Option<Value>,
) -> Result<u8, Fail> {
    loop {
        show(term, &q);
        let id = text(&q["quote"]);
        let n = q["requests"].as_array().map_or(0, Vec::len);
        if n == 0 {
            term.say("Nothing to buy: everything asked for is already had.");
            emit(out, f, &q, &[cells(&[&id, "nothing to buy"])]);
            return Ok(0);
        }
        let paid = q["needs_approval"] != false || q["cost_usd"].as_f64() != Some(0.0);
        let stale = if expires(&q) <= now() {
            "that price has expired".to_owned()
        } else {
            let (path, body) = if paid {
                if !q["cost_usd"].is_number() {
                    return no("its price isn't a number of dollars, so it can't be bought".into());
                }
                if !term.tty() {
                    let row = [&*id, &what(&q), &dollars(&q["cost_usd"]), &until(&q)];
                    let head = cells(&["QUOTE", "WHAT", "COST", "EXPIRES"]);
                    emit(out, f, &q, &[head, cells(&row)]);
                    return Err(Fail(
                        3,
                        format!(
                            "a paid purchase needs a person at a terminal: run `tt approve {id}` there, or approve it in the console"
                        ),
                    ));
                }
                let (what, usd) = (what(&q), dollars(&q["cost_usd"]));
                if !yes(
                    term,
                    &format!(
                        "Buy {what}, {n} requests, for {usd}? (expires {}) [y/N]",
                        until(&q)
                    ),
                ) {
                    return no(format!(
                        "not bought; it waits until it expires, and `tt decline {id}` drops it"
                    ));
                }
                // The quoted value itself, exactly as given: never re-typed or rounded.
                let body = json!({ "confirm_usd": q["cost_usd"], "again": q["again"] });
                (format!("/api/job-quotes/{id}/approve"), body)
            } else {
                ("/api/jobs/submit".to_owned(), json!({ "quote": id }))
            };
            *SUBMITTING.lock().unwrap() = Some(id.clone());
            let a = c.write(term, &path, &body, Some(&id)).await;
            *SUBMITTING.lock().unwrap() = None;
            let a = a?;
            if a.code == 200 {
                let mut v = a.body;
                v["quote"] = json!(id);
                term.say(&format!(
                    "Submitted {n} requests; `tt jobs --follow {id}` follows their filing."
                ));
                let rows: Vec<_> = v["submitted"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|r| {
                        let days = format!("{} → {}", text(&r["first_day"]), text(&r["last_day"]));
                        vec![text(&r["job"]), days, dollars(&r["cost_usd"])]
                    })
                    .collect();
                emit(out, f, &v, &rows);
                return Ok(0);
            }
            let e = why(&a);
            if a.code != 409 || !e.contains("again") {
                if let Some(sent) = a.body["submitted"].as_array() {
                    term.say(&format!("It stopped after {} of {n} requests; price the same request again to buy the rest.", sent.len()));
                }
                return Err(Fail(failed(&a).0, format!("not bought: {e}")));
            }
            e
        };
        term.say(&format!("Not bought: {stale}."));
        let Some(ask) = ask.as_ref() else {
            return no("price it again with `tt fetch`".into());
        };
        if !term.tty() || !yes(term, "Price it again (free)? [y/N]") {
            return Ok(3);
        }
        q = c.price(term, ask).await?;
    }
}

/// The request a quote was priced from, when the quote says all of it (not
/// for contract lists or rules, whose contracts it only counts).
fn asked_again(q: &Value) -> Option<Value> {
    if q["contracts"].is_number() || q["rule"] == true {
        return None;
    }
    let keys = [
        "dataset", "schema", "symbol", "stype_in", "start", "end", "hours", "again",
    ];
    Some(
        keys.iter()
            .filter(|k| !q[**k].is_null())
            .map(|k| ((*k).to_owned(), q[*k].clone()))
            .collect(),
    )
}

/// A job's filing outcome: `filed`, `failed` or `pending`, and why.
fn outcome(j: &Value) -> (&'static str, String) {
    let (t, held) = (&j["transfer"], j["held"]["how"].as_str());
    match (j["state"].as_str(), t["state"].as_str(), held) {
        (_, Some("filed"), _) | (_, _, Some("filed" | "folder")) => {
            ("filed", text(&j["held"]["volume"]))
        }
        (_, Some("failed"), _) => ("failed", text(&t["error"])),
        (Some("done"), None, None) if j["unfileable"].is_string() => (
            "failed",
            format!("not fileable: {}", text(&j["unfileable"])),
        ),
        (Some("expired"), _, None) => ("failed", "expired at Databento before it was filed".into()),
        (s, t, _) => ("pending", t.or(s).unwrap_or("unknown").to_owned()),
    }
}

async fn jobs(
    c: &Console,
    term: &mut dyn Term,
    out: &mut dyn Write,
    f: Format,
    follow: bool,
    ids: Vec<String>,
) -> Result<u8, Fail> {
    let (mut quotes, mut wanted): (Vec<String>, Vec<String>) =
        ids.into_iter().partition(|i| i.starts_with("q-"));
    let (all, mut failed, mut seen) = (
        !follow && wanted.is_empty() && quotes.is_empty(),
        Vec::new(),
        std::collections::HashMap::new(),
    );
    // A job just submitted may not be listed yet; after this, an ID the
    // list doesn't have is no job.
    let (began, grace) = (std::time::Instant::now(), c.poll * 15);
    loop {
        let mut waiting = Vec::new();
        for id in quotes.drain(..) {
            let q = c.get(&format!("/api/jobs/quotes/{id}")).await?;
            match q["state"].as_str() {
                Some("submitting") => waiting.push(id),
                Some("submitted") => {
                    wanted.extend(q["jobs"].as_array().into_iter().flatten().map(text))
                }
                Some("failed") => {
                    wanted.extend(q["jobs"].as_array().into_iter().flatten().map(text));
                    failed.push(json!({ "quote": id, "outcome": "failed", "detail": q["error"] }));
                }
                _ => {
                    return no(format!(
                        "{id} hasn't been submitted (it's {})",
                        text(&q["state"])
                    ));
                }
            }
        }
        quotes = waiting;
        let jobs = c.get("/api/jobs/jobs").await?["jobs"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        if follow && wanted.is_empty() && quotes.is_empty() && seen.is_empty() {
            // Nothing named: the jobs in flight now.
            let flying = jobs
                .iter()
                .filter(|j| j["follow"] == true && outcome(j).0 == "pending");
            wanted = flying.map(|j| text(&j["id"])).collect();
        }
        let mut rows: Vec<Value> = jobs
            .into_iter()
            .filter(|j| all || wanted.contains(&text(&j["id"])))
            .collect();
        for j in &mut rows {
            let (o, d) = outcome(j);
            (j["outcome"], j["detail"]) = (json!(o), json!(d));
            if follow && seen.insert(text(&j["id"]), o) != Some(o) {
                term.say(&format!("{}: {o} {d}", text(&j["id"])));
            }
        }
        let unknown: Vec<&str> = wanted
            .iter()
            .filter(|w| !rows.iter().any(|j| text(&j["id"]) == **w))
            .map(String::as_str)
            .collect();
        if follow && !unknown.is_empty() && began.elapsed() >= grace {
            let secs = grace.as_secs();
            return no(format!(
                "no such job: {} (not in the jobs list after {secs} s)",
                unknown.join(", ")
            ));
        }
        let pending = rows.iter().any(|j| j["outcome"] == "pending");
        if !follow || (!pending && rows.len() == wanted.len() && quotes.is_empty()) {
            rows.sort_by_key(|j| {
                ["pending", "failed", "filed"]
                    .iter()
                    .position(|o| j["outcome"] == *o)
            });
            let bad = !failed.is_empty() || rows.iter().any(|j| j["outcome"] == "failed");
            rows.extend(failed);
            let mut table = vec![cells(&["JOB", "OUTCOME", "WHAT", "DETAIL"])];
            table.extend(rows.iter().map(|j| {
                let id = if j["id"].is_null() {
                    &j["quote"]
                } else {
                    &j["id"]
                };
                let what = format!("{} {}", text(&j["schema"]), text(&j["symbols"]));
                vec![text(id), text(&j["outcome"]), what, text(&j["detail"])]
            }));
            emit(out, f, &json!({ "jobs": rows }), &table);
            return Ok(u8::from(follow && bad));
        }
        tokio::time::sleep(c.poll * 5).await;
    }
}

/// A usage error, said with the `tt:` prefix: exit 2.
fn usage(term: &mut dyn Term, why: &str) -> u8 {
    term.say(&format!("tt: {why}"));
    2
}

/// A path as a server could take it: query cut, percent-decoding undone
/// (repeatedly), lowercased, `\` as `/`, empty and `.` segments dropped, `..`
/// applied. Used only to refuse, never to send.
fn canonical(path: &str) -> String {
    let mut p = path.split(['?', '#']).next().unwrap_or_default().to_owned();
    for _ in 0..8 {
        let (b, mut d, mut i) = (p.as_bytes(), Vec::new(), 0);
        while i < b.len() {
            let hex = b
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok());
            match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                Some(c) if b[i] == b'%' => {
                    d.push(c);
                    i += 3;
                }
                _ => {
                    d.push(b[i]);
                    i += 1;
                }
            }
        }
        let d = String::from_utf8_lossy(&d).into_owned();
        if d == p {
            break;
        }
        p = d;
    }
    let p = p.to_lowercase().replace('\\', "/");
    let mut segs = Vec::new();
    for s in p.split('/') {
        match s.trim() {
            "" | "." => {}
            ".." => {
                segs.pop();
            }
            s => segs.push(s),
        }
    }
    format!("/{}", segs.join("/"))
}

/// A write that approves, declines or submits a purchase: only `tt approve`
/// and `tt decline`, which ask a person, make those.
fn buys(path: &str) -> bool {
    let p = canonical(path);
    p == "/api/job-quotes" || p.starts_with("/api/job-quotes/") || p == "/api/jobs/submit"
}

async fn run(cli: Cli, c: &Console, term: &mut dyn Term, out: &mut dyn Write) -> Result<u8, Fail> {
    let f = cli.format.unwrap_or(Format::Json);
    match cli.command {
        Command::Fetch {
            source: Source::Databento(a),
        } => {
            if a.symbols.contains(',') {
                return Ok(usage(term, "one symbol per purchase (--symbols SPY.OPT)"));
            }
            let mut ask = json!({ "dataset": a.dataset, "schema": a.schema, "symbol": a.symbols, "start": a.start, "end": a.end });
            if let Some(h) = &a.hours {
                let Some((s, e)) = h.split_once('-') else {
                    return Ok(usage(term, "--hours is HH:MM-HH:MM, New York time"));
                };
                ask["hours"] = json!({ "start": s, "end": e, "tz": "America/New_York" });
            }
            if let Some(p) = &a.contracts {
                let read = std::fs::read(p).map_err(|e| e.to_string());
                let v = read.and_then(|b| serde_json::from_slice(&b).map_err(|e| e.to_string()));
                ask["contracts"] = v.map_err(|e| format!("{}: {e}", p.display()))?;
            }
            let q = c.price(term, &ask).await?;
            purchase(c, term, out, f, q, Some(ask)).await
        }
        Command::Quotes => {
            let v = c.get("/api/jobs/quotes").await?;
            let mut rows = vec![cells(&["QUOTE", "WHAT", "COST", "STATE", "LEFT"])];
            for q in v["quotes"].as_array().into_iter().flatten() {
                let state = text(&q["state"]);
                let when = match state.as_str() {
                    "ready" => left(q),
                    "pricing" => format!("priced {} of {}", q["priced"], q["of"]),
                    _ if q["submitted"].is_number() => {
                        format!("submitted {} of {}", q["submitted"], q["of"])
                    }
                    _ => String::new(),
                };
                rows.push(vec![
                    text(&q["quote"]),
                    what(q),
                    dollars(&q["cost_usd"]),
                    state,
                    when,
                ]);
            }
            emit(out, f, &v, &rows);
            Ok(0)
        }
        Command::Approve { quote } => {
            let q = c.get(&format!("/api/jobs/quotes/{quote}")).await?;
            if q["state"] != "ready" {
                return no(format!(
                    "{quote} isn't waiting for approval (it's {})",
                    text(&q["state"])
                ));
            }
            let ask = asked_again(&q);
            purchase(c, term, out, f, q, ask).await
        }
        Command::Decline { quote } => {
            show(term, &c.get(&format!("/api/jobs/quotes/{quote}")).await?);
            if !term.tty() {
                return Err(Fail(3, "declining needs a person at a terminal".into()));
            }
            if !yes(term, &format!("Decline {quote}? [y/N]")) {
                return no("not declined".into());
            }
            let a = c
                .write(
                    term,
                    &format!("/api/job-quotes/{quote}/decline"),
                    &json!({}),
                    None,
                )
                .await?;
            if a.code != 200 {
                return Err(failed(&a));
            }
            emit(out, f, &a.body, &[cells(&[&quote, "declined"])]);
            Ok(0)
        }
        Command::Jobs { follow, ids } => jobs(c, term, out, f, follow, ids).await,
        Command::Store { command } => Ok(store(&store_base(), &command, f, out).await),
        Command::Api { method, path } => {
            if method == "POST" && buys(&path) {
                return Err(Fail(
                    3,
                    "`tt api` doesn't approve, decline or submit purchases: `tt approve QUOTE` and `tt decline QUOTE` ask a person first".into(),
                ));
            }
            if !path.starts_with("/api/") {
                return Ok(usage(term, "the path is a console endpoint: /api/…"));
            }
            let a = if method == "GET" {
                c.send(None, &path, 60).await?
            } else {
                let body = term.input()?;
                let body = if body.trim().is_empty() {
                    json!({})
                } else {
                    serde_json::from_str(&body).map_err(|e| format!("stdin: {e}"))?
                };
                c.write(term, &path, &body, None).await?
            };
            emit(out, Format::Json, &a.body, &[]);
            if (200..300).contains(&a.code) {
                Ok(0)
            } else {
                Err(failed(&a))
            }
        }
    }
}

/// `run`, with its failure said, and the format chosen by whether stdout
/// is a terminal unless `--format` says.
async fn exit(
    mut cli: Cli,
    c: &Console,
    term: &mut dyn Term,
    out: &mut dyn Write,
    out_tty: bool,
) -> u8 {
    cli.format = cli
        .format
        .or(Some(if out_tty { Format::Table } else { Format::Json }));
    run(cli, c, term, out)
        .await
        .unwrap_or_else(|Fail(code, e)| {
            term.say(&format!("tt: {e}"));
            code
        })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> std::process::ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) if !e.use_stderr() => e.exit(), // --help, --version
        Err(e) => {
            let said = e.render().to_string();
            eprint!("tt: {}", said.strip_prefix("error: ").unwrap_or(&said));
            return 2.into();
        }
    };
    if let Command::Store { command } = &cli.command {
        let f = cli.format.unwrap_or(if std::io::stdout().is_terminal() {
            Format::Table
        } else {
            Format::Json
        });
        return store(&store_base(), command, f, &mut std::io::stdout())
            .await
            .into();
    }
    let base = std::env::var("TT_CONSOLE").unwrap_or_default();
    if !base.starts_with("http://") && !base.starts_with("https://") {
        eprintln!(
            "tt: set TT_CONSOLE to the console's address under its proxy, such as https://<console host>"
        );
        return 2.into();
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    tokio::spawn(async {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("a SIGTERM handler");
        let n = tokio::select! { _ = tokio::signal::ctrl_c() => 2, _ = term.recv() => 15 };
        echo(true);
        let busy = SUBMITTING.lock().ok().and_then(|s| s.clone());
        if let Some(id) = busy {
            eprintln!("\n{}", interrupted(&id));
        }
        std::process::exit(128 + n);
    });
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .user_agent("tinytrader-tt")
        // Never carried elsewhere with the order token.
        .redirect(reqwest::redirect::Policy::none())
        .build();
    let base = base.trim_end_matches('/').to_owned();
    let c = Console {
        base,
        http: http.expect("an HTTP client"),
        token: Mutex::new(None),
        poll: Duration::from_secs(2),
    };
    let tty = std::io::stdout().is_terminal();
    exit(cli, &c, &mut Real, &mut std::io::stdout(), tty)
        .await
        .into()
}

#[cfg(test)]
mod tests;
