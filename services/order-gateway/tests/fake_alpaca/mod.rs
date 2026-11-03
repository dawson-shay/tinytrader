//! A loopback fake Alpaca paper server: REST (orders, account, positions, clock,
//! calendar, activities) and `trade_updates` on one port, rendered from docs-shaped
//! fixtures. Tests script failures and push stream events through [`Fake`].
#![allow(dead_code, clippy::many_single_char_names)]
use serde_json::Value;
use std::fmt::Write as _;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};
use tungstenite::Message;

macro_rules! fixture {
    ($name:literal) => {
        include_str!(concat!("../fixtures/alpaca/", $name))
    };
}

pub const KEY: &str = "PKTESTKEY0000000000001";
pub const SECRET: &str = "sEcReT0000000000000000000000000000000001";
const TS: &str = "2026-10-05T14:00:01.123456789Z";

/// A scripted reply for the next matching request.
#[derive(Clone)]
pub enum Act {
    /// Status, extra headers and body.
    Respond(u16, Vec<(String, String)>, String),
    /// Apply the request, then close the connection without answering.
    Lost,
}

#[derive(Clone)]
pub struct FakeLeg {
    pub id: String,
    pub symbol: String,
    pub ratio: u64,
    pub side: String,
    pub intent: String,
    pub filled: u64,
    pub avg: Option<String>,
}

#[derive(Clone)]
pub struct FakeOrder {
    pub id: String,
    pub cid: String,
    pub single: Option<FakeLeg>,
    pub legs: Vec<FakeLeg>,
    pub qty: u64,
    pub filled: u64,
    pub avg: Option<String>,
    pub limit: String,
    pub status: String,
    /// `day` unless the order says otherwise.
    pub tif: String,
}

#[derive(Default)]
pub struct State {
    pub orders: Vec<FakeOrder>,
    pub log: Vec<(String, String, String)>,
    pub next: Vec<(String, String, Act)>,
    pub auth_ok: bool,
    pub ws_out: Vec<String>,
    pub ws_drop: bool,
    pub ws_listening: usize,
    pub ws_auth: Vec<String>,
    pub keys_seen: Vec<String>,
    execs: u64,
    /// Positions for `GET /v2/positions` when a test sets them; the fixture otherwise.
    pub holdings: Option<String>,
}

pub struct Fake {
    pub addr: SocketAddr,
    pub st: Arc<Mutex<State>>,
    stop: Arc<AtomicBool>,
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.stop.store(true, SeqCst);
    }
}

fn q(v: Option<&str>) -> String {
    v.map_or("null".into(), |s| format!("\"{s}\""))
}

fn terminal(s: &str) -> bool {
    matches!(
        s,
        "filled" | "canceled" | "expired" | "rejected" | "done_for_day" | "replaced"
    )
}

impl FakeOrder {
    pub fn render(&self) -> String {
        let fill = |t: &str, l: &FakeLeg, qty: u64, filled: u64, avg: Option<&str>| {
            t.replace("{{ID}}", &l.id)
                .replace("{{SYMBOL}}", &l.symbol)
                .replace("{{SIDE}}", &l.side)
                .replace("{{QTY}}", &qty.to_string())
                .replace("{{FILLED}}", &filled.to_string())
                .replace("{{AVG}}", &q(avg))
        };
        let legs = if self.legs.is_empty() {
            "null".into()
        } else {
            let items: Vec<String> = self
                .legs
                .iter()
                .map(|l| {
                    fill(
                        fixture!("leg.json"),
                        l,
                        self.qty * l.ratio,
                        l.filled,
                        l.avg.as_deref(),
                    )
                    .replace("{{INTENT}}", &l.intent)
                    .replace("{{RATIO}}", &l.ratio.to_string())
                    .replace("{{STATUS}}", &self.status)
                    .replace("{{UPDATED}}", TS)
                })
                .collect();
            format!("[{}]", items.join(","))
        };
        let blank = FakeLeg {
            id: self.id.clone(),
            symbol: String::new(),
            ratio: 1,
            side: "buy".into(),
            intent: String::new(),
            filled: 0,
            avg: None,
        };
        let top = self.single.as_ref().unwrap_or(&blank);
        let top = FakeLeg {
            id: self.id.clone(),
            ..top.clone()
        };
        fill(
            fixture!("order.json"),
            &top,
            self.qty,
            self.filled,
            self.avg.as_deref(),
        )
        .replace("{{CID}}", &self.cid)
        .replace("{{UPDATED}}", TS)
        .replace(
            "{{ASSET_CLASS}}",
            if self.single.is_some() {
                "us_option"
            } else {
                ""
            },
        )
        .replace(
            "{{CLASS}}",
            if self.single.is_some() {
                "simple"
            } else {
                "mleg"
            },
        )
        .replace(
            "{{INTENT}}",
            &q(self.single.as_ref().map(|l| l.intent.as_str())),
        )
        .replace("{{LIMIT}}", &self.limit)
        .replace(
            "{{TIF}}",
            if self.tif.is_empty() {
                "day"
            } else {
                &self.tif
            },
        )
        .replace("{{STATUS}}", &self.status)
        .replace("{{LEGS}}", &legs)
    }
}

fn order_from(id: String, body: &Value) -> FakeOrder {
    let s = |v: &Value, k: &str| v[k].as_str().unwrap_or("").to_owned();
    let leg = |v: &Value, n: usize| FakeLeg {
        id: format!("{}-leg{n}", &id[..30]),
        symbol: s(v, "symbol"),
        ratio: s(v, "ratio_qty").parse().unwrap_or(1),
        side: s(v, "side"),
        intent: s(v, "position_intent"),
        filled: 0,
        avg: None,
    };
    let legs: Vec<FakeLeg> = body["legs"]
        .as_array()
        .map(|a| a.iter().enumerate().map(|(n, l)| leg(l, n)).collect())
        .unwrap_or_default();
    FakeOrder {
        single: legs.is_empty().then(|| leg(body, 0)),
        legs,
        cid: s(body, "client_order_id"),
        qty: s(body, "qty").parse().unwrap(),
        filled: 0,
        avg: None,
        limit: s(body, "limit_price"),
        status: "accepted".into(),
        tif: s(body, "time_in_force"),
        id,
    }
}

fn respond(s: &mut TcpStream, status: u16, headers: &[(String, String)], body: &str) {
    let mut head = format!(
        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (k, v) in headers {
        let _ = write!(head, "{k}: {v}\r\n");
    }
    let _ = s.write_all(format!("{head}\r\n{body}").as_bytes());
}

impl Fake {
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let st = Arc::new(Mutex::new(State {
            auth_ok: true,
            ..State::default()
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let (st2, stop2) = (Arc::clone(&st), Arc::clone(&stop));
        thread::spawn(move || {
            while !stop2.load(SeqCst) {
                match listener.accept() {
                    Ok((s, _)) => {
                        let (st, stop) = (Arc::clone(&st2), Arc::clone(&stop2));
                        thread::spawn(move || serve(s, &st, &stop));
                    }
                    Err(_) => thread::sleep(Duration::from_millis(5)),
                }
            }
        });
        Self { addr, st, stop }
    }

    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.st.lock().unwrap()
    }

    /// Scripts the next request whose method matches and whose path starts with `path`.
    pub fn next(&self, method: &str, path: &str, act: Act) {
        self.lock().next.push((method.into(), path.into(), act));
    }

    /// Sets what `GET /v2/positions` returns from now on: `(symbol, signed qty)` pairs.
    pub fn hold(&self, positions: &[(&str, i64)]) {
        let list: Vec<String> = positions
            .iter()
            .map(|(s, q)| {
                format!(
                    r#"{{"symbol":"{s}","qty":"{}","side":"{}","avg_entry_price":"10.00"}}"#,
                    q.abs(),
                    if *q < 0 { "short" } else { "long" }
                )
            })
            .collect();
        self.lock().holdings = Some(format!("[{}]", list.join(",")));
    }

    pub fn reply(&self, method: &str, path: &str, status: u16, body: &str) {
        self.next(method, path, Act::Respond(status, Vec::new(), body.into()));
    }

    /// Adds an order the broker has, as if it had reached it without a reply: a
    /// lookup finds it from now on. `body` has the POST body's fields.
    pub fn appear(&self, body: &str) {
        let mut st = self.lock();
        let id = format!("00000000-0000-4000-8000-{:012}", st.orders.len() + 1);
        let v: Value = serde_json::from_str(body).unwrap();
        st.orders.push(order_from(id, &v));
    }

    pub fn count(&self, method: &str, path: &str) -> usize {
        let st = self.lock();
        st.log
            .iter()
            .filter(|(m, p, _)| m == method && p.starts_with(path))
            .count()
    }

    pub fn wait_listening(&self, n: usize) {
        let until = Instant::now() + Duration::from_secs(10);
        while self.lock().ws_listening < n {
            assert!(Instant::now() < until, "stream never listened");
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn with_order<T>(&self, cid: &str, f: impl FnOnce(&mut FakeOrder) -> T) -> T {
        let mut st = self.lock();
        let o = st.orders.iter_mut().find(|o| o.cid == cid).unwrap();
        f(o)
    }

    /// Sets the status and pushes a plain event frame.
    pub fn event(&self, cid: &str, event: &str, status: &str) {
        let order = self.with_order(cid, |o| {
            status.clone_into(&mut o.status);
            o.render()
        });
        let frame = fixture!("trade_update.json")
            .replace("{{EVENT}}", event)
            .replace("{{TS}}", TS)
            .replace("{{ORDER}}", &order);
        self.lock().ws_out.push(frame);
    }

    /// A single-leg execution of `qty` at `price`.
    pub fn fill(&self, cid: &str, qty: u64, price: &str) {
        let (order, event, n) = self.with_order(cid, |o| {
            o.filled += qty;
            o.avg = Some(price.into());
            o.status = if o.filled < o.qty {
                "partially_filled"
            } else {
                "filled"
            }
            .into();
            let event = if o.filled < o.qty {
                "partial_fill"
            } else {
                "fill"
            };
            (o.render(), event, o.filled)
        });
        let exec = self.exec();
        let frame = fixture!("trade_update_fill.json")
            .replace("{{EVENT}}", event)
            .replace("{{EXEC}}", &exec)
            .replace("{{ORDER}}", &order)
            .replace("{{POSITION}}", &n.to_string())
            .replace("{{PRICE}}", price)
            .replace("{{QTY}}", &qty.to_string())
            .replace("{{TS}}", TS);
        self.lock().ws_out.push(frame);
    }

    /// A single-leg execution of `qty` at `price` whose order shows nothing more
    /// filled: a cumulative that lags its fill.
    pub fn fill_lagging(&self, cid: &str, qty: u64, price: &str) {
        let (order, n) = self.with_order(cid, |o| (o.render(), o.filled));
        let exec = self.exec();
        let frame = fixture!("trade_update_fill.json")
            .replace("{{EVENT}}", "partial_fill")
            .replace("{{EXEC}}", &exec)
            .replace("{{ORDER}}", &order)
            .replace("{{POSITION}}", &n.to_string())
            .replace("{{PRICE}}", price)
            .replace("{{QTY}}", &qty.to_string())
            .replace("{{TS}}", TS);
        self.lock().ws_out.push(frame);
    }

    /// As [`Fake::mleg_fill`], every leg's execution under one id.
    pub fn mleg_fill_one_exec(&self, cid: &str, units: u64, net: &str, prices: &[&str]) {
        let shared = self.exec();
        self.mleg_fill_with(cid, units, net, prices, &mut || shared.clone());
    }

    /// An MLEG execution of `units` at net `net`, legs at `prices`.
    pub fn mleg_fill(&self, cid: &str, units: u64, net: &str, prices: &[&str]) {
        self.mleg_fill_with(cid, units, net, prices, &mut || self.exec());
    }

    fn mleg_fill_with(
        &self,
        cid: &str,
        units: u64,
        net: &str,
        prices: &[&str],
        exec: &mut dyn FnMut() -> String,
    ) {
        let execs: Vec<String> = prices.iter().map(|_| exec()).collect();
        let (order, event, legs) = self.with_order(cid, |o| {
            o.filled += units;
            o.avg = Some(net.into());
            o.status = if o.filled < o.qty {
                "partially_filled"
            } else {
                "filled"
            }
            .into();
            for (l, p) in o.legs.iter_mut().zip(prices) {
                l.filled += units * l.ratio;
                l.avg = Some((*p).into());
            }
            let event = if o.filled < o.qty {
                "partial_fill"
            } else {
                "fill"
            };
            (o.render(), event, o.legs.clone())
        });
        let fills: Vec<String> = legs
            .iter()
            .zip(prices.iter().zip(&execs))
            .map(|(l, (p, e))| {
                fixture!("leg_fill.json")
                    .replace("{{EXEC}}", e)
                    .replace("{{QTY}}", &(units * l.ratio).to_string())
                    .replace("{{PRICE}}", p)
                    .replace("{{ID}}", &l.id)
                    .replace("{{SYMBOL}}", &l.symbol)
                    .replace("{{TS}}", TS)
            })
            .collect();
        let positions: Vec<String> = legs
            .iter()
            .map(|l| {
                let sign = if l.side == "sell" { "-" } else { "" };
                format!("\"{}\":\"{sign}{}\"", l.symbol, l.filled)
            })
            .collect();
        let frame = fixture!("trade_update_mleg_fill.json")
            .replace("{{EVENT}}", event)
            .replace("{{ORDER}}", &order)
            .replace("{{PRICE}}", net)
            .replace("{{QTY}}", &units.to_string())
            .replace("{{POSITIONS}}", &format!("{{{}}}", positions.join(",")))
            .replace("{{LEG_FILLS}}", &format!("[{}]", fills.join(",")))
            .replace("{{TS}}", TS);
        self.lock().ws_out.push(frame);
    }

    fn exec(&self) -> String {
        let mut st = self.lock();
        st.execs += 1;
        format!("e0000000-0000-4000-8000-{:012}", st.execs)
    }
}

fn serve(mut s: TcpStream, st: &Arc<Mutex<State>>, stop: &AtomicBool) {
    s.set_nonblocking(false).unwrap();
    let mut buf = [0_u8; 8192];
    let deadline = Instant::now() + Duration::from_secs(5);
    let head = loop {
        let n = s.peek(&mut buf).unwrap_or(0);
        let text = String::from_utf8_lossy(&buf[..n]).to_ascii_lowercase();
        if text.contains("\r\n\r\n") || Instant::now() > deadline {
            break text;
        }
        thread::sleep(Duration::from_millis(2));
    };
    if head.contains("upgrade: websocket") {
        websocket(s, st, stop);
        return;
    }
    let mut r = BufReader::new(s.try_clone().unwrap());
    let mut line = String::new();
    let _ = r.read_line(&mut line);
    let mut parts = line.split_whitespace();
    let (method, path) = (
        parts.next().unwrap_or("").to_owned(),
        parts.next().unwrap_or("").to_owned(),
    );
    let mut len = 0;
    loop {
        let mut h = String::new();
        if r.read_line(&mut h).unwrap_or(0) == 0 || h == "\r\n" {
            break;
        }
        let lower = h.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            len = v.trim().parse().unwrap_or(0);
        }
        if lower.starts_with("apca-api-key-id:") {
            st.lock().unwrap().keys_seen.push(h[16..].trim().to_owned());
        }
    }
    let mut body = vec![0; len];
    let _ = r.read_exact(&mut body);
    let body = String::from_utf8(body).unwrap();
    let (status, headers, reply) = {
        let mut g = st.lock().unwrap();
        g.log.push((method.clone(), path.clone(), body.clone()));
        let scripted = g
            .next
            .iter()
            .position(|(m, p, _)| *m == method && path.starts_with(p.as_str()))
            .map(|i| g.next.remove(i).2);
        match scripted {
            Some(Act::Respond(st, h, b)) => (st, h, b),
            Some(Act::Lost) => {
                route(&mut g, &method, &path, &body);
                return;
            }
            None => {
                let (st, b) = route(&mut g, &method, &path, &body);
                (st, Vec::new(), b)
            }
        }
    };
    respond(&mut s, status, &headers, &reply);
}

fn route(g: &mut State, method: &str, path: &str, body: &str) -> (u16, String) {
    let (route, query) = path.split_once('?').unwrap_or((path, ""));
    let find = |g: &State, key: &str| g.orders.iter().position(|o| o.cid == key || o.id == key);
    let not_found = (404, fixture!("error_not_found.json").to_owned());
    match (method, route) {
        ("POST", "/v2/orders") => {
            let v: Value = serde_json::from_str(body).unwrap();
            if find(g, v["client_order_id"].as_str().unwrap()).is_some() {
                return (422, fixture!("error_duplicate.json").into());
            }
            let id = format!("00000000-0000-4000-8000-{:012}", g.orders.len() + 1);
            let o = order_from(id, &v);
            let text = o.render();
            g.orders.push(o);
            (200, text)
        }
        ("GET", "/v2/orders:by_client_order_id") => {
            let cid = query
                .split('&')
                .find_map(|kv| kv.strip_prefix("client_order_id="))
                .unwrap_or("");
            find(g, cid).map_or(not_found, |i| (200, g.orders[i].render()))
        }
        ("GET", "/v2/orders") => {
            let open = query.contains("status=open");
            let items: Vec<String> = g
                .orders
                .iter()
                .filter(|o| !open || !terminal(&o.status))
                .map(FakeOrder::render)
                .collect();
            (200, format!("[\n{}\n]", items.join(",\n")))
        }
        ("DELETE", "/v2/orders") => {
            let mut out = Vec::new();
            for o in g.orders.iter_mut().filter(|o| !terminal(&o.status)) {
                "canceled".clone_into(&mut o.status);
                out.push(format!("{{\"id\":\"{}\",\"status\":200}}", o.id));
            }
            (207, format!("[{}]", out.join(",")))
        }
        ("GET", "/v2/account") => (200, fixture!("account.json").into()),
        ("GET", "/v2/positions") => (
            200,
            g.holdings.clone().unwrap_or_else(|| fixture!("positions.json").into()),
        ),
        // Liquidate all: one market order per position (Alpaca answers 207).
        ("DELETE", "/v2/positions") => (
            207,
            r#"[{"symbol":"SPY261016P00560000","status":200,"body":{"id":"00000000-0000-4000-8000-0000000000aa","client_order_id":"liq-1","symbol":"SPY261016P00560000","side":"buy","qty":"2","type":"market","time_in_force":"day","status":"accepted"}}]"#.into(),
        ),
        ("GET", "/v2/clock") => (200, fixture!("clock.json").into()),
        ("GET", "/v2/calendar") => (200, fixture!("calendar.json").into()),
        ("GET", "/v2/account/activities") => (200, fixture!("activities.json").into()),
        (m, r) if r.starts_with("/v2/orders/") => {
            let Some(i) = find(g, &r[11..]) else {
                return not_found;
            };
            if m == "GET" {
                return (200, g.orders[i].render());
            }
            if terminal(&g.orders[i].status) {
                return (422, fixture!("error_not_cancelable.json").into());
            }
            "pending_cancel".clone_into(&mut g.orders[i].status);
            (204, String::new())
        }
        _ => (404, "{\"message\":\"Not Found\"}".into()),
    }
}

fn websocket(s: TcpStream, st: &Arc<Mutex<State>>, stop: &AtomicBool) {
    let tcp = s.try_clone().unwrap();
    let Ok(mut ws) = tungstenite::accept(s) else {
        return;
    };
    tcp.set_read_timeout(Some(Duration::from_millis(10)))
        .unwrap();
    let mut listening = false;
    let send = |ws: &mut tungstenite::WebSocket<TcpStream>, t: &str| {
        ws.send(Message::Binary(t.as_bytes().to_vec().into()))
            .is_ok()
    };
    while !stop.load(SeqCst) {
        match ws.read() {
            Ok(Message::Text(t)) => {
                let v: Value = serde_json::from_str(t.as_str()).unwrap_or_default();
                if v["action"] == "auth" {
                    let ok = {
                        let mut g = st.lock().unwrap();
                        g.ws_auth.push(t.as_str().to_owned());
                        g.auth_ok && v["key"] == KEY && v["secret"] == SECRET
                    };
                    let status = if ok { "authorized" } else { "unauthorized" };
                    let reply = format!(
                        r#"{{"stream":"authorization","data":{{"status":"{status}","action":"authenticate"}}}}"#
                    );
                    if !send(&mut ws, &reply) || !ok {
                        return;
                    }
                } else if v["action"] == "listen" {
                    listening = true;
                    st.lock().unwrap().ws_listening += 1;
                    let _ = send(
                        &mut ws,
                        r#"{"stream":"listening","data":{"streams":["trade_updates"]}}"#,
                    );
                }
            }
            Err(tungstenite::Error::Io(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Ok(Message::Close(_)) | Err(_) => return,
            _ => {}
        }
        let (out, drop) = {
            let mut g = st.lock().unwrap();
            let out = if listening {
                std::mem::take(&mut g.ws_out)
            } else {
                Vec::new()
            };
            (out, std::mem::take(&mut g.ws_drop))
        };
        if drop {
            return;
        }
        for f in out {
            if !send(&mut ws, &f) {
                return;
            }
        }
    }
}
