//! An example strategy written as an [`Actor`]: the session fixture's toy
//! ([`crate::fixture`]). It buys one call at 1.25 on the 10:00 step, sets a timer
//! for 10:02:30, and reads every report of a step before acting on it. Run in
//! process or over the sockets, it gives the same run, to the audit hash.
use crate::actor::{Actor, ActorStep, Context};
use serde_json::Value;
use std::time::Duration;

/// 2026-10-27, midnight UTC, and 10:00 New York on it.
const D27: u64 = 1_793_059_200_000_000_000;
const T0: u64 = D27 + 14 * 3_600_000_000_000;
const MIN: u64 = 60_000_000_000;

/// The venue it trades.
const VENUE: &str = "options";

/// The fixture's toy, as an actor.
#[derive(Default)]
pub struct Toy {
    last_seq: u64,
    /// The fills it has seen: price and broker time.
    pub fills: Vec<(String, u64)>,
}

impl Actor for Toy {
    fn name(&self) -> (String, String) {
        ("toy".into(), "test".into())
    }

    fn start(&mut self, _ctx: &mut Context) -> Result<Vec<(u64, String)>, String> {
        Ok(Vec::new())
    }

    fn step(&mut self, step: &ActorStep, ctx: &mut Context) -> Result<Vec<(u64, String)>, String> {
        let orders = ctx.orders(VENUE).ok_or("no options venue")?;
        // Every report of the step, before acting on it.
        let want = step.reports.get(VENUE).copied().unwrap_or(0);
        while self.last_seq < want {
            let Some(line) = orders.line(Duration::from_secs(30))? else {
                return Err(format!("no report {want} from the venue in 30 s"));
            };
            let v: Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
            if let Some(s) = v["seq"].as_u64() {
                self.last_seq = s;
            }
            if v["event"] == "fill" {
                self.fills.push((
                    v["fill"]["price"].as_str().unwrap_or_default().to_owned(),
                    v["ts_broker"].as_u64().unwrap_or_default(),
                ));
            }
        }
        let mut timers = Vec::new();
        if step.as_of == T0 {
            let intent = r#"{"client_order_id":"s-1","shape":"single","instrument":{"class":"option","symbol":"SPY261027C00580000"},"side":"buy","effect":"open","qty":1,"type":"limit","limit_price":"1.25","time_in_force":"day"}"#;
            orders.send("submit", &format!(r#"{{"request_id":"r1","intent":{intent}}}"#))?;
            timers.push((T0 + 2 * MIN + MIN / 2, "check".to_owned()));
        }
        Ok(timers)
    }
}
