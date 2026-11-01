//! A small blocking REST wrapper over `ureq` (duplicated, not shared with feed): one
//! wire request per call, no pooling, no redirects, no retries.
use crate::alpaca::trading::Credentials;
use std::io::{ErrorKind, Read};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const BODY_CAP: u64 = 8 << 20;

/// A completed exchange; every HTTP status is data.
pub(crate) struct Resp {
    pub status: u16,
    pub body: String,
    /// `max(Retry-After, x-ratelimit-reset − now)` in ms, when either is sent.
    pub retry_after_ms: Option<u64>,
}

/// A transport failure; the request may have reached the broker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Fail {
    Timeout,
    Connection,
}

impl Fail {
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Connection => "connection",
        }
    }
}

pub(crate) struct Http {
    agent: ureq::Agent,
    base: String,
    creds: Credentials,
}

fn timed_out(e: &ureq::Transport) -> bool {
    let mut src: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(e);
    while let Some(s) = src {
        if let Some(io) = s.downcast_ref::<std::io::Error>() {
            return matches!(io.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock);
        }
        src = s.source();
    }
    e.to_string().contains("timed out")
}

impl Http {
    pub(crate) fn new(base: &str, creds: Credentials, timeout: Duration) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout(timeout)
            .max_idle_connections(0)
            .max_idle_connections_per_host(0)
            .redirects(0)
            .build();
        Self {
            agent,
            base: base.to_owned(),
            creds,
        }
    }

    /// One request; `path` includes the query string.
    pub(crate) fn call(&self, method: &str, path: &str, body: Option<&str>) -> Result<Resp, Fail> {
        let mut req = self.agent.request(method, &format!("{}{path}", self.base));
        for (k, v) in self.creds.headers() {
            req = req.set(k, v);
        }
        let result = match body {
            Some(b) => req.set("Content-Type", "application/json").send_string(b),
            None => req.call(),
        };
        let resp = match result {
            Ok(r) | Err(ureq::Error::Status(_, r)) => r,
            Err(ureq::Error::Transport(t)) => {
                return Err(if timed_out(&t) {
                    Fail::Timeout
                } else {
                    Fail::Connection
                });
            }
        };
        let num = |h: &str| resp.header(h).and_then(|v| v.trim().parse::<u64>().ok());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let after = num("retry-after").map(|s| s.saturating_mul(1000));
        let reset = num("x-ratelimit-reset").map(|t| t.saturating_sub(now).saturating_mul(1000));
        let retry_after_ms = after.max(reset);
        let status = resp.status();
        let mut text = String::new();
        resp.into_reader()
            .take(BODY_CAP)
            .read_to_string(&mut text)
            .map_err(|e| {
                if matches!(e.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock) {
                    Fail::Timeout
                } else {
                    Fail::Connection
                }
            })?;
        Ok(Resp {
            status,
            body: text,
            retry_after_ms,
        })
    }
}
