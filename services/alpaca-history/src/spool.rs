//! A lane's pages on disk, as they arrive: each page's body (zstd), its
//! receipt, and where the lane goes next (`next_page_token`). A crash, a
//! restart, a rate limit or a pause resumes the lane at its next page; what
//! was fetched is never asked for again.
//!
//! A lane's folder holds `page-NNNNNN.json.zst` (the body as received),
//! `receipts.jsonl` (one receipt a page, in order) and `state.json`. A page
//! is committed in that order, each step synced: the page file (written aside
//! and renamed), its receipt appended, then the state (written aside and
//! renamed). The state is the commit point: a receipt or page beyond its
//! count is a leftover of an interrupted commit, dropped on load.
//!
//! Reading the pages back ([`Pages`]) holds one page's rows at a time, and
//! checks each body against its receipt (size and SHA-256) and row count.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Where a lane stands: pages committed, and what comes next.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LaneState {
    /// Pages committed (and receipts kept).
    pub pages: usize,
    /// Rows in those pages.
    pub rows: usize,
    /// The token for the next page; none before the first page, or once done.
    pub next_page_token: Option<String>,
    /// The last page came without a next token.
    pub done: bool,
    /// The time of the last row committed (rows never go back in time).
    pub prev_ns: u64,
}

/// One lane's folder.
#[derive(Clone, Debug)]
pub struct LaneSpool {
    dir: PathBuf,
}

fn io(p: &Path, e: impl std::fmt::Display) -> String {
    format!("{}: {e}", p.display())
}

/// Write `bytes` to `path` by way of a synced temporary file, renamed.
fn replace(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    let mut f = std::fs::File::create(&tmp).map_err(|e| io(&tmp, e))?;
    f.write_all(bytes).map_err(|e| io(&tmp, e))?;
    f.sync_all().map_err(|e| io(&tmp, e))?;
    std::fs::rename(&tmp, path).map_err(|e| io(path, e))
}

impl LaneSpool {
    #[must_use]
    pub fn at(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn state_path(&self) -> PathBuf {
        self.dir.join("state.json")
    }
    fn receipts_path(&self) -> PathBuf {
        self.dir.join("receipts.jsonl")
    }
    fn page_path(&self, page: usize) -> PathBuf {
        self.dir.join(format!("page-{page:06}.json.zst"))
    }

    /// The lane as kept: its state and its pages' receipts. A lane never
    /// started, or one whose receipts fall short of its state (damaged), is
    /// started afresh.
    ///
    /// # Errors
    /// The folder can't be read or written.
    pub fn load(&self) -> Result<(LaneState, Vec<Value>), String> {
        std::fs::create_dir_all(&self.dir).map_err(|e| io(&self.dir, e))?;
        let state: LaneState = match std::fs::read(self.state_path()) {
            Ok(b) => match serde_json::from_slice(&b) {
                Ok(s) => s,
                Err(_) => return self.fresh(),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return self.fresh(),
            Err(e) => return Err(io(&self.state_path(), e)),
        };
        let mut receipts = Vec::with_capacity(state.pages);
        let mut extra = false;
        match std::fs::File::open(self.receipts_path()) {
            Ok(f) => {
                for line in std::io::BufReader::new(f).lines() {
                    let line = line.map_err(|e| io(&self.receipts_path(), e))?;
                    if receipts.len() == state.pages {
                        extra = true;
                        break;
                    }
                    match serde_json::from_str::<Value>(&line) {
                        Ok(v) => receipts.push(v),
                        Err(_) => break,
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io(&self.receipts_path(), e)),
        }
        if receipts.len() < state.pages {
            return self.fresh();
        }
        // A receipt appended by a commit that didn't reach its state: dropped,
        // so the next commit appends in its place.
        if extra {
            let mut text = Vec::new();
            for r in &receipts {
                serde_json::to_writer(&mut text, r).map_err(|e| e.to_string())?;
                text.push(b'\n');
            }
            replace(&self.receipts_path(), &text)?;
        }
        Ok((state, receipts))
    }

    fn fresh(&self) -> Result<(LaneState, Vec<Value>), String> {
        self.reset()?;
        Ok((LaneState::default(), Vec::new()))
    }

    /// Start the lane again: every page, receipt and its state removed.
    ///
    /// # Errors
    /// The folder can't be emptied.
    pub fn reset(&self) -> Result<(), String> {
        match std::fs::remove_dir_all(&self.dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io(&self.dir, e)),
        }
        std::fs::create_dir_all(&self.dir).map_err(|e| io(&self.dir, e))
    }

    /// Commit page `page` (its body, its receipt) and the lane's state after it.
    ///
    /// # Errors
    /// Anything that stops a write; the lane stays as it was before the page.
    pub fn commit(
        &self,
        page: usize,
        body: &[u8],
        receipt: &Value,
        state: &LaneState,
    ) -> Result<(), String> {
        let path = self.page_path(page);
        let packed = zstd::bulk::compress(body, 3).map_err(|e| io(&path, e))?;
        replace(&path, &packed)?;
        let mut line = serde_json::to_vec(receipt).map_err(|e| e.to_string())?;
        line.push(b'\n');
        let rp = self.receipts_path();
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&rp)
            .map_err(|e| io(&rp, e))?;
        f.write_all(&line).map_err(|e| io(&rp, e))?;
        f.sync_all().map_err(|e| io(&rp, e))?;
        replace(
            &self.state_path(),
            &serde_json::to_vec(state).map_err(|e| e.to_string())?,
        )
    }

    /// A committed page's body, checked against its receipt.
    ///
    /// # Errors
    /// Missing, unreadable, or not the bytes its receipt names.
    pub fn read_page(&self, page: usize, receipt: &Value) -> Result<Vec<u8>, String> {
        let path = self.page_path(page);
        let f = std::fs::File::open(&path).map_err(|e| io(&path, e))?;
        let mut body = Vec::new();
        std::io::Read::read_to_end(
            &mut zstd::Decoder::new(f).map_err(|e| io(&path, e))?,
            &mut body,
        )
        .map_err(|e| io(&path, e))?;
        let sha = format!("{:x}", Sha256::digest(&body));
        if receipt["bytes"].as_u64() != Some(body.len() as u64)
            || receipt["sha256"].as_str() != Some(sha.as_str())
        {
            return Err(format!(
                "{}: not the page its receipt names",
                path.display()
            ));
        }
        Ok(body)
    }
}

/// Parses a page's body into rows and its next token.
pub type Parse<T> = fn(&[u8]) -> Result<(Vec<T>, Option<String>), String>;

/// A lane's committed pages read back as rows, one page in memory at a time.
pub struct Pages<T> {
    spool: LaneSpool,
    receipts: Vec<Value>,
    next: usize,
    rows: std::vec::IntoIter<T>,
    parse: Parse<T>,
    failed: bool,
    /// Most rows held at once (one page's), for the memory bound's test.
    pub peak: usize,
}

impl<T> Pages<T> {
    #[must_use]
    pub fn new(spool: LaneSpool, receipts: Vec<Value>, parse: Parse<T>) -> Self {
        Self {
            spool,
            receipts,
            next: 0,
            rows: Vec::new().into_iter(),
            parse,
            failed: false,
            peak: 0,
        }
    }
}

impl<T> Iterator for Pages<T> {
    type Item = Result<T, String>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.failed {
                return None;
            }
            if let Some(r) = self.rows.next() {
                return Some(Ok(r));
            }
            let receipt = self.receipts.get(self.next)?;
            let page = self.next;
            self.next += 1;
            let rows = self
                .spool
                .read_page(page, receipt)
                .and_then(|body| (self.parse)(&body).map(|(rows, _)| rows));
            match rows {
                Ok(rows) if receipt["rows"].as_u64() == Some(rows.len() as u64) => {
                    self.peak = self.peak.max(rows.len());
                    self.rows = rows.into_iter();
                }
                Ok(rows) => {
                    self.failed = true;
                    return Some(Err(format!(
                        "page {page} reads back as {} rows, its receipt says {}",
                        rows.len(),
                        receipt["rows"]
                    )));
                }
                Err(e) => {
                    self.failed = true;
                    return Some(Err(format!("page {page}: {e}")));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("ah-lane-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    fn receipt(page: usize, body: &[u8], rows: usize) -> Value {
        json!({ "page": page, "rows": rows, "bytes": body.len(), "sha256": format!("{:x}", Sha256::digest(body)) })
    }

    /// Commits survive a reload; a receipt appended by a commit that never
    /// reached its state is dropped; a damaged lane starts afresh.
    #[test]
    fn a_lane_reloads_at_its_last_commit() {
        let dir = scratch("reload");
        let s = LaneSpool::at(dir.clone());
        assert_eq!(s.load().unwrap(), (LaneState::default(), vec![]));
        let st = LaneState {
            pages: 1,
            rows: 2,
            next_page_token: Some("t+1".into()),
            done: false,
            prev_ns: 5,
        };
        s.commit(0, b"[1,2]", &receipt(0, b"[1,2]", 2), &st)
            .unwrap();
        let (got, receipts) = s.load().unwrap();
        assert_eq!((got, receipts.len()), (st.clone(), 1));
        assert_eq!(s.read_page(0, &receipts[0]).unwrap(), b"[1,2]");
        // An interrupted commit: page and receipt written, state not.
        std::fs::write(s.page_path(1), b"junk").unwrap();
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(s.receipts_path())
            .unwrap();
        writeln!(f, "{}", receipt(1, b"x", 1)).unwrap();
        let (got, receipts) = s.load().unwrap();
        assert_eq!((got.pages, receipts.len()), (1, 1));
        assert_eq!(
            std::fs::read_to_string(s.receipts_path())
                .unwrap()
                .lines()
                .count(),
            1,
            "the leftover receipt is dropped"
        );
        // A changed page doesn't read back.
        std::fs::write(s.page_path(0), zstd::bulk::compress(b"[1,3]", 3).unwrap()).unwrap();
        assert!(s.read_page(0, &receipts[0]).is_err());
        // Receipts lost: the lane starts again.
        std::fs::remove_file(s.receipts_path()).unwrap();
        assert_eq!(s.load().unwrap(), (LaneState::default(), vec![]));
        let _ = std::fs::remove_dir_all(dir);
    }
}
