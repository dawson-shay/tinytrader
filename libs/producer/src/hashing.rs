//! A reader that hashes what passes through it. The decoder owns it alone; when the
//! decoder is dropped it drains what was left unread (a zstd frame's tail) and
//! publishes the file's hash and size, once, to its [`Digest`]. Nothing is shared
//! while it's read, so nothing is locked.
use sha2::{Digest as _, Sha256};
use std::io::{self, Read};
use std::sync::{Arc, OnceLock};

type Outcome = Result<(String, u64), String>;

pub(crate) struct Hashed {
    reader: Box<dyn Read + Send>,
    hasher: Sha256,
    bytes: u64,
    out: Arc<OnceLock<Outcome>>,
}

/// Where a [`Hashed`] publishes its hash and size once it's dropped.
pub(crate) struct Digest(Arc<OnceLock<Outcome>>);

impl Digest {
    /// The hex SHA-256 and size of everything read, once the reader is dropped.
    pub(crate) fn get(&self) -> Outcome {
        self.0
            .get()
            .cloned()
            .unwrap_or_else(|| Err("the file is still being read".into()))
    }
}

impl Hashed {
    pub(crate) fn new(reader: Box<dyn Read + Send>) -> Self {
        Self {
            reader,
            hasher: Sha256::new(),
            bytes: 0,
            out: Arc::new(OnceLock::new()),
        }
    }

    /// The handle the hash is published to.
    pub(crate) fn digest(&self) -> Digest {
        Digest(Arc::clone(&self.out))
    }

    /// Reads to the end and publishes the hash, if not done.
    fn complete(&mut self) {
        if self.out.get().is_some() {
            return;
        }
        let mut buf = vec![0; 64 * 1024];
        let outcome = loop {
            match self.read(&mut buf) {
                Ok(0) => {
                    let sum = self.hasher.clone().finalize();
                    break Ok((sum.iter().map(|b| format!("{b:02x}")).collect(), self.bytes));
                }
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => break Err(e.to_string()),
            }
        };
        let _ = self.out.set(outcome);
    }
}

/// The hash and size of all of `reader`.
pub(crate) fn hash_all(reader: Box<dyn Read + Send>) -> Outcome {
    let h = Hashed::new(reader);
    let d = h.digest();
    drop(h);
    d.get()
}

impl Read for Hashed {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.reader.read(buf)?;
        self.hasher.update(&buf[..n]);
        self.bytes += n as u64;
        Ok(n)
    }
}

impl Drop for Hashed {
    fn drop(&mut self) {
        self.complete();
    }
}
