//! A volume in today's layout (files at their names), walked read-only: what
//! `store migrate-cas` copies, and what `tt-fsck --against` checks a volume
//! against.

use std::path::{Path, PathBuf};

/// A regular file found.
#[derive(Clone, Debug)]
pub struct Found {
    /// Its path within the walked root, with `/` between parts.
    pub rel: String,
    pub path: PathBuf,
    pub size: u64,
    /// Its modification time, in nanoseconds since 1970 (0 if unknown).
    pub mtime_ns: u128,
}

/// What a walk found.
#[derive(Debug, Default)]
pub struct Walk {
    /// Every regular file, by `rel`.
    pub files: Vec<Found>,
    /// What couldn't be read, and links and special files (never followed).
    pub errors: Vec<String>,
    /// The operating system's own files and folders, skipped (listed).
    pub skipped: Vec<String>,
}

/// The operating system's own files and folders, never data.
#[must_use]
pub fn os_own(name: &str) -> bool {
    matches!(
        name,
        ".DS_Store"
            | ".Spotlight-V100"
            | ".fseventsd"
            | ".Trashes"
            | ".TemporaryItems"
            | ".DocumentRevisions-V100"
            | "lost+found"
            | "$RECYCLE.BIN"
            | "System Volume Information"
    ) || name.starts_with("._")
}

/// Every regular file under `root`, skipping the OS's own and, at the top,
/// a content-addressed volume's `cas/`.
#[must_use]
pub fn walk(root: &Path) -> Walk {
    let mut w = Walk::default();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
                w.errors.push(format!("{}: {e}", dir.display()));
                continue;
            }
        };
        for e in rd {
            let e = match e {
                Ok(e) => e,
                Err(err) => {
                    w.errors.push(format!("{}: {err}", dir.display()));
                    continue;
                }
            };
            let name = e.file_name().to_string_lossy().into_owned();
            let path = e.path();
            if os_own(&name) || (dir == root && name == crate::DIR) {
                w.skipped.push(path.display().to_string());
                continue;
            }
            match e.file_type() {
                Ok(t) if t.is_dir() => stack.push(path),
                Ok(t) if t.is_file() => match e.metadata() {
                    Ok(m) => w.files.push(Found {
                        rel: path
                            .strip_prefix(root)
                            .unwrap_or(&path)
                            .to_string_lossy()
                            .into_owned(),
                        mtime_ns: m
                            .modified()
                            .ok()
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map_or(0, |d| d.as_nanos()),
                        path,
                        size: m.len(),
                    }),
                    Err(err) => w.errors.push(format!("{}: {err}", path.display())),
                },
                Ok(_) => w.errors.push(format!(
                    "{}: a link or special file; not copied",
                    path.display()
                )),
                Err(err) => w.errors.push(format!("{}: {err}", path.display())),
            }
        }
    }
    w.files.sort_by(|a, b| a.rel.cmp(&b.rel));
    w
}
