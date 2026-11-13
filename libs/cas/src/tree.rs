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
    /// Its device, inode and change time (nanoseconds): what a rewrite
    /// with its modification time put back, or a copy, doesn't keep.
    pub dev: u64,
    pub ino: u64,
    pub ctime_ns: i128,
}

/// What a walk found.
#[derive(Debug, Default)]
pub struct Walk {
    /// Every regular file, by `rel`.
    pub files: Vec<Found>,
    /// What couldn't be read, and links and special files (never followed).
    pub errors: Vec<String>,
    /// The operating system's own folders at the root, skipped (each
    /// listed, never silently).
    pub skipped: Vec<String>,
    /// Folders holding nothing (an empty batch receipt folder still says a
    /// batch was filed), by their path within the root.
    pub empty_dirs: Vec<String>,
}

/// The name a migration binds in an empty folder, so the folder is kept.
pub const EMPTY_FOLDER: &str = ".empty-folder";

/// The operating system's own folders at a volume's root, never data: the
/// only things a walk skips, and only at the root. Anything else, whatever
/// its name (`.DS_Store`, `._x`, `lost+found`'s recovered files), is copied.
#[must_use]
pub fn os_own(name: &str) -> bool {
    matches!(
        name,
        ".Spotlight-V100"
            | ".fseventsd"
            | ".Trashes"
            | ".TemporaryItems"
            | ".DocumentRevisions-V100"
            | "$RECYCLE.BIN"
            | "System Volume Information"
    )
}

/// What the volume at `root` is, so a receipt from one drive isn't
/// believed for another mounted at the same place: the id in its
/// `.tinytrader-volume-id` if it has one, else its device.
#[must_use]
pub fn identity(root: &Path) -> String {
    use std::os::unix::fs::MetadataExt as _;
    if let Ok(id) = std::fs::read_to_string(root.join(".tinytrader-volume-id"))
        && !id.trim().is_empty()
    {
        return format!("id:{}", id.trim());
    }
    std::fs::metadata(root).map_or_else(|_| "unknown".into(), |m| format!("dev:{}", m.dev()))
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
        let mut any = false;
        for e in rd {
            any = true;
            let e = match e {
                Ok(e) => e,
                Err(err) => {
                    w.errors.push(format!("{}: {err}", dir.display()));
                    continue;
                }
            };
            let name = e.file_name().to_string_lossy().into_owned();
            let path = e.path();
            if dir == root && (os_own(&name) || name == crate::DIR) {
                w.skipped.push(path.display().to_string());
                continue;
            }
            match e.file_type() {
                Ok(t) if t.is_dir() => stack.push(path),
                Ok(t) if t.is_file() => match e.metadata() {
                    Ok(m) => w.files.push(Found {
                        dev: std::os::unix::fs::MetadataExt::dev(&m),
                        ino: std::os::unix::fs::MetadataExt::ino(&m),
                        ctime_ns: i128::from(std::os::unix::fs::MetadataExt::ctime(&m))
                            * 1_000_000_000
                            + i128::from(std::os::unix::fs::MetadataExt::ctime_nsec(&m)),
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
        if !any && dir != root {
            w.empty_dirs.push(
                dir.strip_prefix(root)
                    .unwrap_or(&dir)
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    w.files.sort_by(|a, b| a.rel.cmp(&b.rel));
    w.empty_dirs.sort();
    w
}
