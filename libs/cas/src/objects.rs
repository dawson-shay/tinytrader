//! Objects: immutable files named by their content (0039 S1).

use std::fs;
use std::io::{Read, Write as _};
use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};

/// An object written or found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectRef {
    pub sha256: String,
    pub size: u64,
    pub path: PathBuf,
    /// Whether it was already held (nothing was written).
    pub existed: bool,
}

fn is_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn is_ext(s: &str) -> bool {
    !s.is_empty()
        && !s.starts_with('.')
        && !s.ends_with('.')
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-' || b == b'_')
}

/// Where the object with this hash and extension is, under a volume's
/// `cas/` folder.
#[must_use]
pub fn object_path(dir: &Path, sha256: &str, ext: &str) -> PathBuf {
    dir.join("objects")
        .join(&sha256[..2.min(sha256.len())])
        .join(sha256.get(2..4).unwrap_or_default())
        .join(format!("{sha256}.{ext}"))
}

pub(crate) fn sync_dir(dir: &Path) -> std::io::Result<()> {
    fs::File::open(dir)?.sync_all()
}

/// Write `path` whole: beside it, flushed, renamed over it, folder flushed.
pub(crate) fn write_whole(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(dir)?;
    let name = path
        .file_name()
        .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
    let tmp = dir.join(format!(".{name}.new"));
    let mut f = fs::File::create(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    drop(f);
    fs::rename(&tmp, path)?;
    sync_dir(dir)
}

pub(crate) fn hash_file(path: &Path) -> Result<String, String> {
    let mut f = fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut h = Sha256::new();
    std::io::copy(&mut f, &mut h).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(hex(&h.finalize()))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Write an object: copied to `tmp/`, hashed as it's copied, flushed,
/// checked against `expect`, renamed into its place, its folders flushed,
/// and made read-only. One already held is kept (its size checked).
pub(crate) fn put(
    dir: &Path,
    mut r: impl Read,
    ext: &str,
    expect: Option<&str>,
) -> Result<ObjectRef, String> {
    if !is_ext(ext) {
        return Err(format!("{ext:?} isn't an object extension"));
    }
    if let Some(e) = expect
        && !is_hash(e)
    {
        return Err(format!("{e:?} isn't a SHA-256"));
    }
    let tmpdir = dir.join("tmp");
    fs::create_dir_all(&tmpdir).map_err(|e| format!("{}: {e}", tmpdir.display()))?;
    let tmp = tmpdir.join(format!(
        "{}-{}.part",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    let written = (|| -> Result<(String, u64), String> {
        let mut out = fs::File::create(&tmp).map_err(|e| e.to_string())?;
        let mut h = Sha256::new();
        let mut buf = vec![0u8; 1 << 20];
        let mut size = 0u64;
        loop {
            let n = r.read(&mut buf).map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            h.update(&buf[..n]);
            out.write_all(&buf[..n]).map_err(|e| e.to_string())?;
            size += n as u64;
        }
        out.sync_all().map_err(|e| e.to_string())?;
        Ok((hex(&h.finalize()), size))
    })();
    let (sha256, size) = match written {
        Ok(w) => w,
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
    };
    if let Some(e) = expect
        && e != sha256
    {
        let _ = fs::remove_file(&tmp);
        return Err(format!("its SHA-256 is {sha256}, not {e}"));
    }
    let path = object_path(dir, &sha256, ext);
    if let Ok(m) = fs::metadata(&path) {
        let _ = fs::remove_file(&tmp);
        if m.len() != size {
            return Err(format!(
                "{} is held at {} bytes, but these {size} bytes have its hash",
                path.display(),
                m.len()
            ));
        }
        return Ok(ObjectRef {
            sha256,
            size,
            path,
            existed: true,
        });
    }
    let parent = path.parent().ok_or("no folder")?;
    let placed = (|| -> std::io::Result<()> {
        fs::create_dir_all(parent)?;
        let mut ro = fs::metadata(&tmp)?.permissions();
        ro.set_readonly(true);
        fs::set_permissions(&tmp, ro)?;
        fs::rename(&tmp, &path)?;
        sync_dir(parent)?;
        // The new subfolders' entries too.
        if let Some(pp) = parent.parent() {
            sync_dir(pp)?;
        }
        sync_dir(&dir.join("objects"))
    })();
    if let Err(e) = placed {
        let _ = fs::remove_file(&tmp);
        return Err(format!("{}: {e}", path.display()));
    }
    Ok(ObjectRef {
        sha256,
        size,
        path,
        existed: false,
    })
}

/// An object found on disk.
pub(crate) struct Held {
    pub sha256: String,
    pub size: u64,
    pub path: PathBuf,
    /// Its name is a hash, in the folders its hash names.
    pub named_right: bool,
}

/// Every object a volume holds.
pub(crate) fn list(dir: &Path) -> std::io::Result<Vec<Held>> {
    let mut out = Vec::new();
    let root = dir.join("objects");
    let rd = match fs::read_dir(&root) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    // Anything that isn't a folder where a folder belongs, or a file where
    // an object belongs, is listed as misnamed rather than failing the list.
    let stray = |path: PathBuf, out: &mut Vec<Held>| -> std::io::Result<()> {
        out.push(Held {
            size: fs::symlink_metadata(&path)?.len(),
            sha256: String::new(),
            path,
            named_right: false,
        });
        Ok(())
    };
    for a in rd {
        let a = a?;
        if !a.file_type()?.is_dir() {
            stray(a.path(), &mut out)?;
            continue;
        }
        for b in fs::read_dir(a.path())? {
            let b = b?;
            if !b.file_type()?.is_dir() {
                stray(b.path(), &mut out)?;
                continue;
            }
            for f in fs::read_dir(b.path())? {
                let f = f?;
                let path = f.path();
                if !f.file_type()?.is_file() {
                    stray(path, &mut out)?;
                    continue;
                }
                let name = f.file_name().to_string_lossy().into_owned();
                let sha256 = name.split('.').next().unwrap_or_default().to_owned();
                let named_right = is_hash(&sha256)
                    && name.len() > 65
                    && a.file_name().to_string_lossy() == sha256[..2]
                    && b.file_name().to_string_lossy() == sha256[2..4];
                out.push(Held {
                    size: f.metadata()?.len(),
                    sha256,
                    path,
                    named_right,
                });
            }
        }
    }
    Ok(out)
}
