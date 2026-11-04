//! Replay files written before the header said "mixed" (decision 0022 section
//! 4): their DBN header named one schema (such as `cbbo-1m`) while the stream
//! also carries symbol mappings and system records. A reader that trusts the
//! header, as Python's `DBNStore.to_df()` does, reads them as fixed-size
//! records of that schema and fails (`DBN file is truncated`, then a field that
//! won't decode). [`schema_none_copy`] writes a corrected copy, with the
//! header's schema unset and every record unchanged; it never writes the
//! original, which readers may have open.
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Where a DBN v3 header keeps its schema: after `DBN`, the version, the
/// header's length (4 bytes) and the dataset (16 bytes).
const SCHEMA_AT: usize = 8 + 16;
/// The schema field's "none" (mixed).
const NONE: [u8; 2] = [0xFF, 0xFF];

/// Where the corrected copy of `file` goes: `….dbn.zst` becomes
/// `….schema-none.dbn.zst`.
#[must_use]
pub fn copy_path(file: &Path) -> PathBuf {
    let name = file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    let stem = name.strip_suffix(".dbn.zst").unwrap_or(name);
    file.with_file_name(format!("{stem}.schema-none.dbn.zst"))
}

/// Writes `src` again as `dst` with its header's schema unset (mixed). Only the
/// first Zstandard frame, which holds the header, is decoded and encoded again
/// (checksummed, its size pledged, as the relay writes frames); every other
/// frame is copied byte for byte. Refuses a `dst` that exists, and a `src` that
/// isn't a DBN v3 replay file. Returns whether the header changed (`false`: it
/// was already mixed, and the copy is identical).
///
/// # Errors
/// An unreadable `src`, an existing `dst`, or a file that isn't DBN v3.
pub fn schema_none_copy(src: &Path, dst: &Path) -> io::Result<bool> {
    let bad = |m: &str| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: {m}", src.display()),
        )
    };
    let raw = fs::read(src)?;
    let first = zstd::zstd_safe::find_frame_compressed_size(&raw)
        .map_err(|_| bad("no Zstandard frame at its start"))?;
    let size = zstd::zstd_safe::get_frame_content_size(&raw[..first])
        .ok()
        .flatten()
        .ok_or_else(|| bad("its first frame doesn't say its size"))?;
    let mut plain = zstd::bulk::decompress(
        &raw[..first],
        usize::try_from(size).map_err(|_| bad("a frame too large"))?,
    )?;
    if plain.len() < SCHEMA_AT + 2 || &plain[..3] != b"DBN" || plain[3] != 3 {
        return Err(bad("not a DBN v3 file"));
    }
    let changed = plain[SCHEMA_AT..SCHEMA_AT + 2] != NONE;
    plain[SCHEMA_AT..SCHEMA_AT + 2].copy_from_slice(&NONE);
    let mut out = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dst)?;
    let written = (|| -> io::Result<()> {
        let mut z = zstd::stream::Encoder::new(&mut out, 3)?;
        z.include_checksum(true)?;
        z.set_pledged_src_size(Some(plain.len() as u64))?;
        z.write_all(&plain)?;
        z.finish()?;
        out.write_all(&raw[first..])?;
        out.sync_all()
    })();
    if let Err(e) = written {
        drop(out);
        let _ = fs::remove_file(dst);
        return Err(e);
    }
    Ok(changed)
}

/// Whether `file` exists and is a regular file.
#[must_use]
pub fn is_file(file: &Path) -> bool {
    File::open(file)
        .and_then(|f| f.metadata())
        .is_ok_and(|m| m.is_file())
}
