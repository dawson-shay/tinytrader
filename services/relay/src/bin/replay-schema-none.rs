//! `replay-schema-none FILE…`: for each replay file, writes
//! `<file>.schema-none.dbn.zst`, the same records with the header's schema
//! unset (mixed), so readers that trust the header read it right. Never
//! writes the original, and refuses a copy that exists.
use std::path::Path;
use std::process::ExitCode;

fn main() -> ExitCode {
    let files: Vec<String> = std::env::args().skip(1).collect();
    if files.is_empty() {
        eprintln!("usage: replay-schema-none FILE.dbn.zst…");
        return ExitCode::from(2);
    }
    let mut status = ExitCode::SUCCESS;
    for f in files {
        let src = Path::new(&f);
        if !relay::replay_fix::is_file(src) {
            eprintln!("{f}: not a file");
            status = ExitCode::FAILURE;
            continue;
        }
        let dst = relay::replay_fix::copy_path(src);
        match relay::replay_fix::schema_none_copy(src, &dst) {
            Ok(true) => println!("{}", dst.display()),
            Ok(false) => println!("{} (the header was already mixed)", dst.display()),
            Err(e) => {
                eprintln!("{f}: {e}");
                status = ExitCode::FAILURE;
            }
        }
    }
    status
}
