//! CLI front-end for `xchannel::migrate::migrate_channel_v2_to_v3`.
//!
//! Converts a v2 xchannel archive into v3 form. The source archive is
//! read but not modified by default; `--delete-source` opts into removing
//! it after a successful migration.
//!
//! Run from a checkout:
//!
//! ```sh
//! cargo run --release --example xch-migrate -- \
//!     --src /old/feed.xch --dst /new/
//! ```
//!
//! With deletion of the source after success:
//!
//! ```sh
//! cargo run --release --example xch-migrate -- \
//!     --src /old/feed.xch --dst /new/ --delete-source
//! ```
//!
//! Exit code 0 on success; non-zero on any error.

use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use xchannel::{cleanup_channel_files, migrate::migrate_channel_v2_to_v3};

#[derive(Parser, Debug)]
#[command(
    name = "xch-migrate",
    version,
    about = "Convert a v2 xchannel archive to v3"
)]
struct Opt {
    /// Source archive base path. Rolled files are discovered automatically
    /// as `<src>.1`, `<src>.2`, ...
    #[arg(long, value_name = "PATH")]
    src: PathBuf,

    /// Destination directory. Must exist, must be a directory, must differ
    /// from the source's parent directory. Converted files preserve the
    /// source's basename: e.g. `--src /old/feed.xch --dst /new/` writes to
    /// `/new/feed.xch`, `/new/feed.xch.1`, ...
    #[arg(long, value_name = "DIR")]
    dst: PathBuf,

    /// After a successful migration, delete the source archive (the base
    /// file and every rolled file). There is no undo.
    #[arg(long)]
    delete_source: bool,
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, msg.into())
}

fn run(opt: &Opt) -> io::Result<usize> {
    // --dst must exist and be a directory.
    if !opt.dst.exists() {
        return Err(invalid(format!(
            "--dst does not exist: {}",
            opt.dst.display()
        )));
    }
    if !opt.dst.is_dir() {
        return Err(invalid(format!(
            "--dst is not a directory: {}",
            opt.dst.display()
        )));
    }

    // --src must have a usable file-name component.
    let src_name = opt.src.file_name().ok_or_else(|| {
        invalid(format!(
            "--src has no file name component: {}",
            opt.src.display()
        ))
    })?;

    // Resolve src's parent (treat bare filenames as the current directory).
    let src_parent_raw = opt
        .src
        .parent()
        .ok_or_else(|| invalid(format!("--src has no parent: {}", opt.src.display())))?;
    let src_parent = if src_parent_raw.as_os_str().is_empty() {
        std::env::current_dir()?
    } else {
        src_parent_raw.to_path_buf()
    };

    // Compare directories by canonical form so that "./a" and "a" are the same.
    let src_parent_canon = src_parent.canonicalize().map_err(|e| {
        invalid(format!(
            "--src parent directory unreachable: {}: {e}",
            src_parent.display()
        ))
    })?;
    let dst_canon = opt
        .dst
        .canonicalize()
        .map_err(|e| invalid(format!("--dst unreachable: {}: {e}", opt.dst.display())))?;
    if src_parent_canon == dst_canon {
        return Err(invalid(format!(
            "--dst must be a different directory than --src's parent (both resolve to {})",
            dst_canon.display()
        )));
    }

    // Destination base preserves the source's basename inside --dst.
    let dst_base = opt.dst.join(src_name);
    let n = migrate_channel_v2_to_v3(&opt.src, &dst_base)?;

    if opt.delete_source {
        cleanup_channel_files(&opt.src);
        // Sanity check: if the base file is still present, cleanup partially
        // failed. cleanup_channel_files swallows individual unlink errors.
        if opt.src.exists() {
            eprintln!(
                "warning: --delete-source requested but {} still exists; \
                 manual cleanup may be required",
                opt.src.display()
            );
        }
    }

    Ok(n)
}

fn main() -> ExitCode {
    let opt = Opt::parse();
    match run(&opt) {
        Ok(n) => {
            println!(
                "migrated {n} file{} from {} into {}",
                if n == 1 { "" } else { "s" },
                opt.src.display(),
                opt.dst.display(),
            );
            if opt.delete_source {
                println!("source archive deleted");
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("migration failed: {e}");
            ExitCode::FAILURE
        }
    }
}
