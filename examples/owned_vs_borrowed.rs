//! What the owned read path costs relative to the borrowed one.
//!
//! `try_read` hands out a `MessageRef<'_>` borrowed from the mapping and
//! touches no refcount. `try_read_owned` clones an `Arc` per message so the
//! result carries no lifetime, which lets a message outlive the reader or cross
//! a thread.
//!
//! Four passes over the same channel are compared:
//!   - `try_read`          — borrowed, the baseline
//!   - `try_read_owned`    — owned, one Arc clone + drop per message
//!   - `read_owned_into`   — the same, drained through a reused buffer
//!   - copy payload out    — what a cloning reader does instead of sharing
//!
//! The last one is the interesting comparison. The refcount costs a roughly
//! fixed few nanoseconds while the copy scales with payload, so which flavour
//! of "owned" is cheaper flips with size — on the author's hardware the
//! crossover sits somewhere near 300 bytes. Past a few KB the refcount
//! disappears into the memory traffic entirely while the copy roughly doubles
//! the pass.
//!
//! Notes on reading the numbers:
//!   - `min` is the cleanest CPU signal; the median carries scheduler noise.
//!   - Message count defaults to whatever keeps the working set near
//!     `--working-set`, so this measures CPU work rather than DRAM bandwidth.
//!     Raise it past cache and every column converges on memory stalls.
//!   - Single-threaded and uncontended, so the refcount line stays in one
//!     core's L1. Cloning or dropping owned messages on another core makes
//!     that line bounce and costs far more than shown here.
//!
//! ```text
//! cargo run --release --example owned_vs_borrowed -- --path /dev/shm/xch-ovb
//! ```

use clap::Parser;
use std::hint::black_box;
use std::io;
use std::path::PathBuf;
use std::time::Instant;

use xchannel::{OwnedMessage, Reader, ReaderMode, WriterBuilder, cleanup_channel_files};

/// Messages drained per `read_owned_into` call in the buffered pass.
const DRAIN_BATCH: usize = 1024;

/// Payload sizes benchmarked. Fixed at compile time because the copy-out pass
/// needs a `[u8; N]` to model a bitwise copy of an owned record without a
/// zeroing memset skewing the result.
const SIZES: [usize; 4] = [64, 256, 400, 4096];

#[derive(Parser, Debug)]
#[command(
    name = "xchan-owned-vs-borrowed",
    version,
    about = "Cost of try_read_owned vs try_read"
)]
struct Opt {
    /// Channel base path. A tmpfs path keeps disk out of the measurement.
    #[arg(long = "path", default_value = "/dev/shm/xch-owned-vs-borrowed")]
    path: PathBuf,

    /// Target working set per pass in bytes (supports k/m/g). Message count is
    /// derived from it so the data stays cache-resident.
    #[arg(long = "working-set", default_value = "8m")]
    working_set: String,

    /// Timed passes per variant; the first pass of each is discarded.
    #[arg(long = "rounds", default_value_t = 11)]
    rounds: usize,

    /// Comma-separated subset of the built-in sizes, e.g. "64,400".
    #[arg(long = "sizes")]
    sizes: Option<String>,
}

fn parse_size(s: &str) -> Result<usize, String> {
    let t = s.trim().to_lowercase();
    if t.is_empty() {
        return Err("empty size".into());
    }
    let (num, mul) = match t.chars().last() {
        Some('k') => (&t[..t.len() - 1], 1024),
        Some('m') => (&t[..t.len() - 1], 1024 * 1024),
        Some('g') => (&t[..t.len() - 1], 1024 * 1024 * 1024),
        _ => (t.as_str(), 1),
    };
    num.parse::<usize>()
        .map(|n| n * mul)
        .map_err(|e| format!("bad size {s:?}: {e}"))
}

struct Stats {
    min: f64,
    median: f64,
}

fn summarise(mut ns: Vec<u64>, msgs: usize) -> Stats {
    ns.sort_unstable();
    Stats {
        min: ns[0] as f64 / msgs as f64,
        median: ns[ns.len() / 2] as f64 / msgs as f64,
    }
}

fn write_channel(base: &str, payload: usize, msgs: usize) -> io::Result<()> {
    let region = 1024 * 1024;
    let mut w = WriterBuilder::new(base)
        .region_size(region)
        // Must be 0 or span at least two regions; keep everything in one file.
        .file_roll_size(0)
        .mtu(0)
        .build()?;
    for i in 0..msgs {
        let buf = w.try_reserve(payload)?;
        buf.fill((i & 0xff) as u8);
        w.commit(1, payload as u32, i as u64)?;
    }
    Ok(())
}

/// Borrowed: no refcount, no copy.
fn pass_borrowed(base: &str) -> io::Result<(usize, u64)> {
    let mut r = Reader::open(base, ReaderMode::LateJoin)?;
    let (mut n, mut sum) = (0usize, 0u64);
    let t = Instant::now();
    while let Some(m) = r.try_read()? {
        sum += black_box(m.payload()[0]) as u64;
        n += 1;
    }
    black_box(sum);
    Ok((n, t.elapsed().as_nanos() as u64))
}

/// Owned: one Arc clone on read, one drop at end of iteration.
fn pass_owned(base: &str) -> io::Result<(usize, u64)> {
    let mut r = Reader::open(base, ReaderMode::LateJoin)?;
    let (mut n, mut sum) = (0usize, 0u64);
    let t = Instant::now();
    while let Some(m) = r.try_read_owned()? {
        sum += black_box(m.payload()[0]) as u64;
        n += 1;
    }
    black_box(sum);
    Ok((n, t.elapsed().as_nanos() as u64))
}

/// Same work drained into a reused buffer, to price the round-trip through it.
fn pass_owned_drain(base: &str) -> io::Result<(usize, u64)> {
    let mut r = Reader::open(base, ReaderMode::LateJoin)?;
    // Allocated outside the timer: on a polling path the buffer is reused.
    let mut buf: Vec<OwnedMessage> = Vec::with_capacity(DRAIN_BATCH);
    let (mut n, mut sum) = (0usize, 0u64);
    let t = Instant::now();
    while r.read_owned_into(&mut buf, Some(DRAIN_BATCH))? > 0 {
        for m in buf.drain(..) {
            sum += black_box(m.payload()[0]) as u64;
            n += 1;
        }
    }
    black_box(sum);
    Ok((n, t.elapsed().as_nanos() as u64))
}

/// Copies each payload out, as an iterator yielding owned records would.
/// `black_box(&owned)` is required or dead-store elimination deletes the copy.
fn pass_copied<const P: usize>(base: &str) -> io::Result<(usize, u64)> {
    let mut r = Reader::open(base, ReaderMode::LateJoin)?;
    let (mut n, mut sum) = (0usize, 0u64);
    let t = Instant::now();
    while let Some(m) = r.try_read()? {
        let owned: [u8; P] = m.payload().try_into().expect("payload size mismatch");
        black_box(&owned);
        sum += owned[0] as u64;
        n += 1;
    }
    black_box(sum);
    Ok((n, t.elapsed().as_nanos() as u64))
}

fn run<const P: usize>(base: &str, working_set: usize, rounds: usize) -> io::Result<()> {
    // +16 for the header slot each record occupies.
    let msgs = (working_set / (P + 16)).max(1);
    cleanup_channel_files(base);
    write_channel(base, P, msgs)?;

    // Warm the page cache and let the branch predictors settle.
    pass_borrowed(base)?;
    pass_owned(base)?;

    let (mut b, mut o, mut i, mut c) = (vec![], vec![], vec![], vec![]);
    for _ in 0..rounds {
        // Interleaved so any drift lands on every variant equally.
        let (n1, t1) = pass_borrowed(base)?;
        let (n2, t2) = pass_owned(base)?;
        let (n3, t3) = pass_owned_drain(base)?;
        let (n4, t4) = pass_copied::<P>(base)?;
        assert!(
            n1 == msgs && n2 == msgs && n3 == msgs && n4 == msgs,
            "a pass did not see every message: {n1}/{n2}/{n3}/{n4} of {msgs}"
        );
        b.push(t1);
        o.push(t2);
        i.push(t3);
        c.push(t4);
    }
    cleanup_channel_files(base);

    let bs = summarise(b, msgs);
    let os = summarise(o, msgs);
    let is = summarise(i, msgs);
    let cs = summarise(c, msgs);
    let d = |s: &Stats| (s.min - bs.min, (s.min / bs.min - 1.0) * 100.0);
    let (od, op) = d(&os);
    let (idd, ip) = d(&is);
    let (cd, cp) = d(&cs);

    println!("payload {P}B, {msgs} msgs/pass, {rounds} rounds");
    println!("  variant            min      median     delta (min)");
    println!(
        "  try_read        {:7.2}  {:8.2}       baseline",
        bs.min, bs.median
    );
    println!(
        "  try_read_owned  {:7.2}  {:8.2}   {:+7.2} ns  {:+6.1}%",
        os.min, os.median, od, op
    );
    println!(
        "  read_owned_into {:7.2}  {:8.2}   {:+7.2} ns  {:+6.1}%",
        is.min, is.median, idd, ip
    );
    println!(
        "  copy out        {:7.2}  {:8.2}   {:+7.2} ns  {:+6.1}%",
        cs.min, cs.median, cd, cp
    );
    println!();
    Ok(())
}

fn main() -> io::Result<()> {
    let opt = Opt::parse();
    let working_set = parse_size(&opt.working_set).map_err(io::Error::other)?;
    let wanted: Option<Vec<usize>> = opt.sizes.as_ref().map(|s| {
        s.split(',')
            .filter_map(|p| p.trim().parse::<usize>().ok())
            .collect()
    });
    let want = |p: usize| wanted.as_ref().is_none_or(|v| v.contains(&p));

    let base = opt.path.to_string_lossy().to_string();
    println!(
        "ns per message, lower is better. working set ~{} bytes, tmpfs path {base}\n",
        working_set
    );

    // Dispatch by hand: the copy-out pass needs each size as a const generic.
    if want(SIZES[0]) {
        run::<{ SIZES[0] }>(&base, working_set, opt.rounds)?;
    }
    if want(SIZES[1]) {
        run::<{ SIZES[1] }>(&base, working_set, opt.rounds)?;
    }
    if want(SIZES[2]) {
        run::<{ SIZES[2] }>(&base, working_set, opt.rounds)?;
    }
    if want(SIZES[3]) {
        run::<{ SIZES[3] }>(&base, working_set, opt.rounds)?;
    }
    Ok(())
}
