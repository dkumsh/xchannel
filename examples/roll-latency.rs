//! Roll latency: writer `try_reserve`+`commit` time, and reader latency from each record's
//! scheduled send time (so a writer stalled by a roll counts), for the records just after each
//! file roll against all others. Uses only APIs 6.3.0 also has, so the same file measures both.
//!
//! ```text
//! roll-latency <path> <writer core> <reader core> <helper core> [rolls] [rate/s]
//! ```
//!
//! Helpers run on `<helper core>` (`-1`: no helpers). Linux only.

use std::time::{Duration, Instant};
use xchannel::{Helper, ReaderBuilder, Unmap, WriterBuilder, cleanup_channel_files};

const REGION: usize = 1 << 20;
const FILE: u64 = 8 << 20;
const PAYLOAD: usize = 96;
/// Records after each roll reported apart: enough to cover a writer stalled by the roll.
const AFTER_ROLL: u64 = 64;

fn pin(core: i64) {
    if core < 0 {
        return;
    }
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core as usize, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

fn now_ns(epoch: Instant) -> u64 {
    epoch.elapsed().as_nanos() as u64
}

fn report(name: &str, mut ns: Vec<u64>) {
    if ns.is_empty() {
        return;
    }
    ns.sort_unstable();
    let at = |q: f64| ns[((ns.len() - 1) as f64 * q) as usize] as f64 / 1e3;
    println!(
        "{name:<28} n={:<9} p50 {:>7.2} µs  p99 {:>7.2}  p99.9 {:>7.2}  p99.99 {:>7.2}  max {:>8.2}",
        ns.len(),
        at(0.5),
        at(0.99),
        at(0.999),
        at(0.9999),
        at(1.0)
    );
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let arg = |i: usize, default: i64| args.get(i).map_or(default, |a| a.parse().unwrap());
    let path = args
        .get(1)
        .cloned()
        .unwrap_or("/dev/shm/roll-latency".into());
    let (writer_core, reader_core, helper_core) = (arg(2, -1), arg(3, -1), arg(4, -1));
    let rolls = arg(5, 200) as u64;
    let rate = arg(6, 100_000) as u64;
    let helper = (helper_core >= 0).then(|| Helper::on_core(helper_core as usize));
    let per_file = FILE / (16 + PAYLOAD as u64); // at most; headers and skips take the rest
    let total = per_file * rolls;

    cleanup_channel_files(&path);
    let mut wb = WriterBuilder::new(&path)
        .region_size(REGION)
        .file_roll_size(FILE)
        .keep_files(4)
        .unmap(Unmap::AtFileRoll);
    let mut rb = ReaderBuilder::new(&path).live().unmap(Unmap::AtFileRoll);
    if let Some(h) = helper {
        wb = wb.helper(h);
        rb = rb.helper(h);
    }
    pin(writer_core);
    let mut writer = wb.build()?;
    let epoch = Instant::now();
    let reading = {
        let path = path.clone();
        std::thread::spawn(move || -> std::io::Result<(Vec<u64>, Vec<u64>, u64)> {
            pin(reader_core);
            let _ = path;
            let mut reader = rb.build()?;
            let (mut after, mut rest) = (Vec::new(), Vec::with_capacity(total as usize));
            let (mut seen, mut since_roll) = (0u64, u64::MAX);
            let mut file = reader.file_sequence();
            while seen < total {
                let Some(m) = reader.try_read()? else {
                    continue;
                };
                let lat = now_ns(epoch).saturating_sub(m.header().user_meta_u64);
                seen += 1;
                if reader.file_sequence() != file {
                    file = reader.file_sequence();
                    since_roll = 0;
                }
                if since_roll < AFTER_ROLL {
                    after.push(lat);
                    since_roll += 1;
                } else {
                    rest.push(lat);
                }
            }
            Ok((after, rest, reader.file_sequence()))
        })
    };
    std::thread::sleep(Duration::from_millis(200));
    let gap = Duration::from_nanos(1_000_000_000 / rate);
    let mut commits = Vec::with_capacity(total as usize);
    let mut next = Instant::now();
    for _ in 0..total {
        while Instant::now() < next {
            std::hint::spin_loop();
        }
        let scheduled = (next - epoch).as_nanos() as u64;
        next += gap;
        let t = Instant::now();
        writer.try_reserve(PAYLOAD)?.fill(1);
        writer.commit(1, PAYLOAD as u32, scheduled)?;
        commits.push(t.elapsed().as_nanos() as u64);
    }
    let (after, rest, rolled) = reading.join().expect("reader")?;
    commits.sort_unstable();
    let slow: Vec<u64> = commits[commits.len() - rolled as usize..].to_vec();
    println!("{rolled} rolls of {FILE} B files, {rate}/s, helpers {helper:?}");
    report("writer commit, all", commits);
    report("writer commit, slowest n=rolls", slow);
    report("reader, other records", rest);
    report("reader, 64 after a roll", after);
    drop(writer);
    cleanup_channel_files(&path);
    Ok(())
}
