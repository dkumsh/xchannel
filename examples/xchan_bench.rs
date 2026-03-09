use clap::{ArgAction, Parser, ValueEnum};
use hdrhistogram::Histogram;
use std::hint::black_box;
use std::io;
use std::path::PathBuf;

use xchannel::{ReaderBuilder, ReaderMode, WriterBuilder, page_size};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum StartMode {
    Live,
    Latejoin,
}

/// Payload layout we write/read.
/// First 8 bytes: sequence (LE)
/// Next 8 bytes: monotonic send timestamp in ns (LE)
const FIXED_HEADER_BYTES: usize = 16;

#[derive(Parser, Debug)]
#[command(
    name = "xchan-bench",
    version,
    about = "Latency benchmark for xchannel"
)]
struct Opt {
    /// Channel file path (base file; rolled files become <base>.1, <base>.2, ...)
    #[arg(short = 'f', long = "file", value_name = "PATH")]
    file: PathBuf,

    /// Run in writer mode
    #[arg(long, action = ArgAction::SetTrue, conflicts_with = "reader")]
    writer: bool,

    /// Run in reader mode
    #[arg(long, action = ArgAction::SetTrue, conflicts_with = "writer")]
    reader: bool,

    /// Message payload size in bytes (supports k/m/g suffixes). Must be >= 16.
    #[arg(short = 's', long = "msg-size", default_value = "64")]
    msg_size: String,

    /// Number of payload bytes (after the 16-byte header) to actually touch.
    /// Default: msg_size - 16 (i.e. touch the entire payload).
    #[arg(long = "touch-bytes")]
    touch_bytes: Option<String>,

    /// Region size in bytes (supports k/m/g; default = OS page * 256)
    #[arg(long = "region-size")]
    region_size: Option<String>,

    /// File roll size in bytes (supports k/m/g; default: 0 == no rolling)
    #[arg(long = "roll-size")]
    roll_size: Option<String>,

    /// MTU (max payload) in bytes; 0 == unlimited
    #[arg(long = "mtu", default_value = "0")]
    mtu: String,

    /// Report interval in milliseconds
    #[arg(long = "report-ms", default_value = "1000")]
    report_ms: u64,

    /// Start mode for the reader (live: begin from wp; latejoin: from earliest)
    #[arg(long = "start", value_enum, default_value_t = StartMode::Live)]
    start: StartMode,

    /// Optional CPU core to pin this process to (0-based). Requires 'core_affinity' at build time.
    #[arg(long = "affinity")]
    affinity: Option<usize>,

    /// Throttle writer to messages-per-second (0 = as fast as possible)
    #[arg(short = 'i', long = "interval-µs", default_value = "0")]
    interval_us: u64,

    /// Print a little more detail
    #[arg(long = "verbose", action = ArgAction::SetTrue)]
    verbose: bool,
}

fn parse_size(s: &str) -> Result<usize, String> {
    let t = s.trim().to_lowercase();
    if t.is_empty() {
        return Err("empty size".into());
    }
    let (num_part, mul) = if t.ends_with('k') {
        (&t[..t.len() - 1], 1024usize)
    } else if t.ends_with('m') {
        (&t[..t.len() - 1], 1024usize * 1024)
    } else if t.ends_with('g') {
        (&t[..t.len() - 1], 1024usize * 1024 * 1024)
    } else {
        (&t[..], 1usize)
    };
    let n: usize = num_part.parse().map_err(|_| format!("bad size: {s}"))?;
    Ok(n.saturating_mul(mul))
}

#[inline]
fn fmt_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.2} µs", ns as f64 / 1e3)
    } else if ns < 1_000_000_000 {
        format!("{:.2} ms", ns as f64 / 1e6)
    } else {
        format!("{:.2} s", ns as f64 / 1e9)
    }
}

#[cfg(unix)]
#[inline]
fn mono_time_ns() -> u64 {
    use libc::{CLOCK_MONOTONIC, clock_gettime, timespec};
    unsafe {
        let mut ts = timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        clock_gettime(CLOCK_MONOTONIC, &mut ts);
        (ts.tv_sec as u64) * 1_000_000_000 + (ts.tv_nsec as u64)
    }
}

fn main() -> io::Result<()> {
    let opt = Opt::parse();

    if !(opt.writer ^ opt.reader) {
        eprintln!("Choose exactly one of --writer or --reader");
        std::process::exit(2);
    }

    let msg_size = parse_size(&opt.msg_size).expect("bad --msg-size");
    if msg_size < FIXED_HEADER_BYTES {
        eprintln!(
            "--msg-size must be >= {} (got {})",
            FIXED_HEADER_BYTES, msg_size
        );
        std::process::exit(2);
    }

    let payload_bytes = msg_size - FIXED_HEADER_BYTES;
    let touch_bytes = opt
        .touch_bytes
        .as_deref()
        .map(parse_size)
        .transpose()
        .expect("bad --touch-bytes")
        .unwrap_or(payload_bytes);

    if touch_bytes > payload_bytes && !opt.reader {
        eprintln!(
            "--touch-bytes ({}) cannot exceed payload size {} (msg_size - 16)",
            touch_bytes, payload_bytes
        );
        std::process::exit(2);
    }

    let default_region = page_size() * 256; // good general default
    let region_size = opt
        .region_size
        .as_deref()
        .map(parse_size)
        .transpose()
        .expect("bad --region-size")
        .unwrap_or(default_region);

    let roll_size = opt
        .roll_size
        .as_deref()
        .map(parse_size)
        .transpose()
        .expect("bad --roll-size")
        .unwrap_or(0usize) as u64;

    let mtu = parse_size(&opt.mtu).expect("bad --mtu") as u64;

    if opt.writer {
        run_writer(opt, msg_size, touch_bytes, region_size, roll_size, mtu)
    } else {
        run_reader(opt, msg_size, touch_bytes)
    }
}

// ---------------- Writer ----------------

fn run_writer(
    opt: Opt,
    msg_size: usize,
    touch_bytes: usize,
    region_size: usize,
    roll_size: u64,
    mtu: u64,
) -> io::Result<()> {
    let mut writer = WriterBuilder::new(&opt.file)
        .region_size(region_size)
        .file_roll_size(roll_size)
        .mtu(mtu)
        .build()?;

    let pps = if opt.interval_us == 0 {
        0
    } else {
        1_000_000 / opt.interval_us
    };

    if opt.verbose {
        eprintln!(
            "[writer] file={:?} region={} roll={} mtu={} msg_size={} touch_bytes={}  pps={}",
            opt.file.display(),
            region_size,
            roll_size,
            mtu,
            msg_size,
            touch_bytes,
            pps
        );
    }

    let mut seq: u64 = 0;
    let payload_len = msg_size.saturating_sub(FIXED_HEADER_BYTES);

    // Simple rate control (optional)
    let interval_ns = if opt.interval_us > 0 {
        1_000u64 * opt.interval_us
    } else {
        0
    };
    let data = vec![0xA5u8; touch_bytes];
    let mut reserve_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();
    let mut copy_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();
    let mut commit_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();
    let mut total_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();
    let report_interval_ns = opt.report_ms.saturating_mul(1_000_000);
    let mut next_deadline = mono_time_ns();
    let mut next_report_deadline = next_deadline + report_interval_ns;

    loop {
        // throttle if requested
        if interval_ns > 0 || report_interval_ns > 0 {
            let now = mono_time_ns();
            if report_interval_ns > 0 && now >= next_report_deadline {
                let p50_tot = total_hist.value_at_quantile(0.50);
                let p99_tot = total_hist.value_at_quantile(0.99);
                let max_tot = total_hist.max();

                let p50_reserve = reserve_hist.value_at_quantile(0.50);
                let p99_reserve = reserve_hist.value_at_quantile(0.99);
                let max_reserve = reserve_hist.max();

                let p50_copy = copy_hist.value_at_quantile(0.50);
                let p99_copy = copy_hist.value_at_quantile(0.99);
                let max_copy = copy_hist.max();

                let p50_commit = commit_hist.value_at_quantile(0.50);
                let p99_commit = commit_hist.value_at_quantile(0.99);
                let max_commit = commit_hist.max();

                eprintln!(
                    "[writer] total: p50={} p99={} max={}|reserve: p50={} p99={} max={}| copy: p50={} p99={} max={}| commit: p50={} p99={} max={}",
                    fmt_ns(p50_tot),
                    fmt_ns(p99_tot),
                    fmt_ns(max_tot),
                    fmt_ns(p50_reserve),
                    fmt_ns(p99_reserve),
                    fmt_ns(max_reserve),
                    fmt_ns(p50_copy),
                    fmt_ns(p99_copy),
                    fmt_ns(max_copy),
                    fmt_ns(p50_commit),
                    fmt_ns(p99_commit),
                    fmt_ns(max_commit),
                );

                total_hist.reset();
                reserve_hist.reset();
                copy_hist.reset();
                commit_hist.reset();

                next_report_deadline = next_report_deadline.saturating_add(report_interval_ns);
            }
            if interval_ns > 0 {
                if now < next_deadline {
                    // busy wait very lightly
                    while mono_time_ns() < next_deadline {
                        std::hint::spin_loop();
                    }
                }
                next_deadline = next_deadline.saturating_add(interval_ns);
            }
        }

        let t0 = mono_time_ns();
        let buf = writer.try_reserve(msg_size)?;
        let t1 = mono_time_ns();
        if touch_bytes > 0 {
            let n = touch_bytes.min(payload_len);
            let payload = &mut buf[FIXED_HEADER_BYTES..];
            payload[..n].copy_from_slice(&data[..n]);
        }
        let t2 = mono_time_ns();
        {
            let t_ns = t0;
            seq = seq.wrapping_add(1);
            // Write sequence (LE) and timestamp (LE)
            buf[..8].copy_from_slice(&seq.to_le_bytes());
            buf[8..16].copy_from_slice(&t_ns.to_le_bytes());
            writer.commit(1, msg_size as u32, t_ns)?;
        }
        let t3 = mono_time_ns();
        // Histograms:
        let dt_reserve = (t1 - t0).max(1); // “copy only” (plus any stall happening during copy)
        let dt_copy = (t2 - t1).max(1); // “copy only” (plus any stall happening during copy)
        let dt_commit = (t3 - t2).max(1);
        let dt_total = (t3 - t0).max(1);

        let _ = reserve_hist.record(dt_reserve);
        let _ = copy_hist.record(dt_copy);
        let _ = commit_hist.record(dt_commit);
        let _ = total_hist.record(dt_total);
    }
}

// ---------------- Reader ----------------

fn run_reader(opt: Opt, msg_size: usize, touch_bytes: usize) -> io::Result<()> {
    let mode = match opt.start {
        StartMode::Live => ReaderMode::Live,
        StartMode::Latejoin => ReaderMode::LateJoin,
    };
    let mut reader = ReaderBuilder::new(&opt.file).mode(mode).build()?;

    if opt.verbose {
        eprintln!(
            "[reader] file={:?} start={:?} report={}ms msg_size={}  touch_bytes={}",
            opt.file.display(),
            opt.start,
            opt.report_ms,
            msg_size,
            touch_bytes
        );
    }

    let mut active = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();
    let mut report = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();

    let mut last_report = mono_time_ns();
    let interval_ns = opt.report_ms.saturating_mul(1_000_000);

    let mut total_msgs: u64 = 0;
    let mut last_seq: Option<u64> = None;
    let mut gaps: u64 = 0;

    loop {
        if let Some(msg) = reader.try_read()? {
            // Safety: we wrote at least 16 bytes
            if msg.len() < FIXED_HEADER_BYTES {
                continue;
            }
            let payload = msg.payload();
            let mut seq_bytes = [0u8; 8];
            seq_bytes.copy_from_slice(&payload[..8]);
            let seq = u64::from_le_bytes(seq_bytes);

            let mut ts_bytes = [0u8; 8];
            ts_bytes.copy_from_slice(&payload[8..16]);
            let sent_ns = u64::from_le_bytes(ts_bytes);

            // Touch the configured number of payload bytes (after the 16-byte header)
            let touch_bytes = (msg.len() - FIXED_HEADER_BYTES).min(touch_bytes);
            if touch_bytes > 0 {
                let body = &payload[FIXED_HEADER_BYTES..];
                let to_touch = &body[..touch_bytes];
                let mut acc: u8 = 0;
                for b in to_touch {
                    acc ^= *b;
                }
                // Prevent the compiler from optimizing the loop away
                black_box(acc);
            }

            let now_ns = mono_time_ns();
            let mut delta = now_ns.saturating_sub(sent_ns);
            if delta == 0 {
                // avoid zero for hdrhistogram lower bound
                delta = 1;
            }

            let _ = active.record(delta);

            // simple gap detection
            if let Some(prev) = last_seq {
                if seq == prev.wrapping_add(1) {
                    // ok, contiguous
                } else if seq > prev {
                    gaps = gaps.saturating_add(seq - (prev + 1));
                } else {
                    // seq <= prev : writer reset or restarted; don't count as gaps
                    gaps = 0; // or keep a 'resets' counter if you want
                }
            }
            last_seq = Some(seq);

            total_msgs = total_msgs.wrapping_add(1);
        } else {
            // Nothing available right now; busy-spin lightly
            // std::hint::spin_loop();
        }

        // Periodic report
        let now = mono_time_ns();
        if now.wrapping_sub(last_report) >= interval_ns {
            std::mem::swap(&mut active, &mut report);
            active.reset();

            let count = report.len();
            let secs = (now - last_report) as f64 / 1e9;
            let rate = (count as f64) / secs;
            if count > 0 {
                let p50 = report.value_at_quantile(0.50);
                let p75 = report.value_at_quantile(0.75);
                let p90 = report.value_at_quantile(0.90);
                let p95 = report.value_at_quantile(0.95);
                let p99 = report.value_at_quantile(0.99);
                let min = report.min();
                let max = report.max();

                println!(
                    "[{:>8.3}s] msgs/s {:>10.0} |  p50 {:>8} | p75 {:>8} | p90 {:>8} | p95 {:>8} | p99 {:>8} | min {:>8} | max {:>8} | gaps {}",
                    secs,
                    rate,
                    fmt_ns(p50),
                    fmt_ns(p75),
                    fmt_ns(p90),
                    fmt_ns(p95),
                    fmt_ns(p99),
                    fmt_ns(min),
                    fmt_ns(max),
                    gaps
                );
            } else {
                println!(
                    "[{:>8.3}s] msgs/s {:>10} | no samples",
                    (now - last_report) as f64 / 1e9,
                    0
                );
            }

            last_report = now;
            report.reset();
        }
    }
}
