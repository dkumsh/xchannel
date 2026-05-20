#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("xch-bench is Linux-only (uses sched_setaffinity for reproducible results).");
    std::process::exit(1);
}

#[cfg(target_os = "linux")]
fn main() -> std::io::Result<()> {
    bench::run()
}

#[cfg(target_os = "linux")]
mod bench {
    use clap::{ArgAction, Parser, ValueEnum};
    use hdrhistogram::Histogram;
    use std::hint::black_box;
    use std::io;
    use std::path::PathBuf;

    use xchannel::{Reader, ReaderBuilder, ReaderMode, WriterBuilder, page_size};

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

        /// Report interval in milliseconds (streaming mode only).
        #[arg(long = "report-ms", default_value = "1000")]
        report_ms: u64,

        /// Start mode for the reader (live: begin from wp; latejoin: from earliest)
        #[arg(long = "start", value_enum, default_value_t = StartMode::Live)]
        start: StartMode,

        /// Pin this process to the given CPU core (0-based) via sched_setaffinity.
        #[arg(long = "affinity")]
        affinity: Option<usize>,

        /// Minimum nanoseconds between successive writer publishes (busy-wait).
        /// 0 = unthrottled (writer's natural saturation rate). Use this to model
        /// real publish cadences: e.g. `--gap-ns 1000` ≈ 1 M msg/s ceiling.
        #[arg(long = "gap-ns", default_value = "0")]
        gap_ns: u64,

        /// Run for this many seconds, then exit. 0 = run forever (streaming reports).
        /// In duration mode, the reader prints a single JSON summary line on stdout
        /// at end of run.
        #[arg(long = "duration-secs", default_value = "0")]
        duration_secs: u64,

        /// Discard samples for this many seconds after start (only used when
        /// --duration-secs > 0). Allows the channel to reach steady state before
        /// the measurement window opens.
        #[arg(long = "warmup-secs", default_value = "3")]
        warmup_secs: u64,

        /// Cap the number of channel files retained on disk to this value
        /// (active + N-1 historical). 0 = unlimited (default). Writer-only.
        #[arg(long = "keep-files", default_value = "0")]
        keep_files: u64,

        /// Print extra diagnostics to stderr.
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

    fn set_affinity(core: usize) -> io::Result<()> {
        use libc::{CPU_SET, CPU_ZERO, cpu_set_t, sched_setaffinity};
        use std::mem;
        unsafe {
            let mut set: cpu_set_t = mem::zeroed();
            CPU_ZERO(&mut set);
            CPU_SET(core, &mut set);
            if sched_setaffinity(0, mem::size_of::<cpu_set_t>(), &set) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }

    pub fn run() -> io::Result<()> {
        let opt = Opt::parse();

        if !(opt.writer ^ opt.reader) {
            eprintln!("Choose exactly one of --writer or --reader");
            std::process::exit(2);
        }

        if let Some(core) = opt.affinity {
            set_affinity(core)?;
            if opt.verbose {
                let role = if opt.writer { "writer" } else { "reader" };
                eprintln!("[{role}] pinned to CPU {core}");
            }
        }

        let msg_size = parse_size(&opt.msg_size).expect("bad --msg-size");
        if msg_size < FIXED_HEADER_BYTES {
            eprintln!("--msg-size must be >= {FIXED_HEADER_BYTES} (got {msg_size})");
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
                "--touch-bytes ({touch_bytes}) cannot exceed payload size {payload_bytes} (msg_size - 16)"
            );
            std::process::exit(2);
        }

        let default_region = page_size() * 256;
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
        let mut wb = WriterBuilder::new(&opt.file)
            .region_size(region_size)
            .file_roll_size(roll_size)
            .mtu(mtu);
        if opt.keep_files > 0 {
            wb = wb.keep_files(opt.keep_files);
        }
        let mut writer = wb.build()?;

        if opt.verbose {
            eprintln!(
                "[writer] file={} region={} roll={} mtu={} msg_size={} touch_bytes={} duration_secs={}",
                opt.file.display(),
                region_size,
                roll_size,
                mtu,
                msg_size,
                touch_bytes,
                opt.duration_secs
            );
        }

        let gap_ns = opt.gap_ns;
        let payload_len = msg_size.saturating_sub(FIXED_HEADER_BYTES);
        let data = vec![0xA5u8; touch_bytes];

        let start_ns = mono_time_ns();
        let stop_at_ns: Option<u64> = if opt.duration_secs > 0 {
            Some(start_ns + opt.duration_secs.saturating_mul(1_000_000_000))
        } else {
            None
        };

        let mut next_deadline = start_ns;
        let mut seq: u64 = 0;

        loop {
            if let Some(stop) = stop_at_ns
                && mono_time_ns() >= stop
            {
                break;
            }

            if gap_ns > 0 {
                let now = mono_time_ns();
                if now < next_deadline {
                    while mono_time_ns() < next_deadline {
                        std::hint::spin_loop();
                    }
                }
                next_deadline = next_deadline.saturating_add(gap_ns);
            }

            let t0 = mono_time_ns();
            let buf = writer.try_reserve(msg_size)?;
            if touch_bytes > 0 {
                let n = touch_bytes.min(payload_len);
                let payload = &mut buf[FIXED_HEADER_BYTES..];
                payload[..n].copy_from_slice(&data[..n]);
            }
            seq = seq.wrapping_add(1);
            buf[..8].copy_from_slice(&seq.to_le_bytes());
            buf[8..16].copy_from_slice(&t0.to_le_bytes());
            writer.commit(1, msg_size as u32, t0)?;
        }

        if opt.verbose {
            eprintln!("[writer] sent {seq} messages, exiting");
        }
        Ok(())
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
                "[reader] file={} start={:?} duration_secs={} warmup_secs={} msg_size={} touch_bytes={}",
                opt.file.display(),
                opt.start,
                opt.duration_secs,
                opt.warmup_secs,
                msg_size,
                touch_bytes
            );
        }

        if opt.duration_secs > 0 {
            reader_duration(opt, msg_size, touch_bytes, &mut reader)
        } else {
            reader_streaming(opt, msg_size, touch_bytes, &mut reader)
        }
    }

    /// Fixed-duration mode: warm up, then collect samples for `duration_secs`,
    /// then print one JSON summary line on stdout and exit.
    fn reader_duration(
        opt: Opt,
        msg_size: usize,
        touch_bytes: usize,
        reader: &mut Reader,
    ) -> io::Result<()> {
        let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();

        let start_ns = mono_time_ns();
        let warmup_end_ns = start_ns + opt.warmup_secs.saturating_mul(1_000_000_000);
        let stop_at_ns = warmup_end_ns + opt.duration_secs.saturating_mul(1_000_000_000);

        let mut samples: u64 = 0;
        let mut last_seq: Option<u64> = None;
        let mut gaps: u64 = 0;
        let mut seen_any = false;

        loop {
            let now = mono_time_ns();
            if now >= stop_at_ns {
                break;
            }

            let Some(msg) = reader.try_read()? else {
                continue;
            };
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

            // Touch the configured number of payload bytes.
            let n = (msg.len() - FIXED_HEADER_BYTES).min(touch_bytes);
            if n > 0 {
                let body = &payload[FIXED_HEADER_BYTES..];
                let mut acc: u8 = 0;
                for b in &body[..n] {
                    acc ^= *b;
                }
                black_box(acc);
            }

            // During warmup we track sequence (so post-warmup gap detection is
            // accurate) but discard latency samples.
            if now < warmup_end_ns {
                last_seq = Some(seq);
                seen_any = true;
                continue;
            }

            let mut delta = now.saturating_sub(sent_ns);
            if delta == 0 {
                delta = 1;
            }
            let _ = hist.record(delta);
            samples = samples.saturating_add(1);

            if let Some(prev) = last_seq
                && seq > prev + 1
            {
                gaps = gaps.saturating_add(seq - prev - 1);
            }
            last_seq = Some(seq);
            seen_any = true;
        }

        let measure_secs = opt.duration_secs as f64;
        let rate = if measure_secs > 0.0 {
            samples as f64 / measure_secs
        } else {
            0.0
        };

        let (p50, p90, p95, p99, p999, min, max) = if samples > 0 {
            (
                hist.value_at_quantile(0.50),
                hist.value_at_quantile(0.90),
                hist.value_at_quantile(0.95),
                hist.value_at_quantile(0.99),
                hist.value_at_quantile(0.999),
                hist.min(),
                hist.max(),
            )
        } else {
            (0, 0, 0, 0, 0, 0, 0)
        };

        // Single JSON line on stdout for the runner to parse.
        println!(
            "{{\"role\":\"reader\",\"msg_size\":{},\"duration_secs\":{},\"warmup_secs\":{},\"samples\":{},\"msgs_per_sec\":{:.2},\"p50_ns\":{},\"p90_ns\":{},\"p95_ns\":{},\"p99_ns\":{},\"p999_ns\":{},\"min_ns\":{},\"max_ns\":{},\"gaps\":{},\"saw_writer\":{}}}",
            msg_size,
            opt.duration_secs,
            opt.warmup_secs,
            samples,
            rate,
            p50,
            p90,
            p95,
            p99,
            p999,
            min,
            max,
            gaps,
            seen_any
        );
        Ok(())
    }

    /// Streaming mode (duration == 0): print rolling per-interval reports to
    /// stdout. Mirrors the original behaviour, with p99.9 added.
    fn reader_streaming(
        opt: Opt,
        msg_size: usize,
        touch_bytes: usize,
        reader: &mut Reader,
    ) -> io::Result<()> {
        let _ = msg_size;
        let mut active = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();
        let mut report = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();

        let mut last_report = mono_time_ns();
        let interval_ns = opt.report_ms.saturating_mul(1_000_000);

        let mut last_seq: Option<u64> = None;
        let mut gaps: u64 = 0;

        loop {
            if let Some(msg) = reader.try_read()? {
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

                let n = (msg.len() - FIXED_HEADER_BYTES).min(touch_bytes);
                if n > 0 {
                    let body = &payload[FIXED_HEADER_BYTES..];
                    let mut acc: u8 = 0;
                    for b in &body[..n] {
                        acc ^= *b;
                    }
                    black_box(acc);
                }

                let now_ns = mono_time_ns();
                let mut delta = now_ns.saturating_sub(sent_ns);
                if delta == 0 {
                    delta = 1;
                }
                let _ = active.record(delta);

                if let Some(prev) = last_seq {
                    if seq == prev.wrapping_add(1) {
                        // contiguous
                    } else if seq > prev {
                        gaps = gaps.saturating_add(seq - (prev + 1));
                    } else {
                        gaps = 0;
                    }
                }
                last_seq = Some(seq);
            }

            let now = mono_time_ns();
            if now.wrapping_sub(last_report) >= interval_ns {
                std::mem::swap(&mut active, &mut report);
                active.reset();

                let count = report.len();
                let secs = (now - last_report) as f64 / 1e9;
                let rate = (count as f64) / secs;
                if count > 0 {
                    println!(
                        "[{:>8.3}s] msgs/s {:>10.0} | p50 {:>8} | p90 {:>8} | p95 {:>8} | p99 {:>8} | p99.9 {:>8} | min {:>8} | max {:>8} | gaps {}",
                        secs,
                        rate,
                        fmt_ns(report.value_at_quantile(0.50)),
                        fmt_ns(report.value_at_quantile(0.90)),
                        fmt_ns(report.value_at_quantile(0.95)),
                        fmt_ns(report.value_at_quantile(0.99)),
                        fmt_ns(report.value_at_quantile(0.999)),
                        fmt_ns(report.min()),
                        fmt_ns(report.max()),
                        gaps
                    );
                } else {
                    println!("[{:>8.3}s] msgs/s {:>10} | no samples", secs, 0);
                }

                last_report = now;
                report.reset();
            }
        }
    }
}
