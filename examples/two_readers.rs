use clap::{ArgAction, Parser};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use xchannel::{ReaderBuilder, WriterBuilder, cleanup_channel_files};

const MSG_SIZE: usize = 16; // 8-byte seq + 4-byte key + padding
const STALL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Parser, Debug)]
#[command(
    name = "xchan-two-readers",
    version,
    about = "Two Live readers validate gap-free sequence numbers"
)]
struct Opt {
    /// Channel file path (base file; rolled files become <base>.1, <base>.2, ...)
    #[arg(short = 'f', long = "file", value_name = "PATH")]
    file: PathBuf,

    /// Number of messages to publish (supports k/m/g suffixes). Default: 1m.
    #[arg(short = 'n', long = "messages", default_value = "1m")]
    messages: String,

    /// Region size in bytes (supports k/m/g; default = xchannel default).
    #[arg(long = "region-size")]
    region_size: Option<String>,

    /// File roll size in bytes (supports k/m/g; default: 0 == no rolling).
    #[arg(long = "roll-size")]
    roll_size: Option<String>,

    /// MTU (max payload) in bytes; 0 == unlimited.
    #[arg(long = "mtu")]
    mtu: Option<String>,

    /// Batch size for the batch-mode reader.
    #[arg(long = "batch-size", default_value_t = 10)]
    batch_size: u16,

    /// Messages per burst (0 == continuous).
    #[arg(long = "burst-size", default_value = "0")]
    burst_size: String,

    /// Pause between bursts in microseconds (0 == no pause).
    #[arg(long = "burst-pause-us", default_value_t = 0)]
    burst_pause_us: u64,

    /// Key space for batch coalescing (0 == disabled).
    #[arg(long = "key-space", default_value = "0")]
    key_space: String,

    /// Simulated work per processed message in microseconds.
    #[arg(long = "work-us", default_value_t = 0)]
    work_us: u64,

    /// Skip cleanup of channel files after the run.
    #[arg(long = "no-cleanup", action = ArgAction::SetTrue)]
    no_cleanup: bool,
}

#[derive(Debug)]
struct ReaderStats {
    id: usize,
    messages: u64,
}

struct Cleanup {
    base: PathBuf,
    enabled: bool,
}

impl Cleanup {
    fn run(&mut self) {
        if self.enabled {
            cleanup_channel_files(&self.base);
            self.enabled = false;
        }
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        self.run();
    }
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

fn parse_count(s: &str) -> Result<u64, String> {
    let t = s.trim().to_lowercase();
    if t.is_empty() {
        return Err("empty count".into());
    }
    let (num_part, mul) = if t.ends_with('k') {
        (&t[..t.len() - 1], 1_000u64)
    } else if t.ends_with('m') {
        (&t[..t.len() - 1], 1_000_000u64)
    } else if t.ends_with('g') {
        (&t[..t.len() - 1], 1_000_000_000u64)
    } else {
        (&t[..], 1u64)
    };
    let n: u64 = num_part.parse().map_err(|_| format!("bad count: {s}"))?;
    Ok(n.saturating_mul(mul))
}

fn count_channel_files(base_path: &Path) -> io::Result<usize> {
    if base_path.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "channel path cannot be a directory",
        ));
    }
    let parent_dir = match base_path.parent() {
        Some(parent) if parent.as_os_str().is_empty() => std::env::current_dir(),
        Some(parent) => Ok(parent.to_path_buf()),
        None => std::env::current_dir(),
    }?;
    let base_name = base_path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid base file name"))?
        .to_str()
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "file name is not valid UTF-8")
        })?;
    let prefix = format!("{base_name}.");
    let mut count = 0usize;
    for entry in std::fs::read_dir(&parent_dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name = match file_name.to_str() {
            Some(name) => name,
            None => continue,
        };
        if file_name == base_name {
            count += 1;
            continue;
        }
        if let Some(suffix) = file_name.strip_prefix(&prefix)
            && suffix.parse::<u64>().is_ok()
        {
            count += 1;
        }
    }
    Ok(count)
}

fn main() -> io::Result<()> {
    let opt = Opt::parse();
    let mut cleanup = Cleanup {
        base: opt.file.clone(),
        enabled: !opt.no_cleanup,
    };
    let messages = parse_count(&opt.messages).expect("bad --messages");
    if opt.batch_size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--batch-size must be > 0",
        ));
    }
    let burst_size = parse_count(&opt.burst_size).expect("bad --burst-size");
    let key_space_u64 = parse_count(&opt.key_space).expect("bad --key-space");
    let key_space = usize::try_from(key_space_u64)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "--key-space too large"))?;
    let batch_size = opt.batch_size;
    let work_us = opt.work_us;
    let burst_pause = Duration::from_micros(opt.burst_pause_us);

    let region_size = opt
        .region_size
        .as_deref()
        .map(parse_size)
        .transpose()
        .expect("bad --region-size");
    let roll_size = opt
        .roll_size
        .as_deref()
        .map(parse_size)
        .transpose()
        .expect("bad --roll-size");
    let mtu = opt
        .mtu
        .as_deref()
        .map(parse_size)
        .transpose()
        .expect("bad --mtu");

    let mut writer_builder = WriterBuilder::new(&opt.file);
    if let Some(size) = region_size {
        writer_builder = writer_builder.region_size(size);
    }
    if let Some(size) = roll_size {
        writer_builder = writer_builder.file_roll_size(size as u64);
    }
    if let Some(size) = mtu {
        writer_builder = writer_builder.mtu(size as u64);
    }

    writer_builder.clone().precreate()?;

    let reader_builder = ReaderBuilder::new(&opt.file).live();

    let barrier = Arc::new(Barrier::new(3));
    let done = Arc::new(AtomicBool::new(false));

    let writer_handle = {
        let barrier = barrier.clone();
        let done = done.clone();
        let builder = writer_builder.clone();
        thread::spawn(move || -> io::Result<()> {
            let mut writer = builder.build()?;
            barrier.wait();
            let mut seq: u64 = 0;
            let mut sent: u64 = 0;
            while sent < messages {
                let remaining = messages - sent;
                let burst = if burst_size == 0 {
                    remaining
                } else {
                    remaining.min(burst_size)
                };
                for _ in 0..burst {
                    seq = seq.wrapping_add(1);
                    let buf = writer.try_reserve(MSG_SIZE)?;
                    buf[..8].copy_from_slice(&seq.to_le_bytes());
                    if key_space > 0 {
                        let key = (seq % key_space as u64) as u32;
                        buf[8..12].copy_from_slice(&key.to_le_bytes());
                    } else {
                        buf[8..12].fill(0);
                    }
                    buf[12..16].fill(0);
                    writer.commit(1, MSG_SIZE as u32, 0)?;
                }
                sent = sent.wrapping_add(burst);
                if burst_size > 0 && sent < messages && burst_pause > Duration::ZERO {
                    thread::sleep(burst_pause);
                }
            }
            done.store(true, Ordering::Release);
            println!("writer done: {sent} messages");
            Ok(())
        })
    };

    let reader1_handle = spawn_reader_thread(
        1,
        reader_builder.clone(),
        barrier.clone(),
        done.clone(),
        ReaderRun {
            messages,
            kind: ReaderKind::Batch(batch_size),
            key_space,
            work_us,
        },
    );
    let reader2_handle = spawn_reader_thread(
        2,
        reader_builder,
        barrier,
        done,
        ReaderRun {
            messages,
            kind: ReaderKind::Single,
            key_space,
            work_us,
        },
    );

    writer_handle
        .join()
        .map_err(|_| io::Error::other("writer thread panicked"))??;

    let r1 = reader1_handle
        .join()
        .map_err(|_| io::Error::other("reader-1 thread panicked"))??;
    let r2 = reader2_handle
        .join()
        .map_err(|_| io::Error::other("reader-2 thread panicked"))??;

    println!("reader {} ok: {} messages", r1.id, r1.messages);
    println!("reader {} ok: {} messages", r2.id, r2.messages);
    let file_count = count_channel_files(&opt.file)?;
    println!("files created: {file_count}");
    cleanup.run();

    Ok(())
}

fn spawn_reader_thread(
    id: usize,
    builder: ReaderBuilder,
    barrier: Arc<Barrier>,
    done: Arc<AtomicBool>,
    run: ReaderRun,
) -> thread::JoinHandle<io::Result<ReaderStats>> {
    thread::spawn(move || -> io::Result<ReaderStats> {
        let mut reader = builder.build()?;
        barrier.wait();

        if run.messages == 0 {
            return Ok(ReaderStats { id, messages: 0 });
        }

        let mut expected: u64 = 1;
        let mut last_progress = Instant::now();
        let mut last_pos: Vec<Option<usize>> = if run.key_space > 0 {
            vec![None; run.key_space]
        } else {
            Vec::new()
        };
        let mut batch_counts = match run.kind {
            ReaderKind::Batch(limit) => Some(vec![0u64; limit as usize + 1]),
            ReaderKind::Single => None,
        };

        while expected <= run.messages {
            let mut progressed = false;
            match run.kind {
                ReaderKind::Batch(limit) => {
                    if let Some(batch) = reader.try_read_batch(Some(limit))? {
                        last_progress = Instant::now();
                        progressed = true;
                        if let Some(counts) = batch_counts.as_mut() {
                            let size = batch.len();
                            if size < counts.len() {
                                counts[size] += 1;
                            } else {
                                counts[0] += 1;
                            }
                        }
                        if run.key_space > 0 {
                            for slot in last_pos.iter_mut() {
                                *slot = None;
                            }
                        }
                        for (i, msg) in batch.iter().enumerate() {
                            let payload = msg.payload();
                            if payload.len() < MSG_SIZE {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!("reader {id}: short message payload"),
                                ));
                            }
                            let mut seq_bytes = [0u8; 8];
                            seq_bytes.copy_from_slice(&payload[..8]);
                            let seq = u64::from_le_bytes(seq_bytes);
                            if seq != expected {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!("reader {id}: expected {expected}, got {seq}"),
                                ));
                            }
                            expected = expected.wrapping_add(1);
                            if run.key_space > 0 {
                                let mut key_bytes = [0u8; 4];
                                key_bytes.copy_from_slice(&payload[8..12]);
                                let key = u32::from_le_bytes(key_bytes) as usize;
                                last_pos[key % run.key_space] = Some(i);
                            } else if run.work_us > 0 {
                                burn_work(run.work_us);
                            }
                            if expected > run.messages {
                                break;
                            }
                        }
                        if run.key_space > 0 && run.work_us > 0 {
                            for idx in last_pos.iter().copied().flatten() {
                                if let Some(msg) = batch.get(idx) {
                                    let _payload = msg.payload();
                                    burn_work(run.work_us);
                                }
                            }
                        }
                    }
                }
                ReaderKind::Single => {
                    if let Some(msg) = reader.try_read()? {
                        last_progress = Instant::now();
                        progressed = true;
                        let payload = msg.payload();
                        if payload.len() < MSG_SIZE {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("reader {id}: short message payload"),
                            ));
                        }
                        let mut seq_bytes = [0u8; 8];
                        seq_bytes.copy_from_slice(&payload[..8]);
                        let seq = u64::from_le_bytes(seq_bytes);
                        if seq != expected {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("reader {id}: expected {expected}, got {seq}"),
                            ));
                        }
                        expected = expected.wrapping_add(1);
                        if run.work_us > 0 {
                            burn_work(run.work_us);
                        }
                    }
                }
            }

            if !progressed {
                if done.load(Ordering::Acquire) && last_progress.elapsed() > STALL_TIMEOUT {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("reader {id}: stalled at {}/{}", expected - 1, run.messages),
                    ));
                }
                std::hint::spin_loop();
            }
        }

        let read = expected - 1;
        if let Some(counts) = batch_counts {
            let mut summary = String::new();
            for (size, count) in counts.iter().enumerate() {
                if *count == 0 {
                    continue;
                }
                if !summary.is_empty() {
                    summary.push(' ');
                }
                summary.push_str(&format!("{size}:{count}"));
            }
            if summary.is_empty() {
                summary.push_str("none");
            }
            println!("reader {id} batch sizes: {summary}");
        }
        println!("reader {id} done: {read} messages");
        Ok(ReaderStats { id, messages: read })
    })
}

#[derive(Clone, Copy, Debug)]
struct ReaderRun {
    messages: u64,
    kind: ReaderKind,
    key_space: usize,
    work_us: u64,
}

#[derive(Clone, Copy, Debug)]
enum ReaderKind {
    Batch(u16),
    Single,
}

fn burn_work(work_us: u64) {
    if work_us == 0 {
        return;
    }
    let end = Instant::now() + Duration::from_micros(work_us);
    while Instant::now() < end {
        std::hint::spin_loop();
    }
}
