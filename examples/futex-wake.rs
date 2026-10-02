//! What it costs a writer to wake a sleeping reader through a futex in a shared file, and how
//! long the reader takes to run again. Measurements behind `doc/waking-a-reader.md`.
//!
//! ```text
//! futex-wake nowait  <file> <core>
//!     cost per commit of fetch_add + FUTEX_WAKE when nobody is waiting
//! futex-wake latency <file> <writer_core> <reader_core> <wakes>
//!     a writer stamps the clock, bumps the word and wakes; a separate process that mapped
//!     the file READ-ONLY waits on the word and records now - stamp when it runs again
//! ```
//!
//! `<file>` is created (4 KiB); put it on tmpfs, e.g. `/dev/shm/futex-wake`. A core of `-1`
//! leaves that side unpinned. Each wake follows a pause of 0.2–1.2 ms so the reader is really
//! asleep, and its CPU free to idle, which is the case being measured.
//!
//! The futex is a shared one (no `FUTEX_PRIVATE_FLAG`): the kernel keys it on the file's inode
//! and offset, so the two processes meet on the same word without arranging anything.

#[cfg(target_os = "linux")]
fn main() {
    linux::main();
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("futex-wake needs Linux: it measures futex(2)");
}

#[cfg(target_os = "linux")]
mod linux {
    use std::ffi::CString;
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use std::time::Duration;

    const FILE_SIZE: usize = 4096;
    /// Offset of the wake counter and of the writer's timestamp in the shared file.
    const WORD_AT: usize = 0;
    const STAMP_AT: usize = 8;
    /// Word value that tells the reader to stop.
    const STOP: u32 = u32::MAX;

    const USAGE: &str = "usage:\n  futex-wake nowait  <file> <core>\n  \
                         futex-wake latency <file> <writer_core> <reader_core> <wakes>";

    pub fn main() {
        let args: Vec<String> = std::env::args().collect();
        let int = |i: usize| -> i64 {
            args.get(i)
                .and_then(|a| a.parse().ok())
                .unwrap_or_else(|| fail(USAGE))
        };
        match args.get(1).map(String::as_str) {
            Some("nowait") if args.len() == 4 => nowait(&args[2], int(3) as i32),
            Some("latency") if args.len() == 6 => {
                latency(&args[2], int(3) as i32, int(4) as i32, int(5) as usize)
            }
            _ => fail(USAGE),
        }
    }

    fn fail(msg: &str) -> ! {
        eprintln!("{msg}");
        std::process::exit(2)
    }

    fn now_ns() -> u64 {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    }

    fn pin(core: i32) {
        if core < 0 {
            return;
        }
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(core as usize, &mut set);
            if libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set) != 0 {
                fail(&format!(
                    "cannot pin to core {core}: {}",
                    std::io::Error::last_os_error()
                ));
            }
        }
    }

    /// Map the file `MAP_SHARED`, read-write (creating it) or read-only.
    fn map(path: &str, writable: bool) -> *mut u8 {
        let c = CString::new(path).unwrap_or_else(|_| fail("file name contains a NUL byte"));
        unsafe {
            let flags = if writable {
                libc::O_RDWR | libc::O_CREAT
            } else {
                libc::O_RDONLY
            };
            let fd = libc::open(c.as_ptr(), flags, 0o644);
            if fd < 0 {
                fail(&format!("open {path}: {}", std::io::Error::last_os_error()));
            }
            if writable && libc::ftruncate(fd, FILE_SIZE as libc::off_t) != 0 {
                fail(&format!("size {path}: {}", std::io::Error::last_os_error()));
            }
            let prot = if writable {
                libc::PROT_READ | libc::PROT_WRITE
            } else {
                libc::PROT_READ
            };
            let p = libc::mmap(
                std::ptr::null_mut(),
                FILE_SIZE,
                prot,
                libc::MAP_SHARED,
                fd,
                0,
            );
            if p == libc::MAP_FAILED {
                fail(&format!("mmap {path}: {}", std::io::Error::last_os_error()));
            }
            libc::close(fd);
            p as *mut u8
        }
    }

    fn word(base: *mut u8) -> &'static AtomicU32 {
        unsafe { &*(base.add(WORD_AT) as *const AtomicU32) }
    }

    fn stamp(base: *mut u8) -> &'static AtomicU64 {
        unsafe { &*(base.add(STAMP_AT) as *const AtomicU64) }
    }

    fn futex_wake(word: &AtomicU32) -> i64 {
        unsafe { libc::syscall(libc::SYS_futex, word.as_ptr(), libc::FUTEX_WAKE, i32::MAX) }
    }

    /// `Ok(())` when woken or when the word no longer held `expected`; `Err` with the errno
    /// otherwise (e.g. `ETIMEDOUT`).
    fn futex_wait(word: &AtomicU32, expected: u32, timeout: Duration) -> Result<(), i32> {
        let ts = libc::timespec {
            tv_sec: timeout.as_secs() as _,
            tv_nsec: timeout.subsec_nanos() as _,
        };
        let r = unsafe {
            libc::syscall(
                libc::SYS_futex,
                word.as_ptr(),
                libc::FUTEX_WAIT,
                expected,
                &ts as *const libc::timespec,
            )
        };
        if r == 0 {
            return Ok(());
        }
        match std::io::Error::last_os_error().raw_os_error().unwrap_or(0) {
            libc::EAGAIN | libc::EINTR => Ok(()),
            errno => Err(errno),
        }
    }

    fn report(what: &str, samples: &mut [u64]) {
        samples.sort_unstable();
        let at = |p: f64| samples[((samples.len() - 1) as f64 * p).round() as usize];
        println!(
            "{what}: n={} p50={} p90={} p99={} p99.9={} max={} ns",
            samples.len(),
            at(0.5),
            at(0.9),
            at(0.99),
            at(0.999),
            samples[samples.len() - 1]
        );
    }

    fn nowait(path: &str, core: i32) {
        pin(core);
        let w = word(map(path, true));
        let n = 5_000_000u64;
        for _ in 0..100_000 {
            w.fetch_add(1, Ordering::Release);
            futex_wake(w);
        }
        let t = now_ns();
        for _ in 0..n {
            w.fetch_add(1, Ordering::Release);
        }
        let add_only = (now_ns() - t) as f64 / n as f64;
        let t = now_ns();
        for _ in 0..n {
            w.fetch_add(1, Ordering::Release);
            futex_wake(w);
        }
        let with_wake = (now_ns() - t) as f64 / n as f64;
        println!(
            "nobody waiting: fetch_add {add_only:.1} ns/commit; \
             fetch_add + FUTEX_WAKE {with_wake:.1} ns/commit"
        );
        let mut per_call: Vec<u64> = (0..1_000_000)
            .map(|_| {
                let t = now_ns();
                w.fetch_add(1, Ordering::Release);
                futex_wake(w);
                now_ns() - t
            })
            .collect();
        report("per commit, including two clock reads", &mut per_call);
    }

    fn latency(path: &str, writer_core: i32, reader_core: i32, wakes: usize) {
        let base = map(path, true);
        word(base).store(0, Ordering::SeqCst);
        let child = unsafe { libc::fork() };
        if child < 0 {
            fail(&format!("fork: {}", std::io::Error::last_os_error()));
        }
        if child == 0 {
            reader(path, writer_core, reader_core, wakes);
            unsafe { libc::_exit(0) };
        }
        writer(base, wakes);
        let mut status = 0;
        unsafe { libc::waitpid(child, &mut status, 0) };
    }

    /// The reader: its own read-only mapping, like an xchannel reader's.
    fn reader(path: &str, writer_core: i32, reader_core: i32, wakes: usize) {
        pin(reader_core);
        let base = map(path, false);
        let (w, stamp) = (word(base), stamp(base));
        let mut latencies = Vec::with_capacity(wakes);
        let mut timeouts = 0u64;
        while latencies.len() < wakes {
            let seen = w.load(Ordering::Acquire);
            if seen == STOP {
                break;
            }
            match futex_wait(w, seen, Duration::from_millis(200)) {
                Ok(()) => {}
                Err(libc::ETIMEDOUT) => {
                    timeouts += 1;
                    continue;
                }
                Err(errno) => fail(&format!(
                    "futex_wait: {}",
                    std::io::Error::from_raw_os_error(errno)
                )),
            }
            let running = now_ns();
            if w.load(Ordering::Acquire) != seen {
                latencies.push(running.saturating_sub(stamp.load(Ordering::Acquire)));
            }
        }
        if !latencies.is_empty() {
            report(
                &format!("wake latency (writer core {writer_core}, reader core {reader_core})"),
                &mut latencies,
            );
        }
        println!("  reader: {timeouts} timeouts");
    }

    /// The writer: stamp, bump, wake, after a pause long enough for the reader to sleep.
    fn writer(base: *mut u8, wakes: usize) {
        let (w, stamp) = (word(base), stamp(base));
        std::thread::sleep(Duration::from_millis(200));
        let mut cost = Vec::with_capacity(wakes);
        let mut found_sleeper = 0usize;
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        // A few extra rounds so the reader collects `wakes` samples even if one is missed.
        for _ in 0..wakes + 50 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            std::thread::sleep(Duration::from_micros(200 + rng % 1000));
            let t = now_ns();
            stamp.store(t, Ordering::Release);
            w.fetch_add(1, Ordering::Release);
            if futex_wake(w) > 0 {
                found_sleeper += 1;
            }
            cost.push(now_ns() - t);
        }
        w.store(STOP, Ordering::Release);
        futex_wake(w);
        report("writer cost per commit with a sleeper", &mut cost);
        println!(
            "  writer: {found_sleeper} of {} wakes found a sleeper",
            wakes + 50
        );
    }
}
