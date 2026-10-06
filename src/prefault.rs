//! Keeping the writer's pages faulted in ahead of it, off its thread.
//!
//! A segment is a sparse file mapped one region at a time, so the first write to each page faults
//! and allocates the page's block, and each region boundary grows the file, maps the next region
//! and unmaps the last. All of that lands on the writer. Here a thread follows the writer's
//! `write_position`: it populates the pages just ahead of it in the writer's own mapping with
//! `MADV_POPULATE_WRITE`, which builds the page-table entries and allocates the blocks without
//! changing a byte, maps and populates the next region before the writer reaches it, and unmaps the
//! regions the writer has left. Data and format are untouched; a region not ready in time is
//! mapped by the writer as before.

use crate::region::{RegionMapping, Writable, page_size};
use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Pages are kept populated about this far ahead of the writer, in time: a page populated long
/// before it is written can be written back meanwhile, made read-only again, and fault anyway.
const AHEAD_TIME: Duration = Duration::from_millis(100);
const AHEAD_MIN: usize = 64 << 10;
const AHEAD_MAX: usize = 1 << 20;
const POLL: Duration = Duration::from_micros(50);
/// Linux 5.14+; not in every `libc` release.
#[cfg(target_os = "linux")]
const MADV_POPULATE_WRITE: libc::c_int = 23;

pub(crate) struct Shared {
    stop: AtomicBool,
    /// The OS error that stopped the thread, or 0. The writer then maps every region itself.
    pub(crate) failed: AtomicI32,
    region_size: usize,
    file: File,
    /// The file's length, and the lock both threads take to grow it: a grow from a stale view
    /// would truncate what the other mapped.
    pub(crate) file_len: Mutex<u64>,
    current_ptr: AtomicPtr<u8>,
    current_index: AtomicU64,
    next: Mutex<Option<(u64, RegionMapping<Writable>)>>,
    retired: Mutex<Vec<RegionMapping<Writable>>>,
}

pub(crate) struct Prefaulter {
    pub(crate) shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}

impl Prefaulter {
    /// `write_position` is the writer's channel-header word, valid until the writer stops this.
    pub(crate) fn start(
        file: &File,
        file_len: u64,
        region_size: usize,
        current: &mut RegionMapping<Writable>,
        current_index: u64,
        write_position: &AtomicU64,
        core: Option<usize>,
    ) -> io::Result<Self> {
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            failed: AtomicI32::new(0),
            region_size,
            file: file.try_clone()?,
            file_len: Mutex::new(file_len),
            current_ptr: AtomicPtr::new(current.as_mut_ptr()),
            current_index: AtomicU64::new(current_index),
            next: Mutex::new(None),
            retired: Mutex::new(Vec::new()),
        });
        let wp = write_position as *const AtomicU64 as usize;
        let for_thread = shared.clone();
        let thread = std::thread::Builder::new()
            .name("xch-prefault".into())
            .spawn(move || {
                if let Some(core) = core {
                    pin(core);
                }
                if let Err(why) = run(&for_thread, wp) {
                    for_thread.failed.store(why.raw_os_error().unwrap_or(-1), Ordering::Release);
                }
            })?;
        Ok(Self {
            shared,
            thread: Some(thread),
        })
    }

    /// The next region, if it was mapped and populated ahead for `index`.
    pub(crate) fn take(&self, index: u64) -> Option<RegionMapping<Writable>> {
        let mut next = self.shared.next.lock().unwrap_or_else(|e| e.into_inner());
        match next.take() {
            Some((at, region)) if at == index => Some(region),
            other => {
                *next = other;
                None
            }
        }
    }

    /// The writer moved to `region`; `left` is unmapped here rather than on the writer.
    pub(crate) fn moved(&self, region: &mut RegionMapping<Writable>, index: u64, left: RegionMapping<Writable>) {
        self.shared.current_ptr.store(region.as_mut_ptr(), Ordering::Release);
        self.shared.current_index.store(index, Ordering::Release);
        self.shared
            .retired
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(left);
    }
}

impl Drop for Prefaulter {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn run(shared: &Shared, wp_addr: usize) -> io::Result<()> {
    // Safety: the writer stops and joins this thread before the header it points into is unmapped.
    let wp = unsafe { &*(wp_addr as *const AtomicU64) };
    let size = shared.region_size;
    let page = page_size();
    let (mut done_index, mut done_to) = (u64::MAX, 0usize);
    let mut next_head_done = u64::MAX;
    let (mut since, mut since_pos) = (Instant::now(), wp.load(Ordering::Acquire));
    let mut ahead = AHEAD_MIN;
    while !shared.stop.load(Ordering::Acquire) {
        let index = shared.current_index.load(Ordering::Acquire);
        let base = shared.current_ptr.load(Ordering::Acquire);
        let pos = wp.load(Ordering::Acquire) as usize;
        let elapsed = since.elapsed();
        if elapsed >= AHEAD_TIME {
            let moved = (pos as u64).saturating_sub(since_pos) as f64;
            let in_window = moved * AHEAD_TIME.as_secs_f64() / elapsed.as_secs_f64();
            ahead = (in_window as usize).clamp(AHEAD_MIN, AHEAD_MAX);
            (since, since_pos) = (Instant::now(), pos as u64);
        }
        let off = if (pos / size) as u64 == index { pos % size } else { 0 };
        if index != done_index {
            done_index = index;
            done_to = off / page * page;
        }
        let target = (off + ahead).div_ceil(page).saturating_mul(page).min(size);
        if done_to < target && !base.is_null() {
            populate(base.wrapping_add(done_to), target - done_to)?;
            done_to = target;
        }
        if off >= size / 2 {
            prepare(shared, index + 1);
        }
        if size - off < ahead && next_head_done != index + 1 {
            let next = shared.next.lock().unwrap_or_else(|e| e.into_inner());
            if let Some((at, region)) = next.as_ref()
                && *at == index + 1
            {
                populate(region.as_ptr() as *mut u8, ahead.min(size))?;
                next_head_done = index + 1;
            }
        }
        shared.retired.lock().unwrap_or_else(|e| e.into_inner()).clear();
        std::thread::sleep(POLL);
    }
    Ok(())
}

fn prepare(shared: &Shared, index: u64) {
    if matches!(&*shared.next.lock().unwrap_or_else(|e| e.into_inner()), Some((at, _)) if *at == index) {
        return;
    }
    let size = shared.region_size;
    let mapped = {
        let mut len = shared.file_len.lock().unwrap_or_else(|e| e.into_inner());
        let needed = (index + 1) * size as u64;
        if *len < needed {
            if shared.file.set_len(needed).is_err() {
                return;
            }
            *len = needed;
        }
        RegionMapping::create_writable(&shared.file, index * size as u64, size)
    };
    let Ok(region) = mapped else {
        return;
    };
    *shared.next.lock().unwrap_or_else(|e| e.into_inner()) = Some((index, region));
}

#[cfg(target_os = "linux")]
fn populate(at: *mut u8, len: usize) -> io::Result<()> {
    // Safety: `at..at + len` lies inside a live mapping, page-aligned; populating writes nothing.
    match unsafe { libc::madvise(at.cast(), len, MADV_POPULATE_WRITE) } {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

#[cfg(not(target_os = "linux"))]
fn populate(_at: *mut u8, _len: usize) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn pin(core: usize) {
    // Safety: a zeroed cpu_set_t is empty; CPU_SET stays inside it for any core it can hold.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

#[cfg(not(target_os = "linux"))]
fn pin(_core: usize) {}
