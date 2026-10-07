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
//!
//! Halfway through a rolling segment's last region the thread also creates the next segment under
//! its `.partial` name, which is the slow part of a roll, so the writer only has to stamp its base
//! and rename it. It also deletes the segments retention drops.
//!
//! The thread follows the writer across file rolls. Mappings the writer leaves, the old segment's
//! included, are unmapped only here and only at the top of an iteration, before the thread reads
//! the writer's pointers again, so a pointer it has read stays valid until it is done with it.
//!
//! If the thread stops early (an error, such as `EINVAL` from `madvise` before Linux 5.14, or a
//! panic), the writer notices at its next hand-over and from then on unmaps and deletes for itself,
//! what it queued before included, as it does without a helper.

use crate::helper::{Failure, Thread};
use crate::region::{RegionMapping, Writable, page_size};
use crate::v4::{Identity, InstanceId};
use crate::{CHANNEL_NAME_MAX, Helper, Unmap, Writer, make_partial_channel_file_path};
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Pages are kept populated about this far ahead of the writer, in time: a page populated long
/// before it is written can be written back meanwhile, made read-only again, and fault anyway.
const AHEAD_TIME: Duration = Duration::from_millis(100);
const AHEAD_MIN: usize = 64 << 10;
const AHEAD_MAX: usize = 1 << 20;
const POLL: Duration = Duration::from_micros(50);

/// The segment being written, and the lock both threads take to grow it: a grow from a stale view
/// would truncate what the other mapped. If the thread could not open the segment the writer
/// rolled to, this stays on the old one, and the thread leaves the new one to the writer.
pub(crate) struct Segment {
    pub(crate) sequence: u64,
    /// Its instance ID: the parent its successor names.
    instance: InstanceId,
    pub(crate) file: File,
    pub(crate) len: u64,
}

struct Next {
    sequence: u64,
    index: u64,
    region: RegionMapping<Writable>,
}

/// What `Writer::prepare_segment_at` returns.
pub(crate) type PreparedSegment = (
    File,
    RegionMapping<Writable>,
    RegionMapping<Writable>,
    u64,
    u64,
    usize,
);

/// What creating a segment of this channel takes, all but its base, stamped at the roll.
pub(crate) struct SegmentSpec {
    pub(crate) base_path: PathBuf,
    pub(crate) file_roll_size: u64,
    pub(crate) mtu: u64,
    pub(crate) channel_name: [u8; CHANNEL_NAME_MAX],
    pub(crate) generation: u64,
}

struct NextSegment {
    sequence: u64,
    identity: Identity,
    path: PathBuf,
    segment: PreparedSegment,
}

struct NextSegments {
    /// Segments up to this one belong to the writer: taken, or created by it.
    claimed: u64,
    ready: Option<NextSegment>,
    /// A segment that could not be created ahead (no space, no permission): not tried again,
    /// the writer creates it at the roll as it does without a helper.
    refused: Option<u64>,
}

pub(crate) struct Shared {
    stop: AtomicBool,
    /// Set if the thread stopped early. The writer then maps every region itself, and unmaps and
    /// deletes what it would have handed over.
    pub(crate) failure: Arc<Failure>,
    region_size: usize,
    /// No region is prepared at or past this index: a rolling segment ends there.
    regions_per_file: Option<u64>,
    unmap: Unmap,
    pub(crate) segment: Mutex<Segment>,
    write_position: AtomicPtr<AtomicU64>,
    current_ptr: AtomicPtr<u8>,
    current_index: AtomicU64,
    current_sequence: AtomicU64,
    next: Mutex<Option<Next>>,
    /// Mappings the writer left, with the segment they belong to.
    retired: Mutex<Vec<(u64, RegionMapping<Writable>)>>,
    /// Set for a rolling channel: its next segment is prepared ahead.
    spec: Option<SegmentSpec>,
    /// Held for the whole of a segment's creation, so the writer never creates the same one
    /// alongside: both would claim the `.partial` name.
    next_segment: Mutex<NextSegments>,
    /// Segments retention dropped, to delete.
    doomed: Mutex<Vec<PathBuf>>,
    /// Tests: make the thread stop as it would on an old kernel or a bug.
    #[cfg(test)]
    pub(crate) inject: crate::helper::Inject,
    /// Tests: fail the thread's handle on the segment the writer rolls to, as `EMFILE` would.
    #[cfg(test)]
    pub(crate) fail_clone: AtomicBool,
    /// Tests: fail creating the next segment ahead, as `ENOSPC` would, and count the attempts.
    #[cfg(test)]
    pub(crate) fail_prepare: AtomicBool,
    #[cfg(test)]
    pub(crate) prepare_attempts: AtomicU64,
}

pub(crate) struct Prefaulter {
    pub(crate) shared: Arc<Shared>,
    thread: Thread,
}

/// Where the writer is. `write_position` must stay mapped until it is retired through
/// [`Prefaulter::rolled`] or the prefaulter is dropped.
pub(crate) struct Position<'a> {
    pub(crate) sequence: u64,
    pub(crate) instance: InstanceId,
    pub(crate) file: &'a File,
    pub(crate) file_len: u64,
    pub(crate) region: &'a mut RegionMapping<Writable>,
    pub(crate) index: u64,
    pub(crate) write_position: &'a AtomicU64,
}

impl Prefaulter {
    pub(crate) fn start(
        at: Position<'_>,
        region_size: usize,
        regions_per_file: Option<u64>,
        spec: Option<SegmentSpec>,
        unmap: Unmap,
        helper: Helper,
    ) -> io::Result<Self> {
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            failure: Arc::default(),
            region_size,
            regions_per_file,
            unmap,
            segment: Mutex::new(Segment {
                sequence: at.sequence,
                instance: at.instance,
                file: at.file.try_clone()?,
                len: at.file_len,
            }),
            write_position: AtomicPtr::new(at.write_position as *const AtomicU64 as *mut _),
            current_ptr: AtomicPtr::new(at.region.as_mut_ptr()),
            current_index: AtomicU64::new(at.index),
            current_sequence: AtomicU64::new(at.sequence),
            next: Mutex::new(None),
            retired: Mutex::new(Vec::with_capacity(64)),
            spec,
            next_segment: Mutex::new(NextSegments {
                claimed: at.sequence,
                ready: None,
                refused: None,
            }),
            doomed: Mutex::new(Vec::with_capacity(4)),
            #[cfg(test)]
            inject: Default::default(),
            #[cfg(test)]
            fail_clone: AtomicBool::new(false),
            #[cfg(test)]
            fail_prepare: AtomicBool::new(false),
            #[cfg(test)]
            prepare_attempts: AtomicU64::new(0),
        });
        let for_thread = shared.clone();
        let thread = helper.spawn("xch-prefault", shared.failure.clone(), move || {
            run(&for_thread)
        })?;
        Ok(Self { shared, thread })
    }

    /// Region `index` of segment `sequence`, if it was mapped and populated ahead.
    pub(crate) fn take(&self, sequence: u64, index: u64) -> Option<RegionMapping<Writable>> {
        let mut next = lock(&self.shared.next);
        match next.take() {
            Some(n) if (n.sequence, n.index) == (sequence, index) => Some(n.region),
            other => {
                *next = other;
                None
            }
        }
    }

    /// Segment `sequence`, if it was created ahead under its `.partial` name, with the identity
    /// it was created with.
    pub(crate) fn take_segment(&self, sequence: u64) -> Option<(Identity, PreparedSegment)> {
        let mut next = lock(&self.shared.next_segment);
        next.claimed = next.claimed.max(sequence);
        match next.ready.take() {
            Some(n) if n.sequence == sequence => Some((n.identity, n.segment)),
            other => {
                next.ready = other;
                None
            }
        }
    }

    /// Delete `path` here rather than on the writer: freeing a large file's blocks takes time.
    pub(crate) fn unlink(&self, path: PathBuf) {
        lock(&self.shared.doomed).push(path);
        self.release_if_stopped();
    }

    /// If the thread has stopped, do what it did at the top of each iteration, on the writer:
    /// unmap the mappings the writer left (only those of earlier segments under
    /// `Unmap::AtFileRoll`) and delete the segments retention dropped.
    #[inline]
    fn release_if_stopped(&self) {
        let shared = &self.shared;
        if !shared.failure.stopped() {
            return;
        }
        let current = shared.current_sequence.load(Ordering::Relaxed);
        let mut retired = lock(&shared.retired);
        match shared.unmap {
            Unmap::Immediate => retired.clear(),
            Unmap::AtFileRoll => retired.retain(|(s, _)| *s >= current),
        }
        drop(retired);
        for path in lock(&shared.doomed).drain(..) {
            let _ = std::fs::remove_file(path);
        }
    }

    /// The writer moved to `region`; `left` is unmapped here rather than on the writer.
    pub(crate) fn moved(
        &self,
        region: &mut RegionMapping<Writable>,
        index: u64,
        left: RegionMapping<Writable>,
    ) {
        let shared = &self.shared;
        shared
            .current_ptr
            .store(region.as_mut_ptr(), Ordering::Release);
        shared.current_index.store(index, Ordering::Release);
        let sequence = shared.current_sequence.load(Ordering::Relaxed);
        lock(&shared.retired).push((sequence, left));
        self.release_if_stopped();
    }

    /// The writer rolled to the segment at `at`, leaving the old segment's mappings `left`.
    pub(crate) fn rolled(
        &self,
        at: Position<'_>,
        left: impl IntoIterator<Item = RegionMapping<Writable>>,
    ) {
        let shared = &self.shared;
        let old = shared.current_sequence.load(Ordering::Relaxed);
        let file = at.file.try_clone();
        #[cfg(test)]
        let file = file.and_then(|f| match shared.fail_clone.load(Ordering::Acquire) {
            true => Err(io::Error::from_raw_os_error(libc::EMFILE)),
            false => Ok(f),
        });
        // Without a handle of its own (out of file descriptors), the thread stays on the old
        // segment: it prepares nothing in the new one, and the writer grows it itself.
        if let Ok(file) = file {
            *lock(&shared.segment) = Segment {
                sequence: at.sequence,
                instance: at.instance,
                file,
                len: at.file_len,
            };
        }
        shared.write_position.store(
            at.write_position as *const AtomicU64 as *mut _,
            Ordering::Release,
        );
        shared
            .current_ptr
            .store(at.region.as_mut_ptr(), Ordering::Release);
        shared.current_index.store(at.index, Ordering::Release);
        shared
            .current_sequence
            .store(at.sequence, Ordering::Release);
        let stale = lock(&shared.next).take();
        let mut retired = lock(&shared.retired);
        retired.extend(left.into_iter().map(|region| (old, region)));
        retired.extend(stale.map(|n| (old, n.region)));
        drop(retired);
        self.release_if_stopped();
    }
}

impl Drop for Prefaulter {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        self.thread.join();
        if let Some(unused) = lock(&self.shared.next_segment).ready.take() {
            let _ = std::fs::remove_file(unused.path);
        }
        for path in lock(&self.shared.doomed).drain(..) {
            let _ = std::fs::remove_file(path);
        }
    }
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn run(shared: &Shared) -> io::Result<()> {
    let size = shared.region_size;
    let page = page_size();
    let mut unmapping = Vec::with_capacity(64);
    let mut deleting = Vec::with_capacity(4);
    let (mut done_at, mut done_to) = ((u64::MAX, u64::MAX), 0usize);
    let mut next_head_done = (u64::MAX, u64::MAX);
    let (mut since, mut since_at) = (Instant::now(), (u64::MAX, 0u64));
    let mut ahead = AHEAD_MIN;
    loop {
        {
            let current = shared.current_sequence.load(Ordering::Acquire);
            let mut retired = lock(&shared.retired);
            match shared.unmap {
                Unmap::Immediate => std::mem::swap(&mut *retired, &mut unmapping),
                Unmap::AtFileRoll => {
                    unmapping.extend(retired.extract_if(.., |(s, _)| *s < current))
                }
            }
        }
        for (_, region) in &unmapping {
            crate::helper::drop_page_tables(region.as_ptr(), region.region_size());
        }
        unmapping.clear();
        std::mem::swap(&mut *lock(&shared.doomed), &mut deleting);
        for path in deleting.drain(..) {
            let _ = std::fs::remove_file(path);
        }
        if shared.stop.load(Ordering::Acquire) {
            return Ok(());
        }
        #[cfg(test)]
        shared.inject.fire()?;

        let sequence = shared.current_sequence.load(Ordering::Acquire);
        let index = shared.current_index.load(Ordering::Acquire);
        let base = shared.current_ptr.load(Ordering::Acquire);
        // Safety: a `write_position` the writer leaves is retired, and unmapped only at the top
        // of a later iteration.
        let wp = unsafe { &*shared.write_position.load(Ordering::Acquire) };
        let pos = wp.load(Ordering::Acquire) as usize;

        let elapsed = since.elapsed();
        if since_at.0 != sequence {
            (since, since_at) = (Instant::now(), (sequence, pos as u64));
        } else if elapsed >= AHEAD_TIME {
            let moved = (pos as u64).saturating_sub(since_at.1) as f64;
            let in_window = moved * AHEAD_TIME.as_secs_f64() / elapsed.as_secs_f64();
            ahead = (in_window as usize).clamp(AHEAD_MIN, AHEAD_MAX);
            (since, since_at) = (Instant::now(), (sequence, pos as u64));
        }

        let off = if (pos / size) as u64 == index {
            pos % size
        } else {
            0
        };
        if (sequence, index) != done_at {
            done_at = (sequence, index);
            done_to = off / page * page;
        }
        let target = (off + ahead).div_ceil(page).saturating_mul(page).min(size);
        if done_to < target && !base.is_null() {
            populate(base.wrapping_add(done_to), target - done_to)?;
            done_to = target;
        }
        if off >= size / 2 {
            prepare(shared, sequence, index + 1);
        }
        if size - off < ahead && next_head_done != (sequence, index + 1) {
            let head = lock(&shared.next)
                .as_ref()
                .filter(|n| (n.sequence, n.index) == (sequence, index + 1))
                .map(|n| n.region.as_ptr() as *mut u8);
            // Taken by the writer or retired meanwhile, the region is still unmapped only here.
            if let Some(head) = head {
                populate(head, ahead.min(size))?;
                next_head_done = (sequence, index + 1);
            }
        }
        if let Some(spec) = &shared.spec
            && shared.regions_per_file == Some(index + 1)
            && off >= size / 2
        {
            prepare_segment(shared, spec, sequence + 1, ahead)?;
        }
        std::thread::sleep(POLL);
    }
}

fn prepare_segment(
    shared: &Shared,
    spec: &SegmentSpec,
    sequence: u64,
    ahead: usize,
) -> io::Result<()> {
    let mut next = lock(&shared.next_segment);
    if sequence <= next.claimed
        || next.refused == Some(sequence)
        || next.ready.as_ref().is_some_and(|n| n.sequence == sequence)
    {
        return Ok(());
    }
    // The parent the new file names: the segment being written, if the thread is still on it.
    let parent = {
        let segment = lock(&shared.segment);
        (segment.sequence + 1 == sequence).then_some(segment.instance)
    };
    let Some(parent) = parent else {
        return Ok(());
    };
    let identity = Identity::successor(InstanceId::fresh()?, parent, sequence - 1);
    let path = make_partial_channel_file_path(&spec.base_path, sequence)?;
    #[cfg(test)]
    shared.prepare_attempts.fetch_add(1, Ordering::Relaxed);
    let prepared = Writer::prepare_segment_at(
        &path,
        sequence,
        shared.region_size,
        spec.file_roll_size,
        spec.mtu,
        &spec.channel_name,
        0,
        spec.generation,
        &identity,
    );
    #[cfg(test)]
    let prepared = prepared.and_then(|s| match shared.fail_prepare.load(Ordering::Acquire) {
        true => Err(io::Error::from_raw_os_error(libc::ENOSPC)),
        false => Ok(s),
    });
    let Ok(mut segment) = prepared else {
        // Out of space or not allowed: retrying every pass would only repeat it. Leave no
        // `.partial` behind; the writer creates this segment at the roll, and reports its error.
        let _ = std::fs::remove_file(&path);
        next.refused = Some(sequence);
        return Ok(());
    };
    let populated = populate(segment.2.as_mut_ptr(), ahead.min(shared.region_size));
    let stale = next.ready.replace(NextSegment {
        sequence,
        identity,
        path,
        segment,
    });
    drop(next);
    drop(stale);
    populated
}

fn prepare(shared: &Shared, sequence: u64, index: u64) {
    if shared.regions_per_file.is_some_and(|n| index >= n) {
        return;
    }
    if matches!(&*lock(&shared.next), Some(n) if (n.sequence, n.index) == (sequence, index)) {
        return;
    }
    let size = shared.region_size;
    let mapped = {
        let mut segment = lock(&shared.segment);
        if segment.sequence != sequence {
            return;
        }
        let needed = (index + 1) * size as u64;
        if segment.len < needed {
            if segment.file.set_len(needed).is_err() {
                return;
            }
            segment.len = needed;
        }
        RegionMapping::create_writable(&segment.file, index * size as u64, size)
    };
    let Ok(region) = mapped else {
        return;
    };
    let stale = lock(&shared.next).replace(Next {
        sequence,
        index,
        region,
    });
    drop(stale);
}

#[cfg(target_os = "linux")]
fn populate(at: *mut u8, len: usize) -> io::Result<()> {
    // Safety: `at..at + len` lies inside a live mapping, page-aligned; populating writes nothing.
    match unsafe { libc::madvise(at.cast(), len, libc::MADV_POPULATE_WRITE) } {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

#[cfg(not(target_os = "linux"))]
fn populate(_at: *mut u8, _len: usize) -> io::Result<()> {
    Ok(())
}
