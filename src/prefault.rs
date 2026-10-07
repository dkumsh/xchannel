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
//! Halfway through a rolling segment's last region the thread also creates the next segment and
//! installs it under its final name, still `PREPARED`, so the writer's roll only stamps its base
//! and publishes it. It also deletes the segments retention drops.
//!
//! The thread follows the writer across file rolls. Mappings the writer leaves, the old segment's
//! included, are unmapped only here and only at the top of an iteration, before the thread reads
//! the writer's pointers again, so a pointer it has read stays valid until it is done with it.
//!
//! If the thread stops early (an error, such as `EINVAL` from `madvise` before Linux 5.14, or a
//! panic), the writer notices at its next hand-over and from then on unmaps and deletes for itself,
//! what it queued before included, as it does without a helper.

use crate::helper::{Bounded, Failure, Thread};
use crate::region::{RegionMapping, Writable, page_size};
use crate::v4::{Identity, InstanceId};
use crate::{
    CHANNEL_NAME_MAX, Helper, Unmap, Writer, make_attempt_path, make_channel_file_path, v4,
};
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
/// Bound of the queues of descriptors to close.
const SMALL_QUEUE: usize = 8;
/// Bound of the queue of segments to delete: past it, retention deletes on the writer, so
/// `keep_files` is exceeded by at most this many.
const DOOMED_QUEUE: usize = 2;

/// Bound of the queue of mappings to unmap: a file's worth and the next's under
/// `Unmap::AtFileRoll`, unbounded if the file never rolls (the user's choice).
fn retire_bound(unmap: Unmap, regions_per_file: Option<u64>) -> usize {
    match (unmap, regions_per_file) {
        (Unmap::Immediate, _) => 64,
        (Unmap::AtFileRoll, Some(n)) => 2 * n as usize + 8,
        (Unmap::AtFileRoll, None) => usize::MAX,
    }
}

/// `in_flight` when nothing is being installed.
pub(crate) const NONE: u64 = u64::MAX;

/// The segment being written, and the lock both threads take to grow it: a grow from a stale view
/// would truncate what the other mapped. If the thread could not open the segment the writer
/// rolled to, this stays on the old one, and the thread leaves the new one to the writer.
pub(crate) struct Segment {
    pub(crate) sequence: u64,
    /// Its instance ID: the parent its successor names.
    instance: InstanceId,
    pub(crate) file: Arc<File>,
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

pub(crate) struct NextSegment {
    sequence: u64,
    identity: Identity,
    path: PathBuf,
    segment: PreparedSegment,
    /// The thread's own handle, so the roll needs no `dup`.
    worker: Arc<File>,
}

pub(crate) struct NextSegments {
    /// Installed ahead, not yet taken.
    pub(crate) ready: Option<NextSegment>,
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
    pub(crate) retired: Mutex<Bounded<(u64, RegionMapping<Writable>)>>,
    /// Hand-overs the writer did itself because a queue was full.
    pub(crate) saturated: AtomicU64,
    /// Set for a rolling channel: its next segment is prepared ahead.
    spec: Option<SegmentSpec>,
    /// Held only to hand a segment over; the writer only `try_lock`s it.
    pub(crate) next_segment: Mutex<NextSegments>,
    /// The newest segment the writer has rolled to, or is rolling to.
    claimed: AtomicU64,
    /// The segment the thread may be installing, or `NONE`. With `claimed` (both SeqCst), it
    /// keeps a late install from bringing back a segment retention has removed.
    pub(crate) in_flight: AtomicU64,
    base_path: PathBuf,
    /// Segments retention dropped, to delete.
    pub(crate) doomed: Mutex<Bounded<u64>>,
    /// Descriptors the writer left, to close.
    pub(crate) retired_files: Mutex<Bounded<File>>,
    /// The thread's handles on segments the writer left, to close.
    retired_workers: Mutex<Bounded<Arc<File>>>,
    /// A segment the writer rolled to, waiting for `segment` to be free. Writer-side only.
    handoff: Mutex<Option<Segment>>,
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        base_path: PathBuf,
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
                file: Arc::new(at.file.try_clone()?),
                len: at.file_len,
            }),
            write_position: AtomicPtr::new(at.write_position as *const AtomicU64 as *mut _),
            current_ptr: AtomicPtr::new(at.region.as_mut_ptr()),
            current_index: AtomicU64::new(at.index),
            current_sequence: AtomicU64::new(at.sequence),
            next: Mutex::new(None),
            retired: Mutex::new(Bounded::new(retire_bound(unmap, regions_per_file))),
            saturated: AtomicU64::new(0),
            spec,
            next_segment: Mutex::new(NextSegments {
                ready: None,
                refused: None,
            }),
            claimed: AtomicU64::new(at.sequence),
            in_flight: AtomicU64::new(NONE),
            base_path,
            doomed: Mutex::new(Bounded::new(DOOMED_QUEUE)),
            retired_files: Mutex::new(Bounded::new(SMALL_QUEUE)),
            retired_workers: Mutex::new(Bounded::new(SMALL_QUEUE)),
            handoff: Mutex::new(None),
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

    /// The writer is rolling to `sequence`.
    pub(crate) fn claim(&self, sequence: u64) {
        self.shared.claimed.store(sequence, Ordering::SeqCst);
    }

    /// Segment `sequence`, if it was installed ahead, with its identity and the thread's handle on
    /// it. Never waits: a busy hand-over is a miss.
    pub(crate) fn try_take_segment(
        &self,
        sequence: u64,
    ) -> Option<(Identity, PreparedSegment, Arc<File>)> {
        let mut next = match self.shared.next_segment.try_lock() {
            Ok(next) => next,
            Err(std::sync::TryLockError::Poisoned(e)) => e.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        match next.ready.take() {
            Some(n) if n.sequence == sequence => Some((n.identity, n.segment, n.worker)),
            other => {
                next.ready = other;
                None
            }
        }
    }

    /// Whether the thread may still install segment `sequence`.
    pub(crate) fn installing(&self, sequence: u64) -> bool {
        self.shared.in_flight.load(Ordering::SeqCst) == sequence
    }

    /// Delete segment `sequence` here rather than on the writer.
    pub(crate) fn unlink(&self, sequence: u64) {
        let full = lock(&self.shared.doomed).push(sequence);
        if let Err(sequence) = full {
            self.saturated();
            delete(&self.shared, &mut vec![sequence]);
        }
        self.release_if_stopped();
    }

    fn saturated(&self) {
        self.shared.saturated.fetch_add(1, Ordering::Relaxed);
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
            Unmap::Immediate => retired.items().clear(),
            Unmap::AtFileRoll => retired.items().retain(|(s, _)| *s >= current),
        }
        drop(retired);
        lock(&shared.retired_files).items().clear();
        lock(&shared.retired_workers).items().clear();
        delete(shared, lock(&shared.doomed).items());
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
        let full = lock(&shared.retired).push((sequence, left));
        if full.is_err() {
            self.saturated(); // unmapped here, on drop
        }
        self.hand_off();
        self.release_if_stopped();
    }

    /// Give the thread the segment the writer rolled to, if `segment` is free now.
    fn hand_off(&self) {
        let mut handoff = lock(&self.shared.handoff);
        if handoff.is_none() {
            return;
        }
        let mut segment = match self.shared.segment.try_lock() {
            Ok(segment) => segment,
            Err(std::sync::TryLockError::Poisoned(e)) => e.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return,
        };
        let left = std::mem::replace(&mut *segment, handoff.take().expect("checked"));
        drop(segment);
        if lock(&self.shared.retired_workers).push(left.file).is_err() {
            self.saturated();
        }
    }

    /// The writer rolled to the segment at `at`, leaving the old segment's mappings `left` and its
    /// descriptor `left_file`. `worker` is the thread's handle on the new one, if it made it.
    pub(crate) fn rolled(
        &self,
        at: Position<'_>,
        left: impl IntoIterator<Item = RegionMapping<Writable>>,
        left_file: File,
        worker: Option<Arc<File>>,
    ) {
        let shared = &self.shared;
        let old = shared.current_sequence.load(Ordering::Relaxed);
        let file = match worker {
            Some(worker) => Ok(worker),
            None => at.file.try_clone().map(Arc::new),
        };
        #[cfg(test)]
        let file = file.and_then(|f| match shared.fail_clone.load(Ordering::Acquire) {
            true => Err(io::Error::from_raw_os_error(libc::EMFILE)),
            false => Ok(f),
        });
        // Without a handle (out of descriptors) the thread stays on the old segment.
        if let Ok(file) = file {
            *lock(&shared.handoff) = Some(Segment {
                sequence: at.sequence,
                instance: at.instance,
                file,
                len: at.file_len,
            });
            self.hand_off();
        }
        if lock(&shared.retired_files).push(left_file).is_err() {
            self.saturated();
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
        let mut full = false;
        for region in left.into_iter().chain(stale.map(|n| n.region)) {
            full |= retired.push((old, region)).is_err();
        }
        drop(retired);
        if full {
            self.saturated();
        }
        self.release_if_stopped();
    }
}

impl Drop for Prefaulter {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        self.thread.join();
        // An unused successor goes, but only while unpublished: the writer may have adopted it.
        if let Some(unused) = lock(&self.shared.next_segment).ready.take()
            && crate::is_published(unused.segment.1.as_ptr()).is_ok_and(|published| !published)
        {
            let _ = std::fs::remove_file(unused.path);
        }
        delete(&self.shared, lock(&self.shared.doomed).items());
    }
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Unlink the segments in `doomed`, emptying it.
fn delete(shared: &Shared, doomed: &mut Vec<u64>) {
    for sequence in doomed.drain(..) {
        if let Ok(path) = make_channel_file_path(&shared.base_path, sequence) {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn run(shared: &Shared) -> io::Result<()> {
    let size = shared.region_size;
    let page = page_size();
    let mut unmapping =
        Vec::with_capacity(retire_bound(shared.unmap, shared.regions_per_file).min(4096));
    let mut deleting = Vec::with_capacity(DOOMED_QUEUE);
    let mut closing = Vec::with_capacity(SMALL_QUEUE);
    let mut releasing = Vec::with_capacity(SMALL_QUEUE);
    let (mut done_at, mut done_to) = ((u64::MAX, u64::MAX), 0usize);
    let mut next_head_done = (u64::MAX, u64::MAX);
    let (mut since, mut since_at) = (Instant::now(), (u64::MAX, 0u64));
    let mut ahead = AHEAD_MIN;
    loop {
        #[cfg(test)]
        if shared.inject.stalled() {
            if shared.stop.load(Ordering::Acquire) {
                return Ok(());
            }
            std::thread::sleep(POLL);
            continue;
        }
        {
            let current = shared.current_sequence.load(Ordering::Acquire);
            let mut retired = lock(&shared.retired);
            match shared.unmap {
                Unmap::Immediate => retired.swap(&mut unmapping),
                Unmap::AtFileRoll => {
                    unmapping.extend(retired.items().extract_if(.., |(s, _)| *s < current))
                }
            }
        }
        for (_, region) in &unmapping {
            crate::helper::drop_page_tables(region.as_ptr(), region.region_size());
        }
        unmapping.clear();
        lock(&shared.doomed).swap(&mut deleting);
        delete(shared, &mut deleting);
        lock(&shared.retired_files).swap(&mut closing);
        closing.clear();
        lock(&shared.retired_workers).swap(&mut releasing);
        releasing.clear();
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
    {
        let mut next = lock(&shared.next_segment);
        if next
            .ready
            .as_ref()
            .is_some_and(|n| n.sequence <= shared.claimed.load(Ordering::SeqCst))
        {
            next.ready = None; // passed by the writer
        }
        if next.refused == Some(sequence)
            || next.ready.as_ref().is_some_and(|n| n.sequence == sequence)
        {
            return Ok(());
        }
    }
    let parent = {
        let segment = lock(&shared.segment);
        (segment.sequence + 1 == sequence).then_some(segment.instance)
    };
    let Some(parent) = parent else {
        return Ok(());
    };
    // Announce, then check: the writer claims, then checks before retention unlinks.
    shared.in_flight.store(sequence, Ordering::SeqCst);
    let installed = if shared.claimed.load(Ordering::SeqCst) >= sequence {
        Ok(None)
    } else {
        install_ahead(shared, spec, sequence, parent, ahead)
    };
    shared.in_flight.store(NONE, Ordering::SeqCst);
    let Some((identity, path, segment, worker)) = installed? else {
        return Ok(());
    };
    let mut next = lock(&shared.next_segment);
    if shared.claimed.load(Ordering::SeqCst) < sequence {
        next.ready = Some(NextSegment {
            sequence,
            identity,
            path,
            segment,
            worker,
        });
    }
    Ok(())
}

/// Create segment `sequence` under a private name and install it under its final name.
/// `None` if the writer installed it first or it could not be created (not retried).
#[allow(clippy::type_complexity)]
fn install_ahead(
    shared: &Shared,
    spec: &SegmentSpec,
    sequence: u64,
    parent: InstanceId,
    ahead: usize,
) -> io::Result<Option<(Identity, PathBuf, PreparedSegment, Arc<File>)>> {
    let identity = Identity::successor(InstanceId::fresh()?, parent, sequence - 1);
    let attempt = make_attempt_path(&spec.base_path, sequence, identity.instance)?;
    let path = make_channel_file_path(&spec.base_path, sequence)?;
    #[cfg(test)]
    shared.prepare_attempts.fetch_add(1, Ordering::Relaxed);
    let prepared = Writer::prepare_segment_at(
        &attempt,
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
    let refuse = || {
        let _ = std::fs::remove_file(&attempt);
        lock(&shared.next_segment).refused = Some(sequence);
        Ok(None)
    };
    let Ok(mut segment) = prepared else {
        return refuse(); // no space or permission: the writer creates it at the roll
    };
    let populated = populate(segment.2.as_mut_ptr(), ahead.min(shared.region_size));
    match v4::install_no_replace(&attempt, &path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            let _ = std::fs::remove_file(&attempt);
            return Ok(None);
        }
        Err(_) => return refuse(),
    }
    populated?;
    let worker = Arc::new(segment.0.try_clone()?);
    Ok(Some((identity, path, segment, worker)))
}

fn prepare(shared: &Shared, sequence: u64, index: u64) {
    if shared.regions_per_file.is_some_and(|n| index >= n) {
        return;
    }
    if matches!(&*lock(&shared.next), Some(n) if (n.sequence, n.index) == (sequence, index)) {
        return;
    }
    let size = shared.region_size;
    let file = {
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
        segment.file.clone()
    };
    let Ok(region) = RegionMapping::create_writable(&file, index * size as u64, size) else {
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
