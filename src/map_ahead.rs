//! Keeping the reader's next region mapped ahead of it, and its old regions unmapped, off its
//! thread.
//!
//! Moving to a region costs a reader an `mmap` and a minor fault on each page it then touches;
//! leaving one costs a `munmap` that tears down every page-table entry the region filled, which
//! for a large region is the longest stall a reader sees. Here a thread maps the region after the
//! reader's and populates it with `MADV_POPULATE_READ`, so the reader takes it ready and faults
//! nothing, and drops the regions the reader hands back. A region not ready in time is mapped by
//! the reader as before.
//!
//! Near the end of a segment the thread also opens the next one, once it is installed under its
//! final name (published or still `PREPARED`), and maps and populates its first region. The reader
//! takes it at the roll if it is the file the committed `Roll` names: its instance ID and parent,
//! read from its header here, checked in memory there, with no `stat`.

use crate::helper::{Bounded, Failure, Thread, drop_page_tables};
use crate::region::{ReadOnly, RegionMapping, page_size};
use crate::v4::Identity;
use crate::{Helper, MappedRegion, make_channel_file_path};
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

const POLL: Duration = Duration::from_micros(200);
/// How often the next segment is looked for while it does not exist yet.
const LOOK_FOR_NEXT: Duration = Duration::from_millis(5);

struct Ready {
    file_sequence: u64,
    region_idx: u64,
    mapping: RegionMapping<ReadOnly>,
}

/// The segment after the reader's, opened and mapped ahead of the roll.
pub(crate) struct NextSegment {
    pub(crate) sequence: u64,
    pub(crate) file: File,
    /// Region 0, ready to push onto the reader's maps.
    pub(crate) region0: Arc<MappedRegion>,
    pub(crate) header: RegionMapping<ReadOnly>,
    /// Who the file says it is, its header already validated; none of it changes.
    pub(crate) identity: Identity,
}

/// What the reader leaves behind for the thread to release; held only to be dropped there.
#[allow(dead_code)]
pub(crate) enum Retired {
    Region(Arc<MappedRegion>),
    Segment(NextSegment),
    File(File),
    Header(RegionMapping<ReadOnly>),
}

pub(crate) struct Shared {
    stop: AtomicBool,
    /// Set if the thread stopped early. The reader then maps every region itself, and drops what
    /// it would have handed over.
    pub(crate) failure: Arc<Failure>,
    region_size: usize,
    base_path: PathBuf,
    want_sequence: AtomicU64,
    want_region: AtomicU64,
    /// A segment the reader rolled into, for the thread to map from: the file it opened, or `None`
    /// when it took the one the thread opened ahead.
    rolled: Mutex<Option<(u64, Option<File>)>>,
    ready: Mutex<Option<Ready>>,
    next_segment: Mutex<Option<NextSegment>>,
    pub(crate) retired: Mutex<Bounded<Retired>>,
    /// Hand-overs the reader did itself because the queue was full.
    pub(crate) saturated: AtomicU64,
    /// Tests: make the thread stop as it would on an old kernel or a bug.
    #[cfg(test)]
    pub(crate) inject: crate::helper::Inject,
    /// Tests: stop with an error once the next segment is opened ahead.
    #[cfg(test)]
    pub(crate) fail_after_open: AtomicBool,
}

pub(crate) struct MapAhead {
    pub(crate) shared: Arc<Shared>,
    thread: Thread,
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl MapAhead {
    pub(crate) fn start(
        helper: Helper,
        base_path: PathBuf,
        file: &File,
        file_sequence: u64,
        region_idx: u64,
        region_size: usize,
    ) -> io::Result<Self> {
        // A file's worth and the next's, held under `Unmap::AtFileRoll` until the roll.
        let regions = file.metadata()?.len() / region_size as u64;
        let retire_bound = (2 * regions as usize + 8).max(64);
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            failure: Arc::default(),
            region_size,
            base_path,
            want_sequence: AtomicU64::new(file_sequence),
            want_region: AtomicU64::new(region_idx + 1),
            rolled: Mutex::new(Some((file_sequence, Some(file.try_clone()?)))),
            ready: Mutex::new(None),
            next_segment: Mutex::new(None),
            retired: Mutex::new(Bounded::new(retire_bound)),
            saturated: AtomicU64::new(0),
            #[cfg(test)]
            inject: Default::default(),
            #[cfg(test)]
            fail_after_open: AtomicBool::new(false),
        });
        let for_thread = shared.clone();
        let thread = helper.spawn("xch-map-ahead", shared.failure.clone(), move || {
            run(&for_thread)
        })?;
        Ok(Self { shared, thread })
    }

    /// Region `region_idx` of segment `file_sequence`, if it was mapped ahead.
    pub(crate) fn take(
        &self,
        file_sequence: u64,
        region_idx: u64,
    ) -> Option<RegionMapping<ReadOnly>> {
        let mut ready = lock(&self.shared.ready);
        match ready.take() {
            Some(r) if r.file_sequence == file_sequence && r.region_idx == region_idx => {
                Some(r.mapping)
            }
            other => {
                *ready = other;
                None
            }
        }
    }

    /// Segment `sequence`, if it was opened ahead.
    pub(crate) fn take_segment(&self, sequence: u64) -> Option<NextSegment> {
        let mut next = lock(&self.shared.next_segment);
        match next.take() {
            Some(n) if n.sequence == sequence => Some(n),
            other => {
                *next = other;
                None
            }
        }
    }

    /// The reader is now in `region_idx` of segment `file_sequence`.
    pub(crate) fn moved(&self, file_sequence: u64, region_idx: u64) {
        self.shared
            .want_region
            .store(region_idx + 1, Ordering::Release);
        self.shared
            .want_sequence
            .store(file_sequence, Ordering::Release);
    }

    /// The reader rolled into segment `file_sequence`, open as `file`, unless the thread opened it
    /// itself.
    pub(crate) fn rolled(&self, file_sequence: u64, file: Option<&File>) {
        let file = match file.map(File::try_clone) {
            Some(Ok(file)) => Some(file),
            Some(Err(_)) => return self.moved(file_sequence, 0),
            None => None,
        };
        *lock(&self.shared.rolled) = Some((file_sequence, file));
        self.moved(file_sequence, 0);
    }

    /// Hand `left` to the thread to release; or, once it has stopped, release it here, with
    /// whatever was handed over before the reader noticed.
    pub(crate) fn retire(&self, left: impl IntoIterator<Item = Retired>) {
        let mut retired = lock(&self.shared.retired);
        if self.shared.failure.stopped() {
            retired.items().clear();
            drop(retired);
            left.into_iter().for_each(drop);
            return;
        }
        let mut full = false;
        for item in left {
            full |= retired.push(item).is_err(); // released here, on drop
        }
        if full {
            self.shared.saturated.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Drop for MapAhead {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        self.thread.join();
    }
}

fn run(shared: &Shared) -> io::Result<()> {
    let size = shared.region_size as u64;
    let mut segment: Option<(u64, File)> = None;
    let mut upcoming: Option<(u64, File)> = None;
    let mut releasing = Vec::with_capacity(lock(&shared.retired).items().capacity());
    let mut done = (u64::MAX, u64::MAX);
    let mut looked_for_next = Instant::now() - LOOK_FOR_NEXT;
    while !shared.stop.load(Ordering::Acquire) {
        #[cfg(test)]
        if shared.inject.stalled() {
            std::thread::sleep(POLL);
            continue;
        }
        #[cfg(test)]
        shared.inject.fire()?;
        // Only the reader says which file a segment is: a file opened ahead may be an unpublished
        // attempt that was abandoned and replaced, and only the reader checks that at the roll.
        if let Some((seq, file)) = lock(&shared.rolled).take() {
            segment = match file {
                Some(file) => Some((seq, file)),
                None => upcoming.take().filter(|(s, _)| *s == seq),
            };
        }
        let file_sequence = shared.want_sequence.load(Ordering::Acquire);
        let region_idx = shared.want_region.load(Ordering::Acquire);
        if let Some((seq, file)) = &segment
            && *seq == file_sequence
        {
            let len = file.metadata()?.len();
            if done != (file_sequence, region_idx) && len >= (region_idx + 1) * size {
                let mapping =
                    RegionMapping::create_read_only(file, region_idx * size, size as usize)?;
                populate(&mapping)?;
                let stale = lock(&shared.ready).replace(Ready {
                    file_sequence,
                    region_idx,
                    mapping,
                });
                drop(stale);
                done = (file_sequence, region_idx);
            }
            let in_last_region = len < (region_idx + 1) * size;
            if in_last_region
                && looked_for_next.elapsed() >= LOOK_FOR_NEXT
                && !matches!(&*lock(&shared.next_segment), Some(n) if n.sequence == file_sequence + 1)
            {
                looked_for_next = Instant::now();
                if let Some(next) = open_next(shared, file_sequence + 1) {
                    upcoming = next.file.try_clone().ok().map(|f| (next.sequence, f));
                    let stale = lock(&shared.next_segment).replace(next);
                    drop(stale);
                    #[cfg(test)]
                    if shared.fail_after_open.load(Ordering::Acquire) {
                        return Err(io::Error::from_raw_os_error(libc::EINVAL));
                    }
                }
            }
        }
        lock(&shared.retired).swap(&mut releasing);
        for left in &releasing {
            match left {
                Retired::Region(region) if Arc::strong_count(region) == 1 => {
                    let mapping = &region.mapping;
                    drop_page_tables(mapping.as_ptr(), mapping.region_size());
                }
                Retired::Segment(next) => drop_page_tables(
                    next.region0.mapping.as_ptr(),
                    next.region0.mapping.region_size(),
                ),
                _ => {}
            }
        }
        releasing.clear();
        std::thread::sleep(POLL);
    }
    Ok(())
}

/// Segment `sequence` opened, its first region mapped and populated, if it is installed yet.
/// A file under the final name is complete, published or not; its identity is read once here.
fn open_next(shared: &Shared, sequence: u64) -> Option<NextSegment> {
    let size = shared.region_size;
    let file = File::open(make_channel_file_path(&shared.base_path, sequence).ok()?).ok()?;
    if file.metadata().ok()?.len() < size as u64 {
        return None;
    }
    let region0 = RegionMapping::create_read_only(&file, 0, size).ok()?;
    let page0 = region0.as_ptr();
    let mh = unsafe { &*(page0 as *const crate::MessageHeader) };
    if mh.parsed_header_type().ok()? != crate::HeaderType::Channel {
        return None;
    }
    crate::validate_channel_header(crate::get_channel_header(page0), size, sequence).ok()?;
    let identity = crate::validate_v4_prefix(page0).ok()?;
    populate(&region0).ok()?;
    let header = RegionMapping::create_read_only(&file, 0, page_size()).ok()?;
    populate(&header).ok()?;
    Some(NextSegment {
        sequence,
        file,
        region0: Arc::new(MappedRegion {
            file_sequence: sequence,
            region_idx: 0,
            mapping: region0,
        }),
        header,
        identity,
    })
}

#[cfg(target_os = "linux")]
fn populate(mapping: &RegionMapping<ReadOnly>) -> io::Result<()> {
    // Safety: the whole mapping, page-aligned; populating reads nothing into the program.
    match unsafe {
        libc::madvise(
            mapping.as_ptr() as *mut _,
            mapping.region_size(),
            libc::MADV_POPULATE_READ,
        )
    } {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

#[cfg(not(target_os = "linux"))]
fn populate(_mapping: &RegionMapping<ReadOnly>) -> io::Result<()> {
    Ok(())
}
