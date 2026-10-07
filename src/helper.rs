//! The background thread a writer or reader can hand its mapping work to.

use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;

/// Where a writer's or reader's helper thread runs.
///
/// A thread inherits the CPU mask of the thread that spawns it, so a helper started from a thread
/// pinned to an isolated core would land on that core and compete with it. Name the core.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Helper {
    core: Option<usize>,
}

impl Helper {
    /// Pin the helper to `core`, such as a housekeeping core shared with other background threads.
    /// Building the writer or reader fails if the helper cannot be pinned there: a core the
    /// process may not run on, or one past the highest the OS can name.
    pub fn on_core(core: usize) -> Self {
        Self { core: Some(core) }
    }

    /// Leave the helper on the CPU mask of the thread that builds the writer or reader.
    pub fn inherit() -> Self {
        Self { core: None }
    }

    /// Start the thread and wait until it is pinned and named, so a failure to pin is the
    /// builder's error. `f` runs only once it is; how it ends is recorded in `failure`.
    pub(crate) fn spawn<F: FnOnce() -> io::Result<()> + Send + 'static>(
        self,
        name: &str,
        failure: impl AsRef<Failure> + Send + 'static,
        f: F,
    ) -> io::Result<Thread> {
        let (started, pinned) = mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name(name.into())
            .spawn(move || {
                let pinning = self.core.map_or(Ok(()), pin);
                let go = pinning.is_ok();
                let _ = started.send(pinning);
                if go {
                    failure.as_ref().guard(f);
                }
            })?;
        let pinning = pinned
            .recv()
            .unwrap_or_else(|_| Err(io::Error::other("helper thread exited before it started")));
        match pinning {
            Ok(()) => Ok(Thread(Some(AssertUnwindSafe(thread)))),
            Err(e) => {
                let _ = thread.join();
                Err(e)
            }
        }
    }
}

/// A helper's join handle. Joined only when its writer or reader drops it, so a panic elsewhere
/// cannot leave it half-updated: asserting unwind safety keeps `Writer` and `Reader` as
/// `UnwindSafe` and `RefUnwindSafe` as they were before they had a helper.
pub(crate) struct Thread(Option<AssertUnwindSafe<JoinHandle<()>>>);

impl Thread {
    pub(crate) fn join(&mut self) {
        if let Some(thread) = self.0.take() {
            let _ = thread.0.join();
        }
    }
}

/// Why a helper thread stopped early, if it did. Once it has, the writer or reader takes back
/// the work it handed over, including what it queued before it noticed.
#[derive(Default)]
pub(crate) struct Failure {
    stopped: AtomicBool,
    why: Mutex<Option<(Option<i32>, io::ErrorKind, String)>>,
}

impl Failure {
    /// Run `f`, recording an error it returns or a panic.
    fn guard(&self, f: impl FnOnce() -> io::Result<()>) {
        let why = match panic::catch_unwind(AssertUnwindSafe(f)) {
            Ok(Ok(())) => return,
            Ok(Err(e)) => (e.raw_os_error(), e.kind(), e.to_string()),
            Err(payload) => {
                let message = payload
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_default();
                (
                    None,
                    io::ErrorKind::Other,
                    format!("helper thread panicked: {message}"),
                )
            }
        };
        *self.why.lock().unwrap_or_else(|e| e.into_inner()) = Some(why);
        self.stopped.store(true, Ordering::Release);
    }

    /// Whether the thread stopped early. One load; checked on every hand-over.
    #[inline]
    pub(crate) fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// The error that stopped the thread: the OS error itself, or one of the same kind and text.
    pub(crate) fn error(&self) -> Option<io::Error> {
        if !self.stopped() {
            return None;
        }
        let why = self.why.lock().unwrap_or_else(|e| e.into_inner());
        let (os, kind, text) = why.as_ref()?;
        Some(match os {
            Some(code) => io::Error::from_raw_os_error(*code),
            None => io::Error::new(*kind, text.clone()),
        })
    }
}

/// Tests: make a helper thread stop early at the top of its next iteration, as `madvise` before
/// Linux 5.14 would make it, or as a bug would.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct Inject(std::sync::atomic::AtomicU8);

#[cfg(test)]
impl Inject {
    /// Stop draining until `resume`.
    pub(crate) fn stall(&self) {
        self.0.store(3, Ordering::Release);
    }

    pub(crate) fn resume(&self) {
        self.0.store(0, Ordering::Release);
    }

    pub(crate) fn stalled(&self) -> bool {
        self.0.load(Ordering::Acquire) == 3
    }

    /// Stop in the middle of an iteration, with the writer's pointers read, until `resume`.
    pub(crate) fn stall_mid_iteration(&self) {
        self.0.store(4, Ordering::Release);
    }

    pub(crate) fn stalled_mid_iteration(&self) -> bool {
        self.0.load(Ordering::Acquire) == 4
    }

    pub(crate) fn error(&self) {
        self.0.store(1, Ordering::Release);
    }

    pub(crate) fn panic(&self) {
        self.0.store(2, Ordering::Release);
    }

    pub(crate) fn fire(&self) -> io::Result<()> {
        match self.0.load(Ordering::Acquire) {
            1 => Err(io::Error::from_raw_os_error(libc::EINVAL)),
            2 => panic!("injected"),
            _ => Ok(()),
        }
    }
}

/// A queue of work handed to a helper, never grown past its bound on the hot thread: when full,
/// the caller does the work itself.
pub(crate) struct Bounded<T> {
    items: Vec<T>,
    bound: usize,
}

impl<T> Bounded<T> {
    /// `bound` items fit without allocating; `usize::MAX` grows as needed (the user's choice).
    pub(crate) fn new(bound: usize) -> Self {
        Self {
            items: Vec::with_capacity(if bound == usize::MAX { 64 } else { bound }),
            bound,
        }
    }

    /// Queue `item`, or hand it back if the queue is full.
    pub(crate) fn push(&mut self, item: T) -> Result<(), T> {
        if self.items.len() >= self.bound {
            return Err(item);
        }
        self.items.push(item);
        Ok(())
    }

    pub(crate) fn items(&mut self) -> &mut Vec<T> {
        &mut self.items
    }

    /// Swap contents with `other`, which must hold `bound` without allocating.
    pub(crate) fn swap(&mut self, other: &mut Vec<T>) {
        std::mem::swap(&mut self.items, other);
    }

    pub(crate) fn capacity(&self) -> usize {
        self.items.capacity()
    }
}

/// When a writer or reader releases the regions it has moved past.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Unmap {
    /// As soon as it leaves them: on the helper if it has one, otherwise on its own thread.
    #[default]
    Immediate,
    /// Not until it rolls to the next file. Unmapping a region from any thread takes the process's
    /// memory-map lock, which stalls a page fault or `mmap` on every other thread meanwhile, and
    /// interrupts every core running a thread of the process to flush its TLB. This keeps both
    /// away from the writer or reader between rolls, at the cost of keeping up to a file's worth
    /// of address space mapped. With no file roll configured, regions stay mapped until the writer
    /// or reader is dropped.
    AtFileRoll,
}

/// Drop a mapping's page-table entries ahead of unmapping it. `munmap` tears them down holding the
/// process's memory-map lock for writing, which stalls a page fault on every other thread for as
/// long as it takes; `MADV_DONTNEED` holds it only for reading, and leaves `munmap` nothing to do.
/// The file's pages stay in the page cache.
pub(crate) fn drop_page_tables(at: *const u8, len: usize) {
    #[cfg(target_os = "linux")]
    // Safety: `at..at + len` is a live shared file mapping that nothing reads any more; were it
    // read, the page would fault back in from the page cache unchanged.
    unsafe {
        libc::madvise(at as *mut _, len, libc::MADV_DONTNEED);
    }
}

#[cfg(target_os = "linux")]
fn pin(core: usize) -> io::Result<()> {
    if core >= libc::CPU_SETSIZE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "helper core {core} is past the highest core the OS can name ({})",
                libc::CPU_SETSIZE - 1
            ),
        ));
    }
    // Safety: a zeroed cpu_set_t is empty, and `core` is inside it.
    let set = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        set
    };
    // Safety: `set` is a valid cpu_set_t of the size given.
    match unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) } {
        0 => Ok(()),
        _ => {
            let e = io::Error::last_os_error();
            Err(io::Error::new(
                e.kind(),
                format!("pinning the helper to core {core}: {e}"),
            ))
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn pin(_core: usize) -> io::Result<()> {
    Ok(())
}
