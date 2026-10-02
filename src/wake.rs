//! Futex calls behind reader waking (`WriterBuilder::wake_readers`).
//!
//! The wake word lives in a segment's `ChannelHeader`, in a `MAP_SHARED` file mapping, so the
//! futex is a *shared* one (no `FUTEX_PRIVATE_FLAG`): the kernel keys it on the file's inode and
//! offset, and a writer and its readers meet on the same word without arranging anything.
//! Readers map segments read-only; `FUTEX_WAIT` only reads the word, which that allows.
//!
//! Linux only. Elsewhere [`SUPPORTED`] is false, `wake_all` only bumps the word, and readers
//! keep the backoff.

use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

/// Whether this platform can sleep on a wake word.
pub(crate) const SUPPORTED: bool = cfg!(target_os = "linux");

/// How a wait ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Waited {
    /// Woken, or the word no longer held the expected value: look again.
    Changed,
    /// The timeout passed with the word unchanged.
    TimedOut,
    /// The kernel cannot do this wait (`futex_waitv` before Linux 5.16): use the backoff.
    Unsupported,
}

/// Bump the word, then wake everyone sleeping on it. The bump comes first, so a reader that
/// loaded the old value and is about to sleep on it finds the word changed and returns at once.
#[inline]
pub(crate) fn wake_all(word: &AtomicU32) {
    word.fetch_add(1, Ordering::Release);
    #[cfg(target_os = "linux")]
    unsafe {
        libc::syscall(libc::SYS_futex, word.as_ptr(), libc::FUTEX_WAKE, i32::MAX);
    }
}

/// Sleep while `word` holds `expected`, for at most `timeout`.
#[cfg(target_os = "linux")]
pub(crate) fn wait(word: &AtomicU32, expected: u32, timeout: Duration) -> io::Result<Waited> {
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
        return Ok(Waited::Changed);
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EAGAIN) | Some(libc::EINTR) => Ok(Waited::Changed),
        Some(libc::ETIMEDOUT) => Ok(Waited::TimedOut),
        _ => Err(err),
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn wait(_word: &AtomicU32, _expected: u32, _timeout: Duration) -> io::Result<Waited> {
    Ok(Waited::Unsupported)
}

/// Most words one `futex_waitv` call accepts.
pub(crate) const WAITV_MAX: usize = 128;

/// Set once the kernel has refused `futex_waitv` — it is too old (`ENOSYS`), or a seccomp
/// profile that predates it forbids it (`EPERM`, as older container runtimes do) — so it is
/// not asked again.
#[cfg(target_os = "linux")]
static NO_WAITV: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Whether `wait_any` can sleep on several words at once, as far as is known yet.
pub(crate) fn waitv_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        !NO_WAITV.load(Ordering::Relaxed)
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Test hook: behave as on a kernel without `futex_waitv`.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn disable_waitv() {
    NO_WAITV.store(true, Ordering::Relaxed);
}

/// Sleep while every `word` holds its `expected` value, for at most `timeout`: one
/// `futex_waitv` over all of them. At most [`WAITV_MAX`] words.
#[cfg(target_os = "linux")]
pub(crate) fn wait_any(words: &[(&AtomicU32, u32)], timeout: Duration) -> io::Result<Waited> {
    /// `struct futex_waitv` from `<linux/futex.h>`.
    #[repr(C)]
    struct FutexWaitv {
        val: u64,
        uaddr: u64,
        flags: u32,
        reserved: u32,
    }
    /// `FUTEX2_SIZE_U32`; no `FUTEX2_PRIVATE`, as these are shared futexes.
    const FUTEX2_SIZE_U32: u32 = 0x02;

    debug_assert!(words.len() <= WAITV_MAX);
    if words.is_empty() || NO_WAITV.load(Ordering::Relaxed) {
        return Ok(Waited::Unsupported);
    }
    let waiters: Vec<FutexWaitv> = words
        .iter()
        .map(|(word, expected)| FutexWaitv {
            val: *expected as u64,
            uaddr: word.as_ptr() as u64,
            flags: FUTEX2_SIZE_U32,
            reserved: 0,
        })
        .collect();
    // futex_waitv takes an absolute deadline on the given clock.
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now) };
    let deadline_ns =
        (now.tv_sec as u128) * 1_000_000_000 + now.tv_nsec as u128 + timeout.as_nanos();
    let deadline = libc::timespec {
        tv_sec: (deadline_ns / 1_000_000_000) as _,
        tv_nsec: (deadline_ns % 1_000_000_000) as _,
    };
    let r = unsafe {
        libc::syscall(
            libc::SYS_futex_waitv,
            waiters.as_ptr(),
            waiters.len() as libc::c_uint,
            0 as libc::c_uint,
            &deadline as *const libc::timespec,
            libc::CLOCK_MONOTONIC,
        )
    };
    if r >= 0 {
        return Ok(Waited::Changed);
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EAGAIN) | Some(libc::EINTR) => Ok(Waited::Changed),
        Some(libc::ETIMEDOUT) => Ok(Waited::TimedOut),
        Some(libc::ENOSYS) | Some(libc::EPERM) => {
            NO_WAITV.store(true, Ordering::Relaxed);
            Ok(Waited::Unsupported)
        }
        _ => Err(err),
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn wait_any(_words: &[(&AtomicU32, u32)], _timeout: Duration) -> io::Result<Waited> {
    Ok(Waited::Unsupported)
}
