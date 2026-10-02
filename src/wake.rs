//! Futex calls behind reader waking (`WriterBuilder::wake_readers`).
//!
//! The wake word lives in a segment's `ChannelHeader`, in a `MAP_SHARED` file mapping, so the
//! futex is a *shared* one (no `FUTEX_PRIVATE_FLAG`): the kernel keys it on the file's inode and
//! offset, and a writer and its readers meet on the same word without arranging anything.
//! Readers map segments read-only; `FUTEX_WAIT` only reads the word, which that allows.
//!
//! Linux only. Elsewhere `wake_all` only bumps the word.

use std::sync::atomic::{AtomicU32, Ordering};

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
