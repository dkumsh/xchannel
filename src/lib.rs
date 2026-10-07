//! xchannel: mmap-backed IPC channels with rolling files.
//!
//! # Overview
//! - Regionized file layout; region 0 starts with a `MessageHeader(Channel)` followed by `ChannelHeader`.
//! - **Pre-header pipeline** for user records:
//!   For record *i*: header(i) is pre-installed (committed=0) at the
//!   end of `try_reserve(i-1)` (and the very first one at file
//!   create time). `try_reserve(i)` then pre-installs header(i+1)
//!   before returning the buffer for slot i. The caller fills the
//!   payload and calls `commit`, which fills header(i) and
//!   release-stores `committed=1`. At any point a reader observes
//!   `committed[i] = 1` with acquire semantics, slot i+1 is
//!   guaranteed to bear the pre-install signature.
//! - Special markers: `Skip` (pad to next region), `Roll` (file rolled).
//!
//! # Safety
//! Writers produce `&mut` references into an mmap; do **not** run a reader in the
//! same process concurrently with a writer to the same file/region. For cross-process
//! IPC this is fine. Publishing uses `Release` and reading uses `Acquire`.

mod channel;
mod helper;
mod map_ahead;
mod prefault;
mod region;
mod v4;
mod wake;

use channel::{
    ChannelHeader, ENDIANNESS_LE, FORMAT_VERSION, HeaderType, MessageHeader, SYSTEM_HEADER_SIZE,
    USER_HEADER_KIND_DEFAULT, USER_HEADER_SIZE, WAKE_FLAG_WAKES,
};
pub use helper::{Helper, Unmap};
pub use region::{ReadOnly, RegionMapping, Writable, page_size};
use v4::{Identity, InstanceId, RollEdge};

use std::fs::{File, OpenOptions, read_dir};
use std::io::{self, ErrorKind};
use std::mem::{align_of, size_of};
use std::path::{Path, PathBuf};
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

// ========== Constants ==========
const MESSAGE_HEADER_SIZE: usize = size_of::<MessageHeader>();

// Keep 8-byte alignment (can be revisited later).
const ALIGN: usize = align_of::<MessageHeader>(); // 8 on supported targets

// Header slot size == struct size (16B) for now.
const HEADER_SLOT: usize = MESSAGE_HEADER_SIZE;
/// File offset of a segment's first record: after the Channel record and its v4 extension.
const FIRST_RECORD: usize = v4::FIRST_RECORD_V4;
/// Room a writer keeps free at its next header slot, so a `Roll` fits wherever it stops.
const ROLL_TOTAL: usize = v4::ROLL_TOTAL_V4;
const DEFAULT_BATCH_SEGS_CAP: usize = 16;
const DEFAULT_BATCH_POS_CAP: usize = 1024;
const DEFAULT_BATCH_MAPS_CAP: usize = 16;

#[inline(always)]
fn align_up(x: usize) -> usize {
    (x + (ALIGN - 1)) & !(ALIGN - 1)
}

#[inline]
fn err_other<S: Into<String>>(s: S) -> io::Error {
    io::Error::other(s.into())
}

#[inline]
fn err_invalid_data<S: Into<String>>(s: S) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, s.into())
}

#[inline]
fn get_channel_header_ptr(region_ptr: *const u8) -> *const ChannelHeader {
    unsafe { region_ptr.add(MESSAGE_HEADER_SIZE) as *const ChannelHeader }
}
fn get_channel_header<'a>(region_ptr: *const u8) -> &'a ChannelHeader {
    unsafe { &*get_channel_header_ptr(region_ptr) }
}

/// Validate the v1 format invariants of a `ChannelHeader` (see FORMAT.md §8).
/// `expected_region_size` is checked against the value in the header.
/// `expected_sequence` is checked against `channel_sequence`: the header's
/// self-described file ordinal must match the sequence parsed from the file's
/// path, catching a renamed, misplaced, or swapped segment file.
/// `user_header_kind` must equal `USER_HEADER_KIND_DEFAULT`; the wire field is
/// reserved for future user-defined layouts and has no public opt-in today.
fn validate_channel_header(
    ch: &ChannelHeader,
    expected_region_size: usize,
    expected_sequence: u64,
) -> io::Result<()> {
    if ch.format_version != FORMAT_VERSION {
        return Err(err_invalid_data(format!(
            "unsupported format_version {} (this build expects {})",
            ch.format_version, FORMAT_VERSION
        )));
    }
    if ch.endianness != ENDIANNESS_LE {
        return Err(err_invalid_data(format!(
            "unsupported endianness 0x{:02x} (this build expects 0x{:02x})",
            ch.endianness, ENDIANNESS_LE
        )));
    }
    if ch.system_header_size != SYSTEM_HEADER_SIZE || ch.user_header_size != USER_HEADER_SIZE {
        return Err(err_invalid_data(format!(
            "header-size mismatch: file=({}/{}) build=({}/{})",
            ch.system_header_size, ch.user_header_size, SYSTEM_HEADER_SIZE, USER_HEADER_SIZE
        )));
    }
    if ch.user_header_kind != USER_HEADER_KIND_DEFAULT {
        return Err(err_invalid_data(format!(
            "unsupported user_header_kind 0x{:08x} (this build only reads 0x{:08x})",
            ch.user_header_kind, USER_HEADER_KIND_DEFAULT
        )));
    }
    if ch.region_size as usize != expected_region_size {
        return Err(err_invalid_data(format!(
            "region_size mismatch: file={} expected={}",
            ch.region_size, expected_region_size
        )));
    }
    if ch.channel_sequence != expected_sequence {
        return Err(err_invalid_data(format!(
            "channel_sequence mismatch: header={} but file is at sequence {} \
             (renamed, misplaced, or swapped segment file?)",
            ch.channel_sequence, expected_sequence
        )));
    }
    Ok(())
}

/// The v4 Channel record length and extension of the segment mapped at `page0`; its identity.
fn validate_v4_prefix(page0: *const u8) -> io::Result<Identity> {
    let mh = unsafe { &*(page0 as *const MessageHeader) };
    if mh.length as usize != v4::CHANNEL_PAYLOAD_V4 {
        return Err(err_invalid_data(format!(
            "Channel record is {} bytes, but v4 makes it {}",
            mh.length,
            v4::CHANNEL_PAYLOAD_V4
        )));
    }
    unsafe { v4::ext_at(page0) }.identity()
}

/// Whether the file mapped at `page0` is published (acquire).
fn is_published(page0: *const u8) -> io::Result<bool> {
    Ok(unsafe { v4::ext_at(page0) }.state()? == v4::State::Published)
}

/// Whether segment `sequence` exists and is published.
fn segment_is_published(base_path: &Path, sequence: u64) -> io::Result<bool> {
    let Some(file) = open_segment_if_present(base_path, sequence)? else {
        return Ok(false);
    };
    if file.metadata()?.len() < FIRST_RECORD as u64 {
        return Ok(false);
    }
    let page0 = RegionMapping::create_read_only(&file, 0, region::page_size())?;
    let ch = get_channel_header(page0.as_ptr());
    validate_channel_header(ch, ch.region_size as usize, sequence)?;
    validate_v4_prefix(page0.as_ptr())?;
    is_published(page0.as_ptr())
}

/// The newest published segment; one this build cannot read is returned for its open to report.
fn find_latest_published_sequence(base_path: &Path) -> io::Result<u64> {
    let seqs = find_all_sequences(base_path)?;
    for &seq in seqs.iter().rev() {
        match segment_is_published(base_path, seq) {
            Ok(true) | Err(_) => return Ok(seq),
            Ok(false) => {}
        }
    }
    Ok(seqs.first().copied().unwrap_or(0))
}

/// Tests: abort the process at `point` if `XCH_CRASH_AT` names it.
#[cfg(test)]
fn crash_point(point: &str) {
    if std::env::var("XCH_CRASH_AT").is_ok_and(|p| p == point) {
        std::process::abort();
    }
}

#[cfg(not(test))]
#[inline(always)]
fn crash_point(_: &str) {}

/// A "pre-installed" header is what `try_reserve()` lays down one
/// slot ahead of itself before returning the buffer:
/// `{committed: 0, header_type: User, length: 0, message_type: 0, user_meta_u64: 0}`.
/// Crashed-writer recovery advances past the orphaned record and asserts this
/// signature on the new slot, rejecting raw fresh-extended bytes
/// (`header_type = 0 = Channel`) or any partially-populated state.
fn verify_preinstall_signature(hdr: &MessageHeader) -> io::Result<()> {
    if hdr.is_committed()? {
        return Err(err_invalid_data(
            "crashed writer recovery: advanced slot is committed \
             (multi-record publish_wp lag, unsupported)",
        ));
    }
    if hdr.header_type != HeaderType::User as u8
        || hdr.length != 0
        || hdr.message_type != 0
        || hdr.user_meta_u64 != 0
    {
        return Err(err_invalid_data(
            "crashed writer recovery: advanced slot is not a pre-installed header",
        ));
    }
    Ok(())
}
// -------- Error types: --------

/// The channel at a reader's path is not the incarnation the caller expected.
///
/// Returned (wrapped in an `io::Error` of kind `InvalidData`) by a reader built with
/// [`ReaderBuilder::expect_generation`], and by [`Reader::seek`] / [`Reader::rewind`] /
/// [`Reader::seek_to_head`] when the path has been deleted and recreated under the reader.
/// A recreated channel restarts at record index 0, so a saved index would silently point into
/// unrelated data; this is checked before the index is, so it is never misreported as a
/// pruned or out-of-range index. Recover it from the `io::Error` with [`GenerationMismatch::of`].
///
/// The check is only as good as the generations writers stamp. A channel created without
/// [`WriterBuilder::generation`] has generation 0, so a recreated channel whose writers also
/// leave it at 0 is indistinguishable from the original and is **not** caught. Stamp a fresh
/// value per incarnation (a creation timestamp, a random id) if cursors are persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerationMismatch {
    /// The generation the caller asked for.
    pub expected: u64,
    /// The generation found on disk.
    pub found: u64,
}

impl GenerationMismatch {
    /// The mismatch carried by `err`, if that is what it is.
    pub fn of(err: &io::Error) -> Option<Self> {
        err.get_ref()?.downcast_ref::<Self>().copied()
    }

    fn into_io(self) -> io::Error {
        io::Error::new(ErrorKind::InvalidData, self)
    }
}

impl std::fmt::Display for GenerationMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "channel generation is {} but {} was expected (path deleted and recreated?)",
            self.found, self.expected
        )
    }
}

impl std::error::Error for GenerationMismatch {}

/// The requested record index is older than anything still on disk: retention (`keep_files`)
/// has removed the segment that held it.
///
/// Returned (wrapped in an `io::Error` of kind `NotFound`) by [`ReaderMode::At`] /
/// [`ReaderBuilder::start_at`] and [`Reader::seek`]. `NotFound` alone can also mean there is no
/// channel at the path; recover this payload with [`IndexPruned::of`] to tell the two apart, and
/// to learn where the retained records begin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct IndexPruned {
    /// The index that was asked for.
    pub index: u64,
    /// The oldest index still retained: the earliest segment's `base_record_index`.
    pub earliest: u64,
}

impl IndexPruned {
    /// The pruned-index details carried by `err`, if that is what it is.
    pub fn of(err: &io::Error) -> Option<Self> {
        err.get_ref()?.downcast_ref::<Self>().copied()
    }

    fn into_io(self) -> io::Error {
        io::Error::new(ErrorKind::NotFound, self)
    }
}

impl std::fmt::Display for IndexPruned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "record {} has been pruned: the earliest retained record is {}",
            self.index, self.earliest
        )
    }
}

impl std::error::Error for IndexPruned {}

/// Internal: a `Live` open found a newer segment than the one it opened, so the file it holds
/// is no longer the tail. `Reader::open` retries on the newest segment; never surfaces.
#[derive(Debug)]
struct StaleSegment;

impl StaleSegment {
    fn is(err: &io::Error) -> bool {
        err.get_ref().is_some_and(|e| e.is::<Self>())
    }

    fn into_io(self) -> io::Error {
        io::Error::other(self)
    }
}

impl std::fmt::Display for StaleSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a newer segment exists; the opened segment is no longer the tail")
    }
}

impl std::error::Error for StaleSegment {}

// -------- Small internal helpers (low-level ops) --------
#[inline]
fn with_ch_mut<F>(file: &File, region_size: usize, f: F) -> io::Result<()>
where
    F: FnOnce(&mut ChannelHeader),
{
    let mut z = RegionMapping::create_writable(file, 0, region_size)?;
    let ch_mut = unsafe { &mut *(z.as_mut_ptr().add(MESSAGE_HEADER_SIZE) as *mut ChannelHeader) };
    f(ch_mut);
    Ok(())
}

/// Why [`walk_segment`] stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalkStop {
    /// The visitor asked to stop at this record.
    Visitor,
    /// The slot is not committed yet: the end of what has been written.
    Uncommitted,
    /// A `Roll` marker: the segment continues in the next file.
    Roll,
    /// The next region lies past the end of the file.
    EndOfFile,
}

/// Where [`walk_segment`] stopped, and how many user records it stepped over to get there.
#[derive(Debug, Clone, Copy)]
struct WalkEnd {
    pos: usize,
    users: u64,
    stop: WalkStop,
}

/// Header-only walk over one segment from offset 0, stepping over records by their `length`
/// and never touching a payload.
///
/// `stop(pos, header_type, users_before)` is asked about each committed record before it is
/// stepped over; returning `true` ends the walk *at* that record. The walk also ends at the
/// first uncommitted slot, at a `Roll` marker (without stepping over it), or at the end of the
/// file. Cost is one dependent header load per record plus a mapping per region, so this is
/// for seeking and recovery, not for a hot path.
fn walk_segment(
    file: &File,
    region_size: usize,
    stop: impl FnMut(usize, HeaderType, u64) -> bool,
) -> io::Result<WalkEnd> {
    walk_segment_from(file, region_size, 0, stop)
}

/// [`walk_segment`] starting at `start`, which must be a record boundary: offset 0, or the
/// start of any region (records never cross a region boundary, so each region begins with
/// one). `users` then counts the user records from `start`, not from the segment's start.
fn walk_segment_from(
    file: &File,
    region_size: usize,
    start: usize,
    mut stop: impl FnMut(usize, HeaderType, u64) -> bool,
) -> io::Result<WalkEnd> {
    let file_len = file.metadata()?.len();
    let mut map: Option<(usize, RegionMapping<ReadOnly>)> = None;
    let mut pos = start;
    let mut users = 0u64;
    let end = |pos, users, stop| Ok(WalkEnd { pos, users, stop });
    loop {
        let region_idx = pos / region_size;
        if ((region_idx + 1) * region_size) as u64 > file_len {
            return end(pos, users, WalkStop::EndOfFile);
        }
        let region = match &map {
            Some((idx, region)) if *idx == region_idx => region,
            _ => {
                let region = RegionMapping::create_read_only(
                    file,
                    (region_idx * region_size) as u64,
                    region_size,
                )?;
                &map.insert((region_idx, region)).1
            }
        };
        let off = pos % region_size;
        let leftover = region_size - off;
        if leftover < HEADER_SLOT {
            return Err(err_invalid_data(
                "segment walk landed in a region's tail hole",
            ));
        }
        let mh = unsafe { &*(region.as_ptr().add(off) as *const MessageHeader) };
        if !mh.is_committed()? {
            return end(pos, users, WalkStop::Uncommitted);
        }
        let header_type = mh.parsed_header_type()?;
        if stop(pos, header_type, users) {
            return end(pos, users, WalkStop::Visitor);
        }
        if header_type == HeaderType::Roll {
            if mh.length as usize != v4::ROLL_PAYLOAD_V4 || off + ROLL_TOTAL > region_size {
                return Err(err_invalid_data(format!(
                    "segment walk: Roll at {pos} has a {}-byte body, not {}",
                    mh.length,
                    v4::ROLL_PAYLOAD_V4
                )));
            }
            return end(pos, users, WalkStop::Roll);
        }
        let total_len = HEADER_SLOT + mh.length as usize;
        if total_len > leftover {
            return Err(err_invalid_data(
                "segment walk: record extends past its region",
            ));
        }
        if header_type == HeaderType::User {
            users += 1;
        }
        pos = align_up(pos + total_len);
    }
}

/// Set or clear a segment's `wake_flags` bit 0 through the writer's mapping of its region 0.
/// Readers load the byte on every wait, so a change applies from their next wait.
fn set_wake_flag(region0: &RegionMapping<Writable>, wake: bool) {
    let ch = get_channel_header(region0.as_ptr());
    let flags = unsafe { &*(std::ptr::addr_of!(ch.wake_flags) as *const AtomicU8) };
    if wake {
        flags.fetch_or(WAKE_FLAG_WAKES, Ordering::Release);
    } else {
        flags.fetch_and(!WAKE_FLAG_WAKES, Ordering::Release);
    }
}

// ========== Builders ==========
/// Maximum bytes available for a channel name in `ChannelHeader`.
pub const CHANNEL_NAME_MAX: usize = 48;

#[derive(Clone, Debug)]
pub struct WriterBuilder {
    path: PathBuf,
    region_size: usize,
    file_roll_size: u64,
    mtu: u64,
    keep_files: Option<u64>,
    channel_name: [u8; CHANNEL_NAME_MAX],
    base_record_index: u64,
    generation: u64,
    wake_readers: bool,
    helper: Option<Helper>,
    unmap: Unmap,
}

impl WriterBuilder {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            region_size: 1024 * 1024, // default: 1M
            file_roll_size: 0,        // default: no file rolling
            mtu: 0,                   // default: no MTU limit
            keep_files: None,         // default: keep all rolled files
            channel_name: [0; CHANNEL_NAME_MAX],
            base_record_index: 0, // default: genesis channel starts at index 0
            generation: 0,        // default: unset incarnation id
            wake_readers: false,  // default: readers poll with backoff
            helper: None,
            unmap: Unmap::Immediate,
        }
    }

    /// Hand the writer's mapping work to a thread of its own (`xch-prefault`): it keeps the pages
    /// just ahead of the writer faulted in, maps the next region before the writer reaches it and
    /// unmaps the ones it left, so none of that costs the writer a fault or a syscall. Off by
    /// default. `build` fails if the helper cannot be pinned where [`Helper::on_core`] says.
    ///
    /// Linux only, and it needs 5.14+ for `MADV_POPULATE_WRITE`: on an older kernel the helper
    /// stops at once, [`Writer::helper_error`] says why, and the writer works as it does without
    /// one. Elsewhere it is accepted and does nothing.
    pub fn helper(mut self, helper: Helper) -> Self {
        self.helper = Some(helper);
        self
    }

    /// When the writer releases the regions it has written past; [`Unmap::Immediate`] by default.
    pub fn unmap(mut self, unmap: Unmap) -> Self {
        self.unmap = unmap;
        self
    }

    /// Wake readers that wait for this channel, instead of leaving them to poll.
    ///
    /// After every commit, and after committing a segment's `Roll`, the writer bumps the
    /// segment's wake word and `futex_wake`s anyone sleeping on it.
    /// [`Reader::wait_for_message`], [`Reader::read_blocking`] and [`wait_any`] then sleep on
    /// that word and wake within microseconds of a commit, instead of on a backoff timer that
    /// grows to 500 µs. Readers stay read-only; nothing about them is in shared memory.
    ///
    /// The cost is one futex syscall on **every** commit, whether or not anyone is waiting:
    /// measured at about 165 ns on a recent laptop CPU and 730 ns on an older,
    /// heavily mitigated server CPU, plus 1–2.5 µs on a commit that actually wakes a sleeper.
    /// So it suits a channel that is often idle, not one that runs at millions of records a
    /// second. How fast a sleeping reader runs again depends on its CPU's idle states: about
    /// 3 µs on a host with deep C-states disabled, around 80 µs on an untuned laptop.
    ///
    /// Off by default. A channel that does not opt in is unchanged: no load, no store, no
    /// syscall. The setting is stamped into every segment this writer creates or reopens
    /// (FORMAT.md §3), so readers know to sleep on the word. Linux only; elsewhere it is
    /// accepted and does nothing, and readers keep the backoff.
    pub fn wake_readers(mut self, wake: bool) -> Self {
        self.wake_readers = wake;
        self
    }

    /// Stamp an opaque **incarnation id** into every segment of this channel, used only
    /// when *creating* it (ignored when reopening an existing one — the on-disk value
    /// wins, and rolls carry it forward). Defaults to 0.
    ///
    /// Lets a consumer distinguish "this log continues" from "this path was deleted and
    /// recreated": a recreated channel restarts at sequence 0 and record index 0, so
    /// without this it is indistinguishable from a channel that was merely truncated,
    /// and a persisted cursor silently refers to a different log. Pair a stored cursor
    /// with [`Reader::generation`] and treat a change as a new channel, not a gap.
    ///
    /// xchannel assigns no meaning to the value.
    #[inline]
    pub fn generation(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }

    /// Set the absolute record index of the **first** record this channel will
    /// hold, used only when *creating* a new channel (ignored when reopening an
    /// existing one — the on-disk value wins). Defaults to 0 (genesis).
    ///
    /// The intended use is replicas: a node rebuilding a remote channel whose
    /// genesis has been retention-truncated seeds the replica with the absolute
    /// index of its first received record, so the replica's headers report
    /// absolute (not replica-local) indices. `base_record_index + message_count`
    /// remains the absolute index of the next record.
    #[inline]
    pub fn base_record_index(mut self, base: u64) -> Self {
        self.base_record_index = base;
        self
    }

    #[inline]
    pub fn region_size(mut self, region_size: usize) -> Self {
        self.region_size = region_size;
        self
    }
    /// Max bytes per segment file before rolling to the next. `0`
    /// (default) disables rolling: a single file grown region-by-region.
    /// A non-zero size is eagerly preallocated (sparse) on segment
    /// creation, rounded up to a `region_size` multiple, must be at least
    /// `2 * region_size`, and must not exceed `i64::MAX` (the OS
    /// file-offset limit).
    #[inline]
    pub fn file_roll_size(mut self, file_roll_size: u64) -> Self {
        self.file_roll_size = file_roll_size;
        self
    }
    #[inline]
    pub fn mtu(mut self, mtu: u64) -> Self {
        self.mtu = mtu;
        self
    }

    /// Set an optional channel name persisted in the `ChannelHeader`. The
    /// name is UTF-8 bytes, up to `CHANNEL_NAME_MAX` (48) bytes; longer
    /// names return `ErrorKind::InvalidInput`. Read back with
    /// `Reader::channel_name`.
    pub fn channel_name(mut self, name: &str) -> io::Result<Self> {
        let bytes = name.as_bytes();
        if bytes.len() > CHANNEL_NAME_MAX {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "channel_name is {} bytes; max is {}",
                    bytes.len(),
                    CHANNEL_NAME_MAX
                ),
            ));
        }
        self.channel_name = [0; CHANNEL_NAME_MAX];
        self.channel_name[..bytes.len()].copy_from_slice(bytes);
        Ok(self)
    }

    /// Cap the number of channel files retained on disk to `n` (the active
    /// file plus `n - 1` historical rolled files). Each successful file roll
    /// unlinks the file at sequence `current_seq − n`.
    ///
    /// Default: unlimited retention.
    ///
    /// `n` must be at least 1. Readers that are still mapped on a file when
    /// it is unlinked will continue to read it (POSIX `unlink` keeps the
    /// inode alive while it is open or mapped); they will only fail with
    /// `ENOENT` if they fall further behind than `n` files and try to open
    /// a file that has already been pruned.
    ///
    /// That same rule means retention only bounds on-disk usage if readers let
    /// pruned files go. A reader that keeps an [`OwnedMessage`] from a pruned
    /// segment holds that segment's inode alive in full, so its bytes are not
    /// reclaimed and the cap is not the bound it looks like — see
    /// [`OwnedMessage`]'s retention notes.
    #[inline]
    pub fn keep_files(mut self, n: u64) -> Self {
        assert!(n >= 1, "WriterBuilder::keep_files: n must be >= 1");
        self.keep_files = Some(n);
        self
    }

    /// Create or open the latest sequence file and return a Writer.
    #[inline]
    pub fn build(self) -> io::Result<Writer> {
        // Nonzero roll size needs >= 2 regions: region 0's head holds the
        // channel header, so one region can't fit a full-size record.
        if self.file_roll_size != 0
            && self.file_roll_size < (self.region_size as u64).saturating_mul(2)
        {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "file_roll_size {} must be 0 or span at least two regions \
                     (2 * region_size = {})",
                    self.file_roll_size,
                    (self.region_size as u64).saturating_mul(2),
                ),
            ));
        }
        sweep_stale_partial_files(&self.path);
        let helper = self.helper.filter(|_| cfg!(target_os = "linux"));
        let mut writer = Writer::open_or_create(
            self.path,
            self.region_size,
            self.file_roll_size,
            self.mtu,
            self.keep_files,
            self.channel_name,
            self.base_record_index,
            self.generation,
            self.wake_readers,
        )?;
        writer.unmap = self.unmap;
        if helper.is_some() {
            writer.helper = helper;
            writer.start_prefault()?;
        } else if self.unmap == Unmap::AtFileRoll {
            writer.held.reserve(64);
        }
        Ok(writer)
    }

    /// Convenience: just ensure the channel file exists and is initialized, then drop.
    #[inline]
    pub fn precreate(self) -> io::Result<()> {
        let _w = self.build()?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ReaderBuilder {
    path: PathBuf,
    mode: ReaderMode,
    batch_limit: Option<u16>,
    expected_generation: Option<u64>,
    helper: Option<Helper>,
    unmap: Unmap,
}

impl ReaderBuilder {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            mode: ReaderMode::LateJoin,
            batch_limit: None,
            expected_generation: None,
            helper: None,
            unmap: Unmap::Immediate,
        }
    }

    #[inline]
    pub fn mode(mut self, mode: ReaderMode) -> Self {
        self.mode = mode;
        self
    }
    #[inline]
    pub fn live(mut self) -> Self {
        self.mode = ReaderMode::Live;
        self
    }
    #[inline]
    pub fn late_join(mut self) -> Self {
        self.mode = ReaderMode::LateJoin;
        self
    }
    /// Start so the next user record read is absolute index `index`; shorthand for
    /// `mode(ReaderMode::At(index))`. See [`ReaderMode::At`] for the errors and the cost.
    #[inline]
    pub fn start_at(mut self, index: u64) -> Self {
        self.mode = ReaderMode::At(index);
        self
    }

    /// Refuse to open unless the channel's generation is `generation`.
    ///
    /// A resumed cursor is an index *and* the generation it was taken from: a channel deleted
    /// and recreated at the same path restarts at index 0, so the index alone would silently
    /// point into unrelated data. With this set, `build` fails with a [`GenerationMismatch`]
    /// (checked before the index, so a recreated channel is never misreported as a pruned or
    /// out-of-range index).
    ///
    /// This only distinguishes incarnations whose writers stamp different generations; the
    /// default is 0 (see [`GenerationMismatch`]).
    #[inline]
    pub fn expect_generation(mut self, generation: u64) -> Self {
        self.expected_generation = Some(generation);
        self
    }

    /// Default batch size limit used when `try_read_batch(None)` is called.
    /// `None` means unlimited.
    #[inline]
    pub fn batch_limit(mut self, limit: u16) -> Self {
        self.batch_limit = Some(limit);
        self
    }

    /// Hand the reader's mapping work to a thread of its own (`xch-map-ahead`): it maps and
    /// populates the next region before the reader reaches it, and unmaps the regions the reader
    /// has left, so moving between regions costs the reader neither. Off by default. `build`
    /// fails if the helper cannot be pinned where [`Helper::on_core`] says.
    ///
    /// Linux only, and it needs 5.14+ for `MADV_POPULATE_READ`: on an older kernel the helper
    /// stops at once, [`Reader::helper_error`] says why, and the reader works as it does without
    /// one. Elsewhere it is accepted and does nothing.
    #[inline]
    pub fn helper(mut self, helper: Helper) -> Self {
        self.helper = Some(helper);
        self
    }

    /// When the reader releases the regions it has read past; [`Unmap::Immediate`] by default.
    #[inline]
    pub fn unmap(mut self, unmap: Unmap) -> Self {
        self.unmap = unmap;
        self
    }

    /// Open a Reader according to the configured mode.
    #[inline]
    pub fn build(self) -> io::Result<Reader> {
        let mut reader = Reader::open_with(self.path, self.mode, self.expected_generation)?;
        reader.batch_limit = self.batch_limit;
        reader.unmap = self.unmap;
        if self.unmap == Unmap::AtFileRoll {
            reader.held.reserve(64);
        }
        reader.helper = self.helper.filter(|_| cfg!(target_os = "linux"));
        reader.start_map_ahead()?;
        Ok(reader)
    }
}

// ========== Channel Writer ==========
pub struct Writer {
    /// First, so it stops before the header it reads is unmapped.
    prefault: Option<prefault::Prefaulter>,
    helper: Option<Helper>,
    unmap: Unmap,
    /// Regions written past and kept mapped until the roll (`AtFileRoll` without a helper).
    held: Vec<RegionMapping<Writable>>,
    base_path: PathBuf,
    file_sequence: u64,
    /// The current file's instance ID: the predecessor its successor names.
    instance: InstanceId,
    file: File,
    file_len: u64,

    channel_region: RegionMapping<Writable>,
    current_region: RegionMapping<Writable>,
    current_region_index: u64,

    region_size: usize,
    file_roll_size: u64,
    mtu: u64,
    keep_files: Option<u64>,
    channel_name: [u8; CHANNEL_NAME_MAX],

    // Pre-header pipeline state:
    next_hdr_pos: usize, // absolute file offset of the pre-installed header slot
    /// Size passed to the last `try_reserve` call. `commit`'s `length`
    /// argument must be `<= pending_msg_size`. When `length ==
    /// pending_msg_size`, the slot-i+1 pre-install laid down by
    /// `try_reserve` at the matching offset is reused as-is (fast
    /// path). When `length < pending_msg_size`, `commit` re-lays the
    /// pre-install at the *actual* `next_hdr_pos` so the reader's
    /// walk past slot i still lands on a well-formed slot.
    /// `length > pending_msg_size` is rejected. `None` means no
    /// pending reservation.
    pending_msg_size: Option<usize>,
    /// Wake readers after every commit (`WriterBuilder::wake_readers`). Branched on alone,
    /// never on shared state, so a writer that does not wake pays nothing else.
    wake: bool,
    /// Segments retention could not unlink yet: the helper might still install them.
    deferred_prune: Vec<u64>,
}

impl Writer {
    /// Create/open the latest channel file.
    /// Validates that `region_size` is a multiple of OS page size and large enough.
    // Six of these are "how to create a segment" and travel together through
    // build → open_or_create → open_file → prepare_segment_at. Worth folding into a
    // SegmentSpec before a seventh is added; not worth churning this path for one field.
    #[allow(clippy::too_many_arguments)]
    fn open_or_create<P: AsRef<Path>>(
        path: P,
        region_size: usize,
        file_roll_size: u64,
        mtu: u64,
        keep_files: Option<u64>,
        channel_name: [u8; CHANNEL_NAME_MAX],
        base_record_index: u64,
        generation: u64,
        wake: bool,
    ) -> io::Result<Self> {
        // Validate region invariants
        let ps = region::page_size();
        // Region must be a multiple of the OS page size and header alignment
        if !region_size.is_multiple_of(ps) {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "region_size ({}) must be a multiple of OS page size ({})",
                    region_size, ps
                ),
            ));
        }
        if !region_size.is_multiple_of(ALIGN) {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "region_size ({}) must be a multiple of header alignment ({})",
                    region_size, ALIGN
                ),
            ));
        }
        if region_size < FIRST_RECORD + ROLL_TOTAL {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "region_size ({}) must be >= header space ({})",
                    region_size,
                    FIRST_RECORD + ROLL_TOTAL
                ),
            ));
        }
        if region_size > u32::MAX as usize {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "region_size too large for u32",
            ));
        }

        let base_path = path.as_ref().to_path_buf();
        let sequence = discard_unpublished_tail(&base_path)?;
        let (file, channel_region, current_region, current_region_index, file_len, next_hdr_pos) =
            Self::open_file(
                &base_path,
                sequence,
                region_size,
                file_roll_size,
                mtu,
                &channel_name,
                base_record_index,
                generation,
            )?;
        // Readers trust the flag of the segment they read, so it follows this writer's
        // setting, set or cleared, whoever created the segment.
        set_wake_flag(&channel_region, wake);
        let identity = validate_v4_prefix(channel_region.as_ptr())?;
        if sequence > 0 {
            let ch = get_channel_header(channel_region.as_ptr());
            let (generation, base) = (ch.generation, ch.base_record_index);
            Self::commit_stranded_roll(
                &base_path,
                sequence - 1,
                region_size,
                generation,
                &identity,
                base,
            )?;
        }

        Ok(Self {
            prefault: None,
            helper: None,
            unmap: Unmap::Immediate,
            held: Vec::new(),
            base_path,
            file_sequence: sequence,
            instance: identity.instance,
            file,
            file_len,
            channel_region,
            current_region,
            current_region_index,
            region_size,
            file_roll_size,
            mtu,
            keep_files,
            channel_name,
            next_hdr_pos,
            pending_msg_size: None,
            wake,
            deferred_prune: Vec::with_capacity(2),
        })
    }

    /// Finish the roll into `successor` that a crashed writer left staged, or committed with its
    /// position behind, once its `Roll` is checked to name `successor` exactly. Idempotent; a
    /// mismatch is an error, never repaired.
    fn commit_stranded_roll(
        base_path: &Path,
        sequence: u64,
        region_size: usize,
        generation: u64,
        successor: &Identity,
        successor_base: u64,
    ) -> io::Result<()> {
        let path = make_channel_file_path(base_path, sequence)?;
        let file = match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()), // pruned by retention
            Err(e) => return Err(e),
        };
        // Not a rolled predecessor; `create_writable` would grow it.
        if file.metadata()?.len() < region_size as u64 {
            return Ok(());
        }
        let mut region0 = RegionMapping::create_writable(&file, 0, region_size)?;
        let ch = get_channel_header(region0.as_ptr());
        validate_channel_header(ch, region_size, sequence)?;
        let identity = validate_v4_prefix(region0.as_ptr())?;
        let corrupt = |what: String| {
            Err(err_invalid_data(format!(
                "segment {} follows segment {sequence}, but {what}",
                sequence + 1
            )))
        };
        if !is_published(region0.as_ptr())? {
            return corrupt("that one is unpublished".into());
        }
        if ch.generation != generation
            || successor.predecessor != identity.instance
            || successor.predecessor_sequence != sequence
        {
            return corrupt("does not name it as its parent".into());
        }
        let count = ch.message_count.load(Ordering::Acquire);
        if ch.base_record_index.checked_add(count) != Some(successor_base) {
            return corrupt(format!(
                "starts at record {successor_base} while that one ends at {}",
                ch.base_record_index.saturating_add(count)
            ));
        }

        // Staged, the hint is the Roll's slot + 16; complete, its slot + 16 + 56.
        let wp = ch.write_position.load(Ordering::Acquire) as usize;
        let earliest = wp.saturating_sub(HEADER_SLOT + ROLL_TOTAL);
        let walked = walk_segment_from(
            &file,
            region_size,
            earliest - earliest % region_size,
            |_, _, _| false,
        )?;
        let slot = walked.pos;
        let (staged, behind) = match walked.stop {
            WalkStop::Roll if wp == slot + HEADER_SLOT + ROLL_TOTAL => (false, false), // complete
            WalkStop::Roll if wp == slot + HEADER_SLOT => (false, true), // position behind
            WalkStop::Uncommitted if wp == slot + HEADER_SLOT => (true, true),
            other => {
                return corrupt(format!(
                    "its record chain stops at {slot} ({other:?}) with write_position {wp}"
                ));
            }
        };
        let region_start = slot - slot % region_size;
        let mut region = if region_start == 0 {
            None
        } else {
            Some(RegionMapping::create_writable(
                &file,
                region_start as u64,
                region_size,
            )?)
        };
        let base_ptr = match region.as_mut() {
            Some(r) => r.as_mut_ptr(),
            None => region0.as_mut_ptr(),
        };
        let hdr = unsafe { base_ptr.add(slot - region_start) as *mut MessageHeader };
        let roll = unsafe { &*hdr };
        if roll.parsed_header_type()? != HeaderType::Roll
            || roll.length as usize != v4::ROLL_PAYLOAD_V4
        {
            return corrupt(format!(
                "the record at its write position {slot} is not a Roll"
            ));
        }
        let body = unsafe {
            slice::from_raw_parts(
                base_ptr.add(slot - region_start + HEADER_SLOT),
                v4::ROLL_PAYLOAD_V4,
            )
        };
        let edge = RollEdge::decode(body)?;
        if !edge.names(sequence, identity.instance, successor)
            || edge.next_base_record_index != successor_base
        {
            return corrupt(format!("its Roll names another file: {edge:?}"));
        }
        if !behind {
            return Ok(()); // complete: nothing to do, nothing to wake
        }
        if staged {
            MessageHeader::commit(hdr);
        }
        let ch = get_channel_header(region0.as_ptr());
        let _ = ch.write_position.compare_exchange(
            (slot + HEADER_SLOT) as u64,
            (slot + HEADER_SLOT + ROLL_TOTAL) as u64,
            Ordering::Release,
            Ordering::Relaxed,
        );
        wake::wake_all(&ch.wake_word); // whatever this writer's own setting
        Ok(())
    }

    /// A new `PREPARED` segment at the private path `partial_path`, for the caller to install
    /// and publish. A successor's base is stamped at its roll.
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    fn prepare_segment_at(
        partial_path: &Path,
        sequence: u64,
        region_size: usize,
        file_roll_size: u64,
        mtu: u64,
        channel_name: &[u8; CHANNEL_NAME_MAX],
        base_record_index: u64,
        generation: u64,
        identity: &Identity,
    ) -> io::Result<(
        File,
        RegionMapping<Writable>,
        RegionMapping<Writable>,
        u64,
        u64,
        usize,
    )> {
        let initial_len = preallocation_len(region_size, file_roll_size)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(partial_path)?;
        let remove_on_error = |e: io::Error| {
            let _ = std::fs::remove_file(partial_path);
            e
        };
        Self::init_segment(
            file,
            sequence,
            region_size,
            initial_len,
            mtu,
            channel_name,
            base_record_index,
            generation,
            identity,
        )
        .map_err(remove_on_error)
    }

    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    fn init_segment(
        file: File,
        sequence: u64,
        region_size: usize,
        initial_len: u64,
        mtu: u64,
        channel_name: &[u8; CHANNEL_NAME_MAX],
        base_record_index: u64,
        generation: u64,
        identity: &Identity,
    ) -> io::Result<(
        File,
        RegionMapping<Writable>,
        RegionMapping<Writable>,
        u64,
        u64,
        usize,
    )> {
        file.set_len(initial_len).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "set_len({initial_len}) failed preallocating segment \
                     (region_size {region_size}): {e}"
                ),
            )
        })?;
        let mut region0 = RegionMapping::create_writable(&file, 0, region_size)?;

        // 1) message header (Channel)
        let mh_ptr = region0.as_mut_ptr();
        let mh = unsafe { &mut *(mh_ptr as *mut MessageHeader) };
        mh.committed = 1;
        mh.length = v4::CHANNEL_PAYLOAD_V4 as u32;
        mh.header_type = HeaderType::Channel as u8;
        mh.message_type = 0;
        mh.user_meta_u64 = 0;

        // 2) channel header
        let ch_ptr = unsafe { mh_ptr.add(MESSAGE_HEADER_SIZE) as *mut ChannelHeader };
        unsafe {
            (*ch_ptr).write_position = AtomicU64::new(0);
            // Per-file user-record count, starting at 0. The Channel header and
            // Skip markers are not counted; only `commit` (via `publish_wp`) bumps it.
            (*ch_ptr).message_count = AtomicU64::new(0);
            (*ch_ptr).base_record_index = base_record_index;
            (*ch_ptr).channel_sequence = sequence;
            (*ch_ptr).region_size = region_size as u32;
            (*ch_ptr).mtu = mtu as u32;
            (*ch_ptr).format_version = FORMAT_VERSION;
            (*ch_ptr).endianness = ENDIANNESS_LE;
            (*ch_ptr).system_header_size = SYSTEM_HEADER_SIZE;
            (*ch_ptr).user_header_kind = USER_HEADER_KIND_DEFAULT;
            (*ch_ptr).user_header_size = USER_HEADER_SIZE;
            (*ch_ptr).channel_name = *channel_name;
            (*ch_ptr).wake_flags = 0; // stamped by the writer that opens the segment
            (*ch_ptr)._reserved_pad = [0; 2];
            (*ch_ptr).wake_word = AtomicU32::new(0);
            (*ch_ptr)._reserved2 = [0; 16];
            (*ch_ptr).generation = generation;
            // Nothing else has this file yet.
            v4::ext_at_mut(mh_ptr).init_prepared(identity);
        }

        // 3) current region + first user header pre-install
        let mut current_region = RegionMapping::create_writable(&file, 0, region_size)?;
        let start = FIRST_RECORD;
        let first_user_hdr = current_region
            .get_bytes_mut(start, MESSAGE_HEADER_SIZE)
            .ok_or_else(|| err_other("prepare_segment_at: cannot pre-install first header"))?;
        unsafe {
            *(first_user_hdr.as_mut_ptr() as *mut MessageHeader) = MessageHeader {
                committed: 0,
                header_type: HeaderType::User as u8,
                message_type: 0,
                length: 0,
                user_meta_u64: 0,
            };
        }

        // Publish wp through the already-held `region0` mapping —
        // avoids a third map of region 0 (one was used above for the
        // channel header init, and `current_region` is the second
        // when the first slot lives in region 0).
        let ch_ptr = unsafe { region0.as_mut_ptr().add(MESSAGE_HEADER_SIZE) as *mut ChannelHeader };
        unsafe {
            (*ch_ptr)
                .write_position
                .store((start + HEADER_SLOT) as u64, Ordering::Release);
        }

        Ok((file, region0, current_region, 0, initial_len, start))
    }

    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    fn open_file(
        base_path: &Path,
        sequence: u64,
        region_size: usize,
        file_roll_size: u64,
        mtu: u64,
        channel_name: &[u8; CHANNEL_NAME_MAX],
        base_record_index: u64,
        generation: u64,
    ) -> io::Result<(
        File,
        RegionMapping<Writable>,
        RegionMapping<Writable>,
        u64,
        u64,
        usize,
    )> {
        let file_path = make_channel_file_path(base_path, sequence)?;

        let existing_meta = std::fs::metadata(&file_path).ok();
        let recover_existing = match &existing_meta {
            Some(m) => m.len() > 0,
            None => false,
        };

        if !recover_existing {
            if existing_meta.is_some() {
                // 0-byte stub at the final path would block create_new.
                let _ = std::fs::remove_file(&file_path);
            }
            // Fresh genesis segment: the builder-supplied base (0 for a brand-new
            // channel; the absolute start for a replica). When recovering an
            // existing file below, the on-disk `base_record_index` wins instead.
            let identity = Identity::initial(InstanceId::fresh()?);
            let partial_path = make_attempt_path(base_path, sequence, identity.instance)?;
            let prepared = Self::prepare_segment_at(
                &partial_path,
                sequence,
                region_size,
                file_roll_size,
                mtu,
                channel_name,
                base_record_index,
                generation,
                &identity,
            )?;
            v4::install_no_replace(&partial_path, &file_path).inspect_err(|_| {
                let _ = std::fs::remove_file(&partial_path);
            })?;
            unsafe { v4::ext_at(prepared.1.as_ptr()) }.publish();
            Ok(prepared)
        } else {
            let file = OpenOptions::new().read(true).write(true).open(&file_path)?;
            let initial_len = preallocation_len(region_size, file_roll_size)?;
            // Existing file: adopt next header slot from write_position.
            // Migration step: v3.0.0 writers left files at
            // region-by-region growth; promote to the preallocated
            // layout so future `roll_over_region` calls don't grow
            // the file under a reader's mmap.
            let region0 = RegionMapping::create_writable(&file, 0, region_size)?;
            let ch = get_channel_header(region0.as_ptr());
            validate_channel_header(ch, region_size, sequence)?;
            validate_v4_prefix(region0.as_ptr())?;
            if !is_published(region0.as_ptr())? {
                // An initial file its writer installed but did not publish.
                if sequence != 0 {
                    return Err(err_invalid_data(format!(
                        "segment {sequence} is unpublished, but is the newest file a writer resumes"
                    )));
                }
                unsafe { v4::ext_at(region0.as_ptr()) }.publish();
            }

            // wp denotes the **next header slot offset**
            let wp_payload = ch.write_position.load(Ordering::Relaxed) as usize;
            let mut next_hdr = wp_payload.saturating_sub(HEADER_SLOT);
            let mut region_index = (next_hdr / region_size) as u64;

            let needed_end = (region_index + 1) * region_size as u64;
            let target_len = needed_end.max(initial_len);
            let mut file_len = file.metadata()?.len();
            if target_len > file_len {
                file.set_len(target_len)?;
                file_len = target_len;
            }
            let mut current_region = RegionMapping::create_writable(
                &file,
                region_index * region_size as u64,
                region_size,
            )?;

            // INV5: a clean writer always leaves the header at `next_hdr` pre-
            // installed with committed=0. If we observe it committed, the
            // previous writer crashed between `MessageHeader::commit` and
            // `publish_wp` — `publish_wp` is unconditional in every commit
            // path today, so the lag is bounded to one record. We attempt
            // one-step recovery: advance past the orphaned record by its
            // own `length`, and verify the next slot bears the writer's
            // pre-install signature. Deeper lag (multi-record) or any
            // non-recoverable header type refuses; the supported fallback
            // is `cleanup_channel_files` + a fresh channel.
            let next_hdr_off = next_hdr % region_size;
            let stale_hdr =
                unsafe { &*(current_region.as_ptr().add(next_hdr_off) as *const MessageHeader) };
            if stale_hdr.is_committed()? {
                let stale_type = stale_hdr.parsed_header_type()?;
                let stale_len = stale_hdr.length as usize;
                let advance = HEADER_SLOT + align_up(stale_len);
                match stale_type {
                    HeaderType::User => {
                        // Recover within the current region.
                        if next_hdr_off + advance + HEADER_SLOT > region_size {
                            return Err(err_invalid_data(
                                "crashed writer: User-record recovery would cross region \
                                 boundary; clean up the channel files and start fresh",
                            ));
                        }
                        let advanced_off = next_hdr_off + advance;
                        let advanced_hdr = unsafe {
                            &*(current_region.as_ptr().add(advanced_off) as *const MessageHeader)
                        };
                        verify_preinstall_signature(advanced_hdr)?;
                        next_hdr += advance;
                    }
                    HeaderType::Skip => {
                        // Recover into the next region. By construction in
                        // `roll_over_region`, the Skip's length fills the
                        // remainder of the current region.
                        if next_hdr_off + advance != region_size {
                            return Err(err_invalid_data(
                                "crashed writer: Skip length does not align to region boundary",
                            ));
                        }
                        let next_region_index = region_index + 1;
                        let needed_end = (next_region_index + 1) * region_size as u64;
                        if needed_end > file_len {
                            return Err(err_invalid_data(
                                "crashed writer: Skip points past end of file",
                            ));
                        }
                        let new_region = RegionMapping::create_writable(
                            &file,
                            next_region_index * region_size as u64,
                            region_size,
                        )?;
                        let new_hdr_ref =
                            unsafe { &*(new_region.as_ptr() as *const MessageHeader) };
                        verify_preinstall_signature(new_hdr_ref)?;
                        next_hdr += advance;
                        region_index = next_region_index;
                        current_region = new_region;
                    }
                    HeaderType::Roll | HeaderType::Channel => {
                        return Err(err_invalid_data(format!(
                            "crashed writer: unexpected header_type {:?} at write_position \
                             slot; clean up the channel files and start fresh",
                            stale_type
                        )));
                    }
                }

                // `message_count` must count every committed user record, the orphan included —
                // readers deliver it, and the next segment's `base_record_index` is derived from
                // this count. Whether the crash landed before or after the count was bumped cannot
                // be told from the header alone, so recount the segment. Recovery only.
                let walked = walk_segment(&file, region_size, |_, _, _| false)?;
                if walked.pos != next_hdr {
                    return Err(err_invalid_data(format!(
                        "crashed writer: recount stopped at {} ({:?}) but the recovered \
                         write slot is {}",
                        walked.pos, walked.stop, next_hdr
                    )));
                }
                // Nothing is written until every fallible step above has passed, and then in the
                // publish order (count, then position, both Release). A recovery that dies before
                // the position store leaves the orphan still at `write_position`, so the next open
                // simply recovers again; a `Live` reader never sees the new position with the old
                // count.
                with_ch_mut(&file, region_size, |ch| {
                    ch.message_count.store(walked.users, Ordering::Release);
                    ch.write_position
                        .store((next_hdr + HEADER_SLOT) as u64, Ordering::Release);
                })?;
            }

            Ok((
                file,
                region0,
                current_region,
                region_index,
                file_len,
                next_hdr,
            ))
        }
    }

    #[inline]
    fn channel_header(&self) -> &ChannelHeader {
        unsafe { &*(self.channel_region.as_ptr().add(MESSAGE_HEADER_SIZE) as *const ChannelHeader) }
    }

    /// Absolute index (from channel genesis, across all rolls) of the **next**
    /// user record this writer will commit — i.e. the current channel head.
    /// Equals `base_record_index + message_count` of the active file.
    #[inline]
    pub fn next_record_index(&self) -> u64 {
        let ch = self.channel_header();
        ch.base_record_index + ch.message_count.load(Ordering::Relaxed)
    }

    /// This channel's incarnation id (see [`WriterBuilder::generation`]). Read from the
    /// file, so reopening an existing channel reports the value it was created with —
    /// not whatever the builder was told.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.channel_header().generation
    }

    /// Ordinal of the segment file the writer is writing.
    #[inline]
    pub fn file_sequence(&self) -> u64 {
        self.file_sequence
    }

    /// Publish the advisory `(message_count, write_position)` pair after a commit.
    ///
    /// Both are Release stores, `message_count` first. That order is what lets a `Live` reader
    /// read the pair consistently (see `Reader::live_start`): having acquired `write_position`,
    /// it sees at least the count published with it, and a count that ran ahead of that position
    /// implies the record at the position is already visibly committed — which the reader checks.
    ///
    /// The count stays a `fetch_add` even though there is a single writer and a plain load +
    /// store would be enough for the protocol. On x86 the `fetch_add` is a `lock xadd`, which is
    /// also a full fence: it pushes the just-committed record out to readers at once. Replacing
    /// it raised writer throughput when saturated but cost readers 30–90 ns of p50/p90 latency at
    /// equal publish rates (tuned server, 2026-10-01). Release compiles to the same instructions as the
    /// Relaxed this replaced, so the commit path is unchanged from 5.x.
    #[inline]
    fn publish_wp(&self, pos: usize) {
        let ch = self.channel_header();
        ch.message_count.fetch_add(1, Ordering::Release);
        ch.write_position.store(pos as u64, Ordering::Release);
        if self.wake {
            wake::wake_all(&ch.wake_word);
        }
    }

    /// Store `val` to the channel header's `write_position` through
    /// the writer's already-mapped `channel_region`. Infallible —
    /// no extra mmap, no syscall. Used during roll publish to keep
    /// the entire path past the rename non-fallible. Release for the
    /// same reason as `publish_wp`: any `write_position` a reader
    /// acquires must carry the `message_count` stored before it.
    #[inline]
    fn store_wp_local(&self, val: u64) {
        let ch = self.channel_header();
        ch.write_position.store(val, Ordering::Release);
    }

    /// Reserve space for a message payload of length `msg_size` placed **after** a pre-installed header.
    /// Returns a mutable slice the caller can fill, or `None` on failure (e.g. MTU/roll).
    ///
    /// `msg_size` is the **upper bound** on the eventual `commit(length)`:
    /// callers may commit any `length <= msg_size`. This supports the
    /// worst-case-reserve / serialize-then-commit pattern — reserve enough
    /// for the largest possible serialised form, then commit the actual
    /// (smaller) byte count. Committing a `length` greater than the
    /// reserved `msg_size` is a contract violation and returns `Err`.
    pub fn try_reserve(&mut self, msg_size: usize) -> io::Result<&mut [u8]> {
        if self.mtu > 0 && msg_size as u64 > self.mtu {
            return Err(err_other("MTU exceeded"));
        }

        // The record and room for a Roll after it must fit in one region, and in a fresh
        // segment, or no roll could ever satisfy the reservation.
        let record_size = HEADER_SLOT + msg_size;
        let record_with_padding = align_up(record_size);
        let needed_total = record_with_padding + ROLL_TOTAL;
        if needed_total > self.region_size {
            return Err(err_other(format!(
                "reservation size {msg_size} cannot fit in region_size {} \
                 (needs {needed_total} bytes including header + padding + room for a Roll)",
                self.region_size,
            )));
        }
        if self.file_roll_size > 0 && (FIRST_RECORD + needed_total) as u64 > self.file_roll_size {
            return Err(err_other(format!(
                "reservation size {msg_size} cannot fit in file_roll_size {} \
                 (needs {needed_total} bytes)",
                self.file_roll_size,
            )));
        }

        loop {
            let wp = self.next_hdr_pos; // header slot for this record
            debug_assert_eq!(wp % ALIGN, 0, "next header must be 8-byte aligned");

            // Region-local offsets
            let off = wp % self.region_size;

            // file roll check includes next-header requirement
            if self.file_roll_size > 0 && wp + needed_total > self.file_roll_size as usize {
                self.roll_file()?;
                continue;
            }

            // region boundary: if we cannot fit record+next-header, roll to next region
            if off + needed_total > self.region_size {
                self.roll_over_region()?;
                continue;
            }

            // Pre-install slot i+1 BEFORE returning slot i's buffer. This
            // keeps FORMAT.md §9.6 strict (next slot pre-installed before
            // commit i) while removing the pre-install cacheline write
            // from `commit()`'s producer→consumer path. Recovery's
            // `verify_preinstall_signature` finds the expected signature
            // whether the crash is between reserve and commit, between
            // commit and publish_wp, or after publish_wp.
            let next_hdr_off = off + record_with_padding;
            let next_hdr_bytes = self
                .current_region
                .get_bytes_mut(next_hdr_off, MESSAGE_HEADER_SIZE)
                .ok_or_else(|| err_other("Failed to pre-install next header"))?;
            unsafe {
                *(next_hdr_bytes.as_mut_ptr() as *mut MessageHeader) = MessageHeader {
                    committed: 0,
                    header_type: HeaderType::User as u8,
                    message_type: 0,
                    length: 0,
                    user_meta_u64: 0,
                };
            }

            // Record what was reserved. `commit(length)` enforces
            // `length <= msg_size`; on a shorter commit it re-lays
            // the pre-install at the actual offset so the reader's
            // walk past slot i still finds the signature.
            self.pending_msg_size = Some(msg_size);

            let payload_off = off + HEADER_SLOT;
            return self
                .current_region
                .get_bytes_mut(payload_off, msg_size)
                .ok_or_else(|| err_other("message crosses region boundary"));
        }
    }

    /// Commit the message after filling the payload slice returned by `try_reserve`.
    /// Fills the header at `next_hdr_pos`, sets committed=1 (Release),
    /// then publishes write_position. The slot-i+1 pre-install was
    /// laid down by `try_reserve(reserved)`; when `length == reserved`
    /// (the common case) no cacheline write happens here. When
    /// `length < reserved` (a worst-case-reserve / serialize-then-commit
    /// pattern), the pre-install is re-laid at the actual `next_hdr_pos`
    /// so reader walks past slot i still land on a well-formed
    /// pre-installed slot.
    pub fn commit(&mut self, msg_type: u16, length: u32, user_meta_u64: u64) -> io::Result<()> {
        let reserved = self
            .pending_msg_size
            .take()
            .ok_or_else(|| err_other("commit without preceding try_reserve"))?;
        if length as usize > reserved {
            return Err(err_other(format!(
                "commit length {length} exceeds try_reserve size {reserved}",
            )));
        }

        let hdr_off = self.next_hdr_pos % self.region_size;

        let hdr_slice = self
            .current_region
            .get_bytes_mut(hdr_off, MESSAGE_HEADER_SIZE)
            .ok_or_else(|| err_other("No header to commit"))?;
        let hdr_ptr = hdr_slice.as_mut_ptr() as *mut MessageHeader;

        unsafe {
            (*hdr_ptr).committed = 0;
            (*hdr_ptr).length = length;
            (*hdr_ptr).header_type = HeaderType::User as u8;
            (*hdr_ptr).message_type = msg_type;
            (*hdr_ptr).user_meta_u64 = user_meta_u64;
        }

        // Compute the position of the next header slot from the
        // *committed* length. If shorter than reserved, re-lay the
        // pre-install at the new (closer) slot BEFORE flipping
        // committed=1 — FORMAT.md §9.6 demands slot i+1 be
        // pre-installed when a reader observes commit on slot i.
        let payload_end = self.next_hdr_pos + HEADER_SLOT + length as usize;
        let next_pos = align_up(payload_end);
        if (length as usize) < reserved {
            let new_next_off = next_pos % self.region_size;
            let bytes = self
                .current_region
                .get_bytes_mut(new_next_off, MESSAGE_HEADER_SIZE)
                .ok_or_else(|| err_other("Failed to re-install next header on short commit"))?;
            unsafe {
                *(bytes.as_mut_ptr() as *mut MessageHeader) = MessageHeader {
                    committed: 0,
                    header_type: HeaderType::User as u8,
                    message_type: 0,
                    length: 0,
                    user_meta_u64: 0,
                };
            }
        }

        // Release-store committed=1. Slot i+1's pre-install is durable
        // (either from `try_reserve` for matched-length commits, or
        // re-laid above for short commits).
        MessageHeader::commit(hdr_ptr);

        self.next_hdr_pos = next_pos;

        let next_payload = next_pos + HEADER_SLOT;
        self.publish_wp(next_payload);

        Ok(())
    }

    /// Roll to the next file, in the order of FORMAT.md §6.2: take or prepare the successor,
    /// stage the Roll, publish the successor, commit the Roll, switch, retention.
    pub fn roll_file(&mut self) -> io::Result<()> {
        self.pending_msg_size = None;

        let old_seq = self.file_sequence;
        let roll_pos = self.next_hdr_pos;
        let roll_off = roll_pos % self.region_size;
        if self.region_size - roll_off < ROLL_TOTAL {
            return Err(err_invalid_data(format!(
                "no room for a Roll at {roll_pos}"
            )));
        }
        let (old_base, old_count, generation) = {
            let ch = self.channel_header();
            (
                ch.base_record_index,
                ch.message_count.load(Ordering::Relaxed),
                ch.generation,
            )
        };
        let next_seq = old_seq
            .checked_add(1)
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "file sequence exhausted"))?;

        // 1) The successor, installed under its final name and still PREPARED.
        let taken = self.prefault.as_ref().and_then(|p| {
            p.claim(next_seq);
            p.try_take_segment(next_seq)
        });
        let (identity, prepared, worker) = match taken.filter(|(identity, _, _)| {
            identity.predecessor == self.instance && identity.predecessor_sequence == old_seq
        }) {
            Some((identity, prepared, worker)) => (identity, prepared, Some(worker)),
            None => {
                let (identity, prepared) = self.prepare_successor(next_seq, generation)?;
                (identity, prepared, None)
            }
        };
        crash_point("installed");
        let (
            new_file,
            mut new_channel_region,
            new_current_region,
            new_index,
            new_file_len,
            new_next_hdr,
        ) = prepared;
        let edge = RollEdge::after(old_seq, old_base, old_count, identity.instance)?;

        // 2) Stage the whole Roll in the region already mapped.
        let roll_hdr_ptr = {
            let bytes = self
                .current_region
                .get_bytes_mut(roll_off, ROLL_TOTAL)
                .ok_or_else(|| err_other("roll header outside region"))?;
            bytes[HEADER_SLOT..].copy_from_slice(&edge.encode());
            bytes.as_mut_ptr() as *mut MessageHeader
        };
        unsafe {
            *roll_hdr_ptr = MessageHeader {
                committed: 0,
                length: v4::ROLL_PAYLOAD_V4 as u32,
                header_type: HeaderType::Roll as u8,
                message_type: 0,
                user_meta_u64: 0,
            };
        }
        crash_point("staged");

        // 3) Stamp the base and publish. Nothing below fails.
        unsafe {
            (*(new_channel_region.as_mut_ptr().add(MESSAGE_HEADER_SIZE) as *mut ChannelHeader))
                .base_record_index = edge.next_base_record_index;
        }
        set_wake_flag(&new_channel_region, self.wake);
        crash_point("stamped");
        unsafe { v4::ext_at(new_channel_region.as_ptr()) }.publish();
        crash_point("published");

        // 4) Commit the Roll; then the terminal hint, one slot past it.
        MessageHeader::commit(roll_hdr_ptr);
        crash_point("committed");
        self.store_wp_local((roll_pos + ROLL_TOTAL + HEADER_SLOT) as u64);
        crash_point("positioned");

        if self.wake {
            wake::wake_all(&self.channel_header().wake_word);
        }

        self.file_sequence = next_seq;
        self.instance = identity.instance;
        let old_file = std::mem::replace(&mut self.file, new_file);
        let old_channel = std::mem::replace(&mut self.channel_region, new_channel_region);
        let old_current = std::mem::replace(&mut self.current_region, new_current_region);
        self.current_region_index = new_index;
        self.file_len = new_file_len;
        self.next_hdr_pos = new_next_hdr;
        if let Some(prefault) = &self.prefault {
            let wp = &self.channel_header().write_position as *const AtomicU64;
            prefault.rolled(
                prefault::Position {
                    sequence: self.file_sequence,
                    instance: self.instance,
                    file: &self.file,
                    file_len: self.file_len,
                    region: &mut self.current_region,
                    index: self.current_region_index,
                    // Safety: `channel_region` is handed to the prefaulter before it is dropped.
                    write_position: unsafe { &*wp },
                },
                [old_channel, old_current],
                old_file,
                worker,
            );
        } else {
            self.held.clear();
        }

        // Retention: best-effort; the roll has happened, so its errors are not the roll's.
        for _ in 0..self.deferred_prune.len() {
            let seq = self.deferred_prune.remove(0);
            self.prune(seq);
        }
        if let Some(n) = self.keep_files
            && next_seq >= n
        {
            self.prune(next_seq - n);
        }

        Ok(())
    }

    /// Unlink segment `seq`, unless the helper may still install it: then on a later roll.
    fn prune(&mut self, seq: u64) {
        match &self.prefault {
            Some(p) if p.installing(seq) => self.deferred_prune.push(seq),
            Some(p) => p.unlink(seq),
            None => {
                if let Ok(path) = make_channel_file_path(&self.base_path, seq) {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }

    /// Prepare and install segment `next_seq` here, or adopt the one the helper installed first.
    fn prepare_successor(
        &self,
        next_seq: u64,
        generation: u64,
    ) -> io::Result<(Identity, prefault::PreparedSegment)> {
        let identity = Identity::successor(InstanceId::fresh()?, self.instance, self.file_sequence);
        let attempt = make_attempt_path(&self.base_path, next_seq, identity.instance)?;
        let final_path = make_channel_file_path(&self.base_path, next_seq)?;
        let segment = Self::prepare_segment_at(
            &attempt,
            next_seq,
            self.region_size,
            self.file_roll_size,
            self.mtu,
            &self.channel_name,
            0,
            generation,
            &identity,
        )?;
        crash_point("attempt");
        match v4::install_no_replace(&attempt, &final_path) {
            Ok(()) => Ok((identity, segment)),
            Err(e) => {
                drop(segment);
                let _ = std::fs::remove_file(&attempt);
                if e.kind() != ErrorKind::AlreadyExists {
                    return Err(e);
                }
                self.adopt_successor(next_seq, generation, &final_path)
            }
        }
    }

    /// The successor another attempt installed first: used only if it is an empty, `PREPARED`
    /// file of this channel naming this file as its parent.
    fn adopt_successor(
        &self,
        next_seq: u64,
        generation: u64,
        path: &Path,
    ) -> io::Result<(Identity, prefault::PreparedSegment)> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let len = preallocation_len(self.region_size, self.file_roll_size)?;
        let refuse = |why: &str| {
            Err(err_invalid_data(format!(
                "segment {next_seq} is already installed and {why}"
            )))
        };
        if file.metadata()?.len() != len {
            return refuse("has another length");
        }
        let region0 = RegionMapping::create_writable(&file, 0, self.region_size)?;
        let ch = get_channel_header(region0.as_ptr());
        validate_channel_header(ch, self.region_size, next_seq)?;
        let identity = validate_v4_prefix(region0.as_ptr())?;
        if is_published(region0.as_ptr())? {
            return refuse("is published");
        }
        if identity.predecessor != self.instance
            || identity.predecessor_sequence != self.file_sequence
            || ch.generation != generation
            || ch.mtu as u64 != self.mtu
            || ch.channel_name != self.channel_name
        {
            return refuse("belongs elsewhere");
        }
        if ch.message_count.load(Ordering::Acquire) != 0
            || ch.write_position.load(Ordering::Acquire) != (FIRST_RECORD + HEADER_SLOT) as u64
        {
            return refuse("is not empty");
        }
        let current = RegionMapping::create_writable(&file, 0, self.region_size)?;
        verify_preinstall_signature(unsafe {
            &*(current.as_ptr().add(FIRST_RECORD) as *const MessageHeader)
        })?;
        Ok((identity, (file, region0, current, 0, len, FIRST_RECORD)))
    }

    fn roll_over_region(&mut self) -> io::Result<()> {
        // Same rationale as `roll_file`: any pending reservation is
        // about to be superseded by a Skip in the OLD region. Clear
        // so a follow-up `commit` doesn't act on stale length.
        self.pending_msg_size = None;

        let wp = self.next_hdr_pos;
        debug_assert_eq!(wp % ALIGN, 0, "roll_over_region: wp must be aligned");

        let off = wp % self.region_size;
        let leftover = self.region_size - off;

        if leftover >= HEADER_SLOT {
            let skip_len = leftover - HEADER_SLOT;
            let new_wp = wp + HEADER_SLOT + skip_len; // == next region start
            let next_idx = (new_wp / self.region_size) as u64;

            // 1) Grow file and map the *next* region first.
            let mut new_region = self.region_at(next_idx)?;

            // Pre-install header at the start of the new region (committed = 0).
            if let Some(h) = new_region.get_bytes_mut(0, MESSAGE_HEADER_SIZE) {
                unsafe {
                    *(h.as_mut_ptr() as *mut MessageHeader) = MessageHeader {
                        committed: 0,
                        header_type: HeaderType::User as u8,
                        message_type: 0,
                        length: 0,
                        user_meta_u64: 0,
                    };
                }
            } else {
                return Err(err_other(
                    "roll_over_region: cannot pre-install next header",
                ));
            }

            // 2) Now write and commit the Skip in the *old* region.
            {
                let hdr_slice = self
                    .current_region
                    .get_bytes_mut(off, MESSAGE_HEADER_SIZE)
                    .ok_or_else(|| err_other("roll_over_region: header bytes"))?;
                unsafe {
                    let hdr_ptr = hdr_slice.as_mut_ptr() as *mut MessageHeader;
                    *hdr_ptr = MessageHeader {
                        committed: 0,
                        length: skip_len as u32,
                        header_type: HeaderType::Skip as u8,
                        message_type: 0,
                        user_meta_u64: 0,
                    };
                    MessageHeader::commit(hdr_ptr);
                }
            }

            // 3) Switch writer state to the new region and publish wp.
            self.switch_region(new_region, next_idx);
            self.next_hdr_pos = new_wp;
            // A Skip is not a user record: advance the advisory write_position but
            // do not bump `message_count` (which counts user records only).
            self.store_wp_local((new_wp + HEADER_SLOT) as u64);
            Ok(())
        } else {
            // Not even space for a header: jump straight to next region start
            let next_region_start = ((wp / self.region_size) + 1) * self.region_size;

            let next_idx = (next_region_start / self.region_size) as u64;
            let new_region = self.region_at(next_idx)?;
            self.switch_region(new_region, next_idx);

            // Pre-install header at start
            if let Some(h) = self.current_region.get_bytes_mut(0, MESSAGE_HEADER_SIZE) {
                unsafe {
                    *(h.as_mut_ptr() as *mut MessageHeader) = MessageHeader {
                        committed: 0,
                        header_type: HeaderType::User as u8,
                        message_type: 0,
                        length: 0,
                        user_meta_u64: 0,
                    };
                }
            }

            self.next_hdr_pos = next_region_start;
            // Skip (no header fit either): advisory wp only, no user-record count bump.
            self.store_wp_local((next_region_start + HEADER_SLOT) as u64);
            Ok(())
        }
    }

    #[inline]
    fn start_prefault(&mut self) -> io::Result<()> {
        let wp = &self.channel_header().write_position as *const AtomicU64;
        let regions_per_file = match self.file_roll_size {
            0 => None,
            roll => Some(preallocation_len(self.region_size, roll)? / self.region_size as u64),
        };
        let spec = (self.file_roll_size > 0).then(|| prefault::SegmentSpec {
            base_path: self.base_path.clone(),
            file_roll_size: self.file_roll_size,
            mtu: self.mtu,
            channel_name: self.channel_name,
            generation: self.channel_header().generation,
        });
        let prefaulter = prefault::Prefaulter::start(
            self.base_path.clone(),
            prefault::Position {
                sequence: self.file_sequence,
                instance: self.instance,
                file: &self.file,
                file_len: self.file_len,
                region: &mut self.current_region,
                index: self.current_region_index,
                // Safety: the header outlives the prefaulter, which is dropped first.
                write_position: unsafe { &*wp },
            },
            self.region_size,
            regions_per_file,
            spec,
            self.unmap,
            self.helper.unwrap_or(Helper::inherit()),
        )?;
        self.prefault = Some(prefaulter);
        Ok(())
    }

    /// Why the helper thread stopped, if it did: an OS error, such as `EINVAL` from `madvise` on
    /// a kernel older than 5.14, or a panic. The writer then carries on as it does without a
    /// helper: it maps, faults and unmaps its own regions and deletes the segments retention
    /// drops, those it handed over before it noticed included.
    pub fn helper_error(&self) -> Option<io::Error> {
        self.prefault.as_ref()?.shared.failure.error()
    }

    /// The writable mapping of region `index`: the one prepared ahead if there is one.
    fn region_at(&mut self, index: u64) -> io::Result<RegionMapping<Writable>> {
        let needed_end = (index + 1) * self.region_size as u64;
        let sequence = self.file_sequence;
        if let Some(region) = self.prefault.as_ref().and_then(|p| p.take(sequence, index)) {
            self.file_len = self.file_len.max(needed_end);
            return Ok(region);
        }
        self.ensure_len(needed_end)?;
        RegionMapping::create_writable(
            &self.file,
            index * self.region_size as u64,
            self.region_size,
        )
    }

    fn switch_region(&mut self, region: RegionMapping<Writable>, index: u64) {
        let left = std::mem::replace(&mut self.current_region, region);
        self.current_region_index = index;
        if let Some(prefault) = &self.prefault {
            prefault.moved(&mut self.current_region, index, left);
        } else if self.unmap == Unmap::AtFileRoll {
            self.held.push(left);
        }
    }

    fn ensure_len(&mut self, want: u64) -> io::Result<()> {
        if let Some(prefault) = &self.prefault {
            let mut segment = prefault::lock(&prefault.shared.segment);
            // Through the helper's handle only while it is on this segment: one it could not
            // open at the roll is grown here, as without a helper. Growing the old one instead
            // would leave this file short of the region the writer maps next.
            if segment.sequence == self.file_sequence {
                if want > segment.len {
                    segment.file.set_len(want)?;
                    segment.len = want;
                }
                self.file_len = self.file_len.max(segment.len);
                return Ok(());
            }
        }
        if want > self.file_len {
            // ftruncate to grow before any mmap touches those pages.
            self.file.set_len(want)?;
            self.file_len = want;
        }
        Ok(())
    }
}

// ========== Reader ==========

/// Where a new [`Reader`] starts. Non-exhaustive: further modes may be added without a major
/// version, so a `match` on it needs a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReaderMode {
    LateJoin, // start from earliest existing file
    Live,     // start from latest existing file (at next header slot)
    /// Start so the next user record read is absolute index `i` (see [`Reader::position`]).
    ///
    /// `i` may equal the channel head, in which case the reader waits for the next record like
    /// `Live`. Opening fails with `ErrorKind::InvalidInput` if `i` is past the head, and with
    /// `ErrorKind::NotFound` if `i` is older than the earliest retained segment (pruned by
    /// `keep_files`); that error carries an [`IndexPruned`] naming the earliest retained index.
    /// A `NotFound` without it means there is no channel at the path, or that its segments kept
    /// disappearing under retention during the search.
    ///
    /// Cost: the segment holding `i` is found by binary search over the segments' headers
    /// (one page each); inside it the reader steps over the records before `i` one header at
    /// a time, without touching payloads. That is linear in the records ahead of `i` *in its
    /// segment*, not O(1): about 14 ns per record skipped on `/dev/shm` with 64-byte records,
    /// so roughly 280 ms to reach the end of a 20M-record segment. Open once, not per message;
    /// a smaller `file_roll_size` caps the worst case.
    At(u64),
}

/// Borrowed view of a message payload and header.
pub struct MessageRef<'a> {
    mapping: &'a RegionMapping<ReadOnly>,
    header_offset: usize,
    payload_len: usize,
}

impl<'a> MessageRef<'a> {
    #[inline]
    fn payload_offset(&self) -> usize {
        self.header_offset + HEADER_SLOT
    }

    #[inline]
    pub fn header(&self) -> &MessageHeader {
        let ptr = unsafe { self.mapping.as_ptr().add(self.header_offset) };
        unsafe { &*(ptr as *const MessageHeader) }
    }

    #[inline]
    pub fn payload(&self) -> &'a [u8] {
        let payload_offset = self.payload_offset();
        let ptr = unsafe { self.mapping.as_ptr().add(payload_offset) };
        unsafe { slice::from_raw_parts(ptr, self.payload_len) }
    }

    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.payload_len
    }
}

/// Where a located record lives, decoded so the borrow does not freeze `self`.
#[derive(Clone, Copy, Debug)]
struct RecordLoc {
    map_idx: usize,
    header_offset: usize,
    payload_len: usize,
}

struct FoundRecord {
    loc: RecordLoc,
    message_type: u16,
    user_meta_u64: u64,
}

/// What the next user record says about itself, read without consuming it.
///
/// Enough to decide whether to read that record, or which of several channels to read from
/// first — both without decoding a payload or copying one out.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PeekedHeader {
    /// The producer's record kind.
    pub message_type: u16,
    /// The producer's 8 opaque bytes. By convention across most publishers this is when the
    /// record reached the channel, which is what makes ordering several channels possible here.
    pub user_meta_u64: u64,
    /// Payload length in bytes.
    pub length: u32,
}

/// An owned message: a share of the mapped region plus the record's position
/// within it. Carries no lifetime, so it can be stored, collected into a
/// `Vec`, or sent to another thread.
///
/// # Retention
///
/// Holding one keeps its **entire region** mapped — `region_size` bytes, 1 MiB
/// by default — even after the Reader has pruned it, rolled past it, or been
/// dropped. Retaining messages therefore makes the reader's mapped footprint
/// consumer-controlled rather than bounded by the read cursor. Copy the payload
/// out if you need to hold it for long.
///
/// The mapped bytes are the floor, not the whole cost. A live mapping is a
/// reference to the file's inode, so once a writer's `keep_files` retention has
/// unlinked that segment, one retained message keeps the **whole segment file**
/// alive — `file_roll_size` bytes, not `region_size`. On a memory-backed
/// filesystem those bytes are RAM that is not reclaimed until the last message
/// from that segment is dropped, so a consumer keeping even one message per
/// segment defeats the bound `keep_files` exists to enforce and can exhaust the
/// filesystem (a writer then hits `ENOSPC`, or `SIGBUS` on its next page
/// touch). On tmpfs with retention configured, treat a retained message as
/// pinning a file rather than a region, and copy the payload out instead of
/// keeping messages across rolls.
///
/// # Truncation
///
/// As with [`MessageRef`], the mapping outlives the Reader's file descriptor but
/// not a writer that *shrinks* the file: touching a page past a truncation
/// raises `SIGBUS`. A long-lived `OwnedMessage` widens that window
/// considerably.
pub struct OwnedMessage {
    region: Arc<MappedRegion>,
    header_offset: usize,
    payload_len: usize,
}

impl OwnedMessage {
    #[inline]
    pub fn header(&self) -> &MessageHeader {
        let ptr = unsafe { self.region.mapping.as_ptr().add(self.header_offset) };
        unsafe { &*(ptr as *const MessageHeader) }
    }

    /// Payload bytes. Borrowed from `self` rather than the mapping, so the
    /// message must outlive the slice.
    #[inline]
    pub fn payload(&self) -> &[u8] {
        let ptr = unsafe {
            self.region
                .mapping
                .as_ptr()
                .add(self.header_offset + HEADER_SLOT)
        };
        unsafe { slice::from_raw_parts(ptr, self.payload_len) }
    }

    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.payload_len
    }

    /// Borrowed view of the same record, for code generic over `MessageRef`.
    #[inline]
    pub fn as_ref(&self) -> MessageRef<'_> {
        MessageRef {
            mapping: &self.region.mapping,
            header_offset: self.header_offset,
            payload_len: self.payload_len,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MsgPos {
    // Index into Reader.batch_segs (not maps).
    seg: u16,
    // Offset within the segment's mapping where the message header starts.
    off: u32,
}

#[derive(Clone, Copy, Debug)]
struct BatchSeg {
    // Index into Reader.maps for the mapping that backs this segment.
    map_idx: usize,
    // Start offset within the mapping (inclusive).
    start: u32,
    // End offset within the mapping (exclusive).
    end: u32,
}

struct MappedRegion {
    file_sequence: u64,
    region_idx: u64,
    mapping: RegionMapping<ReadOnly>,
}

#[derive(Debug, Clone, Copy)]
struct ScannedHeader {
    is_committed: bool,
    header_type: HeaderType,
    payload_len: usize,
    total_len: usize,
    message_type: u16,
    user_meta_u64: u64,
}

/// Borrowed view over a batch of user messages.
pub struct MessageBatch<'a> {
    segs: &'a [BatchSeg],
    pos: &'a [MsgPos],
    maps: &'a [Arc<MappedRegion>],
}

impl<'a> MessageBatch<'a> {
    #[inline]
    /// Number of user messages in this batch.
    pub fn len(&self) -> usize {
        self.pos.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pos.is_empty()
    }

    #[inline]
    /// Access a user message by index in scan order (0..len()).
    pub fn get(&self, index: usize) -> Option<MessageRef<'a>> {
        self.pos.get(index).map(|pos| self.message_at(*pos))
    }

    #[inline]
    /// Access a user message by index without bounds checks.
    ///
    /// # Safety
    /// Caller must ensure `index < self.len()`.
    pub unsafe fn get_unchecked(&self, index: usize) -> MessageRef<'a> {
        let pos = *unsafe { self.pos.get_unchecked(index) };
        self.message_at(pos)
    }

    #[inline]
    fn message_at(&self, pos: MsgPos) -> MessageRef<'a> {
        let seg = &self.segs[pos.seg as usize];
        debug_assert!(pos.off >= seg.start);
        debug_assert!(pos.off < seg.end);
        let map = &self.maps[seg.map_idx].mapping;
        let header_offset = pos.off as usize;
        let header_end = header_offset + MESSAGE_HEADER_SIZE;
        assert!(
            header_end <= map.region_size(),
            "message header out of bounds"
        );
        let hdr_ptr = unsafe { map.as_ptr().add(header_offset) as *const MessageHeader };
        let mh = unsafe { &*hdr_ptr };
        let payload_end = header_offset + HEADER_SLOT + mh.length as usize;
        assert!(
            payload_end <= map.region_size(),
            "message payload out of bounds"
        );
        debug_assert_eq!(mh.header_type, HeaderType::User as u8);
        MessageRef {
            mapping: map,
            header_offset,
            payload_len: mh.length as usize,
        }
    }

    #[inline]
    /// Iterate user messages in this batch (supports `.rev()`).
    pub fn iter(&'a self) -> impl DoubleEndedIterator<Item = MessageRef<'a>> + 'a {
        self.pos.iter().map(|p| self.message_at(*p))
    }

    /// Promote one message to an [`OwnedMessage`], which carries no lifetime and
    /// so may outlive both the batch and the Reader.
    ///
    /// Shares the mapping rather than copying the payload, so this is the cheap
    /// way to keep a few records out of an otherwise borrowed batch — at the
    /// retention cost described on [`OwnedMessage`].
    #[inline]
    pub fn get_owned(&self, index: usize) -> Option<OwnedMessage> {
        self.pos.get(index).map(|pos| self.owned_at(*pos))
    }

    #[inline]
    fn owned_at(&self, pos: MsgPos) -> OwnedMessage {
        // Via `message_at` so the bounds assertions live in exactly one place.
        let msg = self.message_at(pos);
        OwnedMessage {
            region: Arc::clone(&self.maps[self.segs[pos.seg as usize].map_idx]),
            header_offset: msg.header_offset,
            payload_len: msg.payload_len,
        }
    }
}

pub struct Reader {
    map_ahead: Option<map_ahead::MapAhead>,
    helper: Option<Helper>,
    unmap: Unmap,
    /// Regions of the current segment read past and kept mapped until the roll (`AtFileRoll`).
    held: Vec<Arc<MappedRegion>>,
    base_path: PathBuf,
    file_sequence: u64,
    file: File,
    read_position: usize,
    /// Absolute index of the next user record this reader will return (see `position()`).
    position: u64,
    region_size_cached: usize,
    mtu_cached: u32,
    channel_name_cached: [u8; CHANNEL_NAME_MAX],
    /// `base_record_index` of the file currently open (updated on each roll).
    base_record_index_cached: u64,
    /// The channel's incarnation id — identical in every segment, so it is read once at
    /// open and re-verified on each roll.
    generation_cached: u64,
    batch_limit: Option<u16>,
    batch_segs: Vec<BatchSeg>,
    batch_pos: Vec<MsgPos>,
    // Refcounted so an `OwnedMessage` can keep its region mapped after the
    // Reader has pruned it, rolled past it, or been dropped entirely.
    maps: Vec<Arc<MappedRegion>>, // last entry is current; older entries kept for batch segments
    /// Page 0 of the segment being read, for its wake flag and wake word wherever the cursor
    /// is. Replaced at every roll.
    header: RegionMapping<ReadOnly>,
    /// The segment's instance ID: the parent its successor must name.
    instance: InstanceId,
    /// This segment's wake flag proved wrong: twice a capped sleep ran out with a record
    /// waiting and the word unmoved in between, so nobody is waking us (an older writer
    /// reopened the channel). Back off instead for the rest of the segment. Reset at every roll.
    wake_untrusted: bool,
    /// The word's value at the last capped sleep that ran out with a record waiting, while it
    /// still held the value slept on. One such miss is not enough to distrust the flag: a
    /// waking writer preempted between publishing a record and bumping the word looks the
    /// same once. Reset at every roll.
    wake_miss: Option<u32>,
    /// Futex waits that ended because the word changed: proof in tests that the reader was
    /// woken rather than backed off.
    #[cfg(test)]
    woken: u32,
}

/// Where in a segment a new Reader starts.
#[derive(Debug, Clone, Copy)]
enum SegmentStart {
    /// Offset 0: the segment's first record.
    Beginning,
    /// The next header slot, read from `write_position` (`Live`).
    Head,
    /// A header slot already located, and the absolute index of the record there.
    At { read_pos: usize, position: u64 },
}

/// The fields of a segment's `ChannelHeader` that locating a record needs.
#[derive(Debug, Clone, Copy)]
struct SegmentHeader {
    region_size: usize,
    base_record_index: u64,
    message_count: u64,
    generation: u64,
}

/// Map one page of `file`, check it opens with a valid `ChannelHeader` for `sequence`, and
/// return the fields a seek needs. `message_count` is acquired, so every record it counts is
/// visibly committed.
fn read_segment_header(file: &File, sequence: u64) -> io::Result<SegmentHeader> {
    let map = RegionMapping::create_read_only(file, 0, region::page_size())?;
    let mh = unsafe { &*(map.as_ptr() as *const MessageHeader) };
    if mh.parsed_header_type()? != HeaderType::Channel {
        return Err(err_invalid_data(format!(
            "segment {sequence} does not begin with a Channel header"
        )));
    }
    let ch = get_channel_header(map.as_ptr());
    validate_channel_header(ch, ch.region_size as usize, sequence)?;
    validate_v4_prefix(map.as_ptr())?;
    if !is_published(map.as_ptr())? {
        return Err(io::Error::new(
            ErrorKind::NotFound,
            format!("segment {sequence} is not published yet"),
        ));
    }
    Ok(SegmentHeader {
        region_size: ch.region_size as usize,
        base_record_index: ch.base_record_index,
        message_count: ch.message_count.load(Ordering::Acquire),
        generation: ch.generation,
    })
}

/// Open segment `sequence` read-only; `Ok(None)` if it does not exist (never created, or
/// unlinked by retention since the directory scan).
fn open_segment_if_present(base_path: &Path, sequence: u64) -> io::Result<Option<File>> {
    let path = make_channel_file_path(base_path, sequence)?;
    match OpenOptions::new().read(true).write(false).open(&path) {
        Ok(file) => Ok(Some(file)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

impl Reader {
    /// Open a Reader:
    /// - LateJoin => earliest file; read_position = 0
    /// - Live => latest file; read_position = write_position (next header slot)
    /// - At(i) => the file holding record `i`; read_position = that record's header slot
    ///
    /// LateJoin and At race with a writer configured with `keep_files(N)`:
    /// a sequence returned by the directory scan can be unlinked by the
    /// writer's next roll before this call's `open()` syscall runs,
    /// surfacing as `ENOENT`. The next-lowest sequence is almost always
    /// still present, so we re-scan and try again up to
    /// `MAX_OPEN_RETRIES` times. A genuinely missing channel still fails
    /// fast — after the retries are exhausted the `ENOENT` propagates.
    /// Live retries too: the latest sequence can roll, or under fast rolls
    /// be unlinked, between the scan and the open, and the reader then joins
    /// the tail in the newest segment instead.
    pub fn open<P: AsRef<Path>>(path: P, mode: ReaderMode) -> io::Result<Self> {
        Self::open_with(path, mode, None)
    }

    /// [`Reader::open`], refusing a channel whose generation is not `expected_generation`.
    fn open_with<P: AsRef<Path>>(
        path: P,
        mode: ReaderMode,
        expected_generation: Option<u64>,
    ) -> io::Result<Self> {
        const MAX_OPEN_RETRIES: usize = 8;
        let base_path = path.as_ref().to_path_buf();
        let reader = match mode {
            ReaderMode::Live => {
                // The newest segment can stop being the newest while we open it: the writer
                // rolls (`live_start` notices and reports it stale), or rolls often enough
                // under retention that the file is unlinked before the open. Either way the
                // tail has moved on; list again and join it there.
                let mut opened = None;
                for _ in 0..MAX_OPEN_RETRIES {
                    let seq = find_latest_published_sequence(&base_path)?;
                    let Some(file) = open_segment_if_present(&base_path, seq)? else {
                        continue;
                    };
                    match Self::from_segment(base_path.clone(), seq, file, SegmentStart::Head) {
                        Ok(reader) => {
                            opened = Some(reader);
                            break;
                        }
                        Err(e) if StaleSegment::is(&e) => continue,
                        Err(e) => return Err(e),
                    }
                }
                opened.ok_or_else(|| Self::retries_exhausted(&base_path))?
            }
            ReaderMode::LateJoin => {
                let mut opened = None;
                for _ in 0..MAX_OPEN_RETRIES {
                    let seq = find_earliest_sequence(&base_path)?;
                    if let Some(file) = open_segment_if_present(&base_path, seq)? {
                        opened = Some(Self::from_segment(
                            base_path.clone(),
                            seq,
                            file,
                            SegmentStart::Beginning,
                        )?);
                        break;
                    }
                }
                opened.ok_or_else(|| Self::retries_exhausted(&base_path))?
            }
            ReaderMode::At(index) => {
                let mut opened = None;
                for _ in 0..MAX_OPEN_RETRIES {
                    opened = Self::locate(&base_path, index, expected_generation)?;
                    if opened.is_some() {
                        break;
                    }
                }
                opened.ok_or_else(|| Self::retries_exhausted(&base_path))?
            }
        };
        if let Some(expected) = expected_generation
            && reader.generation_cached != expected
        {
            return Err(GenerationMismatch {
                expected,
                found: reader.generation_cached,
            }
            .into_io());
        }
        Ok(reader)
    }

    fn retries_exhausted(base_path: &Path) -> io::Error {
        io::Error::new(
            ErrorKind::NotFound,
            format!(
                "Reader::open: the segments of {:?} kept rolling or disappearing under retention \
                 while opening (or none exist)",
                base_path
            ),
        )
    }

    /// Find the segment holding absolute record `index` and open a Reader positioned on it.
    /// `Ok(None)` means a segment vanished under retention mid-search; the caller rescans.
    fn locate(
        base_path: &Path,
        index: u64,
        expected_generation: Option<u64>,
    ) -> io::Result<Option<Self>> {
        let mut seqs = find_all_sequences(base_path)?;
        // A newest file installed ahead of its roll is not history.
        while seqs.len() > 1
            && let Some(&last) = seqs.last()
            && !segment_is_published(base_path, last)?
        {
            seqs.pop();
        }
        let (Some(&first), Some(&last)) = (seqs.first(), seqs.last()) else {
            return Err(io::Error::new(
                ErrorKind::NotFound,
                format!("no channel at {:?}", base_path),
            ));
        };
        let probe = |seq: u64| -> io::Result<Option<(File, SegmentHeader)>> {
            let Some(file) = open_segment_if_present(base_path, seq)? else {
                return Ok(None);
            };
            let header = read_segment_header(&file, seq)?;
            Ok(Some((file, header)))
        };

        let Some((_, earliest)) = probe(first)? else {
            return Ok(None);
        };
        // Generation first: a recreated channel restarts at index 0, and must be reported as a
        // different channel rather than as a pruned or out-of-range index.
        if let Some(expected) = expected_generation
            && earliest.generation != expected
        {
            return Err(GenerationMismatch {
                expected,
                found: earliest.generation,
            }
            .into_io());
        }
        if index < earliest.base_record_index {
            return Err(IndexPruned {
                index,
                earliest: earliest.base_record_index,
            }
            .into_io());
        }
        let Some((_, latest)) = probe(last)? else {
            return Ok(None);
        };
        let head = latest.base_record_index + latest.message_count;
        if index > head {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("record {index} is past the channel head {head}"),
            ));
        }

        // The last segment whose first record is at or before `index`. Bases only grow with
        // the sequence, and `seqs[0]` qualifies, so the search keeps `base(lo) <= index`.
        let (mut lo, mut hi) = (0, seqs.len() - 1);
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            let Some((_, header)) = probe(seqs[mid])? else {
                return Ok(None);
            };
            if header.base_record_index <= index {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        let seq = seqs[lo];
        let Some((file, header)) = probe(seq)? else {
            return Ok(None);
        };
        if header.generation != earliest.generation {
            return Err(err_invalid_data(format!(
                "generation mismatch: segment {seq} has generation {} but segment {first} has {} \
                 (segments from a different incarnation of this path?)",
                header.generation, earliest.generation
            )));
        }

        let skip = index - header.base_record_index;
        let walked = walk_segment(&file, header.region_size, |_, header_type, users| {
            header_type == HeaderType::User && users == skip
        })?;
        if walked.users != skip || walked.stop == WalkStop::EndOfFile {
            return Err(err_invalid_data(format!(
                "segment {seq} holds {} user records before stopping ({:?}), \
                 but its header implies record {index} is in it",
                walked.users, walked.stop
            )));
        }
        Self::from_segment(
            base_path.to_path_buf(),
            seq,
            file,
            SegmentStart::At {
                read_pos: walked.pos,
                position: index,
            },
        )
        .map(Some)
    }

    /// Where a `Live` reader starts, and the absolute index of the record there.
    ///
    /// The writer publishes `message_count` then `write_position`, both Release, after
    /// committing a record. Acquiring `write_position` therefore guarantees a count at least
    /// as new as that position; a count *newer* than the position means the writer has
    /// since committed the record at that position, and the count was acquired before the
    /// slot is checked. So an uncommitted slot proves the count matches the position exactly.
    /// A committed User or Skip there means the writer is mid-publish, and a moment later the
    /// pair has moved on.
    ///
    /// Rolls need care, because after one the old segment's `write_position` points one slot
    /// *past* its `Roll` marker: at bytes nothing will ever commit, which may hold leftovers of
    /// an earlier payload or lie beyond the end of the file. So before the slot is looked at,
    /// and after `write_position` was acquired, `newer_segment_exists` is asked; if a newer
    /// segment exists this one is no longer the tail and the open is reported stale
    /// ([`StaleSegment`]), for the caller to retry on the newest segment. A roll that finished
    /// before the load renamed its segment in earlier still, so this cannot miss it. A `Roll`
    /// committed after the check is terminal rather than transient: start on it; the count is
    /// final, since no user record follows a `Roll`.
    ///
    /// A writer that died between commit and publish leaves the slot committed for good; past
    /// a short budget the count is taken by walking the segment instead.
    fn live_start(
        file: &File,
        ch: &ChannelHeader,
        region_size: usize,
        newer_segment_exists: &dyn Fn() -> io::Result<bool>,
    ) -> io::Result<(usize, u64)> {
        const SPIN_BUDGET: Duration = Duration::from_millis(1);
        let started = Instant::now();
        let mut map: Option<(usize, RegionMapping<ReadOnly>)> = None;
        loop {
            let wp = ch.write_position.load(Ordering::Acquire) as usize;
            let count = ch.message_count.load(Ordering::Acquire);
            if newer_segment_exists()? {
                return Err(StaleSegment.into_io());
            }
            let read_pos = wp.saturating_sub(HEADER_SLOT);
            let region_idx = read_pos / region_size;
            let region = match &map {
                Some((idx, region)) if *idx == region_idx => region,
                _ => {
                    // Only a finished roll leaves `write_position` past the end of the file,
                    // and that was ruled out above.
                    if ((region_idx + 1) * region_size) as u64 > file.metadata()?.len() {
                        return Err(err_invalid_data(format!(
                            "Live open: write_position slot {read_pos} is past the end of the \
                             segment, but no newer segment exists"
                        )));
                    }
                    let region = RegionMapping::create_read_only(
                        file,
                        (region_idx * region_size) as u64,
                        region_size,
                    )?;
                    &map.insert((region_idx, region)).1
                }
            };
            let mh =
                unsafe { &*(region.as_ptr().add(read_pos % region_size) as *const MessageHeader) };
            if !mh.is_committed()? {
                return Ok((read_pos, ch.base_record_index + count));
            }
            if mh.parsed_header_type()? == HeaderType::Roll {
                return Ok((read_pos, ch.base_record_index + count));
            }
            if started.elapsed() >= SPIN_BUDGET {
                let walked = walk_segment(file, region_size, |pos, _, _| pos >= read_pos)?;
                if walked.pos != read_pos {
                    return Err(err_invalid_data(format!(
                        "Live open: write_position slot {read_pos} is not on the record chain \
                         (walk stopped at {}, {:?})",
                        walked.pos, walked.stop
                    )));
                }
                return Ok((read_pos, ch.base_record_index + walked.users));
            }
            std::hint::spin_loop();
        }
    }

    /// Build a Reader over an already-open segment: validate its header, work out where to
    /// start, and map that region.
    fn from_segment(
        base_path: PathBuf,
        sequence: u64,
        file: File,
        start: SegmentStart,
    ) -> io::Result<Self> {
        let page0 = RegionMapping::create_read_only(&file, 0, region::page_size())?;
        let mh = unsafe { &*(page0.as_ptr() as *const MessageHeader) };
        let header_type = mh.parsed_header_type()?;
        if header_type != HeaderType::Channel {
            return Err(err_invalid_data(format!(
                "file has first {:?}, expected Channel header",
                header_type
            )));
        }
        let ch = get_channel_header(page0.as_ptr());
        let region_size = ch.region_size as usize;
        validate_channel_header(ch, region_size, sequence)?;
        let identity = validate_v4_prefix(page0.as_ptr())?;
        if !is_published(page0.as_ptr())? {
            return Err(io::Error::new(
                ErrorKind::NotFound,
                format!("segment {sequence} of {base_path:?} is not published yet"),
            ));
        }

        let (read_pos, position) = match start {
            SegmentStart::Beginning => (0, ch.base_record_index),
            SegmentStart::Head => {
                // Newer: the next segment is published, or this one was pruned (oldest first).
                let this = make_channel_file_path(&base_path, sequence)?;
                let newer_segment_exists =
                    || Ok(segment_is_published(&base_path, sequence + 1)? || !this.try_exists()?);
                Self::live_start(&file, ch, region_size, &newer_segment_exists)?
            }
            SegmentStart::At { read_pos, position } => (read_pos, position),
        };
        let channel_name = ch.channel_name;
        let base_record_index = ch.base_record_index;
        let mtu = ch.mtu;
        let generation = ch.generation;

        let region_index = (read_pos / region_size) as u64;
        let current_region =
            RegionMapping::create_read_only(&file, region_index * region_size as u64, region_size)?;
        let mut maps = Vec::with_capacity(DEFAULT_BATCH_MAPS_CAP);
        maps.push(Arc::new(MappedRegion {
            file_sequence: sequence,
            region_idx: region_index,
            mapping: current_region,
        }));

        Ok(Self {
            map_ahead: None,
            helper: None,
            unmap: Unmap::Immediate,
            held: Vec::new(),
            base_path,
            file_sequence: sequence,
            file,
            read_position: read_pos,
            position,
            region_size_cached: region_size,
            mtu_cached: mtu,
            channel_name_cached: channel_name,
            base_record_index_cached: base_record_index,
            generation_cached: generation,
            batch_limit: None,
            batch_segs: Vec::with_capacity(DEFAULT_BATCH_SEGS_CAP),
            batch_pos: Vec::with_capacity(DEFAULT_BATCH_POS_CAP),
            maps,
            header: page0,
            instance: identity.instance,
            wake_untrusted: false,
            wake_miss: None,
            #[cfg(test)]
            woken: 0,
        })
    }

    /// Absolute index (from channel genesis, across all rolls) of the next user record this
    /// reader will return. Advances by one for every user record consumed, on every read path
    /// (`try_read`, `try_read_owned`, `read_owned_into`, `read_blocking`, and by the batch
    /// length for `try_read_batch`); `peek_header` and `wait_for_message` leave it alone.
    ///
    /// Together with [`generation`](Self::generation) this is a resumable cursor: persist
    /// both, and reopen with `ReaderBuilder::new(path).expect_generation(g).start_at(i)`.
    /// Resynchronised from the on-disk numbering (`base_record_index`) at every roll.
    #[inline]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Absolute index of the oldest record still on disk — the earliest retained segment's
    /// `base_record_index`. With [`head_record_index`](Self::head_record_index) it bounds the
    /// indices [`seek`](Self::seek) accepts (`tail..=head`). Scans the directory and reads one
    /// page; not a hot-path accessor.
    ///
    /// Fails with a [`GenerationMismatch`] if the path now holds a channel with a different
    /// generation than this reader was opened on, as [`seek`](Self::seek) does: an index from a
    /// recreated channel would not be comparable with this reader's
    /// [`position`](Self::position).
    pub fn tail_record_index(&self) -> io::Result<u64> {
        const MAX_OPEN_RETRIES: usize = 8;
        for _ in 0..MAX_OPEN_RETRIES {
            let seq = find_earliest_sequence(&self.base_path)?;
            if let Some(file) = open_segment_if_present(&self.base_path, seq)? {
                let header = read_segment_header(&file, seq)?;
                if header.generation != self.generation_cached {
                    return Err(GenerationMismatch {
                        expected: self.generation_cached,
                        found: header.generation,
                    }
                    .into_io());
                }
                return Ok(header.base_record_index);
            }
        }
        Err(Self::retries_exhausted(&self.base_path))
    }

    /// Reposition so the next user record read is absolute index `index`.
    ///
    /// Same rules and cost as opening with [`ReaderMode::At`]: `ErrorKind::NotFound` carrying an
    /// [`IndexPruned`] if `index` has been pruned (a plain `NotFound` if the channel is gone),
    /// `ErrorKind::InvalidInput` if it is
    /// past the head, and a [`GenerationMismatch`] if the path now holds a channel with a
    /// different generation than this reader was opened on — which catches a recreated
    /// channel only if its writers stamp generations (see [`GenerationMismatch`]). On any
    /// error the reader is left exactly where it was.
    ///
    /// Messages already taken with `try_read_owned` stay valid; they hold their own share of
    /// their region.
    pub fn seek(&mut self, index: u64) -> io::Result<()> {
        self.reopen(ReaderMode::At(index))
    }

    /// Reposition at the oldest retained record, as a fresh `LateJoin` reader would start.
    /// Errors as for [`seek`](Self::seek).
    pub fn rewind(&mut self) -> io::Result<()> {
        self.reopen(ReaderMode::LateJoin)
    }

    /// Reposition at the channel head, as a fresh `Live` reader would start: everything
    /// already written is skipped. Errors as for [`seek`](Self::seek).
    pub fn seek_to_head(&mut self) -> io::Result<()> {
        self.reopen(ReaderMode::Live)
    }

    fn reopen(&mut self, mode: ReaderMode) -> io::Result<()> {
        let mut fresh = Self::open_with(&self.base_path, mode, Some(self.generation_cached))?;
        fresh.batch_limit = self.batch_limit;
        fresh.unmap = self.unmap;
        fresh.held.reserve(self.held.capacity());
        fresh.helper = self.helper;
        fresh.start_map_ahead()?;
        *self = fresh;
        Ok(())
    }
    /// Channel name as set by `WriterBuilder::channel_name`, trimmed of trailing zero bytes.
    /// Returns `""` if no name was set. Invalid UTF-8 yields a lossy conversion.
    pub fn channel_name(&self) -> std::borrow::Cow<'_, str> {
        let end = self
            .channel_name_cached
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(self.channel_name_cached.len());
        String::from_utf8_lossy(&self.channel_name_cached[..end])
    }

    /// Absolute index (from channel genesis) of the first user record in the file
    /// the reader currently has open. Updated as the reader follows rolls, including rolls
    /// crossed inside a [`try_read_batch`](Self::try_read_batch). Genesis files report 0.
    /// For the index of the reader's own cursor use [`position`](Self::position).
    #[inline]
    pub fn base_record_index(&self) -> u64 {
        self.base_record_index_cached
    }

    /// This channel's incarnation id (see [`WriterBuilder::generation`]). Constant across
    /// the channel's segments — following a roll into a segment carrying a different value
    /// is refused as a mixed-incarnation directory, so this never changes under a reader.
    ///
    /// A consumer that persists a read position must persist this alongside it: a channel
    /// deleted and recreated at the same path restarts at sequence 0 and record index 0, so
    /// nothing else distinguishes "the log was truncated" from "this is a different log",
    /// and a resumed cursor would silently point into unrelated data. Reports 0 for channels
    /// created without one.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation_cached
    }

    /// Ordinal of the segment file the reader currently has open, matching the `.NNN`
    /// suffix on disk. Monotonically increasing as the reader follows rolls.
    ///
    /// Rolls are otherwise invisible to a reader — `Roll` markers are consumed
    /// transparently by [`try_read`](Self::try_read), [`read_blocking`](Self::read_blocking)
    /// and [`wait_for_message`](Self::wait_for_message) — so this accessor is how a
    /// consumer *locates* a roll: sample it around a single-record read, and a change means
    /// the record just returned is the first user record of a new segment. That makes the
    /// writer's segmentation observable downstream, so a replicator can reproduce the
    /// origin's file boundaries (and therefore its `keep_files` retention) rather than
    /// inventing its own:
    ///
    /// ```ignore
    /// let before = reader.file_sequence();
    /// if let Some(msg) = reader.try_read()? {
    ///     let rolled = reader.file_sequence() != before; // `msg` starts a new segment
    /// }
    /// ```
    ///
    /// [`try_read_batch`](Self::try_read_batch) may span a roll, after which this reports
    /// the last segment the batch touched; the boundary's position *within* that batch is
    /// not recoverable. Use the single-record path where boundaries matter.
    #[inline]
    pub fn file_sequence(&self) -> u64 {
        self.file_sequence
    }

    /// The channel's MTU — max user payload bytes; `0` = unlimited (from its header). Constant
    /// for a channel's life. Together with [`region_size`](Self::region_size) this is the
    /// geometry needed to re-register or replicate a channel without re-deriving it.
    #[inline]
    pub fn mtu(&self) -> u32 {
        self.mtu_cached
    }

    /// Absolute index (from channel genesis) of the **next** user record the channel
    /// will hold — its current head / high-water mark, equal to the writer's
    /// [`Writer::next_record_index`] at the moment of the call. Independent of where this
    /// reader's cursor sits: it consults the newest file on disk, so a `LateJoin` reader
    /// still catching up (or one parked on an older rolled file) reports the true channel
    /// head, not the end of the file it currently reads. Reads one page of the latest
    /// segment's header; not a hot-path accessor.
    pub fn head_record_index(&self) -> io::Result<u64> {
        let latest = find_latest_published_sequence(&self.base_path)?;
        let file_path = make_channel_file_path(&self.base_path, latest)?;
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(&file_path)?;
        let header = read_segment_header(&file, latest)?;
        Ok(header.base_record_index + header.message_count)
    }

    /// The channel's region size in bytes (from its header). Constant for a channel's life.
    #[inline(always)]
    pub fn region_size(&self) -> usize {
        self.region_size_cached
    }

    #[inline]
    fn current_map(&self) -> Option<&MappedRegion> {
        self.maps.last().map(Arc::as_ref)
    }

    #[inline]
    fn current_region(&self) -> Option<&RegionMapping<ReadOnly>> {
        self.current_map().map(|map| &map.mapping)
    }

    fn ensure_scan_region_mapped(
        &mut self,
        scan_file: Option<&File>,
        scan_file_sequence: u64,
        region_idx: u64,
    ) -> io::Result<()> {
        if let Some(last) = self.maps.last() {
            if last.file_sequence == scan_file_sequence && last.region_idx == region_idx {
                return Ok(());
            }
            if last.file_sequence == scan_file_sequence && last.region_idx > region_idx {
                return Err(err_invalid_data("batch scan moved backward across regions"));
            }
        }

        let file = scan_file.unwrap_or(&self.file);
        let map = self.map_region(file, scan_file_sequence, region_idx)?;
        self.maps.push(Arc::new(MappedRegion {
            file_sequence: scan_file_sequence,
            region_idx,
            mapping: map,
        }));
        Ok(())
    }

    fn map_region(
        &self,
        file: &File,
        file_sequence: u64,
        region_idx: u64,
    ) -> io::Result<RegionMapping<ReadOnly>> {
        let region_size = self.region_size();
        let map = match self
            .map_ahead
            .as_ref()
            .and_then(|a| a.take(file_sequence, region_idx))
        {
            Some(map) => map,
            None => {
                RegionMapping::create_read_only(file, region_idx * region_size as u64, region_size)?
            }
        };
        if let Some(ahead) = &self.map_ahead {
            ahead.moved(file_sequence, region_idx);
        }
        Ok(map)
    }

    fn start_map_ahead(&mut self) -> io::Result<()> {
        let Some(helper) = self.helper else {
            return Ok(());
        };
        let region_idx = self.current_map().map_or(0, |m| m.region_idx);
        self.map_ahead = Some(map_ahead::MapAhead::start(
            helper,
            self.base_path.clone(),
            &self.file,
            self.file_sequence,
            region_idx,
            self.region_size(),
        )?);
        Ok(())
    }

    /// Why the helper thread stopped, if it did: an OS error, such as `EINVAL` from `madvise` on
    /// a kernel older than 5.14, or a panic. The reader then carries on as it does without a
    /// helper: it maps, faults and unmaps its own regions.
    pub fn helper_error(&self) -> Option<io::Error> {
        self.map_ahead.as_ref()?.shared.failure.error()
    }

    /// Release regions the reader has left: to the helper, or kept until the roll under
    /// `Unmap::AtFileRoll` while they belong to the segment being read.
    fn retire(&mut self, region: Arc<MappedRegion>) {
        if self.unmap == Unmap::AtFileRoll && region.file_sequence == self.file_sequence {
            self.held.push(region);
        } else if let Some(ahead) = &self.map_ahead {
            ahead.retire([map_ahead::Retired::Region(region)]);
        }
    }

    /// Keep only the last mapping, as the first.
    fn retire_all_but_last(&mut self) {
        let last = self.maps.len() - 1;
        self.maps.swap(0, last);
        while self.maps.len() > 1 {
            let region = self.maps.pop().expect("more than one mapping");
            self.retire(region);
        }
    }

    /// The reader moved into a new segment: release what it held of the old ones.
    fn rolled(&mut self, opened_ahead: bool, old_file: File, old_header: RegionMapping<ReadOnly>) {
        if let Some(ahead) = &self.map_ahead {
            ahead.rolled(self.file_sequence, (!opened_ahead).then_some(&self.file));
            ahead.retire(self.held.drain(..).map(map_ahead::Retired::Region).chain([
                map_ahead::Retired::File(old_file),
                map_ahead::Retired::Header(old_header),
            ]));
        } else {
            self.held.clear();
        }
    }

    fn prune_to_current(&mut self) {
        let Some(last) = self.current_map() else {
            panic!("no current map");
        };
        let region_size = self.region_size();
        let expected_region = (self.read_position / region_size) as u64;
        let expected_file = self.file_sequence;
        if last.file_sequence != expected_file || last.region_idx != expected_region {
            panic!("current map does not match reader position");
        }
        if self.maps.len() > 1 {
            self.retire_all_but_last();
        }
    }

    /// Read currently-available user messages into a batch.
    /// `None` uses the reader's default; if unset, unlimited.
    /// `Some(0)` returns `None` without scanning.
    /// Advances the reader position past scanned records when progress is made.
    /// Returns `Ok(None)` if there are no user messages available.
    pub fn try_read_batch(
        &mut self,
        max_batch: Option<u16>,
    ) -> io::Result<Option<MessageBatch<'_>>> {
        let max_batch = max_batch.or(self.batch_limit).unwrap_or(u16::MAX) as usize;
        if max_batch == 0 {
            return Ok(None);
        }
        self.batch_segs.clear();
        self.batch_pos.clear();

        self.prune_to_current();

        let end = match self.scan_batch(max_batch) {
            Ok(end) => end,
            Err(e) => {
                // Nothing was committed; drop what the scan mapped so the current region is
                // `maps.last()` again, as every read path expects.
                self.maps.truncate(1);
                self.batch_segs.clear();
                self.batch_pos.clear();
                return Err(e);
            }
        };

        let Some(end) = end else {
            self.batch_segs.clear();
            self.batch_pos.clear();
            return Ok(None);
        };

        self.read_position = end.cursor;
        self.position = end.position;
        self.file_sequence = end.file_sequence;
        if let Some(rolled) = end.rolled {
            let old_file = std::mem::replace(&mut self.file, rolled.file);
            let old_header = std::mem::replace(&mut self.header, rolled.header);
            self.instance = rolled.instance;
            self.wake_untrusted = false;
            self.wake_miss = None;
            self.channel_name_cached = rolled.channel_name;
            self.base_record_index_cached = rolled.base_record_index;
            self.rolled(rolled.opened_ahead, old_file, old_header);
        }

        if self.batch_pos.is_empty() {
            self.batch_segs.clear();
            self.batch_pos.clear();
            self.prune_to_current();
            return Ok(None);
        }

        Ok(Some(MessageBatch {
            segs: &self.batch_segs,
            pos: &self.batch_pos,
            maps: &self.maps,
        }))
    }

    /// The scan behind `try_read_batch`: fills `batch_segs` / `batch_pos` and maps regions,
    /// but leaves the cursor alone and returns where it should move to. `Ok(None)` means no
    /// progress at all.
    fn scan_batch(&mut self, max_batch: usize) -> io::Result<Option<BatchEnd>> {
        let region_size = self.region_size();
        let mut scan_file_sequence = self.file_sequence;
        let mut rolled: Option<BatchRoll> = None;
        let mut cursor = self.read_position;
        let mut position = self.position;
        let mut progressed = false;

        'scan: loop {
            // Outer loop: advance across regions/files; each iteration starts a new segment.
            let region_index = (cursor / region_size) as u64;
            let region_start = region_index as usize * region_size;
            let mut cursor_off = cursor - region_start;

            let scan_file = rolled.as_ref().map(|r| &r.file);
            self.ensure_scan_region_mapped(scan_file, scan_file_sequence, region_index)?;
            let map_idx = self.maps.len() - 1;

            if self.batch_segs.len() > u16::MAX as usize {
                return Err(err_other("too many batch segments"));
            }
            let seg_idx = self.batch_segs.len();
            self.batch_segs.push(BatchSeg {
                map_idx,
                start: cursor_off as u32,
                end: cursor_off as u32,
            });

            loop {
                // Inner loop: sequential scan within a single mapping segment.
                if cursor_off + HEADER_SLOT > region_size {
                    return Err(err_invalid_data(
                        "batch scan landed in an invalid header slot",
                    ));
                }

                let hdr = unsafe { self.current_message_header_info(cursor_off)? };

                if !hdr.is_committed {
                    // Stop at the first uncommitted header; next call will resume here.
                    std::hint::spin_loop();
                    self.batch_segs[seg_idx].end = cursor_off as u32;
                    break 'scan;
                }

                if cursor_off + hdr.total_len > region_size {
                    return Err(err_invalid_data(
                        "message payload extends past region boundary",
                    ));
                }

                let roll_pos = region_start + cursor_off;
                let next_pos = align_up(roll_pos + hdr.total_len);
                let next_off = next_pos - region_start;

                match hdr.header_type {
                    HeaderType::User => {
                        self.batch_pos.push(MsgPos {
                            seg: seg_idx as u16,
                            off: cursor_off as u32,
                        });
                        position += 1;
                        if self.batch_pos.len() >= max_batch {
                            // Cap batch size to avoid scanning too far in one call.
                            progressed = true;
                            cursor = next_pos;
                            self.batch_segs[seg_idx].end = next_off as u32;
                            break 'scan;
                        }
                    }
                    HeaderType::Channel | HeaderType::Skip => {}
                    HeaderType::Roll => {
                        // Switch to the next file and continue scanning from its start, with
                        // the same continuity checks a single-record roll makes.
                        let (prev, prev_instance) = rolled
                            .as_ref()
                            .map_or((&self.header, self.instance), |r| (&r.header, r.instance));
                        let next = match self
                            .roll_edge(map_idx, cursor_off, hdr.payload_len)
                            .and_then(|edge| {
                                self.open_successor(prev, scan_file_sequence, prev_instance, &edge)
                            }) {
                            Ok(next) => next,
                            // Hand over what was collected before the Roll, as single reads
                            // would; the cursor stops on the Roll, so the next call meets the
                            // same refusal and reports it.
                            Err(_) if !self.batch_pos.is_empty() => {
                                cursor = roll_pos;
                                self.batch_segs[seg_idx].end = cursor_off as u32;
                                break 'scan;
                            }
                            Err(e) => return Err(e),
                        };
                        progressed = true;
                        self.batch_segs[seg_idx].end = next_off as u32;
                        scan_file_sequence = next.sequence;
                        position = next.base_record_index;
                        // Its region 0 is already mapped; hand it to the next segment's scan.
                        self.maps.push(next.region0);
                        rolled = Some(BatchRoll {
                            opened_ahead: next.opened_ahead,
                            instance: next.instance,
                            file: next.file,
                            header: next.header,
                            channel_name: next.channel_name,
                            base_record_index: next.base_record_index,
                        });
                        cursor = 0;
                        continue 'scan;
                    }
                }

                progressed = true;
                cursor = next_pos;
                if next_off == region_size {
                    self.batch_segs[seg_idx].end = next_off as u32;
                    continue 'scan;
                }
                cursor_off = next_off;
            }
        }

        Ok(progressed.then_some(BatchEnd {
            cursor,
            position,
            file_sequence: scan_file_sequence,
            rolled,
        }))
    }

    /// Read next message if available. Roll to next file on `Roll`.
    /// Steady path: rely on per-record `committed` plus Skip/Roll markers; no `write_position`.
    pub fn try_read(&mut self) -> io::Result<Option<MessageRef<'_>>> {
        let Some(loc) = self.advance_to_user_record()? else {
            return Ok(None);
        };
        Ok(Some(MessageRef {
            mapping: &self.maps[loc.map_idx].mapping,
            header_offset: loc.header_offset,
            payload_len: loc.payload_len,
        }))
    }

    /// The next user record's header, **without consuming the record**.
    ///
    /// Returned by value and borrowing nothing, so a caller holding several readers can peek all
    /// of them, decide which to take, and take only that one. That is what merging channels in
    /// timestamp order needs: without it the only way to learn a record's time is to read it, and
    /// a record read from the wrong channel has to be copied somewhere until its turn comes.
    ///
    /// `Ok(None)` means caught up — the writer has not committed the next record yet — not end of
    /// stream. Peeking twice returns the same header; the record is still there.
    ///
    /// Service records (Skip / Channel / Roll) encountered on the way are consumed transparently,
    /// exactly as [`try_read`](Reader::try_read) consumes them, and this may open the next file.
    /// They carry no data, so stepping over them changes nothing a caller can observe.
    ///
    /// ```no_run
    /// # use xchannel::Reader;
    /// # fn pick(readers: &mut [Reader]) -> std::io::Result<Option<usize>> {
    /// let mut earliest: Option<(u64, usize)> = None;
    /// for (i, reader) in readers.iter_mut().enumerate() {
    ///     if let Some(hdr) = reader.peek_header()?
    ///         && earliest.is_none_or(|(at, _)| hdr.user_meta_u64 < at)
    ///     {
    ///         earliest = Some((hdr.user_meta_u64, i));
    ///     }
    /// }
    /// // only the winner is read, and its payload can be borrowed in place
    /// Ok(earliest.map(|(_, i)| i))
    /// # }
    /// ```
    pub fn peek_header(&mut self) -> io::Result<Option<PeekedHeader>> {
        Ok(self.scan_to_user_record(false)?.map(|found| PeekedHeader {
            message_type: found.message_type,
            user_meta_u64: found.user_meta_u64,
            length: found.loc.payload_len as u32,
        }))
    }

    /// Like [`Reader::try_read`], but the returned message owns a share of the
    /// region it points into, so it carries no lifetime and may outlive the
    /// Reader.
    ///
    /// Prefer `try_read` on hot paths: this clones an `Arc` per message, and a
    /// retained message keeps its whole region mapped (see [`OwnedMessage`]).
    pub fn try_read_owned(&mut self) -> io::Result<Option<OwnedMessage>> {
        let Some(loc) = self.advance_to_user_record()? else {
            return Ok(None);
        };
        Ok(Some(OwnedMessage {
            region: Arc::clone(&self.maps[loc.map_idx]),
            header_offset: loc.header_offset,
            payload_len: loc.payload_len,
        }))
    }

    /// Drain currently-available user messages into `out`, appending them in
    /// stream order, and return how many were appended.
    ///
    /// `max` bounds the pass; `None` drains until the reader is caught up.
    /// `Some(0)` returns `Ok(0)` without touching the cursor. Unlike
    /// [`Reader::try_read_batch`], `None` here is plain unbounded — it does not
    /// consult the builder's `batch_limit`, which governs the batch scan.
    ///
    /// A short count — including `0` — means *caught up*, not end of stream: a
    /// later call yields more once the writer appends. Service records (Skip /
    /// Channel / Roll) are consumed transparently and do not count against
    /// `max`.
    ///
    /// `out` is appended to, never cleared, and its capacity is reused across
    /// polls — this is the allocation-free way to work through owned messages.
    ///
    /// On error, messages read before the failure stay in `out` and the cursor
    /// has advanced past them; the caller can still process them, and should
    /// compare `out.len()` before and after to know how many arrived.
    ///
    /// # Choosing a bound
    ///
    /// `None` terminates only when the reader reaches an uncommitted header, so
    /// against a writer that keeps committing it may not terminate at all —
    /// `out` grows without bound, and every retained message pins its whole
    /// region mapped (see [`OwnedMessage`]). Use it for a drain-and-stop pass,
    /// and pass `Some(n)` on a steady polling loop so each poll is bounded and
    /// the buffer settles at a known capacity.
    ///
    /// # Why this instead of an `Iterator` over the Reader
    ///
    /// A lazy iterator driving the read cursor cannot be combined safely with
    /// the adapters that buffer or discard an item — `peekable`, `take_while`,
    /// `zip`, `chunks` — because every pull *consumes* from the channel and
    /// there is no way to put a message back. `Peekable::peek` advances the
    /// cursor and parks the message inside the adapter, so dropping the adapter
    /// loses that message permanently. Draining into `out` first makes all of
    /// them sound: what an adapter discards is a message you already hold.
    ///
    /// ```no_run
    /// # use xchannel::{OwnedMessage, Reader, ReaderMode};
    /// # fn f(reader: &mut Reader) -> std::io::Result<()> {
    /// let mut buf: Vec<OwnedMessage> = Vec::with_capacity(1024);
    /// loop {
    ///     if reader.read_owned_into(&mut buf, Some(1024))? == 0 {
    ///         reader.wait_for_message(None)?;
    ///         continue;
    ///     }
    ///     let mut it = buf.drain(..).peekable();
    ///     while let Some(msg) = it.next() {
    ///         // Safe here: anything `peek` holds is a message we own.
    ///         let more_of_the_same =
    ///             it.peek().is_some_and(|n| n.header().message_type == msg.header().message_type);
    ///         let _ = (msg.payload(), more_of_the_same);
    ///     }
    /// }
    /// # }
    /// ```
    pub fn read_owned_into(
        &mut self,
        out: &mut Vec<OwnedMessage>,
        max: Option<usize>,
    ) -> io::Result<usize> {
        let max = max.unwrap_or(usize::MAX);
        if max == 0 {
            return Ok(0);
        }
        // No reserve: `max` may be huge (unbounded passes it as `usize::MAX`)
        // and a reused buffer settles at the right capacity after one pass.
        let mut n = 0;
        while n < max {
            match self.try_read_owned()? {
                Some(msg) => {
                    out.push(msg);
                    n += 1;
                }
                None => break,
            }
        }
        Ok(n)
    }

    /// [`Reader::read_owned_into`] with the buffer allocated for you. `None`
    /// drains until caught up; see there on choosing a bound.
    ///
    /// Convenience for scripts and tests; on a polling path prefer
    /// `read_owned_into` with a reused buffer, which allocates once rather than
    /// once per call.
    pub fn owned_batch(&mut self, max: Option<usize>) -> io::Result<Vec<OwnedMessage>> {
        let mut out = Vec::new();
        self.read_owned_into(&mut out, max)?;
        Ok(out)
    }

    /// Advance the cursor to the next committed user record, transparently
    /// consuming Skip / Channel / Roll service records.
    ///
    /// Returns where the record lives rather than a view of it, so the borrow
    /// does not freeze `self` and both the borrowed and owned readers can build
    /// their own view afterwards.
    fn advance_to_user_record(&mut self) -> io::Result<Option<RecordLoc>> {
        Ok(self.scan_to_user_record(true)?.map(|found| found.loc))
    }

    /// Scan forward to the next user record.
    ///
    /// Service records (Skip / Channel / Roll) are consumed either way — they carry no data, so
    /// stepping over them is not observable. `consume` decides only what happens at the user
    /// record: `true` advances the cursor past it, `false` leaves the cursor on it so the next read
    /// returns that same record.
    fn scan_to_user_record(&mut self, consume: bool) -> io::Result<Option<FoundRecord>> {
        self.prune_to_current();
        loop {
            let region_size = self.region_size();
            let off = self.read_position % region_size;
            let leftover = region_size - off;

            // Invariant: for any header slot produced by Writer,
            // there is always at least HEADER_SLOT bytes left in the region.
            debug_assert!(
                leftover >= HEADER_SLOT,
                "xchannel: read_position in impossible boundary hole; \
             writer invariants violated or file corrupted"
            );

            let hdr = unsafe { self.current_message_header_info(off)? };

            if !hdr.is_committed {
                // not ready yet
                std::hint::spin_loop();
                return Ok(None);
            }

            if hdr.total_len > leftover {
                return Err(err_invalid_data(
                    "message payload extends past remaining region bytes",
                ));
            }

            let next_pos = align_up(self.read_position + hdr.total_len);

            match hdr.header_type {
                HeaderType::User => {
                    let region_size = self.region_size();
                    // Captured before `switch_region`, which only ever pushes,
                    // so the index stays valid for the record just located.
                    let msg_map_idx = self.maps.len() - 1;
                    if consume {
                        // Map the next region before moving the cursor: if the map fails, the
                        // cursor still names a mapped region and the record can be read again.
                        if next_pos.is_multiple_of(region_size) {
                            self.switch_region((next_pos / region_size) as u64)?;
                        }
                        self.position += 1;
                        self.read_position = next_pos;
                    }
                    return Ok(Some(FoundRecord {
                        loc: RecordLoc {
                            map_idx: msg_map_idx,
                            header_offset: off,
                            payload_len: hdr.payload_len,
                        },
                        message_type: hdr.message_type,
                        user_meta_u64: hdr.user_meta_u64,
                    }));
                }
                HeaderType::Skip | HeaderType::Channel => {
                    let region_size = self.region_size();
                    if next_pos.is_multiple_of(region_size) {
                        self.switch_region((next_pos / region_size) as u64)?;
                        self.read_position = next_pos;
                        self.prune_to_current();
                    } else {
                        self.read_position = next_pos;
                    }
                    continue;
                }
                HeaderType::Roll => {
                    // `open_next_file` resets the cursor to the new segment's start; moving it
                    // here first would only strand it inside the old file if the roll failed.
                    let edge = self.roll_edge(self.maps.len() - 1, off, hdr.payload_len)?;
                    self.open_next_file(&edge)?;
                    continue;
                }
            }
        }
    }

    /// Block until a user message is at the read cursor, returning `Ok(true)`;
    /// or until the optional `timeout` elapses, returning `Ok(false)`.
    ///
    /// On `Ok(true)` return, the next call to `try_read` is guaranteed to
    /// observe a committed user record at the current read position — the
    /// caller can `try_read()?.expect("...")` without re-checking. Skip /
    /// Roll / Channel service records encountered while polling are
    /// transparently consumed; only User records gate the return.
    ///
    /// **On a channel whose writer wakes readers** ([`WriterBuilder::wake_readers`],
    /// Linux) it sleeps on the segment's wake word and runs again within microseconds of
    /// the next commit. Each sleep is capped at 10 ms, so a writer that stops waking (an
    /// older one reopening the channel) costs slow polling, never a hang; once capped sleeps
    /// have run out twice with a record waiting and the wake word unmoved in between, it backs
    /// off instead for the rest of that segment.
    ///
    /// **Otherwise** it sleeps with backoff: 50 µs, doubling to a 500 µs cap, re-checking
    /// after each sleep. So the first record after a quiet spell waits about 250 µs on average
    /// and at most about 550 µs (the cap plus Linux's 50 µs timer slack), and an idle reader
    /// wakes about 2,000 times a second. There is no spinning; a reader that needs less
    /// latency than that should spin on `try_read`, or read a channel whose writer wakes
    /// readers.
    ///
    /// This is a synchronous helper. **Do not call from an async runtime
    /// task** — it blocks the calling thread. Async callers should compose
    /// `try_read` with their runtime's own sleep primitive, or write an
    /// equivalent polling helper around the runtime's sleep.
    ///
    /// `timeout = None` waits indefinitely.
    pub fn wait_for_message(&mut self, timeout: Option<Duration>) -> io::Result<bool> {
        let deadline = timeout.map(|d| Instant::now() + d);
        let mut backoff = BACKOFF_START;

        loop {
            // Load the word *before* looking: a commit after the look then changes it, and
            // the futex returns at once instead of sleeping through the record.
            let sequence = self.file_sequence;
            let seen = self.wake_word_if_waking();
            if self.poll_for_user_message()? {
                return Ok(true);
            }
            let Some(left) = time_left(deadline) else {
                return Ok(false);
            };
            match seen {
                // The look followed a roll: the word belongs to the old segment. Look again.
                Some(_) if self.file_sequence != sequence => {}
                Some(value) => {
                    let nap = left.min(WAKE_SLEEP_CAP);
                    match wake::wait(self.wake_word(), value, nap)? {
                        wake::Waited::Changed => {
                            #[cfg(test)]
                            {
                                self.woken += 1;
                            }
                        }
                        wake::Waited::TimedOut => {
                            if nap == WAKE_SLEEP_CAP && self.poll_for_user_message()? {
                                // A full capped sleep ran out, yet a record is there.
                                self.distrust_flag_if_unwoken(sequence, value);
                                return Ok(true);
                            }
                        }
                        wake::Waited::Unsupported => self.wake_untrusted = true,
                    }
                }
                None => {
                    thread::sleep(backoff.min(left));
                    backoff = (backoff * 2).min(BACKOFF_CAP);
                }
            }
        }
    }

    /// The current segment's wake word value, if its writer wakes readers and that is still
    /// believed. `Acquire`, so a value observed here carries the commit bumped before it.
    #[inline]
    fn wake_word_if_waking(&self) -> Option<u32> {
        if !wake::SUPPORTED || self.wake_untrusted {
            return None;
        }
        let ch = get_channel_header(self.header.as_ptr());
        let flags = unsafe { &*(std::ptr::addr_of!(ch.wake_flags) as *const AtomicU8) };
        (flags.load(Ordering::Acquire) & WAKE_FLAG_WAKES != 0)
            .then(|| ch.wake_word.load(Ordering::Acquire))
    }

    #[inline]
    fn wake_word(&self) -> &AtomicU32 {
        &get_channel_header(self.header.as_ptr()).wake_word
    }

    /// After a full capped sleep that ran out with a record waiting: count a miss if the
    /// reader is still on that segment and the word still holds `seen`, the value slept on,
    /// and stop trusting the segment's wake flag at the second miss on the same value.
    ///
    /// A writer that does not wake never changes the word, so it misses on every record that
    /// arrives during a sleep and is caught at the second. A waking writer publishes a record
    /// a moment before it bumps the word, and if it is preempted in between just as a sleep
    /// runs out, it misses once; but its bump then moves the word, so the next miss, if any,
    /// is on a different value and the flag stays trusted.
    fn distrust_flag_if_unwoken(&mut self, sequence: u64, seen: u32) {
        if self.file_sequence != sequence || self.wake_word().load(Ordering::Acquire) != seen {
            return;
        }
        if self.wake_miss == Some(seen) {
            self.wake_untrusted = true;
        } else {
            self.wake_miss = Some(seen);
        }
    }

    /// Non-blocking peek: advance past any Skip/Roll/Channel service
    /// records and report whether the slot at `read_position` is now a
    /// committed user record. Returns `Ok(true)` if so (without consuming
    /// the record), `Ok(false)` if not (the uncommitted slot or the
    /// out-of-data tail).
    fn poll_for_user_message(&mut self) -> io::Result<bool> {
        self.prune_to_current();
        loop {
            let region_size = self.region_size();
            let off = self.read_position % region_size;
            let leftover = region_size - off;
            debug_assert!(leftover >= HEADER_SLOT);

            let hdr = unsafe { self.current_message_header_info(off)? };

            if !hdr.is_committed {
                return Ok(false);
            }

            if hdr.total_len > leftover {
                return Err(err_invalid_data(
                    "message payload extends past remaining region bytes",
                ));
            }

            let next_pos = align_up(self.read_position + hdr.total_len);

            match hdr.header_type {
                HeaderType::User => {
                    // Do not advance — `try_read` will consume the record.
                    return Ok(true);
                }
                HeaderType::Skip | HeaderType::Channel => {
                    if next_pos.is_multiple_of(region_size) {
                        self.switch_region((next_pos / region_size) as u64)?;
                        self.read_position = next_pos;
                        self.prune_to_current();
                    } else {
                        self.read_position = next_pos;
                    }
                    continue;
                }
                HeaderType::Roll => {
                    // See the Roll arm in `scan_to_user_record`: the cursor moves only once
                    // the next segment is actually open.
                    let edge = self.roll_edge(self.maps.len() - 1, off, hdr.payload_len)?;
                    self.open_next_file(&edge)?;
                    continue;
                }
            }
        }
    }

    /// Block until a user message is available and return it; or return
    /// `Ok(None)` if the optional `timeout` elapses first.
    ///
    /// Convenience wrapper around [`Reader::wait_for_message`] followed by
    /// [`Reader::try_read`]. See `wait_for_message` for the backoff,
    /// blocking, and runtime caveats.
    ///
    /// Tokio analogue (replace `std::thread::sleep` with the runtime's
    /// sleep). The cursor API decomposes cleanly across the await point;
    /// no raw-pointer or `unsafe` lifetime workaround is needed:
    ///
    /// ```ignore
    /// use std::time::{Duration, Instant};
    /// use std::io;
    /// use xchannel::{Reader, MessageRef};
    ///
    /// async fn read_async(
    ///     reader: &mut Reader,
    ///     timeout: Option<Duration>,
    /// ) -> io::Result<Option<MessageRef<'_>>> {
    ///     let deadline = timeout.map(|d| Instant::now() + d);
    ///     let mut backoff_us: u64 = 1;
    ///     loop {
    ///         // try_read does not block; if it returns Some, we're done.
    ///         if let Some(msg) = reader.try_read()? {
    ///             return Ok(Some(msg));
    ///         }
    ///         if let Some(d) = deadline {
    ///             if Instant::now() >= d { return Ok(None); }
    ///         }
    ///         tokio::time::sleep(Duration::from_micros(backoff_us)).await;
    ///         backoff_us = (backoff_us * 2).min(10_000);
    ///     }
    /// }
    /// ```
    pub fn read_blocking(
        &mut self,
        timeout: Option<Duration>,
    ) -> io::Result<Option<MessageRef<'_>>> {
        if self.wait_for_message(timeout)? {
            // `wait_for_message` returned true: a committed user record is at
            // the read cursor. `try_read` consumes it.
            Ok(Some(self.try_read()?.expect(
                "wait_for_message reported a user message ready but try_read returned None",
            )))
        } else {
            Ok(None)
        }
    }

    // The header lives in the last mapped region. We decode the fields we need into a small value
    // type so the borrow does not escape and freeze `self`: both `try_read()` and
    // `try_read_batch()` need to inspect the header first and then mutate reader state afterwards.
    // The batch path relies on the invariant that the region currently being scanned is always
    // `self.maps.last()`.
    //
    // This remains unsafe because it casts raw mmap bytes to `&MessageHeader`. The caller must
    // ensure `off` points to a full, aligned header slot containing a valid header representation.
    #[inline]
    unsafe fn current_message_header_info(&self, off: usize) -> io::Result<ScannedHeader> {
        let region = self
            .current_region()
            .ok_or_else(|| err_other("reader has no current region mapped"))?;
        let mh = unsafe { &*(region.as_ptr().add(off) as *const MessageHeader) };

        if !mh.is_committed()? {
            return Ok(ScannedHeader {
                is_committed: false,
                header_type: HeaderType::User, // ignored by callers in this case
                payload_len: 0,
                total_len: 0,
                message_type: 0,
                user_meta_u64: 0,
            });
        }

        let payload_len = mh.length as usize;
        Ok(ScannedHeader {
            is_committed: true,
            header_type: mh.parsed_header_type()?,
            payload_len,
            total_len: HEADER_SLOT + payload_len,
            message_type: mh.message_type,
            user_meta_u64: mh.user_meta_u64,
        })
    }

    fn switch_region(&mut self, idx: u64) -> io::Result<()> {
        if let Some(last) = self.current_map()
            && last.file_sequence == self.file_sequence
            && last.region_idx == idx
        {
            return Ok(());
        }
        let new_map = self.map_region(&self.file, self.file_sequence, idx)?;
        self.maps.push(Arc::new(MappedRegion {
            file_sequence: self.file_sequence,
            region_idx: idx,
            mapping: new_map,
        }));
        Ok(())
    }

    /// Roll into the next segment.
    ///
    /// Nothing on `self` moves until the new segment is open and validated. A failure — the
    /// segment not visible yet, retention having removed it, or one of the checks in
    /// `open_successor` refusing it — leaves the reader exactly where it was, so the error can
    /// be returned again, or the same call retried once the segment appears.
    fn open_next_file(&mut self, edge: &RollEdge) -> io::Result<()> {
        let next = self.open_successor(&self.header, self.file_sequence, self.instance, edge)?;

        // Past the last fallible step: commit the new segment in one go.
        self.file_sequence = next.sequence;
        // Refresh cached channel_name from the new file (the bytes are authoritative
        // even though in practice the name carries across rolls).
        self.channel_name_cached = next.channel_name;
        // Each rolled file has its own base; refresh so `base_record_index()` tracks it.
        self.base_record_index_cached = next.base_record_index;
        // The on-disk numbering is authoritative; it equals the count carried so far unless a
        // segment was written by a pre-fix writer that lost a record's count in crash recovery.
        self.position = next.base_record_index;
        let old_file = std::mem::replace(&mut self.file, next.file);
        let old_header = std::mem::replace(&mut self.header, next.header);
        self.instance = next.instance;
        self.wake_untrusted = false;
        self.wake_miss = None;
        self.read_position = 0;
        self.maps.push(next.region0);
        self.retire_all_but_last();
        self.rolled(next.opened_ahead, old_file, old_header);
        Ok(())
    }

    /// The body of the committed `Roll` at `off` in the region mapped as `maps[map_idx]`.
    fn roll_edge(&self, map_idx: usize, off: usize, payload_len: usize) -> io::Result<RollEdge> {
        let map = &self.maps[map_idx].mapping;
        if off + HEADER_SLOT + payload_len > map.region_size() {
            return Err(err_invalid_data("Roll body extends past its region"));
        }
        let body =
            unsafe { slice::from_raw_parts(map.as_ptr().add(off + HEADER_SLOT), payload_len) };
        RollEdge::decode(body)
    }

    /// Open the published segment the committed `edge` names after `prev_sequence` (mapped as
    /// `prev_header`), checked by identity in memory, with no `stat`. Touches nothing on `self`.
    fn open_successor(
        &self,
        prev_header: &RegionMapping<ReadOnly>,
        prev_sequence: u64,
        prev_instance: InstanceId,
        edge: &RollEdge,
    ) -> io::Result<Successor> {
        let expected_base = {
            let ch = get_channel_header(prev_header.as_ptr());
            ch.base_record_index
                .checked_add(ch.message_count.load(Ordering::Acquire))
        };
        let next_sequence = prev_sequence
            .checked_add(1)
            .filter(|&n| n == edge.next_sequence)
            .ok_or_else(|| {
                err_invalid_data(format!(
                    "segment {prev_sequence} rolls to segment {}, not the next one",
                    edge.next_sequence
                ))
            })?;
        let region_size = self.region_size();
        let ahead = self
            .map_ahead
            .as_ref()
            .and_then(|a| a.take_segment(next_sequence));
        let (file, region0, header, opened_ahead) = match ahead {
            Some(next) if edge.names(prev_sequence, prev_instance, &next.identity) => (
                next.file,
                next.region0,
                Some((next.header, next.identity)),
                true,
            ),
            stale => {
                if let (Some(stale), Some(ahead)) = (stale, &self.map_ahead) {
                    ahead.retire([map_ahead::Retired::Segment(stale)]);
                }
                let file_path = make_channel_file_path(&self.base_path, next_sequence)?;
                let file = OpenOptions::new()
                    .read(true)
                    .write(false)
                    .open(&file_path)?;
                let region0 = Arc::new(MappedRegion {
                    file_sequence: next_sequence,
                    region_idx: 0,
                    mapping: RegionMapping::create_read_only(&file, 0, region_size)?,
                });
                (file, region0, None, false)
            }
        };
        let page0 = region0.mapping.as_ptr();
        let ch = get_channel_header(page0);
        let identity = match &header {
            Some((_, identity)) => *identity, // validated by the helper; immutable
            None => {
                let mh = unsafe { &*(page0 as *const MessageHeader) };
                if mh.parsed_header_type()? != HeaderType::Channel {
                    return Err(err_other("next file missing Channel header"));
                }
                validate_channel_header(ch, region_size, next_sequence)?;
                validate_v4_prefix(page0)?
            }
        };
        if !edge.names(prev_sequence, prev_instance, &identity) {
            return Err(err_invalid_data(format!(
                "segment {next_sequence} is instance {:?} of parent {:?}, but the Roll that \
                 leads to it names instance {:?} of parent {prev_instance:?}",
                identity.instance, identity.predecessor, edge.next_instance
            )));
        }
        if !is_published(page0)? {
            return Err(err_invalid_data(format!(
                "segment {next_sequence} is not published, but a committed Roll leads to it"
            )));
        }
        if Some(ch.base_record_index) != expected_base
            || ch.base_record_index != edge.next_base_record_index
        {
            return Err(err_invalid_data(format!(
                "base_record_index discontinuity: segment {} starts at {} but the previous \
                 segment ends at {} and its Roll says {} (segments from a different series?)",
                next_sequence,
                ch.base_record_index,
                expected_base.map_or("past u64::MAX".into(), |b| b.to_string()),
                edge.next_base_record_index
            )));
        }
        // Unlike the base, the generation is constant across a channel's segments, so a
        // mismatch means this segment belongs to a different incarnation of the path —
        // files from two channels mixed in one directory. Refuse rather than splice them,
        // the same reasoning as the `channel_sequence` check above.
        if ch.generation != self.generation_cached {
            return Err(err_invalid_data(format!(
                "generation mismatch: segment {} has generation {} but the channel is {} \
                 (segments from a different incarnation of this path?)",
                next_sequence, ch.generation, self.generation_cached
            )));
        }
        let channel_name = ch.channel_name;
        let base_record_index = ch.base_record_index;
        let header = match header {
            Some((header, _)) => header,
            None => RegionMapping::create_read_only(&file, 0, region::page_size())?,
        };
        Ok(Successor {
            sequence: next_sequence,
            instance: identity.instance,
            opened_ahead,
            file,
            region0,
            header,
            channel_name,
            base_record_index,
        })
    }
}

/// Where a batch scan ended, for `try_read_batch` to commit.
struct BatchEnd {
    cursor: usize,
    position: u64,
    file_sequence: u64,
    /// The last segment the scan rolled into, if it crossed a roll.
    rolled: Option<BatchRoll>,
}

struct BatchRoll {
    opened_ahead: bool,
    instance: InstanceId,
    file: File,
    header: RegionMapping<ReadOnly>,
    channel_name: [u8; CHANNEL_NAME_MAX],
    base_record_index: u64,
}

/// A validated next segment, opened but not yet committed to the reader.
struct Successor {
    sequence: u64,
    instance: InstanceId,
    /// The helper opened it ahead of the roll.
    opened_ahead: bool,
    file: File,
    region0: Arc<MappedRegion>,
    /// Page 0, kept for the wake flag and word.
    header: RegionMapping<ReadOnly>,
    channel_name: [u8; CHANNEL_NAME_MAX],
    base_record_index: u64,
}

/// Where `wait_for_message`'s backoff starts and where it stops growing. Linux's default timer
/// slack (50 µs) makes any shorter sleep take about that long anyway; the cap bounds the delay
/// before an idle reader sees the next record, at about 2,000 wake-ups a second per idle reader.
const BACKOFF_START: Duration = Duration::from_micros(50);
const BACKOFF_CAP: Duration = Duration::from_micros(500);
/// Longest single sleep on a wake word. A woken reader runs at once, so this adds no latency;
/// it only bounds what a writer that stops waking (a stale flag) costs per wait: slow polling,
/// never a hang.
const WAKE_SLEEP_CAP: Duration = Duration::from_millis(10);

/// Time until `deadline`: `None` once it has passed, effectively forever without one.
fn time_left(deadline: Option<Instant>) -> Option<Duration> {
    match deadline {
        None => Some(Duration::MAX),
        Some(d) => {
            let left = d.saturating_duration_since(Instant::now());
            (!left.is_zero()).then_some(left)
        }
    }
}

/// Block until any of `readers` has a user message at its cursor, and return its index; or
/// return `Ok(None)` once the optional `timeout` has passed. `timeout = None` waits
/// indefinitely.
///
/// Like [`Reader::wait_for_message`] over several channels at once: on `Ok(Some(i))`, the next
/// `readers[i].try_read()` returns a record. Service records are consumed on the way, as there.
///
/// Readers whose writers wake them ([`WriterBuilder::wake_readers`]) are slept on together,
/// with one `futex_waitv` over all their wake words (Linux 5.16+, and kernels that backport
/// it, such as RHEL 9). The rest are polled with the same backoff `wait_for_message` uses, and
/// when the group is mixed the sleeps are kept that short. On a kernel without `futex_waitv`
/// (or where a seccomp profile forbids it), or with more than 128 waking readers, everything
/// falls back to the backoff. Earlier readers in
/// the slice win ties.
///
/// `readers` must not be empty.
pub fn wait_any(
    readers: &mut [&mut Reader],
    timeout: Option<Duration>,
) -> io::Result<Option<usize>> {
    if readers.is_empty() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "wait_any needs at least one reader",
        ));
    }
    let deadline = timeout.map(|d| Instant::now() + d);
    let mut backoff = BACKOFF_START;
    let mut seen: Vec<(u64, Option<u32>)> = Vec::with_capacity(readers.len());
    loop {
        seen.clear();
        seen.extend(
            readers
                .iter()
                .map(|r| (r.file_sequence, r.wake_word_if_waking())),
        );
        for (i, reader) in readers.iter_mut().enumerate() {
            if reader.poll_for_user_message()? {
                return Ok(Some(i));
            }
        }
        let Some(left) = time_left(deadline) else {
            return Ok(None);
        };
        // A reader that followed a roll while looking has a stale word; leave it out this
        // round, which makes the group count as mixed and keeps the sleep short.
        let words: Vec<(&AtomicU32, u32)> = readers
            .iter()
            .zip(&seen)
            .filter(|(r, (sequence, _))| r.file_sequence == *sequence)
            .filter_map(|(r, (_, value))| value.map(|v| (r.wake_word(), v)))
            .collect();
        let all_wake = words.len() == readers.len();
        // Decide first whether the futex call is possible at all; only then may a sleep be as
        // long as the cap. Otherwise it is the backoff step, as for any non-waking reader.
        let can_futex =
            !words.is_empty() && words.len() <= wake::WAITV_MAX && wake::waitv_available();
        let nap = left.min(if all_wake && can_futex {
            WAKE_SLEEP_CAP
        } else {
            backoff
        });
        let waited = if can_futex {
            wake::wait_any(&words, nap)?
        } else {
            wake::Waited::Unsupported
        };
        drop(words);
        match waited {
            wake::Waited::Changed => {}
            wake::Waited::TimedOut if all_wake && nap == WAKE_SLEEP_CAP => {
                // A full capped sleep ran out: a reader that now has a record may have a
                // writer that does not wake, whatever its flag says.
                for (i, reader) in readers.iter_mut().enumerate() {
                    if reader.poll_for_user_message()? {
                        if let (sequence, Some(value)) = seen[i] {
                            reader.distrust_flag_if_unwoken(sequence, value);
                        }
                        return Ok(Some(i));
                    }
                }
            }
            wake::Waited::TimedOut => backoff = (backoff * 2).min(BACKOFF_CAP),
            wake::Waited::Unsupported => {
                // Also reached when the kernel turned out to lack futex_waitv just now, before
                // anything slept: sleep the backoff step, never a flat cap.
                thread::sleep(left.min(backoff));
                backoff = (backoff * 2).min(BACKOFF_CAP);
            }
        }
    }
}

#[cfg(test)]
fn now_ns() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    now.as_nanos() as u64
}

// ========== Utility for building file names ==========
fn make_channel_file_path(base_path: &Path, sequence: u64) -> io::Result<PathBuf> {
    if base_path.is_dir() {
        return Err(io::Error::new(
            ErrorKind::IsADirectory,
            format!("Channel path {:?} cannot be a directory.", base_path),
        ));
    }
    Ok(if sequence == 0 {
        base_path.to_path_buf()
    } else {
        // Keep original file name + ".<seq>" (e.g. "foo.log.1")
        let mut pb = base_path.to_path_buf();
        let file_name = pb
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| err_other(format!("Cannot get file name from path {:?}", base_path)))?;
        let new_name = format!("{}.{}", file_name, sequence);
        pb.set_file_name(new_name);
        pb
    })
}

/// Suffix of a segment's private name before installation; `find_all_sequences` skips it.
const PARTIAL_SUFFIX: &str = "partial";

/// `<base>[.<N>].<instance>.partial`: one attempt's private name for segment `sequence`.
fn make_attempt_path(base_path: &Path, sequence: u64, attempt: InstanceId) -> io::Result<PathBuf> {
    let final_path = make_channel_file_path(base_path, sequence)?;
    let file_name = final_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| err_other(format!("Cannot get file name from path {:?}", final_path)))?;
    let mut pb = final_path.clone();
    pb.set_file_name(format!("{file_name}.{attempt:?}.{PARTIAL_SUFFIX}"));
    Ok(pb)
}

/// Size a freshly-created segment should be born at. Rounded up to
/// the next `region_size` boundary so readers' whole-region mmaps
/// never extend past EOF. `file_roll_size = 0` (unbounded) falls
/// back to one region; `ensure_len` then grows on demand and the
/// intra-file race is exposed in that mode only.
///
/// Returns `InvalidInput` if `file_roll_size` cannot be rounded up
/// without overflowing `u64` (i.e. within `region_size` of
/// `u64::MAX`).
fn preallocation_len(region_size: usize, file_roll_size: u64) -> io::Result<u64> {
    if file_roll_size == 0 {
        return Ok(region_size as u64);
    }
    let r = region_size as u64;
    let rem = file_roll_size % r;
    let len = if rem == 0 {
        file_roll_size
    } else {
        file_roll_size.checked_add(r - rem).ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "file_roll_size {file_roll_size} cannot be rounded up to a \
                     region_size {region_size} multiple without overflowing u64",
                ),
            )
        })?
    };
    // `set_len`/`ftruncate` use an i64 `off_t`; a length past i64::MAX is
    // unrepresentable and otherwise fails deep in the OS with an opaque error.
    if len > i64::MAX as u64 {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "file_roll_size {file_roll_size} (region-aligned to {len}) exceeds \
                 the max file offset i64::MAX; use 0 to disable rolling",
            ),
        ));
    }
    Ok(len)
}

/// The newest segment, after unlinking (never truncating) newer ones left unpublished by an
/// unfinished roll. An unpublished initial file is kept for `open_file` to publish.
fn discard_unpublished_tail(base_path: &Path) -> io::Result<u64> {
    loop {
        let seq = find_latest_sequence(base_path)?;
        if seq == 0 {
            return Ok(0);
        }
        let Some(file) = open_segment_if_present(base_path, seq)? else {
            continue;
        };
        if file.metadata()?.len() < FIRST_RECORD as u64 {
            return Ok(seq); // for `open_file` to report
        }
        let page0 = RegionMapping::create_read_only(&file, 0, region::page_size())?;
        let ch = get_channel_header(page0.as_ptr());
        validate_channel_header(ch, ch.region_size as usize, seq)?;
        validate_v4_prefix(page0.as_ptr())?;
        if is_published(page0.as_ptr())? {
            return Ok(seq);
        }
        std::fs::remove_file(make_channel_file_path(base_path, seq)?)?;
    }
}

/// Unlink any `<base>.partial` / `<base>.<N>.partial` siblings.
/// Called by [`WriterBuilder::build`] so a previous crashed prep
/// leaves nothing for `create_new` to trip over. Errors are
/// swallowed — stale partials are inert.
fn sweep_stale_partial_files(base_path: &Path) {
    let parent = match base_path.parent() {
        Some(p) if p.as_os_str().is_empty() => std::path::PathBuf::from("."),
        Some(p) => p.to_path_buf(),
        None => std::path::PathBuf::from("."),
    };
    let Some(file_name) = base_path.file_name().and_then(|s| s.to_str()) else {
        return;
    };
    let Ok(entries) = read_dir(&parent) else {
        return;
    };
    for ent in entries.flatten() {
        let name_os = ent.file_name();
        let Some(name) = name_os.to_str() else {
            continue;
        };
        if is_partial_segment_name(name, file_name) {
            let _ = std::fs::remove_file(ent.path());
        }
    }
}

/// `<base>[.<N>].<instance>.partial`, or the older `<base>[.<N>].partial`.
fn is_partial_segment_name(name: &str, base_name: &str) -> bool {
    let base_partial = format!("{}.{}", base_name, PARTIAL_SUFFIX);
    if name == base_partial {
        return true;
    }
    let dotted = format!("{}.", base_name);
    let suffix = format!(".{}", PARTIAL_SUFFIX);
    let Some(middle) = name
        .strip_prefix(&dotted)
        .and_then(|m| m.strip_suffix(&suffix))
    else {
        return false;
    };
    let is_sequence = |s: &str| s.parse::<u64>().is_ok();
    let is_instance = |s: &str| s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit());
    match middle.split_once('.') {
        None => is_sequence(middle) || is_instance(middle),
        Some((sequence, instance)) => is_sequence(sequence) && is_instance(instance),
    }
}

fn find_earliest_sequence(base_path: &Path) -> io::Result<u64> {
    find_sequence(base_path, false)
}
fn find_latest_sequence(base_path: &Path) -> io::Result<u64> {
    find_sequence(base_path, true)
}

/// Find earliest or latest sequence number of a file.
/// If file(s) do not exist, returns Ok(0).
fn find_sequence(path: &Path, latest: bool) -> io::Result<u64> {
    let sequences = find_all_sequences(path)?;
    let result = if latest {
        sequences.into_iter().max().unwrap_or(0)
    } else {
        sequences.into_iter().min().unwrap_or(0)
    };
    Ok(result)
}

/// Scan the directory containing `base_path` for files matching `base` and
/// `base.<N>`, returning all sequence numbers found (0 for the base file)
/// in ascending order.
pub(crate) fn find_all_sequences(base_path: &Path) -> io::Result<Vec<u64>> {
    let parent_dir = match base_path.parent() {
        Some(parent) if parent.as_os_str().is_empty() => std::env::current_dir(),
        Some(parent) => Ok(parent.to_path_buf()),
        None => std::env::current_dir(),
    }?;
    let base_name = base_path
        .file_name()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "Invalid file name in path"))?
        .to_str()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "File name is not valid UTF-8"))?;

    let dotted = format!("{}.", base_name);
    let mut sequences: Vec<u64> = read_dir(&parent_dir)?
        .filter_map(|entry| {
            entry.ok().and_then(|e| {
                let file_name = e.file_name();
                let file_name = file_name.to_str()?;
                if file_name == base_name {
                    Some(0)
                } else if let Some(suffix) = file_name.strip_prefix(&dotted) {
                    suffix.parse().ok()
                } else {
                    None
                }
            })
        })
        .collect();
    sequences.sort_unstable();
    Ok(sequences)
}

/// Remove channel base and all rolled files created by this crate.
/// Scans the parent directory for entries matching `base` and `base.<N>`
/// and removes them, so this works correctly even when retention has left
/// a sparse set of rolled files (e.g. with `WriterBuilder::keep_files`).
pub fn cleanup_channel_files<P: AsRef<std::path::Path>>(base: P) {
    use std::fs;
    let base_path = base.as_ref();

    // Remove the base file (sequence 0); its attempt names are swept below.
    let _ = fs::remove_file(base_path);

    let parent = match base_path.parent() {
        Some(p) if p.as_os_str().is_empty() => std::path::PathBuf::from("."),
        Some(p) => p.to_path_buf(),
        None => std::path::PathBuf::from("."),
    };
    let Some(file_name) = base_path.file_name().and_then(|s| s.to_str()) else {
        return;
    };
    let prefix = format!("{file_name}.");

    let Ok(entries) = read_dir(&parent) else {
        return;
    };
    for ent in entries.flatten() {
        let name_os = ent.file_name();
        let Some(name) = name_os.to_str() else {
            continue;
        };
        let is_rolled = name
            .strip_prefix(&prefix)
            .and_then(|rest| rest.parse::<u64>().ok())
            .is_some();
        if is_rolled || is_partial_segment_name(name, file_name) {
            let _ = fs::remove_file(ent.path());
        }
    }
}

// ========== TESTS ==========
#[cfg(test)]
mod tests {
    use super::*;

    /// Peeking says what the next record is without taking it, and says the same thing twice.
    #[test]
    fn peek_header_does_not_consume() -> anyhow::Result<()> {
        let base = "test_peek_header";
        cleanup_channel_files(base);

        let mut writer = WriterBuilder::new(base).build()?;
        for (i, body) in [b"one".as_slice(), b"two".as_slice()].iter().enumerate() {
            let buf = writer.try_reserve(body.len())?;
            buf.copy_from_slice(body);
            writer.commit(7 + i as u16, body.len() as u32, 1_000 + i as u64)?;
        }

        let mut reader = ReaderBuilder::new(base)
            .mode(ReaderMode::LateJoin)
            .build()?;

        let first = reader.peek_header()?.expect("a record is there");
        assert_eq!(first.message_type, 7);
        assert_eq!(first.user_meta_u64, 1_000);
        assert_eq!(first.length, 3);

        // nothing was taken, so it says the same thing again
        assert_eq!(reader.peek_header()?.expect("still there"), first);

        // and the record itself is still the one waiting
        let msg = reader.try_read()?.expect("still there");
        assert_eq!(msg.header().message_type, 7);
        assert_eq!(msg.payload(), b"one");

        // now the second is on top
        let second = reader.peek_header()?.expect("the second");
        assert_eq!(second.message_type, 8);
        assert_eq!(second.user_meta_u64, 1_001);

        cleanup_channel_files(base);
        Ok(())
    }

    /// Caught up is `None`, not an error — and the reader keeps working once more arrives.
    #[test]
    fn peek_header_when_caught_up() -> anyhow::Result<()> {
        let base = "test_peek_header_empty";
        cleanup_channel_files(base);

        let mut writer = WriterBuilder::new(base).build()?;
        let mut reader = ReaderBuilder::new(base)
            .mode(ReaderMode::LateJoin)
            .build()?;
        assert!(reader.peek_header()?.is_none());

        let buf = writer.try_reserve(2)?;
        buf.copy_from_slice(b"hi");
        writer.commit(1, 2, 42)?;

        assert_eq!(reader.peek_header()?.expect("arrived").user_meta_u64, 42);
        assert_eq!(reader.try_read()?.expect("arrived").payload(), b"hi");

        cleanup_channel_files(base);
        Ok(())
    }

    /// A roll that cannot open the next segment is an error the caller can handle, not a
    /// state the reader never recovers from. The reader keeps its position in the segment it
    /// still holds, reports the failure as often as it is asked, and rolls through once the
    /// segment appears — which is what a reader lagging behind `keep_files` retention, or one
    /// reaching a segment the writer has not published yet, actually runs into.
    #[test]
    fn failed_roll_is_reported_not_fatal() -> anyhow::Result<()> {
        let base = "test_failed_roll_no_poison";
        cleanup_channel_files(base);

        let mut writer = WriterBuilder::new(base).build()?;
        let buf = writer.try_reserve(2)?;
        buf.copy_from_slice(b"a1");
        writer.commit(1, 2, 0)?;
        writer.roll_file()?;
        let buf = writer.try_reserve(2)?;
        buf.copy_from_slice(b"b1");
        writer.commit(2, 2, 0)?;

        let mut reader = ReaderBuilder::new(base)
            .mode(ReaderMode::LateJoin)
            .build()?;
        assert_eq!(reader.try_read()?.expect("first segment").payload(), b"a1");

        // The next segment goes missing before the reader rolls into it.
        let next = make_channel_file_path(Path::new(base), 1)?;
        let stashed = next.with_extension("stashed");
        std::fs::rename(&next, &stashed)?;

        assert!(
            reader.try_read().is_err(),
            "the roll must surface the error"
        );
        // ... and every later call must keep reporting it rather than panicking on a
        // half-advanced cursor.
        assert!(reader.try_read().is_err(), "the error must repeat");
        assert!(reader.peek_header().is_err(), "peeking must repeat it too");
        assert!(
            reader.try_read_batch(None).is_err(),
            "so must the batch path"
        );

        // Once the segment is there, the same reader rolls through as if nothing happened.
        std::fs::rename(&stashed, &next)?;
        assert_eq!(reader.try_read()?.expect("second segment").payload(), b"b1");

        cleanup_channel_files(base);
        Ok(())
    }

    /// Demonstrate earliest vs latest file usage (explicit roll).
    #[test]
    fn test_earliest_and_latest_sequences() -> anyhow::Result<()> {
        let base = "test_rolling_seq";
        cleanup_channel_files(base);

        let region_size = crate::page_size(); // portable
        let file_roll_size = (region_size as u64) * 100;

        let mut writer = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size(file_roll_size)
            .mtu(0)
            .build()?;

        // #101 in file0
        {
            let buf = writer.try_reserve(500)?;
            for b in buf.iter_mut() {
                *b = 0xAA;
            }
            writer.commit(101, 500, 0)?;
        }

        // Roll to file1
        writer.roll_file()?;

        // #102 and #103 in file1
        {
            let buf = writer.try_reserve(600)?;
            for b in buf.iter_mut() {
                *b = 0xBB;
            }
            writer.commit(102, 600, 1)?;
        }
        {
            let buf = writer.try_reserve(300)?;
            for b in buf.iter_mut() {
                *b = 0xCC;
            }
            writer.commit(103, 300, 2)?;
        }

        // remove file0 so earliest existing is file1
        std::fs::remove_file(base).ok();

        // LateJoin => we expect #102 then #103
        {
            let mut reader = ReaderBuilder::new(base)
                .mode(ReaderMode::LateJoin)
                .build()?;
            let msg1 = reader.try_read()?.expect("missing msg #102");
            let hdr1 = msg1.header();
            assert_eq!(hdr1.message_type, 102);
            assert_eq!(hdr1.length, 600);
            let payload = msg1.payload();
            for &b in payload {
                assert_eq!(b, 0xBB);
            }

            let msg2 = reader.try_read()?.expect("missing msg #103");
            let hdr2 = msg2.header();
            assert_eq!(hdr2.message_type, 103);
            assert_eq!(hdr2.length, 300);
            let payload2 = msg2.payload();
            for &b in payload2 {
                assert_eq!(b, 0xCC);
            }

            assert!(reader.try_read()?.is_none());
        }

        // Live => picks latest existing (file1), read_position=write_position => no new messages
        {
            let mut reader = Reader::open(base, ReaderMode::Live)?;
            assert!(reader.try_read()?.is_none());
        }

        cleanup_channel_files(base);
        Ok(())
    }

    /// `head_record_index` reports the true channel head even when the reader is parked on
    /// an older rolled file — the case a naive `base_record_index + message_count` of the
    /// reader's *current* file would get wrong.
    #[test]
    fn test_reader_exposes_region_size_and_mtu() -> anyhow::Result<()> {
        let base = "test_geometry_accessors";
        cleanup_channel_files(base);
        let region_size = crate::page_size();
        {
            let mut w = WriterBuilder::new(base)
                .region_size(region_size)
                .mtu(4096)
                .build()?;
            let buf = w.try_reserve(8)?;
            buf.copy_from_slice(&[0u8; 8]);
            w.commit(1, 8, 0)?;
        }
        let r = Reader::open(base, ReaderMode::LateJoin)?;
        assert_eq!(r.region_size(), region_size);
        assert_eq!(r.mtu(), 4096);
        cleanup_channel_files(base);
        Ok(())
    }

    #[test]
    fn test_head_record_index_across_rolls() -> anyhow::Result<()> {
        let base = "test_head_record_index";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        let file_roll_size = (region_size as u64) * 2; // small => frequent rolls

        let mut writer = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size(file_roll_size)
            .mtu(0)
            .build()?;

        // Enough records to roll across several files (genesis retained, no keep_files).
        let n = 200u64;
        for i in 0..n {
            let buf = writer.try_reserve(500)?;
            for b in buf.iter_mut() {
                *b = 0xAB;
            }
            writer.commit((i % 7) as u16, 500, i)?;
        }
        assert_eq!(writer.next_record_index(), n);

        // A LateJoin reader is parked at the earliest (genesis) file: base far below head.
        let reader = ReaderBuilder::new(base)
            .mode(ReaderMode::LateJoin)
            .build()?;
        assert_eq!(
            reader.base_record_index(),
            0,
            "reader parked at genesis file"
        );
        assert_eq!(
            reader.head_record_index()?,
            n,
            "head must reflect the channel frontier (latest file), not the reader's file"
        );

        cleanup_channel_files(base);
        Ok(())
    }

    /// `file_sequence` makes the writer's segmentation observable to a reader: sampled
    /// around a single-record read, a change identifies the record that begins a new
    /// segment. Written the way a replicator uses it — explicit `roll_file()` with no
    /// `file_roll_size`, so every boundary is one the application chose.
    #[test]
    fn test_file_sequence_locates_roll_boundaries() -> anyhow::Result<()> {
        let base = "test_file_sequence";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        let mut writer = WriterBuilder::new(base).region_size(region_size).build()?;

        // Three segments of two records each; the roll boundaries fall at records 2 and 4.
        for i in 0..6u64 {
            if i > 0 && i.is_multiple_of(2) {
                writer.roll_file()?;
            }
            let buf = writer.try_reserve(32)?;
            buf.fill(0xC3);
            writer.commit(1, 32, i)?;
        }
        assert_eq!(writer.file_sequence, 2, "two rolls ⇒ writer on segment 2");

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        assert_eq!(reader.file_sequence(), 0, "LateJoin starts at the earliest");

        // Replay, recording which record indices the reader saw a segment change on.
        let mut boundaries = Vec::new();
        for expected in 0..6u64 {
            let before = reader.file_sequence();
            let meta = reader
                .try_read()?
                .map(|m| m.header().user_meta_u64)
                .ok_or_else(|| err_other("expected 6 records"))?;
            if reader.file_sequence() != before {
                boundaries.push(meta);
            }
            assert_eq!(meta, expected, "records must replay in order");
        }
        assert_eq!(
            boundaries,
            vec![2, 4],
            "a change must land on the first record of each new segment"
        );
        assert_eq!(reader.file_sequence(), 2, "reader followed both rolls");

        // A reader that joins after retention pruned the genesis segment reports the
        // sequence it actually opened, not 0 — so sequences are absolute, not relative.
        std::fs::remove_file(make_channel_file_path(std::path::Path::new(base), 0)?)?;
        let late = Reader::open(base, ReaderMode::LateJoin)?;
        assert_eq!(late.file_sequence(), 1);

        cleanup_channel_files(base);
        Ok(())
    }

    /// The generation is stamped at creation, carried into every rolled segment, and
    /// preserved when a writer reopens the channel — so it identifies the *log*, not a file.
    #[test]
    fn test_generation_is_stamped_and_survives_rolls_and_reopen() -> anyhow::Result<()> {
        let base = "test_generation";
        cleanup_channel_files(base);
        let region_size = crate::page_size();

        let commit_one = |w: &mut Writer, ts: u64| -> io::Result<()> {
            let payload = w.try_reserve(32)?;
            payload.fill(0x7E);
            w.commit(1, 32, ts)
        };

        {
            let mut w = WriterBuilder::new(base)
                .region_size(region_size)
                .generation(0xFEED_1234)
                .build()?;
            assert_eq!(w.generation(), 0xFEED_1234);
            commit_one(&mut w, 0)?;
            w.roll_file()?;
            commit_one(&mut w, 1)?;
            assert_eq!(
                w.generation(),
                0xFEED_1234,
                "carried into the rolled segment"
            );
        }

        // Reopening ignores the builder's value — the on-disk one wins, as with
        // `base_record_index`. A writer must not be able to relabel an existing log.
        {
            let mut w = WriterBuilder::new(base)
                .region_size(region_size)
                .generation(0xBAD)
                .build()?;
            assert_eq!(w.generation(), 0xFEED_1234);
            commit_one(&mut w, 2)?;
        }

        // A reader sees it from genesis and still sees it after following the roll.
        let mut r = Reader::open(base, ReaderMode::LateJoin)?;
        assert_eq!(r.generation(), 0xFEED_1234);
        for _ in 0..3 {
            assert!(r.try_read()?.is_some());
        }
        assert_eq!(r.file_sequence(), 1, "reader followed the roll");
        assert_eq!(r.generation(), 0xFEED_1234);

        cleanup_channel_files(base);
        Ok(())
    }

    /// Recreating a channel at the same path yields a different generation — the case a
    /// path plus a record index cannot distinguish, since both restart at 0.
    #[test]
    fn test_generation_distinguishes_a_recreated_channel() -> anyhow::Result<()> {
        let base = "test_generation_recreate";
        cleanup_channel_files(base);
        let region_size = crate::page_size();

        for generation in [7u64, 8] {
            cleanup_channel_files(base);
            let mut w = WriterBuilder::new(base)
                .region_size(region_size)
                .generation(generation)
                .build()?;
            let payload = w.try_reserve(16)?;
            payload.fill(0x11);
            w.commit(1, 16, 0)?;
            drop(w);

            let r = Reader::open(base, ReaderMode::LateJoin)?;
            assert_eq!(
                r.base_record_index(),
                0,
                "both incarnations start at genesis"
            );
            assert_eq!(
                r.generation(),
                generation,
                "only the generation tells the two apart"
            );
        }

        cleanup_channel_files(base);
        Ok(())
    }

    /// A segment that does not continue the absolute numbering is refused rather than
    /// spliced. This is the case nothing else catches: a channel deleted and rebuilt at the
    /// same path restarts at sequence 0 and reuses the same filenames, so a reader holding an
    /// unlinked file can follow a roll into the *rebuilt* series with `channel_sequence` and
    /// `generation` both matching. Only the base gives it away.
    #[test]
    fn test_roll_into_a_foreign_segment_is_refused() -> anyhow::Result<()> {
        let region_size = crate::page_size();
        let write_segments = |base: &str, start: u64, first: u64, second: u64| -> io::Result<()> {
            let mut w = WriterBuilder::new(base)
                .region_size(region_size)
                .base_record_index(start)
                .build()?;
            let commit = |w: &mut Writer, n: u64| -> io::Result<()> {
                for _ in 0..n {
                    let buf = w.try_reserve(16)?;
                    buf.fill(0x2C);
                    w.commit(1, 16, 0)?;
                }
                Ok(())
            };
            commit(&mut w, first)?;
            w.roll_file()?;
            commit(&mut w, second)?;
            Ok(())
        };

        let ours = "test_continuity_ours";
        let theirs = "test_continuity_theirs";
        cleanup_channel_files(ours);
        cleanup_channel_files(theirs);
        write_segments(ours, 0, 2, 2)?; // segments at bases 0 and 2
        write_segments(theirs, 100, 5, 1)?; // segments at bases 100 and 105

        // Intact, the series reads straight through — the check must not fire on a real roll.
        {
            let mut r = Reader::open(ours, ReaderMode::LateJoin)?;
            let mut seen = 0;
            while r.try_read()?.is_some() {
                seen += 1;
            }
            assert_eq!(seen, 4);
        }

        // Swap in a segment from the other series. Same sequence number, same generation,
        // same geometry — but not the file the Roll names: its instance ID gives it away before
        // its base is even read.
        std::fs::copy(
            make_channel_file_path(Path::new(theirs), 1)?,
            make_channel_file_path(Path::new(ours), 1)?,
        )?;

        let mut r = Reader::open(ours, ReaderMode::LateJoin)?;
        assert!(r.try_read()?.is_some());
        assert!(r.try_read()?.is_some());
        let err = match r.try_read() {
            Ok(_) => panic!("following the roll must refuse the foreign segment"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(
            err.to_string()
                .contains("but the Roll that leads to it names instance"),
            "unexpected error: {err}"
        );

        cleanup_channel_files(ours);
        cleanup_channel_files(theirs);
        Ok(())
    }

    /// `keep_files(N)` should retain only the active file plus N-1
    /// historical rolled files. Each successful `roll_file` unlinks the
    /// file at `current_seq - N` (if it exists). Files past the retention
    /// window must no longer exist on disk.
    #[test]
    fn test_keep_files_retention() -> anyhow::Result<()> {
        let base = "test_keep_files_retention";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        let mut writer = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size((region_size as u64) * 100)
            .keep_files(2) // keep current + 1 historical
            .build()?;

        // Write something into each file then roll to the next.
        let commit_one = |w: &mut Writer, ts: u64| -> io::Result<()> {
            let payload = w.try_reserve(64)?;
            for b in payload.iter_mut() {
                *b = 0x5A;
            }
            w.commit(1, 64, ts)
        };

        for ts in 0..5u64 {
            commit_one(&mut writer, ts)?;
            writer.roll_file()?;
        }

        // After 5 rolls, writer is on file 5. With keep_files(2) the
        // expected on-disk files are {4, 5}. Anything below 4 must be gone.
        for seq in 0..=3u64 {
            let p = make_channel_file_path(std::path::Path::new(base), seq)?;
            assert!(
                !p.exists(),
                "expected pruned file to be gone: {} (seq {})",
                p.display(),
                seq
            );
        }
        for seq in 4..=5u64 {
            let p = make_channel_file_path(std::path::Path::new(base), seq)?;
            assert!(
                p.exists(),
                "expected retained file to exist: {} (seq {})",
                p.display(),
                seq
            );
        }

        cleanup_channel_files(base);
        Ok(())
    }

    /// `keep_files` should not affect the unbounded default. Without it,
    /// every file from the run remains on disk.
    #[test]
    fn test_keep_files_default_unlimited() -> anyhow::Result<()> {
        let base = "test_keep_files_default";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        let mut writer = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size((region_size as u64) * 100)
            .build()?;

        for _ in 0..3 {
            let payload = writer.try_reserve(32)?;
            payload.fill(0xC3);
            writer.commit(1, 32, 0)?;
            writer.roll_file()?;
        }

        for seq in 0..=3u64 {
            let p = make_channel_file_path(std::path::Path::new(base), seq)?;
            assert!(
                p.exists(),
                "default retention should keep all files; missing {} (seq {})",
                p.display(),
                seq
            );
        }

        cleanup_channel_files(base);
        Ok(())
    }

    /// `read_blocking(Some(timeout))` should return `Ok(None)` once the
    /// timeout elapses if no message is available.
    #[test]
    fn test_read_blocking_times_out() -> anyhow::Result<()> {
        let base = "test_read_blocking_timeout";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        WriterBuilder::new(base)
            .region_size(region_size)
            .precreate()?;

        let mut reader = ReaderBuilder::new(base).live().build()?;

        let start = std::time::Instant::now();
        let msg = reader.read_blocking(Some(std::time::Duration::from_millis(50)))?;
        let elapsed = start.elapsed();

        assert!(msg.is_none(), "expected timeout, got message");
        assert!(
            elapsed >= std::time::Duration::from_millis(45),
            "returned too early: {elapsed:?}"
        );
        assert!(
            elapsed < std::time::Duration::from_millis(200),
            "returned too late: {elapsed:?}"
        );

        cleanup_channel_files(base);
        Ok(())
    }

    /// `read_blocking` should return a message that arrives after the call
    /// starts (here: a writer thread publishes 25 ms in).
    #[test]
    fn test_read_blocking_wakes_on_publish() -> anyhow::Result<()> {
        let base = "test_read_blocking_wake";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        WriterBuilder::new(base)
            .region_size(region_size)
            .precreate()?;

        // Background writer publishes one message after a short delay.
        let writer_base = base.to_string();
        let writer_thread = std::thread::spawn(move || -> anyhow::Result<()> {
            let mut writer = WriterBuilder::new(&writer_base)
                .region_size(region_size)
                .build()?;
            std::thread::sleep(std::time::Duration::from_millis(25));
            let payload = writer.try_reserve(8)?;
            payload.copy_from_slice(b"deadbeef");
            writer.commit(42, 8, 0)?;
            Ok(())
        });

        let mut reader = ReaderBuilder::new(base).live().build()?;
        let start = std::time::Instant::now();
        let msg = reader.read_blocking(Some(std::time::Duration::from_secs(2)))?;
        let elapsed = start.elapsed();

        let msg = msg.expect("expected a message before timeout");
        assert_eq!(msg.header().message_type, 42);
        assert_eq!(msg.payload(), b"deadbeef");
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "took too long to wake: {elapsed:?}"
        );

        writer_thread.join().expect("writer thread panicked")?;
        cleanup_channel_files(base);
        Ok(())
    }

    /// `wait_for_message` returns `Ok(true)` once a user record is at the
    /// read cursor, and a subsequent `try_read` retrieves that same record
    /// (the cursor was not consumed by the wait).
    #[test]
    fn test_wait_for_message_ready() -> anyhow::Result<()> {
        let base = "test_wait_for_message_ready";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        let mut writer = WriterBuilder::new(base).region_size(region_size).build()?;
        let buf = writer.try_reserve(8)?;
        buf.copy_from_slice(b"abcdefgh");
        writer.commit(7, 8, 0)?;
        drop(writer);

        let mut reader = ReaderBuilder::new(base).build()?;
        // Already published; wait_for_message should return immediately.
        assert!(reader.wait_for_message(Some(std::time::Duration::from_millis(100)))?);
        let msg = reader
            .try_read()?
            .expect("try_read after ready must return Some");
        assert_eq!(msg.header().message_type, 7);
        assert_eq!(msg.payload(), b"abcdefgh");

        cleanup_channel_files(base);
        Ok(())
    }

    /// `wait_for_message(Some(d))` returns `Ok(false)` once `d` elapses.
    #[test]
    fn test_wait_for_message_times_out() -> anyhow::Result<()> {
        let base = "test_wait_for_message_timeout";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        WriterBuilder::new(base)
            .region_size(region_size)
            .precreate()?;

        let mut reader = ReaderBuilder::new(base).live().build()?;

        let start = std::time::Instant::now();
        let ready = reader.wait_for_message(Some(std::time::Duration::from_millis(50)))?;
        let elapsed = start.elapsed();

        assert!(!ready, "expected timeout, got ready");
        assert!(
            elapsed >= std::time::Duration::from_millis(45),
            "early: {elapsed:?}"
        );
        assert!(
            elapsed < std::time::Duration::from_millis(200),
            "late: {elapsed:?}"
        );

        cleanup_channel_files(base);
        Ok(())
    }

    /// `wait_for_message` must advance past Skip records transparently, so
    /// that after it returns `Ok(true)` the cursor sits on a User record
    /// even if the writer's last published record was a region-rolling
    /// Skip followed by a user message in the next region.
    #[test]
    fn test_wait_for_message_skips_service_records() -> anyhow::Result<()> {
        let base = "test_wait_for_message_skips_service";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        // Force a region roll: a payload large enough that the next 8-byte
        // try_reserve triggers roll_over_region, then a small follow-up
        // message in the new region.
        let big = vec![0xAAu8; 3968];
        let small: [u8; 8] = [0xBB; 8];

        let mut writer = WriterBuilder::new(base).region_size(region_size).build()?;
        let buf = writer.try_reserve(big.len())?;
        buf.copy_from_slice(&big);
        writer.commit(1, big.len() as u32, 0)?;
        let buf = writer.try_reserve(small.len())?;
        buf.copy_from_slice(&small);
        writer.commit(2, small.len() as u32, 0)?;
        drop(writer);

        let mut reader = ReaderBuilder::new(base).build()?;

        // Drain the big User record so the cursor advances to the Skip.
        let first = reader.try_read()?.expect("first message");
        assert_eq!(first.payload().len(), 3968);

        // Cursor now points at the Skip. wait_for_message must advance past
        // it and land on the small User record in region 1.
        assert!(reader.wait_for_message(Some(std::time::Duration::from_millis(100)))?);
        let second = reader.try_read()?.expect("second message after Skip");
        assert_eq!(second.header().message_type, 2);
        assert_eq!(second.payload(), &small);

        cleanup_channel_files(base);
        Ok(())
    }

    /// A genuinely missing channel must still surface `ErrorKind::NotFound`
    /// — the retry loop on Reader::open's directory-scan race must not
    /// swallow a real "no such channel" condition.
    #[test]
    fn test_reader_open_missing_channel_returns_notfound() -> anyhow::Result<()> {
        let base = "test_reader_open_missing_channel";
        cleanup_channel_files(base);

        let err = Reader::open(base, ReaderMode::LateJoin)
            .err()
            .expect("must fail for missing channel");
        assert_eq!(err.kind(), ErrorKind::NotFound);

        let err = Reader::open(base, ReaderMode::Live)
            .err()
            .expect("must fail for missing channel");
        assert_eq!(err.kind(), ErrorKind::NotFound);

        Ok(())
    }

    /// Simple write/read across file rolls.
    #[test]
    fn test_write_and_read_full_payload() -> anyhow::Result<()> {
        let base = "test_write_read_payload";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        let file_roll_size = (region_size as u64) * 100; // won't auto-roll
        let mtu = 0;

        let mut writer = Writer::open_or_create(
            base,
            region_size,
            file_roll_size,
            mtu,
            None,
            [0; CHANNEL_NAME_MAX],
            0,     // base_record_index: genesis
            0,     // generation: unset
            false, // wake_readers
        )?;

        let msg1: Vec<u8> = (0..100).map(|i| i as u8).collect();
        let msg2: Vec<u8> = vec![0x55; 200];
        let msg3: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];

        {
            let payload = writer.try_reserve(msg1.len())?;
            payload.copy_from_slice(&msg1);
            writer.commit(201, msg1.len() as u32, 0)?;
        }
        writer.roll_file()?;
        {
            let payload = writer.try_reserve(msg2.len())?;
            payload.copy_from_slice(&msg2);
            writer.commit(202, msg2.len() as u32, 1)?;
        }
        writer.roll_file()?;
        {
            let payload = writer.try_reserve(msg3.len())?;
            payload.copy_from_slice(&msg3);
            writer.commit(203, msg3.len() as u32, 2)?;
        }

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        {
            let msg = reader.try_read()?.expect("missing msg1");
            let hdr = msg.header();
            assert_eq!(hdr.message_type, 201);
            assert_eq!(msg.payload(), &msg1[..]);
        }
        {
            let msg = reader.try_read()?.expect("missing msg2");
            let hdr = msg.header();
            assert_eq!(hdr.message_type, 202);
            assert_eq!(msg.payload(), &msg2[..]);
        }
        {
            let msg = reader.try_read()?.expect("missing msg3");
            let hdr = msg.header();
            assert_eq!(hdr.message_type, 203);
            assert_eq!(msg.payload(), &msg3[..]);
        }
        assert!(reader.try_read()?.is_none());

        cleanup_channel_files(base);
        Ok(())
    }

    #[test]
    fn test_live_roll_reads_new_file_from_start() -> anyhow::Result<()> {
        let base = "test_live_roll_from_start";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        let file_roll_size = (region_size as u64) * 10;
        let mut writer = Writer::open_or_create(
            base,
            region_size,
            file_roll_size,
            0,
            None,
            [0; CHANNEL_NAME_MAX],
            0,     // base_record_index: genesis
            0,     // generation: unset
            false, // wake_readers
        )?;

        let payload0 = vec![0x10; 16];
        {
            let buf = writer.try_reserve(payload0.len())?;
            buf.copy_from_slice(&payload0);
            writer.commit(1, payload0.len() as u32, 0)?;
        }

        let mut reader = Reader::open(base, ReaderMode::Live)?;

        writer.roll_file()?;

        let payload1 = vec![0x22; 24];
        let payload2 = vec![0x33; 8];
        {
            let buf = writer.try_reserve(payload1.len())?;
            buf.copy_from_slice(&payload1);
            writer.commit(2, payload1.len() as u32, 0)?;
        }
        {
            let buf = writer.try_reserve(payload2.len())?;
            buf.copy_from_slice(&payload2);
            writer.commit(3, payload2.len() as u32, 0)?;
        }

        let msg1 = reader.try_read()?.expect("missing msg1");
        assert_eq!(msg1.payload(), &payload1[..]);
        let msg2 = reader.try_read()?.expect("missing msg2");
        assert_eq!(msg2.payload(), &payload2[..]);
        assert!(reader.try_read()?.is_none());

        cleanup_channel_files(base);
        Ok(())
    }

    #[test]
    fn test_boundary_skip_and_alignment() -> anyhow::Result<()> {
        let base = "test_boundary_skip";
        cleanup_channel_files(base);

        let region = crate::page_size();
        let file_roll_size = (region as u64) * 10;
        let mut w = Writer::open_or_create(
            base,
            region,
            file_roll_size,
            0,
            None,
            [0; CHANNEL_NAME_MAX],
            0,
            0,     // generation: unset
            false, // wake_readers
        )?;

        // Choose len so that after header + payload the aligned end leaves just the room a
        // Roll needs, which every record must leave behind it.
        let record_with_padding = region - ROLL_TOTAL;
        assert_eq!(record_with_padding % ALIGN, 0);
        let len = record_with_padding - HEADER_SLOT;
        {
            let buf = w.try_reserve(len)?;
            for b in buf.iter_mut() {
                *b = 0xAB;
            }
            w.commit(1, len as u32, 0)?;
        }

        // Next small message should force a Skip and write at the start of next region.
        {
            let buf = w.try_reserve(32)?;
            for b in buf.iter_mut() {
                *b = 0xCD;
            }
            w.commit(2, 32, 1)?;
        }

        let mut r = Reader::open(base, ReaderMode::LateJoin)?;
        let m1 = r.try_read()?.expect("m1");
        assert_eq!(m1.header().message_type, 1);
        assert_eq!(m1.header_offset % ALIGN, 0);

        let m2 = r.try_read()?.expect("m2");
        assert_eq!(m2.header().message_type, 2);
        assert_eq!(m2.header_offset % ALIGN, 0);

        cleanup_channel_files(base);
        Ok(())
    }

    #[test]
    fn test_try_read_batch_skips_service_messages() -> anyhow::Result<()> {
        let base = "test_batch_skip_service";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        let file_roll_size = (region_size as u64) * 10;
        let mut writer = Writer::open_or_create(
            base,
            region_size,
            file_roll_size,
            0,
            None,
            [0; CHANNEL_NAME_MAX],
            0,     // base_record_index: genesis
            0,     // generation: unset
            false, // wake_readers
        )?;

        let payload1 = vec![0xA1; 32];
        let payload2 = vec![0xB2; 48];

        {
            let buf = writer.try_reserve(payload1.len())?;
            buf.copy_from_slice(&payload1);
            writer.commit(1, payload1.len() as u32, 0)?;
        }
        {
            let buf = writer.try_reserve(payload2.len())?;
            buf.copy_from_slice(&payload2);
            writer.commit(2, payload2.len() as u32, 0)?;
        }

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        let batch = reader.try_read_batch(None)?.expect("missing batch");
        assert_eq!(batch.len(), 2);

        let msg0 = batch.get(0).unwrap();
        assert_eq!(msg0.header().parsed_header_type()?, HeaderType::User);
        assert_eq!(msg0.payload(), &payload1[..]);

        let msg1 = batch.get(1).unwrap();
        assert_eq!(msg1.header().parsed_header_type()?, HeaderType::User);
        assert_eq!(msg1.payload(), &payload2[..]);

        assert!(reader.try_read_batch(None)?.is_none());

        cleanup_channel_files(base);
        Ok(())
    }

    #[test]
    fn test_try_read_batch_across_regions() -> anyhow::Result<()> {
        let base = "test_batch_across_regions";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        let file_roll_size = (region_size as u64) * 10;
        let mut writer = Writer::open_or_create(
            base,
            region_size,
            file_roll_size,
            0,
            None,
            [0; CHANNEL_NAME_MAX],
            0,     // base_record_index: genesis
            0,     // generation: unset
            false, // wake_readers
        )?;

        let start = FIRST_RECORD;
        let record_with_padding = region_size - start - HEADER_SLOT;
        assert_eq!(record_with_padding % ALIGN, 0);
        let len = record_with_padding - HEADER_SLOT;

        let payload1 = vec![0x11; len];
        let payload2 = vec![0x22; 32];

        {
            let buf = writer.try_reserve(payload1.len())?;
            buf.copy_from_slice(&payload1);
            writer.commit(10, payload1.len() as u32, 0)?;
        }
        {
            let buf = writer.try_reserve(payload2.len())?;
            buf.copy_from_slice(&payload2);
            writer.commit(11, payload2.len() as u32, 0)?;
        }

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        let batch = reader.try_read_batch(None)?.expect("missing batch");
        assert_eq!(batch.len(), 2);
        assert!(batch.maps.len() > 1);
        let file_seq = batch.maps[0].file_sequence;
        assert!(batch.maps.iter().all(|m| m.file_sequence == file_seq));

        let msg0 = batch.get(0).unwrap();
        assert_eq!(msg0.payload(), &payload1[..]);
        let msg1 = batch.get(1).unwrap();
        assert_eq!(msg1.payload(), &payload2[..]);

        cleanup_channel_files(base);
        Ok(())
    }

    #[test]
    fn test_try_read_batch_across_files() -> anyhow::Result<()> {
        let base = "test_batch_across_files";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        let file_roll_size = (region_size as u64) * 10;
        let mut writer = Writer::open_or_create(
            base,
            region_size,
            file_roll_size,
            0,
            None,
            [0; CHANNEL_NAME_MAX],
            0,     // base_record_index: genesis
            0,     // generation: unset
            false, // wake_readers
        )?;

        let payload1 = vec![0x3A; 64];
        let payload2 = vec![0x7B; 48];

        {
            let buf = writer.try_reserve(payload1.len())?;
            buf.copy_from_slice(&payload1);
            writer.commit(20, payload1.len() as u32, 0)?;
        }
        writer.roll_file()?;
        {
            let buf = writer.try_reserve(payload2.len())?;
            buf.copy_from_slice(&payload2);
            writer.commit(21, payload2.len() as u32, 0)?;
        }

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        let batch = reader.try_read_batch(None)?.expect("missing batch");
        assert_eq!(batch.len(), 2);
        assert!(batch.maps.len() > 1);
        let file_seq = batch.maps[0].file_sequence;
        assert!(batch.maps.iter().any(|m| m.file_sequence != file_seq));

        let msg0 = batch.get(0).unwrap();
        assert_eq!(msg0.payload(), &payload1[..]);
        let msg1 = batch.get(1).unwrap();
        assert_eq!(msg1.payload(), &payload2[..]);

        cleanup_channel_files(base);
        Ok(())
    }

    #[test]
    fn test_try_read_batch_empty_does_not_advance() -> anyhow::Result<()> {
        let base = "test_batch_empty";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        let file_roll_size = (region_size as u64) * 10;
        let _writer = Writer::open_or_create(
            base,
            region_size,
            file_roll_size,
            0,
            None,
            [0; CHANNEL_NAME_MAX],
            0,     // base_record_index: genesis
            0,     // generation: unset
            false, // wake_readers
        )?;

        let mut reader = Reader::open(base, ReaderMode::Live)?;
        let before = reader.read_position;
        assert!(reader.try_read_batch(None)?.is_none());
        assert_eq!(reader.read_position, before);

        cleanup_channel_files(base);
        Ok(())
    }

    #[test]
    fn test_try_read_batch_service_only_advances() -> anyhow::Result<()> {
        let base = "test_batch_service_only_advances";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        let file_roll_size = (region_size as u64) * 10;
        let _writer = Writer::open_or_create(
            base,
            region_size,
            file_roll_size,
            0,
            None,
            [0; CHANNEL_NAME_MAX],
            0,     // base_record_index: genesis
            0,     // generation: unset
            false, // wake_readers
        )?;

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        let before = reader.read_position;
        assert!(reader.try_read_batch(None)?.is_none());
        assert!(reader.read_position > before);

        cleanup_channel_files(base);
        Ok(())
    }

    #[test]
    fn test_invalid_committed_flag_returns_error() -> anyhow::Result<()> {
        let base = "test_invalid_committed_flag";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size((region_size as u64) * 10)
            .precreate()?;

        let start = FIRST_RECORD;
        let file = OpenOptions::new().read(true).write(true).open(base)?;
        let mut region0 = RegionMapping::create_writable(&file, 0, region_size)?;
        let header = region0
            .get_bytes_mut(start, MESSAGE_HEADER_SIZE)
            .expect("first user header must exist");
        header[0] = 2;
        drop(region0);
        drop(file);

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        let err = match reader.try_read() {
            Ok(_) => panic!("invalid committed flag should error"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::InvalidData);

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        let err = match reader.try_read_batch(None) {
            Ok(_) => panic!("invalid committed flag should error"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::InvalidData);

        cleanup_channel_files(base);
        Ok(())
    }

    #[test]
    fn test_invalid_header_type_returns_error() -> anyhow::Result<()> {
        let base = "test_invalid_header_type";
        cleanup_channel_files(base);

        let region_size = crate::page_size();
        WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size((region_size as u64) * 10)
            .precreate()?;

        let start = FIRST_RECORD;
        let file = OpenOptions::new().read(true).write(true).open(base)?;
        let mut region0 = RegionMapping::create_writable(&file, 0, region_size)?;
        let header = region0
            .get_bytes_mut(start, MESSAGE_HEADER_SIZE)
            .expect("first user header must exist");
        header[0] = 1;
        header[1] = 99;
        drop(region0);
        drop(file);

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        let err = match reader.try_read() {
            Ok(_) => panic!("invalid header_type should error"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::InvalidData);

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        let err = match reader.try_read_batch(None) {
            Ok(_) => panic!("invalid header_type should error"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::InvalidData);

        cleanup_channel_files(base);
        Ok(())
    }

    /// Round-trip `channel_name`: a value written via `WriterBuilder` is
    /// visible via `Reader::channel_name`, and a too-long name is rejected
    /// at the builder.
    #[test]
    fn test_channel_name_round_trip() -> anyhow::Result<()> {
        let base = "test_channel_name_round_trip";
        cleanup_channel_files(base);

        // 28 bytes — would not have fit the 20-byte field before format_version 3.
        const NAME: &str = "fills.prod.options-mm.emea-1";

        let _w = WriterBuilder::new(base)
            .region_size(page_size())
            .channel_name(NAME)?
            .build()?;

        let reader = ReaderBuilder::new(base).build()?;
        assert_eq!(reader.channel_name(), NAME);
        drop(reader);

        // Channel name longer than CHANNEL_NAME_MAX is rejected by the builder.
        let too_long = "x".repeat(CHANNEL_NAME_MAX + 1);
        let err = WriterBuilder::new(base)
            .channel_name(&too_long)
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);

        cleanup_channel_files(base);
        Ok(())
    }

    /// Overwrite `ChannelHeader.write_position` on disk to simulate the byte
    /// pattern a writer would leave if it crashed before a publish_wp.
    fn rewind_write_position_on_disk(base: &str, rewind_bytes: u64) -> anyhow::Result<()> {
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(make_channel_file_path(Path::new(base), 0)?)?;
        // ChannelHeader sits immediately after the 16-byte system MessageHeader.
        // Its first field is `write_position: AtomicU64` at byte offset 16.
        const WP_OFFSET: u64 = MESSAGE_HEADER_SIZE as u64;
        f.seek(SeekFrom::Start(WP_OFFSET))?;
        let mut bytes = [0u8; 8];
        f.read_exact(&mut bytes)?;
        let wp = u64::from_le_bytes(bytes);
        let new_wp = wp - rewind_bytes;
        f.seek(SeekFrom::Start(WP_OFFSET))?;
        f.write_all(&new_wp.to_le_bytes())?;
        f.sync_all()?;
        Ok(())
    }

    /// INV5 — single-step recovery (User-record case).
    ///
    /// A writer that crashes between `MessageHeader::commit` (setting the
    /// current slot's committed=1) and `publish_wp` (advancing
    /// `ChannelHeader.write_position` past it) leaves the file with one
    /// committed User record at `write_position - HEADER_SLOT`. The
    /// next slot is the pre-installed header for the *next* record
    /// (committed=0, header_type=User, all-zero fields).
    ///
    /// `Writer::open_or_create` must detect this, advance past the
    /// orphaned record, verify the pre-install signature on the next
    /// slot, update `write_position`, and resume — without losing or
    /// rewriting the committed record.
    #[test]
    fn test_writer_recovers_single_step_user_crash() -> anyhow::Result<()> {
        let base = "test_writer_recovers_single_step_user_crash";
        cleanup_channel_files(base);

        let region_size = page_size();
        let payload0: [u8; 8] = [0xAB; 8];
        let payload1: [u8; 8] = [0xCD; 8];

        // 1) Clean writer: write one message, drop.
        {
            let mut w = WriterBuilder::new(base).region_size(region_size).build()?;
            let buf = w.try_reserve(payload0.len())?;
            buf.copy_from_slice(&payload0);
            w.commit(1, payload0.len() as u32, 0)?;
        }

        // 2) Inject the crash state: rewind wp by one full record.
        let record_size = (HEADER_SLOT + payload0.len()).next_multiple_of(ALIGN) as u64;
        rewind_write_position_on_disk(base, record_size)?;

        // 3) Reopen — must succeed (single-step recovery).
        let mut w = WriterBuilder::new(base).region_size(region_size).build()?;

        // 4) The recovered writer must keep going.
        let buf = w.try_reserve(payload1.len())?;
        buf.copy_from_slice(&payload1);
        w.commit(2, payload1.len() as u32, 0)?;
        drop(w);

        // 5) Reader sees both messages, in order, with original payloads.
        let mut r = Reader::open(base, ReaderMode::LateJoin)?;
        let m0 = r.try_read()?.expect("message 0 should be visible");
        assert_eq!(m0.payload(), &payload0);
        assert_eq!(m0.header().message_type, 1);
        let m1 = r
            .try_read()?
            .expect("message 1 (post-recovery) should be visible");
        assert_eq!(m1.payload(), &payload1);
        assert_eq!(m1.header().message_type, 2);
        assert!(r.try_read()?.is_none(), "no further messages");

        cleanup_channel_files(base);
        Ok(())
    }

    /// INV5 — single-step recovery (Skip-record case).
    ///
    /// `roll_over_region` writes a Skip in the old region and pre-installs
    /// the next region's first header *before* calling `publish_wp`. A
    /// crash in that window leaves a committed Skip at
    /// `write_position - HEADER_SLOT`. Recovery follows the Skip into
    /// the next region (re-mapping it), verifies the pre-install
    /// signature, and resumes there.
    #[test]
    fn test_writer_recovers_single_step_skip_crash() -> anyhow::Result<()> {
        let base = "test_writer_recovers_single_step_skip_crash";
        cleanup_channel_files(base);

        let region_size = page_size();
        // Big payload to wedge near the end of region 0, so the next
        // try_reserve triggers a region roll. With region_size=4096 and
        // the first record at 208 (16-byte Channel MessageHeader, 128-byte
        // ChannelHeader, 64-byte v4 extension), 3816 puts the post-big
        // next-header slot at 4040: the 56 bytes every record leaves free
        // for a Roll, and too few for an 8-byte message plus that room.
        let big = vec![0x77u8; 3816];
        let small_payload: [u8; 8] = [0xEE; 8];

        // 1) Write one big message, then `try_reserve` an 8-byte slot —
        //    this triggers roll_over_region (Skip in region 0, pre-install
        //    in region 1, wp advanced to start of region 1's payload area).
        //    Drop the writer WITHOUT committing the second message — the
        //    pre-installed slot in region 1 stays pristine.
        {
            let mut w = WriterBuilder::new(base).region_size(region_size).build()?;
            let buf = w.try_reserve(big.len())?;
            buf.copy_from_slice(&big);
            w.commit(1, big.len() as u32, 0)?;
            // This try_reserve forces the roll; the returned buffer is
            // never filled and we never call commit.
            let _ = w.try_reserve(small_payload.len())?;
        }

        // 2) Rewind wp from its post-roll value back to the value it
        //    held *before* roll_over_region's publish_wp. With
        //    region_size=4096 and big payload=3816: the Skip sits at
        //    offset 4040 with skip_len=40 (total Skip record = 56 bytes,
        //    filling exactly to the region boundary). Pre-roll wp was
        //    4056 (set by the big message's publish_wp at the end of
        //    commit). Post-roll wp is 4112 (set by roll_over_region's
        //    publish_wp). The rewind is exactly the Skip record size,
        //    which is also the publish_wp delta inside roll_over_region.
        rewind_write_position_on_disk(base, 56)?;
        // A wrong count proves recovery recounts rather than trusting it: the
        // orphan is a Skip, which is not a user record, so the true count is 1.
        set_message_count_on_disk(base, 7)?;

        // 3) Reopen — must succeed (recovery follows the Skip into region 1).
        let mut w = WriterBuilder::new(base).region_size(region_size).build()?;
        assert_eq!(
            w.next_record_index(),
            1,
            "Skip orphan recounted, not counted"
        );

        // 4) Recovered writer writes a new message in region 1.
        let buf = w.try_reserve(small_payload.len())?;
        buf.copy_from_slice(&small_payload);
        w.commit(2, small_payload.len() as u32, 0)?;
        assert_eq!(w.next_record_index(), 2);
        drop(w);

        // 5) Reader: big message, then the post-recovery small message.
        let mut r = Reader::open(base, ReaderMode::LateJoin)?;
        let m0 = r.try_read()?.expect("big message should be visible");
        assert_eq!(m0.payload(), &big[..]);
        let m1 = r.try_read()?.expect("small message should be visible");
        assert_eq!(m1.payload(), &small_payload);
        assert_eq!(m1.header().message_type, 2);
        assert!(r.try_read()?.is_none());

        cleanup_channel_files(base);
        Ok(())
    }

    /// `base_record_index` accumulates across rolls so the absolute record
    /// index is monotonic and restart-stable, and Skip markers do not inflate
    /// it (only user records count).
    #[test]
    fn test_base_record_index_accumulates_across_rolls() -> anyhow::Result<()> {
        let base = "test_base_record_index_accumulates";
        cleanup_channel_files(base);

        let region_size = page_size();
        let file_roll_size = (region_size as u64) * 2; // small => many file rolls
        let payload = [0x5Au8; 64]; // small => many region rolls (Skips) per file
        let n = 200u64;

        {
            let mut w = WriterBuilder::new(base)
                .region_size(region_size)
                .file_roll_size(file_roll_size)
                .build()?;
            for i in 0..n {
                // Head == count of user records committed so far, regardless of
                // how many region/file rolls have happened in between.
                assert_eq!(w.next_record_index(), i, "head before commit #{i}");
                let buf = w.try_reserve(payload.len())?;
                buf.copy_from_slice(&payload);
                w.commit(0, payload.len() as u32, i)?;
            }
            // Exactly n — if Skip markers were counted, this would be larger.
            assert_eq!(w.next_record_index(), n);
        }

        // Head is cumulative and survives reopen (read from the latest file header).
        {
            let w = WriterBuilder::new(base)
                .region_size(region_size)
                .file_roll_size(file_roll_size)
                .build()?;
            assert_eq!(
                w.next_record_index(),
                n,
                "head survives reopen across rolls"
            );
        }

        // Reader: every record's payload index is contiguous from 0..n, and the
        // current file's base advances past 0 once we cross into a rolled file.
        let mut r = ReaderBuilder::new(base).build()?; // LateJoin from earliest
        assert_eq!(
            r.base_record_index(),
            0,
            "earliest (genesis) file base is 0"
        );
        let mut count = 0u64;
        let mut max_base = 0u64;
        while let Some(m) = r.try_read()? {
            assert_eq!(m.header().user_meta_u64, count, "records contiguous");
            max_base = max_base.max(r.base_record_index());
            count += 1;
        }
        assert_eq!(count, n);
        assert!(
            max_base > 0,
            "expected a roll so a later file reports base > 0"
        );

        cleanup_channel_files(base);
        Ok(())
    }

    /// A segment file whose `channel_sequence` disagrees with the sequence in its
    /// path (renamed/misplaced/swapped) is refused on open, by both Writer and Reader.
    #[test]
    fn test_rejects_segment_with_wrong_sequence() -> anyhow::Result<()> {
        let base = "test_rejects_wrong_sequence";
        cleanup_channel_files(base);
        let region_size = page_size();

        // One segment at sequence 0 (its header records channel_sequence = 0).
        {
            let mut w = WriterBuilder::new(base).region_size(region_size).build()?;
            let buf = w.try_reserve(8)?;
            buf.copy_from_slice(&[1u8; 8]);
            w.commit(0, 8, 0)?;
        }

        // Move that file to the sequence-1 path: now the only segment sits at
        // sequence 1 but its header still claims channel_sequence = 0.
        let p0 = make_channel_file_path(Path::new(base), 0)?;
        let p1 = make_channel_file_path(Path::new(base), 1)?;
        std::fs::copy(&p0, &p1)?;
        std::fs::remove_file(&p0)?;

        // Writer reopen (latest sequence = 1) rejects the mismatch.
        let werr = WriterBuilder::new(base)
            .region_size(region_size)
            .build()
            .err()
            .expect("writer must reject sequence-mismatched segment");
        assert_eq!(werr.kind(), ErrorKind::InvalidData);
        assert!(werr.to_string().contains("channel_sequence"));

        // Reader (earliest = latest = 1) rejects it too.
        let rerr = ReaderBuilder::new(base)
            .build()
            .err()
            .expect("reader must reject sequence-mismatched segment");
        assert_eq!(rerr.kind(), ErrorKind::InvalidData);
        assert!(rerr.to_string().contains("channel_sequence"));

        cleanup_channel_files(base);
        Ok(())
    }

    /// INV5 — multi-step crash refuses.
    ///
    /// If publish_wp lagged by more than one record, the slot we'd advance
    /// into would also be committed. That can't happen today (publish_wp
    /// is unconditional in every commit path) but the recovery code
    /// must still refuse cleanly if the assumption ever breaks.
    #[test]
    fn test_writer_refuses_multi_step_crash() -> anyhow::Result<()> {
        let base = "test_writer_refuses_multi_step_crash";
        cleanup_channel_files(base);

        let region_size = page_size();
        let payload: [u8; 8] = [0xAB; 8];

        // 1) Write two messages cleanly.
        {
            let mut w = WriterBuilder::new(base).region_size(region_size).build()?;
            for n in 0..2 {
                let buf = w.try_reserve(payload.len())?;
                buf.copy_from_slice(&payload);
                w.commit(n as u16, payload.len() as u32, 0)?;
            }
        }

        // 2) Rewind wp by two full records: simulates a writer that
        //    committed two messages without ever calling publish_wp.
        let record_size = (HEADER_SLOT + payload.len()).next_multiple_of(ALIGN) as u64;
        rewind_write_position_on_disk(base, 2 * record_size)?;

        // 3) Reopen must refuse — the advanced slot is committed=1, not
        //    a pre-installed header.
        let err = WriterBuilder::new(base)
            .region_size(region_size)
            .build()
            .err()
            .expect("writer must refuse multi-step crash state");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("multi-record publish_wp lag")
                || err.to_string().contains("not a pre-installed header"),
            "unexpected error: {err}"
        );

        // 4) Readers can still drain both committed messages.
        let mut r = Reader::open(base, ReaderMode::LateJoin)?;
        assert!(r.try_read()?.is_some());
        assert!(r.try_read()?.is_some());
        assert!(r.try_read()?.is_none());

        cleanup_channel_files(base);
        Ok(())
    }

    /// Fresh segment creation goes through a `<base>.partial` (seq 0)
    /// or `<base>.<N>.partial` (seq N>0) temp file. The temp file
    /// exists only between `OpenOptions::create_new` and the final
    /// `rename` — we exercise both pieces here:
    ///
    /// 1. A leftover `.partial` from a previous "crashed" run is
    ///    swept by `WriterBuilder::build` and doesn't block a fresh
    ///    create.
    /// 2. The reader's directory scan never sees a `.partial` file,
    ///    even if one is left lying around, so its sequence list
    ///    stays clean.
    #[test]
    fn test_partial_sweep_and_invisibility() -> anyhow::Result<()> {
        let base = "test_partial_sweep_and_invisibility";
        cleanup_channel_files(base);
        // Also clear the partial-named siblings explicitly so test
        // reruns from a previous failed run don't poison the state.
        let _ = std::fs::remove_file(format!("{base}.{}", PARTIAL_SUFFIX));
        let _ = std::fs::remove_file(format!("{base}.1.{}", PARTIAL_SUFFIX));

        // Synthesise crashed-mid-prep state: an orphan `.partial`
        // file at sequence 0 *and* one at sequence 1.
        std::fs::write(format!("{base}.{}", PARTIAL_SUFFIX), b"stale-bytes")?;
        std::fs::write(format!("{base}.1.{}", PARTIAL_SUFFIX), b"stale-bytes")?;

        // The reader's scan must already be tolerant of these even
        // before the writer runs — `.partial` shouldn't match
        // either the bare `<base>` (seq 0) name or `<base>.<N>`.
        let sequences = find_all_sequences(Path::new(base))?;
        assert!(
            sequences.is_empty(),
            "find_all_sequences leaked partial sibling: {sequences:?}"
        );

        // Build the writer — its startup sweep must unlink both
        // stale `.partial` files before `open_file`'s `create_new`
        // tries to take their place.
        let region_size = page_size();
        let mut w = WriterBuilder::new(base).region_size(region_size).build()?;
        assert!(
            !Path::new(&format!("{base}.{}", PARTIAL_SUFFIX)).exists(),
            "sweep_stale_partial_files did not unlink seq-0 orphan"
        );
        assert!(
            !Path::new(&format!("{base}.1.{}", PARTIAL_SUFFIX)).exists(),
            "sweep_stale_partial_files did not unlink seq-1 orphan"
        );

        // Sanity: writer is functional after the sweep.
        let payload = [0xCD_u8; 8];
        let buf = w.try_reserve(payload.len())?;
        buf.copy_from_slice(&payload);
        w.commit(7, payload.len() as u32, 0)?;
        drop(w);

        // And a reader can drain it.
        let mut r = Reader::open(base, ReaderMode::LateJoin)?;
        let msg = r.try_read()?.expect("first message visible");
        assert_eq!(msg.header().message_type, 7);
        assert!(r.try_read()?.is_none());

        cleanup_channel_files(base);
        Ok(())
    }

    /// `file_roll_size` within `region_size` of `u64::MAX` cannot
    /// be rounded up to a region boundary without overflowing.
    /// `WriterBuilder::build` must reject with `InvalidInput`
    /// rather than panicking (debug) or wrapping (release), AND
    /// must not leave a `.partial` orphan on disk — validation has
    /// to happen before `create_new` runs.
    #[test]
    fn test_preallocation_rejects_overflowing_roll_size() {
        let base = "test_preallocation_rejects_overflowing_roll_size";
        cleanup_channel_files(base);
        let err = WriterBuilder::new(base)
            .region_size(page_size())
            .file_roll_size(u64::MAX)
            .build()
            .err()
            .expect("must refuse u64::MAX roll size");
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert!(
            err.to_string().contains("overflowing u64"),
            "unexpected error: {err}",
        );
        let partial = format!("{base}.{}", PARTIAL_SUFFIX);
        assert!(
            !Path::new(&partial).exists(),
            "validation failure leaked a .partial orphan at {partial}",
        );
        cleanup_channel_files(base);
    }

    /// A roll size that rounds up past `i64::MAX` (the `off_t` ceiling
    /// `set_len` accepts) must be rejected up front with a clear error,
    /// not fail deep in the OS. `i64::MAX` is not region-aligned, so it
    /// rounds up to `2^63`, one past the limit.
    #[test]
    fn test_preallocation_rejects_roll_size_past_i64_max() {
        let base = "test_preallocation_rejects_roll_size_past_i64_max";
        cleanup_channel_files(base);
        let err = WriterBuilder::new(base)
            .region_size(page_size())
            .file_roll_size(i64::MAX as u64)
            .build()
            .err()
            .expect("must refuse a roll size past i64::MAX");
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert!(
            err.to_string().contains("i64::MAX"),
            "unexpected error: {err}",
        );
        let partial = format!("{base}.{}", PARTIAL_SUFFIX);
        assert!(!Path::new(&partial).exists());
        cleanup_channel_files(base);
    }

    /// A nonzero roll size below two regions is non-viable: region 0's
    /// head holds the channel header, so one region can't fit a
    /// full-size record. `build` must reject it up front.
    #[test]
    fn test_build_rejects_roll_size_below_two_regions() {
        let base = "test_build_rejects_roll_size_below_two_regions";
        cleanup_channel_files(base);
        let r = page_size();
        let err = WriterBuilder::new(base)
            .region_size(r)
            .file_roll_size(r as u64) // exactly one region — needs >= 2
            .build()
            .err()
            .expect("must refuse a sub-two-region roll size");
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert!(
            err.to_string().contains("two regions"),
            "unexpected error: {err}",
        );
        let partial = format!("{base}.{}", PARTIAL_SUFFIX);
        assert!(!Path::new(&partial).exists());
        cleanup_channel_files(base);
    }

    /// `file_roll_size` not a multiple of `region_size` (e.g. the
    /// README's 10_000_000) must round up to a whole region, since
    /// readers' mmaps always cover whole regions and would
    /// otherwise extend past EOF.
    #[test]
    fn test_preallocation_rounds_up_to_region_boundary() -> anyhow::Result<()> {
        let base = "test_preallocation_rounds_up_to_region_boundary";
        cleanup_channel_files(base);

        let region_size = page_size();
        let file_roll_size: u64 = 10_000_000;
        let expected = file_roll_size.div_ceil(region_size as u64) * region_size as u64;
        assert_ne!(
            expected, file_roll_size,
            "test premise: roll size unaligned"
        );

        let _w = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size(file_roll_size)
            .build()?;
        assert_eq!(std::fs::metadata(base)?.len(), expected);

        cleanup_channel_files(base);
        Ok(())
    }

    /// `Writer.file_len` must reflect the preallocation. Otherwise
    /// `ensure_len(want)` (called from `roll_over_region`) compares
    /// `want` against a stale `region_size` and `set_len` *shrinks*
    /// the preallocated file. We cross a region boundary by hand
    /// and assert the file size is unchanged.
    #[test]
    fn test_writer_does_not_shrink_preallocation_on_region_roll() -> anyhow::Result<()> {
        let base = "test_writer_does_not_shrink_preallocation_on_region_roll";
        cleanup_channel_files(base);

        let region_size = page_size();
        let file_roll_size = (region_size as u64) * 4;
        let initial_size = std::fs::metadata({
            let _w = WriterBuilder::new(base)
                .region_size(region_size)
                .file_roll_size(file_roll_size)
                .build()?;
            base
        })?
        .len();
        assert_eq!(initial_size, file_roll_size);

        // Reopen and write until at least one intra-file region roll
        // happens. Payload sized to need a fresh region per write
        // (>= half a region).
        let mut w = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size(file_roll_size)
            .build()?;
        let payload_size = region_size / 2 + 64;
        let payload = vec![0xAB_u8; payload_size];
        // 4 regions in this file → 3 region-rolls before a file roll.
        for n in 0..3 {
            let buf = w.try_reserve(payload.len())?;
            buf.copy_from_slice(&payload);
            w.commit(n as u16, payload.len() as u32, 0)?;
        }
        drop(w);

        let after_size = std::fs::metadata(base)?.len();
        assert_eq!(
            after_size, file_roll_size,
            "preallocation was undone: file shrank from {file_roll_size} to {after_size}",
        );

        cleanup_channel_files(base);
        Ok(())
    }

    /// A v3.0.0-format file (created at `region_size`, not
    /// preallocated) reopened by a 3.0.1+ writer must be promoted
    /// to the full preallocated layout. Otherwise the migrated
    /// channel keeps growing region-by-region under live readers.
    #[test]
    fn test_existing_file_reopen_promotes_to_preallocation() -> anyhow::Result<()> {
        let base = "test_existing_file_reopen_promotes_to_preallocation";
        cleanup_channel_files(base);

        let region_size = page_size();
        let file_roll_size = (region_size as u64) * 4;

        // Simulate a v3.0.0-created file: drop a writer with
        // file_roll_size = 0 so it preallocates only one region.
        {
            let _w = WriterBuilder::new(base).region_size(region_size).build()?;
        }
        assert_eq!(std::fs::metadata(base)?.len(), region_size as u64);

        // Reopen with full preallocation configured.
        let _w = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size(file_roll_size)
            .build()?;
        assert_eq!(std::fs::metadata(base)?.len(), file_roll_size);

        cleanup_channel_files(base);
        Ok(())
    }

    /// The partial sweep must parse the middle component as `u64`
    /// — unrelated siblings like `<base>.notes.partial` must
    /// survive a `WriterBuilder::build`.
    #[test]
    fn test_partial_sweep_does_not_match_non_numeric() -> anyhow::Result<()> {
        let base = "test_partial_sweep_does_not_match_non_numeric";
        cleanup_channel_files(base);
        let unrelated = format!("{base}.notes.partial");
        let _ = std::fs::remove_file(&unrelated);

        std::fs::write(&unrelated, b"user notes - keep me")?;
        let _w = WriterBuilder::new(base)
            .region_size(page_size())
            .file_roll_size((page_size() as u64) * 2)
            .build()?;
        assert!(
            Path::new(&unrelated).exists(),
            "sweep destroyed a non-segment sibling",
        );

        std::fs::remove_file(&unrelated)?;
        cleanup_channel_files(base);
        Ok(())
    }

    /// `cleanup_channel_files` must also remove crate-created
    /// `.partial` siblings, otherwise the public "fresh start"
    /// recipe leaks artifacts.
    #[test]
    fn test_cleanup_removes_partial_siblings() -> anyhow::Result<()> {
        let base = "test_cleanup_removes_partial_siblings";
        cleanup_channel_files(base);

        std::fs::write(format!("{base}.{}", PARTIAL_SUFFIX), b"seq0")?;
        std::fs::write(format!("{base}.1.{}", PARTIAL_SUFFIX), b"seq1")?;
        std::fs::write(format!("{base}.2.{}", PARTIAL_SUFFIX), b"seq2")?;
        std::fs::write(format!("{base}.keep.partial"), b"unrelated")?;

        cleanup_channel_files(base);

        assert!(!Path::new(&format!("{base}.{}", PARTIAL_SUFFIX)).exists());
        assert!(!Path::new(&format!("{base}.1.{}", PARTIAL_SUFFIX)).exists());
        assert!(!Path::new(&format!("{base}.2.{}", PARTIAL_SUFFIX)).exists());
        assert!(
            Path::new(&format!("{base}.keep.partial")).exists(),
            "cleanup destroyed an unrelated sibling",
        );

        std::fs::remove_file(format!("{base}.keep.partial"))?;
        Ok(())
    }

    /// `WriterBuilder::build` with a non-zero `file_roll_size`
    /// preallocates the first segment to that full size (rather
    /// than just one region). Eliminates the intra-file mmap-vs-
    /// `set_len` race in `roll_over_region` — by the time the
    /// writer crosses a region boundary, the file already has all
    /// the backing pages it'll ever need within this segment.
    ///
    /// With `file_roll_size = 0` (unbounded growth), preallocation
    /// has no upper bound to target, so the file is born at one
    /// region size and grown on demand via `ensure_len`.
    #[test]
    fn test_fresh_file_preallocates_to_file_roll_size() -> anyhow::Result<()> {
        let base = "test_fresh_file_preallocates_to_file_roll_size";
        cleanup_channel_files(base);

        let region_size = page_size();
        let file_roll_size = (region_size as u64) * 8;

        let _w = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size(file_roll_size)
            .build()?;
        let actual = std::fs::metadata(base)?.len();
        assert_eq!(
            actual, file_roll_size,
            "preallocation: fresh file size mismatch"
        );

        cleanup_channel_files(base);

        // `file_roll_size = 0` ⇒ no upper bound to preallocate to;
        // fall back to one region.
        let _w = WriterBuilder::new(base).region_size(region_size).build()?;
        let actual = std::fs::metadata(base)?.len();
        assert_eq!(
            actual, region_size as u64,
            "no-roll case should fall back to one region"
        );

        cleanup_channel_files(base);
        Ok(())
    }

    /// `try_reserve` rejects a `msg_size` that cannot ever fit:
    /// record + header + padding + next-header must be at most
    /// `region_size`, and at most `file_roll_size` when rolling is
    /// configured. Without the upfront check, the writer would roll
    /// regions or files indefinitely, creating unbounded segment
    /// files.
    #[test]
    fn test_try_reserve_rejects_oversized_payload() -> anyhow::Result<()> {
        let base = "test_try_reserve_rejects_oversized_payload";
        cleanup_channel_files(base);

        // Region case: msg_size larger than region_size.
        let region_size = page_size();
        let mut w = WriterBuilder::new(base).region_size(region_size).build()?;
        let err = w
            .try_reserve(region_size * 2)
            .expect_err("oversized reservation must error");
        assert!(
            err.to_string().contains("cannot fit in region_size"),
            "unexpected error: {err}",
        );
        drop(w);
        cleanup_channel_files(base);

        // The file-roll capacity path (needed_total > file_roll_size) is
        // unreachable for valid configs: file_roll_size >= 2 * region_size,
        // so the region-size cap above always fires first. Sub-two-region
        // roll sizes are rejected at build (see
        // test_build_rejects_roll_size_below_two_regions).

        Ok(())
    }

    /// `commit(length)` may be ≤ the size passed to `try_reserve`
    /// (the worst-case-reserve / serialize-then-commit pattern).
    /// `length > reserved` is rejected because the user would have
    /// written past the buffer; bare `commit` with no preceding
    /// reserve is also an error.
    #[test]
    fn test_commit_length_contract() -> anyhow::Result<()> {
        let base = "test_commit_length_contract";
        cleanup_channel_files(base);

        let mut w = WriterBuilder::new(base).region_size(page_size()).build()?;

        // length > reserved: error, pending_msg_size cleared.
        let _buf = w.try_reserve(64)?;
        let err = w
            .commit(0, 128, 0)
            .expect_err("commit length > reserved must error");
        assert!(
            err.to_string()
                .contains("commit length 128 exceeds try_reserve size 64"),
            "unexpected error: {err}",
        );

        // commit without a preceding reserve errors.
        let err = w
            .commit(0, 0, 0)
            .expect_err("commit with no preceding reserve must error");
        assert!(
            err.to_string()
                .contains("commit without preceding try_reserve"),
            "unexpected error: {err}",
        );

        // length < reserved: succeeds. Worst-case-reserve pattern —
        // write into the reserved buffer, commit the smaller actual
        // size. Pre-install is re-laid at the actual next slot.
        let buf = w.try_reserve(128)?;
        buf[..16].copy_from_slice(&[0xAA_u8; 16]);
        w.commit(11, 16, 0)?;

        // length == reserved: succeeds (the common case).
        let buf = w.try_reserve(8)?;
        buf.copy_from_slice(&[0xBB_u8; 8]);
        w.commit(22, 8, 0)?;
        drop(w);

        // Reader sees both records with their committed lengths.
        let mut r = Reader::open(base, ReaderMode::LateJoin)?;
        let m = r.try_read()?.expect("first record");
        assert_eq!(m.header().message_type, 11);
        assert_eq!(m.header().length, 16);
        let m = r.try_read()?.expect("second record");
        assert_eq!(m.header().message_type, 22);
        assert_eq!(m.header().length, 8);
        assert!(r.try_read()?.is_none());

        cleanup_channel_files(base);
        Ok(())
    }

    /// An abandoned reservation (try_reserve without commit) must
    /// not survive an explicit `roll_file()` — otherwise a follow-up
    /// `commit` would publish a zero-payload record at slot 0 of the
    /// NEW segment. Same hazard applies to internal region rolls.
    #[test]
    fn test_abandoned_reservation_does_not_survive_roll() -> anyhow::Result<()> {
        let base = "test_abandoned_reservation_does_not_survive_roll";
        cleanup_channel_files(base);

        let mut w = WriterBuilder::new(base)
            .region_size(page_size())
            .file_roll_size((page_size() as u64) * 4)
            .build()?;

        // Abandoned reservation in OLD segment.
        let _abandoned = w.try_reserve(8)?;

        // Explicit roll. Pending reservation refers to a slot in the
        // file we're leaving.
        w.roll_file()?;

        // commit with the same length the abandoned reserve used —
        // would have erroneously published a zero-payload record at
        // NEW segment's slot 0 without the invalidation fix.
        let err = w
            .commit(0, 8, 0)
            .expect_err("commit must require a fresh reserve after roll_file");
        assert!(
            err.to_string()
                .contains("commit without preceding try_reserve"),
            "unexpected error: {err}",
        );

        // Sanity: a fresh reserve+commit on NEW segment still works.
        let buf = w.try_reserve(8)?;
        buf.copy_from_slice(&[0xCD_u8; 8]);
        w.commit(9, 8, 0)?;
        drop(w);

        cleanup_channel_files(base);
        Ok(())
    }

    /// Crash-recovery invariant after the Route-C refactor:
    /// `try_reserve` pre-installs slot i+1 *before* `commit` flips
    /// committed=1, so a crash between `commit(i)` and the publish_wp
    /// that follows still leaves the channel reopenable. Simulate by
    /// dropping a writer mid-stream — the next `WriterBuilder::build`
    /// must succeed.
    #[test]
    fn test_writer_reopens_after_commit_without_publish_wp() -> anyhow::Result<()> {
        let base = "test_writer_reopens_after_commit_without_publish_wp";
        cleanup_channel_files(base);

        let region_size = page_size();
        let payload: [u8; 16] = [0xCD; 16];

        // Write a couple of records cleanly so there's prior state.
        {
            let mut w = WriterBuilder::new(base).region_size(region_size).build()?;
            for n in 0..2 {
                let buf = w.try_reserve(payload.len())?;
                buf.copy_from_slice(&payload);
                w.commit(n as u16, payload.len() as u32, 0)?;
            }
        }

        // Synthesise the post-commit-pre-publish_wp state: rewind the
        // on-disk wp by one record. With the Route-C pre-install in
        // `try_reserve`, the slot the recovery code lands on must
        // already bear the pre-install signature.
        let record_size = (HEADER_SLOT + payload.len()).next_multiple_of(ALIGN) as u64;
        rewind_write_position_on_disk(base, record_size)?;

        // Reopen must succeed and recover by advancing one record.
        let _w = WriterBuilder::new(base).region_size(region_size).build()?;

        // Two records still readable.
        let mut r = Reader::open(base, ReaderMode::LateJoin)?;
        assert!(r.try_read()?.is_some());
        assert!(r.try_read()?.is_some());
        assert!(r.try_read()?.is_none());

        cleanup_channel_files(base);
        Ok(())
    }

    /// Hammer test: a writer rolling segments aggressively (tiny
    /// `file_roll_size`, `keep_files=2`) plus a late-joining reader
    /// walking the channel from segment 0 forward. The `.partial` +
    /// rename design means the reader's directory scan never
    /// surfaces a partially-initialised file. The assertion floor
    /// is operational: many rolls happen, the reader sees a strict
    /// prefix of writes, no `try_read` returns an `Err`, and the
    /// test process does not abort (i.e. no SIGBUS).
    #[test]
    fn test_concurrent_rolls_and_latejoin_reader() -> anyhow::Result<()> {
        use std::thread;
        use std::time::Duration;

        let base = "test_concurrent_rolls_and_latejoin_reader";
        cleanup_channel_files(base);

        let region_size = page_size();
        // Force many rolls: smallest valid roll size (two regions).
        let file_roll_size = region_size as u64 * 2;
        let n_writes: u16 = 200;

        let writer_base = base.to_string();
        let writer_thread = thread::spawn(move || -> anyhow::Result<()> {
            let mut w = WriterBuilder::new(&writer_base)
                .region_size(region_size)
                .file_roll_size(file_roll_size)
                .keep_files(2)
                .build()?;
            for n in 0..n_writes {
                let buf = w.try_reserve(16)?;
                buf.copy_from_slice(&[n as u8; 16]);
                w.commit(n, 16, 0)?;
                if n.is_multiple_of(7) {
                    thread::sleep(Duration::from_micros(50));
                }
            }
            Ok(())
        });

        // Reader joins late-ish and is allowed to lag. Retention can outrun an open's retries
        // under load; that is not what this test is about.
        thread::sleep(Duration::from_millis(20));
        let open_by = std::time::Instant::now() + Duration::from_secs(5);
        let mut r = loop {
            match Reader::open(base, ReaderMode::LateJoin) {
                Err(e)
                    if e.kind() == ErrorKind::NotFound && std::time::Instant::now() < open_by => {}
                opened => break opened?,
            }
        };
        let mut seen: u32 = 0;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            match r.try_read() {
                Ok(Some(_msg)) => {
                    seen += 1;
                }
                Ok(None) => {
                    if writer_thread.is_finished() {
                        // Drain any remaining buffered messages once
                        // more before declaring done.
                        while let Ok(Some(_)) = r.try_read() {
                            seen += 1;
                        }
                        break;
                    }
                    thread::sleep(Duration::from_millis(2));
                }
                Err(e) => {
                    panic!("reader try_read returned an error: {e:?}");
                }
            }
        }
        writer_thread.join().expect("writer thread")?;
        assert!(
            seen > 0,
            "reader should observe at least some of the writes (saw {seen})"
        );
        cleanup_channel_files(base);
        Ok(())
    }

    fn types(msgs: &[OwnedMessage]) -> Vec<u16> {
        msgs.iter().map(|m| m.header().message_type).collect()
    }

    fn write_msgs(base: &str, region_size: usize, ids: &[u16], fill: u8) -> anyhow::Result<()> {
        let mut writer = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size((region_size as u64) * 8)
            .mtu(0)
            .build()?;
        for (i, id) in ids.iter().enumerate() {
            let buf = writer.try_reserve(64)?;
            buf.fill(fill.wrapping_add(i as u8));
            writer.commit(*id, 64, i as u64)?;
        }
        Ok(())
    }

    /// The whole point of the owned flavour: the region stays mapped after the
    /// Reader that produced the message is gone.
    #[test]
    fn owned_message_outlives_its_reader() -> anyhow::Result<()> {
        let base = "test_owned_outlives_reader";
        cleanup_channel_files(base);
        let region_size = crate::page_size();
        write_msgs(base, region_size, &[7], 0xA1)?;

        let msg = {
            let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
            reader
                .try_read_owned()?
                .expect("one committed user record is available")
        };
        // Reader dropped here; the mapping must survive with it.

        assert_eq!(msg.len(), 64);
        assert_eq!(msg.header().message_type, 7);
        assert!(msg.payload().iter().all(|&b| b == 0xA1));
        assert_eq!(msg.as_ref().payload(), msg.payload());

        cleanup_channel_files(base);
        Ok(())
    }

    /// Prune and region switches drop the Reader's share only.
    #[test]
    fn owned_message_survives_prune_and_region_switch() -> anyhow::Result<()> {
        let base = "test_owned_survives_prune";
        cleanup_channel_files(base);
        let region_size = crate::page_size();
        // Enough 64-byte records to spill well past the first region. Counted in
        // usize: `region_size as u16` truncates to 0 on a 64 KiB-page host.
        let ids: Vec<u16> = (0..(region_size / 64) * 3).map(|i| i as u16).collect();
        write_msgs(base, region_size, &ids, 0xB2)?;

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        let first = reader.try_read_owned()?.expect("first record");
        let first_payload: Vec<u8> = first.payload().to_vec();

        // Drain the rest, forcing switch_region + prune_to_current repeatedly.
        let mut drained = 0usize;
        while reader.try_read_owned()?.is_some() {
            drained += 1;
        }
        assert!(
            drained >= ids.len() - 1,
            "expected to drain the remaining records, got {drained} of {}",
            ids.len() - 1
        );

        // The first message still points at intact bytes.
        assert_eq!(first.payload(), first_payload.as_slice());
        assert!(first.payload().iter().all(|&b| b == 0xB2));

        cleanup_channel_files(base);
        Ok(())
    }

    /// A short count means caught up, not finished: a later drain sees more once
    /// the writer appends.
    #[test]
    fn read_owned_into_resumes_after_more_writes() -> anyhow::Result<()> {
        let base = "test_owned_drain_resumes";
        cleanup_channel_files(base);
        let region_size = crate::page_size();
        let file_roll_size = (region_size as u64) * 8;

        let mut writer = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size(file_roll_size)
            .mtu(0)
            .build()?;
        let publish = |w: &mut Writer, id: u16| -> anyhow::Result<()> {
            let buf = w.try_reserve(64)?;
            buf.fill(id as u8);
            w.commit(id, 64, id as u64)?;
            Ok(())
        };

        publish(&mut writer, 1)?;
        publish(&mut writer, 2)?;

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        let mut buf: Vec<OwnedMessage> = Vec::new();

        // Unbounded: drains until caught up.
        assert_eq!(reader.read_owned_into(&mut buf, None)?, 2);
        assert_eq!(types(&buf), vec![1, 2], "first pass drains what was there");
        buf.clear();

        // Caught up, not finished — an unbounded pass still returns.
        assert_eq!(reader.read_owned_into(&mut buf, None)?, 0);
        assert!(buf.is_empty());

        publish(&mut writer, 3)?;
        assert_eq!(reader.read_owned_into(&mut buf, None)?, 1);
        assert_eq!(types(&buf), vec![3], "draining resumes after a zero count");

        cleanup_channel_files(base);
        Ok(())
    }

    /// `max` bounds the pass exactly, and appends rather than clearing.
    #[test]
    fn read_owned_into_bounds_a_pass_and_appends() -> anyhow::Result<()> {
        let base = "test_owned_drain_max";
        cleanup_channel_files(base);
        let region_size = crate::page_size();
        write_msgs(base, region_size, &[10, 11, 12, 13], 0xC3)?;

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        let mut buf: Vec<OwnedMessage> = Vec::new();

        assert_eq!(reader.read_owned_into(&mut buf, Some(2))?, 2);
        assert_eq!(types(&buf), vec![10, 11]);

        // Appends to the same buffer; the cursor advanced by exactly two.
        assert_eq!(reader.read_owned_into(&mut buf, Some(2))?, 2);
        assert_eq!(types(&buf), vec![10, 11, 12, 13]);

        assert_eq!(
            reader.read_owned_into(&mut buf, Some(0))?,
            0,
            "Some(0) is a no-op"
        );
        assert_eq!(types(&buf), vec![10, 11, 12, 13]);
        assert_eq!(
            reader.read_owned_into(&mut buf, None)?,
            0,
            "and there is nothing left for an unbounded pass"
        );

        cleanup_channel_files(base);
        Ok(())
    }

    /// The reason the drain exists: adapters that buffer or discard are sound
    /// over an owned buffer. Peeking here cannot lose a message, because the
    /// peeked one is already ours — the pre-batch iterator lost it on drop.
    #[test]
    fn draining_buffer_survives_peekable() -> anyhow::Result<()> {
        let base = "test_owned_drain_peekable";
        cleanup_channel_files(base);
        let region_size = crate::page_size();
        write_msgs(base, region_size, &[21, 22, 23], 0xD4)?;

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        let mut buf = reader.owned_batch(None)?;
        assert_eq!(types(&buf), vec![21, 22, 23]);

        // Peek, then abandon the adapter mid-pass. Nothing is lost: the records
        // are still in `buf`, because `drain` was never started.
        {
            let mut it = buf.iter().peekable();
            assert_eq!(it.peek().map(|m| m.header().message_type), Some(21));
        }
        assert_eq!(types(&buf), vec![21, 22, 23]);

        // And a real drain with a peek ahead reaches every message.
        let mut seen = Vec::new();
        let mut it = buf.drain(..).peekable();
        while let Some(msg) = it.next() {
            let next_type = it.peek().map(|m| m.header().message_type);
            seen.push((msg.header().message_type, next_type));
        }
        assert_eq!(seen, vec![(21, Some(22)), (22, Some(23)), (23, None)]);

        cleanup_channel_files(base);
        Ok(())
    }

    /// `owned_batch` is the allocating wrapper over the same drain.
    #[test]
    fn owned_batch_matches_read_owned_into() -> anyhow::Result<()> {
        let base = "test_owned_batch_wrapper";
        cleanup_channel_files(base);
        let region_size = crate::page_size();
        write_msgs(base, region_size, &[41, 42, 43, 44], 0xF6)?;

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        assert_eq!(types(&reader.owned_batch(Some(2))?), vec![41, 42]);
        assert_eq!(types(&reader.owned_batch(None)?), vec![43, 44]);
        assert!(reader.owned_batch(None)?.is_empty());

        cleanup_channel_files(base);
        Ok(())
    }

    /// A borrowed batch can hand out owned messages for the few records the
    /// consumer wants to keep past the next poll.
    #[test]
    fn batch_get_owned_outlives_the_batch_and_reader() -> anyhow::Result<()> {
        let base = "test_batch_get_owned";
        cleanup_channel_files(base);
        let region_size = crate::page_size();
        write_msgs(base, region_size, &[51, 52, 53], 0x17)?;

        let kept = {
            let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
            let batch = reader.try_read_batch(None)?.expect("three records");
            assert_eq!(batch.len(), 3);
            assert!(batch.get_owned(3).is_none(), "out of range");
            let kept = batch.get_owned(1).expect("second record");
            assert_eq!(kept.payload(), batch.get(1).expect("second").payload());
            kept
        };
        // Batch and Reader both gone; the retained share keeps the region mapped.
        assert_eq!(kept.header().message_type, 52);
        assert_eq!(kept.len(), 64);
        assert!(kept.payload().iter().all(|&b| b == 0x17u8.wrapping_add(1)));

        cleanup_channel_files(base);
        Ok(())
    }

    /// Borrowed and owned readers must agree, and interleave on one cursor.
    #[test]
    fn borrowed_and_owned_reads_share_one_cursor() -> anyhow::Result<()> {
        let base = "test_owned_borrowed_interleave";
        cleanup_channel_files(base);
        let region_size = crate::page_size();
        write_msgs(base, region_size, &[21, 22, 23], 0xD4)?;

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        let a = reader.try_read()?.expect("first").header().message_type;
        let b = reader
            .try_read_owned()?
            .expect("second")
            .header()
            .message_type;
        let c = reader.try_read()?.expect("third").header().message_type;
        assert_eq!((a, b, c), (21, 22, 23));
        assert!(reader.try_read()?.is_none());

        cleanup_channel_files(base);
        Ok(())
    }

    /// `Send` and `Sync` here are auto-trait properties, not declarations: they
    /// hold only because the mapping behind the `Arc` is itself `Send + Sync`.
    /// Swapping `Arc` for `Rc` would strip them from `Reader` too, and nothing
    /// else in the suite would notice, so pin them.
    #[test]
    fn owned_message_and_reader_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<OwnedMessage>();
        assert_send_sync::<Reader>();
        assert_send_sync::<MessageRef<'static>>();
    }

    /// The property `Send` is there for: hand a message to another thread and
    /// read it there, after the reader that produced it is already gone.
    #[test]
    fn owned_message_can_be_read_on_another_thread() -> anyhow::Result<()> {
        let base = "test_owned_crosses_thread";
        cleanup_channel_files(base);
        let region_size = crate::page_size();
        write_msgs(base, region_size, &[31], 0xE5)?;

        let msg = {
            let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
            reader.try_read_owned()?.expect("one record")
        };

        let (len, first, msg_type) =
            std::thread::spawn(move || (msg.len(), msg.payload()[0], msg.header().message_type))
                .join()
                .expect("worker panicked");

        assert_eq!((len, first, msg_type), (64, 0xE5, 31));

        cleanup_channel_files(base);
        Ok(())
    }

    // ---------- position / start_at / seek ----------

    /// Small geometry so a few hundred records span several regions and files.
    fn rolling_writer(base: &str) -> io::Result<WriterBuilder> {
        let region_size = page_size();
        Ok(WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size(region_size as u64 * 2))
    }

    /// Commit records `range`, each carrying its own absolute index as payload and user meta.
    fn write_indexed(w: &mut Writer, range: std::ops::Range<u64>) -> io::Result<()> {
        for i in range {
            let buf = w.try_reserve(8)?;
            buf.copy_from_slice(&i.to_le_bytes());
            w.commit(0, 8, i)?;
        }
        Ok(())
    }

    /// `position()` names the next record on every read path, across region and file rolls,
    /// and a batch that crosses a roll refreshes `base_record_index()` like a single read does.
    #[test]
    fn position_tracks_every_read_path_across_rolls() -> anyhow::Result<()> {
        let base = "test_position_tracks_every_read_path";
        cleanup_channel_files(base);
        let n = 1000u64;
        let mut w = rolling_writer(base)?.build()?;
        write_indexed(&mut w, 0..n)?;
        assert!(w.file_sequence > 1, "expected several rolls");

        let mut r = ReaderBuilder::new(base).build()?;
        assert_eq!(r.position(), 0);
        let mut step = 0u64;
        while r.position() < n {
            let before = r.position();
            assert_eq!(r.peek_header()?.expect("record").user_meta_u64, before);
            assert_eq!(r.position(), before, "peek does not consume");
            match step % 3 {
                0 => {
                    let m = r.try_read()?.expect("record");
                    assert_eq!(m.header().user_meta_u64, before);
                }
                1 => {
                    let m = r.try_read_owned()?.expect("record");
                    assert_eq!(m.header().user_meta_u64, before);
                }
                _ => {
                    let batch = r.try_read_batch(Some(37))?.expect("records");
                    for (k, m) in batch.iter().enumerate() {
                        assert_eq!(m.header().user_meta_u64, before + k as u64);
                    }
                    let len = batch.len() as u64;
                    assert_eq!(r.position(), before + len);
                    assert!(r.base_record_index() <= r.position());
                }
            }
            step += 1;
        }
        assert_eq!(r.position(), n);
        assert_eq!(r.position(), r.head_record_index()?);
        assert!(r.try_read()?.is_none());
        assert_eq!(r.file_sequence(), w.file_sequence);
        assert!(r.base_record_index() > 0);

        cleanup_channel_files(base);
        Ok(())
    }

    /// Opening at any index from 0 through the head lands exactly there.
    #[test]
    fn start_at_every_index() -> anyhow::Result<()> {
        let base = "test_start_at_every_index";
        cleanup_channel_files(base);
        let n = 1000u64;
        let mut w = rolling_writer(base)?.build()?;
        write_indexed(&mut w, 0..n)?;

        for i in 0..=n {
            let mut r = ReaderBuilder::new(base).start_at(i).build()?;
            assert_eq!(r.position(), i);
            for k in i..(i + 3).min(n) {
                let m = r.try_read()?.expect("record");
                assert_eq!(m.header().user_meta_u64, k, "start_at({i})");
                assert_eq!(m.payload(), &k.to_le_bytes());
            }
            if i == n {
                assert!(r.try_read()?.is_none(), "start_at(head) waits");
            }
        }

        // At the head, the reader picks up what is written next.
        let mut r = ReaderBuilder::new(base).start_at(n).build()?;
        write_indexed(&mut w, n..n + 1)?;
        assert_eq!(r.try_read()?.expect("new record").header().user_meta_u64, n);
        assert_eq!(r.position(), n + 1);

        cleanup_channel_files(base);
        Ok(())
    }

    /// Pruned indices are `NotFound`, indices past the head are `InvalidInput`, and the
    /// boundaries themselves open.
    #[test]
    fn start_at_out_of_range() -> anyhow::Result<()> {
        let base = "test_start_at_out_of_range";
        cleanup_channel_files(base);
        let n = 2000u64;
        let mut w = rolling_writer(base)?.keep_files(2).build()?;
        write_indexed(&mut w, 0..n)?;

        let r = ReaderBuilder::new(base).build()?;
        let tail = r.tail_record_index()?;
        assert!(tail > 0, "retention should have pruned the start");
        assert_eq!(tail, r.position(), "LateJoin starts at the tail");
        assert_eq!(r.head_record_index()?, n);

        let err = ReaderBuilder::new(base)
            .start_at(tail - 1)
            .build()
            .err()
            .expect("pruned");
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(
            IndexPruned::of(&err),
            Some(IndexPruned {
                index: tail - 1,
                earliest: tail
            })
        );
        let missing = ReaderBuilder::new("test_start_at_no_such_channel")
            .start_at(0)
            .build()
            .err()
            .expect("no channel");
        assert_eq!(missing.kind(), ErrorKind::NotFound);
        assert_eq!(
            IndexPruned::of(&missing),
            None,
            "a missing channel is not a pruned index"
        );
        let err = ReaderBuilder::new(base).start_at(n + 1).build().err();
        assert_eq!(err.map(|e| e.kind()), Some(ErrorKind::InvalidInput));

        let mut r = ReaderBuilder::new(base).start_at(tail).build()?;
        assert_eq!(r.try_read()?.expect("tail").header().user_meta_u64, tail);
        let mut r = ReaderBuilder::new(base).start_at(n).build()?;
        assert!(r.try_read()?.is_none());

        cleanup_channel_files(base);
        Ok(())
    }

    /// `seek` moves both ways; `rewind` and `seek_to_head` match fresh LateJoin / Live readers;
    /// a refused seek leaves the reader where it was.
    #[test]
    fn seek_rewind_and_seek_to_head() -> anyhow::Result<()> {
        let base = "test_seek_rewind_head";
        cleanup_channel_files(base);
        let n = 1000u64;
        let mut w = rolling_writer(base)?.build()?;
        write_indexed(&mut w, 0..n)?;

        let mut r = ReaderBuilder::new(base).batch_limit(5).build()?;
        for target in [700, 3, 999, 0, 512] {
            r.seek(target)?;
            assert_eq!(r.position(), target);
            assert_eq!(
                r.try_read()?.expect("record").header().user_meta_u64,
                target
            );
        }
        // The builder's batch limit survives a seek.
        assert_eq!(r.try_read_batch(None)?.expect("batch").len(), 5);
        assert_eq!(r.position(), 518);

        let err = r.seek(n + 5).expect_err("past head");
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(r.position(), 518, "refused seek leaves the reader alone");
        assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, 518);

        r.rewind()?;
        assert_eq!(r.position(), 0);
        assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, 0);

        r.seek_to_head()?;
        assert_eq!(r.position(), n);
        assert!(r.try_read()?.is_none());
        write_indexed(&mut w, n..n + 1)?;
        assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, n);

        cleanup_channel_files(base);
        Ok(())
    }

    /// A resumed cursor is refused against a recreated channel, before the index is judged.
    #[test]
    fn expect_generation_guards_the_cursor() -> anyhow::Result<()> {
        let base = "test_expect_generation";
        cleanup_channel_files(base);
        let mut w = rolling_writer(base)?.generation(7).build()?;
        write_indexed(&mut w, 0..10)?;

        let mut r = ReaderBuilder::new(base)
            .expect_generation(7)
            .start_at(4)
            .build()?;
        assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, 4);

        for mode in [
            ReaderMode::LateJoin,
            ReaderMode::Live,
            ReaderMode::At(1_000_000),
        ] {
            let err = ReaderBuilder::new(base)
                .mode(mode)
                .expect_generation(8)
                .build()
                .err()
                .expect("wrong generation");
            assert_eq!(
                GenerationMismatch::of(&err),
                Some(GenerationMismatch {
                    expected: 8,
                    found: 7
                }),
                "{mode:?}"
            );
        }

        // The path is deleted and recreated under the reader: seeking is refused.
        drop(w);
        cleanup_channel_files(base);
        let mut w = rolling_writer(base)?.generation(9).build()?;
        write_indexed(&mut w, 0..10)?;
        let err = r.seek(0).expect_err("different channel now");
        assert_eq!(
            GenerationMismatch::of(&err),
            Some(GenerationMismatch {
                expected: 7,
                found: 9
            })
        );
        let err = r.tail_record_index().expect_err("different channel now");
        assert_eq!(
            GenerationMismatch::of(&err),
            Some(GenerationMismatch {
                expected: 7,
                found: 9
            })
        );

        cleanup_channel_files(base);
        Ok(())
    }

    /// A Live reader knows the index of the record it starts at.
    #[test]
    fn live_open_knows_its_position() -> anyhow::Result<()> {
        let base = "test_live_open_position";
        cleanup_channel_files(base);
        let n = 777u64;
        let mut w = rolling_writer(base)?.build()?;
        write_indexed(&mut w, 0..n)?;

        let mut r = ReaderBuilder::new(base).live().build()?;
        assert_eq!(r.position(), n);
        assert!(r.try_read()?.is_none());
        write_indexed(&mut w, n..n + 2)?;
        assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, n);
        assert_eq!(r.position(), n + 1);

        cleanup_channel_files(base);
        Ok(())
    }

    /// Overwrite `ChannelHeader.message_count` on disk (byte offset 16 + 8).
    fn set_message_count_on_disk(base: &str, count: u64) -> anyhow::Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = OpenOptions::new()
            .write(true)
            .open(make_channel_file_path(Path::new(base), 0)?)?;
        f.seek(SeekFrom::Start(MESSAGE_HEADER_SIZE as u64 + 8))?;
        f.write_all(&count.to_le_bytes())?;
        f.sync_all()?;
        Ok(())
    }

    const RECORD_8: u64 = (HEADER_SLOT + 8).next_multiple_of(ALIGN) as u64;

    /// A writer that died between commit and publish leaves the `write_position` slot committed
    /// for good. Whether or not the count was bumped before it died, a Live reader starts on that
    /// record and knows its index.
    #[test]
    fn live_open_past_a_dead_writer_publish() -> anyhow::Result<()> {
        let base = "test_live_open_dead_publish";
        for count_bumped in [false, true] {
            cleanup_channel_files(base);
            {
                let mut w = WriterBuilder::new(base).build()?;
                write_indexed(&mut w, 0..3)?;
            }
            rewind_write_position_on_disk(base, RECORD_8)?;
            set_message_count_on_disk(base, if count_bumped { 3 } else { 2 })?;

            let mut r = ReaderBuilder::new(base).live().build()?;
            assert_eq!(r.position(), 2, "count_bumped={count_bumped}");
            assert_eq!(r.try_read()?.expect("orphan").header().user_meta_u64, 2);
            assert_eq!(r.position(), 3);
        }
        cleanup_channel_files(base);
        Ok(())
    }

    /// Overwrite `bytes` at `offset` in segment `seq` of channel `base`.
    fn poke_on_disk(base: &str, seq: u64, offset: u64, bytes: &[u8]) -> anyhow::Result<()> {
        use std::os::unix::fs::FileExt;
        let f = OpenOptions::new()
            .write(true)
            .open(make_channel_file_path(Path::new(base), seq)?)?;
        f.write_all_at(bytes, offset)?;
        f.sync_all()?;
        Ok(())
    }

    /// Read the little-endian `u64` at `offset` in segment `seq` of channel `base`.
    fn peek_u64_on_disk(base: &str, seq: u64, offset: u64) -> anyhow::Result<u64> {
        use std::os::unix::fs::FileExt;
        let f = File::open(make_channel_file_path(Path::new(base), seq)?)?;
        let mut bytes = [0u8; 8];
        f.read_exact_at(&mut bytes, offset)?;
        Ok(u64::from_le_bytes(bytes))
    }

    /// File offsets of `ChannelHeader.write_position` / `message_count`, and of the first user
    /// record's header (right after the Channel record).
    const WP_AT: u64 = MESSAGE_HEADER_SIZE as u64;
    const COUNT_AT: u64 = MESSAGE_HEADER_SIZE as u64 + 8;
    const BASE_AT: u64 = MESSAGE_HEADER_SIZE as u64 + 16;
    const FIRST_RECORD_AT: u64 = FIRST_RECORD as u64;

    /// A recovery that fails partway writes nothing: the orphan is still at `write_position` and
    /// the count is untouched, so the next open recovers from scratch. (A recovery that dies after
    /// storing the count but before the position leaves the "count already bumped" state that
    /// `writer_recovery_counts_the_orphan` covers.)
    #[test]
    fn failed_recovery_leaves_the_header_untouched() -> anyhow::Result<()> {
        let base = "test_failed_recovery_untouched";
        cleanup_channel_files(base);
        {
            let mut w = WriterBuilder::new(base).build()?;
            write_indexed(&mut w, 0..3)?;
        }
        rewind_write_position_on_disk(base, RECORD_8)?;
        set_message_count_on_disk(base, 2)?;
        let wp = peek_u64_on_disk(base, 0, WP_AT)?;

        // Make the recount fail: record 0's committed flag becomes invalid.
        poke_on_disk(base, 0, FIRST_RECORD_AT, &[2])?;
        let err = WriterBuilder::new(base)
            .build()
            .err()
            .expect("recount fails");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(peek_u64_on_disk(base, 0, WP_AT)?, wp, "position untouched");
        assert_eq!(peek_u64_on_disk(base, 0, COUNT_AT)?, 2, "count untouched");

        // Repaired, the next open recovers as if the failed attempt never happened.
        poke_on_disk(base, 0, FIRST_RECORD_AT, &[1])?;
        let w = WriterBuilder::new(base).build()?;
        assert_eq!(w.next_record_index(), 3);
        assert_eq!(peek_u64_on_disk(base, 0, WP_AT)?, wp + RECORD_8);

        cleanup_channel_files(base);
        Ok(())
    }

    /// Segment 0 holds records 0..3 and has rolled; segment 1 holds 3..5. Returns the offset of
    /// segment 0's Roll marker.
    fn rolled_once(base: &str) -> anyhow::Result<u64> {
        cleanup_channel_files(base);
        let mut w = WriterBuilder::new(base).build()?;
        write_indexed(&mut w, 0..3)?;
        w.roll_file()?;
        write_indexed(&mut w, 3..5)?;
        // After a roll the writer leaves `write_position` one slot past the whole Roll.
        Ok(peek_u64_on_disk(base, 0, WP_AT)? - (ROLL_TOTAL + HEADER_SLOT) as u64)
    }

    /// Open segment 0 the way `Live` would after a directory listing that still showed it as
    /// the newest segment.
    fn live_open_segment_0(base: &str) -> io::Result<Reader> {
        let file = File::open(make_channel_file_path(Path::new(base), 0)?)?;
        Reader::from_segment(PathBuf::from(base), 0, file, SegmentStart::Head)
    }

    /// Segment 0 is no longer the tail, whatever state its roll is in: an open of it must say
    /// so (never start past the Roll, never touch bytes there), and a real `Live` open joins
    /// the newest segment at `expected` instead.
    fn assert_stale_then_joins_tail(base: &str, expected: u64) -> anyhow::Result<()> {
        let err = live_open_segment_0(base).err().expect("segment 0 is stale");
        assert!(StaleSegment::is(&err), "{err}");
        let mut r = ReaderBuilder::new(base).live().build()?;
        assert_eq!(r.position(), expected);
        assert!(r.file_sequence() > 0);
        assert!(r.try_read()?.is_none(), "joined at the tail");
        Ok(())
    }

    /// Every state a rolled segment can be seen in: roll finished (`write_position` past the
    /// Roll), Roll committed but `write_position` not yet bumped, Roll staged but not committed.
    #[test]
    fn live_open_of_a_rolled_segment_joins_the_newest() -> anyhow::Result<()> {
        let base = "test_live_open_rolled_segment";
        for state in ["finished", "committed", "staged"] {
            let roll_at = rolled_once(base)?;
            if state != "finished" {
                poke_on_disk(
                    base,
                    0,
                    WP_AT,
                    &(roll_at + HEADER_SLOT as u64).to_le_bytes(),
                )?;
            }
            if state == "staged" {
                poke_on_disk(base, 0, roll_at, &[0])?;
            }
            assert_stale_then_joins_tail(base, 5).map_err(|e| e.context(state))?;
        }
        cleanup_channel_files(base);
        Ok(())
    }

    /// The Roll is committed but the newer segment was not visible yet when the open checked
    /// (the rename and the commit both landed after that check): start on the Roll and follow it.
    #[test]
    fn live_open_on_a_committed_roll_starts_there() -> anyhow::Result<()> {
        let base = "test_live_open_committed_roll";
        let roll_at = rolled_once(base)?;
        poke_on_disk(
            base,
            0,
            WP_AT,
            &(roll_at + HEADER_SLOT as u64).to_le_bytes(),
        )?;
        let seg1 = make_channel_file_path(Path::new(base), 1)?;
        let hidden = PathBuf::from(format!("{base}.hidden"));
        std::fs::rename(&seg1, &hidden)?;
        let mut r = live_open_segment_0(base)?;
        std::fs::rename(&hidden, &seg1)?;
        assert_eq!(r.position(), 3);
        let m = r.try_read()?.expect("the roll is followed");
        assert_eq!(m.header().user_meta_u64, 3);
        assert_eq!((r.file_sequence(), r.position()), (1, 4));
        cleanup_channel_files(base);
        Ok(())
    }

    /// A short commit leaves the rest of its reservation behind; after a roll the slot past the
    /// Roll holds those leftover payload bytes, not a header. They must never be read as one.
    #[test]
    fn live_open_ignores_leftover_bytes_past_a_roll() -> anyhow::Result<()> {
        let base = "test_live_open_leftover_bytes";
        cleanup_channel_files(base);
        let mut w = WriterBuilder::new(base).build()?;
        write_indexed(&mut w, 0..2)?;
        w.try_reserve(256)?.fill(0xFF);
        w.commit(0, 8, 2)?; // short commit: 248 bytes of 0xFF stay behind
        w.roll_file()?;
        write_indexed(&mut w, 3..4)?;
        assert_stale_then_joins_tail(base, 4)?;
        cleanup_channel_files(base);
        Ok(())
    }

    /// The open picked segment 0, then retention removed it and the segment after it (oldest
    /// first, as `keep_files` does) before the open looked: it must still notice segment 0 is
    /// stale, not wait past its Roll forever.
    #[test]
    fn live_open_when_retention_removed_the_next_segment() -> anyhow::Result<()> {
        let base = "test_live_open_next_pruned";
        rolled_once(base)?;
        let mut w = WriterBuilder::new(base).build()?;
        w.roll_file()?;
        write_indexed(&mut w, 5..6)?;
        let held = File::open(make_channel_file_path(Path::new(base), 0)?)?;
        for seq in [0, 1] {
            std::fs::remove_file(make_channel_file_path(Path::new(base), seq)?)?;
        }
        let err = Reader::from_segment(PathBuf::from(base), 0, held, SegmentStart::Head)
            .err()
            .expect("segment 0 is stale");
        assert!(StaleSegment::is(&err), "{err}");
        let mut r = ReaderBuilder::new(base).live().build()?;
        assert_eq!((r.file_sequence(), r.position()), (2, 6));
        assert!(r.try_read()?.is_none());
        cleanup_channel_files(base);
        Ok(())
    }

    /// A Roll in the file's very last slot leaves `write_position` past the end of the file; a
    /// Live open must not touch the bytes beyond EOF (SIGBUS).
    #[test]
    fn live_open_when_the_roll_took_the_last_slot() -> anyhow::Result<()> {
        let base = "test_live_open_roll_last_slot";
        cleanup_channel_files(base);
        let region_size = page_size();
        let file_roll_size = region_size as u64 * 2;
        let mut w = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size(file_roll_size)
            .build()?;
        let mut put = |len: usize, index: u64| -> io::Result<()> {
            w.try_reserve(len)?.fill(index as u8);
            w.commit(0, len as u32, index)
        };
        put(8, 0)?;
        // Region 1 exactly: header + payload end where the room kept for a Roll begins.
        put(region_size - HEADER_SLOT - ROLL_TOTAL, 1)?;
        put(8, 2)?; // no room left: the Roll takes the last slot, this goes to segment 1
        assert_eq!(
            peek_u64_on_disk(base, 0, WP_AT)?,
            file_roll_size + HEADER_SLOT as u64,
            "write_position is past the end of segment 0"
        );
        assert_stale_then_joins_tail(base, 3)?;
        cleanup_channel_files(base);
        Ok(())
    }

    /// A writer died after renaming the next segment in but before committing the old
    /// segment's Roll. A reader on the old segment waits on the staged Roll; the next writer's
    /// open finishes the roll, and the reader moves on.
    #[test]
    fn writer_open_finishes_a_roll_its_predecessor_left_staged() -> anyhow::Result<()> {
        let base = "test_writer_finishes_staged_roll";
        let roll_at = rolled_once(base)?;
        poke_on_disk(base, 0, roll_at, &[0])?;
        poke_on_disk(
            base,
            0,
            WP_AT,
            &(roll_at + HEADER_SLOT as u64).to_le_bytes(),
        )?;

        let mut r = ReaderBuilder::new(base).build()?;
        for i in 0..3 {
            assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, i);
        }
        assert!(r.try_read()?.is_none(), "stuck on the staged Roll");

        let word_before = wake_word_on_disk(base, 0)?;
        let _w = WriterBuilder::new(base).build()?;
        assert_eq!(
            wake_word_on_disk(base, 0)?,
            word_before + 1,
            "the repair wakes readers asleep on the old segment"
        );
        assert_eq!(
            r.try_read()?.expect("roll followed").header().user_meta_u64,
            3
        );
        assert_eq!(
            peek_u64_on_disk(base, 0, WP_AT)?,
            roll_at + (HEADER_SLOT + ROLL_TOTAL) as u64,
            "write_position advanced past the Roll, as roll_file does"
        );
        cleanup_channel_files(base);
        Ok(())
    }

    /// The roll repair only touches a real predecessor: a staged Roll in a segment whose
    /// numbering does not continue into this one (copied in from another series) is not the
    /// parent of the published successor: the open fails rather than splice them, and leaves its
    /// Roll uncommitted. A truncated stub is left alone, and must not be grown either.
    #[test]
    fn writer_open_leaves_a_foreign_or_truncated_predecessor_alone() -> anyhow::Result<()> {
        let base = "test_writer_leaves_foreign_predecessor";
        let roll_at = rolled_once(base)?;
        poke_on_disk(base, 0, roll_at, &[0])?;
        poke_on_disk(
            base,
            0,
            WP_AT,
            &(roll_at + HEADER_SLOT as u64).to_le_bytes(),
        )?;
        poke_on_disk(base, 0, BASE_AT, &100u64.to_le_bytes())?; // no longer continues into seg 1
        let err = WriterBuilder::new(base).build().err().expect("refused");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("ends at 103"), "{err}");
        let seg0 = std::fs::read(make_channel_file_path(Path::new(base), 0)?)?;
        assert_eq!(
            seg0[roll_at as usize], 0,
            "foreign segment's Roll left staged"
        );

        let seg0_path = make_channel_file_path(Path::new(base), 0)?;
        OpenOptions::new()
            .write(true)
            .open(&seg0_path)?
            .set_len(64)?;
        let _w = WriterBuilder::new(base).build()?;
        assert_eq!(std::fs::metadata(&seg0_path)?.len(), 64, "stub not grown");
        cleanup_channel_files(base);
        Ok(())
    }

    /// A segment whose roll completed is left byte-for-byte alone by the next writer's open.
    #[test]
    fn writer_open_leaves_a_completed_roll_alone() -> anyhow::Result<()> {
        let base = "test_writer_leaves_completed_roll";
        rolled_once(base)?;
        let path = make_channel_file_path(Path::new(base), 0)?;
        let before = std::fs::read(&path)?;
        let _w = WriterBuilder::new(base).build()?;
        assert!(std::fs::read(&path)? == before, "segment 0 unchanged");
        cleanup_channel_files(base);
        Ok(())
    }

    /// Live opens racing a writer that rolls every few hundred records: each must deliver the
    /// record its position names, and none may be stranded on a segment that has rolled.
    #[test]
    fn live_opens_racing_rolls_are_never_stranded() -> anyhow::Result<()> {
        let base = "test_live_opens_racing_rolls";
        cleanup_channel_files(base);
        let n = 200_000u64;
        let mut w = rolling_writer(base)?.keep_files(4).build()?;
        write_indexed(&mut w, 0..1)?;
        let writer = thread::spawn(move || -> io::Result<()> {
            for chunk in (1..n).step_by(16) {
                write_indexed(&mut w, chunk..(chunk + 16).min(n))?;
                let until = Instant::now() + Duration::from_micros(2);
                while Instant::now() < until {
                    std::hint::spin_loop();
                }
            }
            Ok(())
        });
        let mut opens = 0u64;
        while !writer.is_finished() {
            let mut r = match ReaderBuilder::new(base).live().build() {
                Ok(r) => r,
                // Retention can unlink the segment between the listing and the open.
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            let at = r.position();
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match r.try_read() {
                    Ok(Some(m)) => {
                        assert_eq!(m.header().user_meta_u64, at, "Live open position");
                        break;
                    }
                    Ok(None) if at >= n => break,
                    Ok(None) => {
                        assert!(
                            Instant::now() < deadline,
                            "Live reader stranded at {at} on segment {}",
                            r.file_sequence()
                        );
                        std::hint::spin_loop();
                    }
                    // A reader that fell behind retention: not what this test is about.
                    Err(e) if e.kind() == ErrorKind::NotFound => break,
                    Err(e) => return Err(e.into()),
                }
            }
            opens += 1;
        }
        writer.join().expect("writer thread")?;
        assert!(opens > 0);
        cleanup_channel_files(base);
        Ok(())
    }

    /// Crash recovery counts the orphaned record whichever side of the count bump the writer
    /// died on, so the head and the next segment's base stay true and a reader rolls cleanly.
    #[test]
    fn writer_recovery_counts_the_orphan() -> anyhow::Result<()> {
        let base = "test_writer_recovery_counts_orphan";
        for count_bumped in [false, true] {
            cleanup_channel_files(base);
            {
                let mut w = WriterBuilder::new(base).build()?;
                write_indexed(&mut w, 0..1)?;
            }
            rewind_write_position_on_disk(base, RECORD_8)?;
            set_message_count_on_disk(base, if count_bumped { 1 } else { 0 })?;

            let mut w = WriterBuilder::new(base).build()?;
            assert_eq!(w.next_record_index(), 1, "count_bumped={count_bumped}");
            write_indexed(&mut w, 1..2)?;
            w.roll_file()?;
            write_indexed(&mut w, 2..3)?;
            assert_eq!(w.next_record_index(), 3);

            let mut r = ReaderBuilder::new(base).build()?;
            for i in 0..3 {
                assert_eq!(r.position(), i);
                assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, i);
            }
            assert_eq!(r.base_record_index(), 2);
            assert_eq!(ReaderBuilder::new(base).start_at(2).build()?.position(), 2);
        }
        cleanup_channel_files(base);
        Ok(())
    }

    /// Live and At opens racing a busy writer: whatever the reader computes as its position must
    /// be the index of the first record it then reads.
    #[test]
    fn opens_racing_a_writer_agree_with_the_records() -> anyhow::Result<()> {
        let base = "test_opens_racing_a_writer";
        cleanup_channel_files(base);
        let n = 300_000u64;
        // Big enough not to roll: this exercises the publish pair, not roll timing.
        let mut w = WriterBuilder::new(base).file_roll_size(64 << 20).build()?;
        write_indexed(&mut w, 0..1)?;
        // Paced so the reader gets many opens in while records are still landing.
        let writer = thread::spawn(move || -> io::Result<()> {
            for chunk in (1..n).step_by(16) {
                write_indexed(&mut w, chunk..(chunk + 16).min(n))?;
                let until = Instant::now() + Duration::from_micros(2);
                while Instant::now() < until {
                    std::hint::spin_loop();
                }
            }
            Ok(())
        });

        let first_record = |r: &mut Reader| -> anyhow::Result<Option<u64>> {
            loop {
                if let Some(m) = r.try_read()? {
                    return Ok(Some(m.header().user_meta_u64));
                }
                if r.position() >= n {
                    return Ok(None);
                }
                std::hint::spin_loop();
            }
        };
        let mut opens = 0u64;
        while !writer.is_finished() {
            let mut live = ReaderBuilder::new(base).live().build()?;
            let at = live.position();
            if let Some(got) = first_record(&mut live)? {
                assert_eq!(got, at, "Live open");
            }
            let target = at / 2;
            let mut seeker = ReaderBuilder::new(base).start_at(target).build()?;
            assert_eq!(first_record(&mut seeker)?, Some(target), "At open");
            opens += 1;
        }
        writer.join().expect("writer thread")?;
        assert!(opens > 0);

        cleanup_channel_files(base);
        Ok(())
    }

    /// Records 0..n across a rolled pair of segments; returns how many landed in segment 0.
    fn two_segments(base: &str, n: u64) -> anyhow::Result<u64> {
        cleanup_channel_files(base);
        let mut w = rolling_writer(base)?.build()?;
        write_indexed(&mut w, 0..n)?;
        assert_eq!(w.file_sequence, 1, "expected exactly one roll");
        Ok(w.next_record_index() - peek_u64_on_disk(base, 1, COUNT_AT)?)
    }

    /// A batch that reaches a roll it cannot follow (here: the next segment is gone) hands over
    /// the records before the Roll, then reports the error on the next call, as single reads do.
    /// It used to drop those records and fail on every call; before 6.0.0 it also left the failed
    /// scan's mappings behind, and the next read panicked.
    #[test]
    fn failed_batch_leaves_the_reader_usable() -> anyhow::Result<()> {
        let base = "test_failed_batch_leaves_reader_usable";
        let in_first = two_segments(base, 400)?;
        std::fs::remove_file(make_channel_file_path(Path::new(base), 1)?)?;

        let mut r = ReaderBuilder::new(base).build()?;
        let batch = r.try_read_batch(None)?.expect("records before the roll");
        assert_eq!(batch.len() as u64, in_first);
        for (i, m) in batch.iter().enumerate() {
            assert_eq!(m.header().user_meta_u64, i as u64);
        }
        assert_eq!(r.position(), in_first);
        for _ in 0..2 {
            let err = r.try_read_batch(None).err().expect("next segment is gone");
            assert_eq!(err.kind(), ErrorKind::NotFound);
            assert_eq!(r.position(), in_first, "a failed batch consumes nothing");
        }
        let err = r.try_read().err().expect("single reads meet the same roll");
        assert_eq!(err.kind(), ErrorKind::NotFound);

        // A batch that fails before collecting anything leaves the reader usable too.
        let mut fresh = ReaderBuilder::new(base).build()?;
        for i in 0..in_first {
            assert_eq!(fresh.try_read()?.expect("record").header().user_meta_u64, i);
        }
        let err = fresh
            .try_read_batch(None)
            .err()
            .expect("roll into the missing segment");
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(
            fresh.try_read().err().map(|e| e.kind()),
            Some(ErrorKind::NotFound)
        );

        cleanup_channel_files(base);
        Ok(())
    }

    /// A batch that crosses into a segment breaking the absolute numbering refuses it, as a
    /// single read does — it used to open the next file unchecked.
    #[test]
    fn batch_refuses_a_discontinuous_next_segment() -> anyhow::Result<()> {
        let base = "test_batch_refuses_discontinuous";
        let in_first = two_segments(base, 400)?;
        poke_on_disk(base, 1, BASE_AT, &(in_first + 5).to_le_bytes())?;

        let mut r = ReaderBuilder::new(base).build()?;
        let batch = r.try_read_batch(None)?.expect("records before the roll");
        assert_eq!(batch.len() as u64, in_first);
        let err = r.try_read_batch(None).err().expect("discontinuity");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("discontinuity"), "{err}");

        let mut single = ReaderBuilder::new(base).build()?;
        for _ in 0..in_first {
            single.try_read()?.expect("record before the roll");
        }
        let err = single.try_read().err().expect("same refusal");
        assert_eq!(err.kind(), ErrorKind::InvalidData);

        cleanup_channel_files(base);
        Ok(())
    }

    /// A batch that crosses rolls refreshes `base_record_index()` as single reads do.
    #[test]
    fn batch_across_rolls_tracks_the_segment_base() -> anyhow::Result<()> {
        let base = "test_batch_across_rolls_tracks_base";
        cleanup_channel_files(base);
        let region_size = page_size();
        let mut w = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size(region_size as u64 * 2)
            .build()?;
        write_indexed(&mut w, 0..1000)?;

        let mut r = ReaderBuilder::new(base).build()?;
        let mut next = 0u64;
        while let Some(batch) = r.try_read_batch(Some(37))? {
            for m in batch.iter() {
                assert_eq!(m.header().user_meta_u64, next);
                next += 1;
            }
            assert!(r.base_record_index() <= next);
        }
        assert_eq!(next, 1000);
        assert_eq!(r.file_sequence(), w.file_sequence);
        assert!(
            r.base_record_index() > 0,
            "base refreshed across batch rolls"
        );

        cleanup_channel_files(base);
        Ok(())
    }

    // ---------- waking readers ----------

    /// File offsets of `wake_flags` and `wake_word` in a segment.
    const WAKE_FLAGS_AT: u64 = MESSAGE_HEADER_SIZE as u64 + 97;
    const WAKE_WORD_AT: u64 = MESSAGE_HEADER_SIZE as u64 + 100;

    fn wake_flag_on_disk(base: &str, seq: u64) -> anyhow::Result<u8> {
        let bytes = std::fs::read(make_channel_file_path(Path::new(base), seq)?)?;
        Ok(bytes[WAKE_FLAGS_AT as usize])
    }

    fn wake_word_on_disk(base: &str, seq: u64) -> anyhow::Result<u32> {
        Ok(peek_u64_on_disk(base, seq, WAKE_WORD_AT)? as u32)
    }

    /// The flag follows the writer's setting in every segment it creates or reopens, and a
    /// writer that does not wake never touches the word.
    #[test]
    fn wake_flag_follows_the_writers_setting() -> anyhow::Result<()> {
        let base = "test_wake_flag_follows_writer";
        cleanup_channel_files(base);
        {
            let mut w = WriterBuilder::new(base).build()?;
            write_indexed(&mut w, 0..5)?;
        }
        assert_eq!(wake_flag_on_disk(base, 0)?, 0);
        assert_eq!(
            wake_word_on_disk(base, 0)?,
            0,
            "a writer that does not wake leaves it"
        );
        {
            let mut w = WriterBuilder::new(base).wake_readers(true).build()?;
            assert_eq!(
                wake_flag_on_disk(base, 0)?,
                1,
                "a waking writer stamps a reopened segment"
            );
            write_indexed(&mut w, 5..8)?;
            assert_eq!(wake_word_on_disk(base, 0)?, 3, "bumped once per commit");
            w.roll_file()?;
            assert_eq!(wake_word_on_disk(base, 0)?, 4, "and once more for the Roll");
            assert_eq!(
                wake_flag_on_disk(base, 1)?,
                1,
                "new segments carry the flag"
            );
        }
        let _w = WriterBuilder::new(base).build()?;
        assert_eq!(
            wake_flag_on_disk(base, 1)?,
            0,
            "a writer that does not wake clears it"
        );
        cleanup_channel_files(base);
        Ok(())
    }

    /// Latency from each commit (stamped into the record) to the reader having it, for a
    /// reader that waits with `wait_for_message` while the writer pauses `gap` between commits.
    fn wait_latencies(
        base: &str,
        wake: bool,
        rounds: u64,
        gap: Duration,
    ) -> anyhow::Result<(Vec<u64>, u32)> {
        cleanup_channel_files(base);
        WriterBuilder::new(base).wake_readers(wake).precreate()?;
        let mut r = ReaderBuilder::new(base).build()?;
        let writer_base = base.to_string();
        let writer = thread::spawn(move || -> anyhow::Result<()> {
            let mut w = WriterBuilder::new(&writer_base)
                .wake_readers(wake)
                .build()?;
            for i in 0..rounds {
                thread::sleep(gap);
                w.try_reserve(8)?.copy_from_slice(&now_ns().to_le_bytes());
                w.commit(0, 8, i)?;
            }
            Ok(())
        });
        let mut latencies = Vec::new();
        for _ in 0..rounds {
            assert!(
                r.wait_for_message(Some(Duration::from_secs(10)))?,
                "a record arrives"
            );
            let received = now_ns();
            let m = r.try_read()?.expect("wait_for_message promised a record");
            let sent = u64::from_le_bytes(m.payload().try_into()?);
            latencies.push(received.saturating_sub(sent));
        }
        writer.join().expect("writer thread")?;
        cleanup_channel_files(base);
        latencies.sort_unstable();
        Ok((latencies, r.woken))
    }

    /// A reader asleep in `wait_for_message` on a waking channel is woken by the commit, not by
    /// a timer: its futex waits end because the word changed, and it runs again soon after.
    #[test]
    fn woken_reader_runs_soon_after_the_commit() -> anyhow::Result<()> {
        if !wake::SUPPORTED {
            return Ok(());
        }
        let (lat, woken) = wait_latencies(
            "test_woken_reader_latency",
            true,
            20,
            Duration::from_millis(30),
        )?;
        assert!(woken >= 10, "only {woken} of 20 waits ended in a wake");
        let median = lat[lat.len() / 2];
        assert!(
            median < 2_000_000,
            "median wake latency {median} ns; all: {lat:?}"
        );
        Ok(())
    }

    /// The wake on the old segment's word after a `Roll` gets a sleeping reader across the roll.
    #[test]
    fn wake_reaches_a_reader_across_a_roll() -> anyhow::Result<()> {
        if !wake::SUPPORTED {
            return Ok(());
        }
        let base = "test_wake_across_roll";
        cleanup_channel_files(base);
        WriterBuilder::new(base).wake_readers(true).precreate()?;
        let mut r = ReaderBuilder::new(base).build()?;
        let writer_base = base.to_string();
        let writer = thread::spawn(move || -> anyhow::Result<()> {
            let mut w = WriterBuilder::new(&writer_base)
                .wake_readers(true)
                .build()?;
            thread::sleep(Duration::from_millis(30));
            w.roll_file()?;
            thread::sleep(Duration::from_millis(30));
            Ok(write_indexed(&mut w, 0..1)?)
        });
        let started = Instant::now();
        assert!(r.wait_for_message(Some(Duration::from_secs(10)))?);
        assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, 0);
        assert_eq!(r.file_sequence(), 1, "followed the roll");
        assert!(started.elapsed() < Duration::from_secs(2));
        writer.join().expect("writer thread")?;
        cleanup_channel_files(base);
        Ok(())
    }

    /// A segment flagged as waking whose writer does not wake (an older writer reopened it)
    /// costs at most a capped sleep per record, and after two such records the reader backs
    /// off for that segment.
    #[test]
    fn stale_wake_flag_falls_back_to_backoff() -> anyhow::Result<()> {
        if !wake::SUPPORTED {
            return Ok(());
        }
        let base = "test_stale_wake_flag";
        cleanup_channel_files(base);
        WriterBuilder::new(base).precreate()?;
        poke_on_disk(base, 0, WAKE_FLAGS_AT, &[1])?; // flagged, but nobody will wake
        let mut r = ReaderBuilder::new(base).build()?;
        let writer_base = base.to_string();
        let writer = thread::spawn(move || -> anyhow::Result<()> {
            let mut w = WriterBuilder::new(&writer_base).build()?;
            // Reopening clears the flag; set it again behind the writer's back.
            poke_on_disk(&writer_base, 0, WAKE_FLAGS_AT, &[1])?;
            for i in 0..2 {
                thread::sleep(Duration::from_millis(30));
                write_indexed(&mut w, i..i + 1)?;
            }
            Ok(())
        });
        assert!(r.wait_for_message(Some(Duration::from_secs(10)))?);
        assert!(!r.wake_untrusted, "one miss is not enough");
        assert!(r.try_read()?.is_some());
        assert!(r.wait_for_message(Some(Duration::from_secs(10)))?);
        assert!(
            r.wake_untrusted,
            "the flag is no longer believed for this segment"
        );
        assert!(r.wake_word_if_waking().is_none());
        writer.join().expect("writer thread")?;
        cleanup_channel_files(base);
        Ok(())
    }

    /// `wait_any` returns the reader that has something: at once when one already does, after a
    /// wake when the record comes later, by polling when that channel does not wake, and `None`
    /// once the timeout passes.
    #[test]
    fn wait_any_returns_the_ready_reader() -> anyhow::Result<()> {
        let (a, b, c) = ("test_wait_any_a", "test_wait_any_b", "test_wait_any_c");
        for (base, wake) in [(a, true), (b, true), (c, false)] {
            cleanup_channel_files(base);
            WriterBuilder::new(base).wake_readers(wake).precreate()?;
        }
        let mut ra = ReaderBuilder::new(a).build()?;
        let mut rb = ReaderBuilder::new(b).build()?;
        let mut rc = ReaderBuilder::new(c).build()?;

        let none = wait_any(&mut [&mut ra, &mut rb], Some(Duration::from_millis(30)))?;
        assert_eq!(none, None, "nothing written: times out");

        for (target, wake) in [(b, true), (c, false)] {
            let target = target.to_string();
            let writer = thread::spawn(move || -> anyhow::Result<()> {
                let mut w = WriterBuilder::new(&target).wake_readers(wake).build()?;
                thread::sleep(Duration::from_millis(30));
                Ok(write_indexed(&mut w, 0..1)?)
            });
            let ready = if wake {
                wait_any(&mut [&mut ra, &mut rb], Some(Duration::from_secs(10)))?
            } else {
                wait_any(&mut [&mut ra, &mut rc], Some(Duration::from_secs(10)))?
            };
            assert_eq!(ready, Some(1), "the reader whose channel was written");
            writer.join().expect("writer thread")?;
        }
        // Already there: returned without sleeping.
        assert_eq!(
            wait_any(&mut [&mut rb, &mut ra], Some(Duration::ZERO))?,
            Some(0)
        );
        assert_eq!(rb.try_read()?.expect("record").header().user_meta_u64, 0);

        let err = wait_any(&mut [], None).expect_err("empty");
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        for base in [a, b, c] {
            cleanup_channel_files(base);
        }
        Ok(())
    }

    /// Without `futex_waitv` (a kernel before 5.16, or a seccomp profile that forbids it),
    /// `wait_any` over waking channels backs off like any other wait. It used to sleep a flat
    /// 10 ms per round, so a record arriving 2 ms in waited about 8 ms more.
    #[cfg(target_os = "linux")]
    #[test]
    fn wait_any_without_futex_waitv_backs_off() -> anyhow::Result<()> {
        wake::disable_waitv(); // process-wide: other wait_any tests then use backoff too
        let (a, b) = ("test_wait_any_no_waitv_a", "test_wait_any_no_waitv_b");
        for base in [a, b] {
            cleanup_channel_files(base);
            WriterBuilder::new(base).wake_readers(true).precreate()?;
        }
        let mut ra = ReaderBuilder::new(a).build()?;
        let mut rb = ReaderBuilder::new(b).build()?;
        let mut latencies = Vec::new();
        for i in 0..5 {
            let target = b.to_string();
            let writer = thread::spawn(move || -> anyhow::Result<()> {
                let mut w = WriterBuilder::new(&target).wake_readers(true).build()?;
                thread::sleep(Duration::from_millis(2));
                w.try_reserve(8)?.copy_from_slice(&now_ns().to_le_bytes());
                w.commit(0, 8, i)?;
                Ok(())
            });
            assert_eq!(
                wait_any(&mut [&mut ra, &mut rb], Some(Duration::from_secs(5)))?,
                Some(1)
            );
            let received = now_ns();
            let sent = u64::from_le_bytes(rb.try_read()?.expect("record").payload().try_into()?);
            latencies.push(received.saturating_sub(sent));
            writer.join().expect("writer thread")?;
        }
        latencies.sort_unstable();
        let median = latencies[latencies.len() / 2];
        assert!(median < 5_000_000, "median {median} ns: {latencies:?}");
        for base in [a, b] {
            cleanup_channel_files(base);
        }
        Ok(())
    }

    /// A capped sleep that ran out while the writer was between publishing a record and bumping
    /// the word must not condemn the flag, whether the bump has landed by the time the reader
    /// checks or lands only after. Only two misses on the same value, as a writer that does not
    /// wake leaves it, mark the flag untrusted.
    #[test]
    fn a_late_wake_keeps_the_flag_trusted() -> anyhow::Result<()> {
        if !wake::SUPPORTED {
            return Ok(());
        }
        let base = "test_late_wake_keeps_flag";
        cleanup_channel_files(base);
        WriterBuilder::new(base).wake_readers(true).precreate()?;
        let mut r = ReaderBuilder::new(base).build()?;
        let seen = r.wake_word_if_waking().expect("flagged");

        poke_on_disk(base, 0, WAKE_WORD_AT, &(seen + 1).to_le_bytes())?; // the late bump
        r.distrust_flag_if_unwoken(0, seen);
        assert!(!r.wake_untrusted, "the word moved: the writer does wake");

        // The writer is still preempted when the reader checks: a miss, but only one.
        let moved = r.wake_word_if_waking().expect("still flagged");
        r.distrust_flag_if_unwoken(0, moved);
        assert!(!r.wake_untrusted, "a single miss keeps the flag");
        // Its bump lands, and a later sleep misses again on the new value: still one miss.
        poke_on_disk(base, 0, WAKE_WORD_AT, &(moved + 1).to_le_bytes())?;
        let bumped = r.wake_word_if_waking().expect("still flagged");
        r.distrust_flag_if_unwoken(0, bumped);
        assert!(!r.wake_untrusted, "the word moved between the misses");
        // A second miss on the same value: nobody is waking.
        r.distrust_flag_if_unwoken(0, bumped);
        assert!(r.wake_untrusted, "the word never moved: nobody is waking");
        cleanup_channel_files(base);
        Ok(())
    }

    // ---------- helpers that stop early ----------

    /// `Writer` and `Reader` keep the auto traits they had before they could own a helper thread:
    /// losing one breaks code that names it, such as a `catch_unwind` over a writer.
    #[test]
    fn writer_and_reader_keep_their_auto_traits() {
        fn assert_auto<T: Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe>() {}
        assert_auto::<Writer>();
        assert_auto::<Reader>();
    }

    /// Segment files of channel `base` on disk, `.partial` ones included.
    #[cfg(target_os = "linux")]
    fn segment_files(base: &str) -> usize {
        std::fs::read_dir(".")
            .unwrap()
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|n| n == base || n.starts_with(&format!("{base}.")))
            .count()
    }

    /// Live mappings of channel `base`'s segments in this process, unlinked ones included.
    #[cfg(target_os = "linux")]
    fn segment_mappings(base: &str) -> usize {
        let path = std::env::current_dir().unwrap().join(base);
        let path = path.to_str().unwrap();
        std::fs::read_to_string("/proc/self/maps")
            .unwrap()
            .lines()
            .filter_map(|line| line.split_once('/').map(|(_, p)| format!("/{p}")))
            .filter(|p| p == path || p.starts_with(&format!("{path}.")))
            .count()
    }

    #[cfg(target_os = "linux")]
    fn until(mut done: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if done() {
                return true;
            }
            thread::sleep(Duration::from_millis(1));
        }
        false
    }

    /// A writer whose helper stopped carries on as without one: it unmaps the regions it leaves
    /// and retention deletes segments, instead of queuing both for a thread that is gone.
    #[cfg(target_os = "linux")]
    fn writer_outlives_its_helper(base: &str, stop: impl Fn(&Writer)) -> anyhow::Result<Writer> {
        cleanup_channel_files(base);
        let mut w = WriterBuilder::new(base)
            .region_size(page_size() * 16)
            .file_roll_size(page_size() as u64 * 64)
            .keep_files(2)
            .helper(Helper::inherit())
            .build()?;
        stop(&w);
        assert!(until(|| w.helper_error().is_some()), "the helper stopped");
        for i in 0..50_000u64 {
            w.try_reserve(96)?[..8].copy_from_slice(&i.to_le_bytes());
            w.commit(0, 96, i)?;
        }
        assert!(w.file_sequence > 10, "rolled many times");
        let (files, maps) = (segment_files(base), segment_mappings(base));
        assert!(
            files <= 3,
            "{files} segment files on disk: retention stopped"
        );
        assert!(
            maps <= 4,
            "{maps} segment mappings: regions left are not unmapped"
        );
        Ok(w)
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_writer_whose_helper_fails_unmaps_and_deletes_for_itself() -> anyhow::Result<()> {
        let base = "test_writer_helper_fails";
        let w = writer_outlives_its_helper(base, |w| {
            w.prefault.as_ref().unwrap().shared.inject.error()
        })?;
        let e = w.helper_error().expect("stopped");
        assert_eq!(e.raw_os_error(), Some(libc::EINVAL), "{e}");
        drop(w);
        cleanup_channel_files(base);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_writer_whose_helper_panics_reports_it_and_carries_on() -> anyhow::Result<()> {
        let base = "test_writer_helper_panics";
        let w = writer_outlives_its_helper(base, |w| {
            w.prefault.as_ref().unwrap().shared.inject.panic()
        })?;
        let e = w.helper_error().expect("stopped");
        assert_eq!(e.kind(), ErrorKind::Other);
        assert!(e.to_string().contains("panicked"), "{e}");
        drop(w);
        cleanup_channel_files(base);
        Ok(())
    }

    /// The reader's side of the same: once its helper stopped, it unmaps what it leaves itself.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_reader_whose_helper_fails_unmaps_for_itself() -> anyhow::Result<()> {
        let base = "test_reader_helper_fails";
        cleanup_channel_files(base);
        {
            let mut w = WriterBuilder::new(base)
                .region_size(page_size() * 16)
                .file_roll_size(page_size() as u64 * 64)
                .build()?;
            for i in 0..50_000u64 {
                w.try_reserve(96)?[..8].copy_from_slice(&i.to_le_bytes());
                w.commit(0, 96, i)?;
            }
        }
        let mut r = ReaderBuilder::new(base)
            .late_join()
            .helper(Helper::inherit())
            .build()?;
        r.map_ahead.as_ref().unwrap().shared.inject.error();
        assert!(until(|| r.helper_error().is_some()), "the helper stopped");
        let mut read = 0u64;
        while let Some(m) = r.try_read()? {
            assert_eq!(m.header().user_meta_u64, read);
            read += 1;
        }
        assert_eq!(read, 50_000);
        let maps = segment_mappings(base);
        assert!(
            maps <= 4,
            "{maps} segment mappings: regions left are not unmapped"
        );
        assert_eq!(
            r.helper_error().and_then(|e| e.raw_os_error()),
            Some(libc::EINVAL)
        );
        drop(r);
        cleanup_channel_files(base);
        Ok(())
    }

    /// Read every record of channel `base` from its first segment.
    #[cfg(target_os = "linux")]
    fn records_in(base: &str) -> io::Result<u64> {
        let mut r = ReaderBuilder::new(base).late_join().build()?;
        let mut read = 0u64;
        while let Some(m) = r.try_read()? {
            assert_eq!(m.header().user_meta_u64, read);
            read += 1;
        }
        Ok(read)
    }

    /// If the helper cannot open the segment the writer rolls to (out of file descriptors), the
    /// writer grows that segment itself. It used to grow the old one through the helper's
    /// handle instead: the old file swelled to the new one's size, and the writer's idea of the
    /// new file's length was the old one's.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_writer_grows_a_segment_its_helper_could_not_open() -> anyhow::Result<()> {
        let base = "test_helper_clone_fails";
        cleanup_channel_files(base);
        let mut w = WriterBuilder::new(base)
            .region_size(page_size() * 16)
            .helper(Helper::inherit())
            .build()?;
        let shared = w.prefault.as_ref().unwrap().shared.clone();
        shared.fail_clone.store(true, Ordering::Release);
        w.roll_file()?; // not rolling by size: the new segment starts one region long
        let old = make_channel_file_path(Path::new(base), 0)?;
        let old_len = std::fs::metadata(&old)?.len();
        for i in 0..20_000u64 {
            w.try_reserve(96)?[..8].copy_from_slice(&i.to_le_bytes());
            w.commit(0, 96, i)?;
        }
        let new_len = std::fs::metadata(make_channel_file_path(Path::new(base), 1)?)?.len();
        assert_eq!(
            std::fs::metadata(&old)?.len(),
            old_len,
            "the old segment is left alone"
        );
        assert!(
            new_len > 30 * page_size() as u64 * 16,
            "the new one grew: {new_len}"
        );
        assert_eq!(w.file_len, new_len, "the writer knows the new one's length");
        assert!(w.helper_error().is_none(), "the helper carries on");
        drop(w);
        assert_eq!(records_in(base)?, 20_000);
        cleanup_channel_files(base);
        Ok(())
    }

    /// A next segment the helper cannot create (no space, no permission) is tried once, leaves
    /// no `.partial` behind, and is created by the writer at the roll, instead of being retried
    /// on every 50 µs pass until then.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_segment_the_helper_cannot_create_is_tried_once() -> anyhow::Result<()> {
        let base = "test_helper_prepare_fails";
        cleanup_channel_files(base);
        let mut w = WriterBuilder::new(base)
            .region_size(page_size() * 16)
            .file_roll_size(page_size() as u64 * 64)
            .helper(Helper::inherit())
            .build()?;
        let shared = w.prefault.as_ref().unwrap().shared.clone();
        shared.fail_prepare.store(true, Ordering::Release);
        for i in 0..20_000u64 {
            w.try_reserve(96)?[..8].copy_from_slice(&i.to_le_bytes());
            w.commit(0, 96, i)?;
            if i % 200 == 199 {
                thread::sleep(Duration::from_millis(1)); // let the helper make its passes
            }
        }
        let rolls = w.file_sequence;
        let attempts = shared.prepare_attempts.load(Ordering::Relaxed);
        assert!(rolls >= 5, "rolled {rolls} times");
        assert!(
            (1..=rolls + 1).contains(&attempts),
            "{attempts} attempts at creating segments ahead over {rolls} rolls"
        );
        assert!(w.helper_error().is_none(), "the helper carries on");
        drop(w); // the helper may be mid-attempt until then
        let partials = std::fs::read_dir(".")?
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|n| n.starts_with(base) && n.ends_with(PARTIAL_SUFFIX))
            .count();
        assert_eq!(partials, 0, "a failed attempt leaves no .partial");
        assert_eq!(records_in(base)?, 20_000);
        cleanup_channel_files(base);
        Ok(())
    }

    // ---------- format v4 ----------

    /// File offset of the publication state in the v4 extension.
    const STATE_AT: u64 = 192;

    /// The identity a segment's extension carries.
    fn identity_on_disk(base: &str, seq: u64) -> anyhow::Result<Identity> {
        let bytes = std::fs::read(make_channel_file_path(Path::new(base), seq)?)?;
        Ok(validate_v4_prefix(bytes.as_ptr())?)
    }

    /// A segment's layout, as FORMAT.md gives it: a 192-byte Channel record, the extension at
    /// 144 with the file published, the first record at 208.
    #[test]
    fn a_v4_segment_has_the_documented_layout() -> anyhow::Result<()> {
        let base = "test_v4_layout";
        cleanup_channel_files(base);
        let mut w = WriterBuilder::new(base).build()?;
        write_indexed(&mut w, 0..1)?;
        let bytes = std::fs::read(make_channel_file_path(Path::new(base), 0)?)?;
        assert_eq!(bytes[1], HeaderType::Channel as u8);
        assert_eq!(
            u32::from_le_bytes(bytes[4..8].try_into()?),
            192,
            "Channel length"
        );
        assert_eq!(
            u16::from_le_bytes(bytes[56..58].try_into()?),
            4,
            "format_version"
        );
        assert_eq!(bytes[STATE_AT as usize], v4::PUBLISHED);
        let identity = identity_on_disk(base, 0)?;
        assert_eq!(identity, Identity::initial(identity.instance));
        assert_eq!(bytes[208], 1, "first record committed at 208");
        assert_eq!(
            u64::from_le_bytes(bytes[216..224].try_into()?),
            0,
            "its index"
        );
        assert_eq!(w.instance, identity.instance);
        cleanup_channel_files(base);
        Ok(())
    }

    /// A v3 file is not read or reopened: v4 changes what a file under its final name means.
    #[test]
    fn a_v3_segment_is_refused() -> anyhow::Result<()> {
        let base = "test_v3_refused";
        cleanup_channel_files(base);
        WriterBuilder::new(base).precreate()?;
        poke_on_disk(base, 0, 56, &3u16.to_le_bytes())?;
        for err in [
            ReaderBuilder::new(base)
                .build()
                .err()
                .expect("reader refuses"),
            ReaderBuilder::new(base)
                .live()
                .build()
                .err()
                .expect("live reader refuses"),
            WriterBuilder::new(base)
                .build()
                .err()
                .expect("writer refuses"),
        ] {
            assert_eq!(err.kind(), ErrorKind::InvalidData);
            assert!(err.to_string().contains("format_version 3"), "{err}");
        }
        cleanup_channel_files(base);
        Ok(())
    }

    /// A roll's `Roll` names its successor exactly: sequence, instance ID and base, and the
    /// successor names the file it follows.
    #[test]
    fn a_roll_names_its_successor() -> anyhow::Result<()> {
        let base = "test_roll_names_successor";
        let roll_at = rolled_once(base)?;
        let bytes = std::fs::read(make_channel_file_path(Path::new(base), 0)?)?;
        let at = roll_at as usize;
        assert_eq!(bytes[at], 1, "committed");
        assert_eq!(bytes[at + 1], HeaderType::Roll as u8);
        assert_eq!(u32::from_le_bytes(bytes[at + 4..at + 8].try_into()?), 40);
        assert_eq!(
            u64::from_le_bytes(bytes[at + 8..at + 16].try_into()?),
            0,
            "no timestamp"
        );
        let edge = RollEdge::decode(&bytes[at + 16..at + 56])?;
        let (parent, child) = (identity_on_disk(base, 0)?, identity_on_disk(base, 1)?);
        assert!(edge.names(0, parent.instance, &child));
        assert_eq!(edge.next_base_record_index, 3);
        assert_ne!(child.instance, parent.instance);
        cleanup_channel_files(base);
        Ok(())
    }

    /// A writer died after installing the next segment but before publishing it: the newest
    /// file is `PREPARED`, and the old segment's Roll staged. Readers see no newer history,
    /// and the next writer discards that file and goes on writing in the old segment.
    #[test]
    fn an_unpublished_newest_segment_is_not_history() -> anyhow::Result<()> {
        let base = "test_unpublished_newest";
        let roll_at = rolled_once(base)?;
        // Back to the moment before publication: Roll staged, successor PREPARED and empty.
        cleanup_from(base, 1)?;
        let mut w = WriterBuilder::new(base).build()?;
        w.roll_file()?;
        drop(w);
        poke_on_disk(base, 0, roll_at, &[0])?;
        poke_on_disk(
            base,
            0,
            WP_AT,
            &(roll_at + HEADER_SLOT as u64).to_le_bytes(),
        )?;
        poke_on_disk(base, 1, STATE_AT, &[v4::PREPARED])?;
        let prepared = identity_on_disk(base, 1)?;

        let mut live = ReaderBuilder::new(base).live().build()?;
        assert_eq!((live.file_sequence(), live.position()), (0, 3));
        assert_eq!(live.head_record_index()?, 3);
        let err = ReaderBuilder::new(base)
            .start_at(4)
            .build()
            .err()
            .expect("past head");
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        let mut from_start = ReaderBuilder::new(base).build()?;
        for i in 0..3 {
            assert_eq!(
                from_start
                    .try_read()?
                    .expect("record")
                    .header()
                    .user_meta_u64,
                i
            );
        }
        assert!(from_start.try_read()?.is_none(), "waits on the staged Roll");

        let mut w = WriterBuilder::new(base).build()?;
        assert_eq!(w.file_sequence, 0, "resumed in the old segment");
        assert!(
            !make_channel_file_path(Path::new(base), 1)?.exists(),
            "unpublished file discarded"
        );
        write_indexed(&mut w, 3..4)?;
        w.roll_file()?;
        write_indexed(&mut w, 4..5)?;
        assert_ne!(
            identity_on_disk(base, 1)?.instance,
            prepared.instance,
            "a new file"
        );
        for r in [&mut live, &mut from_start] {
            for i in r.position()..5 {
                assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, i);
            }
        }
        cleanup_channel_files(base);
        Ok(())
    }

    /// Remove segments `from..` of `base`.
    fn cleanup_from(base: &str, from: u64) -> anyhow::Result<()> {
        for seq in find_all_sequences(Path::new(base))? {
            if seq >= from {
                std::fs::remove_file(make_channel_file_path(Path::new(base), seq)?)?;
            }
        }
        Ok(())
    }

    /// A committed Roll is written only after its successor is published, so one that leads
    /// to an unpublished file is a broken protocol, reported, not waited on.
    #[test]
    fn a_committed_roll_to_an_unpublished_segment_is_reported() -> anyhow::Result<()> {
        let base = "test_roll_to_unpublished";
        rolled_once(base)?;
        poke_on_disk(base, 1, STATE_AT, &[v4::PREPARED])?;
        let mut r = ReaderBuilder::new(base).build()?;
        for i in 0..3 {
            assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, i);
        }
        let err = r.try_read().err().expect("refused");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("not published"), "{err}");
        cleanup_channel_files(base);
        Ok(())
    }

    /// A channel's initial file installed but never published (its first writer died in
    /// between) has no history for readers; the next writer publishes it.
    #[test]
    fn an_unpublished_initial_segment_is_published_by_the_next_writer() -> anyhow::Result<()> {
        let base = "test_unpublished_initial";
        cleanup_channel_files(base);
        WriterBuilder::new(base).base_record_index(7).precreate()?;
        poke_on_disk(base, 0, STATE_AT, &[v4::PREPARED])?;
        let err = ReaderBuilder::new(base)
            .build()
            .err()
            .expect("nothing published");
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert!(err.to_string().contains("not published"), "{err}");
        let mut w = WriterBuilder::new(base).build()?;
        assert_eq!(w.next_record_index(), 7, "its base kept");
        write_indexed(&mut w, 7..8)?;
        let mut r = ReaderBuilder::new(base).build()?;
        assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, 7);
        cleanup_channel_files(base);
        Ok(())
    }

    /// Installation never replaces a file already under the final name.
    #[test]
    fn install_never_replaces() -> anyhow::Result<()> {
        let (from, to) = ("test_install_from", "test_install_to");
        std::fs::write(from, b"new")?;
        std::fs::write(to, b"old")?;
        let err = v4::install_no_replace(Path::new(from), Path::new(to)).expect_err("refused");
        assert_eq!(err.kind(), ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(to)?, b"old");
        std::fs::remove_file(to)?;
        v4::install_no_replace(Path::new(from), Path::new(to))?;
        assert_eq!(std::fs::read(to)?, b"new");
        assert!(!Path::new(from).exists());
        std::fs::remove_file(to)?;
        Ok(())
    }

    // ---------- successors installed ahead ----------

    /// Install segment 1 of `w`'s channel as another attempt would: `PREPARED`, naming `parent`.
    fn install_successor(w: &Writer, parent: InstanceId) -> anyhow::Result<InstanceId> {
        let identity = Identity::successor(InstanceId::fresh()?, parent, 0);
        let attempt = make_attempt_path(&w.base_path, 1, identity.instance)?;
        let segment = Writer::prepare_segment_at(
            &attempt,
            1,
            w.region_size,
            w.file_roll_size,
            w.mtu,
            &w.channel_name,
            0,
            w.generation(),
            &identity,
        )?;
        drop(segment);
        v4::install_no_replace(&attempt, &make_channel_file_path(&w.base_path, 1)?)?;
        Ok(identity.instance)
    }

    /// The helper installs the successor ahead; the roll takes it and only publishes it.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_roll_takes_the_successor_its_helper_installed() -> anyhow::Result<()> {
        let base = "test_roll_takes_installed";
        cleanup_channel_files(base);
        let mut w = rolling_writer(base)?.helper(Helper::inherit()).build()?;
        let next = make_channel_file_path(Path::new(base), 1)?;
        let mut i = 0;
        while w.next_hdr_pos < w.region_size * 3 / 2 {
            write_indexed(&mut w, i..i + 1)?;
            i += 1;
        }
        assert!(
            until(|| next.exists() && lock_ready(&w)),
            "installed and handed over"
        );
        let installed = identity_on_disk(base, 1)?.instance;
        assert_eq!(std::fs::read(&next)?[STATE_AT as usize], v4::PREPARED);
        w.roll_file()?;
        assert_eq!(w.instance, installed, "the same file, published");
        assert_eq!(std::fs::read(&next)?[STATE_AT as usize], v4::PUBLISHED);
        cleanup_channel_files(base);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn lock_ready(w: &Writer) -> bool {
        let shared = &w.prefault.as_ref().unwrap().shared;
        prefault::lock(&shared.next_segment).ready.is_some()
    }

    /// Another attempt installed the successor first: the roll adopts it instead of replacing it.
    #[test]
    fn a_roll_adopts_a_successor_installed_first() -> anyhow::Result<()> {
        let base = "test_roll_adopts_installed";
        cleanup_channel_files(base);
        let mut w = rolling_writer(base)?.build()?;
        write_indexed(&mut w, 0..3)?;
        let installed = install_successor(&w, w.instance)?;
        w.roll_file()?;
        write_indexed(&mut w, 3..4)?;
        assert_eq!(identity_on_disk(base, 1)?.instance, installed);
        assert_eq!(w.instance, installed);
        let attempts = std::fs::read_dir(".")?
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|n| n.starts_with(base) && n.ends_with(PARTIAL_SUFFIX))
            .count();
        assert_eq!(attempts, 0, "the losing attempt is removed");
        let mut r = ReaderBuilder::new(base).build()?;
        for i in 0..4 {
            assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, i);
        }
        cleanup_channel_files(base);
        Ok(())
    }

    /// A file at the successor's name that does not name this one as parent is refused, and the
    /// writer stays where it was with the Roll uncommitted.
    #[test]
    fn a_roll_refuses_a_successor_of_another_parent() -> anyhow::Result<()> {
        let base = "test_roll_refuses_foreign";
        cleanup_channel_files(base);
        let mut w = rolling_writer(base)?.build()?;
        write_indexed(&mut w, 0..3)?;
        install_successor(&w, InstanceId::fresh()?)?;
        let err = w.roll_file().expect_err("refused");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("belongs elsewhere"), "{err}");
        assert_eq!(w.file_sequence, 0);
        write_indexed(&mut w, 3..4)?;
        let mut r = ReaderBuilder::new(base).build()?;
        for i in 0..4 {
            assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, i);
        }
        cleanup_channel_files(base);
        Ok(())
    }

    /// Retention leaves a segment the helper may still install, and removes it on a later roll.
    #[cfg(target_os = "linux")]
    #[test]
    fn retention_waits_for_an_install_in_flight() -> anyhow::Result<()> {
        let base = "test_retention_waits_in_flight";
        cleanup_channel_files(base);
        let mut w = rolling_writer(base)?
            .keep_files(1)
            .helper(Helper::inherit())
            .build()?;
        let path = |seq| make_channel_file_path(Path::new(base), seq);
        w.roll_file()?;
        let shared = w.prefault.as_ref().unwrap().shared.clone();
        shared.in_flight.store(1, Ordering::SeqCst);
        w.roll_file()?;
        assert!(path(1)?.exists(), "kept while an install may land on it");
        shared.in_flight.store(prefault::NONE, Ordering::SeqCst);
        w.roll_file()?;
        assert!(until(
            || !path(1).unwrap().exists() && !path(2).unwrap().exists()
        ));
        cleanup_channel_files(base);
        Ok(())
    }

    #[test]
    fn attempt_names_are_swept_and_others_are_not() {
        let id = "0123456789abcdef0123456789abcdef";
        for name in [
            "c.partial".to_string(),
            "c.3.partial".into(),
            format!("c.{id}.partial"),
            format!("c.3.{id}.partial"),
        ] {
            assert!(is_partial_segment_name(&name, "c"), "{name}");
        }
        for name in [
            "c.notes.partial".to_string(),
            "c.3".into(),
            format!("c.x.{id}.partial"),
            format!("c.3.{}.partial", &id[1..]),
            format!("d.3.{id}.partial"),
        ] {
            assert!(!is_partial_segment_name(&name, "c"), "{name}");
        }
    }

    // ---------- bounded hand-over queues ----------

    /// With its helper stalled, the writer queues no more than the bounds and does the rest
    /// itself; nothing is lost, and the helper catches up once it runs again.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_stalled_writer_helper_never_grows_its_queues() -> anyhow::Result<()> {
        let base = "test_stalled_writer_helper";
        cleanup_channel_files(base);
        let mut w = WriterBuilder::new(base)
            .region_size(page_size())
            .file_roll_size(page_size() as u64 * 4)
            .keep_files(2)
            .helper(Helper::inherit())
            .build()?;
        let shared = w.prefault.as_ref().unwrap().shared.clone();
        shared.inject.stall();
        let capacities = || {
            (
                prefault::lock(&shared.retired).capacity(),
                prefault::lock(&shared.doomed).capacity(),
            )
        };
        let before = capacities();
        write_indexed(&mut w, 0..20_000)?;
        assert!(w.file_sequence > 20, "rolled {} times", w.file_sequence);
        assert_eq!(capacities(), before, "no queue grew");
        assert!(
            shared.saturated.load(Ordering::Relaxed) > 0,
            "the writer did it itself"
        );
        assert!(
            segment_mappings(base) <= 64 + 8,
            "{} mappings",
            segment_mappings(base)
        );
        // keep_files, the deletion queue, and the successor being installed.
        assert!(
            segment_files(base) <= 2 + 2 + 1,
            "{} files",
            segment_files(base)
        );
        shared.inject.resume();
        let kept = w.file_sequence - 1;
        assert!(
            until(|| find_earliest_sequence(Path::new(base)).is_ok_and(|e| e >= kept)),
            "the helper catches up"
        );
        let mut r = ReaderBuilder::new(base).build()?;
        let first = r.position();
        let mut next = first;
        while let Some(m) = r.try_read()? {
            assert_eq!(m.header().user_meta_u64, next);
            next += 1;
        }
        assert_eq!(next, 20_000);
        drop((r, w, shared));
        assert_eq!(segment_mappings(base), 0);
        cleanup_channel_files(base);
        Ok(())
    }

    /// The reader's side: a stalled helper's queue stays within its bound.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_stalled_reader_helper_never_grows_its_queue() -> anyhow::Result<()> {
        let base = "test_stalled_reader_helper";
        cleanup_channel_files(base);
        let mut w = WriterBuilder::new(base)
            .region_size(page_size())
            .file_roll_size(page_size() as u64 * 4)
            .build()?;
        write_indexed(&mut w, 0..20_000)?;
        let mut r = ReaderBuilder::new(base).helper(Helper::inherit()).build()?;
        let shared = r.map_ahead.as_ref().unwrap().shared.clone();
        shared.inject.stall();
        let before = map_ahead::lock(&shared.retired).capacity();
        for i in 0..20_000 {
            assert_eq!(r.try_read()?.expect("record").header().user_meta_u64, i);
        }
        assert_eq!(map_ahead::lock(&shared.retired).capacity(), before);
        assert!(shared.saturated.load(Ordering::Relaxed) > 0);
        assert!(
            segment_mappings(base) <= before + 8,
            "{} mappings",
            segment_mappings(base)
        );
        shared.inject.resume();
        drop((r, w, shared));
        assert_eq!(segment_mappings(base), 0);
        cleanup_channel_files(base);
        Ok(())
    }

    // ---------- a writer killed at each step of a roll ----------

    const CRASH_POINTS: [&str; 7] = [
        "attempt",
        "installed",
        "staged",
        "stamped",
        "published",
        "committed",
        "positioned",
    ];

    /// Run by `a_writer_killed_at_any_step_of_a_roll_recovers`: fill most of segment 0, then
    /// roll, dying at `XCH_CRASH_AT`.
    #[test]
    #[ignore]
    fn crash_child() -> anyhow::Result<()> {
        let (Ok(base), Ok(helper)) = (
            std::env::var("XCH_CRASH_BASE"),
            std::env::var("XCH_CRASH_HELPER"),
        ) else {
            return Ok(());
        };
        let mut b = rolling_writer(&base)?;
        if helper == "1" {
            b = b.helper(Helper::inherit());
        }
        let mut w = b.build()?;
        let mut i = 0;
        while w.next_hdr_pos < w.region_size * 3 / 2 {
            write_indexed(&mut w, i..i + 1)?;
            i += 1;
        }
        #[cfg(target_os = "linux")]
        if helper == "1" {
            assert!(until(|| lock_ready(&w)));
        }
        w.roll_file()?;
        Ok(())
    }

    /// Every segment file's bytes, to tell whether a recovery changed anything.
    fn snapshot(base: &str) -> anyhow::Result<Vec<(u64, Vec<u8>)>> {
        find_all_sequences(Path::new(base))?
            .into_iter()
            .map(|seq| {
                Ok((
                    seq,
                    std::fs::read(make_channel_file_path(Path::new(base), seq)?)?,
                ))
            })
            .collect()
    }

    /// A writer killed at each step of a roll, with or without its helper: readers already there
    /// carry on, recovery keeps exactly the history written and a published successor's
    /// identity, a second recovery changes nothing, and writing goes on.
    #[test]
    fn a_writer_killed_at_any_step_of_a_roll_recovers() -> anyhow::Result<()> {
        let helpers: &[&str] = if cfg!(target_os = "linux") {
            &["0", "1"]
        } else {
            &["0"]
        };
        for &helper in helpers {
            for point in CRASH_POINTS {
                if helper == "1" && point == "attempt" {
                    continue; // the helper's successor is taken, never attempted
                }
                let base = format!("test_crash_{point}_{helper}");
                let base = base.as_str();
                cleanup_channel_files(base);
                let status = std::process::Command::new(std::env::current_exe()?)
                    .args(["--ignored", "--exact", "tests::crash_child"])
                    .env("XCH_CRASH_AT", point)
                    .env("XCH_CRASH_BASE", base)
                    .env("XCH_CRASH_HELPER", helper)
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()?;
                let at = format!("{point} with helper {helper}");
                use std::os::unix::process::ExitStatusExt;
                assert_eq!(status.signal(), Some(libc::SIGABRT), "{at}: {status}");

                let mut early = ReaderBuilder::new(base).build()?;
                let mut written = 0;
                while let Some(m) = early.try_read()? {
                    assert_eq!(m.header().user_meta_u64, written, "{at}");
                    written += 1;
                }
                let mut live = ReaderBuilder::new(base).live().build()?;
                let published = segment_is_published(Path::new(base), 1)?
                    .then(|| identity_on_disk(base, 1))
                    .transpose()?;

                let w = rolling_writer(base)?.build()?;
                assert_eq!(w.next_record_index(), written, "{at}");
                drop(w);
                let recovered = snapshot(base)?;
                drop(rolling_writer(base)?.build()?);
                assert!(
                    snapshot(base)? == recovered,
                    "{at}: a second recovery changed bytes"
                );
                if let Some(identity) = published {
                    assert_eq!(identity_on_disk(base, 1)?, identity, "{at}: kept");
                }

                let mut b = rolling_writer(base)?;
                if helper == "1" {
                    b = b.helper(Helper::inherit());
                }
                let mut w = b.build()?;
                write_indexed(&mut w, written..written + 300)?;
                let total = written + 300;
                drop(w);
                for r in [&mut early, &mut live] {
                    while let Some(m) = r.try_read()? {
                        assert_eq!(m.header().user_meta_u64 + 1, r.position(), "{at}");
                    }
                    assert_eq!(r.position(), total, "{at}");
                }
                let mut fresh = ReaderBuilder::new(base).build()?;
                for i in 0..total {
                    assert_eq!(fresh.try_read()?.expect("record").header().user_meta_u64, i);
                }
                fresh.seek(written)?;
                assert_eq!(
                    fresh.try_read()?.expect("record").header().user_meta_u64,
                    written
                );
                let attempts = std::fs::read_dir(".")?
                    .filter_map(|e| e.ok()?.file_name().into_string().ok())
                    .filter(|n| n.starts_with(base) && n.ends_with(PARTIAL_SUFFIX))
                    .count();
                assert_eq!(attempts, 0, "{at}");
                cleanup_channel_files(base);
            }
        }
        Ok(())
    }

    // ---------- helper configurations against many readers ----------

    #[cfg(target_os = "linux")]
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum HelperCase {
        Off,
        On,
        FailsAtOnce,
        FailsLate,
    }

    #[cfg(target_os = "linux")]
    const HELPER_CASES: [HelperCase; 4] = [
        HelperCase::Off,
        HelperCase::On,
        HelperCase::FailsAtOnce,
        HelperCase::FailsLate,
    ];

    #[cfg(target_os = "linux")]
    fn matrix_writer(base: &str, case: HelperCase) -> io::Result<Writer> {
        let mut b = WriterBuilder::new(base)
            .region_size(page_size())
            .file_roll_size(page_size() as u64 * 4);
        if case != HelperCase::Off {
            b = b.helper(Helper::inherit());
        }
        let w = b.build()?;
        if let Some(p) = &w.prefault {
            match case {
                HelperCase::FailsAtOnce => p.shared.inject.error(),
                HelperCase::FailsLate => p.shared.fail_after_install.store(true, Ordering::Release),
                _ => {}
            }
        }
        Ok(w)
    }

    #[cfg(target_os = "linux")]
    fn matrix_reader(base: &str, case: HelperCase) -> io::Result<Reader> {
        let mut b = ReaderBuilder::new(base);
        if case != HelperCase::Off {
            b = b.helper(Helper::inherit());
        }
        let r = b.build()?;
        if let Some(a) = &r.map_ahead {
            match case {
                HelperCase::FailsAtOnce => a.shared.inject.error(),
                HelperCase::FailsLate => a.shared.fail_after_open.store(true, Ordering::Release),
                _ => {}
            }
        }
        Ok(r)
    }

    /// Every writer helper case against 1, 2, 16 and 64 readers with mixed helper cases: each
    /// reader gets every record once, in order, across regions and rolls.
    #[cfg(target_os = "linux")]
    #[test]
    fn every_helper_case_delivers_every_record_to_every_reader() -> anyhow::Result<()> {
        const TOTAL: u64 = 4_000;
        for writer_case in HELPER_CASES {
            for readers in [1usize, 2, 16, 64] {
                let base = format!("test_matrix_{writer_case:?}_{readers}");
                let base = base.as_str();
                cleanup_channel_files(base);
                let mut w = matrix_writer(base, writer_case)?;
                let rs = (0..readers)
                    .map(|i| matrix_reader(base, HELPER_CASES[i % 4]))
                    .collect::<io::Result<Vec<_>>>()?;
                thread::scope(|s| -> anyhow::Result<()> {
                    let handles: Vec<_> = rs
                        .into_iter()
                        .enumerate()
                        .map(|(i, mut r)| {
                            s.spawn(move || -> anyhow::Result<(HelperCase, bool)> {
                                let deadline = Instant::now() + Duration::from_secs(20);
                                while r.position() < TOTAL {
                                    match r.try_read()? {
                                        Some(m) => assert_eq!(
                                            m.header().user_meta_u64 + 1,
                                            r.position(),
                                            "reader {i}"
                                        ),
                                        None => {
                                            assert!(Instant::now() < deadline, "reader {i}");
                                            thread::yield_now();
                                        }
                                    }
                                }
                                let case = HELPER_CASES[i % 4];
                                let failed = r.helper_error().is_some();
                                match case {
                                    HelperCase::Off | HelperCase::On => assert!(!failed),
                                    HelperCase::FailsAtOnce => assert!(failed),
                                    // Only if it opened a successor ahead in time.
                                    HelperCase::FailsLate => {}
                                }
                                Ok((case, failed))
                            })
                        })
                        .collect();
                    // Slow enough that a file lasts longer than a reader helper's 5 ms look-ahead.
                    for chunk in (0..TOTAL).step_by(100) {
                        write_indexed(&mut w, chunk..chunk + 100)?;
                        thread::sleep(Duration::from_millis(3));
                    }
                    let mut late_failed = false;
                    for r in handles {
                        let (case, failed) = r.join().expect("reader thread")?;
                        late_failed |= case == HelperCase::FailsLate && failed;
                    }
                    if writer_case == HelperCase::On && readers >= 16 {
                        assert!(late_failed, "a reader helper opened a successor ahead");
                    }
                    Ok(())
                })
                .map_err(|e| e.context(format!("writer {writer_case:?}, {readers} readers")))?;
                assert!(w.file_sequence > 5, "rolled");
                let failing =
                    matches!(writer_case, HelperCase::FailsAtOnce | HelperCase::FailsLate);
                assert_eq!(w.helper_error().is_some(), failing, "{writer_case:?}");
                drop(w);
                cleanup_channel_files(base);
            }
        }
        Ok(())
    }
}
