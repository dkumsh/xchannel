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
mod region;

use channel::{
    ChannelHeader, ENDIANNESS_LE, FORMAT_VERSION, HeaderType, MessageHeader, SYSTEM_HEADER_SIZE,
    USER_HEADER_KIND_DEFAULT, USER_HEADER_SIZE,
};
pub use region::{ReadOnly, RegionMapping, Writable, page_size};

use std::fs::{File, OpenOptions, read_dir};
use std::io::{self, ErrorKind};
use std::mem::{align_of, size_of};
use std::path::{Path, PathBuf};
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ========== Constants ==========
const MESSAGE_HEADER_SIZE: usize = size_of::<MessageHeader>();
const CHANNEL_HEADER_SIZE: usize = size_of::<ChannelHeader>();

// Keep 8-byte alignment (can be revisited later).
const ALIGN: usize = align_of::<MessageHeader>(); // 8 on supported targets

// Header slot size == struct size (16B) for now.
const HEADER_SLOT: usize = MESSAGE_HEADER_SIZE;
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
        }
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
        Writer::open_or_create(
            self.path,
            self.region_size,
            self.file_roll_size,
            self.mtu,
            self.keep_files,
            self.channel_name,
            self.base_record_index,
            self.generation,
        )
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
}

impl ReaderBuilder {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            mode: ReaderMode::LateJoin,
            batch_limit: None,
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

    /// Default batch size limit used when `try_read_batch(None)` is called.
    /// `None` means unlimited.
    #[inline]
    pub fn batch_limit(mut self, limit: u16) -> Self {
        self.batch_limit = Some(limit);
        self
    }

    /// Open a Reader according to the configured mode.
    #[inline]
    pub fn build(self) -> io::Result<Reader> {
        let mut reader = Reader::open(self.path, self.mode)?;
        reader.batch_limit = self.batch_limit;
        Ok(reader)
    }
}

// ========== Channel Writer ==========
pub struct Writer {
    base_path: PathBuf,
    file_sequence: u64,
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
        if region_size < HEADER_SLOT + CHANNEL_HEADER_SIZE {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "region_size ({}) must be >= header space ({})",
                    region_size,
                    HEADER_SLOT + CHANNEL_HEADER_SIZE
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
        let sequence = find_latest_sequence(&base_path)?;
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

        Ok(Self {
            base_path,
            file_sequence: sequence,
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
        })
    }

    /// Open a specific sequence file. If new => init region0's ChannelHeader and **pre-install first user header**.
    /// Fresh-segment preparation at an explicit (partial) path. Does
    /// `set_len`, channel header init, and first user-header
    /// pre-install. The file is **not** renamed to its final name —
    /// callers do that themselves to control the publish-visibility
    /// window. `Writer::open_file` renames immediately; `roll_file`
    /// renames after the OLD Roll header is staged (committed=0) but
    /// before it's release-stored to `committed=1`, so readers
    /// observing Roll can immediately resolve NEW.
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
    ) -> io::Result<(
        File,
        RegionMapping<Writable>,
        RegionMapping<Writable>,
        u64,
        u64,
        usize,
    )> {
        let initial_len = preallocation_len(region_size, file_roll_size)?;
        let _ = std::fs::remove_file(partial_path); // tolerate prior crash
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(partial_path)?;
        file.set_len(initial_len).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "set_len({initial_len}) failed preallocating segment \
                     (file_roll_size {file_roll_size}, region_size {region_size}): {e}"
                ),
            )
        })?;
        let mut region0 = RegionMapping::create_writable(&file, 0, region_size)?;

        // 1) message header (Channel)
        let mh_ptr = region0.as_mut_ptr();
        let mh = unsafe { &mut *(mh_ptr as *mut MessageHeader) };
        mh.committed = 1;
        mh.length = CHANNEL_HEADER_SIZE as u32;
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
            (*ch_ptr)._reserved2 = [0; 23];
            (*ch_ptr).generation = generation;
        }

        // 3) current region + first user header pre-install
        let mut current_region = RegionMapping::create_writable(&file, 0, region_size)?;
        let start = align_up(MESSAGE_HEADER_SIZE + CHANNEL_HEADER_SIZE);
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
            let partial_path = make_partial_channel_file_path(base_path, sequence)?;
            // Fresh genesis segment: the builder-supplied base (0 for a brand-new
            // channel; the absolute start for a replica). When recovering an
            // existing file below, the on-disk `base_record_index` wins instead.
            let prepared = Self::prepare_segment_at(
                &partial_path,
                sequence,
                region_size,
                file_roll_size,
                mtu,
                channel_name,
                base_record_index,
                generation,
            )?;
            std::fs::rename(&partial_path, &file_path)?;
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
                        with_ch_mut(&file, region_size, |ch| {
                            ch.write_position
                                .store((next_hdr + HEADER_SLOT) as u64, Ordering::Release);
                        })?;
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
                        with_ch_mut(&file, region_size, |ch| {
                            ch.write_position
                                .store((next_hdr + HEADER_SLOT) as u64, Ordering::Release);
                        })?;
                    }
                    HeaderType::Roll | HeaderType::Channel => {
                        return Err(err_invalid_data(format!(
                            "crashed writer: unexpected header_type {:?} at write_position \
                             slot; clean up the channel files and start fresh",
                            stale_type
                        )));
                    }
                }
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

    #[inline]
    fn publish_wp(&self, pos: usize) {
        let ch = self.channel_header();
        ch.message_count.fetch_add(1, Ordering::Relaxed);
        ch.write_position.store(pos as u64, Ordering::Relaxed);
    }

    /// Store `val` to the channel header's `write_position` through
    /// the writer's already-mapped `channel_region`. Infallible —
    /// no extra mmap, no syscall. Used during roll publish to keep
    /// the entire path past the rename non-fallible.
    #[inline]
    fn store_wp_local(&self, val: u64) {
        let ch = self.channel_header();
        ch.write_position.store(val, Ordering::Relaxed);
    }

    /// Add `delta` to the channel header's `write_position` through
    /// the writer's already-mapped `channel_region`. Infallible —
    /// same rationale as `store_wp_local`.
    #[inline]
    fn fetch_add_wp_local(&self, delta: u64) -> u64 {
        let ch = self.channel_header();
        ch.write_position.fetch_add(delta, Ordering::Relaxed)
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

        // Capacity pre-check: the record (header + payload + padding +
        // next-header slot) must fit in a single region — readers and
        // writers always mmap whole regions, and a record straddling a
        // region boundary cannot be expressed in the wire format. It
        // must also fit in a fresh segment when `file_roll_size > 0`,
        // otherwise no roll can ever satisfy the reservation and the
        // loop below would roll forever, creating unbounded segment
        // files.
        let record_size = HEADER_SLOT + msg_size;
        let record_with_padding = align_up(record_size);
        let needed_total = record_with_padding + HEADER_SLOT;
        if needed_total > self.region_size {
            return Err(err_other(format!(
                "reservation size {msg_size} cannot fit in region_size {} \
                 (needs {needed_total} bytes including header + padding + next-header slot)",
                self.region_size,
            )));
        }
        if self.file_roll_size > 0 && needed_total as u64 > self.file_roll_size {
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

    /// Roll to the next file. The publish order is the load-bearing
    /// part — readers that observe the Roll marker on the OLD file
    /// will immediately try to `open()` the NEW file's final path, so
    /// the NEW file must exist on disk under that path *before* the
    /// Roll marker becomes visible.
    ///
    /// Steps:
    /// 1) Compute the Roll header position in OLD (current header slot or next region start).
    /// 2) Prepare NEW segment at `<base>.<N+1>.partial` (fully initialised: set_len, channel header, first user header). Not yet visible to readers.
    /// 3) Grow OLD if needed; jump OLD's `wp` to the next region if `leftover < HEADER_SLOT`.
    /// 4) Stage OLD's Roll header with `committed = 0` (invisible to readers).
    /// 5) Atomically `rename` NEW's `.partial` to its final name — NEW is now discoverable.
    /// 6) Release-store `committed = 1` on OLD's Roll header — readers wake here.
    /// 7) Bump OLD's `write_position` past the Roll marker.
    /// 8) Swap `self` to NEW.
    /// 9) Retention sweep: unlink the file at sequence `next_seq - keep_files`, if configured.
    ///
    /// If any step before (8) fails, `self` is unchanged and a retry
    /// can clean up the orphan `.partial` (or `WriterBuilder::build`
    /// will sweep it on next startup).
    pub fn roll_file(&mut self) -> io::Result<()> {
        // Any pending reservation refers to a slot in OLD that's about
        // to be replaced by a Roll marker (or live elsewhere in OLD
        // that we won't return to). Invalidate it so a follow-up
        // `commit` doesn't write into the NEW segment's slot 0 with
        // stale length state.
        self.pending_msg_size = None;

        // OLD context
        let old_region_size = self.region_size;
        let old_seq = self.file_sequence;
        let old_file = self.file.try_clone()?;

        // Decide roll_pos in OLD file
        let wp = self.next_hdr_pos; // current header slot
        let off = wp % old_region_size;
        let leftover = old_region_size - off;

        let (roll_pos, grow_to_end) = if leftover < HEADER_SLOT {
            // put Roll at next region start in OLD file
            let next_region_start = ((wp / old_region_size) + 1) * old_region_size;
            let next_idx = (next_region_start / old_region_size) as u64;
            let needed_end = (next_idx + 1) * old_region_size as u64;
            (next_region_start, Some(needed_end))
        } else {
            (wp, None)
        };

        // Prepare NEW segment at its `.partial` path. The file is fully
        // initialised (set_len + channel header + first user header
        // pre-installed) but invisible to readers. The rename to NEW's
        // final name happens between OLD's Roll-header staging
        // (committed=0) and the release-store of `committed=1`, so a
        // reader that observes Roll resolves NEW on the very next
        // path lookup.
        let next_seq = old_seq + 1;
        // The new segment's first record continues the absolute numbering: it is
        // OLD's base plus the count of user records written to OLD. `self` is still
        // on OLD here, so its channel header carries both. (Skip markers never
        // bumped `message_count`, so this is a pure user-record count.)
        // The generation is a property of the channel, not of a file: every segment
        // carries the same value, so it is read from OLD and stamped into NEW.
        let (new_base_record_index, generation) = {
            let ch = self.channel_header();
            (
                ch.base_record_index + ch.message_count.load(Ordering::Relaxed),
                ch.generation,
            )
        };
        let new_partial_path = make_partial_channel_file_path(&self.base_path, next_seq)?;
        let new_final_path = make_channel_file_path(&self.base_path, next_seq)?;
        let (
            new_file,
            new_channel_region,
            new_current_region,
            new_index,
            new_file_len,
            new_next_hdr,
        ) = Self::prepare_segment_at(
            &new_partial_path,
            next_seq,
            self.region_size,
            self.file_roll_size,
            self.mtu,
            &self.channel_name,
            new_base_record_index,
            generation,
        )?;

        // Publish Roll in OLD file BEFORE swapping `self` to NEW. If
        // any step here errors, `self` is still consistent on OLD.
        //
        // Once the Roll marker becomes reader-visible (the
        // release-store on `committed=1`), every remaining step must
        // be infallible — otherwise external state would say
        // "rolled" while `self` is still on OLD. We achieve that by:
        //   * Mapping OLD's Roll region exactly once and reusing it
        //     for both the staged write and the release-store.
        //   * Updating OLD's `write_position` through the writer's
        //     already-mapped `channel_region` (no fresh mmap, no
        //     syscall).
        if let Some(needed_end) = grow_to_end {
            // Grow-only: never let `set_len` shrink a preallocated
            // OLD segment back down to a region boundary.
            if needed_end > self.file_len {
                old_file.set_len(needed_end)?;
            }
        }

        // Map the OLD Roll region once and stage the Roll header
        // with `committed = 0`. Invisible to readers until the
        // release-store below.
        let old_roll_region_idx = (roll_pos / old_region_size) as u64;
        let mut old_roll_region = RegionMapping::<Writable>::create_writable(
            &old_file,
            old_roll_region_idx * old_region_size as u64,
            old_region_size,
        )?;
        let roll_off_in_region = roll_pos % old_region_size;
        let roll_hdr_ptr = {
            let bytes = old_roll_region
                .get_bytes_mut(roll_off_in_region, MESSAGE_HEADER_SIZE)
                .ok_or_else(|| err_other("roll header outside region"))?;
            bytes.as_mut_ptr() as *mut MessageHeader
        };
        unsafe {
            *roll_hdr_ptr = MessageHeader {
                committed: 0,
                length: 0,
                header_type: HeaderType::Roll as u8,
                message_type: 0,
                user_meta_u64: now_ns(),
            };
        }

        // If we jumped past leftover bytes, jump `wp` to roll_pos.
        // Infallible: goes through self.channel_region.
        if leftover < HEADER_SLOT {
            self.store_wp_local(roll_pos as u64);
        }

        // Publish NEW under its final name. Last fallible step.
        // If this fails, OLD's Roll is still committed=0, no reader
        // observes anything; cleanup unlinks the orphan partial on
        // the next `WriterBuilder::build` or via `cleanup_channel_files`.
        std::fs::rename(&new_partial_path, &new_final_path)?;

        // Release-commit the Roll marker through the already-held
        // mapping. Infallible — readers wake here.
        MessageHeader::commit(roll_hdr_ptr);

        // Advance OLD's `wp` past the Roll slot. Infallible.
        self.fetch_add_wp_local(HEADER_SLOT as u64);

        // The OLD Roll mapping is no longer needed; let it drop
        // before we overwrite `self` fields so there's no aliasing
        // with the newly-installed `current_region` if they happen
        // to be the same region 0.
        drop(old_roll_region);

        self.file_sequence = next_seq;
        self.file = new_file;
        self.channel_region = new_channel_region;
        self.current_region = new_current_region;
        self.current_region_index = new_index;
        self.file_len = new_file_len;
        self.next_hdr_pos = new_next_hdr;

        // Retention: best-effort. The roll itself has already
        // committed (Roll marker is released, NEW is on disk, `self`
        // is swapped); a failure to unlink an old segment must not
        // be returned as `Err` here because callers would interpret
        // it as "roll failed" and retry, double-rolling. Readers
        // that still have the pruned file mapped continue via the
        // inode reference until they finish it; lagging readers
        // get ENOENT on path-based open, which is the documented
        // contract for `keep_files`.
        if let Some(n) = self.keep_files
            && next_seq >= n
        {
            let prune_seq = next_seq - n;
            if let Ok(prune_path) = make_channel_file_path(&self.base_path, prune_seq) {
                let _ = std::fs::remove_file(&prune_path);
            }
        }

        Ok(())
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
            let needed_end = (next_idx + 1) * self.region_size as u64;

            // 1) Grow file and map the *next* region first.
            self.ensure_len(needed_end)?;
            let mut new_region = RegionMapping::create_writable(
                &self.file,
                next_idx * self.region_size as u64,
                self.region_size,
            )?;

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
            self.current_region = new_region;
            self.current_region_index = next_idx;
            self.next_hdr_pos = new_wp;
            // A Skip is not a user record: advance the advisory write_position but
            // do not bump `message_count` (which counts user records only).
            self.store_wp_local((new_wp + HEADER_SLOT) as u64);
            Ok(())
        } else {
            // Not even space for a header: jump straight to next region start
            let next_region_start = ((wp / self.region_size) + 1) * self.region_size;

            let next_idx = (next_region_start / self.region_size) as u64;
            let needed_end = (next_idx + 1) * self.region_size as u64;
            self.ensure_len(needed_end)?;
            self.current_region = RegionMapping::create_writable(
                &self.file,
                next_idx * self.region_size as u64,
                self.region_size,
            )?;
            self.current_region_index = next_idx;

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
    fn ensure_len(&mut self, want: u64) -> io::Result<()> {
        if want > self.file_len {
            // ftruncate to grow before any mmap touches those pages.
            self.file.set_len(want)?;
            self.file_len = want;
        }
        Ok(())
    }
}

// ========== Reader ==========

#[derive(Debug, Clone, Copy)]
pub enum ReaderMode {
    LateJoin, // start from earliest existing file
    Live,     // start from latest existing file (at next header slot)
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
    base_path: PathBuf,
    file_sequence: u64,
    file: File,
    read_position: usize,
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
}

impl Reader {
    /// Open a Reader:
    /// - LateJoin => earliest file; read_position = 0
    /// - Live => latest file; read_position = write_position (next header slot)
    ///
    /// LateJoin races with a writer configured with `keep_files(N)`: the
    /// earliest sequence returned by the directory scan can be unlinked by
    /// the writer's next roll before this call's `open()` syscall runs,
    /// surfacing as `ENOENT`. The next-lowest sequence is almost always
    /// still present, so we re-scan and try again up to
    /// `MAX_OPEN_RETRIES` times. A genuinely missing channel still fails
    /// fast — after the retries are exhausted the `ENOENT` propagates.
    /// Live mode does not retry: it targets the *latest* sequence, which
    /// the writer is actively writing to and will not unlink.
    pub fn open<P: AsRef<Path>>(path: P, mode: ReaderMode) -> io::Result<Self> {
        const MAX_OPEN_RETRIES: usize = 8;
        let base_path = path.as_ref().to_path_buf();
        let mut last_err: Option<io::Error> = None;
        for _ in 0..MAX_OPEN_RETRIES {
            let seq = match mode {
                ReaderMode::LateJoin => find_earliest_sequence(&base_path)?,
                ReaderMode::Live => find_latest_sequence(&base_path)?,
            };
            match Self::open_sequence_file(base_path.clone(), seq, mode) {
                Ok(r) => return Ok(r),
                Err(e)
                    if e.kind() == ErrorKind::NotFound && matches!(mode, ReaderMode::LateJoin) =>
                {
                    last_err = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap_or_else(|| err_other("Reader::open: exhausted retries with no error")))
    }

    /// Map region 0, validate format invariants (including that `channel_sequence`
    /// matches `expected_sequence`), and return
    /// `(read_pos, region_size, channel_name, base_record_index, mtu, generation)`.
    fn read_channel_header(
        file: &File,
        mode: ReaderMode,
        expected_sequence: u64,
    ) -> io::Result<(usize, usize, [u8; CHANNEL_NAME_MAX], u64, u32, u64)> {
        let ps = region::page_size();
        let tmp_map = RegionMapping::create_read_only(file, 0, ps)?; // map one OS page

        // Verify first record is Channel
        let mh = unsafe { &*(tmp_map.as_ptr() as *const MessageHeader) };
        let header_type = mh.parsed_header_type()?;
        if header_type != HeaderType::Channel {
            return Err(err_invalid_data(format!(
                "file has first {:?}, expected Channel header",
                header_type
            )));
        }

        let ch = get_channel_header(tmp_map.as_ptr());
        let region_size = ch.region_size as usize;
        validate_channel_header(ch, region_size, expected_sequence)?;

        let wp = ch.write_position.load(Ordering::Relaxed) as usize; // next header slot
        let read_pos = match mode {
            ReaderMode::LateJoin => 0,
            ReaderMode::Live => wp.saturating_sub(HEADER_SLOT), // header slot
        };
        let channel_name = ch.channel_name;
        let base_record_index = ch.base_record_index;
        let mtu = ch.mtu;
        let generation = ch.generation;
        drop(tmp_map);
        Ok((
            read_pos,
            region_size,
            channel_name,
            base_record_index,
            mtu,
            generation,
        ))
    }

    fn open_sequence_file(base_path: PathBuf, sequence: u64, mode: ReaderMode) -> io::Result<Self> {
        let file_path = make_channel_file_path(&base_path, sequence)?;
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(&file_path)?;

        let (read_pos, region_size, channel_name, base_record_index, mtu, generation) =
            Self::read_channel_header(&file, mode, sequence)?;
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
            base_path,
            file_sequence: sequence,
            file,
            read_position: read_pos,
            region_size_cached: region_size,
            mtu_cached: mtu,
            channel_name_cached: channel_name,
            base_record_index_cached: base_record_index,
            generation_cached: generation,
            batch_limit: None,
            batch_segs: Vec::with_capacity(DEFAULT_BATCH_SEGS_CAP),
            batch_pos: Vec::with_capacity(DEFAULT_BATCH_POS_CAP),
            maps,
        })
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
    /// the reader currently has open. Updated as the reader follows rolls. Combine
    /// with the number of user records read so far in this file to get the absolute
    /// index of any record. Genesis files report 0.
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
        let latest = find_latest_sequence(&self.base_path)?;
        let file_path = make_channel_file_path(&self.base_path, latest)?;
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(&file_path)?;
        let ps = region::page_size();
        let map = RegionMapping::create_read_only(&file, 0, ps)?;
        let mh = unsafe { &*(map.as_ptr() as *const MessageHeader) };
        if mh.parsed_header_type()? != HeaderType::Channel {
            return Err(err_invalid_data(
                "head_record_index: latest segment does not begin with a Channel header",
            ));
        }
        let ch = get_channel_header(map.as_ptr());
        Ok(ch.base_record_index + ch.message_count.load(Ordering::Relaxed))
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

        let region_size = self.region_size();
        let file = scan_file.unwrap_or(&self.file);
        let map =
            RegionMapping::create_read_only(file, region_idx * region_size as u64, region_size)?;
        self.maps.push(Arc::new(MappedRegion {
            file_sequence: scan_file_sequence,
            region_idx,
            mapping: map,
        }));
        Ok(())
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
            let first = 0;
            let last = self.maps.len() - 1;
            self.maps.swap(first, last);
            self.maps.truncate(1);
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

        let region_size = self.region_size();
        let mut scan_file_sequence = self.file_sequence;
        let mut scan_file: Option<File> = None;
        let mut cursor = self.read_position;
        let mut progressed = false;

        'scan: loop {
            // Outer loop: advance across regions/files; each iteration starts a new segment.
            let region_index = (cursor / region_size) as u64;
            let region_start = region_index as usize * region_size;
            let mut cursor_off = cursor - region_start;

            self.ensure_scan_region_mapped(scan_file.as_ref(), scan_file_sequence, region_index)?;
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
                        // Switch to the next file and continue scanning from its start.
                        let next_seq = scan_file_sequence + 1;
                        let file_path = make_channel_file_path(&self.base_path, next_seq)?;
                        let next_file = OpenOptions::new()
                            .read(true)
                            .write(false)
                            .open(&file_path)?;
                        progressed = true;
                        self.batch_segs[seg_idx].end = next_off as u32;
                        scan_file_sequence = next_seq;
                        scan_file = Some(next_file);
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

        if !progressed {
            self.batch_segs.clear();
            self.batch_pos.clear();
            return Ok(None);
        }

        self.read_position = cursor;
        self.file_sequence = scan_file_sequence;
        if let Some(file) = scan_file {
            self.file = file;
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
                        self.read_position = next_pos;
                        if next_pos.is_multiple_of(region_size) {
                            self.switch_region((next_pos / region_size) as u64)?;
                        }
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
                    self.read_position = next_pos;
                    if next_pos.is_multiple_of(region_size) {
                        self.switch_region((next_pos / region_size) as u64)?;
                        self.prune_to_current();
                    }
                    continue;
                }
                HeaderType::Roll => {
                    // `open_next_file` resets the cursor to the new segment's start; moving it
                    // here first would only strand it inside the old file if the roll failed.
                    self.open_next_file()?;
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
    /// Uses adaptive sleep-based backoff (1 µs → 2 → 4 → ... up to 10 ms
    /// cap). At high publish rates the loop catches the next message in
    /// the spinning regime; when the channel is idle the thread sleeps
    /// and worst-case wake-up latency is bounded by the cap.
    ///
    /// This is a synchronous helper. **Do not call from an async runtime
    /// task** — it uses `std::thread::sleep` and will block the executor
    /// thread. Async callers should compose `try_read` with their
    /// runtime's own sleep primitive, or write an equivalent polling
    /// helper around the runtime's sleep.
    ///
    /// `timeout = None` waits indefinitely.
    pub fn wait_for_message(&mut self, timeout: Option<Duration>) -> io::Result<bool> {
        const INITIAL_BACKOFF_US: u64 = 1;
        const MAX_BACKOFF_US: u64 = 10_000;

        let deadline = timeout.map(|d| Instant::now() + d);
        let mut backoff_us: u64 = INITIAL_BACKOFF_US;

        loop {
            if self.poll_for_user_message()? {
                return Ok(true);
            }
            if let Some(d) = deadline
                && Instant::now() >= d
            {
                return Ok(false);
            }
            let mut sleep_us = backoff_us;
            if let Some(d) = deadline {
                let remaining = d.saturating_duration_since(Instant::now());
                let remaining_us = remaining.as_micros().min(u64::MAX as u128) as u64;
                if remaining_us == 0 {
                    return Ok(false);
                }
                sleep_us = sleep_us.min(remaining_us);
            }
            thread::sleep(Duration::from_micros(sleep_us));
            backoff_us = (backoff_us * 2).min(MAX_BACKOFF_US);
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
                    self.read_position = next_pos;
                    if next_pos.is_multiple_of(region_size) {
                        self.switch_region((next_pos / region_size) as u64)?;
                        self.prune_to_current();
                    }
                    continue;
                }
                HeaderType::Roll => {
                    // See the Roll arm in `scan_to_user_record`: the cursor moves only once
                    // the next segment is actually open.
                    self.open_next_file()?;
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
        let region_size = self.region_size();
        let new_map =
            RegionMapping::create_read_only(&self.file, idx * region_size as u64, region_size)?;
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
    /// segment not visible yet, retention having removed it, or one of the checks below
    /// refusing it — leaves the reader exactly where it was, so the error can be returned
    /// again, or the same call retried once the segment appears.
    fn open_next_file(&mut self) -> io::Result<()> {
        // The absolute index the next segment must begin at, computed from the one we are
        // leaving: a roll stamps the new file's base as the old file's `base + message_count`.
        // Read from the file we still hold open, so this works even after retention unlinked
        // it (readers finish a pruned file through their inode reference).
        let expected_base = {
            let map = RegionMapping::create_read_only(&self.file, 0, region::page_size())?;
            let ch = get_channel_header(map.as_ptr());
            ch.base_record_index + ch.message_count.load(Ordering::Relaxed)
        };
        let next_sequence = self.file_sequence + 1;
        let file_path = make_channel_file_path(&self.base_path, next_sequence)?;
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(&file_path)?;

        let region_size = self.region_size();
        let region0 = RegionMapping::create_read_only(&file, 0, region_size)?;
        let mh = unsafe { &*(region0.as_ptr() as *const MessageHeader) };
        if mh.parsed_header_type()? != HeaderType::Channel {
            return Err(err_other("next file missing Channel header"));
        }
        let ch = get_channel_header(region0.as_ptr());
        validate_channel_header(ch, region_size, next_sequence)?;
        // The next segment must continue the absolute numbering. Anything else means this
        // file did not come from the series we have been reading — segments from two
        // different logs sharing a directory, an out-of-order or hand-copied file, or a
        // channel that was deleted and rebuilt at the same path while we held an unlinked
        // file open (the rebuilt series restarts at sequence 0 and reuses these very
        // filenames, so nothing else about it looks wrong). Refuse rather than splice.
        if ch.base_record_index != expected_base {
            return Err(err_invalid_data(format!(
                "base_record_index discontinuity: segment {} starts at {} but the previous \
                 segment ends at {} (segments from a different series?)",
                next_sequence, ch.base_record_index, expected_base
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

        // Past the last fallible step: commit the new segment in one go.
        self.file_sequence = next_sequence;
        // Refresh cached channel_name from the new file (the bytes are authoritative
        // even though in practice the name carries across rolls).
        self.channel_name_cached = ch.channel_name;
        // Each rolled file has its own base; refresh so `base_record_index()` tracks it.
        self.base_record_index_cached = ch.base_record_index;
        self.file = file;
        self.read_position = 0;
        self.maps.clear();
        self.maps.push(Arc::new(MappedRegion {
            file_sequence: next_sequence,
            region_idx: 0,
            mapping: region0,
        }));
        Ok(())
    }
}

fn now_ns() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
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

/// Suffix appended to a segment file while it is being prepared by
/// the writer; renamed away atomically once initialised. Files with
/// this suffix are invisible to `find_all_sequences` (its u64
/// suffix parse rejects anything non-numeric).
const PARTIAL_SUFFIX: &str = "partial";

/// `<base>.partial` (seq 0) or `<base>.<N>.partial` (seq N > 0).
fn make_partial_channel_file_path(base_path: &Path, sequence: u64) -> io::Result<PathBuf> {
    let final_path = make_channel_file_path(base_path, sequence)?;
    let file_name = final_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| err_other(format!("Cannot get file name from path {:?}", final_path)))?;
    let mut pb = final_path.clone();
    pb.set_file_name(format!("{}.{}", file_name, PARTIAL_SUFFIX));
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

/// Match `<base>.partial` or `<base>.<N>.partial`. The numeric
/// parse on `<N>` keeps unrelated siblings (e.g.
/// `<base>.notes.partial`) out of the sweep.
fn is_partial_segment_name(name: &str, base_name: &str) -> bool {
    let base_partial = format!("{}.{}", base_name, PARTIAL_SUFFIX);
    if name == base_partial {
        return true;
    }
    let dotted = format!("{}.", base_name);
    let suffix = format!(".{}", PARTIAL_SUFFIX);
    name.strip_prefix(&dotted)
        .and_then(|m| m.strip_suffix(&suffix))
        .and_then(|n| n.parse::<u64>().ok())
        .is_some()
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

    // Remove the base file (sequence 0) and the seq-0 partial sibling.
    let _ = fs::remove_file(base_path);
    if let Some(name) = base_path.file_name().and_then(|s| s.to_str()) {
        let mut partial0 = base_path.to_path_buf();
        partial0.set_file_name(format!("{}.{}", name, PARTIAL_SUFFIX));
        let _ = fs::remove_file(partial0);
    }

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
        // same geometry — every other check passes.
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
            err.to_string().contains("discontinuity"),
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
            0, // base_record_index: genesis
            0, // generation: unset
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
            0, // base_record_index: genesis
            0, // generation: unset
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
            0, // generation: unset
        )?;

        // Choose len so that after header + payload the aligned end is region - header_size.
        let record_with_padding = region - HEADER_SLOT;
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
            0, // base_record_index: genesis
            0, // generation: unset
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
            0, // base_record_index: genesis
            0, // generation: unset
        )?;

        let start = align_up(MESSAGE_HEADER_SIZE + CHANNEL_HEADER_SIZE);
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
            0, // base_record_index: genesis
            0, // generation: unset
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
            0, // base_record_index: genesis
            0, // generation: unset
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
            0, // base_record_index: genesis
            0, // generation: unset
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

        let start = align_up(MESSAGE_HEADER_SIZE + CHANNEL_HEADER_SIZE);
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

        let start = align_up(MESSAGE_HEADER_SIZE + CHANNEL_HEADER_SIZE);
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
        // 144 bytes of region-0 overhead (16-byte Channel MessageHeader +
        // 128-byte ChannelHeader), payload >= 3897 forces a roll on a
        // subsequent 8-byte message. 3904 is the next aligned size, chosen
        // so the post-big next-header slot lands at 4064 (identical roll
        // geometry to the pre-128-byte-header layout).
        let big = vec![0x77u8; 3904];
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
        //    region_size=4096 and big payload=3904: the Skip sits at
        //    offset 4064 with skip_len=16 (total Skip record = 32 bytes,
        //    filling exactly to the region boundary). Pre-roll wp was
        //    4080 (set by the big message's publish_wp at the end of
        //    commit). Post-roll wp is 4112 (set by roll_over_region's
        //    publish_wp). The rewind is exactly the Skip record size,
        //    which is also the publish_wp delta inside roll_over_region.
        rewind_write_position_on_disk(base, 32)?;

        // 3) Reopen — must succeed (recovery follows the Skip into region 1).
        let mut w = WriterBuilder::new(base).region_size(region_size).build()?;

        // 4) Recovered writer writes a new message in region 1.
        let buf = w.try_reserve(small_payload.len())?;
        buf.copy_from_slice(&small_payload);
        w.commit(2, small_payload.len() as u32, 0)?;
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

        // Reader joins late-ish and is allowed to lag.
        thread::sleep(Duration::from_millis(20));
        let mut r = Reader::open(base, ReaderMode::LateJoin)?;
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
}
