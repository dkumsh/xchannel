//! xchannel: mmap-backed IPC channels with rolling files.
//!
//! # Overview
//! - Regionized file layout; region 0 starts with a `MessageHeader(Channel)` followed by `ChannelHeader`.
//! - **Pre-header pipeline** for user records:
//!   For record *i*: header(i) is pre-installed (committed=0), writer copies payload(i) after it,
//!   then fills header(i) and sets committed=1 (Release), and finally pre-installs header(i+1).
//! - Special markers: `Skip` (pad to next region), `Roll` (file rolled).
//!
//! # Safety
//! Writers produce `&mut` references into an mmap; do **not** run a reader in the
//! same process concurrently with a writer to the same file/region. For cross-process
//! IPC this is fine. Publishing uses `Release` and reading uses `Acquire`.

mod channel;
pub mod migrate;
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
/// `user_header_kind` must equal `USER_HEADER_KIND_DEFAULT`; the wire field is
/// reserved for future user-defined layouts and has no public opt-in today.
fn validate_channel_header(ch: &ChannelHeader, expected_region_size: usize) -> io::Result<()> {
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
    Ok(())
}

/// A "pre-installed" header is what `commit()` writes one step ahead of itself:
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

#[inline]
fn store_wp(file: &File, region_size: usize, val: u64) -> io::Result<()> {
    with_ch_mut(file, region_size, |ch| {
        ch.write_position.store(val, Ordering::Relaxed);
    })
}

#[inline]
fn fetch_add_wp(file: &File, region_size: usize, delta: u64) -> io::Result<u64> {
    let mut out = 0u64;
    with_ch_mut(file, region_size, |ch| {
        out = ch.write_position.fetch_add(delta, Ordering::Relaxed);
    })?;
    Ok(out)
}

#[inline]
fn write_roll_header_at(
    file: &File,
    region_size: usize,
    pos: usize,
    user_meta_u64: u64,
) -> io::Result<()> {
    let hdr: MessageHeader = MessageHeader {
        committed: 0,
        length: 0,
        header_type: HeaderType::Roll as u8,
        message_type: 0,
        user_meta_u64,
    };
    let ridx = (pos / region_size) as u64;
    let off = pos % region_size;
    let mut rm = RegionMapping::create_writable(file, ridx * region_size as u64, region_size)?;
    if let Some(bytes) = rm.get_bytes_mut(off, MESSAGE_HEADER_SIZE) {
        unsafe {
            let hdr_ptr = bytes.as_mut_ptr() as *mut MessageHeader;
            *hdr_ptr = hdr;
            MessageHeader::commit(hdr_ptr);
        }
        Ok(())
    } else {
        Err(err_other("write_roll_header_at: failed to get bytes"))
    }
}

// ========== Builders ==========
/// Maximum bytes available for a channel name in `ChannelHeader`.
pub const CHANNEL_NAME_MAX: usize = 20;

#[derive(Clone, Debug)]
pub struct WriterBuilder {
    path: PathBuf,
    region_size: usize,
    file_roll_size: u64,
    mtu: u64,
    keep_files: Option<u64>,
    channel_name: [u8; CHANNEL_NAME_MAX],
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
        }
    }

    #[inline]
    pub fn region_size(mut self, region_size: usize) -> Self {
        self.region_size = region_size;
        self
    }
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
    /// name is UTF-8 bytes, up to `CHANNEL_NAME_MAX` (20) bytes; longer
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
    #[inline]
    pub fn keep_files(mut self, n: u64) -> Self {
        assert!(n >= 1, "WriterBuilder::keep_files: n must be >= 1");
        self.keep_files = Some(n);
        self
    }

    /// Create or open the latest sequence file and return a Writer.
    #[inline]
    pub fn build(self) -> io::Result<Writer> {
        Writer::open_or_create(
            self.path,
            self.region_size,
            self.file_roll_size,
            self.mtu,
            self.keep_files,
            self.channel_name,
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
}

impl Writer {
    /// Create/open the latest channel file.
    /// Validates that `region_size` is a multiple of OS page size and large enough.
    fn open_or_create<P: AsRef<Path>>(
        path: P,
        region_size: usize,
        file_roll_size: u64,
        mtu: u64,
        keep_files: Option<u64>,
        channel_name: [u8; CHANNEL_NAME_MAX],
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
            Self::open_file(&base_path, sequence, region_size, mtu, &channel_name)?;

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
        })
    }

    /// Open a specific sequence file. If new => init region0's ChannelHeader and **pre-install first user header**.
    #[allow(clippy::type_complexity)]
    fn open_file(
        base_path: &Path,
        sequence: u64,
        region_size: usize,
        mtu: u64,
        channel_name: &[u8; CHANNEL_NAME_MAX],
    ) -> io::Result<(
        File,
        RegionMapping<Writable>,
        RegionMapping<Writable>,
        u64,
        u64,
        usize,
    )> {
        let file_path = make_channel_file_path(base_path, sequence)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&file_path)?;

        let meta = file.metadata()?;
        if meta.len() == 0 {
            // New file => initialize region 0
            file.set_len(region_size as u64)?;
            let mut region0 = RegionMapping::create_writable(&file, 0, region_size)?;

            // 1) message header (Channel)
            let mh_ptr = region0.as_mut_ptr();
            let mh = unsafe { &mut *(mh_ptr as *mut MessageHeader) };
            mh.committed = 1; // committed system record
            mh.length = CHANNEL_HEADER_SIZE as u32;
            mh.header_type = HeaderType::Channel as u8;
            mh.message_type = 0;
            mh.user_meta_u64 = 0;

            // 2) channel header
            let ch_ptr = unsafe { mh_ptr.add(MESSAGE_HEADER_SIZE) as *mut ChannelHeader };
            unsafe {
                (*ch_ptr).write_position = AtomicU64::new(0); // set below after pre-install
                (*ch_ptr).message_count = AtomicU64::new(1);
                (*ch_ptr).channel_sequence = sequence;
                (*ch_ptr).region_size = region_size as u32;
                (*ch_ptr).mtu = mtu as u32;
                (*ch_ptr).format_version = FORMAT_VERSION;
                (*ch_ptr).endianness = ENDIANNESS_LE;
                (*ch_ptr).system_header_size = SYSTEM_HEADER_SIZE;
                (*ch_ptr).user_header_size = USER_HEADER_SIZE;
                (*ch_ptr)._reserved = [0; 3];
                (*ch_ptr).user_header_kind = USER_HEADER_KIND_DEFAULT;
                (*ch_ptr).channel_name = *channel_name;
            }

            // 3) current region and first user header pre-install
            let mut current_region = RegionMapping::create_writable(&file, 0, region_size)?;
            let start = align_up(MESSAGE_HEADER_SIZE + CHANNEL_HEADER_SIZE);
            // pre-install first user header (committed=0)
            if let Some(h) = current_region.get_bytes_mut(start, MESSAGE_HEADER_SIZE) {
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
                return Err(err_other("open_file: cannot pre-install first header"));
            }

            // Publish write_position to the **next header slot** (start)
            with_ch_mut(&file, region_size, |ch| {
                ch.write_position
                    .store((start + HEADER_SLOT) as u64, Ordering::Release);
            })?;

            Ok((file, region0, current_region, 0, region_size as u64, start))
        } else {
            // Existing file: adopt next header slot from write_position
            let mut file_len = meta.len();
            let region0 = RegionMapping::create_writable(&file, 0, region_size)?;
            let ch = get_channel_header(region0.as_ptr());
            validate_channel_header(ch, region_size)?;

            // wp denotes the **next header slot offset**
            let wp_payload = ch.write_position.load(Ordering::Relaxed) as usize;
            let mut next_hdr = wp_payload.saturating_sub(HEADER_SLOT);
            let mut region_index = (next_hdr / region_size) as u64;

            // Ensure the file actually covers the region we’re about to map.
            let needed_end = (region_index + 1) as u64 * region_size as u64;
            if needed_end > file_len {
                file.set_len(needed_end)?;
                file_len = needed_end;
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

    #[inline]
    fn publish_wp(&self, pos: usize) {
        let ch = self.channel_header();
        ch.message_count.fetch_add(1, Ordering::Relaxed);
        ch.write_position.store(pos as u64, Ordering::Relaxed);
    }

    /// Reserve space for a message payload of length `msg_size` placed **after** a pre-installed header.
    /// Returns a mutable slice the caller can fill, or `None` on failure (e.g. MTU/roll).
    pub fn try_reserve(&mut self, msg_size: usize) -> io::Result<&mut [u8]> {
        if self.mtu > 0 && msg_size as u64 > self.mtu {
            return Err(err_other("MTU exceeded"));
        }

        loop {
            let wp = self.next_hdr_pos; // header slot for this record
            debug_assert_eq!(wp % ALIGN, 0, "next header must be 8-byte aligned");

            // Region-local offsets
            let off = wp % self.region_size;

            // Layout now:
            //   [HeaderSlot (16B)] [payload(msg_size)] [padding(to ALIGN)]
            // We also require room for the **next** header slot immediately after this record.
            let record_size = HEADER_SLOT + msg_size;
            let record_with_padding = align_up(record_size);
            let needed_total = record_with_padding + HEADER_SLOT; // +next header slot

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

            // There is enough room. Return the payload slice after the header slot.
            let payload_off = off + HEADER_SLOT;
            return self
                .current_region
                .get_bytes_mut(payload_off, msg_size)
                .ok_or_else(|| err_other("message crosses region boundary"));
        }
    }

    /// Commit the message after filling the payload slice returned by `try_reserve`.
    /// Fills the pre-installed header at `next_hdr_pos`, sets committed=1 (Release),
    /// and **pre-installs** the next header slot (committed=0).
    pub fn commit(&mut self, msg_type: u16, length: u32, user_meta_u64: u64) -> io::Result<()> {
        let hdr_off = self.next_hdr_pos % self.region_size;

        // 1) Fill fields (committed=0)
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

        // 2) Publish (commit flag last)
        MessageHeader::commit(hdr_ptr);

        // 3) Advance to next header slot after payload+pad
        let payload_end = self.next_hdr_pos + HEADER_SLOT + length as usize;
        let next_pos = align_up(payload_end);
        self.next_hdr_pos = next_pos;

        // 4) Pre-install next header slot (committed=0)
        let next_off = next_pos % self.region_size;
        if let Some(bytes) = self
            .current_region
            .get_bytes_mut(next_off, MESSAGE_HEADER_SIZE)
        {
            unsafe {
                *(bytes.as_mut_ptr() as *mut MessageHeader) = MessageHeader {
                    committed: 0,
                    header_type: HeaderType::User as u8,
                    message_type: 0,
                    length: 0,
                    user_meta_u64: 0,
                };
            }
        } else {
            return Err(err_other("Failed to pre-install next header"));
        }

        // 5) Publish write_position = *payload start* of the next record
        let next_payload = next_pos + HEADER_SLOT;
        self.publish_wp(next_payload);

        Ok(())
    }

    /// Explicitly roll to the next file (writes a `Roll` marker first in the OLD file).
    /// Steps:
    /// 1) compute where to put the Roll header in OLD file (current header slot or next region start),
    /// 2) create & initialize the NEW file and pre-install its first header,
    /// 3) switch Writer to the new file,
    /// 4) publish Roll header into the OLD file and bump its write_position.
    pub fn roll_file(&mut self) -> io::Result<()> {
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

        // NEW file: open and pre-install its first header
        let next_seq = old_seq + 1;
        let (
            new_file,
            new_channel_region,
            new_current_region,
            new_index,
            new_file_len,
            new_next_hdr,
        ) = Self::open_file(
            &self.base_path,
            next_seq,
            self.region_size,
            self.mtu,
            &self.channel_name,
        )?;

        // Switch writer to NEW file
        self.file_sequence = next_seq;
        self.file = new_file;
        self.channel_region = new_channel_region;
        self.current_region = new_current_region;
        self.current_region_index = new_index;
        self.file_len = new_file_len;
        self.next_hdr_pos = new_next_hdr;
        // Publish wp for new file (Release already done in open_file new-case)

        // Publish Roll in OLD file
        if let Some(needed_end) = grow_to_end {
            old_file.set_len(needed_end)?; // ensure next region exists
        }

        // If we jumped to next region earlier in OLD file, put wp there first
        if leftover < HEADER_SLOT {
            store_wp(&old_file, old_region_size, roll_pos as u64)?;
        }

        // Now map & write the Roll header
        write_roll_header_at(&old_file, old_region_size, roll_pos, now_ns())?;
        // Bump old wp by one header
        fetch_add_wp(&old_file, old_region_size, HEADER_SLOT as u64)?;

        // Retention: if `keep_files(N)` was configured, the file at sequence
        // `next_seq - N` (if any) is now beyond the retention window. Unlink
        // it; readers that still have it mapped keep their inode reference
        // until they finish that file.
        if let Some(n) = self.keep_files
            && next_seq >= n
        {
            let prune_seq = next_seq - n;
            let prune_path = make_channel_file_path(&self.base_path, prune_seq)?;
            match std::fs::remove_file(&prune_path) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    fn roll_over_region(&mut self) -> io::Result<()> {
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
            self.publish_wp(new_wp + HEADER_SLOT);
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
            self.publish_wp(next_region_start + HEADER_SLOT);
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
}

/// Borrowed view over a batch of user messages.
pub struct MessageBatch<'a> {
    segs: &'a [BatchSeg],
    pos: &'a [MsgPos],
    maps: &'a [MappedRegion],
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
}

pub struct Reader {
    base_path: PathBuf,
    file_sequence: u64,
    file: File,
    read_position: usize,
    region_size_cached: usize,
    channel_name_cached: [u8; CHANNEL_NAME_MAX],
    batch_limit: Option<u16>,
    batch_segs: Vec<BatchSeg>,
    batch_pos: Vec<MsgPos>,
    maps: Vec<MappedRegion>, // last entry is current; older entries kept for batch segments
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

    /// Map region 0, validate v1 format invariants, and return
    /// `(read_pos, region_size, channel_name)`.
    fn read_channel_header(
        file: &File,
        mode: ReaderMode,
    ) -> io::Result<(usize, usize, [u8; CHANNEL_NAME_MAX])> {
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
        validate_channel_header(ch, region_size)?;

        let wp = ch.write_position.load(Ordering::Relaxed) as usize; // next header slot
        let read_pos = match mode {
            ReaderMode::LateJoin => 0,
            ReaderMode::Live => wp.saturating_sub(HEADER_SLOT), // header slot
        };
        let channel_name = ch.channel_name;
        drop(tmp_map);
        Ok((read_pos, region_size, channel_name))
    }

    fn open_sequence_file(base_path: PathBuf, sequence: u64, mode: ReaderMode) -> io::Result<Self> {
        let file_path = make_channel_file_path(&base_path, sequence)?;
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(&file_path)?;

        let (read_pos, region_size, channel_name) = Self::read_channel_header(&file, mode)?;
        let region_index = (read_pos / region_size) as u64;
        let current_region =
            RegionMapping::create_read_only(&file, region_index * region_size as u64, region_size)?;
        let mut maps = Vec::with_capacity(DEFAULT_BATCH_MAPS_CAP);
        maps.push(MappedRegion {
            file_sequence: sequence,
            region_idx: region_index,
            mapping: current_region,
        });

        Ok(Self {
            base_path,
            file_sequence: sequence,
            file,
            read_position: read_pos,
            region_size_cached: region_size,
            channel_name_cached: channel_name,
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

    #[inline(always)]
    fn region_size(&self) -> usize {
        self.region_size_cached
    }

    #[inline]
    fn current_map(&self) -> Option<&MappedRegion> {
        self.maps.last()
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
        self.maps.push(MappedRegion {
            file_sequence: scan_file_sequence,
            region_idx,
            mapping: map,
        });
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
                    let msg_map_idx = self.maps.len() - 1;
                    self.read_position = next_pos;
                    if next_pos.is_multiple_of(region_size) {
                        self.switch_region((next_pos / region_size) as u64)?;
                    }
                    let msg = MessageRef {
                        mapping: &self.maps[msg_map_idx].mapping,
                        header_offset: off,
                        payload_len: hdr.payload_len,
                    };
                    return Ok(Some(msg));
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
                    self.read_position = next_pos;
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
                    self.read_position = next_pos;
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
            });
        }

        let payload_len = mh.length as usize;
        Ok(ScannedHeader {
            is_committed: true,
            header_type: mh.parsed_header_type()?,
            payload_len,
            total_len: HEADER_SLOT + payload_len,
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
        self.maps.push(MappedRegion {
            file_sequence: self.file_sequence,
            region_idx: idx,
            mapping: new_map,
        });
        Ok(())
    }

    fn open_next_file(&mut self) -> io::Result<()> {
        self.file_sequence += 1;
        let file_path = make_channel_file_path(&self.base_path, self.file_sequence)?;
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
        validate_channel_header(ch, region_size)?;
        // Refresh cached channel_name from the new file (the bytes are authoritative
        // even though in practice the name carries across rolls).
        self.channel_name_cached = ch.channel_name;

        self.file = file;
        self.read_position = 0;
        self.maps.clear();
        self.maps.push(MappedRegion {
            file_sequence: self.file_sequence,
            region_idx: 0,
            mapping: region0,
        });
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

    // Remove the base file (sequence 0).
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
        if let Some(rest) = name.strip_prefix(&prefix)
            && rest.parse::<u64>().is_ok()
        {
            let _ = fs::remove_file(ent.path());
        }
    }
}

// ========== TESTS ==========
#[cfg(test)]
mod tests {
    use super::*;

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
        let mut w =
            Writer::open_or_create(base, region, file_roll_size, 0, None, [0; CHANNEL_NAME_MAX])?;

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

        const NAME: &str = "market-data-feed";

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
        // 80 bytes of region-0 overhead, payload >= 3961 forces a roll
        // on a subsequent 8-byte message. 3968 is the next aligned size.
        let big = vec![0x77u8; 3968];
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
        //    region_size=4096 and big payload=3968: the Skip sits at
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
}
