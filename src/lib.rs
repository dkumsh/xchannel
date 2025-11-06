//! xchannel: mmap-backed IPC channels with rolling files.
//!
//! # Overview
//! - Regionized file layout; region 0 starts with a `MessageHeader(Channel)` followed by `ChannelHeader`.
//! - Messages are written as `MessageHeader(User)` + payload.
//! - Special markers: `Skip` (pad to next region), `Roll` (file rolled).
//! - Active file naming: writer always writes to `<base>[.<seq>].current`; when a file is rolled
//!   or the writer drops, the `.current` suffix is atomically removed to mark the file complete.
//!
//! # Safety
//! Writers produce `&mut` references into an mmap; do **not** run a reader in the
//! same process concurrently with a writer to the same file/region. For cross-process
//! IPC this is fine. Publishing uses `Release` and reading uses `Acquire/SeqCst`.

mod channel;
mod region;

use channel::{ChannelHeader, HeaderType, MessageHeader};
pub use region::{ReadOnly, RegionMapping, Writable, page_size};

use std::fs::{File, OpenOptions, read_dir, rename};
use std::io::{self, ErrorKind};
use std::mem::{align_of, size_of};
use std::path::{Path, PathBuf};
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// ========== Constants ==========
const MESSAGE_HEADER_SIZE: usize = size_of::<MessageHeader>();
const CHANNEL_HEADER_SIZE: usize = size_of::<ChannelHeader>();

// New: alignment controls
const ALIGN: usize = align_of::<MessageHeader>(); // 8 on all supported targets
#[inline]
fn align_up(x: usize) -> usize {
    (x + (ALIGN - 1)) & !(ALIGN - 1)
}

#[inline]
fn err_other<S: Into<String>>(s: S) -> io::Error {
    io::Error::other(s.into())
}

#[inline]
fn get_channel_header_ptr(region_ptr: *const u8) -> *const ChannelHeader {
    unsafe { region_ptr.add(MESSAGE_HEADER_SIZE) as *const ChannelHeader }
}
fn get_channel_header<'a>(region_ptr: *const u8) -> &'a ChannelHeader {
    unsafe { &*get_channel_header_ptr(region_ptr) }
}

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
        ch.write_position.store(val, Ordering::SeqCst);
    })
}

#[inline]
fn fetch_add_wp(file: &File, region_size: usize, delta: u64) -> io::Result<u64> {
    let mut out = 0u64;
    with_ch_mut(file, region_size, |ch| {
        out = ch.write_position.fetch_add(delta, Ordering::Release);
    })?;
    Ok(out)
}

#[inline]
fn write_header_at(
    file: &File,
    region_size: usize,
    pos: usize,
    hdr: MessageHeader,
) -> io::Result<()> {
    let ridx = (pos / region_size) as u64;
    let off = pos % region_size;
    let mut rm = RegionMapping::create_writable(file, ridx * region_size as u64, region_size)?;
    if let Some(bytes) = rm.get_bytes_mut(off, MESSAGE_HEADER_SIZE) {
        unsafe {
            *(bytes.as_mut_ptr() as *mut MessageHeader) = hdr;
        }
        Ok(())
    } else {
        Err(err_other("write_header_at: failed to get bytes"))
    }
}

#[inline]
fn emit_skip_at(file: &File, region_size: usize, pos: usize, skip_len: usize) -> io::Result<()> {
    write_header_at(
        file,
        region_size,
        pos,
        MessageHeader {
            length: skip_len as u32,
            header_type: HeaderType::Skip,
            _reserved: 0,
            message_type: 0,
            timestamp_ns: 0,
        },
    )?;
    fetch_add_wp(
        file,
        region_size,
        MESSAGE_HEADER_SIZE as u64 + skip_len as u64,
    )?;
    // message_count bump:
    with_ch_mut(file, region_size, |ch| ch.message_count += 1)?;
    Ok(())
}

// ========== Builders ==========
#[derive(Clone, Debug)]
pub struct WriterBuilder {
    path: PathBuf,
    region_size: usize,
    file_roll_size: u64,
    mtu: u64,
}

impl WriterBuilder {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            region_size: 1024 * 1024, // default: 1M
            file_roll_size: 0,        // default: no file rolling
            mtu: 0,                   // default: no MTU limit
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

    /// Create or open the latest sequence file and return a Writer.
    #[inline]
    pub fn build(self) -> io::Result<Writer> {
        Writer::open_or_create(self.path, self.region_size, self.file_roll_size, self.mtu)
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
}

impl ReaderBuilder {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            mode: ReaderMode::LateJoin,
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

    /// Open a Reader according to the configured mode.
    #[inline]
    pub fn build(self) -> io::Result<Reader> {
        Reader::open(self.path, self.mode)
    }
}

// ========== Channel Writer ==========
pub struct Writer {
    base_path: PathBuf,
    file_sequence: u64,
    file: File,

    channel_region: RegionMapping<Writable>,
    current_region: RegionMapping<Writable>,
    current_region_index: u64,

    region_size: usize,
    file_roll_size: u64,
    mtu: u64,
}

impl Writer {
    /// Create/open the latest channel file.
    /// Validates that `region_size` is a multiple of OS page size and large enough.
    fn open_or_create<P: AsRef<Path>>(
        path: P,
        region_size: usize,
        file_roll_size: u64,
        mtu: u64,
    ) -> io::Result<Self> {
        // Validate region invariants
        let ps = region::page_size();
        // Region must be a multiple of the OS page size (and the header alignment too)
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
        if region_size < MESSAGE_HEADER_SIZE + CHANNEL_HEADER_SIZE {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "region_size ({}) must be >= header space ({})",
                    region_size,
                    MESSAGE_HEADER_SIZE + CHANNEL_HEADER_SIZE
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
        let (file, channel_region, current_region, current_region_index) =
            Self::open_file(&base_path, sequence, region_size, mtu)?;

        Ok(Self {
            base_path,
            file_sequence: sequence,
            file,
            channel_region,
            current_region,
            current_region_index,
            region_size,
            file_roll_size,
            mtu,
        })
    }

    /// Open a specific sequence file. If new => init region0's ChannelHeader. Else read existing.
    fn open_file(
        base_path: &Path,
        sequence: u64,
        region_size: usize,
        mtu: u64,
    ) -> io::Result<(File, RegionMapping<Writable>, RegionMapping<Writable>, u64)> {
        // Prefer current (active) variant. If only the completed file exists, promote it.
        let current_path = make_current_file_path(base_path, sequence)?;
        let completed_path = make_channel_file_path(base_path, sequence)?;
        let (open_path, newly_created) = if path_exists(&current_path) {
            (current_path.clone(), false)
        } else if path_exists(&completed_path) {
            rename(&completed_path, &current_path)?;
            (current_path.clone(), false)
        } else {
            (current_path.clone(), true)
        };
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(newly_created)
            .truncate(false)
            .open(&open_path)?;

        let meta = file.metadata()?;
        if meta.len() == 0 {
            // New file => initialize region 0
            file.set_len(region_size as u64)?;
            let mut region0 = RegionMapping::create_writable(&file, 0, region_size)?;

            // 1) message header
            let mh_ptr = region0.as_mut_ptr();
            let mh = unsafe { &mut *(mh_ptr as *mut MessageHeader) };
            mh.length = CHANNEL_HEADER_SIZE as u32;
            mh.header_type = HeaderType::Channel;
            mh._reserved = 0;
            mh.message_type = 0;
            mh.timestamp_ns = 0;

            // 2) channel header
            let ch_ptr = unsafe { mh_ptr.add(MESSAGE_HEADER_SIZE) as *mut ChannelHeader };
            unsafe {
                (*ch_ptr).write_position =
                    AtomicU64::new((MESSAGE_HEADER_SIZE + CHANNEL_HEADER_SIZE) as u64);
                (*ch_ptr).message_count = 1;
                (*ch_ptr).region_size = region_size as u32;
                (*ch_ptr).mtu = mtu as u32;
                (*ch_ptr).channel_sequence = sequence;
                (*ch_ptr).channel_name = [0; 64];
            }

            let current_region = RegionMapping::create_writable(&file, 0, region_size)?;
            Ok((file, region0, current_region, 0))
        } else {
            // Existing => verify region size and position
            let region0 = RegionMapping::create_writable(&file, 0, region_size)?;
            let ch = get_channel_header(region0.as_ptr());
            if ch.region_size as usize != region_size {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "region_size mismatch with existing file",
                ));
            }

            let wp = ch.write_position.load(Ordering::SeqCst) as usize;
            let region_index = (wp / region_size) as u64;
            let current_region = RegionMapping::create_writable(
                &file,
                region_index * region_size as u64,
                region_size,
            )?;
            Ok((file, region0, current_region, region_index))
        }
    }

    #[inline]
    fn channel_header(&self) -> &ChannelHeader {
        unsafe { &*(self.channel_region.as_ptr().add(MESSAGE_HEADER_SIZE) as *const ChannelHeader) }
    }
    #[inline]
    fn channel_header_mut(&mut self) -> &mut ChannelHeader {
        let ptr = unsafe { self.channel_region.as_mut_ptr().add(MESSAGE_HEADER_SIZE) }
            as *mut ChannelHeader;
        unsafe { &mut *ptr }
    }
    #[inline]
    fn load_wp_usize(&self) -> usize {
        self.channel_header().write_position.load(Ordering::Acquire) as usize
    }

    /// Reserve space for a message payload of length `msg_size` + align padding.
    /// Returns a mutable slice the caller can fill, or `None` on failure (e.g. MTU/roll).
    pub fn try_reserve(&mut self, msg_size: usize) -> Option<&mut [u8]> {
        if self.mtu > 0 && msg_size as u64 > self.mtu {
            return None;
        }

        loop {
            let wp = self.load_wp_usize();
            debug_assert_eq!(wp % ALIGN, 0, "writer header must be 8-byte aligned");

            // Region-local offsets
            let offset_in_region = wp % self.region_size;

            // Layout we're reserving now in THIS region:
            //   [MessageHeader(User)] [payload(msg_size)] [padding(to ALIGN)]
            // and additionally we require that after this record there remains
            // space for at least ONE MORE MessageHeader so a Skip can be written.
            let record_size = MESSAGE_HEADER_SIZE + msg_size;
            let record_with_padding = align_up(record_size);

            let needed_total = record_with_padding + MESSAGE_HEADER_SIZE; // spare header

            // file roll check includes the spare header requirement
            if self.file_roll_size > 0 && wp + needed_total > self.file_roll_size as usize {
                if self.roll_file().is_err() {
                    return None;
                }
                continue;
            }

            // region boundary check: if we cannot fit header+payload+pad+spare_header,
            // proactively fill the region with a Skip and move to the next region.
            if offset_in_region + needed_total > self.region_size {
                if self.roll_over_region().is_err() {
                    return None;
                }
                continue;
            }

            // There is enough room. Return just the payload slice; commit() will
            // account for padding, and the leftover >= MESSAGE_HEADER_SIZE invariant holds.
            let payload_off = offset_in_region + MESSAGE_HEADER_SIZE;
            return self.current_region.get_bytes_mut(payload_off, msg_size);
        }
    }

    /// Commit the message after filling the payload slice returned by `try_reserve`.
    pub fn commit(&mut self, msg_type: u16, length: u32) -> io::Result<()> {
        let wp = self.load_wp_usize();
        debug_assert_eq!(wp % ALIGN, 0, "writer header must be 8-byte aligned");

        let offset_in_region = wp % self.region_size;

        // Write header at an aligned address
        let hdr_slice = self
            .current_region
            .get_bytes_mut(offset_in_region, MESSAGE_HEADER_SIZE)
            .ok_or_else(|| err_other("No header to commit"))?;
        let hdr_ptr = hdr_slice.as_mut_ptr() as *mut MessageHeader;
        let header = MessageHeader {
            length,
            header_type: HeaderType::User,
            _reserved: 0,
            message_type: msg_type,
            timestamp_ns: now_ns(),
        };
        unsafe { *hdr_ptr = header };

        // Advance by header + payload + padding (to align the NEXT header).
        let payload_end = wp + MESSAGE_HEADER_SIZE + length as usize;
        let aligned_end = align_up(payload_end);
        let advance_by = aligned_end - wp;

        // Actual commit: advancing write_position
        self.channel_header()
            .write_position
            .fetch_add(advance_by as u64, Ordering::Release);

        self.channel_header_mut().message_count += 1;
        Ok(())
    }

    /// Explicitly roll to the next file (writes a `Roll` marker first).
    /// Explicitly roll to the next file:
    /// 1) ensure room in the old file for a Roll header (move to next region if needed),
    /// 2) create & initialize the new file,
    /// 3) switch Writer to the new file,
    /// 4) publish a Roll header into the old file (after the new file exists).
    pub fn roll_file(&mut self) -> io::Result<()> {
        // --- Capture OLD context up front ---
        let old_region_size = self.region_size;
        let old_seq = self.file_sequence;

        // Make a duplicate FD for the old file so we can write to it after we switch `self` to the new file
        let old_file = self.file.try_clone()?;

        // Ensure there's space in the OLD file for at least one header. If not, move to next region there.
        let wp = self.load_wp_usize();
        let off = wp % old_region_size;
        let leftover = old_region_size - off;

        // Compute where we will write the Roll header in the OLD file
        let roll_pos: usize = if leftover < MESSAGE_HEADER_SIZE {
            // Not enough for even a header: jump to next region start (no Skip written).
            let next_region_start = ((wp / old_region_size) + 1) * old_region_size;
            store_wp(&old_file, old_region_size, next_region_start as u64)?;
            next_region_start
        } else {
            // We can write a header right now at `roll_pos`.
            wp
        };

        // --- Step 2: create NEW file fully initialized (region 0 with Channel header) ---
        let next_seq = old_seq + 1;
        let (new_file, new_channel_region, new_current_region, new_index) =
            Self::open_file(&self.base_path, next_seq, self.region_size, self.mtu)?;

        // --- Step 3: switch Writer to the NEW file ---
        self.file_sequence = next_seq;
        self.file = new_file;
        self.channel_region = new_channel_region;
        self.current_region = new_current_region;
        self.current_region_index = new_index;

        // --- Step 4: publish Roll record in the OLD file at `roll_pos` ---
        write_header_at(
            &old_file,
            old_region_size,
            roll_pos,
            MessageHeader {
                length: 0,
                header_type: HeaderType::Roll,
                _reserved: 0,
                message_type: 0,
                timestamp_ns: now_ns(),
            },
        )?;
        // Bump old file write_position by one header (Release => header visible first)
        fetch_add_wp(&old_file, old_region_size, MESSAGE_HEADER_SIZE as u64)?;
        with_ch_mut(&old_file, old_region_size, |ch| ch.message_count += 1)?;
        // Finalize old file name: remove `.current` suffix if present.
        if let (Ok(curr), Ok(done)) = (
            make_current_file_path(&self.base_path, old_seq),
            make_channel_file_path(&self.base_path, old_seq),
        ) {
            if path_exists(&curr) {
                let _ = rename(curr, done);
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn get_payload(&mut self, offset_in_region: usize, msg_size: usize) -> Option<&mut [u8]> {
        let payload_off = offset_in_region + MESSAGE_HEADER_SIZE;
        self.current_region.get_bytes_mut(payload_off, msg_size)
    }

    #[allow(dead_code)]
    fn get_message_header(&mut self, offset_in_region: usize) -> Option<&mut MessageHeader> {
        let hdr_slice = self
            .current_region
            .get_bytes_mut(offset_in_region, MESSAGE_HEADER_SIZE)?;
        let hdr_ptr = hdr_slice.as_mut_ptr() as *mut MessageHeader;
        Some(unsafe { &mut *hdr_ptr })
    }

    fn roll_over_region(&mut self) -> io::Result<()> {
        let wp = self.load_wp_usize();
        debug_assert_eq!(wp % ALIGN, 0, "roll_over_region: wp must be aligned");

        let offset_in_region = wp % self.region_size;
        let leftover = self.region_size - offset_in_region;

        if leftover >= MESSAGE_HEADER_SIZE {
            // Emit a Skip that consumes the entire remainder of this region.
            let skip_len = leftover - MESSAGE_HEADER_SIZE;
            emit_skip_at(&self.file, self.region_size, wp, skip_len)?;
        } else {
            // Not enough for even a header: jump to next region start.
            let next_region_start = ((wp / self.region_size) + 1) * self.region_size;
            store_wp(&self.file, self.region_size, next_region_start as u64)?;
        }

        // Remap to the new region (writer's current view)
        let new_wp = self.load_wp_usize();
        self.remap_current_region_at(new_wp)?;

        Ok(())
    }

    #[inline]
    fn remap_current_region_at(&mut self, pos: usize) -> io::Result<()> {
        let idx = (pos / self.region_size) as u64;
        self.current_region = RegionMapping::create_writable(
            &self.file,
            idx * self.region_size as u64,
            self.region_size,
        )?;
        self.current_region_index = idx;
        Ok(())
    }
}

// ========== Reader ==========

#[derive(Debug, Clone, Copy)]
pub enum ReaderMode {
    LateJoin, // start from earliest existing file
    Live,     // start from latest existing file
}

#[derive(Clone)]
pub struct Message {
    pub mapping: Arc<RegionMapping<ReadOnly>>,
    pub header_offset: usize,
    pub payload_len: usize,
}

impl Message {
    #[inline]
    fn payload_offset(&self) -> usize {
        self.header_offset + MESSAGE_HEADER_SIZE
    }

    pub fn header(&self) -> Option<&MessageHeader> {
        if self.header_offset + MESSAGE_HEADER_SIZE > self.mapping.region_size() {
            return None;
        }
        let ptr = self.mapping.as_ptr().wrapping_add(self.header_offset);
        Some(unsafe { &*(ptr as *const MessageHeader) })
    }

    pub fn payload(&self) -> Option<&[u8]> {
        let payload_offset = self.payload_offset();
        let end = payload_offset + self.payload_len;
        if end > self.mapping.region_size() {
            return None;
        }
        let ptr = self.mapping.as_ptr().wrapping_add(payload_offset);
        Some(unsafe { slice::from_raw_parts(ptr, self.payload_len) })
    }
}

pub struct Reader {
    base_path: PathBuf,
    file_sequence: u64,
    file: File,

    zero_region: Arc<RegionMapping<ReadOnly>>,
    current_region: Arc<RegionMapping<ReadOnly>>,
    read_position: usize,

    mode: ReaderMode,
}

impl Reader {
    /// Open a Reader:
    /// - LateJoin => earliest file; read_position = 0
    /// - Live => latest file; read_position = write_position
    pub fn open<P: AsRef<Path>>(path: P, mode: ReaderMode) -> io::Result<Self> {
        let base_path = path.as_ref().to_path_buf();
        let seq = match mode {
            ReaderMode::LateJoin => find_earliest_sequence(&base_path)?,
            ReaderMode::Live => find_latest_sequence(&base_path)?,
        };
        Self::open_sequence_file(base_path, seq, mode)
    }

    fn get_current_read_position(file: &File, mode: ReaderMode) -> io::Result<(usize, usize)> {
        let ps = region::page_size();
        let tmp_map = RegionMapping::create_read_only(file, 0, ps)?; // map one OS page
        let mh = unsafe { &*(tmp_map.as_ptr() as *const MessageHeader) };
        if mh.header_type != HeaderType::Channel {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "file has first {:?}, expected Channel header",
                    mh.header_type
                ),
            ));
        }

        let ch = get_channel_header(tmp_map.as_ptr());
        let region_size = ch.region_size as usize;
        let wp = ch.write_position.load(Ordering::SeqCst) as usize;

        let read_pos = match mode {
            ReaderMode::LateJoin => 0,
            ReaderMode::Live => wp,
        };
        drop(tmp_map);
        Ok((read_pos, region_size))
    }

    fn open_sequence_file(base_path: PathBuf, sequence: u64, mode: ReaderMode) -> io::Result<Self> {
        let completed = make_channel_file_path(&base_path, sequence)?;
        let current = make_current_file_path(&base_path, sequence)?;
        let file_path = if path_exists(&current) { current } else { completed };
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(&file_path)?;

        let (read_pos, region_size) = Self::get_current_read_position(&file, mode)?;
        let region_index = (read_pos / region_size) as u64;
        let zero_region = Arc::new(RegionMapping::create_read_only(&file, 0, region_size)?);
        let current_region = Arc::new(RegionMapping::create_read_only(
            &file,
            region_index * region_size as u64,
            region_size,
        )?);

        Ok(Self {
            base_path,
            file_sequence: sequence,
            file,
            zero_region,
            read_position: read_pos,
            current_region,
            mode,
        })
    }

    #[inline]
    fn channel_header(&self) -> &ChannelHeader {
        unsafe { &*(self.zero_region.as_ptr().add(MESSAGE_HEADER_SIZE) as *const ChannelHeader) }
    }
    #[inline]
    fn load_wp(&self) -> usize {
        self.channel_header().write_position.load(Ordering::SeqCst) as usize
    }
    #[inline]
    fn region_size(&self) -> usize {
        self.channel_header().region_size as usize
    }
    #[inline]
    fn offset_in_region(&self) -> usize {
        self.read_position % self.region_size()
    }

    /// Read next message if available. If we see `Roll` => open next file.
    pub fn try_read(&mut self) -> Option<Message> {
        loop {
            let wp = self.load_wp();
            if self.read_position >= wp {
                return None;
            }

            let offset_in_region = self.offset_in_region();
            let leftover = self.region_size() - offset_in_region;
            if leftover < MESSAGE_HEADER_SIZE {
                self.roll_over_region();
                continue;
            }

            let base_ptr = unsafe { self.current_region.as_ptr().add(offset_in_region) }
                as *const MessageHeader;
            // Safe to take a reference: headers are guaranteed aligned
            let mh = unsafe { &*base_ptr };
            debug_assert_eq!((base_ptr as usize) % ALIGN, 0, "header must be aligned");

            let total_size = MESSAGE_HEADER_SIZE + mh.length as usize;
            if total_size > leftover {
                return None; // partial in this region
            }
            let end_of_rec = self.read_position + total_size;
            if end_of_rec > wp {
                return None; // not fully committed yet
            }

            // Align to the next header boundary (skip padding)
            let next_pos = align_up(end_of_rec);

            match mh.header_type {
                HeaderType::Channel | HeaderType::Skip => {
                    self.read_position = next_pos;
                    self.maybe_switch_region_for_pos(next_pos);
                }
                HeaderType::Roll => {
                    self.read_position = next_pos;
                    // Opening next file remaps `zero_region` and `current_region`.
                    if self.open_next_file().is_err() {
                        return None;
                    }
                }
                HeaderType::User => {
                    let msg = Message {
                        mapping: Arc::clone(&self.current_region),
                        header_offset: offset_in_region,
                        payload_len: mh.length as usize,
                    };
                    self.read_position = next_pos;
                    self.maybe_switch_region_for_pos(next_pos);
                    return Some(msg);
                }
            }
        }
    }
    fn roll_over_region(&mut self) {
        let region_size = self.region_size();
        let next_start = ((self.read_position / region_size) + 1) * region_size;
        self.read_position = next_start;
        let _ = self.switch_region((next_start / region_size) as u64);
    }

    fn switch_region(&mut self, idx: u64) -> io::Result<()> {
        let region_size = self.region_size();
        let new_map =
            RegionMapping::create_read_only(&self.file, idx * region_size as u64, region_size)?;
        self.current_region = Arc::new(new_map);
        Ok(())
    }

    fn open_next_file(&mut self) -> io::Result<()> {
        self.file_sequence += 1;
        let completed = make_channel_file_path(&self.base_path, self.file_sequence)?;
        let current = make_current_file_path(&self.base_path, self.file_sequence)?;
        let file_path = if path_exists(&current) { current } else { completed };
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(&file_path)?;

        // Map region 0 to learn the region size & wp
        let zero_region = Arc::new(RegionMapping::create_read_only(
            &file,
            0,
            self.region_size(),
        )?);
        let ch =
            unsafe { &*(zero_region.as_ptr().add(MESSAGE_HEADER_SIZE) as *const ChannelHeader) };
        let wp = ch.write_position.load(Ordering::SeqCst) as usize;

        let read_pos = match self.mode {
            ReaderMode::LateJoin => 0,
            ReaderMode::Live => wp,
        };
        let idx = read_pos / self.region_size();
        let current_region = Arc::new(RegionMapping::create_read_only(
            &file,
            (idx * self.region_size()) as u64,
            self.region_size(),
        )?);

        self.file = file;
        self.zero_region = zero_region;
        self.read_position = read_pos;
        self.current_region = current_region;
        Ok(())
    }

    #[inline]
    fn maybe_switch_region_for_pos(&mut self, pos: usize) {
        if pos.is_multiple_of(self.region_size()) {
            let idx = (pos / self.region_size()) as u64;
            // Best-effort; any real error will surface on the next access
            let _ = self.switch_region(idx);
        }
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

/// Build the path for the active (currently written) file.
fn make_current_file_path(base_path: &Path, sequence: u64) -> io::Result<PathBuf> {
    let mut p = make_channel_file_path(base_path, sequence)?;
    let name = p
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| err_other("invalid file name"))?;
    let mut new_name = name.to_string();
    new_name.push_str(".current");
    p.set_file_name(new_name);
    Ok(p)
}

#[inline]
fn path_exists(p: &Path) -> bool { p.exists() }

fn find_earliest_sequence(base_path: &Path) -> io::Result<u64> {
    find_sequence(base_path, false)
}
fn find_latest_sequence(base_path: &Path) -> io::Result<u64> {
    find_sequence(base_path, true)
}

/// Find earliest or latest sequence number of a file.
/// If file(s) do not exist, returns Ok(0).
fn find_sequence(path: &Path, latest: bool) -> io::Result<u64> {
    let parent_dir = match path.parent() {
        Some(parent) if parent.as_os_str().is_empty() => std::env::current_dir(),
        Some(parent) => Ok(parent.to_path_buf()),
        None => std::env::current_dir(),
    }?;
    let base_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "Invalid file name in path"))?
        .to_str()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "File name is not valid UTF-8"))?;

    let sequences: Vec<_> = read_dir(&parent_dir)?
        .filter_map(|entry| {
            entry.ok().and_then(|e| {
                let file_name = e.file_name();
                let file_name = file_name.to_str()?;
                if file_name == base_name || file_name == format!("{}.current", base_name) {
                    Some(0)
                } else if let Some(rest) = file_name.strip_prefix(&format!("{}.", base_name)) {
                    let seq_part = rest.strip_suffix(".current").unwrap_or(rest);
                    seq_part.parse().ok()
                } else {
                    None
                }
            })
        })
        .collect();

    let result = if latest {
        *sequences.iter().max().unwrap_or(&0)
    } else {
        *sequences.iter().min().unwrap_or(&0)
    };
    Ok(result)
}

/// Remove channel base and all rolled files created by this crate.
pub fn cleanup_channel_files<P: AsRef<std::path::Path>>(base: P) {
    use std::fs;
    let base_path = base.as_ref();
    let _ = fs::remove_file(base_path); // sequence 0 completed
    if let Ok(curr0) = make_current_file_path(base_path, 0) { let _ = fs::remove_file(&curr0); }
    // Remove a generous range of possible rolled files; ignore gaps.
    for i in 1..10_000 { // arbitrary upper bound for cleanup in tests
        if let Ok(p) = make_channel_file_path(base_path, i) { let _ = fs::remove_file(&p); }
        if let Ok(cp) = make_current_file_path(base_path, i) { let _ = fs::remove_file(&cp); }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        // Finalize active file: remove .current suffix so readers know it's complete.
        if let (Ok(curr), Ok(done)) = (
            make_current_file_path(&self.base_path, self.file_sequence),
            make_channel_file_path(&self.base_path, self.file_sequence),
        ) {
            if path_exists(&curr) {
                let _ = rename(curr, done);
            }
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

        let region_size = crate::page_size(); // portable: 4K/16K/etc.
        let file_roll_size = (region_size as u64) * 100;

        let mut writer = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size(file_roll_size)
            .mtu(0)
            .build()?;

        // #101 in file0
        if let Some(buf) = writer.try_reserve(500) {
            for b in buf.iter_mut() {
                *b = 0xAA;
            }
            writer.commit(101, 500)?;
        }

        // Roll to file1
        writer.roll_file()?;

        // #102 and #103 in file1
        if let Some(buf) = writer.try_reserve(600) {
            for b in buf.iter_mut() {
                *b = 0xBB;
            }
            writer.commit(102, 600)?;
        }
        if let Some(buf) = writer.try_reserve(300) {
            for b in buf.iter_mut() {
                *b = 0xCC;
            }
            writer.commit(103, 300)?;
        }

        // remove file0 so earliest existing is file1
        std::fs::remove_file(base).ok();

        // LateJoin => we expect #102 then #103
        {
            let mut reader = ReaderBuilder::new(base)
                .mode(ReaderMode::LateJoin)
                .build()?;
            let msg1 = reader.try_read().expect("missing msg #102");
            let hdr1 = msg1.header().unwrap();
            assert_eq!(hdr1.message_type, 102);
            assert_eq!(hdr1.length, 600);
            let payload = msg1.payload().unwrap();
            for &b in payload {
                assert_eq!(b, 0xBB);
            }

            let msg2 = reader.try_read().expect("missing msg #103");
            let hdr2 = msg2.header().unwrap();
            assert_eq!(hdr2.message_type, 103);
            assert_eq!(hdr2.length, 300);
            let payload2 = msg2.payload().unwrap();
            for &b in payload2 {
                assert_eq!(b, 0xCC);
            }

            assert!(reader.try_read().is_none());
        }

        // Live => picks latest existing (file1), read_position=write_position => no new messages
        {
            let mut reader = Reader::open(base, ReaderMode::Live)?;
            assert!(reader.try_read().is_none());
        }

        cleanup_channel_files(base);
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

        let mut writer = Writer::open_or_create(base, region_size, file_roll_size, mtu)?;

        let msg1: Vec<u8> = (0..100).map(|i| i as u8).collect();
        let msg2: Vec<u8> = vec![0x55; 200];
        let msg3: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];

        if let Some(payload) = writer.try_reserve(msg1.len()) {
            payload.copy_from_slice(&msg1);
            writer.commit(201, msg1.len() as u32)?;
        }
        writer.roll_file()?;
        if let Some(payload) = writer.try_reserve(msg2.len()) {
            payload.copy_from_slice(&msg2);
            writer.commit(202, msg2.len() as u32)?;
        }
        writer.roll_file()?;
        if let Some(payload) = writer.try_reserve(msg3.len()) {
            payload.copy_from_slice(&msg3);
            writer.commit(203, msg3.len() as u32)?;
        }

        let mut reader = Reader::open(base, ReaderMode::LateJoin)?;
        {
            let msg = reader.try_read().expect("missing msg1");
            let hdr = msg.header().unwrap();
            assert_eq!(hdr.message_type, 201);
            assert_eq!(msg.payload().unwrap(), &msg1[..]);
        }
        {
            let msg = reader.try_read().expect("missing msg2");
            let hdr = msg.header().unwrap();
            assert_eq!(hdr.message_type, 202);
            assert_eq!(msg.payload().unwrap(), &msg2[..]);
        }
        {
            let msg = reader.try_read().expect("missing msg3");
            let hdr = msg.header().unwrap();
            assert_eq!(hdr.message_type, 203);
            assert_eq!(msg.payload().unwrap(), &msg3[..]);
        }
        assert!(reader.try_read().is_none());

        cleanup_channel_files(base);
        Ok(())
    }

    #[test]
    fn test_boundary_skip_and_alignment() -> anyhow::Result<()> {
        let base = "test_boundary_skip";
        cleanup_channel_files(base);

        let region = crate::page_size();
        let file_roll_size = (region as u64) * 10;
        let mut w = Writer::open_or_create(base, region, file_roll_size, 0)?;

        // Choose len so that after header + payload the aligned end is region - header_size.
        let record_with_padding = region - MESSAGE_HEADER_SIZE;
        assert_eq!(record_with_padding % ALIGN, 0);
        let len = record_with_padding - MESSAGE_HEADER_SIZE;
        if let Some(buf) = w.try_reserve(len) {
            for b in buf.iter_mut() {
                *b = 0xAB;
            }
            w.commit(1, len as u32)?;
        }

        // Next small message should force a Skip and write at the start of next region.
        if let Some(buf) = w.try_reserve(32) {
            for b in buf.iter_mut() {
                *b = 0xCD;
            }
            w.commit(2, 32)?;
        }

        let mut r = Reader::open(base, ReaderMode::LateJoin)?;
        let m1 = r.try_read().expect("m1");
        assert_eq!(m1.header().unwrap().message_type, 1);
        assert_eq!(m1.header_offset % ALIGN, 0);

        let m2 = r.try_read().expect("m2");
        assert_eq!(m2.header().unwrap().message_type, 2);
        assert_eq!(m2.header_offset % ALIGN, 0);

        cleanup_channel_files(base);
        Ok(())
    }
}
