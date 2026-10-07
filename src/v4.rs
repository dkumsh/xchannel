//! Format v4: the header extension (instance ID, parent, publication state) and the `Roll` body
//! that names a successor. Layout in FORMAT.md.

use std::io::{self, ErrorKind};
use std::sync::atomic::{AtomicU8, Ordering};

pub(crate) const FORMAT_VERSION_V4: u16 = 4;

/// File offset of [`ChannelHeaderExt`]: after the 16-byte Channel envelope and the 128-byte
/// `ChannelHeader`.
pub(crate) const EXT_FILE_OFFSET: usize = 144;
/// The Channel record's payload in v4: `ChannelHeader` and [`ChannelHeaderExt`]. Its envelope's
/// `length` must say 192.
pub(crate) const CHANNEL_PAYLOAD_V4: usize = 192;
/// File offset of the first record header.
pub(crate) const FIRST_RECORD_V4: usize = 208;
/// `write_position` of a fresh file: the payload of its first record, 16 bytes past its header.
pub(crate) const INITIAL_WRITE_POSITION_V4: usize = 224;
/// Body of a v4 `Roll`.
pub(crate) const ROLL_PAYLOAD_V4: usize = 40;
/// A v4 `Roll` with its envelope. A writer must always leave this much room at its next header
/// slot, so a `Roll` can be staged wherever it stops.
pub(crate) const ROLL_TOTAL_V4: usize = 56;

/// `publication_state`: complete and safe to open and map, but not part of the channel's history.
pub(crate) const PREPARED: u8 = 0;
/// `publication_state`: published by the writer; its base record index is final.
pub(crate) const PUBLISHED: u8 = 1;

fn invalid(what: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, what.into())
}

// ---------- CRC32C ----------

/// CRC32C (Castagnoli), reflected polynomial `0x82F63B78`, initial value and final XOR
/// `0xFFFFFFFF`: the standard one, so other implementations can check v4 files.
const CRC32C_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut bit = 0;
        while bit < 8 {
            c = if c & 1 != 0 {
                (c >> 1) ^ 0x82F6_3B78
            } else {
                c >> 1
            };
            bit += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
};

pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("sse4.2") {
        // Safety: SSE4.2 is present.
        return unsafe { crc32c_sse42(bytes) };
    }
    crc32c_table(bytes)
}

fn crc32c_table(bytes: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in bytes {
        c = CRC32C_TABLE[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    !c
}

/// The same CRC with the `crc32` instruction: no table to miss in cache.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_sse42(bytes: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u64};
    let mut c = !0u64;
    let mut words = bytes.chunks_exact(8);
    for w in &mut words {
        c = _mm_crc32_u64(c, u64::from_le_bytes(w.try_into().expect("8 bytes")));
    }
    let mut c = c as u32;
    for &b in words.remainder() {
        c = _mm_crc32_u8(c, b);
    }
    !c
}

// ---------- instance IDs ----------

/// The identity of one physical file: one creation attempt, whatever its name and sequence. A
/// file rebuilt at the same sequence gets a new one.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct InstanceId([u8; 16]);

impl InstanceId {
    /// The initial file's predecessor: no file.
    pub(crate) const NONE: Self = Self([0; 16]);

    pub(crate) fn is_none(self) -> bool {
        self == Self::NONE
    }

    /// A fresh nonzero ID from the OS's random source. Called while preparing a file, never on a
    /// roll. Collisions are negligible, not impossible.
    pub(crate) fn fresh() -> io::Result<Self> {
        loop {
            let mut bytes = [0u8; 16];
            fill_random(&mut bytes)?;
            if bytes != [0; 16] {
                return Ok(Self(bytes));
            }
        }
    }
}

impl std::fmt::Debug for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn fill_random(buf: &mut [u8]) -> io::Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        // Safety: the pointer and length name the unfilled rest of `buf`.
        let n =
            unsafe { libc::getrandom(buf[filled..].as_mut_ptr().cast(), buf.len() - filled, 0) };
        if n < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        filled += n as usize;
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn fill_random(buf: &mut [u8]) -> io::Result<()> {
    use std::io::Read;
    std::fs::File::open("/dev/urandom")?.read_exact(buf)
}

// ---------- header extension ----------

/// Who a file is: its own instance and the exact file it follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Identity {
    pub(crate) instance: InstanceId,
    /// [`InstanceId::NONE`] for a channel's initial file.
    pub(crate) predecessor: InstanceId,
    /// `u64::MAX` for a channel's initial file.
    pub(crate) predecessor_sequence: u64,
}

impl Identity {
    pub(crate) fn initial(instance: InstanceId) -> Self {
        Self {
            instance,
            predecessor: InstanceId::NONE,
            predecessor_sequence: u64::MAX,
        }
    }

    pub(crate) fn successor(
        instance: InstanceId,
        predecessor: InstanceId,
        predecessor_sequence: u64,
    ) -> Self {
        Self {
            instance,
            predecessor,
            predecessor_sequence,
        }
    }

    /// Extension bytes 0..40, which `identity_crc32c` covers.
    fn bytes(&self) -> [u8; 40] {
        let mut b = [0u8; 40];
        b[0..16].copy_from_slice(&self.instance.0);
        b[16..32].copy_from_slice(&self.predecessor.0);
        b[32..40].copy_from_slice(&self.predecessor_sequence.to_le_bytes());
        b
    }
}

/// The 64-byte v4 extension at file offset 144. All but `publication_state` is written once,
/// before the file is installed under its final name, and never changed.
#[repr(C)]
pub(crate) struct ChannelHeaderExt {
    file_instance_id: [u8; 16],        // 0..16
    predecessor_instance_id: [u8; 16], // 16..32
    predecessor_sequence: [u8; 8],     // 32..40, little-endian
    _reserved0: [u8; 8],               // 40..48
    publication_state: AtomicU8,       // 48
    _reserved1: [u8; 3],               // 49..52
    identity_crc32c: [u8; 4],          // 52..56, little-endian, over bytes 0..40
    _reserved2: [u8; 8],               // 56..64
}

const _: () = {
    use std::mem::offset_of;
    assert!(size_of::<ChannelHeaderExt>() == 64);
    assert!(offset_of!(ChannelHeaderExt, predecessor_instance_id) == 16);
    assert!(offset_of!(ChannelHeaderExt, predecessor_sequence) == 32);
    assert!(offset_of!(ChannelHeaderExt, publication_state) == 48);
    assert!(offset_of!(ChannelHeaderExt, identity_crc32c) == 52);
    assert!(EXT_FILE_OFFSET == 16 + 128);
    assert!(EXT_FILE_OFFSET + offset_of!(ChannelHeaderExt, publication_state) == 192);
    assert!(CHANNEL_PAYLOAD_V4 == 128 + size_of::<ChannelHeaderExt>());
    assert!(FIRST_RECORD_V4 == 16 + CHANNEL_PAYLOAD_V4);
    assert!(FIRST_RECORD_V4.is_multiple_of(8));
    assert!(INITIAL_WRITE_POSITION_V4 == FIRST_RECORD_V4 + 16);
    assert!(ROLL_TOTAL_V4 == 16 + ROLL_PAYLOAD_V4);
    assert!(ROLL_TOTAL_V4.is_multiple_of(8));
};

/// A file's publication state, read with acquire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum State {
    Prepared,
    Published,
}

impl ChannelHeaderExt {
    /// Write a prepared file's extension: its identity, zero reserved bytes, `PREPARED`. Only
    /// while the file is still private, before anything else can map it.
    pub(crate) fn init_prepared(&mut self, identity: &Identity) {
        let bytes = identity.bytes();
        self.file_instance_id = identity.instance.0;
        self.predecessor_instance_id = identity.predecessor.0;
        self.predecessor_sequence = identity.predecessor_sequence.to_le_bytes();
        self._reserved0 = [0; 8];
        *self.publication_state.get_mut() = PREPARED;
        self._reserved1 = [0; 3];
        self.identity_crc32c = crc32c(&bytes).to_le_bytes();
        self._reserved2 = [0; 8];
    }

    /// The file's identity, if the immutable bytes are well formed: a nonzero instance, reserved
    /// bytes zero, a matching CRC, and an initial-file sentinel on both predecessor fields or on
    /// neither. Safe on a `PREPARED` file: none of these bytes change after installation.
    pub(crate) fn identity(&self) -> io::Result<Identity> {
        let identity = Identity {
            instance: InstanceId(self.file_instance_id),
            predecessor: InstanceId(self.predecessor_instance_id),
            predecessor_sequence: u64::from_le_bytes(self.predecessor_sequence),
        };
        if identity.instance.is_none() {
            return Err(invalid("v4 file has a zero instance ID"));
        }
        if self._reserved0 != [0; 8] || self._reserved1 != [0; 3] || self._reserved2 != [0; 8] {
            return Err(invalid("v4 header extension has nonzero reserved bytes"));
        }
        let crc = u32::from_le_bytes(self.identity_crc32c);
        if crc != crc32c(&identity.bytes()) {
            return Err(invalid("v4 header extension fails its CRC"));
        }
        if identity.predecessor.is_none() != (identity.predecessor_sequence == u64::MAX) {
            return Err(invalid(
                "v4 header extension names a predecessor by only one of ID and sequence",
            ));
        }
        Ok(identity)
    }

    /// The publication state, with acquire: once `Published`, the base record index is final.
    pub(crate) fn state(&self) -> io::Result<State> {
        match self.publication_state.load(Ordering::Acquire) {
            PREPARED => Ok(State::Prepared),
            PUBLISHED => Ok(State::Published),
            other => Err(invalid(format!("v4 file has publication state {other}"))),
        }
    }

    /// Publish the file. Only the writer, after it has stamped the base record index and
    /// staged the predecessor's `Roll` in full.
    pub(crate) fn publish(&self) {
        self.publication_state.store(PUBLISHED, Ordering::Release);
    }
}

/// The extension of the file whose region 0 (or page 0) is mapped at `region0`.
///
/// # Safety
/// `region0` must point at a live mapping of at least [`FIRST_RECORD_V4`] bytes of a file's start.
pub(crate) unsafe fn ext_at<'a>(region0: *const u8) -> &'a ChannelHeaderExt {
    unsafe { &*(region0.add(EXT_FILE_OFFSET) as *const ChannelHeaderExt) }
}

/// Mutable access for initialising a file nothing else has mapped yet.
///
/// # Safety
/// As [`ext_at`], and nothing else may access these bytes meanwhile.
pub(crate) unsafe fn ext_at_mut<'a>(region0: *mut u8) -> &'a mut ChannelHeaderExt {
    unsafe { &mut *(region0.add(EXT_FILE_OFFSET) as *mut ChannelHeaderExt) }
}

// ---------- installation ----------

/// Rename `from` to `to` atomically, never replacing: `AlreadyExists` if `to` exists.
pub(crate) fn install_no_replace(from: &std::path::Path, to: &std::path::Path) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let c = |p: &std::path::Path| {
            CString::new(p.as_os_str().as_bytes())
                .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "path has a NUL byte"))
        };
        let (cfrom, cto) = (c(from)?, c(to)?);
        // Safety: two valid C strings; the remaining arguments are plain integers.
        let r = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                cfrom.as_ptr(),
                libc::AT_FDCWD,
                cto.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if r == 0 {
            return Ok(());
        }
        let e = io::Error::last_os_error();
        match e.raw_os_error() {
            Some(libc::ENOSYS) | Some(libc::EINVAL) => {} // no renameat2 here: link instead
            _ => return Err(e),
        }
    }
    std::fs::hard_link(from, to)?;
    let _ = std::fs::remove_file(from);
    Ok(())
}

// ---------- Roll body ----------

/// What a committed v4 `Roll` says about the file after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RollEdge {
    pub(crate) next_sequence: u64,
    pub(crate) next_instance: InstanceId,
    pub(crate) next_base_record_index: u64,
}

impl RollEdge {
    /// The edge out of the file at `sequence` with base `base` and `count` user records, to the
    /// file `next`. Overflow is an error, never a wrap.
    pub(crate) fn after(
        sequence: u64,
        base: u64,
        count: u64,
        next: InstanceId,
    ) -> io::Result<Self> {
        let exhausted = |what| io::Error::new(ErrorKind::InvalidInput, format!("{what} exhausted"));
        Ok(Self {
            next_sequence: sequence
                .checked_add(1)
                .ok_or_else(|| exhausted("file sequence"))?,
            next_instance: next,
            next_base_record_index: base
                .checked_add(count)
                .ok_or_else(|| exhausted("record index"))?,
        })
    }

    pub(crate) fn encode(&self) -> [u8; ROLL_PAYLOAD_V4] {
        let mut b = [0u8; ROLL_PAYLOAD_V4];
        b[0..8].copy_from_slice(&self.next_sequence.to_le_bytes());
        b[8..24].copy_from_slice(&self.next_instance.0);
        b[24..32].copy_from_slice(&self.next_base_record_index.to_le_bytes());
        let crc = crc32c(&b[0..32]);
        b[32..36].copy_from_slice(&crc.to_le_bytes());
        b
    }

    /// Read a committed `Roll`'s body: exactly 40 bytes, a matching CRC, zero reserved bytes and
    /// a nonzero successor ID. Only after the `Roll` is seen committed.
    pub(crate) fn decode(payload: &[u8]) -> io::Result<Self> {
        let b: &[u8; ROLL_PAYLOAD_V4] = payload
            .try_into()
            .map_err(|_| invalid(format!("v4 Roll body is {} bytes, not 40", payload.len())))?;
        let crc = u32::from_le_bytes(b[32..36].try_into().expect("4 bytes"));
        if crc != crc32c(&b[0..32]) {
            return Err(invalid("v4 Roll fails its CRC"));
        }
        if b[36..40] != [0; 4] {
            return Err(invalid("v4 Roll has nonzero reserved bytes"));
        }
        let edge = Self {
            next_sequence: u64::from_le_bytes(b[0..8].try_into().expect("8 bytes")),
            next_instance: InstanceId(b[8..24].try_into().expect("16 bytes")),
            next_base_record_index: u64::from_le_bytes(b[24..32].try_into().expect("8 bytes")),
        };
        if edge.next_instance.is_none() {
            return Err(invalid("v4 Roll names a zero successor ID"));
        }
        Ok(edge)
    }

    /// Whether `next` is the file this edge names, as the predecessor `(sequence, instance)` left
    /// it: its sequence, its own ID and its parent's. Its base is checked once it is published.
    pub(crate) fn names(&self, from_sequence: u64, from: InstanceId, next: &Identity) -> bool {
        self.next_instance == next.instance
            && next.predecessor == from
            && next.predecessor_sequence == from_sequence
            && Some(self.next_sequence) == from_sequence.checked_add(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C, align(8))]
    struct Aligned([u8; 64]);

    fn ext(buf: &mut Aligned) -> &mut ChannelHeaderExt {
        // Safety: 64 bytes, aligned, and every bit pattern is a valid `ChannelHeaderExt`.
        unsafe { &mut *(buf.0.as_mut_ptr() as *mut ChannelHeaderExt) }
    }

    fn id(first: u8) -> InstanceId {
        InstanceId(std::array::from_fn(|i| first + i as u8))
    }

    #[test]
    fn crc32c_matches_the_standard_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c_table(b"123456789"), 0xE306_9283);
        let bytes: Vec<u8> = (0..100u8).map(|b| b.wrapping_mul(37)).collect();
        for len in 0..bytes.len() {
            assert_eq!(crc32c(&bytes[..len]), crc32c_table(&bytes[..len]), "{len}");
        }
    }

    #[test]
    fn the_extension_has_the_golden_bytes() {
        let mut buf = Aligned([0xAA; 64]);
        ext(&mut buf).init_prepared(&Identity::successor(id(0x01), id(0x11), 7));
        let mut want = [0u8; 64];
        want[0..16].copy_from_slice(&id(0x01).0);
        want[16..32].copy_from_slice(&id(0x11).0);
        want[32..40].copy_from_slice(&7u64.to_le_bytes());
        want[48] = PREPARED;
        want[52..56].copy_from_slice(&0xF984_7ACDu32.to_le_bytes());
        assert_eq!(buf.0, want);
    }

    #[test]
    fn an_initial_file_carries_both_sentinels() {
        let mut buf = Aligned([0; 64]);
        let identity = Identity::initial(id(0x01));
        ext(&mut buf).init_prepared(&identity);
        assert_eq!(buf.0[16..32], [0; 16]);
        assert_eq!(buf.0[32..40], [0xFF; 8]);
        assert_eq!(buf.0[52..56], 0xFB04_EE98u32.to_le_bytes());
        assert_eq!(ext(&mut buf).identity().unwrap(), identity);
    }

    #[test]
    fn identity_reads_back_and_publication_moves_once() {
        let mut buf = Aligned([0; 64]);
        let identity = Identity::successor(id(0x01), id(0x11), 7);
        ext(&mut buf).init_prepared(&identity);
        let e = ext(&mut buf);
        assert_eq!(e.identity().unwrap(), identity);
        assert_eq!(e.state().unwrap(), State::Prepared);
        e.publish();
        assert_eq!(e.state().unwrap(), State::Published);
        assert_eq!(buf.0[48], PUBLISHED);
    }

    #[test]
    fn a_malformed_extension_is_rejected() {
        let good = {
            let mut buf = Aligned([0; 64]);
            ext(&mut buf).init_prepared(&Identity::successor(id(0x01), id(0x11), 7));
            buf.0
        };
        let rejects = |change: &dyn Fn(&mut [u8; 64]), why: &str| {
            let mut buf = Aligned(good);
            change(&mut buf.0);
            let e = ext(&mut buf).identity().expect_err(why).to_string();
            assert!(e.contains(why), "{why}: {e}");
        };
        rejects(&|b| b[3] ^= 1, "CRC");
        rejects(&|b| b[33] ^= 1, "CRC");
        rejects(&|b| b[41] = 1, "reserved");
        rejects(&|b| b[50] = 1, "reserved");
        rejects(&|b| b[60] = 1, "reserved");
        rejects(&|b| b[52] ^= 1, "CRC");
        rejects(
            &|b| {
                b[0..16].fill(0);
                let crc = crc32c(&b[0..40]);
                b[52..56].copy_from_slice(&crc.to_le_bytes());
            },
            "zero instance",
        );
        rejects(
            &|b| {
                b[16..32].fill(0); // no parent, but a parent sequence
                let crc = crc32c(&b[0..40]);
                b[52..56].copy_from_slice(&crc.to_le_bytes());
            },
            "only one of",
        );

        let mut buf = Aligned(good);
        buf.0[48] = 2;
        let e = ext(&mut buf)
            .state()
            .expect_err("unknown state")
            .to_string();
        assert!(e.contains("state 2"), "{e}");
    }

    #[test]
    fn the_roll_body_has_the_golden_bytes_and_reads_back() {
        let edge = RollEdge {
            next_sequence: 8,
            next_instance: id(0x21),
            next_base_record_index: 1000,
        };
        let b = edge.encode();
        let mut want = [0u8; 40];
        want[0..8].copy_from_slice(&8u64.to_le_bytes());
        want[8..24].copy_from_slice(&id(0x21).0);
        want[24..32].copy_from_slice(&1000u64.to_le_bytes());
        want[32..36].copy_from_slice(&0xF9D7_100Bu32.to_le_bytes());
        assert_eq!(b, want);
        assert_eq!(RollEdge::decode(&b).unwrap(), edge);
    }

    #[test]
    fn a_malformed_roll_body_is_rejected() {
        let good = RollEdge {
            next_sequence: 8,
            next_instance: id(0x21),
            next_base_record_index: 1000,
        }
        .encode();
        let rejects = |b: &[u8], why: &str| {
            let e = RollEdge::decode(b).expect_err(why).to_string();
            assert!(e.contains(why), "{why}: {e}");
        };
        rejects(&[], "0 bytes");
        rejects(&good[..39], "39 bytes");
        rejects(&[good.as_slice(), &[0]].concat(), "41 bytes");
        let mut b = good;
        b[25] ^= 1;
        rejects(&b, "CRC");
        let mut b = good;
        b[37] = 1;
        rejects(&b, "reserved");
        let mut b = good;
        b[8..24].fill(0);
        let crc = crc32c(&b[0..32]);
        b[32..36].copy_from_slice(&crc.to_le_bytes());
        rejects(&b, "zero successor");
    }

    #[test]
    fn an_edge_follows_its_predecessor_or_reports_exhaustion() {
        let edge = RollEdge::after(7, 900, 100, id(0x21)).unwrap();
        assert_eq!(
            edge,
            RollEdge {
                next_sequence: 8,
                next_instance: id(0x21),
                next_base_record_index: 1000,
            }
        );
        assert!(edge.names(7, id(0x11), &Identity::successor(id(0x21), id(0x11), 7)));
        // A file rebuilt at the same sequence, or hung under another parent, is not the one named.
        assert!(!edge.names(7, id(0x11), &Identity::successor(id(0x31), id(0x11), 7)));
        assert!(!edge.names(7, id(0x11), &Identity::successor(id(0x21), id(0x41), 7)));
        assert!(!edge.names(6, id(0x11), &Identity::successor(id(0x21), id(0x11), 6)));

        let e = RollEdge::after(u64::MAX, 0, 0, id(0x21)).unwrap_err();
        assert!(e.to_string().contains("file sequence exhausted"));
        let e = RollEdge::after(0, u64::MAX, 1, id(0x21)).unwrap_err();
        assert!(e.to_string().contains("record index exhausted"));
    }

    #[test]
    fn fresh_ids_are_nonzero_and_distinct() {
        let ids: std::collections::HashSet<_> =
            (0..1000).map(|_| InstanceId::fresh().unwrap()).collect();
        assert_eq!(ids.len(), 1000);
        assert!(!ids.contains(&InstanceId::NONE));
    }
}
