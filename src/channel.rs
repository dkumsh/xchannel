use std::io::{self, ErrorKind};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

/// Header written before every record.
///
/// Wire layout (16 bytes, little-endian, 8-byte aligned):
///
/// | offset | size | field           | owner  |
/// |-------:|-----:|-----------------|--------|
/// |      0 |    1 | `committed`     | system |
/// |      1 |    1 | `header_type`   | system |
/// |      2 |    2 | `message_type`  | user   |
/// |      4 |    4 | `length`        | system |
/// |      8 |    8 | `user_meta_u64` | user   |
///
/// The system-owned fields (`committed`, `header_type`, `length`) are
/// load-bearing for the publish/scan algorithm; the user-owned fields
/// (`message_type`, `user_meta_u64`) are opaque to xchannel and are
/// the application's to use as it sees fit (timestamp, sequence,
/// schema tag, packed flags, ...). See `FORMAT.md` for the cross-
/// language wire contract.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct MessageHeader {
    pub committed: u8,
    pub header_type: u8,
    pub message_type: u16,
    pub length: u32,
    pub user_meta_u64: u64,
}
static _MESSAGE_HEADER_SIZE: () = {
    assert!(size_of::<MessageHeader>() == 16);
};

impl MessageHeader {
    const NOT_COMMITTED: u8 = 0;
    const COMMITTED: u8 = 1;

    #[inline]
    pub fn is_committed(&self) -> io::Result<bool> {
        let cptr = std::ptr::addr_of!(self.committed) as *const AtomicU8;
        match unsafe { (*cptr).load(Ordering::Acquire) } {
            Self::NOT_COMMITTED => Ok(false),
            Self::COMMITTED => Ok(true),
            _ => Err(io::Error::new(
                ErrorKind::InvalidData,
                "invalid committed flag",
            )),
        }
    }

    #[inline]
    pub fn parsed_header_type(&self) -> io::Result<HeaderType> {
        HeaderType::from_raw(self.header_type)
    }

    #[inline]
    pub fn commit(hdr_ptr: *mut MessageHeader) {
        let cptr = unsafe { std::ptr::addr_of_mut!((*hdr_ptr).committed) as *mut AtomicU8 };
        unsafe { (*cptr).store(Self::COMMITTED, Ordering::Release) }
    }
}

/// The kind of record at the current offset.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderType {
    Channel = 0,
    User = 1,
    Skip = 2,
    Roll = 3,
}

impl HeaderType {
    #[inline]
    pub fn from_raw(raw: u8) -> io::Result<Self> {
        match raw {
            0 => Ok(Self::Channel),
            1 => Ok(Self::User),
            2 => Ok(Self::Skip),
            3 => Ok(Self::Roll),
            _ => Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid header_type: {raw}"),
            )),
        }
    }
}

/// Wire format version emitted and accepted by this crate. See `FORMAT.md`.
pub(crate) const FORMAT_VERSION: u16 = 1;
/// Endianness discriminant for `ChannelHeader::endianness`. Only LE is defined.
pub(crate) const ENDIANNESS_LE: u8 = 0x01;
/// Default user-metadata layout: `{message_type:u16 @ 2, user_meta_u64:u64 @ 8}`.
pub(crate) const USER_HEADER_KIND_DEFAULT: u32 = 0;
/// System-owned bytes inside `MessageHeader` (`committed`+`header_type`+`length`+pad to 8).
pub(crate) const SYSTEM_HEADER_SIZE: u8 = 8;
/// User-owned bytes inside `MessageHeader` (`message_type` slot + `user_meta_u64` slot).
pub(crate) const USER_HEADER_SIZE: u8 = 8;

/// The first record in region 0 is a `MessageHeader(Channel)`
/// immediately followed by this `ChannelHeader`. Total size: 64 bytes.
/// See `FORMAT.md` §3 for the cross-language byte layout.
#[repr(C)]
#[derive(Debug)]
pub(crate) struct ChannelHeader {
    /// Advisory: absolute byte offset of the next header slot.
    pub write_position: AtomicU64, //  0..8
    /// Advisory: monotonic count of records published in this file.
    pub message_count: AtomicU64, //  8..16
    /// Rolling sequence: 0 for `<base>`, 1 for `<base>.1`, etc.
    pub channel_sequence: u64, // 16..24
    /// Region size in bytes (multiple of OS page size).
    pub region_size: u32, // 24..28
    /// Max payload size; 0 == unlimited.
    pub mtu: u32, // 28..32
    /// Wire format version. Always equal to `FORMAT_VERSION`.
    pub format_version: u16, // 32..34
    /// Endianness discriminant. Always `ENDIANNESS_LE` (0x01) on supported targets.
    pub endianness: u8, // 34..35
    /// Size of the system-owned bytes inside `MessageHeader` (`SYSTEM_HEADER_SIZE`).
    pub system_header_size: u8, // 35..36
    /// Size of the user-owned bytes inside `MessageHeader` (`USER_HEADER_SIZE`).
    pub user_header_size: u8, // 36..37
    pub _reserved: [u8; 3], // 37..40
    /// Discriminant identifying the user-metadata layout. `USER_HEADER_KIND_DEFAULT`
    /// for the default `{message_type, user_meta_u64}` layout.
    pub user_header_kind: u32, // 40..44
    /// Optional channel name (unused bytes are 0).
    pub channel_name: [u8; 20], // 44..64
}

const _: () = {
    assert!(size_of::<MessageHeader>() == 16);
    assert!(size_of::<ChannelHeader>() == 64);
    assert!(align_of::<MessageHeader>() == 8);
    assert!(align_of::<ChannelHeader>() == 8);
};
