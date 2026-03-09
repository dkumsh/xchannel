use std::io::{self, ErrorKind};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

/// Header written before every record.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct MessageHeader {
    pub committed: u8,
    pub header_type: u8,
    pub message_type: u16,
    pub length: u32,
    pub timestamp_ns: u64,
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

/// The first record in region 0 is a `MessageHeader(Channel)`
/// immediately followed by this `ChannelHeader`.
#[repr(C)]
#[derive(Debug)]
pub(crate) struct ChannelHeader {
    /// Absolute write position (bytes from file start).
    pub write_position: AtomicU64,
    pub message_count: AtomicU64,
    /// Rolling sequence: 0 for `<base>`, 1 for `<base>.1`, etc.
    pub channel_sequence: u64,
    /// Region size in bytes (multiple of OS page size).
    pub region_size: u32,
    /// Max payload size; 0 == unlimited.
    pub mtu: u32,
    /// Optional channel name (unused bytes are 0).
    pub channel_name: [u8; 32], // read-only field
}

const _: () = {
    assert!(size_of::<MessageHeader>() == 16);
    assert!(size_of::<ChannelHeader>() == 64);
    assert!(align_of::<MessageHeader>() == 8);
    assert!(align_of::<ChannelHeader>() == 8);
};
