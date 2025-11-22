use std::sync::atomic::AtomicU64;

/// Header written before every record.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct MessageHeader {
    pub committed: u8,
    pub header_type: HeaderType,
    pub message_type: u16,
    pub length: u32,
    pub timestamp_ns: u64,
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
