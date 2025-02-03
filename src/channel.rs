use std::sync::atomic::AtomicU64;

/// Header written before every record.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct MessageHeader {
    pub length: u32,
    pub header_type: HeaderType,
    pub _reserved: u8,
    pub message_type: u16,
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
pub struct ChannelHeader {
    /// Absolute write position (bytes from file start).
    /// Writer publishes with `Release`; readers load with `Acquire/SeqCst`.
    pub write_position: AtomicU64,

    pub message_count: u64,

    /// Region size in bytes (multiple of OS page size).
    pub region_size: u32,

    /// Max payload size; 0 == unlimited.
    pub mtu: u32,

    /// Rolling sequence: 0 for `<base>`, 1 for `<base>.1`, etc.
    pub channel_sequence: u64,

    /// Optional channel name (unused bytes are 0).
    pub channel_name: [u8; 64],
}
