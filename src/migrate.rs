//! One-time migration from xchannel v2 channel files to v3 format.
//!
//! v3 widened `ChannelHeader` with `format_version`, `endianness`,
//! header-size fields, and a reserved `user_header_kind`, reusing 12 bytes
//! at the start of v2's previously-unused `channel_name: [u8; 32]`. The
//! `channel_name` field shrank from 32 to 20 bytes in the process. Every
//! other byte of a channel file — the Channel `MessageHeader` at offset 0,
//! the first 32 bytes of `ChannelHeader` (`write_position`, `message_count`,
//! `channel_sequence`, `region_size`, `mtu`), and the entire records area
//! from byte 80 onward — is unchanged between v2 and v3.
//!
//! Migration is therefore mechanical: rewrite file bytes 48–79 (the second
//! half of `ChannelHeader`) and copy the rest verbatim. There is no
//! re-encoding of records.
//!
//! Only LE-framed files are supported. v2 only ever ran on LE targets, so
//! this matches what's on disk in practice.

use std::fs::OpenOptions;
use std::io::{self, ErrorKind, Read, Write};
use std::path::Path;

use crate::channel::{
    ENDIANNESS_LE, FORMAT_VERSION, SYSTEM_HEADER_SIZE, USER_HEADER_KIND_DEFAULT, USER_HEADER_SIZE,
};
use crate::{find_all_sequences, make_channel_file_path};

// File offsets for the bytes we touch.
const REGION0_PREFIX_LEN: usize = 80;
const CH_OFFSET: usize = 16; // ChannelHeader starts at file offset 16
const FORMAT_VERSION_AT: usize = CH_OFFSET + 32; // 48
const ENDIANNESS_AT: usize = CH_OFFSET + 34; // 50
const SYS_HDR_SIZE_AT: usize = CH_OFFSET + 35; // 51
const USR_HDR_SIZE_AT: usize = CH_OFFSET + 36; // 52
const RESERVED_AT: usize = CH_OFFSET + 37; // 53..56
const USER_HEADER_KIND_AT: usize = CH_OFFSET + 40; // 56..60
const CHANNEL_NAME_V3_AT: usize = CH_OFFSET + 44; // 60..80 (20 bytes)
const CHANNEL_NAME_V2_AT: usize = CH_OFFSET + 32; // 48..80 (32 bytes)
const CHANNEL_NAME_V3_LEN: usize = 20;
const CHANNEL_NAME_V2_LEN: usize = 32;

// Fields we validate in the source.
const COMMITTED_AT: usize = 0; // committed byte of the Channel MessageHeader
const HEADER_TYPE_AT: usize = 1; // header_type byte
const MH_LENGTH_AT: usize = 4; // u32 length field of the Channel MessageHeader
const REGION_SIZE_AT: usize = CH_OFFSET + 24; // u32 region_size

const HEADER_TYPE_CHANNEL: u8 = 0;
const COMMITTED_FLAG: u8 = 1;
const CHANNEL_HEADER_LEN: u32 = 64;

#[inline]
fn invalid<S: Into<String>>(msg: S) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, msg.into())
}

/// Convert a single v2 channel file to v3 format. The source is read but
/// not modified; the destination is created (refused with
/// `ErrorKind::AlreadyExists` if it exists).
///
/// The source file must look like a v2 channel file: a committed Channel
/// `MessageHeader` at offset 0, a v2 `ChannelHeader` (where `format_version`
/// at file offset 48 reads as zero — v2 had the first byte of
/// `channel_name` there, which the v2 writer always initialized to zero).
/// A non-zero value at that offset suggests the file is already v3 or
/// otherwise non-v2; migration refuses.
pub fn migrate_file_v2_to_v3(src: &Path, dst: &Path) -> io::Result<()> {
    let mut src_file = OpenOptions::new().read(true).open(src)?;
    let src_meta = src_file.metadata()?;
    if (src_meta.len() as usize) < REGION0_PREFIX_LEN {
        return Err(invalid(format!(
            "source too small to be a channel file ({} bytes): {:?}",
            src_meta.len(),
            src
        )));
    }

    // Read the first 80 bytes (Channel MessageHeader + ChannelHeader).
    let mut prefix = [0u8; REGION0_PREFIX_LEN];
    src_file.read_exact(&mut prefix)?;

    // Validate the Channel MessageHeader.
    if prefix[COMMITTED_AT] != COMMITTED_FLAG {
        return Err(invalid("source: first MessageHeader is not committed"));
    }
    if prefix[HEADER_TYPE_AT] != HEADER_TYPE_CHANNEL {
        return Err(invalid(format!(
            "source: first record header_type is {} (expected Channel=0)",
            prefix[HEADER_TYPE_AT]
        )));
    }
    let mh_length = u32::from_le_bytes(prefix[MH_LENGTH_AT..MH_LENGTH_AT + 4].try_into().unwrap());
    if mh_length != CHANNEL_HEADER_LEN {
        return Err(invalid(format!(
            "source: Channel record length is {} (expected {})",
            mh_length, CHANNEL_HEADER_LEN
        )));
    }

    // Confirm this looks like v2 (format_version field reads as 0).
    let probe = u16::from_le_bytes(
        prefix[FORMAT_VERSION_AT..FORMAT_VERSION_AT + 2]
            .try_into()
            .unwrap(),
    );
    if probe != 0 {
        return Err(invalid(format!(
            "source does not look like v2 (format_version probe = {}); \
             only v2 -> v3 migration is supported",
            probe
        )));
    }

    // region_size must be positive.
    let region_size = u32::from_le_bytes(
        prefix[REGION_SIZE_AT..REGION_SIZE_AT + 4]
            .try_into()
            .unwrap(),
    );
    if region_size == 0 {
        return Err(invalid("source: ChannelHeader.region_size is zero"));
    }

    // Capture the v2 channel_name (offset 48..80) before we overwrite it.
    let mut old_channel_name = [0u8; CHANNEL_NAME_V2_LEN];
    old_channel_name
        .copy_from_slice(&prefix[CHANNEL_NAME_V2_AT..CHANNEL_NAME_V2_AT + CHANNEL_NAME_V2_LEN]);

    // Rewrite the v3 fields in place. (The first 32 bytes of ChannelHeader —
    // write_position, message_count, channel_sequence, region_size, mtu —
    // stay untouched.)
    prefix[FORMAT_VERSION_AT..FORMAT_VERSION_AT + 2].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    prefix[ENDIANNESS_AT] = ENDIANNESS_LE;
    prefix[SYS_HDR_SIZE_AT] = SYSTEM_HEADER_SIZE;
    prefix[USR_HDR_SIZE_AT] = USER_HEADER_SIZE;
    prefix[RESERVED_AT..RESERVED_AT + 3].fill(0);
    prefix[USER_HEADER_KIND_AT..USER_HEADER_KIND_AT + 4]
        .copy_from_slice(&USER_HEADER_KIND_DEFAULT.to_le_bytes());

    // Truncate the channel_name from 32 -> 20 bytes (take the first 20 of
    // the old value).
    prefix[CHANNEL_NAME_V3_AT..CHANNEL_NAME_V3_AT + CHANNEL_NAME_V3_LEN]
        .copy_from_slice(&old_channel_name[..CHANNEL_NAME_V3_LEN]);

    // Write to dst. `create_new` refuses if it already exists.
    let mut dst_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(dst)?;
    dst_file.write_all(&prefix)?;

    // Copy the rest of the file (records) verbatim.
    io::copy(&mut src_file, &mut dst_file)?;
    Ok(())
}

/// Convert an entire v2 channel (all rolled files matching `src_base`) to
/// v3 format, written under `dst_base`. Returns the number of files
/// migrated. Refuses if `dst_base` already has any matching files.
pub fn migrate_channel_v2_to_v3<P: AsRef<Path>, Q: AsRef<Path>>(
    src_base: P,
    dst_base: Q,
) -> io::Result<usize> {
    let src_base = src_base.as_ref();
    let dst_base = dst_base.as_ref();

    let sequences = find_all_sequences(src_base)?;
    if sequences.is_empty() {
        return Err(io::Error::new(
            ErrorKind::NotFound,
            format!("no channel files found at base path {:?}", src_base),
        ));
    }

    for &seq in &sequences {
        let src = make_channel_file_path(src_base, seq)?;
        let dst = make_channel_file_path(dst_base, seq)?;
        migrate_file_v2_to_v3(&src, &dst)?;
    }
    Ok(sequences.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ReaderBuilder, WriterBuilder, cleanup_channel_files, page_size};
    use std::io::{Seek, SeekFrom, Write};

    /// Overwrite file bytes 48..80 (= the v3-specific tail of ChannelHeader)
    /// with zeros. After this, the file has the on-disk bytes a v2 writer
    /// would have produced (v2 had `channel_name: [u8; 32]` initialized to
    /// zero in those bytes; v3's `format_version`/`endianness`/etc. live
    /// at the same offsets).
    fn downgrade_to_v2(path: &Path) -> io::Result<()> {
        let mut f = OpenOptions::new().read(true).write(true).open(path)?;
        f.seek(SeekFrom::Start(CHANNEL_NAME_V2_AT as u64))?;
        f.write_all(&[0u8; CHANNEL_NAME_V2_LEN])?;
        f.sync_all()?;
        Ok(())
    }

    /// Single-file: produce a v3 file, downgrade it to v2-on-disk, migrate
    /// back to v3, verify a Reader can read the records.
    #[test]
    fn test_migrate_file_v2_to_v3_round_trip() -> anyhow::Result<()> {
        let v2_base = "test_migrate_file_v2_src";
        let v3_base = "test_migrate_file_v2_dst";
        cleanup_channel_files(v2_base);
        cleanup_channel_files(v3_base);

        let region_size = page_size();
        let payload_a: [u8; 8] = *b"ABCDEFGH";
        let payload_b: [u8; 16] = *b"abcdefghijklmnop";

        // 1) Build a normal v3 channel with two records.
        {
            let mut w = WriterBuilder::new(v2_base)
                .region_size(region_size)
                .build()?;
            let buf = w.try_reserve(payload_a.len())?;
            buf.copy_from_slice(&payload_a);
            w.commit(1, payload_a.len() as u32, 0)?;
            let buf = w.try_reserve(payload_b.len())?;
            buf.copy_from_slice(&payload_b);
            w.commit(2, payload_b.len() as u32, 0)?;
        }
        // 2) Downgrade to v2-on-disk.
        downgrade_to_v2(&make_channel_file_path(Path::new(v2_base), 0)?)?;

        // 3) Reader::open must refuse the v2 file (format_version=0).
        let err = ReaderBuilder::new(v2_base).build().err().unwrap();
        assert_eq!(err.kind(), ErrorKind::InvalidData);

        // 4) Migrate to a fresh destination.
        migrate_file_v2_to_v3(
            &make_channel_file_path(Path::new(v2_base), 0)?,
            &make_channel_file_path(Path::new(v3_base), 0)?,
        )?;

        // 5) v3 Reader on the migrated file sees both records intact.
        let mut r = ReaderBuilder::new(v3_base).build()?;
        let m0 = r.try_read()?.expect("first record");
        assert_eq!(m0.header().message_type, 1);
        assert_eq!(m0.payload(), &payload_a);
        let m1 = r.try_read()?.expect("second record");
        assert_eq!(m1.header().message_type, 2);
        assert_eq!(m1.payload(), &payload_b);
        assert!(r.try_read()?.is_none());

        cleanup_channel_files(v2_base);
        cleanup_channel_files(v3_base);
        Ok(())
    }

    /// Multi-file: produce a v3 channel that file-rolls, downgrade each
    /// file, migrate the whole channel set, verify a LateJoin Reader
    /// drains every record across rolls.
    #[test]
    fn test_migrate_channel_v2_to_v3_round_trip() -> anyhow::Result<()> {
        let v2_base = "test_migrate_channel_v2_src";
        let v3_base = "test_migrate_channel_v2_dst";
        cleanup_channel_files(v2_base);
        cleanup_channel_files(v3_base);

        let region_size = page_size();
        // Small file_roll_size so we get multiple files.
        let file_roll_size = (region_size as u64) * 2;
        let n_messages = 300u64;
        let payload: [u8; 32] = [0x55; 32];

        // 1) Write enough to force at least one file roll.
        {
            let mut w = WriterBuilder::new(v2_base)
                .region_size(region_size)
                .file_roll_size(file_roll_size)
                .build()?;
            for i in 0..n_messages {
                let buf = w.try_reserve(payload.len())?;
                buf.copy_from_slice(&payload);
                w.commit((i % 256) as u16, payload.len() as u32, i)?;
            }
        }

        // 2) Confirm there's more than one file, then downgrade each.
        let seqs = find_all_sequences(Path::new(v2_base))?;
        assert!(seqs.len() >= 2, "test expected file rolls; got {seqs:?}");
        for &seq in &seqs {
            downgrade_to_v2(&make_channel_file_path(Path::new(v2_base), seq)?)?;
        }

        // 3) Migrate the whole channel set.
        let migrated = migrate_channel_v2_to_v3(v2_base, v3_base)?;
        assert_eq!(migrated, seqs.len());

        // 4) LateJoin Reader on the migrated channel reads every record.
        let mut r = ReaderBuilder::new(v3_base).build()?;
        let mut seen = 0u64;
        while let Some(m) = r.try_read()? {
            assert_eq!(m.payload(), &payload);
            assert_eq!(m.header().user_meta_u64, seen);
            seen += 1;
        }
        assert_eq!(seen, n_messages);

        cleanup_channel_files(v2_base);
        cleanup_channel_files(v3_base);
        Ok(())
    }

    /// Source that is already v3 (format_version=1) must be refused — the
    /// migrator only handles v2 -> v3 and shouldn't silently re-stamp a v3
    /// file or corrupt it.
    #[test]
    fn test_migrate_refuses_v3_source() -> anyhow::Result<()> {
        let v3_src = "test_migrate_refuses_v3_src";
        let v3_dst = "test_migrate_refuses_v3_dst";
        cleanup_channel_files(v3_src);
        cleanup_channel_files(v3_dst);

        WriterBuilder::new(v3_src)
            .region_size(page_size())
            .precreate()?;

        let err = migrate_file_v2_to_v3(
            &make_channel_file_path(Path::new(v3_src), 0)?,
            &make_channel_file_path(Path::new(v3_dst), 0)?,
        )
        .expect_err("must refuse v3 source");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("does not look like v2"));

        cleanup_channel_files(v3_src);
        cleanup_channel_files(v3_dst);
        Ok(())
    }

    /// Destination that already exists must be refused — the migrator
    /// must not clobber a file that may already hold a v3 channel.
    #[test]
    fn test_migrate_refuses_existing_dst() -> anyhow::Result<()> {
        let v2_src = "test_migrate_refuses_existing_src";
        let v3_dst = "test_migrate_refuses_existing_dst";
        cleanup_channel_files(v2_src);
        cleanup_channel_files(v3_dst);

        WriterBuilder::new(v2_src)
            .region_size(page_size())
            .precreate()?;
        downgrade_to_v2(&make_channel_file_path(Path::new(v2_src), 0)?)?;

        WriterBuilder::new(v3_dst)
            .region_size(page_size())
            .precreate()?;

        let err = migrate_file_v2_to_v3(
            &make_channel_file_path(Path::new(v2_src), 0)?,
            &make_channel_file_path(Path::new(v3_dst), 0)?,
        )
        .expect_err("must refuse existing dst");
        assert_eq!(err.kind(), ErrorKind::AlreadyExists);

        cleanup_channel_files(v2_src);
        cleanup_channel_files(v3_dst);
        Ok(())
    }

    /// `migrate_channel_v2_to_v3` against a missing source returns
    /// `ErrorKind::NotFound`.
    #[test]
    fn test_migrate_channel_missing_source_returns_notfound() {
        let missing = "test_migrate_channel_missing_src";
        let dst = "test_migrate_channel_missing_dst";
        cleanup_channel_files(missing);
        cleanup_channel_files(dst);

        let err = migrate_channel_v2_to_v3(missing, dst).err().unwrap();
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }
}
