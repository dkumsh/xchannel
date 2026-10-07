//! Marks one prepared file roll on the writer and one on a reader with `getppid`, for
//! `strace -f` to show what each hot thread calls in between:
//!
//! ```text
//! cargo build --release --example roll-syscalls
//! strace -f -o /tmp/roll.trace target/release/examples/roll-syscalls /dev/shm/roll-syscalls
//! ```
//!
//! Prints the thread ids to look for. A second argument, `wake` or `nohelper`, rolls a channel
//! whose writer wakes readers, or one without helpers. Linux only.

use std::time::Duration;
use xchannel::{Helper, ReaderBuilder, WriterBuilder, cleanup_channel_files, page_size};

fn mark() {
    unsafe { libc::syscall(libc::SYS_getppid) };
}

fn tid() -> i64 {
    unsafe { libc::syscall(libc::SYS_gettid) }
}

fn main() -> std::io::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/dev/shm/roll-syscalls".into());
    let mode = std::env::args().nth(2).unwrap_or_default();
    let helper = (mode != "nohelper").then(Helper::inherit);
    cleanup_channel_files(&path);
    let region = page_size() * 16;
    let file = region as u64 * 4;
    let mut writer = WriterBuilder::new(&path)
        .region_size(region)
        .file_roll_size(file)
        .keep_files(2)
        .wake_readers(mode == "wake");
    let mut reader = ReaderBuilder::new(&path);
    if let Some(helper) = helper {
        writer = writer.helper(helper);
        reader = reader.helper(helper);
    }
    let (mut writer, mut reader) = (writer.build()?, reader.build()?);

    let per_record = 16 + 96;
    let to_last_half = (file as usize - region / 4) / per_record;
    let mut seq = 0u64;
    let mut write = |w: &mut xchannel::Writer, n: u64| -> std::io::Result<()> {
        for _ in 0..n {
            w.try_reserve(96)?.fill(seq as u8);
            w.commit(1, 96, seq)?;
            seq += 1;
        }
        Ok(())
    };
    write(&mut writer, to_last_half as u64)?;
    while reader.try_read()?.is_some() {}
    std::thread::sleep(Duration::from_millis(100)); // both helpers prepare the next file

    let writer_tid = tid();
    mark();
    writer.roll_file()?;
    mark();
    write(&mut writer, 1)?;
    std::thread::sleep(Duration::from_millis(100));

    let reader_tid = std::thread::scope(|s| {
        s.spawn(|| -> std::io::Result<i64> {
            let tid = tid();
            mark();
            let got = reader.try_read()?.is_some();
            mark();
            assert!(got && reader.file_sequence() == 1, "rolled and read");
            Ok(tid)
        })
        .join()
        .expect("reader thread")
    })?;
    assert_eq!(writer.file_sequence(), 1);
    assert!(writer.helper_error().is_none() && reader.helper_error().is_none());
    println!("writer tid {writer_tid}, reader tid {reader_tid}");
    drop((writer, reader));
    cleanup_channel_files(&path);
    Ok(())
}
