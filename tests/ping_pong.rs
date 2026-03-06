use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};
use xchannel::{ReaderBuilder, WriterBuilder, cleanup_channel_files, page_size};

/// Forward messages from the ping channel to the pong channel.
///
/// `in_chan` is the channel from which to read (e.g. channel A), and
/// `out_chan` is the channel to which to write (e.g. channel B).  The
/// `region_size` and `file_roll_size` configure the channel mappings.  The
/// process will forward exactly `count` messages before exiting.
fn run_pong(in_chan: &str, out_chan: &str, region_size: usize, file_roll_size: u64, count: u64) {
    let mut reader = ReaderBuilder::new(in_chan)
        .late_join()
        .build()
        .expect("pong failed to open reader");
    let mut writer = WriterBuilder::new(out_chan)
        .region_size(region_size)
        .file_roll_size(file_roll_size)
        .build()
        .expect("pong failed to open writer");

    let mut forwarded = 0u64;
    while forwarded < count {
        if let Some(msg) = reader.try_read() {
            let payload = msg.payload();
            if payload.len() != 8 {
                // Ignore malformed messages
                continue;
            }
            // Decode the payload
            let mut arr = [0u8; 8];
            arr.copy_from_slice(payload);
            let value = u64::from_le_bytes(arr);
            // Forward the payload to the out channel.  Use message_type=0.
            loop {
                if let Ok(buf) = writer.try_reserve(std::mem::size_of::<u64>()) {
                    buf.copy_from_slice(&value.to_le_bytes());
                    writer
                        .commit(0, std::mem::size_of::<u64>() as u32, 0)
                        .expect("pong commit failed");
                    break;
                } else {
                    // Retry after yielding to handle region/file roll
                    std::thread::yield_now();
                }
            }
            forwarded += 1;
        } else {
            std::thread::yield_now();
        }
    }
}

/// Send a sequence of messages on `out_chan` and read back the forwarded
/// messages from `in_chan`, ensuring they match the order they were
/// sent.  If a message does not arrive within one second or if an
/// unexpected value is received, the function panics.
fn run_ping(out_chan: &str, in_chan: &str, region_size: usize, file_roll_size: u64, count: u64) {
    let mut writer = WriterBuilder::new(out_chan)
        .region_size(region_size)
        .file_roll_size(file_roll_size)
        .build()
        .expect("ping failed to open writer");
    let mut reader = ReaderBuilder::new(in_chan)
        .late_join()
        .build()
        .expect("ping failed to open reader");

    let mut pending: VecDeque<u64> = VecDeque::new();
    // Send and read each message sequentially.  After sending a value,
    // we yield once to allow the pong process to run, then wait up to
    // one second for the corresponding pong message.  This per-message
    // timeout ensures that if forwarding stalls, the test fails early.
    for seq in 0..count {
        // Write the sequence value to the out channel
        loop {
            if let Ok(buf) = writer.try_reserve(std::mem::size_of::<u64>()) {
                buf.copy_from_slice(&seq.to_le_bytes());
                writer
                    .commit(0, std::mem::size_of::<u64>() as u32, 0)
                    .expect("ping commit failed");
                pending.push_back(seq);
                break;
            } else {
                std::thread::yield_now();
            }
        }

        // Give the pong process a chance to forward the message
        std::thread::yield_now();
        // Read back the corresponding pong value, timing out after 1 second
        let start = Instant::now();
        loop {
            if let Some(msg) = reader.try_read() {
                let payload = msg.payload();
                assert_eq!(payload.len(), 8);
                let mut arr = [0u8; 8];
                arr.copy_from_slice(payload);
                let value = u64::from_le_bytes(arr);
                let expected = pending.pop_front().expect("ping pending empty");
                assert_eq!(
                    value, expected,
                    "ping expected {} but got {}",
                    expected, value
                );
                break;
            } else {
                if start.elapsed() > Duration::from_secs(1) {
                    panic!(
                        "ping timed out waiting for pong of {}",
                        pending.front().unwrap()
                    );
                }
                std::thread::yield_now();
            }
        }
    }
}

/// Test a simple ping-pong pattern between two processes using two channel files.
///
/// Process A (ping) writes a sequence of `count` messages to channel `a`
/// and reads them back from channel `b` after they have been forwarded
/// by process B (pong).  Process B reads from `a` and forwards each
/// message to `b`.  The test ensures that messages arrive in order,
/// that mismatches are detected, and that timeouts are handled.
#[test]
fn xchannel_ping_pong() -> io::Result<()> {
    let chan_a = "ping_ch_a";
    let chan_b = "ping_ch_b";
    // Remove any leftover files
    cleanup_channel_files(chan_a);
    cleanup_channel_files(chan_b);

    // Use a relatively small region size to exercise region rollovers. Each
    // message consumes 24 bytes (header + payload), so a 4 KiB region can
    // store around 170 messages before rolling.
    let region_size = page_size();
    // Set the file roll size to cause at least one file roll during the
    // test. With 100k messages at ~24 bytes each, a 100 kB file roll size
    // will cause multiple rolls.
    let file_roll_size = 100_000u64;
    let count = 100_000u64;

    // Pre-create the channel files so readers can open them immediately.
    WriterBuilder::new(chan_a)
        .region_size(region_size)
        .file_roll_size(file_roll_size)
        .precreate()
        .expect("failed to pre-create chan_a");

    WriterBuilder::new(chan_b)
        .region_size(region_size)
        .file_roll_size(file_roll_size)
        .precreate()
        .expect("failed to pre-create chan_b");

    unsafe {
        let pid = libc::fork();
        if pid < 0 {
            panic!("fork failed");
        }
        if pid == 0 {
            // Child process runs pong
            run_pong(chan_a, chan_b, region_size, file_roll_size, count);
            libc::_exit(0);
        }
        // Parent runs ping
        run_ping(chan_a, chan_b, region_size, file_roll_size, count);
        let mut status: libc::c_int = 0;
        libc::waitpid(pid, &mut status, 0);
    }
    // Cleanup
    cleanup_channel_files(chan_a);
    cleanup_channel_files(chan_b);
    Ok(())
}
