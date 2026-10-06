#![cfg(target_os = "linux")]

use std::sync::Mutex;
use std::time::Duration;
use xchannel::{ReaderBuilder, WriterBuilder, cleanup_channel_files, page_size};

/// The thread checks below count threads by name across the whole process.
static SERIAL: Mutex<()> = Mutex::new(());

fn channel(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("xch-prefault-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("chan").to_str().unwrap().to_owned()
}

fn prefault_threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("comm")).ok())
        .filter(|comm| comm.trim() == "xch-prefault")
        .count()
}

fn minor_faults_of_this_thread() -> i64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    unsafe {
        libc::getrusage(libc::RUSAGE_THREAD, usage.as_mut_ptr());
        usage.assume_init().ru_minflt
    }
}

fn write(path: &str, prefault: bool, records: u64, pace: Option<Duration>) -> i64 {
    let mut writer = WriterBuilder::new(path)
        .region_size(page_size() * 16)
        .file_roll_size(page_size() as u64 * 64)
        .prefault(prefault)
        .build()
        .unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let before = minor_faults_of_this_thread();
    for seq in 0..records {
        let buf = writer.try_reserve(96).unwrap();
        buf[..8].copy_from_slice(&seq.to_le_bytes());
        writer.commit(1, 96, seq).unwrap();
        if let Some(pace) = pace {
            std::thread::sleep(pace);
        }
    }
    minor_faults_of_this_thread() - before
}

#[test]
fn every_record_reads_back_across_regions_and_rolls() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let path = channel("readback");
    write(&path, true, 20_000, None);

    let mut reader = ReaderBuilder::new(&path).late_join().build().unwrap();
    let mut next = 0u64;
    while let Some(msg) = reader.try_read().unwrap() {
        let seq = u64::from_le_bytes(msg.payload()[..8].try_into().unwrap());
        assert_eq!(seq, next);
        assert_eq!(msg.header().user_meta_u64, seq);
        next += 1;
    }
    assert_eq!(next, 20_000);
    cleanup_channel_files(&path);
}

#[test]
fn the_prefault_thread_stops_with_its_writer() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let path = channel("stops");
    let before = prefault_threads();
    let writer = WriterBuilder::new(&path).prefault(true).build().unwrap();
    assert_eq!(prefault_threads(), before + 1);
    drop(writer);
    assert_eq!(prefault_threads(), before);
    cleanup_channel_files(&path);
}

#[test]
fn the_writer_thread_takes_far_fewer_faults() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let pace = Some(Duration::from_micros(50));
    let off = channel("faults-off");
    let on = channel("faults-on");
    let without = write(&off, false, 3_000, pace);
    let with = write(&on, true, 3_000, pace);
    assert!(with * 4 < without, "{with} faults with prefault, {without} without");
    cleanup_channel_files(&off);
    cleanup_channel_files(&on);
}
