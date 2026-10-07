#![cfg(target_os = "linux")]

use std::sync::Mutex;
use std::time::Duration;
use xchannel::{Helper, ReaderBuilder, Unmap, WriterBuilder, cleanup_channel_files, page_size};

/// The checks below count threads and mappings across the whole process.
static SERIAL: Mutex<()> = Mutex::new(());

const REGION_PAGES: usize = 16;
const FILE_PAGES: u64 = 64;

fn channel(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("xch-helper-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("chan").to_str().unwrap().to_owned()
}

fn threads_named(name: &str) -> usize {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("comm")).ok())
        .filter(|comm| comm.trim() == name)
        .count()
}

fn mappings_of(file: &str) -> usize {
    std::fs::read_to_string("/proc/self/maps")
        .unwrap()
        .lines()
        .filter(|line| line.ends_with(&format!(" {file}")))
        .count()
}

fn minor_faults_of_this_thread() -> i64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    unsafe {
        libc::getrusage(libc::RUSAGE_THREAD, usage.as_mut_ptr());
        usage.assume_init().ru_minflt
    }
}

fn eventually(mut done: impl FnMut() -> bool) -> bool {
    for _ in 0..500 {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    false
}

fn writer_builder(path: &str, helper: Option<Helper>) -> WriterBuilder {
    let builder = WriterBuilder::new(path)
        .region_size(page_size() * REGION_PAGES)
        .file_roll_size(page_size() as u64 * FILE_PAGES);
    match helper {
        Some(helper) => builder.helper(helper),
        None => builder,
    }
}

fn write(path: &str, helper: Option<Helper>, records: u64, pace: Option<Duration>) -> i64 {
    let mut writer = writer_builder(path, helper).build().unwrap();
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

fn read_all(reader: &mut xchannel::Reader, pace: Option<Duration>) -> u64 {
    let mut next = 0u64;
    while let Some(msg) = reader.try_read().unwrap() {
        let seq = u64::from_le_bytes(msg.payload()[..8].try_into().unwrap());
        assert_eq!(seq, next);
        assert_eq!(msg.header().user_meta_u64, seq);
        next += 1;
        if let Some(pace) = pace {
            std::thread::sleep(pace);
        }
    }
    next
}

#[test]
fn every_record_reads_back_across_regions_and_rolls() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let path = channel("readback");
    write(&path, Some(Helper::inherit()), 20_000, None);

    for unmap in [Unmap::Immediate, Unmap::AtFileRoll] {
        let mut reader = ReaderBuilder::new(&path)
            .late_join()
            .helper(Helper::inherit())
            .unmap(unmap)
            .build()
            .unwrap();
        assert_eq!(read_all(&mut reader, None), 20_000);
        assert!(reader.helper_error().is_none());

        let mut reader = ReaderBuilder::new(&path)
            .late_join()
            .helper(Helper::inherit())
            .unmap(unmap)
            .build()
            .unwrap();
        let mut read = 0;
        while let Some(batch) = reader.try_read_batch(Some(300)).unwrap() {
            for msg in batch.iter() {
                assert_eq!(msg.header().user_meta_u64, read);
                read += 1;
            }
        }
        assert_eq!(read, 20_000);
    }
    cleanup_channel_files(&path);
}

#[test]
fn the_helpers_stop_with_their_writer_and_reader() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let path = channel("stops");
    // Helpers of the test before may still be exiting.
    assert!(eventually(|| threads_named("xch-prefault") == 0));
    assert!(eventually(|| threads_named("xch-map-ahead") == 0));
    let (writers, readers) = (0, 0);
    let writer = WriterBuilder::new(&path)
        .helper(Helper::inherit())
        .build()
        .unwrap();
    let reader = ReaderBuilder::new(&path)
        .helper(Helper::inherit())
        .build()
        .unwrap();
    assert!(eventually(|| threads_named("xch-prefault") == writers + 1));
    assert!(eventually(|| threads_named("xch-map-ahead") == readers + 1));
    drop(reader);
    drop(writer);
    // Joined, but the kernel may list a thread for a moment after it has exited.
    assert!(eventually(|| threads_named("xch-prefault") == writers));
    assert!(eventually(|| threads_named("xch-map-ahead") == readers));
    cleanup_channel_files(&path);
}

/// The highest core this thread may run on, and so a helper too.
fn last_allowed_core() -> usize {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::cpu_set_t>();
    assert_eq!(unsafe { libc::sched_getaffinity(0, size, &mut set) }, 0);
    (0..libc::CPU_SETSIZE as usize)
        .rev()
        .find(|&core| unsafe { libc::CPU_ISSET(core, &set) })
        .unwrap()
}

#[test]
fn a_helper_on_a_core_runs_there() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let path = channel("pinned");
    let core = last_allowed_core();
    let _writer = writer_builder(&path, None).build().unwrap();
    let _reader = ReaderBuilder::new(&path)
        .helper(Helper::on_core(core))
        .build()
        .unwrap();
    // `build` returns once the helper is pinned and named.
    let task = std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|t| t.ok().map(|t| t.path()))
        .find(|t| {
            std::fs::read_to_string(t.join("comm")).is_ok_and(|c| c.trim() == "xch-map-ahead")
        })
        .expect("the helper is named by the time build returns");
    let allowed = std::fs::read_to_string(task.join("status"))
        .unwrap()
        .lines()
        .find_map(|l| {
            l.strip_prefix("Cpus_allowed_list:")
                .map(|v| v.trim().to_owned())
        });
    assert_eq!(allowed, Some(core.to_string()));
    cleanup_channel_files(&path);
}

/// A core the helper cannot be pinned to fails the build instead of leaving the helper on the
/// builder's core, or aborting the process for a core past what the OS can name.
#[test]
fn a_core_the_helper_cannot_run_on_fails_the_build() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let path = channel("bad-core");
    assert!(eventually(|| threads_named("xch-prefault") == 0));
    assert!(eventually(|| threads_named("xch-map-ahead") == 0));
    let (writers, readers) = (0, 0);
    let offline = (0..libc::CPU_SETSIZE as usize)
        .rev()
        .find(|&core| !std::path::Path::new(&format!("/sys/devices/system/cpu/cpu{core}")).exists())
        .unwrap();
    for core in [usize::MAX, libc::CPU_SETSIZE as usize, offline] {
        let err = writer_builder(&path, Some(Helper::on_core(core)))
            .build()
            .err()
            .unwrap_or_else(|| panic!("core {core} accepted"));
        assert!(err.to_string().contains(&format!("core {core}")), "{err}");
        let err = ReaderBuilder::new(&path)
            .helper(Helper::on_core(core))
            .build()
            .err()
            .unwrap_or_else(|| panic!("core {core} accepted"));
        assert!(err.to_string().contains(&format!("core {core}")), "{err}");
    }
    assert!(eventually(|| threads_named("xch-prefault") == writers));
    assert!(eventually(|| threads_named("xch-map-ahead") == readers));
    cleanup_channel_files(&path);
}

#[test]
fn the_writer_thread_takes_far_fewer_faults() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let pace = Some(Duration::from_micros(50));
    let off = channel("writer-faults-off");
    let on = channel("writer-faults-on");
    let without = write(&off, None, 3_000, pace);
    let with = write(&on, Some(Helper::inherit()), 3_000, pace);
    assert!(
        with * 4 < without,
        "{with} faults with a helper, {without} without"
    );
    cleanup_channel_files(&off);
    cleanup_channel_files(&on);
}

#[test]
fn the_reader_thread_takes_far_fewer_faults() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let path = channel("reader-faults");
    let mut writer = WriterBuilder::new(&path)
        .region_size(1 << 20)
        .build()
        .unwrap();
    for seq in 0..40_000u64 {
        writer.try_reserve(96).unwrap()[..8].copy_from_slice(&seq.to_le_bytes());
        writer.commit(1, 96, seq).unwrap();
    }
    let faults = |helper: Option<Helper>| {
        let mut builder = ReaderBuilder::new(&path).late_join();
        if let Some(helper) = helper {
            builder = builder.helper(helper);
        }
        let mut reader = builder.build().unwrap();
        for _ in 0..9_000 {
            reader.try_read().unwrap().unwrap();
        }
        let mut faults = 0;
        for _ in 0..3 {
            std::thread::sleep(Duration::from_millis(20));
            let before = minor_faults_of_this_thread();
            for _ in 0..9_000 {
                reader.try_read().unwrap().unwrap();
            }
            faults += minor_faults_of_this_thread() - before;
        }
        faults
    };
    let without = faults(None);
    let with = faults(Some(Helper::inherit()));
    assert!(
        with * 4 < without,
        "{with} faults with a helper, {without} without"
    );
    cleanup_channel_files(&path);
}

#[test]
fn unmap_at_file_roll_keeps_a_segment_mapped_until_the_reader_leaves_it() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let path = channel("at-roll");
    let regions_per_file = (FILE_PAGES as usize / REGION_PAGES) as u64;
    let per_region = (page_size() * REGION_PAGES / 112) as u64;
    write(&path, None, per_region * regions_per_file + 100, None);

    for helper in [None, Some(Helper::inherit())] {
        let mut builder = ReaderBuilder::new(&path)
            .late_join()
            .unmap(Unmap::AtFileRoll);
        if let Some(helper) = helper {
            builder = builder.helper(helper);
        }
        let mut reader = builder.build().unwrap();
        for _ in 0..per_region * (regions_per_file - 1) + 10 {
            reader.try_read().unwrap().unwrap();
        }
        assert!(
            mappings_of(&path) >= regions_per_file as usize,
            "{} mappings of the first segment",
            mappings_of(&path)
        );
        while reader.file_sequence() == 0 {
            reader.try_read().unwrap().unwrap();
        }
        reader.try_read().unwrap();
        assert!(
            eventually(|| mappings_of(&path) == 0),
            "{} left",
            mappings_of(&path)
        );
    }
    cleanup_channel_files(&path);
}

fn tids_named(name: &str) -> Vec<String> {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|t| {
            let t = t.ok()?.path();
            let comm = std::fs::read_to_string(t.join("comm")).ok()?;
            (comm.trim() == name).then(|| t.file_name().unwrap().to_string_lossy().into_owned())
        })
        .collect()
}

#[test]
fn the_writer_helper_follows_rolls_and_keeps_segments_their_size() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let path = channel("writer-rolls");
    let before = tids_named("xch-prefault");
    let mut writer = writer_builder(&path, Some(Helper::inherit()))
        .build()
        .unwrap();
    let ours = || -> Vec<_> {
        tids_named("xch-prefault")
            .into_iter()
            .filter(|t| !before.contains(t))
            .collect()
    };
    assert!(eventually(|| ours().len() == 1));
    let ours = ours();
    for seq in 0..20_000u64 {
        writer.try_reserve(96).unwrap()[..8].copy_from_slice(&seq.to_le_bytes());
        writer.commit(1, 96, seq).unwrap();
        if seq % 500 == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    let rolled = (1..)
        .take_while(|n| std::path::Path::new(&format!("{path}.{n}")).exists())
        .count() as u64;
    assert!(rolled >= 5);
    assert!(tids_named("xch-prefault").contains(&ours[0]));
    assert!(writer.helper_error().is_none());
    let segment = page_size() as u64 * FILE_PAGES;
    for seq in 0..rolled {
        let file = if seq == 0 {
            path.clone()
        } else {
            format!("{path}.{seq}")
        };
        assert_eq!(std::fs::metadata(&file).unwrap().len(), segment, "{file}");
    }
    drop(writer);
    let mut reader = ReaderBuilder::new(&path).late_join().build().unwrap();
    assert_eq!(read_all(&mut reader, None), 20_000);
    cleanup_channel_files(&path);
}

#[test]
fn a_writer_unmapping_at_file_roll_keeps_a_segment_mapped_until_it_leaves_it() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let regions_per_file = (FILE_PAGES as usize / REGION_PAGES) as u64;
    let per_region = (page_size() * REGION_PAGES / 112) as u64;
    for helper in [None, Some(Helper::inherit())] {
        let path = channel("writer-at-roll");
        let mut writer = writer_builder(&path, helper)
            .unmap(Unmap::AtFileRoll)
            .build()
            .unwrap();
        let mut seq = 0u64;
        let mut write = |writer: &mut xchannel::Writer, n: u64| {
            for _ in 0..n {
                writer.try_reserve(96).unwrap()[..8].copy_from_slice(&seq.to_le_bytes());
                writer.commit(1, 96, seq).unwrap();
                seq += 1;
            }
        };
        write(&mut writer, per_region * (regions_per_file - 1) + 10);
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            mappings_of(&path) >= regions_per_file as usize,
            "{} mappings of the first segment",
            mappings_of(&path)
        );
        while writer.file_sequence() == 0 {
            write(&mut writer, 1);
        }
        assert!(
            eventually(|| mappings_of(&path) == 0),
            "{} left",
            mappings_of(&path)
        );
        drop(writer);
        cleanup_channel_files(&path);
    }
}

#[test]
fn the_next_segment_is_created_before_the_roll() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let path = channel("next-segment");
    let regions_per_file = (FILE_PAGES as usize / REGION_PAGES) as u64;
    let per_region = (page_size() * REGION_PAGES / 112) as u64;
    let mut writer = writer_builder(&path, Some(Helper::inherit()))
        .build()
        .unwrap();
    let mut seq = 0u64;
    let mut write = |writer: &mut xchannel::Writer, n: u64| {
        for _ in 0..n {
            writer.try_reserve(96).unwrap()[..8].copy_from_slice(&seq.to_le_bytes());
            writer.commit(1, 96, seq).unwrap();
            seq += 1;
        }
    };
    write(
        &mut writer,
        per_region * (regions_per_file - 1) + per_region * 3 / 4,
    );
    // Installed under its final name ahead of the roll, but not history yet.
    assert!(eventually(|| exists(&format!("{path}.1"))));
    assert_eq!(writer.file_sequence(), 0);
    assert_eq!(partials(&path), 0);
    let live = ReaderBuilder::new(&path).live().build().unwrap();
    assert_eq!(live.file_sequence(), 0);
    write(&mut writer, per_region);
    assert_eq!(writer.file_sequence(), 1);
    assert_eq!(partials(&path), 0);
    drop(writer);

    let mut reader = ReaderBuilder::new(&path).late_join().build().unwrap();
    let total = per_region * regions_per_file + per_region * 3 / 4;
    assert_eq!(read_all(&mut reader, None), total);
    assert_eq!(reader.position(), total);
    cleanup_channel_files(&path);
}

#[test]
fn a_live_reader_follows_rolls_with_both_helpers() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let path = channel("live-rolls");
    let mut writer = writer_builder(&path, Some(Helper::inherit()))
        .unmap(Unmap::AtFileRoll)
        .keep_files(3)
        .build()
        .unwrap();
    let mut reader = ReaderBuilder::new(&path)
        .live()
        .helper(Helper::inherit())
        .unmap(Unmap::AtFileRoll)
        .build()
        .unwrap();
    let total = 30_000u64;
    let writing = std::thread::spawn(move || {
        for seq in 0..total {
            writer.try_reserve(96).unwrap()[..8].copy_from_slice(&seq.to_le_bytes());
            writer.commit(1, 96, seq).unwrap();
            if seq % 200 == 0 {
                std::thread::sleep(Duration::from_micros(300));
            }
        }
    });
    let mut next = 0u64;
    while next < total {
        if let Some(msg) = reader.try_read().unwrap() {
            assert_eq!(msg.header().user_meta_u64, next);
            next += 1;
        }
    }
    writing.join().unwrap();
    assert_eq!(reader.position(), total);
    assert!(reader.helper_error().is_none());
    cleanup_channel_files(&path);
}

fn write_from(writer: &mut xchannel::Writer, from: u64, n: u64) -> u64 {
    for seq in from..from + n {
        writer.try_reserve(96).unwrap()[..8].copy_from_slice(&seq.to_le_bytes());
        writer.commit(1, 96, seq).unwrap();
    }
    from + n
}

fn last_region_three_quarters() -> u64 {
    let regions_per_file = (FILE_PAGES as usize / REGION_PAGES) as u64;
    let per_region = (page_size() * REGION_PAGES / 112) as u64;
    per_region * (regions_per_file - 1) + per_region * 3 / 4
}

fn exists(path: &str) -> bool {
    std::path::Path::new(path).exists()
}

/// Private attempt files of the channel at `path`.
fn partials(path: &str) -> usize {
    let p = std::path::Path::new(path);
    let name = p.file_name().unwrap().to_str().unwrap().to_owned();
    std::fs::read_dir(p.parent().unwrap())
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n.starts_with(&name) && n.ends_with(".partial"))
        .count()
}

#[test]
fn a_writer_with_a_helper_rolls_fast_without_losing_a_segment() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for round in 0..5 {
        let path = channel(&format!("fast-rolls-{round}"));
        let mut writer = writer_builder(&path, Some(Helper::inherit()))
            .build()
            .unwrap();
        let total = write_from(&mut writer, 0, 100_000);
        assert!(writer.helper_error().is_none());
        drop(writer);
        let mut reader = ReaderBuilder::new(&path).late_join().build().unwrap();
        assert_eq!(read_all(&mut reader, None), total);
        cleanup_channel_files(&path);
    }
}

#[test]
fn a_writer_dropped_with_the_next_segment_prepared_leaves_none_and_resumes() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let path = channel("dropped-prepared");
    let next = format!("{path}.1");
    let mut writer = writer_builder(&path, Some(Helper::inherit()))
        .build()
        .unwrap();
    let written = write_from(&mut writer, 0, last_region_three_quarters());
    assert!(eventually(|| exists(&next)));
    drop(writer);
    assert!(
        !exists(&next),
        "the unpublished successor goes with its writer"
    );

    let mut writer = writer_builder(&path, Some(Helper::inherit()))
        .build()
        .unwrap();
    let written = write_from(&mut writer, written, 2_000);
    assert!(exists(&format!("{path}.1")));
    drop(writer);
    let mut reader = ReaderBuilder::new(&path).late_join().build().unwrap();
    assert_eq!(read_all(&mut reader, None), written);
    cleanup_channel_files(&path);
}

/// Run by `a_writer_killed_with_the_next_segment_prepared_is_recovered` in a child process.
#[test]
#[ignore]
fn killed_writer_child() {
    let Ok(path) = std::env::var("XCH_KILLED_WRITER") else {
        return;
    };
    let mut writer = writer_builder(&path, Some(Helper::inherit()))
        .build()
        .unwrap();
    write_from(&mut writer, 0, last_region_three_quarters());
    assert!(eventually(|| exists(&format!("{path}.1"))));
    std::process::abort();
}

#[test]
fn a_writer_killed_with_the_next_segment_prepared_is_recovered() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let path = channel("killed-prepared");
    let next = format!("{path}.1");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "killed_writer_child", "--nocapture"])
        .env("XCH_KILLED_WRITER", &path)
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success());
    assert!(exists(&next), "the killed writer left its prepared segment");
    let live = ReaderBuilder::new(&path).live().build().unwrap();
    assert_eq!(
        live.file_sequence(),
        0,
        "an unpublished segment is not the tail"
    );
    drop(live);

    for helper in [None, Some(Helper::inherit())] {
        let mut writer = writer_builder(&path, helper).build().unwrap();
        if helper.is_none() {
            assert!(!exists(&next), "discarded by the next writer");
        }
        let mut reader = ReaderBuilder::new(&path).late_join().build().unwrap();
        let from = read_all(&mut reader, None);
        drop(reader);
        write_from(&mut writer, from, 2_000);
        drop(writer);
    }
    let mut reader = ReaderBuilder::new(&path).late_join().build().unwrap();
    assert_eq!(
        read_all(&mut reader, None),
        last_region_three_quarters() + 4_000
    );
    assert!(exists(&format!("{path}.1")));
    cleanup_channel_files(&path);
}

#[test]
fn a_reader_that_opened_an_abandoned_segment_ahead_reads_the_real_one() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let per_region = (page_size() * REGION_PAGES / 112) as u64;
    for restarted_helper in [None, Some(Helper::inherit())] {
        let path = channel("abandoned-ahead");
        let next = format!("{path}.1");
        let mut writer = writer_builder(&path, Some(Helper::inherit()))
            .build()
            .unwrap();
        let mut reader = ReaderBuilder::new(&path)
            .late_join()
            .helper(Helper::inherit())
            .build()
            .unwrap();
        let mut written = write_from(&mut writer, 0, last_region_three_quarters());
        assert!(eventually(|| exists(&next)));
        let mut read = read_all(&mut reader, None);
        assert_eq!(read, written);
        std::thread::sleep(Duration::from_millis(30));
        drop(writer);

        let mut writer = writer_builder(&path, restarted_helper).build().unwrap();
        written = write_from(&mut writer, written, per_region * 2);
        while read < written {
            let msg = reader
                .try_read()
                .unwrap()
                .expect("a record the writer committed");
            assert_eq!(msg.header().user_meta_u64, read);
            assert_eq!(
                u64::from_le_bytes(msg.payload()[..8].try_into().unwrap()),
                read
            );
            read += 1;
        }
        assert_eq!(reader.position(), written);
        drop(writer);
        drop(reader);
        cleanup_channel_files(&path);
    }
}
