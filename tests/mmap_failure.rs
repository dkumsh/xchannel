//! A reader whose next region cannot be mapped reports the error and stays usable.
//!
//! Lives in its own test binary because it caps the whole process's address space
//! (`RLIMIT_AS`) to make `mmap` fail; no other test may run alongside it.

use xchannel::{ReaderBuilder, WriterBuilder, cleanup_channel_files, page_size};

/// Current virtual memory size of this process, in bytes.
fn vm_size() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    let kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmSize:"))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|v| v.parse().ok())
        .expect("VmSize in /proc/self/status");
    kb * 1024
}

fn set_as_limit(soft: libc::rlim_t) -> libc::rlimit {
    let mut old = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    unsafe {
        assert_eq!(libc::getrlimit(libc::RLIMIT_AS, &mut old), 0);
        let new = libc::rlimit {
            rlim_cur: soft,
            rlim_max: old.rlim_max,
        };
        assert_eq!(libc::setrlimit(libc::RLIMIT_AS, &new), 0);
    }
    old
}

#[test]
fn failed_region_map_leaves_the_reader_usable() -> anyhow::Result<()> {
    let base = "test_failed_region_map";
    cleanup_channel_files(base);
    let region_size = page_size();
    let n = 400u64; // ~170 records per region: crosses two region boundaries
    {
        let mut w = WriterBuilder::new(base).region_size(region_size).build()?;
        for i in 0..n {
            w.try_reserve(8)?.copy_from_slice(&i.to_le_bytes());
            w.commit(0, 8, i)?;
        }
    }

    let mut r = ReaderBuilder::new(base).build()?;
    let mut next = 0u64;
    let mut failures = 0;
    // No new mapping can be made while the cap is on, so the first region crossing fails.
    let old = set_as_limit(vm_size() as libc::rlim_t);
    let capped = loop {
        match r.try_read() {
            Ok(Some(m)) => {
                assert_eq!(m.header().user_meta_u64, next);
                next += 1;
            }
            Ok(None) => break None,
            Err(e) => break Some(e),
        }
    };
    set_as_limit(old.rlim_cur);
    let err = capped.expect("crossing into region 1 needs a new mapping");
    assert!(
        next > 0 && next < n,
        "failed at a region boundary, not at the start or end"
    );
    failures += 1;

    // The record that hit the failure is delivered on retry; nothing is lost or repeated.
    while let Some(m) = r.try_read()? {
        assert_eq!(m.header().user_meta_u64, next);
        next += 1;
    }
    assert_eq!(
        next, n,
        "all records delivered after {failures} failure ({err})"
    );
    assert_eq!(r.position(), n);

    cleanup_channel_files(base);
    Ok(())
}
