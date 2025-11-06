use std::path::Path;
use xchannel::{WriterBuilder, cleanup_channel_files, page_size};

#[test]
fn test_current_suffix_behavior() {
    let base = "test_current_suffix";
    cleanup_channel_files(base);
    let region_size = page_size();
    let file_roll_size = (region_size as u64) * 10; // allow manual roll

    {
        let mut writer = WriterBuilder::new(base)
            .region_size(region_size)
            .file_roll_size(file_roll_size)
            .build()
            .expect("build writer");

        // Active file should have .current suffix
    let current0_name = format!("{}.current", base);
    let current0 = Path::new(&current0_name);
        assert!(current0.exists(), "expected initial active file with .current suffix");
        assert!(!Path::new(base).exists(), "completed base file should not exist yet");

        // Write one message
        if let Some(buf) = writer.try_reserve(16) {
            buf.fill(0xAA);
            writer.commit(1, 16).expect("commit");
        }

        // Roll to next file
        writer.roll_file().expect("roll file");

        // After roll: sequence 0 finalized, sequence 1 active
        assert!(Path::new(base).exists(), "sequence 0 file should be finalized without .current");
        assert!(!current0.exists(), "sequence 0 .current should have been renamed");
    let current1_name = format!("{}.1.current", base);
    let current1 = Path::new(&current1_name);
        assert!(current1.exists(), "sequence 1 active file should have .current suffix");
        assert!(!Path::new(&format!("{}.1", base)).exists(), "sequence 1 completed file not yet present");
    } // drop writer finalizes current file

    // After drop: sequence 1 should be finalized (no .current)
    assert!(Path::new(&format!("{}.1", base)).exists(), "sequence 1 completed file should exist after drop");
    assert!(!Path::new(&format!("{}.1.current", base)).exists(), "sequence 1 .current should be gone after drop");

    cleanup_channel_files(base);
}
