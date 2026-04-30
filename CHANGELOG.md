# Changelog

## 2.1.1 (2026-04-30)

### Changed
- License: dropped Apache-2.0; xchannel is now distributed under the MIT
  license only. The `LICENSE-APACHE` file has been removed and
  `Cargo.toml` reflects `license = "MIT"`.

## 2.1.0 (2026-04-30)

### Added
- `WriterBuilder::keep_files(N)` caps the number of channel files retained
  on disk to the active file plus N-1 historical rolled files. Each
  successful roll unlinks the file at sequence `current - N`. Default is
  unlimited retention (no behavior change for existing users). Readers
  still mapped to a pruned file keep reading via the open inode; they
  only fail with `ENOENT` if they fall further behind than N files.
- `cleanup_channel_files` now scans the parent directory for matching
  entries, so it correctly handles the sparse on-disk layouts that
  `keep_files` can leave behind.

### Examples / tests / docs
- Reworked `examples/xchan_bench` for reproducible latency benchmarking:
  Linux-only, real CPU pinning via `sched_setaffinity`, fixed-duration
  runs with a JSON summary on stdout, configurable publish gap
  (`--gap-ns N`), and `--keep-files` pass-through.
- Added `bench/run.sh` matrix runner, `just bench` / `just bench-quick`
  recipes, and reference results (`bench/results-montblanc.md`,
  `bench/results-lse.md`).
- README gains a Benchmarks section with multi-host tables at three
  publish cadences and a "How to read these numbers" interpretation
  guide.
- Test coverage for retention behavior under `keep_files(N)` and for
  the default unlimited-retention case.

## 2.0.0 (2026-03-09)

### Breaking changes
- `Reader::try_read(&mut self)` now returns `io::Result<Option<MessageRef<'_>>>`.
- `Reader::try_read_batch(&mut self, max_batch: Option<u16>)` now returns `io::Result<Option<MessageBatch<'_>>>`.
- `MessageHeader.header_type` is now stored as raw `u8` in the mmap/on-disk layout.
  - Use `MessageHeader::parsed_header_type()` or `HeaderType::from_raw(...)` for checked reads.

### Behavior updates
- Readers now validate raw `committed` and `header_type` bytes and return `InvalidData` on malformed headers.
- `try_read` and `try_read_batch` now distinguish "no committed message yet" from corrupted input.
- Batch scanning reuses a shared header-decode path and a simpler "last mapped region is current scan target" invariant.

### Examples/tests/docs
- Added regression coverage for invalid `header_type` values in mapped headers.
- README examples now show `parsed_header_type()` when code needs to inspect record kind.

## 1.3.0 (2026-03-08)

### Breaking changes
- Replaced `Message` with `MessageRef` in reader-facing APIs.
- Added batched read API:
  - `Reader::try_read_batch(&mut self, max_batch: Option<u16>) -> Option<MessageBatch<'_>>`
- Batch API no longer exposes positional internals (`MsgPos` / positions list) publicly.
  - Use `MessageBatch::len`, `MessageBatch::get`, `MessageBatch::get_unchecked`, and `MessageBatch::iter`.
- Reader builder now supports default batch limit via:
  - `ReaderBuilder::batch_limit(u16)`

### Behavior updates
- `try_read_batch(None)` uses the builder-configured batch limit when present, otherwise unlimited.
- `try_read_batch` can advance over service messages and still return `None` when no user messages are present.
- `open_next_file` now continues from the beginning of the next file in both `Live` and `LateJoin` reader modes.

### Internal/runtime updates
- Reader mapping lifecycle simplified around a single current mapping invariant (`maps.last()` is current).
- Hot path avoids per-batch file-handle cloning and only performs open/mmap operations on actual roll/region transitions.

### Examples/tests
- Added threaded `two_readers` example with burst/batch options and batch-size distribution output.
- Expanded tests for batch reads across regions/files and service-message handling.
