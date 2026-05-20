# Changelog

## 3.0.0 (2026-05-20)

### Breaking
- `MessageHeader::timestamp_ns` → `user_meta_u64`. Wire bytes
  unchanged; field-access callers must rename.
- `ChannelHeader` v1: adds `format_version`, `endianness`,
  header-size fields, reserved `user_header_kind`. `channel_name`
  shrinks 32 → 20 bytes. Struct still 64 bytes.
- Pre-v1 channel files (`format_version = 0`) refused on open.
  Regenerate on upgrade.

### Added
- `WriterBuilder::channel_name(&str)` + `Reader::channel_name()`.
- `Reader::wait_for_message(timeout) -> io::Result<bool>` cursor
  API. Advances past Skip/Roll/Channel; doesn't consume User
  records.
- `Reader::read_blocking` rewritten on top of `wait_for_message`;
  raw-pointer reborrow removed. Behavior unchanged.
- Single-step writer-crash recovery in `WriterBuilder::build`. A
  committed record at `wp - HEADER_SLOT` (the crash signature
  between `commit` and `publish_wp`) is advanced past, and the
  next slot is verified as a pre-installed header. Multi-record
  lag and other non-pre-installed slots still refuse.
- `Reader::open` (`LateJoin`) retries on the `keep_files`
  scan-then-open race; truly missing channels still fail fast.
- `xchannel::migrate` module: `migrate_file_v2_to_v3` and
  `migrate_channel_v2_to_v3` convert pre-v3 archive files to v3
  out-of-place. Only `ChannelHeader` bytes 48–79 are rewritten;
  records are copied verbatim.
- `FORMAT.md` — language-neutral wire-format spec.

### Docs
- README: `Wire format` section; `Limitations` entries for
  single-step crash recovery and local-filesystem-only constraint.

## 2.2.0 (2026-04-30)

### Added
- `Reader::read_blocking(timeout: Option<Duration>) -> io::Result<Option<MessageRef<'_>>>`
  — synchronous helper that polls `try_read` with adaptive sleep-based
  backoff (1 µs doubling up to a 10 ms cap), returning `Ok(None)` if
  the optional `timeout` elapses first. No writer cooperation required;
  zero impact on existing fast paths. Not safe to call from an async
  executor task — use `try_read` plus the runtime's own sleep instead.

### Docs
- README gains a `Limitations` section documenting the deliberate
  design constraints (single writer, no back-pressure, no
  kernel-mediated wake-up, retention-eviction semantics) so users can
  evaluate the library's fit upfront.
- `Reader::read_blocking` rustdoc now includes a tokio example showing
  the async-runtime equivalent (same backoff loop with the runtime's
  sleep). Keeps xchannel runtime-agnostic — no async deps — while
  giving async callers a tested starting point they can adapt.

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
