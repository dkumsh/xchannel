# Changelog

## 3.0.0 (2026-05-20)

### Breaking
- `MessageHeader::timestamp_ns` renamed to `user_meta_u64`. The 8-byte
  slot is unchanged on the wire; xchannel itself never reads it.
  Applications can use it as a timestamp, sequence number, schema tag,
  packed flags, or any other 64-bit value. The third parameter of
  `Writer::commit` is renamed accordingly. Positional callers are
  unaffected; field-access and named-argument callers must rename.
- `ChannelHeader` layout changes to add `format_version`, `endianness`,
  `system_header_size`, `user_header_size`, and a reserved
  `user_header_kind` field (reusing space from the previously unused
  `channel_name`, which shrinks from 32 to 20 bytes). The struct is
  still 64 bytes.
- Files written by xchannel ≤ 2.2 (`format_version = 0`) are no longer
  supported. The new writer emits `format_version = 1`; readers refuse
  any other version or mismatched endianness. Regenerate channel files
  on upgrade.

### Added
- `WriterBuilder::channel_name(&str)` — persist a short channel label
  in the file's `ChannelHeader` (up to 20 UTF-8 bytes). Exposed via
  `Reader::channel_name()`.
- `FORMAT.md` — language-neutral byte-level specification of the wire
  format. Intended as the contract for non-Rust implementations and as
  documentation of the system-owned vs user-owned header invariants.
  Documents `user_header_kind` as a reserved discriminant for
  forward-compatible alternative user-meta layouts; current writers
  emit 0 and current readers refuse anything else.

### Added (crash safety)
- `WriterBuilder::build` on an existing channel file now performs
  single-step writer-crash recovery. If the slot at
  `ChannelHeader.write_position - HEADER_SLOT` holds a committed
  record (the on-disk signature of a writer that crashed between
  `commit` and `publish_wp`), the open advances past it by the
  record's own `length`, verifies the new slot bears the pre-install
  signature `commit()` writes one step ahead of itself, updates
  `write_position`, and resumes. Handles both User-record and
  Skip-record (in-region roll) crashes. Multi-record lag and any
  non-pre-installed slot still refuse with `ErrorKind::InvalidData`;
  the fallback is `cleanup_channel_files` and a fresh channel.
  Recovery touches only `write_position`, never record bytes —
  readers continue draining what was committed.

### Docs
- README gains a `Wire format` section pointing at `FORMAT.md` and
  explaining the `user_meta_u64` model.
- README `Limitations` gains two new entries: an explicit no-crash-
  recovery contract for writers (matching the detection above), and a
  note that channel files must live on a local filesystem — `mmap`
  cross-process coherence does not hold over NFS / SMB / some FUSE
  backends.

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
