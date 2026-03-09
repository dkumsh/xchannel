# Changelog

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
