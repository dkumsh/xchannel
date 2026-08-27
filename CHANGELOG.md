# Changelog

## 5.2.0 (2026-08-27)

### Added
- **`Reader::peek_header` and `PeekedHeader`** — the next user record's header (message type,
  user meta, payload length) without consuming the record. Returned by value and borrowing
  nothing, so a caller holding several readers can peek all of them, decide which to take, and
  take only that one — what merging channels in timestamp order needs. Service records on the
  way are consumed exactly as `try_read` consumes them; `Ok(None)` means caught up, not end of
  stream, and peeking twice returns the same header.
- **`Reader::try_read_owned` and `OwnedMessage`** — the same zero-copy view as `try_read`, but
  holding a share of the mapped region instead of borrowing it, so it carries no lifetime.
  A `MessageRef<'_>` is tied to the `&mut self` call that produced it and cannot be stored,
  returned, or sent anywhere; an owned message has no lifetime to leak, and since it is `Send`
  it can cross a thread boundary or outlive the reader entirely.
- **`Reader::read_owned_into`**, which drains available messages into a caller-owned
  `Vec<OwnedMessage>` and returns how many, plus **`Reader::owned_batch`** as the allocating
  wrapper. `max` bounds the pass; `None` drains until caught up. A short count — including `0` —
  means *caught up*, not end of stream: nothing in a channel records that a writer is finished,
  so a later call may yield more. Prefer `Some(n)` on a polling loop: an unbounded pass returns
  only once the reader reaches an uncommitted header, so a writer that keeps committing can keep
  it going, growing the buffer and pinning a region per retained message.

  This is deliberately **not** an `Iterator` over the reader. A lazy iterator driving the read
  cursor is unsound to combine with any adapter that buffers or discards an item — `peekable`,
  `take_while`, `zip`, `chunks` — because every pull *consumes* from the channel and there is no
  way to put a message back. `Peekable::peek` advances the cursor and parks the message inside
  the adapter, and since the adapter borrows the reader you must drop it before reading again,
  which loses that message permanently. Draining into a buffer first makes all of them sound:
  what an adapter discards is a message the caller already holds. It also lets read errors
  surface at the call instead of being flattened into "no more messages".
- **`MessageBatch::get_owned`** — promote one message out of a borrowed batch, for the few
  records a consumer wants to keep past the next poll.
- An `owned_vs_borrowed` example (`just owned-vs-borrowed`) pricing the owned path against the
  borrowed one, and against copying the payload out as a cloning reader would. The refcount
  costs a roughly fixed few nanoseconds while a copy scales with payload, so which is cheaper
  flips with record size — near 300 bytes on the author's hardware.

### Changed
- Region mappings are stored behind `Arc` so an `OwnedMessage` stays valid after the reader has
  pruned that region, rolled past it, or been dropped entirely. The borrowed path is unaffected
  in behaviour and in cost — no refcount is touched unless a message is taken by
  `try_read_owned`; region setup and prune touch the count once per region, not per message.
- **`prune_to_current` is now best-effort rather than a guarantee.** It drops the reader's share;
  the `munmap` happens when the last share goes. Readers that only ever call `try_read` behave
  exactly as before, but a retained `OwnedMessage` keeps its whole region mapped
  (`region_size`, 1 MiB by default), so a consumer that stores messages makes the mapped
  footprint its own responsibility. Copy the payload out if you need to hold it for long.

Purely additive; no format change (`format_version` stays 3).

## 5.1.0 (2026-08-07)

### Fixed
- **A reader following a roll now verifies that the next segment continues the absolute
  numbering** — its `base_record_index` must equal the previous segment's
  `base_record_index + message_count` — and refuses it otherwise, joining the existing
  `channel_sequence` and `generation` checks.
  This catches the case none of the others can: a channel deleted and rebuilt at the same path
  restarts at `channel_sequence` 0 and reuses the very same filenames, so a reader holding an
  unlinked segment (normal under `keep_files` retention) could follow a roll straight into the
  *rebuilt* series with sequence and generation both matching, and silently splice two
  unrelated histories. It also catches segments from two logs sharing a directory, and
  hand-copied or out-of-order files.
  Note an inode check would not work here: retention unlinks files under live readers as a
  matter of course, so "my file vanished" is a false positive on every prune — continuity is
  the discriminator. Purely additive; no format change (`format_version` stays 3).

## 5.0.0 (2026-08-06)

### Changed
- **`format_version = 3`**: `channel_name` widened from 20 to 48 bytes (offsets 49..97),
  taking the space from `_reserved2` (now 23 bytes at 97..120). The header stays 128 bytes
  and every other field keeps its offset — `generation` is still at 120..128, exactly the
  property that placing it last was meant to guarantee — so a v2 file is structurally
  readable. It is nonetheless a version bump and not an additive change: a v3 writer can
  store a name that a v2 reader would silently truncate at 20 bytes, which redefines the
  meaning of bytes [69, 97) rather than adding to unused space. 20 bytes was too small for
  real names (`fills.prod.options-mm` is 21).
- `CHANNEL_NAME_MAX` is now 48, and `ChannelHeader.channel_name` is sized from it directly
  so the public limit and the on-disk field cannot drift apart.

### Removed
- Files at `format_version` 0, 1, or 2 are refused. Like the v2 change before it, v3 is
  greenfield — there is no in-place migration; regenerate with a 5.0 writer or pin an
  older crate version to read them.

## 4.4.0 (2026-08-06)

### Added
- `ChannelHeader.generation` (u64, offset 120..128, consuming reserved space): an opaque
  **incarnation id** for a channel, set at creation via `WriterBuilder::generation(u64)`,
  stamped identically into every segment, carried across rolls, and preserved when a writer
  reopens an existing channel (the on-disk value wins, as with `base_record_index`). Read it
  via `Reader::generation()` / `Writer::generation()`.
  It answers a question a path and a record index cannot: a channel deleted and recreated at
  the same path restarts at sequence 0 and index 0, so it is indistinguishable from a
  truncated one, and a persisted cursor silently refers to a different log. A consumer that
  stores a read position should store the generation with it and treat a change as a
  different channel rather than a gap. A reader that follows a roll into a segment carrying a
  different generation now refuses it (mixed-incarnation directory), mirroring the existing
  `channel_sequence` check. xchannel assigns no meaning to the value.
  Purely additive; no format change (`format_version` stays 2), and the field is last in the
  header so future additive fields cannot move it.

## 4.3.0 (2026-08-06)

### Added
- `Reader::file_sequence() -> u64`: ordinal of the segment file the reader currently has
  open, updated as it follows rolls. Rolls are otherwise invisible to a reader (`Roll`
  markers are consumed transparently), so this is how a consumer *locates* one: sampled
  around a single-record read, a change means the record just returned is the first user
  record of a new segment. Lets a replicator reproduce the origin's file boundaries — and
  therefore its `keep_files` retention — instead of inventing its own. `try_read_batch`
  may span a roll and then reports the last segment touched; the boundary's position
  within that batch is not recoverable. Purely additive; no format change
  (`format_version` stays 2).

## 4.2.0 (2026-07-04)

### Added
- `Reader::region_size() -> usize` and `Reader::mtu() -> u32`: read a channel's geometry
  (region size, and max user payload / MTU) from its header. Lets a consumer recover the
  geometry needed to re-register or replicate a channel without re-deriving it. Purely
  additive; no format change (`format_version` stays 2).

## 4.1.0 (2026-07-04)

### Added
- `Reader::head_record_index() -> io::Result<u64>`: the channel's current head /
  high-water mark (equal to the writer's `next_record_index()`), computed from the
  newest segment on disk so a `LateJoin` reader still catching up — or one parked on
  an older rolled file — reports the true channel frontier rather than the end of the
  file it currently reads. Purely additive; no format change (`format_version` stays 2).

## 4.0.0 (2026-06-21)

### Added
- **`format_version = 2`**: `ChannelHeader` widened from 64 to 128 bytes.
  New field `base_record_index` records the absolute index of a file's first
  user record, counted from channel genesis across all rolls, so the absolute
  record index is monotonic and survives rolls, retention, and writer restart.
  59 bytes are reserved for future additive fields.
- `WriterBuilder::base_record_index(u64)` seeds the first record's absolute
  index when *creating* a channel (default 0; ignored when reopening). Intended
  for replicas of a remote channel whose genesis was retention-truncated.
- `Writer::next_record_index() -> u64` (the channel head, `base_record_index +
  message_count`) and `Reader::base_record_index() -> u64` (the current file's
  base, refreshed as the reader follows rolls).
- On open, writers and readers now verify a segment's `channel_sequence` matches
  the sequence parsed from its file path, refusing a renamed/misplaced/swapped
  segment with an `InvalidData` error.

### Removed
- The v2→v3 migration (`migrate` module and the `xch-migrate` example). With
  the `format_version = 2` change the format is greenfield; cross-version
  migration is no longer provided.

### Changed
- **Breaking / greenfield:** `message_count` is now a per-file count of **user**
  records only — it starts at 0 and is no longer bumped by the `Channel` header
  or `Skip` markers. Combined with the header growth, the records area shifts
  (first user record at offset 144 instead of 80), so v2 does not read v0/v1
  files in place. There is no in-place migration; regenerate channels with a v2
  writer, or keep an older crate version to read old files. A reader that sees
  `format_version != 2` refuses the file.
- `WriterBuilder::build` now rejects a nonzero `file_roll_size` smaller
  than `2 * region_size`. Region 0's head holds the channel header and
  pre-installed first user header, so a single-region file cannot host a
  full-region record — it would roll on the first large message. (Breaking
  for sub-two-region roll sizes, which were never viable.)

### Fixed
- `WriterBuilder::build` now rejects a `file_roll_size` that rounds up
  past `i64::MAX` (e.g. `i64::MAX` itself) with a clear `InvalidInput`
  error naming the limit, instead of failing deep inside `set_len` with
  an opaque "out of range integral type conversion". The prior guard
  only checked `u64` overflow, but `set_len`'s offset is an i64 `off_t`.
- `set_len` failures during segment preallocation (e.g. `EFBIG` when the
  rounded `file_roll_size` exceeds the filesystem's max file size) are
  wrapped with context naming `file_roll_size` / `region_size`.

### Docs
- `WriterBuilder::file_roll_size` documents the `0` = no-rolling
  sentinel, eager (sparse) preallocation, region rounding, and the
  `i64::MAX` cap.

## 3.0.1 (2026-06-04)

### Changed
- Fresh segment files are prepared in `<base>.<N>.partial` (or
  `<base>.partial` for sequence 0), `set_len` to the
  region-rounded `file_roll_size`, channel header + first user
  header pre-installed, then atomically renamed. Closes two
  reader-side SIGBUS races:

  1. **Fresh-file race.** A concurrent reader could open a
     newly-created segment before the writer's `set_len` landed
     and fault on phantom pages. The temp file is invisible to
     `find_all_sequences` until renamed.
  2. **Intra-file region-extension race.** `roll_over_region`
     used to grow the file by one region when crossing a region
     boundary inside a segment. Fresh files now ship preallocated
     to the full `file_roll_size` (rounded up to a region
     boundary), so the `ensure_len` call in `roll_over_region`
     is a no-op for any `file_roll_size > 0`.

  Effect for `file_roll_size = 0` (unbounded single file): the
  intra-file race survives in that one configuration only —
  there is no upper bound to preallocate against.

- Existing files reopened by a 3.0.1+ writer with
  `file_roll_size > 0` are promoted to the preallocated layout
  on first open (migration of channels created by 3.0.0).
- `WriterBuilder::build` sweeps stale `<base>.partial` /
  `<base>.<N>.partial` siblings. The middle component is parsed
  as `u64` so unrelated siblings (`<base>.notes.partial`)
  survive.
- `cleanup_channel_files` also removes `<base>.partial` /
  `<base>.<N>.partial` siblings — partials are crate-created
  artifacts and the public cleanup helper is the canonical
  "fresh start" entry point.
- `WriterBuilder::build` returns `InvalidInput` if
  `file_roll_size` is within `region_size` of `u64::MAX` (i.e.
  rounding up to a region boundary would overflow). Previously
  this would panic in debug or silently wrap in release.
- The slot-i+1 pre-install primarily moves from `commit()` into
  `try_reserve()`, laid down at the reserved-size offset before
  the buffer is returned. The fast path (`length == reserved`)
  needs no cacheline write in `commit`; the short-commit path
  (`length < reserved`) re-lays the pre-install at the actual
  offset inside `commit` before flipping `committed = 1`. Either
  way, FORMAT.md §9.6 holds (slot i+1 is pre-installed before
  commit i is observable) and crash recovery sees the
  pre-installed slot whether the crash falls between reserve and
  commit, between commit and publish_wp, or after publish_wp.
- `roll_file()` publishes the rolled segment in two phases so a
  reader following the Roll marker always finds the next file
  on disk:

  1. Prepare NEW as `<base>.<N+1>.partial`.
  2. Stage OLD's Roll header with `committed=0` (invisible to
     readers).
  3. `rename` NEW's `.partial` to its final name — NEW is now on
     disk under the path readers will open.
  4. Release-store `committed=1` on OLD's Roll header — readers
     wake here, immediately resolve NEW via the path that became
     visible in step 3.
  5. Bump OLD's `write_position` past the Roll marker, then swap
     `self` to NEW.

  Fixes a `ping_pong` regression where readers observed the
  Roll marker before NEW's final name existed, failing the
  reader-side `open()` with `NotFound`.
- `commit(length)` requires `length <= try_reserve(reserved)`.
  `length == reserved` is the common case and goes through the
  fast path (no extra cacheline write). `length < reserved`
  (the "worst-case reserve + commit actual size" pattern) is
  supported: `commit` re-lays the slot-i+1 pre-install at the
  smaller offset before flipping `committed=1`, so the reader's
  walk past record `i` still lands on a well-formed slot.
  `length > reserved` is rejected (the caller would have written
  past the buffer).
- `roll_file()`'s rare grow-to-end branch is `set_len`
  shrink-safe: extends only when `needed_end > self.file_len`,
  so a future code path can't truncate the preallocated OLD
  segment back down to a region boundary.
- Internal refactor: extracted `Writer::prepare_segment_at(partial_path, ...)`
  from `open_file`. Both call sites (initial open and roll) use it;
  `open_file` renames immediately, `roll_file` renames between the
  OLD Roll header's staged write (`committed=0`) and the
  release-store of `committed=1`, so a reader that observes Roll
  always finds NEW under its final name.
- **Default-behaviour change:** `MAP_POPULATE` is no longer set on
  any `mmap` (writer or reader). 3.0.0 pre-faulted every mapping at
  map time, trading a first-touch stall for higher map-time cost;
  with the duplicate region-0 mappings on the roll path now
  collapsed, the front-loaded fault cost was no longer paying for
  itself. Callers that relied on the implicit pre-fault for
  tail-latency SLOs should issue an explicit warmup touch after
  open / after a roll.
- No wire-format change. The `<base>.<N>` final names and the
  bytes inside them are unchanged from 3.0.0.

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
