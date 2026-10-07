# xchannel on-disk format

This document specifies the byte-level format of an xchannel file so that
non-Rust implementations (readers, validators, archival tools) can interoperate
with files produced by the Rust crate. The Rust source in `src/channel.rs`
and `src/lib.rs` is the executable reference; this document is the contract.

Status: **draft, format_version = 4.**

---

## 1. Conventions

- **Endianness:** all multi-byte integer fields of `ChannelHeader` and
  `MessageHeader` (including `user_meta_u64`) are encoded little-endian.
  Payload bytes are opaque to xchannel and may use any encoding the
  application chooses; the `endianness` discriminant in `ChannelHeader`
  describes the framing only. Big-endian framing hosts are not currently
  supported; a future version may declare `endianness = 0x02` for
  big-endian framing.
- **Alignment:** all integer fields are naturally aligned within the header
  structs. Header slots are 8-byte aligned within a region.
- **Atomic semantics:** the `committed` byte of every `MessageHeader` is the
  single synchronization point between writer and readers. Writers publish
  with `memory_order_release`; readers observe with `memory_order_acquire`.
  In C11/C++20 terms: `atomic_store_explicit(&hdr->committed, 1,
  memory_order_release)` / `atomic_load_explicit(&hdr->committed,
  memory_order_acquire)`. No other field in the file requires atomic access
  on the steady-state path — `write_position` and `message_count` in
  `ChannelHeader` are advisory only. When a writer publishes them it stores
  `message_count` first and `write_position` second, both with
  `memory_order_release`; a reader that needs them as a consistent pair
  (`Live` join, §7) acquires `write_position` first. A file's
  `publication_state` (§3.1) is the other synchronization point: the writer
  sets it with release, and a reader acquires it before trusting
  `base_record_index`.

---

## 2. File and region structure

A channel is one or more **files**, each divided into fixed-size **regions**.
Region size is a multiple of the OS page size; it is declared in the
`ChannelHeader` and is identical across all files of the channel.

```
file 0                           file 1 (on roll)
+--------------------------+    +--------------------------+
| region 0                 |    | region 0                 |
|   MessageHeader(Channel) |    |   MessageHeader(Channel) |
|   ChannelHeader          |    |   ChannelHeader          |
|   ChannelHeaderExt       |    |   ChannelHeaderExt       |
|   user records...        |    |   user records...        |
+--------------------------+    +--------------------------+
| region 1                 |    | region 1                 |
|   user records...        |    |   ...                    |
+--------------------------+    +--------------------------+
| ...                      |
+--------------------------+
| region N-1               |
|   ... [Roll]             |
+--------------------------+
```

File names: the base file is `<base>`; rolled files are `<base>.1`,
`<base>.2`, ... A reader follows a `Roll` marker to the file it names
(§5.1). A file is built under a private name, `<base>[.<N>].<instance>.partial`
with its instance ID in 32 hex digits, and installed under its final name only
when complete. A file under its final name may still be unpublished (§3.1):
it is not part of the channel's history until it is published.

---

## 3. ChannelHeader (format_version = 4)

Located at byte offset `16` of file region 0 (immediately after the
`MessageHeader(Channel)` that opens region 0). Total size: 128 bytes.

| Offset | Size | Field                 | Type   | Description |
|-------:|-----:|-----------------------|--------|-------------|
|      0 |    8 | `write_position`      | u64    | Advisory: byte offset (from file start) of the next header slot to be written. Used only by `Live` reader join and writer reopen; not on the read steady-state path. |
|      8 |    8 | `message_count`       | u64    | Advisory: count of **user** records published in *this file*. Starts at `0`; incremented once per `commit`. Does **not** count the `Channel` header or `Skip` markers. |
|     16 |    8 | `base_record_index`   | u64    | Absolute index of this file's first user record, counted from channel genesis across all rolls. `0` for a genesis channel. Final once the file is published (§3.1), immutable after; read it only after acquiring `PUBLISHED`. `base_record_index + message_count` is the absolute index of the next user record (the channel head). |
|     24 |    8 | `channel_sequence`    | u64    | Rolling file ordinal: `0` for `<base>`, `1` for `<base>.1`, etc. On open, readers and writers verify this equals the sequence parsed from the file's path and refuse a mismatch (catches a renamed/misplaced/swapped segment). |
|     32 |    4 | `region_size`         | u32    | Region size in bytes. Multiple of OS page size. |
|     36 |    4 | `mtu`                 | u32    | Max user payload bytes; `0` = unlimited. |
|     40 |    2 | `format_version`      | u16    | This document describes version `4`. Versions `0`–`3` are earlier formats this build does not read (see §8). |
|     42 |    1 | `endianness`          | u8     | `0x01` = little-endian. Other values reserved. |
|     43 |    1 | `system_header_size`  | u8     | Size of the system-owned bytes inside `MessageHeader` (`8`). |
|     44 |    4 | `user_header_kind`    | u32    | Reserved discriminant identifying the layout of the user-metadata bytes. Current writers emit `0` (the default `{message_type:u16, user_meta_u64:u64}` layout described in §4) and current readers refuse anything else. Non-zero values are reserved for future user-defined layouts; a Rust opt-in API for those layouts is intentionally not exposed today. Placed at this 4-aligned offset so the surrounding byte fields need no padding. |
|     48 |    1 | `user_header_size`    | u8     | Size of the user-metadata bytes inside `MessageHeader` (`8`). |
|     49 |   48 | `channel_name`        | u8[48] | Optional channel name; unused bytes are zero. Widened from 20 in version 3. |
|     97 |    1 | `wake_flags`          | u8     | Bit 0: this segment's writer wakes readers (§6.3). Other bits reserved, zero. Set or cleared by every writer that creates or reopens the segment, to match its own setting. Zero (no waking) in files written before this field existed. |
|     98 |    2 | `_reserved_pad`       | u8[2]  | Zero-filled padding. |
|    100 |    4 | `wake_word`           | u32    | Wake counter (§6.3). Bumped by a writer whose segment has `wake_flags` bit 0 set, and by any writer finishing a stranded `Roll` in this segment (§6.2), whatever its own setting. Readers wait on it and never write it. Otherwise zero and untouched. On a different cache line from `write_position` and `message_count`. |
|    104 |   16 | `_reserved2`          | u8[16] | Reserved for future additive fields. Zero-filled; readers must ignore. Additive, optional, zero-default fields may consume this space **without** a `format_version` bump; any field that changes existing semantics must bump the version. |
|    120 |    8 | `generation`          | u64    | Opaque incarnation id for the channel, chosen at creation and stamped identically into every segment (immutable, carried across rolls, preserved when a writer reopens). `0` when unset. Distinguishes "this log continues" from "this path was deleted and recreated" — a recreated channel restarts at `channel_sequence = 0` and `base_record_index = 0`, so nothing else tells the two apart, and a persisted cursor would silently refer to unrelated data. A consumer that stores a read position should store this alongside it and treat a change as a different channel, not a gap. xchannel assigns no meaning to the value. Placed **last** so that additive fields consuming `_reserved2` from the front never move it. |

The `MessageHeader(Channel)` at offset `0` covers the bytes `[16, 208)`: the
`ChannelHeader` and the `ChannelHeaderExt` after it. Its `length` field is
`192` and its `committed` byte is `1`. The first user record therefore begins
at file offset `208`, and a fresh file's `write_position` is `224`.

### 3.1 ChannelHeaderExt

At file offset `144`, 64 bytes. All of it but `publication_state` is written
before the file is installed under its final name and never changed.

| File offset | Size | Field                     | Type   | Description |
|------------:|-----:|---------------------------|--------|-------------|
|         144 |   16 | `file_instance_id`        | u8[16] | Opaque, nonzero, random per physical file. A file rebuilt at the same sequence gets a new one. |
|         160 |   16 | `predecessor_instance_id` | u8[16] | The file this one follows; all zero for a channel's initial file. |
|         176 |    8 | `predecessor_sequence`    | u64    | That file's sequence; `u64::MAX` for the initial file. Zero ID and `u64::MAX` go together. |
|         184 |    8 | reserved                  | —      | Zero. |
|         192 |    1 | `publication_state`       | u8     | `0` = `PREPARED`: complete, safe to open and map, not history. `1` = `PUBLISHED`: published by the writer; `base_record_index` is final. Other values are invalid. Release store by the writer, acquire load by readers. |
|         193 |    3 | reserved                  | —      | Zero. |
|         196 |    4 | `identity_crc32c`         | u32    | CRC32C of file bytes `[144, 184)`. |
|         200 |    8 | reserved                  | —      | Zero. |

Readers refuse a file whose extension has a zero instance ID, nonzero reserved
bytes, a CRC mismatch, or only one of the two initial-file sentinels.

CRC32C throughout is the standard Castagnoli CRC: reflected polynomial
`0x82F63B78`, initial value and final XOR `0xFFFFFFFF`, stored little-endian.
`"123456789"` gives `0xE3069283`. It detects torn or malformed metadata; it is
not authentication.

---

## 4. MessageHeader

Every record in the file begins with a `MessageHeader`. Total size: 16 bytes
(`HEADER_SLOT`).

| Offset | Size | Field            | Type | Owner  | Description |
|-------:|-----:|------------------|------|--------|-------------|
|      0 |    1 | `committed`      | u8   | system | `0` = not yet committed; `1` = committed. Any other value indicates corruption. Synchronization point (see §1). |
|      1 |    1 | `header_type`    | u8   | system | Record kind. See §5. |
|      2 |    2 | `message_type`   | u16  | user   | Opaque to xchannel. Applications use this to discriminate payload types. |
|      4 |    4 | `length`         | u32  | system | Payload length in bytes (excludes the 16-byte header and any trailing alignment padding). |
|      8 |    8 | `user_meta_u64`  | u64  | user   | Opaque 8-byte slot. Applications may use this as a timestamp (`CLOCK_MONOTONIC` nanoseconds), a sequence number, packed flags, or any other 64-bit value. xchannel itself never reads this field. |

A record occupies `align_up(16 + length, 8)` bytes; trailing bytes are
padding and must not be interpreted.

The system-owned fields are `{committed, header_type, length}`. The
user-owned region is bytes `[2, 4)` and `[8, 16)`. This interleaving is
preserved by `user_header_kind = 0`; alternative layouts may redefine the
user-owned bytes but must preserve the system fields at their fixed offsets.

---

## 5. HeaderType discriminants

| Value | Name      | Meaning |
|------:|-----------|---------|
|     0 | `Channel` | First record in region 0. Followed by a `ChannelHeader` and a `ChannelHeaderExt`. Length = 192. |
|     1 | `User`    | User payload record. Length = payload bytes. |
|     2 | `Skip`    | Padding to the end of the current region. Length = bytes of padding (excluding the 16-byte header itself). Readers skip past `16 + length` bytes. |
|     3 | `Roll`    | Last record in this file. Length = 40 (§5.1). Readers open the file it names and continue from its offset 0. `user_meta_u64` is 0. |

Other values are invalid and must cause readers to fail.

### 5.1 Roll body

| Body offset | Size | Field                    | Type   |
|------------:|-----:|--------------------------|--------|
|           0 |    8 | `next_sequence`          | u64    |
|           8 |   16 | `next_file_instance_id`  | u8[16] |
|          24 |    8 | `next_base_record_index` | u64    |
|          32 |    4 | `body_crc32c`            | u32, CRC32C of body bytes `[0, 32)` |
|          36 |    4 | reserved                 | zero   |

A reader accepts the file after a committed `Roll` only if it is published,
its `channel_sequence` is `next_sequence` (the old file's plus one), its
`file_instance_id` is `next_file_instance_id`, its predecessor fields name the
old file, its `base_record_index` is `next_base_record_index`, and that equals
the old file's `base_record_index + message_count`. Generation, geometry and
format must match too. A `Roll` with a bad length, CRC or reserved bytes is
corruption. The successor is identified by these bytes, not by its path: a file
a reader opened ahead remains the successor even after retention unlinks the
name, and a file replaced at the same name is not.

---

## 6. Publish protocol (pre-header pipeline)

For each user record `i`:

1. Header slot `i` already exists in the file with `committed = 0`
   (pre-installed by the writer's `try_reserve(i-1)` call, or when the
   file/region was created for the very first slot).
2. Writer obtains a `length`-byte payload buffer immediately after header
   slot `i`, writes the payload.
3. Writer fills the user-owned fields of header `i` (`message_type`,
   `user_meta_u64`) and the `length`/`header_type` fields. `committed`
   remains `0`.
4. Writer computes the position of header slot `i+1` =
   `align_up(off(i) + 16 + length, 8)`.
5. Header slot `i+1` must be pre-installed with `committed = 0`,
   `header_type = User`, `length = 0`, `message_type = 0`,
   `user_meta_u64 = 0` — at the position computed in step 4 —
   before step 6 stores `committed = 1` on slot `i`. The reference
   implementation realises this as two sub-cases:

   - `length == reserved` (the common case): the pre-install at
     the matching offset was laid down by the earlier
     `try_reserve(reserved)` call. No additional write is needed
     here.
   - `length <  reserved` (worst-case-reserve / serialize-then-
     commit): the earlier `try_reserve` pre-install sits at the
     *reserved* offset, not the actual-length offset. The writer
     therefore re-lays the pre-install at
     `align_up(off(i) + 16 + length, 8)` before step 6.

   In either case, the invariant readers depend on is that slot
   `i+1`'s pre-install signature is durable on disk before step 6
   releases `committed = 1`, which is guaranteed by program order
   plus the release-store in step 6.
6. Writer publishes record `i` by storing `committed = 1` to header `i`
   with release semantics.
7. Writer increments `ChannelHeader.message_count`, then stores
   `ChannelHeader.write_position`, both with release semantics and in that
   order. Both are advisory, but the order is part of the contract: it is
   what lets a `Live` reader read the pair consistently (§7). Any other
   update of `write_position` (`Skip`, `Roll`) is also a release store.
   `message_count` counts **user** records only — see §6.1 for why `Skip`
   does not increment it.
8. If the segment's `wake_flags` bit 0 is set, the writer increments
   `wake_word` (release) and wakes everyone waiting on it (§6.3).

   A writer that reopens a file and finds the slot at `write_position - 16`
   already committed (it crashed between steps 6 and 7) steps over that
   record and must then **recount** `message_count` from the segment's
   records: the crash may have landed on either side of the increment, and
   the orphaned record is delivered to readers, so it must be counted. The
   next segment's `base_record_index` is derived from this count. The writer
   stores nothing until the recount has succeeded, and then stores the new
   `message_count` before the new `write_position`, the same order as a
   publish. A recovery interrupted partway therefore leaves the orphan at
   `write_position`, and the next open simply recovers again.

Readers observe `committed = 1` with acquire semantics and then read the
header fields and payload. The pre-installed header at slot `i+1`
guarantees that a reader scanning past record `i` will land on a
well-formed (though not necessarily committed) header.

### 6.1 Region boundary

Every record must leave **56 bytes** free behind it in its region: room for a
`Roll` (16-byte header, 40-byte body), so a roll can be staged at the next
header slot whenever it happens, including a manual one. If a record plus those
56 bytes does not fit in the remaining region, the writer publishes a `Skip`
record at the current
position covering the remaining bytes of the region, then begins record
`i` at offset 0 of the next region. A `Skip` is not a user record: the
writer advances `write_position` but does **not** increment
`message_count`, so `message_count` remains a count of user records only.

### 6.2 File boundary

If a record would exceed `file_roll_size`, the writer rolls: it publishes a
`Roll` (§5.1) at the current header slot of the old file, then begins record
`i` at offset 208 of file `<base>.<seq+1>`.

A file is prepared before it is published:

1. Build the successor under a private name: sized, `ChannelHeader` and
   `ChannelHeaderExt` written (a fresh instance ID, the old file as
   predecessor, `PREPARED`), first header slot pre-installed.
2. Install it under its final name, atomically and never replacing an
   existing file (`renameat2(RENAME_NOREPLACE)`, or link and unlink). If a
   file is already there, a writer may adopt it only if it is an empty
   `PREPARED` file naming the old file as predecessor, with the same geometry,
   generation, MTU and name; otherwise it fails. Steps 1–2 may happen long
   before the roll, on another thread.

The roll itself, in this order:

3. Stage the old file's `Roll` with `committed = 0`, its whole body written.
4. Stamp the successor's `base_record_index` and `wake_flags`, then store
   `publication_state = PUBLISHED` (release).
5. Store `committed = 1` on the `Roll` (release).
6. Store the old file's `write_position` as `roll + 16 + 56`: one slot past
   the whole `Roll`. That is not a record slot, and may lie past the end of
   the file.
7. If the writer wakes readers, increment the **old** file's `wake_word` and
   wake its waiters (§6.3).

A committed `Roll` therefore always names a published file. A `PREPARED` file
contributes nothing to history: no records, no tail, no seek range, no
retention count. Retention never removes a file the writer's own preparation
might still install.

**A writer that crashes mid-roll.** The next writer, before writing:

- **Before step 2:** removes the private file (a startup sweep of `.partial`
  names) and resumes in the old file.
- **After step 2, before step 4:** the newest final file is `PREPARED`. It
  unlinks it (never truncates it, so a reader's speculative mapping stays
  valid) and resumes in the old file; a staged `Roll` there is overwritten by
  the next record. The next roll prepares a new file with a new instance ID,
  so a stale mapping of the old one can never be mistaken for it.
- **After step 4:** the successor is published and is the newest file; the
  writer resumes in it. It walks the old file's record chain from the region
  `write_position` names, requires the record there to be a `Roll` whose body
  names the successor exactly (§5.1) and whose old file is that successor's
  predecessor, commits it if still staged, and moves `write_position` to the
  terminal hint (steps 5–7, waking unconditionally). A `Roll` naming anything
  else, or no `Roll`, is corruption and fails the open; it is never repaired by
  guessing. A complete roll is left untouched, so recovering twice changes
  nothing. A predecessor already removed by retention is not an error.

A channel's initial file is installed `PREPARED` and published at once (its
base is final at creation); the next writer publishes one left unpublished.

### 6.3 Waking readers (optional)

A writer may let readers sleep instead of polling. It sets `wake_flags`
bit 0 in every segment it creates or reopens, and after every commit (§6
step 8) and every `Roll` (§6.2 step 7) it increments that segment's
`wake_word` with release semantics and wakes all waiters. Readers only read
the word, so they keep read-only mappings.

On Linux the wait and the wake are a **shared** futex on `wake_word`:
`FUTEX_WAIT` / `FUTEX_WAKE` (or `futex_waitv` over several words), without
`FUTEX_PRIVATE_FLAG`. The kernel keys a shared futex on the file's inode and
offset, so a writer and readers that map the file at different addresses
meet on the same word.

A reader that finds no record and wants to wait:

1. Load `wake_word` with acquire semantics (`seen`).
2. Look for a record again. If there is one, stop.
3. Wait until `wake_word` is no longer `seen`, for at most a bounded time
   (the reference implementation uses 10 ms).

A commit after step 2 changes the word after step 1, so the wait in step 3
either returns at once or is woken. The bound matters: the flag can be stale
(a writer that predates this field reopened the segment and does not wake),
and a stale flag must cost slow polling, never a hang. A reader that sees a
full bounded wait run out with a record waiting, and `wake_word` still at
`seen`, should stop trusting the flag for that segment once this has happened
twice with `wake_word` unmoved in between. Once is not enough: a waking writer
preempted between publishing a record (§6 step 7) and bumping the word
(step 8) looks the same, but its bump then moves the word.

A crashed writer's stranded `Roll` (§6.2) is woken by the writer that
finishes it, unconditionally.

---

## 7. Reader algorithms (informative)

Only published files are history. Every algorithm below skips a newest file
that is still `PREPARED`, and acquires `PUBLISHED` before reading a file's
`base_record_index`.

**LateJoin:** open the earliest-sequence file, start scanning at offset 0,
follow `Skip`/`Roll`/`Channel` records transparently, deliver `User`
records to the application.

**Live:** open the latest published file, read `ChannelHeader.write_position`
once, start scanning from the header slot at `write_position - 16`, follow
records as above. Subsequent reads do not need `write_position`.

A Live reader that also wants the absolute index of the record it starts at
reads the pair as follows:

1. Acquire `write_position` (`w`), then acquire `message_count` (`c`).
2. **Check that this segment is still the tail.** If `<base>.<seq+1>`
   exists **and is published**, or this segment's own path no longer does, a
   roll has happened. (Retention unlinks oldest first, so a pruned successor
   means this segment was pruned before it.) A successor that is only
   installed, still `PREPARED`, does not count. After a roll, `w - 16` is not
   a record slot (§6.2 step 6): it may hold leftovers of an earlier payload, or
   lie past the end of the file. Do not read it; open the newest published
   segment instead and start over. The check must come after the acquire of
   `w`: the successor was published before the `Roll` was committed and `w`
   moved past it, so the check cannot miss it.
3. Acquire `committed` of the slot at `w - 16`.
   - **Uncommitted:** start there, at index `base_record_index + c` exactly.
     The acquire of `w` guarantees `c` is at least as new as `w`, and had
     `c` already counted the record at `w - 16`, its commit would be visible.
   - **Committed `Roll`:** the publication and the commit both landed after
     the check in step 2. This is terminal, not transient. Start on the `Roll`
     with index `base_record_index + c`; `c` is final, since no user record
     follows a `Roll`. Do not retry: the writer is about to move
     `write_position` past it.
   - **Committed `User` or `Skip`:** the writer is between steps 6 and 7, or
     between a `Skip` and its `write_position` update. Retry from step 1. A
     writer that died in that window leaves the slot committed for good, so
     after a bounded wait the reader counts the user records before `w - 16`
     by walking the segment instead.

**Start at index `i`:** list the segments, leaving out a newest one that is
still `PREPARED`; the earliest one's `base_record_index` is the oldest index
still retained, and the latest one's `base_record_index + message_count` is
the head. Indices outside
`[oldest, head]` are refused. Otherwise binary-search the segments for the
last one whose `base_record_index <= i`, then scan it from offset 0 stepping
over records by `length` (headers only), counting `User` records, and start
at the first `User` record with exactly `i - base_record_index` user records
before it in the segment. (When `i == base_record_index`, that is the
segment's first user record.) If the scan reaches an uncommitted slot or the
`Roll` with that many behind it, start there: `i` is the head, or the first
record of the next segment. There is no per-record index in the format, so
the in-segment step is linear.

A reader that observes `committed = 0` on a header slot must not advance;
it must retry (busy/backoff is implementation-defined) until `committed`
transitions to `1`.

---

## 8. Versioning and forward compatibility

xchannel 7.0 introduces `format_version = 4`, which separates a file's
preparation from its publication: the `ChannelHeaderExt` (§3.1) after the
`ChannelHeader`, so the Channel record is 192 bytes and the first user record
moves from offset 144 to 208; a 40-byte `Roll` body naming the successor
(§5.1); and every record leaving 56 bytes for a `Roll`. A v4 file under its
final name may be unpublished, which a v3 reader would take for history, so v4
is **greenfield**: no in-place migration, and no mixing of v3 and v4 on one
channel. Upgrade a channel's writer and readers together, on a fresh path.

xchannel 5.0 introduced `format_version = 3`, which widens `channel_name`
from 20 to 48 bytes, taking the space from `_reserved2`. The header stays 128
bytes and every other field — including `generation`, pinned at offset 120 —
keeps its offset, so a v2 file is structurally readable. It is still a version
bump rather than an additive change, because a v3 writer can store a name that
a v2 reader would silently truncate at 20 bytes: the change redefines the
meaning of bytes `[69, 97)` instead of adding to unused space. Like v2 before
it, v3 is **greenfield** — there is no in-place migration.

xchannel 4.0 introduced `format_version = 2`, which widened `ChannelHeader`
to 128 bytes (adding `base_record_index` and reserved space) and redefined
`message_count` as a per-file user-record count. Because the header grew,
the records area shifted (first user record at offset 144 instead of 80).

Files at `format_version` `0` to `3` are not read by this build — keep using
an older crate version (6.x reads v3) to read them.

- A reader that sees `format_version != 4` must refuse the file.
- A reader that sees `endianness != 0x01` must refuse the file. (Only
  little-endian is defined today; values are reserved for future use.)
- A reader that sees `user_header_kind != 0` must refuse the file unless
  it specifically implements that alternative layout. The Rust crate's
  current readers refuse all non-zero values; an opt-in API may be added
  in a future version when a concrete alternative layout is defined.

---

## 9. Invariants (contract)

The following invariants are load-bearing for the algorithm. Any
alternative `user_header_kind` layout must preserve them:

1. `MessageHeader` is exactly 16 bytes and 8-byte aligned.
2. `committed` is at byte offset 0 and is the only field accessed
   concurrently by writer and readers.
3. `header_type` is at byte offset 1.
4. `length` is at byte offset 4, encodes the payload length (not
   including the 16-byte header), and is at most `region_size − 16` for
   any single record. (For `Skip`, it is at most the remaining region
   bytes minus 16.)
5. Each record occupies `align_up(16 + length, 8)` bytes.
6. The writer pre-installs the next header slot before committing the
   current one; readers may rely on the next slot being well-formed
   (even if `committed = 0`).
7. Every header slot has at least 56 bytes before the end of its region, so
   a `Roll` fits at any of them.
8. A file is published before any committed `Roll` leads to it; its
   identity fields never change after installation, nor its
   `base_record_index` after publication.
