# xchannel on-disk format

This document specifies the byte-level format of an xchannel file so that
non-Rust implementations (readers, validators, archival tools) can interoperate
with files produced by the Rust crate. The Rust source in `src/channel.rs`
and `src/lib.rs` is the executable reference; this document is the contract.

Status: **draft, format_version = 1.**

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
  `ChannelHeader` are advisory only.

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
`<base>.2`, ... A reader follows a `Roll` marker by opening the next
sequence number.

---

## 3. ChannelHeader (format_version = 1)

Located at byte offset `16` of file region 0 (immediately after the
`MessageHeader(Channel)` that opens region 0). Total size: 64 bytes.

| Offset | Size | Field                 | Type   | Description |
|-------:|-----:|-----------------------|--------|-------------|
|      0 |    8 | `write_position`      | u64    | Advisory: byte offset (from file start) of the next header slot to be written. Used only by `Live` reader join and writer reopen; not on the read steady-state path. |
|      8 |    8 | `message_count`       | u64    | Advisory: monotonic count of records published in this file. |
|     16 |    8 | `channel_sequence`    | u64    | `0` for `<base>`, `1` for `<base>.1`, etc. |
|     24 |    4 | `region_size`         | u32    | Region size in bytes. Multiple of OS page size. |
|     28 |    4 | `mtu`                 | u32    | Max user payload bytes; `0` = unlimited. |
|     32 |    2 | `format_version`      | u16    | This document describes version `1`. Version `0` denotes a pre-spec file (see §8). |
|     34 |    1 | `endianness`          | u8     | `0x01` = little-endian. Other values reserved. |
|     35 |    1 | `system_header_size`  | u8     | Size of the system-owned bytes inside `MessageHeader` (`8` for version 1). |
|     36 |    1 | `user_header_size`    | u8     | Size of the user-metadata bytes inside `MessageHeader` (`8` for version 1). |
|     37 |    3 | `_reserved`           | u8[3]  | Must be zero. |
|     40 |    4 | `user_header_kind`    | u32    | Reserved discriminant identifying the layout of the user-metadata bytes. Current writers emit `0` (the default `{message_type:u16, user_meta_u64:u64}` layout described in §4) and current readers refuse anything else. Non-zero values are reserved for future user-defined layouts; a Rust opt-in API for those layouts is intentionally not exposed today. |
|     44 |   20 | `channel_name`        | u8[20] | Optional channel name; unused bytes are zero. |

The `MessageHeader(Channel)` at offset `0` covers the bytes `[16, 80)`; its
`length` field is `64` (size of `ChannelHeader`). Its `committed` byte is
`1`.

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
|     0 | `Channel` | First record in region 0. Followed by a `ChannelHeader`. Length = 64. |
|     1 | `User`    | User payload record. Length = payload bytes. |
|     2 | `Skip`    | Padding to the end of the current region. Length = bytes of padding (excluding the 16-byte header itself). Readers skip past `16 + length` bytes. |
|     3 | `Roll`    | Last record in this file. Length = 0. Readers open the next file (`<base>.<n+1>`) and continue from offset 0. |

Other values are invalid and must cause readers to fail.

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
5. (Already done inside `try_reserve(i)` before step 2: header slot
   `i+1` is pre-installed with `committed = 0` and
   `header_type = User`. The invariant readers rely on is that this
   pre-install is durable on disk before step 6 stores `committed = 1`
   on slot `i`, which is guaranteed by the program order plus the
   release-store in step 6.)
6. Writer publishes record `i` by storing `committed = 1` to header `i`
   with release semantics.
7. Writer updates `ChannelHeader.write_position` and
   `ChannelHeader.message_count` (relaxed; advisory).

Readers observe `committed = 1` with acquire semantics and then read the
header fields and payload. The pre-installed header at slot `i+1`
guarantees that a reader scanning past record `i` will land on a
well-formed (though not necessarily committed) header.

### 6.1 Region boundary

If a record plus a pre-installed next-header slot does not fit in the
remaining region, the writer publishes a `Skip` record at the current
position covering the remaining bytes of the region, then begins record
`i` at offset 0 of the next region.

### 6.2 File boundary

If a record would exceed `file_roll_size`, the writer publishes a `Roll`
record (length 0) at the current position in the old file, then begins
record `i` at offset 0 of region 0 of file `<base>.<seq+1>`, after that
file's `Channel` record and `ChannelHeader`.

---

## 7. Reader algorithms (informative)

**LateJoin:** open the earliest-sequence file, start scanning at offset 0,
follow `Skip`/`Roll`/`Channel` records transparently, deliver `User`
records to the application.

**Live:** open the latest-sequence file, read `ChannelHeader.write_position`
once, start scanning from the header slot at `write_position - 16`, follow
records as above. Subsequent reads do not need `write_position`.

A reader that observes `committed = 0` on a header slot must not advance;
it must retry (busy/backoff is implementation-defined) until `committed`
transitions to `1`.

---

## 8. Versioning and forward compatibility

xchannel 3.0 introduces this format with `format_version = 1`. Files
produced by xchannel ≤ 2.2 are not supported; open them with the older
crate version or regenerate.

- A reader that sees `format_version != 1` must refuse the file.
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
