# xchannel

**xchannel** is a tiny, zero-copy, mmap-backed IPC channel with automatic region and file rolling. It lets a single writer append messages to a persistent stream that multiple readers can replay from the beginning (**LateJoin**) or tail in real time (**Live**)—without a broker or background service.

## Features
* Shared‑memory / IPC logs without a broker.
* Constant-time tailing (readers only track a byte offset).
* Works on Linux and macOS (16 KiB) and typical 4 KiB page systems.
* **Zero‑copy access**: messages are written directly into a memory‑mapped
  region and read back without additional copying.
* **Rolling regions and files**: large channels are segmented into
  fixed‑size regions. When a region fills up the writer rolls over to
  the next region; when the end of a file is reached a new file with
  an incremented sequence number is created automatically.
* **Two reader modes**:
    * **LateJoin** – start from the beginning of the earliest existing
      channel file.
    * **Live** – join the channel at the current write position and only
      observe new messages.
* **MTU enforcement**: optional maximum message size to defend against
  unbounded memory usage or corrupted input.
* **Atomic state management**: the shared write position and message
  count are tracked using atomic variables with proper memory ordering
* **Very simple, low maintenance**: the system relies on a minimal set of concepts. 
 There are no background services, no complex synchronization mechanisms, and no external dependencies.
* **No back pressure**: readers cannot slow down the writer, retention is controlled by rolling policy rather than consumer speed.
* **Clear non-aliasing contract (single writer)**: readers never observe bytes 
  while they’re being written. This is a language-agnostic safety property (C/C++/Rust/..), 
  and fits Rust’s `&mut`/`&` guarantees naturally.

## Minimum example

```rust
use xchannel::{WriterBuilder, ReaderBuilder};

let region = xchannel::page_size();           // ensure page-aligned regions
let mut w = WriterBuilder::new("demo.xch")
    .region_size(region)
    .file_roll_size(10_000_000)
    .build()?;
            
// write a message
let payload = b"hello world";
let buf = w.try_reserve(payload.len())?;
buf.copy_from_slice(payload);
w.commit(1, payload.len() as u32, timestamp)?;

// read it back
let mut r = ReaderBuilder::new("demo.xch")
    .late_join()
    .batch_limit(1000)
    .build()?;
if let Some(msg) = r.try_read()? {
    let hdr = msg.header();
    println!("type={}, len={}", hdr.message_type, hdr.length);
    println!("payload={:?}", msg.payload());
}
```

## Batch read example

```rust
use xchannel::ReaderBuilder;

let mut r = ReaderBuilder::new("demo.xch").late_join().build()?;
if let Some(batch) = r.try_read_batch(None)? {
    for idx in (0..batch.len()).rev() {
        let msg = batch.get(idx).unwrap();
        let hdr = msg.header();
        let payload = msg.payload();
        // payload is opaque bytes; parse as needed.
        println!("type={}, len={}, first={:?}", hdr.message_type, hdr.length, payload.get(0));
    }
}
```

---


----------

<!--
Goal: explain design ideas + show Rust examples
-->

# xchannel

## mmap-backed IPC channels for Rust

Zero-copy • Append-only • Multi-reader

---

# Why another channel?

Typical Rust channels:

* `std::sync::mpsc`
* `tokio::mpsc`
* `crossbeam`

These are great but they:

* work **inside one process**
* messages exist **only in memory**
* cannot **replay history**
* cannot easily **tail from another process**

---

# Motivation

Some systems need:

* **cross-process communication**
* **persistent message streams**
* **very low overhead**
* **simple debugging**

Typical examples:

* trading systems
* logging pipelines
* market data distribution
* real-time analytics

---

# Core idea

Instead of a queue:

**Use an append-only log stored in a memory-mapped file**

```
Writer ---> mmap file ---> Readers
```

Properties:

* writer **appends messages**
* readers **scan sequentially**
* messages remain **persistent**

Readers can start:

* from **beginning**
* from **current tail**

---

# Architecture overview

```
                                                  Writer
                                                  │
                                                  │ append messages
                                                  ▼
        ┌─────────────────────────────────────────────┐
        │ mmap file                                   │
        │                                             │
        │ msg1 msg2 msg3 msg4 msg5 msg6 msg7 msg8 ... │
        │                                             │
        └─────────────────────────────────────────────┘
            ▲                                ▲
            │                                │
        Reader A                         Reader B
        (LateJoin)                        (Live)
```

Key property:

Readers **never block the writer**.

---

# File structure

Channel files are divided into **regions**.

```
File
┌─────────────────────────────┐
│ Region 0                    │
│ ChannelHeader + messages    │
├─────────────────────────────┤
│ Region 1                    │
│ messages                    │
├─────────────────────────────┤
│ Region 2                    │
│ messages                    │
└─────────────────────────────┘
```

Regions provide:

* predictable memory layout
* simple boundary handling
* easier file rolling

---

# Record layout

Each record looks like:

```
[ MessageHeader ][ payload ][ padding ]
```

Header fields include:

* committed flag (`u8`: `0` = not committed, `1` = committed)
* message type
* payload length
* timestamp

Readers check:

```
header.is_committed()?
```

---

# Record memory layout

```
┌──────────────────────────────┐
│ MessageHeader                │
│                              │
│ committed                    │
│ message_type                 │
│ payload_length               │
│ timestamp_ns                 │
└──────────────────────────────┘
              │
              ▼
┌──────────────────────────────┐
│ Payload bytes                │
│ user message                 │
└──────────────────────────────┘
              │
              ▼
┌──────────────────────────────┐
│ Padding (optional)           │
└──────────────────────────────┘
```

---

# Writer workflow

```
reserve → write payload → publish
```

Steps:

1. reserve memory
2. write payload
3. prepare next header
4. commit current message

The **commit flag is written last**.

---

# Publish protocol

Writer publishes a message in this order:

```
1. write payload(i)
2. prepare header(i+1)      // write-ahead header
3. commit header(i) = true  (Release)
```

Meaning:

* the **next header slot exists before publication**
* the message becomes visible **only after commit**

---

# Why prepare the next header first?

When a reader sees:

```
header(i).is_committed()? == true
```

then:

* payload(i) is fully written
* header(i+1) already exists

So the reader can continue scanning safely:

```
header(i) → payload(i) → header(i+1)
```

No global metadata required.

---

# Writer pipeline visualization

```
header(i) ready
      │
      ▼
write payload(i)
      │
      ▼
prepare header(i+1)
      │
      ▼
commit header(i)
```

Key property:

The **commit flag is the only synchronization point**.

---

# Cache contention problem

Naive design:

```
writer updates global head pointer
readers poll the same pointer
```

Result:

```
CPU1 (writer)  <---->  CPU2 (reader)
      cache invalidations
```

This causes unnecessary **cache coherence traffic**.

---

# Commit flag solution

Each message has its **own commit flag**.

Readers check different cache lines as they scan.

```
msg1.header.is_committed()?
msg2.header.is_committed()?
msg3.header.is_committed()?
```

Benefits:

* minimal contention
* scalable readers
* avoids cache bouncing

---

# Rust example: writer

```rust
use xchannel::WriterBuilder;

fn main() -> std::io::Result<()> {
    let mut writer = WriterBuilder::new("demo.xch")
        .build()?;
    let payload = b"hello xchannel";
    let buf = writer.try_reserve(payload.len())?;
    buf.copy_from_slice(payload);
    writer.commit(1, payload.len() as u32, 0 )?;
    Ok(())
}
```

---

# Rust example: reader

```rust
use xchannel::ReaderBuilder;

fn main() -> std::io::Result<()> {

    let mut reader = ReaderBuilder::new("demo.xch")
        .late_join()
        .build()?;

    while let Some(msg) = reader.try_read()? {

        let header = msg.header();
        let payload = msg.payload();

        println!(
            "type={} len={} payload={:?}",
            header.message_type,
            header.length,
            payload
        );
    }

    Ok(())
}
```

---

# Rust aliasing requirement

Rust enforces strict aliasing rules:

```
&mut T  → exclusive access
&T      → shared access
```

Simultaneous read/write would violate:

```
&mut [u8] vs &[u8]
```

This is especially important with **mmap memory**.

---

# The aliasing challenge

Writer and readers operate on the **same mapped memory**.

Naively this could allow:

```
writer writing payload
reader reading same payload
```

This would break Rust’s **non-aliasing contract**.

---

# xchannel solution

The algorithm guarantees:

**Writer and readers never access the same memory region simultaneously**

Except one field:

```
AtomicU8 committed
```

---

# Access separation

Writer accesses:

```
payload(i)
header(i)
```

Readers access only:

```
payload(j)
header(j)
```

Where

```
j < committed_index
```

Meaning the message is already **published**.

---

# Publish ordering

Writer:

```
write payload
prepare next header
commit = 1 (Release)
```

Reader:

```
if committed.load(Acquire) == 1 {
    read payload
}
```

with `0 = not committed`, `1 = committed`, and any other value treated as invalid data.

Guarantees:

* no partial reads
* correct memory ordering

---

# Why this satisfies Rust aliasing rules

After commit:

```
writer never touches payload again
```

Then readers access:

```
&[u8]
```

Timeline:

```
writer (&mut) → finished
reader (&) → begins
```

No overlapping access.

Rust’s **non-aliasing guarantee is preserved**.

---

# The only shared memory location

Both sides access only:

```
AtomicU8 committed
```

Safe because:

* atomic operations
* Acquire / Release ordering
* tiny memory footprint

---

# Reader modes

### LateJoin

```
start from beginning
```

Useful for:

* replay
* debugging
* analytics

---

### Live

```
start from tail
```

Useful for:

* real-time consumers
* monitoring
* streaming pipelines

---

# Rolling files

Channels can run indefinitely.

Files roll when necessary:

```
demo.xch
demo.xch.1
demo.xch.2
```

Process:

1. writer writes **Roll marker**
2. creates next file
3. readers follow automatically

---

# Why mmap?

Benefits:

* zero-copy payload access
* OS page cache handles IO
* sequential reads are extremely fast
* minimal syscalls

Readers simply scan memory:

```
header → header → header
```

---

# Why this design fits Rust well

The design aligns with Rust principles:

Ownership transfer:

```
writer owns payload → commit → reader observes
```

Concurrency:

```
atomic publication
```

Memory layout:

```
simple + predictable
```

---

# Key design principles

xchannel relies on:

1. append-only log
2. commit-flag publication
3. write-ahead headers
4. sequential scanning
5. strict memory ownership transfer

Result:

```
safe + zero-copy + low latency + scalable
```

---

# When to use xchannel

Good fit:

* market data distribution
* logging pipelines
* inter-process messaging
* persistent event streams
* historical replay (simulation)


---

# Summary

xchannel provides:

* mmap-based IPC channels
* zero-copy message access
* append-only persistent log
* minimal contention design
* Rust-safe memory access model

---
