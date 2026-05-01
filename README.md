# xchannel

**xchannel** is a **single-writer**, **multi-reader** **broadcast queue** built on memory-mapped storage, designed for **ultra-low-latency** IPC where a producer appends variable-size, typed and timestamped messages and multiple independent readers can tail or replay the stream using their own cursors without copying or coordination. It prioritizes simplicity and predictability: there is no ownership transfer and no backpressure by design, ensuring complete producer isolation and deterministic latency while enabling true fan-out semantics where readers never interfere with each other. Its minimal design makes it easy to reason about, extend (e.g., via a lightweight network bridge), and reimplement in other languages. **Persistence** is not the primary goal but emerges naturally from the append-and-retain model, providing replay, **late joiners**, and recovery with minimal added complexity, making it a compact, composable primitive for high-performance data-plane systems.

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
use xchannel::{HeaderType, ReaderBuilder};

let mut r = ReaderBuilder::new("demo.xch").late_join().build()?;
if let Some(batch) = r.try_read_batch(None)? {
    for idx in (0..batch.len()).rev() {
        let msg = batch.get(idx).unwrap();
        let hdr = msg.header();
        let kind = hdr.parsed_header_type()?;
        let payload = msg.payload();
        // payload is opaque bytes; parse as needed.
        if kind == HeaderType::User {
            println!(
                "type={}, len={}, first={:?}",
                hdr.message_type,
                hdr.length,
                payload.get(0)
            );
        }
    }
}
```

## Benchmarks

End-to-end **reader-side latency** — wall-clock time from the writer
stamping a payload with `CLOCK_MONOTONIC` to the reader observing it.
Writer and reader are separate processes, each pinned to a dedicated CPU
core. Measured on two hosts representing different deployment realities:
a non-isolated developer laptop and a fully latency-tuned box.

### TL;DR

At a realistic 10 K msg/s publish cadence (`100 µs` gap), end-to-end
latency on the latency-tuned host (`lse`):

| msg size | p50 | p99 | p99.9 |
|---:|---:|---:|---:|
| **64 B**  | 90 ns  | **344 ns** | 448 ns |
| **256 B** | 117 ns | 456 ns     | 655 µs |
| **4 KiB** | 723 ns | 647 µs ¹   | 879 µs |

¹ 4 KiB shows a host-specific tail on `lse` we have not fully diagnosed
— see [Open question](#open-question-4-kib-tail-on-lse). The same
workload on `montblanc/disk/4 KiB` runs at p99 = 17 µs.

Saturation runs (writer pushing as fast as possible) show much higher p99
/ p99.9 — that is **queue-depth tail**, not the channel's intrinsic
latency. See [How to read these numbers](#how-to-read-these-numbers).

### What we measured

Each cell runs:

- Writer and reader as separate processes, each pinned to its own core
  via `sched_setaffinity`. Cores 3 and 4 by default.
- 3 s warmup + 30 s measurement window per cell.
- File rolling on (`--region-size 16m --roll-size 1g`), retention
  `--keep-files 2` so the working set stays bounded.
- Reader does an **XOR-fold over the whole payload** before recording
  the timestamp — this prevents dead-store elimination and is a
  realistic proxy for downstream processing.

Latency is recorded into HdrHistogram (3 sig figs, 1 ns – 60 s) on the
reader side, post-warmup. p50 / p99 / p99.9 in the tables below.

### Publish-rate matrix

To separate **intrinsic per-message latency** from **queue-pressure
latency**, each (size × backend) is measured at three publish cadences.
The writer busy-waits between publishes (`--gap-ns N`):

| label | gap | writer rate | regime |
|---|---|---|---|
| `sat`    | 0      | unthrottled (~0.5–9 M msg/s) | absolute throughput / queue-pressure tail |
| `1 µs`   | 1 µs   | up to 1 M msg/s | high-rate steady load |
| `100 µs` | 100 µs | up to 10 K msg/s | realistic application cadence |

Rates are **ceilings**: when the natural per-message cost exceeds the
gap (e.g. 4 KiB at 1 µs gap), the writer effectively runs at its
saturation rate and the column should be read as "saturation".

### Hosts

- **`montblanc`** — laptop, no isolation. i9-11900H, Ubuntu 25.10,
  kernel 6.17. A normal developer machine; expect noisier tails.
- **`lse`** — latency-tuned production-style box. Xeon Gold 6146 @ 3.2 GHz,
  RHEL 9.6, kernel 5.14. Kernel cmdline includes
  `isolcpus=1-11,13-23 nohz_full=1-11,13-23 irqaffinity=0,12 intel_idle.max_cstate=0 idle=poll`.
  Cores 3 and 4 (used by the bench) are both isolated and on NUMA node 0.

### Results: `montblanc` — i9-11900H, no isolation

#### `tmpfs` (`/dev/shm`)

**Publish gap: `sat`**

| msg size | rate | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 64 B  | 9.40 M/s |  86 ns | 952 µs | 1.77 ms |
| 256 B | 5.13 M/s | 160 ns | 700 µs | 1.11 ms |
| 4 KiB |  479 K/s | 828 ns | 816 µs | 1.51 ms |

**Publish gap: `1 µs`**

| msg size | rate | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 64 B  | 1.00 M/s |  52 ns | 406 µs | 794 µs |
| 256 B | 1.00 M/s |  57 ns | 486 µs | 814 µs |
| 4 KiB |  474 K/s | 773 ns | 782 µs | 1.14 ms |

**Publish gap: `100 µs`**

| msg size | rate | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 64 B  | 10.00 K/s |  54 ns | 2.85 µs |   8.13 µs |
| 256 B | 10.00 K/s |  55 ns | 2.19 µs | 251.78 µs |
| 4 KiB | 10.00 K/s | 446 ns | 469 µs |    870 µs |

#### `disk` (ext4)

**Publish gap: `sat`**

| msg size | rate | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 64 B  | 8.60 M/s |  75 ns | 36 µs | 233 µs |
| 256 B | 4.52 M/s | 131 ns | 74 µs | 310 µs |
| 4 KiB |  384 K/s | 1.5 µs | 74 µs | 176 µs |

**Publish gap: `1 µs`**

| msg size | rate | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 64 B  | 1.00 M/s |  51 ns | 5.9 µs |  93 µs |
| 256 B | 1.00 M/s |  57 ns |  35 µs | 155 µs |
| 4 KiB |  386 K/s | 1.5 µs |  78 µs | 181 µs |

**Publish gap: `100 µs`**

| msg size | rate | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 64 B  | 10.00 K/s |  60 ns | 3.1 µs |  13 µs |
| 256 B | 10.00 K/s |  55 ns | 4.6 µs |  19 µs |
| 4 KiB | 10.00 K/s | 1.5 µs |  17 µs |  91 µs |

### Results: `lse` — Xeon Gold 6146, full isolation

#### `tmpfs` (`/dev/shm`)

**Publish gap: `sat`**

| msg size | rate | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 64 B  | 5.82 M/s | 108 ns | 614 µs | 735 µs |
| 256 B | 3.54 M/s | 269 ns | 617 µs | 704 µs |
| 4 KiB |  317 K/s | 899 ns | 671 µs | 934 µs |

**Publish gap: `1 µs`**

| msg size | rate | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 64 B  | 1.00 M/s |  74 ns | 561 µs | 692 µs |
| 256 B | 1.00 M/s | 114 ns | 603 µs | 693 µs |
| 4 KiB |  314 K/s | 896 ns | 670 µs | 934 µs |

**Publish gap: `100 µs`**

| msg size | rate | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 64 B  | 10.00 K/s |  90 ns | 344 ns | 448 ns |
| 256 B | 10.00 K/s | 117 ns | 456 ns | 655 µs |
| 4 KiB | 10.00 K/s | 723 ns | 647 µs | 879 µs |

#### `disk` (xfs)

**Publish gap: `sat`**

| msg size | rate | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 64 B  | 5.21 M/s |  91 ns | 424 µs | 559 µs |
| 256 B | 2.73 M/s | 213 ns | 446 µs | 568 µs |
| 4 KiB |  232 K/s | 2.8 µs | 474 µs | 594 µs |

**Publish gap: `1 µs`**

| msg size | rate | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 64 B  | 1.00 M/s |  77 ns | 386 µs | 549 µs |
| 256 B | 1.00 M/s | 127 ns | 439 µs | 571 µs |
| 4 KiB |  232 K/s | 2.8 µs | 472 µs | 605 µs |

**Publish gap: `100 µs`**

| msg size | rate | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 64 B  | 10.00 K/s |  91 ns | 2.9 µs |  3.4 µs |
| 256 B | 10.00 K/s | 164 ns | 3.2 µs |  8.5 µs |
| 4 KiB | 10.00 K/s | 2.8 µs | 454 µs |    631 µs |

Full per-host detail (p90, p95, max, samples, complete system info) is in:

- [`bench/results-montblanc.md`](bench/results-montblanc.md)
- [`bench/results-lse.md`](bench/results-lse.md)

### How to read these numbers

#### p50 — what one message normally costs

p50 is the median end-to-end cost of one message: writer reserves a slot,
copies payload, commits; reader observes commit, copies a timestamp out,
folds the payload. It is the closest single number to "intrinsic
xchannel latency."

For all configurations and all loads, p50 is sub-microsecond up to 1 KiB
and a few microseconds at 4 KiB. Between hosts, lse's higher p50 (90 ns
vs 60 ns for 64 B) reflects its 3.2 GHz Xeon vs montblanc's 4–5 GHz
boost — slower clock, similar count of instructions per message.

#### p99 / p99.9 — what the worst 1 % / 0.1 % look like

This is where the choice of load matters a lot:

**Under saturation (`sat` column),** the writer outpaces the reader
slightly, the in-channel queue grows during whatever burst of bad
scheduling luck happens in 30 s, and the worst few percent of samples
read back the full queue depth. p99 / p99.9 in this regime measure
**how badly the OS interrupted the reader during the worst burst** — not
the channel's intrinsic latency. The clearest example is
`montblanc/disk/64 B`:

| gap | p99 | p99.9 |
|---|---|---|
| `sat`    | 36 µs | 233 µs |
| `1 µs`   | 5.9 µs | 93 µs |
| `100 µs` | 3.1 µs | 13 µs |

Same code, same hardware — only the publish rate changed. xchannel did
not get faster; the queue stopped forming.

**Under realistic load (`100 µs` column),** every cell has the reader at
≥ 50 × headroom. Now p99 reflects what's left after queue pressure goes
away: a small handful of structural events (region transitions, page
faults on freshly grown pages, residual kernel work the isolation
doesn't fully suppress). Most cells settle into the few-µs range.

The most striking single result is `lse/tmpfs/64 B` at `100 µs` gap:
**p99 = 344 ns, p99.9 = 448 ns.** No queue, isolated cores, no scheduling
noise — just the channel's per-message work, measured cleanly.

#### When tail does *not* collapse with publish gap

There are two cases where reducing load doesn't help the tail:

1. **The writer's natural rate is already below the gap ceiling.** At
   1 µs gap on `lse/tmpfs/4 KiB` the writer hits 314 K msg/s — that
   *is* its saturation rate; the gap is a no-op. The 1 µs and `sat`
   numbers should look identical for 4 KiB, and they do. Don't read
   that as a structural floor; the writer just isn't being throttled.
2. **There's a genuine residual structural event.** All `lse` 4 KiB
   cells (both tmpfs and disk, every gap) show p99 between 450 and
   670 µs while `montblanc/disk/4 KiB` at 100 µs gap is at p99 = 17 µs.
   That is a real lse-specific effect — see the next subsection.

#### Saturation is not back-pressure

xchannel has **no back-pressure** by design — a slow reader cannot slow
the writer; the writer keeps producing into rolled files and old data
gets pruned (see `keep_files`). So the saturation numbers are a stress
test of the OS, not an apples-to-apples benchmark for an application
publishing well below the channel's ceiling. **Most users should look
at the `100 µs` column.**

#### A note on `disk`

The `disk` rows are not measuring a round-trip to storage — both writer
and reader operate on `mmap`'d pages that stay hot in the page cache. In
fact, on both hosts the `disk` p99 is *better* than `tmpfs` p99 at
saturation, because writeback pressure briefly slows the writer and
naturally smooths queue depth. Disk-backed channels do pay at very
large message sizes (visible as ~100 ms `max` values in the per-host
files when writeback flushes a large chunk), but at the percentiles in
the headline tables they're indistinguishable from tmpfs in shape.

### Open question: 4 KiB tail on `lse`

Every `lse` cell — both `tmpfs` and `disk`, every publish gap including
the lightly-loaded 100 µs case — shows p99 between 450 and 670 µs for
4 KiB messages. The same workload on `montblanc/disk/4 KiB` at 10 K msg/s
is at p99 = 17 µs, so xchannel itself is fine. The lse 4 KiB tail does
not collapse with reduced load, which rules out queue pressure.

Plausible suspects we have not isolated yet: the page-fault path on the
older RHEL 9.6 / kernel 5.14, NUMA / memory-subsystem behaviour on the
Xeon Gold 6146, a `idle=poll` interaction at fresh-page allocation, or
something specific to how `xfs` (and tmpfs on the same kernel) handle
4 KiB-aligned writes. We will update this section when we have data
from a third box and / or a region-size sweep that isolates the cause.

### Reproducing

The benchmark is Linux-only (uses `sched_setaffinity`).

```sh
cargo install just   # one-time
just bench           # writes bench/results-<hostname>.md
```

Defaults: writer on core 4, reader on core 3, 3 message sizes × 3 publish
gaps × 2 backends = 18 cells, 30 s per cell (~10 minutes per host).
Override on the command line:

```sh
just WRITER_CORE=10 READER_CORE=11 bench           # use other cores
just SIZES="64" GAPS_NS="100000" bench             # one cell only
just GAPS_NS="0 100000" DURATION=10 bench          # quicker, fewer gaps
just bench-quick                                    # smoke test
```

## Limitations

A few things xchannel deliberately doesn't do. These are explicit design
choices — they keep the algorithm small and the hot path cheap — but
they shape what the library is and isn't suited for.

- **Single writer per channel.** Concurrent writers will corrupt the
  shared state. Fan-in from N producers requires N channels and a
  downstream multiplexer.

- **No back-pressure.** A slow reader cannot slow the writer; the
  writer keeps producing into rolled files and the gap grows. Use
  `WriterBuilder::keep_files(N)` to bound retention, and make sure
  your reader can sustain the steady-state rate.

- **A reader that falls more than `keep_files(N)` files behind will
  get `ENOENT`** when it tries to follow a Roll into a file that has
  already been pruned. There is no "skip ahead" recovery — opening a
  fresh `Reader` is the supported path.

- **No kernel-mediated wake-up.** `try_read` is strictly non-blocking;
  `Reader::read_blocking(timeout)` is a sleep-backoff helper
  (1 µs → 10 ms cap, no syscall on the writer side). Sub-µs wake-up
  would require a futex- or eventfd-based notification primitive that
  xchannel does not currently provide. Async runtimes should compose
  `try_read` with their own sleep — `read_blocking` uses
  `std::thread::sleep` and will block an executor thread.

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
* header type (`u8` on disk; parse with `header.parsed_header_type()?`)
* message type
* payload length
* timestamp

Readers check:

```
header.is_committed()?
```

When code needs the record kind, prefer:

```rust
let kind = msg.header().parsed_header_type()?;
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
