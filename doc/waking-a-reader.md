# Waking a reader without polling

**Status: a request, not an agreed design.** The numbers below are measured with
`examples/futex-wake.rs`, on a laptop and on a latency-tuned server.

## What is wanted

A reader that has caught up should learn of the next commit in about ten microseconds, without
polling, and a channel that does not ask for this should pay nothing.

`wait_for_message` sleeps well — 1 µs doubling to a 10 ms cap — but it wakes on a timer, not on the
commit. Ten microseconds by looking is a hundred thousand looks a second, per channel, per reader.

## The constraint: readers cannot write

Readers open segments read-only and map them `PROT_READ`. That is a property worth keeping: a buggy
or hostile reader cannot corrupt the channel, and a reader running as a different user needs only
read permission. So the design may not ask a reader to write to a segment. That rules out the usual
waiters bit, where a reader announces that it is about to sleep.

## The design: the writer always signals

One **wake word** per segment, a `u32` in `ChannelHeader::_reserved2`, written only by the writer.

- **A writer on a channel that opted in**, after each commit: `wake_word.fetch_add(1, Release)`,
  then `futex_wake(wake_word, i32::MAX)`.
- **A reader with nothing to read**: loads the word (`Acquire`), re-checks for a record, and if
  there is none calls `futex_wait(wake_word, seen)` with a capped timeout.

Readers only read the word and wait on it. `FUTEX_WAIT` on a read-only mapping works; this was
checked as part of the measurements below.

### Why this is race-free

The writer commits the record, then bumps the word, then wakes. The reader loads the word, then
re-checks for a record, then waits on the value it loaded.

- If the commit happened before the reader's re-check, the reader sees the record and does not
  sleep.
- If it happened after, the writer's bump comes after the reader's load. Either the bump lands
  before the kernel compares the word in `futex_wait`, which then returns at once, or the reader is
  already queued and the wake reaches it.

The reader's load must be `Acquire`, so that a word value it observes carries the commit that came
before it. No `SeqCst` is needed: the futex's own compare-and-block closes the race.

### What it costs

Cost added to the writer's commit path:

| | laptop | server |
|---|---:|---:|
| `fetch_add` alone | 4.6 ns | 4.6 ns |
| `fetch_add` + `futex_wake`, nobody asleep | 165 ns | 733 ns |
| `fetch_add` + `futex_wake`, a reader asleep (p50) | 2.1–2.5 µs | 1.2–1.4 µs |

The laptop is an i9-11900H with no isolation, kernel 7.0. The server is a Xeon Gold 6146 with
isolated cores and `idle=poll`, RHEL 9.6, kernel 5.14.

So opting in costs a syscall on every commit, whether or not anyone waits, and what a syscall costs
depends on the CPU. The server's Skylake-generation Xeon runs with page-table isolation, IBRS and
CPU-buffer clearing on every kernel exit; the laptop's newer CPU has hardware fixes and needs none
of that. At 100K msg/s the wake takes 1.7% of the writer's core on the laptop and 7.3% on the
server; at 1M msg/s, 17% and 73%. It is a per-channel choice: worth it on a channel that is often
idle, not on a busy one, and less often worth it on older, heavily mitigated CPUs. When a reader
*is* asleep, waking it costs the writer another 1–2.5 µs, the price of the kernel scheduling the
reader.

### Why not rate-limit it

Rate-limiting the wake does not keep a latency promise: a writer that goes quiet inside a window
never sends the last wake, so keeping "never more than N behind" needs a trailing wake from a timer
or a thread.

## The alternative: a waiters bit in a writable sidecar

If a syscall per commit is too much for a channel that needs waking, the writer can skip the
syscall when nobody is asleep. That needs readers to announce themselves, which they may not do in
the segment, so the announcement goes in a separate file: `<base>.wake`, the only file readers open
read-write.

- The word lives in the sidecar: bit 31 is "someone is asleep", the low 31 bits a counter.
- A reader about to sleep compare-and-swaps the word from `w` to `w | BIT` (reading the value and
  announcing itself in one step), re-checks for a record, then `futex_wait(word, w | BIT)`.
- The writer, after each commit, loads the word. Only if the bit is set does it swap in
  `(counter + 1) & !BIT` and call `futex_wake`. This needs store-load ordering on both sides; on
  x86 the `lock xadd` already in `publish_wp` provides it for free.

What it buys and what it costs:

- **No syscall when nobody is asleep:** one load per commit instead of 165–733 ns.
- **Segments stay read-only.** A reader that misbehaves through the sidecar can only set the bit
  (the writer makes wasted syscalls) or clear it (another reader misses a wake and falls back to its
  capped sleep). It cannot touch data.
- **One word per channel, not per segment,** so rolls need no handling at all.
- **A file the crate must own:** creation with permissions that let readers write (which matters
  when reader and writer run as different users), removal in `cleanup_channel_files`, and a
  generation stamp so a recreated channel's sidecar is not mistaken for the old one. A reader that
  cannot open it read-write falls back to the backoff.

Build it only if a channel needs waking at a rate where a syscall per commit hurts. On CPUs like
the server's, where that syscall costs over 700 ns, the threshold is about ten times lower than on
the laptop.

## Why not a wake word per directory

`futex_waitv` waits on up to 128 words in one call and is available on the kernels we run (RHEL 9
has it backported to 5.14), so a reader of five channels sleeps on five per-channel words directly.
A directory-wide word would also make every writer in it do a locked read-modify-write on one cache
line on every commit, wake every reader in the directory for every channel, and add a file whose
creation, permissions and cleanup the crate does not own.

## Layout

Both fields come out of `_reserved2`, which is additive and zero-default, so no format bump:

| `ChannelHeader` offset | size | field | meaning |
|---:|---:|---|---|
| 97 | 1 | `wake_flags` | bit 0: the writer that created this segment wakes readers |
| 100 | 4 | `wake_word` | counter bumped on every commit; the first 4-byte-aligned slot in `_reserved2` |

The word sits on a different cache line from `write_position` and `message_count` (file offsets
16–32), so readers polling it do not disturb the writer's publish stores.

## Opting in

**`WriterBuilder::wake_readers(true)`, default off.** It stamps `wake_flags` in every segment the
writer creates. A writer that did not opt in branches only on its own setting, never on shared
state, so its commit path stays byte-identical to today's.

## Rules the reader needs

- **Trust the flag, not the version.** A segment without `wake_flags` bit 0 gets today's backoff.
- **Always cap the sleep**, at today's 10 ms backoff cap.
- **Detect a wrong flag.** An older writer reopening a channel leaves the flag set with nobody
  waking. Capped sleeps alone would then cost up to 10 ms on every wait, worse than today's backoff,
  which restarts at 1 µs on each wait. So if a capped sleep times out and a record turns out to be
  waiting, treat the flag as wrong and use the backoff for the rest of that segment. The exposure
  is one segment: the next one an older writer creates has the flag clear.

## Rolls

The word is per segment, so a roll moves to a new one. The writer's roll already release-commits a
`Roll` marker in the old segment. Immediately after it, the writer bumps the **old** word and wakes
on it. Sleepers wake, read the `Roll`, follow, and sleep next on the new word.

A futex on a file-backed mapping is keyed on the inode, which outlives an unlink for anyone still
mapping it, so a sleeper on a retired segment is still woken.

## The reader's API

- **`wait_for_message(timeout)` keeps its signature.** On a flagged segment it sleeps on the futex;
  otherwise it keeps today's backoff.
- **A wait over several readers**, built on `futex_waitv`. It loads every flagged segment's word,
  re-checks every reader for a record, then waits on all the words at once. It returns which reader
  has something, or that the timeout passed. Readers on unflagged segments, or a kernel without
  `futex_waitv`, fall back to the backoff.

## Not everywhere

macOS has no futex. Opting in is a no-op there and readers fall back to the backoff.

## How the two sides agree on a futex

They do not: a futex has no name. The kernel keys it on the inode and offset of a `MAP_SHARED`
mapping, so the word at a fixed offset in the header is shared by construction. No
`FUTEX_PRIVATE_FLAG`, which is the same-address-space fast path.

## What was measured, and what it means for the target

Wake latency is the time from the writer stamping the clock just before its `fetch_add` to the
waiting reader running again. The reader is a separate process with a read-only mapping. Each wake
follows a pause of 0.2–1.2 ms, so the reader is really asleep. 3,000 wakes per run.

| machine and setup | p50 | p90 | p99 | max |
|---|---:|---:|---:|---:|
| laptop, both pinned | 79 µs | 96 µs | 115 µs | 734 µs |
| laptop, unpinned | 79 µs | 97 µs | 119 µs | 193 µs |
| laptop, reader's core kept awake by spinning on its hyperthread sibling | 5.4 µs | 7.2 µs | 175 µs | 1.05 ms |
| server, pinned to isolated cores | 2.7 µs | 2.9 µs | 3.1 µs | 12.7 µs |
| server, unpinned (housekeeping cores) | 2.8 µs | 3.1 µs | 3.7 µs | 11.1 µs |

**The futex is fast; the sleeping CPU is not.** On the laptop, an idle core drops into deep C-states
whose exit latencies are 253 µs (C2) and 1048 µs (C3). That, not the futex, sets the 79 µs. Keep
the core awake and the wake takes about 5 µs.

So the ten-microsecond target is met only where the reader's core does not sleep deeply:

- **On a tuned host** (`idle=poll` or C-states capped), it is met with room to spare: 2.7 µs at
  p50 and 3.1 µs at p99 on the server, pinned or not.
- **On an untuned machine,** a sleeping reader typically takes 80–120 µs. That is still far better
  than polling at a hundred thousand looks a second, and better than today's backoff once it has
  grown, but it is not ten microseconds. Getting there needs the process to hold C-states off (for
  example through `/dev/cpu_dma_latency`, which needs privileges), or a short spin before sleeping
  to catch the next record of a burst.

## Still to measure

- That a channel which does not opt in is unchanged: the same before/after runs on the tuned server
  as the last two releases. This can only be done once the code exists.

## Reproducing

```text
cargo build --release --example futex-wake
target/release/examples/futex-wake nowait  /dev/shm/futex-wake <core>
target/release/examples/futex-wake latency /dev/shm/futex-wake <writer_core> <reader_core> 3000
```

A core of `-1` leaves that side unpinned.
