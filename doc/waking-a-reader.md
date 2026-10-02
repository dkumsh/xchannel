# Waking a reader without polling

**Status: a request, not an agreed design.** Numbers below are targets, not measurements.

## What is wanted

A reader that has caught up should learn of the next commit in about ten microseconds, without
polling, and a channel that does not ask for this should pay nothing.

`wait_for_message` sleeps well — 1 µs doubling to a 10 ms cap — but it wakes on a timer, not on the
commit. Ten microseconds by looking is a hundred thousand looks a second, per channel, per reader.

## The design

A **waiters bit** and a **wake word** per segment, both in `ChannelHeader::_reserved2`. Additive and
zero-default, so no format bump.

- A reader about to sleep sets the bit, re-checks for a record, then `futex_wait`s on the word.
- A writer that finds the bit set clears it, bumps the word, and `futex_wake`s.
- A writer that finds it clear does nothing but the load.

This is the waiters bit of a glibc mutex. The per-commit syscall is paid only when somebody is
actually asleep, which is what makes opting in affordable on a channel that is sometimes busy.

**It needs store-load ordering** between the commit and the check of the bit. On x86 the `lock xadd`
already in `publish_wp` provides it; elsewhere it is an explicit fence.

### Why not signal unconditionally

Simpler — nothing of a reader in shared memory — but it is a syscall on every commit whether or not
anyone waits. That rules out any channel that is ever busy. Rate-limiting it does not rescue the
promise: a writer that goes quiet inside a window never sends the last wake, so keeping "never more
than N behind" needs a trailing wake from a timer or a thread.

### Why not a wake word per directory

`futex_waitv` waits on up to 128 words in one call and is available on the kernels we run, so a
reader of five channels sleeps on five per-channel words directly. A directory-wide word would also
make every writer in it do a locked read-modify-write on one cache line on every commit, wake every
reader in the directory for every channel, and add a file whose creation, permissions and cleanup
the crate does not own.

## Rules the reader needs

- **A per-segment flag, stamped at creation, saying this writer wakes.** Without it a reader cannot
  tell whether to expect a wake.
- **Always cap the sleep**, at today's 10 ms backoff cap. A flag can be wrong — an older writer
  reopening the channel leaves it set with nobody bumping the word — and a wrong flag must cost
  slow polling, never a hang.
- **The load of the word is Acquire.**
- **`FUTEX_WAIT` works on a read-only mapping**, which readers have.

## Rolls

The word is per segment, so a roll moves to a new one. The writer's roll already release-commits a
`Roll` marker in the old segment; the wake goes immediately after it, on the **old** word. Sleepers
wake, read the `Roll`, follow, and sleep next on the new word.

A futex on a file-backed mapping is keyed on the inode, which outlives an unlink for anyone still
mapping it, so a sleeper on a retired segment is still woken.

## Not everywhere

macOS has no futex. Opting in is a no-op there and readers fall back to the backoff.

## How the two sides agree on a futex

They do not: a futex has no name. The kernel keys it on the inode and offset of a `MAP_SHARED`
mapping, so the word at a fixed offset in the header is shared by construction. No
`FUTEX_PRIVATE_FLAG`, which is the same-address-space fast path.

## To measure before building

- Whether a sleeping reader actually wakes within ten microseconds, on a tuned box and on an
  untuned one. It depends on idle states and scheduler load.
- What `futex_wake` costs on the commit path when somebody is asleep.
