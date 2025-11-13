# xchannel

**xchannel** is a tiny, zero‑copy, mmap‑backed IPC channel format with file rolling.

- Regionized file layout. Region 0 begins with a `Channel` header; the rest is append‑only.
- Messages are `MessageHeader(User)` + payload.
- Special records: `Skip` (pad to next region), `Roll` (file rolled).
- Readers can **LateJoin** (from start) or **Live** (tail).

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
  count are tracked using atomic variables with proper memory orderin

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
if let Some(buf) = w.try_reserve(payload.len()) {
    buf.copy_from_slice(payload);
    w.commit(1, payload.len() as u32, timestamp)?;
}

// read it back
let mut r = ReaderBuilder::new("demo.xch").late_join().build()?;
if let Some(msg) = r.try_read() {
    let hdr = msg.header().unwrap();
    println!("type={}, len={}", hdr.message_type, hdr.length);
    println!("payload={:?}", msg.payload().unwrap());
}
```