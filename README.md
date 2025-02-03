# xchannel

**xchannel** is a tiny, zero‑copy, mmap‑backed IPC channel format with file rolling.

- Regionized file layout. Region 0 begins with a `Channel` header; the rest is append‑only.
- Messages are `MessageHeader(User)` + payload.
- Special records: `Skip` (pad to next region), `Roll` (file rolled).
- Readers can **LateJoin** (from start) or **Live** (tail).

## Why?

- Shared‑memory / IPC logs without a broker.
- Constant-time tailing (readers only track a byte offset).
- Works on Linux and macOS (16 KiB) and typical 4 KiB page systems.

## Minimum example

```rust
use xchannel::{Writer, Reader, ReaderMode};

let region = xchannel::page_size();           // ensure page-aligned regions
let mut w = Writer::open_or_create("demo.xch", region, 10_000_000, 0)?;

// write a message
let payload = b"hello world";
if let Some(buf) = w.try_reserve(payload.len()) {
    buf.copy_from_slice(payload);
    w.commit(1, payload.len() as u32)?;
}

// read it back
let mut r = Reader::open("demo.xch", ReaderMode::LateJoin)?;
if let Some(msg) = r.try_read() {
    let hdr = msg.header().unwrap();
    println!("type={}, len={}", hdr.message_type, hdr.length);
    println!("payload={:?}", msg.payload().unwrap());
}
# Ok::<(), Box<dyn std::error::Error>>(())
