---
title: I/O handle conformance shape
description: The standard shape every Volt async byte source exposes — fd field, sync read/write/readFull/writeAll methods, std.Io.Reader / std.Io.Writer adapter pair. Downstream volt-fs / volt-net libraries mirror this so std-library code composes cleanly.
---

Volt's core ships the following I/O-handle types — every one
conforms to the shape described below, so std-library code
(formatters, parsers, anything that takes `*std.Io.Reader`)
composes across the whole surface:

- **Networking**: TcpStream, TcpListener, UdpSocket, UnixStream,
  UnixListener, UnixDatagram (the last three POSIX-only)
- **Filesystem**: File

DNS / TLS live downstream (`volt-tls`); their handle types follow
the same shape so consumers don't see a difference.

This page defines that shape. If you're authoring a Volt-adjacent
library that exposes an async byte source, conform to this shape —
your handle composes with `std.Io` for free.

## The shape

```zig
pub const MyHandle = struct {
    /// Raw OS handle — fd on POSIX, SOCKET / HANDLE on Windows.
    /// Exposed for advanced users that need direct syscall access.
    fd: i32,

    // ─── Direct synchronous methods ──────────────────────────────

    /// One-shot read; returns bytes read (may be partial). Parks
    /// the calling coroutine on EAGAIN.
    pub fn read(self: *MyHandle, buf: []u8) IoError!usize;

    /// One-shot write; returns bytes written (may be partial).
    pub fn write(self: *MyHandle, buf: []const u8) IoError!usize;

    /// Read until `buf` is full or EOF. Returns total bytes
    /// (may be less than `buf.len` on EOF).
    pub fn readFull(self: *MyHandle, buf: []u8) IoError!usize;

    /// Write every byte in `buf`. Errors if the connection breaks
    /// partway through.
    pub fn writeAll(self: *MyHandle, buf: []const u8) IoError!void;

    // ─── std.Io adapters ─────────────────────────────────────────

    /// Wrap as `std.Io.Reader` so std-library code can consume
    /// bytes from this handle. Caller provides the backing buffer
    /// for std's internal buffered-read protocol.
    pub fn reader(self: *MyHandle, buffer: []u8) Reader;

    /// Wrap as `std.Io.Writer`. Same shape.
    pub fn writer(self: *MyHandle, buffer: []u8) Writer;

    /// `std.Io.Reader` wrapper. `interface` is the field std-library
    /// code consumes (e.g. `&reader.interface`); `err` carries the
    /// typed `IoError` after the std interface surfaces
    /// `ReadFailed` / `EndOfStream`.
    pub const Reader = struct {
        stream: *MyHandle,
        interface: std.Io.Reader,
        err: ?IoError = null,
        // ... vtable.stream calls reactor read; sets err on failure.
    };

    /// `std.Io.Writer` wrapper. Same shape.
    pub const Writer = struct {
        stream: *MyHandle,
        interface: std.Io.Writer,
        err: ?IoError = null,
        // ... vtable.drain calls reactor write; sets err on failure.
    };
};
```

## What's NOT in the handle

By design, the handle does **not** carry:

- **An explicit `*Runtime` pointer.** The implicit-via-TLS (`current.require().runtime`) model keeps handle size at 8 bytes and matches Tokio's "handles know their runtime via thread-local context." Use [`Runtime.runDetached`](../runtime/) for embedding scenarios — that path doesn't need handles to carry `*Runtime`.
- **An async-aware Reader/Writer trait of Volt's own.** Volt uses `std.Io.Reader` / `std.Io.Writer` directly. No parallel abstraction. Std-library buffering / formatting / parsing composes out of the box.
- **A Drop method.** Zig idiom is explicit `defer handle.close()`. No RAII.

## Why this shape

**Typed error stashing.** The std.Io vtable only surfaces opaque
`error.ReadFailed` / `error.WriteFailed` / `error.EndOfStream`. Volt
needs to preserve the categorical `IoError` (`ConnectionReset`,
`BrokenPipe`, etc.) for `catch err switch` patterns. The `err`
field on the wrapper struct is the standard place to stash it; the
caller pattern is:

```zig
var buf: [4096]u8 = undefined;
var r = stream.reader(&buf);
const byte = r.interface.takeByte() catch |e| switch (e) {
    error.EndOfStream => return,
    error.ReadFailed => return r.err.?,  // unwrap typed IoError
};
```

**No middleware between std and the reactor.** The vtable's `stream`
/ `drain` calls reactor.readAsync / writeAll directly. No buffering
beyond what std's `Reader.buffer` and `Writer.buffer` already
provide. No Volt-specific framing.

**Caller-provided buffers.** Both `reader(buf)` and `writer(buf)`
take a buffer the caller owns. No internal allocation. Stack
allocation is the common case (`var buf: [4096]u8 = undefined;`).
This matches the rest of Volt: explicit allocator-injection or
caller-stack-storage, never hidden mallocs.

## Reference implementation

`src/net.zig` — `TcpStream.Reader` and `TcpStream.Writer`. About 80
lines, demonstrates every required piece (vtable impls,
`@fieldParentPtr` recovery, typed-err stashing, both adapter
patterns).

## Why no `pub const VTable = ...` re-export?

Each Volt handle's `Reader` / `Writer` is a *named* type that
contains a `std.Io.Reader` / `std.Io.Writer` interface as a field.
The vtable functions are private (`fn streamImpl`, `fn drainImpl`)
because the std vtable's signature requires them to recover the
wrapping struct via `@fieldParentPtr` — they're not callable
standalone. Users interact only with the `interface` field, not the
vtable.

## Conformance checklist for downstream libs

When implementing this shape in `volt-fs` / `volt-net` / similar:

- [ ] Handle struct has a public `fd` (or platform-equivalent) field
- [ ] Direct methods (`read`, `write`, `readFull`, `writeAll`) return `IoError` (re-export from `volt.IoError`)
- [ ] `reader(buffer)` and `writer(buffer)` methods exist; return wrapper types
- [ ] Wrapper structs have a public `interface` field (`std.Io.Reader` / `std.Io.Writer`)
- [ ] Wrapper structs have a public `err: ?IoError` field for typed error recovery
- [ ] vtable functions live on the wrapper struct as `fn streamImpl` / `fn drainImpl` (private)
- [ ] vtable recovers the wrapper via `@fieldParentPtr("interface", io_r)`
- [ ] On reactor I/O error: set `self.err = e`; return `error.ReadFailed` / `error.WriteFailed`
- [ ] On EOF: return `error.EndOfStream` (do not stash on `err`)

A round-trip test that uses `reader.interface.takeByte` + `writer.interface.writeAll` against a loopback TCP socket validates the shape end-to-end. See `src/net.zig` test `"TcpStream: std.Io.Reader/Writer adapter round-trip"`.
