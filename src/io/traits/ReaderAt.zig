//! `volt.io.ReaderAt` — positional read trait. Wraps `pread` semantics.
//!
//! Distinct from `Reader` so multiple coroutines can read the same
//! underlying resource concurrently without seek-then-read races. The
//! offset is per-call; the source's seek cursor (if any) is unaffected.
//!
//! ## EOF
//!
//! Same as `Reader.read`: returns 0 when `offset` is at or past the
//! end of the source. Short returns ARE legal even before EOF (pread
//! semantics) — callers that demand a specific byte count use
//! `readAllAt`, which converts EOF to `error.EndOfStream`.

const std = @import("std");
const io_errors = @import("../errors.zig");

pub const ReadError = io_errors.ReadError;

pub const ReaderAt = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        readAt: *const fn (ctx: *anyopaque, buf: []u8, offset: u64) ReadError!usize,
    };

    pub fn readAt(self: ReaderAt, buf: []u8, offset: u64) ReadError!usize {
        return self.vtable.readAt(self.ctx, buf, offset);
    }

    /// Read exactly `buf.len` bytes starting at `offset`. Returns
    /// `error.EndOfStream` if EOF arrives before the buffer is full.
    pub fn readAllAt(self: ReaderAt, buf: []u8, offset: u64) ReadError!void {
        var i: usize = 0;
        while (i < buf.len) {
            const n = try self.readAt(buf[i..], offset + i);
            if (n == 0) return error.EndOfStream;
            i += n;
        }
    }
};
