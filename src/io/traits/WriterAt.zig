//! `volt.io.WriterAt` — positional write trait. Wraps `pwrite` semantics.
//!
//! Companion to `ReaderAt` (see that file for the multi-coroutine
//! rationale). Per-call offset; source's seek cursor unaffected.

const std = @import("std");
const io_errors = @import("../errors.zig");

pub const WriteError = io_errors.WriteError;

pub const WriterAt = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        writeAt: *const fn (ctx: *anyopaque, buf: []const u8, offset: u64) WriteError!usize,
    };

    pub fn writeAt(self: WriterAt, buf: []const u8, offset: u64) WriteError!usize {
        return self.vtable.writeAt(self.ctx, buf, offset);
    }

    /// Write all bytes starting at `offset`. Loops on partial writes.
    pub fn writeAllAt(self: WriterAt, buf: []const u8, offset: u64) WriteError!void {
        var i: usize = 0;
        while (i < buf.len) {
            const n = try self.writeAt(buf[i..], offset + i);
            if (n == 0) return error.BrokenPipe;
            i += n;
        }
    }
};
