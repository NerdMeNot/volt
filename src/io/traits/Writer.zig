//! `volt.io.Writer` — vtable-based byte stream sink.
//!
//! Companion to `Reader` (see `Reader.zig` for design notes). Same
//! shape, mirrored: `write` returns bytes consumed, `writeAll` loops
//! on partial writes, `flush` is optional for buffered writers.
//!
//! ## as_fd marker
//!
//! Same contract as on `Reader`: present if and only if the sink is a
//! real fd that can participate in kernel-level zero-copy. Transforming
//! sinks (TLS encrypter, gzip writer) MUST leave it null.

const std = @import("std");
const posix = std.posix;
const io_errors = @import("../errors.zig");

pub const WriteError = io_errors.WriteError;

pub const Writer = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Write some prefix of `buf`. Returns bytes consumed (may be
        /// less than buf.len for streams under backpressure; callers
        /// that need full delivery use `writeAll`).
        write: *const fn (ctx: *anyopaque, buf: []const u8) WriteError!usize,

        /// If the sink is a real fd, return it; otherwise null. See
        /// `Reader.zig` for the contract.
        as_fd: ?*const fn (ctx: *anyopaque) ?posix.fd_t = null,

        /// Optional vectored write.
        writev: ?*const fn (ctx: *anyopaque, iovs: []const posix.iovec_const) WriteError!usize = null,

        /// Optional flush — drains any internal buffer to the underlying
        /// sink. No-op for unbuffered writers.
        flush: ?*const fn (ctx: *anyopaque) WriteError!void = null,
    };

    comptime {
        // Cap on vtable size: see traits.zig header for the marker
        // policy. Catches future bloat before it ships.
        std.debug.assert(@sizeOf(VTable) <= 64);
    }

    /// Write some prefix of `buf`. Returns bytes consumed.
    pub fn write(self: Writer, buf: []const u8) WriteError!usize {
        return self.vtable.write(self.ctx, buf);
    }

    /// Write all bytes — loops on partial writes until done. Returns
    /// `error.BrokenPipe` if the sink reports 0 bytes written without
    /// erroring (a broken stream that doesn't surface the error).
    pub fn writeAll(self: Writer, buf: []const u8) WriteError!void {
        var i: usize = 0;
        while (i < buf.len) {
            const n = try self.write(buf[i..]);
            if (n == 0) return error.BrokenPipe;
            i += n;
        }
    }

    pub fn writeByte(self: Writer, b: u8) WriteError!void {
        const arr = [_]u8{b};
        return self.writeAll(&arr);
    }

    /// Vectored write. Falls back to looping `writeAll` over each iovec
    /// if the vtable doesn't supply `writev`.
    pub fn writev(self: Writer, iovs: []const posix.iovec_const) WriteError!usize {
        if (self.vtable.writev) |fn_ptr| return fn_ptr(self.ctx, iovs);
        var total: usize = 0;
        for (iovs) |iov| {
            const slice = iov.base[0..iov.len];
            try self.writeAll(slice);
            total += slice.len;
        }
        return total;
    }

    /// Drain any internal buffer. No-op for unbuffered sinks.
    pub fn flush(self: Writer) WriteError!void {
        if (self.vtable.flush) |fn_ptr| return fn_ptr(self.ctx);
    }

    /// True if the sink has an `as_fd` shortcut.
    pub fn hasFd(self: Writer) bool {
        if (self.vtable.as_fd) |fn_ptr| {
            return fn_ptr(self.ctx) != null;
        }
        return false;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

/// Companion fixture to SliceReader — collects writes into a fixed
/// buffer. Used by adapter tests.
pub const BufferWriter = struct {
    buf: []u8,
    pos: usize = 0,
    flushed: u32 = 0,

    pub fn writer(self: *BufferWriter) Writer {
        return .{
            .ctx = @ptrCast(self),
            .vtable = &.{ .write = &buf_write, .flush = &buf_flush },
        };
    }

    pub fn written(self: *const BufferWriter) []const u8 {
        return self.buf[0..self.pos];
    }

    fn buf_write(ctx: *anyopaque, buf: []const u8) WriteError!usize {
        const self: *BufferWriter = @ptrCast(@alignCast(ctx));
        const room = self.buf.len - self.pos;
        if (room == 0) return error.NoSpaceLeft;
        const n = @min(buf.len, room);
        @memcpy(self.buf[self.pos..][0..n], buf[0..n]);
        self.pos += n;
        return n;
    }

    fn buf_flush(ctx: *anyopaque) WriteError!void {
        const self: *BufferWriter = @ptrCast(@alignCast(ctx));
        self.flushed += 1;
    }
};

test "Writer.writeAll: round-trip" {
    var buf: [32]u8 = undefined;
    var bw = BufferWriter{ .buf = &buf };
    try bw.writer().writeAll("hello world");
    try std.testing.expectEqualStrings("hello world", bw.written());
}

test "Writer.writeByte: appends one byte" {
    var buf: [4]u8 = undefined;
    var bw = BufferWriter{ .buf = &buf };
    try bw.writer().writeByte('z');
    try std.testing.expectEqualStrings("z", bw.written());
}

test "Writer.flush: forwarded when present" {
    var buf: [4]u8 = undefined;
    var bw = BufferWriter{ .buf = &buf };
    try bw.writer().flush();
    try std.testing.expectEqual(@as(u32, 1), bw.flushed);
}
