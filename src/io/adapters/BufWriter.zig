//! `volt.io.BufWriter` — buffered Writer wrapper.
//!
//! Coalesces small writes into large flushes. Mirror image of
//! `BufReader` — same allocation pattern, same vtable shape, same
//! Risk #2 mitigation rationale (small writes don't hit the sink
//! vtable on every call).
//!
//! ## Flush discipline
//!
//! Buffered bytes are NOT delivered to the underlying sink until
//! `flush()` is called or the buffer fills. **You must `flush()` (or
//! `deinit` after a successful flush) before dropping a `BufWriter`
//! you've written to**, or trailing bytes are lost. The `Writer.flush`
//! trait method forwards here.

const std = @import("std");
const traits = @import("../traits/traits.zig");
const Writer = traits.Writer;
const WriteError = traits.WriteError;

pub const BufWriter = struct {
    sink: Writer,
    buf: []u8,
    pos: usize = 0,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, sink: Writer, capacity: usize) std.mem.Allocator.Error!BufWriter {
        std.debug.assert(capacity > 0);
        const buf = try allocator.alloc(u8, capacity);
        return .{ .sink = sink, .buf = buf, .allocator = allocator };
    }

    /// Releases the internal buffer. Does NOT flush — call `flush`
    /// first if you have unwritten bytes you care about.
    pub fn deinit(self: *BufWriter) void {
        self.allocator.free(self.buf);
        self.* = undefined;
    }

    pub fn writer(self: *BufWriter) Writer {
        return .{ .ctx = @ptrCast(self), .vtable = &vtable };
    }

    /// Bytes currently buffered (haven't been written to the sink).
    pub fn buffered(self: *const BufWriter) usize {
        return self.pos;
    }

    /// Drain the buffer to the sink. Called automatically when the
    /// buffer fills; call manually before reading back from the sink
    /// or before close.
    pub fn flush(self: *BufWriter) WriteError!void {
        if (self.pos == 0) return;
        try self.sink.writeAll(self.buf[0..self.pos]);
        self.pos = 0;
    }
};

const vtable: Writer.VTable = .{ .write = &vtableWrite, .flush = &vtableFlush };

fn vtableWrite(ctx: *anyopaque, buf: []const u8) WriteError!usize {
    const self: *BufWriter = @ptrCast(@alignCast(ctx));
    // If the incoming write is larger than our buffer, flush what we
    // have and pass the request straight through to the sink. Avoids
    // splitting one big write into many buffer-sized ones.
    if (buf.len >= self.buf.len) {
        try self.flush();
        return self.sink.write(buf);
    }
    // If it doesn't fit alongside what's already buffered, flush first.
    if (self.pos + buf.len > self.buf.len) {
        try self.flush();
    }
    @memcpy(self.buf[self.pos..][0..buf.len], buf);
    self.pos += buf.len;
    return buf.len;
}

fn vtableFlush(ctx: *anyopaque) WriteError!void {
    const self: *BufWriter = @ptrCast(@alignCast(ctx));
    return self.flush();
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const BufferWriter = traits.BufferWriter;

test "BufWriter: small writes coalesce until flush" {
    var sink_buf: [64]u8 = undefined;
    var sink = BufferWriter{ .buf = &sink_buf };

    var bw = try BufWriter.init(std.testing.allocator, sink.writer(), 16);
    defer bw.deinit();

    try bw.writer().writeAll("ab");
    try bw.writer().writeAll("cd");
    try std.testing.expectEqual(@as(usize, 0), sink.pos); // not flushed yet
    try bw.flush();
    try std.testing.expectEqualStrings("abcd", sink.written());
}

test "BufWriter: large write bypasses buffer" {
    var sink_buf: [64]u8 = undefined;
    var sink = BufferWriter{ .buf = &sink_buf };

    var bw = try BufWriter.init(std.testing.allocator, sink.writer(), 4);
    defer bw.deinit();

    try bw.writer().writeAll("xy");
    // 16 bytes > 4 byte buffer → triggers flush + passthrough.
    try bw.writer().writeAll("0123456789abcdef");
    try std.testing.expectEqualStrings("xy0123456789abcdef", sink.written());
}

test "BufWriter: filling buffer auto-flushes" {
    var sink_buf: [64]u8 = undefined;
    var sink = BufferWriter{ .buf = &sink_buf };

    var bw = try BufWriter.init(std.testing.allocator, sink.writer(), 4);
    defer bw.deinit();

    try bw.writer().writeAll("aabb"); // exactly fills, no auto-flush yet
    try bw.writer().writeAll("c"); // doesn't fit → flush + buffer "c"
    try std.testing.expectEqualStrings("aabb", sink.written());
    try bw.flush();
    try std.testing.expectEqualStrings("aabbc", sink.written());
}

test "BufWriter: flush forwarded via trait" {
    var sink_buf: [64]u8 = undefined;
    var sink = BufferWriter{ .buf = &sink_buf };

    var bw = try BufWriter.init(std.testing.allocator, sink.writer(), 16);
    defer bw.deinit();

    try bw.writer().writeAll("hi");
    try bw.writer().flush();
    try std.testing.expectEqualStrings("hi", sink.written());
}
