//! `volt.io.BufReader` — buffered Reader wrapper.
//!
//! Reduces syscall count by reading large chunks from the source into
//! an internal buffer and serving small consumer reads from the
//! buffer. Critical for two reasons:
//!
//! 1. **Header / line / framed-protocol parsing** wants to look ahead
//!    a byte at a time without paying a syscall per byte. `readUntil`
//!    scans the buffered region and only refills on miss.
//!
//! 2. **Risk #2 (vtable cost) mitigation.** The hot byte-read path
//!    (`Reader.read` calls inside parser loops) doesn't hit the source
//!    vtable on every call — only on refill. The trait wrapper's
//!    `read` is a memcpy from the buffer plus pointer bumps; no
//!    indirect call until the buffer drains.
//!
//! ## Allocation
//!
//! Caller-supplied allocator. The buffer is allocated at `init` and
//! freed at `deinit`. Capacity is fixed for the BufReader's lifetime
//! — there's no resize. 64 KiB is the right default for most uses
//! (one disk page, smaller than most pipe buffers).

const std = @import("std");
const traits = @import("../traits/traits.zig");
const Reader = traits.Reader;
const ReadError = traits.ReadError;

pub const BufReader = struct {
    source: Reader,
    buf: []u8,
    start: usize = 0, // first unread byte in `buf`
    end: usize = 0, // one past last valid byte in `buf`
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, source: Reader, capacity: usize) std.mem.Allocator.Error!BufReader {
        std.debug.assert(capacity > 0);
        const buf = try allocator.alloc(u8, capacity);
        return .{ .source = source, .buf = buf, .allocator = allocator };
    }

    pub fn deinit(self: *BufReader) void {
        self.allocator.free(self.buf);
        self.* = undefined;
    }

    /// Adapt as a `Reader` trait value. The returned `Reader` is valid
    /// for the lifetime of `self`.
    pub fn reader(self: *BufReader) Reader {
        return .{ .ctx = @ptrCast(self), .vtable = &vtable };
    }

    /// Bytes currently buffered (haven't been served to the consumer).
    pub fn buffered(self: *const BufReader) usize {
        return self.end - self.start;
    }

    /// Peek at the next `n` buffered bytes without consuming them.
    /// Returns a slice into the internal buffer; valid only until the
    /// next `read` / `readUntil` / `discard` call. If fewer than `n`
    /// bytes are buffered, refills from the source first; if EOF
    /// arrives before `n` bytes are available, returns whatever was
    /// gathered (possibly empty).
    pub fn peek(self: *BufReader, n: usize) ReadError![]const u8 {
        if (self.buffered() >= n) {
            return self.buf[self.start .. self.start + n];
        }
        // Compact buffered bytes to the front, then refill.
        if (self.start > 0) {
            std.mem.copyForwards(u8, self.buf[0..self.buffered()], self.buf[self.start..self.end]);
            self.end -= self.start;
            self.start = 0;
        }
        while (self.end < n and self.end < self.buf.len) {
            const got = try self.source.read(self.buf[self.end..]);
            if (got == 0) break;
            self.end += got;
        }
        return self.buf[self.start..self.end];
    }

    /// Discard up to `n` buffered + sourced bytes. Returns the number
    /// actually discarded.
    pub fn discard(self: *BufReader, n: u64) ReadError!u64 {
        var remaining: u64 = n;
        const buffered_avail: u64 = @intCast(self.buffered());
        const from_buf: u64 = @min(remaining, buffered_avail);
        self.start += @intCast(from_buf);
        remaining -= from_buf;
        if (remaining == 0) return n;
        // Drain the rest from the source.
        const from_source = try self.reader().discard(remaining);
        return n - (remaining - from_source);
    }

    /// Read up to and including the first byte equal to `delim`. The
    /// delimiter is consumed but NOT written to `dst`. Returns the
    /// number of bytes written to `dst` (excluding the delim).
    ///
    /// Errors:
    ///   - `error.StreamTooLong` — delim not found before `dst.len`
    ///     exhausted (or before `max_bytes` in `readUntilAlloc`).
    ///   - `error.EndOfStream` — EOF arrived before any bytes were
    ///     read AND before the delim. If EOF arrives mid-line, the
    ///     partial line is returned without erroring; the next call
    ///     will see EOF cleanly.
    pub fn readUntil(self: *BufReader, delim: u8, dst: []u8) ReadError!usize {
        var written: usize = 0;
        while (true) {
            if (self.buffered() == 0) {
                const got = try self.source.read(self.buf);
                if (got == 0) {
                    if (written == 0) return error.EndOfStream;
                    return written;
                }
                self.start = 0;
                self.end = got;
            }
            const slice = self.buf[self.start..self.end];
            if (std.mem.indexOfScalar(u8, slice, delim)) |idx| {
                if (written + idx > dst.len) return error.StreamTooLong;
                @memcpy(dst[written..][0..idx], slice[0..idx]);
                written += idx;
                self.start += idx + 1; // consume delim too
                return written;
            }
            if (written + slice.len > dst.len) return error.StreamTooLong;
            @memcpy(dst[written..][0..slice.len], slice);
            written += slice.len;
            self.start = self.end;
        }
    }

    /// Read up to and including `delim` into a freshly-allocated slice.
    /// Caller owns the result and must free with `allocator`. Stops
    /// with `error.StreamTooLong` if total bytes (excluding delim)
    /// would exceed `max_bytes`.
    pub fn readUntilAlloc(
        self: *BufReader,
        allocator: std.mem.Allocator,
        delim: u8,
        max_bytes: usize,
    ) (ReadError || std.mem.Allocator.Error)![]u8 {
        var list = std.array_list.Managed(u8).init(allocator);
        errdefer list.deinit();
        while (true) {
            if (self.buffered() == 0) {
                const got = try self.source.read(self.buf);
                if (got == 0) {
                    if (list.items.len == 0) return error.EndOfStream;
                    return list.toOwnedSlice();
                }
                self.start = 0;
                self.end = got;
            }
            const slice = self.buf[self.start..self.end];
            if (std.mem.indexOfScalar(u8, slice, delim)) |idx| {
                if (list.items.len + idx > max_bytes) return error.StreamTooLong;
                try list.appendSlice(slice[0..idx]);
                self.start += idx + 1;
                return list.toOwnedSlice();
            }
            if (list.items.len + slice.len > max_bytes) return error.StreamTooLong;
            try list.appendSlice(slice);
            self.start = self.end;
        }
    }
};

// ─────────────────────────────────────────────────────────────────────
// Vtable
// ─────────────────────────────────────────────────────────────────────

const vtable: Reader.VTable = .{ .read = &vtableRead };

fn vtableRead(ctx: *anyopaque, buf: []u8) ReadError!usize {
    const self: *BufReader = @ptrCast(@alignCast(ctx));
    // Fast path: serve from internal buffer.
    if (self.buffered() > 0) {
        const avail = self.buffered();
        const n = @min(buf.len, avail);
        @memcpy(buf[0..n], self.buf[self.start..][0..n]);
        self.start += n;
        return n;
    }
    // Buffer empty. If consumer's request is bigger than our buffer,
    // skip the indirection and read directly into `buf`. Avoids a
    // copy for sequential bulk reads.
    if (buf.len >= self.buf.len) {
        return self.source.read(buf);
    }
    // Refill internal buffer, then serve.
    const got = try self.source.read(self.buf);
    self.start = 0;
    self.end = got;
    if (got == 0) return 0;
    const n = @min(buf.len, got);
    @memcpy(buf[0..n], self.buf[0..n]);
    self.start = n;
    return n;
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const SliceReader = traits.SliceReader;

test "BufReader: serves small reads from internal buffer" {
    var sr = SliceReader{ .bytes = "abcdefghij" };
    var br = try BufReader.init(std.testing.allocator, sr.reader(), 4);
    defer br.deinit();

    var buf: [3]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 3), try br.reader().read(&buf));
    try std.testing.expectEqualStrings("abc", &buf);
    try std.testing.expectEqual(@as(usize, 1), try br.reader().read(&buf));
    try std.testing.expectEqualStrings("d", buf[0..1]);
}

test "BufReader: large request bypasses internal buffer" {
    var sr = SliceReader{ .bytes = "abcdefghij" };
    var br = try BufReader.init(std.testing.allocator, sr.reader(), 4);
    defer br.deinit();

    var buf: [16]u8 = undefined;
    // Request > buffer size and buffer empty → direct passthrough.
    try std.testing.expectEqual(@as(usize, 10), try br.reader().read(&buf));
    try std.testing.expectEqualStrings("abcdefghij", buf[0..10]);
}

test "BufReader.readUntil: finds delim across multiple refills" {
    var sr = SliceReader{ .bytes = "alpha\nbeta\ngamma\n" };
    var br = try BufReader.init(std.testing.allocator, sr.reader(), 4);
    defer br.deinit();

    var line: [16]u8 = undefined;
    var n = try br.readUntil('\n', &line);
    try std.testing.expectEqualStrings("alpha", line[0..n]);
    n = try br.readUntil('\n', &line);
    try std.testing.expectEqualStrings("beta", line[0..n]);
    n = try br.readUntil('\n', &line);
    try std.testing.expectEqualStrings("gamma", line[0..n]);
    try std.testing.expectError(error.EndOfStream, br.readUntil('\n', &line));
}

test "BufReader.readUntil: errors StreamTooLong if dst too small" {
    var sr = SliceReader{ .bytes = "this-is-a-very-long-line-with-no-delim" };
    var br = try BufReader.init(std.testing.allocator, sr.reader(), 8);
    defer br.deinit();

    var dst: [4]u8 = undefined;
    try std.testing.expectError(error.StreamTooLong, br.readUntil('\n', &dst));
}

test "BufReader.readUntilAlloc: returns owned slice" {
    var sr = SliceReader{ .bytes = "ok\ndone" };
    var br = try BufReader.init(std.testing.allocator, sr.reader(), 8);
    defer br.deinit();

    const line = try br.readUntilAlloc(std.testing.allocator, '\n', 64);
    defer std.testing.allocator.free(line);
    try std.testing.expectEqualStrings("ok", line);
}

test "BufReader.peek: refills + returns view, doesn't consume" {
    var sr = SliceReader{ .bytes = "hello world" };
    var br = try BufReader.init(std.testing.allocator, sr.reader(), 16);
    defer br.deinit();

    // peek returns AT LEAST n bytes (may overshoot if more was buffered);
    // assert the prefix.
    const view = try br.peek(5);
    try std.testing.expect(view.len >= 5);
    try std.testing.expectEqualStrings("hello", view[0..5]);

    // Peeking doesn't consume — a subsequent read still sees "hello…".
    var dst: [5]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 5), try br.reader().read(&dst));
    try std.testing.expectEqualStrings("hello", &dst);
}
