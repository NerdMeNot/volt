//! `volt.io.Reader` — vtable-based byte stream source.
//!
//! Stackful Volt suspends transparently at I/O sites, so this trait is
//! **blocking-shaped** like Go's `io.Reader` rather than poll-based
//! like Rust's `AsyncRead`. Concrete sources (TcpStream, File, BufReader,
//! Mmap, …) populate the vtable; consumers (`copy`, `readAll`, …)
//! speak only `Reader` and don't care what's behind it.
//!
//! ## EOF
//!
//! `read` returns `0` on EOF (POSIX stream semantics). Subsequent reads
//! after EOF MAY succeed — pipes can get a new writer, sockets can be
//! re-armed, etc. Helpers that demand a specific byte count
//! (`readAll`, `readByte`) convert EOF to `error.EndOfStream`.
//!
//! ## as_fd marker
//!
//! `vtable.as_fd` is the optimisation hook `volt.io.copy` reaches for
//! to dispatch to `sendfile` / `splice` / `copy_file_range` when both
//! ends are real fds. Sources that **transform** bytes (TLS,
//! compression, framing) MUST leave `as_fd = null` even if they have
//! an underlying fd — otherwise `copy` will skip the transform and
//! pipe the raw bytes through the kernel.

const std = @import("std");
const posix = std.posix;
const io_errors = @import("../errors.zig");

pub const ReadError = io_errors.ReadError;

pub const Reader = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Read up to `buf.len` bytes. Returns 0 only on EOF.
        read: *const fn (ctx: *anyopaque, buf: []u8) ReadError!usize,

        /// If the source is a real fd, return it; otherwise null. Used
        /// by `volt.io.copy` for sendfile/splice specialisation. MUST
        /// be null for transforming readers (TLS, gzip, etc.).
        as_fd: ?*const fn (ctx: *anyopaque) ?posix.fd_t = null,

        /// If the source is a contiguous in-memory byte slice (Mmap,
        /// in-RAM buffer), return it. Used by `volt.io.copy` to skip
        /// the read-loop entirely and write the slice directly.
        as_byte_slice: ?*const fn (ctx: *anyopaque) ?[]const u8 = null,

        /// Optional vectored read. `Reader.readv` falls back to
        /// looping `read` if null.
        readv: ?*const fn (ctx: *anyopaque, iovs: []posix.iovec) ReadError!usize = null,
    };

    /// Read up to `buf.len` bytes. Returns 0 on EOF.
    pub fn read(self: Reader, buf: []u8) ReadError!usize {
        return self.vtable.read(self.ctx, buf);
    }

    /// Read exactly `buf.len` bytes. Returns `error.EndOfStream` if EOF
    /// arrives before the buffer is full.
    pub fn readAll(self: Reader, buf: []u8) ReadError!void {
        var i: usize = 0;
        while (i < buf.len) {
            const n = try self.read(buf[i..]);
            if (n == 0) return error.EndOfStream;
            i += n;
        }
    }

    /// Read a single byte. Returns `error.EndOfStream` on EOF.
    pub fn readByte(self: Reader) ReadError!u8 {
        var b: [1]u8 = undefined;
        try self.readAll(&b);
        return b[0];
    }

    /// Read into `iovs`. Uses the vtable's `readv` if present; otherwise
    /// falls back to a single `read` over the first iovec (Go's
    /// behaviour). Returns the bytes copied across all populated iovecs.
    pub fn readv(self: Reader, iovs: []posix.iovec) ReadError!usize {
        if (self.vtable.readv) |fn_ptr| return fn_ptr(self.ctx, iovs);
        if (iovs.len == 0) return 0;
        const first = iovs[0];
        return self.read(first.base[0..first.len]);
    }

    /// Discard up to `n` bytes from the source. Returns the number
    /// actually discarded — short returns mean EOF was reached first.
    pub fn discard(self: Reader, n: u64) ReadError!u64 {
        var scratch: [4096]u8 = undefined;
        var remaining: u64 = n;
        while (remaining > 0) {
            const want: usize = if (remaining < scratch.len) @intCast(remaining) else scratch.len;
            const got = try self.read(scratch[0..want]);
            if (got == 0) return n - remaining;
            remaining -= got;
        }
        return n;
    }

    /// True if the source has an `as_fd` shortcut. `copy()` checks this
    /// to decide whether to attempt sendfile/splice.
    pub fn hasFd(self: Reader) bool {
        if (self.vtable.as_fd) |fn_ptr| {
            return fn_ptr(self.ctx) != null;
        }
        return false;
    }

    /// True if the source is backed by a contiguous byte slice.
    pub fn hasByteSlice(self: Reader) bool {
        if (self.vtable.as_byte_slice) |fn_ptr| {
            return fn_ptr(self.ctx) != null;
        }
        return false;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

/// In-memory test fixture. Used by adapter tests and the trait tests
/// here. Kept inside the file so every Reader-using test gets a
/// trivial source without spinning up a runtime.
pub const SliceReader = struct {
    bytes: []const u8,
    pos: usize = 0,

    pub fn reader(self: *SliceReader) Reader {
        return .{
            .ctx = @ptrCast(self),
            .vtable = &.{ .read = &slice_read, .as_byte_slice = &slice_as_bytes },
        };
    }

    fn slice_read(ctx: *anyopaque, buf: []u8) ReadError!usize {
        const self: *SliceReader = @ptrCast(@alignCast(ctx));
        const remaining = self.bytes.len - self.pos;
        if (remaining == 0) return 0;
        const n = @min(buf.len, remaining);
        @memcpy(buf[0..n], self.bytes[self.pos..][0..n]);
        self.pos += n;
        return n;
    }

    fn slice_as_bytes(ctx: *anyopaque) ?[]const u8 {
        const self: *SliceReader = @ptrCast(@alignCast(ctx));
        return self.bytes[self.pos..];
    }
};

test "Reader: read returns 0 on EOF" {
    var sr = SliceReader{ .bytes = "hello" };
    var buf: [16]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 5), try sr.reader().read(&buf));
    try std.testing.expectEqual(@as(usize, 0), try sr.reader().read(&buf));
}

test "Reader.readAll: errors EndOfStream on short" {
    var sr = SliceReader{ .bytes = "hi" };
    var buf: [4]u8 = undefined;
    try std.testing.expectError(error.EndOfStream, sr.reader().readAll(&buf));
}

test "Reader.readByte: round-trip + EOF" {
    var sr = SliceReader{ .bytes = "ab" };
    const r = sr.reader();
    try std.testing.expectEqual(@as(u8, 'a'), try r.readByte());
    try std.testing.expectEqual(@as(u8, 'b'), try r.readByte());
    try std.testing.expectError(error.EndOfStream, r.readByte());
}

test "Reader.discard: stops at EOF" {
    var sr = SliceReader{ .bytes = "0123456789" };
    try std.testing.expectEqual(@as(u64, 4), try sr.reader().discard(4));
    try std.testing.expectEqual(@as(u64, 6), try sr.reader().discard(100));
}

test "Reader.hasByteSlice: SliceReader exposes its backing" {
    var sr = SliceReader{ .bytes = "data" };
    try std.testing.expect(sr.reader().hasByteSlice());
    try std.testing.expect(!sr.reader().hasFd());
}
