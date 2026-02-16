//! I/O Interfaces - Reader and Writer Traits
//!
//! Provides composable I/O interfaces that enable layered stream processing:
//! - TLS encryption/decryption
//! - Compression
//! - Buffering
//! - Logging/tracing
//!
//! ## Design
//!
//! Unlike Rust's trait system, Zig uses explicit function pointers for polymorphism.
//! This gives the same composability without runtime overhead when not needed.
//!
//! ## Example: TLS Wrapping
//!
//! ```zig
//! // Connect TCP
//! var tcp = try TcpStream.connect(addr);
//! defer tcp.close();
//!
//! // Wrap with TLS (hypothetical)
//! var tls = try TlsStream.init(tcp.reader(), tcp.writer());
//! defer tls.close();
//!
//! // Use TLS stream - same interface as TCP
//! try tls.writer().writeAll("GET / HTTP/1.1\r\n\r\n");
//! const n = try tls.reader().read(&buf);
//! ```
//!
//! ## Example: Buffered I/O
//!
//! ```zig
//! var tcp = try TcpStream.connect(addr);
//! var buf_reader = BufReader(@TypeOf(tcp)).init(tcp);
//!
//! // Read line by line
//! var line_buf: [1024]u8 = undefined;
//! while (try buf_reader.readLine(&line_buf)) |line| {
//!     process(line);
//! }
//! ```
//!
//! ## Example: Copy Between Streams
//!
//! ```zig
//! const io = @import("volt").io;
//!
//! // Copy all data from source to destination
//! const bytes_copied = try io.copy(source, dest);
//! ```

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════════
// I/O Utilities
// ═══════════════════════════════════════════════════════════════════════════════

pub const util = @import("io/util.zig");

// Buffered I/O
pub const BufReader = util.BufReader;
pub const bufReader = util.bufReader;
pub const BufWriter = util.BufWriter;
pub const bufWriter = util.bufWriter;

// Copy utilities
pub const copy = util.copy;
pub const copyBuf = util.copyBuf;
pub const copyN = util.copyN;
pub const copyNBuf = util.copyNBuf;

// Line iteration
pub const Lines = util.Lines;
pub const lines = util.lines;
pub const linesWithBuffer = util.linesWithBuffer;
pub const Split = util.Split;
pub const split = util.split;

// ═══════════════════════════════════════════════════════════════════════════════
// Error Types
// ═══════════════════════════════════════════════════════════════════════════════

/// Common I/O errors across all stream types.
pub const Error = error{
    /// Operation would block (non-blocking I/O).
    WouldBlock,
    /// Connection was reset by peer.
    ConnectionReset,
    /// Connection was refused.
    ConnectionRefused,
    /// Connection timed out.
    TimedOut,
    /// Broken pipe - peer closed connection.
    BrokenPipe,
    /// End of stream reached.
    EndOfStream,
    /// Network is unreachable.
    NetworkUnreachable,
    /// Host is unreachable.
    HostUnreachable,
    /// Invalid argument.
    InvalidArgument,
    /// Operation not supported.
    NotSupported,
    /// Out of memory.
    OutOfMemory,
    /// Generic I/O error.
    IoError,
    /// Unexpected error.
    Unexpected,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Reader Interface
// ═══════════════════════════════════════════════════════════════════════════════

/// A polymorphic reader interface.
///
/// Enables reading from any source: sockets, files, TLS streams, buffers, etc.
/// Wrappers can add functionality (decryption, decompression) transparently.
///
/// Thread Safety: NOT thread-safe. Use external synchronization if shared between threads.
pub const Reader = struct {
    /// Opaque pointer to the underlying implementation.
    context: *anyopaque,

    /// Read function pointer.
    readFn: *const fn (context: *anyopaque, buffer: []u8) Error!usize,

    /// Read data into buffer.
    /// Returns number of bytes read, 0 on EOF.
    pub fn read(self: Reader, buffer: []u8) Error!usize {
        return self.readFn(self.context, buffer);
    }

    /// Read exactly `buffer.len` bytes or return error.
    pub fn readAll(self: Reader, buffer: []u8) Error!usize {
        var index: usize = 0;
        while (index < buffer.len) {
            const n = try self.read(buffer[index..]);
            if (n == 0) return error.EndOfStream;
            index += n;
        }
        return index;
    }

    /// Read until delimiter or buffer full.
    /// Returns slice up to and including delimiter, or null if not found.
    pub fn readUntilDelimiter(self: Reader, buffer: []u8, delimiter: u8) Error!?[]u8 {
        var index: usize = 0;
        while (index < buffer.len) {
            const n = try self.read(buffer[index .. index + 1]);
            if (n == 0) {
                if (index == 0) return null;
                return buffer[0..index];
            }
            index += 1;
            if (buffer[index - 1] == delimiter) {
                return buffer[0..index];
            }
        }
        return buffer[0..index];
    }

    /// Skip exactly n bytes.
    pub fn skipBytes(self: Reader, n: usize) Error!void {
        var buf: [256]u8 = undefined;
        var remaining = n;
        while (remaining > 0) {
            const to_read = @min(remaining, buf.len);
            const read_count = try self.read(buf[0..to_read]);
            if (read_count == 0) return error.EndOfStream;
            remaining -= read_count;
        }
    }

    /// Create a reader that reads at most `limit` bytes.
    pub fn limitedReader(self: Reader, limit: usize) LimitedReader {
        return LimitedReader.init(self, limit);
    }
};

/// A reader that limits the number of bytes that can be read.
pub const LimitedReader = struct {
    inner: Reader,
    remaining: usize,

    pub fn init(inner: Reader, limit: usize) LimitedReader {
        return .{ .inner = inner, .remaining = limit };
    }

    pub fn read(self: *LimitedReader, buffer: []u8) Error!usize {
        if (self.remaining == 0) return 0;
        const to_read = @min(buffer.len, self.remaining);
        const n = try self.inner.read(buffer[0..to_read]);
        self.remaining -= n;
        return n;
    }

    pub fn reader(self: *LimitedReader) Reader {
        return .{
            .context = @ptrCast(self),
            .readFn = @ptrCast(&read),
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Writer Interface
// ═══════════════════════════════════════════════════════════════════════════════

/// A polymorphic writer interface.
///
/// Enables writing to any destination: sockets, files, TLS streams, buffers, etc.
/// Wrappers can add functionality (encryption, compression) transparently.
///
/// Thread Safety: NOT thread-safe. Use external synchronization if shared between threads.
pub const Writer = struct {
    /// Opaque pointer to the underlying implementation.
    context: *anyopaque,

    /// Write function pointer.
    writeFn: *const fn (context: *anyopaque, data: []const u8) Error!usize,

    /// Flush function pointer (optional, may be no-op).
    flushFn: ?*const fn (context: *anyopaque) Error!void = null,

    /// Write data to the stream.
    /// Returns number of bytes written.
    pub fn write(self: Writer, data: []const u8) Error!usize {
        return self.writeFn(self.context, data);
    }

    /// Write all data or return error.
    pub fn writeAll(self: Writer, data: []const u8) Error!void {
        var index: usize = 0;
        while (index < data.len) {
            const n = try self.write(data[index..]);
            if (n == 0) return error.BrokenPipe;
            index += n;
        }
    }

    /// Flush any buffered data.
    pub fn flush(self: Writer) Error!void {
        if (self.flushFn) |f| {
            return f(self.context);
        }
    }

    /// Write a single byte.
    pub fn writeByte(self: Writer, byte: u8) Error!void {
        const buf = [_]u8{byte};
        try self.writeAll(&buf);
    }

    /// Write bytes repeated n times.
    pub fn writeByteNTimes(self: Writer, byte: u8, n: usize) Error!void {
        var buf: [256]u8 = undefined;
        @memset(&buf, byte);

        var remaining = n;
        while (remaining > 0) {
            const to_write = @min(remaining, buf.len);
            try self.writeAll(buf[0..to_write]);
            remaining -= to_write;
        }
    }

    /// Print formatted output.
    ///
    /// Note: Uses a fixed 4096-byte internal buffer. If formatted output exceeds
    /// this limit, returns `error.InvalidArgument`. For larger formatted output,
    /// use `std.fmt.bufPrint` with a custom buffer and then `writeAll`.
    pub fn print(self: Writer, comptime fmt: []const u8, args: anytype) Error!void {
        var buf: [4096]u8 = undefined;
        const slice = std.fmt.bufPrint(&buf, fmt, args) catch return error.InvalidArgument;
        try self.writeAll(slice);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// ReadWriter Interface
// ═══════════════════════════════════════════════════════════════════════════════

/// Combined reader and writer for bidirectional streams.
pub const ReadWriter = struct {
    reader_ctx: *anyopaque,
    writer_ctx: *anyopaque,
    readFn: *const fn (context: *anyopaque, buffer: []u8) Error!usize,
    writeFn: *const fn (context: *anyopaque, data: []const u8) Error!usize,
    flushFn: ?*const fn (context: *anyopaque) Error!void = null,

    pub fn reader(self: ReadWriter) Reader {
        return .{
            .context = self.reader_ctx,
            .readFn = self.readFn,
        };
    }

    pub fn writer(self: ReadWriter) Writer {
        return .{
            .context = self.writer_ctx,
            .writeFn = self.writeFn,
            .flushFn = self.flushFn,
        };
    }

    /// Create from separate reader and writer.
    pub fn from(r: Reader, w: Writer) ReadWriter {
        return .{
            .reader_ctx = r.context,
            .writer_ctx = w.context,
            .readFn = r.readFn,
            .writeFn = w.writeFn,
            .flushFn = w.flushFn,
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Utility: Null Writer (discards all data)
// ═══════════════════════════════════════════════════════════════════════════════

/// A writer that discards all data (like /dev/null).
pub const NullWriter = struct {
    pub fn write(_: *anyopaque, data: []const u8) Error!usize {
        return data.len;
    }

    pub fn writer() Writer {
        return .{
            .context = undefined,
            .writeFn = @ptrCast(&write),
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Utility: Fixed Buffer Reader/Writer
// ═══════════════════════════════════════════════════════════════════════════════

/// A reader backed by a fixed buffer.
///
/// Thread Safety: NOT thread-safe. Use external synchronization if shared between threads.
pub const FixedBufferReader = struct {
    buffer: []const u8,
    pos: usize = 0,

    pub fn init(buffer: []const u8) FixedBufferReader {
        return .{ .buffer = buffer };
    }

    pub fn read(ctx: *anyopaque, dest: []u8) Error!usize {
        const self: *FixedBufferReader = @ptrCast(@alignCast(ctx));
        const remaining = self.buffer[self.pos..];
        const n = @min(dest.len, remaining.len);
        if (n == 0) return 0;
        @memcpy(dest[0..n], remaining[0..n]);
        self.pos += n;
        return n;
    }

    pub fn reader(self: *FixedBufferReader) Reader {
        return .{
            .context = @ptrCast(self),
            .readFn = @ptrCast(&read),
        };
    }

    pub fn reset(self: *FixedBufferReader) void {
        self.pos = 0;
    }
};

/// Create a FixedBufferReader from a buffer (convenience function).
pub fn fixedBufferReader(buffer: []const u8) FixedBufferReader {
    return FixedBufferReader.init(buffer);
}

/// A writer backed by a fixed buffer.
///
/// Thread Safety: NOT thread-safe. Use external synchronization if shared between threads.
pub const FixedBufferWriter = struct {
    buffer: []u8,
    pos: usize = 0,

    pub fn init(buffer: []u8) FixedBufferWriter {
        return .{ .buffer = buffer };
    }

    pub fn write(ctx: *anyopaque, data: []const u8) Error!usize {
        const self: *FixedBufferWriter = @ptrCast(@alignCast(ctx));
        const remaining = self.buffer[self.pos..];
        const n = @min(data.len, remaining.len);
        if (n == 0) return 0;
        @memcpy(remaining[0..n], data[0..n]);
        self.pos += n;
        return n;
    }

    pub fn writer(self: *FixedBufferWriter) Writer {
        return .{
            .context = @ptrCast(self),
            .writeFn = @ptrCast(&write),
        };
    }

    pub fn getWritten(self: FixedBufferWriter) []const u8 {
        return self.buffer[0..self.pos];
    }

    pub fn reset(self: *FixedBufferWriter) void {
        self.pos = 0;
    }
};

/// Create a FixedBufferWriter from a buffer (convenience function).
pub fn fixedBufferWriter(buffer: []u8) FixedBufferWriter {
    return FixedBufferWriter.init(buffer);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Utility: Counting Writer
// ═══════════════════════════════════════════════════════════════════════════════

/// A writer that counts bytes and forwards to inner writer.
pub const CountingWriter = struct {
    inner: Writer,
    bytes_written: usize = 0,

    pub fn init(inner: Writer) CountingWriter {
        return .{ .inner = inner };
    }

    pub fn write(ctx: *anyopaque, data: []const u8) Error!usize {
        const self: *CountingWriter = @ptrCast(@alignCast(ctx));
        const n = try self.inner.write(data);
        self.bytes_written += n;
        return n;
    }

    pub fn writer(self: *CountingWriter) Writer {
        return .{
            .context = @ptrCast(self),
            .writeFn = @ptrCast(&write),
            .flushFn = self.inner.flushFn,
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "FixedBufferReader" {
    const data = "hello world";
    var fbr = FixedBufferReader.init(data);
    var r = fbr.reader();

    var buf: [5]u8 = undefined;
    const n1 = try r.read(&buf);
    try std.testing.expectEqual(@as(usize, 5), n1);
    try std.testing.expectEqualStrings("hello", &buf);

    const n2 = try r.read(&buf);
    try std.testing.expectEqual(@as(usize, 5), n2);
    try std.testing.expectEqualStrings(" worl", &buf);

    const n3 = try r.read(&buf);
    try std.testing.expectEqual(@as(usize, 1), n3);
    try std.testing.expectEqualStrings("d", buf[0..1]);

    const n4 = try r.read(&buf);
    try std.testing.expectEqual(@as(usize, 0), n4);
}

test "FixedBufferWriter" {
    var buf: [20]u8 = undefined;
    var fbw = FixedBufferWriter.init(&buf);
    var w = fbw.writer();

    try w.writeAll("hello");
    try w.writeAll(" world");

    try std.testing.expectEqualStrings("hello world", fbw.getWritten());
}

test "CountingWriter" {
    var buf: [100]u8 = undefined;
    var fbw = FixedBufferWriter.init(&buf);
    var cw = CountingWriter.init(fbw.writer());
    var w = cw.writer();

    try w.writeAll("hello");
    try w.writeAll(" world");

    try std.testing.expectEqual(@as(usize, 11), cw.bytes_written);
}

test "Reader.readAll" {
    const data = "hello";
    var fbr = FixedBufferReader.init(data);
    var r = fbr.reader();

    var buf: [5]u8 = undefined;
    const n = try r.readAll(&buf);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqualStrings("hello", &buf);
}

test "Writer.print" {
    var buf: [100]u8 = undefined;
    var fbw = FixedBufferWriter.init(&buf);
    var w = fbw.writer();

    try w.print("count: {}, name: {s}", .{ 42, "test" });

    try std.testing.expectEqualStrings("count: 42, name: test", fbw.getWritten());
}

test "Writer.print - exceeds buffer limit returns error" {
    var buf: [100]u8 = undefined;
    var fbw = FixedBufferWriter.init(&buf);
    var w = fbw.writer();

    // Create a format that would exceed 4096 bytes
    // Format "{s}" with 5000-char string will exceed the 4096 internal buffer
    var large_input: [5000]u8 = undefined;
    @memset(&large_input, 'x');

    const result = w.print("{s}", .{&large_input});
    try std.testing.expectError(error.InvalidArgument, result);
}

test "Reader.readUntilDelimiter" {
    const data = "hello\nworld";
    var fbr = FixedBufferReader.init(data);
    var r = fbr.reader();

    var buf: [20]u8 = undefined;
    const line = try r.readUntilDelimiter(&buf, '\n');
    try std.testing.expectEqualStrings("hello\n", line.?);
}

test "Reader.readUntilDelimiter - delimiter not found" {
    const data = "hello";
    var fbr = FixedBufferReader.init(data);
    var r = fbr.reader();

    var buf: [20]u8 = undefined;
    const line = try r.readUntilDelimiter(&buf, '\n');
    try std.testing.expectEqualStrings("hello", line.?);
}

test "Reader.readUntilDelimiter - empty" {
    const data = "";
    var fbr = FixedBufferReader.init(data);
    var r = fbr.reader();

    var buf: [20]u8 = undefined;
    const line = try r.readUntilDelimiter(&buf, '\n');
    try std.testing.expect(line == null);
}

test "Reader.skipBytes" {
    const data = "hello world";
    var fbr = FixedBufferReader.init(data);
    var r = fbr.reader();

    try r.skipBytes(6); // Skip "hello "

    var buf: [10]u8 = undefined;
    const n = try r.read(&buf);
    try std.testing.expectEqualStrings("world", buf[0..n]);
}

test "Reader.skipBytes - not enough bytes" {
    const data = "hi";
    var fbr = FixedBufferReader.init(data);
    var r = fbr.reader();

    const result = r.skipBytes(10);
    try std.testing.expectError(error.EndOfStream, result);
}

test "LimitedReader - limits bytes read" {
    const data = "hello world";
    var fbr = FixedBufferReader.init(data);
    var r = fbr.reader();

    var lr = r.limitedReader(5);
    var limited = lr.reader();

    var buf: [20]u8 = undefined;
    const n1 = try limited.read(&buf);
    try std.testing.expectEqual(@as(usize, 5), n1);
    try std.testing.expectEqualStrings("hello", buf[0..n1]);

    // Should return 0 now (limit reached)
    const n2 = try limited.read(&buf);
    try std.testing.expectEqual(@as(usize, 0), n2);
}

test "LimitedReader - partial reads" {
    const data = "hello world";
    var fbr = FixedBufferReader.init(data);
    var r = fbr.reader();

    var lr = r.limitedReader(8);
    var limited = lr.reader();

    // Read in small chunks
    var buf: [3]u8 = undefined;
    var total: usize = 0;
    while (true) {
        const n = try limited.read(&buf);
        if (n == 0) break;
        total += n;
    }
    try std.testing.expectEqual(@as(usize, 8), total);
}

test "NullWriter - discards all data" {
    var w = NullWriter.writer();

    try w.writeAll("this data is discarded");
    try w.writeByte('x');
    try w.writeByteNTimes('y', 100);
    try w.print("formatted: {d}", .{42});

    // No crash, no error - that's the test
}

test "fixedBufferReader convenience function" {
    const data = "test";
    var fbr = fixedBufferReader(data);
    var r = fbr.reader();

    var buf: [10]u8 = undefined;
    const n = try r.read(&buf);
    try std.testing.expectEqualStrings("test", buf[0..n]);
}

test "fixedBufferWriter convenience function" {
    var buf: [20]u8 = undefined;
    var fbw = fixedBufferWriter(&buf);
    var w = fbw.writer();

    try w.writeAll("test");
    try std.testing.expectEqualStrings("test", fbw.getWritten());
}

test "Writer.writeByte" {
    var buf: [10]u8 = undefined;
    var fbw = FixedBufferWriter.init(&buf);
    var w = fbw.writer();

    try w.writeByte('A');
    try w.writeByte('B');
    try w.writeByte('C');

    try std.testing.expectEqualStrings("ABC", fbw.getWritten());
}

test "Writer.writeByteNTimes" {
    var buf: [20]u8 = undefined;
    var fbw = FixedBufferWriter.init(&buf);
    var w = fbw.writer();

    try w.writeByteNTimes('-', 5);
    try w.writeByteNTimes('=', 3);

    try std.testing.expectEqualStrings("-----===", fbw.getWritten());
}

test "Reader.readAll - not enough data" {
    const data = "hi";
    var fbr = FixedBufferReader.init(data);
    var r = fbr.reader();

    var buf: [10]u8 = undefined;
    const result = r.readAll(&buf);
    try std.testing.expectError(error.EndOfStream, result);
}

test "FixedBufferWriter - buffer full" {
    var buf: [5]u8 = undefined;
    var fbw = FixedBufferWriter.init(&buf);
    var w = fbw.writer();

    // Write exactly buffer size
    const n1 = try w.write("hello");
    try std.testing.expectEqual(@as(usize, 5), n1);

    // Write when buffer is full - returns 0
    const n2 = try w.write("more");
    try std.testing.expectEqual(@as(usize, 0), n2);
}

test "FixedBufferReader.reset" {
    const data = "hello";
    var fbr = FixedBufferReader.init(data);

    var buf: [10]u8 = undefined;
    _ = try fbr.reader().read(&buf);

    fbr.reset();

    const n = try fbr.reader().read(&buf);
    try std.testing.expectEqualStrings("hello", buf[0..n]);
}

test "FixedBufferWriter.reset" {
    var buf: [20]u8 = undefined;
    var fbw = FixedBufferWriter.init(&buf);

    try fbw.writer().writeAll("first");
    try std.testing.expectEqualStrings("first", fbw.getWritten());

    fbw.reset();
    try std.testing.expectEqualStrings("", fbw.getWritten());

    try fbw.writer().writeAll("second");
    try std.testing.expectEqualStrings("second", fbw.getWritten());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Timeout Utilities
// ═══════════════════════════════════════════════════════════════════════════════

const time = @import("time.zig");
const timeout_mod = @import("time/timeout.zig");

/// Timeout error returned when deadline expires.
pub const TimeoutError = timeout_mod.TimeoutError;

/// Deadline for tracking operation timeouts.
pub const Deadline = timeout_mod.Deadline;

/// Create a deadline that expires after the given duration.
///
/// ## Example
///
/// ```zig
/// var deadline = io.deadline(Duration.fromMillis(100));
///
/// // Do operation
/// const data = try socket.read(&buf);
///
/// // Check if we timed out
/// try deadline.check();  // Returns error.TimedOut if expired
/// ```
pub const deadline = timeout_mod.deadline;

/// Run a blocking function with a timeout.
///
/// Note: This does not cancel the operation - it just checks if the
/// deadline expired after the operation completes. For true async
/// timeout with cancellation, use the runtime's timeout facilities.
///
/// ## Example
///
/// ```zig
/// // Times out if operation takes more than 100ms
/// const result = try io.withTimeout(u32, Duration.fromMillis(100), myBlockingOp);
/// ```
pub const withTimeout = timeout_mod.withTimeout;

/// Try to run a function within a timeout, returning a result union.
///
/// This is more ergonomic than withTimeout for common patterns where you
/// want to distinguish between timeout, success, and errors.
///
/// ## Example
///
/// ```zig
/// const result = io.tryTimeout(io.Duration.fromSecs(5), doWork, .{arg1});
///
/// switch (result) {
///     .ok => |value| handleSuccess(value),
///     .err => |e| handleError(e),
///     .timeout => log.warn("Timed out", .{}),
/// }
///
/// // Or convert to error union:
/// const value = try result.unwrap();
/// ```
pub const tryTimeout = timeout_mod.tryTimeout;

/// Result type for tryTimeout operations.
pub const TryTimeoutResult = timeout_mod.TryTimeoutResult;

/// Run a function with a deadline callback.
pub const withDeadline = timeout_mod.withDeadline;

/// Create a deadline at a specific instant.
pub const deadlineAt = timeout_mod.deadlineAt;

test {
    _ = util;
}
