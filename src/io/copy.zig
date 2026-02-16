//! Copy Utility
//!
//! Efficiently copies data from a reader to a writer.
//!
//! ## Usage
//!
//! ```zig
//! const io = @import("volt").io;
//!
//! // Copy all data from source to destination
//! const bytes_copied = try io.copy(source_reader, dest_writer);
//!
//! // Copy with custom buffer size
//! const bytes = try io.copyBuf(source_reader, dest_writer, &buffer);
//! ```

const std = @import("std");

/// Default buffer size for copy operations.
pub const DEFAULT_BUF_SIZE: usize = 8192;

/// Copy all data from reader to writer.
/// Returns the total number of bytes copied.
pub fn copy(reader: anytype, writer: anytype) !u64 {
    var buf: [DEFAULT_BUF_SIZE]u8 = undefined;
    return copyBuf(reader, writer, &buf);
}

/// Copy all data from reader to writer using a provided buffer.
/// Returns the total number of bytes copied.
pub fn copyBuf(reader: anytype, writer: anytype, buf: []u8) !u64 {
    var total: u64 = 0;

    while (true) {
        const n = try reader.read(buf);
        if (n == 0) break; // EOF

        try writer.writeAll(buf[0..n]);
        total += n;
    }

    return total;
}

/// Copy exactly `limit` bytes from reader to writer.
/// Returns error if reader has fewer bytes available.
pub fn copyN(reader: anytype, writer: anytype, limit: u64) !u64 {
    var buf: [DEFAULT_BUF_SIZE]u8 = undefined;
    return copyNBuf(reader, writer, limit, &buf);
}

/// Copy exactly `limit` bytes using a provided buffer.
pub fn copyNBuf(reader: anytype, writer: anytype, limit: u64, buf: []u8) !u64 {
    var remaining: u64 = limit;
    var total: u64 = 0;

    while (remaining > 0) {
        const to_read = @min(remaining, buf.len);
        const n = try reader.read(buf[0..to_read]);
        if (n == 0) return error.EndOfStream; // EOF before limit

        try writer.writeAll(buf[0..n]);
        remaining -= n;
        total += n;
    }

    return total;
}

/// Copy data between two bidirectional streams (full duplex copy).
/// Useful for proxying connections.
/// Returns total bytes copied in each direction.
pub const BidirectionalResult = struct {
    a_to_b: u64,
    b_to_a: u64,
};

/// Non-blocking bidirectional copy attempt.
/// Returns null if either direction would block.
pub fn tryBidirectionalCopy(
    a_reader: anytype,
    a_writer: anytype,
    b_reader: anytype,
    b_writer: anytype,
    buf_a_to_b: []u8,
    buf_b_to_a: []u8,
) !?BidirectionalResult {
    var a_to_b: u64 = 0;
    var b_to_a: u64 = 0;

    // Try to copy A -> B
    while (true) {
        const n = a_reader.tryRead(buf_a_to_b) catch |err| return err;
        if (n) |bytes| {
            if (bytes == 0) break; // EOF from A
            b_writer.tryWriteAll(buf_a_to_b[0..bytes]) catch |err| return err;
            a_to_b += bytes;
        } else {
            break; // Would block
        }
    }

    // Try to copy B -> A
    while (true) {
        const n = b_reader.tryRead(buf_b_to_a) catch |err| return err;
        if (n) |bytes| {
            if (bytes == 0) break; // EOF from B
            a_writer.tryWriteAll(buf_b_to_a[0..bytes]) catch |err| return err;
            b_to_a += bytes;
        } else {
            break; // Would block
        }
    }

    return .{ .a_to_b = a_to_b, .b_to_a = b_to_a };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

const TestReader = struct {
    data: []const u8,
    pos: usize = 0,

    pub fn read(self: *TestReader, dest: []u8) !usize {
        if (self.pos >= self.data.len) return 0;

        const remaining = self.data[self.pos..];
        const to_copy = @min(remaining.len, dest.len);
        @memcpy(dest[0..to_copy], remaining[0..to_copy]);
        self.pos += to_copy;
        return to_copy;
    }
};

const TestWriter = struct {
    data: std.ArrayList(u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) TestWriter {
        return .{ .data = .empty, .allocator = allocator };
    }

    pub fn deinit(self: *TestWriter) void {
        self.data.deinit(self.allocator);
    }

    pub fn write(self: *TestWriter, data_to_write: []const u8) !usize {
        try self.data.appendSlice(self.allocator, data_to_write);
        return data_to_write.len;
    }

    pub fn writeAll(self: *TestWriter, data_to_write: []const u8) !void {
        try self.data.appendSlice(self.allocator, data_to_write);
    }

    pub fn getWritten(self: *const TestWriter) []const u8 {
        return self.data.items;
    }
};

test "copy - basic" {
    var reader = TestReader{ .data = "Hello, World!" };
    var writer = TestWriter.init(std.testing.allocator);
    defer writer.deinit();

    const copied = try copy(&reader, &writer);

    try std.testing.expectEqual(@as(u64, 13), copied);
    try std.testing.expectEqualStrings("Hello, World!", writer.getWritten());
}

test "copy - empty source" {
    var reader = TestReader{ .data = "" };
    var writer = TestWriter.init(std.testing.allocator);
    defer writer.deinit();

    const copied = try copy(&reader, &writer);

    try std.testing.expectEqual(@as(u64, 0), copied);
    try std.testing.expectEqualStrings("", writer.getWritten());
}

test "copyN - exact bytes" {
    var reader = TestReader{ .data = "Hello, World!" };
    var writer = TestWriter.init(std.testing.allocator);
    defer writer.deinit();

    const copied = try copyN(&reader, &writer, 5);

    try std.testing.expectEqual(@as(u64, 5), copied);
    try std.testing.expectEqualStrings("Hello", writer.getWritten());
}

test "copyN - not enough bytes" {
    var reader = TestReader{ .data = "Hi" };
    var writer = TestWriter.init(std.testing.allocator);
    defer writer.deinit();

    const result = copyN(&reader, &writer, 10);
    try std.testing.expectError(error.EndOfStream, result);
}

test "copy - large data" {
    // Create larger test data
    var large_data: [20000]u8 = undefined;
    for (&large_data, 0..) |*b, i| {
        b.* = @truncate(i);
    }

    var reader = TestReader{ .data = &large_data };
    var writer = TestWriter.init(std.testing.allocator);
    defer writer.deinit();

    const copied = try copy(&reader, &writer);

    try std.testing.expectEqual(@as(u64, 20000), copied);
    try std.testing.expectEqualSlices(u8, &large_data, writer.getWritten());
}
