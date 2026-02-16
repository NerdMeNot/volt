//! Buffered Writer
//!
//! Wraps a writer with an internal buffer for efficient small writes.
//!
//! ## Usage
//!
//! ```zig
//! const io = @import("volt").io;
//!
//! var file = try std.fs.cwd().createFile("data.txt", .{});
//! var buf_writer = io.BufWriter(std.fs.File).init(file);
//!
//! try buf_writer.writeAll("Hello, ");
//! try buf_writer.writeAll("World!\n");
//! try buf_writer.flush(); // Don't forget to flush!
//! ```

const std = @import("std");

/// Buffered writer that wraps any writer type.
pub fn BufWriter(comptime WriterType: type) type {
    return struct {
        inner: WriterType,
        buf: [DEFAULT_BUF_SIZE]u8 = undefined,
        pos: usize = 0,

        const Self = @This();
        const DEFAULT_BUF_SIZE: usize = 8192;

        /// Initialize with a writer.
        pub fn init(inner: WriterType) Self {
            return .{ .inner = inner };
        }

        /// Get a reference to the underlying writer.
        pub fn writer(self: *Self) *WriterType {
            return &self.inner;
        }

        /// Get the amount of buffered data.
        pub fn bufferedLen(self: *const Self) usize {
            return self.pos;
        }

        /// Get available buffer space.
        pub fn available(self: *const Self) usize {
            return self.buf.len - self.pos;
        }

        /// Flush the internal buffer to the underlying writer.
        pub fn flush(self: *Self) !void {
            if (self.pos > 0) {
                try self.inner.writeAll(self.buf[0..self.pos]);
                self.pos = 0;
            }
        }

        /// Write data to the buffer.
        pub fn write(self: *Self, data: []const u8) !usize {
            // If data is larger than buffer, flush and write directly
            if (data.len >= self.buf.len) {
                try self.flush();
                return self.inner.write(data);
            }

            // If data doesn't fit, flush first
            if (data.len > self.available()) {
                try self.flush();
            }

            // Copy to buffer
            const to_copy = @min(data.len, self.available());
            @memcpy(self.buf[self.pos .. self.pos + to_copy], data[0..to_copy]);
            self.pos += to_copy;
            return to_copy;
        }

        /// Write all data to the buffer (may flush multiple times).
        pub fn writeAll(self: *Self, data: []const u8) !void {
            var offset: usize = 0;
            while (offset < data.len) {
                offset += try self.write(data[offset..]);
            }
        }

        /// Write a single byte.
        pub fn writeByte(self: *Self, byte: u8) !void {
            if (self.pos >= self.buf.len) {
                try self.flush();
            }
            self.buf[self.pos] = byte;
            self.pos += 1;
        }

        /// Write a byte repeated n times.
        pub fn writeByteNTimes(self: *Self, byte: u8, n: usize) !void {
            var remaining = n;
            while (remaining > 0) {
                if (self.pos >= self.buf.len) {
                    try self.flush();
                }

                const space = self.buf.len - self.pos;
                const to_write = @min(space, remaining);
                @memset(self.buf[self.pos .. self.pos + to_write], byte);
                self.pos += to_write;
                remaining -= to_write;
            }
        }

        /// Write formatted output.
        pub fn print(self: *Self, comptime fmt: []const u8, args: anytype) !void {
            // For small formats, try to fit in buffer
            if (self.available() >= 256) {
                const result = std.fmt.bufPrint(self.buf[self.pos..], fmt, args) catch {
                    // Too big for remaining buffer, flush and try again
                    try self.flush();
                    const written = std.fmt.bufPrint(&self.buf, fmt, args) catch |err| {
                        // Still too big, write directly
                        return self.inner.print(fmt, args) catch return err;
                    };
                    self.pos = written.len;
                    return;
                };
                self.pos += result.len;
            } else {
                try self.flush();
                const written = std.fmt.bufPrint(&self.buf, fmt, args) catch |err| {
                    return self.inner.print(fmt, args) catch return err;
                };
                self.pos = written.len;
            }
        }
    };
}

/// Create a buffered writer from any writer.
pub fn bufWriter(writer: anytype) BufWriter(@TypeOf(writer)) {
    return BufWriter(@TypeOf(writer)).init(writer);
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

const TestWriter = struct {
    data: std.ArrayList(u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) TestWriter {
        return .{ .data = .empty, .allocator = allocator };
    }

    pub fn deinit(self: *TestWriter) void {
        self.data.deinit(self.allocator);
    }

    pub fn write(self: *TestWriter, data: []const u8) !usize {
        try self.data.appendSlice(self.allocator, data);
        return data.len;
    }

    pub fn writeAll(self: *TestWriter, data: []const u8) !void {
        try self.data.appendSlice(self.allocator, data);
    }

    pub fn getWritten(self: *const TestWriter) []const u8 {
        return self.data.items;
    }
};

test "BufWriter - basic write" {
    const writer = TestWriter.init(std.testing.allocator);
    var buf_writer = BufWriter(TestWriter).init(writer);
    defer buf_writer.writer().deinit();

    _ = try buf_writer.write("Hello");
    try std.testing.expectEqual(@as(usize, 0), buf_writer.writer().getWritten().len); // Still buffered

    try buf_writer.flush();
    try std.testing.expectEqualStrings("Hello", buf_writer.writer().getWritten());
}

test "BufWriter - writeAll" {
    const writer = TestWriter.init(std.testing.allocator);
    var buf_writer = BufWriter(TestWriter).init(writer);
    defer buf_writer.writer().deinit();

    try buf_writer.writeAll("Hello, ");
    try buf_writer.writeAll("World!");
    try buf_writer.flush();

    try std.testing.expectEqualStrings("Hello, World!", buf_writer.writer().getWritten());
}

test "BufWriter - writeByte" {
    const writer = TestWriter.init(std.testing.allocator);
    var buf_writer = BufWriter(TestWriter).init(writer);
    defer buf_writer.writer().deinit();

    try buf_writer.writeByte('H');
    try buf_writer.writeByte('i');
    try buf_writer.flush();

    try std.testing.expectEqualStrings("Hi", buf_writer.writer().getWritten());
}

test "BufWriter - writeByteNTimes" {
    const writer = TestWriter.init(std.testing.allocator);
    var buf_writer = BufWriter(TestWriter).init(writer);
    defer buf_writer.writer().deinit();

    try buf_writer.writeByteNTimes('-', 5);
    try buf_writer.flush();

    try std.testing.expectEqualStrings("-----", buf_writer.writer().getWritten());
}
