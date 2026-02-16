//! Buffered Reader
//!
//! Wraps a reader with an internal buffer for efficient small reads.
//!
//! ## Usage
//!
//! ```zig
//! const io = @import("volt").io;
//!
//! var file = try std.fs.cwd().openFile("data.txt", .{});
//! var buf_reader = io.BufReader(std.fs.File).init(file);
//!
//! // Read line by line
//! while (try buf_reader.readLine()) |line| {
//!     std.debug.print("{s}\n", .{line});
//! }
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Buffered reader that wraps any reader type.
pub fn BufReader(comptime ReaderType: type) type {
    return struct {
        inner: ReaderType,
        buf: [DEFAULT_BUF_SIZE]u8 = undefined,
        pos: usize = 0,
        cap: usize = 0,

        const Self = @This();
        const DEFAULT_BUF_SIZE: usize = 8192;

        /// Initialize with a reader.
        pub fn init(inner: ReaderType) Self {
            return .{ .inner = inner };
        }

        /// Get a reference to the underlying reader.
        pub fn reader(self: *Self) *ReaderType {
            return &self.inner;
        }

        /// Get the buffered data that hasn't been read yet.
        pub fn buffered(self: *const Self) []const u8 {
            return self.buf[self.pos..self.cap];
        }

        /// Check if the buffer is empty.
        pub fn isEmpty(self: *const Self) bool {
            return self.pos >= self.cap;
        }

        /// Discard all buffered data.
        pub fn discard(self: *Self) void {
            self.pos = 0;
            self.cap = 0;
        }

        /// Fill the internal buffer if empty.
        fn fillBuf(self: *Self) !void {
            if (self.pos >= self.cap) {
                self.cap = try self.inner.read(&self.buf);
                self.pos = 0;
            }
        }

        /// Read into a buffer.
        pub fn read(self: *Self, dest: []u8) !usize {
            // First, return any buffered data
            if (self.pos < self.cap) {
                const available = self.cap - self.pos;
                const to_copy = @min(available, dest.len);
                @memcpy(dest[0..to_copy], self.buf[self.pos .. self.pos + to_copy]);
                self.pos += to_copy;
                return to_copy;
            }

            // If dest is larger than our buffer, read directly
            if (dest.len >= self.buf.len) {
                return self.inner.read(dest);
            }

            // Fill buffer and read from it
            try self.fillBuf();
            if (self.cap == 0) return 0; // EOF

            const to_copy = @min(self.cap, dest.len);
            @memcpy(dest[0..to_copy], self.buf[0..to_copy]);
            self.pos = to_copy;
            return to_copy;
        }

        /// Read until a delimiter or EOF.
        pub fn readUntilDelimiter(self: *Self, dest: []u8, delimiter: u8) !?[]u8 {
            var total: usize = 0;

            while (total < dest.len) {
                try self.fillBuf();
                if (self.pos >= self.cap) {
                    // EOF
                    return if (total > 0) dest[0..total] else null;
                }

                // Search for delimiter in buffer
                const remaining = self.buf[self.pos..self.cap];
                if (std.mem.indexOfScalar(u8, remaining, delimiter)) |idx| {
                    const to_copy = @min(idx + 1, dest.len - total);
                    @memcpy(dest[total .. total + to_copy], remaining[0..to_copy]);
                    self.pos += to_copy;
                    total += to_copy;
                    return dest[0..total];
                }

                // Copy all remaining and continue
                const to_copy = @min(remaining.len, dest.len - total);
                @memcpy(dest[total .. total + to_copy], remaining[0..to_copy]);
                self.pos += to_copy;
                total += to_copy;
            }

            return dest[0..total];
        }

        /// Read a line (up to and including '\n').
        pub fn readLine(self: *Self, dest: []u8) !?[]u8 {
            return self.readUntilDelimiter(dest, '\n');
        }

        /// Read all data to end.
        pub fn readToEnd(self: *Self, allocator: Allocator) ![]u8 {
            var result: std.ArrayList(u8) = .empty;
            errdefer result.deinit(allocator);

            // First, append buffered data
            if (self.pos < self.cap) {
                try result.appendSlice(allocator, self.buf[self.pos..self.cap]);
                self.pos = self.cap;
            }

            // Read remaining
            while (true) {
                const n = try self.inner.read(&self.buf);
                if (n == 0) break;
                try result.appendSlice(allocator, self.buf[0..n]);
            }

            return result.toOwnedSlice(allocator);
        }

        /// Peek at buffered data without consuming it.
        pub fn peek(self: *Self, dest: []u8) !usize {
            try self.fillBuf();
            if (self.pos >= self.cap) return 0;

            const available = self.cap - self.pos;
            const to_copy = @min(available, dest.len);
            @memcpy(dest[0..to_copy], self.buf[self.pos .. self.pos + to_copy]);
            return to_copy;
        }

        /// Skip n bytes.
        pub fn skip(self: *Self, n: usize) !usize {
            var remaining = n;

            // Skip buffered data first
            if (self.pos < self.cap) {
                const available = self.cap - self.pos;
                const to_skip = @min(available, remaining);
                self.pos += to_skip;
                remaining -= to_skip;
            }

            // Skip by reading and discarding
            while (remaining > 0) {
                try self.fillBuf();
                if (self.pos >= self.cap) break; // EOF

                const available = self.cap - self.pos;
                const to_skip = @min(available, remaining);
                self.pos += to_skip;
                remaining -= to_skip;
            }

            return n - remaining;
        }
    };
}

/// Create a buffered reader from any reader.
pub fn bufReader(reader: anytype) BufReader(@TypeOf(reader)) {
    return BufReader(@TypeOf(reader)).init(reader);
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

test "BufReader - basic read" {
    const reader = TestReader{ .data = "Hello, World!" };
    var buf_reader = BufReader(TestReader).init(reader);

    var buf: [5]u8 = undefined;
    const n = try buf_reader.read(&buf);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqualStrings("Hello", &buf);
}

test "BufReader - read line" {
    const reader = TestReader{ .data = "line1\nline2\nline3" };
    var buf_reader = BufReader(TestReader).init(reader);

    var buf: [20]u8 = undefined;

    const line1 = try buf_reader.readLine(&buf);
    try std.testing.expectEqualStrings("line1\n", line1.?);

    const line2 = try buf_reader.readLine(&buf);
    try std.testing.expectEqualStrings("line2\n", line2.?);

    const line3 = try buf_reader.readLine(&buf);
    try std.testing.expectEqualStrings("line3", line3.?);

    const eof = try buf_reader.readLine(&buf);
    try std.testing.expect(eof == null);
}

test "BufReader - readToEnd" {
    const reader = TestReader{ .data = "Hello, World! This is a test." };
    var buf_reader = BufReader(TestReader).init(reader);

    const data = try buf_reader.readToEnd(std.testing.allocator);
    defer std.testing.allocator.free(data);

    try std.testing.expectEqualStrings("Hello, World! This is a test.", data);
}

test "BufReader - skip" {
    const reader = TestReader{ .data = "Hello, World!" };
    var buf_reader = BufReader(TestReader).init(reader);

    const skipped = try buf_reader.skip(7);
    try std.testing.expectEqual(@as(usize, 7), skipped);

    var buf: [10]u8 = undefined;
    const n = try buf_reader.read(&buf);
    try std.testing.expectEqualStrings("World!", buf[0..n]);
}
