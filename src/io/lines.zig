//! Lines Iterator
//!
//! Iterates over lines in a reader, yielding each line without the newline.
//!
//! ## Usage
//!
//! ```zig
//! const io = @import("volt").io;
//!
//! var file = try std.fs.cwd().openFile("data.txt", .{});
//! var buf_reader = io.BufReader(std.fs.File).init(file);
//! var lines = io.lines(&buf_reader);
//!
//! while (try lines.next()) |line| {
//!     std.debug.print("{s}\n", .{line});
//! }
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Default maximum line length.
pub const DEFAULT_MAX_LINE_LEN: usize = 65536;

/// Iterator over lines in a reader.
/// Each line is returned without trailing newline characters.
pub fn Lines(comptime ReaderType: type) type {
    return struct {
        reader: *ReaderType,
        buf: []u8,
        allocator: ?Allocator,
        owned_buf: bool,

        const Self = @This();

        /// Initialize with a reader and allocator for dynamic line buffer.
        pub fn init(reader: *ReaderType, allocator: Allocator) Self {
            return .{
                .reader = reader,
                .buf = &[_]u8{},
                .allocator = allocator,
                .owned_buf = true,
            };
        }

        /// Initialize with a reader and fixed buffer.
        pub fn initWithBuffer(reader: *ReaderType, buf: []u8) Self {
            return .{
                .reader = reader,
                .buf = buf,
                .allocator = null,
                .owned_buf = false,
            };
        }

        /// Get the next line, or null on EOF.
        /// Returns line without trailing newline.
        pub fn next(self: *Self) !?[]const u8 {
            if (self.allocator) |alloc| {
                return self.nextAlloc(alloc);
            } else {
                return self.nextFixed();
            }
        }

        /// Get next line with fixed buffer.
        fn nextFixed(self: *Self) !?[]const u8 {
            const line = try self.reader.readLine(self.buf);
            if (line) |l| {
                return stripNewline(l);
            }
            return null;
        }

        /// Get next line with dynamic allocation.
        fn nextAlloc(self: *Self, alloc: Allocator) !?[]const u8 {
            // Free previous line buffer if we own it
            if (self.owned_buf and self.buf.len > 0) {
                alloc.free(self.buf);
                self.buf = &[_]u8{};
            }

            var result: std.ArrayList(u8) = .empty;
            errdefer result.deinit(alloc);

            var tmp_buf: [4096]u8 = undefined;

            while (true) {
                const n = try self.reader.read(&tmp_buf);
                if (n == 0) {
                    // EOF
                    if (result.items.len > 0) {
                        self.buf = try result.toOwnedSlice(alloc);
                        return stripNewline(self.buf);
                    }
                    return null;
                }

                // Check for newline in chunk
                if (std.mem.indexOfScalar(u8, tmp_buf[0..n], '\n')) |idx| {
                    try result.appendSlice(alloc, tmp_buf[0 .. idx + 1]);
                    self.buf = try result.toOwnedSlice(alloc);
                    return stripNewline(self.buf);
                }

                try result.appendSlice(alloc, tmp_buf[0..n]);

                // Safety limit
                if (result.items.len > DEFAULT_MAX_LINE_LEN) {
                    return error.LineTooLong;
                }
            }
        }

        pub fn deinit(self: *Self) void {
            if (self.owned_buf and self.allocator != null and self.buf.len > 0) {
                self.allocator.?.free(self.buf);
                self.buf = &[_]u8{};
            }
        }
    };
}

/// Create a lines iterator from any reader with an allocator.
pub fn lines(reader: anytype, allocator: Allocator) Lines(@TypeOf(reader.*)) {
    return Lines(@TypeOf(reader.*)).init(reader, allocator);
}

/// Create a lines iterator with a fixed buffer.
pub fn linesWithBuffer(reader: anytype, buf: []u8) Lines(@TypeOf(reader.*)) {
    return Lines(@TypeOf(reader.*)).initWithBuffer(reader, buf);
}

/// Strip trailing newline characters from a line.
pub fn stripNewline(line: []const u8) []const u8 {
    var end = line.len;
    while (end > 0 and (line[end - 1] == '\n' or line[end - 1] == '\r')) {
        end -= 1;
    }
    return line[0..end];
}

// ─────────────────────────────────────────────────────────────────────────────
// Split Iterator
// ─────────────────────────────────────────────────────────────────────────────

/// Iterator that splits reader content by a delimiter.
pub fn Split(comptime ReaderType: type) type {
    return struct {
        reader: *ReaderType,
        delimiter: u8,
        buf: []u8,
        allocator: ?Allocator,
        owned_buf: bool,

        const Self = @This();

        /// Initialize with a reader and allocator.
        pub fn init(reader: *ReaderType, delimiter: u8, allocator: Allocator) Self {
            return .{
                .reader = reader,
                .delimiter = delimiter,
                .buf = &[_]u8{},
                .allocator = allocator,
                .owned_buf = true,
            };
        }

        /// Initialize with a reader and fixed buffer.
        pub fn initWithBuffer(reader: *ReaderType, delimiter: u8, buf: []u8) Self {
            return .{
                .reader = reader,
                .delimiter = delimiter,
                .buf = buf,
                .allocator = null,
                .owned_buf = false,
            };
        }

        /// Get the next segment, or null on EOF.
        pub fn next(self: *Self) !?[]const u8 {
            if (self.allocator) |alloc| {
                return self.nextAlloc(alloc);
            } else {
                return self.nextFixed();
            }
        }

        fn nextFixed(self: *Self) !?[]const u8 {
            return self.reader.readUntilDelimiter(self.buf, self.delimiter);
        }

        fn nextAlloc(self: *Self, alloc: Allocator) !?[]const u8 {
            if (self.owned_buf and self.buf.len > 0) {
                alloc.free(self.buf);
                self.buf = &[_]u8{};
            }

            var result: std.ArrayList(u8) = .empty;
            errdefer result.deinit(alloc);

            var tmp_buf: [4096]u8 = undefined;

            while (true) {
                const n = try self.reader.read(&tmp_buf);
                if (n == 0) {
                    if (result.items.len > 0) {
                        self.buf = try result.toOwnedSlice(alloc);
                        return self.buf;
                    }
                    return null;
                }

                if (std.mem.indexOfScalar(u8, tmp_buf[0..n], self.delimiter)) |idx| {
                    try result.appendSlice(alloc, tmp_buf[0..idx]);
                    self.buf = try result.toOwnedSlice(alloc);
                    return self.buf;
                }

                try result.appendSlice(alloc, tmp_buf[0..n]);

                if (result.items.len > DEFAULT_MAX_LINE_LEN) {
                    return error.SegmentTooLong;
                }
            }
        }

        pub fn deinit(self: *Self) void {
            if (self.owned_buf and self.allocator != null and self.buf.len > 0) {
                self.allocator.?.free(self.buf);
                self.buf = &[_]u8{};
            }
        }
    };
}

/// Create a split iterator from any reader.
pub fn split(reader: anytype, delimiter: u8, allocator: Allocator) Split(@TypeOf(reader.*)) {
    return Split(@TypeOf(reader.*)).init(reader, delimiter, allocator);
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

const BufReader = @import("buf_reader.zig").BufReader;

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

    pub fn readLine(self: *TestReader, dest: []u8) !?[]u8 {
        var total: usize = 0;
        while (total < dest.len) {
            if (self.pos >= self.data.len) {
                return if (total > 0) dest[0..total] else null;
            }

            dest[total] = self.data[self.pos];
            self.pos += 1;
            total += 1;

            if (dest[total - 1] == '\n') {
                return dest[0..total];
            }
        }
        return dest[0..total];
    }

    pub fn readUntilDelimiter(self: *TestReader, dest: []u8, delimiter: u8) !?[]u8 {
        var total: usize = 0;
        while (total < dest.len) {
            if (self.pos >= self.data.len) {
                return if (total > 0) dest[0..total] else null;
            }

            dest[total] = self.data[self.pos];
            self.pos += 1;
            total += 1;

            if (dest[total - 1] == delimiter) {
                return dest[0 .. total - 1]; // Exclude delimiter
            }
        }
        return dest[0..total];
    }
};

test "Lines - basic iteration" {
    var reader = TestReader{ .data = "line1\nline2\nline3\n" };
    var buf: [100]u8 = undefined;
    var iter = Lines(TestReader).initWithBuffer(&reader, &buf);

    const line1 = try iter.next();
    try std.testing.expectEqualStrings("line1", line1.?);

    const line2 = try iter.next();
    try std.testing.expectEqualStrings("line2", line2.?);

    const line3 = try iter.next();
    try std.testing.expectEqualStrings("line3", line3.?);

    const eof = try iter.next();
    try std.testing.expect(eof == null);
}

test "Lines - no trailing newline" {
    var reader = TestReader{ .data = "line1\nline2" };
    var buf: [100]u8 = undefined;
    var iter = Lines(TestReader).initWithBuffer(&reader, &buf);

    const line1 = try iter.next();
    try std.testing.expectEqualStrings("line1", line1.?);

    const line2 = try iter.next();
    try std.testing.expectEqualStrings("line2", line2.?);

    const eof = try iter.next();
    try std.testing.expect(eof == null);
}

test "Lines - CRLF" {
    var reader = TestReader{ .data = "line1\r\nline2\r\n" };
    var buf: [100]u8 = undefined;
    var iter = Lines(TestReader).initWithBuffer(&reader, &buf);

    const line1 = try iter.next();
    try std.testing.expectEqualStrings("line1", line1.?);

    const line2 = try iter.next();
    try std.testing.expectEqualStrings("line2", line2.?);
}

test "Lines - empty input" {
    var reader = TestReader{ .data = "" };
    var buf: [100]u8 = undefined;
    var iter = Lines(TestReader).initWithBuffer(&reader, &buf);

    const eof = try iter.next();
    try std.testing.expect(eof == null);
}

test "stripNewline" {
    try std.testing.expectEqualStrings("hello", stripNewline("hello\n"));
    try std.testing.expectEqualStrings("hello", stripNewline("hello\r\n"));
    try std.testing.expectEqualStrings("hello", stripNewline("hello\r"));
    try std.testing.expectEqualStrings("hello", stripNewline("hello"));
    try std.testing.expectEqualStrings("", stripNewline("\n"));
    try std.testing.expectEqualStrings("", stripNewline(""));
}

test "Split - basic" {
    var reader = TestReader{ .data = "a,b,c,d" };
    var buf: [100]u8 = undefined;
    var iter = Split(TestReader).initWithBuffer(&reader, ',', &buf);

    const a = try iter.next();
    try std.testing.expectEqualStrings("a", a.?);

    const b = try iter.next();
    try std.testing.expectEqualStrings("b", b.?);

    const c = try iter.next();
    try std.testing.expectEqualStrings("c", c.?);

    const d = try iter.next();
    try std.testing.expectEqualStrings("d", d.?);

    const eof = try iter.next();
    try std.testing.expect(eof == null);
}
