//! File - Async File Handle
//!
//! A handle to an open file on the filesystem. Provides read, write, seek,
//! and metadata operations.
//!
//! ## Example
//!
//! ```zig
//! // Read a file
//! var file = try File.open("data.txt");
//! defer file.close();
//!
//! var buf: [1024]u8 = undefined;
//! const n = try file.read(&buf);
//!
//! // Write a file
//! var out = try File.create("output.txt");
//! defer out.close();
//!
//! try out.writeAll("Hello, world!");
//! try out.sync();
//! ```
//!
//! ## Reader/Writer Interfaces
//!
//! Files support the polymorphic Reader/Writer interfaces for composition:
//!
//! ```zig
//! var file = try File.open("data.txt");
//! var r = file.reader();  // io.Reader interface
//!
//! // Can be wrapped with buffering, compression, etc.
//! var buffered = BufferedReader.init(r, &buffer);
//! ```

const std = @import("std");
const posix = std.posix;
const builtin = @import("builtin");

const OpenOptions = @import("open_options.zig").OpenOptions;
const Metadata = @import("metadata.zig").Metadata;
const Permissions = @import("metadata.zig").Permissions;
const io = @import("../stream.zig");

/// A handle to an open file.
pub const File = struct {
    handle: posix.fd_t,

    // ═══════════════════════════════════════════════════════════════════════════
    // Construction
    // ═══════════════════════════════════════════════════════════════════════════

    /// Open a file for reading.
    pub fn open(path: []const u8) !File {
        const std_file = try OpenOptions.new().setRead(true).open(path);
        return .{ .handle = std_file.handle };
    }

    /// Create a file for writing. Truncates if exists, creates if not.
    pub fn create(path: []const u8) !File {
        const std_file = try OpenOptions.new()
            .setWrite(true)
            .setCreate(true)
            .setTruncate(true)
            .open(path);
        return .{ .handle = std_file.handle };
    }

    /// Create a new file, failing if it already exists.
    pub fn createNew(path: []const u8) !File {
        const std_file = try OpenOptions.new()
            .setWrite(true)
            .setRead(true)
            .setCreateNew(true)
            .open(path);
        return .{ .handle = std_file.handle };
    }

    /// Open a file with custom options.
    pub fn openWithOptions(path: []const u8, opts: OpenOptions) !File {
        const std_file = try opts.open(path);
        return .{ .handle = std_file.handle };
    }

    /// Get an OpenOptions builder.
    pub fn getOptions() OpenOptions {
        return OpenOptions.new();
    }

    /// Create from a raw file descriptor (takes ownership).
    pub fn fromRawFd(raw_fd: posix.fd_t) File {
        return .{ .handle = raw_fd };
    }

    /// Create from a std.fs.File (takes ownership).
    pub fn fromStd(std_file: std.fs.File) File {
        return .{ .handle = std_file.handle };
    }

    /// Convert to std.fs.File.
    pub fn toStd(self: File) std.fs.File {
        return .{ .handle = self.handle };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Reading
    // ═══════════════════════════════════════════════════════════════════════════

    /// Read data from the file.
    /// Returns the number of bytes read, or 0 at EOF.
    pub fn read(self: *File, buf: []u8) !usize {
        return posix.read(self.handle, buf);
    }

    /// Read data at a specific offset without changing the file position.
    pub fn pread(self: *File, buf: []u8, offset: u64) !usize {
        return posix.pread(self.handle, buf, offset);
    }

    /// Read exactly `buf.len` bytes or return an error.
    pub fn readAll(self: *File, buf: []u8) !usize {
        var index: usize = 0;
        while (index < buf.len) {
            const n = try self.read(buf[index..]);
            if (n == 0) return error.EndOfStream;
            index += n;
        }
        return index;
    }

    /// Read data into multiple buffers (scatter read).
    /// More efficient than multiple read calls for structured data.
    pub fn readVectored(self: *File, iovecs: []posix.iovec) !usize {
        return posix.readv(self.handle, iovecs);
    }

    /// Read into multiple buffers at a specific offset.
    pub fn readVectoredAt(self: *File, iovecs: []posix.iovec, offset: u64) !usize {
        return posix.preadv(self.handle, iovecs, offset);
    }

    /// Read the entire file into an allocated buffer.
    pub fn readToEnd(self: *File, allocator: std.mem.Allocator) ![]u8 {
        // Get file size for pre-allocation
        const meta = try self.metadata();
        const size = meta.size();

        var buf = try allocator.alloc(u8, size);
        errdefer allocator.free(buf);

        var total: usize = 0;
        while (total < size) {
            const n = try self.read(buf[total..]);
            if (n == 0) break;
            total += n;
        }

        // Resize if we read less (shouldn't happen normally)
        if (total < size) {
            buf = try allocator.realloc(buf, total);
        }

        return buf;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Writing
    // ═══════════════════════════════════════════════════════════════════════════

    /// Write data to the file.
    /// Returns the number of bytes written.
    pub fn write(self: *File, data: []const u8) !usize {
        return posix.write(self.handle, data);
    }

    /// Write data at a specific offset without changing the file position.
    pub fn pwrite(self: *File, data: []const u8, offset: u64) !usize {
        return posix.pwrite(self.handle, data, offset);
    }

    /// Write all data to the file.
    pub fn writeAll(self: *File, data: []const u8) !void {
        var index: usize = 0;
        while (index < data.len) {
            const n = try self.write(data[index..]);
            if (n == 0) return error.WriteZero;
            index += n;
        }
    }

    /// Write data from multiple buffers (gather write).
    /// More efficient than multiple write calls for structured data.
    pub fn writeVectored(self: *File, iovecs: []const posix.iovec_const) !usize {
        return posix.writev(self.handle, iovecs);
    }

    /// Write from multiple buffers at a specific offset.
    pub fn writeVectoredAt(self: *File, iovecs: []const posix.iovec_const, offset: u64) !usize {
        return posix.pwritev(self.handle, iovecs, offset);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Seeking
    // ═══════════════════════════════════════════════════════════════════════════

    /// Seek to a position in the file.
    pub const SeekFrom = enum {
        start,
        current,
        end,
    };

    /// Seek to a position. Returns the new position from the start.
    pub fn seek(self: *File, offset: i64, whence: SeekFrom) !u64 {
        var std_file = self.toStd();
        switch (whence) {
            .start => try std_file.seekTo(@intCast(offset)),
            .current => try std_file.seekBy(offset),
            .end => {
                const end_pos = try std_file.getEndPos();
                const new_pos = @as(i64, @intCast(end_pos)) + offset;
                try std_file.seekTo(@intCast(new_pos));
            },
        }
        return std_file.getPos();
    }

    /// Seek to the start of the file.
    pub fn rewind(self: *File) !void {
        _ = try self.seek(0, .start);
    }

    /// Get the current position in the file.
    pub fn getPos(self: *File) !u64 {
        var std_file = self.toStd();
        return std_file.getPos();
    }

    /// Seek to a specific position from the start.
    /// Convenience wrapper around seek(pos, .start).
    pub fn seekTo(self: *File, pos: u64) !void {
        _ = try self.seek(@intCast(pos), .start);
    }

    /// Seek by a relative offset from current position.
    /// Returns the new position.
    pub fn seekBy(self: *File, offset: i64) !u64 {
        return self.seek(offset, .current);
    }

    /// Get the length of the file.
    pub fn getLen(self: *File) !u64 {
        const meta = try self.metadata();
        return meta.size();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Sync
    // ═══════════════════════════════════════════════════════════════════════════

    /// Sync all OS-internal metadata and data to disk.
    pub fn syncAll(self: *File) !void {
        try posix.fsync(self.handle);
    }

    /// Sync data to disk (may not sync metadata).
    pub fn syncData(self: *File) !void {
        if (comptime builtin.os.tag == .linux) {
            try posix.fdatasync(self.handle);
        } else {
            try posix.fsync(self.handle);
        }
    }

    /// Alias for syncAll.
    pub fn sync(self: *File) !void {
        try self.syncAll();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Metadata
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get metadata about the file.
    pub fn metadata(self: *File) !Metadata {
        const stat = try posix.fstat(self.handle);
        return Metadata.fromStat(stat);
    }

    /// Set the length of the file.
    /// If `size` is less than current, the file is truncated.
    /// If `size` is greater, the file is extended with zeros.
    pub fn setLen(self: *File, size: u64) !void {
        try posix.ftruncate(self.handle, @intCast(size));
    }

    /// Set file permissions.
    pub fn setPermissions(self: *File, perm: Permissions) !void {
        try posix.fchmod(self.handle, @intCast(perm.mode));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Reader/Writer Interfaces
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get a polymorphic Reader interface for this file.
    pub fn reader(self: *File) io.Reader {
        return .{
            .context = @ptrCast(self),
            .readFn = fileReadFn,
        };
    }

    /// Get a polymorphic Writer interface for this file.
    pub fn writer(self: *File) io.Writer {
        return .{
            .context = @ptrCast(self),
            .writeFn = fileWriteFn,
            .flushFn = fileFlushFn,
        };
    }

    fn fileReadFn(ctx: *anyopaque, buffer: []u8) io.Error!usize {
        const self: *File = @ptrCast(@alignCast(ctx));
        const n = posix.read(self.handle, buffer) catch |err| switch (err) {
            error.WouldBlock => return error.WouldBlock,
            error.InputOutput => return error.IoError,
            error.BrokenPipe => return error.BrokenPipe,
            error.ConnectionResetByPeer => return error.ConnectionReset,
            else => return error.Unexpected,
        };
        return n;
    }

    fn fileWriteFn(ctx: *anyopaque, data: []const u8) io.Error!usize {
        const self: *File = @ptrCast(@alignCast(ctx));
        const n = posix.write(self.handle, data) catch |err| switch (err) {
            error.WouldBlock => return error.WouldBlock,
            error.InputOutput => return error.IoError,
            error.BrokenPipe => return error.BrokenPipe,
            error.NoSpaceLeft => return error.IoError,
            error.DiskQuota => return error.IoError,
            else => return error.Unexpected,
        };
        return n;
    }

    fn fileFlushFn(ctx: *anyopaque) io.Error!void {
        const self: *File = @ptrCast(@alignCast(ctx));
        posix.fsync(self.handle) catch |err| switch (err) {
            error.InputOutput => return error.IoError,
            else => return error.Unexpected,
        };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Lifecycle
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get the underlying file descriptor.
    pub fn fd(self: File) posix.fd_t {
        return self.handle;
    }

    /// Close the file.
    pub fn close(self: *File) void {
        posix.close(self.handle);
        self.handle = -1;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "File - create and write" {
    const path = "/tmp/blitz_io_test_file.txt";

    // Create and write
    {
        var file = try File.create(path);
        defer file.close();

        try file.writeAll("Hello, blitz-io!");
        try file.sync();
    }

    // Read back
    {
        var file = try File.open(path);
        defer file.close();

        var buf: [64]u8 = undefined;
        const n = try file.read(&buf);
        try std.testing.expectEqualStrings("Hello, blitz-io!", buf[0..n]);
    }

    // Cleanup
    try std.fs.deleteFileAbsolute(path);
}

test "File - seek" {
    const path = "/tmp/blitz_io_test_seek.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("0123456789");
    }

    {
        var file = try File.open(path);
        defer file.close();

        _ = try file.seek(5, .start);

        var buf: [5]u8 = undefined;
        const n = try file.read(&buf);
        try std.testing.expectEqualStrings("56789", buf[0..n]);
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - metadata" {
    const path = "/tmp/blitz_io_test_meta.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("test content");
    }

    {
        var file = try File.open(path);
        defer file.close();

        const meta = try file.metadata();
        try std.testing.expect(meta.isFile());
        try std.testing.expectEqual(@as(u64, 12), meta.size());
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - reader/writer interface" {
    const path = "/tmp/blitz_io_test_rw.txt";

    // Write using writer interface
    {
        var file = try File.create(path);
        defer file.close();

        var w = file.writer();
        try w.writeAll("via io.Writer");
    }

    // Read using reader interface
    {
        var file = try File.open(path);
        defer file.close();

        var r = file.reader();
        var buf: [64]u8 = undefined;
        const n = try r.read(&buf);
        try std.testing.expectEqualStrings("via io.Writer", buf[0..n]);
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - vectored I/O" {
    const path = "/tmp/blitz_io_test_vec.txt";

    // Write using vectored I/O (gather write)
    {
        var file = try File.create(path);
        defer file.close();

        const header = "HEADER:";
        const body = "body data";
        const footer = ":END";

        const iovecs = [_]posix.iovec_const{
            .{ .base = header.ptr, .len = header.len },
            .{ .base = body.ptr, .len = body.len },
            .{ .base = footer.ptr, .len = footer.len },
        };

        const written = try file.writeVectored(&iovecs);
        try std.testing.expectEqual(header.len + body.len + footer.len, written);
    }

    // Read back and verify
    {
        var file = try File.open(path);
        defer file.close();

        var buf: [64]u8 = undefined;
        const n = try file.read(&buf);
        try std.testing.expectEqualStrings("HEADER:body data:END", buf[0..n]);
    }

    // Read using vectored I/O (scatter read)
    {
        var file = try File.open(path);
        defer file.close();

        var buf1: [7]u8 = undefined; // "HEADER:"
        var buf2: [9]u8 = undefined; // "body data"
        var buf3: [4]u8 = undefined; // ":END"

        var iovecs = [_]posix.iovec{
            .{ .base = &buf1, .len = buf1.len },
            .{ .base = &buf2, .len = buf2.len },
            .{ .base = &buf3, .len = buf3.len },
        };

        const n = try file.readVectored(&iovecs);
        try std.testing.expectEqual(@as(usize, 20), n);
        try std.testing.expectEqualStrings("HEADER:", &buf1);
        try std.testing.expectEqualStrings("body data", &buf2);
        try std.testing.expectEqualStrings(":END", &buf3);
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - setLen truncate" {
    const path = "/tmp/blitz_io_test_trunc.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("Hello, World!");
    }

    // Truncate
    {
        var file = try File.openWithOptions(path, File.getOptions().setWrite(true));
        defer file.close();
        try file.setLen(5);
    }

    // Verify
    {
        var file = try File.open(path);
        defer file.close();
        var buf: [64]u8 = undefined;
        const n = try file.read(&buf);
        try std.testing.expectEqualStrings("Hello", buf[0..n]);
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - file not found error" {
    const result = File.open("/tmp/nonexistent_file_blitz_io_test_12345.txt");
    try std.testing.expectError(error.FileNotFound, result);
}

test "File - read past EOF returns 0" {
    const path = "/tmp/blitz_io_test_eof.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("short");
    }

    {
        var file = try File.open(path);
        defer file.close();

        // First read gets the content
        var buf: [64]u8 = undefined;
        const n1 = try file.read(&buf);
        try std.testing.expectEqual(@as(usize, 5), n1);

        // Second read at EOF returns 0
        const n2 = try file.read(&buf);
        try std.testing.expectEqual(@as(usize, 0), n2);
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - pread and pwrite (positioned I/O)" {
    const path = "/tmp/blitz_io_test_preadwrite.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("0123456789");
    }

    {
        var file = try File.openWithOptions(path, File.getOptions().setRead(true).setWrite(true));
        defer file.close();

        // pwrite at offset 5
        const written = try file.pwrite("XXXXX", 5);
        try std.testing.expectEqual(@as(usize, 5), written);

        // Position should not have changed (still at 0)
        const pos = try file.getPos();
        try std.testing.expectEqual(@as(u64, 0), pos);

        // pread at offset 3
        var buf: [7]u8 = undefined;
        const n = try file.pread(&buf, 3);
        try std.testing.expectEqual(@as(usize, 7), n);
        try std.testing.expectEqualStrings("34XXXXX", &buf);

        // Position still unchanged
        try std.testing.expectEqual(@as(u64, 0), try file.getPos());
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - readToEnd" {
    const path = "/tmp/blitz_io_test_readtoend.txt";
    const content = "This is a longer piece of content for testing readToEnd functionality.";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll(content);
    }

    {
        var file = try File.open(path);
        defer file.close();

        const data = try file.readToEnd(std.testing.allocator);
        defer std.testing.allocator.free(data);

        try std.testing.expectEqualStrings(content, data);
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - position tracking" {
    const path = "/tmp/blitz_io_test_pos.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("0123456789ABCDEF");
    }

    {
        var file = try File.open(path);
        defer file.close();

        // Initial position is 0
        try std.testing.expectEqual(@as(u64, 0), try file.getPos());

        // Read moves position
        var buf: [4]u8 = undefined;
        _ = try file.read(&buf);
        try std.testing.expectEqual(@as(u64, 4), try file.getPos());

        // Seek to position 10
        _ = try file.seek(10, .start);
        try std.testing.expectEqual(@as(u64, 10), try file.getPos());

        // Relative seek
        _ = try file.seek(-3, .current);
        try std.testing.expectEqual(@as(u64, 7), try file.getPos());

        // Seek from end
        _ = try file.seek(-4, .end);
        try std.testing.expectEqual(@as(u64, 12), try file.getPos());

        // Rewind
        try file.rewind();
        try std.testing.expectEqual(@as(u64, 0), try file.getPos());
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - seekTo and seekBy" {
    const path = "/tmp/blitz_io_test_seekto.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("0123456789");
    }

    {
        var file = try File.open(path);
        defer file.close();

        // seekTo absolute position
        try file.seekTo(5);
        try std.testing.expectEqual(@as(u64, 5), try file.getPos());

        // seekBy relative offset (forward)
        const pos1 = try file.seekBy(2);
        try std.testing.expectEqual(@as(u64, 7), pos1);
        try std.testing.expectEqual(@as(u64, 7), try file.getPos());

        // seekBy relative offset (backward)
        const pos2 = try file.seekBy(-3);
        try std.testing.expectEqual(@as(u64, 4), pos2);
        try std.testing.expectEqual(@as(u64, 4), try file.getPos());
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - getLen" {
    const path = "/tmp/blitz_io_test_len.txt";
    const content = "exactly 20 bytes!!!";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll(content);
    }

    {
        var file = try File.open(path);
        defer file.close();

        const len = try file.getLen();
        try std.testing.expectEqual(@as(u64, 19), len);
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - setLen extend" {
    const path = "/tmp/blitz_io_test_extend.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("Hi");
    }

    // Extend
    {
        var file = try File.openWithOptions(path, File.getOptions().setWrite(true));
        defer file.close();
        try file.setLen(10);
    }

    // Verify extended file (should be padded with zeros)
    {
        var file = try File.open(path);
        defer file.close();

        try std.testing.expectEqual(@as(u64, 10), try file.getLen());

        var buf: [10]u8 = undefined;
        _ = try file.read(&buf);
        try std.testing.expectEqualStrings("Hi", buf[0..2]);
        // Rest should be zeros
        try std.testing.expectEqual(@as(u8, 0), buf[2]);
        try std.testing.expectEqual(@as(u8, 0), buf[9]);
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - fromRawFd" {
    const path = "/tmp/blitz_io_test_rawfd.txt";

    // Create file using std library
    const std_file = try std.fs.createFileAbsolute(path, .{});

    // Wrap in our File type
    var file = File.fromRawFd(std_file.handle);
    defer file.close();

    try file.writeAll("from raw fd");
    try file.sync();

    // Read back using our open
    {
        var read_file = try File.open(path);
        defer read_file.close();

        var buf: [64]u8 = undefined;
        const n = try read_file.read(&buf);
        try std.testing.expectEqualStrings("from raw fd", buf[0..n]);
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - createNew fails if exists" {
    const path = "/tmp/blitz_io_test_createnew.txt";

    // Create the file first
    {
        var file = try File.create(path);
        file.close();
    }

    // createNew should fail
    const result = File.createNew(path);
    try std.testing.expectError(error.PathAlreadyExists, result);

    try std.fs.deleteFileAbsolute(path);
}

test "File - syncData" {
    const path = "/tmp/blitz_io_test_syncdata.txt";

    var file = try File.create(path);
    defer file.close();

    try file.writeAll("data to sync");
    try file.syncData(); // Should not error

    try std.fs.deleteFileAbsolute(path);
}

test "File - readAll error on short file" {
    const path = "/tmp/blitz_io_test_readall.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("short");
    }

    {
        var file = try File.open(path);
        defer file.close();

        // Try to read more than file contains
        var buf: [100]u8 = undefined;
        const result = file.readAll(&buf);
        try std.testing.expectError(error.EndOfStream, result);
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - empty file operations" {
    const path = "/tmp/blitz_io_test_empty.txt";

    // Create empty file
    {
        var file = try File.create(path);
        file.close();
    }

    // Read from empty file
    {
        var file = try File.open(path);
        defer file.close();

        var buf: [64]u8 = undefined;
        const n = try file.read(&buf);
        try std.testing.expectEqual(@as(usize, 0), n);

        const len = try file.getLen();
        try std.testing.expectEqual(@as(u64, 0), len);
    }

    try std.fs.deleteFileAbsolute(path);
}

test "File - large write and read" {
    const path = "/tmp/blitz_io_test_large.txt";
    const size: usize = 1024 * 1024; // 1MB

    // Generate test data
    const data = try std.testing.allocator.alloc(u8, size);
    defer std.testing.allocator.free(data);
    for (data, 0..) |*b, i| {
        b.* = @intCast(i % 256);
    }

    // Write large file
    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll(data);
    }

    // Read back and verify
    {
        var file = try File.open(path);
        defer file.close();

        const read_data = try file.readToEnd(std.testing.allocator);
        defer std.testing.allocator.free(read_data);

        try std.testing.expectEqual(size, read_data.len);
        try std.testing.expectEqualSlices(u8, data, read_data);
    }

    try std.fs.deleteFileAbsolute(path);
}
