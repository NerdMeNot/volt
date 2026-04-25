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
const syscall = @import("../internal/syscall.zig");
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
        const raw_fd = try OpenOptions.new().setRead(true).open(path);
        return .{ .handle = raw_fd };
    }

    /// Create a file for writing. Truncates if exists, creates if not.
    pub fn create(path: []const u8) !File {
        const raw_fd = try OpenOptions.new()
            .setWrite(true)
            .setCreate(true)
            .setTruncate(true)
            .open(path);
        return .{ .handle = raw_fd };
    }

    /// Create a new file, failing if it already exists.
    pub fn createNew(path: []const u8) !File {
        const raw_fd = try OpenOptions.new()
            .setWrite(true)
            .setRead(true)
            .setCreateNew(true)
            .open(path);
        return .{ .handle = raw_fd };
    }

    /// Open a file with custom options.
    pub fn openWithOptions(path: []const u8, opts: OpenOptions) !File {
        const raw_fd = try opts.open(path);
        return .{ .handle = raw_fd };
    }

    /// Get an OpenOptions builder.
    pub fn getOptions() OpenOptions {
        return OpenOptions.new();
    }

    /// Create from a raw file descriptor (takes ownership).
    pub fn fromRawFd(raw_fd: posix.fd_t) File {
        return .{ .handle = raw_fd };
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
        return syscall.pread(self.handle, buf, offset);
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

    /// Read as many bytes as possible into `buf`, retrying on partial reads.
    /// Unlike `readAll`, does NOT return an error if EOF is reached before
    /// the buffer is full. Returns the total number of bytes read.
    pub fn readFull(self: *File, buf: []u8) !usize {
        var total: usize = 0;
        while (total < buf.len) {
            const n = try self.read(buf[total..]);
            if (n == 0) break;
            total += n;
        }
        return total;
    }

    /// Read data into multiple buffers (scatter read).
    /// More efficient than multiple read calls for structured data.
    pub fn readVectored(self: *File, iovecs: []posix.iovec) !usize {
        return syscall.readv(self.handle, iovecs);
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
        return syscall.write(self.handle, data);
    }

    /// Write data at a specific offset without changing the file position.
    pub fn pwrite(self: *File, data: []const u8, offset: u64) !usize {
        return syscall.pwrite(self.handle, data, offset);
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
        return syscall.writev(self.handle, iovecs);
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
        switch (whence) {
            .start => try syscall.lseekSet(self.handle, @intCast(offset)),
            .current => try syscall.lseekCur(self.handle, offset),
            .end => _ = try syscall.lseekEnd(self.handle, offset),
        }
        return syscall.lseekCurPos(self.handle);
    }

    /// Seek to the start of the file.
    pub fn rewind(self: *File) !void {
        _ = try self.seek(0, .start);
    }

    /// Get the current position in the file.
    pub fn getPos(self: *File) !u64 {
        return syscall.lseekCurPos(self.handle);
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
        try syscall.fsync(self.handle);
    }

    /// Sync data to disk (may not sync metadata).
    pub fn syncData(self: *File) !void {
        if (comptime builtin.os.tag == .linux) {
            try posix.fdatasync(self.handle);
        } else {
            try syscall.fsync(self.handle);
        }
    }

    /// Alias for syncAll.
    pub fn sync(self: *File) !void {
        try self.syncAll();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Advisory Hints
    // ═══════════════════════════════════════════════════════════════════════════

    /// Access pattern hint for the kernel.
    pub const FileAdvice = enum {
        /// Default behavior.
        normal,
        /// Reading front-to-back — increase read-ahead.
        sequential,
        /// Random access — disable read-ahead.
        random,
        /// Will need this data soon — start prefetching.
        will_need,
        /// Done with this data — can evict from cache.
        dont_need,
    };

    /// Advise the kernel about the expected access pattern for this file.
    /// This is a hint only — the kernel may ignore it.
    /// On macOS, only `sequential` and `random` have effect (via F_RDAHEAD).
    pub fn advise(self: *File, advice: FileAdvice) !void {
        return self.adviseRange(0, 0, advice);
    }

    /// Advise the kernel about the expected access pattern for a byte range.
    /// `offset` and `len` specify the range (0, 0 means entire file).
    /// On macOS, range is ignored — only whole-file sequential/random hints work.
    pub fn adviseRange(self: *File, offset: u64, len: u64, advice: FileAdvice) !void {
        if (comptime builtin.os.tag == .linux) {
            const linux = std.os.linux;
            const adv: usize = switch (advice) {
                .normal => linux.POSIX_FADV.NORMAL,
                .sequential => linux.POSIX_FADV.SEQUENTIAL,
                .random => linux.POSIX_FADV.RANDOM,
                .will_need => linux.POSIX_FADV.WILLNEED,
                .dont_need => linux.POSIX_FADV.DONTNEED,
            };
            const rc = linux.fadvise(self.handle, @intCast(offset), @intCast(len), adv);
            switch (posix.errno(rc)) {
                .SUCCESS => return,
                .BADF => unreachable, // We own the fd.
                .INVAL => unreachable, // Invalid advice value.
                else => |err| return posix.unexpectedErrno(err),
            }
        } else if (comptime builtin.os.tag == .macos or builtin.os.tag == .ios) {
            // macOS: only sequential/random via fcntl(F_RDAHEAD).
            // will_need/dont_need/normal have no equivalent for regular files.
            // Range parameters are not supported — macOS has no posix_fadvise.
            _ = .{ offset, len };
            switch (advice) {
                .sequential => {
                    _ = try syscall.fcntl(self.handle, std.c.F.RDAHEAD, 1);
                },
                .random => {
                    _ = try syscall.fcntl(self.handle, std.c.F.RDAHEAD, 0);
                },
                .normal, .will_need, .dont_need => {},
            }
        }
        // Other platforms: no-op (best effort).
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Metadata
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get metadata about the file.
    pub fn metadata(self: *File) !Metadata {
        const stat = try syscall.fstat(self.handle);
        return Metadata.fromStat(stat);
    }

    /// Set the length of the file.
    /// If `size` is less than current, the file is truncated.
    /// If `size` is greater, the file is extended with zeros.
    pub fn setLen(self: *File, size: u64) !void {
        try syscall.ftruncate(self.handle, @intCast(size));
    }

    /// Set file permissions.
    pub fn setPermissions(self: *File, perm: Permissions) !void {
        try syscall.fchmod(self.handle, @intCast(perm.mode));
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
            error.ConnectionResetByPeer => return error.ConnectionReset,
            else => return error.Unexpected,
        };
        return n;
    }

    fn fileWriteFn(ctx: *anyopaque, data: []const u8) io.Error!usize {
        const self: *File = @ptrCast(@alignCast(ctx));
        const n = syscall.write(self.handle, data) catch |err| switch (err) {
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
        syscall.fsync(self.handle) catch |err| switch (err) {
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
        syscall.close(self.handle);
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
    try syscall.unlink(path);
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

    try syscall.unlink(path);
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

    try syscall.unlink(path);
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

    try syscall.unlink(path);
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

    try syscall.unlink(path);
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

    try syscall.unlink(path);
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

    try syscall.unlink(path);
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

    try syscall.unlink(path);
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

    try syscall.unlink(path);
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

    try syscall.unlink(path);
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

    try syscall.unlink(path);
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

    try syscall.unlink(path);
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

    try syscall.unlink(path);
}

test "File - fromRawFd" {
    const path = "/tmp/blitz_io_test_rawfd.txt";

    // Create file using raw openat (std.fs.createFileAbsolute is gone in 0.16)
    const path_z = try posix.toPosixPath(path);
    const flags: posix.O = .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true };
    const raw_fd = try posix.openatZ(posix.AT.FDCWD, &path_z, flags, 0o644);

    // Wrap in our File type
    var file = File.fromRawFd(raw_fd);
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

    try syscall.unlink(path);
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

    try syscall.unlink(path);
}

test "File - syncData" {
    const path = "/tmp/blitz_io_test_syncdata.txt";

    var file = try File.create(path);
    defer file.close();

    try file.writeAll("data to sync");
    try file.syncData(); // Should not error

    try syscall.unlink(path);
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

    try syscall.unlink(path);
}

test "File - advise sequential" {
    const path = "/tmp/blitz_io_test_advise.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("test data for advisory hints");
    }

    {
        var file = try File.open(path);
        defer file.close();
        try file.advise(.sequential);
    }

    try syscall.unlink(path);
}

test "File - advise random" {
    const path = "/tmp/blitz_io_test_advise_random.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("test data for random access");
    }

    {
        var file = try File.open(path);
        defer file.close();
        try file.advise(.random);
    }

    try syscall.unlink(path);
}

test "File - adviseRange" {
    const path = "/tmp/blitz_io_test_advise_range.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("a" ** 4096);
    }

    {
        var file = try File.open(path);
        defer file.close();
        try file.adviseRange(0, 1024, .will_need);
        try file.adviseRange(1024, 1024, .dont_need);
        try file.adviseRange(0, 0, .normal);
    }

    try syscall.unlink(path);
}

test "File - readFull complete" {
    const path = "/tmp/blitz_io_test_readfull.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("Hello, readFull!");
    }

    {
        var file = try File.open(path);
        defer file.close();

        var buf: [16]u8 = undefined;
        const n = try file.readFull(&buf);
        try std.testing.expectEqual(@as(usize, 16), n);
        try std.testing.expectEqualStrings("Hello, readFull!", buf[0..n]);
    }

    try syscall.unlink(path);
}

test "File - readFull partial" {
    const path = "/tmp/blitz_io_test_readfull_partial.txt";

    {
        var file = try File.create(path);
        defer file.close();
        try file.writeAll("short");
    }

    {
        var file = try File.open(path);
        defer file.close();

        var buf: [100]u8 = undefined;
        const n = try file.readFull(&buf);
        try std.testing.expectEqual(@as(usize, 5), n);
        try std.testing.expectEqualStrings("short", buf[0..n]);
    }

    try syscall.unlink(path);
}

test "File - readFull empty file" {
    const path = "/tmp/blitz_io_test_readfull_empty.txt";

    {
        var file = try File.create(path);
        file.close();
    }

    {
        var file = try File.open(path);
        defer file.close();

        var buf: [64]u8 = undefined;
        const n = try file.readFull(&buf);
        try std.testing.expectEqual(@as(usize, 0), n);
    }

    try syscall.unlink(path);
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

    try syscall.unlink(path);
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

    try syscall.unlink(path);
}
