//! AsyncFile - Async File I/O with Backend Integration
//!
//! Provides true async file I/O that integrates with the volt runtime:
//! - **Linux with io_uring**: True async - operations submitted to kernel ring
//! - **Other platforms**: Uses blocking pool - sync I/O on dedicated threads
//!
//! ## Why Two File Types?
//!
//! - `File` (file.zig): Synchronous, works without a runtime, simpler
//! - `AsyncFile`: Requires runtime, non-blocking, better for high-concurrency
//!
//! ## Why Use a Blocking Pool?
//!
//! On most platforms (macOS, BSD, older Linux), the kernel does NOT support
//! truly async file I/O. kqueue and epoll are for sockets/pipes, not regular
//! files. If we did sync file I/O on the event loop thread, it would BLOCK
//! the entire event loop:
//!
//! ```
//! Event Loop Thread (BAD - blocks everything):
//!   accept() -> read_file() [BLOCKS 50ms] -> accept() -> ...
//!              ^^^^^^^^^^^^^^^^^^^^^^^^
//!              All network I/O stalls!
//!
//! With Blocking Pool (GOOD - non-blocking):
//!   Event Loop: accept() -> spawn_blocking(read_file) -> accept() -> ...
//!   Blocking Thread 1: read_file() [50ms, doesn't block event loop]
//!   Blocking Thread 2: read_file() [30ms]
//! ```
//!
//! ## Example
//!
//! ```zig
//! var rt = try Runtime.init(allocator, .{});
//! defer rt.deinit();
//!
//! // Open file asynchronously
//! var file = try AsyncFile.open(&rt, "data.txt");
//! defer file.close();
//!
//! // Read - uses io_uring on Linux, blocking pool elsewhere
//! var buf: [4096]u8 = undefined;
//! const n = try file.read(&buf);
//! ```

const std = @import("std");
const posix = std.posix;
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const backend_mod = @import("../internal/backend.zig");
const Backend = backend_mod.Backend;
const BackendType = backend_mod.BackendType;
const Operation = backend_mod.completion.Operation;
const Completion = backend_mod.completion.Completion;

const blocking_mod = @import("../internal/blocking.zig");
const BlockingPool = blocking_mod.BlockingPool;

const runtime_mod = @import("../runtime.zig");
const Runtime = runtime_mod.Runtime;

const OpenOptions = @import("open_options.zig").OpenOptions;
const Metadata = @import("metadata.zig").Metadata;
const Permissions = @import("metadata.zig").Permissions;
const io = @import("../stream.zig");

/// Async file handle that uses the runtime's I/O backend.
pub const AsyncFile = struct {
    handle: posix.fd_t,
    runtime: *Runtime,

    const Self = @This();

    // ═══════════════════════════════════════════════════════════════════════════
    // Construction
    // ═══════════════════════════════════════════════════════════════════════════

    /// Open a file for reading.
    pub fn open(rt: *Runtime, path: []const u8) !Self {
        const handle = try openSync(path, .{ .read = true });
        return .{ .handle = handle, .runtime = rt };
    }

    /// Create a file for writing (creates or truncates).
    pub fn create(rt: *Runtime, path: []const u8) !Self {
        const handle = try openSync(path, .{
            .write = true,
            .create = true,
            .truncate = true,
        });
        return .{ .handle = handle, .runtime = rt };
    }

    /// Open with custom options.
    pub fn openWithOptions(rt: *Runtime, path: []const u8, opts: OpenOptions) !Self {
        const std_file = try opts.open(path);
        return .{ .handle = std_file.handle, .runtime = rt };
    }

    /// Create from raw fd (takes ownership).
    pub fn fromRawFd(rt: *Runtime, raw_fd: posix.fd_t) Self {
        return .{ .handle = raw_fd, .runtime = rt };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Reading
    // ═══════════════════════════════════════════════════════════════════════════

    /// Read data from the file.
    /// Uses io_uring on Linux, blocking pool on other platforms.
    pub fn read(self: *Self, buf: []u8) !usize {
        return self.readAt(buf, null);
    }

    /// Read at a specific offset.
    pub fn readAt(self: *Self, buf: []u8, offset: ?u64) !usize {
        const backend_type = self.runtime.scheduler.getBackendType();

        if (backend_type == .io_uring) {
            return self.readIoUring(buf, offset);
        } else {
            return self.readBlocking(buf, offset);
        }
    }

    /// Read using io_uring (true async).
    fn readIoUring(self: *Self, buf: []u8, offset: ?u64) !usize {
        const backend = self.runtime.scheduler.getBackend();

        const op = Operation{
            .op = .{
                .read = .{
                    .fd = self.handle,
                    .buffer = buf,
                    .offset = offset,
                },
            },
            .user_data = 0,
        };

        // Submit and wait for completion
        _ = try backend.io_uring.submit(op);
        try backend.io_uring.flush();

        // Wait for completion
        var completions: [1]Completion = undefined;
        const count = try backend.io_uring.wait(&completions, 1);

        if (count == 0) return error.IoError;

        const result = completions[0].result;
        if (result < 0) {
            return posix.unexpectedErrno(@enumFromInt(@as(u16, @intCast(-result))));
        }

        return @intCast(result);
    }

    /// Read using blocking pool.
    /// Runs the actual I/O on a dedicated blocking thread to avoid
    /// blocking the async event loop.
    fn readBlocking(self: *Self, buf: []u8, offset: ?u64) !usize {
        const pool = self.runtime.getBlockingPool();

        // Create context for blocking operation
        const Context = struct {
            fd: posix.fd_t,
            buffer: []u8,
            off: ?u64,

            fn doRead(ctx: *@This()) !usize {
                if (ctx.off) |o| {
                    return posix.pread(ctx.fd, ctx.buffer, o);
                } else {
                    return posix.read(ctx.fd, ctx.buffer);
                }
            }
        };

        var ctx = Context{
            .fd = self.handle,
            .buffer = buf,
            .off = offset,
        };

        // Run on blocking pool and wait for result
        const handle = try blocking_mod.runBlocking(pool, Context.doRead, .{&ctx});
        return handle.wait();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Writing
    // ═══════════════════════════════════════════════════════════════════════════

    /// Write data to the file.
    pub fn write(self: *Self, data: []const u8) !usize {
        return self.writeAt(data, null);
    }

    /// Write at a specific offset.
    pub fn writeAt(self: *Self, data: []const u8, offset: ?u64) !usize {
        const backend_type = self.runtime.scheduler.getBackendType();

        if (backend_type == .io_uring) {
            return self.writeIoUring(data, offset);
        } else {
            return self.writeBlocking(data, offset);
        }
    }

    /// Write using io_uring.
    fn writeIoUring(self: *Self, data: []const u8, offset: ?u64) !usize {
        const backend = self.runtime.scheduler.getBackend();

        const op = Operation{
            .op = .{
                .write = .{
                    .fd = self.handle,
                    .buffer = data,
                    .offset = offset,
                },
            },
            .user_data = 0,
        };

        _ = try backend.io_uring.submit(op);
        try backend.io_uring.flush();

        var completions: [1]Completion = undefined;
        const count = try backend.io_uring.wait(&completions, 1);

        if (count == 0) return error.IoError;

        const result = completions[0].result;
        if (result < 0) {
            return posix.unexpectedErrno(@enumFromInt(@as(u16, @intCast(-result))));
        }

        return @intCast(result);
    }

    /// Write using blocking pool.
    /// Runs the actual I/O on a dedicated blocking thread to avoid
    /// blocking the async event loop.
    fn writeBlocking(self: *Self, data: []const u8, offset: ?u64) !usize {
        const pool = self.runtime.getBlockingPool();

        // Create context for blocking operation
        const Context = struct {
            fd: posix.fd_t,
            buffer: []const u8,
            off: ?u64,

            fn doWrite(ctx: *@This()) !usize {
                if (ctx.off) |o| {
                    return posix.pwrite(ctx.fd, ctx.buffer, o);
                } else {
                    return posix.write(ctx.fd, ctx.buffer);
                }
            }
        };

        var ctx = Context{
            .fd = self.handle,
            .buffer = data,
            .off = offset,
        };

        // Run on blocking pool and wait for result
        const handle = try blocking_mod.runBlocking(pool, Context.doWrite, .{&ctx});
        return handle.wait();
    }

    /// Write all data.
    pub fn writeAll(self: *Self, data: []const u8) !void {
        var written: usize = 0;
        while (written < data.len) {
            const n = try self.write(data[written..]);
            if (n == 0) return error.WriteZero;
            written += n;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Vectored I/O (Scatter/Gather)
    // ═══════════════════════════════════════════════════════════════════════════

    /// Read data into multiple buffers (scatter read).
    /// More efficient than multiple read calls for structured data.
    pub fn readVectored(self: *Self, iovecs: []posix.iovec) !usize {
        // readv doesn't benefit from io_uring significantly for files,
        // and io_uring readv support varies by kernel version
        return posix.readv(self.handle, iovecs);
    }

    /// Read into multiple buffers at a specific offset.
    pub fn readVectoredAt(self: *Self, iovecs: []posix.iovec, offset: u64) !usize {
        return posix.preadv(self.handle, iovecs, offset);
    }

    /// Write data from multiple buffers (gather write).
    /// More efficient than multiple write calls for structured data.
    pub fn writeVectored(self: *Self, iovecs: []const posix.iovec_const) !usize {
        return posix.writev(self.handle, iovecs);
    }

    /// Write from multiple buffers at a specific offset.
    pub fn writeVectoredAt(self: *Self, iovecs: []const posix.iovec_const, offset: u64) !usize {
        return posix.pwritev(self.handle, iovecs, offset);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Reading Convenience
    // ═══════════════════════════════════════════════════════════════════════════

    /// Read exactly `buf.len` bytes or return an error.
    pub fn readAll(self: *Self, buf: []u8) !usize {
        var index: usize = 0;
        while (index < buf.len) {
            const n = try self.read(buf[index..]);
            if (n == 0) return error.EndOfStream;
            index += n;
        }
        return index;
    }

    /// Read the entire file into an allocated buffer.
    pub fn readToEnd(self: *Self, allocator: std.mem.Allocator) ![]u8 {
        // Get file size for pre-allocation
        const size = try self.getLen();

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

    /// Read entire file as a string (UTF-8).
    pub fn readToString(self: *Self, allocator: std.mem.Allocator) ![]u8 {
        return self.readToEnd(allocator);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Seeking
    // ═══════════════════════════════════════════════════════════════════════════

    /// Seek position reference.
    pub const SeekFrom = enum {
        start,
        current,
        end,
    };

    /// Seek to a position. Returns the new position from the start.
    /// Note: Seek is synchronous even with io_uring as it's just an lseek syscall.
    pub fn seek(self: *Self, offset: i64, whence: SeekFrom) !u64 {
        const w: posix.lseek_whence_t = switch (whence) {
            .start => .SET,
            .current => .CUR,
            .end => .END,
        };
        const result = posix.lseek(self.handle, offset, w);
        return @intCast(result);
    }

    /// Seek to the start of the file.
    pub fn rewind(self: *Self) !void {
        _ = try self.seek(0, .start);
    }

    /// Get the current position in the file.
    pub fn getPos(self: *Self) !u64 {
        return self.seek(0, .current);
    }

    /// Seek to a specific position from the start.
    pub fn seekTo(self: *Self, pos: u64) !void {
        _ = try self.seek(@intCast(pos), .start);
    }

    /// Seek by a relative offset from current position.
    pub fn seekBy(self: *Self, offset: i64) !u64 {
        return self.seek(offset, .current);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Sync
    // ═══════════════════════════════════════════════════════════════════════════

    /// Sync all data and metadata to disk.
    pub fn syncAll(self: *Self) !void {
        const backend_type = self.runtime.scheduler.getBackendType();

        if (backend_type == .io_uring) {
            try self.fsyncIoUring(false);
        } else {
            try posix.fsync(self.handle);
        }
    }

    /// Sync data only (not metadata).
    pub fn syncData(self: *Self) !void {
        const backend_type = self.runtime.scheduler.getBackendType();

        if (backend_type == .io_uring) {
            try self.fsyncIoUring(true);
        } else {
            if (comptime builtin.os.tag == .linux) {
                try posix.fdatasync(self.handle);
            } else {
                try posix.fsync(self.handle);
            }
        }
    }

    /// Alias for syncAll (consistency with File).
    pub fn sync(self: *Self) !void {
        try self.syncAll();
    }

    fn fsyncIoUring(self: *Self, datasync: bool) !void {
        const backend = self.runtime.scheduler.getBackend();

        const op = Operation{
            .op = .{
                .fsync = .{
                    .fd = self.handle,
                    .datasync = datasync,
                },
            },
            .user_data = 0,
        };

        _ = try backend.io_uring.submit(op);
        try backend.io_uring.flush();

        var completions: [1]Completion = undefined;
        const count = try backend.io_uring.wait(&completions, 1);

        if (count == 0) return error.IoError;

        const result = completions[0].result;
        if (result < 0) {
            return posix.unexpectedErrno(@enumFromInt(@as(u16, @intCast(-result))));
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Metadata
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get file metadata.
    pub fn metadata(self: *Self) !Metadata {
        const stat = try posix.fstat(self.handle);
        return Metadata.fromStat(stat);
    }

    /// Get file size.
    pub fn getLen(self: *Self) !u64 {
        const meta = try self.metadata();
        return meta.size();
    }

    /// Set the length of the file.
    /// If `size` is less than current, the file is truncated.
    /// If `size` is greater, the file is extended with zeros.
    pub fn setLen(self: *Self, size: u64) !void {
        const backend_type = self.runtime.scheduler.getBackendType();

        if (backend_type == .io_uring) {
            try self.ftruncateIoUring(size);
        } else {
            try posix.ftruncate(self.handle, @intCast(size));
        }
    }

    fn ftruncateIoUring(self: *Self, size: u64) !void {
        // io_uring supports ftruncate via IORING_OP_FTRUNCATE (kernel 6.0+)
        // Fall back to sync for older kernels
        // For now, use sync path as ftruncate is a fast operation
        try posix.ftruncate(self.handle, @intCast(size));
    }

    /// Set file permissions.
    pub fn setPermissions(self: *Self, perm: Permissions) !void {
        // fchmod is typically fast, no async benefit
        try posix.fchmod(self.handle, @intCast(perm.mode));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Reader/Writer Interfaces
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get a polymorphic Reader interface.
    /// Note: This uses blocking reads for compatibility with the sync interface.
    pub fn reader(self: *Self) io.Reader {
        return .{
            .context = @ptrCast(self),
            .readFn = asyncFileReadFn,
        };
    }

    /// Get a polymorphic Writer interface.
    pub fn writer(self: *Self) io.Writer {
        return .{
            .context = @ptrCast(self),
            .writeFn = asyncFileWriteFn,
            .flushFn = asyncFileFlushFn,
        };
    }

    fn asyncFileReadFn(ctx: *anyopaque, buffer: []u8) io.Error!usize {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.read(buffer) catch |err| switch (err) {
            error.WouldBlock => return error.WouldBlock,
            error.BrokenPipe => return error.BrokenPipe,
            error.InputOutput => return error.IoError,
            else => return error.Unexpected,
        };
    }

    fn asyncFileWriteFn(ctx: *anyopaque, data: []const u8) io.Error!usize {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.write(data) catch |err| switch (err) {
            error.WouldBlock => return error.WouldBlock,
            error.BrokenPipe => return error.BrokenPipe,
            error.InputOutput => return error.IoError,
            error.NoSpaceLeft => return error.IoError,
            else => return error.Unexpected,
        };
    }

    fn asyncFileFlushFn(ctx: *anyopaque) io.Error!void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        self.syncAll() catch |err| switch (err) {
            error.InputOutput => return error.IoError,
            else => return error.Unexpected,
        };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Lifecycle
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get the underlying file descriptor.
    pub fn fd(self: Self) posix.fd_t {
        return self.handle;
    }

    /// Close the file.
    pub fn close(self: *Self) void {
        // TODO: Could use io_uring close for true async close
        posix.close(self.handle);
        self.handle = -1;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Internal helpers
    // ═══════════════════════════════════════════════════════════════════════════

    fn openSync(path: []const u8, opts: struct {
        read: bool = false,
        write: bool = false,
        create: bool = false,
        truncate: bool = false,
    }) !posix.fd_t {
        var flags: posix.O = .{};

        if (opts.read and opts.write) {
            flags.ACCMODE = .RDWR;
        } else if (opts.write) {
            flags.ACCMODE = .WRONLY;
        } else {
            flags.ACCMODE = .RDONLY;
        }

        if (opts.create) flags.CREAT = true;
        if (opts.truncate) flags.TRUNC = true;
        flags.CLOEXEC = true;

        const path_z = try posix.toPosixPath(path);
        return posix.openZ(&path_z, flags, 0o666);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Convenience functions that use runtime
// ═══════════════════════════════════════════════════════════════════════════════

/// Read entire file contents using async I/O.
pub fn readFileAsync(rt: *Runtime, allocator: Allocator, path: []const u8) ![]u8 {
    var file = try AsyncFile.open(rt, path);
    defer file.close();

    const size = try file.getLen();
    var buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);

    var total: usize = 0;
    while (total < size) {
        const n = try file.read(buf[total..]);
        if (n == 0) break;
        total += n;
    }

    if (total < size) {
        buf = try allocator.realloc(buf, total);
    }

    return buf;
}

/// Write entire file contents using async I/O.
pub fn writeFileAsync(rt: *Runtime, path: []const u8, data: []const u8) !void {
    var file = try AsyncFile.create(rt, path);
    defer file.close();

    try file.writeAll(data);
    try file.syncAll();
}

/// Append data to a file using async I/O.
pub fn appendFileAsync(rt: *Runtime, path: []const u8, data: []const u8) !void {
    const handle = try openSyncAppend(path);
    var file = AsyncFile{ .handle = handle, .runtime = rt };
    defer file.close();

    try file.writeAll(data);
    try file.syncAll();
}

fn openSyncAppend(path: []const u8) !posix.fd_t {
    var flags: posix.O = .{};
    flags.ACCMODE = .WRONLY;
    flags.CREAT = true;
    flags.APPEND = true;
    flags.CLOEXEC = true;

    const path_z = try posix.toPosixPath(path);
    return posix.openZ(&path_z, flags, 0o666);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════════════
// Tests — Synchronous operations that don't need a runtime
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;
const tmp_dir = std.testing.tmpDir;

test "AsyncFile - openSync read mode" {
    // Create a temp file first
    var dir = tmp_dir(.{});
    defer dir.cleanup();

    const path = "test_open.txt";
    const file = try dir.dir.createFile(path, .{});
    try file.writeAll("hello");
    file.close();

    // Test openSync in read mode
    var full_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const full_path = try dir.dir.realpath(path, &full_path_buf);

    const fd = try AsyncFile.openSync(full_path, .{ .read = true });
    defer posix.close(fd);

    var buf: [16]u8 = undefined;
    const n = try posix.read(fd, &buf);
    try testing.expectEqualStrings("hello", buf[0..n]);
}

test "AsyncFile - openSync write+create+truncate mode" {
    var dir = tmp_dir(.{});
    defer dir.cleanup();

    const path = "test_create.txt";

    var full_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    // Create file first so realpath works
    const file = try dir.dir.createFile(path, .{});
    try file.writeAll("old content");
    file.close();

    const full_path = try dir.dir.realpath(path, &full_path_buf);

    const fd = try AsyncFile.openSync(full_path, .{
        .write = true,
        .create = true,
        .truncate = true,
    });
    defer posix.close(fd);

    // Write new content
    _ = try posix.write(fd, "new");

    // Verify truncation + new content
    const rd_fd = try AsyncFile.openSync(full_path, .{ .read = true });
    defer posix.close(rd_fd);

    var buf: [16]u8 = undefined;
    const n = try posix.read(rd_fd, &buf);
    try testing.expectEqualStrings("new", buf[0..n]);
}

test "AsyncFile - openSync read+write mode" {
    var dir = tmp_dir(.{});
    defer dir.cleanup();

    const path = "test_rw.txt";
    const file = try dir.dir.createFile(path, .{});
    try file.writeAll("data");
    file.close();

    var full_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const full_path = try dir.dir.realpath(path, &full_path_buf);

    const fd = try AsyncFile.openSync(full_path, .{ .read = true, .write = true });
    defer posix.close(fd);

    // Read existing
    var buf: [16]u8 = undefined;
    const n = try posix.read(fd, &buf);
    try testing.expectEqualStrings("data", buf[0..n]);

    // Write at start (seek back first)
    try posix.lseek_SET(fd, 0);
    _ = try posix.write(fd, "DATA");

    // Read back
    try posix.lseek_SET(fd, 0);
    const n2 = try posix.read(fd, &buf);
    try testing.expectEqualStrings("DATA", buf[0..n2]);
}

test "AsyncFile - SeekFrom enum values" {
    try testing.expectEqual(AsyncFile.SeekFrom.start, .start);
    try testing.expectEqual(AsyncFile.SeekFrom.current, .current);
    try testing.expectEqual(AsyncFile.SeekFrom.end, .end);
}

test "openSyncAppend creates and appends" {
    var dir = tmp_dir(.{});
    defer dir.cleanup();

    const path = "test_append.txt";
    const file = try dir.dir.createFile(path, .{});
    try file.writeAll("first");
    file.close();

    var full_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const full_path = try dir.dir.realpath(path, &full_path_buf);

    const fd = try openSyncAppend(full_path);
    defer posix.close(fd);

    _ = try posix.write(fd, "second");

    // Verify
    const rd_fd = try AsyncFile.openSync(full_path, .{ .read = true });
    defer posix.close(rd_fd);

    var buf: [32]u8 = undefined;
    const n = try posix.read(rd_fd, &buf);
    try testing.expectEqualStrings("firstsecond", buf[0..n]);
}
