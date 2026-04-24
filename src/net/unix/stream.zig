//! Unix Domain Stream Socket
//!
//! Provides async Unix domain stream sockets for local IPC.
//!
//! ## Usage
//!
//! ```zig
//! const unix = @import("volt").net.unix;
//!
//! // Server
//! var listener = try unix.UnixListener.bind("/tmp/my.sock");
//! defer listener.close();
//!
//! while (true) {
//!     if (try listener.tryAccept()) |conn| {
//!         // Handle connection
//!     }
//! }
//!
//! // Client
//! var stream = try unix.UnixStream.connect("/tmp/my.sock");
//! defer stream.close();
//! try stream.writeAll("Hello!");
//! ```

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const syscall = @import("../../internal/syscall.zig");
const Allocator = std.mem.Allocator;

const LinkedList = @import("../../internal/util/linked_list.zig").LinkedList;
const Pointers = @import("../../internal/util/linked_list.zig").Pointers;

// ─────────────────────────────────────────────────────────────────────────────
// Unix Socket Address
// ─────────────────────────────────────────────────────────────────────────────

/// Unix socket address (path-based or abstract).
pub const UnixAddr = struct {
    inner: posix.sockaddr.un,

    const Self = @This();

    /// Create from filesystem path.
    pub fn fromPath(socket_path: []const u8) !Self {
        if (socket_path.len > 107) return error.PathTooLong;

        var addr: posix.sockaddr.un = undefined;
        addr.family = posix.AF.UNIX;
        @memset(&addr.path, 0);
        @memcpy(addr.path[0..socket_path.len], socket_path);

        return .{ .inner = addr };
    }

    /// Create abstract socket address (Linux only).
    /// Abstract sockets start with a null byte.
    pub fn fromAbstract(name: []const u8) !Self {
        if (builtin.os.tag != .linux) return error.AbstractNotSupported;
        if (name.len > 106) return error.PathTooLong;

        var addr: posix.sockaddr.un = undefined;
        addr.family = posix.AF.UNIX;
        @memset(&addr.path, 0);
        addr.path[0] = 0; // Abstract socket indicator
        @memcpy(addr.path[1 .. name.len + 1], name);

        return .{ .inner = addr };
    }

    /// Get the path as a slice.
    pub fn path(self: *const Self) []const u8 {
        // Find null terminator
        var path_len: usize = 0;
        for (self.inner.path) |c| {
            if (c == 0) break;
            path_len += 1;
        }
        return self.inner.path[0..path_len];
    }

    /// Check if abstract socket (Linux).
    pub fn isAbstract(self: *const Self) bool {
        return self.inner.path[0] == 0;
    }

    /// Get the raw sockaddr pointer.
    pub fn sockaddr(self: *Self) *posix.sockaddr {
        return @ptrCast(&self.inner);
    }

    /// Get sockaddr length.
    pub fn len(_: *const Self) posix.socklen_t {
        return @sizeOf(posix.sockaddr.un);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer type for waking a suspended task.
pub const WakerFn = *const fn (*anyopaque) void;

pub const StreamWaiter = struct {
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,
    complete: bool = false,
    result: ?usize = null,
    err: ?anyerror = null,
    pointers: Pointers(StreamWaiter) = .{},

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    pub fn setWaker(self: *Self, ctx: *anyopaque, wake_fn: WakerFn) void {
        self.waker_ctx = ctx;
        self.waker = wake_fn;
    }

    pub fn wake(self: *Self) void {
        if (self.waker) |wf| {
            if (self.waker_ctx) |ctx| {
                wf(ctx);
            }
        }
    }

    pub fn isComplete(self: *const Self) bool {
        return self.complete;
    }

    pub fn reset(self: *Self) void {
        self.complete = false;
        self.result = null;
        self.err = null;
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// UnixStream
// ─────────────────────────────────────────────────────────────────────────────

/// A connected Unix domain stream socket.
pub const UnixStream = struct {
    fd: posix.socket_t,
    peer_addr: ?UnixAddr = null,
    local_addr: ?UnixAddr = null,

    const Self = @This();

    /// Connect to a Unix socket path.
    pub fn connect(path: []const u8) !Self {
        const addr = try UnixAddr.fromPath(path);
        return connectAddr(addr);
    }

    /// Connect to a Unix socket address.
    pub fn connectAddr(addr: UnixAddr) !Self {
        const fd = try syscall.socket(posix.AF.UNIX, posix.SOCK.STREAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK, 0);
        errdefer syscall.close(fd);

        var addr_copy = addr;
        syscall.connect(fd, addr_copy.sockaddr(), addr_copy.len()) catch |err| switch (err) {
            error.WouldBlock, error.ConnectionPending => {
                // Non-blocking connect in progress - that's fine
            },
            else => return err,
        };

        return .{
            .fd = fd,
            .peer_addr = addr,
        };
    }

    /// Create from an existing file descriptor.
    pub fn fromFd(fd: posix.socket_t) Self {
        return .{ .fd = fd };
    }

    /// Get the file descriptor.
    pub fn fileno(self: Self) posix.socket_t {
        return self.fd;
    }

    /// Get peer address.
    pub fn peerAddr(self: *Self) !UnixAddr {
        if (self.peer_addr) |addr| return addr;

        var addr: posix.sockaddr.un = undefined;
        var len: posix.socklen_t = @sizeOf(posix.sockaddr.un);
        try posix.getpeername(self.fd, @ptrCast(&addr), &len);
        self.peer_addr = .{ .inner = addr };
        return self.peer_addr.?;
    }

    /// Get local address.
    pub fn localAddr(self: *Self) !UnixAddr {
        if (self.local_addr) |addr| return addr;

        var addr: posix.sockaddr.un = undefined;
        var len: posix.socklen_t = @sizeOf(posix.sockaddr.un);
        try syscall.getsockname(self.fd, @ptrCast(&addr), &len);
        self.local_addr = .{ .inner = addr };
        return self.local_addr.?;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Read/Write
    // ─────────────────────────────────────────────────────────────────────────

    /// Try to read without blocking.
    pub fn tryRead(self: *Self, buf: []u8) !?usize {
        const n = posix.read(self.fd, buf) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };
        return n;
    }

    /// Try to write without blocking.
    pub fn tryWrite(self: *Self, data: []const u8) !?usize {
        const n = syscall.write(self.fd, data) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };
        return n;
    }

    /// Blocking read.
    pub fn read(self: *Self, buf: []u8) !usize {
        return posix.read(self.fd, buf);
    }

    /// Blocking write.
    pub fn write(self: *Self, data: []const u8) !usize {
        return syscall.write(self.fd, data);
    }

    /// Write all data.
    pub fn writeAll(self: *Self, data: []const u8) !void {
        var offset: usize = 0;
        while (offset < data.len) {
            offset += try syscall.write(self.fd, data[offset..]);
        }
    }

    /// Read with scatter/gather.
    pub fn readv(self: *Self, iovecs: []posix.iovec) !usize {
        return syscall.readv(self.fd, iovecs);
    }

    /// Write with scatter/gather.
    pub fn writev(self: *Self, iovecs: []const posix.iovec_const) !usize {
        return syscall.writev(self.fd, iovecs);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Shutdown
    // ─────────────────────────────────────────────────────────────────────────

    pub const ShutdownHow = enum {
        read,
        write,
        both,

        fn toNative(self: ShutdownHow) syscall.ShutdownHow {
            return switch (self) {
                .read => .recv,
                .write => .send,
                .both => .both,
            };
        }
    };

    /// Shutdown the socket.
    pub fn shutdown(self: *Self, how: ShutdownHow) !void {
        try syscall.shutdown(self.fd, how.toNative());
    }

    /// Close the socket.
    pub fn close(self: *Self) void {
        syscall.close(self.fd);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Splitting
    // ─────────────────────────────────────────────────────────────────────────

    /// Split into read and write halves.
    pub fn split(self: *Self) struct { read: *ReadHalf, write: *WriteHalf } {
        return .{
            .read = @ptrCast(self),
            .write = @ptrCast(self),
        };
    }
};

/// Read half of a split UnixStream.
pub const ReadHalf = struct {
    stream: *UnixStream,

    pub fn tryRead(self: *ReadHalf, buf: []u8) !?usize {
        return self.stream.tryRead(buf);
    }

    pub fn read(self: *ReadHalf, buf: []u8) !usize {
        return self.stream.read(buf);
    }
};

/// Write half of a split UnixStream.
pub const WriteHalf = struct {
    stream: *UnixStream,

    pub fn tryWrite(self: *WriteHalf, data: []const u8) !?usize {
        return self.stream.tryWrite(data);
    }

    pub fn write(self: *WriteHalf, data: []const u8) !usize {
        return self.stream.write(data);
    }

    pub fn writeAll(self: *WriteHalf, data: []const u8) !void {
        return self.stream.writeAll(data);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "UnixAddr - from path" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const addr = try UnixAddr.fromPath("/tmp/test.sock");
    try std.testing.expectEqualStrings("/tmp/test.sock", addr.path());
    try std.testing.expect(!addr.isAbstract());
}

test "UnixAddr - path too long" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var long_path: [200]u8 = undefined;
    @memset(&long_path, 'x');

    const result = UnixAddr.fromPath(&long_path);
    try std.testing.expectError(error.PathTooLong, result);
}

test "UnixStream - create socket" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    // Just test that we can create a socket (connecting will fail without a server)
    const fd = syscall.socket(posix.AF.UNIX, posix.SOCK.STREAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK, 0) catch |err| {
        // Skip on systems that don't support Unix sockets
        if (err == error.AddressFamilyNotSupported) return error.SkipZigTest;
        return err;
    };
    defer syscall.close(fd);

    const stream = UnixStream.fromFd(fd);
    try std.testing.expect(stream.fileno() == fd);
}

test "UnixAddr - abstract namespace (Linux)" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    const addr = try UnixAddr.fromAbstract("my-abstract-socket");
    try std.testing.expect(addr.isAbstract());
    // Abstract sockets have null first byte
    try std.testing.expectEqual(@as(u8, 0), addr.inner.path[0]);
}

test "UnixAddr - abstract not supported on non-Linux" {
    if (builtin.os.tag == .linux) return error.SkipZigTest;
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const result = UnixAddr.fromAbstract("test");
    try std.testing.expectError(error.AbstractNotSupported, result);
}

test "UnixStream - shutdown modes" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    // Create a socketpair for testing shutdown
    var fds: [2]posix.socket_t = undefined;
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        const sock_type = linux.SOCK.STREAM | linux.SOCK.CLOEXEC;
        const rc2 = linux.socketpair(linux.AF.UNIX, sock_type, 0, &fds);
        if (std.posix.errno(rc2) != .SUCCESS) return error.SkipZigTest;
    } else {
        const rc = std.c.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &fds);
        if (rc != 0) return error.SkipZigTest;
    }

    var stream1 = UnixStream.fromFd(fds[0]);
    defer stream1.close();
    var stream2 = UnixStream.fromFd(fds[1]);
    defer stream2.close();

    // Write some data
    _ = try stream1.write("hello");

    // Shutdown write on stream1
    try stream1.shutdown(.write);

    // Stream2 should still be able to read
    var buf: [10]u8 = undefined;
    const n = try stream2.read(&buf);
    try std.testing.expectEqualStrings("hello", buf[0..n]);

    // Reading more should get EOF (0 bytes)
    const n2 = try stream2.read(&buf);
    try std.testing.expectEqual(@as(usize, 0), n2);
}

test "UnixStream - writeAll" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    // Create a socketpair
    var fds: [2]posix.socket_t = undefined;
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        const sock_type = linux.SOCK.STREAM | linux.SOCK.CLOEXEC;
        const rc2 = linux.socketpair(linux.AF.UNIX, sock_type, 0, &fds);
        if (std.posix.errno(rc2) != .SUCCESS) return error.SkipZigTest;
    } else {
        const rc = std.c.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &fds);
        if (rc != 0) return error.SkipZigTest;
    }

    var stream1 = UnixStream.fromFd(fds[0]);
    defer stream1.close();
    var stream2 = UnixStream.fromFd(fds[1]);
    defer stream2.close();

    // Write all data
    const data = "Hello, Unix sockets!";
    try stream1.writeAll(data);

    // Read it all back
    var buf: [64]u8 = undefined;
    var total: usize = 0;
    while (total < data.len) {
        const n = try stream2.read(buf[total..]);
        if (n == 0) break;
        total += n;
    }
    try std.testing.expectEqualStrings(data, buf[0..total]);
}

test "UnixStream - tryRead tryWrite" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    // Create non-blocking socketpair
    var fds: [2]posix.socket_t = undefined;
    if (builtin.os.tag == .linux) {
        const sock_type = std.os.linux.SOCK.STREAM | std.os.linux.SOCK.CLOEXEC | std.os.linux.SOCK.NONBLOCK;
        const rc = std.os.linux.socketpair(std.os.linux.AF.UNIX, sock_type, 0, &fds);
        if (std.posix.errno(rc) != .SUCCESS) return error.SkipZigTest;
    } else {
        const rc = std.c.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &fds);
        if (rc != 0) return error.SkipZigTest;
        for (&fds) |fd| {
            _ = syscall.fcntl(fd, posix.F.SETFL, @as(u32, @bitCast(posix.O{ .NONBLOCK = true }))) catch {};
        }
    }

    var stream1 = UnixStream.fromFd(fds[0]);
    defer stream1.close();
    var stream2 = UnixStream.fromFd(fds[1]);
    defer stream2.close();

    // tryRead on empty socket returns null
    var buf: [10]u8 = undefined;
    const read_result = try stream2.tryRead(&buf);
    try std.testing.expect(read_result == null);

    // tryWrite should work
    const write_result = try stream1.tryWrite("test");
    try std.testing.expect(write_result != null);
    try std.testing.expectEqual(@as(usize, 4), write_result.?);

    // Now tryRead should return data
    const read_result2 = try stream2.tryRead(&buf);
    try std.testing.expect(read_result2 != null);
    try std.testing.expectEqualStrings("test", buf[0..read_result2.?]);
}
