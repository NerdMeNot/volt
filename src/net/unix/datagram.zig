//! Unix Domain Datagram Socket
//!
//! Provides async Unix domain datagram sockets for local IPC.
//!
//! ## Usage
//!
//! ```zig
//! const unix = @import("volt").net.unix;
//!
//! // Bound socket
//! var socket = try unix.UnixDatagram.bind("/tmp/my.sock");
//! defer socket.close();
//!
//! // Receive
//! var buf: [1024]u8 = undefined;
//! const result = try socket.recvFrom(&buf);
//! std.debug.print("Got {} bytes from {s}\n", .{result.len, result.addr.path()});
//!
//! // Send
//! try socket.sendTo("Hello!", try unix.UnixAddr.fromPath("/tmp/other.sock"));
//! ```

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const syscall = @import("../../internal/syscall.zig");

const stream_mod = @import("stream.zig");
const UnixAddr = stream_mod.UnixAddr;

const LinkedList = @import("../../internal/util/linked_list.zig").LinkedList;
const Pointers = @import("../../internal/util/linked_list.zig").Pointers;

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer type for waking a suspended task.
pub const WakerFn = *const fn (*anyopaque) void;

pub const DatagramWaiter = struct {
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,
    complete: bool = false,
    pointers: Pointers(DatagramWaiter) = .{},

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
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// RecvFromResult
// ─────────────────────────────────────────────────────────────────────────────

/// Result of receiving a datagram.
pub const RecvFromResult = struct {
    len: usize,
    addr: UnixAddr,
};

// ─────────────────────────────────────────────────────────────────────────────
// UnixDatagram
// ─────────────────────────────────────────────────────────────────────────────

/// A Unix domain datagram socket.
pub const UnixDatagram = struct {
    fd: posix.socket_t,
    local_addr: ?UnixAddr = null,
    peer_addr: ?UnixAddr = null,

    const Self = @This();

    /// Create an unbound datagram socket.
    pub fn unbound() !Self {
        const fd = try syscall.socket(posix.AF.UNIX, posix.SOCK.DGRAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK, 0);
        return .{ .fd = fd };
    }

    /// Bind to a filesystem path.
    pub fn bind(path: []const u8) !Self {
        const addr = try UnixAddr.fromPath(path);
        return bindAddr(addr);
    }

    /// Bind to an address.
    pub fn bindAddr(addr: UnixAddr) !Self {
        const fd = try syscall.socket(posix.AF.UNIX, posix.SOCK.DGRAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK, 0);
        errdefer syscall.close(fd);

        var addr_copy = addr;

        // Remove existing socket file if it exists
        if (!addr.isAbstract()) {
            syscall.unlink(addr.path()) catch {};
        }

        try syscall.bind(fd, addr_copy.sockaddr(), addr_copy.len());

        return .{
            .fd = fd,
            .local_addr = addr,
        };
    }

    /// Create a socket pair (for IPC between parent/child).
    pub fn pair() ![2]Self {
        if (builtin.os.tag == .windows) {
            return error.NotSupported;
        }

        var fds: [2]posix.socket_t = undefined;

        if (builtin.os.tag == .linux) {
            // Linux: use direct syscall to avoid libc dependency.
            // SOCK_CLOEXEC and SOCK_NONBLOCK are set atomically.
            const linux = std.os.linux;
            const sock_type = linux.SOCK.DGRAM | linux.SOCK.CLOEXEC | linux.SOCK.NONBLOCK;
            const rc = linux.socketpair(linux.AF.UNIX, sock_type, 0, &fds);
            switch (std.posix.errno(rc)) {
                .SUCCESS => {},
                else => return error.SocketPairFailed,
            }
        } else {
            // macOS/BSD: use libc (always linked on macOS).
            const rc = std.c.socketpair(posix.AF.UNIX, posix.SOCK.DGRAM, 0, &fds);
            if (rc != 0) {
                return error.SocketPairFailed;
            }
            // Set NONBLOCK and CLOEXEC manually
            for (&fds) |fd| {
                _ = syscall.fcntl(fd, posix.F.SETFL, @as(u32, @bitCast(posix.O{ .NONBLOCK = true }))) catch {};
                _ = syscall.fcntl(fd, posix.F.SETFD, @as(u32, 1)) catch {}; // FD_CLOEXEC
            }
        }

        return .{
            .{ .fd = fds[0] },
            .{ .fd = fds[1] },
        };
    }

    /// Get the file descriptor.
    pub fn fileno(self: Self) posix.socket_t {
        return self.fd;
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

    /// Get peer address (if connected).
    pub fn peerAddr(self: *Self) !UnixAddr {
        if (self.peer_addr) |addr| return addr;

        var addr: posix.sockaddr.un = undefined;
        var len: posix.socklen_t = @sizeOf(posix.sockaddr.un);
        try posix.getpeername(self.fd, @ptrCast(&addr), &len);
        self.peer_addr = .{ .inner = addr };
        return self.peer_addr.?;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Connect (for connected datagrams)
    // ─────────────────────────────────────────────────────────────────────────

    /// Connect to a peer (enables send/recv instead of sendTo/recvFrom).
    pub fn connect(self: *Self, path: []const u8) !void {
        const addr = try UnixAddr.fromPath(path);
        return self.connectAddr(addr);
    }

    /// Connect to a peer address.
    pub fn connectAddr(self: *Self, addr: UnixAddr) !void {
        var addr_copy = addr;
        try syscall.connect(self.fd, addr_copy.sockaddr(), addr_copy.len());
        self.peer_addr = addr;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Send/Recv (connected mode)
    // ─────────────────────────────────────────────────────────────────────────

    /// Try to send without blocking (connected mode).
    pub fn trySend(self: *Self, data: []const u8) !?usize {
        const n = syscall.send(self.fd, data, 0) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };
        return n;
    }

    /// Try to receive without blocking (connected mode).
    pub fn tryRecv(self: *Self, buf: []u8) !?usize {
        const n = syscall.recv(self.fd, buf, 0) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };
        return n;
    }

    /// Send data (connected mode, blocking).
    pub fn send(self: *Self, data: []const u8) !usize {
        return syscall.send(self.fd, data, 0);
    }

    /// Receive data (connected mode, blocking).
    pub fn recv(self: *Self, buf: []u8) !usize {
        return syscall.recv(self.fd, buf, 0);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SendTo/RecvFrom (unconnected mode)
    // ─────────────────────────────────────────────────────────────────────────

    /// Try to send to address without blocking.
    pub fn trySendTo(self: *Self, data: []const u8, addr: UnixAddr) !?usize {
        var addr_copy = addr;
        const n = syscall.sendto(self.fd, data, 0, addr_copy.sockaddr(), addr_copy.len()) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };
        return n;
    }

    /// Try to receive from any address without blocking.
    pub fn tryRecvFrom(self: *Self, buf: []u8) !?RecvFromResult {
        var addr: posix.sockaddr.un = undefined;
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.un);

        const n = syscall.recvfrom(self.fd, buf, 0, @ptrCast(&addr), &addr_len) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };

        return .{
            .len = n,
            .addr = .{ .inner = addr },
        };
    }

    /// Send to address (blocking).
    pub fn sendTo(self: *Self, data: []const u8, addr: UnixAddr) !usize {
        var addr_copy = addr;
        return syscall.sendto(self.fd, data, 0, addr_copy.sockaddr(), addr_copy.len());
    }

    /// Receive from any address (blocking).
    pub fn recvFrom(self: *Self, buf: []u8) !RecvFromResult {
        var addr: posix.sockaddr.un = undefined;
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.un);

        const n = try syscall.recvfrom(self.fd, buf, 0, @ptrCast(&addr), &addr_len);

        return .{
            .len = n,
            .addr = .{ .inner = addr },
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Peek
    // ─────────────────────────────────────────────────────────────────────────

    /// Peek at data without consuming it.
    pub fn peek(self: *Self, buf: []u8) !usize {
        return syscall.recv(self.fd, buf, posix.MSG.PEEK);
    }

    /// Peek at data and get sender address.
    pub fn peekFrom(self: *Self, buf: []u8) !RecvFromResult {
        var addr: posix.sockaddr.un = undefined;
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.un);

        const n = try syscall.recvfrom(self.fd, buf, posix.MSG.PEEK, @ptrCast(&addr), &addr_len);

        return .{
            .len = n,
            .addr = .{ .inner = addr },
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Close
    // ─────────────────────────────────────────────────────────────────────────

    /// Close the socket.
    pub fn close(self: *Self) void {
        syscall.close(self.fd);

        // Clean up socket file
        if (self.local_addr) |addr| {
            if (!addr.isAbstract()) {
                syscall.unlink(addr.path()) catch {};
            }
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "UnixDatagram - unbound" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var socket = UnixDatagram.unbound() catch |err| {
        if (err == error.AddressFamilyNotSupported) return error.SkipZigTest;
        return err;
    };
    defer socket.close();

    try std.testing.expect(socket.fileno() >= 0);
}

test "UnixDatagram - bind" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const path = "/tmp/blitz-io-test-dgram.sock";

    var socket = UnixDatagram.bind(path) catch |err| {
        if (err == error.AddressFamilyNotSupported) return error.SkipZigTest;
        return err;
    };
    defer socket.close();

    const addr = try socket.localAddr();
    try std.testing.expectEqualStrings(path, addr.path());
}

test "UnixDatagram - pair" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var sockets = UnixDatagram.pair() catch |err| {
        if (err == error.AddressFamilyNotSupported) return error.SkipZigTest;
        return err;
    };
    defer sockets[0].close();
    defer sockets[1].close();

    // Send from one to the other
    const sent = try sockets[0].send("hello");
    try std.testing.expectEqual(@as(usize, 5), sent);

    var buf: [10]u8 = undefined;
    const received = try sockets[1].recv(&buf);
    try std.testing.expectEqual(@as(usize, 5), received);
    try std.testing.expectEqualStrings("hello", buf[0..received]);
}

test "UnixDatagram - tryRecv returns null when empty" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var socket = UnixDatagram.unbound() catch |err| {
        if (err == error.AddressFamilyNotSupported) return error.SkipZigTest;
        return err;
    };
    defer socket.close();

    var buf: [10]u8 = undefined;
    const result = try socket.tryRecv(&buf);
    try std.testing.expect(result == null);
}

test "UnixDatagram - sendTo recvFrom" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const path1 = "/tmp/blitz-io-test-dgram1.sock";
    const path2 = "/tmp/blitz-io-test-dgram2.sock";

    var socket1 = UnixDatagram.bind(path1) catch |err| {
        if (err == error.AddressFamilyNotSupported) return error.SkipZigTest;
        return err;
    };
    defer socket1.close();

    var socket2 = UnixDatagram.bind(path2) catch |err| {
        if (err == error.AddressFamilyNotSupported) return error.SkipZigTest;
        return err;
    };
    defer socket2.close();

    // Send from socket1 to socket2
    const addr2 = try UnixAddr.fromPath(path2);
    const sent = try socket1.sendTo("datagram message", addr2);
    try std.testing.expectEqual(@as(usize, 16), sent);

    // Receive on socket2
    var buf: [64]u8 = undefined;
    const result = try socket2.recvFrom(&buf);
    try std.testing.expectEqual(@as(usize, 16), result.len);
    try std.testing.expectEqualStrings("datagram message", buf[0..result.len]);
}

test "UnixDatagram - connected mode" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const path1 = "/tmp/blitz-io-test-dgram-conn1.sock";
    const path2 = "/tmp/blitz-io-test-dgram-conn2.sock";

    var socket1 = UnixDatagram.bind(path1) catch |err| {
        if (err == error.AddressFamilyNotSupported) return error.SkipZigTest;
        return err;
    };
    defer socket1.close();

    var socket2 = UnixDatagram.bind(path2) catch |err| {
        if (err == error.AddressFamilyNotSupported) return error.SkipZigTest;
        return err;
    };
    defer socket2.close();

    // Connect socket1 to socket2
    try socket1.connect(path2);

    // Now can use send() instead of sendTo()
    const sent = try socket1.send("connected dgram");
    try std.testing.expectEqual(@as(usize, 15), sent);

    // Receive on socket2
    var buf: [64]u8 = undefined;
    const result = try socket2.recvFrom(&buf);
    try std.testing.expectEqual(@as(usize, 15), result.len);
    try std.testing.expectEqualStrings("connected dgram", buf[0..result.len]);
}

test "UnixDatagram - peek" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var sockets = UnixDatagram.pair() catch |err| {
        if (err == error.AddressFamilyNotSupported or err == error.SocketPairFailed) return error.SkipZigTest;
        return err;
    };
    defer sockets[0].close();
    defer sockets[1].close();

    // Send data
    _ = try sockets[0].send("peek test");

    // Peek should return data without consuming it
    var buf: [64]u8 = undefined;
    const peek_n = try sockets[1].peek(&buf);
    try std.testing.expectEqual(@as(usize, 9), peek_n);
    try std.testing.expectEqualStrings("peek test", buf[0..peek_n]);

    // Regular recv should still get the same data
    const recv_n = try sockets[1].recv(&buf);
    try std.testing.expectEqual(@as(usize, 9), recv_n);
    try std.testing.expectEqualStrings("peek test", buf[0..recv_n]);

    // Now the data is consumed, tryRecv should return null
    const result = try sockets[1].tryRecv(&buf);
    try std.testing.expect(result == null);
}

test "UnixDatagram - trySend trySendTo" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var sockets = UnixDatagram.pair() catch |err| {
        if (err == error.AddressFamilyNotSupported or err == error.SocketPairFailed) return error.SkipZigTest;
        return err;
    };
    defer sockets[0].close();
    defer sockets[1].close();

    // trySend should work on connected pair
    const result = try sockets[0].trySend("try send");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 8), result.?);

    // Verify data received
    var buf: [64]u8 = undefined;
    const n = try sockets[1].recv(&buf);
    try std.testing.expectEqualStrings("try send", buf[0..n]);
}
