//! Shared imports, types, and helpers for TCP sub-modules.

const std = @import("std");
pub const posix = std.posix;
pub const mem = std.mem;
pub const builtin = @import("builtin");

pub const Address = @import("../address.zig").Address;

const sio_mod = @import("../../internal/backend/scheduled_io.zig");
pub const ScheduledIo = sio_mod.ScheduledIo;
pub const Ready = sio_mod.Ready;
pub const Interest = sio_mod.Interest;
pub const Waker = sio_mod.Waker;

pub const FutureWaker = @import("../../future/Waker.zig").Waker;
pub const FutureContext = @import("../../future/Waker.zig").Context;
pub const FuturePollResult = @import("../../future/Poll.zig").PollResult;

pub const Duration = @import("../../time.zig").Duration;
pub const io = @import("../../stream.zig");
pub const runtime_mod = @import("../../runtime.zig");
pub const IoDriver = @import("../../internal/io_driver.zig").IoDriver;

// ═══════════════════════════════════════════════════════════════════════════════
// Cross-Platform Socket Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Invalid socket sentinel value (cross-platform).
/// On Windows, socket_t is an opaque pointer, so we can't use -1.
pub const INVALID_SOCKET: posix.socket_t = if (builtin.os.tag == .windows)
    @ptrFromInt(~@as(usize, 0)) // INVALID_SOCKET = ~0 on Windows
else
    -1;

/// Check if a socket is valid (cross-platform).
pub fn isValidSocket(fd: posix.socket_t) bool {
    if (comptime builtin.os.tag == .windows) {
        return fd != INVALID_SOCKET;
    } else {
        return fd >= 0;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Socket Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Cross-platform getsockopt. On Windows, uses ws2_32 directly to avoid libc dependency.
pub fn getsockopt(fd: posix.socket_t, level: i32, opt: u32, buf: []u8) !void {
    if (comptime builtin.os.tag == .windows) {
        const ws2 = std.os.windows.ws2_32;
        var optlen: i32 = @intCast(buf.len);
        const rc = ws2.getsockopt(fd, level, @intCast(opt), buf.ptr, &optlen);
        if (rc != 0) return error.Unexpected;
    } else {
        try posix.getsockopt(fd, level, opt, buf);
    }
}

/// Cross-platform recvfrom. On Windows, uses ws2_32 directly to avoid libc dependency.
pub fn recvfrom(fd: posix.socket_t, buf: []u8, flags: u32, src_addr: *posix.sockaddr, addrlen: *posix.socklen_t) !usize {
    if (comptime builtin.os.tag == .windows) {
        const ws2 = std.os.windows.ws2_32;
        const rc = ws2.recvfrom(fd, buf.ptr, @intCast(buf.len), @intCast(flags), src_addr, @ptrCast(addrlen));
        if (rc < 0) {
            const err = ws2.WSAGetLastError();
            return switch (err) {
                .WSAEWOULDBLOCK => error.WouldBlock,
                else => error.Unexpected,
            };
        }
        return @intCast(rc);
    } else {
        return posix.recvfrom(fd, buf, flags, src_addr, addrlen);
    }
}

/// Cross-platform recv. On Windows, uses ws2_32 directly to avoid libc dependency.
/// Returns the same error set as posix.recv for caller compatibility.
pub fn recv(fd: posix.socket_t, buf: []u8, flags: u32) posix.RecvFromError!usize {
    if (comptime builtin.os.tag == .windows) {
        const ws2 = std.os.windows.ws2_32;
        const rc = ws2.recv(fd, buf.ptr, @intCast(buf.len), @intCast(flags));
        if (rc < 0) {
            const err = ws2.WSAGetLastError();
            return switch (err) {
                .WSAEWOULDBLOCK => error.WouldBlock,
                .WSAECONNRESET => error.ConnectionResetByPeer,
                .WSAECONNREFUSED => error.ConnectionRefused,
                .WSAECONNABORTED => error.ConnectionResetByPeer,
                else => error.Unexpected,
            };
        }
        return @intCast(rc);
    } else {
        return posix.recv(fd, buf, flags);
    }
}

/// Cross-platform send. On Windows, uses ws2_32 directly to avoid libc dependency.
/// Returns the same error set as posix.send for caller compatibility.
pub fn send(fd: posix.socket_t, buf: []const u8, flags: u32) posix.SendError!usize {
    if (comptime builtin.os.tag == .windows) {
        const ws2 = std.os.windows.ws2_32;
        const rc = ws2.send(fd, buf.ptr, @intCast(buf.len), @intCast(flags));
        if (rc < 0) {
            const err = ws2.WSAGetLastError();
            return switch (err) {
                .WSAEWOULDBLOCK => error.WouldBlock,
                .WSAECONNRESET => error.ConnectionResetByPeer,
                .WSAECONNABORTED => error.BrokenPipe,
                else => error.Unexpected,
            };
        }
        return @intCast(rc);
    } else {
        return posix.send(fd, buf, flags);
    }
}

/// Cross-platform sendto. On Windows, uses ws2_32 directly to avoid libc dependency.
pub fn sendto(fd: posix.socket_t, buf: []const u8, flags: u32, dest_addr: *const posix.sockaddr, addrlen: posix.socklen_t) !usize {
    if (comptime builtin.os.tag == .windows) {
        const ws2 = std.os.windows.ws2_32;
        const rc = ws2.sendto(fd, buf.ptr, @intCast(buf.len), @intCast(flags), dest_addr, @intCast(addrlen));
        if (rc < 0) {
            const err = ws2.WSAGetLastError();
            return switch (err) {
                .WSAEWOULDBLOCK => error.WouldBlock,
                .WSAECONNRESET => error.ConnectionResetByPeer,
                else => error.Unexpected,
            };
        }
        return @intCast(rc);
    } else {
        return posix.sendto(fd, buf, flags, dest_addr, addrlen);
    }
}

/// Cross-platform writev (gather write). On Windows, uses WSASend.
pub fn writev(fd: posix.socket_t, iovs: []const posix.iovec_const) !usize {
    if (comptime builtin.os.tag == .windows) {
        const ws2 = std.os.windows.ws2_32;
        var wsabufs: [16]ws2.WSABUF = undefined;
        const count: u32 = @intCast(@min(iovs.len, 16));
        for (0..count) |i| {
            wsabufs[i] = .{ .len = @intCast(iovs[i].len), .buf = @constCast(iovs[i].base) };
        }
        var bytes_sent: u32 = 0;
        const rc = ws2.WSASend(fd, &wsabufs, count, &bytes_sent, 0, null, null);
        if (rc != 0) {
            const err = ws2.WSAGetLastError();
            return switch (err) {
                .WSAEWOULDBLOCK => error.WouldBlock,
                .WSAECONNRESET => error.ConnectionResetByPeer,
                .WSAECONNABORTED => error.BrokenPipe,
                else => error.Unexpected,
            };
        }
        return bytes_sent;
    } else {
        return posix.writev(fd, iovs);
    }
}

/// Cross-platform readv (scatter read). On Windows, uses WSARecv.
pub fn readv(fd: posix.socket_t, iovs: []const posix.iovec) !usize {
    if (comptime builtin.os.tag == .windows) {
        const ws2 = std.os.windows.ws2_32;
        var wsabufs: [16]ws2.WSABUF = undefined;
        const count: u32 = @intCast(@min(iovs.len, 16));
        for (0..count) |i| {
            wsabufs[i] = .{ .len = @intCast(iovs[i].len), .buf = iovs[i].base };
        }
        var bytes_recv: u32 = 0;
        var flags: u32 = 0;
        const rc = ws2.WSARecv(fd, &wsabufs, count, &bytes_recv, &flags, null, null);
        if (rc != 0) {
            const err = ws2.WSAGetLastError();
            return switch (err) {
                .WSAEWOULDBLOCK => error.WouldBlock,
                .WSAECONNRESET => error.ConnectionResetByPeer,
                .WSAECONNABORTED => error.ConnectionResetByPeer,
                else => error.Unexpected,
            };
        }
        return bytes_recv;
    } else {
        return posix.readv(fd, iovs);
    }
}

/// Cross-platform setsockopt. On Windows, uses ws2_32 directly to avoid libc dependency.
pub fn setsockopt(fd: posix.socket_t, level: i32, opt: u32, value: []const u8) !void {
    if (comptime builtin.os.tag == .windows) {
        const ws2 = std.os.windows.ws2_32;
        const rc = ws2.setsockopt(fd, level, @intCast(opt), value.ptr, @intCast(value.len));
        if (rc != 0) return error.Unexpected;
    } else {
        try posix.setsockopt(fd, level, opt, value);
    }
}

pub fn setBoolOption(fd: posix.socket_t, level: i32, opt: u32, value: bool) !void {
    const v: u32 = if (value) 1 else 0;
    try setsockopt(fd, level, opt, &mem.toBytes(v));
}

pub fn getBoolOption(fd: posix.socket_t, level: i32, opt: u32) !bool {
    var buf: [4]u8 = undefined;
    try getsockopt(fd, level, opt, &buf);
    return mem.readInt(u32, &buf, .little) != 0;
}

pub fn setIntOption(fd: posix.socket_t, level: i32, opt: u32, value: u32) !void {
    try setsockopt(fd, level, opt, &mem.toBytes(value));
}

pub fn getIntOption(fd: posix.socket_t, level: i32, opt: u32) !u32 {
    var buf: [4]u8 = undefined;
    try getsockopt(fd, level, opt, &buf);
    return mem.readInt(u32, &buf, .little);
}

pub fn setNonBlocking(fd: posix.socket_t, value: bool) !void {
    if (comptime builtin.os.tag == .windows) {
        // Windows uses ioctlsocket with FIONBIO
        var mode: u32 = if (value) 1 else 0;
        const rc = std.os.windows.ws2_32.ioctlsocket(fd, @bitCast(@as(i32, std.os.windows.ws2_32.FIONBIO)), &mode);
        if (rc != 0) return error.Unexpected;
    } else {
        const flags = try posix.fcntl(fd, posix.F.GETFL, 0);
        const new_flags = if (value)
            flags | @as(u32, @bitCast(posix.O{ .NONBLOCK = true }))
        else
            flags & ~@as(u32, @bitCast(posix.O{ .NONBLOCK = true }));

        _ = try posix.fcntl(fd, posix.F.SETFL, new_flags);
    }
}

pub fn waitForConnect(sock_fd: posix.socket_t) !void {
    if (comptime builtin.os.tag == .windows) {
        // Windows: use select() for connect completion
        // For now, connect is synchronous on Windows (IOCP handles async)
        var err_buf: [4]u8 = undefined;
        getsockopt(sock_fd, posix.SOL.SOCKET, posix.SO.ERROR, &err_buf) catch return;
        const err = mem.readInt(u32, &err_buf, .little);
        if (err != 0) return error.ConnectionRefused;
    } else {
        // POSIX: poll for writability (connect complete)
        var pfd = [_]posix.pollfd{
            .{
                .fd = sock_fd,
                .events = posix.POLL.OUT,
                .revents = 0,
            },
        };

        const timeout_ms = 30000; // 30 second timeout
        const n = try posix.poll(&pfd, timeout_ms);

        if (n == 0) return error.TimedOut;

        // Check for connection error
        var err_buf: [4]u8 = undefined;
        try getsockopt(sock_fd, posix.SOL.SOCKET, posix.SO.ERROR, &err_buf);
        const err = mem.readInt(u32, &err_buf, .little);

        if (err != 0) {
            return posix.unexpectedErrno(@enumFromInt(err));
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Future Waker Bridge Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Update a stored Future Waker from poll context.
/// Clones the new waker only if it differs from the stored one.
pub fn updateStoredWaker(slot: *?FutureWaker, ctx: *FutureContext) void {
    const new_waker = ctx.getWaker();
    if (slot.*) |*old| {
        if (!old.willWakeSame(new_waker)) {
            old.deinit();
            slot.* = new_waker.clone();
        }
    } else {
        slot.* = new_waker.clone();
    }
}

/// Clean up a stored Future Waker.
pub fn cleanupStoredWaker(slot: *?FutureWaker) void {
    if (slot.*) |*w| {
        w.deinit();
        slot.* = null;
    }
}

/// Create a backend Waker that bridges to a stored Future Waker.
pub fn bridgeWaker(slot: *?FutureWaker) Waker {
    return .{
        .context = @ptrCast(slot),
        .wake_fn = bridgeWakeFn,
    };
}

fn bridgeWakeFn(ctx: *anyopaque) void {
    const slot: *?FutureWaker = @ptrCast(@alignCast(ctx));
    if (slot.*) |*w| {
        w.wakeByRef();
    }
}
