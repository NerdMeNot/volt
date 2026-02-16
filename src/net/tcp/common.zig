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
// Socket Helpers
// ═══════════════════════════════════════════════════════════════════════════════

pub fn setBoolOption(fd: posix.socket_t, level: i32, opt: u32, value: bool) !void {
    const v: u32 = if (value) 1 else 0;
    try posix.setsockopt(fd, level, opt, &mem.toBytes(v));
}

pub fn getBoolOption(fd: posix.socket_t, level: i32, opt: u32) !bool {
    var buf: [4]u8 = undefined;
    try posix.getsockopt(fd, level, opt, &buf);
    return mem.readInt(u32, &buf, .little) != 0;
}

pub fn setIntOption(fd: posix.socket_t, level: i32, opt: u32, value: u32) !void {
    try posix.setsockopt(fd, level, opt, &mem.toBytes(value));
}

pub fn getIntOption(fd: posix.socket_t, level: i32, opt: u32) !u32 {
    var buf: [4]u8 = undefined;
    try posix.getsockopt(fd, level, opt, &buf);
    return mem.readInt(u32, &buf, .little);
}

pub fn setNonBlocking(fd: posix.socket_t, value: bool) !void {
    const flags = try posix.fcntl(fd, posix.F.GETFL, 0);
    const new_flags = if (value)
        flags | @as(u32, @bitCast(posix.O{ .NONBLOCK = true }))
    else
        flags & ~@as(u32, @bitCast(posix.O{ .NONBLOCK = true }));

    _ = try posix.fcntl(fd, posix.F.SETFL, new_flags);
}

pub fn waitForConnect(sock_fd: posix.socket_t) !void {
    // Poll for writability (connect complete)
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
    try posix.getsockopt(sock_fd, posix.SOL.SOCKET, posix.SO.ERROR, &err_buf);
    const err = mem.readInt(u32, &err_buf, .little);

    if (err != 0) {
        return posix.unexpectedErrno(@enumFromInt(err));
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
