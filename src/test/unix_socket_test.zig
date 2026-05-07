//! P2.D — Unix domain socket integration tests.
//!
//! Loopback exchange across two coroutines for both stream and
//! datagram variants. Path is derived from the test name + pid so
//! parallel runs don't collide.

const std = @import("std");
const volt = @import("../lib.zig");
const syscall = volt.internal.syscall;

fn unique_path(buf: []u8, prefix: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}-{d}.sock", .{ prefix, std.posix.system.getpid() });
}

// ─────────────────────────────────────────────────────────────────────
// Test 1 — UnixListener / UnixStream loopback
// ─────────────────────────────────────────────────────────────────────

const StreamCtx = struct {
    listener_path: []const u8,
    received: [32]u8 = undefined,
    received_len: usize = 0,
};

fn streamServer(listener: *volt.net.UnixListener, ctx: *StreamCtx) !void {
    var stream = try listener.accept();
    defer stream.close();
    var buf: [64]u8 = undefined;
    const n = try stream.read(&buf);
    @memcpy(ctx.received[0..n], buf[0..n]);
    ctx.received_len = n;
}

fn streamClient(path: []const u8) !void {
    try volt.yield();
    const addr = try volt.net.UnixAddress.fromPath(path);
    var stream = try volt.net.UnixStream.connect(addr);
    defer stream.close();
    try stream.writeAll("unix-loopback");
}

fn streamRoot(path: []const u8) !StreamCtx {
    // Best-effort cleanup of stale socket files from prior runs.
    var path_z_buf: [128]u8 = undefined;
    const path_z = try std.fmt.bufPrintZ(&path_z_buf, "{s}", .{path});
    _ = std.posix.system.unlink(path_z.ptr);

    const addr = try volt.net.UnixAddress.fromPath(path);
    var listener = try volt.net.UnixListener.bind(addr);
    defer listener.close();
    defer _ = std.posix.system.unlink(path_z.ptr);

    var ctx = StreamCtx{ .listener_path = path };
    var server = try volt.spawn(streamServer, .{ &listener, &ctx });
    defer volt.destroyTask(server);
    var client = try volt.spawn(streamClient, .{path});
    defer volt.destroyTask(client);

    try server.join();
    try client.join();
    return ctx;
}

test "P2.D: UnixStream loopback round-trip" {
    var path_buf: [128]u8 = undefined;
    const path = try unique_path(&path_buf, "/tmp/volt-stream");
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, streamRoot, .{path});
    try std.testing.expectEqualStrings("unix-loopback", ctx.received[0..ctx.received_len]);
}

// ─────────────────────────────────────────────────────────────────────
// Test 2 — UnixDatagram sendTo / recvFrom
// ─────────────────────────────────────────────────────────────────────

const DgramCtx = struct {
    server_path: []const u8,
    client_path: []const u8,
    received: [32]u8 = undefined,
    received_len: usize = 0,
};

fn dgramServer(sock: *volt.net.UnixDatagram, ctx: *DgramCtx) !void {
    var buf: [64]u8 = undefined;
    const dgram = try sock.recvFrom(&buf);
    @memcpy(ctx.received[0..dgram.len], buf[0..dgram.len]);
    ctx.received_len = dgram.len;
}

fn dgramClient(server_path: []const u8) !void {
    try volt.yield();
    var path_buf: [128]u8 = undefined;
    const cpath = try std.fmt.bufPrint(&path_buf, "/tmp/volt-dgram-c-{d}.sock", .{std.posix.system.getpid()});
    var path_z_buf: [128]u8 = undefined;
    const cpath_z = try std.fmt.bufPrintZ(&path_z_buf, "{s}", .{cpath});
    _ = std.posix.system.unlink(cpath_z.ptr);
    defer _ = std.posix.system.unlink(cpath_z.ptr);

    const client_addr = try volt.net.UnixAddress.fromPath(cpath);
    var client_sock = try volt.net.UnixDatagram.bind(client_addr);
    defer client_sock.close();

    const server_addr = try volt.net.UnixAddress.fromPath(server_path);
    _ = try client_sock.sendTo("dgram-loop", server_addr);
}

fn dgramRoot(server_path: []const u8) !DgramCtx {
    var path_z_buf: [128]u8 = undefined;
    const path_z = try std.fmt.bufPrintZ(&path_z_buf, "{s}", .{server_path});
    _ = std.posix.system.unlink(path_z.ptr);

    const server_addr = try volt.net.UnixAddress.fromPath(server_path);
    var server_sock = try volt.net.UnixDatagram.bind(server_addr);
    defer server_sock.close();
    defer _ = std.posix.system.unlink(path_z.ptr);

    var ctx = DgramCtx{ .server_path = server_path, .client_path = "" };
    var server = try volt.spawn(dgramServer, .{ &server_sock, &ctx });
    defer volt.destroyTask(server);
    var client = try volt.spawn(dgramClient, .{server_path});
    defer volt.destroyTask(client);

    try server.join();
    try client.join();
    return ctx;
}

// ─────────────────────────────────────────────────────────────────────
// P3.x.6 — UnixStream shutdown / readv / writev via socketpair
// ─────────────────────────────────────────────────────────────────────

const UnixDepthCtx = struct {
    a: volt.net.UnixStream,
    b: volt.net.UnixStream,
    eof_seen: bool = false,
    written_via_v: usize = 0,
    read_via_v: usize = 0,
    combined: [24]u8 = undefined,
};

fn unixDepthRoot(ctx: *UnixDepthCtx) !void {
    var fds: [2]std.posix.fd_t = undefined;
    const rc = std.c.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds);
    if (rc != 0) return error.SocketPairFailed;
    try volt.io.lowlevel.setNonblock(fds[0]);
    try volt.io.lowlevel.setNonblock(fds[1]);
    ctx.a = .{ .fd = fds[0] };
    ctx.b = .{ .fd = fds[1] };
    defer ctx.a.close();
    defer ctx.b.close();

    // 1. writev / readv vectored I/O.
    const x = "uno-";
    const y = "dos-";
    const z = "tres";
    const iovs = [_]std.posix.iovec_const{
        .{ .base = x.ptr, .len = x.len },
        .{ .base = y.ptr, .len = y.len },
        .{ .base = z.ptr, .len = z.len },
    };
    ctx.written_via_v = try ctx.a.writev(&iovs);

    var buf1: [8]u8 = undefined;
    var buf2: [8]u8 = undefined;
    var iovs_in = [_]std.posix.iovec{
        .{ .base = &buf1, .len = buf1.len },
        .{ .base = &buf2, .len = buf2.len },
    };
    ctx.read_via_v = try ctx.b.readv(&iovs_in);
    @memcpy(ctx.combined[0..buf1.len], &buf1);
    @memcpy(ctx.combined[buf1.len..][0..buf2.len], &buf2);

    // 2. shutdown(.write) — peer's next read sees EOF.
    try ctx.a.shutdown(.write);
    var eof_buf: [8]u8 = undefined;
    const n = try ctx.b.read(&eof_buf);
    ctx.eof_seen = (n == 0);
}

test "P3.x.6: UnixStream writev / readv / shutdown" {
    var ctx = UnixDepthCtx{ .a = undefined, .b = undefined };
    try volt.run(.{ .allocator = std.testing.allocator }, unixDepthRoot, .{&ctx});
    try std.testing.expectEqual(@as(usize, 12), ctx.written_via_v);
    try std.testing.expectEqual(@as(usize, 12), ctx.read_via_v);
    try std.testing.expectEqualStrings("uno-dos-tres", ctx.combined[0..12]);
    try std.testing.expect(ctx.eof_seen);
}

test "P2.D: UnixDatagram sendTo / recvFrom round-trip" {
    var path_buf: [128]u8 = undefined;
    const server_path = try unique_path(&path_buf, "/tmp/volt-dgram-s");
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, dgramRoot, .{server_path});
    try std.testing.expectEqualStrings("dgram-loop", ctx.received[0..ctx.received_len]);
}
