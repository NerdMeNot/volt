//! v0.2 TCP integration tests — TcpListener accept + TcpStream connect/read/write
//! all running as coroutines under the kqueue reactor.

const std = @import("std");
const volt = @import("../lib.zig");

// ─────────────────────────────────────────────────────────────────────────────
// Loopback echo: client sends "ping", server replies "pong".
// ─────────────────────────────────────────────────────────────────────────────

const EchoCtx = struct {
    listener: *volt.io.TcpListener,
    server_received: [16]u8 = undefined,
    server_received_len: usize = 0,
    client_received: [16]u8 = undefined,
    client_received_len: usize = 0,
};

fn echoServer(ctx: *EchoCtx) !void {
    var conn = try ctx.listener.accept();
    defer conn.close();

    const n = try conn.read(&ctx.server_received);
    ctx.server_received_len = n;

    try conn.writeAll("pong");
}

fn echoClient(ctx: *EchoCtx, addr: volt.io.Address) !void {
    var stream = try volt.io.TcpStream.connect(addr);
    defer stream.close();

    try stream.writeAll("ping");

    const n = try stream.read(&ctx.client_received);
    ctx.client_received_len = n;
}

fn echoRoot() !EchoCtx {
    // Bind to 127.0.0.1:0 — kernel picks a free port.
    const localhost = volt.io.Address.loopback4(0);
    var listener = try volt.io.TcpListener.bind(localhost);
    defer listener.close();

    const bound_addr = try listener.localAddress();

    var ctx = EchoCtx{ .listener = &listener };

    var server = try volt.spawn(echoServer, .{&ctx});
    defer volt.destroyTask(server);
    var client = try volt.spawn(echoClient, .{ &ctx, bound_addr });
    defer volt.destroyTask(client);

    try server.join();
    try client.join();

    return ctx;
}

test "v0.2: TCP loopback echo (TcpListener + TcpStream + reactor)" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, echoRoot, .{});
    try std.testing.expectEqualStrings("ping", ctx.server_received[0..ctx.server_received_len]);
    try std.testing.expectEqualStrings("pong", ctx.client_received[0..ctx.client_received_len]);
}

// ─────────────────────────────────────────────────────────────────────────────
// P3.x.6 — TcpStream depth coverage: sockopts, shutdown, readv/writev.
// ─────────────────────────────────────────────────────────────────────────────

const SockoptCtx = struct {
    listener: *volt.net.TcpListener,
    server_stream: ?volt.net.TcpStream = null,
};

fn sockoptAccept(ctx: *SockoptCtx) !void {
    ctx.server_stream = try ctx.listener.accept();
}

fn sockoptRoot() !void {
    var listener = try volt.net.TcpListener.bind(volt.net.Address.loopback4(0));
    defer listener.close();
    const addr = try listener.localAddress();

    var ctx = SockoptCtx{ .listener = &listener };
    var accept_task = try volt.spawn(sockoptAccept, .{&ctx});
    defer volt.destroyTask(accept_task);

    var client = try volt.net.TcpStream.connect(addr);
    defer client.close();
    try accept_task.join();
    var server = ctx.server_stream.?;
    defer server.close();

    try client.setNoDelay(true);
    try client.setKeepAlive(true);
    try client.setKeepAliveParams(volt.Duration.fromSecs(60), volt.Duration.fromSecs(10), 5);
    try client.setLinger(null);
    try client.setLinger(volt.Duration.fromSecs(2));
    try client.setRecvBufSize(64 * 1024);
    try client.setSendBufSize(64 * 1024);
}

test "P3.x.6: TcpStream.setNoDelay / setKeepAlive / setLinger smoke" {
    try volt.run(.{ .allocator = std.testing.allocator }, sockoptRoot, .{});
}

const ShutdownCtx = struct {
    listener: *volt.net.TcpListener,
    server_stream: ?volt.net.TcpStream = null,
    n1: usize = 0,
    n2: usize = 0,
    received: [16]u8 = undefined,
};

fn shutdownAccept(ctx: *ShutdownCtx) !void {
    ctx.server_stream = try ctx.listener.accept();
}

fn shutdownRoot() !ShutdownCtx {
    var listener = try volt.net.TcpListener.bind(volt.net.Address.loopback4(0));
    defer listener.close();
    const addr = try listener.localAddress();

    var ctx = ShutdownCtx{ .listener = &listener };
    var accept_task = try volt.spawn(shutdownAccept, .{&ctx});
    defer volt.destroyTask(accept_task);

    var client = try volt.net.TcpStream.connect(addr);
    defer client.close();
    try accept_task.join();
    var server = ctx.server_stream.?;
    defer server.close();

    try client.writeAll("hello");
    try client.shutdown(.write);
    ctx.n1 = try server.read(&ctx.received);
    ctx.n2 = try server.read(&ctx.received);
    return ctx;
}

test "P3.x.6: TcpStream.shutdown(.write) yields EOF on peer" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, shutdownRoot, .{});
    try std.testing.expectEqual(@as(usize, 5), ctx.n1);
    try std.testing.expectEqualStrings("hello", ctx.received[0..ctx.n1]);
    try std.testing.expectEqual(@as(usize, 0), ctx.n2);
}

const VectorCtx = struct {
    listener: *volt.net.TcpListener,
    server_stream: ?volt.net.TcpStream = null,
    total_received: usize = 0,
    combined: [24]u8 = undefined,
};

fn vectorAccept(ctx: *VectorCtx) !void {
    ctx.server_stream = try ctx.listener.accept();
}

fn vectorRoot() !VectorCtx {
    var listener = try volt.net.TcpListener.bind(volt.net.Address.loopback4(0));
    defer listener.close();
    const addr = try listener.localAddress();

    var ctx = VectorCtx{ .listener = &listener };
    var accept_task = try volt.spawn(vectorAccept, .{&ctx});
    defer volt.destroyTask(accept_task);

    var client = try volt.net.TcpStream.connect(addr);
    defer client.close();
    try accept_task.join();
    var server = ctx.server_stream.?;
    defer server.close();

    const a = "alpha-";
    const b = "bravo-";
    const c = "charlie";
    const iovs = [_]std.posix.iovec_const{
        .{ .base = a.ptr, .len = a.len },
        .{ .base = b.ptr, .len = b.len },
        .{ .base = c.ptr, .len = c.len },
    };
    _ = try client.writev(&iovs);

    var buf1: [12]u8 = undefined;
    var buf2: [12]u8 = undefined;
    var iovs_in = [_]std.posix.iovec{
        .{ .base = &buf1, .len = buf1.len },
        .{ .base = &buf2, .len = buf2.len },
    };
    ctx.total_received = try server.readv(&iovs_in);
    @memcpy(ctx.combined[0..buf1.len], &buf1);
    @memcpy(ctx.combined[buf1.len..][0..buf2.len], &buf2);
    return ctx;
}

test "P3.x.6: TcpStream.writev / readv vectored I/O" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, vectorRoot, .{});
    try std.testing.expectEqual(@as(usize, 19), ctx.total_received);
    try std.testing.expectEqualStrings("alpha-bravo-charlie", ctx.combined[0..19]);
}
