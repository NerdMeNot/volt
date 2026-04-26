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
    const ctx = try volt.run(std.testing.allocator, echoRoot, .{});
    try std.testing.expectEqualStrings("ping", ctx.server_received[0..ctx.server_received_len]);
    try std.testing.expectEqualStrings("pong", ctx.client_received[0..ctx.client_received_len]);
}
