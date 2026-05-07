//! P2.C — UdpSocket integration tests.
//!
//! Loopback IPv4 sendTo/recvFrom round-trip across two coroutines.
//! Multicast tests require a network interface available in the
//! test environment, so they're left for a follow-up.

const std = @import("std");
const volt = @import("../lib.zig");

const SendCtx = struct {
    receiver_addr: volt.net.Address,
    received: [32]u8 = undefined,
    received_len: usize = 0,
};

fn receiverCoro(sock: *volt.net.UdpSocket, ctx: *SendCtx) !void {
    var buf: [64]u8 = undefined;
    const dgram = try sock.recvFrom(&buf);
    @memcpy(ctx.received[0..dgram.len], buf[0..dgram.len]);
    ctx.received_len = dgram.len;
}

fn senderCoro(target: volt.net.Address) !void {
    var sender = try volt.net.UdpSocket.bind(volt.net.Address.loopback4(0));
    defer sender.close();
    // Yield once so the receiver's recvFrom has registered on the
    // reactor first.
    try volt.yield();
    _ = try sender.sendTo("hello-udp", target);
}

fn udpRoundtripRoot() !SendCtx {
    var receiver = try volt.net.UdpSocket.bind(volt.net.Address.loopback4(0));
    defer receiver.close();
    const local = try receiver.localAddress();

    var ctx = SendCtx{ .receiver_addr = local };

    var recv_task = try volt.spawn(receiverCoro, .{ &receiver, &ctx });
    defer volt.destroyTask(recv_task);
    var send_task = try volt.spawn(senderCoro, .{local});
    defer volt.destroyTask(send_task);

    try recv_task.join();
    try send_task.join();
    return ctx;
}

test "P2.C: UDP loopback sendTo/recvFrom round-trip" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, udpRoundtripRoot, .{});
    try std.testing.expectEqual(@as(usize, 9), ctx.received_len);
    try std.testing.expectEqualStrings("hello-udp", ctx.received[0..ctx.received_len]);
}

// ─────────────────────────────────────────────────────────────────────
// P3.x.6 — UDP connect / send / recv (post-connect API)
// ─────────────────────────────────────────────────────────────────────

const ConnectedDgramCtx = struct {
    receiver_addr: volt.net.Address,
    received: [32]u8 = undefined,
    received_len: usize = 0,
};

fn connectedReceiver(sock: *volt.net.UdpSocket, ctx: *ConnectedDgramCtx) !void {
    var buf: [64]u8 = undefined;
    const dgram = try sock.recvFrom(&buf);
    @memcpy(ctx.received[0..dgram.len], buf[0..dgram.len]);
    ctx.received_len = dgram.len;
}

fn connectedSender(target: volt.net.Address) !void {
    var sender = try volt.net.UdpSocket.bind(volt.net.Address.loopback4(0));
    defer sender.close();
    try sender.connect(target);
    try volt.yield();
    // After connect: send / recv replace sendTo / recvFrom.
    _ = try sender.send("hello-connected");
}

fn connectedRoot() !ConnectedDgramCtx {
    var receiver = try volt.net.UdpSocket.bind(volt.net.Address.loopback4(0));
    defer receiver.close();
    const local = try receiver.localAddress();

    var ctx = ConnectedDgramCtx{ .receiver_addr = local };

    var recv_task = try volt.spawn(connectedReceiver, .{ &receiver, &ctx });
    defer volt.destroyTask(recv_task);
    var send_task = try volt.spawn(connectedSender, .{local});
    defer volt.destroyTask(send_task);

    try recv_task.join();
    try send_task.join();
    return ctx;
}

test "P3.x.6: UDP connect + send / recv (post-connect API)" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, connectedRoot, .{});
    try std.testing.expectEqualStrings("hello-connected", ctx.received[0..ctx.received_len]);
}

test "P2.C: UDP setBroadcast / setMulticastTtl don't error on a fresh socket" {
    // Smoke test: setsockopt paths reach the kernel without surprising errors.
    // Doesn't actually exercise multicast traffic — that needs a routable
    // interface available to the runner.
    const root = struct {
        fn run() !void {
            var sock = try volt.net.UdpSocket.bind(volt.net.Address.loopback4(0));
            defer sock.close();
            try sock.setBroadcast(true);
            try sock.setMulticastTtl(8);
            try sock.setMulticastLoopback(true);
        }
    }.run;
    try volt.run(.{ .allocator = std.testing.allocator }, root, .{});
}
