//! UDP echo — one socket bouncing 4 messages.
//!
//! Demonstrates: UdpSocket.bind, recvFrom, sendTo, unbound client
//! socket with connect + send/recv. Both sides on one runtime.
//!
//! Run: zig build run-udp-echo

const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}

fn root() !void {
    // Server bound to ephemeral loopback port.
    var server = try volt.net.UdpSocket.bind(volt.net.Address.loopback4(0));
    defer server.close();
    const server_addr = try server.localAddress();
    std.debug.print("server listening on {f}\n", .{server_addr});

    var server_task = try volt.spawn(serverLoop, .{&server});
    var ctx = ClientCtx{ .server_addr = server_addr };
    var client_task = try volt.spawn(clientSend, .{&ctx});

    _ = try (client_task.join());
    server.close(); // make recvFrom error so the server task exits
    _ = server_task.join();
}

fn serverLoop(server: *volt.net.UdpSocket) void {
    var i: u32 = 0;
    while (i < 4) : (i += 1) {
        var buf: [128]u8 = undefined;
        const r = server.recvFrom(&buf) catch return;
        std.debug.print("server got {d} bytes from {f}\n", .{ r.len, r.addr });
        _ = server.sendTo(buf[0..r.len], r.addr) catch return;
    }
}

const ClientCtx = struct { server_addr: volt.net.Address };

fn clientSend(ctx: *ClientCtx) !void {
    var sock = try volt.net.UdpSocket.unbound();
    defer sock.close();
    try sock.connect(ctx.server_addr);
    var i: u32 = 0;
    while (i < 4) : (i += 1) {
        var msg_buf: [32]u8 = undefined;
        const msg = try std.fmt.bufPrint(&msg_buf, "ping-{d}", .{i});
        _ = try sock.send(msg);
        var reply: [128]u8 = undefined;
        const n = try sock.recv(&reply);
        std.debug.print("client got reply: {s}\n", .{reply[0..n]});
    }
}
