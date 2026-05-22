---
title: UDP echo
description: A UDP echo server + client. Demonstrates UdpSocket.bind / recvFrom / sendTo and the connected mode with .connect / .send / .recv.
---

UDP is connection-less by default — `recvFrom` returns the source
address per packet so you know who to reply to:

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}

fn root() !void {
    var server = try volt.net.UdpSocket.bind(volt.net.Address.loopback4(0));
    defer server.close();
    const addr = try server.localAddress();

    var server_task = try volt.spawn(serverLoop, .{&server});
    var ctx = ClientCtx{ .server_addr = addr };
    var client_task = try volt.spawn(clientSend, .{&ctx});

    _ = try (client_task.join());
    server.close(); // make recvFrom error so the server task exits cleanly
    _ = server_task.join();
}

fn serverLoop(server: *volt.net.UdpSocket) void {
    var buf: [128]u8 = undefined;
    while (true) {
        const r = server.recvFrom(&buf) catch return;
        _ = server.sendTo(buf[0..r.len], r.addr) catch return;
    }
}

const ClientCtx = struct { server_addr: volt.net.Address };

fn clientSend(ctx: *ClientCtx) !void {
    var sock = try volt.net.UdpSocket.unbound();
    defer sock.close();
    try sock.connect(ctx.server_addr);  // sets default peer
    _ = try sock.send("ping");
    var reply: [128]u8 = undefined;
    const n = try sock.recv(&reply);
    std.debug.print("got: {s}\n", .{reply[0..n]});
}
```

## Connected vs unconnected

Two modes:

- **Unconnected** (server style): `recvFrom` / `sendTo` per packet,
  carry the address explicitly. Right for servers serving many
  peers.
- **Connected** (`.connect(addr)` first): `send` / `recv` like a
  byte-stream. Right for clients talking to one peer. UDP is still
  connectionless under the hood — `.connect` just stashes the
  default peer in kernel state.

## Cancellation

Every blocking op has a `*Cancel`-suffixed variant:

```zig
var c = volt.Cancel.init(rt);
defer c.deinit();
// ... fire from a side coro after timeout etc. ...
const r = try sock.recvCancel(&buf, &c);  // returns error.Cancelled on fire
```

## Multicast

`joinMulticast4` / `joinMulticast6` join an IPv4 / IPv6 multicast
group; `setMulticastTtl4` / `setMulticastLoop4` (and v6
equivalents) set the per-group options.

```zig
try sock.joinMulticast4(.{224, 0, 0, 1}, .{0, 0, 0, 0});  // default iface
try sock.setMulticastTtl4(1);                              // link-local
try sock.setMulticastLoop4(true);                          // receive own sends
```

## See also

- [`examples/udp_echo.zig`](https://github.com/NerdMeNot/blitz-io/tree/main/examples/udp_echo.zig) — the runnable version of this.
- [TCP echo recipe](/cookbook/echo-server/) — same shape over a connection-oriented socket.
