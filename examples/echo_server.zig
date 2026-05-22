//! TCP echo server — one coroutine per connection.
//!
//! Demonstrates: Runtime bootstrap, listener accept loop, per-connection
//! spawn, sync-shape read/write, graceful drain via runWithSignals.
//!
//! Run:  zig build run-echo-server
//! Test: echo "hello volt" | nc 127.0.0.1 8080

const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try rt.runWithSignals(serve, .{});
}

fn serve(c: *volt.Cancel) !void {
    var listener = try volt.net.TcpListener.bind(.loopback4(8080));
    defer listener.close();
    std.debug.print("echo server listening on 127.0.0.1:8080 (Ctrl-C to stop)\n", .{});

    while (!c.isFired()) {
        // accept blocks on read-readable; on Ctrl-C the SIGINT
        // handler fires `c`, the existing reactor-cancel path
        // would wake us — but `accept` here uses the non-cancel
        // path, so a tighter shutdown story will land alongside
        // a `acceptCancel` API. For now, hitting Ctrl-C exits the
        // listener loop on the NEXT cycle (after one final accept).
        const conn = listener.accept() catch break;
        _ = volt.spawn(handle, .{conn}) catch |e| {
            std.debug.print("spawn failed: {s}\n", .{@errorName(e)});
        };
    }
    std.debug.print("echo server: shutting down\n", .{});
}

fn handle(conn: volt.net.TcpStream) void {
    var s = conn;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;
        if (n == 0) return;
        s.writeAll(buf[0..n]) catch return;
    }
}
