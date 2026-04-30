//! Cookbook: TCP echo server with graceful shutdown.
//!
//! Demonstrates the canonical Volt server shape:
//!   - `volt.run` for bootstrap.
//!   - `volt.io.TcpListener` accept loop.
//!   - One coroutine per connection (`volt.spawn`).
//!   - `volt.signal.shutdown()` listener that breaks the accept loop
//!     on Ctrl+C / SIGTERM.
//!
//! ```sh
//! zig build run-echo
//! # in another terminal:
//! echo "hello volt" | nc 127.0.0.1 8080
//! # → prints "hello volt" back
//! # Ctrl+C the server: drains active connections, then exits.
//! ```

const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, serve, .{});
}

fn serve() !void {
    volt.coroutine.setCurrentName("echo-server-root");

    var listener = try volt.io.TcpListener.bind(volt.io.Address.loopback4(8080));
    defer listener.close();
    std.debug.print("echo server listening on :8080 (Ctrl+C to stop)\n", .{});

    var shutdown = try volt.signal.shutdown();
    defer shutdown.deinit();

    // The accept loop runs until SIGINT/SIGTERM. `volt.spawn` per
    // connection — the slab-pool keeps spawn cost low.
    while (true) {
        if (shutdown.handler.read()) |_| {
            std.debug.print("\nshutdown signal received; draining\n", .{});
            return;
        } else |_| {}

        const conn = listener.accept() catch |err| switch (err) {
            error.Cancelled => return,
            else => return err,
        };
        _ = try volt.launch(handle, .{conn});
    }
}

fn handle(conn: volt.io.TcpStream) void {
    volt.coroutine.setCurrentName("echo-conn");
    var s = conn;
    defer s.close();

    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;
        if (n == 0) return; // peer closed
        s.writeAll(buf[0..n]) catch return;
    }
}
