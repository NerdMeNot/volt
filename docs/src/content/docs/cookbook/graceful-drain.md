---
title: Graceful Drain on Shutdown
description: Let active requests finish before the program exits — no killed connections, no half-written responses.
---

The "right" shutdown semantic for a server: stop accepting new
work, let active work finish, then exit. Volt makes this clean
with `volt.scope`.

## The pattern

```zig
const std = @import("std");
const volt = @import("volt");

fn serve() !void {
    var listener = try volt.io.TcpListener.bind(volt.io.Address.any4(8080));
    defer listener.close();

    try volt.scope(struct {
        fn body(s: *volt.Scope) !void {
            var shutdown = try volt.signal.shutdown();
            defer shutdown.deinit();

            while (true) {
                if (shutdown.handler.read()) |_| {
                    std.debug.print("draining...\n", .{});
                    return;
                } else |_| {}

                const conn = listener.accept() catch |err| switch (err) {
                    error.Cancelled => return,
                    else => return err,
                };
                try s.spawn(handle, .{conn});
            }
        }
    }.body);
    // Past this point: scope has joined every spawned handler.
    std.debug.print("drained; bye\n", .{});
}

fn handle(conn: volt.io.TcpStream) void {
    var s = conn;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;
        if (n == 0) return;
        s.writeAll(buf[0..n]) catch return;
    }
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, serve, .{});
}
```

## Why it works

The trick is **the scope is the drain point.** When `body` returns
(because the shutdown signal fired), `volt.scope`:

1. Stops; the accept loop is gone.
2. Joins every spawned handler.

Joining doesn't *cancel* the handlers — they keep running until
their own `s.read` returns 0 (peer closed) or they hit an error.
The scope just waits.

If you want to add a max-drain time so a buggy client can't keep
the server alive forever, wrap the scope itself:

```zig
volt.withTimeout(volt.Duration.fromSecs(30), serveDrain, .{}) catch |err| switch (err) {
    error.Timeout => std.debug.print("drain exceeded 30s; killing remaining\n", .{}),
    else => return err,
};
```

After 30 seconds, the timeout cancels everything inside —
including the scope's still-running handlers. They surface
`error.Cancelled` from whatever they were doing, unwind, the
scope finishes.

## Variant: drain via Notify

If you don't want signals (e.g., shutdown comes from an admin RPC):

```zig
var quit: volt.sync.Notify = .{};

// Server:
try volt.scope(struct {
    fn body(s: *volt.Scope) !void {
        try s.spawn(acceptLoop, .{ &listener, s });
        try quit.wait();      // park until shutdown signaled
        // Returning here drains the scope.
    }
}.body);

// Elsewhere, when ready to shutdown:
quit.notifyAll();
```

## What you might be tempted to do (and shouldn't)

```zig
// DON'T do this:
fn serve() !void {
    while (running.load(.acquire)) {
        const conn = try listener.accept();
        _ = try volt.launch(handle, .{conn});  // detached
    }
    // ... how do we wait for handlers to finish? ...
}
```

`volt.launch` returns a `*Job` you'd have to track yourself if you
wanted to join. You'd build a list, iterate `j.join()`. That's
manual `volt.scope` reimplemented.

The scope-based pattern at the top of this page is structurally
guaranteed to drain. There's no list to track and no way to forget
a handler.
