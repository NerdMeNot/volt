---
title: Fan Out, Take First Answer
description: Race N tasks, return whichever finishes first, fire a Cancel so the losers wake out of any blocking op cleanly.
---

A common pattern in distributed systems: query N replicas, return
the fastest answer, abandon the rest. Volt's `scope` + `Oneshot` +
`Cancel` make this clean.

## The pattern

```zig
const std = @import("std");
const volt = @import("volt");

const Result = struct { backend: u32, value: u32 };

const Ctx = struct {
    winner: *volt.Oneshot(Result),
    cancel: *volt.Cancel,
};

fn backend(ctx: *Ctx, id: u32, latency_ms: u32) void {
    // Parking on sleep is *not* cancel-aware today, so we do a
    // best-effort check before and after. A future cancel-aware
    // sleep variant would make this trivial.
    if (ctx.cancel.isFired()) return;
    volt.sleep(@as(u64, latency_ms) * std.time.ns_per_ms);
    if (ctx.cancel.isFired()) return;
    _ = ctx.winner.send(.{ .backend = id, .value = id * 100 }) catch {};
}

fn root() !void {
    var os: volt.Oneshot(Result) = .{};

    try volt.scope(struct {
        fn body(c: *volt.Cancel) anyerror!void {
            var ctx = Ctx{ .winner = &os, .cancel = c };

            const t1 = try volt.spawn(backend, .{ &ctx, @as(u32, 1), @as(u32, 80)  });
            const t2 = try volt.spawn(backend, .{ &ctx, @as(u32, 2), @as(u32, 30)  });
            const t3 = try volt.spawn(backend, .{ &ctx, @as(u32, 3), @as(u32, 120) });

            const winner = try os.recv();
            std.debug.print("winner: backend {d} -> {d}\n", .{
                winner.backend, winner.value,
            });

            // Cancel the rest, then join.
            c.fire();
            os.close();
            t1.join();
            t2.join();
            t3.join();
        }
    }.body);
}

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}
```

## Why it works

**The Oneshot is the race resolver.** All three backends call
`ctx.winner.send`. The first call wins; the second and third get
`error.Closed` (swallowed by the `catch {}`). So whichever backend
finishes first is the only one whose value lands in the Oneshot.

**The Cancel is the loser-cleanup signal.** When the winner has
been received, the body calls `c.fire()`. Any cancel-aware
blocking op on a backend coroutine would wake with
`error.Cancelled`. The `isFired()` checkpoint in `backend` handles
the case where the sleep hasn't fired yet — backends bail without
trying to `send`.

**The scope owns lifetime.** When `body` returns OK, the spawned
Tasks have all been joined; when `body` errors, `scope` fires the
Cancel before propagating. Either way, no leaked coroutines.

**Joining each Task** is necessary because `volt.spawn` returns
`*Task(T)` which you're responsible for freeing via `join`. The
joins here happen after `c.fire()` so they're effectively waiting
for clean shutdown of the losers.

## Variant: race a real network call

Replace `volt.sleep` with an actual call:

```zig
fn fetchFromBackend(ctx: *Ctx, id: u32, addr: volt.net.Address) void {
    if (ctx.cancel.isFired()) return;
    const stream = volt.net.TcpStream.connect(addr) catch return;
    defer { var s = stream; s.close(); }

    if (ctx.cancel.isFired()) return;
    var s = stream;
    s.writeAll("GET / HTTP/1.0\r\n\r\n") catch return;

    if (ctx.cancel.isFired()) return;
    var buf: [4096]u8 = undefined;
    const n = s.read(&buf) catch return;

    _ = ctx.winner.send(.{
        .backend = id,
        .value = parseResponse(buf[0..n]),
    }) catch {};
}
```

Same shape. `connect` / `writeAll` / `read` all park on the
reactor; closing the TcpStream from the body coroutine would break
them out (the reactor fault path returns an error). A first-class
cancel-aware I/O variant would make the `isFired()` checks
unnecessary.

## Variant: take the first N answers

Use a `Mpmc(T, cap)` instead of a `Oneshot(T)`, and read N times
before closing:

```zig
fn raceN(n_winners: usize) !void {
    try volt.scope(struct {
        fn body(c: *volt.Cancel) anyerror!void {
            var ch = volt.Mpmc(Result, 8).init();
            defer ch.deinit();

            const t1 = try volt.spawn(backendCh, .{ &ch, c, @as(u32, 1) });
            // ... t2, t3, t4, t5 ...

            var got: usize = 0;
            while (got < n_winners) : (got += 1) {
                const r = try ch.recv();
                std.debug.print("answer {d}: backend {d}\n", .{ got, r.backend });
            }

            c.fire();
            ch.close();
            _ = t1.join();
            // ... join the rest ...
        }
    }.body);
}
```

We accept N of M answers and fire the Cancel for the others.

## See also

- [Structured Concurrency](/usage/structured-concurrency/) — Cancel + scope semantics.
- [Channels](/usage/channels/) — Oneshot and Mpmc.
- [Connection Pool](/cookbook/connection-pool/) — when "first wins" is for connection acquisition.
