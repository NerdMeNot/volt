---
title: Fan Out, Take First Answer
description: Race N backends, return whichever responds first, cancel the losers cleanly.
---

A common pattern in distributed systems: query N replicas, return
the fastest answer, abandon the rest. Volt's `Scope` + `Oneshot`
make this clean.

## The pattern

```zig
const std = @import("std");
const volt = @import("volt");

const Result = struct { backend: u32, value: u32 };

const Ctx = struct {
    winner: *volt.channel.Oneshot(Result),
};

fn backend(ctx: *Ctx, id: u32, latency_ms: u32) void {
    volt.sleep(volt.Duration.fromMillis(latency_ms)) catch return;
    _ = ctx.winner.send(.{ .backend = id, .value = id * 100 }) catch {};
}

fn race(scope: *volt.Scope) !void {
    const ctx = race_ctx.?;
    try scope.spawn(backend, .{ ctx, @as(u32, 1), @as(u32, 80)  });
    try scope.spawn(backend, .{ ctx, @as(u32, 2), @as(u32, 30)  });
    try scope.spawn(backend, .{ ctx, @as(u32, 3), @as(u32, 120) });

    const winner = try ctx.winner.recv();
    std.debug.print("winner: backend {d} → {d}\n", .{ winner.backend, winner.value });
    ctx.winner.close();
}

threadlocal var race_ctx: ?*Ctx = null;

fn root() !void {
    var os = volt.channel.Oneshot(Result){};
    var ctx = Ctx{ .winner = &os };
    race_ctx = &ctx;
    defer race_ctx = null;
    try volt.scope(race);
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, root, .{});
}
```

## Why it works

**The Oneshot is the race resolver.** All three backends call
`ctx.winner.send`. The first call wins; the second and third get
`error.Closed` and silently discard. So whichever backend finishes
first is the only one whose value lands in the Oneshot.

**The scope owns lifetime.** When `race` returns, `volt.scope`
joins the remaining backends. They're either parked on
`volt.sleep` (if they hadn't woken yet) or finished their `send`
call (if they raced and lost). Either way, the scope cleans them
up — Park is cancellable, so `volt.sleep` surfaces
`error.Cancelled` when the scope cancels them.

**`ctx.winner.close()` after winning** wakes any backend that's
still blocked trying to send (none here, but the close is good
hygiene; with a buffered channel the close would matter for
preventing waste).

## Threadlocal? Really?

The example uses `threadlocal var race_ctx: ?*Ctx = null` to give
the scope-body access to the context without changing its
signature. That's a workaround for Zig's lack of closures —
`volt.scope(body)` takes a function, not a closure, so the body
can't capture surrounding scope.

In production code you'd thread the context through more cleanly:

```zig
fn race(scope: *volt.Scope, ctx: *Ctx) !void {
    try scope.spawn(backend, .{ ctx, ... });
    // ...
}

// And use a wrapper:
try volt.scope(struct {
    fn body(s: *volt.Scope) !void {
        try race(s, &ctx);
    }
}.body);
```

The threadlocal version is fine for examples; the explicit-arg
version is fine for real code.

## Variant: race a real network call

Replace `volt.sleep` with an actual call:

```zig
fn fetchFromBackend(ctx: *Ctx, id: u32, addr: volt.io.Address) void {
    const stream = volt.io.TcpStream.connect(addr) catch return;
    defer { var s = stream; s.close(); }

    var buf: [4096]u8 = undefined;
    var s = stream;
    s.writeAll("GET / HTTP/1.0\r\n\r\n") catch return;
    const n = s.read(&buf) catch return;

    _ = ctx.winner.send(.{
        .backend = id,
        .value = parseResponse(buf[0..n]),
    }) catch {};
}
```

Same shape. `connect` / `writeAll` / `read` all suspend on the
reactor; cancelling them on scope-exit unblocks them with
`error.Cancelled` and the function returns.

## Variant: take the first N answers, not just one

Use a `Channel(T)` instead of a `Oneshot(T)`, and read N times
before closing:

```zig
fn race(scope: *volt.Scope) !void {
    var ch = try volt.channel.Channel(Result).init(allocator, 8);
    defer ch.deinit();

    try scope.spawn(backend, .{ ... });
    try scope.spawn(backend, .{ ... });
    try scope.spawn(backend, .{ ... });
    try scope.spawn(backend, .{ ... });
    try scope.spawn(backend, .{ ... });

    var got: u32 = 0;
    while (got < 3) {
        const r = try ch.recv();
        std.debug.print("answer {d}: backend {d}\n", .{ got, r.backend });
        got += 1;
    }
    ch.close();   // wakes remaining backends still blocked on send
}
```

We accept 3 of 5 answers and let the other 2 see `error.Closed` on
their `send` call. Scope joins all 5.

## Source

[`examples/fan_out.zig`](https://github.com/NerdMeNot/volt/blob/main/examples/fan_out.zig) in the repo.

```sh
zig build run-fan-out
```
