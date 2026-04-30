---
title: Quick Start
description: A working TCP echo server, a fan-out task, and a CPU-bound offload — all in idiomatic Volt.
---

This page is three small programs, each one runnable. Skim the first
one and the rest will make sense by analogy.

## 1. Echo server

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, serve, .{});
}

fn serve() !void {
    var listener = try volt.io.TcpListener.bind(volt.io.Address.any4(8080));
    defer listener.close();

    while (true) {
        const conn = try listener.accept();   // suspends; reactor wakes us
        _ = try volt.launch(handle, .{conn}); // one coroutine per connection
    }
}

fn handle(conn: volt.io.TcpStream) void {
    var s = conn;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;  // suspends until data arrives
        if (n == 0) return;                   // peer closed
        s.writeAll(buf[0..n]) catch return;   // suspends if the kernel buffer is full
    }
}
```

Test it:

```sh
zig build run                 # in one terminal
echo "hello volt" | nc 127.0.0.1 8080  # in another
# → hello volt
```

What's happening: `listener.accept()` parks the coroutine on the
reactor. When the kernel says "an incoming connection is ready," the
reactor wakes the coroutine. `volt.launch(handle, .{conn})` spawns a
new coroutine for that connection — the parent goes back to
accepting; the child reads/writes that one socket. There's no
explicit event loop, no callback, no state machine.

## 2. Fan out and take the first answer

Three "backends" race; the fastest wins, the rest are cancelled
cleanly.

```zig
const std = @import("std");
const volt = @import("volt");

const Result = struct { backend: u32, value: u32 };

const Ctx = struct { winner: *volt.channel.Oneshot(Result) };

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
    std.debug.print("winner: backend {d} -> {d}\n", .{ winner.backend, winner.value });
    // Returning here joins remaining children; they observe the
    // closed Oneshot or get cancelled, and exit.
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

Two things to notice:

- `volt.scope(race)` is **structured concurrency** — when `race`
  returns, the scope has joined every child. You cannot accidentally
  leave a coroutine running past the scope's body.
- `Oneshot(T).send` is single-shot; second sender gets `error.Closed`
  and silently discards. That's how the losers tear down.

## 3. Offload CPU-bound work

`spawnBlocking` runs a synchronous function on a separate thread pool
and parks the calling coroutine until it finishes. Use it for hashing,
parsing, library calls that block — anything that would otherwise
hold a worker thread for too long.

```zig
const std = @import("std");
const volt = @import("volt");

fn cpuHash(seed: u64) u64 {
    var s = seed;
    var i: u64 = 0;
    while (i < 5_000_000) : (i += 1) s = s *% 6364136223846793005 +% 1442695040888963407;
    return s;
}

const Sink = struct { out: [3]u64 = .{ 0, 0, 0 } };

fn worker(sink: *Sink, idx: usize, seed: u64) !void {
    sink.out[idx] = try volt.spawnBlocking(cpuHash, .{seed});
}

fn root() !void {
    var sink = Sink{};
    const j1 = try volt.launch(worker, .{ &sink, @as(usize, 0), @as(u64, 1) });
    const j2 = try volt.launch(worker, .{ &sink, @as(usize, 1), @as(u64, 2) });
    const j3 = try volt.launch(worker, .{ &sink, @as(usize, 2), @as(u64, 3) });
    defer { volt.destroyJob(j1); volt.destroyJob(j2); volt.destroyJob(j3); }
    try j1.join(); try j2.join(); try j3.join();
    std.debug.print("hashes: {x} {x} {x}\n", .{ sink.out[0], sink.out[1], sink.out[2] });
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, root, .{});
}
```

`spawnBlocking` parks the *calling* coroutine, so if you want
multiple blocking operations to run concurrently you spawn one
coroutine per blocking call (as `worker` does here).

## Where to go next

- [Basic Concepts](/getting-started/basic-concepts/) — the model
  underneath these examples.
- [Glossary](/getting-started/glossary/) — terms used throughout the
  docs.
- The `examples/` directory in the repo has each of these as a
  `zig build run-*` target you can run immediately.
