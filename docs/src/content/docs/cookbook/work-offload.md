---
title: Offloading CPU-Bound Work
description: spawnBlocking — push synchronous CPU-heavy code to a thread pool so the async workers can keep dispatching.
---

When your code does real CPU work — hashing, parsing, compression,
calling a sync C library — running it on a Volt worker thread
blocks every other coroutine that worker would have run.
`volt.spawnBlocking` exists for exactly this.

## The pattern

```zig
const std = @import("std");
const volt = @import("volt");

fn cpuHash(seed: u64) u64 {
    var s = seed;
    var i: u64 = 0;
    while (i < 5_000_000) : (i += 1) {
        s = s *% 6364136223846793005 +% 1442695040888963407;
    }
    return s;
}

const Sink = struct { out: [3]u64 = .{ 0, 0, 0 } };

fn worker(sink: *Sink, idx: usize, seed: u64) !void {
    sink.out[idx] = try volt.spawnBlocking(cpuHash, .{seed});
}

fn root() !void {
    const start = volt.Instant.now();
    var sink = Sink{};

    const j1 = try volt.launch(worker, .{ &sink, @as(usize, 0), @as(u64, 1) });
    const j2 = try volt.launch(worker, .{ &sink, @as(usize, 1), @as(u64, 2) });
    const j3 = try volt.launch(worker, .{ &sink, @as(usize, 2), @as(u64, 3) });
    defer volt.destroyJob(j1);
    defer volt.destroyJob(j2);
    defer volt.destroyJob(j3);

    try j1.join(); try j2.join(); try j3.join();

    const elapsed = start.elapsed().asMillis();
    std.debug.print("3 hashes done in {d} ms: {x} {x} {x}\n", .{
        elapsed, sink.out[0], sink.out[1], sink.out[2],
    });
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, root, .{});
}
```

Output:

```
3 hashes done in 55 ms: 251797b4326d7d41 9b297887d88d2242 113b595b7eacc743
```

## Why three coroutines?

`volt.spawnBlocking(fn, args)` parks the **calling coroutine** until
the work finishes, then returns the value. That's a single
sequential dependency.

To run three blocking calls in **parallel**, we spawn three
coroutines (one per call) and `spawnBlocking` from each. Each
coroutine parks; the blocking pool services all three in parallel
on separate threads. The whole thing finishes in roughly the time
of the slowest call instead of the sum of the three.

If we just called `cpuHash` three times in a row from a single
coroutine, we'd see roughly 3× the latency. The trick is concurrent
parking, not concurrent calling.

## When to use spawnBlocking

| Use it for | Don't use it for |
|---|---|
| CPU-heavy synchronous code | Microsecond work (pool overhead > work) |
| Sync C libraries that block | Code already async-friendly |
| File I/O without io_uring | TCP I/O (use `volt.io` directly) |
| `getaddrinfo` / DNS | Anything you'd `await` in Tokio |

The blocking pool starts up to a configurable number of threads
(currently a generous default, more than enough for typical
workloads). Idle threads expire after 10s.

## Variant: bounded concurrency on the blocking pool

If you have N items to process and N is large, you don't want N
parked coroutines on the blocking pool simultaneously — that
defeats the pool's resource cap. Use a Semaphore:

```zig
fn processAll(items: []const []const u8, alloc: std.mem.Allocator) !void {
    var sem = volt.sync.Semaphore.init(8);   // at most 8 concurrent

    try volt.scope(struct {
        fn body(scope: *volt.Scope) !void {
            const args = body_args.?;
            for (args.items) |item| {
                try scope.spawn(processOne, .{ item, args.sem });
            }
        }
    }.body);
}

fn processOne(item: []const u8, sem: *volt.sync.Semaphore) !void {
    sem.acquire(1);
    defer sem.release(1);
    _ = try volt.spawnBlocking(heavyWork, .{item});
}
```

Now even if `items.len == 10000`, at most 8 spawnBlocking calls
are in flight at once.

## Variant: timeout on a blocking call

```zig
const result = volt.withTimeout(
    volt.Duration.fromSecs(5),
    callBlocking,
    .{},
) catch |err| switch (err) {
    error.Timeout => return error.SlowCall,
    else => return err,
};

fn callBlocking() !u64 {
    return try volt.spawnBlocking(slowFn, .{});
}
```

The catch: cancelling the calling coroutine cancels its **park**,
not the blocking work. The pool thread keeps running until
`slowFn` returns. So `withTimeout` here gives you "I stopped
caring about the result" — not "I killed the work." For most use
cases that's the right semantic; if you need actual cancellation
of CPU-bound work, you have to write it cooperatively (check a
flag in the loop).

## Source

[`examples/work_offload.zig`](https://github.com/NerdMeNot/volt/blob/main/examples/work_offload.zig) in the repo.

```sh
zig build run-work-offload
```
