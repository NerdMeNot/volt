---
title: Offloading CPU-Bound Work
description: Volt core has no spawnBlocking primitive. Use std.Thread + volt.Mpmc to bridge synchronous CPU-heavy code to the coroutine world.
---

When your code does real CPU work — hashing, parsing, compression,
calling a sync C library — running it inside a coroutine blocks
the worker thread that coroutine is on. Volt core does **not**
ship a `spawnBlocking` primitive that off-loads to a background
thread pool. The bridge is: spawn an OS thread via
`std.Thread.spawn`, communicate via `volt.Mpmc`, park the
coroutine on the channel.

A first-class `volt.spawnBlocking` may land later; for now this is
the recipe.

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

// A request includes the input + a Mpmc(T, 1) for the response.
const HashReq = struct {
    seed: u64,
    reply: *volt.Mpmc(u64, 1),
};

// The OS thread reads requests from the inbox and processes them.
fn cpuWorker(inbox: *volt.Mpmc(HashReq, 64)) void {
    while (true) {
        const req = inbox.recv() catch return;   // error.Closed → shutdown
        const result = cpuHash(req.seed);
        _ = req.reply.trySend(result) catch {};   // 1-deep channel; first send wins
    }
}

// Inside a coroutine: enqueue a request, park on the reply channel.
fn computeHash(inbox: *volt.Mpmc(HashReq, 64), seed: u64) !u64 {
    var reply = volt.Mpmc(u64, 1).init();
    defer reply.deinit();

    try inbox.send(.{ .seed = seed, .reply = &reply });
    return try reply.recv();
}

fn root() !void {
    // Inbox shared between the coroutine world and the OS thread.
    var inbox = volt.Mpmc(HashReq, 64).init();
    defer inbox.deinit();

    // Spawn the CPU worker as an OS thread (NOT a coroutine).
    var cpu_thread = try std.Thread.spawn(.{}, cpuWorker, .{&inbox});
    defer {
        inbox.close();           // signal worker to exit
        cpu_thread.join();
    }

    const start = std.time.nanoTimestamp();

    // Three parallel hash requests from coroutines:
    const t1 = try volt.spawn(struct {
        fn run(ib: *volt.Mpmc(HashReq, 64), seed: u64) u64 {
            return computeHash(ib, seed) catch 0;
        }
    }.run, .{ &inbox, @as(u64, 1) });
    const t2 = try volt.spawn(struct {
        fn run(ib: *volt.Mpmc(HashReq, 64), seed: u64) u64 {
            return computeHash(ib, seed) catch 0;
        }
    }.run, .{ &inbox, @as(u64, 2) });
    const t3 = try volt.spawn(struct {
        fn run(ib: *volt.Mpmc(HashReq, 64), seed: u64) u64 {
            return computeHash(ib, seed) catch 0;
        }
    }.run, .{ &inbox, @as(u64, 3) });

    const r1 = t1.join();
    const r2 = t2.join();
    const r3 = t3.join();

    const elapsed_ms = @divTrunc(std.time.nanoTimestamp() - start, std.time.ns_per_ms);
    std.debug.print("3 hashes done in {d} ms: {x} {x} {x}\n", .{
        elapsed_ms, r1, r2, r3,
    });
}

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}
```

## Why this works

`cpuHash` is synchronous and CPU-bound. Running it directly inside
a coroutine would tie up the worker thread for ~55 ms (no
suspension points inside the loop), preventing other coroutines on
the same worker from running.

The pattern moves the CPU work to a **separate OS thread** that
isn't a Volt worker:

1. `std.Thread.spawn(.{}, cpuWorker, .{&inbox})` creates a raw OS
   thread. This thread is *not* a Volt worker — it's outside
   Volt's M:N scheduler.
2. The OS thread runs `cpuWorker`, which loops on `inbox.recv()`.
3. Coroutines send hash requests into `inbox` via `inbox.send` —
   from inside a coroutine, this parks the coroutine if the
   inbox is full and resumes when there's room. The OS thread
   eventually receives the request.
4. The OS thread processes (synchronously, no Volt awareness) and
   sends the result via `req.reply.trySend`. `trySend` doesn't
   park because the OS thread isn't a coroutine — and the reply
   channel has capacity 1 so it always succeeds for the first
   send.
5. The coroutine that posted the request was parked on
   `reply.recv()`; the OS thread's send wakes it via the parking
   lot.

**Why Mpmc and not Spsc:** Mpmc supports cross-thread sends from
non-coroutine threads. Spsc has stricter contracts.

## Concurrent blocking calls

To run three blocking calls in parallel, we spawn three
coroutines (one per call). Each parks on its own reply channel.
The CPU worker processes them sequentially — one OS thread, one
core. For true parallelism, spawn N CPU worker threads:

```zig
var threads: [4]std.Thread = undefined;
for (&threads) |*t| {
    t.* = try std.Thread.spawn(.{}, cpuWorker, .{&inbox});
}
defer {
    inbox.close();
    for (&threads) |*t| t.join();
}
```

Now four OS threads service the inbox in parallel. The
coroutine-side code doesn't change.

## When to use this bridge

| Use it for | Don't use it for |
|---|---|
| CPU-heavy synchronous code | Microsecond work (Mpmc round-trip > work) |
| Sync C libraries that block | Code that's already coroutine-friendly |
| File I/O (no async fs in core) | TCP I/O (use `volt.net.*`) |
| `getaddrinfo` / blocking DNS | Anything `volt.spawn` can run |

Microsecond work is faster done inline — the channel round-trip
costs ~100-200 ns, plus the OS thread's scheduling latency. The
bridge starts paying off around 100 µs of work.

## Bounded concurrency on the bridge

If you have N items to process and N is large, you don't want N
coroutines all parked waiting on the bridge — at minimum it
inflates memory. Use a Semaphore:

```zig
fn processAll(items: []const u64, inbox: *volt.Mpmc(HashReq, 64)) !void {
    var sem = volt.Semaphore.init(8);    // at most 8 in flight
    defer sem.deinit();

    try volt.scope(struct {
        fn body(c: *volt.Cancel) anyerror!void {
            var join_list = std.ArrayList(*volt.Task(void)).init(...);
            defer join_list.deinit();

            for (items) |seed| {
                sem.acquire();
                const t = try volt.spawn(struct {
                    fn run(ib: *volt.Mpmc(HashReq, 64), s: u64, sem_: *volt.Semaphore) void {
                        defer sem_.release();
                        _ = computeHash(ib, s) catch {};
                    }
                }.run, .{ inbox, seed, &sem });
                try join_list.append(t);
            }

            for (join_list.items) |t| t.join();
            _ = c;
        }
    }.body);
}
```

At most 8 concurrent hash-requests in flight at a time, regardless
of `items.len`.

## Cancellation

The CPU worker is **not cancel-aware** — once a request is in
its hands, it runs to completion. The coroutine that posted the
request can be cancelled (its `reply.recv()` is a parking-lot
wait that wakes on Cancel via a cancel-aware variant), but the
CPU thread keeps churning.

If you need cancellable CPU work, embed the check inside the
function:

```zig
fn cpuHashCancellable(seed: u64, c: *std.atomic.Value(bool)) u64 {
    var s = seed;
    var i: u64 = 0;
    while (i < 5_000_000) : (i += 1) {
        if (i % 100_000 == 0 and c.load(.acquire)) return 0;
        s = s *% 6364136223846793005;
    }
    return s;
}
```

The flag is shared between the coroutine world (sets it on cancel)
and the OS thread (reads it periodically).

## See also

- [Channels: Mpmc](/usage/channels/) — the cross-thread channel.
- [Roadmap](/appendix/roadmap/) — first-class `spawnBlocking` is a
  candidate for later inclusion.
- `std.Thread.spawn` — the OS thread primitive Volt builds atop.
