---
title: Blitz Integration
description: How to use Volt with Blitz for combined async I/O and CPU parallelism.
slug: v1.0.0-zig0.15.2/guides/blitz-integration
---

Volt handles async I/O; [Blitz](https://github.com/NerdMeNot/blitz) handles CPU parallelism. Together, they cover the same ground as Tokio + Rayon in the Rust ecosystem. This guide explains when and how to combine them.

## When You Need Blitz

The blocking pool (`io.concurrent`) is sufficient for most CPU-intensive work. Consider Blitz when you need:

* **Data-parallel computation** -- processing large arrays, images, or matrices with parallel-for patterns
* **Fork-join parallelism** -- recursive divide-and-conquer algorithms (mergesort, tree traversals)
* **Sustained CPU throughput** -- long-running computations where the blocking pool's thread creation overhead matters

For occasional CPU work (hashing, compression, parsing), `io.concurrent` is simpler and requires no extra dependency.

## Enabling the Dependency

Blitz is a lazy dependency in Volt's `build.zig.zon`. Enable it with a build flag:

```bash
zig build -Denable_blitz=true
```

This fetches and links Blitz. Without the flag, Blitz is not downloaded or compiled.

## Usage Pattern

The typical pattern is: Volt manages connections and I/O, Blitz runs the heavy computation, and results flow back through channels or futures.

```zig
const volt = @import("volt");
const blitz = @import("blitz");

fn handleRequest(io: volt.Io, data: []const u8) !void {
    // Offload CPU-parallel work to Blitz via the blocking pool
    const handle = try io.concurrent(struct {
        fn run(input: []const u8) ![]const u8 {
            // Blitz parallel computation
            var pool = try blitz.ThreadPool.init(.{});
            defer pool.deinit();
            return pool.parallelMap(input, processChunk);
        }
    }.run, .{data});

    const result = try handle.wait();
    // Send result back to the client via Volt networking
    _ = result;
}
```

## Architecture

```
Incoming connections
       |
       v
+------------------+
| Volt Runtime     |     Async I/O: accept, read, write
| (worker threads) |     Channels, sync primitives
+--------+---------+
         |
         | io.concurrent(...)
         v
+------------------+
| Blocking Pool    |     Bridge to Blitz
+--------+---------+
         |
         v
+------------------+
| Blitz            |     CPU-parallel: parallel_for, fork-join
| (thread pool)    |
+------------------+
```

Volt worker threads never block on CPU work. The blocking pool acts as a bridge, ensuring the async scheduler stays responsive.
