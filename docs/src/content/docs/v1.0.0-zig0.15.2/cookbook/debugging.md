---
title: Debugging Async Code
description: Techniques for diagnosing deadlocks, panics, memory issues, and
  other problems in Volt async applications.
slug: v1.0.0-zig0.15.2/cookbook/debugging
---

Async code introduces failure modes that do not exist in synchronous programs: tasks can deadlock without blocking threads, panics in detached tasks can be silently swallowed, and memory issues are harder to trace when tasks migrate between workers. This recipe covers practical debugging techniques.

## Task Panics

When a task panics, Volt catches the panic and stores it in the task's result. If the task is awaited, the panic propagates to the caller. If the task is detached (the `Future` is discarded), the panic is lost.

### Catching panics from spawned tasks

```zig
fn app(io: volt.Io) !void {
    var f = try io.@"async"(riskyWork, .{});
    const result = f.@"await"(io);
    // If riskyWork panicked, the panic propagates here.
    _ = result;
}
```

### Detached tasks: don't lose panics

If you discard the `Future` returned by `io.@"async"`, any panic in that task is silently lost. Always capture the future and await it, or use a group:

```zig
// BAD: Panic in processItem is silently lost
_ = try io.@"async"(processItem, .{data});

// GOOD: Group catches panics from all spawned tasks
var group = volt.Group.init(io);
_ = group.spawn(processItem, .{data});
group.wait(); // Panics propagate here
```

## Detecting Deadlocks with Timeouts

The most common async deadlock is two tasks waiting on each other through channels or mutexes. Since these are task-level waits (not OS-level), standard deadlock detectors do not help.

### Timeout pattern

Wrap any operation that might deadlock in a timeout:

```zig
const volt = @import("volt");

fn fetchWithTimeout(io: volt.Io) !void {
    var mutex = volt.sync.Mutex.init();

    // If the lock isn't acquired within 5 seconds, something is wrong
    var lock_f = mutex.lockFuture();
    var timeout_f = volt.time.sleep(volt.Duration.fromSecs(5));

    // Use select to race the lock against the timeout
    // If timeout wins, you likely have a deadlock
    _ = lock_f;
    _ = timeout_f;
    _ = io;
}
```

### Common deadlock patterns

**Channel cycle:** Task A sends to channel X and receives from channel Y. Task B sends to Y and receives from X. Both channels are full.

```zig
// DEADLOCK: Both channels full, both tasks blocked
// Task A: ch_x.send(io, val); _ = ch_y.recv(io);
// Task B: ch_y.send(io, val); _ = ch_x.recv(io);

// FIX: Use trySend/tryRecv with fallback logic,
// or ensure one direction always has capacity.
```

**Lock ordering:** Task A holds mutex 1 and waits for mutex 2. Task B holds mutex 2 and waits for mutex 1.

```zig
// FIX: Always acquire mutexes in the same order.
// Convention: lower address first, or assign a numeric order.
```

## Stack Traces

Volt tasks are stackless state machines, so stack traces show the scheduler's call stack, not the logical task call chain. To trace task ancestry:

### Print-based tracing

Add context to your task functions:

```zig
fn handleRequest(io: volt.Io, request_id: u64) void {
    std.debug.print("[req-{}] starting\n", .{request_id});
    defer std.debug.print("[req-{}] done\n", .{request_id});

    // ... work ...
    _ = io;
}
```

### Error return traces

Zig's error return traces work normally within a single task. If a function returns an error, the trace shows the call chain within that task's execution:

```zig
fn processData(data: []const u8) !void {
    const parsed = try parseHeader(data);
    try validateChecksum(parsed);
    // Error return traces show the chain: processData -> parseHeader/validateChecksum
}
```

## Memory Debugging

### Use GeneralPurposeAllocator in development

The explicit `Io.init` pattern lets you plug in any allocator, including GPA for leak detection:

```zig
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{
        .stack_trace_frames = 8, // Capture allocation stack traces
    }){};
    defer {
        const status = gpa.deinit();
        if (status == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }

    var io = try volt.Io.init(gpa.allocator(), .{});
    defer io.deinit();

    try io.run(myApp);
}
```

### Common memory issues

**Forgetting `deinit` on channels:** `Channel(T)` and `BroadcastChannel(T)` allocate ring buffers and must be deinitialized:

```zig
var ch = try volt.channel.bounded(u32, allocator, 100);
defer ch.deinit(); // Don't forget this!
```

**Capturing stack pointers across yield points:** A task may resume on a different worker thread. Do not store pointers to local variables if the task might yield:

```zig
// BAD: buf is on the stack frame, but tryRead may yield
var buf: [4096]u8 = undefined;
// This is fine for tryRead (non-blocking, no yield)
// But be careful with futures that suspend
```

## Diagnosing Slow Tasks

### Cooperative budget exhaustion

If a task does heavy computation without yielding, it consumes the entire cooperative budget (128 polls) and starves other tasks on the same worker. Symptoms: latency spikes for unrelated tasks.

**Fix:** Offload CPU-heavy work to the blocking pool:

```zig
// Instead of computing inline:
const result = try io.concurrent(heavyComputation, .{data});
const value = try result.wait();
```

### Contention on sync primitives

If many tasks contend on a single mutex, throughput drops. Use `mutex.waiterCount()` and `sem.availablePermits()` to check contention levels at runtime:

```zig
// Diagnostic: check how many tasks are waiting
const waiting = mutex.waiterCount();
if (waiting > 10) {
    std.debug.print("High mutex contention: {} waiters\n", .{waiting});
}
```

## Checklist

When debugging an async issue, check these in order:

1. **Is the runtime running?** Ensure `volt.run()` or `io.run()` was called.
2. **Are futures being awaited?** Discarded futures mean lost results and panics.
3. **Is anything blocking the worker thread?** `std.Thread.sleep`, CPU loops, or synchronous I/O on a worker thread starves all tasks on that worker.
4. **Is there a deadlock?** Add timeouts to narrow down which operation hangs.
5. **Is there contention?** Check waiter counts on mutexes and semaphores.
6. **Is there a memory leak?** Use GPA with `defer gpa.deinit()` and check for leaks.
