---
title: Error handling
description: Volt's error vocabulary, how cancellation propagates, and patterns for clean error paths.
---

Volt is a Zig runtime, so error handling is just Zig error handling
— `try`, `catch`, error unions. There are a few Volt-specific
errors and a few patterns worth knowing.

## The error vocabulary

| Error | Where it comes from |
|---|---|
| `error.Cancelled` | A cancel-aware blocking op (`recvCancel`, `lockCancel`, etc.) observed `Cancel.fire()` |
| `error.Closed` | Channel closed; send or recv couldn't complete |
| `error.Lagged` | `Broadcast.Receiver.recv` — receiver fell more than `cap` messages behind |
| `error.ArenaExhausted` | `volt.spawn` couldn't allocate a stack — `Config.max_concurrent_stacks` reached |
| `error.OutOfMemory` | allocator failed (Coroutine struct, Task, Frame) |
| `error.MmapFailed` / `error.MprotectFailed` | slab arena setup failed at runtime init |
| `error.InvalidAddress` | `Address.parse4` couldn't parse the host |
| `error.{BindFailed, ListenFailed, AcceptFailed, ConnectFailed, ...}` | Network syscall errors from `volt.net.*` |

Channel errors share `error.Closed` (and `error.Cancelled` via the
cancel-aware variants), so one `catch` can cover any channel
operation:

```zig
const v = ch.recv() catch |err| switch (err) {
    error.Closed => return,
    // recvCancel adds error.Cancelled:
    // error.Cancelled => return,
};
```

## Runtime-init errors

```zig
var rt = try volt.Runtime.init(.{ .allocator = a }) catch |err| switch (err) {
    error.OutOfMemory => { /* allocator failed */ },
    error.MmapFailed => { /* could not reserve the arena */ },
    error.TooManyWorkers => { /* workers > MAX_WORKERS (64) */ },
    else => return err,
};
```

`Runtime.init` can fail; `try` is correct most of the time. The
named errors above are the catalogue if you want to handle each.

## Cancellation semantics

`Cancel.fire()` does two things:

1. Atomically flips `Cancel.fired = true`.
2. Walks the Cancel's waiter list and calls `parking_lot.unparkAll`
   on each waiter's `park_addr`.

A coroutine that registered a Waiter on the Cancel (via a
cancel-aware blocking op) wakes from its park. The cancel-aware
op then re-checks `Cancel.isFired()` and returns
`error.Cancelled`.

```zig
fn worker(c: *volt.Cancel) error{Cancelled}!void {
    try c.checkpoint();              // before any work
    const v = try ch.recvCancel(c);  // parks; wakes with error.Cancelled if fired
    process(v);
}
```

You **do** thread the `*Cancel` through functions that might
park. This is the Go `context.Context` model — cancellation as
data flow, not magic on a handle. See
[Cancellation](/usage/structured-concurrency/).

For CPU-only loops with no suspension, `c.checkpoint()` is the
explicit observation point. One atomic load + branch.

## Where errors come from

A typical request trace:

```
Runtime.run(root, .{...})
  ├─ root() returns App.HandlerError!Response
  │  ├─ handle() returns App.HandlerError!Response
  │  │  ├─ ch.recvCancel(c)
  │  │  │   → error.{Closed, Cancelled}
  │  │  ├─ stream.writeAll(buf)
  │  │  │   → connection / errno errors (BrokenPipe etc.)
  │  │  └─ returns App.HandlerError!Response
  │  └─ returns App.HandlerError!Response
  └─ returns !(App.HandlerError!Response)   ← runtime errors outside
```

Each layer adds whatever errors *that layer's calls* can produce.
The outer `!` from `Runtime.run` wraps allocator / arena /
worker-spawn failures; your fn's `!` wraps your fn's errors.
Hence the `try (try rt.run(root, .{}))` idiom for `!T` user fns.

## Pattern: explicit handle vs propagate

For most code, propagate:

```zig
fn handle(conn: volt.net.TcpStream) !void {
    var s = conn;
    defer s.close();
    var buf: [4096]u8 = undefined;
    const n = try s.read(&buf);    // propagate read errors
    try s.writeAll(buf[0..n]);     // propagate write errors
}
```

For fire-and-forget where errors are "the connection died, move
on," swallow them at the function boundary:

```zig
fn handle(conn: volt.net.TcpStream) void {   // void, not !void
    var s = conn;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;     // any error: just return
        if (n == 0) return;                       // peer closed cleanly
        s.writeAll(buf[0..n]) catch return;
    }
}
```

The `void` signature is intentional — handlers are
fire-and-forget; their errors are mostly "the connection died,
move on." Returning swallows the error and lets the coroutine
finish cleanly.

## Pattern: handle one error explicitly, propagate the rest

```zig
const v = ch.recvCancel(c) catch |err| switch (err) {
    error.Cancelled => return,                // expected; recover
    error.Closed => return errorReply(),      // expected; propagate something
    else => return err,                        // unexpected; bubble
};
```

This is the standard Zig idiom; works with Volt errors the same
way it works with anything else.

## Don't use `catch unreachable` on Volt errors

```zig
const v = ch.recv() catch unreachable;   // ← will panic on close
```

Channels can close, coroutines can hit Cancel. Both happen during
shutdown even if your "happy path" never sees them. `catch
unreachable` turns those into runtime panics.

If you genuinely don't care about an error, write `catch {}` or
`catch return` — they discard cleanly.

## Errors across `volt.scope` boundaries

```zig
try volt.scope(struct {
    fn body(c: *volt.Cancel) anyerror!void {
        const a = try volt.spawn(workerA, .{c});
        const b = try volt.spawn(workerB, .{c});

        // If workerA returns an error, a.join() returns the error.
        _ = try a.join();
        _ = try b.join();
    }
}.body);
```

If `a.join()` errors, the body returns that error. `volt.scope`
fires the Cancel before propagating — so `workerB`, if it's
parked on a cancel-aware op, wakes with `error.Cancelled`. The
body's `b.join()` then surfaces that as the unwind continues.

Scope cancels but does **not** auto-await. If the body returns
without joining all children, those children outlive the scope
and their Task structs leak. Always join inside the body.

## Stack overflow

When a coroutine writes past its 256 KiB virtual reservation
(into the bottom guard page), the SIGSEGV handler chains to the
default handler. **The process aborts.** Volt does not (today)
catch this and continue — the coroutine's stack is in an unknown
state by the time we observe the fault.

The typical fix: don't recurse without bound. If you're allocating
large stack locals, either move them to the heap or raise
`RESERVATION_SIZE` (compile-time in `src/stack.zig`).

`error.StackOverflow` is **not** in the runtime today as a
surfaced error. The Phase 4 design attempted this and was reverted
([postmortem](/performance/phase-4-postmortem/)); the current
design aborts cleanly instead of pretending recovery is possible.

## What about panics?

Panics in user code (not just errors) propagate through Volt the
same way they would in synchronous code: the worker's stack
unwinds, the panic handler runs, the process aborts. Volt does
not isolate panics — a panic in one coroutine takes down the
runtime.

If you need fault isolation (e.g., for plugin systems or
multi-tenant), wrap the entry point in `try ... catch` for
expected errors, but accept that genuine panics terminate the
process. Per-coroutine panic isolation isn't in v1.0.

## See also

- [Structured Concurrency](/usage/structured-concurrency/) — Cancel, scope, cancel-aware variants.
- [Channels](/usage/channels/) — full error vocabulary per channel.
- [Common pitfalls](/guides/common-pitfalls/) — the bugs to avoid.
- [Architecture: cancellation internals](/architecture/cancellation-internals/) — how the Cancel + parking-lot integration works.
