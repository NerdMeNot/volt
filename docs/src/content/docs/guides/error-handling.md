---
title: Error Handling
description: Volt's error vocabulary, how cancellation propagates, and patterns for clean error paths.
---

Volt is a Zig runtime, so error handling is just Zig error handling
— `try`, `catch`, error unions. There are a few Volt-specific
errors and a few patterns worth knowing.

## The error vocabulary

| Error | When it fires |
|---|---|
| `error.Cancelled` | Coroutine was cancelled while parked or before its next suspension |
| `error.Closed` | Channel closed; sender or receiver couldn't complete |
| `error.Timeout` | `volt.withTimeout` deadline fired |
| `error.StackOverflow` | Coroutine exhausted its 8 MiB stack reservation |
| `error.WouldBlock` | Non-blocking try* operation; nothing fatal |

Channels share `error.Closed` and `error.Cancelled` — defined in
`src/channel/errors.zig` and re-exported as
`volt.channel.{SendError, RecvError}`. So a single `catch` can
cover any channel operation:

```zig
ch.recv() catch |err| switch (err) {
    error.Closed => return,
    error.Cancelled => return,
};
```

## `volt.RunError`

`volt.run` adds runtime-bootstrap errors to whatever your root
function might return. Exported so you can build unified
application error sets:

```zig
const AppError = volt.RunError || error{ BadRequest, Forbidden, NotFound };

fn handler() AppError!void {
    // your logic
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, handler, .{});
}
```

`RunError` includes `OutOfMemory`, `Cancelled`, `StackOverflow`,
worker spawn failures, reactor init failures, etc. — all the
things a runtime startup can encounter. About 25 tags total.

## Cancellation semantics

`Job.cancel()` does two things atomically:

1. Sets `cancel_flag = true` on the coroutine.
2. Unparks whatever the coroutine is currently parked on (sleep,
   I/O wait, channel, mutex, etc.).

When the coroutine wakes (or hits its next suspension if it wasn't
parked), the Park surfaces `error.Cancelled`. That error
propagates up through whatever called the suspending operation:

```zig
fn worker() !void {
    try volt.sleep(volt.Duration.fromSecs(60));   // wakes early on cancel
    // ↑ surfaces error.Cancelled if cancelled during sleep
}
```

You don't need to thread a cancellation token through every
function. The cancel happens *to the coroutine*, and any
suspending operation in that coroutine surfaces it.

For CPU-only loops with no suspension, add `try volt.yield()` —
that's the explicit cancellation point.

## Where errors come from

Trace through a typical request:

```
volt.run(...)
  ├─ root() returns App.HandlerError!Response
  │  ├─ handle()
  │  │  ├─ ch.recv() — error.Closed | Cancelled
  │  │  ├─ try volt.spawnBlocking(parse, .{...}) — propagates parse's errors
  │  │  ├─ stream.writeAll(buf) — error.BrokenPipe | Cancelled (TcpStream errors)
  │  │  └─ returns App.HandlerError!Response
  │  └─ returns App.HandlerError!Response
  └─ returns (App.HandlerError || RunError)!Response
```

Each layer adds whatever errors *that layer's calls* can produce.
The error union grows as you go up. At the top, you switch over a
union that includes everything reachable.

## Pattern: explicit handle vs propagate

For most code:

```zig
fn handle(conn: TcpStream) !void {
    var s = conn;
    defer s.close();
    var buf: [4096]u8 = undefined;
    const n = try s.read(&buf);    // propagate read errors
    try s.writeAll(buf[0..n]);     // propagate write errors
}
```

For graceful shutdown — handle `error.Cancelled` explicitly
without bubbling it up:

```zig
fn handle(conn: TcpStream) void {  // void, not !void
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

The `void` signature here is intentional — handlers are
fire-and-forget; their errors are mostly "the connection died,
move on." Returning swallows the error and lets the coroutine
finish cleanly.

## Pattern: handle one error explicitly, propagate the rest

```zig
const result = volt.withTimeout(deadline, work, .{}) catch |err| switch (err) {
    error.Timeout => return defaultValue(),  // expected; recover
    else => return err,                       // unexpected; bubble
};
```

This is the standard Zig idiom; it works with Volt errors the
same way it works with anything else.

## Don't use `catch unreachable` on Volt errors

```zig
const v = ch.recv() catch unreachable;   // ← will panic on close or cancel
```

Channels can close, coroutines can be cancelled. Both happen during
shutdown even if your "happy path" never sees them. `catch
unreachable` turns those into runtime panics in production.

If you genuinely don't care about an error path, write `catch {}`
or `catch return` — they discard cleanly.

## Stack overflow handling

```zig
const j = try volt.launch(possibly_recursive_thing, .{});
defer volt.destroyJob(j);
j.join() catch |err| switch (err) {
    error.StackOverflow => std.log.err("recursion went too deep", .{}),
    else => return err,
};
```

`error.StackOverflow` fires when a coroutine writes past its 8 MiB
stack reservation. The SIGSEGV handler catches the fault, marks
the coroutine, and `siglongjmp`s back to the scheduler — the
coroutine dies but the runtime keeps running. The coroutine's
`Job.join()` surfaces `error.StackOverflow` to the joiner.

This is meant as a guardrail, not a normal error. If you're hitting
it routinely, raise the stack reservation cap (compile-time
constant in `src/coroutine/stack.zig`) or restructure to avoid
deep recursion.

## Errors across `volt.scope` boundaries

```zig
try volt.scope(struct {
    fn body(s: *volt.Scope) !void {
        try s.spawn(workerA, .{});
        try s.spawn(workerB, .{});
        return error.OopsBodyFailed;   // ← what happens?
    }
}.body);
```

Scope cancels remaining children, joins them, then returns the
body's error. Both workers see `error.Cancelled` from their next
suspension; the scope returns `error.OopsBodyFailed`.

For child errors:

```zig
fn workerA(s: *volt.Scope) void {
    doWork() catch |e| s.fault(e);
}
```

The first `s.fault` call wins; subsequent are ignored. Scope
cancels remaining siblings and returns the first fault.

## Errors in `JoinSet`

```zig
while (set.joinNext()) |result| {
    switch (result) {
        .ok => |v| process(v),
        .err => |e| std.log.warn("task failed: {}", .{e}),
        .cancelled => {},
    }
}
```

`JoinSet.Result` is a tagged union with `.ok`, `.err`, and
`.cancelled` variants. You decide per-task whether to bail on the
first error or collect them all.

## What about panics?

Panics in user code (not just errors) propagate through Volt the
same way they would in synchronous code: the worker's stack
unwinds, the panic handler runs, the process aborts. Volt does not
isolate panics — a panic in one coroutine takes down the runtime.

If you need fault isolation (e.g., for plugin systems or
multi-tenant), use `std.debug.captureStackTrace` + your own
recovery wrapper. Per-coroutine panic isolation isn't part of
v1.0.
