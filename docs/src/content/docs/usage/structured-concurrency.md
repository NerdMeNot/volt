---
title: Structured Concurrency
description: volt.scope, JoinSet, CancellationToken — making sure every spawned coroutine outlives the scope that spawned it.
---

The biggest mistake in concurrent programs is leaking a child task
past its parent's scope. The child keeps running, the parent thinks
it's done, the resources the child held are gone, and you debug
mysterious use-after-free crashes for a week.

Volt borrows the fix from Trio (Python) and Kotlin coroutines:
**every spawn happens inside a scope, and every scope joins its
children on exit**. There is no equivalent to Go's bare `go` keyword
in idiomatic Volt code.

## volt.scope — the workhorse

```zig
try volt.scope(struct {
    fn body(s: *volt.Scope) !void {
        try s.spawn(workerA, .{ctx});
        try s.spawn(workerB, .{ctx});
        // ... do other work in the parent ...
    }
}.body);
// At this point, workerA and workerB have BOTH finished.
// No leaked coroutines. Resource lifetimes match lexical scope.
```

The scope is a barrier:

```
   parent enters scope
       │
       │  volt.scope(body)
       │
       ▼
   ┌──────────────────────────────────────────────────┐
   │   body(s):                                        │
   │     │                                             │
   │     │   s.spawn(A) ──► child A starts running     │
   │     │   s.spawn(B) ──► child B starts running     │
   │     │   ... do other work ...                     │
   │     │                                             │
   │     ▼                                             │
   │   body returns                                    │
   │     │                                             │
   │     │   ◄── scope WAITS for A and B to finish     │
   │     ▼                                             │
   │   all children joined                             │
   └──────────────────────────────────────────────────┘
       │
       ▼
   parent continues — A and B are guaranteed done

   on body error or s.fault(err):
   ┌──────────────────────────────────────────────────┐
   │   ◄── scope CANCELS A and B,                      │
   │       waits for them to surface error.Cancelled,  │
   │       returns the error                           │
   └──────────────────────────────────────────────────┘
```

The contract:

- On normal `body` return: scope joins all children, returns void.
- On `body` error: scope **cancels** all children, joins them,
  returns the body's error.
- On a child fault (see below): scope cancels remaining siblings,
  joins them, returns the first fault.

Children that error don't auto-propagate their error to the scope;
they get reaped silently. To propagate a child error, use
`scope.fault(err)`:

```zig
fn workerA(s: *volt.Scope) void {
    doWork() catch |e| s.fault(e);
}
```

The first `fault` call wins; subsequent faults are ignored. After
`body` returns, the scope returns whichever error fired first
(body's own, or the first child's fault).

## When NOT to use scope

Use `volt.launch` (no scope) when the spawned coroutine genuinely
needs to outlive the calling function. The canonical case is a
TCP server's per-connection handler:

```zig
fn serve() !void {
    var listener = try volt.io.TcpListener.bind(addr);
    defer listener.close();
    while (true) {
        const conn = try listener.accept();
        _ = try volt.launch(handle, .{conn});  // outlives this iteration
    }
}
```

The handler outlives the `accept` call by design — that's the whole
point. A scope would join the handler before the next `accept`,
which is the opposite of what you want.

## JoinSet — dynamic homogeneous tasks

Use when:

- You don't know how many children you'll spawn until runtime.
- All children return the same type.
- You want to consume results as they finish, not wait for all.

```zig
var set = volt.JoinSet(u32).init(allocator);
defer set.deinit();

for (urls) |url| {
    try set.spawn(fetch, .{url});
}

while (set.joinNext()) |result| {
    switch (result) {
        .ok => |v| processed += v,
        .err => |e| std.log.warn("fetch failed: {}", .{e}),
        .cancelled => {},
    }
}
```

`joinNext()` returns the next-finished result, or `null` when the
set is empty. Children are reaped in completion order, so the
fastest one surfaces first.

`set.deinit()` cancels and reaps any unfinished children. Use it
defensively even on the happy path:

```zig
var set = volt.JoinSet(u32).init(alloc);
defer set.deinit();
// ... spawn + joinNext loop ...
// If we exit early (return / error), deinit cancels the rest.
```

JoinSet is the dynamic-N analogue of `volt.scope` for static-N. Both
guarantee no child outlives their containing region.

## CancellationToken — explicit, hierarchical cancellation

Most cancellation in Volt happens automatically (timeouts, scope
errors, parent cancellation). For cases where you want to manually
cancel a group of children — say, a "stop button" that aborts an
ongoing batch — use `CancellationToken`:

```zig
var token = volt.CancellationToken.init();
defer token.deinit();

// Spawn workers that observe the token:
const j = try volt.launch(worker, .{ &token });

// Later:
token.cancel();   // sets isCancelled() = true on all linked tokens

j.cancel();       // also cancels the coroutine itself
try j.join();
```

Workers check the token at suspension points or explicitly:

```zig
fn worker(token: *volt.CancellationToken) !void {
    while (!token.isCancelled()) {
        try volt.yield();
        // ... work ...
    }
}
```

### Hierarchical cancellation

Tokens can have parents:

```zig
var root_token = volt.CancellationToken.init();
defer root_token.deinit();

var child_token = volt.CancellationToken.init();
defer child_token.deinit();

child_token.linkParent(&root_token);

// Cancelling root cancels child:
root_token.cancel();
// child_token.isCancelled() is now true.
```

`linkParent` is one-shot — once linked, the parent's cancel
cascades. Use this for request-scoped cancellation: a top-level
token for the request, sub-tokens for sub-tasks within the request,
all cancelled together when the request times out or is aborted.

## What gets cancelled when

| Action | Effect |
|---|---|
| `Scope.body` errors | Scope cancels remaining children + joins |
| `JoinSet.deinit` | Cancels unfinished children + joins |
| `withTimeout` deadline fires | Cancels child task |
| `Job.cancel()` / `Task.cancel()` | Sets cancel_flag + unparks current park → next suspension surfaces `error.Cancelled` |
| `CancellationToken.cancel()` | Sets isCancelled=true on token + linked children |
| `volt.run` returns | Cancels orphaned coroutines as part of teardown |

In every case, the cancelled coroutine wakes from its current park
(if any) and observes `error.Cancelled` from whatever it was
blocked on. This is what makes timeouts and structured-concurrency
errors "just work" — you don't have to thread a context through
every layer of code.

## Recommended pattern

For any region that spawns more than one task:

```zig
try volt.scope(struct {
    fn body(s: *volt.Scope) !void {
        try s.spawn(...);
        try s.spawn(...);
        // any logic here uses scope.fault(e) to surface child errors
    }
}.body);
```

For dynamic N:

```zig
var set = volt.JoinSet(T).init(alloc);
defer set.deinit();
for (items) |item| try set.spawn(work, .{item});
while (set.joinNext()) |r| { /* ... */ }
```

For "this might outlive me" (rare):

```zig
const j = try volt.launch(handler, .{conn});
// — at top level, defer destroyJob and join,
// — or hand off the Job to a long-lived owner.
```

Reaching for `volt.launch` directly should feel like reaching for
raw threads. Sometimes you need it; usually you don't.
