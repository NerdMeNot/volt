---
title: Structured Concurrency
description: volt.Cancel is data you thread through code. volt.scope ties Cancel lifetime to a lexical block. Cancel-aware variants of every blocking op wake with error.Cancelled.
---

Volt's cancellation model is Go's `context.Context` in shape, with
the parking lot integration making cancellation propagate cleanly
through arbitrary blocking ops.

The pieces:

- **`volt.Cancel`** — handle. An atomic flag + a spinlock-protected
  waiter list. `c.fire()` flips the flag and wakes everyone parked
  on it.
- **`*Cancel` as parameter** — functions opt in by accepting one.
  Library code that may park threads it through; leaf functions
  use it.
- **Cancel-aware blocking ops** — `Mutex.lockCancel`,
  `Spsc.recvCancel`, `Mpmc.sendCancel`, `Oneshot.recvCancel`. They
  return `error.Cancelled` when the held Cancel fires.
- **`volt.scope(body)`** — lexical lifetime. Constructs a Cancel,
  runs `body(*Cancel)`, fires the Cancel automatically on error.

## `volt.Cancel`

```zig
pub const Cancel = struct {
    pub fn init(rt: *Runtime) Cancel { ... }
    pub fn deinit(self: *Cancel) void { ... }   // asserts waiter list empty
    pub fn isFired(self: *const Cancel) bool { ... }
    pub fn fire(self: *Cancel) void { ... }     // idempotent
    pub fn checkpoint(self: *const Cancel) error{Cancelled}!void { ... }
    // (register / deregister are internal — used by cancel-aware ops)
};
```

Construct on a Runtime; pass `&cancel` to any function that may
park; call `fire()` from anywhere to cancel.

```zig
var c = volt.Cancel.init(volt.runtime());
defer c.deinit();

const t = try volt.spawn(longWork, .{&c});

volt.sleep(100 * std.time.ns_per_ms);
c.fire();                  // wakes longWork from any cancel-aware op
_ = t.join();              // longWork's return propagated
```

`Cancel.fire()` is idempotent — calling twice is a no-op. Safe to
call from any thread (driver or worker).

`Cancel.deinit()` asserts the waiter list is empty. If a coroutine
is parked on this Cancel when `deinit` runs, you have a use-after-
free bug. The typical pattern (`defer c.deinit()` after children
have joined) makes this trivially correct.

## `Cancel.checkpoint()`

For CPU-bound loops that don't otherwise park:

```zig
fn cpuLoop(c: *volt.Cancel) error{Cancelled}!u64 {
    var sum: u64 = 0;
    var i: u64 = 0;
    while (i < 10_000_000) : (i += 1) {
        if (i % 1000 == 0) try c.checkpoint();
        sum +%= i *% 2654435761;
    }
    return sum;
}
```

`checkpoint` is a single atomic load + branch when not fired —
cheap enough to call every ~1000 iterations of inner work. Returns
`error.Cancelled` if fired.

## Cancel-aware blocking ops

Every blocking primitive has a `*Cancel`-taking variant. The
non-Cancel form stays for callers that don't care; library code
that might cancel takes the Cancel form.

```zig
// Mutex
try mu.lockCancel(c);
// Channels
const v = try spsc.recvCancel(c);
try mpmc.sendCancel(v, c);
const result = try oneshot.recvCancel(c);
```

Internally, the cancel-aware variants register a waiter on the
Cancel under the primitive's bucket lock. The validator-under-lock
pattern (see [parking lot](/architecture/parking-lot/)) closes the
register-then-park race: the check for "is this Cancel already
fired?" and the queueing of the waiter happen atomically. Cancel.fire
walks the waiter list and unparks each on the primitive's address —
the primitive's regular wake path observes the unpark, the
cancel-aware op re-checks the Cancel flag, sees it fired, and
returns `error.Cancelled`.

## `volt.scope`

Run a body that gets a `*Cancel`; on error, fire the Cancel before
propagating. The minimum-viable structured-concurrency primitive:

```zig
pub fn scope(comptime body: anytype) anyerror!void {
    var c = Cancel.init(runtime());
    defer c.deinit();
    body(&c) catch |e| {
        c.fire();
        return e;
    };
}
```

Usage:

```zig
fn parallelFetch() !void {
    try volt.scope(struct {
        fn body(c: *volt.Cancel) anyerror!void {
            const ta = try volt.spawn(fetchOne, .{ "https://a", c });
            const tb = try volt.spawn(fetchOne, .{ "https://b", c });
            const a = try ta.join();
            const b = try tb.join();
            // process a, b
        }
    }.body);
}

fn fetchOne(url: []const u8, c: *volt.Cancel) ![]u8 {
    try c.checkpoint();
    var conn = try volt.net.TcpStream.connect(...);
    defer conn.close();
    // ... use c.checkpoint() / recvCancel between blocking ops
    return result;
}
```

If `body` returns OK, the Cancel never fires (children are assumed
done — `body` is responsible for its own joins).

If `body` returns an error (because `ta.join()` propagated an error
from `fetchOne`, or because some inline check failed), `scope`
fires the Cancel. Any sibling child parked on a cancel-aware op
wakes with `error.Cancelled`. `body` then propagates the original
error up.

**`scope` does not auto-await children.** If `body` returns
without joining its spawns, you'll leak the Tasks (and possibly
the children themselves will outlive the scope — which is a
correctness bug). Always join.

## Why this shape

Volt rejected Tokio's "abort the future from outside" model (where
you call `task.abort()` and the runtime forces the future to
yield with an `Aborted` error). That works for Rust because every
async fn is a state machine and aborting it just drops the state.
For stackful coroutines, "drop the future" doesn't map cleanly — the
coroutine has a real stack with destructors, in-flight syscalls,
held locks. You can't just yank the rug out.

Go's `context.Context` is the model that works for stackful: pass
the cancellation **as data** through code, let library functions
check it at their own suspension points, return errors to unwind.
Volt does this with `*Cancel` and the parking-lot integration that
makes the propagation prompt.

The tradeoff: `*Cancel` is plumbing. Every cancel-aware function
takes one. You can't retroactively make a function cancellable
without changing its signature. The benefit: cancellation is
explicit and visible — you can see exactly where cancellation can
flow into your code by looking at parameter lists.

## Multiple Cancels

A function can hold multiple Cancels (e.g. a request-scoped one
and a global shutdown one). Cancel-aware ops only take a single
`*Cancel`; for the OR-of-Cancels case, you compose by checking
multiple via `checkpoint`:

```zig
fn workWithBoth(req: *volt.Cancel, shutdown: *volt.Cancel) !void {
    while (true) {
        try req.checkpoint();
        try shutdown.checkpoint();
        // ... do a unit of work ...
    }
}
```

A more elegant abstraction (composite Cancel) is library territory
and may land later.

## See also

- [Spawning](/usage/spawning/) — `volt.spawn`, `Task(T).join`.
- [The parking lot](/architecture/parking-lot/) — how cancel-aware blocking ops integrate.
- [Cancellation internals](/architecture/) — the waiter list, validator pattern, race analysis.
