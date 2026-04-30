---
title: Spawning
description: launch, spawn, spawnBlocking — when to use which, and how Job and Task differ.
---

Volt has three flavors of "go run this concurrently." Pick by what
the function returns and where it should run:

| You want… | Use | Returns |
|---|---|---|
| Fire-and-forget on a worker | `volt.launch(fn, args)` | `*Job` |
| Value-returning on a worker | `volt.spawn(fn, args)` | `*Task(T)` |
| Synchronous code on a thread pool | `volt.spawnBlocking(fn, args)` | `T` (parks caller) |

## launch — fire-and-forget

Returns `*Job` — a handle that lets you cancel and join the
coroutine but doesn't carry a typed return value:

```zig
const j = try volt.launch(handler, .{conn});
defer volt.destroyJob(j);

// later, if you need to:
j.cancel();
try j.join();
```

`Job.join()` returns `error.Cancelled`, `error.StackOverflow`, or
void.

Use `launch` when you don't care about the return value — TCP
connection handlers, periodic background work, fan-out workers
writing into shared state.

## spawn — typed value

Returns `*Task(T)` — same handle plus a typed `join()`:

```zig
var t = try volt.spawn(parseRequest, .{buf});
defer volt.destroyTask(t);

const req = try t.join();   // returns the user fn's value or error
```

`Task(T).join()` returns the user fn's value, or its error union
type unioned with `error{Cancelled, StackOverflow}`.

Use `spawn` when you need the result. The extra cost over `launch`
is one heap-allocated result slot.

## spawnBlocking — synchronous code on a thread pool

```zig
const hash = try volt.spawnBlocking(sha256, .{data});
```

This is *not* the same shape as `launch`/`spawn`. `spawnBlocking`:

- Submits `sha256(data)` to a separate **blocking thread pool**
  (lazily created on first call; idle workers expire after 10s).
- **Parks the calling coroutine** until the work finishes.
- Returns the value (or error) directly. There is no Job/Task
  handle; the parked coroutine resumes when the pool thread
  finishes.

Use `spawnBlocking` for:

- CPU-heavy work (hashing, parsing, compression).
- Sync C library calls that block (most third-party libs).
- File I/O on platforms without io_uring (Volt's `volt.fs` already
  uses the blocking pool internally).

Don't use `spawnBlocking` for:

- Microsecond work — submitting to the pool costs more than the work.
- Already-async code — just `volt.spawn` it instead.

To run multiple blocking calls **concurrently**, spawn one Volt
coroutine per call:

```zig
fn worker(sink: *Sink, idx: usize, data: []const u8) !void {
    sink.out[idx] = try volt.spawnBlocking(sha256, .{data});
}

const j1 = try volt.launch(worker, .{ &sink, 0, blob1 });
const j2 = try volt.launch(worker, .{ &sink, 1, blob2 });
defer volt.destroyJob(j1);
defer volt.destroyJob(j2);
try j1.join();
try j2.join();
```

Each `spawnBlocking` call only parks its own coroutine; concurrent
coroutines park independently and the blocking pool services them
in parallel.

## Job and Task — the handle API

Lifecycle of a Job from your code's perspective:

```
   volt.launch(fn, args)
            │
            ▼
       ┌─────────┐    cancel      ┌────────────┐
       │ running │ ─────────────► │ cancelled  │
       └────┬────┘                 └────────────┘
            │                            │
   fn returns│                           │ join surfaces
            │                            │ error.Cancelled
            ▼                            │
     ┌────────────┐                       │
     │ completed  │ ───── join ──────────►│
     └────┬───────┘   surfaces value     │
          │                              │
   stack ovf            ┌────────────┐  │
   detected by ────────►│ overflowed │──┤
   SIGSEGV handler      └────────────┘  │
                                         ▼
                                   ┌──────────────┐
                                   │ destroyJob() │
                                   └──────────────┘
                                   you free the handle
```

Both share the same surface for cancellation and state queries:

```zig
const j = try volt.launch(work, .{ctx});

j.cancel();             // wake whatever the coroutine is parked on; cancel_flag = true
j.isActive();           // true while running or parked
j.isCompleted();        // true once .done
j.isCancelled();        // true if cancel was observed and propagated
j.isOverflowed();       // true if stack overflow caught by SIGSEGV handler

j.state();              // single-shot enum: .running | .completed | .cancelled | .overflowed

j.setName("name");                  // surfaces in observability snapshots
j.setSpawnSite(@src());             // " " " "

try j.join();           // park caller until done; returns Cancelled/StackOverflow/void
```

`Task(T)` adds a typed `join() T` (well, `(E||RunErr)!T`) and
mirrors all the predicates and setters.

## Lifetime: who owns what

The Job/Task handle is **heap-allocated** and **owned by you**.
Always pair `launch`/`spawn` with `destroyJob`/`destroyTask`. The
cleanest pattern:

```zig
const j = try volt.launch(work, .{});
defer volt.destroyJob(j);
try j.join();   // or j.cancel() + j.join()
```

The coroutine itself (the stack, the `Coroutine` struct) is owned
by the runtime. The handle holds a pointer; the runtime reaps the
coroutine on its own schedule once `.done`. You free the handle;
the runtime frees the coroutine.

## When NOT to use these directly

If you're spawning more than one task in a region and want them to
all complete before the region exits, use `volt.scope` (structured
concurrency) instead. It owns the join-on-exit guarantee for you:

```zig
try volt.scope(struct {
    fn body(s: *volt.Scope) !void {
        try s.spawn(workerA, .{});
        try s.spawn(workerB, .{});
        // when this returns, both workers have joined.
    }
}.body);
```

See [Structured Concurrency](/usage/structured-concurrency/) for
when `scope` is the right choice (almost always).

## Spawning across threads

`launch` and `spawn` work the same regardless of which worker they
run on. You can call them from inside any coroutine; the runtime
finds a worker for the new task (typically the LIFO slot of the
calling worker, falling back to the local deque). Cross-thread
spawn from a non-coroutine context (e.g., a signal handler) is
**not** supported — those paths assume a current coroutine. To
publish work from outside the runtime, use a `Channel(T)` whose
sender side is `trySend` (lock-free, callable from anywhere) and
whose receiver runs inside a coroutine.

## yield

```zig
try volt.yield();
```

Voluntarily release the worker to other coroutines. Returns
`error.Cancelled` if the calling coroutine was cancelled. Useful in
two cases:

- A CPU-bound loop with no other suspension points needs an
  explicit cancellation check.
- Manually balancing fairness — you've held the worker for a while
  and want to give siblings a chance.

For most code, you don't need `yield` — the next I/O / channel /
sleep call already releases the worker.
