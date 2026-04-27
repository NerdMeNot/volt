# Volt Scheduler Protocol

The park/wake/dispatch protocol that the multi-worker runtime is built on.
Adopted from the [`may`](https://github.com/Xudong-Huang/may) Rust crate
(stackful coroutines + work-stealing + reactor — same architecture as Volt)
because it solves the wake-loss / pre-park race in a way that doesn't
require per-coroutine state-machine gymnastics.

## Invariants

> **Possession of `*Coroutine` is the state.** At any moment, exactly one
> location holds the pointer. Transfers between locations are atomic.

Locations a `*Coroutine` can be:

- **Worker dispatch frame** — a local variable in `Worker.dispatch`, held
  while the coroutine is on-CPU.
- **Worker run queue (Chase-Lev deque)** — runnable, waiting to be popped.
- **Global injection queue** — runnable, waiting to be picked up by any worker.
- **`Park.wait_co`** — parked, held by the thing the coroutine is waiting on.
- **Done sink** — finished, owned by the runtime for eventual reaping.

There is no atomic `state` enum on the coroutine. There is no "lifecycle"
field with PARKED/RUNNABLE/RUNNING transitions. The coroutine is wherever
its pointer is.

## EventSource — the dispatch contract

When a coroutine yields, it tells the worker "what to do with me" by setting
a pointer to an `EventSource`:

```zig
pub const EventSource = struct {
    subscribe_fn: *const fn (*anyopaque, *Coroutine) void,
};
```

The worker dispatch loop, after swap-back from the coroutine:

```zig
const es = coro.pending_event.?;       // coroutine set this before yielding
coro.pending_event = null;
es.subscribe_fn(es, coro);             // hand off ownership
```

Three EventSources cover the entire scheduling vocabulary:

| EventSource | `subscribe` does |
|---|---|
| `Yield` (singleton) | Push `coro` back onto the calling worker's run queue. |
| `Park` (per-waitable) | Store `coro` in `wait_co`; re-check `state` for fast-wake. |
| `Done` (singleton) | Mark `coro.done_flag = true`; unpark any waiter on `coro.join_park`. |

The trampoline always sets `pending_event = &Done` before its final swap,
so a coroutine that returns from its user fn signals "done" the same way
it signals "park" or "yield".

## The Park struct

`Park` is the universal suspend-and-resume primitive. Embedded in any
waitable type (channel, mutex, join handle, reactor wait, timer).

```zig
pub const Park = struct {
    es: EventSource = .{ .subscribe_fn = &subscribe },
    state: atomic.Value(bool) = .init(false),       // unpark fired
    wait_co: atomic.Value(?*Coroutine) = .init(null),
};
```

### Park flow (the calling coroutine):

```
parkCurrent(park):
  if park.state.swap(false): return         # unpark already arrived
  yieldWith(&park.es)                        # swap to scheduler
  _ = park.state.swap(false)                 # drain residue
```

### Park.subscribe (runs on the worker, post-yield):

```
subscribe(park, coro):
  park.wait_co.store(coro)                   # register
  if park.state.load:                        # unpark slipped in?
    if c = park.wait_co.swap(null):          # take back
      schedule(c)                            # fast-wake
```

### Park.unpark (called by the waker):

```
unpark(park):
  if !park.state.swap(true):                 # we're the first unpark
    if c = park.wait_co.swap(null):          # take registered coro
      schedule(c)                            # wake
```

### Why this is race-free

Every interleaving of `subscribe` and `unpark` is safe:

**Case A: subscribe completes, then unpark.** subscribe stores `coro` in
`wait_co`, sees `state == false`, returns. unpark swaps `state` to true,
takes `coro` from `wait_co`, schedules. ✓

**Case B: unpark completes, then subscribe.** unpark sets `state = true`
(no wait_co to take). subscribe stores `coro`, sees `state == true`, takes
`coro` back from `wait_co`, fast-wakes. ✓

**Case C: subscribe and unpark interleave.** Both atomic operations on
`state` and `wait_co` happen. Whichever order they land:
- Either unpark's `state.swap(true)` returns false AND its `wait_co.swap(null)` returns the coro → unpark schedules.
- Or unpark's `wait_co.swap(null)` returns null AND subscribe's `state.load` returns true → subscribe fast-wakes.

The atomic happens-before from `state.swap` (release on unpark, acquire on
subscribe) and `wait_co.swap` (release on subscribe, acquire on unpark)
guarantees one of the two paths schedules the coroutine.

**Case D: multiple unparks.** Only the first `state.swap(true)` returns
false, so only one unpark performs the wake. Idempotent.

## Schedule — placing a runnable coroutine

```
schedule(coro):
  if currentWorker():                        # we're on a worker thread
    push to local deque
    worker.unpark()                          # no-op if not parked
  else:                                      # cross-thread caller
    inject to global queue
    runtime.notifyOneWorker()
```

The `notifyOneWorker` race (worker between findWork and parker.park) is
absorbed by the parker's `unpark_pending` flag — `unpark` always sets it,
even if the worker isn't observably parked.

## Done — coroutine completion

```zig
const Done = struct {
    es: EventSource = .{ .subscribe_fn = &subscribe },
};

fn subscribe(_: *anyopaque, coro: *Coroutine) void {
    coro.done_flag.store(true, .release);
    coro.join_park.unpark();      // wake whoever's joining (if anyone)
}
```

`Job.join` becomes a thin wrapper over `Park`:

```zig
fn join(job: *Job) !void {
    if (job.coro.done_flag.load(.acquire)) return;
    job.coro.join_park.parkCurrent();
    // resumed by Done.subscribe's unpark; coro is now done
}
```

## Cancellation

Cancellation stays on the coroutine because it must propagate across
arbitrary suspension points. `parkCurrent` checks `cancel_flag` before
calling `yieldWith`, and again on resume. The same atomic bool we have
today.

## Why this is better than the state-bit-on-coroutine model

The state-bit model forces the coroutine to coordinate with every possible
waker about "where is this coroutine right now?" — and waker/parker races
need explicit handling (pre-wake, NOTIFY bit, etc.). Each new waker
introduces a new race-shape.

The EventSource model decouples them. The coroutine doesn't know who's
waking it; the waker doesn't know what state the coroutine is in. They
synchronize via the `Park` instance — a small, fixed atomic vocabulary
that's local to the waitable thing.

Adding a new primitive (channel, semaphore, etc.) means embedding a `Park`
or implementing a custom EventSource. No changes to the scheduler. No new
races to reason about.

## Mapping to existing Volt code

Files that change:

- `src/coroutine/coroutine.zig`: drop the atomic `state` enum; add
  `pending_event: ?*EventSource`, `done_flag: atomic.Value(bool)`,
  `join_park: Park`. Keep `cancel_flag`.
- `src/coroutine/spawn.zig`: trampoline sets `pending_event = &Done` before
  its final swap.
- `src/scheduler/event_source.zig` (new): EventSource struct + Yield singleton.
- `src/scheduler/park.zig`: rewrite as the `Park` struct from this doc.
- `src/scheduler/worker.zig`: dispatch reads `pending_event` and calls
  `subscribe` instead of switching on a state enum.
- `src/api/yield.zig`: `volt.yield()` becomes `yieldWith(&Yield.singleton)`.
- `src/task/job.zig`: `join` becomes parkCurrent on `coro.join_park`.

Files unchanged:
- `coroutine/context_arm64.zig` (asm trampoline)
- `coroutine/stack.zig`
- `scheduler/deque.zig`
- `scheduler/injection.zig`
- `io/reactor.zig` (reactor's wakeFn becomes a Park.unpark equivalent)
- `io/wait.zig`, `io/io.zig`, `io/net.zig`
