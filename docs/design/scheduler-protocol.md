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
    /// Single-atomic state. See encoding below.
    state: atomic.Value(usize) = .init(0),
};
```

### State encoding

The state is a single `usize` atomic. Bit 0 is the `NOTIFIED` flag
(`*Coroutine` alignment is ≥ 16, so we steal the low bit). Bits 1+ hold
either zero or a coroutine pointer.

| State value | Meaning |
|---|---|
| `0` | empty — no waiter, no pending notification |
| `NOTIFIED` (`= 1`) | unpark fired, no waiter registered |
| `coro_ptr` (low bit clear) | coroutine registered, no notification yet |

> **Why one atomic, not two.** The original "two atomics — `state: bool` +
> `wait_co: ?*Coroutine`" design (left visible in the case-analysis below
> for context) failed the IRIW litmus on ARM64: under release/acquire,
> `subscribe` could see `state == false` and `unpark` could see
> `wait_co == null` simultaneously, both returning without scheduling
> the coroutine — a lost wake. Collapsing into one atomic puts every
> transition into a single modification order, which is total. No
> cross-atomic interleavings exist.

### Park flow (the calling coroutine):

```
parkCurrent(park):
  if cmpxchg(state, NOTIFIED, 0) succeeded: return    # consume pending
  pending_event = &park.es
  swap to scheduler                                    # subscribe runs there
  # On resume, state is already 0 (cleared by whichever side scheduled us).
```

### Park.subscribe (runs on the worker, post-yield):

```
subscribe(park, coro):
  loop on CAS over state:
    if state == 0:        cmpxchg(state, 0, coro_ptr) — install
    if state == NOTIFIED: cmpxchg(state, NOTIFIED, 0) — consume + fast-wake
    else: panic (concurrent waiter — single-waiter invariant violated)
```

### Park.unpark (called by the waker):

```
unpark(park):
  loop on CAS over state:
    if state has NOTIFIED bit: return (idempotent)
    if state == 0:           cmpxchg(state, 0, NOTIFIED) — buffer
    if state == coro_ptr:    cmpxchg(state, coro_ptr, 0) — take + schedule
```

### Why this is race-free

Every transition is a CAS-loop on the same atomic. The modification order
is total. For each pair (subscribe, unpark) interleaving, the CAS that
lands first picks an unambiguous next state, and the loser retries against
the new state. Exactly one of subscribe and unpark schedules the coroutine.

**Case A: subscribe lands first.** state: `0 → coro_ptr`. unpark observes
`coro_ptr`, CAS-takes (`coro_ptr → 0`), schedules. ✓

**Case B: unpark lands first.** state: `0 → NOTIFIED`. subscribe observes
`NOTIFIED`, CAS-consumes (`NOTIFIED → 0`), fast-wakes. ✓

**Case C: lost CAS retry.** Both subscribe and unpark observe stale state,
race to CAS. Whichever wins moves the state forward. Loser re-reads under
the new state and converges into Case A or B. ✓

**Case D: multiple unparks.** First unpark: `0 → NOTIFIED` (or
`coro_ptr → 0`). Subsequent unparks observe the NOTIFIED bit set (or
state already cleared) and return. Idempotent.

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
