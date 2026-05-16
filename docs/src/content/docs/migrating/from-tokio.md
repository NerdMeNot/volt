---
title: Coming from Tokio (Rust)
description: Same runtime shape, opposite programming model. Stackful instead of stackless means no async/await, no Future, no Pin, no lifetime gymnastics. Same M:N scheduler.
---

If you've used Tokio, the runtime architecture will feel
familiar — work-stealing scheduler, parking-lot wait/wake,
single-poller reactor claim. The implementation borrows from
Tokio's design.

The programming model is the inverse. Tokio is **stackless**:
every `async fn` compiles to a state machine, the runtime polls
it, lifetimes plumb through the future type. Volt is **stackful**:
each task has a real OS stack, code reads like blocking I/O, the
runtime suspends at every wait point.

For Zig this is the right call (no `async`/`await` keyword;
won't get one) — but moving from Tokio to Volt is the biggest
conceptual shift of the three migration paths.

## Mental model

> Forget `Future`, `Poll`, `Pin`. A coroutine in Volt is a
> function that runs on its own stack. When it calls
> `recv()` / `lock()` / `read()`, the runtime saves its
> registers, runs other coroutines on the same worker thread,
> and resumes it when the wait completes. From inside the
> function, the call returned a value — there's no
> indication a suspend happened.
>
> No state machine generation. No "every async fn has a
> different type." No `Pin<Box<dyn Future>>`. No
> `'static + Send` lifetimes on spawn arguments. Pointers to
> stack-locals stay valid across suspensions.

The cost: ~16 KiB resident per coroutine (vs ~256 bytes for a
typical Tokio task's state machine). For workloads with
millions of mostly-idle tasks, stackless wins on memory.
For everything else — HTTP servers, pipelines, network proxies,
CLIs — stackful's ergonomics dominate.

## The 60-second translation

| Tokio | Volt |
|---|---|
| `#[tokio::main]` | `Runtime.init` + `rt.run(root, .{})` |
| `tokio::spawn(future)` | `try volt.spawn(fn, args)` |
| `JoinHandle<T>` | `*volt.Task(T)` |
| `handle.await` | `t.join()` |
| `handle.abort()` | Fire a `*Cancel` the task holds |
| `tokio::sync::Mutex` | `volt.Mutex` |
| `tokio::sync::Notify` | `volt.Notify` |
| `tokio::sync::Semaphore` | `volt.Semaphore` |
| `tokio::sync::mpsc::channel(cap)` | `volt.Mpmc(T, cap)` (with single consumer; or fan-in) |
| `tokio::sync::oneshot::channel()` | `volt.Oneshot(T)` |
| `tokio::sync::watch::channel(init)` | `volt.Watch(T)` |
| `tokio::sync::broadcast::channel(cap)` | `volt.Broadcast(T, cap)` |
| `tokio::time::sleep(dur).await` | `volt.sleep(ns)` |
| `tokio::time::timeout(dur, future)` | `volt.scope` + watchdog (see below) |
| `tokio::select! { ... }` | **Not in Volt today.** Multiplex via Mpmc. |
| `TcpListener::bind(addr).await` | `volt.net.TcpListener.bind(.any4(port))` |
| `stream.read(buf).await` | `stream.read(buf)` |
| `CancellationToken` | `volt.Cancel` |
| `Future::poll` / `task::yield_now()` | `volt.yield()` |

## Side-by-side: spawn-then-await

```rust
// Tokio
use tokio::task;

async fn parallel_sum() -> u64 {
    let a = task::spawn(compute(1, 100));
    let b = task::spawn(compute(101, 200));
    a.await.unwrap() + b.await.unwrap()
}

async fn compute(lo: u64, hi: u64) -> u64 {
    (lo..=hi).sum()
}
```

```zig
// Volt
fn parallelSum() !u64 {
    const a = try volt.spawn(compute, .{ @as(u64, 1), @as(u64, 100) });
    const b = try volt.spawn(compute, .{ @as(u64, 101), @as(u64, 200) });
    return a.join() + b.join();
}

fn compute(lo: u64, hi: u64) u64 {
    var sum: u64 = 0;
    var i: u64 = lo;
    while (i <= hi) : (i += 1) sum += i;
    return sum;
}
```

The shape is the same: spawn two children, await/join both.
Differences:

- **No `.await`**: `a.join()` parks the calling coroutine; from
  the source it looks like a blocking call.
- **No error union from join**: `Task.join` returns `T`, not
  `JoinResult<T>`. If the spawned function panicked, the
  process aborts (no `JoinError::Panic` recovery).
- **No `'static`**: spawn args can reference local stack values,
  as long as the caller stays alive until `join`. The borrow
  checker isn't policing it — it's the manual lifetime
  contract.

## Side-by-side: cancellation

```rust
// Tokio — abort via JoinHandle
async fn deadline_work() {
    let handle = tokio::spawn(long_running());
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.abort();
    let _ = handle.await;
}
```

```zig
// Volt — Cancel as data
fn deadlineWork() !void {
    try volt.scope(struct {
        fn body(c: *volt.Cancel) anyerror!void {
            const t = try volt.spawn(longRunning, .{c});
            volt.sleep(100 * std.time.ns_per_ms);
            c.fire();
            _ = t.join();
        }
    }.body);
}

fn longRunning(c: *volt.Cancel) error{Cancelled}!void {
    try c.checkpoint();
    // ... cancel-aware blocking ops...
}
```

Tokio's model: `abort()` drops the future. The future's
destructors run; the runtime wakes the future with
`Poll::Ready(Err(JoinError::Cancelled))`. Works because dropping
a state machine is well-defined.

Volt's model: `Cancel.fire()` wakes the task from any
cancel-aware blocking op with `error.Cancelled`. The task
returns normally up its call stack — destructors run via Zig's
`defer`. **You can't "yank the rug" on a stackful task** because
the stack is in the middle of executing code; the task has to
observe the cancel itself.

Implication: every cancel-aware function takes `*Cancel`.
Cancellation is explicit data flow, like Go's `context.Context`.
See [Cancellation internals](/architecture/cancellation-internals/)
for why this is the only model that works for stackful.

## Side-by-side: select / race

```rust
// Tokio — select!
tokio::select! {
    msg = ch1.recv() => handle1(msg),
    msg = ch2.recv() => handle2(msg),
    _ = tokio::time::sleep(Duration::from_secs(5)) => timeout(),
}
```

```zig
// Volt — no select; multiplex via Mpmc
const Msg = union(enum) {
    from_ch1: T1,
    from_ch2: T2,
    timeout: void,
};

fn multiplex(ch1: *volt.Spsc(T1, 16), ch2: *volt.Spsc(T2, 16)) !void {
    var fanin: volt.Mpmc(Msg, 4) = .{};
    defer fanin.deinit();

    try volt.scope(struct {
        fn body(c: *volt.Cancel) anyerror!void {
            // Forwarders for each "branch":
            _ = try volt.spawn(forwarder1, .{ ch1, &fanin, c });
            _ = try volt.spawn(forwarder2, .{ ch2, &fanin, c });
            _ = try volt.spawn(timer, .{ &fanin, c, 5 * std.time.ns_per_s });

            const msg = try fanin.recv();
            switch (msg) {
                .from_ch1 => |v| handle1(v),
                .from_ch2 => |v| handle2(v),
                .timeout => timeout(),
            }

            c.fire();   // cancel the losers
        }
    }.body);
}
```

Volt doesn't ship a `select` primitive. The workaround is to
fan-in via an `Mpmc(Msg, cap)` channel where each branch has a
forwarder coroutine. First message to land wins; Cancel fires
the losers.

A first-class `volt.select` may land later. For most cases the
fan-in pattern is acceptable; the verbosity is real but
isolated.

## Side-by-side: timeout

```rust
// Tokio
let result = tokio::time::timeout(
    Duration::from_secs(5),
    do_work(),
).await;
match result {
    Ok(v) => use_value(v),
    Err(_) => handle_timeout(),
}
```

```zig
// Volt — scope + watchdog (no first-class timeout helper)
try withTimeout(5 * std.time.ns_per_s, struct {
    fn b(c: *volt.Cancel) anyerror!void {
        try doWorkCancel(c);
    }
}.b);

fn withTimeout(ns: u64, comptime body: anytype) !void {
    try volt.scope(struct {
        fn b(c: *volt.Cancel) anyerror!void {
            const watchdog = try volt.spawn(struct {
                fn run(deadline_ns: u64, cancel: *volt.Cancel) void {
                    volt.sleep(deadline_ns);
                    cancel.fire();
                }
            }.run, .{ ns, c });

            const result = body(c);
            c.fire();
            watchdog.join();
            return result;
        }
    }.b);
}
```

Full recipe in [Timeout with Retry](/cookbook/timeout-retry/).

## Gotchas for Rust developers

### No borrow checker

Zig has no borrow checker. Lifetimes are manual. The discipline
that the borrow checker enforces for you in Rust — "don't hold
a reference across a suspension if the reference's owner can
move" — is on you in Zig.

In practice this is rarely an issue with Volt because:

- Stack locals stay valid across suspensions (the stackful
  model). No `Pin` problem.
- Heap allocations are explicit; you control their lifetime.
- Cross-thread references work because there's no marker trait
  requirement, but you're responsible for synchronisation.

The bugs that bite are different: missing `deinit`, holding a
`Mutex` across a suspension that creates lock inversion, etc.
See [Common pitfalls](/guides/common-pitfalls/).

### No `Send` / `Sync` marker traits

Spawn arguments can be anything. The runtime doesn't check that
they're thread-safe — if the spawned coroutine touches a
non-thread-safe value from a different worker, you have a data
race. You opt in to synchronisation manually with `Mutex` /
atomic types / channels.

### Channels don't take a sender count

```rust
// Tokio
let (tx, rx) = mpsc::channel(16);
tokio::spawn(producer1(tx.clone()));
tokio::spawn(producer2(tx.clone()));
drop(tx);
```

```zig
// Volt
var ch = volt.Mpmc(T, 16).init();
defer ch.deinit();
_ = try volt.spawn(producer1, .{&ch});
_ = try volt.spawn(producer2, .{&ch});
// No "drop the last sender" semantic; explicitly close:
// ch.close();
```

Tokio tracks sender count via `Sender::clone` / `Drop`. When the
last sender drops, receivers see channel closed. Volt has no
equivalent — channels are closed explicitly via `close()`. The
ownership / drop-tracking shape of Rust doesn't transfer; you
write the close call yourself.

### Errors are values, not `Result<T, E>`

Same fundamental shape as Rust's `Result`, but Zig's error union
is a different type than the success type:

```rust
let v: Result<u64, MyError> = compute();
```

```zig
const v: MyError!u64 = compute();
```

`try` is the propagation shortcut; `catch |e| switch (e) { ... }`
is the matched-on-error form. No `?` operator (that's Rust's
`Result::?` chained against `From::from`); no `From` trait
conversions.

### No async traits

Async traits in Rust are still gnarly (associated types,
`async-trait` crate, `dyn Future`). Volt doesn't have traits at
all in the Rust sense — it has comptime polymorphism and
`*anyopaque` pointers for type-erased dispatch. Different
ergonomics, different gotchas.

For "I want to pass a function that does async work as a
parameter," pass a function pointer + closure-style struct
explicitly. See how `volt.scope(body)` takes a comptime function
parameter.

### Coroutines move; data they touched might not

A coroutine can resume on a different worker thread than the one
it suspended on. Anything thread-local that you read before a
suspension may be different after. In Rust you'd carry the
context as a parameter; in Volt do the same.

`volt.runtime()` is safe across yields (it always returns the
same Runtime — there's one per process bound by the current
coroutine).

### Stack size

Tokio futures are typically a few hundred bytes of state. Volt
coroutines are 16 KiB committed (one Darwin page) per
coroutine, growing in 16 KiB increments via the SIGSEGV
handler. For workloads with millions of mostly-idle tasks,
this is a substantial memory difference; for thousands of
active connections, it's negligible.

If you need 1M+ idle connections, Tokio is the right tool.
For 10K+ active connections, Volt's ergonomics dominate.

## What's missing (relative to Tokio)

| Tokio feature | Volt status |
|---|---|
| `async` / `await` syntax | Not applicable (Zig has no async keyword) |
| `tokio::select!` macro | Not in core; multiplex via Mpmc |
| `tokio::time::timeout` | Not in core; scope + watchdog (recipe) |
| `tokio::time::interval` | Not in core; loop with `volt.sleep` |
| `tokio::sync::RwLock` | Not in core; build on parking lot |
| `tokio::sync::Barrier` | Not in core; `Semaphore.init(0)` + N releases |
| `tokio::sync::OnceCell` | Not in core; atomic flag + Mutex |
| `tokio::fs` | Out of scope; coming in volt-fs library |
| `tokio::net::UdpSocket` | Out of scope; coming in volt-net |
| `tokio::net::TcpStream` (full) | TCP only on Darwin today |
| `tokio::process::Command` | Out of scope |
| `tokio::signal` | Out of scope; use std.posix.sigaction directly |
| `Stream` / `Sink` traits | Not applicable; iterator-style loops instead |
| `tokio::pin!` | Not applicable; no Pin needed |
| `JoinSet` | Not in core; ArrayList of `*Task(T)` |
| `JoinHandle::is_finished` | `Task.isDone()` |
| `LocalSet` (non-Send tasks) | Not applicable; no Send trait |

## Performance shape

| Workload | Tokio (approx) | Volt |
|---|---|---|
| Context switch / yield | ~100 ns (poll + state machine update) | 9 ns |
| Channel send+recv (Mpsc) | ~50-100 ns | 12 ns Spsc / 54 ns Mpmc |
| Mutex contended | ~50-100 ns (depends on contention) | 15 ns |
| Spawn + wait | ~150 ns | 101 ns (w=1) / 575 ns (w=11) |

Volt is faster on context switch because stackful skips the
poll + state-machine-step overhead. Behind on multi-worker
spawn-heavy because Tokio's task allocation is much cheaper
(state machines, not stacks). Real-world workloads land
closer to parity than the synthetic micros suggest.

## When to stay with Tokio

- You need 100K+ idle connections per process (the per-task
  memory matters).
- You want the existing Rust ecosystem (hyper, tonic,
  sqlx, ...).
- You want the borrow checker to enforce lifetime correctness.

## When Volt is the better fit

- You're writing in Zig and want a coroutine runtime.
- You want code that reads like blocking I/O without
  `async`/`await` syntax noise.
- Per-task memory of ~16 KiB is fine for your workload.
- You're OK with Darwin-only today, Linux coming.

## Further reading

- [Stackful by design](/architecture/stackful-design/) — the
  detailed why-stackful-not-stackless argument.
- [Tokio + Go comparison](/appendix/tokio-go-comparison/) — the
  feature-mapping table.
- [Architecture overview](/architecture/) — Volt borrows the
  same scheduler shape Tokio uses; the architecture chapter is
  shaped similar to Tokio's design docs.
- [Cancellation internals](/architecture/cancellation-internals/) —
  why Volt's cancellation is Go-style data flow, not
  Tokio-style abort.
