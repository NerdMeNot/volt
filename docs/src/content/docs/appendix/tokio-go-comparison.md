---
title: Tokio + Go comparison
description: Feature-by-feature mapping of Volt against Tokio (Rust) and Go's runtime. Useful when you're coming from one and want to find the equivalent.
---

This page maps Volt's vocabulary against the two most-likely
reference points: Tokio (Rust) and Go's runtime. The table isn't
prescriptive about "which is best" — it's about helping readers
find the equivalent shape they already know.

For narrative-form migration walkthroughs (mental-model shifts,
side-by-side code, gotchas), see:

- [Coming from Go](/migrating/from-go/)
- [Coming from Tokio (Rust)](/migrating/from-tokio/)
- [Coming from Node.js](/migrating/from-nodejs/)

This page is the dense reference table they cross-link to.

## Concept mapping

| Concept | Volt | Tokio (Rust) | Go |
|---|---|---|---|
| Concurrency unit | Coroutine (stackful) | Future (state machine) | Goroutine (stackful) |
| Spawn | `volt.spawn(fn, args)` | `tokio::spawn(future)` | `go fn()` |
| Spawn handle | `*Task(T)` | `JoinHandle<T>` | (none — no return value, errors via channels) |
| Bootstrap | `Runtime.init` + `rt.run(fn, args)` | `tokio::main` macro or `Builder::build`+`block_on` | `main()` runs as goroutine 1 |
| OS-thread workers | `M` (per-Runtime) | tokio runtime threads | `M` (per process) |
| Per-worker sched state | `P` | (internal scheduler queues per thread) | `P` (Go calls it P too) |
| Cancellation primitive | `volt.Cancel` (data) | `JoinHandle::abort` (in-future) + `CancellationToken` (data) | `context.Context` (data) |
| Cooperative yield | `volt.yield()` | `tokio::task::yield_now().await` | `runtime.Gosched()` |
| Sleep | `volt.sleep(ns)` | `tokio::time::sleep(dur).await` | `time.Sleep(dur)` |
| TCP listen | `volt.net.TcpListener.bind(...)` | `TcpListener::bind(...).await` | `net.Listen("tcp", ...)` |
| Mutex | `volt.Mutex` (coroutine) | `tokio::sync::Mutex` (async) | `sync.Mutex` (OS-blocking) |
| Channel (1:1) | `volt.Spsc(T, cap)` | `tokio::sync::mpsc::channel(cap)` (with single consumer) | `make(chan T, cap)` |
| Channel (M:N) | `volt.Mpmc(T, cap)` | (no built-in; via `crossbeam`/`flume`) | `make(chan T, cap)` |
| One-shot | `volt.Oneshot(T)` | `tokio::sync::oneshot::channel()` | (built from chan T with buffer=1) |
| Latest-value | `volt.Watch(T)` | `tokio::sync::watch::channel(init)` | (none — built manually) |
| Broadcast | `volt.Broadcast(T, cap)` | `tokio::sync::broadcast::channel(cap)` | (none — built manually) |
| Structured concurrency | `volt.scope(body)` (cancels on body error) | (no first-class; `task::scope` in stable lib by 2026 timing — RFC stage) | (none — Pirsch's [conc] library) |
| Reactor backend | kqueue (Darwin only today) | tokio's mio (kqueue/epoll/IOCP) | netpoll (kqueue/epoll/IOCP) |
| File I/O | (out of core) | `tokio::fs::*` | `os.OpenFile` etc., backed by blocking pool |

## API shape differences

### Spawn returns a Task you join

```zig
// Volt
const t = try volt.spawn(work, .{ arg });
const result = t.join();
```

```rust
// Tokio
let h = tokio::spawn(work(arg));
let result = h.await.unwrap();
```

```go
// Go: no return value; use a channel
ch := make(chan Result, 1)
go func() { ch <- work(arg) }()
result := <-ch
```

Volt and Tokio both return typed handles. Go does not — values
come back through channels. The handle model gives you a place
to attach `cancel`, `is_done`, etc.; channels give you flexibility
about how the result is consumed.

### Cancellation model

```zig
// Volt — explicit *Cancel
var c = volt.Cancel.init(volt.runtime());
defer c.deinit();

const t = try volt.spawn(work, .{ &c });
volt.sleep(100 * std.time.ns_per_ms);
c.fire();
```

```rust
// Tokio — abort via JoinHandle (drops the future)
let h = tokio::spawn(work());
tokio::time::sleep(Duration::from_millis(100)).await;
h.abort();
```

```go
// Go — context.Context as data parameter
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
go work(ctx)
time.Sleep(100 * time.Millisecond)
cancel()
```

Volt's model is closer to Go's — cancellation is explicit data
flowing through parameters, not magic on a handle. Tokio's abort
model works for stackless because dropping a future just drops
the state; for stackful you can't yank the rug, so Go's model
(observe-and-unwind) is the only one that fits.

See [Cancellation internals](/architecture/cancellation-internals/)
for why.

### Sync primitives

| | Volt | Tokio | Go |
|---|---|---|---|
| `Mutex.lock()` | parks coroutine | `mu.lock().await` — parks future | parks goroutine |
| Mutex contended bench (8×50k) | 15 ns | (varies) | 81 ns |
| `RwLock` | not in core | `tokio::sync::RwLock` | `sync.RWMutex` |
| Barrier | not in core (use Semaphore) | `tokio::sync::Barrier` | (none in std; via wait group) |
| One-shot signal | `volt.Notify` | `tokio::sync::Notify` | (built via chan struct{}) |

### Channels closing semantics

| Channel | Volt close behaviour | Tokio close behaviour | Go close behaviour |
|---|---|---|---|
| Bounded MPMC | wakes all with `error.Closed` | sender-only; receiver gets `None` | drains; recv returns zero value + ok=false |
| Oneshot | wakes recv with `error.Closed` | drop-sender ≈ close | (built on chan T 1-buffer) |
| Watch | wakes `changed()` with `error.Closed` | drop-sender → recv `None` | (none) |
| Broadcast | recv returns `error.Closed` | recv returns `Err(Closed)` | (none) |

### File I/O

Volt does not include file I/O. Tokio has `tokio::fs::*` (which
internally uses a blocking thread pool on platforms without
io_uring). Go has full `os.File` integration with the netpoll
backend on supported platforms.

Volt's stance: file I/O on platforms without io_uring is a
blocking-thread-pool problem; that's an opinionated abstraction
that belongs in a `volt-fs` library on top of the runtime, not
in the runtime itself. See [Roadmap](/appendix/roadmap/).

## Performance shape (where comparable)

| Bench | Volt | Go 1.26 | Tokio (approx, from public benches) |
|---|---|---|---|
| Context switch / yield | 9 ns | 42 ns | ~100 ns (poll + state machine update) |
| Mutex contended | 15 ns | 81 ns | ~50-100 ns (depends on contention model) |
| Spawn + wait (workers=1) | 101 ns | 136 ns | ~150 ns |
| Spawn + wait (workers=11, synthetic) | 575 ns | 213 ns | (varies — runtime config dependent) |

Volt's stackful model wins on context switch (no `Future::poll`
dispatch overhead) and Mutex (no GC write barriers). Loses on
multi-worker spawn-heavy because the runtime hasn't been
optimised for that synthetic shape (and real workloads don't
look like it). Numbers are Darwin arm64; Linux numbers will
differ when the Linux backend ships.

See [Benchmarks](/performance/benchmarks/) for full methodology.

## Idiom translation

### "Sleep then do thing"

```zig
// Volt
volt.sleep(50 * std.time.ns_per_ms);
doThing();
```

```rust
// Tokio
tokio::time::sleep(Duration::from_millis(50)).await;
do_thing();
```

```go
// Go
time.Sleep(50 * time.Millisecond)
doThing()
```

### "Receive from one of two channels"

```zig
// Volt — no select primitive; multiplex via Notify + parallel coros, or use Mpmc
//        as a multiplexer. Future work to add proper select.
```

```rust
// Tokio
tokio::select! {
    msg = ch1.recv() => handle1(msg),
    msg = ch2.recv() => handle2(msg),
}
```

```go
// Go
select {
case msg := <-ch1:
    handle1(msg)
case msg := <-ch2:
    handle2(msg)
}
```

Volt's `select` is intentionally not in core today. Go's select
is a compiler-level construct; Tokio's `select!` is a macro;
Volt would need either macros or a runtime-coordinated multi-
channel-wait primitive. Tracked as future work.

### "Timeout on an operation"

```zig
// Volt — manual watchdog
fn withTimeout(ns: u64, comptime body: anytype) !void {
    try volt.scope(struct {
        fn b(c: *volt.Cancel) anyerror!void {
            _ = try volt.spawn(struct {
                fn w(n: u64, ctx: *volt.Cancel) void {
                    volt.sleep(n);
                    ctx.fire();
                }
            }.w, .{ ns, c });
            try body(c);
        }
    }.b);
}
```

```rust
// Tokio
tokio::time::timeout(Duration::from_secs(1), do_work()).await?;
```

```go
// Go
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()
err := doWork(ctx)
```

Volt is the most verbose here today. A `volt.timeout` helper is
on the roadmap; the building blocks (scope + Cancel + sleep)
already work.

## When to use which runtime

- **Volt** — Zig codebase, want synchronous-shape API for I/O,
  Darwin-first today. No package ecosystem (yet); roll-your-own
  HTTP / DB drivers / TLS.

- **Tokio** — Rust codebase, mature ecosystem (HTTP / gRPC / DB
  drivers via crates), willing to write `async` / `await`
  syntax, OK with `Pin` and lifetime complexity. Linux + macOS
  + Windows.

- **Go** — Need a complete platform with everything in stdlib,
  willing to accept GC + runtime opinions, fastest path to a
  working production system for typical web workloads.

Volt is not trying to replace either. It's the substrate Zig
asks for: stackful coroutines so you don't write state machines,
M:N work-stealing so you don't write thread pools, parking-lot
sync so you don't write futexes. What you build on top is yours.

## Further reading

- [Stackful by design](/architecture/stackful-design/) — the why-stackful tradeoff in detail.
- Tokio's [Async in depth](https://tokio.rs/tokio/tutorial/async) — the stackless model.
- Go runtime documentation: [`runtime/proc.go`](https://github.com/golang/go/blob/master/src/runtime/proc.go) — the M:P:G architecture Volt borrows.
- "Why Go is the way it is" — Pike's design talks on the runtime.
- "Programming Rust", chapter on async — Pin / Future / state machines.
