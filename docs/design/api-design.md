# Volt API Design — Kotlin Inspiration, Zig Idiom

**Status:** Design doc for the stackful rewrite. Scope: define the public surface
of Volt v0.1 → v1.0, using Kotlin coroutines as the ergonomic reference and Zig's
own idioms (defer, error unions, explicit allocators, comptime, no overloading)
to express it.

**Principle:** Kotlin's coroutines are the reference for *what* the user
experiences. Zig's idioms are the source of truth for *how* it's written. We
don't force-port Kotlin syntax; we steal Kotlin's *concepts* and rebuild them
in the way Zig wants.

---

## Table of contents

1. [Mapping summary (TL;DR)](#1-mapping-summary)
2. [Builders](#2-builders)
3. [Job / Deferred (task handles)](#3-job--deferred)
4. [Cancellation](#4-cancellation)
5. [Channels](#5-channels)
6. [Flow (streams)](#6-flow-streams)
7. [Select](#7-select)
8. [Sync primitives](#8-sync-primitives)
9. [Context / Dispatcher](#9-context--dispatcher)
10. [Timing](#10-timing)
11. [What we OMIT from Kotlin and why](#11-omit)
12. [What we ADD that Kotlin doesn't have](#12-add)
13. [v0.1 → v1.0 mapping](#13-roadmap)
14. [Open questions](#14-open-questions)

---

## 1. Mapping summary

The core translation rules:

| Kotlin construct | Zig idiom | Why |
|---|---|---|
| `coroutineScope { ... }` (block with receiver) | `var s = volt.scope(); defer s.join();` | Zig's RAII-via-defer is more natural than callback blocks |
| `mutex.withLock { ... }` | `mu.lock(); defer mu.unlock();` | Same — defer is Zig's `with` |
| `CancellationException` | `error.Cancelled` | Error unions, not exceptions |
| `delay(t)` | `volt.sleep(.{ .ms = t })` | Zig has typed Duration; `sleep` is more honest than `delay` |
| `.await()` on Deferred | `.join()` returning `T` | Match thread/process terminology; no special await syntax |
| Lambdas with implicit receiver | Function pointers + explicit handle when state matters | Zig has no closures; receivers go via explicit param |
| `CoroutineContext` (composable interface) | Explicit handle (`*Runtime`, `*Scope`) for runtime ops; TLS for "current coroutine" | We avoid the abstract context framework — Zig users prefer explicit |
| `Dispatchers.IO`, `.Default`, `.Main` | `volt.blocking(fn, args)`, default scheduler, no Main equivalent | Concrete fn names, less abstraction |
| `select { ch.onReceive { v -> } }` | `volt.select(.{ .a = ch.onRecv() })` returning a tagged union | Comptime field-name → variant tag |
| `Flow<T>` cold streams | `volt.Stream(T)` with explicit collect callback | "Stream" is more familiar; same semantics |

The full breakdown follows.

---

## 2. Builders

Kotlin's coroutine builders are functions that take a lambda and start a
coroutine. We translate to Zig functions that take a `comptime fn` + args.

### 2.1 `runBlocking { ... }` — bootstrap

**Kotlin:**
```kotlin
fun main() = runBlocking {
    val data = fetch("https://...")
    println(data)
}
```

**Volt:**
```zig
pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(gpa.allocator(), root);
}

fn root() !void {
    const data = try fetch("https://...");
    std.debug.print("{s}\n", .{data});
}
```

`volt.run(allocator, fn)` initializes the runtime, calls `fn` as the root
coroutine, drives the scheduler until everything's done, then tears down. The
allocator is explicit (Zig idiom). `root` is a regular function.

### 2.2 `coroutineScope { ... }` — structured concurrency

**Kotlin:**
```kotlin
suspend fun parallel(urls: List<String>): List<String> = coroutineScope {
    urls.map { url -> async { fetch(url) } }.awaitAll()
}
```

**Volt:**
```zig
fn parallel(urls: []const []const u8) ![][]const u8 {
    var s = try volt.scope();
    defer s.join();   // ← waits for all children, propagates cancel

    var results = try volt.allocator.alloc([]const u8, urls.len);
    for (urls, 0..) |url, i| {
        _ = try s.launch(fetchInto, .{ url, &results[i] });
    }
    // children are running; defer fires here — joins all children
    return results;
}
```

Two key properties:
- **`scope.join()` is the join point.** Falling off the block waits for all
  children. If `parallel()` returns early via error, defer still fires, scope
  still waits. No orphan coroutines.
- **Cancel propagates.** If the parent of `scope` is cancelled, `scope.join()`
  cancels all children too.

This *is* `coroutineScope`. Defer + scope = structured concurrency in Zig.

### 2.3 `supervisorScope { ... }` — children fail independently

**Kotlin:**
```kotlin
supervisorScope {
    launch { riskyOp1() }   // failure here doesn't cancel sibling
    launch { riskyOp2() }
}
```

**Volt:**
```zig
var s = try volt.supervisorScope();
defer s.join();

_ = try s.launch(riskyOp1, .{});
_ = try s.launch(riskyOp2, .{});
```

Same surface, different scope variant. In a supervisor scope, a child's
failure doesn't trigger sibling cancellation; siblings run to completion. In
a regular scope (above), the first failure causes all siblings to be
cancelled.

### 2.4 `launch { ... }` — fire-and-forget child

**Kotlin:**
```kotlin
val job: Job = launch { doWork() }
```

**Volt:**
```zig
const job: *volt.Job = try volt.launch(doWork, .{});
// or scoped:
_ = try s.launch(doWork, .{});
```

`volt.launch(fn, args)` returns `*Job` — handle for `cancel()` / `join()`.
You can ignore the handle if you're inside a scope; the scope tracks
children.

### 2.5 `async { ... }` — child returning a value

**Kotlin:**
```kotlin
val deferred: Deferred<Int> = async { computeAnswer() }
val result = deferred.await()
```

**Volt:**
```zig
const task: *volt.Task(i32) = try volt.spawn(computeAnswer, .{});
const result = try task.join();   // returns i32 (or error if fn errored)
```

`volt.spawn(fn, args)` returns `*volt.Task(T)` where `T` is `fn`'s return
type. `Task.join()` blocks the calling coroutine until completion and returns
the value. Naming: we use `spawn` (Tokio convention) over `async` (reserved-
ish word in Zig) and `join` over `await` (no special await syntax).

### 2.6 `withContext(dispatcher) { ... }` — switch dispatcher

**Kotlin:**
```kotlin
val data = withContext(Dispatchers.IO) { readFileSync(path) }
```

**Volt:**
```zig
const data = try volt.blocking(readFileSync, .{path});
```

We don't have an abstract Dispatcher. We have concrete functions: `volt.blocking`
runs on the blocking thread pool, `volt.cpu` runs on the work-stealing scheduler
(default). If we ever need a 3rd dispatcher we add a 3rd named function.

### 2.7 `withTimeout(t) { ... }` — bounded duration

**Kotlin:**
```kotlin
val result = withTimeout(1.seconds) { fetch(url) }
val maybe = withTimeoutOrNull(1.seconds) { fetch(url) }
```

**Volt:**
```zig
const result = try volt.withTimeout(.{ .ms = 1000 }, fetch, .{url});
// returns error.Timeout if hit

const maybe = volt.withTimeout(.{ .ms = 1000 }, fetch, .{url}) catch |e| switch (e) {
    error.Timeout => null,
    else => return e,
};
```

`withTimeoutOrNull` is just `catch` — no separate function needed.

---

## 3. Job / Deferred

Kotlin: `Job` for fire-and-forget, `Deferred<T>` (extends Job) for value-returning.

**Volt:**

```zig
pub const Job = struct {
    pub fn cancel(self: *Job) void { ... }
    pub fn join(self: *Job) error{Cancelled, ...}!void { ... }
    pub fn isActive(self: *const Job) bool { ... }
    pub fn isCompleted(self: *const Job) bool { ... }
    pub fn isCancelled(self: *const Job) bool { ... }
};

pub fn Task(comptime T: type) type {
    return struct {
        pub fn cancel(self: *@This()) void { ... }
        pub fn join(self: *@This()) error{Cancelled, ...}!T { ... }
        pub fn isActive(self: *const @This()) bool { ... }
        // ...
    };
}
```

Key differences from Kotlin:
- No `Job` interface hierarchy — `*Task(T)` is a generic struct, no inheritance.
- `Deferred.await()` and `Job.join()` are unified: `Task(T).join()` returns `T`,
  `Task(void).join()` returns `void`. Same name, different return type.
- No separate `CompletableDeferred<T>` — if you want a one-shot promise, use
  `volt.Channel(T)` with capacity 1, or a dedicated `volt.Promise(T)` type
  added later if needed.

---

## 4. Cancellation

Kotlin's cancellation:
- Throws `CancellationException` at suspension points
- `isActive` / `ensureActive()` for cooperative checks
- `yield()` is a cancellation check + reschedule
- `NonCancellable` context for "this section can't be cancelled"

**Volt:** all of these as plain Zig with error unions.

```zig
fn longRunning() !void {
    while (true) {
        try volt.sleep(.{ .ms = 100 });   // returns error.Cancelled if cancelled
        try doWork();
    }
}
```

Cancellation is **not** a special exception type — it's `error.Cancelled` from
suspending operations. `try` propagates naturally. No special handling in
user code.

```zig
// Cooperative check (rare — most users let try propagate)
if (!volt.isActive()) return error.Cancelled;

// Yield and check (yield is itself cancellable)
try volt.yield();

// Section that can't be cancelled (uncommon, but real)
volt.uncancellable(criticalCleanup, .{});
```

**Why this is better than Kotlin's design:**
- No exception ceremony — Zig errors are just sum types, free
- Cleanup via `defer` is automatic and obvious
- Type-checked: any function that can be cancelled has `error.Cancelled` in
  its error union, visible in the signature

---

## 5. Channels

Kotlin's `Channel<T>` API:
- Constructors: `Channel<T>()`, `Channel<T>(capacity)`, `Channel.RENDEZVOUS`,
  `Channel.UNLIMITED`, `Channel.CONFLATED`, `Channel.BUFFERED`
- Operations: `send`, `receive`, `trySend`, `tryReceive`, `close`, `cancel`
- Iteration: `for (msg in channel) { ... }`
- Builders: `produce { ... }` (returns ReceiveChannel), `actor { ... }`
- Types: `SendChannel<T>`, `ReceiveChannel<T>`, `Channel<T>`

**Volt:**

```zig
// Construct
var ch = try volt.Channel(u32).init(allocator, 16);   // bounded MPMC, capacity 16
defer ch.deinit();

var ch_unbounded = try volt.Channel(u32).initUnbounded(allocator);
var ch_rendezvous = try volt.Channel(u32).init(allocator, 0);

// Send / receive — these suspend if blocked
try ch.send(42);
const v = try ch.recv();   // returns u32 or error.Closed

// Non-blocking variants
const sent = ch.trySend(42);     // returns SendResult { .ok, .full, .closed }
const got = ch.tryRecv();         // returns RecvResult { .value: T, .empty, .closed }

// Close
ch.close();

// Iterate (drains until closed)
var iter = ch.iterator();
while (try iter.next()) |msg| {
    process(msg);
}
```

**Differences from Kotlin:**
- Capacity types are constructors (`init`, `initUnbounded`, etc.), not enum
  values — Zig idiom for variant constructors.
- One channel type, no separate `SendChannel`/`ReceiveChannel` — Zig users
  can pass `*Channel(T)` and the type system tracks intent. We may add
  `SendOnly` / `RecvOnly` view types if needed.
- `for msg in channel` not built into Zig; we use `iterator()` + `while (try iter.next())`.
- `produce { ... }` builder: maps to `volt.Channel.fromCoroutine(fn, args)` —
  spawns a coroutine that holds the send side, returns a recv-only handle.
- `actor { ... }`: not built-in for v1; can be a recipe in cookbook.

The `SendResult` / `RecvResult` shapes are unified across all channel kinds
(addressing the post-port roadmap item from the prior tree).

---

## 6. Flow (streams)

Kotlin's `Flow<T>` is a cold async stream — code that emits values when
collected. Operators (map, filter, etc.) transform.

**Volt:**

```zig
// Build a stream
fn produceNumbers(emit: volt.Emit(u32)) !void {
    for (0..100) |i| try emit(@intCast(i));
}

// Use it
var stream = volt.stream(produceNumbers);
const doubled = stream.map(struct {
    fn x(v: u32) u32 { return v * 2; }
}.x);
const filtered = doubled.filter(struct {
    fn x(v: u32) bool { return v > 10; }
}.x);
try filtered.collect(struct {
    fn x(v: u32) !void { std.debug.print("{}\n", .{v}); }
}.x);
```

**Operator coverage (v1):**
- `map(fn)`, `filter(fn)`, `transform(fn)`, `take(n)`, `drop(n)`
- `flatMap(fn)`, `zip(other, fn)`, `combine(other, fn)`
- `onEach(fn)`, `catch(fn)`, `retry(n)`, `timeout(d)`
- Terminal: `collect(fn)`, `toList(allocator)`, `first()`, `single()`,
  `fold(init, fn)`, `count()`

The operator-as-anon-struct-fn-pointer pattern is the verbosity tax. It's
real — Kotlin's `{ it * 2 }` becomes our `struct { fn x(v: T) T { return v*2; } }.x`.
We could provide common shortcuts (`stream.mapMul(2)`, `stream.filterGt(10)`)
to cut the boilerplate for common cases.

**StateFlow / SharedFlow:** not in v1. Add as separate types (`volt.StateFlow(T)`,
`volt.SharedFlow(T)`) when needed. Different semantics from cold Flow.

---

## 7. Select

Kotlin:
```kotlin
val result = select<String> {
    ch1.onReceive { v -> "ch1: $v" }
    ch2.onReceive { v -> "ch2: $v" }
    onTimeout(100.ms) { "timeout" }
}
```

**Volt:**
```zig
const winner = try volt.select(.{
    .first = ch1.onRecv(),
    .second = ch2.onRecv(),
    .timeout = volt.afterMs(100),
});

switch (winner) {
    .first => |v| handleA(v),
    .second => |v| handleB(v),
    .timeout => handleTimeout(),
}
```

`volt.select` takes an anon struct of `*Op`-style values; comptime generates
a tagged union with one variant per field. The result discriminates by field
name. Each Op is one of:
- `ch.onRecv()` — recv branch
- `ch.onSend(value)` — send branch (suspends on full, fires when sent)
- `task.onJoin()` — task completion
- `volt.afterMs(N)` / `volt.afterDuration(d)` — timeout
- `volt.onCancellation()` — when current coroutine is cancelled

Implementation: each op exposes `arm`, `disarm`, `complete` callbacks; the
runtime arms all of them, suspends the coroutine, the first one to fire
disarms the others and resumes the coroutine.

---

## 8. Sync primitives

### 8.1 Mutex

**Kotlin:**
```kotlin
mutex.withLock { counter++ }
```

**Volt:**
```zig
mu.lock();
defer mu.unlock();
counter += 1;
```

`Mutex.lock()` suspends if contended, finds the current coroutine via TLS,
parks it on the mutex's wait queue. `unlock()` wakes one. Same algorithm as
parking_lot or Tokio's Mutex.

### 8.2 Semaphore

**Kotlin:**
```kotlin
semaphore.withPermit { doWork() }
```

**Volt:**
```zig
try sem.acquire(1);
defer sem.release(1);
try doWork();
```

### 8.3 Other

Kotlin doesn't have these built-in (it has `Mutex`, `Semaphore`, full stop).
Volt adds:
- `volt.Notify` — single-shot or many-shot notification primitive
- `volt.OnceCell(T)` — lazy one-time initialization
- `volt.RwLock` — reader/writer lock
- `volt.Barrier(n)` — N-coroutine synchronization point

These are all standard async sync primitives. Tokio has them; we replicate.

---

## 9. Context / Dispatcher

Kotlin's `CoroutineContext` is a composable map of context elements (Job,
Dispatcher, Name, ExceptionHandler). `coroutineContext` accessor gives current
context.

**Volt:** we don't have an abstract Context framework. Concrete handles
instead:
- The currently-executing coroutine is in TLS — `volt.currentJob()` returns
  `?*Job`, `volt.currentRuntime()` returns `*Runtime`.
- Dispatcher is concrete: `volt.blocking(fn, args)` runs on blocking pool.
- Cancellation comes via the Job (current coroutine's parent chain).
- Errors propagate through error unions, not exception handler context.

Why drop CoroutineContext: the abstraction is JVM-flavored ceremony. Zig
users prefer explicit. Concrete functions (`volt.blocking`) read better than
`withContext(Dispatchers.IO)`.

---

## 10. Timing

| Kotlin | Volt |
|---|---|
| `delay(ms)` | `try volt.sleep(.{ .ms = ms })` |
| `delay(Duration)` | `try volt.sleep(duration)` |
| Currently elapsed: `TimeSource.Monotonic.markNow()` | `volt.Instant.now()` |
| Compute elapsed | `since_start.elapsed()` returns Duration |

**Intervals:**
```zig
var iv = volt.interval(.{ .ms = 1000 });
defer iv.deinit();
while (try iv.tick()) {   // returns error.Cancelled if cancelled
    try doWork();
}
```

Kotlin doesn't have a built-in interval primitive (you write `while (true) { delay(); ... }`).
We add one because it's common and gets the missed-tick policy right.

---

## 11. What we OMIT from Kotlin and why <a name="11-omit"></a>

| Kotlin feature | Why omit |
|---|---|
| `CoroutineContext` interface + composable elements | JVM-flavored over-abstraction. Concrete handles + TLS in Zig. |
| `CoroutineExceptionHandler` (uncaught error policy) | Errors propagate via error unions; uncaught is a runtime panic. |
| `Dispatchers.Unconfined` | Magical: runs in caller thread. Zig users prefer explicit dispatchers; we have `default` and `blocking`. |
| `Dispatchers.Main` (UI thread) | Not relevant for systems Volt targets. Add if/when there's demand. |
| `CoroutineName` (for debugging) | Add later via `Job.setName()` if needed; not core. |
| `actor { ... }` builder | Niche. Cookbook recipe instead of built-in. |
| `produce { ... }` builder | Maps to `volt.Channel.fromCoroutine` if we even need it as a builder. |
| `runInterruptible { ... }` (interrupts JVM threads) | JVM-specific; thread interruption isn't a concept in Zig. |
| `flowOn(dispatcher)` (mid-pipeline dispatcher switch) | Defer; complicated, not v1. |
| `MutableStateFlow.compareAndSet` | Defer; specialized. |
| Channels as iterators (`for (msg in channel)`) | Zig doesn't have for-loop over arbitrary iterators yet. We use `iterator()` + `while (try iter.next())`. |
| Implicit lambdas with `it` | Zig has no closures or lambdas. Function pointers + struct fields. |
| Suspended fn coloring (`suspend` keyword) | Stackful: every fn can suspend. No coloring problem. |

---

## 12. What we ADD that Kotlin doesn't have <a name="12-add"></a>

These come from Zig's strengths or Volt's positioning:

1. **Explicit allocators per scope.** Every `volt.run`, `volt.scope`, `volt.spawn`
   takes (or inherits) an allocator. No hidden allocation.
2. **Structured cancellation as type-checked errors.** `error.Cancelled` is
   visible in function signatures. No implicit propagation through context.
3. **Comptime-known channel/select shapes.** No runtime type erasure; the
   channel's element type is comptime, the select's variant set is comptime-known.
4. **Stack guard pages by default.** Per-coroutine stack overflow detection;
   not a Kotlin concept.
5. **Slab-pooled stacks.** Spawn cost amortized; not a Kotlin concept.
6. **`volt.interval`** as a first-class primitive (Kotlin makes you build it).
7. **Native io_uring on Linux.** Concrete perf delta; not a Kotlin concept.
8. **`volt.timeout` as a wrapper** (Kotlin's `withTimeout` works; ours is
   the same shape).
9. **`scope.detach(handle)`** — explicitly opt out of scope tracking for a
   long-running coroutine. Kotlin has GlobalScope which is the same idea but
   discouraged.
10. **`volt.uncancellable(fn, args)`** — section that can't be cancelled.
    Kotlin has NonCancellable context; we make it a function call.

---

## 13. v0.1 → v1.0 mapping <a name="13-roadmap"></a>

Each version is a tight scope; we don't move on until the previous is solid.

### v0.1 — Bootstrap and minimal scheduler
- `volt.run(allocator, fn)` — entry point
- `volt.launch(fn, args)` — fire-and-forget
- `volt.spawn(fn, args)` returning `*Task(T)`
- `volt.yield()` — manual reschedule
- `*Task(T).join()` — wait for completion
- `*Job.cancel()` — request cancellation
- TLS: `currentRuntime()`, `currentJob()`
- Single-threaded scheduler (work-stealing comes later)
- All on Darwin-ARM64 first; Linux next

### v0.2 — I/O integration
- `volt.tcp.connect`, `volt.tcp.listen`, `Conn.read`, `Conn.write`, `Listener.accept`
- `volt.fs.openFile`, `File.read`, `File.write`
- `volt.sleep` (timer-driven, not blocking)
- I/O backends: kqueue (Darwin), epoll (Linux). io_uring later.

### v0.3 — Channels + select
- `volt.Channel(T)` — bounded, unbounded, rendezvous
- `volt.select(.{...})` — comptime-generated tagged union
- Channel iterator

### v0.4 — Sync primitives
- `Mutex`, `RwLock`, `Semaphore`, `Notify`, `OnceCell`, `Barrier`
- All TLS-aware (no rt threading)

### v0.5 — Structured concurrency
- `volt.scope()` — children must complete before parent returns
- `volt.supervisorScope()` — sibling failures don't cancel siblings
- Cancellation propagation through scope hierarchy
- `volt.uncancellable(fn, args)`

### v0.6 — Streams (Flow equivalents)
- `volt.Stream(T)`, `volt.stream(builderFn)`
- Operators: map, filter, transform, take, drop, flatMap, zip, combine,
  onEach, catch, retry, timeout
- Terminal: collect, toList, first, single, fold, count

### v0.7 — Timing
- `volt.interval(duration)`
- `volt.withTimeout(duration, fn, args)`
- `volt.Instant.now()`, `Duration` types

### v0.8 — Blocking pool & dispatcher abstraction
- `volt.blocking(fn, args)` — run on blocking pool
- `volt.cpu(fn, args)` — run on default work-stealing pool (this is just `spawn`)
- Dedicated thread pool sizing config

### v0.9 — Hardening
- Linux-x86_64 / Linux-ARM64 asm
- 4KB stacks + guard pages (currently 64KB)
- Slab-pooled stacks (currently per-spawn mmap)
- Multi-worker scheduler with work stealing
- Loom-style stress tests for coroutine sync primitives

### v1.0 — Ship
- Documentation, cookbook, integration tests
- Benchmark suite (vs Go, vs Tokio)
- Initial dependent libs (HTTP client) as integration tests

---

## 14. Open questions <a name="14-open-questions"></a>

These are decisions we should make before writing v0.1:

### Q1: Runtime handle threading

Should every fn that can suspend take `*Runtime` explicitly, or do we use TLS
for `currentRuntime()`?

- **Pro explicit**: Zig idiom (Allocator), no hidden state, self-documenting
- **Pro TLS**: Kotlin/Go-feel, less function signature noise

**Provisional decision:** TLS for the runtime, explicit for scopes/allocators
when relevant. Reasoning: the runtime is "the thing you're running on" — it
doesn't change mid-fn. Threading it through every signature is signature
pollution. Allocators DO change between scopes (use parent's, use scoped, etc.)
so they stay explicit.

### Q2: Are Tasks pointers or values?

`volt.spawn(fn, args)` could return `*Task(T)` (heap-allocated, pointer-stable)
or `Task(T)` (stack value, must not move).

- **Pro pointer**: easy to pass around, compose, store in collections
- **Pro value**: no allocator needed for the Task itself

**Provisional decision:** Pointer. Task lifetime spans suspensions; it MUST be
heap-allocated. The pointer is owned by either the parent scope (auto-cleanup)
or the caller (manual cleanup with `task.deinit()` after `join`).

### Q3: Sync primitives — global TLS or scope-local?

Should `Mutex` find the current coroutine via global TLS, or via a passed
parameter?

- **Pro TLS**: clean syntax (`mu.lock()` not `mu.lock(rt)`)
- **Pro explicit**: no hidden state

**Provisional decision:** TLS. One of the few places where the TLS lookup is
worth it; otherwise every sync primitive needs a runtime handle which makes
them indistinguishable from any other API call.

### Q4: Stack size default

4KB (target) vs 8KB (Go) vs 64KB (current spike).

**Provisional decision:** 16KB initial in v0.1, with guard page. Lower to 4KB
after Phase 0.9 hardening when growing-stack support lands. Makes the v0.1-v0.8
window safe (won't blow 16KB stack often) without committing to growing-stack
infrastructure prematurely.

### Q5: `volt.spawn` vs `volt.async`

Kotlin uses `async` for value-returning. Go uses `go` for fire-and-forget. We
have both:

```zig
volt.spawn(fn, args) -> *Task(T)   // value-returning
volt.launch(fn, args) -> *Job       // fire-and-forget
```

`spawn` is a slight Zig keyword brush (we have `comptime spawn` etc.) but
should be fine. `launch` is unambiguous.

**Provisional decision:** keep both names.

### Q6: How aggressive are we about no-`*Runtime`?

Some functions need the runtime to allocate or query state:

```zig
volt.scope() vs volt.scope(rt)
volt.Channel(T).init(allocator, capacity)  // allocator only, finds rt via TLS
volt.tcp.connect(addr) vs volt.tcp.connect(rt, addr)
```

**Provisional decision:** TLS finds rt for runtime ops. Allocators are still
explicit. So `volt.tcp.connect(addr)` (rt via TLS), but `volt.Channel(T).init(allocator, cap)`
(allocator explicit).

### Q7: Cancellation policy — automatic propagation

When a `Mutex.lock()` is suspended and the coroutine is cancelled, does the
lock-pending get cancelled (`error.Cancelled`) or is it uninterruptible?

**Provisional decision:** All suspending operations are cancellable by
default, returning `error.Cancelled`. Wrap with `volt.uncancellable(...)` to
opt out. Matches Kotlin and is what users expect.

---

## Net assessment

This design captures Kotlin's coroutine ergonomics in Zig idiom without
forcing concepts where they don't fit. Estimated lines of code per version:

| Version | LOC |
|---|---|
| v0.1 (bootstrap) | ~800 |
| v0.2 (I/O) | ~1500 |
| v0.3 (channels + select) | ~1200 |
| v0.4 (sync) | ~1000 |
| v0.5 (structured concurrency) | ~600 |
| v0.6 (streams) | ~1500 |
| v0.7 (timing) | ~400 |
| v0.8 (blocking pool) | ~500 |
| v0.9 (hardening) | ~1500 |
| v1.0 polish | ~500 |
| **Total** | **~9500 LOC** |

This is comparable to Tokio's core (~12K LOC) and substantially less than
Kotlin's coroutines library (~40K LOC of Kotlin + JVM ceremony).

The hardest part is v0.5 (structured concurrency) — getting cancellation,
parent-child tracking, and supervision right. Everything else is mechanical
once the foundation is sound.
