---
title: Coming from Go
description: Goroutines → coroutines is nearly 1:1. The real differences are explicit Task handles, cancellation as a typed parameter, no select primitive, and Zig's manual memory model.
---

Volt's runtime architecture borrows extensively from Go's. The
M:N scheduler, the work-stealing queues, the `gopark`/`goready`
direct-handoff trick, the per-P pools — all match shapes you
already know. Most of your Go intuition transfers directly.

What's different is mostly at the surface API and the
language-level concerns (manual memory management, error
unions instead of panic + recover). This page walks through the
mapping.

## Mental model

> A **coroutine** in Volt is a goroutine with a typed return
> value and a manual lifetime. `volt.spawn(fn, args)` returns
> `*Task(T)`; `t.join()` parks until completion and frees the
> handle. There's no `go fn()` syntax; spawn is a function call.
>
> A **`*volt.Cancel`** is a `context.Context` you pass around
> as a value. Cancel-aware blocking ops accept it; they wake with
> `error.Cancelled` when `Cancel.fire()` runs. The pattern is
> identical to Go's `ctx.Done()` model — explicit data flow,
> not magic.

The scheduler runs in `getCpuCount()` worker threads (Go calls
them M's, Volt calls them M's). Each worker has its own
work-stealing queue (Go calls it P, Volt calls it P) plus a
single-slot LIFO cache and a per-P mailbox.

If you've read `runtime/proc.go` in Go, you'll recognise the
pieces — and the [M:N scheduler](/architecture/mn-scheduler/)
page is the same content shape as `proc.go`'s comments.

## The 60-second translation

| Go | Volt |
|---|---|
| `go work(arg)` | `_ = try volt.spawn(work, .{arg})` |
| `ch := make(chan T, 16)` | `var ch: volt.Spsc(T, 16) = .{}` (or `Mpmc` for M:N) |
| `ch <- v` | `try ch.send(v)` |
| `v := <-ch` | `const v = try ch.recv()` |
| `close(ch)` | `ch.close()` |
| `time.Sleep(dur)` | `volt.sleep(ns)` |
| `runtime.Gosched()` | `volt.yield()` |
| `sync.Mutex.Lock/Unlock` | `volt.Mutex.lock/unlock` |
| `sync.WaitGroup.Wait` | One `Task.join` per child, or `volt.scope` |
| `context.WithCancel` | `volt.Cancel.init(runtime())` |
| `ctx.Done() <- received` | `c.fire()` (any thread) |
| `<-ctx.Done()` | `try c.checkpoint()` or cancel-aware op |
| `select { case ...}` | **Not in Volt today.** Workaround: fan-in via Mpmc. |
| `net.Listen("tcp", addr)` | `volt.net.TcpListener.bind(.any4(port))` |
| `conn.Read(buf)` | `conn.read(buf)` |
| `defer x.Close()` | `defer x.close()` — same shape |

## Side-by-side: TCP echo

```go
// Go
package main

import (
    "io"
    "net"
)

func main() {
    l, _ := net.Listen("tcp", ":8080")
    defer l.Close()
    for {
        c, _ := l.Accept()
        go handle(c)
    }
}

func handle(c net.Conn) {
    defer c.Close()
    io.Copy(c, c)
}
```

```zig
// Volt
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(serve, .{}));
}

fn serve() !void {
    var l = try volt.net.TcpListener.bind(.any4(8080));
    defer l.close();
    while (true) {
        const c = try l.accept();
        _ = try volt.spawn(handle, .{c});
    }
}

fn handle(c: volt.net.TcpStream) void {
    var s = c;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;
        if (n == 0) return;
        s.writeAll(buf[0..n]) catch return;
    }
}
```

The shapes mirror each other. Volt's overhead is the Runtime
init/deinit ceremony at the top and the explicit `Task` return
value from spawn (`_ = try volt.spawn`). Otherwise the bodies
read identically.

## Side-by-side: fan-out + first wins

```go
// Go — race three replicas via select
result := make(chan int, 1)
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

for i, latency := range []int{80, 30, 120} {
    go func(id, ms int) {
        select {
        case <-time.After(time.Duration(ms) * time.Millisecond):
            select {
            case result <- id:
            case <-ctx.Done():
            }
        case <-ctx.Done():
        }
    }(i+1, latency)
}

winner := <-result
cancel()
fmt.Printf("winner: %d\n", winner)
```

```zig
// Volt — race three replicas via Oneshot
const Result = struct { backend: u32 };

fn backend(c: *volt.Cancel, winner: *volt.Oneshot(Result),
           id: u32, latency_ms: u32) void {
    if (c.isFired()) return;
    volt.sleep(@as(u64, latency_ms) * std.time.ns_per_ms);
    if (c.isFired()) return;
    _ = winner.send(.{ .backend = id }) catch {};
}

fn race() !void {
    var winner: volt.Oneshot(Result) = .{};
    try volt.scope(struct {
        fn body(c: *volt.Cancel) anyerror!void {
            const t1 = try volt.spawn(backend, .{ c, &winner, 1, 80 });
            const t2 = try volt.spawn(backend, .{ c, &winner, 2, 30 });
            const t3 = try volt.spawn(backend, .{ c, &winner, 3, 120 });

            const r = try winner.recv();
            std.debug.print("winner: {d}\n", .{r.backend});
            c.fire();
            winner.close();
            t1.join();
            t2.join();
            t3.join();
        }
    }.body);
}
```

Volt has no `select`. The Go idiom of "race two channels" is
replaced by:

- A `Oneshot(T)` where the first sender wins; subsequent senders
  see `error.Closed`.
- A `*Cancel` that the race body fires after observing the
  winner. Losers observe `isFired()` and bail.

`volt.scope` wraps the whole thing — if the body errors mid-race,
the Cancel auto-fires.

See [Fan out, take first answer](/cookbook/fan-out-first-wins/)
for the full recipe.

## Side-by-side: cancellation through layers

```go
// Go — context plumbed through every layer
func fetchUser(ctx context.Context, id int) (*User, error) {
    conn, err := db.Acquire(ctx)
    if err != nil { return nil, err }
    defer conn.Release()
    return queryUser(ctx, conn, id)
}

func queryUser(ctx context.Context, conn *Conn, id int) (*User, error) {
    return conn.QueryRow(ctx, "SELECT ...", id)
}
```

```zig
// Volt — *Cancel plumbed through every layer (same shape)
fn fetchUser(c: *volt.Cancel, id: u32) !*User {
    const conn = try db.acquireCancel(c);
    defer conn.release();
    return try queryUser(c, conn, id);
}

fn queryUser(c: *volt.Cancel, conn: *Conn, id: u32) !*User {
    return try conn.queryRowCancel(c, "SELECT ...", .{id});
}
```

Same data flow as Go's context. The cancel-aware variants
(`acquireCancel`, `queryRowCancel`) take the Cancel and wake with
`error.Cancelled` if it fires while they're parked.

The big difference: in Go, every cancel-aware function takes
`ctx context.Context` — convention. In Volt, every cancel-aware
function takes `c: *volt.Cancel` — same convention, different
type name.

## Gotchas for Go developers

### `defer` runs at function exit, not block exit

Same as Go. Zig's `defer` is statement-scoped (block exit), but
in practice you use it at the top of a function, same way you
would in Go. The semantic gotcha that bites Go programmers
elsewhere (Rust, Python `with`) doesn't apply here.

### No GC; you free what you allocate

```zig
const buf = try allocator.alloc(u8, 4096);
defer allocator.free(buf);
```

If you `try volt.spawn(...)`, you must `join` the returned
`*Task(T)` to free it. No `go fn()` that auto-cleans up. This is
the biggest behaviour difference — leak detection is on you (or
on `std.testing.allocator` if you're testing).

### Errors are values, not panics

```zig
const v = try ch.recv();         // try = if err return err
const v2 = ch.recv() catch |e| switch (e) {
    error.Closed => return,
    else => return e,
};
```

Go's `error` is a return value; same in Zig. The `try` keyword
is the propagation shortcut. No panic + recover; panics in Zig
are fatal.

### Channels are typed differently

```go
ch := make(chan int, 16)
```

```zig
var ch: volt.Spsc(i32, 16) = .{}   // 1:1
// or
var ch = volt.Mpmc(i32, 16).init()  // N:M
```

Go's `chan T` is implicitly M:N. Volt picks the shape at the
type level. `Spsc` is faster (12 ns/op vs ~60 ns/op) when the
1:1 contract holds; use `Mpmc` when more than one producer or
consumer is involved.

### `select` doesn't exist (yet)

Go:

```go
select {
case msg := <-cmdCh:
    handle(msg)
case <-quitCh:
    return
case <-time.After(5 * time.Second):
    timeout()
}
```

Volt: no equivalent today. Workarounds:

- Fan-in via `Mpmc(Tagged, cap)` with each "branch" as a
  forwarder coroutine sending tagged messages.
- For timeout-on-recv, use the `scope` + watchdog pattern (see
  [Timeout with Retry](/cookbook/timeout-retry/)).

A first-class `volt.select` may land later. For now, multiplex
manually.

### `Task.join` is required; no detach

Every `volt.spawn(...)` returns a `*Task(T)` you have to join.
There's no equivalent of "fire-and-forget" `go fn()` that
auto-cleans up — leaking the Task struct is the cost. For
real fire-and-forget patterns, accept the leak (small) or scope
the spawns inside a `volt.scope` that joins them.

### No `recover` for panics

Go's `recover` lets you catch panics. Zig has no equivalent. A
panic in any coroutine terminates the process. If you need
fault isolation for plugin systems, that's outside Volt's
scope — handle expected errors explicitly via `error.X` returns.

### Background threadlocal: don't cache across yield

In Go, you generally don't cache `runtime.GOMAXPROCS()` or P
identity across function calls because the goroutine can
migrate. Same in Volt — your coroutine can resume on a
different M after `await`-shaped operations (`recv`, `lock`,
`sleep`).

Concretely: don't store the result of `volt.runtime()` in a
threadlocal across a yield. Re-read it.

## What's missing (relative to Go)

| Go feature | Volt status |
|---|---|
| `select { case ...}` | Not in core; multiplex via Mpmc |
| `time.After(dur)` | Not in core; use `volt.sleep` + watchdog |
| `time.Ticker` / `Interval` | Not in core; loop with `volt.sleep` |
| `sync.RWMutex` | Not in core; build on parking lot |
| `sync.WaitGroup` | Not in core; use Task.join per child or scope |
| `sync.Once` | Not in core; build with atomic flag + Mutex |
| `sync.Pool` | Not in core; runtime has internal pools (per-P stack/coro pools) |
| `runtime.Goexit()` | Not in core; return from the coroutine fn |
| `context.WithDeadline` | Manual: scope + watchdog |
| File I/O (`os.File`) | Out of scope; coming in volt-fs library |
| HTTP (`net/http`) | Out of scope; coming in volt-http library |
| DNS (`net.Resolver`) | Out of scope; coming in volt-net |
| `runtime.SetFinalizer` | No GC, so no finalizers |
| `runtime.NumGoroutine()` | Use `rt.dumpState()` for diagnostics |
| `defer` (block-scoped) | Zig has `defer`; semantically similar enough |
| Generics | Zig has comptime; functionally similar |

## Performance shape

| Workload | Go | Volt |
|---|---|---|
| Yield / Gosched | 42 ns | 9 ns |
| Mutex contended | 81 ns | 15 ns |
| Channel send+recv (cap=16) | 33 ns | 12 ns (Spsc) / 54 ns (Mpmc 1×1) |
| TCP echo RTT | 9,050 ns | 8,449 ns |
| Spawn + wait (workers=1) | 136 ns | 101 ns |
| Spawn + wait (workers=11, synthetic) | 213 ns | 575 ns |

Volt is competitive on single-thread micros (no GC write
barriers, simpler context switch). Behind on multi-worker
spawn-heavy synthetic shapes — see [Multi-worker
profile](/performance/multi-worker-profile/).

## Further reading

- [Tokio + Go comparison](/appendix/tokio-go-comparison/) — the
  feature-mapping table version of this page.
- [Architecture: M:N scheduler](/architecture/mn-scheduler/) —
  Volt's runtime borrows the M:P:G shape from Go.
- [Structured Concurrency](/usage/structured-concurrency/) —
  the `Cancel` model in detail.
- [Cookbook: timeout with retry](/cookbook/timeout-retry/) — the
  recipe for what `context.WithTimeout` does in Go.
