---
title: Coming from Node.js
description: "Single-threaded event loop → true M:N. Promise → Task. Callback → coroutine. The biggest mental shift: data races are now a thing you have to think about."
---

If your concurrency intuition was built on Node.js, the biggest
shift moving to Volt isn't the language (Zig vs JS — substantial,
but expected). It's the **threading model**.

Node.js is single-threaded by design. Two pieces of JS code never
run literally at the same time; the event loop interleaves them
at `await` boundaries. You can `count++` from a thousand places
in your code and it'll always be consistent because only one is
ever actually executing.

Volt runs `getCpuCount()` worker threads, scheduling coroutines
across them. Two coroutines can be running **literally
simultaneously** on different cores. `count++` is a data race
unless you synchronise. Welcome to actual concurrency.

This page maps Node.js idioms to Volt and calls out where the
"single-threaded" assumption no longer holds.

## Mental model

> A **coroutine** in Volt is a stackful unit of work that the
> runtime schedules onto OS threads. There's no event loop —
> the runtime is a thread pool of workers, each running its own
> dispatch loop, sharing work via work-stealing queues.
>
> When a coroutine calls `read()` or `recv()`, the runtime parks
> it on a kqueue event or a wait list, runs other coroutines on
> the worker thread, and resumes the parked one when its wait
> completes. From the function's perspective, the call returned
> a value — there's no callback, no `.then`, no `await`.

The killer feature you're getting: synchronous-looking code
that scales across cores. The killer constraint you're picking
up: you can have data races now. You shall need locks. (Or
channels. Channels are usually better.)

## The 60-second translation

| Node.js | Volt |
|---|---|
| `node app.js` | `Runtime.init` + `rt.run(root, .{})` |
| `await fn(args)` | `try fn(args)` (Volt is synchronous-shape) |
| `setImmediate(cb)` or `queueMicrotask(cb)` | `_ = try volt.spawn(cb, args)` |
| `setTimeout(cb, ms)` | `_ = try volt.spawn(struct { fn b() void { volt.sleep(ms * std.time.ns_per_ms); cb(); }}.b, .{})` |
| `new Promise((res) => ...)` then `await p` | A coroutine that returns a value (no Promise type) |
| `Promise.all([a, b])` | `const ra = a.join(); const rb = b.join();` |
| `Promise.race([a, b])` | `Oneshot(T)` + spawn N coros |
| `EventEmitter` | `volt.Broadcast(T, cap)` |
| `fs.promises.readFile` | Out of core; needs std.Thread bridge (see [work-offload](/cookbook/work-offload/)) |
| `net.createServer` | `volt.net.TcpListener.bind(.any4(port))` |
| `socket.on('data', cb)` | A coroutine doing `socket.read(buf)` in a loop |
| `AbortController` / `AbortSignal` | `volt.Cancel` |
| `worker_threads` | Built-in — Volt is multi-threaded by default |
| `cluster` | Built-in (each worker is an OS thread, same process) |

## Side-by-side: HTTP-ish echo

```js
// Node.js
import net from 'node:net';

const server = net.createServer((sock) => {
    sock.on('data', (chunk) => sock.write(chunk));
});
server.listen(8080, () => console.log('listening :8080'));
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
    std.debug.print("listening :8080\n", .{});
    while (true) {
        const sock = try l.accept();
        _ = try volt.spawn(handle, .{sock});
    }
}

fn handle(sock: volt.net.TcpStream) void {
    var s = sock;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;
        if (n == 0) return;
        s.writeAll(buf[0..n]) catch return;
    }
}
```

Same shape. Node's `.on('data', cb)` callback becomes a `while`
loop with `read(buf)` that suspends — the runtime parks the
coroutine on kqueue when no data is available and resumes when
the kernel delivers it. From inside the function, it looks
synchronous; from outside, all connections run concurrently
because each is its own coroutine.

The Volt version handles **multiple connections in parallel**
across N cores. The Node version handles them concurrently on
a single thread. For CPU-bound work mixed in, Volt scales —
Node needs `worker_threads`.

## Side-by-side: Promise.all

```js
// Node.js
async function fetchBoth() {
    const [a, b] = await Promise.all([
        fetch('https://a.example.com'),
        fetch('https://b.example.com'),
    ]);
    return process(a, b);
}
```

```zig
// Volt
fn fetchBoth() !Result {
    const ta = try volt.spawn(fetchOne, .{"https://a.example.com"});
    const tb = try volt.spawn(fetchOne, .{"https://b.example.com"});
    const a = try ta.join();
    const b = try tb.join();
    return try process(a, b);
}

fn fetchOne(url: []const u8) !Response { ... }
```

`Promise.all` is "spawn each, await both". In Volt that's spawn
each, join both. The spawned coroutines run in parallel — across
worker threads if available.

## Side-by-side: Promise.race / cancellation

```js
// Node.js
const ctrl = new AbortController();
const result = await Promise.race([
    fetch(urlA, { signal: ctrl.signal }),
    fetch(urlB, { signal: ctrl.signal }),
    fetch(urlC, { signal: ctrl.signal }),
]);
ctrl.abort();   // cancel the losers
```

```zig
// Volt
fn firstWinner() !Result {
    var winner: volt.Oneshot(Result) = .{};

    try volt.scope(struct {
        fn body(c: *volt.Cancel) anyerror!void {
            const t1 = try volt.spawn(fetchOne, .{ urlA, &winner, c });
            const t2 = try volt.spawn(fetchOne, .{ urlB, &winner, c });
            const t3 = try volt.spawn(fetchOne, .{ urlC, &winner, c });

            // ... process winner ...
            c.fire();
            winner.close();
            t1.join();
            t2.join();
            t3.join();
        }
    }.body);
}
```

Node's `AbortController` is closest to Volt's `Cancel`: both
flow through code as a "cancellation token." The differences:

- `Cancel` is passed explicitly to each function; no implicit
  signal threading.
- `Cancel.fire()` wakes coroutines parked on cancel-aware ops
  with `error.Cancelled` — the function unwinds normally, runs
  its `defer`s. No `signal.aborted` polling.

See the full [Fan out, take first answer](/cookbook/fan-out-first-wins/) recipe.

## Side-by-side: timer

```js
// Node.js
setTimeout(() => { sendKeepalive(); }, 30000);
```

```zig
// Volt — spawn a coroutine that sleeps then runs the work
_ = try volt.spawn(struct {
    fn b() void {
        volt.sleep(30 * std.time.ns_per_s);
        sendKeepalive();
    }
}.b, .{});
```

Or for periodic work:

```zig
fn keepaliveLoop() void {
    while (true) {
        volt.sleep(30 * std.time.ns_per_s);
        sendKeepalive();
    }
}

// Then:
_ = try volt.spawn(keepaliveLoop, .{});
```

No `setInterval` / `setTimeout` primitives — they're patterns,
not types. `volt.sleep(ns)` parks the coroutine on the kqueue
timer; the worker runs other coroutines during the sleep.

## Side-by-side: events / pub-sub

```js
// Node.js
import { EventEmitter } from 'node:events';

const bus = new EventEmitter();
bus.on('message', (msg) => process(msg));
bus.on('shutdown', () => cleanup());

// elsewhere:
bus.emit('message', { ... });
bus.emit('shutdown');
```

```zig
// Volt — Broadcast for fan-out
const Event = union(enum) {
    message: Message,
    shutdown: void,
};

var bus = volt.Broadcast(Event, 64).init();
defer bus.deinit();

fn subscriber(rx_init: volt.Broadcast(Event, 64).Receiver) void {
    var rx = rx_init;
    while (true) {
        const e = rx.recv() catch return;
        switch (e) {
            .message => |m| process(m),
            .shutdown => cleanup(),
        }
    }
}

// Spawn subscribers:
_ = try volt.spawn(subscriber, .{bus.receiver()});

// elsewhere:
bus.send(.{ .message = ... });
bus.send(.shutdown);
```

`Broadcast(T, cap)` is the fan-out channel. Each subscriber has
its own cursor; the producer never blocks; slow subscribers get
`error.Lagged` when they fall too far behind (Node's
EventEmitter has no equivalent — slow listeners block the
emitter).

For "latest value" semantics (e.g., current config):
`volt.Watch(T)`. See [Pub/Sub](/cookbook/pub-sub/) and [Config
hot-reload](/cookbook/config-hot-reload/).

## Gotchas for Node.js developers

### Data races are real now

```zig
// BAD — two coroutines on two cores running this:
var counter: u64 = 0;
counter += 1;   // ← race
```

In Node, you could increment a shared counter from a thousand
async functions and it'd always be consistent because only one
ran at a time. **Not true in Volt.** Two coroutines on two
cores can interleave the read-modify-write.

Fixes:

```zig
var counter: std.atomic.Value(u64) = .init(0);
_ = counter.fetchAdd(1, .acq_rel);
```

Or:

```zig
var mu = volt.Mutex.init();
var counter: u64 = 0;

mu.lock();
defer mu.unlock();
counter += 1;
```

For most "shared mutable state" patterns, prefer a `volt.Mpmc`
channel: one consumer owns the state, producers send updates.
No lock, no race.

### Stack traces ≠ execution path across yield

In Node, the stack trace at any `await` point shows you the
full chain of async functions you came from. In Volt, the
stack trace shows the coroutine's stack — same shape, but
the coroutine started at `volt.spawn(...)`, not at
`main()`. The chain of "who spawned me" is something you have
to track yourself (e.g., correlation IDs in logs).

### No global timeout

Node has implicit "wait forever" semantics. A `fetch()` with no
abort signal is a forever-pending Promise. Volt's I/O ops are
the same — they park forever unless cancelled.

If you want deadlines, build them in via the
[timeout recipe](/cookbook/timeout-retry/) (scope + watchdog).
Node's `AbortSignal.timeout(ms)` doesn't have a one-liner
equivalent in Volt today.

### No await; calls return values

```js
const v = await ch.recv();
```

```zig
const v = try ch.recv();   // try, not await
```

The function call is synchronous-shape. There's no `await`.
The runtime suspends the coroutine inside the call when needed.
This is the whole point of stackful coroutines.

The only `try` keyword in the Volt code is for **error
propagation** — `recv` can return `error.Closed`. It has
nothing to do with suspension.

### Stack size is real

In Node, a 1 KB stack per coroutine would be plenty (state
machine, not real stack). Volt commits 16 KiB per coroutine.
For 10K connections that's 160 MiB resident — fine. For 1M
connections that's 16 GiB — not fine. Node + worker_threads
scales further on idle-task count.

If your workload is "1M mostly-idle Promises waiting for I/O,"
Node or Tokio is the better fit. If it's "10K-100K active
HTTP connections", Volt's stackful ergonomics dominate.

### Buffer / Uint8Array vs []u8

```js
const buf = Buffer.alloc(4096);
sock.read(buf, 0, 4096);
```

```zig
var buf: [4096]u8 = undefined;
const n = try sock.read(&buf);
```

Zig slices are `(pointer, length)` pairs. `&buf` (passing a
pointer to the array) decays to `[]u8` (a slice). Node's
`Buffer` is more capable; in Volt you do the same things by
slicing `[]u8` and using `std.mem.copyForwards` / `std.fmt.bufPrint`.

### Logging is synchronous (but cheap)

`std.debug.print` writes to stderr synchronously. In hot paths
this is a real cost. For production logging, buffer in memory
+ flush async via a dedicated logging coroutine.

### Module / package system

Node has `require` / `import`. Zig has `@import("...")` for
files in the source tree and the build-system-controlled
package system for external deps (declared in `build.zig.zon`).
Different ergonomics; see [Installation](/getting-started/installation/).

## What's missing (relative to Node.js)

| Node.js | Volt status |
|---|---|
| `async`/`await` keywords | Not applicable; coroutines look synchronous |
| `Promise.all` | Manual: `task.join()` per child |
| `Promise.race` | Manual: `Oneshot` + N spawns |
| `Promise.allSettled` | Manual: collect each `Task.join()` result |
| `setTimeout` / `setInterval` | Spawn a coroutine with `volt.sleep` |
| `EventEmitter` | `volt.Broadcast(T, cap)` |
| `Stream` (Readable/Writable) | Use TcpStream directly; no Stream abstraction |
| `fs.promises.*` | Out of scope; std.fs blocks the worker — bridge via std.Thread |
| `child_process` | Out of scope; use std.process directly |
| `worker_threads` | Built-in to runtime; no separate API |
| `cluster` | Built-in via worker count |
| `process.on('exit'/'SIGTERM')` | Out of scope; use std.posix.sigaction directly |
| `console.log` | `std.debug.print` |
| `JSON.parse`/`JSON.stringify` | `std.json` (synchronous) |
| `npm` / package management | `build.zig.zon` |
| Web standards (fetch, FormData, URL) | Out of scope; build with TcpStream or future volt-http |

## Performance shape

Direct comparisons are tricky because Node is single-threaded.
Per-core:

- Volt's context switch (9 ns) and channel send/recv (12 ns
  Spsc) are faster than the V8 microtask queue (~100s of ns for
  a Promise resolution).
- Volt's TCP echo (8,449 ns/RTT) is close to Node's (similar
  ballpark — both go through OS networking).
- Volt scales across cores; Node doesn't without
  worker_threads.

For CPU-bound workloads, Volt × N cores > Node × 1 core. For
I/O-bound workloads at moderate concurrency, both runtimes are
in the same ballpark per request — Volt's edge is scaling
across cores from one process.

## When to stay with Node.js

- You need the npm ecosystem (Express, Prisma, Next.js, the
  thousand packages you depend on).
- You're writing a web app where the V8 / TypeScript developer
  experience matters more than performance.
- You're scaling out via Kubernetes pods, not via cores per
  pod.

## When Volt is the better fit

- You're writing in Zig (or willing to learn).
- You want to scale across cores without `worker_threads`
  ceremony.
- You want predictable memory behaviour (no V8 GC pauses).
- You're building infrastructure (databases, proxies, network
  services) where per-request performance matters at scale.

## Further reading

- [Stackful by design](/architecture/stackful-design/) — the
  detailed tradeoff with stackless (which is what Node-style
  async also is).
- [Common pitfalls](/guides/common-pitfalls/) — the data-race
  gotchas you don't have in Node.
- [Choosing a primitive](/guides/choosing-primitive/) — picking
  the right Volt primitive for the Node idiom you have in mind.
- [Cookbook](/cookbook/) — concrete recipes for the patterns
  Node devs reach for daily (pub/sub, rate limiting,
  connection pooling).
