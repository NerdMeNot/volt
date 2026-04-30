<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/src/assets/logo-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="docs/src/assets/logo-light.png">
    <img alt="Volt" src="docs/src/assets/logo-dark.png" height="80">
  </picture>
</p>

<p align="center">
  <strong>Stackful coroutine runtime for Zig. No async/await. Code reads like blocking I/O; the runtime suspends at every wait point and resumes you on whichever worker the reactor wakes first.</strong>
</p>

<p align="center">
  <a href="https://github.com/NerdMeNot/volt/actions/workflows/ci.yml"><img src="https://github.com/NerdMeNot/volt/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License"></a>
  <a href="https://volt.nerdmenot.in"><img src="https://img.shields.io/badge/docs-volt.nerdmenot.in-blue" alt="Docs"></a>
</p>

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, serve, .{});
}

fn serve() !void {
    var listener = try volt.io.TcpListener.bind(volt.io.Address.any4(8080));
    defer listener.close();
    while (true) {
        const conn = try listener.accept();
        _ = try volt.launch(echo, .{conn});
    }
}

fn echo(conn: volt.io.TcpStream) void {
    var s = conn;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;
        if (n == 0) return;
        s.writeAll(buf[0..n]) catch return;
    }
}
```

That's the whole shape. No `async`, no `await`, no `Future`, no `.poll()`, no manual state machines. The `s.read(&buf)` call suspends the coroutine when the socket isn't ready and resumes it when the reactor delivers readiness — possibly on a different worker thread.

## What it is

- **Stackful coroutines.** Each task owns a growable virtual stack: 1 page committed up-front, grows in place via `mprotect` (POSIX) or `VirtualAlloc(MEM_COMMIT)` (Windows) on guard-page hit, capped at 8 MiB and surfaced as `error.StackOverflow`. Pointers to stack-locals stay valid across suspension; no compiler stackmaps required.
- **Multi-worker work-stealing scheduler.** Per-worker Chase-Lev deque, LIFO slot, global injection queue. `volt.run(.{ .allocator = ..., .workers = 4 }, ...)` lets you tune; default is `getCpuCount()`.
- **Park-based primitives, zero allocation per wait.** `Mutex`, `RwLock`, `Semaphore`, `Notify`, `Barrier`, `OnceCell`, `Channel`, `Oneshot`, `Watch`, `Broadcast`, `select`, `withTimeout`, `Scope`, `JoinSet`, `CancellationToken` — all built on a single `Park` substrate; intrusive waiter lists; no allocator on the hot path.
- **Cancellable from anywhere.** Cancelling a task wakes it from any park (sleep / I/O / channel / sync) and surfaces `error.Cancelled`. Timeouts propagate cleanly even through uncooperative blocking calls.
- **Per-OS reactor.** kqueue (Darwin/BSD), epoll (Linux), with parallel io_uring + IOCP backends compiled in. The default backend is selected at compile time.

## Status

Volt is at v1.0.0 against Zig 0.16.0. Version tags follow `vX.Y.Z-zigA.B.C` so the Zig version is always explicit.

| Target | Backend | Status |
|---|---|---|
| macOS arm64 / x86_64 | kqueue | runtime + CI |
| Linux x86_64 / arm64 | epoll | runtime + CI |
| Linux x86_64 | io_uring | parallel backend; cross-compile clean |
| Windows x86_64 / arm64 | IOCP | cross-compile clean; runtime port pending |

The Windows IOCP backend itself, the VirtualAlloc-backed stack, the `WaitOnAddress` futex, and `QueryPerformanceCounter` time all compile and link cleanly for Windows. What's still missing for runtime-default: ioctlsocket / WriteFile / CreateProcess arms in `io/net.zig`, `io/io.zig`, `process/Command.zig`, and a Windows CI runner. See `src/io/reactor.zig` for the punch list.

## Install

```zig
// build.zig.zon
.{
    .name = .my_project,
    .version = "0.1.0",
    .minimum_zig_version = "0.16.0",
    .dependencies = .{
        .volt = .{
            .url = "https://github.com/NerdMeNot/volt/archive/refs/tags/v1.0.0-zig0.16.0.tar.gz",
            .hash = "...", // run `zig build` to get the correct hash
        },
    },
    .paths = .{ "build.zig", "build.zig.zon", "src" },
}
```

```zig
// build.zig
const volt_dep = b.dependency("volt", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("volt", volt_dep.module("volt"));
```

Volt requires libc (the `sigsetjmp` / `mprotect` / `signalfd` paths), which Zig links automatically when the consuming module sets `link_libc = true` or imports a Volt-aware build helper.

## API at a glance

```zig
// Bootstrap.
volt.run(.{ .allocator = a }, fn, args)                // run a root coroutine to completion
volt.run(.{ .allocator = a, .workers = 4 }, fn, args)  // override defaults

// Spawning.
const j = try volt.launch(handler, .{conn});           // *Job — fire-and-forget
const t = try volt.spawn(parse, .{buf});               // *Task(T) — returns a typed value
const v = try volt.spawnBlocking(sha256, .{data});     // off the loop, on a thread pool

// Job / Task.
j.cancel(); j.state(); j.setName("name"); try j.join();
const v = try t.join();

// Structured concurrency.
try volt.scope(struct {
    fn body(s: *volt.Scope) !void {
        try s.spawn(workerA, .{});
        try s.spawn(workerB, .{});
    }
}.body);

// Channels.
var ch = try volt.channel.Channel(u32).init(alloc, 64);
try ch.send(7);                  const v = try ch.recv();
var os = volt.channel.Oneshot(Result){};
try os.send(.ok);                const r = try os.recv();
var w = volt.channel.Watch(Cfg).init(initial); var rx = w.subscribe();
w.send(new_cfg);                 try rx.changed(); const cfg = rx.current();
var b = try volt.channel.Broadcast(Event).init(alloc, 128); var brx = b.subscribe();

// Select first-ready over channels.
switch (try volt.select(.{
    .msg = volt.channel.OnRecv(u32){ .ch = &ch },
    .quit = volt.channel.OnRecv(void){ .ch = &shutdown_ch },
})) { .msg => |v| ..., .quit => return }

// Synchronization.
var mu: volt.sync.Mutex = .{};       mu.lock(); defer mu.unlock();
var sem = volt.sync.Semaphore.init(8); sem.acquire(1); defer sem.release(1);
var notify: volt.sync.Notify = .{};  notify.notifyOne(); try notify.wait();
var barrier = volt.sync.Barrier.init(4);
switch (barrier.wait()) { .leader => ..., .follower => ... }

// Time.
try volt.sleep(volt.Duration.fromMillis(50));
const v = try volt.withTimeout(volt.Duration.fromSecs(2), fetchUser, .{42});
var tick = volt.Interval.start(volt.Duration.fromMillis(100));
while (true) { try tick.tick(); try emit(); }

// I/O.
var listener = try volt.io.TcpListener.bind(volt.io.Address.any4(8080));
const conn = try listener.accept();
const n = try conn.read(&buf); try conn.writeAll(buf[0..n]);
```

For the full surface see `src/lib.zig`. For runnable end-to-end programs see `examples/`.

## Run it locally

```sh
zig build              # build the library
zig build test         # run the unit + integration test suite
zig build bench        # core perf benchmarks (ReleaseFast)

zig build run-echo            # examples
zig build run-fan-out
zig build run-work-offload
zig build run-timeout-retry
```

## Cancellation, in one paragraph

There is no `context.Context` to thread through every function and no `?` operator on every call. Cancelling a `Job` or `Task` sets the cancel flag *and* unparks whatever the task is currently parked on — sleep, I/O wait, channel recv, mutex acquire — so the task wakes promptly and surfaces `error.Cancelled` from its current suspension point. `volt.withTimeout(dur, fn, args)` is a watcher built on this. `volt.CancellationToken` provides a hierarchical handle for chained cancellation when you need it. If you have a CPU-only loop with no implicit suspension points, call `volt.yield()` periodically — that's the explicit cancellation point.

## Why stackful

The honest tradeoff: stackful coroutines pay ~4-16 KiB resident per coroutine for "code looks synchronous, no function coloring, no Pin, no manual state machines." Stackless futures pay ~256-512 bytes per coroutine for "every async function has a different type, the language needs `async`/`await`, lifetimes follow the state machine."

Zig has no `async`/`await` keyword and (by maintainer statement) won't add one again. That removes stackless's main ergonomic argument in this language. Stackful gives Zig users the synchronous-shape API that Tokio gives Rust users — without the surface tax that compiling Tokio without `async fn` would require.

The cost is real: one million parked coroutines costs ~4 GiB of resident memory if every page is touched. For workloads that genuinely need 1M+ concurrent waiters with tiny per-task state, stackless is the right tool. For everything else — HTTP servers, pipelines, CLIs, system tools, service meshes — stackful's ergonomics dominate, and Volt is targeting that majority.

## What's intentionally NOT here

- **No HTTP / TLS / DNS** in the runtime. Volt is a runtime, not a framework. Build those on top.
- **No global runtime.** `volt.run` owns the worker pool and the reactor. Library code that wants to suspend has to be called from within `volt.run`. There is no init-on-first-use mode.
- **No async-await syntax.** That's the point. Code that suspends looks identical to code that doesn't.
- **No goroutine-style "spawn and forget."** Use `volt.scope` (structured concurrency) by default; reach for `volt.launch` only when the lifetime genuinely needs to outlive the current scope.

## Documentation

- **Docs site**: [volt.nerdmenot.in](https://volt.nerdmenot.in) — guides, API reference, internals.
- **Examples**: `examples/` in this repo. Each is a runnable cookbook recipe.
- **Source**: `src/lib.zig` is the public surface; every primitive's source has inline tests that double as usage demos.
- **CHANGELOG**: see `CHANGELOG.md` for what shipped in v1.0.0-zig0.16.0 and what's pending.

## Contributing

PRs welcome. Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`). Don't add `Co-Authored-By` lines. Tests for new primitives must include a multi-worker stress harness — see `src/sync/Mutex.zig` for the pattern. See `CONTRIBUTING.md` for the full guide.

## License

Apache 2.0. See [LICENSE](LICENSE).

## Acknowledgments

- [Tokio](https://github.com/tokio-rs/tokio) — the async-runtime architecture this is built on. The work-stealing scheduler design, the parking-lot-style waiter lists, the cooperative budgeting idea all come from Tokio's playbook.
- [may](https://github.com/Xudong-Huang/may) — the Rust stackful coroutine library whose EventSource protocol Volt's wake protocol mirrors.
- [Trio](https://trio.readthedocs.io/) — `volt.scope` is a direct port of Trio's nursery concept; Kotlin's `coroutineScope` is the same idea via a different language.
- [Vyukov's MPMC bounded queue](https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue) — `Channel(T)`'s ring is a direct port.
- The Zig community for building a language where this kind of runtime fits in 10K lines and stays explainable.
