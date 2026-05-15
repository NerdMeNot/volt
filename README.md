<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/src/assets/logo-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="docs/src/assets/logo-light.png">
    <img alt="Volt" src="docs/src/assets/logo-dark.png" height="80">
  </picture>
</p>

<p align="center">
  <strong>Stackful coroutine runtime for Zig.</strong>
  <br/>
  No <code>async</code>/<code>await</code>. Code reads like blocking I/O. The runtime suspends at every wait point and resumes you on whichever worker the reactor wakes first.
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License"></a>
  <a href="https://volt.nerdmenot.in"><img src="https://img.shields.io/badge/docs-volt.nerdmenot.in-blue" alt="Docs"></a>
</p>

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try rt.run(echoServer, .{});
}

fn echoServer() !void {
    var listener = try volt.net.TcpListener.bind(.any4(8080));
    defer listener.close();
    const rt: *volt.Runtime = @ptrCast(@alignCast(volt.current.require().runtime));
    while (true) {
        const conn = try listener.accept();
        // Fire-and-forget: spawn a task and let it run; no join needed.
        _ = try rt.spawn(echoOne, .{conn});
    }
}

fn echoOne(conn: volt.net.TcpStream) void {
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

That's the whole shape. No `async`, no `await`, no `Future`, no `.poll()`, no manual state machines. The `s.read(&buf)` call suspends the coroutine when the socket isn't ready and resumes it when the kqueue/epoll reactor delivers readiness — possibly on a different worker thread.

## What's in the box (currently)

- **Stackful coroutines** with a 16 KiB heap-allocated stack per task. mmap-grow stacks with guard pages are planned (see `docs/internals/phase-4-postmortem.md` for the in-progress design).
- **M:N work-stealing scheduler.** Each OS thread (`M`) is bound to a logical processor (`P`) with its own work-stealing queue, LIFO slot, mailbox, and per-P coroutine/stack pools. The driver thread participates as a worker.
- **Typed `Task(T)` handle** with `join()` returning the spawned function's result.
- **Direct handoff in `Task.join`** when the joinee is in the same M's lifo slot — skips the park/unpark round trip for the common spawn-then-await pattern (Go's `gopark`/`goready` shape).
- **Channels** comptime-specialized at the call site — `volt.Spsc(T, cap)` for single-producer/single-consumer, `volt.Mpmc(T, cap)` (Vyukov bounded ring) for the general case. Both block on full/empty via the parking lot.
- **Sync primitives** — `Mutex`, `Notify`, `Semaphore` — built on a shared parking lot.
- **kqueue reactor** for Darwin/BSD — non-blocking sockets with single-poller claim. Linux (epoll/io_uring) and Windows (IOCP) backends planned; not currently shipped.

## Performance snapshot vs Go 1.26

Numbers measured on the same Darwin arm64 hardware, ReleaseFast vs `go build`. See `BENCHMARKS.md` for the full table + methodology.

| Workload | Volt | Go | Volt/Go |
|---|---|---|---|
| yield (one-way ctx switch) | **9 ns** | 42 ns | **0.21× — 4.7× faster** |
| Spsc send+recv (cap=16) | **12 ns** | 33 ns | **0.36× — 2.8× faster** |
| TCP echo (64 clients × 16 RTT × 1 KB) | **~7,000 ns** | 9,050 ns | **0.77× — 1.3× faster** |
| spawn+wait_all workers=1 | **106 ns** | 137 ns | **0.77× — 1.3× faster** |
| fan-out scaling workers=11 (real parallel work) | **117 ns** | 106 ns | 1.10× — parity |
| parallel-compute (8 workers, CPU-bound) | **6.6× speedup** | n/a | near-ideal |
| spawn+wait_all workers=11 (synthetic) | 490 ns | 172 ns | 2.84× behind |

**Single-worker we beat Go decisively. Real-work multi-worker is at parity. The gap is on synthetic spawn-heavy patterns (one driver, many workers, trivial work per task) where adding workers can only hurt because there's no parallel work to amortize coordination overhead.**

## Status

**Not yet released.** The runtime works for what it claims (see benches + stress test) but several pieces are still in flight:

| | Status |
|---|---|
| Darwin arm64 kqueue | **Working** — primary dev platform |
| Linux x86_64 / arm64 | Not yet — epoll backend planned |
| Windows | Not yet — IOCP backend planned |
| Cancellation | Not implemented — earlier design retired, re-landing planned |
| File I/O / DNS / TLS | Not yet — these belong in libraries on top of Volt, not in core |
| Mutex throughput | Real but slow — 8× behind Go on contended micro-bench; redesign planned |

The honest case for using Volt today: you want a stackful coroutine substrate for Zig on Darwin arm64, you want the synchronous-shape ergonomics, you can live with the multi-worker spawn-heavy gap to Go, and you can wait for the Linux/Windows backends.

## Install

```zig
// build.zig.zon
.{
    .name = .my_project,
    .version = "0.1.0",
    .minimum_zig_version = "0.16.0",
    .dependencies = .{
        .volt = .{
            .url = "https://github.com/NerdMeNot/volt/archive/refs/heads/main.tar.gz",
            // .hash = ... (run `zig build` to get the correct hash)
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

Volt requires libc.

## API at a glance

```zig
// Bootstrap.
var rt = try volt.Runtime.init(.{ .allocator = a });
defer rt.deinit();
const result = try rt.run(myFn, .{ arg1, arg2 });

// Or with explicit worker count:
var rt = try volt.Runtime.init(.{ .allocator = a, .workers = 4 });

// Spawning (from inside a coroutine):
const t = try rt.spawn(parse, .{buf});  // *Task(T)
const v = t.join();                     // wait + retrieve typed result

// Cooperative yield.
volt.yield();

// Synchronization.
var mu = volt.Mutex.init();             mu.lock(); defer mu.unlock();
var note = volt.Notify.init();          note.notifyOne(); note.wait();
var sem = volt.Semaphore.init(8);       try sem.acquire(); sem.release();

// Channels (single-producer / single-consumer, comptime-specialized).
var ch = volt.Spsc(u32, 16){};
try ch.send(7);
const v = try ch.recv();

// Networking (TCP only, kqueue/Darwin).
var listener = try volt.net.TcpListener.bind(.any4(8080));
const conn = try listener.accept();
const n = try conn.read(&buf);
try conn.writeAll(buf[0..n]);
```

For the full surface see `src/lib.zig`. Each module has inline tests that double as usage demos.

## Run it locally

```sh
zig build              # build the volt module
zig build test         # run the unit test suite (30+ tests, leak-detecting)
zig build stress       # 45 s mixed-primitive stress test
zig build bench-rss    # per-coro RSS
zig build bench-spawn-hot       # canonical multi-worker (Go-shaped)
zig build bench-fanout-scaling  # multi-driver real-parallelism scaling
zig build bench-mutex
zig build bench-tcp-echo
# ...see build.zig for the full list
```

## Why stackful

The honest tradeoff: stackful coroutines pay ~16 KiB resident per task (planned: 4 KiB on Linux via mmap-grow) for "code looks synchronous, no function coloring, no `Pin`, no manual state machines." Stackless futures pay ~hundreds of bytes per task for "every async function has a different type and the language needs `async`/`await`."

Zig has no `async`/`await` keyword and (by maintainer statement) won't add one. That removes stackless's main ergonomic argument in this language. Stackful gives Zig users the synchronous-shape API that Tokio gives Rust users — without compiling Tokio.

For workloads that genuinely need millions of concurrent waiters with tiny per-task state, stackless is the right tool. For everything else — HTTP servers, pipelines, CLIs, service tools, network proxies — stackful's ergonomics dominate.

## Documentation

- **Architecture**: [`docs/internals/architecture.md`](docs/src/content/docs/internals/architecture.md)
- **Multi-worker profile + measurement discipline**: [`docs/internals/multi-worker-profile.md`](docs/src/content/docs/internals/multi-worker-profile.md)
- **Direct-handoff design**: [`docs/internals/direct-handoff-design.md`](docs/src/content/docs/internals/direct-handoff-design.md)
- **Parking lot**: [`docs/internals/parking-lot.md`](docs/src/content/docs/internals/parking-lot.md)
- **Benchmarks**: [`BENCHMARKS.md`](BENCHMARKS.md)
- **Contributing guide**: [`CONTRIBUTING.md`](CONTRIBUTING.md)

## License

Apache 2.0. See [LICENSE](LICENSE).

## Acknowledgments

- [Tokio](https://github.com/tokio-rs/tokio) — the async-runtime architecture this is built on. The work-stealing scheduler design, the parking-lot waiter lists, the LIFO-slot trick all come from Tokio's playbook.
- [Go's runtime](https://go.dev/src/runtime/proc.go) — the `gopark`/`goready` direct-handoff pattern in `Task.join`, the `wakep` anti-herd via `nmspinning`.
- [may](https://github.com/Xudong-Huang/may) — Rust stackful coroutine library whose protocol Volt's wake design mirrors.
- The Zig community for building a language where a 5K-line runtime stays explainable.
