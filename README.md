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
    try (try rt.run(echoServer, .{}));
}

fn echoServer() !void {
    var listener = try volt.net.TcpListener.bind(.any4(8080));
    defer listener.close();
    while (true) {
        const conn = try listener.accept();
        // Fire-and-forget: spawn a task and let it run; no join needed.
        _ = try volt.spawn(echoOne, .{conn});
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

That's the whole shape. No `async`, no `await`, no `Future`, no `.poll()`, no manual state machines. The `s.read(&buf)` call suspends the coroutine when the socket isn't ready and resumes it when the reactor (kqueue / epoll / io_uring / IOCP, depending on platform) delivers readiness — possibly on a different worker thread.

## What's in the box (currently)

- **Stackful coroutines** backed by a slab arena. One `mmap` at `Runtime.init` reserves `max_concurrent_stacks × 256 KiB` of virtual address space; per-slot `mprotect` is lazy. 16 KiB committed RSS per active coroutine; stacks grow on demand via the SIGSEGV handler.
- **M:N work-stealing scheduler.** Each OS thread (`M`) is bound to a per-worker scheduler state (`P`) with its own Chase-Lev work-stealing queue, single-slot LIFO cache, MPMC mailbox, and per-P pools. The driver thread participates as a worker.
- **Typed `Task(T)` handle** with `join()` returning the spawned function's result.
- **Direct handoff in `Task.join`** when the joinee is in the same M's lifo slot — skips the park/unpark round trip for the common spawn-then-await pattern (Go's `gopark`/`goready` shape).
- **Channels** comptime-specialized at the call site — `Spsc(T, cap)` (single-producer/single-consumer), `Mpmc(T, cap)` (Vyukov bounded ring), `Oneshot(T)` (1:1 handoff), `Watch(T)` (1:N latest-value, seqlock), `Broadcast(T, cap)` (1:N history-aware). All block via the parking lot.
- **Sync primitives** — `Mutex`, `Notify`, `Semaphore` — built on a shared parking lot.
- **Cancellation** — `volt.Cancel` carries an atomic flag + waiter list. Cancel-aware variants (`Mutex.lockCancel`, `Spsc.recvCancel`, etc.) wake with `error.Cancelled` when fired. `volt.scope` ties Cancel lifetime to a lexical block.
- **Reactor with four backend implementations** behind one interface — kqueue (Darwin/BSD), epoll (Linux), io_uring (Linux ≥ 5.10, poll mode), IOCP (Windows, polyfilled as readiness via zero-byte `WSARecv`/`WSASend`). Single-poller claim, one-shot registrations, `*Coroutine` as the wake identity. Darwin is the primary dev platform; Linux backends cross-compile and run their unit tests; Windows cross-compiles, runtime validation pending.

## Performance — Go as scale reference

Numbers on Darwin arm64, ReleaseFast vs `go build`. Go has been
optimised over a decade by people with deep systems expertise; the
comparison below is to know we're in a sensible range for a stackful
coroutine runtime, **not** a "we beat Go" claim. When Volt is
faster, it's usually because Go pays a cost we don't (GC write
barriers, function colouring) rather than because we out-engineered
them. See `BENCHMARKS.md` for full methodology + receipts.

| Workload | Volt | Go | Volt/Go |
|---|---|---|---|
| yield (one-way ctx switch) | 9 ns | 42 ns | 0.21× |
| Mutex contended (8 × 50k) | 15 ns | 81 ns | 0.18× |
| Spsc send+recv (cap=16) | 12 ns | 33 ns | 0.36× |
| TCP echo (64 × 16 RTT × 1 KB) | 8,449 ns | 9,050 ns | 0.93× |
| spawn+wait workers=1 | 101 ns | 136 ns | 0.74× |
| fan-out scaling workers=11 | 117 ns | 107 ns | 1.10× |
| parallel-compute (8 workers, CPU-bound) | 5.8× speedup | — | near-ideal |
| spawn+wait workers=11 (synthetic) | 575 ns | 213 ns | 2.70× |

Single-worker and real-work multi-worker land near or below the Go
reference. The 2.84× on workers=11 is a synthetic spawn-heavy
shape — one driver feeding 11 workers trivial tasks — where adding
workers can only hurt because there's no parallel work to amortise
the coordination cost. On any shape with actual parallel work
(fan-out, TCP, parallel-compute) the scheduler matches Go.

## Status

**Not yet released.** The runtime works for what it claims (see benches + stress test) but several pieces are still in flight:

| | Status |
|---|---|
| Darwin arm64 kqueue | **Working** — primary dev platform; full bench suite + 45 s stress green |
| Linux arm64 epoll | **Working** — cross-compile + epoll-specific tests green; runtime CI pass pending |
| Linux arm64 io_uring (poll mode) | **Working** — `Runtime.Config.io_backend = .io_uring`, kernel ≥ 5.10 |
| Windows arm64 IOCP (readiness polyfill) | **Cross-compiles cleanly** — implementation via zero-byte `WSARecv`/`WSASend`; runtime validation deferred to a Windows VM/CI pass |
| x86_64 (Linux + Windows) | **Cross-compile only** — needs an x86_64 context switch (#149); ARM64 ctx switch is the only one shipping today |
| Cancellation | **Shipping** — `Cancel`, cancel-aware variants of every blocking op, `scope` for lexical lifetime |
| File I/O / DNS / TLS | Not yet — these belong in libraries on top of Volt, not in core |
| Mutex throughput | Parking-lot + spin loop redesign on 2026-05-16 — contended-Mutex bench now 15 ns/op, ~5.4× faster than Go's 81 ns |
| Stack allocation | Slab-arena redesign on 2026-05-16 — one `mmap` at runtime init, lazy per-slot `mprotect`, per-P pool with fair-share cap overflows to arena. Removed the VM-lock cliff that the prior pool-of-64 design hit at BATCH > 64. |

The honest case for using Volt today: you want a stackful coroutine substrate for Zig on ARM64, you want the synchronous-shape ergonomics, you can live with the multi-worker spawn-heavy gap to Go, and you're OK being an early user on Linux (epoll + io_uring backends are written but not yet CI-validated) or willing to wait on Windows (cross-compiles cleanly; runtime validation pass pending).

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
// run() returns `!T` where T is your fn's return type. If your fn
// returns `!U` (an error union), the outer `!` is from run, the
// inner `!` is yours — hence `try (try ...)`. Tests use this idiom.
try (try rt.run(myFn, .{ arg1, arg2 }));

// Or with explicit worker count and arena size:
var rt = try volt.Runtime.init(.{
    .allocator = a,
    .workers = 4,
    .max_concurrent_stacks = 4096,
});

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

// Networking — TCP across kqueue / epoll / io_uring / IOCP.
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
zig build bench-spawn-hot          # canonical multi-worker (Go-shaped)
zig build bench-fanout-scaling     # multi-driver real-parallelism scaling
zig build bench-mutex
zig build bench-tcp-echo
zig build bench-reactor-throughput # tight register/wake loop — cross-platform receipt
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
