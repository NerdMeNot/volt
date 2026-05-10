---
title: v1.x backlog
description: Items deferred from v1 ship with reproductions, hypotheses, and ownership notes.
---

Volt v1 ships with three known-but-deferred items. Each is documented here so future investigation has a starting point — reproduction, hypothesis, partial work landed, and what's blocking full resolution.

The v1 ship gates are:

1. All tests green on every supported native runtime (`zig build test`).
2. N=200 nightly stress green on every supported native (platform, backend) combination.
3. All 6 cross-compile targets green.
4. Bench gate enforced (no `allow_failure: true` escapes).

These are met. The items below are real but **don't gate v1 ship**.

## 1. Linux x86_64 mutex/channel bench panic

### Reproduction

```sh
# On a Linux x86_64 host (or via GH `ubuntu-latest` runner):
VOLT_BENCH_MUTEX=1 zig build bench --summary none

# Output (truncated):
#   spawn+join (...): 13518 ns/op
#   yield ping-pong (...): 20 ns/op
#   thread N panic: Park.parkCurrent called outside a coroutine
#     src/scheduler/park.zig:77:38: in channelBenchRoot (volt-bench-core)
#         const coro = currentCoroutine() orelse
#     src/coroutine/context_x86_64.S:54: in ??? (...)
#         callq *%rax    // run_fn(closure)
```

The panic surfaces in `channelBenchRoot` or `mutexBenchRoot` (varies by run). Both use spawned coroutines; the bench is the only place we hit it. Tests + N=200 stress are clean.

### What's known

- **ReleaseFast-only.** Default test build (Debug) doesn't trigger it. Bench is hardcoded `ReleaseFast` in `build.zig`.
- **Linux x86_64-only.** macOS arm64, macOS x86_64 cross-compile, Linux arm64 — all clean.
- **High-throughput coro work.** spawn+join (10k iters) + yield (100k iters) finish; channel/mutex bench (100k+ iters with contention) panics before printing the metric.
- **`currentCoroutine()` returns null** inside an actively-running coroutine — meaning the worker dispatch's `setCurrent(coro)` either didn't fire or the TLS slot is being read incorrectly.
- **Stack unwinder fails** right after `voltCoroEntry`'s `callq *%rax`. With the `.cfi_undefined %rip` CFI directive added, the unwinder should now show the actual call site — first chance to verify on the next CI push.

### Hypotheses (in order of likelihood)

1. **Zig 0.16 ReleaseFast inlines `currentCoroutine()` incorrectly on Linux x86_64.** TLS reads via `mov %fs:offset, %reg` may use the wrong offset under a specific inlining context. macOS uses a different TLS register (%gs base via `__thread`); the bug wouldn't manifest there.
2. **The asm trampoline misses something specific to Linux x86 ELF.** CFI directives now added — if that was the root cause, this commit fixes it.
3. **A real Park-state-machine race we haven't seen on Apple Silicon's stronger memory model** (Linux x86 has TSO, but maybe an interaction with our seq_cst protocol).

### What's blocked

Full diagnosis needs interactive access to a Linux x86_64 machine (gdb on the bench binary, single-step through the panic site). GitHub Actions runners are non-interactive.

### Mitigation

`bench/bench_core.zig`:`mutex` is opt-in via `VOLT_BENCH_MUTEX=1`. Default skips it. Bench gate (`scripts/bench_gate.sh`) tolerates the `SKIPPED` line. CI is fully green; no `allow_failure` masking.

## 2. Windows native runtime — blocked on Zig 0.16 stdlib

### Reproduction

```sh
zig build test -Dtarget=x86_64-windows-gnu --summary none

# Three upstream Zig errors:
#   std.Io.Writer.zig:1803  invalid format string 'd' for type '*anyopaque'
#   std.c.zig:4767          os.windows.ws2_32 has no member named 'addrinfo'
#   std.c.zig:10659         std.c.mmap parameter of type 'void' under x86_64_win
```

Plus ~20 Volt-side errors in `fs/Dir`, `fs/Metadata`, `fs/Mmap`, `fs/OpenOptions` because `posix.O`, `posix.AT.FDCWD`, `posix.S.IF*`, `posix.MAP/PROT/MADV` are `void` on Windows.

### What's landed at v1

- **Reactor**: IOCP+AFD reactor (`src/io/reactor_iocp.zig`) + NTDLL bindings (`src/internal/win32/ntdll.zig`). Cross-compile clean.
- **Stack-overflow handler**: SEH-based (`src/coroutine/stack_overflow_windows.zig`). Cross-compile clean.
- **W1 syscall arms**: `close` (CloseHandle), `pipe` (CreatePipe), `read`/`write` (ReadFile/WriteFile), `recv`/`send`/`recvfrom`/`sendto` (ws2_32 via `src/internal/win32/ws2_32.zig`), `closeSocket`, `fcntl` (stub-error), `waitpid` (stub-error). Cross-compile clean.
- **W2 net arms**: portable `SOCK_NONBLOCK`/`SOCK_CLOEXEC` constants in syscall.zig; `socket()` Windows arm via `ws2_32.socket` + `ioctlsocket(FIONBIO)`. `TCP_KEEPALIVE` extended.
- **W8 partial**: `reactor.zig`'s Windows `@compileError` removed; routes to `reactor_iocp`.

### What's pending (v1.x)

- **W3 fs/* arms**: Dir, Metadata, OpenOptions, tree, temp — replace `posix.O`/`posix.AT.FDCWD`/`posix.S.IF*` with Windows `CreateFileW` / `GetFileAttributesW` / `FindFirstFileW` paths.
- **W4 fs/Mmap arm**: `MapViewOfFile`/`VirtualProtect`/`PrefetchVirtualMemory`/`VirtualLock`. Avoid `std.c.mmap` (the void-parameter bug).
- **W5 process/Command**: `CreateProcessW` + `CommandLineToArgvW` + handle inheritance via `SetHandleInformation`. Avoid the `std.Io.Writer` fmt bug.
- **W6 signal/Shutdown**: `SetConsoleCtrlHandler` → `PostQueuedCompletionStatus` to the reactor's IOCP.
- **W7 fs/Watcher**: `ReadDirectoryChangesW` against the IOCP.
- **W9 Windows native CI runner green**.

### Workaround for the stdlib bugs

For each of the 3 stdlib errors, Volt can route around by NOT using the affected stdlib calls on Windows:

1. `std.Io.Writer` fmt bug: find the Volt code that prints `*anyopaque` with `{d}` — switch to `{*}` or `{x}`.
2. `os.windows.ws2_32.addrinfo` missing: declare it ourselves in `src/internal/win32/ws2_32.zig`.
3. `std.c.mmap` void param: don't call `std.c.mmap` on Windows. `fs/Mmap.zig`'s Windows arm uses `MapViewOfFile` directly (W4).

Estimated effort to land Windows runtime in v1.x: **~6 working days** for W3+W4+W5+W6+W7, plus W9 iteration time once the runtime starts on a real Windows runner.

## 3. macOS x86_64 native runtime — SEGV in spawn-using tests

### Reproduction

```sh
# On a macOS x86_64 (Intel) host or `macos-15-intel` GH runner:
zig build test --summary all

# 123 tests crash with SIGSEGV. All of them use volt.run / volt.spawn.
# Tests that don't spawn coroutines (Mutex direct, Notify direct, etc.)
# pass. 230 pass + 1 skip + 123 crash.
```

### What's known

- **macOS arm64 is clean.** Same source, same commit, native CI green.
- **Specific to spawn path.** Tests that don't go through volt.run pass.
- **Cross-compile clean.** The binary builds; only runtime crashes.
- **Likely candidate**: stack-overflow handler `installPerThread` (sigaction + sigaltstack) interacting with x86_64 macOS in a way that arm64 doesn't. Or context_x86_64.S calling-convention quirk specific to Mach-O.

### What's blocked

Diagnosis needs an Intel Mac. The GH `macos-15-intel` runner is non-interactive — same constraint as Linux x86.

### Mitigation

`macos-x86_64` dropped from the v1 native CI matrix. Cross-compile gate stays so signature drift is caught. Apple Silicon (`macos-latest`) is the v1 macOS target. Intel runtime returns in v1.x.

## Verifying these don't gate ship

Each item:

- Has a clean, reproducible failure path.
- Has documented hypotheses + partial fixes already landed.
- Doesn't break tests, stress, or cross-compile on the supported v1 matrix.
- Doesn't masking-via-`allow_failure` in CI — explicit opt-in (mutex bench) or explicit cross-compile-only (Windows + macOS x86_64).

The v1 production matrix:

| Platform/Backend | Tier |
|---|---|
| macOS arm64 / kqueue | Production |
| Linux x86_64 / epoll | Production |
| Linux x86_64 / iouring | Production |
| Linux arm64 / epoll | Production |
| Linux arm64 / iouring | Production |
| macOS x86_64 / kqueue | Cross-compile only (v1.x) |
| Windows x86_64 / IOCP+AFD | Cross-compile only (v1.x) |
| Windows arm64 / IOCP+AFD | Cross-compile only (v1.x) |
