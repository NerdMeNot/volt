---
title: v1.x backlog
description: Items deferred from v1 ship with reproductions, hypotheses, and ownership notes.
---

:::caution
**Historical (pre-v2 flattening).** Refers to the v0.x src tree
(`src/coroutine/stack_overflow_windows.zig` etc.) which no longer
exists. Some items were closed; some are still open against the v2
tree under different file paths. Cross-check against current tasks
before acting.
:::

Volt v1 ships with two known-but-deferred items. Each is documented here so future investigation has a starting point — reproduction, hypothesis, partial work landed, and what's blocking full resolution.

The v1 ship gates are:

1. All tests green on every supported native runtime (`zig build test`).
2. N=200 nightly stress green on every supported native (platform, backend) combination.
3. All 6 cross-compile targets green.
4. Bench gate enforced (no `allow_failure: true` escapes).

These are met. The items below are real but **don't gate v1 ship**.

## 1. Windows native runtime — blocked on Zig 0.16 stdlib

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

## 2. macOS x86_64 native runtime — SEGV in spawn-using tests

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
