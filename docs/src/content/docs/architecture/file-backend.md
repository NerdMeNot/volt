---
title: File-backend dispatch
description: How volt.fs.File hides per-platform async file I/O behind a single API. Today everything bridges through spawnBlocking; native io_uring / IOCP / kqueue paths land as backend swaps without API change.
---

`volt.fs.File` looks synchronous to the caller — `.read`, `.write`,
`.sync`, `.readAt`, `.writeAt`, etc. all return when the syscall
completes. Under the hood, those syscalls go through different
mechanisms depending on the platform and the configured backend.

## Today (v1): the blocking-pool backend

Every blocking file syscall (`read`, `write`, `pread`, `pwrite`,
`fsync`, `fdatasync`, `open`) is bridged through
[`volt.spawnBlocking`](/api/spawn-blocking/):

1. The coroutine submits the syscall as a job to the
   `blocking_pool` (typed thread pool).
2. The coroutine parks via the parking lot, addressed by a
   per-call `done` atomic.
3. A worker thread runs the syscall.
4. On completion the worker stores the result + unparks the
   coroutine via `parking.unparkOne`.
5. The coroutine resumes — possibly on a different OS thread, per
   the work-stealing scheduler.

The trade-off: every syscall costs a parking-lot acquire-release
pair (~25 ns) + the syscall itself + a wake on completion. Cheap
per-call; the dominant cost is the syscall (or the disk).

The advantage: **identical semantics on every platform**. Darwin,
Linux (epoll or io_uring), Windows — same API, same behaviour. No
per-platform regressions, no kernel-version sniffing.

## Future: native backends

Two follow-up backends drop into the same File API:

### `.io_uring` (Linux ≥ 5.6)

Submit file ops as SQEs with `user_data = @intFromPtr(coro)`; park
the coroutine via context-switch; the existing io_uring poll loop
picks up the CQE and unparks via `runtime.unpark`. Lower latency
than blocking-pool for high-IOPS workloads.

### `.iocp` (Windows)

Native overlapped I/O. Submit `ReadFile` / `WriteFile` with an
`OVERLAPPED` + IOCP completion; coroutine parks until the IOCP
poller fires the completion. Mirrors the existing socket path in
`reactor_iocp.zig`.

## `Runtime.Config.fs_backend` (planned)

```zig
const Backend = enum { auto, io_uring, iocp, blocking };

pub fn init(cfg: Config) !Runtime {
    // cfg.fs_backend = .auto picks per platform:
    //   - Linux 5.6+ → .io_uring
    //   - Windows    → .iocp
    //   - otherwise  → .blocking
}
```

## Coroutine compatibility

All three backends share the property that **calls park the
coroutine** rather than blocking the worker. A coroutine can issue
hundreds of concurrent file operations and only one OS thread is
needed to drive them (plus the blocking-pool threads in the v1
path).

## Why "single File type, not split"

Pre-stackful Volt had a split `File` / `AsyncFile` — `File` was
synchronous (assumed OS-thread-blocking is OK), `AsyncFile`
returned futures. The stackful pivot collapsed that distinction:
every File method is async-by-default because we're in a coroutine.
Caller code looks the same as plain synchronous Zig; the runtime
decides whether to park or proceed.
