# Async file I/O via io_uring

**Status:** Phase 0 — design, 2026-05-26
**Owner:** `reactor-hardening` follow-up
**Tracking:** [#17](https://github.com/NerdMeNot/volt/issues/17)
**Driver:** Replace `spawnBlocking`-backed fs ops with a Linux
io_uring backend. Same public API; thread-pool fallback on Darwin
and Windows.

This memo captures the design before any code lands. It exists
because the prior two attempts at io_uring work (Step 2f, reverted
twice on this branch) failed by skipping the design step and trying
to migrate concurrency-sensitive code without a Linux dev loop. This
memo is the gate.

## 1. Diagnosis

`src/fs/file.zig` currently routes every blocking syscall through
the blocking pool:

```
File.read   → spawnBlocking(syncRead, ...)    line 459
File.write  → spawnBlocking(syncWrite, ...)   line 479
File.pread  → spawnBlocking(syncPread, ...)   line 499
File.pwrite → spawnBlocking(syncPwrite, ...)  line 519
File.fsync  → spawnBlocking(syncFsync, ...)   line 539
File.fdatasync → spawnBlocking(syncFdatasync, ...) line 552
File.open   → spawnBlocking(syncOpen, ...)    line 446
```

Every call parks the calling coroutine, hands the syscall to a
pool thread, and resumes the coroutine when the thread returns.
This is correct and race-free, but pays two costs:

1. **Per-op thread-hop latency.** Park + thread-wake + syscall +
   thread-park + coroutine-wake. On a warm thread, this is
   microseconds; on a cold thread, it's a context-switch + cache
   miss + dispatch.
2. **Concurrency capped by pool size.** Default is 128 worker
   threads (`blocking_pool_mod.Config`). A workload doing 10 K
   concurrent file reads queues 9,872 of them behind those 128
   workers — serialised by thread availability, not by disk.

io_uring sidesteps both. The kernel runs the syscalls itself, in
parallel, and signals completion via the CQ. No thread hop, no
pool cap. This is the use case io_uring was designed for and where
its perf delta over the thread-pool fallback is largest.

## 2. Why io_uring on fs is safe (when the §4A race made it unsafe on net)

The §4A close-vs-register race in `reactor_io_uring.zig` is about
**socket fd churn**: an HTTP server closing + recycling thousands
of TCP fds per second can have a stale POLL_ADD SQE fire on a
re-used fd. The race is structural to the per-wait POLL_ADD model
on a workload where fds are short-lived and the kernel recycles fd
numbers aggressively.

File fds are different:

- **Lifetime is explicit and bounded.** A file is opened, used by
  a known caller, closed. There is no equivalent of "accept loop
  recycling fds at line rate".
- **Recycling pressure is low.** A file fd typically lives for
  the duration of a request or task. Kernel fd-number reuse is
  rare on the file path.
- **The fs API doesn't have the "close fd while op is parked
  elsewhere" shape.** Sockets routinely close while a peer
  coroutine is parked in accept/recv on the same fd (the entire
  cancellation story). Files don't have peer access patterns.

So the §4A race is technically possible but practically absent.
The cancel-during-in-flight case (covered in §6 below) is the one
place where careful design is still required.

## 3. Architecture

### 3.1 Separate io_uring instance for fs (Option A)

The Linux reactor today is `union(Backend) { epoll, io_uring }`
where exactly one variant is live per Runtime (`src/reactor_linux.zig`).
The public Linux backend is epoll; the io_uring variant exists in
tree but is not user-selectable (see commit `f2426cd`).

The async-fs migration introduces a **second concern** the reactor
must serve: file I/O completions, in addition to the net-side
readiness events. Two structural options:

- **A: Separate io_uring instance for fs only.** Reactor grows a
  dedicated `IoUring` field used exclusively for fs ops. epoll
  continues to handle net. The Linux reactor poll iteration drains
  both: epoll_wait for net readiness, io_uring_peek_cqe for fs
  completions.
- **B: Unified io_uring instance for everything.** All I/O (net +
  fs) goes through one ring. epoll disappears on Linux. This is
  what Step 2f was attempting for net.

**Decision: Option A.** Rationale:

1. Decouples fs migration from the unfinished net migration. fs
   gets io_uring now without us having to first solve net's §4A
   race or finish Step 2f's persistent-registration story.
2. The two workloads have different optimal io_uring configurations.
   fs benefits from `IORING_REGISTER_FILES` / `REGISTER_BUFFERS`;
   net benefits from `IORING_POLL_ADD_MULTI`. Separate rings let
   each tune independently.
3. If Step 2f eventually lands and we want to collapse to one ring,
   the surface change is internal to `reactor_linux.zig` — public
   API is unaffected.

### 3.2 Reactor surface additions

`src/reactor_linux.zig` gains six methods on the tagged-union
`Reactor`:

```zig
pub fn fsRead   (self: *Reactor, fd: i32, buf: []u8,  offset: u64) IoError!usize
pub fn fsWrite  (self: *Reactor, fd: i32, buf: []const u8, offset: u64) IoError!usize
pub fn fsFsync  (self: *Reactor, fd: i32) IoError!void
pub fn fsFdatasync(self: *Reactor, fd: i32) IoError!void
pub fn fsOpen   (self: *Reactor, dirfd: i32, path: [*:0]const u8, flags: u32, mode: u32) IoError!i32
pub fn fsClose  (self: *Reactor, fd: i32) IoError!void
```

(`offset = ~@as(u64, 0)` sentinel means "use file position",
mirroring io_uring's convention. `fsRead` / `fsWrite` cover both
read/write and pread/pwrite shapes.)

The two reactor variants dispatch differently:

- **`.epoll` variant** — every fs method proxies to the existing
  spawnBlocking path. Behaviour is unchanged; this exists so that
  users who somehow ended up on the epoll variant (or where the
  fs io_uring path is disabled by sysctl) get the correct
  fallback transparently.
- **`.io_uring` variant** — submits an SQE, parks the coroutine,
  resumes on CQE. Real async file I/O.

On Darwin and Windows, the reactor's `fsRead` / etc methods exist
but always proxy to spawnBlocking — the public surface is uniform
across platforms; internal wiring varies.

### 3.3 Runtime construction

`Runtime.init` on Linux probes for io_uring at startup (via the
existing `probeIoUring` in `reactor_linux.zig:47`). If available,
the Reactor is initialised as `.io_uring` for the fs path *only*;
net stays on epoll regardless. Internally this looks like:

```zig
const reactor_inst = if (builtin.os.tag == .linux)
    if (Reactor.probeIoUring()) |_|
        try Reactor.initFsUring(cfg.allocator)   // epoll for net + io_uring for fs
    else |_|
        try Reactor.init(cfg.allocator)           // epoll for everything
else
    try Reactor.init(cfg.allocator);
```

Public Config is unchanged. The user sees `Runtime.init(.{ ... })`
and gets the best available backend for their kernel. **Developer
makes no choice.**

This means the Linux Reactor needs a *third* variant tag —
something like `Backend = enum { epoll, io_uring, epoll_with_fs_uring }`,
or a struct-of-fields rather than a tagged union. Cleaner to make
the Linux reactor a struct holding both an epoll instance and an
optional `IoUring` instance for fs, rather than overloading the
existing tagged union:

```zig
pub const Reactor = struct {
    epoll: epoll.Reactor,
    fs_ring: ?io_uring.FsRing,   // null if kernel lacks io_uring
    // ... dispatch methods route net ops to .epoll, fs ops to
    //     .fs_ring if non-null else spawnBlocking fallback.
};
```

This is a bigger restructure than the tagged-union dispatch, but
it's the right shape for the dual-concern reality. The internal
`initBackend(.io_uring)` constructor stays for conformance tests.

## 4. Buffer ownership

io_uring is completion-based: the kernel keeps a pointer to the
caller's buffer alive until the CQE fires. The buffer must remain
valid for that entire window.

With stackful coroutines this is structurally easier than for
state-machine async runtimes (Tokio, Rust async):

- **The buffer typically lives on the coroutine's own stack.**
  Coroutine stacks have stable VAs for the coroutine's lifetime
  (`docs/src/content/docs/internals/memory-model.md`). The buffer
  doesn't move while the coroutine is parked.
- **The coroutine is the one waiting on the CQE.** It does not
  return until the CQE arrives, so the stack frame holding the
  buffer is unconditionally live for the SQE-in-flight window.
- **No `Pin<&mut [u8]>` ceremony.** The stackful model gives us
  pinning for free; the language doesn't need to express it.

This is one of the places where stackful coroutines pay off vs the
async/await model — the same property that makes them ergonomic
also makes the io_uring buffer contract trivial.

**Open subtle case: heap buffers passed by `*[]u8` that the caller
might free after we return.** Not a concern for the v1 API because
all fs ops take `[]u8` slices owned by the caller for the call's
duration. Document this as part of `fs.File.read`'s contract: the
buffer must outlive the call. (Same as the existing spawnBlocking
contract; no change.)

## 5. Cancellation

When a coroutine with an in-flight io_uring SQE is cancelled, the
kernel still owns the buffer until either (a) the original CQE
fires (with a result or `-EINTR`/`-ECANCELED`), or (b) an
`IORING_OP_ASYNC_CANCEL` SQE fires for it AND that cancel's own
CQE comes back.

Three sub-options were considered (Q2 of the design conversation):

- **B1 — cancel-and-drain.** Submit ASYNC_CANCEL, wait for both
  CQEs (the original's `-ECANCELED` + the cancel ack), then
  return the cancellation error to the caller.
- **B2 — detach.** Submit ASYNC_CANCEL but don't wait; let the
  buffer leak to the kernel for the in-flight window. Requires
  heap-owned buffers (a stack buffer would be unsound if the
  coroutine resumed and returned past the frame).
- **B3 — no in-flight cancel.** Disallow cancellation while an fs
  op is in flight. Cancel only takes effect at op boundaries.

**Decision: B1.** Rationale:

1. Maintains the same cancel semantics fs has today (spawnBlocking
   ops can be cancelled-at-boundary; io_uring ops should not
   silently behave differently).
2. Preserves the "buffer can live on coroutine stack" guarantee
   from §4. B2 would force us to heap-allocate every buffer or
   document a sharp edge.
3. Cancel of in-flight fs ops is rare — bytes-from-disk operations
   don't routinely get cancelled the way long-lived socket reads
   do. The "slow on the rare path" cost is acceptable.

Implementation: `fs.File.read` on Linux io_uring path stores the
SQE's user_data so it can be matched by `cancelCoro`. When
`Cancel.fire` is called, the reactor submits an ASYNC_CANCEL SQE
referencing the user_data and waits for both CQEs before resuming
the coroutine with `error.Cancelled`.

## 6. Migration phases

1. **Phase 0 — Design memo.** This document. Captures decisions
   so the implementation work doesn't re-litigate them.
2. **Phase 1 — Reactor surface.** Add the six `fs*` methods to
   `reactor_linux.zig`. epoll variant proxies to spawnBlocking
   (no behaviour change). io_uring variant added but not wired
   from `Runtime.init` yet — exercised only by tests.
3. **Phase 2 — Probe + dual-instance reactor.** Convert the
   Linux Reactor from tagged union to a struct holding both an
   epoll instance and an optional fs-io_uring instance. Probe at
   `Runtime.init`; populate `fs_ring` if available. Net ops
   continue to route through epoll; fs ops route through fs_ring
   when present, spawnBlocking fallback otherwise.
4. **Phase 3 — fs/file.zig rewire.** `File.read` and friends call
   into the reactor's `fsRead` instead of spawnBlocking directly.
   Public API is unchanged. Darwin and Windows continue to route
   through spawnBlocking via the cross-platform reactor surface.
5. **Phase 4 — Cancellation.** Wire ASYNC_CANCEL drain per §5.
   Add tests that cancel an in-flight read and verify the buffer
   contract holds.
6. **Phase 5 — Benchmarks.** New `bench-fs-read` and `bench-fs-write`:
   N concurrent random reads on a pre-allocated 1 GiB file,
   measure throughput. Go reference uses `os.File.ReadAt` with
   the same shape. Target: ≥ 2× win on Linux with io_uring vs
   the spawnBlocking baseline.
7. **Phase 6 — Optimisations (optional, gated on bench data).**
   `IORING_REGISTER_FILES` for hot files; `IORING_REGISTER_BUFFERS`
   for hot buffers. Only add these if bench data shows they
   actually move the number.

Each phase must pass the full bench suite (per CLAUDE.md's
phase-landing protocol) and 5-run-median the fs benches once
Phase 5 lands.

## 7. Development environment

io_uring code cannot be developed on Darwin. Both Step 2f reverts
on this branch were caused by exactly this mistake: writing
io_uring code on Darwin, hoping CI would catch the bugs, and
discovering at merge time that the kernel-version-dependent
behaviour broke in ways that local tests couldn't see.

**Required dev loop for Phases 2-6:** a Linux machine (cloud VM,
container, or dual-boot) with kernel ≥ 5.10, where the full test
suite + the new fs benches run in under 30 seconds. Iteration
without that loop is a known-failed approach.

**What can be done from Darwin:** Phase 0 (this memo) and Phase 1
(the reactor surface + spawnBlocking-proxy variant — both of
which are portable and don't exercise io_uring at runtime).
Phases 2-6 require Linux.

## 8. Acceptance criteria

- **Same public API.** `fs.File.read` / `.write` / `.pread` /
  `.pwrite` / `.fsync` / `.fdatasync` look identical to today.
  Source-level compatibility for all existing fs callers.
- **No spawnBlocking on Linux fs ops** when io_uring is available
  (verified by instrumenting blocking pool stats during
  `bench-fs-read`).
- **Transparent fallback** to spawnBlocking on older Linux,
  Darwin, and Windows. Same code paths, just different reactor
  internals.
- **≥ 2× throughput** on `bench-fs-read` (4 KB random reads on
  1 GiB file, 64 concurrent ops) vs the spawnBlocking baseline
  on Linux. Target is approximate; actual gain depends on kernel
  version and storage backend.
- **Cancellation correctness.** An in-flight fs read cancelled
  mid-op returns `error.Cancelled`, the buffer is not touched by
  the kernel after `read` returns, and no use-after-free occurs.
  Verified by a dedicated test that cancels reads against a slow
  `tmpfs`-backed file (or `/dev/zero` with a slow consumer).
- **Memory model documented.** Updates to
  `docs/src/content/docs/internals/memory-model.md` covering the
  new orderings: SQE submission, CQ drain, cancel-and-drain
  interleaving with the parked coroutine's wake.

## 9. Open questions (to revisit during implementation)

1. **`fs.openOptions` async path.** `open(2)` is normally fast
   but can block on slow filesystems (network mounts, encrypted
   FS unlock). Worth migrating to `IORING_OP_OPENAT2` for
   parity, or leave on spawnBlocking? Lean: migrate, but only
   after Phases 2-5 stabilise.
2. **Directory ops.** `IORING_OP_GETDENTS` exists (5.20+). Worth
   it for `Dir.entries` / `Dir.walk`? Probably yes, but separate
   phase — not v1 scope.
3. **`MappedFile.{prefetch,sync}`.** These are technically file
   I/O. Worth routing through io_uring? `madvise` and `msync`
   have io_uring equivalents but are infrequent ops. Defer until
   bench data shows they matter.
4. **Per-worker rings vs single shared ring.** Single ring is
   simplest. Per-worker scales better on > 8 cores. Start with
   single; revisit if `bench-fs-read` saturates at the ring's
   lock contention.

## 10. Decision log

| Date | Decision | Rationale |
|---|---|---|
| 2026-05-26 | Async fs via io_uring, not by enlarging the blocking pool. | io_uring is the modern Linux interface for async file I/O; the perf delta on storage is where it actually wins. Blocking-pool scaling has structural limits (thread count, context switch cost). |
| 2026-05-26 | Separate io_uring instance for fs only (Option A). | Decouples fs migration from the unfinished Step 2f net migration. Each ring can tune for its workload. |
| 2026-05-26 | Cancel-and-drain on in-flight cancellation (Option B1). | Matches existing fs cancel semantics; preserves stack-buffer ownership; rare-path latency cost is acceptable. |
| 2026-05-26 | Pure replacement of fs ops' internals (no additive variants). | Same principle as removing `Config.io_backend`: Volt picks the right impl, developer never sees the choice. |
| 2026-05-26 | Reactor becomes a struct (`epoll` + optional `fs_ring`) instead of the existing tagged union. | The dual-concern reality (net needs readiness, fs needs completion) doesn't fit a tagged union cleanly. Struct-of-fields makes the dispatch explicit. |
