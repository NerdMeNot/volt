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

### 3.1 One io_uring per worker for fs (revised after reference research)

The Linux reactor today is `union(Backend) { epoll, io_uring }`
where exactly one variant is live per Runtime (`src/reactor_linux.zig`).
The public Linux backend is epoll; the io_uring variant exists in
tree but is not user-selectable (see commit `f2426cd`).

The async-fs migration introduces a **second concern** the reactor
must serve: file I/O completions, in addition to the net-side
readiness events. The original framing of this memo proposed "one
shared fs io_uring instance." After reading three production
references end-to-end (TigerBeetle, Glommio, Seastar — see §11),
that framing is wrong. **The universal pattern is one io_uring per
OS thread.** None of the three references shares a ring across
threads; the kernel-side optimisations (`SINGLE_ISSUER`,
`DEFER_TASKRUN`) explicitly assume same-thread submit-and-reap.

Volt's natural mapping is **one io_uring per worker (P)**. The
ring lives in `src/p.zig` alongside the work-stealing queue and
the per-P mailbox. A coroutine that calls `fs.File.read` submits
on the current P's ring; when the CQE fires, the coroutine is
re-dispatched (possibly onto a different worker — that's fine
because the buffer lives on the coroutine's stack, not in TLS).

Net still routes through the single shared epoll instance. Two
structural options were considered for the *topology* between net
and fs:

- **A: One epoll (net, shared) + N io_urings (fs, per-worker).**
  This matches the references' "ring per thread" pattern. fs and
  net use different mechanisms because they have different
  characteristics: net is readiness-oriented and Volt's epoll path
  is mature; fs is completion-oriented and benefits most from
  io_uring's no-thread-hop story.
- **B: Unified io_uring per worker for both net and fs.** Would
  eliminate epoll on Linux entirely. This is what Step 2f was
  attempting for net. Couples fs migration to Step 2f's
  outstanding correctness work.

**Decision: A (per-worker fs rings, shared epoll for net).** Rationale:

1. Decouples fs migration from the unfinished Step 2f net migration.
   fs gets io_uring now without first solving net's §4A race.
2. Matches the canonical "ring per OS thread" pattern from
   TigerBeetle, Glommio, and Seastar (§11). No cross-worker ring
   sharing, no mutexes on `get_sqe`/`submit`.
3. Per-worker rings let us use `IORING_SETUP_SINGLE_ISSUER` +
   `DEFER_TASKRUN` on 6.1+ kernels — these flags require
   same-thread submit-and-reap, which a shared ring cannot
   guarantee.
4. If Step 2f eventually lands and net migrates to per-worker
   io_uring too, the shape generalises cleanly: each worker just
   gets a *second* ring (or a separate set of fds on the same
   ring, depending on Glommio-style topology choices).

The Linux reactor surface is unchanged from a public-API
perspective; internally, the dispatch knows about the per-P ring
and routes fs ops there.

### 3.2 Reactor surface additions

`src/reactor_linux.zig` (and the kqueue/IOCP equivalents) gain six
methods. Each is called on the *current worker's* reactor view —
which on Linux dispatches to that worker's io_uring instance.

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

On Linux with per-worker io_uring rings available, these submit an
SQE to the current P's ring, park the coroutine, and resume on
CQE. On Darwin, Windows, older Linux, or when io_uring init
failed, the method body proxies to the existing spawnBlocking
path. Public API is identical across platforms.

**Implementation invariants (lifted from the references, see §11):**

1. **Setup flags**:
   `IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_SUBMIT_ALL`.
   `SINGLE_ISSUER` (6.0+) requires same-thread submit-and-reap —
   which per-worker rings provide naturally. `SUBMIT_ALL` (5.18+)
   keeps submitting on per-SQE error rather than stopping at the
   first failure. Fall back to flags=0 on older kernels.
   **Don't use `DEFER_TASKRUN`** — it defers completion work
   (CQE visibility AND registered-eventfd writes) until the
   submitter calls `io_uring_enter(GETEVENTS)`. Our submit-once /
   park / drain-on-wake pattern doesn't naturally call enter with
   GETEVENTS (just submit, no wait), so completions never become
   visible. DEFER_TASKRUN benefits workloads that batch many SQEs
   between drains; ours is the opposite shape. (Caught during the
   Phase 2C.2 routing implementation when the `fs facade` test
   hung — see Phase 2C decision log.)
2. **Never use SQPOLL.** None of the three references enable it.
   Trades a kernel thread for a syscall; bad fit unless cores are
   idle.
3. **`io_uring_ring_dontfork`** at construction
   (Seastar `reactor_backend.cc:1228`). Fork+exec in a child
   shouldn't inherit locked ring mappings.
4. **Probe twice: features AND opcodes.** Use
   `IORING_FEAT_EXT_ARG | IORING_FEAT_SUBMIT_STABLE |
   IORING_FEAT_NODROP` as the feature floor, and
   `io_uring_opcode_supported` (or the Zig-native equivalent) for
   each opcode we depend on. (`READ`, `WRITE`, `FSYNC`,
   `FDATASYNC`, `OPENAT`, `CLOSE`, `ASYNC_CANCEL`.)
5. **Lazy batched submission.** SQEs are written to the userspace
   ring at `fsRead`/etc time; the actual `io_uring_enter` is one
   call per reactor poll tick. Universal pattern across
   TigerBeetle, Glommio, Seastar.
6. **CQE drain → list → dispatch.** Drain CQEs into a
   `ready_completions` list, then dispatch in a separate pass.
   TigerBeetle's comment is the canonical justification:
   > "We do not run the completion here (instead appending to a
   > linked list) to avoid: recursion through flush_submissions
   > / flush_completions, unbounded stack usage, confusing stack
   > traces."
   A coroutine resumed inside the CQE-drain loop will likely
   re-suspend on another fs op, queuing a new SQE — doing that
   inside the iteration is a recursion footgun.
7. **`abort()` on `io_uring_submit` < 0.** Only `EBADF`/`EINVAL`/
   `EOPNOTSUPP` reach this path and they all mean state
   corruption (Seastar `68cec26e`). Match this; don't try to
   recover.
8. **`IORING_ENTER_EXT_ARG` for reactor tick budget.** Fold the
   "wait up to N ns" into the same syscall as submit (TigerBeetle
   uses this; saves a separate `IORING_OP_TIMEOUT` SQE per tick).

### 3.3 Runtime construction

`Runtime.init` on Linux probes for io_uring at startup (via the
existing `probeIoUring` in `reactor_linux.zig:47`) plus an opcode
probe for every op we depend on (per invariant 4 above). If both
pass, **each P gets its own io_uring instance** at the same time
the P's WSQ and mailbox are initialised in `src/p.zig`.

```zig
// in src/p.zig P.init
self.fs_ring = if (builtin.os.tag == .linux and rt.fs_uring_available)
    try FsRing.init(allocator)
else
    null;
```

The shared epoll instance still lives on the Reactor (for net);
fs ops route to `current.require().p.fs_ring` instead of through
the Reactor. On Darwin, Windows, or older Linux, `fs_ring` is
always null and fs ops proxy to spawnBlocking.

Public Config is unchanged. The user sees `Runtime.init(.{ ... })`
and gets the best available backend for their kernel. **Developer
makes no choice.**

The Linux Reactor itself stays a tagged union (`epoll` /
`io_uring`) — we no longer need to restructure it into a struct,
because the per-worker fs ring lives on the P, not on the
Reactor. This is a strictly smaller change than what the original
draft of this memo proposed.

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

**Decision: B1, refined with Glommio's two-state model.** Rationale:

1. Maintains the same cancel semantics fs has today (spawnBlocking
   ops can be cancelled-at-boundary; io_uring ops should not
   silently behave differently).
2. Preserves the "buffer can live on coroutine stack" guarantee
   from §4. B2 would force us to heap-allocate every buffer or
   document a sharp edge.
3. Cancel of in-flight fs ops is rare — bytes-from-disk operations
   don't routinely get cancelled the way long-lived socket reads
   do. The "slow on the rare path" cost is acceptable.

**Implementation (Glommio's pattern, `sys/source.rs:276-304`):**
the in-flight SQE has two distinct states, and cancellation differs
between them:

- **Enqueued** — SQE has been written to the userspace ring but
  `io_uring_enter` hasn't been called yet (i.e. between the
  coroutine's `fsRead` call and the next reactor poll tick). On
  cancel: just mark the slot cancelled; the submitter skips it at
  flush time. No kernel involvement, no buffer wait, fast path.
- **Dispatched** — SQE is in flight in the kernel. On cancel:
  submit an `IORING_OP_ASYNC_CANCEL` SQE referencing the original
  SQE's `user_data`, then wait for both CQEs (the original's
  `-ECANCELED` and the cancel ack). The buffer remains pinned on
  the coroutine's stack throughout (B1 semantics).

The Enqueued fast path matters because the *common* cancellation
case in practice is "coroutine was cancelled before the reactor
had a chance to flush the SQE" — a timer fire, a sibling task
failing, etc. Glommio measured this; their cancellation queue
hits the Enqueued path far more often than Dispatched.

`fs.File.read` on Linux io_uring path stores the SQE's user_data
in the parked coroutine's PollDesc so it can be matched by
`cancelCoro`. When `Cancel.fire` is called, the reactor
distinguishes Enqueued vs Dispatched and follows the appropriate
path. The buffer contract is unchanged: caller's `[]u8` slice
remains valid until the call returns.

## 6. Migration phases

1. **Phase 0 — Design memo.** This document. Captures decisions
   so the implementation work doesn't re-litigate them.
2. **Phase 1 — Reactor surface (portable).** Add the six `fs*`
   methods to the cross-platform reactor surface. All backends
   (kqueue, epoll, IOCP) proxy to spawnBlocking initially —
   behaviour is unchanged, but the API is now in place. Wire
   `fs/file.zig` to call into the reactor surface so the next
   phases can swap implementations without touching fs callers.
   *Portable; can be done from Darwin.*
3. **Phase 2 — Per-worker io_uring ring on Linux.** Probe at
   `Runtime.init`; if io_uring is available, create one
   `IoUring` instance per P at P initialisation. The reactor
   surface's Linux fs methods now route to the current P's ring
   when present, spawnBlocking when not. Implements the lazy-
   batch submission + drain-list completion patterns from §3.2.
   *Linux-only; requires Linux dev loop.*
4. **Phase 3 — Cancellation.** Wire the Enqueued/Dispatched
   state distinction from §5. Add tests that cancel an in-flight
   read mid-op and verify the buffer contract holds. Tests run
   against a slow `tmpfs`-backed file (predictable wakeup) and a
   real disk file (real-world shape).
5. **Phase 4 — Benchmarks.** New `bench-fs-read` and
   `bench-fs-write`: N concurrent random reads on a pre-allocated
   1 GiB file, measure throughput. Go reference uses
   `os.File.ReadAt` with the same shape. Target: ≥ 2× win on
   Linux with io_uring vs the spawnBlocking baseline.
6. **Phase 5 — Open and close on io_uring (optional).**
   `IORING_OP_OPENAT`, `IORING_OP_CLOSE`. open/close are usually
   fast but block on slow filesystems (network mounts, encrypted
   FS unlock). Worth migrating after Phases 2-4 stabilise.
7. **Phase 6 — Optimisations (gated on bench data).**
   `IORING_REGISTER_FILES` for hot files; `IORING_REGISTER_BUFFERS`
   for hot buffers. Glommio uses a global registered pool with a
   buddy allocator — worth it for them because of O_DIRECT
   alignment requirements; less obvious for Volt's non-DMA path.
   Only add if bench data shows the win.

Each phase must pass the full bench suite (per CLAUDE.md's
phase-landing protocol) and 5-run-median the fs benches once
Phase 4 lands.

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
- **Throughput parity-or-better** on `bench-fs-read` vs the
  spawnBlocking baseline on Linux. Original ≥ 2× target was
  not met in Phase 4 (io_uring was 1.19-1.88× *slower* across
  concurrency levels). **Phase 6A (inline-completion fast
  path) flipped the trend**: at N=256 io_uring is now 1.09×
  *faster*; at N=64 still 1.43× slower (improved from 1.68×).
  Remaining Phase 6 work needed for the full ≥ 2× target:
  batched submission across coroutines (don't `io_uring_enter`
  per op), registered buffers (avoid per-op `get_user_pages`
  on the kernel side). See `bench/bench_fs_read.zig` header
  for the results timeline.
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
4. **Registered buffer pool — per-worker or global?** Glommio
   uses one global pool with a buddy allocator (per-thread
   executor makes that natural). With per-worker rings, options
   are: (a) register the same buffer set on every ring at init
   (cheap, only the register call is duplicated); (b) per-worker
   buffer pool with worker-affinity (best perf but buffers travel
   with coroutines under work-stealing, complicating ownership);
   (c) skip in v1, add later. Lean (c) for Phase 6 decision.

## 10. Decision log

| Date | Decision | Rationale |
|---|---|---|
| 2026-05-26 | Async fs via io_uring, not by enlarging the blocking pool. | io_uring is the modern Linux interface for async file I/O; the perf delta on storage is where it actually wins. Blocking-pool scaling has structural limits (thread count, context switch cost). |
| 2026-05-26 | One io_uring per worker (P), not one shared ring. | Universal "ring per OS thread" pattern in TigerBeetle, Glommio, Seastar. Lets us use `SINGLE_ISSUER`/`DEFER_TASKRUN`, avoids cross-worker mutexes, matches Volt's existing per-P state shape. **Revised from original draft after reference research (§11).** |
| 2026-05-26 | fs on per-worker io_uring; net stays on shared epoll. | Decouples fs migration from the unfinished Step 2f net migration. Same Option A spirit as the original draft, but at per-worker granularity. |
| 2026-05-26 | Cancel-and-drain on in-flight cancellation (Option B1), refined with Glommio's Enqueued/Dispatched two-state model. | Matches existing fs cancel semantics; preserves stack-buffer ownership; the Enqueued fast path matches the empirically-common cancellation case. |
| 2026-05-26 | Pure replacement of fs ops' internals (no additive variants). | Same principle as removing `Config.io_backend`: Volt picks the right impl, developer never sees the choice. |
| 2026-05-26 | Setup flags: `SINGLE_ISSUER \| DEFER_TASKRUN \| SUBMIT_ALL` on ≥6.1, fall back to 0 on older kernels. No SQPOLL. | Modern stack for per-worker rings. SQPOLL is mutually exclusive with DEFER_TASKRUN and none of the three references enable it. |
| 2026-05-27 | **Revised**: setup flags are `SINGLE_ISSUER \| SUBMIT_ALL` only — drop DEFER_TASKRUN. | DEFER_TASKRUN defers CQE-visibility AND eventfd writes until `io_uring_enter(GETEVENTS)`. Our submit-once / park / drain-on-wake pattern doesn't call enter-with-GETEVENTS, so completions never appear. Caught when the `fs facade` test hung during Phase 2C.2 routing. DEFER_TASKRUN is for workloads that BATCH many SQEs between drains. |
| 2026-05-29 | **Revised again**: setup flags are `SUBMIT_ALL` only — drop SINGLE_ISSUER too. | Phase 3's cancel-and-drain path submits ASYNC_CANCEL after `park()`; by then work-stealing may have migrated the coroutine to a different worker. SINGLE_ISSUER causes the kernel to reject cross-thread `io_uring_enter` with EEXIST. The userspace SQ ring is safe under sequential-with-handoff (park acts as release/acquire); only the kernel-level SINGLE_ISSUER restriction rejected it. Discovered via SEGV on the full Linux test suite — see `docs/internals/phase-3-design.md` decision log. |
| 2026-05-26 | Linux Reactor stays a tagged union; fs ring lives on P, not on Reactor. | Smaller restructure than the original "Reactor becomes a struct" proposal. Per-worker rings naturally live alongside the WSQ in `src/p.zig`. |
| 2026-05-26 | No registered buffers / no registered files in v1. Heap-canonical buffer pattern; stack buffers safe given B1. | All three references either skip registration (TB, Seastar) or use it only for DMA (Glommio). Defer to Phase 6 with a benchmark gate. |

## 11. Patterns from production io_uring runtimes

Distilled from end-to-end reads of three reference implementations
(2026-05-26). Cross-references inform the decisions in §10; full
research notes are preserved in the conversation transcript.

### 11.1 TigerBeetle (`tigerbeetle/tigerbeetle@main`, `src/io/linux.zig`)

The closest Zig-native analogue. ~1,800-line io_uring backend
behind a 3-OS unified `IO` interface. Patterns Volt steals
directly:

- **Lazy-batched submission** (`linux.zig:167-186`): `enqueue()`
  only writes the SQE; `flush_submissions()` calls
  `io_uring_enter` once per `run()` iteration with
  `IORING_ENTER_EXT_ARG` folding the loop deadline into the same
  syscall.
- **Drain CQEs into a list, dispatch separately** (`linux.zig`,
  `flush_completions`): the comment "we do not run the
  completion here ... to avoid recursion through
  flush_submissions/flush_completions, unbounded stack usage,
  confusing stack traces" is the canonical justification. Volt
  must do the same.
- **`EINTR`/`EAGAIN` self-requeue** with XFS-specific comment:
  "Some file systems, like XFS, can return EAGAIN even when
  reading from a blocking file without flags like RWF_NOWAIT."
  Volt should reproduce this loop per op.
- **liburing #281 workaround** (`linux.zig:180`):
  `CompletionQueueOvercommitted`/`SystemResources` requires
  draining CQs before retrying submit. Real bug; copy the
  workaround verbatim.
- **No cancellation, no registered buffers, no
  `READ_FIXED`/`WRITE_FIXED`.** Existence proof that a
  production io_uring user can ship without these. Defer.

What does *not* translate: TigerBeetle has one IO instance on
one thread driving one ring. Volt is multi-worker — needs
per-worker rings.

### 11.2 Glommio (`DataDog/glommio@master`)

Production Rust async runtime, per-thread executor model.
Handles both net and fs through io_uring rings. Three rings per
executor (`uring.rs:1276-1284`):
- `main_ring` — fs (non-DMA) + generic ops + cross-executor
  channel wakeups
- `latency_ring` — net RX/accept/connect + preempt timer (so
  high-throughput disk completions can't starve latency-
  sensitive net)
- `poll_ring` — `IORING_SETUP_IOPOLL` for DMA disk I/O on NVMe
  (the IOPOLL flag is ring-wide; you can't mix poll/non-poll
  SQEs in one ring)

Patterns Volt steals:

- **Cancellation as Enqueued/Dispatched state machine**
  (`sys/source.rs:276-304`): Drop walks the in-flight `Source`
  and either marks it cancelled (submitter skips) or posts an
  ASYNC_CANCEL. Buffer/Source lifetime extends through cancel
  CQE. Refined model for §5.
- **Software submission queue in front of SQ**
  (`uring.rs:181-186`, `555-571`): SQ-full doesn't propagate as
  error; SQEs sit in a `VecDeque` until the next reactor turn
  frees slots. Volt's per-P backpressure should follow this
  pattern, not TigerBeetle's panic-on-second-try approach.
- **Defer-and-track cancel rationale**: comments at
  `uring.rs:738-752` document the fd-reuse race that makes
  eager cancel processing necessary. Same race-shape Volt's
  §4A close-vs-register has on net; the fs path avoids it but
  cancellation still needs to handle it correctly.
- **`membarrier::heavy()` before sleep** (`uring.rs:1249-1263`),
  citing Seastar's memory-barriers blog. Worth investigating for
  our parker integration.

What does not translate: Glommio's `RefCell`-not-`Mutex` ring
ownership assumes per-thread executors. Volt's per-worker model
mirrors this; cross-worker submission is forbidden by
construction.

### 11.3 Seastar (`scylladb/seastar@master`, `src/core/reactor_backend.cc:1192-1605`)

ScyllaDB's reactor, ~410 lines of production io_uring code. Per-
shard ring (= per-thread). Patterns Volt steals:

- **`io_uring_ring_dontfork(&ring)`** at construction
  (`reactor_backend.cc:1228`). Fork+exec shouldn't inherit
  locked mappings.
- **Probe twice — features AND opcodes**
  (`reactor_backend.cc:1196-1250`): check
  `IORING_FEAT_SUBMIT_STABLE | IORING_FEAT_NODROP`, then call
  `io_uring_opcode_supported` for every opcode you depend on.
  Volt's existing `probeIoUring` (`reactor_linux.zig:47`) needs
  to grow opcode probing.
- **Single dirty flag for batched submit**
  (`reactor_backend.cc:1380-1389`): `_has_pending_submissions`
  set by every enqueue, checked by the flush. Clean shape.
- **`io_uring_peek_batch_cqe` + `io_uring_cq_advance(n)`**
  (`reactor_backend.cc:1507-1511`): drain in one batch, advance
  once. More efficient than per-CQE `seen()` calls.
- **`abort()` on `io_uring_submit < 0`** (Seastar commit
  `68cec26e`): only `EBADF`/`EINVAL`/`EOPNOTSUPP` reach this
  path; all are state-corruption signals. Don't try to recover.
- **`RLIMIT_MEMLOCK` ≥ 8 MB gate** (`reactor_backend.cc:1264-1290`)
  for kernels < 5.12. Volt's CLAUDE.md floor is 5.15 so this is
  less critical, but worth a graceful-error message.

What does not translate: Seastar has no cancellation
implementation; `reactor_backend.cc:1471-1478` literally
`abort()`s on `poll_remove`/`cancel` opcodes with a code comment
saying "as more features of io_uring are exploited, we'll
utilize more of these opcodes." Don't follow this — Volt
needs cancel from day one (§5).

### 11.4 What Volt must do (consolidated)

1. **One io_uring per worker (P)**, lifetime tied to P.
2. **Setup flags**: `SINGLE_ISSUER | SUBMIT_ALL`, fall back to 0
   below. **Do NOT include DEFER_TASKRUN** — it defers
   CQE-visibility and registered-eventfd writes until
   `io_uring_enter(GETEVENTS)`, which our submit/park/drain
   pattern doesn't call.
3. **`io_uring_ring_dontfork`** every ring.
4. **Probe features + opcodes** at `Runtime.init`; cache
   `rt.fs_uring_available`.
5. **Lazy batched submission** — one `io_uring_enter` per
   reactor tick per ring, gated by a dirty flag.
6. **Drain CQEs to a list, dispatch separately.** No
   resume-inside-drain.
7. **Software queue in front of SQ** — never propagate
   `SubmissionQueueFull` to user code; either re-queue or
   force-flush and retry.
8. **EINTR/EAGAIN self-retry** per op, document XFS reason.
9. **Cancel via Enqueued/Dispatched two-state model**, buffer
   stays pinned until cancel CQE.
10. **`abort()` on submit-side state corruption** — these are
    Volt bugs, not recoverable conditions.

### 11.5 What Volt must NOT do

1. **No SQPOLL.** No reference uses it; the kernel poll thread
   trades a kernel thread for a syscall, bad fit unless cores
   are idle.
2. **No `DEFER_TASKRUN`** for submit/park/drain workloads.
   Defers completion work — including CQE-visibility and
   eventfd writes — until the submitter calls
   `io_uring_enter(GETEVENTS)`. Our pattern doesn't, so ops
   hang. Use it only when batching SQEs and draining many at
   once.
3. **No cross-worker ring sharing.** Breaks SINGLE_ISSUER.
4. **No per-syscall `io_uring_enter`.** Always batch.
5. **No `IOPOLL` without O_DIRECT** — no-op on cached files.
6. **No assumption that a setup flag works because the kernel
   is new enough** — even Axboe's `poll-bench.c:34-40` falls
   back to `flags=0` on `-EINVAL`. Probe the flag.
7. **No `free()` on a probe** — use `io_uring_free_probe` (or
   the Zig equivalent).
8. **No stack-allocated buffer for an op whose coroutine might
   unwind past the call.** The B1 cancel-and-drain guarantees
   the coroutine waits for the cancel CQE, so stack buffers are
   safe in the normal cancel path. A panic past the parked
   coroutine would violate this — same constraint as everywhere
   else in Volt.
9. **No silent cancellation TODO.** Seastar deferred it but
   `abort()`s loudly. If Volt defers any part of cancel, mirror
   that pattern so it can't regress into a silent no-op.
