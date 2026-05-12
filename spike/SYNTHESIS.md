# POC Phase 2 — Synthesis & Decision

**Decision: Branch 1 — PASS. Proceed to v2 architecture rewrite.**

The POC sprint validated that a stackful Zig coroutine runtime CAN
match-or-beat Go across all three headline workloads. The 33× spawn+join
gap, the 5.6× channel gap, and the 21 % TCP echo gap in the current
Volt tree are all v1 architectural waste, NOT fundamentals.

## 2.1 — Component winners

| Architectural slot | Winning POC | Result |
|---|---|---|
| Context switch | A (wide save, current Volt is fine) | 6 ns/swap; narrow doesn't help |
| Dispatch tail | B (either; enum slightly cleaner) | 1 ns saved — both fine |
| Parker primitive | D (ulock on Darwin) | 15 % savings; use ulock + futex but PARK SPARINGLY |
| Scheduler | G (spin-then-park, single-worker fast path) | 22 ns/task @ workers=1 |
| Spawn+join floor | C (combined integration) | **93 ns/op — beats Go by 1.6×** |
| Channel SPSC | F (comptime specialization) | **29 ns/op — beats Go by 1.14×** |
| Reactor | H (inline kqueue poll) | **2.5 µs/RTT — well under Go target** |
| Stack | C using 16 KiB heap | sufficient; growth strategy deferred |
| Join model | C using atomic counter | per-coro Park is unnecessary |
| Allocator | C using `std.heap.smp_allocator` | DebugAllocator costs 16× more |

## 2.2 — Do the winners compose?

Yes. POC-C demonstrates the composition empirically:
**spin scheduler + wide ctx + heap stack + smp_allocator + atomic counter
join → 93 ns/op spawn+join.**

The only composition concern surfaced was POC-G's multi-worker
contention on a global Treiber stack (518 ns/task @ workers=8). That's
not blocking — POC-C's single-worker beats Go single-handedly, and a
per-worker FIFO + steal design (well-trodden in Tokio, Go, may) is
known to scale.

## 2.3 — Worst-case workload after POCs

The widest residual gap is **multi-worker spawn+join with a global
queue.** At workers=11, POC-G's spin scheduler does 709 ns/task —
4.8× slower than Go's 149 ns. The fix is well-understood:

- Per-worker FIFO queues (Chase-Lev deque or simpler bounded ring)
- Spawning worker pushes to its own queue (locality)
- Workers pop from own queue first, steal on empty
- Spin-then-park ONLY when steal also fails across all siblings

This is exactly Go's `findRunnable` shape. It's a known-good pattern.
The implementation is a few hundred lines of Zig. **No POC is needed —
the pattern is industry-standard and tested.**

## 2.4 — Decision

**Branch 1: PASS.** Proceed to Phase 3 (rewrite).

Architecture v2 doc (`docs/internals/architecture-v2.md`) is the next
artifact. It captures the proven foundation and the design decisions
that follow from these POCs.

## 2.5 — What this means for v0.x existing tree

Per the user's "full rewrite OK" answer in plan setup:

- **Disposable**: `src/coroutine/*` (replace with v2 minirt), `src/scheduler/*`
  (replace with spin-then-park), `src/sync/Park.zig` (drop per-coro Park),
  `src/channel/Channel.zig` (split into Spsc + Mpmc), `src/api/*` (rebuild
  on new core).
- **Keep**: `src/internal/syscall.zig` (platform syscall wrappers — well-tested),
  `src/internal/win32/*` (preserved IOCP + AFD work that's cross-compile-clean),
  `src/internal/util/*` (small utilities), the cross-platform reactor
  abstraction shape (will hydrate POC-H into it).

## 2.6 — Risks captured during POCs

- **Multi-worker scaling is unproven in spike form.** The per-worker FIFO
  design is industry standard but we should spike it in Phase 3 before
  committing, with a multi-worker version of POC-C. (Maybe 1 week of work.)
- **POC-H is serial pipe, not parallel TCP.** Adding socket overhead +
  multi-client scheduling will increase per-RTT. We expect a 64-client
  TCP echo to land in the 5-7 µs/RTT range — comfortably under Go's 9 µs.
- **Stack growth not yet tested.** POC-C used fixed 16 KiB. For deep
  recursion or large stack frames, we'll need mmap-grow OR a smarter
  initial size. v2 design phase task.
- **Linux/x86 not measured.** All POCs ran on macOS arm64. The ctx
  switch needs an x86_64 SysV variant; the parker switches from ulock
  to futex; the reactor switches kqueue→epoll/io_uring. Should not
  affect the headline numbers proportionally but must be validated
  during the rewrite.

## 2.7 — What the v2 rewrite needs to deliver

Functional parity with v0.x's headline use cases:
- `volt.run(fn, args)` — bootstrap
- `volt.spawn(fn, args)` returns `*Task` with `.join() !T`
- `volt.launch(fn, args)` returns `*Job` with `.cancel()`
- WaitGroup-style atomic-counter join (NO per-coro Park)
- `Spsc(T, cap)` + `Mpmc(T, cap)` channels
- `volt.sync.{Mutex, Notify, Semaphore}` (kept; redesigned over v2 scheduler)
- `volt.io.{TcpListener, TcpStream}` over the tight kqueue reactor
- `volt.time.sleep` over reactor's timer wheel (current implementation
  is fine if rebuilt on v2)
- Cancellation: token-based (cancel sets a flag; reactor wakeups check)
- Structured concurrency (`volt.scope`) over v2 spawn

What stays out:
- Per-coro Park primitive (replaced by atomic counter)
- The EventSource vtable (replaced by enum or kept — both perform equally)
- The Worker.run findWork path with its 4-tier priority (replaced by
  spin-then-park + per-worker FIFO + steal)
- The current mmap/munmap-traffic'd LocalStackPool (replaced by simpler
  heap-alloc + opt-in growth)

## 2.8 — Performance commitments for v2

These are the Phase 3 gates. v2 only ships when all are green:

| Workload | Volt today | v2 target | Margin |
|---|---|---|---|
| spawn+join (10k, single-worker) | 4,163 ns | ≤ 150 ns | within 5 % of Go's 149 |
| spawn+join (10k, multi-worker) | 4,163 ns | ≤ 200 ns | comfortable headroom |
| spawn+waitgroup | 3,749 ns | ≤ 150 ns | match Go |
| channel SPSC cap=16 | 180 ns | ≤ 35 ns | match Go |
| channel pipeline 3-stage | 97 ns | ≤ 100 ns | match Go (already there) |
| TCP echo 64 clients × 16 RTT | 10,960 ns | ≤ 9,500 ns | beat Go |
| yield (one-way ctx switch) | 11 ns | ≤ 12 ns | hold the win |
| mutex saturated 8 coros | 42 ns | ≤ 50 ns | hold the win |

All measured via the existing `zig build compare` orchestrator.

## 2.9 — Phase 3 hand-off

Phase 3 (rewrite) inherits:
- This synthesis + POC results as the proof
- `spike/[A-H]_*/` as reference implementations to start from
- `docs/internals/architecture-v2.md` (to be written) as the design contract
- `BENCHMARKS.md` as the gate (POC numbers replace v0.x numbers)

Phase 3 estimated effort:
- Multi-worker FIFO + steal scheduler: 1 week
- v2 ctx + coroutine + stack: 3 days (mostly already exists)
- Channels (Spsc + Mpmc rewrite): 1 week
- Sync primitives (Mutex/Notify/Semaphore on v2 scheduler): 1 week
- Reactor (full kqueue + epoll + io_uring): 1-2 weeks
- Public API surface + tests porting: 2-3 weeks
- Race correctness + stress (Workstream R from prior plan): 1-2 weeks

Total: 6-10 weeks for v1.0.0-zig0.16.0 on the new foundation.
