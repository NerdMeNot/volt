# Phase 2C — implementation design

**Status:** Approved 2026-05-27
**Parent:** [docs/internals/async-fs-io.md](async-fs-io.md)
**Tracking:** [#17](https://github.com/NerdMeNot/games/issues/17)
**Driver:** Wire `Reactor.fsRead`/etc to the per-P io_uring rings
populated in Phase 2B (commit `43abf4c`). Submit + park + drain +
unpark, end-to-end, with no fs-side API change.

This memo records the four decisions made before writing the code,
so the implementation work can be reviewed against the design
rather than re-deriving it from the diff.

## 1. `FsOp` lives on the coroutine's stack

The per-op state record carries a coroutine pointer and a result
slot. `user_data` on the SQE points to it; the CQE drainer writes
the result and unparks the coroutine. The record must have a
stable address for the kernel's in-flight window.

Stack-allocated. Coroutine stacks are pinned for the coroutine's
lifetime (CLAUDE.md invariant 5); the address is stable. No pool,
no allocation on the hot path. The coroutine waits for its own CQE
before returning, so the frame is unconditionally live for the
entire in-flight window.

```zig
const FsOp = struct {
    coro: *Coroutine,
    result: FsResult,
};

pub fn fsRead(fd, buf, offset) FsResult {
    var op: FsOp = .{ .coro = current.require(), .result = undefined };
    p.fs_ring.prepRead(fd, buf, offset, @intFromPtr(&op));
    runtime.park();
    return op.result;
}
```

### Phase 3 invariant — cancellation must keep the frame alive

`IORING_OP_ASYNC_CANCEL` is itself asynchronous. The kernel will
eventually write to `user_data` regardless. **A coroutine that
cancels an in-flight fs op cannot unwind or release its stack
until the original CQE (or the cancel CQE) is fully reaped — or
the kernel performs a silent memory write into a dead or
reassigned stack frame.**

The cancel-and-drain semantics from §5 of the parent memo handle
this naturally — the coroutine waits for both CQEs before
returning, keeping the frame alive. The Phase 3 implementation
must not weaken this. (Documented here so the Phase 3 reviewer
catches any "fast-path" that violates it.)

## 2. Wake-up architecture — three layers + the "Signal-Only-If-Idle" rule

A parked coroutine's worker may be in any of three states when
its CQE becomes ready. Each is handled by a different layer:

**Layer 1 — worker is actively running other coroutines.** Add
`peekBatch` on its own P's ring as one more work source in the
find-work loop, between "check mailbox" and "try to steal". Peek
is a userspace memory read when no CQEs are ready — cheap. CQEs
that arrived since the last iteration get drained on the next
one. Locality is perfect: the worker drains its own ring, the
resumed coroutine lands on the same P's local queue.

**Layer 2 — worker is the designated reactor poller** (blocked
in `epoll_wait`). Each P's fs ring gets its own eventfd via
`IORING_REGISTER_EVENTFD`; every eventfd joins the shared epoll
at `Runtime.init`. When any P's CQE fires, the kernel writes to
that ring's eventfd, epoll wakes the poller. (Glommio's "ring-
link" pattern, `glommio/src/sys/uring.rs:1226-1248`.)

**Layer 3 — worker is parked in its M's parker** (idle, no
reactor responsibilities). Already wakes via the existing
`unparkOne` path whenever a coroutine is dispatched onto a P's
queue. The cross-worker dispatch path (Layer 2's drainer pushing
to another P's mailbox) triggers this naturally.

### The "Signal-Only-If-Idle" invariant

The naive Layer 2 implementation has a hidden performance trap.
Under heavy disk I/O, worker B's eventfd fires on *every* CQE.
If worker A is the reactor poller, it wakes from `epoll_wait` on
every single completion — even though worker B is busy draining
its own ring via Layer 1's peek loop. Worker A then signals B
uselessly, the kernel does a cross-CPU IPI, and the notification
storm destroys performance.

**Fix:** the reactor poller checks the owner's run-state before
signalling.

```
[Kernel CQE Populated] ──► fires eventfd ──► [Reactor poller wakes]
                                                       │
                                          read() eventfd (8-byte u64, MANDATORY)
                                                       │
                                          is owner worker Parked?
                                            ╱                   ╲
                                         (Yes)                 (No)
                                          ╱                       ╲
                       [parker.unpark(owner)]      [Drop — owner will peek on its
                                                    next find-work iteration]
```

Cost when owner is busy: one userspace state-load + one branch.
Cost when owner is idle: same as today's unpark (one futex/ulock
wake). Critical correctness: the **kernel eventfd counter MUST
be drained** (`read(fd, &u64_buf, 8)`) on every wake, otherwise
the level-triggered epoll re-fires indefinitely. Edge-triggered
isn't an option for shared eventfds.

### Worker run-state — reuse `parked_workers`, no new atomic

The runtime already has `Runtime.parked_workers: u64` (bitmap,
one bit per M; cache-line padded, every park/unpark already
touches it). For the Signal-Only-If-Idle check the poller needs
exactly one question: "is the owner currently parked?" Answer:
`(parked_workers.load(.acquire) >> owner_id) & 1`. No new atomic
required — the bit on this bitmap is the authoritative "parked"
signal, and the existing parker integration already publishes it
with the right ordering.

The two-state distinction the original draft proposed —
`Running` vs `Searching` — turns out to be unnecessary for this
optimisation. Both states mean "the owner will catch the CQE on
its own peek loop without our help"; we only care about the
third state (`Parked`), and the bitmap tells us that directly.

#### The load-bearing intent-to-park invariant

For this to be race-free, the owner worker MUST set its bit
*before* its final defensive work-check, not after. Otherwise:

```
Worker B (Owner)                       Worker A (Reactor Poller)
----------------                       -------------------------
1. peekBatch() -> empty
                                       2. CQE completes in kernel
                                       3. Eventfd fires, A wakes
                                       4. A reads parked_workers — B's bit == 0
                                       5. A drops the signal
6. Sets B's bit in parked_workers
7. parker.park() → ASLEEP, with a CQE pending
```

The fix is "intent-to-park": set the bit, THEN re-check work
sources, THEN park if still nothing. If a check finds work,
return early and `defer` clears the bit. **The current
`parkWorker` (`src/runtime.zig:1114-1146`) already implements
exactly this sequence** — `markParked` at line 1115 with
`defer unmarkParked`, defensive checks at 1125-1129, spin loop
with more checks at 1135-1143, `parker.park()` only at 1145.
Phase 2C.1 just needs to extend the existing check list to
include "fs_ring CQE ready" (a single `peekBatch` call against
the current P's ring).

#### The poller side (Worker A)

1. Wake from `epoll_wait` because an eventfd fired.
2. **Drain the eventfd** — `read(eventfd, &u64_buf, 8)`, exactly
   8 bytes. Short reads leave the kernel counter non-zero and
   level-triggered epoll re-fires forever.
3. **Load the bitmap** — `parked_workers.load(.acquire)`. The
   acquire pairs with the owner's `.release` (or stronger) store
   in `markParked`.
4. **If owner's bit is set**: `m.parker.unpark()`. The owner
   wakes, runs its defensive recheck (which now drains the
   ring), clears its own bit via `defer unmarkParked`, returns
   to the dispatch loop, and the just-resumed coroutines run.
5. **If owner's bit is clear**: drop. The owner is in find-work
   right now, or between fetchSub-num_searching and markParked
   (the `need_spinning` latch in `wakeOneParked` already closes
   that window — same mechanism, generalises to fs ring CQEs at
   no extra cost).

#### The peek must drain + dispatch, not just check

A subtle but critical point: `peekBatch` returns raw CQEs.
Checking only whether *some* CQE exists doesn't resume any
coroutines. The find-work and pre-park integration both need
to actually **drain the CQEs and push the resumed coroutines
into `m.p.local`** so the subsequent `local.isEmpty()` check
sees them.

The shape Phase 2C.1 lands as a helper on Runtime (or in a
small fs_dispatch module):

```zig
/// Drain ready CQEs on this P's fs ring, resolve each one to
/// its parked coroutine, write the result into the FsOp, and
/// push the coroutine onto the local queue. Returns the count
/// drained. Safe to call from the owner worker only (single-
/// reader per ring; cross-worker drains are forbidden by the
/// owner-only-drain decision, §4).
fn drainFsRingInto(rt: *Runtime, p: *P) usize {
    const rings = rt.fs_rings orelse return 0;
    var cqes: [16]linux.io_uring_cqe = undefined;
    const n = rings[p.id].peekBatch(&cqes) catch return 0;
    for (cqes[0..n]) |cqe| {
        const op: *fs_ring.FsOp = @ptrFromInt(cqe.user_data);
        op.result = .{ .value = cqe.res, .err = if (cqe.res < 0) @intCast(-cqe.res) else 0 };
        p.pushQueue(op.coro);   // resumed coroutine joins local WSQ
    }
    return n;
}
```

This helper gets called from two places:

1. **`tryFindAndDispatch`** — once per find-work iteration, after
   the local/lifo check, before steal attempts. If CQEs drained
   into the local queue, the next dispatch picks them up
   immediately. (Layer 1 of the wake-up architecture.)
2. **`parkWorker`** — both in the post-`markParked` defensive
   check block AND in the `SPIN_BEFORE_PARK` spin loop. After
   the drain, the existing `if (!m.p.local.isEmpty()) return;`
   check naturally catches the resumed coroutines and aborts
   the park.

The drain is cheap — userspace memory reads against the CQ
ring — so doing it twice in parkWorker (once before spin, once
per spin iteration) is fine. The eventfd fast path becomes a
fallback for when the owner is genuinely asleep, never a
critical path under sustained I/O.

## 3. Per-P ring submission is single-writer

Each P's ring has exactly one OS thread bound to it (the M).
Coroutines on that P share the worker by cooperative scheduling
— no concurrent submission within a P. So `get_sqe` / `submit`
are single-writer per P, **no lock needed**.

The cross-worker case is *reaping* — the reactor poller (Layer 2)
may be a different worker draining your ring's eventfd. But under
**owner-only drain** (next section), the poller never calls
`copy_cqes` on a foreign ring; it only signals the owner. So
`copy_cqes` is also single-reader per P. No lock anywhere on the
ring's hot path.

The only contention point is eventfd registration with the shared
epoll, done once at `Runtime.init` before any coroutine runs. No
hot-path contention.

## 4. Owner-only drain

When the reactor poller sees worker B's eventfd fire, it does NOT
drain B's ring. It signals B (Signal-Only-If-Idle) and B drains
its own ring on its next find-work iteration.

### Why not "anyone-drains with CAS"

- **Cache-line bouncing.** If worker A drains worker B's CQ, the
  kernel-shared ring pointers and the CQE structures bounce to
  A's L1. B then drains its next batch and bounces them back.
  Defeats io_uring's whole "stay on one core" win.
- **Double work.** Once A drains the CQE, it discovers the
  coroutine belongs to B and has to push it onto B's mailbox
  anyway. We're not saving a thread hop — we're just moving the
  synchronization point from the parker to the ring buffer, while
  thrashing both cores' caches.
- **Anyone-drains is the wrong shape for this workload.** It only
  makes sense in models where the drainer is also the dispatcher
  (Glommio: per-thread executor, no cross-worker hop possible).
  Volt's work-stealing scheduler can dispatch to any worker, so
  the drainer→dispatcher hop is unavoidable. Better to let the
  owner drain so the drainer→dispatcher path is local.

## 5. Concrete edit list (in landing order)

Each item below is a small commit on its own — same sub-phase
discipline as 2A/2B.

1. **`src/fs_ring.zig`**: add `eventfd: i32` field,
   `registerEventFdWithEpoll(epfd: i32) !void` method (calls
   `IORING_REGISTER_EVENTFD` + `epoll_ctl(EPOLL_CTL_ADD)`), and
   `drainEventFd(self)` helper (reads the 8-byte u64 to clear
   the kernel counter — invariant from §2 above).
2. **`src/reactor_epoll.zig`**: extend the existing
   `poll`/`interrupt_fd` machinery to recognise the per-P fs
   ring eventfds. When one fires: read+clear it, check the
   owner P's worker state, and either signal-or-drop per §2.
3. **`src/runtime.zig`**: after `tryInitFsRings` (already in
   Phase 2B), register each ring's eventfd with the shared
   reactor's epoll. **No new worker state atomic** — the
   existing `parked_workers` bitmap is the authoritative
   "parked" signal (one bit per M). The Signal-Only-If-Idle
   check reads bit N with `.acquire`. Add the
   `drainFsRingInto(rt, p)` helper (drain CQEs → resolve FsOp
   → unpark resumed coroutines). Wire calls at:
   (a) `tryFindAndDispatch` after the local/lifo check;
   (b) `parkWorker` *after* `markParked` and *before* the
   defensive `local.isEmpty()` check (so the drain populates
   the queue first);
   (c) the same again at the top of each `SPIN_BEFORE_PARK`
   spin iteration (catches CQEs arriving during the brief
   spin window without paying the eventfd round-trip).
   `defer unmarkParked` continues to handle the abort-on-
   late-work cleanup.
   **Add `Runtime.fs_in_flight: std.atomic.Value(u32)`** —
   bumped by each `fsRingX` after submit, decremented by
   `drainFsRingInto` per CQE. Included in the
   reactor-poll-claim and parking conditions so workers
   actually enter `epoll_wait` when fs ops are in flight
   (see decision log entry below for why `reactor.pending`
   can't be reused).
4. **Backends' `fs*` methods** (`reactor_io_uring.zig`,
   `reactor_epoll.zig`): check `current.require()` → P →
   `rt.fs_rings`; if non-null, do the SQE+park flow; if null,
   fall back to the existing spawnBlocking proxy. The
   spawnBlocking path stays exactly as Phase 1 implemented it.
5. **`FsOp` struct** in `fs_ring.zig`:
   `{ coro: *Coroutine, result: FsResult }`. Allocated on the
   coroutine's stack at the `fsRead` call site.
6. **Worker dispatch loop**: add `peekBatch` on current P's
   ring between "mailbox check" and "steal" — Layer 1 of §2.
   Resumed coroutines push onto the local queue (best
   locality).

## 6. Implementation gotchas to watch

- **8-byte eventfd reads.** `read(eventfd_fd, &buf, 8)` — not
  less. A short read leaves the kernel counter non-zero, and
  level-triggered epoll will re-fire indefinitely. (Item flagged
  by the reviewer on §2.)
- **Find-work priority order.** `local → lifo_slot → fs ring
  peek → mailbox → steal → reactor poll → park`. fs CQEs go
  AFTER lifo/local (those are warmest) but BEFORE stealing
  (local completions have better data locality than stolen
  work). Confirmed by reviewer.
- **The peek must drain + dispatch, not just check.**
  `ring.peekBatch` returns raw CQEs; checking "is there a CQE?"
  doesn't actually resume any coroutines. Phase 2C.1's helper
  drains CQEs, resolves each via `user_data → *FsOp`, writes
  the result, and pushes the resumed coroutine onto the local
  queue — so subsequent `local.isEmpty()` checks see them. If
  this step is skipped or done in the wrong order, parkWorker
  proceeds to `m.parker.park()` despite having a CQE pending,
  and the worker only wakes when the eventfd fallback fires
  (which still works, but costs an unnecessary syscall round
  trip). Reviewer specifically called out this gotcha.
- **Submitter vs reaper on `has_pending`.** Owner-only drain
  fixes this: the worker that submits is the worker that
  flushes. `has_pending` stays non-atomic (single-thread
  access). The reactor poller never touches `has_pending`.
- **Cross-P dispatch under work-stealing.** When the owner
  drains its ring and the resumed coroutine then yields and
  gets stolen by another worker — fine, that's the normal
  steal path. The buffer (on the coroutine's stack) travels
  with the coroutine; the SQE/CQE have already completed.
  Nothing special needed.

## 7. Decision log

| Date | Decision | Rationale |
|---|---|---|
| 2026-05-27 | `FsOp` on coroutine stack, not pooled. | Pinned stack VA + stackful invariant 5 + zero-alloc hot path. |
| 2026-05-27 | Three-layer wake-up (find-work peek + eventfd-into-epoll + parker). | Each handles a distinct worker state without paying for the others when they're absent. |
| 2026-05-27 | Single-writer per P, no submission lock. | M:P binding + cooperative coroutines = no concurrent submitters. |
| 2026-05-27 | Owner-only drain. | Avoids cache-line bouncing and double work that "anyone-drains" would cost on a work-stealing scheduler. |
| 2026-05-27 | "Signal-Only-If-Idle" optimisation on the eventfd wake-up. | Prevents the notification storm where a busy worker's per-CQE eventfd fires the poller, which would otherwise uselessly signal the (already-running) owner. Cost: one atomic load + one branch on the poller path. |
| 2026-05-27 | Reuse existing `parked_workers` bitmap; no new per-M state atomic. | The bit is the authoritative "parked" signal; `parkWorker:1114-1146` already implements intent-to-park (set bit → defensive checks → spin → park; `defer unmarkParked` on any return). Phase 2C.1 just extends the defensive check list. Saves an atomic per M and reuses existing ordering. |
| 2026-05-27 | Intent-to-park sequence is the load-bearing invariant. | If the bit goes up *after* the final work check, a CQE arriving between the check and the bit-set goes unobserved and the poller drops its signal because the bit is still 0. The current `parkWorker` already gets this right; preserve the ordering when extending the check list. |
| 2026-05-27 | The peek must drain + dispatch into `p.local`, not just check. | Checking readiness alone leaves resumed coroutines stranded — the subsequent `local.isEmpty()` returns true and the worker parks anyway. The `drainFsRingInto(rt, p)` helper does the drain + per-CQE FsOp resolve + push in one call; called from `tryFindAndDispatch`, from `parkWorker`'s post-`markParked` check, and from each iteration of the spin loop. Catches CQEs purely in userspace; eventfd fallback only kicks in when the owner is genuinely parked. |
| 2026-05-27 | Drop `IORING_SETUP_DEFER_TASKRUN` from `SETUP_TIERS`. | DEFER_TASKRUN defers completion work (including making CQEs visible AND writing to the registered eventfd) until the submitter calls `io_uring_enter(GETEVENTS)`. Our `flush()` calls `submit()` with `wait_nr=0` (no GETEVENTS) and the worker then parks; nothing else calls enter-with-GETEVENTS until we manually do, so completions never become visible. DEFER_TASKRUN benefits workloads that BATCH many SQEs between drains; ours (submit one, park, drain on wake) is the opposite shape. Kept `SINGLE_ISSUER \| SUBMIT_ALL` which provide their wins without the same constraint. |
| 2026-05-27 | New `Runtime.fs_in_flight` atomic counter. | Eventfds don't bump `reactor.pending` (they're wake signals, not registered I/O). Without a separate signal, `tryFindAndDispatch`'s reactor-poll-claim and `parkWorker`'s defensive check both see "nothing pending" and the worker parks — eventfd fires with nobody in `epoll_wait` to receive it, op hangs forever. The counter is incremented by `fsRingRead/Write/Fsync/Fdatasync` after submit, decremented by `drainFsRingInto` per CQE. Cache-line-aligned; one bump + one decrement per fs op. |

## 8. Risks for Phase 2C reviewer to verify

1. **Eventfd registration ordering.** Register with `IORING_REGISTER_EVENTFD` *before* adding the eventfd to epoll, so a CQE that lands during registration doesn't get lost. (`io_uring_register` is synchronous; once it returns, the eventfd will fire on every subsequent CQE.)
2. **Owner-only drain assumes worker stays bound to P.** M:P is 1:1 today (CLAUDE.md mentions this is Phase 1). If a later phase introduces M-detach, the "owner drains" model needs revisiting — the eventfd-registered M might not be the same M that ends up running the owner's coroutines.

5. **Intent-to-park ordering** (formalised above). When extending `parkWorker`'s defensive check list with the fs ring peek, the peek MUST sit between `markParked` and `m.parker.park()` — not in `tryFindAndDispatch` only. A peek that happens only before `markParked` exposes the missed-wake race documented in §2. The existing structure of `parkWorker` (check list runs *after* `markParked`) makes this the natural place; the reviewer should still verify the new entry sits inside the `markParked` ... `parker.park()` window.
3. **`has_pending` lifetime.** Set true by `prepX`, cleared by `flush`. Owner-only access. If a later phase introduces a "submit-from-shutdown" path on Runtime.deinit, that path must run on each owner P (not a foreign thread).
4. **Coroutine-stack lifetime in the cancel-CQE window** (Phase 3 concern, captured in §1 above).
