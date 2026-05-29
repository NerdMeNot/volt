# Phase 3 — in-flight cancellation design

**Status:** Draft, 2026-05-28
**Parent:** [docs/internals/async-fs-io.md §5](async-fs-io.md)
**Related:** [docs/internals/phase-2c-design.md](phase-2c-design.md)
**Driver:** Real cancel-during-in-flight for fs ops via
`IORING_OP_ASYNC_CANCEL`. Same public API; only the semantics
improve. Memory-safe by construction.

Phase 2 shipped the io_uring fs path with "cancel-checked at
boundaries only" semantics (caller sees `error.Cancelled` on
the *next* fs op, not the parked one). Phase 3 closes that gap
on the io_uring path. spawnBlocking fallback stays as-is —
kernel syscalls in pool threads aren't safely interruptible
without machinery (e.g. `pthread_cancel`) whose pitfalls outweigh
the benefit.

## 1. The four open questions (Phase 3A sign-off)

### Q1 — Race semantics: cancel-fires-during-completion

If the original CQE lands and the cancel fires at nearly the
same instant, which wins?

**Decision: completion wins.** If the op already produced a
result, return it. The cancel is observable on the *next*
cancel-aware op the caller does.

Rationale: matches the existing cancel-aware primitives in
Volt (`Mutex.lockCancel` returns the acquired lock if it landed
before fire; `PollDesc.waitCancel` only translates to
`Cancelled` if `c.isFired()` was observable post-wait). The
race window is vanishingly small in practice. The opposite
choice ("cancel wins, throw away the result") leaves the caller
with a successful side effect (bytes written) but no return —
strictly worse.

### Q2 — Scope: does cancel extend into the spawnBlocking path?

**Decision: no.** Phase 3 is the io_uring-path cancel
improvement. The spawnBlocking fallback retains the current
"check at submit, wait for thread to complete" semantics.

Rationale: pool threads execute blocking syscalls
(`read(2)`/`write(2)`/etc) that can only be interrupted via
`pthread_cancel` + cleanup handlers, which is a well-known
pitfall garden (cancel during syscall undefined unless
explicitly handled; arbitrary cleanup cleanup ordering;
async-signal-safety constraints). The asymmetry "io_uring path
cancels promptly, spawnBlocking path waits to next op" is
honest, documentable, and aligned with the rest of Volt — the
spawnBlocking path is the fallback when io_uring isn't
available; users who need prompt cancel should ensure io_uring
is available.

Document the asymmetry in `fs/file.zig`'s section comment (already
done in commit `4d23469` for the Phase 2 wrap; will revise for
Phase 3 specificity).

### Q3 — FsOp state machine

**Four-state machine on `FsOp.state: std.atomic.Value(u8)`:**

```
       ┌──────────┐
       │ Pending  │  initial
       └────┬─────┘
            │
        ┌───┴───────────────┐
        │                   │
 drainer CAS         cancel-deliver CAS
 Pending→Completed   Pending→Cancelling
        │                   │
        ▼                   ▼
  ┌──────────┐        ┌─────────────┐
  │Completed │        │ Cancelling  │
  └──────────┘        └──────┬──────┘
   (return                   │
    op.result)         drainer CAS
                       Cancelling→CancelledAndDrained
                              │
                              ▼
                     ┌────────────────────┐
                     │ CancelledAndDrained│
                     └────────────────────┘
                      (return error.Cancelled)
```

Wake-on-state:

| Coro wakes from park, observes state | Action |
|---|---|
| `Completed` | Return `op.result` |
| `Cancelling` | Submit `IORING_OP_ASYNC_CANCEL`, re-park, await drainer's transition to `CancelledAndDrained` |
| `CancelledAndDrained` | Return `error.Cancelled` |
| `Pending` | (unreachable — drainer or cancel must have CAS'd before unparking) |

The CASes from `Pending` are mutually exclusive — at most one of
"drainer wrote Completed" or "cancel-deliver wrote Cancelling"
wins. The losing thread observes the new state and no-ops.

### Q4 — Cancel SQE tracking

**Decision: low-bit-tagged sentinel user_data, no second FsOp.**

`IORING_OP_ASYNC_CANCEL` itself fires a CQE (the cancel-ack).
We don't need an FsOp for it — the coroutine isn't waiting on
the cancel ack; it's waiting on the original op's CQE-with-
ECANCELED, which is what guarantees the kernel has stopped
touching the buffer.

Encoding: the cancel SQE's `user_data = original_user_data | 0x1`.
FsOp pointers are ≥ 8-byte aligned (stack-allocated structs);
bit 0 is naturally free. Drainer fast-path:

```zig
if ((cqe.user_data & 0x1) != 0) {
    // Cancel-ack — kernel confirms cancellation was initiated.
    // Discard; decrement fs_in_flight; don't touch any FsOp.
    _ = rt.fs_in_flight.fetchSub(1, .acq_rel);
    continue;
}
// Original op CQE — normal path, look up FsOp.
const op: *FsOp = @ptrFromInt(cqe.user_data);
```

Why not store the cancel SQE in a second FsOp on the stack?
- Avoids a second stack alloc per cancel.
- The cancel ack carries no useful information for the coroutine
  — `-ENOENT` means the op already completed (will arrive
  normally with the result); `0` means the cancellation was
  initiated (original op will arrive with `-ECANCELED`). Either
  way the coroutine is waiting on the original CQE.
- One fewer state machine to reason about.

## 2. End-to-end flows

### 2.1 Normal completion (no cancel)

```
coro: fsRingReadCancel(fd, buf, offset, &cancel)
  c.registerCallback(&w, &op, fsCancelDeliver)
  ring.prepRead(fd, buf, offset, &op as u64)
  ring.flush()
  fs_in_flight += 1
  park()

[kernel processes SQE, posts CQE]

drainer (on owner):
  drainFsRingInto sees CQE with user_data = &op
  op.state CAS Pending → Completed  (success)
  op.result = { .value = bytes, .err = 0 }
  unpark(op.coro)
  fs_in_flight -= 1

coro wakes:
  c.deregisterRemoved(&w)  // returned true: callback never ran
  load op.state → Completed
  return op.result
```

### 2.2 Cancel fires before completion

```
coro: fsRingReadCancel(fd, buf, offset, &cancel)
  c.registerCallback(&w, &op, fsCancelDeliver)
  ring.prepRead(fd, buf, offset, &op as u64)
  ring.flush()
  fs_in_flight += 1
  park()

[other coro: cancel.fire()]
  fsCancelDeliver(&op):
    op.state CAS Pending → Cancelling  (success)
    unpark(op.coro)
    ctx.completed.store(true, .release)

coro wakes (from cancel-deliver):
  load op.state → Cancelling
  ring.prepCancel(&op as u64, (&op as u64) | 1)  // cancel SQE
  ring.flush()
  fs_in_flight += 1
  park()  // wait for original op CQE

[kernel processes ASYNC_CANCEL, posts cancel-ack CQE]
drainer: user_data & 1 → fast path, fs_in_flight -= 1, no wake

[kernel finishes cancelling original op, posts CQE with -ECANCELED]
drainer:
  user_data has bit 0 clear → original op CQE
  op.state CAS Cancelling → CancelledAndDrained  (success)
  op.result = { .value = -1, .err = ECANCELED }
  unpark(op.coro)
  fs_in_flight -= 1

coro wakes (second time):
  load op.state → CancelledAndDrained
  c.deregisterRemoved(&w)  // already-delivered; callback completed
  return error.Cancelled
```

### 2.3 Race: cancel fires after CQE arrives

```
coro: ... park() ...

[kernel posts original op CQE]
drainer:
  op.state CAS Pending → Completed  (success)
  op.result = { .value = bytes, .err = 0 }
  unpark(op.coro)

[other coro: cancel.fire() — concurrently with drainer above]
  fsCancelDeliver(&op):
    op.state CAS Pending → Cancelling  (FAILS, state is Completed)
    No-op — op is already done.

coro wakes:
  load op.state → Completed
  c.deregisterRemoved(&w)  // may busy-wait briefly for completed flag
  return op.result  (cancel "missed" by losing the CAS race)
```

### 2.4 Race: original CQE arrives while coro is mid-cancel

```
coro: ... cancel fired, state=Cancelling, submitting ASYNC_CANCEL ...

[concurrently: kernel was already completing the original op;
 posts CQE before ASYNC_CANCEL can take effect]
drainer:
  op.state CAS Cancelling → CancelledAndDrained  (success)
  op.result = { .value = bytes, .err = 0 }  // SUCCESS, not -ECANCELED
  unpark(op.coro)

[kernel processes ASYNC_CANCEL, finds nothing to cancel, posts -ENOENT]
drainer: user_data & 1 → fast path, fs_in_flight -= 1, no wake

coro wakes (from drainer):
  load op.state → CancelledAndDrained
  return error.Cancelled
```

The op succeeded but we entered the cancel path — return
`Cancelled` (the coro committed to cancellation when it
observed `Cancelling` and submitted ASYNC_CANCEL; reverting that
decision based on a late successful result would be confusing).
The caller can re-issue if needed. Buffer is safe (kernel done
with it before CQE arrived).

## 3. Implementation invariants

1. **FsOp state CAS is the synchronization point.** The CASes
   from `Pending` are mutually exclusive. The CAS from
   `Cancelling → CancelledAndDrained` is single-writer (only
   the owner's drainer touches it). No locks needed.
2. **`unpark` is idempotent w.r.t. state.** A double-unpark
   (cancel-deliver + drainer-after-completion) is safe because
   `runtime.unpark`'s CAS handles `RUNNING → NOTIFIED` and the
   dispatcher's `.park` branch re-queues. Standard Volt pattern.
3. **`fs_in_flight` accounting balances per CQE.** Each
   submitted SQE bumps; each drained CQE decrements. Cancel
   path: 2 bumps (original + cancel SQE), 2 decrements (op CQE
   + cancel-ack CQE).
4. **Buffer pinned until original CQE arrives.** The kernel
   may still be touching the buffer at the moment cancel-ack
   fires. Coroutine must wait for the ORIGINAL op CQE before
   returning. Buffer lives on coro stack; coro stack is alive
   until coro returns; coro doesn't return until state is
   `Completed` or `CancelledAndDrained`. Memory-safe.
5. **The cancel deliver callback runs from arbitrary thread.**
   `cancel.fire()` is called by some other coroutine on some
   worker. The callback only does: CAS state, unpark, set
   completed flag. No SQE submission from the callback (would
   violate single-writer-per-P). The owner coroutine submits
   the ASYNC_CANCEL SQE itself when it wakes.
6. **`deregisterRemoved` race resolution.** Standard pattern
   from PollDesc.waitCancel (`poll_desc.zig:457-461`): if the
   callback was already in flight when we deregister, busy-
   wait on its `completed` flag so the ctx (on our stack)
   stays valid until the callback finishes touching it.

## 4. Edge cases addressed

- **Cancel fires before register.** `c.registerCallback`
  returns true. fsRingReadCancel returns `error.Cancelled`
  immediately (no SQE submitted; no fs_in_flight bump).
- **Cancel fires between register and park.** Standard
  `park_state` NOTIFIED path: park() finds NOTIFIED, immediately
  re-dispatches. Coro runs, sees `Cancelling`, proceeds to
  cancel path.
- **Double cancel.** `cancel.fire()` is idempotent. Second
  fire calls the callback again with the same FsOp; CAS
  `Pending → Cancelling` fails (state is already Cancelling
  or beyond) — no-op.
- **Cancel on a coroutine that's not even parked yet.**
  Pre-register fast path catches it (registerCallback returns
  true if already fired). If cancel.fire() runs after register
  but before submit: the callback CASes state to Cancelling and
  unparks — but the coro isn't parked yet, so unpark sets
  NOTIFIED. The coro then submits the op (it's still inside
  fsRingReadCancel's prep+flush), parks, immediately wakes
  (NOTIFIED), sees Cancelling, submits ASYNC_CANCEL, etc.
  Suboptimal (we did the submit anyway) but correct.
  Optimization: check `c.isFired()` between register and prep;
  if fired, deregister and bail. Worth doing.
- **Cancel of an op whose ring rejected SubmissionQueueFull.**
  fsRingReadCancel bails to spawnBlocking (returns null), which
  the backend handles. spawnBlocking-path cancel is "check
  before submit" — same semantics as today. No regression.

## 5. Concrete edit list

In landing order (mirrors Phase 2C sub-phases):

1. **`src/fs_ring.zig`**: extend `FsOp` with
   `state: std.atomic.Value(u8)`. Add four state constants
   (`STATE_PENDING/_COMPLETED/_CANCELLING/_CANCELLED_AND_DRAINED`).
   Add `prepCancel(target_user_data, user_data)` method that
   submits `IORING_OP_ASYNC_CANCEL`. Update existing tests if
   they construct FsOps directly.

2. **`src/runtime.zig` drainFsRingInto refactor.** Recognise
   bit-0-tagged user_data as cancel-ack (decrement fs_in_flight,
   no FsOp lookup, no unpark). For the normal path, CAS state
   from `Pending → Completed` OR from `Cancelling →
   CancelledAndDrained`. Write result. Unpark. Decrement
   fs_in_flight per CQE.

3. **Extend existing `fsRingRead/Write/Fsync/Fdatasync` helpers
   with an optional `cancel: ?*Cancel` final parameter.** No
   new helper names. When `cancel == null` the function behaves
   exactly as today (Phase 2 callers pass null, semantics
   unchanged). When non-null the function: registers a cancel
   callback before submit, branches on `op.state` post-wake,
   and either returns the result (Completed), submits
   ASYNC_CANCEL + re-parks (Cancelling), or returns
   `error.Cancelled` encoded as `FsResult{.value=-1, .err=ECANCELED}`
   (CancelledAndDrained). Caller in `fs/file.zig` maps the
   ECANCELED errno to `error.Cancelled` the same way it maps
   other errnos.

4. **`fsCancelDeliver` callback.** CAS `op.state` Pending →
   Cancelling. On success, unpark. Always set
   `ctx.completed = true`. Lives next to the helpers in
   `runtime.zig`.

5. **Extend existing `Reactor.fsRead/Write/Fsync/Fdatasync`
   methods with the same optional `cancel: ?*Cancel` parameter.**
   No new Reactor methods. Both backends (`reactor_io_uring.zig`,
   `reactor_epoll.zig`) forward the cancel to the helper. The
   spawnBlocking-proxy fallback path checks `c.isFired()` before
   submitting (when cancel is non-null) — same semantics as
   today, just plumbed through one signature. Tagged-union
   dispatch in `reactor_linux.zig` forwards the new parameter.

6. **`fs/file.zig`** routing: the `blockingRead`/etc helpers
   gain `blockingReadCancel`/etc counterparts that pass the
   cancel through to `Reactor.fsRead(..., &cancel)`. The
   public `File.readCancel(buf, &c)` etc methods stop being
   "check c.isFired then call self.read"; they call the new
   cancel-aware helpers directly. Public signatures unchanged.
   On the spawnBlocking fallback path the cancel still degrades
   to "check at submit only" — same semantics as today.

7. **Tests** (Phase 3E in the parent plan):
   - Cancel-during-slow-read returns `error.Cancelled`.
   - Cancel-after-completion returns the result.
   - Double-cancel is a no-op.
   - Cancel of a never-parked coro (race) is honored.
   - Memory safety: a test that cancels an op against a slow
     filesystem and asserts the buffer isn't touched after
     return (write a sentinel; check it's preserved).

## 6. Risks the reviewer should flag

1. **The `Cancelling → CancelledAndDrained` transition is the
   only state CAS done by the drainer.** If the order is wrong
   (drainer arrives but sees `Cancelling`, somehow doesn't
   transition), the coroutine deadlocks. Test for this
   explicitly: cancel a slow op, observe both CQEs arriving,
   assert state is `CancelledAndDrained` post-wake.

2. **Re-parking after ASYNC_CANCEL is a second park cycle.**
   `parkWorker`'s defensive checks must still allow this — they
   do (the `fs_in_flight > 0` check stays true because we
   bumped on the cancel SQE). No special handling needed.

3. **The cancel SQE could fail at prep time (SQ full).** If
   `prepCancel` returns SubmissionQueueFull, we have an awkward
   situation: state is `Cancelling`, but no cancel SQE went out.
   The original CQE will eventually arrive normally, drainer
   transitions `Cancelling → CancelledAndDrained`, coro
   returns `error.Cancelled`. So we still surface the cancel,
   just without nudging the kernel to bail earlier. Acceptable.

4. **The cancel-deliver callback runs concurrently with the
   drainer.** Both can CAS `op.state`. The state machine is
   designed so the CASes don't overlap (drainer's `Pending →
   Completed` and callback's `Pending → Cancelling` are
   mutually exclusive from the same source state). The
   `Cancelling → CancelledAndDrained` transition is single-
   writer (drainer only). No data race.

5. **`deregisterRemoved` busy-wait under contention.** The
   PollDesc pattern busy-waits on `ctx.completed`. Bounded by
   one unpark + memory write. Same exposure as today; no
   regression.

## 7. Decision log

| Date | Decision | Rationale |
|---|---|---|
| 2026-05-28 | Race semantics: completion wins. | Matches Mutex.lockCancel / PollDesc.waitCancel; avoids "we did the I/O but threw away the result." |
| 2026-05-28 | scope: io_uring path only; spawnBlocking unchanged. | pthread_cancel pitfalls; asymmetry is documentable; explicit Phase 5 work could revisit. |
| 2026-05-28 | Four-state FsOp machine (Pending / Completed / Cancelling / CancelledAndDrained). | Mutually-exclusive CASes from Pending; single-writer Cancelling→CancelledAndDrained; coro on wake branches on state. |
| 2026-05-28 | Cancel SQE has tagged sentinel user_data (bit 0 set on original FsOp ptr). | Avoids second stack-allocated FsOp; drainer fast-paths on bit 0. |
| 2026-05-28 | Owner submits ASYNC_CANCEL, not the callback. | Preserves single-writer-per-P invariant. Callback only CASes state + unparks. |
| 2026-05-28 | Wait for ORIGINAL op CQE (not cancel-ack) before return. | Kernel guarantees buffer is free when original CQE fires; cancel-ack only signals "cancellation initiated." Memory safety. |
| 2026-05-28 | **Revised**: extend existing `fsRingRead/Write/Fsync/Fdatasync` + `Reactor.fsRead/etc` with optional `cancel: ?*Cancel` parameter. No new `*Cancel`-suffixed helper names. Cancelled result encoded as `FsResult{.value=-1, .err=ECANCELED}` — the kernel's natural CQE shape for cancelled ops. | The user pushed back on the original draft's `fsRingReadCancel/fsRingWriteCancel/fsRingFsyncCancel/fsRingFdatasyncCancel` proliferation as non-ergonomic. Single signature with optional cancel is cleaner: same number of internal symbols pre- and post-Phase-3, Phase 2 callers continue to pass `null` unchanged, no public API churn whatsoever (`File.read` / `File.readCancel` etc. signatures all unchanged). Implementation branches internally on `cancel == null`. |
| 2026-05-29 | **Drop `IORING_SETUP_SINGLE_ISSUER` from Phase 2A's `SETUP_TIERS`.** | Phase 3's cancel-and-drain path submits ASYNC_CANCEL after the coroutine resumes from `park()`. By that time, work-stealing may have migrated the coroutine to a different worker, and SINGLE_ISSUER causes the kernel to reject the cross-thread `io_uring_enter` with `EEXIST` (surfaced as `error.InvalidThread`). Discovered when the Phase 3D full-suite Linux test SEGV'd at the Phase 3C test (the in-isolation 3C test had passed because the scheduler happened to not migrate). The userspace SQ ring itself is safe under sequential-with-handoff submission (park's swap-back acts as release, unpark's CAS as acquire); only the kernel's SINGLE_ISSUER flag rejects it. Dropped to fix correctness; the SUBMIT_ALL flag (the strict perf win) is kept. |

## 8. Phase 3 sub-phases (mirrors Phase 2 cadence)

| Sub-phase | Scope | LOC est. |
|---|---|---|
| **3A** | This memo + sign-off. | — |
| **3B** | FsOp state + `prepCancel` + drainer cancel-ack fast-path. Tests in fs_ring.zig. | ~150 |
| **3C** | Extend `fsRingX` helpers with `cancel: ?*Cancel` param + add `fsCancelDeliver` callback in runtime.zig. Unit test. | ~250 |
| **3D** | Reactor surface methods + fs/file.zig routing. | ~100 |
| **3E** | End-to-end tests (cancel timing, race resolution, memory safety). | ~200 |

Total: ~700 lines new code + ~200 lines tests. Comparable to
Phase 2C in size and risk.
