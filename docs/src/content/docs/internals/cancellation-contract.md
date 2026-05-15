---
title: Cancellation contract
description: The state machine that connects coroutine cancellation, the Park primitive, and reactor I/O — what every implementation MUST guarantee.
---

:::caution
**Stale (pre-v2 flattening).** Cancellation as a first-class
feature (Coroutine.cancel, current_park, error.Cancelled
propagation) is **not implemented** in the current build. This
doc describes a contract the v0.x tree implemented; the design
intent is still valid for re-landing, but the specifics (Park
primitive, EventSource layer) no longer map to the current
parking-lot + per-primitive design. Treat as historical /
aspirational until re-validated.
:::

This page is the formal model of how cancellation interacts with parking and the reactor. Every Park-based primitive in Volt (Channel, Mutex, Sleep, Notify, etc.) and every reactor-backed wait (`waitReadable`, `waitWritable`) sits on top of this contract.

Volt's substrate ambition forces correctness here: a runtime that intermittently leaks parked coroutines or wedges on shutdown can't be the foundation other libraries build on. This doc enumerates the state machine, the invariants every implementation must hold, and the proof obligations that make the implementation correct.

## Actors

Five actors interact with a single `Park`:

1. **Owner** — the coroutine that called `park.parkCurrent()`. Owns the Park's lifetime (it's stack-local in the owner's frame).
2. **Worker.subscribe** — the scheduler thread that, after the owner ctx-swaps out, runs `park.es.subscribe(coro)` to install or fast-wake.
3. **Reactor.poll → wakeFn** — the reactor thread (whichever worker holds `poll_claim`) calls `park.unpark()` when the kernel reports the registered event.
4. **Canceller** — any thread calling `coro.cancel()` on the Owner. Sets `cancel_flag`; if `current_park` is set, also calls `park.unpark()` directly.
5. **wait.zig cancel-arm** — the post-`parkCurrent` code that runs when `parkCurrent` returns `error.Cancelled`. Calls `reactor.unregisterWait()` to tear down the kernel registration.

## State spaces

### Park.state — single u64 atomic

| Value | Meaning |
|---|---|
| `0` | empty — no waiter, no pending notification |
| `NOTIFIED` (= 1) | unpark fired but nobody was registered to wake |
| `coro_ptr` (low bit clear) | a coroutine is parked here; `unpark` will schedule it |

Encoding rationale: collapsing two pieces of state (`is_waiting` + `wait_co`) into one atomic puts every transition into a single modification order — no IRIW litmus failure across two atomics.

### Coroutine.cancel_flag — bool atomic
- `false` — not cancelled
- `true` — cancellation requested

### Coroutine.current_park — usize atomic
- `0` — coroutine is not currently parked
- `ptr` — coroutine is parked on the Park at this address

### Reactor pendingCount + waiters map
- `pendingCount` — atomic counter of in-flight registrations
- `waiters` — mutex-protected `(fd, kind)` → set
- Invariant: `pendingCount == |waiters|` at every point when no thread holds `mutex`

## Invariants

The implementation must hold ALL of these:

**I1. Single-resume.** Every coroutine that calls `parkCurrent` resumes exactly once, regardless of cancel/wake interleaving.

**I2. No UAF on Park.** `park.unpark()` is never invoked on a `*Park` whose owning coroutine has returned from the function holding it. (Park lives on the owner's stack.)

**I3. pendingCount accuracy.** When the runtime exits cleanly, `reactor.pendingCount() == 0`. Equivalently: every `register*` is paired with exactly one `pending--` (by `unregisterWait` or by `poll()` consuming the event).

**I4. Cancel observable.** A coroutine that is parked when `cancel()` fires resumes within bounded time with `error.Cancelled`. (No "set cancel_flag, no unpark, coro sleeps forever.")

**I5. wakeFn target validity.** The reactor's `wakeFn` is only ever invoked on a `*Park` whose owning coroutine is still alive (the function holding the Park hasn't returned).

## Operation pseudocode

### `parkCurrent(self)` — owner side

```
if coro.isCancelled(): return Cancelled
if state.cmpxchgStrong(NOTIFIED, 0): return success    // fast path
coro.current_park.store(self, seq_cst)                 // [A]
if coro.cancel_flag.load(seq_cst): {                   // [B] post-store recheck
    coro.current_park.store(0)
    return Cancelled
}
coro.pending_event = self.es
ctx_swap(coro.ctx → scheduler.ctx)                     // [C] suspend point
coro.current_park.store(0, release)                    // [D] post-resume
if coro.isCancelled(): return Cancelled
return success
```

### `subscribe(coro)` — worker side, post-yield
```
loop:
  cur = state.load
  if cur == 0:
      if cas(0, ptr) succeeds: return                  // installed
  if cur == NOTIFIED:
      if cas(NOTIFIED, 0) succeeds:
          scheduleCoro(coro); return                   // fast-wake
  panic("concurrent waiter")
```

### `unpark(self)` — any thread
```
loop:
  cur = state.load
  if cur & NOTIFIED: return                            // idempotent
  if cur == 0:
      if cas(0, NOTIFIED) succeeds: return             // store NOTIFIED
  // cur is coro_ptr
  if cas(cur, 0) succeeds:
      scheduleCoro(@ptr(cur)); return
```

### `cancel()` — any thread
```
cancel_flag.store(true, seq_cst)                       // [X]
park_addr = current_park.load(seq_cst)                 // [Y]
if park_addr != 0:
    @ptr(park_addr).unpark()
```

### Reactor `poll()` — single polling worker
```
events = kernel_queue.dequeue()
for ev in events:
    key = WaitKey(ev.fd, ev.kind)
    mutex.lock()
    removed = waiters.remove(key)
    mutex.unlock()
    if removed:
        pendingCount -= 1
        wakeFn(target)                                 // = park.unpark
```

### `unregisterWait(fd, kind)` — wait.zig cancel-arm
```
kernel.delete(fd, kind)                                // best-effort
mutex.lock()
removed = waiters.remove(key)
mutex.unlock()
if removed:
    pendingCount -= 1
```

## Race scenarios — exhaustive enumeration

For each scenario, we verify all five invariants hold.

The variables: WHEN cancel arrives relative to parkCurrent's progress (a-f); WHEN/IF the kernel event fires (g-i); WHO wins the cancel-vs-poll race for `waiters.remove` (when both apply).

### S1. Cancel before parkCurrent enters
- `cancel`: sets flag, current_park=0, no unpark
- `parkCurrent`: line 80 `isCancelled`=true → return Cancelled
- `cancel-arm`: unregisterWait → kevent.delete success, waiters.remove=true, pending-- ✓
- I1✓ I2✓ I3 (1 register, 1 unregister, net 0)✓ I4✓ I5✓

### S2. Cancel between parkCurrent's `[A]` store_cp and `[B]` load_cf
- seq_cst total order: cancel.store_cf < parkCurrent.load_cf → load_cf reads true
- parkCurrent: store_cp = ptr; load_cf = true → store_cp = 0; return Cancelled
- cancel: store_cf done; load_cp could be 0 (if before [A]) or ptr (if after [A])
  - If load_cp = 0: no unpark
  - If load_cp = ptr: park.unpark, but Park's state was never installed (subscribe never ran) → state goes 0 → NOTIFIED. Harmless.
- `cancel-arm`: unregisterWait → success, pending-- ✓
- All invariants hold ✓

### S3. Cancel between `[B]` load_cf and `[C]` ctx_swap entry
- parkCurrent: load_cf = false (cancel hasn't fired yet); proceed to ctx_swap
- cancel fires AFTER load_cf but BEFORE ctx_swap stores the registers
  - cancel.load_cp = ptr (parkCurrent stored it at [A])
  - cancel calls park.unpark → state was 0 (no subscribe yet) → state = NOTIFIED
- ctx_swap completes, scheduler runs subscribe(coro)
- subscribe: state = NOTIFIED → consume → fast-wake (scheduleCoro)
- coro resumes from ctx_swap. clears current_park. isCancelled → true → return Cancelled.
- `cancel-arm`: unregisterWait → kevent still armed (event never fired in this scenario) → success → pending-- ✓
- All invariants hold ✓

### S4. Cancel during `[C]` ctx_swap (coro suspended, before subscribe runs)
- Same as S3, just different timing — cancel fires while coro is in the kernel.
- park.unpark sets state = NOTIFIED
- subscribe consumes; schedule
- All invariants hold ✓ (same proof as S3)

### S5. Cancel after subscribe installed coro_ptr (coro suspended)
- parkCurrent: stored cp at [A]; ctx_swap suspends.
- subscribe: state was 0 → CAS to coro_ptr.
- cancel fires: load_cp = ptr; park.unpark.
- park.unpark: state was coro_ptr → CAS to 0; scheduleCoro.
- coro resumes; clears cp; isCancelled → true → return Cancelled.
- cancel-arm: unregisterWait → success → pending-- ✓
- All invariants hold ✓

### S6. Cancel after coro resumed (between `[C]` returning and `[D]` clearing cp)
- subscribe scheduled the coro (e.g., via reactor wake or another path).
- coro resumes from ctx_swap. cp is still ptr (line 119 hasn't run).
- cancel fires: load_cp = ptr; park.unpark.
- park.unpark: state was 0 → NOTIFIED. (Coro is running, no scheduling.)
- coro: line 119 clears cp; isCancelled → true → return Cancelled.
- cancel-arm: unregisterWait. ✓
- All invariants hold ✓

### S7. Cancel after cp cleared at `[D]`
- coro resumed normally (subscribe consumed NOTIFIED, or unpark scheduled).
- coro: clears cp at [D]. cancel_flag still false.
- coro reads isCancelled at line 126 → false → return success.
- function continues; eventually returns from waitOn successfully.
- cancel fires later: load_cp = 0, no unpark. cancel_flag set but coro is past parkCurrent.
- The coro's NEXT cancellation point catches it.
- All invariants hold ✓

### S8. Kernel event fires before cancel arrives, no cancel
- registerWait: pending=1, waiters has key.
- parkCurrent enters slow path; subscribe installs coro_ptr.
- kernel fires event. Reactor poll(): waiters.remove → true; pending--; wakeFn → park.unpark.
- park.unpark: state was coro_ptr → CAS to 0; scheduleCoro.
- coro resumes. parkCurrent returns success. waitOn returns success. ✓

### S9. Kernel event fires; cancel arrives concurrently
- registerWait: pending=1.
- parkCurrent → subscribe installs coro_ptr.
- Two near-simultaneous events:
  - **A**: kernel fires; poll() does `mutex.lock; waiters.remove → true; mutex.unlock; pending--; wakeFn → park.unpark`.
  - **B**: cancel fires; cancel.load_cp = ptr; park.unpark.
- park.unpark is idempotent under racing CAS — exactly one of A/B's unpark schedules the coro; the other becomes a no-op (state already 0 → set to NOTIFIED, harmless).
- coro resumes. Returns Cancelled (cancel_flag is set).
- cancel-arm: unregisterWait → kevent.delete returns ENOENT (kernel auto-removed when event fired); waiters.remove=false (poll already removed); no decrement. ✓
- Net: pending=1 → 0 (one decrement, by poll). ✓

### S10. Cancel arrives first, then kernel event fires (race won by cancel)
- registerWait: pending=1.
- parkCurrent → subscribe installs coro_ptr.
- cancel: park.unpark; coro scheduled.
- coro resumes; runs cancel-arm.
- unregisterWait: kevent.delete → SUCCESS (event was still armed; kernel removes it AND any pending event). waiters.remove=true; pending--. ✓
- Reactor never sees an event for this fd (it was removed). ✓

### S11. Cancel arrives first, then kernel event fires (race won by event delivery)
- registerWait: pending=1.
- parkCurrent → subscribe installs coro_ptr.
- cancel: park.unpark → schedules coro.
- *Before cancel-arm runs*: kernel event was already in the deliverable queue. Reactor poll() retrieves it.
- poll(): waiters.remove → true (cancel-arm hasn't run); pending--; wakeFn → park.unpark; state was 0 (cancel cleared it) → NOTIFIED.
- coro resumes; runs cancel-arm.
- unregisterWait: kevent.delete → ENOENT; waiters.remove=false; no decrement. ✓
- Net: pending=1 → 0. ✓

## The proof obligations

For ALL scenarios above (S1–S11), the implementation must satisfy:

| # | Obligation | Implementation site |
|---|---|---|
| O1 | Cancel-flag store and current_park load are seq_cst-paired | `Coroutine.cancel` |
| O2 | Current_park store and cancel_flag load are seq_cst-paired | `Park.parkCurrent` |
| O3 | Park.unpark is idempotent under concurrent calls | `Park.unpark` CAS-loop |
| O4 | unregisterWait's `waiters.remove` always runs (regardless of EV_DELETE result) | `reactor_kqueue.unregisterWait` |
| O5 | poll() only fires wakeFn when waiters.remove succeeded | `reactor_kqueue.poll` |
| O6 | `pendingCount` is decremented exactly once per registration | by O4 + O5 |

Without O4: cancel that loses the race against EV_ONESHOT auto-removal swallows ENOENT and leaves the waiters map populated. Poll then fires wakeFn on a Park whose owner has unwound the cancel-arm and returned (UAF).

Without O5: poll fires wakeFn on a target that the cancel-arm already cleaned up — same UAF + double-decrement.

Without O1+O2: see S2/S3 — cancel can arrive in the window where parkCurrent has stored cp but not yet observed cancel_flag, with both sides reading their pre-state of the other (the IRIW litmus). Coroutine commits to ctx_swap with cancel_flag set and no unpark scheduled — invariant I4 fails.

## Status (pre-v1)

| Obligation | Status |
|---|---|
| O1 | ✓ Implemented (`Coroutine.cancel`, src/coroutine/coroutine.zig) |
| O2 | ✓ Implemented (`Park.parkCurrent`, src/scheduler/park.zig) |
| O3 | ✓ Pre-existing (CAS-loop already idempotent) |
| O4 | ✓ Implemented (`reactor_kqueue.unregisterWait`) |
| O5 | ✓ Implemented (`reactor_kqueue.poll`) |
| O6 | ✓ Follows from O4+O5 |

Every scenario S1–S11 in this doc has a paired obligation and the implementation satisfies it.

**Despite all of the above being satisfied**, stress runs at N=10 still observe intermittent leaks of 0–80 of 100 reactor registrations on the cancellation torture test (`cancel-audit:100 coroutines parked on TCP read`). At least one race scenario is not in this doc.

The architectural fix in **R2** is to widen the proof obligation: rather than relying on the cancel-arm's `unregisterWait` matching the reactor's `poll()` for each registration, make the reactor `poll()` the **single linearization point** that decrements `pending` and clears the waiters map. wait.zig's cancel-arm becomes a fire-and-forget cancel-signal — it sets a per-Park "cancelled" bit and exits. The reactor, on consuming the kernel event (or cancel signal), reads the bit and routes accordingly. This eliminates the cancel/poll bookkeeping race entirely (no two paths can disagree about who decrements).

This is the design mio and Tokio use, and what R2 will port to Volt.
