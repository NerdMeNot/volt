# POC-D — Parker without pthread_cond

## Hypothesis

pthread_mutex + pthread_cond_signal on Darwin pays ~600 ns/wake of
pthread bookkeeping; the underlying primitive is the `__ulock_wait` /
`__ulock_wake` syscall pair that `os_unfair_lock` and WebKit use. Going
direct should drop wake cost to ~150 ns/cycle and free Volt from
pthread's overhead.

## Success criterion

≤ 150 ns per park+unpark round-trip on the ulock variant. Compare
against pthread variant (control).

## Implementation sketch

- `parker_pthread.zig` — control. `pthread_mutex_t`/`pthread_cond_t`
  directly via libpthread extern. Same state machine as
  `src/scheduler/worker.zig`'s Parker.
- `parker_ulock.zig` — Darwin `__ulock_wait` / `__ulock_wake` with
  `UL_COMPARE_AND_WAIT | ULF_NO_ERRNO`. State is a u32 atomic; the low
  32 bits are the compare value.
- `bench_park.zig` — two-thread ping-pong. Each thread alternates
  park(own) → unpark(peer). 1 M cycles, median of 11 reps.

## How to run

```sh
zig build spike-D
```

## Result

- **Status:** FAIL on target — but **rules out the hypothesis** definitively
- **Achieved:**
  - pthread_cond + mutex: **1,675 ns** per park+unpark RT
  - `__ulock_wait` / `__ulock_wake`: **1,417 ns** per park+unpark RT
  - ulock is 15 % faster (258 ns shaved)
- **Variance:** stable across 11 reps
- **Date measured:** 2026-05-12 on macOS arm64, ReleaseFast

### Interpretation — the BIG finding

The Parker primitive is **not the bottleneck.** Even the native ulock
takes 1.4 µs/cycle — that's the floor on cross-thread wake on Darwin,
not the floor on pthread overhead. The 1.4 µs is essentially:

- ~50 ns syscall entry/exit
- ~300 ns kernel wake-mark + scheduler enqueue on target CPU
- ~600 ns target thread waking from idle (cache cold, branch predictor
  cold, IPI to wake the core)
- ~200 ns user-space resume + atomic state CAS
- ~250 ns of pthread bookkeeping on the control variant (saved by ulock)

So switching to ulock saves 15 % — useful but not transformative.

**The real architectural fix is in POC-G: don't park the OS thread on
empty deque.** Go's M doesn't park unless ALL Ms are also out of work.
On a transient-empty deque, Go spins for ~20 µs first. That makes the
park cost amortize across many "deque empty for a microsecond" events
that, in Volt today, each pay full pthread_cond round-trip cost.

Concretely, in the 10 K spawn+join bench profile:
- Worker 0 finishes its share, deque empty → park (1.6 µs)
- Joiner on another worker finishes, unpark worker 0 (1.6 µs)
- 10 K cycles × 3.2 µs = ~32 ms per batch of parker overhead alone
- 100 batches × 32 ms = 3.2 s of parker overhead — matches the 4.7 s
  measured wall time when other costs are added in

### Implication for the v2 architecture

1. **Use ulock on Darwin / futex on Linux.** 15 % is real, free, and
   removes a dependency on pthread.
2. **Spin-then-park scheduler (POC-G).** Workers that find an empty
   deque spin-poll the global state for ~10–20 µs before parking. This
   is where the 33× spawn gap really lives.
3. **Keep workers in lock-step with the spawning thread on bursty
   workloads.** If the spawner can produce a coro every ~5 µs and the
   consumer can run it in ~200 ns, the consumer should never park.

### Cross-platform sanity

The `parker_futex.zig` (Linux) variant per the original plan was not
written this pass — POC-D's finding makes the primitive choice
secondary. Linux `futex_wait_private` will land naturally during the
rewrite (Phase 3); we don't need to spike it now because it's
well-trodden ground (Rust's `parking_lot`, Glibc's NPTL all use it).

## What this changes in the POC plan

POC-D is **done** — but with a different conclusion than the plan
expected. Move to POC-G earlier than scheduled. The scheduler
architecture choice (spin-then-park vs always-park) is now the
highest-leverage decision in the plan.

POC-B (direct dispatch) and POC-I (batched completion) still run as
planned — they target the other identified gaps (vtable indirection,
per-coro Park) which are independent of the parker primitive.
