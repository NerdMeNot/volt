---
title: The slab arena
description: One mmap at runtime init, fixed-size slots, lazy mprotect, per-P pools with fair-share caps. How Volt allocates a stack in ~5 ns and avoids the Darwin VM-lock cliff.
---

A stackful coroutine runtime lives or dies by its stack allocator.
Every spawn needs a stack; every join returns one. If the cost
per pair is microseconds, "spawn 1000 short-lived coroutines per
second" is a non-starter.

This page is how Volt's stack allocator works, why it's a slab
arena instead of a per-spawn `mmap`, and the postmortem that
forced the design.

## Mental model

> A slab arena is a single big mmap at startup, sliced into
> fixed-size slots. The hot allocate/free path is "pop/push a slot
> index from a free list" — zero syscalls, lock-free or close to
> it. Per-slot setup work happens **once**, lazily, on a slot's
> first use.

Think of the arena like a parking lot at a stadium: one big space
striped into N car-sized slots. Pulling in is "claim the next free
slot"; pulling out is "release my slot to the free list". The lot
itself was paved once. You don't pave a fresh slot every time a
car arrives.

The slot in Volt is a 256 KiB virtual reservation per coroutine.
Only the top 16 KiB is committed-RW (PROT_READ|PROT_WRITE); the
rest is PROT_NONE until needed. The bottom page is the guard —
overflow there aborts the process. See
[Stack growth](/architecture/stack-growth/) for how the middle
region grows on demand.

## The data structure

```
┌─────────────────────────────────────────────────────────────────┐
│           Arena.mapping_base ── ONE mmap (n_slots × 256 KiB)    │
│                                                                 │
│  ┌──────┐ ┌──────┐ ┌──────┐         ┌──────┐ ┌──────┐ ┌──────┐  │
│  │ slot │ │ slot │ │ slot │   ...   │ slot │ │ slot │ │ slot │  │
│  │  0   │ │  1   │ │  2   │         │ N-3  │ │ N-2  │ │ N-1  │  │
│  └──────┘ └──────┘ └──────┘         └──────┘ └──────┘ └──────┘  │
│                                                                 │
│  Each slot = guard page | growable region (PROT_NONE)           │
│              | body (top 16 KiB, PROT_RW once mprotect'd)       │
└─────────────────────────────────────────────────────────────────┘

  free_indices: [u32] stack of slot indices not currently allocated
  free_top:     usize index into free_indices (== count free)
  committed:    bitmap (1 bit per slot, set once mprotect'd)
  lock:         TTAS spinlock guarding free_indices + committed
```

Reference: `src/stack.zig:149-180` (`Arena` struct definition).

The free list is a **side array** of slot indices, not an intrusive
linked list. Reason: linked-list-of-slots needs writable memory in
each slot to hold the `next` pointer, but a fresh slot has its
body PROT_NONE. The side array doesn't have this chicken-and-egg.

## The hot paths

### Allocate

```zig
pub fn alloc(self: *Arena) Error!StackPtr {
    self.acquire();

    if (self.free_top == 0) {
        self.release();
        return error.ArenaExhausted;
    }
    self.free_top -= 1;
    const idx = self.free_indices[self.free_top];

    const base: StackPtr = @alignCast(self.mapping_base + idx * slotSize());

    const byte_idx = idx / 8;
    const bit_mask: u8 = @as(u8, 1) << @intCast(idx & 7);
    const already = (self.committed[byte_idx] & bit_mask) != 0;
    if (!already) self.committed[byte_idx] |= bit_mask;

    self.release();   // release BEFORE mprotect — the syscall serialises on
                       // the kernel's VM lock; no point holding ours

    if (!already) {
        const body_ptr: *anyopaque = @ptrCast(base + slotSize() - BODY_SIZE);
        _ = mprotect_fn(body_ptr, BODY_SIZE, PROT_READ | PROT_WRITE);
    }
    return base;
}
```

Reference: `src/stack.zig:217-260`.

Key properties:

- **Once a slot has been used (committed bit = 1)**, alloc is
  essentially: acquire spinlock, decrement counter, read index,
  release spinlock, return pointer. ~5-10 ns.
- **First-time slots** pay one `mprotect` to commit the top body
  page. Fires at most `n_slots` times across the arena's
  lifetime — by definition, not per-spawn. After warmup, every
  slot is "warm" and alloc never `mprotect`s again.
- **Spinlock released before the mprotect.** mprotect serializes
  on the kernel's `vm_map_lock` (Darwin) / `mmap_sem` (Linux);
  holding our spinlock across it would serialize allocs from
  all P's during warmup.

### Free

```zig
pub fn free(self: *Arena, slot: StackPtr) void {
    const offset = @intFromPtr(slot) - @intFromPtr(self.mapping_base);
    const idx: u32 = @intCast(offset / slotSize());

    self.acquire();
    defer self.release();
    self.free_indices[self.free_top] = idx;
    self.free_top += 1;
}
```

Reference: `src/stack.zig:262-272`.

Just push the slot index back. **No syscall ever.** The slot stays
committed (the kernel doesn't reclaim the physical pages unless we
`madvise(MADV_DONTNEED)`, which we don't — re-committing on next
alloc would cost a syscall we want to avoid).

## Per-P pools — the actual hot path

The arena's spinlock is fast but it's still a global serialization
point. To avoid hitting it on every spawn, each P (worker
scheduler state) has its own **local LIFO pool** of recycled
slots. Free pushes to the pool; alloc pops from the pool. Arena
only fires on local miss / overflow.

```
P[0]                P[1]                ...    P[N-1]
┌─────────┐         ┌─────────┐                ┌─────────┐
│stack_pool│        │stack_pool│               │stack_pool│
│ (LIFO,  │         │ (LIFO,  │                │ (LIFO,  │
│ unboundd│         │ unboundd│                │ unboundd│
│  until  │         │  until  │                │  until  │
│  cap)   │         │  cap)   │                │  cap)   │
└────┬────┘         └────┬────┘                └────┬────┘
     │ miss              │ miss                     │ miss
     │ overflow          │ overflow                 │ overflow
     ▼                   ▼                          ▼
              ┌──────────────────────┐
              │      Arena           │
              │   (spinlock-shared)  │
              └──────────────────────┘
```

Reference: `src/p.zig:187-215` (allocStack / freeStack).

The local pool is intrusive — the slot's body region (already
committed because the slot has been used) stores a `next: ?StackPtr`
pointer at offset `usableOffset()`. Pop is a single load. Push is
a single store + counter bump.

In steady state — same P spawns and frees — the arena spinlock
never fires.

## Fair-share cap

Why is the per-P pool capped? Asymmetric workloads.

Consider single-driver fan-out: one driver P spawns 1000 tasks,
they get stolen and run on workers P[1..N-1], then complete and
free into worker P's. The driver P's pool stays empty (the driver
spawns but doesn't free). The driver keeps pulling from the
arena. Worker P's pools grow unbounded.

After enough rounds, **every freed slot lives in a worker P's pool
that the driver can't reach.** Arena exhausted. `volt.spawn`
returns `error.ArenaExhausted` even though the system has
thousands of recyclable slots — they're just pinned in pools.

The fix: cap each P's pool at `arena.n_slots / n_workers`
("fair share"). Above the cap, free sheds back to the arena.
Reference: `src/runtime.zig` (Runtime.init computes the cap and
stores it on each P).

```zig
pub fn freeStack(self: *P, base: StackPtr) void {
    if (self.stack_pool_count >= self.stack_pool_cap) {
        if (self.arena) |arena| {
            arena.free(base);   // shed to arena
            return;
        }
    }
    // Push to local pool.
    const next_loc: *?StackPtr = @ptrCast(@alignCast(base + stack_mod.usableOffset()));
    next_loc.* = self.stack_pool;
    self.stack_pool = base;
    self.stack_pool_count += 1;
}
```

The cap value isn't a magic number tuned to one benchmark — it's
derived from the user's `max_concurrent_stacks` and `workers`
configuration. Single-worker runtimes get `cap = n_slots` (no
cross-P concern). High-worker runtimes get a smaller cap.

## Tried & rejected: the predecessor designs

> **Why we know the cliff hurts.** Commit `081094d` shipped the
> "mmap per spawn + per-P pool of 64" design. The pool cap was 64
> for memory parsimony reasons. Under `bench-spawn-hot` (BATCH =
> 1000 per round), every round overflowed the pool 936 times per
> worker, each overflow was a `munmap`/`mmap` pair, each pair
> serialised on the Darwin `vm_map_lock`. Result: **spawn-hot
> regressed 30× — from 106 ns/op to 3,300 ns/op** at workers=1,
> and to 5,700 ns/op at workers=11 (workers fighting one VM lock
> compounds).
>
> The full postmortem with bisect receipts is in [Slab arena
> postmortem](/performance/slab-arena-postmortem/). The fix
> shipped 2026-05-16.

### Why not eager mprotect of every slot at init?

We could `mprotect` every slot's top body page during
`Arena.init`. Then alloc never syscalls. The cost: `n_slots ×
BODY_SIZE` committed RSS upfront — at 16384 slots × 16 KiB on
Darwin that's 256 MiB resident even when no coroutines exist. The
`bench-rss` headline ("16.6 KiB per idle coro at N=10000") would
break.

Lazy commit gives us the same steady-state perf (the mprotect is
one-shot per slot) with a much smaller idle footprint. Lazy wins.

### Why not lock-free Treiber stack on slot pointers?

The slot pool could be a Treiber stack (CAS-based head pointer,
next-pointer in each slot's body). But:

1. **Fresh slots can't store a next-pointer** because their body
   is PROT_NONE. So you'd need a parallel "fresh slot index
   counter" + Treiber stack of "recycled slots" — two structures.
2. **ABA on pointer-based Treiber** — Volt would need 128-bit
   tagged-pointer CAS (LDXP/STXP) for ABA freedom, or hazard
   pointers. Both substantial complexity.
3. **Contention is low.** After warmup, the spinlock is hit only
   on local-pool miss/overflow — measured at ~8K acquires/sec
   across all workers under `bench-spawn-hot`. The lock is
   essentially uncontended; lock-free would shave nanoseconds in
   a regime that doesn't need them.

Spinlock wins on simplicity-to-perf ratio.

### Why not pre-seed per-P pools at startup?

We could pull `n_slots / workers` slots from the arena into each
P's pool at runtime init. Eliminates cold-start arena hits.

In practice, cold-start is one batch's worth of arena pops
(~BATCH operations total over the run's first millisecond) —
negligible. Pre-seeding adds startup latency for benefit that's
invisible on a 10-second benchmark. Not worth the complexity.

## Measured behaviour

Steady-state `bench-spawn-hot` workers=1: **101 ns/op**, matching
the pre-cliff baseline. The arena spinlock contributes maybe 2-3
ns when hit (local pool miss); not hit at all in the steady-state
warm-pool case.

`bench-rss` N=10000: **16.6 KiB per coroutine**. Unchanged from
the pre-arena design — the arena's virtual reservation contributes
zero RSS until coroutines actually use the slots.

Full bench gate after the arena landing: all targets green
(`yield`, `spsc`, `mpmc`, `mutex`, `tcp-echo`, `parallel-compute`,
`fanout-scaling`, `stress` 45s 838M ops). See
[Performance](/performance/) for the receipts.

## Further reading

- [Slab arena postmortem](/performance/slab-arena-postmortem/) — the bisect that found the cliff, the receipts.
- [Stack growth](/architecture/stack-growth/) — how the growable middle region works.
- [Phase 4 postmortem](/performance/phase-4-postmortem/) — the prior `mprotect-per-spawn` attempt that taught us mprotect is the cliff.
- Go's `runtime/mheap.go` — Go uses a similar slab-of-stacks shape for goroutine stack allocation.
- jemalloc's slab design — different problem (general-purpose allocator), same per-size-class slab strategy.
