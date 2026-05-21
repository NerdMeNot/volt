---
title: Stack growth on demand
description: Guard pages, the SIGSEGV handler, mprotect-on-fault. How a coroutine starts with 16 KiB committed and grows in page increments without explicit reallocation.
---

A stackful coroutine pays its resident memory in stack. Pay too
much and you can't have a million coroutines. Pay too little and
deep recursion segfaults randomly.

The way out: **pay for the top page, reserve the rest as virtual,
and grow on fault.** SP starts at the top of a 1 MiB virtual
reservation (tunable via `Runtime.Config.stack_reservation_size`);
the top 16 KiB is committed RW; everything below (including the
bottom guard page) is PROT_NONE. When SP walks into
PROT_NONE — recursion past the initial body — the kernel raises
SIGSEGV. A handler catches it, mprotects the touched page to RW,
and returns. The faulting instruction reruns. SP keeps going.

This is the same trick Go's runtime uses for goroutine stack
growth, and that JVM HotSpot uses for thread stack guard pages.
Volt does it under coroutine slabs.

## Mental model

> Each coroutine's stack is a **1 MiB sealed envelope** sitting
> inside the slab arena. The envelope is mostly empty (PROT_NONE).
> A 16 KiB window at the top is open from the start. The bottom
> page is sealed with a "tripwire" — touching it aborts the
> program. Pages between the window and the tripwire are sealed
> but openable; touching one runs a tiny handler that opens it.

The SP points at the top of the window. As code runs, SP moves
down. While SP stays in the window, everything is normal memory
access. When SP crosses into a sealed region, the kernel fires
SIGSEGV; the handler opens that page; SP keeps going. When SP
hits the tripwire (bottom page of the envelope), the program
aborts.

## Layout per slot

```
                                              ┌── high address ──┐
   base + slotSize() ─────────────────────────┤  SP starts here  │
                                              │                  │
                                              │   body region    │  top 16 KiB
                                              │   (PROT_RW       │  committed
                                              │    from alloc)   │  at first use
                                              │                  │
   base + slotSize() - BODY_SIZE  ─── ─── ────┤  ───────────────  │
                                              │                  │
                                              │  growable region │  PROT_NONE
                                              │  (PROT_NONE      │  initially;
                                              │   until SIGSEGV  │  pages mprotect'd
                                              │   handler grows  │  RW on first
                                              │   pages on demand)│  touch
                                              │                  │
                                              │                  │
                                              │                  │
   base + pageSize() ─────────────────────────┤  ───────────────  │
                                              │   guard page     │  PROT_NONE
                                              │   (PROT_NONE     │  forever;
                                              │    forever)      │  fault here
                                              │                  │  → abort
   base ───────────────────────────── low ────┘  ─── ─── ─── ────┘
```

Reference: `src/stack.zig:1-49` (the layout comment).

Sizes:

- `DEFAULT_RESERVATION_SIZE = 1 MiB` — default per-slot virtual
  reservation. Tune via `Runtime.Config.stack_reservation_size` (must
  be a multiple of the page size and strictly greater than guard +
  `BODY_SIZE`).
- `BODY_SIZE = 16 KiB` — top region committed at first allocation.
- Guard = one page = 16 KiB on Darwin arm64 (one Darwin page),
  4 KiB on Linux.
- Growable region = `reservation - guardSize() - BODY_SIZE`
  ≈ 1008 KiB on Darwin / 1020 KiB on Linux at the default. That's
  the recursion budget before clean abort.

## The SIGSEGV handler

```zig
fn handler(sig: c_int, info: *SigInfo, ctx: ?*anyopaque) callconv(.c) void {
    const fault_addr = siInfoAddr(info);

    // Linear scan of the region registry. With one region per
    // Runtime (the whole arena), this is O(few).
    var i: usize = 0;
    while (i < MAX_REGIONS) : (i += 1) {
        const base = regions[i].base.load(.acquire);
        if (base == 0) continue;
        const size = regions[i].size;
        const slot_size = regions[i].slot_size;
        const ps = regions[i].page_size;
        if (fault_addr < base or fault_addr >= base + size) continue;

        // Slot offset: 0 for single-stack regions, modulo for arenas.
        const slot_offset = if (slot_size == 0)
            fault_addr - base
        else
            (fault_addr - base) % slot_size;

        // Bottom-most page of the slot is the guard — real overflow.
        if (slot_offset < ps) break;

        // Otherwise: grow. mprotect the page containing fault_addr
        // to PROT_RW. Page-align downward.
        const page_addr = fault_addr & ~(ps - 1);
        const page_ptr: *anyopaque = @ptrFromInt(page_addr);
        _ = mprotect_fn(page_ptr, ps, PROT_READ | PROT_WRITE);
        return;
    }

    // Not in any registered region — or in a guard page. Chain.
    chainTo(sig, info, ctx);
}
```

Reference: `src/signal.zig:121-149`.

The handler is **async-signal-safe**: no allocator, no parking
lot, no logging, just an atomic load + arithmetic + `mprotect`
(POSIX AS-safe). Reads the region registry under a relaxed
atomic load — concurrent registration is rare (init-time only)
and we don't need a strict ordering for the lookup.

## Arena-aware region registration

Pre-arena, the handler stored one region per coroutine — every
`stack.alloc` registered a stack range, every `stack.free`
unregistered. With 16k concurrent coros, the registry filled and
the scan walked 16k entries per fault.

With the arena, we register **one region per arena** (per
Runtime). The handler does `(fault_addr - base) % slot_size` to
find which slot the fault is in. One registry entry covers all N
slots.

```zig
pub fn registerArena(base: usize, total_size: usize, slot_size: usize) error{RegistryFull}!void {
    std.debug.assert(slot_size > 0);
    std.debug.assert(total_size % slot_size == 0);
    return registerInternal(base, total_size, slot_size);
}
```

Reference: `src/signal.zig:215-219` and the handler's slot-offset
arithmetic.

`MAX_REGIONS = 64` is generous — supports 64 concurrently-live
Runtime instances per process. The original 16384 was sized for
per-coroutine registration; with the arena that's overkill.

## What happens at runtime

A coroutine starts with SP at `base + 1 MiB` (or whatever
`stack_reservation_size` is configured to). The body region (top
16 KiB) is committed. The coroutine runs, calls some functions,
locals push onto the stack. SP moves down.

While SP stays above `base + (reservation - 16 KiB)` — the body's
lower edge — every access is regular memory: the CPU walks the
page tables, finds RW pages, no fault.

If a deep recursion (or a big local array, or an alloca) pushes SP
into the growable region (anywhere below the committed body but
above the guard page), the page containing SP is PROT_NONE. The
CPU faults. Kernel delivers SIGSEGV with `siginfo.si_addr` pointing
at the faulting address. Our handler runs:

1. Linear-scan the region registry. Find the arena entry.
2. Compute `slot_offset = (fault_addr - base_of_slot) % reservation`.
3. If `slot_offset >= pageSize()` (above the guard), this is a
   growable-region fault.
4. Page-align the fault address down to a page boundary.
5. `mprotect(page_addr, pageSize(), PROT_READ | PROT_WRITE)` — that
   page is now committed RW.
6. Return from the handler. The faulting instruction reruns.

The coroutine never knows a fault happened. SP keeps moving down
through committed pages until it hits the next PROT_NONE page,
which triggers another fault, which commits another page, and so
on.

If SP keeps going all the way down — past the growable region into
the bottom guard page (the bottom 16 KiB) — the handler detects
`slot_offset < pageSize()` and **chains to the default handler**:

```zig
fn chainTo(sig: c_int, info: *SigInfo, ctx: ?*anyopaque) void {
    const prev: *const Sigaction = if (sig == SIGSEGV) &prev_segv else &prev_bus;
    if (prev.sa_sigaction) |h| {
        h(sig, info, ctx);
        return;
    }
    @panic("volt: SIGSEGV in unregistered region — real fault");
}
```

The default handler aborts the process with the standard SIGSEGV
behaviour — core dump, traceback, exit code 11. The guard page
is the tripwire: cross it and you get a clean abort, not silent
heap corruption.

## Tried & rejected: per-spawn mprotect

> **Phase 4 postmortem.** An earlier attempt (commit reverted
> 2026-05-15) called `mprotect` from the spawn path itself —
> every spawn `mprotect`'d its stack body. The argument: simpler
> than a signal handler.
>
> The problem: `mprotect` serializes on the kernel's per-process
> VM lock (`vm_map_lock` on Darwin, `mmap_sem` on Linux). Multiple
> workers each doing `mprotect` per spawn fought one lock.
> `bench-spawn-hot` workers=11 regressed ~8×. The design didn't
> ship.
>
> The lesson: `mprotect` is fine **occasionally** (once per slot
> over the runtime's lifetime, as the lazy arena does). It is not
> fine on every spawn.
>
> Full receipts: [Phase 4 postmortem](/performance/phase-4-postmortem/).

## Tried & rejected: fixed-size 8 MiB stacks (no grow)

Allocate 8 MiB committed per coroutine, no growth needed. Simple.

The math: 16384 coroutines × 8 MiB = **128 GiB resident**. On a
laptop with 32 GiB RAM, you can have ~4096 coroutines before the
swapper takes over. Goroutine-style runtimes that brag about
"1M concurrent" need ~few-KiB stacks; 8 MiB doesn't work.

Growable stacks give us the best of both: cheap idle (16 KiB
committed), real recursion budget (~1 MiB usable at the default),
bounded virtual reservation per slot (1 MiB default, tunable).

## What's not yet handled

- **Stack shrinkage.** Once a coroutine grows pages, those pages
  stay committed for the slot's lifetime. We don't `madvise` them
  away on coroutine completion. Reason: re-committing them on
  reuse would cost an `mprotect` per page — pushing cost back to
  the spawn path. The fix would be a periodic reaper that
  `madvise(MADV_DONTNEED)`s slots that have been idle for some
  time; not implemented today.
- **Stack overflow diagnostics.** When the bottom guard fires,
  the chained default handler prints a stack trace and aborts. We
  don't (yet) print "you ran out of stack on coroutine X started
  from `myFn:line`" — would require keeping more metadata per
  slot. Probably worth doing.
- **Linux page sizes.** Linux uses 4 KiB pages by default. The
  same code works (the page-size constants come from
  `std.heap.pageSize()`) but RSS-per-coroutine will be ~4 KiB
  initially instead of 16 KiB. The Linux backend is roadmapped;
  the stack-growth path is portable.

## Further reading

- [The slab arena](/architecture/slab-arena/) — where the slots live.
- [Phase 4 postmortem](/performance/phase-4-postmortem/) — the prior mprotect-per-spawn attempt.
- Go's `runtime/stack.go` — Go uses the same fault-driven growth shape, with the additional copy-and-grow when the stack hits a hard limit. Volt's design is simpler (no copy; the 256 KiB envelope is the hard limit).
- "Why GoLang is doing it wrong" / "Stack growth in concurrent runtimes" — generic background on the design space.
