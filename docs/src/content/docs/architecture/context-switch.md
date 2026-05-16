---
title: The context switch
description: AAPCS64 wide-save register layout, the trampoline, and why 10 ns/swap is the floor. The 28 instructions that make stackful coroutines fast.
---

A context switch is what makes a stackful coroutine actually a
coroutine and not just a function pointer. When a coroutine
suspends, the runtime needs to save its registers, switch to a
different stack, and restore another coroutine's registers — or
the worker's dispatch-loop registers, on a yield.

This page covers Volt's arm64 context switch. The x86_64 port is
roadmapped; the design is parallel.

## Mental model

> A coroutine's "execution state" is a small bundle of CPU
> registers — the stack pointer, the return address, the
> callee-save registers per the ABI, and a couple of misc bits.
> Switching between coroutines means **saving the current
> bundle to memory and loading the other one**. That's 28
> instructions on arm64. The runtime's dispatch loop calls into
> this swap; the swap returns when someone calls swap-back from
> the other side.

The bundle is small because most registers are caller-save (the
caller of `context.swap` already saved them in its own frame
according to the ABI). We only save callee-save ones plus SP and
LR (link register).

## AAPCS64: what we save

Per the ARM Architecture Procedure Call Standard for 64-bit, the
**callee-save** general-purpose registers are `x19`-`x28`, plus
`x29` (frame pointer) and `x30` (link register / return address).
Plus SP. Plus the lower halves of `v8`-`v15` (NEON callee-save).
Plus the platform-specific TPIDR (TLS pointer).

In total:

```
General-purpose callee-save:   x19, x20, ..., x28, x29, x30   (12 × 8 bytes)
Stack pointer:                  sp                              (1 × 8 bytes)
NEON callee-save lower halves:  d8, d9, ..., d15                (8 × 8 bytes)
TLS pointer:                    TPIDR_EL0                       (1 × 8 bytes)
                                                                ───────────
                                                                22 × 8 = 176 bytes
```

Reference: `src/context_arm64.zig` (the `Context` struct's
field layout) and the swap assembly.

## The swap

The actual swap routine is hand-rolled assembly. Its job: given
two `Context*` pointers (source and target), save the current CPU
state into `source` and load the state from `target`.

```
voltCoroSwap(source: *Context, target: *Context):

  // Save source
  stp x19, x20, [x0,  #0]
  stp x21, x22, [x0, #16]
  stp x23, x24, [x0, #32]
  stp x25, x26, [x0, #48]
  stp x27, x28, [x0, #64]
  stp x29, x30, [x0, #80]
  mov x9,  sp
  str x9,        [x0, #96]
  stp d8,  d9,   [x0, #104]
  stp d10, d11,  [x0, #120]
  stp d12, d13,  [x0, #136]
  stp d14, d15,  [x0, #152]
  mrs x9, TPIDR_EL0
  str x9,        [x0, #168]

  // Load target
  ldp x19, x20, [x1,  #0]
  ldp x21, x22, [x1, #16]
  ...
  ldr x9,        [x1, #96]
  mov sp, x9
  ...
  ldr x9,        [x1, #168]
  msr TPIDR_EL0, x9

  ret    // returns to wherever target was waiting at
```

Reference: `src/context_arm64.zig` for the full inline assembly.

28 instructions on the typical path. STP/LDP pairs save/restore
two registers per instruction — twice as fast as individual STR/LDR.

The `ret` at the end is what makes this magic. We loaded `x30`
(LR) from `target.x30` — that's the address `target` was supposed
to return to, set up at spawn time by the trampoline or by a
prior swap-back. `ret` jumps to whatever's in LR. So we don't
literally "return to the caller of swap" — we return to wherever
the *target* coroutine wanted to be.

In effect: when coroutine A calls `swap(&A.ctx, &B.ctx)`, control
flow ends up at the address B had saved as its LR. From B's
perspective, it just returned from its own `swap` call. The
context switch is invisible at the source-language level —
`swap` returns "normally" on both sides, even though it's
returning to different points in different code.

## The trampoline

The first time a coroutine resumes, there's nothing on its stack
yet — no `swap` frame to return from. We need to bootstrap it: set
SP to the top of its stack, jump to its entry function, and
ultimately call the runtime's "I'm done, clean me up" callback.

The bootstrap is the **trampoline**: a tiny assembly stub that
gets the coroutine's "context" set up so the very first `swap`
into the coroutine lands on the trampoline. The trampoline then
calls the user closure.

```zig
pub fn initContext(ctx: *Context, stack_top: [*]u8, frame: *anyopaque) void {
    // SP starts at the top of the stack.
    ctx.sp = @intFromPtr(stack_top);
    // First swap-into lands at the trampoline.
    ctx.lr = @intFromPtr(&voltCoroEntry);
    // The trampoline reads the frame pointer from x19 (which we pre-set).
    ctx.x19 = @intFromPtr(frame);
    // ... other registers zeroed ...
}
```

Reference: `src/context_arm64.zig` — `initContext` and the
`voltCoroEntry` symbol.

The trampoline itself in asm:

```
voltCoroEntry:
  mov x0, x19              // x0 = frame pointer (the closure)
  bl  voltCoroInvoke       // call the closure's invoke fn
  bl  voltCoroDone         // mark coroutine done, swap back to worker
  brk #0                   // unreachable
```

`voltCoroInvoke` is a comptime-generated function (per
coroutine type) that knows how to call the user fn with the right
args. `voltCoroDone` sets `pending = .done` and swaps back to
the dispatch loop. Once we swap back, the worker's `.done` branch
frees the coroutine's resources.

## Tried & rejected: narrow-save context switch

We could save fewer registers — only the GPRs, skipping NEON
(d8-d15) and TPIDR. ARM ABI says NEON callee-save lower halves
must be preserved across function calls, but **across coroutine
suspensions** the contract is fuzzier — if no one in the
coroutine's call chain uses NEON between resume and suspend, the
saves are wasted.

Spike POC-A measured this: narrow-save was ~7 ns/swap vs wide-save
~10 ns/swap. 30% win on the microbenchmark.

The problem: **garbage NEON state after resume**. If the suspending
function happened to be using `d8` for a multiply-accumulate (the
compiler can use NEON regs for f64 math without you knowing), and
we don't save it, the post-resume state has corrupted `d8`. Bugs
that only fire in optimized builds, that only fire on M-series
CPUs (Intel Macs's ABI is different), that take days to debug.

Volt picked **wide-save**: pay 3 ns/swap for ABI-correctness.
Receipt: 9 ns/swap on `bench-yield`. Still well under Go's 42 ns.

Reference: `spike/A_ctx/bench_swap_narrow.zig` — the POC.

## Tried & rejected: setjmp / longjmp

POSIX `sigsetjmp` / `siglongjmp` would handle the register save
for us. Why we don't:

1. **siglongjmp can't switch stacks.** It restores a saved frame,
   not a saved stack pointer to a different stack. Coroutine
   switching needs the stack-switch.
2. **It's slower.** Glibc's setjmp saves more state than we need
   (signal mask, FP control word) and goes through a libc call
   overhead. Our inline asm avoids the call frame.

`makecontext` / `swapcontext` would handle the stack switch but
are deprecated / removed on most platforms.

Hand-rolled asm is the only path. The code is small enough (~28
instructions, plus a tiny trampoline) that maintenance cost is
trivial.

## Tried & rejected: register windows / "linked stacks"

Some coroutine implementations (e.g., legacy Erlang HiPE) use
*split stacks* or linked stack chunks — instead of one
contiguous stack, chains of small chunks linked via pointers,
with the function epilogue checking "do I need a new chunk".

The cost: every function prologue compiles in a check, and
calls across chunks pay an extra branch. Volt picks contiguous
stacks per slot — the slab arena + SIGSEGV grow gives us
contiguous-stack ergonomics without committing 8 MiB upfront.

## What's not yet built

- **x86_64 context switch.** The shape is the same: save SysV
  ABI callee-save regs (`rbx`, `rbp`, `r12`-`r15`, plus `rsp`
  and a return address pushed on the new stack), restore the
  target's. NEON equivalent: `xmm6`-`xmm15` are caller-save on
  SysV ABI but callee-save on Windows ABI. The asm exists in
  Volt's spike (POC-A's earlier iterations); the production port
  is roadmapped.
- **Module-level comptime asm on x86_64-linux ELF.** A Zig 0.16
  gotcha: `comptime { asm(...) }` doesn't always emit symbols on
  the x86_64-linux target. Plan is to use a separate `.S` file
  linked via `build.zig` when the x86_64 port lands. Documented
  in CLAUDE.md.

## Measured

- **`bench-yield` workers=1**: 9 ns/op one-way context switch
  (median of 11 runs). That's two `context.swap`s — one out to
  the dispatch loop, one back into the coroutine — plus the
  scheduling overhead of re-queueing to the worker's tail and
  re-popping.
- **`bench-spawn-hot` workers=1**: 101 ns/op for the full spawn-
  through-join cycle. Multiple context switches plus arena pop,
  Task alloc, Frame alloc, completion-unpark. The 9 ns swap
  is a small fraction of this.

## Further reading

- [Stackful by design](/architecture/stackful-design/) — why stackful at all.
- [The M:N scheduler](/architecture/mn-scheduler/) — what the worker side of the swap does after returning.
- AAPCS64 reference (ARM IHI 0055) — the ABI doc for arm64 calling conventions.
- Boost.Context (Boost C++) — production-grade stackful context switch implementation; same design, more architectures supported.
- `libco` (Tencent) — minimal stackful coroutine library; comparable shape.
