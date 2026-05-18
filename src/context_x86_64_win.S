// Microsoft x64 (Win64) context switch for Volt.
//
// Two symbols:
//   * voltCtxSwap(from, to) — save callee-saved state to *from,
//     load from *to, return into the new context.
//   * voltCoroEntry — naked entry trampoline. First `ret` after a
//     fresh-coroutine swap lands here; reads the closure pointer
//     from rbx (callee-saved, set by initContext) and calls
//     closure.run_fn(closure).
//
// Calling convention: Microsoft x64. Args in rcx, rdx, r8, r9.
// Return in rax. Callee-saved GPRs: rbx, rbp, rdi, rsi, r12-r15.
// Callee-saved XMMs: xmm6-xmm15 (full 128 bits — `movdqa` aligned
// 16-byte moves). Caller must reserve 32 bytes of "shadow space"
// on the stack before any `call`. Stack must be 16-byte aligned
// immediately before any `call`.
//
// COFF/PE on Windows doesn't add an underscore prefix to C
// symbols; symbols emitted here are referenced directly by their
// declared names.

// Win64Context layout — must match src/context_x86_64.zig:
//   [from +   0]  xmm6
//   [from +  16]  xmm7
//   [from +  32]  xmm8
//   [from +  48]  xmm9
//   [from +  64]  xmm10
//   [from +  80]  xmm11
//   [from +  96]  xmm12
//   [from + 112]  xmm13
//   [from + 128]  xmm14
//   [from + 144]  xmm15
//   [from + 160]  rbx
//   [from + 168]  rbp
//   [from + 176]  rdi
//   [from + 184]  rsi
//   [from + 192]  r12
//   [from + 200]  r13
//   [from + 208]  r14
//   [from + 216]  r15
//   [from + 224]  rsp
// Total: 232 bytes, 16-byte aligned (XMM stores need it).

.text

.globl voltCtxSwap
.p2align 4
voltCtxSwap:
    // Save callee-saved XMMs to *from (rcx) via `movdqa` (aligned).
    // Context is 16-byte aligned by the Zig struct definition.
    movdqa %xmm6,    0(%rcx)
    movdqa %xmm7,   16(%rcx)
    movdqa %xmm8,   32(%rcx)
    movdqa %xmm9,   48(%rcx)
    movdqa %xmm10,  64(%rcx)
    movdqa %xmm11,  80(%rcx)
    movdqa %xmm12,  96(%rcx)
    movdqa %xmm13, 112(%rcx)
    movdqa %xmm14, 128(%rcx)
    movdqa %xmm15, 144(%rcx)
    // Save callee-saved GPRs to *from (rcx).
    movq %rbx, 160(%rcx)
    movq %rbp, 168(%rcx)
    movq %rdi, 176(%rcx)
    movq %rsi, 184(%rcx)
    movq %r12, 192(%rcx)
    movq %r13, 200(%rcx)
    movq %r14, 208(%rcx)
    movq %r15, 216(%rcx)
    // Save rsp (caller's return address is at the top — when the
    // counterpart ldp restores rsp, that same return address sits
    // there for `ret` to pop).
    movq %rsp, 224(%rcx)

    // Load callee-saved XMMs from *to (rdx).
    movdqa    0(%rdx), %xmm6
    movdqa   16(%rdx), %xmm7
    movdqa   32(%rdx), %xmm8
    movdqa   48(%rdx), %xmm9
    movdqa   64(%rdx), %xmm10
    movdqa   80(%rdx), %xmm11
    movdqa   96(%rdx), %xmm12
    movdqa  112(%rdx), %xmm13
    movdqa  128(%rdx), %xmm14
    movdqa  144(%rdx), %xmm15
    // Load callee-saved GPRs from *to (rdx).
    movq 160(%rdx), %rbx
    movq 168(%rdx), %rbp
    movq 176(%rdx), %rdi
    movq 184(%rdx), %rsi
    movq 192(%rdx), %r12
    movq 200(%rdx), %r13
    movq 208(%rdx), %r14
    movq 216(%rdx), %r15
    // Load rsp last. [rsp] holds either the caller's return
    // address (resume) or the initContext-injected trampoline
    // address (fresh coroutine). `ret` pops it.
    movq 224(%rdx), %rsp

    ret

// ─────────────────────────────────────────────────────────────────
// voltCoroEntry — naked trampoline. rbx holds the closure pointer
// (set by initContext, preserved through swap because rbx is
// callee-saved). Closure layout: first u64 is the run_fn pointer.
//
// Win64 ABI requires the caller to reserve 32 bytes of "shadow
// space" on the stack for the callee's first 4 register args
// (rcx, rdx, r8, r9) — even though we only pass one arg, we still
// need the shadow space allocation. After `sub rsp, 32`, the
// stack is 16-byte aligned + 8 (`ret` slot from a synthetic
// `call`) - 32 = 8 mod 16. Then `call` pushes 8 more bytes,
// making the callee's entry stack 0 mod 16 — what Win64 requires.
//
// If run_fn returns (it shouldn't — Volt coroutines yield via
// swap, never return), trap with int3.
// ─────────────────────────────────────────────────────────────────

.globl voltCoroEntry
.p2align 4
voltCoroEntry:
    movq %rbx, %rcx        // arg0 = closure pointer (Win64 arg0 reg)
    movq (%rbx), %rax      // rax  = closure.run_fn (offset 0)
    subq $32, %rsp         // reserve shadow space (Win64 ABI)
    callq *%rax            // call run_fn(closure)
    int3                   // run_fn shouldn't return; trap if it does
