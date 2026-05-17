// System V x86-64 context switch for Volt.
//
// Two symbols:
//   * voltCtxSwap(from, to) — save callee-saved state to *from,
//     load it from *to, return into the new context.
//   * voltCoroEntry — naked entry trampoline. First `ret` after
//     swap lands here for a freshly-spawned coroutine. Reads the
//     closure pointer out of rbx (callee-saved, set by
//     initContext) and calls closure.run_fn(closure).
//
// Calling convention: System V x86-64. Args in rdi, rsi, rdx, rcx,
// r8, r9. Return in rax. Callee-saved GPRs: rbx, rbp, r12-r15.
// Stack must be 16-byte aligned immediately before any `call`.
//
// Mach-O (Darwin) prefixes C symbols with an underscore at
// link time; ELF (Linux + BSDs) doesn't. We emit both names so
// `extern fn voltCtxSwap` resolves on every host.

#if defined(__APPLE__)
#  define SYM(x) _##x
#else
#  define SYM(x) x
#endif

// SysVContext layout — must match src/context_x86_64.zig:
//   [from +  0]  rbx
//   [from +  8]  rbp
//   [from + 16]  r12
//   [from + 24]  r13
//   [from + 32]  r14
//   [from + 40]  r15
//   [from + 48]  rsp
// Total: 56 bytes.

.text

.globl SYM(voltCtxSwap)
.p2align 4
SYM(voltCtxSwap):
    // Save callee-saved GPRs to *from (rdi).
    movq %rbx,  0(%rdi)
    movq %rbp,  8(%rdi)
    movq %r12, 16(%rdi)
    movq %r13, 24(%rdi)
    movq %r14, 32(%rdi)
    movq %r15, 40(%rdi)
    // Save rsp. We're inside voltCtxSwap with the caller's return
    // address still on top of the stack — saving rsp here is fine;
    // when the corresponding ldp restores it, the same return
    // address sits at [rsp], and `ret` pops it.
    movq %rsp, 48(%rdi)

    // Load callee-saved GPRs from *to (rsi).
    movq  0(%rsi), %rbx
    movq  8(%rsi), %rbp
    movq 16(%rsi), %r12
    movq 24(%rsi), %r13
    movq 32(%rsi), %r14
    movq 40(%rsi), %r15
    // Load rsp last. Now [rsp] holds whatever was at the top of
    // the destination context's stack — either the caller's
    // return address (for a resume) or the initContext-injected
    // trampoline address (for a fresh coroutine).
    movq 48(%rsi), %rsp

    ret

// ─────────────────────────────────────────────────────────────────
// voltCoroEntry — naked trampoline. rbx holds the closure pointer
// (set by initContext, preserved through the swap because rbx is
// callee-saved). Closure layout: first u64 is the run_fn pointer.
// We:
//   1. Move the closure pointer to rdi (SysV arg 0).
//   2. Load run_fn from [closure + 0] into rax (scratch).
//   3. call rax. Stack is currently 16 mod 16 (top-of-stack is the
//      "return" we'd jump to if run_fn returned, but it never
//      should). `call` pushes 8 bytes, making the callee's
//      entry-point stack 8 mod 16 — which is what SysV requires
//      after `call` and before the callee's prologue.
//   4. If run_fn returns (it shouldn't — Volt's coroutines yield
//      to scheduler via swap, not return), trap with int3.
// ─────────────────────────────────────────────────────────────────

.globl SYM(voltCoroEntry)
.p2align 4
SYM(voltCoroEntry):
    movq %rbx, %rdi        // arg0 = closure pointer
    movq (%rbx), %rax      // rax = closure.run_fn (offset 0)
    callq *%rax            // call run_fn(closure)
    int3                   // run_fn shouldn't return; trap if it does

// Mark this section as not requiring an executable stack on ELF.
// Without this, modern Linux loaders refuse to load the binary
// or mark the whole executable as needing exec-stack (which
// hardening tools flag). Mach-O doesn't need this.
#if !defined(__APPLE__)
.section .note.GNU-stack,"",%progbits
#endif
