// x86_64 (System V ABI) coroutine context-switch primitives.
//
// Built and linked in by build.zig only when the target arch is
// x86_64. The matching Zig file (`context_x86_64.zig`) declares these
// as `extern fn`s.
//
// Linker symbol naming: Mach-O auto-prepends an underscore to extern
// decls, so the symbol on disk is `_voltCtxSwap`. ELF doesn't, so the
// symbol on disk is `voltCtxSwap`. We define both — `.set` is a
// no-cost alias.

#if defined(__APPLE__)
# define SYM(name) _##name
#else
# define SYM(name) name
#endif

.text

// ──── voltCtxSwap(from = %rdi, to = %rsi) ──────────────────────────
.globl SYM(voltCtxSwap)
.p2align 4
SYM(voltCtxSwap):
    movq %rbx,  0(%rdi)
    movq %rbp,  8(%rdi)
    movq %r12, 16(%rdi)
    movq %r13, 24(%rdi)
    movq %r14, 32(%rdi)
    movq %r15, 40(%rdi)
    movq %rsp, 48(%rdi)            // saved SP includes the call's ret addr
    movq  0(%rsi), %rbx
    movq  8(%rsi), %rbp
    movq 16(%rsi), %r12
    movq 24(%rsi), %r13
    movq 32(%rsi), %r14
    movq 40(%rsi), %r15
    movq 48(%rsi), %rsp
    ret                            // pops target's saved ret addr

// ──── voltCoroEntry ────────────────────────────────────────────────
//
// Trampoline run on the new coro's stack the FIRST time it resumes.
// `initContext` placed the closure pointer in ctx.rbx and pushed
// `&voltCoroEntry` as the "ret addr" at the top of the new stack.
// First resume:
//   1. ctx_swap restores rbx (= closure), rsp (= top - 8).
//   2. `ret` pops the pushed voltCoroEntry into rip — we land here
//      with %rsp pointing at the first usable byte of the coro's stack.
.globl SYM(voltCoroEntry)
.p2align 4
SYM(voltCoroEntry):
    movq %rbx, %rdi                // 1st arg = closure ptr
    movq (%rbx), %rax              // closure->run_fn (first field)
    callq *%rax                    // run_fn(closure)
    ud2                            // unreachable; trap if reached

#if defined(__linux__) || defined(__ELF__)
.section .note.GNU-stack,"",@progbits
#endif
