//! Stack-overflow handler — turns guard-page hits into either:
//!   - **Stack growth** (commit one more page, retry the instruction), or
//!   - **Graceful coroutine failure** when the reserved range is exhausted
//!     (mark the coroutine, longjmp back to the scheduler).
//!
//! Either way, the process keeps running.
//!
//! ## Platform dispatch
//!
//! - POSIX (`stack_overflow_posix.zig`): SIGSEGV/SIGBUS sigaction +
//!   sigaltstack + sigsetjmp/siglongjmp.
//! - Windows (`stack_overflow_windows.zig`): Vectored Exception
//!   Handler + SetThreadStackGuarantee + _setjmp/longjmp.
//!
//! Both expose the same surface: `installPerThread`,
//! `beginDispatch` / `endDispatch`, `setjmpDispatch`,
//! `longjmpDispatch`, `currentCheckpoint`, plus a `DispatchCheckpoint`
//! type. Callers don't see the platform split.
//!
//! Layered architecture (unchanged from v1.0):
//!   - `stack.zig` owns the per-coroutine reservation and the
//!     `tryGrow` primitive (the `mprotect` / `VirtualAlloc(MEM_COMMIT)`).
//!   - This module owns the process-wide signal/exception handler and
//!     the per-thread checkpoint state.

const std = @import("std");
const builtin = @import("builtin");

pub const impl = switch (builtin.os.tag) {
    .windows => @import("stack_overflow_windows.zig"),
    .macos, .ios, .linux, .freebsd, .netbsd, .dragonfly => @import("stack_overflow_posix.zig"),
    else => @import("stack_overflow_stub.zig"),
};

pub const SigJmpBuf = impl.SigJmpBuf;
pub const DispatchCheckpoint = impl.DispatchCheckpoint;

pub const installPerThread = impl.installPerThread;
pub const beginDispatch = impl.beginDispatch;
pub const endDispatch = impl.endDispatch;
pub const currentCheckpoint = impl.currentCheckpoint;
pub const longjmpDispatch = impl.longjmpDispatch;
pub const setjmpDispatch = impl.setjmpDispatch;

test {
    _ = impl;
}
