//! Darwin Parker — direct `__ulock_wait` / `__ulock_wake`.
//!
//! Darwin's libsystem exposes a futex-like primitive via two private
//! syscalls. The kernel implementation is essentially a Linux futex
//! with a different ABI. WebKit's `bmalloc` and Apple's `os_unfair_lock`
//! sit on top of it. Used here because pthread_cond on Darwin pulls in
//! pthread's mutex/cond bookkeeping (~600 ns/wake observed), while
//! ulock_wait+ulock_wake is a thin syscall (~150 ns/wake expected).
//!
//! References:
//! - https://opensource.apple.com/source/xnu/xnu-7195.50.7.100.1/bsd/sys/ulock.h.auto.html
//! - WebKit/Source/wtf/threads/Synchronous{Spin,Cross}ProcessLock.h
//!
//! State machine identical to parker_pthread but the wait/wake go
//! through ulock instead of pthread_cond.

const std = @import("std");

// Darwin ulock operation codes.
const UL_COMPARE_AND_WAIT: u32 = 1;

// Darwin ulock flags.
const ULF_NO_ERRNO: u32 = 0x01000000;

extern "c" fn __ulock_wait(operation: u32, addr: *anyopaque, value: u64, timeout: u32) c_int;
extern "c" fn __ulock_wake(operation: u32, addr: *anyopaque, wake_value: u64) c_int;

pub const Parker = struct {
    /// 32-bit atomic state. ulock compares the LOW 32 bits.
    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(EMPTY),

    pub const EMPTY: u32 = 0;
    pub const NOTIFIED: u32 = 1;
    pub const WAITING: u32 = 2;

    pub fn park(self: *Parker) void {
        // Fast path: notification stored before we entered park.
        if (self.state.cmpxchgStrong(NOTIFIED, EMPTY, .acquire, .monotonic) == null) {
            return;
        }
        // Transition EMPTY → WAITING; if we saw NOTIFIED in the CAS, the
        // unparker raced ahead — consume and return.
        if (self.state.cmpxchgStrong(EMPTY, WAITING, .seq_cst, .acquire)) |observed| {
            std.debug.assert(observed == NOTIFIED);
            self.state.store(EMPTY, .release);
            return;
        }
        // Wait while the state is WAITING.
        while (self.state.load(.acquire) == WAITING) {
            const op = UL_COMPARE_AND_WAIT | ULF_NO_ERRNO;
            // ulock_wait blocks while *addr (low 32 bits) == value.
            // Returns 0 on signal, <0 on error (ENOERR negated when
            // ULF_NO_ERRNO is set).
            _ = __ulock_wait(op, &self.state, WAITING, 0);
        }
        self.state.store(EMPTY, .release);
    }

    pub fn unpark(self: *Parker) void {
        const old = self.state.swap(NOTIFIED, .seq_cst);
        if (old == WAITING) {
            const op = UL_COMPARE_AND_WAIT | ULF_NO_ERRNO;
            // Wake one waiter. wake_value=0 means "wake one"; ULF_WAKE_ALL
            // flag (not used here) would wake all.
            _ = __ulock_wake(op, &self.state, 0);
        }
    }
};
