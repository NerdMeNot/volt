//! Per-worker Parker — cross-thread sleep/wake via the OS futex
//! equivalent (Darwin `__ulock_wait`, Linux `FUTEX_WAIT`).
//!
//! Used by workers when they have no local work, no global work, no
//! steal target, and the reactor is idle. Other paths (cross-thread
//! spawn, reactor delivery, sync unpark) call `parker.unpark()` to
//! wake the parked worker.
//!
//! State machine (`u32` atomic — futex word):
//!   EMPTY     — no pending wake, no waiter
//!   NOTIFIED  — wake stored before park (fast path: park consumes and
//!               returns without a syscall)
//!   WAITING   — the waiter is blocked in the futex syscall
//!
//! Why not pthread_cond? Pthread carries ~600 ns/wake of bookkeeping
//! overhead on Darwin. The underlying primitive is `__ulock_wait` /
//! `__ulock_wake` (also what `os_unfair_lock` uses internally). Going
//! direct shaves ~250 ns/cycle and drops a dependency on libpthread
//! internals.

const std = @import("std");
const builtin = @import("builtin");

pub const Parker = struct {
    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(EMPTY),

    pub const EMPTY: u32 = 0;
    pub const NOTIFIED: u32 = 1;
    pub const WAITING: u32 = 2;

    pub fn init(self: *Parker) void {
        self.state.store(EMPTY, .release);
    }

    pub fn deinit(self: *Parker) void {
        _ = self;
    }

    /// Block until `unpark()` is called. Consumes a pre-stored
    /// NOTIFIED without a syscall.
    pub fn park(self: *Parker) void {
        // Fast path: notification already stored.
        if (self.state.cmpxchgStrong(NOTIFIED, EMPTY, .acquire, .monotonic) == null) {
            return;
        }
        // EMPTY → WAITING. seq_cst pairs with unpark's seq_cst swap so
        // a concurrent unpark either sees us as WAITING and wakes us,
        // or stored NOTIFIED before we observed EMPTY (in which case
        // we'd have taken the fast path).
        if (self.state.cmpxchgStrong(EMPTY, WAITING, .seq_cst, .acquire)) |observed| {
            std.debug.assert(observed == NOTIFIED);
            self.state.store(EMPTY, .release);
            return;
        }
        // Loop on syscall — handles spurious wakes (and the case
        // where the futex word changed underneath us between the CAS
        // and the syscall entry).
        while (self.state.load(.acquire) == WAITING) {
            futexWait(&self.state, WAITING);
        }
        self.state.store(EMPTY, .release);
    }

    /// Wake the parked waiter (or store NOTIFIED for a future park).
    pub fn unpark(self: *Parker) void {
        const old = self.state.swap(NOTIFIED, .seq_cst);
        if (old == WAITING) {
            futexWake(&self.state);
        }
    }

    pub fn isParked(self: *const Parker) bool {
        return self.state.load(.acquire) == WAITING;
    }
};

// ─────────────────────────────────────────────────────────────────────
// Platform-specific futex backends
// ─────────────────────────────────────────────────────────────────────

const is_darwin = builtin.os.tag.isDarwin();
const is_linux = builtin.os.tag == .linux;

// ── Darwin: __ulock_wait / __ulock_wake ──────────────────────────────
//
// Operations:
//   UL_COMPARE_AND_WAIT = 1  — wait while *addr (low 32 bits) == value
//   ULF_NO_ERRNO   = 0x01000000  — return negated errno instead of -1
//
// Reference:
//   https://opensource.apple.com/source/xnu/xnu-7195.50.7.100.1/bsd/sys/ulock.h.auto.html

const UL_COMPARE_AND_WAIT: u32 = 1;
const ULF_NO_ERRNO: u32 = 0x01000000;

extern "c" fn __ulock_wait(operation: u32, addr: *anyopaque, value: u64, timeout: u32) c_int;
extern "c" fn __ulock_wake(operation: u32, addr: *anyopaque, wake_value: u64) c_int;

// ── Linux: futex(FUTEX_WAIT_PRIVATE / FUTEX_WAKE_PRIVATE) ────────────

const FUTEX_WAIT_PRIVATE: i32 = 128 | 0;
const FUTEX_WAKE_PRIVATE: i32 = 128 | 1;

fn futexWait(addr: *std.atomic.Value(u32), expected: u32) void {
    if (comptime is_darwin) {
        const op = UL_COMPARE_AND_WAIT | ULF_NO_ERRNO;
        // ulock_wait blocks while *addr == expected.
        _ = __ulock_wait(op, addr, expected, 0);
    } else if (comptime is_linux) {
        const rc = std.os.linux.futex_wait(@ptrCast(addr), FUTEX_WAIT_PRIVATE, @intCast(expected), null);
        _ = rc;
    } else {
        @compileError("Parker: no futex backend for this OS (kqueue+self-pipe fallback can be added)");
    }
}

fn futexWake(addr: *std.atomic.Value(u32)) void {
    if (comptime is_darwin) {
        const op = UL_COMPARE_AND_WAIT | ULF_NO_ERRNO;
        // wake_value = 0 → wake one waiter; ULF_WAKE_ALL (not used) for broadcast.
        _ = __ulock_wake(op, addr, 0);
    } else if (comptime is_linux) {
        const rc = std.os.linux.futex_wake(@ptrCast(addr), FUTEX_WAKE_PRIVATE, 1);
        _ = rc;
    } else {
        @compileError("Parker: no futex backend for this OS");
    }
}
