//! Per-worker Parker — cross-thread sleep/wake via pthread_cond.
//!
//! Each Worker has a Parker. When a worker has no work AND no
//! steal targets AND the reactor is empty, it parks on its
//! Parker. Other paths (cross-thread spawn, reactor poll, sync
//! unpark) call `parker.unpark()` to wake it.
//!
//! State machine:
//!   EMPTY     — no pending wake, no waiter
//!   NOTIFIED  — wake stored before park (fast path: park returns
//!               immediately and consumes the notification)
//!   WAITING   — the waiter is blocked in `condition.wait`
//!
//! POC-D measured pthread_cond on Darwin at ~1.7 µs/wake RT. ulock
//! direct is ~1.4 µs (~15 % savings). For first multi-worker pass we
//! use pthread for cross-platform compatibility (Linux/Darwin both
//! have it). ulock + futex can swap in later (Phase 3.x perf pass).

const std = @import("std");

// pthread types — Darwin/Linux compatible. Sized to fit both, and
// 8-byte aligned because pthread_mutex_t's first field is `long`
// (Darwin) / requires natural alignment on Linux. Using `u64` slots
// gives us the alignment for free.
const PthreadMutex = extern struct { _opaque: [8]u64 = @splat(0) };
const PthreadCond = extern struct { _opaque: [6]u64 = @splat(0) };

extern "c" fn pthread_mutex_init(m: *PthreadMutex, attr: ?*anyopaque) c_int;
extern "c" fn pthread_mutex_destroy(m: *PthreadMutex) c_int;
extern "c" fn pthread_mutex_lock(m: *PthreadMutex) c_int;
extern "c" fn pthread_mutex_unlock(m: *PthreadMutex) c_int;
extern "c" fn pthread_cond_init(c: *PthreadCond, attr: ?*anyopaque) c_int;
extern "c" fn pthread_cond_destroy(c: *PthreadCond) c_int;
extern "c" fn pthread_cond_wait(c: *PthreadCond, m: *PthreadMutex) c_int;
extern "c" fn pthread_cond_signal(c: *PthreadCond) c_int;

pub const Parker = struct {
    mutex: PthreadMutex = .{},
    cond: PthreadCond = .{},
    state: std.atomic.Value(u8) = std.atomic.Value(u8).init(EMPTY),

    pub const EMPTY: u8 = 0;
    pub const NOTIFIED: u8 = 1;
    pub const WAITING: u8 = 2;

    pub fn init(self: *Parker) void {
        _ = pthread_mutex_init(&self.mutex, null);
        _ = pthread_cond_init(&self.cond, null);
    }

    pub fn deinit(self: *Parker) void {
        _ = pthread_cond_destroy(&self.cond);
        _ = pthread_mutex_destroy(&self.mutex);
    }

    /// Block until `unpark` is called. If a notification was stored
    /// before we entered, consume it and return immediately (no
    /// syscall).
    pub fn park(self: *Parker) void {
        // Fast path: notification already stored.
        if (self.state.cmpxchgStrong(NOTIFIED, EMPTY, .acquire, .monotonic) == null) {
            return;
        }
        _ = pthread_mutex_lock(&self.mutex);
        // Transition EMPTY → WAITING. If we observe NOTIFIED, the
        // unparker raced ahead between our two reads; consume it.
        if (self.state.cmpxchgStrong(EMPTY, WAITING, .seq_cst, .acquire)) |observed| {
            std.debug.assert(observed == NOTIFIED);
            self.state.store(EMPTY, .release);
            _ = pthread_mutex_unlock(&self.mutex);
            return;
        }
        while (self.state.load(.acquire) == WAITING) {
            _ = pthread_cond_wait(&self.cond, &self.mutex);
        }
        self.state.store(EMPTY, .release);
        _ = pthread_mutex_unlock(&self.mutex);
    }

    /// Wake the parked waiter (or store a notification for a future
    /// park). Safe to call from any thread.
    pub fn unpark(self: *Parker) void {
        const old = self.state.swap(NOTIFIED, .seq_cst);
        if (old == WAITING) {
            _ = pthread_mutex_lock(&self.mutex);
            _ = pthread_cond_signal(&self.cond);
            _ = pthread_mutex_unlock(&self.mutex);
        }
    }

    /// Cheap probe: is this parker currently sleeping?
    pub fn isParked(self: *const Parker) bool {
        return self.state.load(.acquire) == WAITING;
    }
};
