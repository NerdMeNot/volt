//! Control Parker — pthread_mutex + pthread_cond directly via libpthread.
//!
//! Zig 0.16 std no longer exposes `std.Thread.Mutex` / `.Condition`. We
//! use the pthread C ABI directly so this POC measures exactly what
//! Volt's current Parker (which routes through `volt.internal.thread`)
//! ultimately calls.

const std = @import("std");

// Opaque pthread_t types. Sizes from /usr/include/sys/_pthread/_pthread_types.h
// on macOS:
//   pthread_mutex_t = 64 bytes
//   pthread_cond_t  = 48 bytes
// Plus the leading magic word that pthread uses to detect static init.
// We zero-init and let pthread_*_init populate them.
const PthreadMutex = extern struct { _opaque: [64]u8 = @splat(0) };
const PthreadCond = extern struct { _opaque: [48]u8 = @splat(0) };

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
    initialized: bool = false,

    pub const EMPTY: u8 = 0;
    pub const NOTIFIED: u8 = 1;
    pub const WAITING: u8 = 2;

    pub fn init(self: *Parker) void {
        if (self.initialized) return;
        _ = pthread_mutex_init(&self.mutex, null);
        _ = pthread_cond_init(&self.cond, null);
        self.initialized = true;
    }

    pub fn deinit(self: *Parker) void {
        if (!self.initialized) return;
        _ = pthread_cond_destroy(&self.cond);
        _ = pthread_mutex_destroy(&self.mutex);
        self.initialized = false;
    }

    pub fn park(self: *Parker) void {
        if (self.state.cmpxchgStrong(NOTIFIED, EMPTY, .acquire, .monotonic) == null) {
            return;
        }
        _ = pthread_mutex_lock(&self.mutex);
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

    pub fn unpark(self: *Parker) void {
        const old = self.state.swap(NOTIFIED, .seq_cst);
        if (old == WAITING) {
            _ = pthread_mutex_lock(&self.mutex);
            _ = pthread_cond_signal(&self.cond);
            _ = pthread_mutex_unlock(&self.mutex);
        }
    }
};
