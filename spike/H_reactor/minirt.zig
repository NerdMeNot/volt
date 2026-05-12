//! POC-H — minimal runtime + tight kqueue reactor.
//!
//! Single-worker spin scheduler (POC-C/POC-G) + inline kqueue poll
//! between queue pops. When the queue is empty, instead of spinning
//! blindly, the worker polls kqueue with a short timeout, picks up any
//! ready coros, runs them.
//!
//! Each yield-on-EAGAIN registers the coro's fd + direction, then
//! swaps out. The kqueue event fire re-pushes the coro to the queue.
//!
//! Designed for the bench: pipe-based ping-pong between two coros
//! (one writes 1 KB, one reads 1 KB, repeat). Throughput per RTT
//! should be comparable to TCP echo since the IO mechanics are
//! identical at the kqueue level.

const std = @import("std");
const ctx_mod = @import("ctx.zig");
const posix = std.posix;

pub const STACK_SIZE: usize = 16 * 1024;
const KEV_BATCH: usize = 16;

pub const Coroutine = struct {
    ctx: ctx_mod.Context = .{},
    main_ctx: *ctx_mod.Context = undefined,
    stack: []align(16) u8 = undefined,
    closure: extern struct {
        run_fn: *const fn (*anyopaque) callconv(.c) void,
        user_fn: *const fn (*Coroutine) callconv(.c) void,
        coro: ?*Coroutine = null,
    } = .{ .run_fn = &trampoline, .user_fn = undefined },
    next: ?*Coroutine = null,
    // 0 = runnable; non-zero = waiting on this fd for this direction.
    waiting_fd: i32 = 0,
    /// EVFILT_READ / EVFILT_WRITE
    waiting_filter: i16 = 0,
    rt: *Runtime = undefined,
};

fn trampoline(opaque_ptr: *anyopaque) callconv(.c) void {
    const closure: *@TypeOf(@as(Coroutine, undefined).closure) = @ptrCast(@alignCast(opaque_ptr));
    const coro = closure.coro.?;
    closure.user_fn(coro);
    // Coro returning = done. Swap back; scheduler decrements pending.
    ctx_mod.swap(&coro.ctx, coro.main_ctx);
    unreachable;
}

pub const RunQueue = struct {
    head: std.atomic.Value(?*Coroutine) = std.atomic.Value(?*Coroutine).init(null),

    pub fn push(self: *RunQueue, c: *Coroutine) void {
        var cur = self.head.load(.monotonic);
        while (true) {
            c.next = cur;
            if (self.head.cmpxchgWeak(cur, c, .release, .monotonic)) |observed| {
                cur = observed;
            } else return;
        }
    }

    pub fn pop(self: *RunQueue) ?*Coroutine {
        var cur = self.head.load(.acquire);
        while (cur) |c| {
            const next = c.next;
            if (self.head.cmpxchgWeak(cur, next, .acq_rel, .acquire)) |observed| {
                cur = observed;
            } else return c;
        }
        return null;
    }
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    queue: RunQueue = .{},
    main_ctx: ctx_mod.Context = .{},
    pending: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    kq: i32 = -1,
    current_coro: ?*Coroutine = null,

    pub fn init(allocator: std.mem.Allocator) !Runtime {
        const kq = std.c.kqueue();
        if (kq < 0) return error.KqueueInitFailed;
        return .{ .allocator = allocator, .kq = kq };
    }

    pub fn deinit(self: *Runtime) void {
        if (self.kq >= 0) _ = std.c.close(self.kq);
    }

    pub fn spawn(self: *Runtime, user_fn: *const fn (*Coroutine) callconv(.c) void) !*Coroutine {
        const coro = try self.allocator.create(Coroutine);
        const stack = try self.allocator.alignedAlloc(u8, .@"16", STACK_SIZE);
        coro.* = .{
            .stack = stack,
            .closure = .{
                .run_fn = &trampoline,
                .user_fn = user_fn,
                .coro = coro,
            },
            .rt = self,
        };
        const stack_top: [*]u8 = stack.ptr + STACK_SIZE;
        ctx_mod.initContext(&coro.ctx, stack_top, &coro.closure);
        coro.main_ctx = &self.main_ctx;
        _ = self.pending.fetchAdd(1, .acq_rel);
        self.queue.push(coro);
        return coro;
    }

    /// Yield the current coro, marking it as waiting on (fd, filter).
    /// Caller (in coro context) must set `current_coro.waiting_fd/filter`
    /// before calling this.
    pub fn yieldWait(self: *Runtime, coro: *Coroutine, fd: i32, filter: i16) void {
        coro.waiting_fd = fd;
        coro.waiting_filter = filter;
        // Register oneshot kevent.
        var kev = std.mem.zeroes(posix.Kevent);
        kev.ident = @intCast(fd);
        kev.filter = filter;
        kev.flags = posix.system.EV.ADD | posix.system.EV.ONESHOT;
        kev.udata = @intFromPtr(coro);
        // Submit and swap out in one syscall (no event read).
        var changes = [_]posix.Kevent{kev};
        var dummy: [1]posix.Kevent = undefined;
        _ = posix.system.kevent(self.kq, &changes, 1, &dummy, 0, null);
        ctx_mod.swap(&coro.ctx, &self.main_ctx);
    }

    /// Single-worker run. Spin: pop queue OR poll kqueue (short timeout),
    /// dispatch any ready coros. Park when truly idle.
    pub fn run(self: *Runtime) void {
        var events: [KEV_BATCH]posix.Kevent = undefined;
        while (self.pending.load(.acquire) > 0) {
            if (self.queue.pop()) |coro| {
                self.current_coro = coro;
                ctx_mod.swap(&self.main_ctx, &coro.ctx);
                self.current_coro = null;
                if (coro.waiting_fd != 0) {
                    // Coro yielded waiting on IO. Don't free; kqueue
                    // will deliver it.
                    continue;
                }
                // Coro returned (no waiting_fd set) = done.
                self.allocator.free(coro.stack);
                self.allocator.destroy(coro);
                _ = self.pending.fetchSub(1, .acq_rel);
                continue;
            }
            // Queue empty. Poll kqueue with NO timeout (block).
            const n = posix.system.kevent(self.kq, &.{}, 0, &events, KEV_BATCH, null);
            if (n > 0) {
                var i: usize = 0;
                while (i < @as(usize, @intCast(n))) : (i += 1) {
                    const e = events[i];
                    const coro: *Coroutine = @ptrFromInt(e.udata);
                    coro.waiting_fd = 0;
                    coro.waiting_filter = 0;
                    self.queue.push(coro);
                }
            }
        }
    }
};

// ─────────────────────────────────────────────────────────────────────
// IO helpers
// ─────────────────────────────────────────────────────────────────────

const F_GETFL: c_int = 3;
const F_SETFL: c_int = 4;
const O_NONBLOCK: c_int = 4;
extern "c" fn fcntl(fd: c_int, cmd: c_int, ...) c_int;
extern "c" fn read(fd: c_int, buf: [*]u8, count: usize) isize;
extern "c" fn write(fd: c_int, buf: [*]const u8, count: usize) isize;
extern "c" fn __error() *c_int;

inline fn errnoVal() c_int {
    return __error().*;
}

pub fn setNonblock(fd: i32) !void {
    const flags = fcntl(@intCast(fd), F_GETFL, @as(c_int, 0));
    if (flags < 0) return error.FcntlGetFailed;
    if (fcntl(@intCast(fd), F_SETFL, flags | O_NONBLOCK) < 0) return error.FcntlSetFailed;
}

pub fn readAsync(coro: *Coroutine, fd: i32, buf: []u8) !usize {
    var total: usize = 0;
    while (total < buf.len) {
        const r = read(@intCast(fd), buf.ptr + total, buf.len - total);
        if (r < 0) {
            const e = errnoVal();
            if (e == 35 or e == 11) { // EAGAIN on Darwin (35) / Linux (11)
                coro.rt.yieldWait(coro, fd, posix.system.EVFILT.READ);
                continue;
            }
            return error.ReadFailed;
        }
        if (r == 0) return total; // EOF
        total += @intCast(r);
    }
    return total;
}

pub fn writeAsync(coro: *Coroutine, fd: i32, buf: []const u8) !usize {
    var total: usize = 0;
    while (total < buf.len) {
        const w = write(@intCast(fd), buf.ptr + total, buf.len - total);
        if (w < 0) {
            const e = errnoVal();
            if (e == 35 or e == 11) {
                coro.rt.yieldWait(coro, fd, posix.system.EVFILT.WRITE);
                continue;
            }
            return error.WriteFailed;
        }
        total += @intCast(w);
    }
    return total;
}
