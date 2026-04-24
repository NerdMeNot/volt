//! Raw thread-level futex primitives.
//!
//! Volt owns these because Zig 0.16 moved futex support into std.Io
//! (parameterized by an Io handle). The scheduler can't thread an Io
//! through its own internals — it *is* the thing that would provide
//! the Io. So we port std.Io.Threaded's platform impls here.
//!
//! API:
//!   wait(ptr, expected)                -> blocks until wake or spurious
//!   timedWait(ptr, expected, ns)       -> blocks with timeout; returns error.Timeout
//!   wake(ptr, max_waiters)             -> wake up to N waiters
//!
//! Semantics mirror std.Thread.Futex from Zig 0.15 and std.Io.Threaded's
//! futexWaitUncancelable/futexWake in Zig 0.16. Spurious wakes are
//! allowed; callers must re-check the condition after waking.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const native_os = builtin.os.tag;

pub const TimedWaitError = error{Timeout};

/// Wait on `ptr` until it changes from `expected` or a wake() is received.
/// Returns immediately if `ptr.*` != `expected`.
/// Spurious wakes are possible — caller must re-check the condition.
pub fn wait(ptr: *const std.atomic.Value(u32), expected: u32) void {
    waitInner(&ptr.raw, expected, null) catch |err| switch (err) {
        error.Timeout => unreachable,
    };
}

/// Wait on `ptr` with a timeout in nanoseconds.
/// Returns error.Timeout if the timeout expires.
pub fn timedWait(
    ptr: *const std.atomic.Value(u32),
    expected: u32,
    timeout_ns: u64,
) TimedWaitError!void {
    return waitInner(&ptr.raw, expected, timeout_ns);
}

/// Wake up to `max_waiters` threads waiting on `ptr`.
/// Typical max_waiters values: 1 (signal) or std.math.maxInt(u32) (broadcast).
pub fn wake(ptr: *const std.atomic.Value(u32), max_waiters: u32) void {
    assert(max_waiters != 0);
    wakeInner(&ptr.raw, max_waiters);
}

// ─────────────────────────────────────────────────────────────────────────────
// Platform implementations — ported from std.Io.Threaded.
// ─────────────────────────────────────────────────────────────────────────────

fn waitInner(ptr: *const u32, expected: u32, timeout_ns: ?u64) TimedWaitError!void {
    @branchHint(.cold);

    if (builtin.single_threaded) unreachable; // nobody would ever wake us

    switch (native_os) {
        .linux => {
            const linux = std.os.linux;
            var ts_buffer: linux.timespec = undefined;
            const ts: ?*linux.timespec = if (timeout_ns) |ns| ts: {
                ts_buffer = timestampToPosix(ns);
                break :ts &ts_buffer;
            } else null;
            const rc = linux.futex_4arg(ptr, .{ .cmd = .WAIT, .private = true }, expected, ts);
            switch (linux.errno(rc)) {
                .SUCCESS => {}, // notified by wake()
                .INTR => {}, // caller re-checks
                .AGAIN => {}, // ptr.* != expected
                .INVAL => {}, // possibly timeout overflow — treat as spurious
                .TIMEDOUT => return error.Timeout,
                .FAULT => unreachable, // ptr invalid
                else => unreachable,
            }
        },
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => {
            const c = std.c;
            const flags: c.UL = .{
                .op = .COMPARE_AND_WAIT,
                .NO_ERRNO = true,
            };
            const status = switch (darwin_supports_ulock_wait2) {
                true => c.__ulock_wait2(flags, ptr, expected, ns: {
                    const ns = timeout_ns orelse break :ns 0;
                    if (ns == 0) break :ns 1;
                    break :ns ns;
                }, 0),
                false => c.__ulock_wait(flags, ptr, expected, us: {
                    const ns = timeout_ns orelse break :us 0;
                    const us = std.math.lossyCast(u32, ns / std.time.ns_per_us);
                    if (us == 0) break :us 1;
                    break :us us;
                }),
            };
            if (status >= 0) return;
            switch (@as(c.E, @enumFromInt(-status))) {
                .INTR => {}, // spurious wake
                .FAULT => {}, // paged out; caller retries
                .TIMEDOUT => return error.Timeout,
                else => unreachable,
            }
        },
        .windows => {
            const ms: std.os.windows.DWORD = if (timeout_ns) |ns|
                @intCast(@min(ns / std.time.ns_per_ms, std.math.maxInt(std.os.windows.DWORD) - 1))
            else
                std.os.windows.INFINITE;
            const rc = WaitOnAddress(
                @ptrCast(@constCast(ptr)),
                @ptrCast(@constCast(&expected)),
                @sizeOf(u32),
                ms,
            );
            if (rc == 0) {
                // WAIT_FAILED — check GetLastError; on timeout returns ERROR_TIMEOUT
                const err = std.os.windows.GetLastError();
                if (err == .TIMEOUT) return error.Timeout;
                // Other errors are unexpected; treat as spurious wake.
            }
        },
        .freebsd => {
            const flags = @intFromEnum(std.c.UMTX_OP.WAIT_UINT_PRIVATE);
            var tm_size: usize = 0;
            var tm: std.c._umtx_time = undefined;
            var tm_ptr: ?*const std.c._umtx_time = null;
            if (timeout_ns) |ns| {
                tm_ptr = &tm;
                tm_size = @sizeOf(@TypeOf(tm));
                tm.flags = 0;
                tm.clockid = 0; // CLOCK_REALTIME
                tm.timeout = timestampToPosix(ns);
            }
            const rc = std.c._umtx_op(
                @intFromPtr(ptr),
                flags,
                @as(c_ulong, expected),
                tm_size,
                @intFromPtr(tm_ptr),
            );
            switch (std.posix.errno(rc)) {
                .SUCCESS => {},
                .INTR => {},
                .FAULT => {},
                .TIMEDOUT => return error.Timeout,
                else => unreachable,
            }
        },
        .openbsd => {
            var ts_buffer: std.posix.timespec = undefined;
            const ts: ?*const std.posix.timespec = if (timeout_ns) |ns| ts: {
                ts_buffer = timestampToPosix(ns);
                break :ts &ts_buffer;
            } else null;
            const rc = std.c.futex(
                ptr,
                std.c.FUTEX.WAIT | std.c.FUTEX.PRIVATE_FLAG,
                @bitCast(expected),
                ts,
                null,
            );
            if (rc >= 0) return;
            switch (std.posix.errno(rc)) {
                .SUCCESS => {},
                .INTR => {},
                .AGAIN => {},
                .TIMEDOUT => return error.Timeout,
                else => unreachable,
            }
        },
        else => @compileError("unimplemented: futex wait on " ++ @tagName(native_os)),
    }
}

fn wakeInner(ptr: *const u32, max_waiters: u32) void {
    @branchHint(.cold);

    if (builtin.single_threaded) return;

    switch (native_os) {
        .linux => {
            const linux = std.os.linux;
            const rc = linux.futex_3arg(
                ptr,
                .{ .cmd = .WAKE, .private = true },
                @min(max_waiters, std.math.maxInt(i32)),
            );
            switch (linux.errno(rc)) {
                .SUCCESS => {},
                .INVAL => {},
                .FAULT => {},
                else => {},
            }
        },
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => {
            const c = std.c;
            const flags: c.UL = .{
                .op = .COMPARE_AND_WAIT,
                .NO_ERRNO = true,
                .WAKE_ALL = max_waiters > 1,
            };
            while (true) {
                const status = c.__ulock_wake(flags, ptr, 0);
                if (status >= 0) return;
                switch (@as(c.E, @enumFromInt(-status))) {
                    .INTR, .CANCELED => continue,
                    .FAULT => unreachable,
                    .NOENT => return,
                    .ALREADY => unreachable,
                    else => unreachable,
                }
            }
        },
        .windows => {
            if (max_waiters == 1) {
                WakeByAddressSingle(@ptrCast(@constCast(ptr)));
            } else {
                WakeByAddressAll(@ptrCast(@constCast(ptr)));
            }
        },
        .freebsd => {
            const rc = std.c._umtx_op(
                @intFromPtr(ptr),
                @intFromEnum(std.c.UMTX_OP.WAKE_PRIVATE),
                @as(c_ulong, @min(max_waiters, std.math.maxInt(c_int))),
                0,
                0,
            );
            switch (std.posix.errno(rc)) {
                .SUCCESS => {},
                .FAULT => {},
                else => {},
            }
        },
        .openbsd => {
            const rc = std.c.futex(
                ptr,
                std.c.FUTEX.WAKE | std.c.FUTEX.PRIVATE_FLAG,
                @min(max_waiters, std.math.maxInt(c_int)),
                null,
                null,
            );
            _ = rc;
        },
        else => @compileError("unimplemented: futex wake on " ++ @tagName(native_os)),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Platform helpers
// ─────────────────────────────────────────────────────────────────────────────

const TimespecT = switch (native_os) {
    .linux => std.os.linux.timespec,
    else => std.posix.timespec,
};

fn timestampToPosix(ns: u64) TimespecT {
    const SecT = @TypeOf(@as(TimespecT, undefined).sec);
    return .{
        .sec = std.math.cast(SecT, ns / std.time.ns_per_s) orelse std.math.maxInt(SecT),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
}

// __ulock_wait2 needs macOS 11+ / iOS 14+. On older macOS, fall back to __ulock_wait (microsecond timeout).
// Detection by comptime min OS version — matches what std.Io.Threaded does.
const darwin_supports_ulock_wait2 = switch (native_os) {
    .macos => blk: {
        const min = builtin.os.version_range.semver.min;
        break :blk min.major >= 11;
    },
    .ios, .maccatalyst, .tvos, .watchos, .visionos, .driverkit => true,
    else => false,
};

// Windows API bindings (Synchronization.h).
extern "api-ms-win-core-synch-l1-2-0" fn WaitOnAddress(
    Address: ?*volatile anyopaque,
    CompareAddress: ?*anyopaque,
    AddressSize: usize,
    dwMilliseconds: std.os.windows.DWORD,
) callconv(.winapi) std.os.windows.BOOL;

extern "api-ms-win-core-synch-l1-2-0" fn WakeByAddressSingle(
    Address: ?*anyopaque,
) callconv(.winapi) void;

extern "api-ms-win-core-synch-l1-2-0" fn WakeByAddressAll(
    Address: ?*anyopaque,
) callconv(.winapi) void;

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Futex - wait returns immediately when ptr != expected" {
    var v = std.atomic.Value(u32).init(42);
    // expected=0, actual=42 → returns immediately without blocking
    wait(&v, 0);
}

test "Futex - wake/wait round trip" {
    if (builtin.single_threaded) return error.SkipZigTest;

    var v = std.atomic.Value(u32).init(0);
    var flag = std.atomic.Value(u32).init(0);

    const Ctx = struct {
        v: *std.atomic.Value(u32),
        flag: *std.atomic.Value(u32),
        fn run(ctx: @This()) void {
            // Wait for main thread to set flag=1 before waking.
            while (ctx.flag.load(.acquire) == 0) std.atomic.spinLoopHint();
            ctx.v.store(1, .release);
            wake(ctx.v, 1);
        }
    };

    const t = try std.Thread.spawn(.{}, Ctx.run, .{Ctx{ .v = &v, .flag = &flag }});
    defer t.join();

    flag.store(1, .release);
    // Loop to handle spurious wakes.
    while (v.load(.acquire) == 0) wait(&v, 0);
    try std.testing.expectEqual(@as(u32, 1), v.load(.acquire));
}

test "Futex - timedWait returns Timeout" {
    if (builtin.single_threaded) return error.SkipZigTest;
    var v = std.atomic.Value(u32).init(0);
    // No one will wake us; expect Timeout after ~5ms.
    try std.testing.expectError(error.Timeout, timedWait(&v, 0, 5 * std.time.ns_per_ms));
}
