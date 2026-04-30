//! Raw thread sleep — replaces std.Thread.sleep (removed in Zig 0.16).
//!
//! Used by scheduler internals, timer driver fallbacks, and tests.
//! For async-task sleep, use volt.time.sleep (which yields to the scheduler).

const std = @import("std");
const builtin = @import("builtin");

const native_os = builtin.os.tag;

/// Sleep for at least `nanoseconds`. May sleep longer due to OS granularity.
/// May return earlier if interrupted (no restart-on-EINTR — matches
/// std.Thread.sleep's historical behavior).
pub fn sleep(nanoseconds: u64) void {
    switch (native_os) {
        .windows => {
            const ms: std.os.windows.DWORD = @intCast(@min(
                nanoseconds / std.time.ns_per_ms,
                std.math.maxInt(std.os.windows.DWORD) - 1,
            ));
            // std.os.windows.kernel32 lost `Sleep` in Zig 0.16; declare it
            // locally. The Win32 contract is: void return, infinite if ms==
            // INFINITE (0xFFFFFFFF).
            const Sleep = struct {
                extern "kernel32" fn Sleep(dwMilliseconds: std.os.windows.DWORD) callconv(.winapi) void;
            }.Sleep;
            Sleep(ms);
        },
        else => {
            const s = nanoseconds / std.time.ns_per_s;
            const ns = nanoseconds % std.time.ns_per_s;
            var req: std.posix.timespec = .{
                .sec = std.math.cast(@TypeOf(@as(std.posix.timespec, undefined).sec), s) orelse
                    std.math.maxInt(@TypeOf(@as(std.posix.timespec, undefined).sec)),
                .nsec = @intCast(ns),
            };
            var rem: std.posix.timespec = undefined;
            while (true) {
                const rc = std.posix.system.nanosleep(&req, &rem);
                if (rc == 0) return;
                switch (std.posix.errno(rc)) {
                    .INTR => {
                        req = rem;
                        continue;
                    },
                    else => return, // treat unexpected errors as "done"
                }
            }
        },
    }
}

test "sleep - 1ms sleeps at least ~1ms" {
    if (native_os == .windows) return error.SkipZigTest; // Windows time uses QPC; tested elsewhere
    // std.time.Timer is gone in 0.16; use raw clock_gettime.
    var ts0: std.posix.timespec = undefined;
    var ts1: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts0);
    sleep(1 * std.time.ns_per_ms);
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts1);

    const elapsed_ns =
        (@as(i128, ts1.sec) - @as(i128, ts0.sec)) * std.time.ns_per_s +
        (@as(i128, ts1.nsec) - @as(i128, ts0.nsec));

    // Generous bounds — OS scheduling jitter, CI variance.
    try std.testing.expect(elapsed_ns >= 500 * std.time.ns_per_us);
    try std.testing.expect(elapsed_ns < 100 * std.time.ns_per_ms);
}

test "sleep - zero returns quickly" {
    sleep(0);
}
