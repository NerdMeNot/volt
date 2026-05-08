//! P3.x.6 — cancellation propagates across I/O parks.
//!
//! When a coroutine is parked inside an I/O wait (read on a pipe
//! with no writer, in this test), cancelling its Job wakes the
//! park and the read returns `error.Cancelled` via the wait.zig
//! cancellation path.

const std = @import("std");
const volt = @import("../lib.zig");
const helpers = @import("helpers.zig");

const PipeCancelCtx = struct {
    rfd: std.posix.fd_t,
    wg: *helpers.WaitGroup,
    saw_cancelled: bool = false,
};

fn pipeReaderForCancel(ctx: *PipeCancelCtx) void {
    ctx.wg.done();
    var buf: [16]u8 = undefined;
    // Will park indefinitely — no writer ever sends data. Cancel
    // wakes the park; wait.zig surfaces error.Cancelled which
    // volt.io.lowlevel.read propagates.
    if (volt.io.lowlevel.read(ctx.rfd, &buf)) |_| {
        // Shouldn't reach here — there's no writer.
    } else |err| {
        if (err == error.Cancelled) ctx.saw_cancelled = true;
    }
}

fn pipeCancelRoot() !PipeCancelCtx {
    const fds = try volt.internal.syscall.pipe();
    defer volt.internal.syscall.close(fds[0]);
    defer volt.internal.syscall.close(fds[1]);
    try volt.io.lowlevel.setNonblock(fds[0]);
    try volt.io.lowlevel.setNonblock(fds[1]);

    var wg = helpers.WaitGroup.init(1);
    var ctx = PipeCancelCtx{ .rfd = fds[0], .wg = &wg };
    const reader = try volt.launch(pipeReaderForCancel, .{&ctx});
    defer volt.destroyJob(reader);

    try wg.wait(1 * std.time.ns_per_s);

    reader.cancel();
    // Job.join returns error.Cancelled when the child was cancelled —
    // we expect that. Swallow it; the assertion is on saw_cancelled.
    reader.join() catch {};
    return ctx;
}

test "P3.x.6: cancellation wakes a parked I/O read with error.Cancelled" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, pipeCancelRoot, .{});
    try std.testing.expect(ctx.saw_cancelled);
}
