//! v0.2 integration tests — async I/O across coroutines via the reactor.

const std = @import("std");
const volt = @import("../lib.zig");
const syscall = volt.internal.syscall;

// ─────────────────────────────────────────────────────────────────────────────
// 1. Pipe ping-pong: writer coro signals reader coro through async I/O.
// ─────────────────────────────────────────────────────────────────────────────

const PipeCtx = struct {
    rfd: i32,
    wfd: i32,
    received: [16]u8 = undefined,
    received_len: usize = 0,
};

fn pipeReader(ctx: *PipeCtx) !void {
    var buf: [16]u8 = undefined;
    const n = try volt.io.read(ctx.rfd, &buf);
    @memcpy(ctx.received[0..n], buf[0..n]);
    ctx.received_len = n;
}

fn pipeWriter(ctx: *PipeCtx) !void {
    // Yield once so the reader has a chance to register on the reactor first.
    try volt.yield();
    try volt.io.writeAll(ctx.wfd, "hello reactor!");
}

fn pipeRoot() !PipeCtx {
    const fds = try syscall.pipe();
    errdefer syscall.close(fds[0]);
    errdefer syscall.close(fds[1]);

    try volt.io.setNonblock(fds[0]);
    try volt.io.setNonblock(fds[1]);

    var ctx = PipeCtx{ .rfd = fds[0], .wfd = fds[1] };

    var reader = try volt.spawn(pipeReader, .{&ctx});
    defer volt.destroyTask(reader);
    var writer = try volt.spawn(pipeWriter, .{&ctx});
    defer volt.destroyTask(writer);

    try reader.join();
    try writer.join();

    syscall.close(fds[0]);
    syscall.close(fds[1]);

    return ctx;
}

test "v0.2: async pipe read/write across coroutines" {
    const ctx = try volt.run(std.testing.allocator, pipeRoot, .{});
    try std.testing.expectEqual(@as(usize, 14), ctx.received_len);
    try std.testing.expectEqualStrings("hello reactor!", ctx.received[0..ctx.received_len]);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Reader parks first, writer comes from outside the coroutine via reactor.
//    (Same as #1 but with explicit ordering: reader yields nothing — it goes
//    straight to a blocking read, demonstrating the park-on-EAGAIN path.)
// ─────────────────────────────────────────────────────────────────────────────

fn pipeReaderEager(ctx: *PipeCtx) !void {
    var buf: [16]u8 = undefined;
    const n = try volt.io.read(ctx.rfd, &buf);
    @memcpy(ctx.received[0..n], buf[0..n]);
    ctx.received_len = n;
}

fn pipeWriterEager(ctx: *PipeCtx) !void {
    // No yield — write immediately. Without async we'd need timing luck;
    // with the reactor, the reader either parks (and reactor wakes it) or
    // the data was buffered before the reader even tried.
    try volt.io.writeAll(ctx.wfd, "abc");
}

fn pipeRootEager() !PipeCtx {
    const fds = try syscall.pipe();
    try volt.io.setNonblock(fds[0]);
    try volt.io.setNonblock(fds[1]);

    var ctx = PipeCtx{ .rfd = fds[0], .wfd = fds[1] };

    var reader = try volt.spawn(pipeReaderEager, .{&ctx});
    defer volt.destroyTask(reader);
    var writer = try volt.spawn(pipeWriterEager, .{&ctx});
    defer volt.destroyTask(writer);

    try reader.join();
    try writer.join();

    syscall.close(fds[0]);
    syscall.close(fds[1]);
    return ctx;
}

test "v0.2: pipe write before reader parks (reactor handles either order)" {
    const ctx = try volt.run(std.testing.allocator, pipeRootEager, .{});
    try std.testing.expectEqualStrings("abc", ctx.received[0..ctx.received_len]);
}
