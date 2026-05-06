//! P1 trait-surface integration tests — exercise the trait + adapter
//! + copy machinery end-to-end via the runtime, with real fds.
//!
//! Coverage:
//!   1. `Fd` wraps a pipe; the producer writes; the consumer wraps
//!      the read end in a `BufReader` and pulls lines via
//!      `readUntil('\n')`. Verifies the trait-vtable refill path.
//!
//!   2. `lineIterator` over a `BufReader` over a pipe yields each
//!      line until clean EOF. Verifies the streaming-iterator chain.
//!
//!   3. `copy(dst, src)` from one pipe to another via two `Fd`
//!      wrappers. Verifies the copy() classic-loop path with real
//!      fd-backed Reader and Writer values.

const std = @import("std");
const volt = @import("../lib.zig");
const syscall = volt.internal.syscall;

// ─────────────────────────────────────────────────────────────────────
// Test 1 — BufReader.readUntil over an Fd-wrapped pipe
// ─────────────────────────────────────────────────────────────────────

const ReadUntilCtx = struct {
    rfd: std.posix.fd_t,
    wfd: std.posix.fd_t,
    line1: [16]u8 = undefined,
    line1_len: usize = 0,
    line2: [16]u8 = undefined,
    line2_len: usize = 0,
};

fn writerLines(ctx: *ReadUntilCtx) !void {
    try volt.yield(); // let the reader register on the reactor first
    try volt.io.lowlevel.writeAll(ctx.wfd, "alpha\nbravo\n");
    syscall.close(ctx.wfd);
}

fn readerLines(ctx: *ReadUntilCtx) !void {
    var fd = volt.io.Fd.init(ctx.rfd);
    var br = try volt.io.BufReader.init(std.testing.allocator, fd.reader(), 32);
    defer br.deinit();

    ctx.line1_len = try br.readUntil('\n', &ctx.line1);
    ctx.line2_len = try br.readUntil('\n', &ctx.line2);
}

fn readUntilRoot() !ReadUntilCtx {
    const fds = try syscall.pipe();
    try volt.io.lowlevel.setNonblock(fds[0]);
    try volt.io.lowlevel.setNonblock(fds[1]);

    var ctx = ReadUntilCtx{ .rfd = fds[0], .wfd = fds[1] };

    var reader = try volt.spawn(readerLines, .{&ctx});
    defer volt.destroyTask(reader);
    var writer = try volt.spawn(writerLines, .{&ctx});
    defer volt.destroyTask(writer);

    try reader.join();
    try writer.join();

    syscall.close(fds[0]);
    return ctx;
}

test "P1: BufReader.readUntil over Fd-wrapped pipe yields lines" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, readUntilRoot, .{});
    try std.testing.expectEqualStrings("alpha", ctx.line1[0..ctx.line1_len]);
    try std.testing.expectEqualStrings("bravo", ctx.line2[0..ctx.line2_len]);
}

// ─────────────────────────────────────────────────────────────────────
// Test 2 — lineIterator over a BufReader over a pipe
// ─────────────────────────────────────────────────────────────────────

const LineIterCtx = struct {
    rfd: std.posix.fd_t,
    wfd: std.posix.fd_t,
    lines: std.array_list.Managed([]u8),
};

fn writerMany(ctx: *LineIterCtx) !void {
    try volt.yield();
    try volt.io.lowlevel.writeAll(ctx.wfd, "first\nsecond\nthird\n");
    syscall.close(ctx.wfd);
}

fn readerMany(ctx: *LineIterCtx) !void {
    var fd = volt.io.Fd.init(ctx.rfd);
    var br = try volt.io.BufReader.init(std.testing.allocator, fd.reader(), 8);
    defer br.deinit();

    var it = volt.io.lineIterator(&br, std.testing.allocator, 64);
    while (try it.next()) |line| {
        try ctx.lines.append(line);
    }
}

fn lineIterRoot() !LineIterCtx {
    const fds = try syscall.pipe();
    try volt.io.lowlevel.setNonblock(fds[0]);
    try volt.io.lowlevel.setNonblock(fds[1]);

    var ctx = LineIterCtx{
        .rfd = fds[0],
        .wfd = fds[1],
        .lines = std.array_list.Managed([]u8).init(std.testing.allocator),
    };

    var reader = try volt.spawn(readerMany, .{&ctx});
    defer volt.destroyTask(reader);
    var writer = try volt.spawn(writerMany, .{&ctx});
    defer volt.destroyTask(writer);

    try reader.join();
    try writer.join();

    syscall.close(fds[0]);
    return ctx;
}

test "P1: lineIterator over pipe yields each line then EOF" {
    var ctx = try volt.run(.{ .allocator = std.testing.allocator }, lineIterRoot, .{});
    defer {
        for (ctx.lines.items) |l| std.testing.allocator.free(l);
        ctx.lines.deinit();
    }

    try std.testing.expectEqual(@as(usize, 3), ctx.lines.items.len);
    try std.testing.expectEqualStrings("first", ctx.lines.items[0]);
    try std.testing.expectEqualStrings("second", ctx.lines.items[1]);
    try std.testing.expectEqualStrings("third", ctx.lines.items[2]);
}

// ─────────────────────────────────────────────────────────────────────
// Test 3 — copy() from one pipe to another via Fd wrappers
// ─────────────────────────────────────────────────────────────────────

const CopyCtx = struct {
    src_r: std.posix.fd_t, // source pipe — read end
    src_w: std.posix.fd_t, // source pipe — write end (producer fills)
    dst_r: std.posix.fd_t, // dest pipe — read end (verifier drains)
    dst_w: std.posix.fd_t, // dest pipe — write end (copy() writes here)
    drained: [32]u8 = undefined,
    drained_len: usize = 0,
    copied: u64 = 0,
};

fn copyProducer(ctx: *CopyCtx) !void {
    try volt.yield();
    try volt.io.lowlevel.writeAll(ctx.src_w, "ship-via-copy");
    syscall.close(ctx.src_w);
}

fn copyDriver(ctx: *CopyCtx) !void {
    var src_fd = volt.io.Fd.init(ctx.src_r);
    var dst_fd = volt.io.Fd.init(ctx.dst_w);
    ctx.copied = try volt.io.copy(dst_fd.writer(), src_fd.reader());
    syscall.close(ctx.dst_w);
}

fn copyVerifier(ctx: *CopyCtx) !void {
    var buf: [32]u8 = undefined;
    var got: usize = 0;
    while (got < buf.len) {
        const n = try volt.io.lowlevel.read(ctx.dst_r, buf[got..]);
        if (n == 0) break;
        got += n;
    }
    @memcpy(ctx.drained[0..got], buf[0..got]);
    ctx.drained_len = got;
}

fn copyRoot() !CopyCtx {
    const src = try syscall.pipe();
    const dst = try syscall.pipe();
    inline for (.{ src[0], src[1], dst[0], dst[1] }) |fd| try volt.io.lowlevel.setNonblock(fd);

    var ctx = CopyCtx{
        .src_r = src[0],
        .src_w = src[1],
        .dst_r = dst[0],
        .dst_w = dst[1],
    };

    var prod = try volt.spawn(copyProducer, .{&ctx});
    defer volt.destroyTask(prod);
    var driver = try volt.spawn(copyDriver, .{&ctx});
    defer volt.destroyTask(driver);
    var verifier = try volt.spawn(copyVerifier, .{&ctx});
    defer volt.destroyTask(verifier);

    try prod.join();
    try driver.join();
    try verifier.join();

    syscall.close(src[0]);
    syscall.close(dst[0]);
    return ctx;
}

test "P1: copy() pipe -> pipe via Fd wrappers" {
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, copyRoot, .{});
    try std.testing.expectEqual(@as(u64, 13), ctx.copied);
    try std.testing.expectEqualStrings("ship-via-copy", ctx.drained[0..ctx.drained_len]);
}
