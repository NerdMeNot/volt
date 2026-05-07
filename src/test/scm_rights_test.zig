//! P3.x.4 — SCM_RIGHTS fd-passing.
//!
//! Use socketpair(AF_UNIX) to get two pre-connected UnixStreams
//! without the listener/accept dance. One side opens a file and
//! sends its fd to the other; the other receives the fd, reads from
//! it, and verifies the bytes.

const std = @import("std");
const volt = @import("../lib.zig");

const ScmCtx = struct {
    a: volt.net.UnixStream,
    b: volt.net.UnixStream,
    file_path: []const u8,
    payload: []const u8,
    received: [128]u8 = undefined,
    received_len: usize = 0,
};

fn cleanup(path: []const u8) void {
    var z_buf: [256]u8 = undefined;
    const path_z = std.fmt.bufPrintZ(&z_buf, "{s}", .{path}) catch return;
    _ = std.posix.system.unlink(path_z.ptr);
}

fn senderCoro(ctx: *ScmCtx) !void {
    var f = try volt.fs.File.create(ctx.file_path);
    try f.writeAll(ctx.payload);
    f.close();

    var f2 = try volt.fs.File.openRead(ctx.file_path);
    defer f2.close();

    // Send the file fd along with a one-byte payload (SCM_RIGHTS
    // requires non-empty data).
    _ = try ctx.a.sendFds(&[_]std.posix.fd_t{f2.fd}, "x");
}

fn receiverCoro(ctx: *ScmCtx) !void {
    var buf: [16]u8 = undefined;
    const r = try ctx.b.recvFd(&buf);
    if (r.fd == null) return error.NoFdReceived;

    // Read the file content via the received (duplicated) fd.
    var got: usize = 0;
    while (got < ctx.payload.len) {
        const n = try volt.io.lowlevel.read(r.fd.?, ctx.received[got..]);
        if (n == 0) break;
        got += n;
    }
    ctx.received_len = got;
    _ = std.posix.system.close(r.fd.?);
}

fn scmRoot(ctx: *ScmCtx) !void {
    cleanup(ctx.file_path);

    // socketpair(AF_UNIX, SOCK_STREAM) — two pre-connected fds.
    var fds: [2]std.posix.fd_t = undefined;
    const rc = std.c.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds);
    if (rc != 0) return error.SocketPairFailed;
    try volt.io.lowlevel.setNonblock(fds[0]);
    try volt.io.lowlevel.setNonblock(fds[1]);
    ctx.a = .{ .fd = fds[0] };
    ctx.b = .{ .fd = fds[1] };
    defer ctx.a.close();
    defer ctx.b.close();
    defer cleanup(ctx.file_path);

    var sender = try volt.spawn(senderCoro, .{ctx});
    defer volt.destroyTask(sender);
    var receiver = try volt.spawn(receiverCoro, .{ctx});
    defer volt.destroyTask(receiver);

    try sender.join();
    try receiver.join();
}

// ─────────────────────────────────────────────────────────────────────
// P3.x.6 — SCM_RIGHTS edge cases (input validation)
// ─────────────────────────────────────────────────────────────────────

fn edgeRoot() !void {
    var fds: [2]std.posix.fd_t = undefined;
    const rc = std.c.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds);
    if (rc != 0) return error.SocketPairFailed;
    try volt.io.lowlevel.setNonblock(fds[0]);
    try volt.io.lowlevel.setNonblock(fds[1]);
    var a: volt.net.UnixStream = .{ .fd = fds[0] };
    var b: volt.net.UnixStream = .{ .fd = fds[1] };
    defer a.close();
    defer b.close();

    // Empty fds slice → InvalidArgument.
    try std.testing.expectError(
        error.InvalidArgument,
        a.sendFds(&[_]std.posix.fd_t{}, "x"),
    );

    // Empty payload → InvalidArgument (SCM_RIGHTS is ancillary; needs ≥ 1 byte data).
    const dummy_fd: std.posix.fd_t = -1;
    try std.testing.expectError(
        error.InvalidArgument,
        a.sendFds(&[_]std.posix.fd_t{dummy_fd}, ""),
    );

    // Too many fds → TooManyFds.
    var many: [32]std.posix.fd_t = undefined;
    @memset(&many, -1);
    try std.testing.expectError(
        error.TooManyFds,
        a.sendFds(&many, "x"),
    );
}

test "P3.x.6: SCM_RIGHTS edge cases" {
    try volt.run(.{ .allocator = std.testing.allocator }, edgeRoot, .{});
}

test "P3.x.4: SCM_RIGHTS — send file fd over UnixStream, peer reads it" {
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/volt-scm-{d}.txt", .{std.posix.system.getpid()});
    const payload = "fd-passing payload";

    var ctx = ScmCtx{
        .a = undefined,
        .b = undefined,
        .file_path = path,
        .payload = payload,
    };
    try volt.run(.{ .allocator = std.testing.allocator }, scmRoot, .{&ctx});

    try std.testing.expectEqual(payload.len, ctx.received_len);
    try std.testing.expectEqualStrings(payload, ctx.received[0..ctx.received_len]);
}
