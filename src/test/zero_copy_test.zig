//! P3.x.3 — kernel zero-copy fills.
//!
//! Verifies `volt.io.copy(socket.writer(), file.reader())` actually
//! transfers bytes correctly via the sendfile path. We don't directly
//! observe which path was taken; this is a correctness test for the
//! dispatch (the wrong path would still return correct bytes — but
//! visible regression in throughput / syscall count would surface in
//! benchmarks).

const std = @import("std");
const volt = @import("../lib.zig");

const FdSocketCtx = struct {
    file_path: []const u8,
    payload: []const u8,
    received: [256]u8 = undefined,
    received_len: usize = 0,
};

fn cleanup(path: []const u8) void {
    var z_buf: [256]u8 = undefined;
    const path_z = std.fmt.bufPrintZ(&z_buf, "{s}", .{path}) catch return;
    _ = std.posix.system.unlink(path_z.ptr);
}

fn writeFileFixture(path: []const u8, payload: []const u8) !void {
    cleanup(path);
    var f = try volt.fs.File.create(path);
    try f.writeAll(payload);
    f.close();
}

const SenderArgs = struct {
    listener: *volt.net.TcpListener,
    file_path: []const u8,
};

fn senderCoro(args: SenderArgs) !void {
    var stream = try args.listener.accept();
    defer stream.close();

    var file = try volt.fs.File.openRead(args.file_path);
    defer file.close();

    // copy() should detect file→socket and dispatch to sendfile.
    _ = try volt.io.copy(stream.writer(), file.reader());
}

const ReceiverArgs = struct {
    addr: volt.net.Address,
    ctx: *FdSocketCtx,
};

fn receiverCoro(args: ReceiverArgs) !void {
    var client = try volt.net.TcpStream.connect(args.addr);
    defer client.close();

    var got: usize = 0;
    while (got < args.ctx.payload.len) {
        const n = try client.read(args.ctx.received[got..]);
        if (n == 0) break;
        got += n;
    }
    args.ctx.received_len = got;
}

fn fileToSocketRoot(ctx: *FdSocketCtx) !void {
    try writeFileFixture(ctx.file_path, ctx.payload);
    defer cleanup(ctx.file_path);

    var listener = try volt.net.TcpListener.bind(volt.net.Address.loopback4(0));
    defer listener.close();
    const local = try listener.localAddress();

    var sender = try volt.spawn(senderCoro, .{SenderArgs{ .listener = &listener, .file_path = ctx.file_path }});
    defer volt.destroyTask(sender);
    var receiver = try volt.spawn(receiverCoro, .{ReceiverArgs{ .addr = local, .ctx = ctx }});
    defer volt.destroyTask(receiver);

    try sender.join();
    try receiver.join();
}

test "P3.x.3: copy(socket.writer(), file.reader()) — sendfile dispatch path" {
    const payload = "kernel zero-copy file→socket via sendfile";
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/volt-zc-{d}.txt", .{std.posix.system.getpid()});

    var ctx = FdSocketCtx{ .file_path = path, .payload = payload };
    try volt.run(.{ .allocator = std.testing.allocator }, fileToSocketRoot, .{&ctx});

    try std.testing.expectEqual(payload.len, ctx.received_len);
    try std.testing.expectEqualStrings(payload, ctx.received[0..ctx.received_len]);
}
