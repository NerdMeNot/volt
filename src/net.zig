//! TCP networking — TcpListener + TcpStream over the kqueue reactor.
//! IPv4 / loopback today; IPv6 + DNS later.
//!
//! All blocking ops yield to the reactor on EAGAIN. The API looks
//! synchronous; suspend points are transparent.

const std = @import("std");
const posix = std.posix;
const reactor_mod = @import("reactor_kqueue.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");

const AF_INET: c_int = 2;
const SOCK_STREAM: c_int = 1;
const IPPROTO_TCP: c_int = 6;
const SOL_SOCKET: c_int = 0xFFFF; // Darwin
const SO_REUSEADDR: c_int = 0x0004;
const EINPROGRESS_DARWIN: c_int = 36;

const sockaddr_in = extern struct {
    len: u8 = @sizeOf(sockaddr_in),
    family: u8 = AF_INET,
    port: u16, // network order
    addr: u32, // network order
    zero: [8]u8 = @splat(0),
};

// Use c_-prefixed names to avoid shadowing user method names like
// `TcpListener.close` / `.bind`. Each extern is name-mapped to its
// real C symbol via the @extern intrinsic.
const socket = @extern(*const fn (c_int, c_int, c_int) callconv(.c) c_int, .{ .name = "socket" });
const c_bind = @extern(*const fn (c_int, *const anyopaque, c_uint) callconv(.c) c_int, .{ .name = "bind" });
const c_listen = @extern(*const fn (c_int, c_int) callconv(.c) c_int, .{ .name = "listen" });
const c_accept = @extern(*const fn (c_int, ?*anyopaque, ?*c_uint) callconv(.c) c_int, .{ .name = "accept" });
const c_connect = @extern(*const fn (c_int, *const anyopaque, c_uint) callconv(.c) c_int, .{ .name = "connect" });
const setsockopt = @extern(*const fn (c_int, c_int, c_int, *const anyopaque, c_uint) callconv(.c) c_int, .{ .name = "setsockopt" });
const getsockname = @extern(*const fn (c_int, *anyopaque, *c_uint) callconv(.c) c_int, .{ .name = "getsockname" });
const c_close = @extern(*const fn (c_int) callconv(.c) c_int, .{ .name = "close" });
const c_error = @extern(*const fn () callconv(.c) *c_int, .{ .name = "__error" });
inline fn errnoVal() c_int {
    return c_error().*;
}

const F_GETFL: c_int = 3;
const F_SETFL: c_int = 4;
const O_NONBLOCK: c_int = 4;
extern "c" fn fcntl(fd: c_int, cmd: c_int, ...) c_int;

fn setNonblock(fd: c_int) !void {
    const flags = fcntl(fd, F_GETFL, @as(c_int, 0));
    if (flags < 0) return error.FcntlGetFailed;
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return error.FcntlSetFailed;
}

pub const Address = struct {
    /// IPv4 host bytes in host order (192.168.1.1 → {192,168,1,1}).
    host: [4]u8,
    /// Port in host order.
    port: u16,

    pub fn loopback(port: u16) Address {
        return .{ .host = .{ 127, 0, 0, 1 }, .port = port };
    }

    pub fn any(port: u16) Address {
        return .{ .host = .{ 0, 0, 0, 0 }, .port = port };
    }

    fn toSockaddr(self: Address) sockaddr_in {
        const addr_he: u32 = (@as(u32, self.host[0]) << 24) | (@as(u32, self.host[1]) << 16) |
            (@as(u32, self.host[2]) << 8) | @as(u32, self.host[3]);
        return .{
            .port = std.mem.nativeToBig(u16, self.port),
            .addr = std.mem.nativeToBig(u32, addr_he),
        };
    }

    fn fromSockaddr(sa: sockaddr_in) Address {
        const addr_he = std.mem.bigToNative(u32, sa.addr);
        return .{
            .host = .{
                @intCast((addr_he >> 24) & 0xFF),
                @intCast((addr_he >> 16) & 0xFF),
                @intCast((addr_he >> 8) & 0xFF),
                @intCast(addr_he & 0xFF),
            },
            .port = std.mem.bigToNative(u16, sa.port),
        };
    }
};

pub const TcpListener = struct {
    fd: i32,

    pub fn bindAddress(addr: Address) !TcpListener {
        const fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (fd < 0) return error.SocketCreateFailed;
        errdefer _ = c_close(fd);

        // SO_REUSEADDR so repeated test runs don't get EADDRINUSE.
        const one: c_int = 1;
        _ = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, @sizeOf(c_int));

        const sa = addr.toSockaddr();
        if (c_bind(fd, &sa, @sizeOf(sockaddr_in)) < 0) return error.BindFailed;
        if (c_listen(fd, 128) < 0) return error.ListenFailed;
        try setNonblock(fd);
        return .{ .fd = @intCast(fd) };
    }

    pub fn close(self: *TcpListener) void {
        _ = c_close(@intCast(self.fd));
    }

    /// Get the bound local address (useful when bind port = 0).
    pub fn localAddress(self: *const TcpListener) !Address {
        var sa: sockaddr_in = undefined;
        var len: c_uint = @sizeOf(sockaddr_in);
        if (getsockname(@intCast(self.fd), &sa, &len) < 0) return error.GetSockNameFailed;
        return Address.fromSockaddr(sa);
    }

    /// Accept the next incoming connection. Yields to the reactor on EAGAIN.
    pub fn accept(self: *TcpListener) !TcpStream {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        while (true) {
            const new_fd = c_accept(@intCast(self.fd), null, null);
            if (new_fd >= 0) {
                try setNonblock(new_fd);
                return .{ .fd = @intCast(new_fd) };
            }
            if (!isAgain(errnoVal())) return error.AcceptFailed;
            rt.reactor.waitReadable(self.fd);
        }
    }
};

pub const TcpStream = struct {
    fd: i32,

    pub fn connect(addr: Address) !TcpStream {
        const fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (fd < 0) return error.SocketCreateFailed;
        errdefer _ = c_close(fd);
        try setNonblock(fd);

        const sa = addr.toSockaddr();
        if (c_connect(fd, &sa, @sizeOf(sockaddr_in)) < 0) {
            const e = errnoVal();
            if (e != EINPROGRESS_DARWIN) return error.ConnectFailed;
            // Wait for writable = connect completed.
            const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
            rt.reactor.waitWritable(fd);
        }
        return .{ .fd = @intCast(fd) };
    }

    pub fn close(self: *TcpStream) void {
        _ = c_close(@intCast(self.fd));
    }

    pub fn read(self: *TcpStream, buf: []u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        return reactor_mod.readAsync(&rt.reactor, self.fd, buf);
    }

    pub fn write(self: *TcpStream, buf: []const u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        return reactor_mod.writeAsync(&rt.reactor, self.fd, buf);
    }

    pub fn writeAll(self: *TcpStream, buf: []const u8) !void {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        return reactor_mod.writeAll(&rt.reactor, self.fd, buf);
    }

    pub fn readFull(self: *TcpStream, buf: []u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        return reactor_mod.readFull(&rt.reactor, self.fd, buf);
    }
};

const EAGAIN_DARWIN: c_int = 35;
const EAGAIN_LINUX: c_int = 11;
inline fn isAgain(e: c_int) bool {
    return e == EAGAIN_DARWIN or e == EAGAIN_LINUX;
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const test_allocator = std.testing.allocator;

fn testListenerLifecycle() !void {
    var listener = try TcpListener.bindAddress(Address.loopback(0));
    defer listener.close();
    const addr = try listener.localAddress();
    if (addr.port == 0) return error.BadPort;
}

test "TcpListener: bind + localAddress" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    try rt.run(testListenerLifecycle, .{});
}

const EchoTestCtx = struct {
    listener: *TcpListener,
    server_done: bool = false,
    client_done: bool = false,
    addr: Address = undefined,
    result_byte: u8 = 0,
};

fn testEchoServer(ctx: *EchoTestCtx) !void {
    var stream = try ctx.listener.accept();
    defer stream.close();
    var buf: [4]u8 = undefined;
    _ = try stream.readFull(&buf);
    try stream.writeAll(&buf);
    ctx.server_done = true;
}

fn testEchoClient(ctx: *EchoTestCtx) !void {
    var stream = try TcpStream.connect(ctx.addr);
    defer stream.close();
    const send_buf = [_]u8{ 0xAB, 0xCD, 0xEF, 0x42 };
    try stream.writeAll(&send_buf);
    var recv_buf: [4]u8 = undefined;
    _ = try stream.readFull(&recv_buf);
    ctx.result_byte = recv_buf[3];
    ctx.client_done = true;
}

fn echoTestRoot(ctx: *EchoTestCtx) !void {
    const cur = @import("current.zig");
    const rt: *runtime.Runtime = @ptrCast(@alignCast(cur.require().runtime));
    var server_task = try rt.spawn(testEchoServer, .{ctx});
    var client_task = try rt.spawn(testEchoClient, .{ctx});
    _ = server_task.join() catch |err| return err;
    _ = client_task.join() catch |err| return err;
}

test "TCP echo single client round-trip" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();

    var listener = try TcpListener.bindAddress(Address.loopback(0));
    defer listener.close();
    var ctx = EchoTestCtx{ .listener = &listener };
    ctx.addr = try listener.localAddress();
    try rt.run(echoTestRoot, .{&ctx});

    try std.testing.expect(ctx.server_done);
    try std.testing.expect(ctx.client_done);
    try std.testing.expectEqual(@as(u8, 0x42), ctx.result_byte);
}
