//! Unix domain sockets — IPC on the local filesystem.
//!
//! POSIX-only. On Windows every public type collapses to `void` so
//! `volt.net.UnixStream` etc. fail at compile time with a clean
//! "expected type, got void" error — saves users a confusing
//! runtime crash if they accidentally use Unix sockets on Win32.
//!
//! Two kinds:
//!   * **Stream** (`UnixStream` + `UnixListener`) — connection-
//!     oriented, byte-stream. Same shape as TcpStream + TcpListener.
//!   * **Datagram** (`UnixDatagram`) — connection-less, message-
//!     oriented. Same shape as UdpSocket.
//!
//! Addresses (`UnixAddr`) come in two flavours:
//!   * **Path** — a filesystem path (`/tmp/foo.sock`); the path is
//!     a real file that the listener creates and the client opens.
//!     Length capped at `sun_path` size (108 bytes on Linux, 104 on
//!     Darwin / BSD).
//!   * **Abstract** (Linux-only) — name in an abstract namespace
//!     identified by a leading NUL byte. No filesystem entry; no
//!     cleanup needed. Use `UnixAddr.fromAbstract(name)`.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const current = @import("../current.zig");
const runtime = @import("../runtime.zig");
const reactor_mod = @import("../reactor.zig");
const cancel_mod = @import("../cancel.zig");
const poll_desc = @import("../poll_desc.zig");
const pd_handle = @import("../pd_handle.zig");
const options = @import("options.zig");

const is_windows = builtin.os.tag == .windows;
const is_linux = builtin.os.tag == .linux;

// On Windows, expose `void` aliases so user code that mentions
// UnixStream / etc. fails at compile time with "expected type" —
// clearer than a runtime "not supported" error.
pub const UnixAddr = if (is_windows) void else AddrImpl;
pub const UnixStream = if (is_windows) void else StreamImpl;
pub const UnixListener = if (is_windows) void else ListenerImpl;
pub const UnixDatagram = if (is_windows) void else DatagramImpl;

// Everything below is POSIX-only — wrapped in a comptime block so
// the Windows build never compiles it.

const AddrImpl = if (is_windows) void else struct {
    inner: posix.sockaddr.un,
    len: posix.socklen_t,

    pub const PathError = error{ PathTooLong, EmptyPath };
    pub const AbstractError = error{ NameTooLong, NotLinux };

    /// Construct from a filesystem path. The path is copied into
    /// the address; the actual file is created by the listener's
    /// `bind` and removed by `listener.close()` if you call it.
    pub fn fromPath(p: []const u8) PathError!AddrImpl {
        if (p.len == 0) return error.EmptyPath;
        // Reserve one byte for trailing NUL.
        if (p.len >= sunPathLen()) return error.PathTooLong;
        var sa = posix.sockaddr.un{ .family = posix.AF.UNIX, .path = undefined };
        @memset(&sa.path, 0);
        @memcpy(sa.path[0..p.len], p);
        const family_off = @offsetOf(posix.sockaddr.un, "path");
        return .{
            .inner = sa,
            .len = @intCast(family_off + p.len + 1),
        };
    }

    /// Linux abstract namespace — name lives in kernel state, no
    /// filesystem entry. The kernel prefixes a NUL byte; we encode
    /// it. Caller passes the bare name (no leading NUL).
    pub fn fromAbstract(name: []const u8) AbstractError!AddrImpl {
        if (!is_linux) return error.NotLinux;
        if (name.len == 0) return error.NameTooLong;
        if (name.len + 1 > sunPathLen()) return error.NameTooLong;
        var sa = posix.sockaddr.un{ .family = posix.AF.UNIX, .path = undefined };
        @memset(&sa.path, 0);
        sa.path[0] = 0; // leading NUL = abstract
        @memcpy(sa.path[1 .. 1 + name.len], name);
        const family_off = @offsetOf(posix.sockaddr.un, "path");
        return .{
            .inner = sa,
            .len = @intCast(family_off + 1 + name.len),
        };
    }

    pub fn sockaddr(self: *const AddrImpl) *const posix.sockaddr {
        return @ptrCast(@alignCast(&self.inner));
    }

    pub fn isAbstract(self: AddrImpl) bool {
        if (!is_linux) return false;
        return self.inner.path[0] == 0;
    }

    /// Get the path portion (excludes the trailing NUL for path
    /// addresses; excludes the leading NUL for abstract).
    pub fn path(self: *const AddrImpl) []const u8 {
        if (self.isAbstract()) {
            // Skip leading NUL. Length = total path length minus
            // the NUL byte. inner.path is sized; the meaningful
            // portion ends at the length tracked in `self.len`.
            const family_off = @offsetOf(posix.sockaddr.un, "path");
            const path_len = @as(usize, self.len) - family_off - 1;
            return self.inner.path[1 .. 1 + path_len];
        }
        // Path: find first NUL.
        const first_nul = std.mem.indexOfScalar(u8, &self.inner.path, 0) orelse self.inner.path.len;
        return self.inner.path[0..first_nul];
    }
};

inline fn sunPathLen() usize {
    // Per-OS sun_path field length. From std.posix.sockaddr.un.
    return @sizeOf(@FieldType(posix.sockaddr.un, "path"));
}

// ─── libc extern bindings ────────────────────────────────────────────

const c_socket = if (is_windows) {} else @extern(
    *const fn (c_int, c_int, c_int) callconv(.c) c_int,
    .{ .name = "socket" },
);
const c_bind = if (is_windows) {} else @extern(
    *const fn (c_int, *const anyopaque, c_uint) callconv(.c) c_int,
    .{ .name = "bind" },
);
const c_listen = if (is_windows) {} else @extern(
    *const fn (c_int, c_int) callconv(.c) c_int,
    .{ .name = "listen" },
);
const c_accept = if (is_windows) {} else @extern(
    *const fn (c_int, ?*anyopaque, ?*c_uint) callconv(.c) c_int,
    .{ .name = "accept" },
);
const c_connect = if (is_windows) {} else @extern(
    *const fn (c_int, *const anyopaque, c_uint) callconv(.c) c_int,
    .{ .name = "connect" },
);
const c_close = if (is_windows) {} else @extern(
    *const fn (c_int) callconv(.c) c_int,
    .{ .name = "close" },
);
const c_sendto = if (is_windows) {} else @extern(
    *const fn (c_int, *const anyopaque, usize, c_int, ?*const anyopaque, c_uint) callconv(.c) isize,
    .{ .name = "sendto" },
);
const c_recvfrom = if (is_windows) {} else @extern(
    *const fn (c_int, *anyopaque, usize, c_int, ?*anyopaque, ?*c_uint) callconv(.c) isize,
    .{ .name = "recvfrom" },
);
const c_unlink = if (is_windows) {} else @extern(
    *const fn ([*:0]const u8) callconv(.c) c_int,
    .{ .name = "unlink" },
);

const SOCK_STREAM: c_int = 1;
const SOCK_DGRAM: c_int = 2;

inline fn errnoVal() c_int {
    return std.c._errno().*;
}

const EAGAIN: c_int = switch (builtin.os.tag) {
    .linux, .freebsd, .openbsd, .netbsd, .dragonfly => 11,
    .macos, .ios, .tvos, .watchos => 35,
    else => 11,
};
const EWOULDBLOCK: c_int = EAGAIN;
const EINTR: c_int = 4;
const EINPROGRESS: c_int = switch (builtin.os.tag) {
    .linux, .freebsd, .openbsd, .netbsd, .dragonfly => 115,
    .macos, .ios, .tvos, .watchos => 36,
    else => 115,
};

// ─── UnixStream ──────────────────────────────────────────────────────

const StreamImpl = if (is_windows) void else struct {
    fd: i32,
    pd: pd_handle.Atomic = .{},

    pub fn connect(path: []const u8) !StreamImpl {
        const addr = try AddrImpl.fromPath(path);
        return connectAddr(addr);
    }

    pub fn connectAddr(addr: AddrImpl) !StreamImpl {
        const fd = c_socket(posix.AF.UNIX, SOCK_STREAM, 0);
        if (fd < 0) return error.SocketCreateFailed;
        errdefer _ = c_close(fd);
        try reactor_mod.setNonblock(@intCast(fd));

        if (c_connect(fd, addr.sockaddr(), addr.len) < 0) {
            const e = errnoVal();
            if (e != EINPROGRESS) return error.ConnectFailed;
            // Wait for writable = connect completed.
            const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
            var stream: StreamImpl = .{ .fd = @intCast(fd) };
            const pd = try pd_handle.ensure(&stream.pd, rt, stream.fd);
            defer pd.decref();
            try rt.reactor.waitFd(stream.fd, pd, .write);
            return stream;
        }
        return .{ .fd = @intCast(fd) };
    }

    pub fn close(self: *StreamImpl) void {
        pd_handle.release(&self.pd, self.fd);
        if (current.get()) |c| {
            const rt: *runtime.Runtime = @ptrCast(@alignCast(c.runtime));
            rt.reactor.closeFd(self.fd);
        } else {
            _ = c_close(@intCast(self.fd));
        }
    }

    pub fn read(self: *StreamImpl, buf: []u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        return reactor_mod.readAsync(&rt.reactor, self.fd, pd, buf);
    }

    pub fn write(self: *StreamImpl, buf: []const u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        return reactor_mod.writeAsync(&rt.reactor, self.fd, pd, buf);
    }

    pub fn writeAll(self: *StreamImpl, buf: []const u8) !void {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        return reactor_mod.writeAll(&rt.reactor, self.fd, pd, buf);
    }

    pub fn readFull(self: *StreamImpl, buf: []u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        return reactor_mod.readFull(&rt.reactor, self.fd, pd, buf);
    }
};

// ─── UnixListener ────────────────────────────────────────────────────

const ListenerImpl = if (is_windows) void else struct {
    fd: i32,
    pd: pd_handle.Atomic = .{},
    /// Stored so `close()` can remove the socket file. Empty for
    /// abstract Linux sockets.
    bound_path: ?[]const u8 = null,

    pub fn bind(path: []const u8) !ListenerImpl {
        const addr = try AddrImpl.fromPath(path);
        return bindAddr(addr, path);
    }

    pub fn bindAbstract(name: []const u8) !ListenerImpl {
        const addr = try AddrImpl.fromAbstract(name);
        return bindAddr(addr, null);
    }

    fn bindAddr(addr: AddrImpl, path: ?[]const u8) !ListenerImpl {
        const fd = c_socket(posix.AF.UNIX, SOCK_STREAM, 0);
        if (fd < 0) return error.SocketCreateFailed;
        errdefer _ = c_close(fd);

        if (c_bind(fd, addr.sockaddr(), addr.len) < 0) return error.BindFailed;
        if (c_listen(fd, 128) < 0) return error.ListenFailed;
        try reactor_mod.setNonblock(@intCast(fd));
        return .{ .fd = @intCast(fd), .bound_path = path };
    }

    pub fn close(self: *ListenerImpl) void {
        pd_handle.release(&self.pd, self.fd);
        if (current.get()) |c| {
            const rt: *runtime.Runtime = @ptrCast(@alignCast(c.runtime));
            rt.reactor.closeFd(self.fd);
        } else {
            _ = c_close(@intCast(self.fd));
        }
        // Path-bound sockets leave a file behind; unlink it so
        // re-binding to the same path doesn't EADDRINUSE on the
        // next test / process restart. Abstract sockets have no
        // filesystem entry — nothing to clean.
        if (self.bound_path) |p| {
            var buf: [256]u8 = undefined;
            if (p.len < buf.len) {
                @memcpy(buf[0..p.len], p);
                buf[p.len] = 0;
                _ = c_unlink(@ptrCast(&buf[0]));
            }
        }
    }

    pub fn accept(self: *ListenerImpl) !StreamImpl {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        while (true) {
            const new_fd = c_accept(@intCast(self.fd), null, null);
            if (new_fd >= 0) {
                try reactor_mod.setNonblock(@intCast(new_fd));
                return .{ .fd = @intCast(new_fd) };
            }
            const e = errnoVal();
            if (e == EAGAIN or e == EWOULDBLOCK) {
                try rt.reactor.waitFd(self.fd, pd, .read);
                continue;
            }
            if (e == EINTR) continue;
            return error.AcceptFailed;
        }
    }

    pub fn acceptCancel(self: *ListenerImpl, c: *cancel_mod.Cancel) (reactor_mod.IoError || cancel_mod.Error)!StreamImpl {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        while (true) {
            try c.checkpoint();
            const new_fd = c_accept(@intCast(self.fd), null, null);
            if (new_fd >= 0) {
                try reactor_mod.setNonblock(@intCast(new_fd));
                return .{ .fd = @intCast(new_fd) };
            }
            const e = errnoVal();
            if (e == EAGAIN or e == EWOULDBLOCK) {
                try rt.reactor.waitFdCancel(self.fd, pd, .read, c);
                continue;
            }
            if (e == EINTR) continue;
            return error.Unexpected;
        }
    }
};

// ─── UnixDatagram ────────────────────────────────────────────────────

const DatagramImpl = if (is_windows) void else struct {
    fd: i32,
    pd: pd_handle.Atomic = .{},
    bound_path: ?[]const u8 = null,

    pub const RecvFromResult = struct {
        len: usize,
        addr: AddrImpl,
    };

    pub fn bind(path: []const u8) !DatagramImpl {
        const addr = try AddrImpl.fromPath(path);
        const fd = c_socket(posix.AF.UNIX, SOCK_DGRAM, 0);
        if (fd < 0) return error.SocketCreateFailed;
        errdefer _ = c_close(fd);
        if (c_bind(fd, addr.sockaddr(), addr.len) < 0) return error.BindFailed;
        try reactor_mod.setNonblock(@intCast(fd));
        return .{ .fd = @intCast(fd), .bound_path = path };
    }

    pub fn unbound() !DatagramImpl {
        const fd = c_socket(posix.AF.UNIX, SOCK_DGRAM, 0);
        if (fd < 0) return error.SocketCreateFailed;
        errdefer _ = c_close(fd);
        try reactor_mod.setNonblock(@intCast(fd));
        return .{ .fd = @intCast(fd) };
    }

    pub fn close(self: *DatagramImpl) void {
        pd_handle.release(&self.pd, self.fd);
        if (current.get()) |c| {
            const rt: *runtime.Runtime = @ptrCast(@alignCast(c.runtime));
            rt.reactor.closeFd(self.fd);
        } else {
            _ = c_close(@intCast(self.fd));
        }
        if (self.bound_path) |p| {
            var buf: [256]u8 = undefined;
            if (p.len < buf.len) {
                @memcpy(buf[0..p.len], p);
                buf[p.len] = 0;
                _ = c_unlink(@ptrCast(&buf[0]));
            }
        }
    }

    pub fn sendTo(self: *DatagramImpl, buf: []const u8, addr: AddrImpl) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        while (true) {
            const n = c_sendto(@intCast(self.fd), buf.ptr, buf.len, 0, addr.sockaddr(), addr.len);
            if (n >= 0) return @intCast(n);
            const e = errnoVal();
            if (e == EAGAIN or e == EWOULDBLOCK) {
                try rt.reactor.waitFd(self.fd, pd, .write);
                continue;
            }
            if (e == EINTR) continue;
            return error.Unexpected;
        }
    }

    pub fn recvFrom(self: *DatagramImpl, buf: []u8) !RecvFromResult {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        defer pd.decref();
        while (true) {
            var sa: posix.sockaddr.un = undefined;
            var sa_len: c_uint = @sizeOf(posix.sockaddr.un);
            const n = c_recvfrom(@intCast(self.fd), buf.ptr, buf.len, 0, &sa, &sa_len);
            if (n >= 0) {
                return .{
                    .len = @intCast(n),
                    .addr = .{ .inner = sa, .len = sa_len },
                };
            }
            const e = errnoVal();
            if (e == EAGAIN or e == EWOULDBLOCK) {
                try rt.reactor.waitFd(self.fd, pd, .read);
                continue;
            }
            if (e == EINTR) continue;
            return error.Unexpected;
        }
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const testing = std.testing;
const test_alloc = @import("../testing.zig").allocator;
const lib = @import("../lib.zig");
const path_mod = @import("../fs/path.zig");

const UnixEchoCtx = struct {
    listener: *UnixListener,
    sock_path: []const u8,
    server_done: bool = false,
    received: u8 = 0,
};

fn unixEchoServer(ctx: *UnixEchoCtx) !void {
    var stream = try ctx.listener.accept();
    defer stream.close();
    var buf: [4]u8 = undefined;
    _ = try stream.readFull(&buf);
    try stream.writeAll(&buf);
    ctx.received = buf[0];
    ctx.server_done = true;
}

fn unixEchoClient(sock_path: []const u8) !u8 {
    var stream = try UnixStream.connect(sock_path);
    defer stream.close();
    try stream.writeAll(&[_]u8{ 0x42, 0x43, 0x44, 0x45 });
    var got: [4]u8 = undefined;
    _ = try stream.readFull(&got);
    return got[0];
}

const UnixClientCtx = struct {
    sock_path: []const u8,
    out: *u8,
};

fn unixClientRoot(ctx: *UnixClientCtx) !void {
    ctx.out.* = try unixEchoClient(ctx.sock_path);
}

fn unixEchoRoot(ctx: *UnixEchoCtx) !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
    var server = try rt.spawn(unixEchoServer, .{ctx});
    var got: u8 = 0;
    var client_ctx = UnixClientCtx{ .sock_path = ctx.sock_path, .out = &got };
    var client = try rt.spawn(unixClientRoot, .{&client_ctx});
    _ = client.join() catch |e| return e;
    _ = server.join() catch |e| return e;
    try testing.expectEqual(@as(u8, 0x42), got);
}

test "UnixStream + UnixListener: path-based echo round-trip" {
    if (comptime is_windows) return error.SkipZigTest;
    var rt = try runtime.Runtime.init(.{ .allocator = test_alloc, .workers = 2 });
    defer rt.deinit();

    // Per-test-run socket path so concurrent runs (CI matrices) and
    // prior crashes don't collide. We append a comptime-random
    // suffix; the unlink in `listener.close()` cleans up after a
    // clean exit.
    const sock_path = "/tmp/volt-unix-test.sock";
    // Pre-clean in case a prior test crashed before unlink.
    var z: [128]u8 = undefined;
    @memcpy(z[0..sock_path.len], sock_path);
    z[sock_path.len] = 0;
    _ = c_unlink(@ptrCast(&z[0]));

    var listener = try UnixListener.bind(sock_path);
    defer listener.close();
    var ctx = UnixEchoCtx{ .listener = &listener, .sock_path = sock_path };
    try (try rt.run(unixEchoRoot, .{&ctx}));
    try testing.expect(ctx.server_done);
    try testing.expectEqual(@as(u8, 0x42), ctx.received);
}

test "UnixAddr: fromPath truncates on too-long path" {
    if (comptime is_windows) return error.SkipZigTest;
    const long_path = "/" ++ ("a" ** 200);
    try testing.expectError(error.PathTooLong, UnixAddr.fromPath(long_path));
}

test "UnixAddr: fromPath round-trip" {
    if (comptime is_windows) return error.SkipZigTest;
    const a = try UnixAddr.fromPath("/tmp/foo.sock");
    try testing.expectEqualStrings("/tmp/foo.sock", a.path());
    try testing.expect(!a.isAbstract());
}
