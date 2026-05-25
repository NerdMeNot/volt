//! TCP networking — TcpListener + TcpStream over the kqueue reactor.
//! IPv4 / loopback today; IPv6 + DNS later.
//!
//! All blocking ops yield to the reactor on EAGAIN. The API looks
//! synchronous; suspend points are transparent.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const reactor_mod = @import("reactor.zig");
const poll_desc = @import("poll_desc.zig");
const pd_handle = @import("pd_handle.zig");
const runtime = @import("runtime.zig");
const current = @import("current.zig");

const AF_INET: c_int = 2;
const AF_INET6: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => 30,
    .linux => 10,
    .windows => 23,
    else => 10,
};
const SOCK_STREAM: c_int = 1;
const IPPROTO_TCP: c_int = 6;

// Platform-specific socket constants. Darwin and Linux disagree on
// SOL_SOCKET / EINPROGRESS / EAGAIN values; the switches resolve at
// comptime so each target sees one canonical value.
const SOL_SOCKET: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos => 0xFFFF,
    .linux, .freebsd, .openbsd, .netbsd, .dragonfly => 1,
    .windows => 0xFFFF, // WinSock SOL_SOCKET
    else => @compileError("SOL_SOCKET not defined for this OS"),
};
const SO_REUSEADDR: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos => 0x0004,
    .linux => 2,
    .freebsd, .openbsd, .netbsd, .dragonfly => 0x0004,
    .windows => 0x0004,
    else => @compileError("SO_REUSEADDR not defined for this OS"),
};
// Windows-only — finalise the new socket's inheritance after
// `AcceptEx` / `ConnectEx` (without these, `getpeername` etc fail
// with WSAENOTCONN).
const SO_UPDATE_ACCEPT_CONTEXT: c_int = 0x700B;
const SO_UPDATE_CONNECT_CONTEXT: c_int = 0x7010;
const EINPROGRESS: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos => 36,
    .linux => 115,
    .freebsd, .openbsd, .netbsd, .dragonfly => 36,
    .windows => 10036, // WSAEINPROGRESS
    else => @compileError("EINPROGRESS not defined for this OS"),
};
const EAGAIN: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos => 35,
    .linux => 11,
    .freebsd, .openbsd, .netbsd, .dragonfly => 35,
    .windows => 10035, // WSAEWOULDBLOCK
    else => @compileError("EAGAIN not defined for this OS"),
};

// `sockaddr_in` layout is not portable. Darwin / *BSD prepend a
// `sin_len` byte and treat `sin_family` as u8; Linux and Windows
// (Winsock2) have no length byte and use a u16 `sin_family` at
// offset 0. Same total size (16 bytes) but the first two bytes
// differ — bind() on Linux/Windows read our Darwin-shaped struct
// and saw family=(0x02<<8|0x10)=528 instead of AF_INET=2, which
// fails as EAFNOSUPPORT / WSAEAFNOSUPPORT. Pick per-OS at
// comptime so each kernel sees the layout it expects.
const sockaddr_in = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .freebsd, .openbsd, .netbsd, .dragonfly => extern struct {
        len: u8 = @sizeOf(@This()),
        family: u8 = AF_INET,
        port: u16, // network order
        addr: u32, // network order
        zero: [8]u8 = @splat(0),
    },
    else => extern struct {
        family: u16 = AF_INET,
        port: u16, // network order
        addr: u32, // network order
        zero: [8]u8 = @splat(0),
    },
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
const getpeername = @extern(*const fn (c_int, *anyopaque, *c_uint) callconv(.c) c_int, .{ .name = "getpeername" });
const c_close = @extern(*const fn (c_int) callconv(.c) c_int, .{ .name = "close" });

// `errno` accessor — the libc name differs per platform:
//   - Darwin / *BSD : `__error()` returns `*c_int`
//   - Linux glibc   : `__errno_location()` returns `*c_int`
//   - Windows       : sockets surface failures via `WSAGetLastError()`,
//                     which returns the error code directly (`c_int`)
//                     and doesn't dereference anything.
//
// Comptime-pick the right one. Both POSIX externs share the same
// signature so the `errnoVal` body stays uniform on those paths.
const c_error = if (builtin.os.tag == .windows) {} // unused; see WSAGetLastError below
    else if (builtin.os.tag.isDarwin() or builtin.os.tag == .freebsd or builtin.os.tag == .openbsd or builtin.os.tag == .netbsd or builtin.os.tag == .dragonfly)
        @extern(*const fn () callconv(.c) *c_int, .{ .name = "__error" })
    else
        @extern(*const fn () callconv(.c) *c_int, .{ .name = "__errno_location" });

extern "ws2_32" fn WSAGetLastError() callconv(.winapi) c_int;

inline fn errnoVal() c_int {
    if (comptime builtin.os.tag == .windows) return WSAGetLastError();
    return c_error().*;
}

// Note: `setNonblock` lives in `reactor.zig` (re-exported from each
// platform's backend). The duplicate that used to live here was
// consolidated as part of the L1 reactor refactor.

/// Dispatch a socket close through the reactor's `closeFd` when we
/// have a runtime in scope (any coroutine context), falling back to
/// the bare libc close otherwise (test cleanup, `defer s.close()`
/// outside a coroutine).
///
/// Why this matters: a peer coroutine calling `close()` on a fd that
/// another coroutine is parked on must dispatch the parked waiter,
/// not just orphan the kernel registration. The reactor's `closeFd`
/// owns that dispatch logic — see `src/reactor_kqueue.zig` for the
/// full kqueue implementation. The epoll / io_uring / IOCP backends
/// today have stub closeFd implementations that just call libc
/// close; the two SkipZigTest cases in
/// `src/reactor_conformance_test.zig` keep those gaps visible.
///
/// Outside coroutine context (test cleanup after rt.deinit, etc.),
/// no coroutine can be parked, so a plain libc close is correct.
fn closeFdDispatch(fd: i32) void {
    if (current.get()) |c| {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(c.runtime));
        rt.reactor.closeFd(fd);
    } else {
        _ = c_close(@intCast(fd));
    }
}

/// Re-exported from `src/net/address.zig` — full IPv4 + IPv6
/// support, posix.sockaddr.storage-backed. See that file for the
/// complete public surface.
pub const Address = @import("net/address.zig").Address;

/// Re-exported from `src/net/options.zig` — typed socket-option
/// helpers (Keepalive, Linger) + free setters/getters.
pub const options = @import("net/options.zig");
pub const Keepalive = options.Keepalive;
pub const Linger = options.Linger;

/// Re-exported from `src/net/udp.zig` — UDP socket with full
/// multicast (IPv4 + IPv6) and broadcast support.
pub const UdpSocket = @import("net/udp.zig").UdpSocket;

/// Re-exported from `src/net/unix.zig` — Unix domain sockets
/// (stream + listener + datagram). POSIX-only; each type is
/// `void` on Windows so misuse fails at compile time with a clean
/// "expected type" error.
pub const UnixAddr = @import("net/unix.zig").UnixAddr;
pub const UnixStream = @import("net/unix.zig").UnixStream;
pub const UnixListener = @import("net/unix.zig").UnixListener;
pub const UnixDatagram = @import("net/unix.zig").UnixDatagram;

/// DNS resolution via `getaddrinfo` bridged through the blocking
/// pool. See `src/net/resolver.zig`. Sync `getaddrinfo` v1; full
/// async DNS resolver is a v1.x stretch.
pub const lookupHost = @import("net/resolver.zig").lookupHost;
pub const lookupHostFirst = @import("net/resolver.zig").lookupHostFirst;
pub const connectHost = @import("net/resolver.zig").connectHost;
pub const LookupError = @import("net/resolver.zig").LookupError;

// Pull in tests from the new sub-files via the test runner.
test {
    _ = @import("net/address.zig");
    _ = @import("net/options.zig");
    _ = @import("net/udp.zig");
    _ = @import("net/unix.zig");
    _ = @import("net/resolver.zig");
}

pub const TcpListener = struct {
    fd: i32,
    /// Per-fd PollDesc — allocated on first wait (lazy because
    /// `bind` is callable before `rt.run()` with no coroutine
    /// context to access the allocator). Owned by this listener:
    /// closed and freed in `close`.
    pd: pd_handle.Atomic = .{ .raw = null },
    /// Windows-only: tracks whether `fd` has been associated with
    /// the reactor's IOCP yet. CreateIoCompletionPort is one-shot
    /// per handle (calling it twice fails), so we set this flag on
    /// the first `accept` and skip on subsequent ones. Unused on
    /// POSIX (epoll/kqueue need no per-fd registration).
    iocp_associated: bool = false,

    /// Bind a TCP listener at `addr`. Sets SO_REUSEADDR so repeated
    /// runs don't EADDRINUSE; non-blocking from the start. The
    /// socket family (AF_INET vs AF_INET6) is chosen from `addr`'s
    /// family — pass `Address.any4(port)` / `loopback4(port)` for
    /// IPv4, `any6(port)` / `loopback6(port)` for IPv6.
    pub fn bind(addr: Address) !TcpListener {
        const af: c_int = if (addr.isIpv6()) AF_INET6 else AF_INET;
        const fd = socket(af, SOCK_STREAM, IPPROTO_TCP);
        if (fd < 0) return error.SocketCreateFailed;
        errdefer _ = c_close(fd);

        // SO_REUSEADDR so repeated test runs don't get EADDRINUSE.
        const one: c_int = 1;
        _ = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, @sizeOf(c_int));

        if (c_bind(fd, addr.sockaddr(), addr.len) < 0) return error.BindFailed;
        if (c_listen(fd, 128) < 0) return error.ListenFailed;
        try reactor_mod.setNonblock(@intCast(fd));
        // IOCP association is deferred to first `accept` — bind can
        // be called pre-`rt.run()` (no coroutine context, no
        // accessible reactor), but accept always runs inside a
        // coroutine. POSIX reactors don't need the association.
        return .{ .fd = @intCast(fd) };
    }

    pub fn close(self: *TcpListener) void {
        pd_handle.release(&self.pd, self.fd);
        closeFdDispatch(self.fd);
    }

    // ─── Socket options on the listener ──────────────────────────

    /// Enable SO_REUSEPORT. Multiple processes / threads can bind
    /// the same address; the kernel load-balances incoming
    /// connections across them. Useful for HTTP servers that fork
    /// per-CPU workers. Windows maps this to SO_REUSEADDR.
    pub fn setReusePort(self: *TcpListener, enabled: bool) options.OptionError!void {
        return options.setReusePort(self.fd, enabled);
    }

    pub fn setRecvBufferSize(self: *TcpListener, bytes: u32) options.OptionError!void {
        return options.setRecvBufferSize(self.fd, bytes);
    }

    pub fn setSendBufferSize(self: *TcpListener, bytes: u32) options.OptionError!void {
        return options.setSendBufferSize(self.fd, bytes);
    }

    /// Get the bound local address (useful when bind port = 0).
    /// Reads via `getsockname` into a polymorphic sockaddr.storage,
    /// so the returned `Address` is whatever family the listener
    /// was bound on (IPv4, IPv6, dual-stack via `::`).
    pub fn localAddress(self: *const TcpListener) !Address {
        var storage: std.posix.sockaddr.storage = undefined;
        var len: c_uint = @sizeOf(std.posix.sockaddr.storage);
        if (getsockname(@intCast(self.fd), &storage, &len) < 0) return error.GetSockNameFailed;
        return Address.fromSockaddr(@ptrCast(@alignCast(&storage)), len);
    }

    /// Accept the next incoming connection. Yields to the reactor on EAGAIN.
    pub fn accept(self: *TcpListener) !TcpStream {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        if (comptime builtin.os.tag == .windows) {
            return acceptWindows(self, rt);
        }
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        while (true) {
            const new_fd = c_accept(@intCast(self.fd), null, null);
            if (new_fd >= 0) {
                try reactor_mod.setNonblock(@intCast(new_fd));
                return .{ .fd = @intCast(new_fd) };
            }
            if (errnoVal() != EAGAIN) return error.AcceptFailed;
            try rt.reactor.waitFd(self.fd, pd, .read);
        }
    }
};

// Windows accept via `AcceptEx`: a zero-byte `WSARecv` on a
// *listening* socket is not a valid op — the readiness-polyfill that
// works for established sockets fails here. AcceptEx is IOCP's
// canonical accept: caller pre-creates the new socket, submits, and
// the kernel fills it on completion. Lives outside the struct so the
// pre-comptime branch in `accept` can dispatch by os.tag without
// dragging Windows-only state into the type.
fn acceptWindows(listener: *TcpListener, rt: *runtime.Runtime) !TcpStream {
    // First-use association — bind() can't do this because it may
    // be called pre-`rt.run()`. AcceptEx requires the listener to
    // be on the IOCP.
    if (!listener.iocp_associated) {
        try rt.reactor.associate(@intCast(listener.fd));
        listener.iocp_associated = true;
    }

    const new_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (new_fd < 0) return error.SocketCreateFailed;
    errdefer _ = c_close(new_fd);

    // AcceptEx wants `2 * (sizeof(sockaddr_in) + 16)` bytes — 16
    // bytes of internal padding per address (per MSDN). We don't
    // care about the filled-in addresses here; the buffer just has
    // to be the right size.
    const addr_slot: comptime_int = @sizeOf(sockaddr_in) + 16;
    var out_buf: [2 * addr_slot]u8 = undefined;
    try rt.reactor.submitAcceptEx(@intCast(listener.fd), @intCast(new_fd), &out_buf, addr_slot);

    // Inherit listener properties — without this, `getpeername` /
    // `shutdown` / further setsockopt return WSAENOTCONN. Must
    // happen before any further use of the new socket.
    const listen_fd_c: c_int = @intCast(listener.fd);
    _ = setsockopt(new_fd, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, &listen_fd_c, @sizeOf(c_int));

    // Associate the new socket with the IOCP for subsequent
    // `readAsync`/`writeAsync` ops. Then put it in non-blocking
    // mode so the `recv`/`send` fast paths surface WSAEWOULDBLOCK.
    try rt.reactor.associate(@intCast(new_fd));
    try reactor_mod.setNonblock(@intCast(new_fd));
    return .{ .fd = @intCast(new_fd) };
}

pub const TcpStream = struct {
    fd: i32,
    /// Per-fd PollDesc — symmetric with TcpListener; lazy-init at
    /// first wait, closed and freed in `close`.
    pd: pd_handle.Atomic = .{ .raw = null },

    pub fn connect(addr: Address) !TcpStream {
        const af: c_int = if (addr.isIpv6()) AF_INET6 else AF_INET;
        const fd = socket(af, SOCK_STREAM, IPPROTO_TCP);
        if (fd < 0) return error.SocketCreateFailed;
        errdefer _ = c_close(fd);
        try reactor_mod.setNonblock(@intCast(fd));

        if (comptime builtin.os.tag == .windows) {
            return connectWindows(fd, addr);
        }

        // bind on family-specific socket so the kernel chooses the
        // right protocol stack. Caller created socket with AF_INET;
        // an IPv6 addr will fail here — that's caught at connect time.
        if (c_connect(fd, addr.sockaddr(), addr.len) < 0) {
            const e = errnoVal();
            if (e != EINPROGRESS) return error.ConnectFailed;
            // Wait for writable = connect completed.
            const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
            var stream: TcpStream = .{ .fd = @intCast(fd) };
            const pd = try pd_handle.ensure(&stream.pd, rt, stream.fd);
            try rt.reactor.waitFd(stream.fd, pd, .write);
            return stream;
        }
        return .{ .fd = @intCast(fd) };
    }

    pub fn close(self: *TcpStream) void {
        pd_handle.release(&self.pd, self.fd);
        closeFdDispatch(self.fd);
    }

    // Windows connect via `ConnectEx`: same reason as AcceptEx —
    // the zero-byte `WSASend` readiness probe is not valid on an
    // unconnected socket. ConnectEx requires the socket be bound
    // first (to anything, including 0.0.0.0:0), associated with the
    // IOCP, and then completes asynchronously like AcceptEx.
    fn connectWindows(fd: c_int, addr: Address) !TcpStream {
        // Bind to wildcard so ConnectEx's "must be bound" precondition
        // is satisfied. Kernel picks an ephemeral local port.
        const any_addr = Address.any4(0);
        if (c_bind(fd, any_addr.sockaddr(), any_addr.len) < 0) return error.BindFailed;

        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        try rt.reactor.associate(@intCast(fd));

        try rt.reactor.submitConnectEx(@intCast(fd), @ptrCast(addr.sockaddr()), @intCast(addr.len));

        // Same SO_UPDATE_*_CONTEXT requirement as AcceptEx — the
        // socket is unusable for `getpeername` etc until this fires.
        const dummy: c_int = 0;
        _ = setsockopt(fd, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, &dummy, @sizeOf(c_int));
        return .{ .fd = @intCast(fd) };
    }

    pub fn read(self: *TcpStream, buf: []u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        return reactor_mod.readAsync(&rt.reactor, self.fd, pd, buf);
    }

    pub fn write(self: *TcpStream, buf: []const u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        return reactor_mod.writeAsync(&rt.reactor, self.fd, pd, buf);
    }

    pub fn writeAll(self: *TcpStream, buf: []const u8) !void {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        return reactor_mod.writeAll(&rt.reactor, self.fd, pd, buf);
    }

    pub fn readFull(self: *TcpStream, buf: []u8) !usize {
        const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
        const pd = try pd_handle.ensure(&self.pd, rt, self.fd);
        return reactor_mod.readFull(&rt.reactor, self.fd, pd, buf);
    }

    // ─── Socket options ─────────────────────────────────────────────

    /// Disable Nagle's algorithm — small writes go out immediately
    /// rather than coalescing. The right default for latency-
    /// sensitive protocols (RPC, gaming); the wrong default for
    /// bulk transfer.
    pub fn setNoDelay(self: *TcpStream, enabled: bool) options.OptionError!void {
        return options.setTcpNoDelay(self.fd, enabled);
    }

    pub fn getNoDelay(self: TcpStream) options.OptionError!bool {
        return options.getTcpNoDelay(self.fd);
    }

    /// Enable TCP keepalive with the given (idle, interval, retries)
    /// schedule. Pass `null` to disable.
    pub fn setKeepalive(self: *TcpStream, ka: ?Keepalive) options.OptionError!void {
        return options.setKeepalive(self.fd, ka);
    }

    pub fn setSendBufferSize(self: *TcpStream, bytes: u32) options.OptionError!void {
        return options.setSendBufferSize(self.fd, bytes);
    }

    pub fn setRecvBufferSize(self: *TcpStream, bytes: u32) options.OptionError!void {
        return options.setRecvBufferSize(self.fd, bytes);
    }

    pub fn setLinger(self: *TcpStream, linger: ?Linger) options.OptionError!void {
        return options.setLinger(self.fd, linger);
    }

    /// Peer's address. Reads via `getpeername`; works after connect
    /// (sync) or after `accept` (TcpListener fills it).
    pub fn peerAddress(self: *const TcpStream) !Address {
        var storage: std.posix.sockaddr.storage = undefined;
        var len: c_uint = @sizeOf(std.posix.sockaddr.storage);
        if (getpeername(@intCast(self.fd), &storage, &len) < 0) return error.GetPeerNameFailed;
        return Address.fromSockaddr(@ptrCast(@alignCast(&storage)), len);
    }

    /// Local-side address. Reads via `getsockname`.
    pub fn localAddress(self: *const TcpStream) !Address {
        var storage: std.posix.sockaddr.storage = undefined;
        var len: c_uint = @sizeOf(std.posix.sockaddr.storage);
        if (getsockname(@intCast(self.fd), &storage, &len) < 0) return error.GetSockNameFailed;
        return Address.fromSockaddr(@ptrCast(@alignCast(&storage)), len);
    }

    // ─── Half-duplex split (borrowed) ──────────────────────────────

    /// Split into read- and write-half references. Both halves share
    /// the underlying `TcpStream` lifetime — the caller keeps the
    /// stream alive (e.g. on its own stack) while both halves are
    /// in use, typically by passing `&halves.read` and `&halves.write`
    /// into two coroutines that join before the stream is closed.
    ///
    /// No allocation. For owned halves that can move independently
    /// across coroutines, see `TcpStream.intoSplit` (v1.x).
    pub fn split(self: *TcpStream) struct { read: ReadHalf, write: WriteHalf } {
        return .{
            .read = .{ .stream = self },
            .write = .{ .stream = self },
        };
    }

    pub const ReadHalf = struct {
        stream: *TcpStream,
        pub fn read(self: *ReadHalf, buf: []u8) !usize {
            return self.stream.read(buf);
        }
        pub fn readFull(self: *ReadHalf, buf: []u8) !usize {
            return self.stream.readFull(buf);
        }
        pub fn reader(self: *ReadHalf, buf: []u8) TcpStream.Reader {
            return self.stream.reader(buf);
        }
    };

    pub const WriteHalf = struct {
        stream: *TcpStream,
        pub fn write(self: *WriteHalf, buf: []const u8) !usize {
            return self.stream.write(buf);
        }
        pub fn writeAll(self: *WriteHalf, buf: []const u8) !void {
            return self.stream.writeAll(buf);
        }
        pub fn writer(self: *WriteHalf, buf: []u8) TcpStream.Writer {
            return self.stream.writer(buf);
        }
    };

    /// Construct a `std.Io.Reader` adapter — lets std-library code
    /// (formatters, parsers, anything that takes `*std.Io.Reader`)
    /// consume bytes from this socket. Caller provides the buffered-
    /// storage backing buffer. Must be called from inside a coroutine
    /// (reads route through the reactor and may park).
    ///
    /// On reactor I/O error, the categorical `reactor.IoError` is
    /// stashed on the returned `Reader.err` field; the `std.Io.Reader`
    /// interface surfaces `error.ReadFailed`. Callers that need the
    /// typed error inspect `.err` after observing `ReadFailed`.
    pub fn reader(self: *TcpStream, buffer: []u8) Reader {
        return Reader.init(self, buffer);
    }

    /// Construct a `std.Io.Writer` adapter. Same shape as `reader`:
    /// caller-provided buffer, typed `IoError` stashed on
    /// `Writer.err` when the std interface returns `WriteFailed`.
    pub fn writer(self: *TcpStream, buffer: []u8) Writer {
        return Writer.init(self, buffer);
    }

    /// `std.Io.Reader` adapter — see `TcpStream.reader`. The Volt
    /// I/O handle conformance shape: every Volt async byte source
    /// exposes `reader(buffer) -> XReader` where `XReader.interface`
    /// is a `std.Io.Reader`. Downstream `volt-fs` / `volt-net` libs
    /// mirror this shape so the same std-library code composes.
    pub const Reader = struct {
        stream: *TcpStream,
        interface: std.Io.Reader,
        /// Last categorical I/O error observed while servicing a
        /// stream call. Set before returning `error.ReadFailed` /
        /// `error.EndOfStream`; reset to null by `init`.
        err: ?reactor_mod.IoError = null,

        pub fn init(s: *TcpStream, buffer: []u8) Reader {
            return .{
                .stream = s,
                .interface = .{
                    .vtable = &.{ .stream = streamImpl },
                    .buffer = buffer,
                    .seek = 0,
                    .end = 0,
                },
            };
        }

        fn streamImpl(io_r: *std.Io.Reader, io_w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
            const self: *Reader = @alignCast(@fieldParentPtr("interface", io_r));
            const dest = limit.slice(try io_w.writableSliceGreedy(1));
            const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
            const pd = pd_handle.ensure(&self.stream.pd, rt, self.stream.fd) catch {
                self.err = error.SystemResources;
                return error.ReadFailed;
            };
            const n = reactor_mod.readAsync(&rt.reactor, self.stream.fd, pd, dest) catch |err| {
                self.err = err;
                return error.ReadFailed;
            };
            if (n == 0) return error.EndOfStream;
            io_w.advance(n);
            return n;
        }
    };

    /// `std.Io.Writer` adapter — see `TcpStream.writer`.
    pub const Writer = struct {
        stream: *TcpStream,
        interface: std.Io.Writer,
        err: ?reactor_mod.IoError = null,

        pub fn init(s: *TcpStream, buffer: []u8) Writer {
            return .{
                .stream = s,
                .interface = .{
                    .vtable = &.{ .drain = drainImpl },
                    .buffer = buffer,
                    .end = 0,
                },
            };
        }

        fn drainImpl(io_w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *Writer = @alignCast(@fieldParentPtr("interface", io_w));
            const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
            const pd = pd_handle.ensure(&self.stream.pd, rt, self.stream.fd) catch {
                self.err = error.SystemResources;
                return error.WriteFailed;
            };

            // First, drain the writer's internal buffer.
            const buffered = io_w.buffer[0..io_w.end];
            if (buffered.len > 0) {
                reactor_mod.writeAll(&rt.reactor, self.stream.fd, pd, buffered) catch |err| {
                    self.err = err;
                    return error.WriteFailed;
                };
                io_w.end = 0;
            }

            // Then write each data slice. The last slice is repeated
            // `splat` times; others written once. drain returns the
            // count of `data` bytes written (excluding `buffer`).
            var total: usize = 0;
            const last_idx = data.len - 1;
            for (data, 0..) |slice, i| {
                const repeats: usize = if (i == last_idx) splat else 1;
                var rep: usize = 0;
                while (rep < repeats) : (rep += 1) {
                    reactor_mod.writeAll(&rt.reactor, self.stream.fd, pd, slice) catch |err| {
                        self.err = err;
                        return error.WriteFailed;
                    };
                    total += slice.len;
                }
            }
            return total;
        }
    };
};

// Note: the `EAGAIN` constant is defined at the top of this file
// via a `builtin.os.tag` switch. `isAgain` is no longer needed —
// callers compare against the platform-specific `EAGAIN` directly.

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

// `volt.testing.allocator` — leak-detecting + multi-worker-safe.
// See src/testing.zig for why std.testing.allocator doesn't fit.
const test_allocator = @import("testing.zig").allocator;

fn testListenerLifecycle() !void {
    var listener = try TcpListener.bind(Address.loopback4(0));
    defer listener.close();
    const addr = try listener.localAddress();
    if (addr.port() == 0) return error.BadPort;
}

test "TcpListener: bind + localAddress" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    try (try rt.run(testListenerLifecycle, .{}));
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

    var listener = try TcpListener.bind(Address.loopback4(0));
    defer listener.close();
    var ctx = EchoTestCtx{ .listener = &listener };
    ctx.addr = try listener.localAddress();
    try (try rt.run(echoTestRoot, .{&ctx}));

    try std.testing.expect(ctx.server_done);
    try std.testing.expect(ctx.client_done);
    try std.testing.expectEqual(@as(u8, 0x42), ctx.result_byte);
}

// std.Io.Reader/Writer adapter round-trip — proves the adapters work
// with the new Zig 0.16 vtable. The server reads via the std.Io.Reader
// (formatter-friendly path), and the client writes via std.Io.Writer.

const IoAdapterCtx = struct {
    listener: *TcpListener,
    addr: Address = undefined,
    server_done: bool = false,
    client_done: bool = false,
    received: u8 = 0,
};

fn ioAdapterServer(ctx: *IoAdapterCtx) !void {
    var stream = try ctx.listener.accept();
    defer stream.close();
    var read_buf: [16]u8 = undefined;
    var reader = stream.reader(&read_buf);
    // Use std.Io.Reader's takeByte to consume one byte; proves the
    // adapter participates correctly in std's buffered-read protocol.
    const byte = try reader.interface.takeByte();
    ctx.received = byte;
    ctx.server_done = true;
}

fn ioAdapterClient(ctx: *IoAdapterCtx) !void {
    var stream = try TcpStream.connect(ctx.addr);
    defer stream.close();
    var write_buf: [16]u8 = undefined;
    var writer = stream.writer(&write_buf);
    // Write via std.Io.Writer's writeAll. The byte fits in the
    // 16-byte buffer; flush forces a drain → reactor.writeAll path.
    try writer.interface.writeAll(&[_]u8{0x77});
    try writer.interface.flush();
    ctx.client_done = true;
}

fn ioAdapterRoot(ctx: *IoAdapterCtx) !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
    var server = try rt.spawn(ioAdapterServer, .{ctx});
    var client = try rt.spawn(ioAdapterClient, .{ctx});
    _ = server.join() catch |err| return err;
    _ = client.join() catch |err| return err;
}

test "TcpStream: std.Io.Reader/Writer adapter round-trip" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();

    var listener = try TcpListener.bind(Address.loopback4(0));
    defer listener.close();
    var ctx = IoAdapterCtx{ .listener = &listener };
    ctx.addr = try listener.localAddress();
    try (try rt.run(ioAdapterRoot, .{&ctx}));

    try std.testing.expect(ctx.server_done);
    try std.testing.expect(ctx.client_done);
    try std.testing.expectEqual(@as(u8, 0x77), ctx.received);
}

// Cancel-aware accept: a server parks in accept; a separate
// coro fires the Cancel; the parked accept must wake with
// `error.Cancelled` rather than hanging forever. Exercises the
// reactor's `waitReadableCancel` path end to end on the platform
// reactor.
const CancelAcceptCtx = struct {
    listener: *TcpListener,
    cancel: *@import("cancel.zig").Cancel,
    got: ?anyerror = null,
};

fn cancelAcceptServer(ctx: *CancelAcceptCtx) !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
    // Direct reactor path: we want to test the cancel hook,
    // not the higher-level accept wrapper.
    const pd = try pd_handle.ensure(&ctx.listener.pd, rt, ctx.listener.fd);
    const result = rt.reactor.waitFdCancel(ctx.listener.fd, pd, .read, ctx.cancel);
    ctx.got = if (result) |_| null else |e| e;
}

fn cancelAcceptFirer(ctx: *CancelAcceptCtx) void {
    // Yield a few times so the server actually parks before we fire.
    var i: u32 = 0;
    while (i < 50) : (i += 1) @import("runtime.zig").yield();
    ctx.cancel.fire();
}

fn cancelAcceptRoot(ctx: *CancelAcceptCtx) !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
    var server = try rt.spawn(cancelAcceptServer, .{ctx});
    var firer = try rt.spawn(cancelAcceptFirer, .{ctx});
    _ = server.join() catch |err| return err;
    _ = firer.join();
}

// ─── TCP options + split halves ──────────────────────────────────────

fn testTcpOptionsRoot(_: *void) !void {
    var listener = try TcpListener.bind(Address.loopback4(0));
    defer listener.close();
    // SO_REUSEPORT — should round-trip without error on the listener.
    try listener.setReusePort(true);
    try listener.setRecvBufferSize(64 * 1024);

    const local = try listener.localAddress();
    var client = try TcpStream.connect(local);
    defer client.close();

    // TCP_NODELAY round-trip + KEEPALIVE configure.
    try client.setNoDelay(true);
    try std.testing.expect(try client.getNoDelay());
    try client.setKeepalive(.{ .idle = 30, .interval = 10, .retries = 3 });

    // peerAddress / localAddress accessors.
    const peer = try client.peerAddress();
    try std.testing.expectEqual(local.port(), peer.port());
}

test "TcpStream: TCP options + peerAddress/localAddress" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    var dummy: void = {};
    try (try rt.run(testTcpOptionsRoot, .{&dummy}));
}

const SplitCtx = struct {
    listener: *TcpListener,
    addr: Address = undefined,
    received: [4]u8 = undefined,
    server_done: bool = false,
};

fn splitServer(ctx: *SplitCtx) !void {
    var stream = try ctx.listener.accept();
    defer stream.close();
    var halves = stream.split();
    _ = try halves.read.readFull(&ctx.received);
    try halves.write.writeAll(&ctx.received);
    ctx.server_done = true;
}

fn splitClient(ctx: *SplitCtx) !void {
    var stream = try TcpStream.connect(ctx.addr);
    defer stream.close();
    var halves = stream.split();
    try halves.write.writeAll(&[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF });
    var echo: [4]u8 = undefined;
    _ = try halves.read.readFull(&echo);
    try std.testing.expectEqual([_]u8{ 0xDE, 0xAD, 0xBE, 0xEF }, echo);
}

fn splitRoot(ctx: *SplitCtx) !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
    var srv = try rt.spawn(splitServer, .{ctx});
    var cli = try rt.spawn(splitClient, .{ctx});
    _ = srv.join() catch |e| return e;
    _ = cli.join() catch |e| return e;
}

test "TcpStream.split: borrowed halves echo round-trip" {
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    var listener = try TcpListener.bind(Address.loopback4(0));
    defer listener.close();
    var ctx = SplitCtx{ .listener = &listener };
    ctx.addr = try listener.localAddress();
    try (try rt.run(splitRoot, .{&ctx}));
    try std.testing.expect(ctx.server_done);
}

// ─── IPv6 round-trip ───────────────────────────────────────────────

fn ipv6EchoServer(ctx: *EchoTestCtx) !void {
    var stream = try ctx.listener.accept();
    defer stream.close();
    var buf: [4]u8 = undefined;
    _ = try stream.readFull(&buf);
    try stream.writeAll(&buf);
    ctx.server_done = true;
}

fn ipv6EchoClient(ctx: *EchoTestCtx) !void {
    var stream = try TcpStream.connect(ctx.addr);
    defer stream.close();
    try stream.writeAll(&[_]u8{ 1, 2, 3, 4 });
    var got: [4]u8 = undefined;
    _ = try stream.readFull(&got);
    ctx.result_byte = got[3];
    ctx.client_done = true;
}

fn ipv6EchoRoot(ctx: *EchoTestCtx) !void {
    const rt: *runtime.Runtime = @ptrCast(@alignCast(current.require().runtime));
    var s = try rt.spawn(ipv6EchoServer, .{ctx});
    var c = try rt.spawn(ipv6EchoClient, .{ctx});
    _ = s.join() catch |e| return e;
    _ = c.join() catch |e| return e;
}

test "TCP echo over IPv6 loopback (::1)" {
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;
    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();

    var listener = try TcpListener.bind(Address.loopback6(0));
    defer listener.close();
    var ctx = EchoTestCtx{ .listener = &listener };
    ctx.addr = try listener.localAddress();
    try (try rt.run(ipv6EchoRoot, .{&ctx}));

    try std.testing.expect(ctx.server_done);
    try std.testing.expect(ctx.client_done);
    try std.testing.expectEqual(@as(u8, 4), ctx.result_byte);
}

test "reactor: waitReadableCancel wakes a parked accept on Cancel.fire" {
    // Skipped on Windows: IOCP's readiness probe is a zero-byte
    // WSARecv, which isn't valid on a *listening* socket — the
    // accept path uses AcceptEx (no cancel-aware variant yet).
    // Cancel-during-read on a connected socket is exercised by
    // higher-level libraries.
    if (comptime builtin.os.tag == .windows) return error.SkipZigTest;

    var rt = try runtime.Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();

    var listener = try TcpListener.bind(Address.loopback4(0));
    defer listener.close();
    var cancel = @import("cancel.zig").Cancel.init(rt);
    defer cancel.deinit();
    var ctx = CancelAcceptCtx{ .listener = &listener, .cancel = &cancel };
    try (try rt.run(cancelAcceptRoot, .{&ctx}));

    try std.testing.expectEqual(@as(?anyerror, error.Cancelled), ctx.got);
}

// Linux-only: force the epoll backend (auto-mode silently picks
// io_uring on every modern kernel, leaving the epoll path
// untested in CI). Same workload as the auto-mode TCP echo above
// — gives us regression coverage on the second Linux reactor.
test "TCP echo round-trip on forced .epoll backend (Linux only)" {
    if (comptime builtin.os.tag != .linux) return error.SkipZigTest;
    var rt = try runtime.Runtime.init(.{
        .allocator = test_allocator,
        .workers = 1,
        .io_backend = .epoll,
    });
    defer rt.deinit();

    var listener = try TcpListener.bind(Address.loopback4(0));
    defer listener.close();
    var ctx = EchoTestCtx{ .listener = &listener };
    ctx.addr = try listener.localAddress();
    try (try rt.run(echoTestRoot, .{&ctx}));

    try std.testing.expect(ctx.server_done);
    try std.testing.expect(ctx.client_done);
    try std.testing.expectEqual(@as(u8, 0x42), ctx.result_byte);
}
