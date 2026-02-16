//! TcpListener - Server Socket
//!
//! TCP listener for accepting incoming connections with ScheduledIo integration.

const std = @import("std");
const c = @import("common.zig");
const posix = c.posix;
const mem = c.mem;
const Address = c.Address;
const ScheduledIo = c.ScheduledIo;
const Waker = c.Waker;
const FutureWaker = c.FutureWaker;
const FutureContext = c.FutureContext;
const FuturePollResult = c.FuturePollResult;

const setBoolOption = c.setBoolOption;
const setIntOption = c.setIntOption;
const getIntOption = c.getIntOption;
const updateStoredWaker = c.updateStoredWaker;
const bridgeWaker = c.bridgeWaker;
const cleanupStoredWaker = c.cleanupStoredWaker;

const TcpSocket = @import("socket.zig").TcpSocket;
const TcpStream = @import("stream.zig").TcpStream;
const ReadableFuture = @import("stream.zig").ReadableFuture;

// ═══════════════════════════════════════════════════════════════════════════════
// TcpListener
// ═══════════════════════════════════════════════════════════════════════════════

/// TCP listener for accepting incoming connections.
///
/// Uses ScheduledIo for readiness tracking when registered with the runtime.
///
/// ## Example
///
/// ```zig
/// var listener = try TcpListener.bind(Address.fromPort(8080));
/// defer listener.close();
///
/// while (true) {
///     if (try listener.tryAccept()) |result| {
///         // Handle result.stream, result.peer_addr
///     }
/// }
/// ```
pub const TcpListener = struct {
    fd: posix.socket_t,
    local_addr: Address,
    scheduled_io: ?*ScheduledIo,

    // ═══════════════════════════════════════════════════════════════════════════
    // Construction
    // ═══════════════════════════════════════════════════════════════════════════

    /// Bind and listen on an address with default backlog (128).
    pub fn bind(addr: Address) !TcpListener {
        return bindWithBacklog(addr, 128);
    }

    /// Bind and listen with a specific backlog.
    pub fn bindWithBacklog(addr: Address, backlog: u31) !TcpListener {
        // Create socket
        const family = addr.family();
        const fd = try posix.socket(
            family,
            posix.SOCK.STREAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC,
            0,
        );
        errdefer posix.close(fd);

        // Set SO_REUSEADDR
        try setBoolOption(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, true);

        // Bind
        try posix.bind(fd, addr.sockaddr(), addr.len);

        // Get actual bound address
        var local_addr: Address = undefined;
        local_addr.len = @sizeOf(posix.sockaddr.storage);
        try posix.getsockname(fd, local_addr.sockaddrMut(), &local_addr.len);

        // Listen
        try posix.listen(fd, backlog);

        return .{
            .fd = fd,
            .local_addr = local_addr,
            .scheduled_io = null,
        };
    }

    /// Create from a pre-configured TcpSocket.
    pub fn fromSocket(socket: TcpSocket, backlog: u31) !TcpListener {
        return socket.listen(backlog);
    }

    /// Create from a std.net.Server (takes ownership).
    /// Use this for interop with standard library code.
    pub fn fromStd(std_server: std.net.Server) TcpListener {
        var local_addr: Address = undefined;
        local_addr.len = @sizeOf(posix.sockaddr.storage);
        posix.getsockname(std_server.stream.handle, local_addr.sockaddrMut(), &local_addr.len) catch {
            local_addr = Address.fromPort(0);
        };

        return .{
            .fd = std_server.stream.handle,
            .local_addr = local_addr,
            .scheduled_io = null,
        };
    }

    /// Convert to std.net.Server.
    /// The underlying fd is transferred (this TcpListener becomes invalid).
    pub fn toStd(self: *TcpListener) std.net.Server {
        const fd = self.fd;
        self.fd = -1;
        return .{ .stream = .{ .handle = fd } };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Accept Methods
    // ═══════════════════════════════════════════════════════════════════════════

    /// Non-blocking accept - returns null if no connection waiting.
    pub fn tryAccept(self: *TcpListener) !?AcceptResult {
        var peer_addr: Address = undefined;
        peer_addr.len = @sizeOf(posix.sockaddr.storage);

        const client_fd = posix.accept(
            self.fd,
            peer_addr.sockaddrMut(),
            &peer_addr.len,
            posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC,
        ) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };

        return .{
            .stream = .{
                .fd = client_fd,
                .peer_addr = peer_addr,
                .local_addr = null,
                .scheduled_io = null,
            },
            .peer_addr = peer_addr,
        };
    }

    /// Return a future for async accept.
    /// The future polls for readiness using ScheduledIo.
    pub fn accept(self: *TcpListener) AcceptFuture {
        return .{ .listener = self };
    }

    /// Return a future that resolves when the listener is readable.
    pub fn readable(self: *TcpListener) ReadableFuture {
        return .{ .fd = self.fd, .scheduled_io = self.scheduled_io };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Configuration
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get local address.
    pub fn localAddr(self: TcpListener) Address {
        return self.local_addr;
    }

    /// Get underlying file descriptor.
    pub fn fileno(self: TcpListener) posix.socket_t {
        return self.fd;
    }

    /// Set IP TTL.
    pub fn setTtl(self: *TcpListener, ttl: u8) !void {
        const IP_TTL: u32 = if (@import("builtin").os.tag == .linux) 2 else 4;
        try setIntOption(self.fd, posix.IPPROTO.IP, IP_TTL, ttl);
    }

    /// Get IP TTL.
    pub fn getTtl(self: TcpListener) !u8 {
        const IP_TTL: u32 = 2;
        const val = try getIntOption(self.fd, posix.IPPROTO.IP, IP_TTL);
        return @intCast(val);
    }

    /// Close the listener.
    pub fn close(self: *TcpListener) void {
        if (self.scheduled_io) |sio| {
            sio.shutdown();
        }
        posix.close(self.fd);
        self.fd = -1;
    }

    /// Register with ScheduledIo for async operations.
    pub fn register(self: *TcpListener, sio: *ScheduledIo) void {
        self.scheduled_io = sio;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// AcceptResult
// ═══════════════════════════════════════════════════════════════════════════════

/// Result of a successful accept.
///
/// Contains both the connected stream and the peer's address.
pub const AcceptResult = struct {
    stream: TcpStream,
    peer_addr: Address,

    /// Get just the stream, discarding the peer address.
    pub fn intoStream(self: AcceptResult) TcpStream {
        return self.stream;
    }

    /// Get the peer address IP as a formatted string (without port).
    pub fn peerAddrString(self: AcceptResult, buf: []u8) []u8 {
        return self.peer_addr.ipString(buf);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// AcceptFuture
// ═══════════════════════════════════════════════════════════════════════════════

/// Future for async accept.
pub const AcceptFuture = struct {
    pub const Output = anyerror!AcceptResult;

    listener: *TcpListener,
    stored_waker: ?FutureWaker = null,

    /// Poll for a new connection.
    pub fn poll(self: *AcceptFuture, ctx: *FutureContext) FuturePollResult(Output) {
        // Try non-blocking accept
        if (self.listener.tryAccept()) |result| {
            return .{ .ready = result };
        } else |err| {
            if (err == error.WouldBlock) {
                // Register for readiness notification
                updateStoredWaker(&self.stored_waker, ctx);
                if (self.listener.scheduled_io) |sio| {
                    sio.setReaderWaker(bridgeWaker(&self.stored_waker));
                }
                return .pending;
            }
            return .{ .ready = err };
        }
    }

    pub fn deinit(self: *AcceptFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "TcpListener - bind to ephemeral port" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    // Should have been assigned a port by the OS
    const port = listener.localAddr().port();
    try testing.expect(port > 0);
}

test "TcpListener - bindWithBacklog" {
    var listener = try TcpListener.bindWithBacklog(Address.fromPort(0), 64);
    defer listener.close();

    try testing.expect(listener.localAddr().port() > 0);
}

test "TcpListener - tryAccept returns null when no connections" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    const result = try listener.tryAccept();
    try testing.expect(result == null);
}

test "TcpListener - tryAccept returns AcceptResult on connection" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    const lport = listener.localAddr().port();

    // Connect a client
    var client = try TcpStream.connect(Address.loopbackV4(lport));
    defer client.close();

    // Accept with retry
    var accepted = false;
    for (0..100) |_| {
        if (try listener.tryAccept()) |result| {
            var stream = result.stream;
            defer stream.close();
            try testing.expect(result.peer_addr.port() > 0);
            accepted = true;
            break;
        }
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    try testing.expect(accepted);
}

test "TcpListener - fileno returns valid fd" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    try testing.expect(listener.fileno() >= 0);
}

test "TcpListener - setTtl/getTtl" {
    // IP_TTL getsockopt not supported on macOS
    if (comptime @import("builtin").os.tag != .linux) return error.SkipZigTest;

    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    try listener.setTtl(42);
    try testing.expectEqual(@as(u8, 42), try listener.getTtl());
}

test "TcpListener - fromSocket" {
    var socket = try TcpSocket.newV4();
    try socket.setReuseAddr(true);
    try socket.bind(Address.fromPort(0));

    var listener = try TcpListener.fromSocket(socket, 128);
    defer listener.close();

    try testing.expect(listener.localAddr().port() > 0);
}

test "TcpListener - AcceptResult helpers" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    var client = try TcpStream.connect(Address.loopbackV4(listener.localAddr().port()));
    defer client.close();

    for (0..100) |_| {
        if (try listener.tryAccept()) |result| {
            // Test intoStream
            var stream = result.intoStream();
            defer stream.close();

            // Test peerAddrString
            var buf: [64]u8 = undefined;
            const addr_str = result.peerAddrString(&buf);
            try testing.expect(addr_str.len > 0);
            break;
        }
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
}
