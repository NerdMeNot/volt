//! TcpStream - Connected Socket
//!
//! A connected TCP stream with async read/write, readiness polling, and futures.

const std = @import("std");
const c = @import("common.zig");
const posix = c.posix;
const mem = c.mem;
const builtin = c.builtin;
const Address = c.Address;
const ScheduledIo = c.ScheduledIo;
const Ready = c.Ready;
const Interest = c.Interest;
const Waker = c.Waker;
const FutureWaker = c.FutureWaker;
const FutureContext = c.FutureContext;
const FuturePollResult = c.FuturePollResult;
const Duration = c.Duration;
const io = c.io;

const setBoolOption = c.setBoolOption;
const getBoolOption = c.getBoolOption;
const setIntOption = c.setIntOption;
const getIntOption = c.getIntOption;
const setNonBlocking = c.setNonBlocking;
const updateStoredWaker = c.updateStoredWaker;
const bridgeWaker = c.bridgeWaker;
const cleanupStoredWaker = c.cleanupStoredWaker;

const TcpSocket = @import("socket.zig").TcpSocket;
const Keepalive = @import("socket.zig").Keepalive;
const split = @import("split.zig");
pub const ReadHalf = split.ReadHalf;
pub const WriteHalf = split.WriteHalf;
pub const OwnedReadHalf = split.OwnedReadHalf;
pub const OwnedWriteHalf = split.OwnedWriteHalf;
pub const SharedStream = split.SharedStream;

// ═══════════════════════════════════════════════════════════════════════════════
// TcpStream
// ═══════════════════════════════════════════════════════════════════════════════

/// A connected TCP stream with async read/write.
///
/// Provides both non-blocking try* methods and async futures for I/O.
///
/// ## Example
///
/// ```zig
/// var stream = try TcpStream.connect(addr);
/// defer stream.close();
///
/// // Non-blocking
/// if (try stream.tryWrite("Hello")) |n| {
///     // Wrote n bytes
/// }
///
/// // Or with futures (when runtime is available)
/// const future = stream.write("Hello");
/// ```
pub const TcpStream = struct {
    fd: posix.socket_t,
    peer_addr: Address,
    local_addr: ?Address,
    scheduled_io: ?*ScheduledIo,

    // ═══════════════════════════════════════════════════════════════════════════
    // Construction
    // ═══════════════════════════════════════════════════════════════════════════

    /// Connect to a remote address.
    pub fn connect(addr: Address) !TcpStream {
        var socket = if (addr.isIpv6())
            try TcpSocket.newV6()
        else
            try TcpSocket.newV4();

        return socket.connect(addr);
    }

    /// Create from a TcpSocket (consumes the socket).
    pub fn fromSocket(socket: TcpSocket, addr: Address) !TcpStream {
        return socket.connect(addr);
    }

    /// Create from a std.net.Stream (takes ownership).
    /// Use this for interop with standard library code.
    pub fn fromStd(std_stream: std.net.Stream) TcpStream {
        // Get peer address
        var peer_addr: Address = undefined;
        peer_addr.len = @sizeOf(posix.sockaddr.storage);
        posix.getpeername(std_stream.handle, peer_addr.sockaddrMut(), &peer_addr.len) catch {
            peer_addr = Address.fromPort(0);
        };

        return .{
            .fd = std_stream.handle,
            .peer_addr = peer_addr,
            .local_addr = null,
            .scheduled_io = null,
        };
    }

    /// Convert to std.net.Stream.
    /// The underlying fd is transferred (this TcpStream becomes invalid).
    pub fn toStd(self: *TcpStream) std.net.Stream {
        const fd = self.fd;
        self.fd = c.INVALID_SOCKET;
        return .{ .handle = fd };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Non-Blocking I/O
    // ═══════════════════════════════════════════════════════════════════════════

    /// Try to read without blocking.
    /// Returns: bytes read, 0 for EOF, null for WouldBlock.
    pub fn tryRead(self: *TcpStream, buf: []u8) !?usize {
        const n = posix.recv(self.fd, buf, 0) catch |err| switch (err) {
            error.WouldBlock => return null,
            error.ConnectionResetByPeer => return 0,
            else => return err,
        };
        return n;
    }

    /// Try to write without blocking.
    /// Returns: bytes written, null for WouldBlock.
    pub fn tryWrite(self: *TcpStream, data: []const u8) !?usize {
        const n = posix.send(self.fd, data, 0) catch |err| switch (err) {
            error.WouldBlock => return null,
            error.BrokenPipe, error.ConnectionResetByPeer => return 0,
            else => return err,
        };
        return n;
    }

    /// Try to read vectored (scatter I/O).
    pub fn tryReadVectored(self: *TcpStream, iovs: []posix.iovec) !?usize {
        const n = posix.readv(self.fd, iovs) catch |err| switch (err) {
            error.WouldBlock => return null,
            error.ConnectionResetByPeer => return 0,
            else => return err,
        };
        return n;
    }

    /// Try to write vectored (gather I/O).
    pub fn tryWriteVectored(self: *TcpStream, iovs: []const posix.iovec_const) !?usize {
        const n = posix.writev(self.fd, iovs) catch |err| switch (err) {
            error.WouldBlock => return null,
            error.BrokenPipe, error.ConnectionResetByPeer => return 0,
            else => return err,
        };
        return n;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Async I/O (Futures)
    // ═══════════════════════════════════════════════════════════════════════════

    /// Return a future for async read.
    pub fn read(self: *TcpStream, buf: []u8) ReadFuture {
        return .{ .stream = self, .buf = buf };
    }

    /// Return a future for async write.
    pub fn write(self: *TcpStream, data: []const u8) WriteFuture {
        return .{ .stream = self, .data = data };
    }

    /// Return a future that writes all data.
    pub fn writeAll(self: *TcpStream, data: []const u8) WriteAllFuture {
        return WriteAllFuture.init(self, data);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Reader/Writer Interfaces (for TLS composition)
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get a polymorphic Reader interface for this stream.
    pub fn reader(self: *TcpStream) io.Reader {
        return .{
            .context = @ptrCast(self),
            .readFn = tcpReadFn,
        };
    }

    /// Get a polymorphic Writer interface for this stream.
    pub fn writer(self: *TcpStream) io.Writer {
        return .{
            .context = @ptrCast(self),
            .writeFn = tcpWriteFn,
            .flushFn = null, // TCP doesn't buffer at this level
        };
    }

    fn tcpReadFn(ctx: *anyopaque, buffer: []u8) io.Error!usize {
        const self: *TcpStream = @ptrCast(@alignCast(ctx));
        const n = posix.recv(self.fd, buffer, 0) catch |err| switch (err) {
            error.WouldBlock => return error.WouldBlock,
            error.ConnectionResetByPeer => return error.ConnectionReset,
            error.ConnectionRefused => return error.ConnectionRefused,
            error.ConnectionTimedOut => return error.TimedOut,
            else => return error.Unexpected,
        };
        return n;
    }

    fn tcpWriteFn(ctx: *anyopaque, data: []const u8) io.Error!usize {
        const self: *TcpStream = @ptrCast(@alignCast(ctx));
        const n = posix.send(self.fd, data, 0) catch |err| switch (err) {
            error.WouldBlock => return error.WouldBlock,
            error.ConnectionResetByPeer => return error.ConnectionReset,
            error.BrokenPipe => return error.BrokenPipe,
            error.ConnectionRefused => return error.ConnectionRefused,
            error.NetworkUnreachable => return error.NetworkUnreachable,
            else => return error.Unexpected,
        };
        return n;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Readiness Polling
    // ═══════════════════════════════════════════════════════════════════════════

    /// Return a future that resolves when the stream is readable.
    pub fn readable(self: *TcpStream) ReadableFuture {
        return .{ .fd = self.fd, .scheduled_io = self.scheduled_io };
    }

    /// Return a future that resolves when the stream is writable.
    pub fn writable(self: *TcpStream) WritableFuture {
        return .{ .fd = self.fd, .scheduled_io = self.scheduled_io };
    }

    /// Return a future that resolves when any of the specified interests is ready.
    pub fn ready(self: *TcpStream, interest: Interest) ReadyFuture {
        return .{ .fd = self.fd, .scheduled_io = self.scheduled_io, .interest = interest };
    }

    /// Return a future for async peek (waits for data, then peeks).
    pub fn peekAsync(self: *TcpStream, buf: []u8) PeekFuture {
        return .{ .stream = self, .buf = buf };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Custom I/O Operations
    // ═══════════════════════════════════════════════════════════════════════════

    /// Try to perform a custom I/O operation.
    pub fn tryIo(
        self: *TcpStream,
        interest: Interest,
        comptime io_fn: fn (posix.socket_t) anyerror!usize,
    ) !?usize {
        _ = interest;
        return io_fn(self.fd) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Socket Options
    // ═══════════════════════════════════════════════════════════════════════════

    /// Set TCP_NODELAY (disable Nagle's algorithm).
    pub fn setNoDelay(self: *TcpStream, value: bool) !void {
        try setBoolOption(self.fd, posix.IPPROTO.TCP, std.posix.TCP.NODELAY, value);
    }

    /// Get TCP_NODELAY.
    pub fn getNoDelay(self: TcpStream) !bool {
        return getBoolOption(self.fd, posix.IPPROTO.TCP, std.posix.TCP.NODELAY);
    }

    /// Set IP TTL.
    pub fn setTtl(self: *TcpStream, ttl: u8) !void {
        const IP_TTL: u32 = if (builtin.os.tag == .linux) 2 else 4;
        try setIntOption(self.fd, posix.IPPROTO.IP, IP_TTL, ttl);
    }

    /// Get IP TTL.
    pub fn getTtl(self: TcpStream) !u8 {
        const IP_TTL: u32 = 2;
        const val = try getIntOption(self.fd, posix.IPPROTO.IP, IP_TTL);
        return @intCast(val);
    }

    /// Set keepalive options.
    pub fn setKeepalive(self: *TcpStream, keepalive: ?Keepalive) !void {
        var socket = TcpSocket{ .fd = self.fd };
        try socket.setKeepalive(keepalive);
    }

    /// Set SO_LINGER.
    pub fn setLinger(self: *TcpStream, duration: ?Duration) !void {
        var socket = TcpSocket{ .fd = self.fd };
        try socket.setLinger(duration);
    }

    /// Get and clear the pending socket error (SO_ERROR).
    pub fn takeError(self: *TcpStream) !?anyerror {
        var buf: [4]u8 = undefined;
        try posix.getsockopt(self.fd, posix.SOL.SOCKET, posix.SO.ERROR, &buf);
        const err_val = mem.readInt(u32, &buf, .little);
        if (err_val == 0) return null;
        return posix.unexpectedErrno(@enumFromInt(@as(u16, @intCast(err_val))));
    }

    /// Set TCP_QUICKACK (Linux only) - disable delayed ACKs.
    pub fn setQuickAck(self: *TcpStream, value: bool) !void {
        if (comptime builtin.os.tag != .linux) {
            return error.NotSupported;
        }
        try setBoolOption(self.fd, posix.IPPROTO.TCP, std.posix.TCP.QUICKACK, value);
    }

    /// Get TCP_QUICKACK (Linux only).
    pub fn getQuickAck(self: TcpStream) !bool {
        if (comptime builtin.os.tag != .linux) {
            return error.NotSupported;
        }
        return getBoolOption(self.fd, posix.IPPROTO.TCP, std.posix.TCP.QUICKACK);
    }

    /// Set IP_TOS (Type of Service / DSCP) for IPv4.
    pub fn setTos(self: *TcpStream, tos: u8) !void {
        const IP_TOS: u32 = if (builtin.os.tag == .linux) 1 else 3;
        try setIntOption(self.fd, posix.IPPROTO.IP, IP_TOS, tos);
    }

    /// Get IP_TOS value.
    pub fn getTos(self: TcpStream) !u8 {
        const IP_TOS: u32 = if (builtin.os.tag == .linux) 1 else 3;
        const val = try getIntOption(self.fd, posix.IPPROTO.IP, IP_TOS);
        return @intCast(val);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Stream Control
    // ═══════════════════════════════════════════════════════════════════════════

    /// Shutdown the connection.
    pub fn shutdown(self: *TcpStream, how: ShutdownHow) !void {
        try posix.shutdown(self.fd, how.toNative());
    }

    /// Peek at data without consuming it.
    pub fn peek(self: *TcpStream, buf: []u8) !usize {
        return posix.recv(self.fd, buf, posix.MSG.PEEK) catch |err| switch (err) {
            error.WouldBlock => return 0,
            else => return err,
        };
    }

    /// Get peer address.
    pub fn peerAddr(self: TcpStream) Address {
        return self.peer_addr;
    }

    /// Get local address (lazily resolved).
    pub fn localAddr(self: *TcpStream) !Address {
        if (self.local_addr) |addr| return addr;

        var addr: Address = undefined;
        addr.len = @sizeOf(posix.sockaddr.storage);
        try posix.getsockname(self.fd, addr.sockaddrMut(), &addr.len);
        self.local_addr = addr;
        return addr;
    }

    /// Get underlying file descriptor.
    pub fn fileno(self: TcpStream) posix.socket_t {
        return self.fd;
    }

    /// Close the stream.
    pub fn close(self: *TcpStream) void {
        if (self.scheduled_io) |sio| {
            sio.shutdown();
        }
        posix.close(self.fd);
        self.fd = c.INVALID_SOCKET;
    }

    /// Register with ScheduledIo for async operations.
    pub fn register(self: *TcpStream, sio: *ScheduledIo) void {
        self.scheduled_io = sio;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Splitting
    // ═══════════════════════════════════════════════════════════════════════════

    /// Split into borrowed read and write halves.
    /// The halves borrow from this stream and cannot outlive it.
    pub fn split_halves(self: *TcpStream) struct { read: ReadHalf, write: WriteHalf } {
        return .{
            .read = .{ .stream = self },
            .write = .{ .stream = self },
        };
    }

    // Alias for backward compatibility
    pub const split = split_halves;

    /// Split into owned halves that can be sent to different tasks.
    /// The stream is consumed. Use reunite() to reconstruct.
    pub fn intoSplit(self: *TcpStream, allocator: mem.Allocator) !struct { read: OwnedReadHalf, write: OwnedWriteHalf } {
        const shared = try allocator.create(SharedStream);
        shared.* = .{
            .fd = self.fd,
            .peer_addr = self.peer_addr,
            .local_addr = self.local_addr,
            .scheduled_io = self.scheduled_io,
            .ref_count = std.atomic.Value(u32).init(2),
            .id = @intFromPtr(shared),
            .allocator = allocator,
        };

        // Invalidate the original stream
        self.fd = c.INVALID_SOCKET;

        return .{
            .read = .{ .inner = shared },
            .write = .{ .inner = shared, .shutdown_on_drop = true },
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// ShutdownHow
// ═══════════════════════════════════════════════════════════════════════════════

/// Shutdown direction.
pub const ShutdownHow = enum {
    read,
    write,
    both,

    fn toNative(self: ShutdownHow) posix.ShutdownHow {
        return switch (self) {
            .read => .recv,
            .write => .send,
            .both => .both,
        };
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Future Types
// ═══════════════════════════════════════════════════════════════════════════════

/// Future for async read.
pub const ReadFuture = struct {
    pub const Output = anyerror!usize;

    stream: *TcpStream,
    buf: []u8,
    stored_waker: ?FutureWaker = null,

    pub fn poll(self: *ReadFuture, ctx: *FutureContext) FuturePollResult(Output) {
        if (self.stream.tryRead(self.buf)) |n_opt| {
            if (n_opt) |n| {
                return .{ .ready = n };
            }
        } else |err| {
            return .{ .ready = err };
        }

        // WouldBlock - register for notification
        updateStoredWaker(&self.stored_waker, ctx);
        if (self.stream.scheduled_io) |sio| {
            sio.setReaderWaker(bridgeWaker(&self.stored_waker));
        }
        return .pending;
    }

    pub fn deinit(self: *ReadFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for async write.
pub const WriteFuture = struct {
    pub const Output = anyerror!usize;

    stream: *TcpStream,
    data: []const u8,
    stored_waker: ?FutureWaker = null,

    pub fn poll(self: *WriteFuture, ctx: *FutureContext) FuturePollResult(Output) {
        if (self.stream.tryWrite(self.data)) |n_opt| {
            if (n_opt) |n| {
                return .{ .ready = n };
            }
        } else |err| {
            return .{ .ready = err };
        }

        // WouldBlock - register for notification
        updateStoredWaker(&self.stored_waker, ctx);
        if (self.stream.scheduled_io) |sio| {
            sio.setWriterWaker(bridgeWaker(&self.stored_waker));
        }
        return .pending;
    }

    pub fn deinit(self: *WriteFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for readable readiness.
pub const ReadableFuture = struct {
    pub const Output = void;

    fd: posix.socket_t,
    scheduled_io: ?*ScheduledIo,
    stored_waker: ?FutureWaker = null,

    pub fn poll(self: *ReadableFuture, ctx: *FutureContext) FuturePollResult(Output) {
        if (self.scheduled_io) |sio| {
            const event = sio.readiness();
            if (event.ready.isReadable()) {
                return .{ .ready = {} };
            }
            updateStoredWaker(&self.stored_waker, ctx);
            sio.setReaderWaker(bridgeWaker(&self.stored_waker));
        }
        return .pending;
    }

    pub fn deinit(self: *ReadableFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for writable readiness.
pub const WritableFuture = struct {
    pub const Output = void;

    fd: posix.socket_t,
    scheduled_io: ?*ScheduledIo,
    stored_waker: ?FutureWaker = null,

    pub fn poll(self: *WritableFuture, ctx: *FutureContext) FuturePollResult(Output) {
        if (self.scheduled_io) |sio| {
            const event = sio.readiness();
            if (event.ready.isWritable()) {
                return .{ .ready = {} };
            }
            updateStoredWaker(&self.stored_waker, ctx);
            sio.setWriterWaker(bridgeWaker(&self.stored_waker));
        }
        return .pending;
    }

    pub fn deinit(self: *WritableFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for generic interest-based readiness.
pub const ReadyFuture = struct {
    pub const Output = Ready;

    fd: posix.socket_t,
    scheduled_io: ?*ScheduledIo,
    interest: Interest,
    stored_waker: ?FutureWaker = null,

    pub fn poll(self: *ReadyFuture, ctx: *FutureContext) FuturePollResult(Output) {
        if (self.scheduled_io) |sio| {
            const event = sio.readiness();

            // Check if any requested interest is ready
            var matched = Ready{};
            if (self.interest.readable and event.ready.isReadable()) {
                matched.readable = true;
            }
            if (self.interest.writable and event.ready.isWritable()) {
                matched.writable = true;
            }

            if (matched.readable or matched.writable) {
                return .{ .ready = matched };
            }

            // Register for all requested interests
            updateStoredWaker(&self.stored_waker, ctx);
            if (self.interest.readable) {
                sio.setReaderWaker(bridgeWaker(&self.stored_waker));
            }
            if (self.interest.writable) {
                sio.setWriterWaker(bridgeWaker(&self.stored_waker));
            }
        }
        return .pending;
    }

    pub fn deinit(self: *ReadyFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for async peek.
pub const PeekFuture = struct {
    pub const Output = anyerror!usize;

    stream: *TcpStream,
    buf: []u8,
    stored_waker: ?FutureWaker = null,

    pub fn poll(self: *PeekFuture, ctx: *FutureContext) FuturePollResult(Output) {
        // Try to peek
        const n = posix.recv(self.stream.fd, self.buf, posix.MSG.PEEK) catch |err| switch (err) {
            error.WouldBlock => {
                // Register for readable notification
                updateStoredWaker(&self.stored_waker, ctx);
                if (self.stream.scheduled_io) |sio| {
                    sio.setReaderWaker(bridgeWaker(&self.stored_waker));
                }
                return .pending;
            },
            else => return .{ .ready = err },
        };
        return .{ .ready = n };
    }

    pub fn deinit(self: *PeekFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for write all (loops until complete).
pub const WriteAllFuture = struct {
    pub const Output = anyerror!void;

    stream: *TcpStream,
    data: []const u8,
    written: usize,
    stored_waker: ?FutureWaker = null,

    pub fn init(stream: *TcpStream, data: []const u8) WriteAllFuture {
        return .{ .stream = stream, .data = data, .written = 0 };
    }

    pub fn poll(self: *WriteAllFuture, ctx: *FutureContext) FuturePollResult(Output) {
        while (self.written < self.data.len) {
            if (self.stream.tryWrite(self.data[self.written..])) |n_opt| {
                if (n_opt) |n| {
                    if (n == 0) return .{ .ready = error.BrokenPipe };
                    self.written += n;
                    continue;
                }
            } else |err| {
                return .{ .ready = err };
            }

            // WouldBlock - register for notification
            updateStoredWaker(&self.stored_waker, ctx);
            if (self.stream.scheduled_io) |sio| {
                sio.setWriterWaker(bridgeWaker(&self.stored_waker));
            }
            return .pending;
        }
        return .{ .ready = {} };
    }

    pub fn deinit(self: *WriteAllFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn createConnectedPair() !struct { client: TcpStream, server: TcpStream, listener: @import("listener.zig").TcpListener } {
    const TcpListener = @import("listener.zig").TcpListener;
    var listener = try TcpListener.bind(Address.fromPort(0));

    const lport = listener.localAddr().port();
    const client = try TcpStream.connect(Address.loopbackV4(lport));

    var server_conn: ?TcpStream = null;
    for (0..100) |_| {
        if (try listener.tryAccept()) |result| {
            server_conn = result.stream;
            break;
        }
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }

    return .{
        .client = client,
        .server = server_conn orelse return error.NoConnection,
        .listener = listener,
    };
}

test "TcpStream - tryRead returns null on empty" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    var buf: [64]u8 = undefined;
    // No data sent yet — should return null (WouldBlock)
    const result = try pair.server.tryRead(&buf);
    try testing.expect(result == null);
}

test "TcpStream - tryWrite and tryRead roundtrip" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    // Write from client
    const data = "hello stream";
    var written: usize = 0;
    while (written < data.len) {
        if (try pair.client.tryWrite(data[written..])) |n| {
            written += n;
        } else {
            std.Thread.sleep(1_000_000);
        }
    }
    try testing.expectEqual(data.len, written);

    // Read on server with retry
    var buf: [64]u8 = undefined;
    var total: usize = 0;
    for (0..100) |_| {
        if (try pair.server.tryRead(buf[total..])) |n| {
            total += n;
            if (total >= data.len) break;
        }
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    try testing.expectEqualStrings(data, buf[0..total]);
}

test "TcpStream - EOF returns 0" {
    var pair = try createConnectedPair();
    defer pair.server.close();
    defer pair.listener.close();

    // Close the client side — server should see EOF
    pair.client.close();

    var buf: [64]u8 = undefined;
    var got_eof = false;
    for (0..100) |_| {
        if (try pair.server.tryRead(&buf)) |n| {
            if (n == 0) {
                got_eof = true;
                break;
            }
        }
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    try testing.expect(got_eof);
}

test "TcpStream - peek does not consume data" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    // Write data
    _ = try pair.client.tryWrite("peek test") orelse unreachable;

    // Wait for data to arrive
    std.Thread.sleep(50 * std.time.ns_per_ms);

    // Peek — data should still be in the buffer
    var buf1: [64]u8 = undefined;
    const peeked = try pair.server.peek(&buf1);
    try testing.expect(peeked > 0);

    // Read — should get same data
    var buf2: [64]u8 = undefined;
    const read_n = try pair.server.tryRead(&buf2) orelse 0;
    try testing.expect(read_n > 0);
    try testing.expectEqualStrings(buf1[0..peeked], buf2[0..read_n]);
}

test "TcpStream - shutdown write then read returns EOF" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    // Shutdown write direction on client
    try pair.client.shutdown(.write);

    // Server should see EOF
    var buf: [64]u8 = undefined;
    var got_eof = false;
    for (0..100) |_| {
        if (try pair.server.tryRead(&buf)) |n| {
            if (n == 0) {
                got_eof = true;
                break;
            }
        }
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    try testing.expect(got_eof);
}

test "TcpStream - setNoDelay get/set" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    try pair.client.setNoDelay(true);
    try testing.expect(try pair.client.getNoDelay());

    try pair.client.setNoDelay(false);
    try testing.expect(!try pair.client.getNoDelay());
}

test "TcpStream - setTtl get/set" {
    // IP_TTL getsockopt not supported on macOS
    if (comptime @import("builtin").os.tag != .linux) return error.SkipZigTest;

    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    try pair.client.setTtl(42);
    try testing.expectEqual(@as(u8, 42), try pair.client.getTtl());
}

test "TcpStream - setTos get/set" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    try pair.client.setTos(0x10); // Low delay
    try testing.expectEqual(@as(u8, 0x10), try pair.client.getTos());
}

test "TcpStream - setLinger" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    // Enable linger with 5 second timeout
    try pair.client.setLinger(Duration.fromSecs(5));
    // Disable linger
    try pair.client.setLinger(null);
}

test "TcpStream - setKeepalive" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    try pair.client.setKeepalive(.{ .time = Duration.fromSecs(60) });
    // Disable
    try pair.client.setKeepalive(null);
}

test "TcpStream - takeError returns null on healthy connection" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    const err = try pair.client.takeError();
    try testing.expect(err == null);
}

test "TcpStream - peerAddr" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    const peer = pair.client.peerAddr();
    try testing.expect(peer.port() > 0);
}

test "TcpStream - localAddr" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    const local = try pair.client.localAddr();
    try testing.expect(local.port() > 0);
}

test "TcpStream - fileno returns valid fd" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    const fd = pair.client.fileno();
    try testing.expect(c.isValidSocket(fd));
}

test "TcpStream - reader interface" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    // Write data
    var w = pair.client.writer();
    try w.writeAll("reader test");

    // Read using reader interface
    var r = pair.server.reader();
    var buf: [64]u8 = undefined;
    var total: usize = 0;
    for (0..100) |_| {
        const n = r.read(buf[total..]) catch |err| switch (err) {
            error.WouldBlock => {
                std.Thread.sleep(5 * std.time.ns_per_ms);
                continue;
            },
            else => return err,
        };
        if (n == 0) break;
        total += n;
        if (total >= 11) break;
    }
    try testing.expectEqualStrings("reader test", buf[0..total]);
}

test "TcpStream - vectored write" {
    var pair = try createConnectedPair();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    const part1 = "hello ";
    const part2 = "world";
    const iovecs = [_]posix.iovec_const{
        .{ .base = part1.ptr, .len = part1.len },
        .{ .base = part2.ptr, .len = part2.len },
    };

    var written: usize = 0;
    for (0..50) |_| {
        if (try pair.client.tryWriteVectored(&iovecs)) |n| {
            written = n;
            break;
        }
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    try testing.expectEqual(@as(usize, 11), written);

    // Read combined
    var buf: [64]u8 = undefined;
    var total: usize = 0;
    for (0..100) |_| {
        if (try pair.server.tryRead(buf[total..])) |n| {
            total += n;
            if (total >= 11) break;
        }
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    try testing.expectEqualStrings("hello world", buf[0..total]);
}

test "TcpStream - fromStd roundtrip" {
    var pair = try createConnectedPair();
    defer pair.server.close();
    defer pair.listener.close();

    // Convert to std
    const std_stream = pair.client.toStd();
    // Convert back
    var stream2 = TcpStream.fromStd(std_stream);
    defer stream2.close();

    // Should still be functional
    try stream2.setNoDelay(true);
    try testing.expect(try stream2.getNoDelay());
}
