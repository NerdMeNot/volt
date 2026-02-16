//! TCP Split Halves
//!
//! Borrowed and owned halves for concurrent read/write access to a TcpStream.

const std = @import("std");
const c = @import("common.zig");
const posix = c.posix;
const mem = c.mem;
const Address = c.Address;
const ScheduledIo = c.ScheduledIo;
const Ready = c.Ready;
const Interest = c.Interest;
const Waker = c.Waker;
const FutureWaker = c.FutureWaker;
const FutureContext = c.FutureContext;
const FuturePollResult = c.FuturePollResult;

const updateStoredWaker = c.updateStoredWaker;
const bridgeWaker = c.bridgeWaker;
const cleanupStoredWaker = c.cleanupStoredWaker;

const stream_mod = @import("stream.zig");
const TcpStream = stream_mod.TcpStream;
const ReadFuture = stream_mod.ReadFuture;
const WriteFuture = stream_mod.WriteFuture;
const WriteAllFuture = stream_mod.WriteAllFuture;
const ReadableFuture = stream_mod.ReadableFuture;
const WritableFuture = stream_mod.WritableFuture;
const ReadyFuture = stream_mod.ReadyFuture;
const PeekFuture = stream_mod.PeekFuture;

// ═══════════════════════════════════════════════════════════════════════════════
// Borrowed Halves
// ═══════════════════════════════════════════════════════════════════════════════

/// Borrowed read half - lifetime tied to TcpStream.
pub const ReadHalf = struct {
    stream: *TcpStream,

    pub fn tryRead(self: *ReadHalf, buf: []u8) !?usize {
        return self.stream.tryRead(buf);
    }

    pub fn tryReadVectored(self: *ReadHalf, iovs: []posix.iovec) !?usize {
        return self.stream.tryReadVectored(iovs);
    }

    pub fn read(self: *ReadHalf, buf: []u8) ReadFuture {
        return self.stream.read(buf);
    }

    pub fn readable(self: *ReadHalf) ReadableFuture {
        return self.stream.readable();
    }

    pub fn peek(self: *ReadHalf, buf: []u8) !usize {
        return self.stream.peek(buf);
    }

    pub fn peekAsync(self: *ReadHalf, buf: []u8) PeekFuture {
        return self.stream.peekAsync(buf);
    }

    pub fn ready(self: *ReadHalf, interest: Interest) ReadyFuture {
        return self.stream.ready(interest);
    }

    pub fn peerAddr(self: ReadHalf) Address {
        return self.stream.peer_addr;
    }

    pub fn localAddr(self: *ReadHalf) !Address {
        return self.stream.localAddr();
    }
};

/// Borrowed write half - lifetime tied to TcpStream.
pub const WriteHalf = struct {
    stream: *TcpStream,

    pub fn tryWrite(self: *WriteHalf, data: []const u8) !?usize {
        return self.stream.tryWrite(data);
    }

    pub fn tryWriteVectored(self: *WriteHalf, iovs: []const posix.iovec_const) !?usize {
        return self.stream.tryWriteVectored(iovs);
    }

    pub fn write(self: *WriteHalf, data: []const u8) WriteFuture {
        return self.stream.write(data);
    }

    pub fn writeAll(self: *WriteHalf, data: []const u8) WriteAllFuture {
        return self.stream.writeAll(data);
    }

    pub fn writable(self: *WriteHalf) WritableFuture {
        return self.stream.writable();
    }

    pub fn ready(self: *WriteHalf, interest: Interest) ReadyFuture {
        return self.stream.ready(interest);
    }

    pub fn peerAddr(self: WriteHalf) Address {
        return self.stream.peer_addr;
    }

    pub fn localAddr(self: *WriteHalf) !Address {
        return self.stream.localAddr();
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// SharedStream
// ═══════════════════════════════════════════════════════════════════════════════

/// Shared stream state for owned halves.
pub const SharedStream = struct {
    fd: posix.socket_t,
    peer_addr: Address,
    local_addr: ?Address,
    scheduled_io: ?*ScheduledIo,
    ref_count: std.atomic.Value(u32),
    id: u64,
    allocator: mem.Allocator,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Owned Halves
// ═══════════════════════════════════════════════════════════════════════════════

/// Owned read half - can be moved to different tasks.
pub const OwnedReadHalf = struct {
    inner: *SharedStream,

    pub fn tryRead(self: *OwnedReadHalf, buf: []u8) !?usize {
        const n = posix.recv(self.inner.fd, buf, 0) catch |err| switch (err) {
            error.WouldBlock => return null,
            error.ConnectionResetByPeer => return 0,
            else => return err,
        };
        return n;
    }

    pub fn tryReadVectored(self: *OwnedReadHalf, iovs: []posix.iovec) !?usize {
        const n = posix.readv(self.inner.fd, iovs) catch |err| switch (err) {
            error.WouldBlock => return null,
            error.ConnectionResetByPeer => return 0,
            else => return err,
        };
        return n;
    }

    pub fn read(self: *OwnedReadHalf, buf: []u8) OwnedReadFuture {
        return .{ .half = self, .buf = buf };
    }

    pub fn peek(self: *OwnedReadHalf, buf: []u8) !usize {
        return posix.recv(self.inner.fd, buf, posix.MSG.PEEK) catch |err| switch (err) {
            error.WouldBlock => return 0,
            else => return err,
        };
    }

    pub fn readable(self: *OwnedReadHalf) OwnedReadableFuture {
        return .{ .fd = self.inner.fd, .scheduled_io = self.inner.scheduled_io };
    }

    pub fn ready(self: *OwnedReadHalf, interest: Interest) OwnedReadyFuture {
        return .{ .fd = self.inner.fd, .scheduled_io = self.inner.scheduled_io, .interest = interest };
    }

    pub fn peerAddr(self: OwnedReadHalf) Address {
        return self.inner.peer_addr;
    }

    pub fn localAddr(self: *OwnedReadHalf) !Address {
        if (self.inner.local_addr) |addr| return addr;

        var addr: Address = undefined;
        addr.len = @sizeOf(posix.sockaddr.storage);
        try posix.getsockname(self.inner.fd, addr.sockaddrMut(), &addr.len);
        self.inner.local_addr = addr;
        return addr;
    }

    /// Reunite with write half to reconstruct TcpStream.
    pub fn reunite(self: OwnedReadHalf, write_half: OwnedWriteHalf) !TcpStream {
        if (self.inner != write_half.inner or self.inner.id != write_half.inner.id) {
            return error.ReuniteError;
        }

        const stream = TcpStream{
            .fd = self.inner.fd,
            .peer_addr = self.inner.peer_addr,
            .local_addr = self.inner.local_addr,
            .scheduled_io = self.inner.scheduled_io,
        };

        // Free the shared state
        self.inner.allocator.destroy(self.inner);

        return stream;
    }

    pub fn deinit(self: *OwnedReadHalf) void {
        const prev = self.inner.ref_count.fetchSub(1, .acq_rel);
        if (prev == 1) {
            // Last reference - close the socket
            posix.close(self.inner.fd);
            self.inner.allocator.destroy(self.inner);
        }
    }
};

/// Owned write half - can be moved to different tasks.
pub const OwnedWriteHalf = struct {
    inner: *SharedStream,
    shutdown_on_drop: bool,

    pub fn tryWrite(self: *OwnedWriteHalf, data: []const u8) !?usize {
        const n = posix.send(self.inner.fd, data, 0) catch |err| switch (err) {
            error.WouldBlock => return null,
            error.BrokenPipe, error.ConnectionResetByPeer => return 0,
            else => return err,
        };
        return n;
    }

    pub fn tryWriteVectored(self: *OwnedWriteHalf, iovs: []const posix.iovec_const) !?usize {
        const n = posix.writev(self.inner.fd, iovs) catch |err| switch (err) {
            error.WouldBlock => return null,
            error.BrokenPipe, error.ConnectionResetByPeer => return 0,
            else => return err,
        };
        return n;
    }

    pub fn write(self: *OwnedWriteHalf, data: []const u8) OwnedWriteFuture {
        return .{ .half = self, .data = data };
    }

    /// Return a future that writes all data.
    pub fn writeAll(self: *OwnedWriteHalf, data: []const u8) OwnedWriteAllFuture {
        return OwnedWriteAllFuture.init(self, data);
    }

    pub fn writable(self: *OwnedWriteHalf) OwnedWritableFuture {
        return .{ .fd = self.inner.fd, .scheduled_io = self.inner.scheduled_io };
    }

    pub fn ready(self: *OwnedWriteHalf, interest: Interest) OwnedReadyFuture {
        return .{ .fd = self.inner.fd, .scheduled_io = self.inner.scheduled_io, .interest = interest };
    }

    pub fn peerAddr(self: OwnedWriteHalf) Address {
        return self.inner.peer_addr;
    }

    pub fn localAddr(self: *OwnedWriteHalf) !Address {
        if (self.inner.local_addr) |addr| return addr;

        var addr: Address = undefined;
        addr.len = @sizeOf(posix.sockaddr.storage);
        try posix.getsockname(self.inner.fd, addr.sockaddrMut(), &addr.len);
        self.inner.local_addr = addr;
        return addr;
    }

    /// Prevent shutdown on drop.
    pub fn forget(self: *OwnedWriteHalf) void {
        self.shutdown_on_drop = false;
    }

    /// Reunite with read half to reconstruct TcpStream.
    pub fn reunite(self: OwnedWriteHalf, read_half: OwnedReadHalf) !TcpStream {
        return read_half.reunite(self);
    }

    pub fn deinit(self: *OwnedWriteHalf) void {
        if (self.shutdown_on_drop) {
            posix.shutdown(self.inner.fd, .send) catch {};
        }

        const prev = self.inner.ref_count.fetchSub(1, .acq_rel);
        if (prev == 1) {
            // Last reference - close the socket
            posix.close(self.inner.fd);
            self.inner.allocator.destroy(self.inner);
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Owned Half Futures
// ═══════════════════════════════════════════════════════════════════════════════

/// Future for async read on OwnedReadHalf.
pub const OwnedReadFuture = struct {
    pub const Output = anyerror!usize;

    half: *OwnedReadHalf,
    buf: []u8,
    stored_waker: ?FutureWaker = null,

    pub fn poll(self: *OwnedReadFuture, ctx: *FutureContext) FuturePollResult(Output) {
        if (self.half.tryRead(self.buf)) |n_opt| {
            if (n_opt) |n| {
                return .{ .ready = n };
            }
        } else |err| {
            return .{ .ready = err };
        }

        // WouldBlock - register for notification
        updateStoredWaker(&self.stored_waker, ctx);
        if (self.half.inner.scheduled_io) |sio| {
            sio.setReaderWaker(bridgeWaker(&self.stored_waker));
        }
        return .pending;
    }

    pub fn deinit(self: *OwnedReadFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for async write on OwnedWriteHalf.
pub const OwnedWriteFuture = struct {
    pub const Output = anyerror!usize;

    half: *OwnedWriteHalf,
    data: []const u8,
    stored_waker: ?FutureWaker = null,

    pub fn poll(self: *OwnedWriteFuture, ctx: *FutureContext) FuturePollResult(Output) {
        if (self.half.tryWrite(self.data)) |n_opt| {
            if (n_opt) |n| {
                return .{ .ready = n };
            }
        } else |err| {
            return .{ .ready = err };
        }

        // WouldBlock - register for notification
        updateStoredWaker(&self.stored_waker, ctx);
        if (self.half.inner.scheduled_io) |sio| {
            sio.setWriterWaker(bridgeWaker(&self.stored_waker));
        }
        return .pending;
    }

    pub fn deinit(self: *OwnedWriteFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for write all on OwnedWriteHalf.
pub const OwnedWriteAllFuture = struct {
    pub const Output = anyerror!void;

    half: *OwnedWriteHalf,
    data: []const u8,
    written: usize,
    stored_waker: ?FutureWaker = null,

    pub fn init(half: *OwnedWriteHalf, data: []const u8) OwnedWriteAllFuture {
        return .{ .half = half, .data = data, .written = 0 };
    }

    pub fn poll(self: *OwnedWriteAllFuture, ctx: *FutureContext) FuturePollResult(Output) {
        while (self.written < self.data.len) {
            if (self.half.tryWrite(self.data[self.written..])) |n_opt| {
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
            if (self.half.inner.scheduled_io) |sio| {
                sio.setWriterWaker(bridgeWaker(&self.stored_waker));
            }
            return .pending;
        }
        return .{ .ready = {} };
    }

    pub fn deinit(self: *OwnedWriteAllFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for readable readiness on owned half.
pub const OwnedReadableFuture = struct {
    pub const Output = void;

    fd: posix.socket_t,
    scheduled_io: ?*ScheduledIo,
    stored_waker: ?FutureWaker = null,

    pub fn poll(self: *OwnedReadableFuture, ctx: *FutureContext) FuturePollResult(Output) {
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

    pub fn deinit(self: *OwnedReadableFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for writable readiness on owned half.
pub const OwnedWritableFuture = struct {
    pub const Output = void;

    fd: posix.socket_t,
    scheduled_io: ?*ScheduledIo,
    stored_waker: ?FutureWaker = null,

    pub fn poll(self: *OwnedWritableFuture, ctx: *FutureContext) FuturePollResult(Output) {
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

    pub fn deinit(self: *OwnedWritableFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

/// Future for generic interest-based readiness on owned half.
pub const OwnedReadyFuture = struct {
    pub const Output = Ready;

    fd: posix.socket_t,
    scheduled_io: ?*ScheduledIo,
    interest: Interest,
    stored_waker: ?FutureWaker = null,

    pub fn poll(self: *OwnedReadyFuture, ctx: *FutureContext) FuturePollResult(Output) {
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

    pub fn deinit(self: *OwnedReadyFuture) void {
        cleanupStoredWaker(&self.stored_waker);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn createConnectedPairForSplit() !struct { client: TcpStream, server: TcpStream, listener: @import("listener.zig").TcpListener } {
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

test "ReadHalf/WriteHalf - borrowed split delegates to stream" {
    var pair = try createConnectedPairForSplit();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    const halves = pair.client.split_halves();

    // Both halves should return the same peer address
    const read_peer = halves.read.peerAddr();
    const write_peer = halves.write.peerAddr();
    try testing.expectEqual(read_peer.port(), write_peer.port());
}

test "ReadHalf - tryRead returns null on empty" {
    var pair = try createConnectedPairForSplit();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    var halves = pair.server.split_halves();
    var buf: [64]u8 = undefined;
    const result = try halves.read.tryRead(&buf);
    try testing.expect(result == null);
}

test "WriteHalf - tryWrite sends data" {
    var pair = try createConnectedPairForSplit();
    defer pair.client.close();
    defer pair.server.close();
    defer pair.listener.close();

    var client_halves = pair.client.split_halves();
    _ = try client_halves.write.tryWrite("hello split");

    // Read on server
    var buf: [64]u8 = undefined;
    var n: usize = 0;
    for (0..100) |_| {
        if (try pair.server.tryRead(&buf)) |bytes| {
            n = bytes;
            break;
        }
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    try testing.expectEqualStrings("hello split", buf[0..n]);
}

test "OwnedReadHalf/OwnedWriteHalf - basic I/O" {
    var pair = try createConnectedPairForSplit();
    defer pair.server.close();
    defer pair.listener.close();

    const halves = try pair.client.intoSplit(testing.allocator);
    var read_half = halves.read;
    var write_half = halves.write;
    defer {
        write_half.deinit();
        read_half.deinit();
    }

    // Write from owned write half
    _ = try write_half.tryWrite("owned test");

    // Read on server
    var buf: [64]u8 = undefined;
    var n: usize = 0;
    for (0..100) |_| {
        if (try pair.server.tryRead(&buf)) |bytes| {
            n = bytes;
            break;
        }
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    try testing.expectEqualStrings("owned test", buf[0..n]);
}

test "OwnedReadHalf/OwnedWriteHalf - peerAddr and localAddr" {
    var pair = try createConnectedPairForSplit();
    defer pair.server.close();
    defer pair.listener.close();

    const halves = try pair.client.intoSplit(testing.allocator);
    var read_half = halves.read;
    var write_half = halves.write;
    defer {
        write_half.deinit();
        read_half.deinit();
    }

    try testing.expectEqual(read_half.peerAddr().port(), write_half.peerAddr().port());

    const raddr = try read_half.localAddr();
    const waddr = try write_half.localAddr();
    try testing.expectEqual(raddr.port(), waddr.port());
}

test "OwnedWriteHalf - forget disables shutdown on drop" {
    var pair = try createConnectedPairForSplit();
    defer pair.server.close();
    defer pair.listener.close();

    const halves = try pair.client.intoSplit(testing.allocator);
    var read_half = halves.read;
    var write_half = halves.write;

    // forget() should prevent shutdown on deinit
    write_half.forget();
    try testing.expect(!write_half.shutdown_on_drop);

    write_half.deinit();
    read_half.deinit();
}

test "OwnedHalves - reunite reconstructs TcpStream" {
    var pair = try createConnectedPairForSplit();
    defer pair.server.close();
    defer pair.listener.close();

    const halves = try pair.client.intoSplit(testing.allocator);
    const read_half = halves.read;
    const write_half = halves.write;

    // Reunite back into a TcpStream
    var stream = try read_half.reunite(write_half);
    defer stream.close();

    // Stream should be functional
    try stream.setNoDelay(true);
    try testing.expect(try stream.getNoDelay());
}

test "OwnedHalves - reunite via write half" {
    var pair = try createConnectedPairForSplit();
    defer pair.server.close();
    defer pair.listener.close();

    const halves = try pair.client.intoSplit(testing.allocator);
    const read_half = halves.read;
    const write_half = halves.write;

    // Can also reunite via write_half.reunite(read_half)
    var stream = try write_half.reunite(read_half);
    defer stream.close();

    try testing.expect(c.isValidSocket(stream.fileno()));
}

test "OwnedHalves - vectored write" {
    var pair = try createConnectedPairForSplit();
    defer pair.server.close();
    defer pair.listener.close();

    const halves = try pair.client.intoSplit(testing.allocator);
    var read_half = halves.read;
    var write_half = halves.write;
    defer {
        write_half.deinit();
        read_half.deinit();
    }

    const part1 = "AB";
    const part2 = "CD";
    const iovecs = [_]posix.iovec_const{
        .{ .base = part1.ptr, .len = part1.len },
        .{ .base = part2.ptr, .len = part2.len },
    };

    var written: usize = 0;
    for (0..50) |_| {
        if (try write_half.tryWriteVectored(&iovecs)) |n| {
            written = n;
            break;
        }
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }
    try testing.expectEqual(@as(usize, 4), written);
}

test "OwnedReadHalf - tryRead returns null on empty" {
    var pair = try createConnectedPairForSplit();
    defer pair.client.close();
    defer pair.listener.close();

    const halves = try pair.server.intoSplit(testing.allocator);
    var read_half = halves.read;
    var write_half = halves.write;
    defer {
        write_half.deinit();
        read_half.deinit();
    }

    var buf: [64]u8 = undefined;
    const result = try read_half.tryRead(&buf);
    try testing.expect(result == null);
}
