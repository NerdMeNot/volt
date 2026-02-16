//! TCP Networking - Production-Quality Implementation
//!
//! Provides production-grade TCP networking with:
//! - TcpSocket: Pre-connection socket configuration (buffer sizes, keepalive, etc.)
//! - TcpListener: Async server socket with ScheduledIo integration
//! - TcpStream: Async connection with readiness-based I/O
//! - Split halves: Concurrent read/write access
//!
//! ## Server Example
//!
//! ```zig
//! var listener = try TcpListener.bind(Address.fromPort(8080));
//! defer listener.close();
//!
//! while (true) {
//!     if (try listener.tryAccept()) |result| {
//!         // Handle result.stream with result.peer_addr
//!     }
//! }
//! ```
//!
//! ## Client with Options
//!
//! ```zig
//! var socket = try TcpSocket.newV4();
//! try socket.setRecvBufferSize(65536);
//! try socket.setKeepalive(.{ .time = Duration.fromSecs(60) });
//!
//! var stream = try socket.connect(addr);
//! defer stream.close();
//! ```

const std = @import("std");
const posix = std.posix;

// ═══════════════════════════════════════════════════════════════════════════════
// Sub-modules
// ═══════════════════════════════════════════════════════════════════════════════

const socket_mod = @import("tcp/socket.zig");
const listener_mod = @import("tcp/listener.zig");
const stream_mod = @import("tcp/stream.zig");
const split_mod = @import("tcp/split.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Primary Types
// ═══════════════════════════════════════════════════════════════════════════════

/// TCP socket builder. Configure options before connect/listen.
pub const TcpSocket = socket_mod.TcpSocket;

/// Fluent builder for socket configuration.
pub const SocketConfigBuilder = socket_mod.SocketConfigBuilder;

/// TCP keepalive configuration.
pub const Keepalive = socket_mod.Keepalive;

/// TCP listener for accepting incoming connections.
pub const TcpListener = listener_mod.TcpListener;

/// Result of a successful accept.
pub const AcceptResult = listener_mod.AcceptResult;

/// Future for async accept.
pub const AcceptFuture = listener_mod.AcceptFuture;

/// A connected TCP stream with async read/write.
pub const TcpStream = stream_mod.TcpStream;

/// Shutdown direction.
pub const ShutdownHow = stream_mod.ShutdownHow;

// ═══════════════════════════════════════════════════════════════════════════════
// Split Halves
// ═══════════════════════════════════════════════════════════════════════════════

/// Borrowed read half - lifetime tied to TcpStream.
pub const ReadHalf = split_mod.ReadHalf;

/// Borrowed write half - lifetime tied to TcpStream.
pub const WriteHalf = split_mod.WriteHalf;

/// Owned read half - can be moved to different tasks.
pub const OwnedReadHalf = split_mod.OwnedReadHalf;

/// Owned write half - can be moved to different tasks.
pub const OwnedWriteHalf = split_mod.OwnedWriteHalf;

// ═══════════════════════════════════════════════════════════════════════════════
// Future Types
// ═══════════════════════════════════════════════════════════════════════════════

pub const ReadFuture = stream_mod.ReadFuture;
pub const WriteFuture = stream_mod.WriteFuture;
pub const ReadableFuture = stream_mod.ReadableFuture;
pub const WritableFuture = stream_mod.WritableFuture;
pub const ReadyFuture = stream_mod.ReadyFuture;
pub const PeekFuture = stream_mod.PeekFuture;
pub const WriteAllFuture = stream_mod.WriteAllFuture;

pub const OwnedReadFuture = split_mod.OwnedReadFuture;
pub const OwnedWriteFuture = split_mod.OwnedWriteFuture;
pub const OwnedWriteAllFuture = split_mod.OwnedWriteAllFuture;
pub const OwnedReadableFuture = split_mod.OwnedReadableFuture;
pub const OwnedWritableFuture = split_mod.OwnedWritableFuture;
pub const OwnedReadyFuture = split_mod.OwnedReadyFuture;

// ═══════════════════════════════════════════════════════════════════════════════
// Re-export common for cross-module access
// ═══════════════════════════════════════════════════════════════════════════════

pub const Address = @import("tcp/common.zig").Address;
pub const Duration = @import("tcp/common.zig").Duration;

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test {
    _ = socket_mod;
    _ = listener_mod;
    _ = stream_mod;
    _ = split_mod;
}

test "TcpSocket - create and close" {
    var socket = try TcpSocket.newV4();
    socket.close();
}

test "TcpSocket - set options" {
    var socket = try TcpSocket.newV4();
    defer socket.close();

    try socket.setReuseAddr(true);
    try std.testing.expect(try socket.getReuseAddr());

    try socket.setSendBufferSize(65536);
    // Kernel may adjust the value, just check it's non-zero
    try std.testing.expect((try socket.getSendBufferSize()) > 0);

    try socket.setRecvBufferSize(65536);
    try std.testing.expect((try socket.getRecvBufferSize()) > 0);
}

test "TcpListener - bind and close" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    // Should have been assigned a port
    const port = listener.localAddr().port();
    try std.testing.expect(port > 0);
}

test "TcpListener - accept returns null when no connection" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    // No one connecting, should return null (EAGAIN)
    const conn = try listener.tryAccept();
    try std.testing.expect(conn == null);
}

test "TCP - connect and transfer" {
    // Create listener
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    const lport = listener.localAddr().port();

    // Connect from client
    var client = try TcpStream.connect(Address.loopbackV4(lport));
    defer client.close();

    // Accept on server - retry with backoff
    var server_conn: ?TcpStream = null;
    for (0..50) |_| {
        if (try listener.tryAccept()) |result| {
            server_conn = result.stream;
            break;
        }
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    var conn = server_conn orelse return error.NoConnection;
    defer conn.close();

    // Send data client -> server (use tryWrite since writeAll returns Future)
    const data = "Hello, server!";
    var written: usize = 0;
    while (written < data.len) {
        if (try client.tryWrite(data[written..])) |n| {
            written += n;
        } else {
            std.Thread.sleep(1_000_000); // 1ms
        }
    }

    // Receive on server with retry
    var buf: [64]u8 = undefined;
    var n: usize = 0;
    for (0..50) |_| {
        if (try conn.tryRead(&buf)) |bytes| {
            n = bytes;
            break;
        }
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    if (n == 0) return error.NoData;

    try std.testing.expectEqualStrings("Hello, server!", buf[0..n]);
}

test "TcpStream - socket options" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    var stream = try TcpStream.connect(Address.loopbackV4(listener.localAddr().port()));
    defer stream.close();

    // Wait for connection
    for (0..50) |_| {
        if (try listener.tryAccept()) |_| break;
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    try stream.setNoDelay(true);
    try std.testing.expect(try stream.getNoDelay());

    try stream.setNoDelay(false);
    try std.testing.expect(!try stream.getNoDelay());
}

test "TcpStream - split" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    var stream = try TcpStream.connect(Address.loopbackV4(listener.localAddr().port()));
    defer stream.close();

    // Wait for connection
    for (0..50) |_| {
        if (try listener.tryAccept()) |_| break;
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    const halves = stream.split();
    _ = halves.read.peerAddr();
    _ = halves.write.peerAddr();
}

test "TcpStream - reader/writer interface" {
    // Create listener
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    const lport = listener.localAddr().port();

    // Connect from client
    var client = try TcpStream.connect(Address.loopbackV4(lport));
    defer client.close();

    // Accept on server
    var server_conn: ?TcpStream = null;
    for (0..50) |_| {
        if (try listener.tryAccept()) |result| {
            server_conn = result.stream;
            break;
        }
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    var conn = server_conn orelse return error.NoConnection;
    defer conn.close();

    // Get reader/writer interfaces
    var w = client.writer();
    var r = conn.reader();

    // Write using the io.Writer interface
    try w.writeAll("Hello via Writer!");

    // Read using the io.Reader interface
    var buf: [64]u8 = undefined;
    var total: usize = 0;
    for (0..50) |_| {
        const rn = r.read(buf[total..]) catch |err| switch (err) {
            error.WouldBlock => {
                std.Thread.sleep(10 * std.time.ns_per_ms);
                continue;
            },
            else => return err,
        };
        if (rn == 0) break;
        total += rn;
        if (total >= 17) break; // "Hello via Writer!" is 17 chars
    }

    try std.testing.expectEqualStrings("Hello via Writer!", buf[0..total]);
}

test "TcpStream - takeError" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    var stream = try TcpStream.connect(Address.loopbackV4(listener.localAddr().port()));
    defer stream.close();

    // Accept to complete connection
    for (0..50) |_| {
        if (try listener.tryAccept()) |_| break;
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    // No error should be pending
    const err = try stream.takeError();
    try std.testing.expect(err == null);
}

test "TcpStream - TOS option" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    var stream = try TcpStream.connect(Address.loopbackV4(listener.localAddr().port()));
    defer stream.close();

    // Accept to complete connection
    for (0..50) |_| {
        if (try listener.tryAccept()) |_| break;
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    // Set TOS (low delay)
    try stream.setTos(0x10);
    try std.testing.expectEqual(@as(u8, 0x10), try stream.getTos());
}

test "TcpStream - fromStd/toStd" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    var stream = try TcpStream.connect(Address.loopbackV4(listener.localAddr().port()));

    // Accept to complete connection
    for (0..50) |_| {
        if (try listener.tryAccept()) |_| break;
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    // Convert to std
    const std_stream = stream.toStd();

    // Convert back
    var stream2 = TcpStream.fromStd(std_stream);
    defer stream2.close();

    // Should still work
    try stream2.setNoDelay(true);
}

test "OwnedWriteHalf - forget" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    var stream = try TcpStream.connect(Address.loopbackV4(listener.localAddr().port()));

    // Accept to complete connection
    for (0..50) |_| {
        if (try listener.tryAccept()) |_| break;
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    // Split into owned halves
    const halves = try stream.intoSplit(std.testing.allocator);
    var read_half = halves.read;
    var write_half = halves.write;

    // Forget to prevent shutdown on drop
    write_half.forget();

    // Cleanup
    write_half.deinit();
    read_half.deinit();
}

test "OwnedHalves - localAddr" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    var stream = try TcpStream.connect(Address.loopbackV4(listener.localAddr().port()));

    // Accept to complete connection
    for (0..50) |_| {
        if (try listener.tryAccept()) |_| break;
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    // Split into owned halves
    const halves = try stream.intoSplit(std.testing.allocator);
    var read_half = halves.read;
    var write_half = halves.write;
    defer {
        write_half.deinit();
        read_half.deinit();
    }

    // Both halves should be able to get local address
    const read_addr = try read_half.localAddr();
    const write_addr = try write_half.localAddr();
    try std.testing.expectEqual(read_addr.port(), write_addr.port());
}

test "OwnedHalves - vectored I/O" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    var client = try TcpStream.connect(Address.loopbackV4(listener.localAddr().port()));

    // Accept connection
    var server_conn: ?TcpStream = null;
    for (0..50) |_| {
        if (try listener.tryAccept()) |result| {
            server_conn = result.stream;
            break;
        }
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }
    var conn = server_conn orelse return error.NoConnection;
    defer conn.close();

    // Split client into owned halves
    const halves = try client.intoSplit(std.testing.allocator);
    var read_half = halves.read;
    var write_half = halves.write;
    defer {
        write_half.deinit();
        read_half.deinit();
    }

    // Write vectored
    const header = "HDR:";
    const body = "data";
    const iovecs = [_]posix.iovec_const{
        .{ .base = header.ptr, .len = header.len },
        .{ .base = body.ptr, .len = body.len },
    };

    // May need retries for non-blocking I/O
    var written: usize = 0;
    for (0..50) |_| {
        if (try write_half.tryWriteVectored(&iovecs)) |wn| {
            written = wn;
            break;
        }
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }
    try std.testing.expectEqual(@as(usize, 8), written);
}

test "TcpStream - tryIo custom operation" {
    var listener = try TcpListener.bind(Address.fromPort(0));
    defer listener.close();

    var stream = try TcpStream.connect(Address.loopbackV4(listener.localAddr().port()));
    defer stream.close();

    // Accept to complete connection
    var server_conn: ?TcpStream = null;
    for (0..50) |_| {
        if (try listener.tryAccept()) |result| {
            server_conn = result.stream;
            break;
        }
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }
    var conn = server_conn orelse return error.NoConnection;
    defer conn.close();

    // Try custom I/O - would block since no data
    const result = try stream.tryIo(.{ .readable = true }, struct {
        fn io(fd: posix.socket_t) !usize {
            var buf: [64]u8 = undefined;
            return posix.recv(fd, &buf, 0);
        }
    }.io);

    // Should return null (would block)
    try std.testing.expect(result == null);
}

test "TcpSocket - getKeepalive" {
    var socket = try TcpSocket.newV4();
    defer socket.close();

    // Enable keepalive to exercise the full code path
    try socket.setKeepalive(.{ .time = Duration.fromSecs(60) });

    // This should now hit the Duration.fromSecs bug
    const ka = try socket.getKeepalive();
    try std.testing.expect(ka != null);
}

test "TcpSocket - builder pattern" {
    var socket = try TcpSocket.newV4();
    defer socket.close();

    // Use fluent builder to configure multiple options
    var builder = socket.configure();
    _ = builder.reuseAddr(true);
    _ = builder.recvBufferSize(65536);
    _ = builder.sendBufferSize(65536);
    try builder.apply();

    // Verify options were set
    try std.testing.expect(try socket.getReuseAddr());
    try std.testing.expect((try socket.getRecvBufferSize()) > 0);
    try std.testing.expect((try socket.getSendBufferSize()) > 0);
}

test "TcpSocket - builder pattern with keepalive" {
    var socket = try TcpSocket.newV4();
    defer socket.close();

    // Configure with keepalive
    var builder = socket.configure();
    _ = builder.reuseAddr(true);
    _ = builder.keepalive(.{ .time = Duration.fromSecs(30) });
    try builder.apply();

    const ka = try socket.getKeepalive();
    try std.testing.expect(ka != null);
}
