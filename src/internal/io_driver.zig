//! I/O Driver - Bridges TCP/networking with the platform backend
//!
//! The IoDriver provides the integration layer between high-level I/O types
//! (TcpListener, TcpStream) and the platform-specific backend (kqueue, io_uring, etc.).
//!
//! ## Architecture
//!
//! ```
//! TcpListener/TcpStream
//!         ↓
//!     IoDriver (this module)
//!         ↓
//!     Backend (kqueue/io_uring/epoll)
//!         ↓
//!     Kernel
//! ```
//!
//! The driver handles:
//! - Submitting I/O operations to the backend
//! - Polling for completions
//! - Waking tasks when their operations complete

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;

const backend_mod = @import("backend.zig");
const Backend = backend_mod.Backend;
const Operation = backend_mod.Operation;
const Completion = backend_mod.Completion;
const SubmissionId = backend_mod.SubmissionId;

const Address = @import("../net/address.zig").Address;

/// I/O Driver that manages async operations through the backend.
pub const IoDriver = struct {
    backend: *Backend,
    completions: []Completion,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, backend: *Backend, max_completions: usize) !Self {
        const completions = try allocator.alloc(Completion, max_completions);
        return .{
            .backend = backend,
            .completions = completions,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.completions);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Operation Submission
    // ═══════════════════════════════════════════════════════════════════════════

    /// Submit an accept operation.
    /// Returns the new socket fd on success.
    pub fn submitAccept(
        self: *Self,
        listen_fd: posix.socket_t,
        addr: *posix.sockaddr,
        addr_len: *posix.socklen_t,
        user_data: u64,
    ) !SubmissionId {
        return self.backend.submit(.{
            .op = .{
                .accept = .{
                    .fd = listen_fd,
                    .addr = addr,
                    .addr_len = addr_len,
                },
            },
            .user_data = user_data,
        });
    }

    /// Submit a recv operation.
    /// Returns bytes received on success.
    pub fn submitRecv(
        self: *Self,
        fd: posix.socket_t,
        buffer: []u8,
        user_data: u64,
    ) !SubmissionId {
        return self.backend.submit(.{
            .op = .{
                .recv = .{
                    .fd = fd,
                    .buffer = buffer,
                    .flags = 0,
                },
            },
            .user_data = user_data,
        });
    }

    /// Submit a send operation.
    /// Returns bytes sent on success.
    pub fn submitSend(
        self: *Self,
        fd: posix.socket_t,
        data: []const u8,
        user_data: u64,
    ) !SubmissionId {
        return self.backend.submit(.{
            .op = .{
                .send = .{
                    .fd = fd,
                    .buffer = data,
                    .flags = 0,
                },
            },
            .user_data = user_data,
        });
    }

    /// Submit a connect operation.
    pub fn submitConnect(
        self: *Self,
        fd: posix.socket_t,
        addr: *const posix.sockaddr,
        addr_len: posix.socklen_t,
        user_data: u64,
    ) !SubmissionId {
        return self.backend.submit(.{
            .op = .{
                .connect = .{
                    .fd = fd,
                    .addr = addr,
                    .addr_len = addr_len,
                },
            },
            .user_data = user_data,
        });
    }

    /// Submit a timeout operation.
    pub fn submitTimeout(self: *Self, ns: u64, user_data: u64) !SubmissionId {
        return self.backend.submit(.{
            .op = .{ .timeout = .{ .ns = ns } },
            .user_data = user_data,
        });
    }

    /// Cancel a pending operation.
    pub fn cancel(self: *Self, id: SubmissionId) !void {
        return self.backend.cancel(id);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Completion Polling
    // ═══════════════════════════════════════════════════════════════════════════

    /// Poll for completions without blocking.
    /// Returns a slice of completions that are ready.
    pub fn poll(self: *Self) ![]Completion {
        const count = try self.backend.wait(self.completions, 0);
        return self.completions[0..count];
    }

    /// Wait for completions with a timeout.
    /// Returns a slice of completions that are ready.
    pub fn waitFor(self: *Self, timeout_ns: u64) ![]Completion {
        const count = try self.backend.wait(self.completions, timeout_ns);
        return self.completions[0..count];
    }

    /// Wait for at least one completion (blocks indefinitely).
    pub fn waitOne(self: *Self) ![]Completion {
        const count = try self.backend.wait(self.completions, null);
        return self.completions[0..count];
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Synchronous Helpers (for simple use cases)
    // ═══════════════════════════════════════════════════════════════════════════

    /// Accept a connection synchronously through the backend.
    /// This submits the operation and waits for completion.
    pub fn accept(
        self: *Self,
        listen_fd: posix.socket_t,
        addr: *posix.sockaddr,
        addr_len: *posix.socklen_t,
    ) !posix.socket_t {
        const user_data = @intFromPtr(addr); // Use addr pointer as unique ID
        _ = try self.submitAccept(listen_fd, addr, addr_len, user_data);

        // Wait for this specific completion
        while (true) {
            const completions = try self.waitOne();
            for (completions) |comp| {
                if (comp.user_data == user_data) {
                    if (comp.result < 0) {
                        return posix.unexpectedErrno(@enumFromInt(@as(u16, @intCast(-comp.result))));
                    }
                    return @intCast(comp.result);
                }
            }
        }
    }

    /// Receive data synchronously through the backend.
    pub fn recv(self: *Self, fd: posix.socket_t, buffer: []u8) !usize {
        const user_data = @intFromPtr(buffer.ptr);
        _ = try self.submitRecv(fd, buffer, user_data);

        while (true) {
            const completions = try self.waitOne();
            for (completions) |comp| {
                if (comp.user_data == user_data) {
                    if (comp.result < 0) {
                        return posix.unexpectedErrno(@enumFromInt(@as(u16, @intCast(-comp.result))));
                    }
                    return @intCast(comp.result);
                }
            }
        }
    }

    /// Send data synchronously through the backend.
    pub fn send(self: *Self, fd: posix.socket_t, data: []const u8) !usize {
        const user_data = @intFromPtr(data.ptr);
        _ = try self.submitSend(fd, data, user_data);

        while (true) {
            const completions = try self.waitOne();
            for (completions) |comp| {
                if (comp.user_data == user_data) {
                    if (comp.result < 0) {
                        return posix.unexpectedErrno(@enumFromInt(@as(u16, @intCast(-comp.result))));
                    }
                    return @intCast(comp.result);
                }
            }
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "IoDriver - init and deinit" {
    var backend = try Backend.init(std.testing.allocator, .{});
    defer backend.deinit();

    var driver = try IoDriver.init(std.testing.allocator, &backend, 64);
    defer driver.deinit();
}

test "IoDriver - init with various completion buffer sizes" {
    var backend = try Backend.init(std.testing.allocator, .{});
    defer backend.deinit();

    // Small buffer
    {
        var driver = try IoDriver.init(std.testing.allocator, &backend, 1);
        defer driver.deinit();
        try std.testing.expectEqual(@as(usize, 1), driver.completions.len);
    }

    // Medium buffer
    {
        var driver = try IoDriver.init(std.testing.allocator, &backend, 64);
        defer driver.deinit();
        try std.testing.expectEqual(@as(usize, 64), driver.completions.len);
    }

    // Large buffer
    {
        var driver = try IoDriver.init(std.testing.allocator, &backend, 1024);
        defer driver.deinit();
        try std.testing.expectEqual(@as(usize, 1024), driver.completions.len);
    }
}

test "IoDriver - poll with no pending operations" {
    var backend = try Backend.init(std.testing.allocator, .{});
    defer backend.deinit();

    var driver = try IoDriver.init(std.testing.allocator, &backend, 64);
    defer driver.deinit();

    // Poll should return empty when nothing is pending
    const completions = try driver.poll();
    try std.testing.expectEqual(@as(usize, 0), completions.len);
}

test "IoDriver - waitFor with timeout returns empty" {
    var backend = try Backend.init(std.testing.allocator, .{});
    defer backend.deinit();

    var driver = try IoDriver.init(std.testing.allocator, &backend, 64);
    defer driver.deinit();

    // waitFor with short timeout should return when nothing pending
    const completions = try driver.waitFor(1_000_000); // 1ms
    try std.testing.expectEqual(@as(usize, 0), completions.len);
}

test "IoDriver - submitTimeout and wait" {
    var backend = try Backend.init(std.testing.allocator, .{});
    defer backend.deinit();

    var driver = try IoDriver.init(std.testing.allocator, &backend, 64);
    defer driver.deinit();

    // Submit a short timeout
    const user_data: u64 = 12345;
    _ = try driver.submitTimeout(1_000_000, user_data); // 1ms

    // Wait for completion
    const start = std.time.nanoTimestamp();
    var found = false;

    while (!found) {
        const completions = try driver.waitFor(100_000_000); // 100ms max
        for (completions) |comp| {
            if (comp.user_data == user_data) {
                found = true;
                // Timeout completions typically return 0 or -ETIME
                break;
            }
        }

        // Safety: don't wait forever
        if (std.time.nanoTimestamp() - start > 1_000_000_000) break; // 1s
    }

    try std.testing.expect(found);
}

test "IoDriver - multiple timeout submissions" {
    var backend = try Backend.init(std.testing.allocator, .{});
    defer backend.deinit();

    var driver = try IoDriver.init(std.testing.allocator, &backend, 64);
    defer driver.deinit();

    // Submit multiple timeouts
    _ = try driver.submitTimeout(2_000_000, 1); // 2ms
    _ = try driver.submitTimeout(1_000_000, 2); // 1ms
    _ = try driver.submitTimeout(3_000_000, 3); // 3ms

    // Collect completions
    var completed: [3]bool = .{ false, false, false };
    var total_completed: usize = 0;
    const start = std.time.nanoTimestamp();

    while (total_completed < 3) {
        const completions = try driver.waitFor(100_000_000);
        for (completions) |comp| {
            if (comp.user_data >= 1 and comp.user_data <= 3) {
                const idx = comp.user_data - 1;
                if (!completed[idx]) {
                    completed[idx] = true;
                    total_completed += 1;
                }
            }
        }

        // Safety timeout
        if (std.time.nanoTimestamp() - start > 1_000_000_000) break;
    }

    try std.testing.expectEqual(@as(usize, 3), total_completed);
}

test "IoDriver - backend pointer is stored correctly" {
    var backend = try Backend.init(std.testing.allocator, .{});
    defer backend.deinit();

    var driver = try IoDriver.init(std.testing.allocator, &backend, 64);
    defer driver.deinit();

    try std.testing.expectEqual(&backend, driver.backend);
}

// Socket tests require actual sockets - these test the full path
test "IoDriver - TCP loopback send/recv" {
    var backend = try Backend.init(std.testing.allocator, .{});
    defer backend.deinit();

    var driver = try IoDriver.init(std.testing.allocator, &backend, 64);
    defer driver.deinit();

    // Create BLOCKING listening socket (no NONBLOCK - we need sync accept)
    const listen_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    defer posix.close(listen_fd);

    // Allow address reuse
    try posix.setsockopt(listen_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

    // Bind to ephemeral port
    var bind_addr = Address.initIpv4(.{ 127, 0, 0, 1 }, 0);
    try posix.bind(listen_fd, @ptrCast(&bind_addr.storage), bind_addr.len);
    try posix.listen(listen_fd, 1);

    // Get assigned port
    var name_len: posix.socklen_t = @sizeOf(posix.sockaddr.storage);
    try posix.getsockname(listen_fd, @ptrCast(&bind_addr.storage), &name_len);
    const port = bind_addr.port();

    // Create BLOCKING client socket and connect
    const client_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    defer posix.close(client_fd);

    var connect_addr = Address.initIpv4(.{ 127, 0, 0, 1 }, port);
    try posix.connect(client_fd, @ptrCast(&connect_addr.storage), connect_addr.len);

    // Accept the connection (blocking)
    var accept_addr: posix.sockaddr = undefined;
    var accept_addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);
    const server_fd = try posix.accept(listen_fd, &accept_addr, &accept_addr_len, 0);
    defer posix.close(server_fd);

    // NOW set both to non-blocking for async I/O testing
    _ = try posix.fcntl(client_fd, posix.F.SETFL, @as(u32, @bitCast(posix.O{ .NONBLOCK = true })));
    _ = try posix.fcntl(server_fd, posix.F.SETFL, @as(u32, @bitCast(posix.O{ .NONBLOCK = true })));

    const send_data = "Hello, IoDriver!";
    var recv_buf: [64]u8 = undefined;

    // Submit send on client
    _ = try driver.submitSend(client_fd, send_data, 100);

    // Submit recv on server
    _ = try driver.submitRecv(server_fd, &recv_buf, 200);

    // Wait for completions
    var send_done = false;
    var recv_done = false;
    var recv_len: usize = 0;
    const start = std.time.nanoTimestamp();

    while (!send_done or !recv_done) {
        const completions = try driver.waitFor(100_000_000);
        for (completions) |comp| {
            if (comp.user_data == 100) {
                send_done = true;
            } else if (comp.user_data == 200) {
                recv_done = true;
                if (comp.result > 0) {
                    recv_len = @intCast(comp.result);
                }
            }
        }

        if (std.time.nanoTimestamp() - start > 2_000_000_000) break;
    }

    try std.testing.expect(send_done);
    try std.testing.expect(recv_done);
    try std.testing.expectEqual(send_data.len, recv_len);
    try std.testing.expectEqualStrings(send_data, recv_buf[0..recv_len]);
}

test "IoDriver - TCP loopback accept/connect" {
    var backend = try Backend.init(std.testing.allocator, .{});
    defer backend.deinit();

    var driver = try IoDriver.init(std.testing.allocator, &backend, 64);
    defer driver.deinit();

    // Create non-blocking listening socket
    const listen_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, 0);
    defer posix.close(listen_fd);

    // Allow address reuse
    try posix.setsockopt(listen_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

    // Bind to ephemeral port
    var bind_addr = Address.initIpv4(.{ 127, 0, 0, 1 }, 0);
    try posix.bind(listen_fd, @ptrCast(&bind_addr.storage), bind_addr.len);
    try posix.listen(listen_fd, 1);

    // Get assigned port
    var name_len: posix.socklen_t = @sizeOf(posix.sockaddr.storage);
    try posix.getsockname(listen_fd, @ptrCast(&bind_addr.storage), &name_len);
    const port = bind_addr.port();

    // Create non-blocking client socket
    const client_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, 0);
    defer posix.close(client_fd);

    // Setup accept address storage
    var accept_addr: posix.sockaddr = undefined;
    var accept_addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);

    // For readiness-based backends (kqueue), we must initiate connect() first.
    // Non-blocking connect returns EINPROGRESS, then we wait for writability.
    var connect_addr = Address.initIpv4(.{ 127, 0, 0, 1 }, port);
    posix.connect(client_fd, @ptrCast(&connect_addr.storage), connect_addr.len) catch |err| {
        // EINPROGRESS is expected for non-blocking connect
        if (err != error.WouldBlock) return err;
    };

    // Submit async operations to wait for completion
    _ = try driver.submitConnect(client_fd, @ptrCast(&connect_addr.storage), connect_addr.len, 300);
    _ = try driver.submitAccept(listen_fd, &accept_addr, &accept_addr_len, 400);

    // Wait for both operations to complete
    var connect_done = false;
    var accept_done = false;
    var accepted_fd: posix.socket_t = -1;
    const start = std.time.nanoTimestamp();

    while (!connect_done or !accept_done) {
        const completions = try driver.waitFor(100_000_000);
        for (completions) |comp| {
            if (comp.user_data == 300) {
                connect_done = true;
            } else if (comp.user_data == 400) {
                accept_done = true;
                if (comp.result > 0) {
                    accepted_fd = @intCast(comp.result);
                }
            }
        }

        if (std.time.nanoTimestamp() - start > 2_000_000_000) break;
    }

    // Clean up accepted fd
    if (accepted_fd > 0) {
        posix.close(accepted_fd);
    }

    try std.testing.expect(connect_done);
    try std.testing.expect(accept_done);
    try std.testing.expect(accepted_fd > 0);
}

test "IoDriver - cancel pending operation" {
    var backend = try Backend.init(std.testing.allocator, .{});
    defer backend.deinit();

    var driver = try IoDriver.init(std.testing.allocator, &backend, 64);
    defer driver.deinit();

    // Submit a long timeout
    const id = try driver.submitTimeout(10_000_000_000, 999); // 10 seconds

    // Cancel it
    try driver.cancel(id);

    // Poll should not hang - either returns 0 or the cancelled operation
    const completions = try driver.poll();

    // Either we get no completions (cancel worked) or we get the cancelled one
    // Either is acceptable behavior depending on backend timing
    _ = completions;
}

test "IoDriver - user_data preserved in completions" {
    var backend = try Backend.init(std.testing.allocator, .{});
    defer backend.deinit();

    var driver = try IoDriver.init(std.testing.allocator, &backend, 64);
    defer driver.deinit();

    // Submit timeouts with distinct user_data values
    const user_data_1: u64 = 0xDEADBEEF;
    const user_data_2: u64 = 0xCAFEBABE;
    const user_data_3: u64 = 0x12345678;

    _ = try driver.submitTimeout(1_000_000, user_data_1); // 1ms
    _ = try driver.submitTimeout(2_000_000, user_data_2); // 2ms
    _ = try driver.submitTimeout(3_000_000, user_data_3); // 3ms

    // Wait for all to complete
    var seen_1 = false;
    var seen_2 = false;
    var seen_3 = false;
    const start = std.time.nanoTimestamp();

    while (!seen_1 or !seen_2 or !seen_3) {
        const completions = try driver.waitFor(100_000_000);
        for (completions) |comp| {
            if (comp.user_data == user_data_1) seen_1 = true;
            if (comp.user_data == user_data_2) seen_2 = true;
            if (comp.user_data == user_data_3) seen_3 = true;
        }
        if (std.time.nanoTimestamp() - start > 1_000_000_000) break;
    }

    try std.testing.expect(seen_1);
    try std.testing.expect(seen_2);
    try std.testing.expect(seen_3);
}

test "IoDriver - init with max completions of 1" {
    var backend = try Backend.init(std.testing.allocator, .{});
    defer backend.deinit();

    // Edge case: very small completion buffer
    var driver = try IoDriver.init(std.testing.allocator, &backend, 1);
    defer driver.deinit();

    // Submit multiple timeouts
    _ = try driver.submitTimeout(1_000_000, 1);
    _ = try driver.submitTimeout(2_000_000, 2);

    // Should be able to get completions one at a time
    var count: usize = 0;
    const start = std.time.nanoTimestamp();

    while (count < 2) {
        const completions = try driver.waitFor(100_000_000);
        count += completions.len;
        if (std.time.nanoTimestamp() - start > 1_000_000_000) break;
    }

    try std.testing.expectEqual(@as(usize, 2), count);
}
