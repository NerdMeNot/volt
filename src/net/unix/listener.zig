//! Unix Domain Socket Listener
//!
//! Async Unix domain socket listener for accepting connections.
//!
//! ## Usage (Async - Default)
//!
//! ```zig
//! const unix = @import("volt").net.unix;
//!
//! var listener = try unix.UnixListener.bind("/tmp/my.sock");
//! defer listener.close();
//!
//! // Async accept - returns a Future
//! var accept_future = listener.accept();
//! const result = accept_future.poll(ctx);  // Returns .pending or .ready
//! ```
//!
//! ## Usage (Blocking - Explicit Opt-in)
//!
//! ```zig
//! // Blocking accept - spins until connection available
//! const conn = try listener.blockingAccept();
//! ```

const std = @import("std");
const thr = @import("../../internal/thread.zig");
const builtin = @import("builtin");
const posix = std.posix;
const syscall = @import("../../internal/syscall.zig");

const stream_mod = @import("stream.zig");
const UnixStream = stream_mod.UnixStream;
const UnixAddr = stream_mod.UnixAddr;

const LinkedList = @import("../../internal/util/linked_list.zig").LinkedList;
const Pointers = @import("../../internal/util/linked_list.zig").Pointers;

// Future types
const future_mod = @import("../../future.zig");
const Context = future_mod.Context;
const PollResult = future_mod.PollResult;
const Waker = future_mod.Waker;

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer type for waking a suspended task.
pub const WakerFn = *const fn (*anyopaque) void;

pub const AcceptWaiter = struct {
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,
    complete: bool = false,
    pointers: Pointers(AcceptWaiter) = .{},

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    pub fn setWaker(self: *Self, ctx: *anyopaque, wake_fn: WakerFn) void {
        self.waker_ctx = ctx;
        self.waker = wake_fn;
    }

    pub fn wake(self: *Self) void {
        if (self.waker) |wf| {
            if (self.waker_ctx) |ctx| {
                wf(ctx);
            }
        }
    }

    pub fn isComplete(self: *const Self) bool {
        return self.complete;
    }

    pub fn reset(self: *Self) void {
        self.complete = false;
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
    }
};

const AcceptWaiterList = LinkedList(AcceptWaiter, "pointers");

// ─────────────────────────────────────────────────────────────────────────────
// AcceptResult
// ─────────────────────────────────────────────────────────────────────────────

/// Result of accepting a connection.
///
/// Contains both the connected stream and the peer's address.
/// Use `intoStream()` if you only need the stream.
pub const AcceptResult = struct {
    stream: UnixStream,
    addr: UnixAddr,

    /// Get just the stream, discarding the peer address.
    pub fn intoStream(self: AcceptResult) UnixStream {
        return self.stream;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// AcceptFuture
// ─────────────────────────────────────────────────────────────────────────────

/// Future for async accept operations.
///
/// Polls the listener for incoming connections without blocking.
/// Use with async runtime's spawn() or poll directly.
pub const AcceptFuture = struct {
    pub const Output = AcceptResult;
    pub const Error = std.posix.AcceptError;

    listener: *UnixListener,
    stored_waker: ?Waker = null,
    waiter: AcceptWaiter = .{},
    state: State = .init,

    const State = enum {
        init,
        waiting,
        ready,
    };

    const Self = @This();

    pub fn init(listener: *UnixListener) Self {
        return .{ .listener = listener };
    }

    pub fn poll(self: *Self, ctx: *Context) PollResult(Output) {
        switch (self.state) {
            .init => {
                // Try non-blocking accept first
                if (self.listener.tryAccept()) |result| {
                    self.state = .ready;
                    return .{ .ready = result };
                } else |err| {
                    return .{ .err = err };
                }

                // Would block - set up waiter
                self.stored_waker = ctx.getWaker().clone();
                self.waiter.setWaker(@ptrCast(self), wakeCallback);

                // Register waiter with listener
                self.listener.mutex.lock();
                self.listener.waiters.pushBack(&self.waiter);
                self.listener.mutex.unlock();

                self.state = .waiting;
                return .pending;
            },
            .waiting => {
                // Check if waiter was completed
                if (self.waiter.isComplete()) {
                    self.state = .ready;
                    self.cleanupWaker();

                    // Try to get the result
                    if (self.listener.tryAccept()) |result| {
                        return .{ .ready = result };
                    } else |err| {
                        return .{ .err = err };
                    }
                }

                // Update waker if it changed (task migration)
                if (self.stored_waker) |*old_waker| {
                    const new_waker = ctx.getWaker();
                    if (!old_waker.eql(new_waker)) {
                        old_waker.deinit();
                        self.stored_waker = new_waker.clone();
                    }
                }

                return .pending;
            },
            .ready => {
                // Already complete - try to get result
                if (self.listener.tryAccept()) |result| {
                    return .{ .ready = result };
                } else |err| {
                    return .{ .err = err };
                }
            },
        }
    }

    fn wakeCallback(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.stored_waker) |*w| {
            w.wakeByRef();
        }
    }

    fn cleanupWaker(self: *Self) void {
        if (self.stored_waker) |*w| {
            w.deinit();
            self.stored_waker = null;
        }
    }

    /// Cancel the accept operation.
    pub fn cancel(self: *Self) void {
        self.listener.cancelAccept(&self.waiter);
        self.cleanupWaker();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// UnixListener
// ─────────────────────────────────────────────────────────────────────────────

/// A Unix domain socket listener.
pub const UnixListener = struct {
    fd: posix.socket_t,
    local_addr: UnixAddr,
    waiters: AcceptWaiterList = .{},
    mutex: thr.Mutex = .{},

    const Self = @This();

    /// Bind to a filesystem path.
    pub fn bind(path: []const u8) !Self {
        const addr = try UnixAddr.fromPath(path);
        return bindAddr(addr);
    }

    /// Bind to an address.
    pub fn bindAddr(addr: UnixAddr) !Self {
        const fd = try syscall.socket(posix.AF.UNIX, posix.SOCK.STREAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK, 0);
        errdefer syscall.close(fd);

        var addr_copy = addr;

        // Remove existing socket file if it exists
        if (!addr.isAbstract()) {
            syscall.unlink(addr.path()) catch {};
        }

        try syscall.bind(fd, addr_copy.sockaddr(), addr_copy.len());
        try syscall.listen(fd, 128);

        return .{
            .fd = fd,
            .local_addr = addr,
        };
    }

    /// Bind with specific backlog.
    pub fn bindWithBacklog(path: []const u8, backlog: u31) !Self {
        const addr = try UnixAddr.fromPath(path);
        const fd = try syscall.socket(posix.AF.UNIX, posix.SOCK.STREAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK, 0);
        errdefer syscall.close(fd);

        var addr_copy = addr;

        if (!addr.isAbstract()) {
            syscall.unlink(addr.path()) catch {};
        }

        try syscall.bind(fd, addr_copy.sockaddr(), addr_copy.len());
        try syscall.listen(fd, backlog);

        return .{
            .fd = fd,
            .local_addr = addr,
        };
    }

    /// Get the local address.
    pub fn localAddr(self: *const Self) UnixAddr {
        return self.local_addr;
    }

    /// Get the file descriptor.
    pub fn fileno(self: Self) posix.socket_t {
        return self.fd;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Accept
    // ─────────────────────────────────────────────────────────────────────────

    /// Try to accept a connection without blocking.
    pub fn tryAccept(self: *Self) !?AcceptResult {
        var peer_addr: posix.sockaddr.un = undefined;
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.un);

        const client_fd = syscall.accept(self.fd, @ptrCast(&peer_addr), &addr_len, posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK) catch |err| switch (err) {
            error.WouldBlock => return null,
            else => return err,
        };

        return .{
            .stream = UnixStream.fromFd(client_fd),
            .addr = .{ .inner = peer_addr },
        };
    }

    /// Return a Future for async accept.
    pub fn accept(self: *Self) AcceptFuture {
        return AcceptFuture.init(self);
    }

    /// Async accept with waiter pattern.
    /// Returns true if connection available immediately.
    /// Returns false if waiter was added (task should yield).
    pub fn waitAccept(self: *Self, waiter: *AcceptWaiter) !bool {
        if (try self.tryAccept()) |_| {
            waiter.complete = true;
            return true;
        }

        self.mutex.lock();
        waiter.complete = false;
        self.waiters.pushBack(waiter);
        self.mutex.unlock();

        return false;
    }

    /// Cancel a pending accept wait.
    pub fn cancelAccept(self: *Self, waiter: *AcceptWaiter) void {
        if (waiter.isComplete()) return;

        self.mutex.lock();
        defer self.mutex.unlock();

        if (AcceptWaiterList.isLinked(waiter) or self.waiters.front() == waiter) {
            self.waiters.remove(waiter);
            waiter.pointers.reset();
        }
    }

    /// Close the listener.
    pub fn close(self: *Self) void {
        syscall.close(self.fd);

        // Clean up socket file
        if (!self.local_addr.isAbstract()) {
            syscall.unlink(self.local_addr.path()) catch {};
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "UnixListener - bind and close" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const path = "/tmp/blitz-io-test-listener.sock";

    var listener = UnixListener.bind(path) catch |err| {
        if (err == error.AddressFamilyNotSupported) return error.SkipZigTest;
        return err;
    };
    defer listener.close();

    try std.testing.expectEqualStrings(path, listener.localAddr().path());
}

test "UnixListener - tryAccept returns null when no connection" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const path = "/tmp/blitz-io-test-listener2.sock";

    var listener = UnixListener.bind(path) catch |err| {
        if (err == error.AddressFamilyNotSupported) return error.SkipZigTest;
        return err;
    };
    defer listener.close();

    const result = try listener.tryAccept();
    try std.testing.expect(result == null);
}

test "UnixListener - accept connection" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const path = "/tmp/blitz-io-test-listener3.sock";

    var listener = UnixListener.bind(path) catch |err| {
        if (err == error.AddressFamilyNotSupported) return error.SkipZigTest;
        return err;
    };
    defer listener.close();

    // Connect from a thread
    const thread = try std.Thread.spawn(.{}, struct {
        fn run(p: []const u8) void {
            var client = UnixStream.connect(p) catch return;
            defer client.close();
            client.writeAll("hello") catch {};
        }
    }.run, .{path});

    // Accept
    thr.sleep(10_000_000); // 10ms
    if (try listener.tryAccept()) |res| {
        var stream = res.stream;
        defer stream.close();

        var buf: [10]u8 = undefined;
        const n = stream.read(&buf) catch 0;
        if (n > 0) {
            try std.testing.expectEqualStrings("hello", buf[0..n]);
        }
    }

    thread.join();
}
