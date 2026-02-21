//! Child Process Handle
//!
//! Represents a running child process with access to its I/O streams
//! and methods to wait for completion.
//!
//! ## Usage
//!
//! ```zig
//! const process = @import("volt").process;
//!
//! var child = try process.Command.new("sleep").arg("5").spawn();
//!
//! // Non-blocking check
//! if (child.tryWait()) |status| {
//!     std.debug.print("Exited with: {}\n", .{status.code()});
//! } else {
//!     std.debug.print("Still running...\n", .{});
//! }
//!
//! // Blocking wait
//! const status = try child.waitBlocking();
//! ```
//!

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Allocator = std.mem.Allocator;
const windows = if (builtin.os.tag == .windows) std.os.windows else undefined;

const LinkedList = @import("../internal/util/linked_list.zig").LinkedList;
const Pointers = @import("../internal/util/linked_list.zig").Pointers;

// Cross-platform file handle type
const FileHandle = if (builtin.os.tag == .windows)
    windows.HANDLE
else
    posix.fd_t;

// ─────────────────────────────────────────────────────────────────────────────
// Exit Status
// ─────────────────────────────────────────────────────────────────────────────

/// The exit status of a child process.
pub const ExitStatus = struct {
    /// Raw wait status from the OS
    raw: u32,

    const Self = @This();

    /// Create from raw wait status.
    pub fn fromRaw(raw: u32) Self {
        return .{ .raw = raw };
    }

    /// Create from std.process.Child.Term
    pub fn fromTerm(term: std.process.Child.Term) Self {
        return switch (term) {
            .Exited => |exit_code| .{ .raw = @as(u32, exit_code) << 8 },
            .Signal => |sig| .{ .raw = sig },
            .Stopped => |sig| .{ .raw = 0x7f | (sig << 8) },
            .Unknown => |val| .{ .raw = val },
        };
    }

    /// Check if the process exited successfully (exit code 0).
    pub fn isSuccess(self: Self) bool {
        return self.code() == 0;
    }

    /// Get the exit code if the process exited normally.
    pub fn code(self: Self) ?u8 {
        if (builtin.os.tag == .windows) {
            return @truncate(self.raw);
        } else {
            // POSIX: check WIFEXITED
            if (self.raw & 0x7f == 0) {
                // Normal exit, code is in upper byte
                return @truncate((self.raw >> 8) & 0xff);
            }
            return null;
        }
    }

    /// Get the signal that terminated the process (Unix only).
    pub fn signal(self: Self) ?u8 {
        if (builtin.os.tag == .windows) {
            return null;
        } else {
            const sig = self.raw & 0x7f;
            if (sig != 0 and sig != 0x7f) {
                return @truncate(sig);
            }
            return null;
        }
    }

    /// Check if the process was terminated by a signal.
    pub fn wasSignaled(self: Self) bool {
        return self.signal() != null;
    }

    /// Check if the process produced a core dump.
    pub fn coreDumped(self: Self) bool {
        if (builtin.os.tag == .windows) {
            return false;
        } else {
            return (self.raw & 0x80) != 0;
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Output
// ─────────────────────────────────────────────────────────────────────────────

/// Collected output from a child process.
pub const Output = struct {
    /// Exit status
    status: ExitStatus,

    /// Captured stdout
    stdout: []u8,

    /// Captured stderr
    stderr: []u8,

    /// The allocator used (for freeing)
    allocator: Allocator,

    /// Free the output buffers.
    pub fn deinit(self: *Output) void {
        self.allocator.free(self.stdout);
        self.allocator.free(self.stderr);
    }

    /// Check if the process succeeded.
    pub fn isSuccess(self: Output) bool {
        return self.status.isSuccess();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Waiter
// ─────────────────────────────────────────────────────────────────────────────

/// Function pointer type for waking
pub const WakerFn = *const fn (*anyopaque) void;

/// Waiter for process completion.
pub const WaitWaiter = struct {
    waker: ?WakerFn = null,
    waker_ctx: ?*anyopaque = null,
    complete: bool = false,
    status: ?ExitStatus = null,
    pointers: Pointers(WaitWaiter) = .{},

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

    pub fn getStatus(self: *const Self) ?ExitStatus {
        return self.status;
    }

    pub fn reset(self: *Self) void {
        self.complete = false;
        self.status = null;
        self.waker = null;
        self.waker_ctx = null;
        self.pointers.reset();
    }
};

const WaitWaiterList = LinkedList(WaitWaiter, "pointers");

// ─────────────────────────────────────────────────────────────────────────────
// Futures
// ─────────────────────────────────────────────────────────────────────────────

/// Future for waiting on process completion.
pub const WaitFuture = struct {
    child: *Child,

    pub fn poll(self: *WaitFuture, waiter: *WaitWaiter) bool {
        return self.child.wait(waiter);
    }
};

/// Future for collecting process output.
pub const OutputFuture = struct {
    child: *Child,
    allocator: Allocator,

    pub fn poll(self: *OutputFuture, waiter: *WaitWaiter) bool {
        _ = waiter;
        _ = self;
        // Would need async read implementation
        return false;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Piped I/O
// ─────────────────────────────────────────────────────────────────────────────

/// Write handle for child's stdin.
pub const ChildStdin = struct {
    file: std.fs.File,
    closed: bool = false,

    const Self = @This();

    pub fn init(file_handle: FileHandle) Self {
        return .{ .file = .{ .handle = file_handle } };
    }

    pub fn initFromFile(file: std.fs.File) Self {
        return .{ .file = file };
    }

    /// Write data to stdin.
    pub fn write(self: *Self, data: []const u8) !usize {
        if (self.closed) return error.Closed;
        return self.file.write(data);
    }

    /// Write all data to stdin.
    pub fn writeAll(self: *Self, data: []const u8) !void {
        if (self.closed) return error.Closed;
        try self.file.writeAll(data);
    }

    /// Close the stdin pipe.
    pub fn close(self: *Self) void {
        if (!self.closed) {
            self.file.close();
            self.closed = true;
        }
    }

    /// Get the file descriptor (Unix) or HANDLE (Windows).
    pub fn handle(self: Self) FileHandle {
        return self.file.handle;
    }
};

/// Read handle for child's stdout.
pub const ChildStdout = struct {
    file: std.fs.File,
    closed: bool = false,

    const Self = @This();

    pub fn init(handle_val: FileHandle) Self {
        return .{ .file = .{ .handle = handle_val } };
    }

    pub fn initFromFile(file: std.fs.File) Self {
        return .{ .file = file };
    }

    /// Read from stdout.
    pub fn read(self: *Self, buf: []u8) !usize {
        if (self.closed) return error.Closed;
        return self.file.read(buf);
    }

    /// Read all available data from stdout.
    pub fn readAll(self: *Self, allocator: Allocator) ![]u8 {
        if (self.closed) return error.Closed;

        var result: std.ArrayListUnmanaged(u8) = .{};
        errdefer result.deinit(allocator);

        var buf: [4096]u8 = undefined;
        while (true) {
            const n = self.file.read(&buf) catch |err| switch (err) {
                error.WouldBlock => break,
                else => return err,
            };
            if (n == 0) break;
            try result.appendSlice(allocator, buf[0..n]);
        }

        return try result.toOwnedSlice(allocator);
    }

    /// Close the stdout pipe.
    pub fn close(self: *Self) void {
        if (!self.closed) {
            self.file.close();
            self.closed = true;
        }
    }

    /// Get the file descriptor (Unix) or HANDLE (Windows).
    pub fn handle(self: Self) FileHandle {
        return self.file.handle;
    }
};

/// Read handle for child's stderr.
pub const ChildStderr = struct {
    file: std.fs.File,
    closed: bool = false,

    const Self = @This();

    pub fn init(handle_val: FileHandle) Self {
        return .{ .file = .{ .handle = handle_val } };
    }

    pub fn initFromFile(file: std.fs.File) Self {
        return .{ .file = file };
    }

    /// Read from stderr.
    pub fn read(self: *Self, buf: []u8) !usize {
        if (self.closed) return error.Closed;
        return self.file.read(buf);
    }

    /// Read all available data from stderr.
    pub fn readAll(self: *Self, allocator: Allocator) ![]u8 {
        if (self.closed) return error.Closed;

        var result: std.ArrayListUnmanaged(u8) = .{};
        errdefer result.deinit(allocator);

        var buf: [4096]u8 = undefined;
        while (true) {
            const n = self.file.read(&buf) catch |err| switch (err) {
                error.WouldBlock => break,
                else => return err,
            };
            if (n == 0) break;
            try result.appendSlice(allocator, buf[0..n]);
        }

        return try result.toOwnedSlice(allocator);
    }

    /// Close the stderr pipe.
    pub fn close(self: *Self) void {
        if (!self.closed) {
            self.file.close();
            self.closed = true;
        }
    }

    /// Get the file descriptor (Unix) or HANDLE (Windows).
    pub fn handle(self: Self) FileHandle {
        return self.file.handle;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Child
// ─────────────────────────────────────────────────────────────────────────────

/// A running child process.
pub const Child = struct {
    /// Underlying std process child
    std_child: ?std.process.Child = null,

    /// Process ID (for direct PID access)
    pid: ?posix.pid_t = null,

    /// Optional stdin pipe (if configured with .pipe)
    stdin: ?ChildStdin = null,

    /// Optional stdout pipe (if configured with .pipe)
    stdout: ?ChildStdout = null,

    /// Optional stderr pipe (if configured with .pipe)
    stderr: ?ChildStderr = null,

    /// Cached exit status (after wait completes)
    exit_status: ?ExitStatus = null,

    /// Waiters list
    waiters: WaitWaiterList = .{},

    /// Mutex protecting state
    mutex: std.Thread.Mutex = .{},

    const Self = @This();

    /// Create a new child process handle from raw components.
    pub fn init(
        pid: posix.pid_t,
        stdin_opt: ?ChildStdin,
        stdout_opt: ?ChildStdout,
        stderr_opt: ?ChildStderr,
    ) Self {
        return .{
            .pid = pid,
            .stdin = stdin_opt,
            .stdout = stdout_opt,
            .stderr = stderr_opt,
        };
    }

    /// Create from std.process.Child
    pub fn initFromStd(child: std.process.Child) Self {
        var result = Self{
            .std_child = child,
            .pid = child.id,
        };

        // Wrap the file handles
        if (child.stdin) |stdin_file| {
            result.stdin = ChildStdin.initFromFile(stdin_file);
        }
        if (child.stdout) |stdout_file| {
            result.stdout = ChildStdout.initFromFile(stdout_file);
        }
        if (child.stderr) |stderr_file| {
            result.stderr = ChildStderr.initFromFile(stderr_file);
        }

        return result;
    }

    /// Get the process ID.
    pub fn id(self: *const Self) ?posix.pid_t {
        return self.pid;
    }

    /// Check if the process has exited (non-blocking).
    /// Returns the exit status if complete, null if still running.
    pub fn tryWait(self: *Self) ?ExitStatus {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check cached status
        if (self.exit_status) |status| {
            return status;
        }

        if (self.pid) |p| {
            return self.waitpidNohang(p);
        }

        return null;
    }

    /// Wait for the process to complete (async pattern).
    /// Returns true if already complete (immediate return).
    /// Returns false if waiter was added (task should yield).
    pub fn wait(self: *Self, waiter: *WaitWaiter) bool {
        self.mutex.lock();

        // Check cached status
        if (self.exit_status) |status| {
            self.mutex.unlock();
            waiter.complete = true;
            waiter.status = status;
            return true;
        }

        if (self.pid) |p| {
            if (self.waitpidNohang(p)) |status| {
                self.mutex.unlock();
                waiter.complete = true;
                waiter.status = status;
                return true;
            }
        }

        // Still running - add waiter
        waiter.complete = false;
        waiter.status = null;
        self.waiters.pushBack(waiter);

        self.mutex.unlock();
        return false;
    }

    /// Cancel a pending wait.
    pub fn cancelWait(self: *Self, waiter: *WaitWaiter) void {
        if (waiter.isComplete()) return;

        self.mutex.lock();

        if (waiter.isComplete()) {
            self.mutex.unlock();
            return;
        }

        if (WaitWaiterList.isLinked(waiter) or self.waiters.front() == waiter) {
            self.waiters.remove(waiter);
            waiter.pointers.reset();
        }

        self.mutex.unlock();
    }

    /// Wait for the process to complete (blocking).
    pub fn waitBlocking(self: *Self) !ExitStatus {
        self.mutex.lock();

        // Check cached status
        if (self.exit_status) |status| {
            self.mutex.unlock();
            return status;
        }

        self.mutex.unlock();

        // Blocking wait using std.process.Child
        if (self.std_child) |*child| {
            const term = try child.wait();
            const status = ExitStatus.fromTerm(term);

            self.mutex.lock();
            self.exit_status = status;

            // Wake all waiters
            while (self.waiters.popFront()) |w| {
                w.complete = true;
                w.status = status;
                w.wake();
            }

            self.mutex.unlock();
            return status;
        }

        return error.NoProcess;
    }

    /// Wait and collect stdout/stderr.
    pub fn waitWithOutput(self: *Self, allocator: Allocator) !Output {
        // Read all output first
        var stdout_data: []u8 = &.{};
        var stderr_data: []u8 = &.{};

        if (self.stdout) |*out| {
            stdout_data = try out.readAll(allocator);
            out.close();
        }

        errdefer allocator.free(stdout_data);

        if (self.stderr) |*err_stream| {
            stderr_data = try err_stream.readAll(allocator);
            err_stream.close();
        }

        errdefer allocator.free(stderr_data);

        // Wait for process
        const status = try self.waitBlocking();

        return Output{
            .status = status,
            .stdout = stdout_data,
            .stderr = stderr_data,
            .allocator = allocator,
        };
    }

    /// Kill the child process (SIGKILL on Unix, TerminateProcess on Windows).
    pub fn kill(self: *Self) !void {
        if (builtin.os.tag == .windows) {
            if (self.std_child) |*child| {
                try child.kill();
            }
        } else {
            if (self.pid) |p| {
                try posix.kill(p, posix.SIG.KILL);
            }
        }
    }

    /// Terminate the child process gracefully (SIGTERM on Unix, TerminateProcess on Windows).
    pub fn terminate(self: *Self) !void {
        if (builtin.os.tag == .windows) {
            // Windows doesn't have graceful termination like SIGTERM
            // TerminateProcess is the only option
            if (self.std_child) |*child| {
                try child.kill();
            }
        } else {
            if (self.pid) |p| {
                try posix.kill(p, posix.SIG.TERM);
            }
        }
    }

    /// Send a specific signal to the child process (Unix only).
    /// On Windows, returns error.NotSupported.
    pub fn signal(self: *Self, sig: u8) !void {
        if (comptime builtin.os.tag == .windows) {
            @compileError("signal() is not available on Windows");
        }
        if (self.pid) |p| {
            try posix.kill(p, @enumFromInt(sig));
        }
    }

    /// Poll for completion and wake waiters.
    /// Call this periodically from the event loop.
    pub fn poll(self: *Self) void {
        self.mutex.lock();

        if (self.exit_status != null) {
            self.mutex.unlock();
            return;
        }

        if (self.pid) |p| {
            if (self.waitpidNohang(p)) |status| {
                // Wake all waiters
                while (self.waiters.popFront()) |w| {
                    w.complete = true;
                    w.status = status;
                    self.mutex.unlock();
                    w.wake();
                    self.mutex.lock();
                }
            }
        }

        self.mutex.unlock();
    }

    /// Non-blocking waitpid check. Returns ExitStatus if child exited, null if still running.
    /// Must be called while self.mutex is held. Caches result in self.exit_status.
    fn waitpidNohang(self: *Self, pid_val: posix.pid_t) ?ExitStatus {
        if (builtin.os.tag == .windows) {
            // Windows: not supported via waitpid
            return null;
        } else {
            const result = posix.waitpid(pid_val, posix.W.NOHANG);
            if (result.pid != 0) {
                const status = ExitStatus.fromRaw(result.status);
                self.exit_status = status;
                return status;
            }
            return null;
        }
    }

    /// Get waiter count (for debugging).
    pub fn waiterCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.waiters.count();
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "ExitStatus - success" {
    // POSIX: exit code 0 in upper byte, 0 in lower byte (normal exit)
    const status = ExitStatus.fromRaw(0);
    try std.testing.expect(status.isSuccess());
    try std.testing.expectEqual(@as(?u8, 0), status.code());
    try std.testing.expect(!status.wasSignaled());
}

test "ExitStatus - exit code" {
    // POSIX: exit code 42 in upper byte
    const status = ExitStatus.fromRaw(42 << 8);
    try std.testing.expect(!status.isSuccess());
    try std.testing.expectEqual(@as(?u8, 42), status.code());
    try std.testing.expect(!status.wasSignaled());
}

test "ExitStatus - signaled" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    // POSIX: signal 9 (SIGKILL) in lower byte
    const status = ExitStatus.fromRaw(9);
    try std.testing.expect(status.signal() != null);
    try std.testing.expectEqual(@as(?u8, 9), status.signal());
    try std.testing.expect(status.wasSignaled());
    try std.testing.expect(status.code() == null);
}

test "WaitWaiter - basic" {
    var waiter = WaitWaiter.init();
    try std.testing.expect(!waiter.isComplete());
    try std.testing.expect(waiter.getStatus() == null);

    waiter.complete = true;
    waiter.status = ExitStatus.fromRaw(0);

    try std.testing.expect(waiter.isComplete());
    try std.testing.expect(waiter.getStatus() != null);

    waiter.reset();
    try std.testing.expect(!waiter.isComplete());
}

test "ChildStdin/ChildStdout - basic operations" {
    if (builtin.os.tag == .windows) {
        // Windows: create pipe via kernel32
        var read_handle: windows.HANDLE = undefined;
        var write_handle: windows.HANDLE = undefined;
        if (windows.kernel32.CreatePipe(&read_handle, &write_handle, null, 0) == 0) {
            return error.SkipZigTest; // CreatePipe not available
        }

        var stdin_handle = ChildStdin.init(write_handle);
        var stdout_handle = ChildStdout.init(read_handle);

        try stdin_handle.writeAll("hello");
        stdin_handle.close();

        var buf: [10]u8 = undefined;
        const n = try stdout_handle.read(&buf);
        try std.testing.expectEqualStrings("hello", buf[0..n]);

        stdout_handle.close();
        return;
    }

    // Create a pipe (Unix)
    const pipe_fds = try posix.pipe();

    var stdin_handle = ChildStdin.init(pipe_fds[1]);
    var stdout_handle = ChildStdout.init(pipe_fds[0]);

    // Write to stdin
    try stdin_handle.writeAll("hello");

    // Read from stdout
    var buf: [10]u8 = undefined;
    const n = try stdout_handle.read(&buf);

    try std.testing.expectEqualStrings("hello", buf[0..n]);

    stdin_handle.close();
    stdout_handle.close();
}

test "ExitStatus - fromTerm Exited" {
    const status = ExitStatus.fromTerm(.{ .Exited = 0 });
    try std.testing.expect(status.isSuccess());
    try std.testing.expectEqual(@as(?u8, 0), status.code());

    const status2 = ExitStatus.fromTerm(.{ .Exited = 1 });
    try std.testing.expect(!status2.isSuccess());
    try std.testing.expectEqual(@as(?u8, 1), status2.code());
}

test "ExitStatus - fromTerm Signal" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const status = ExitStatus.fromTerm(.{ .Signal = 9 });
    try std.testing.expect(status.wasSignaled());
    try std.testing.expectEqual(@as(?u8, 9), status.signal());
    try std.testing.expect(status.code() == null);
}

test "ExitStatus - fromTerm Stopped" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const status = ExitStatus.fromTerm(.{ .Stopped = 19 });
    // Stopped: raw = 0x7f | (19 << 8) = 0x137f
    // signal(): raw & 0x7f = 0x7f, which is 0x7f → returns null (stopped, not signaled)
    try std.testing.expect(!status.wasSignaled());
    // code(): raw & 0x7f = 0x7f ≠ 0 → returns null
    try std.testing.expect(status.code() == null);
}

test "ExitStatus - coreDumped" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    // Core dump bit is 0x80
    const no_core = ExitStatus.fromRaw(9); // signal 9, no core
    try std.testing.expect(!no_core.coreDumped());

    const with_core = ExitStatus.fromRaw(9 | 0x80); // signal 9 + core dump
    try std.testing.expect(with_core.coreDumped());
}

test "ExitStatus - max exit code" {
    // Exit code 255 (max for u8)
    const status = ExitStatus.fromRaw(255 << 8);
    try std.testing.expectEqual(@as(?u8, 255), status.code());
    try std.testing.expect(!status.isSuccess());
}

test "WaitWaiter - waker callback" {
    var called = false;
    var waiter = WaitWaiter.init();

    const Context = struct {
        flag: *bool,
        fn wake(ctx_ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            self.flag.* = true;
        }
    };

    var ctx = Context{ .flag = &called };
    waiter.setWaker(@ptrCast(&ctx), Context.wake);

    try std.testing.expect(!called);
    waiter.wake();
    try std.testing.expect(called);
}

test "WaitWaiter - wake without waker is safe" {
    var waiter = WaitWaiter.init();
    // Should not crash when no waker is set
    waiter.wake();
    try std.testing.expect(!waiter.isComplete());
}

test "Output - struct fields" {
    comptime {
        _ = @offsetOf(Output, "status");
        _ = @offsetOf(Output, "stdout");
        _ = @offsetOf(Output, "stderr");
        _ = @offsetOf(Output, "allocator");
    }
}

test "Output - isSuccess delegates to status" {
    const output = Output{
        .status = ExitStatus.fromRaw(0),
        .stdout = &.{},
        .stderr = &.{},
        .allocator = std.testing.allocator,
    };
    try std.testing.expect(output.isSuccess());

    const failed = Output{
        .status = ExitStatus.fromRaw(1 << 8),
        .stdout = &.{},
        .stderr = &.{},
        .allocator = std.testing.allocator,
    };
    try std.testing.expect(!failed.isSuccess());
}

test "Child - default fields" {
    var child = Child{};
    try std.testing.expect(child.pid == null);
    try std.testing.expect(child.stdin == null);
    try std.testing.expect(child.stdout == null);
    try std.testing.expect(child.stderr == null);
    try std.testing.expect(child.exit_status == null);
    try std.testing.expectEqual(@as(usize, 0), child.waiterCount());
}

test "Child - init with pid" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var child = Child.init(12345, null, null, null);
    try std.testing.expectEqual(@as(?posix.pid_t, 12345), child.id());
    try std.testing.expect(child.stdin == null);
    try std.testing.expect(child.stdout == null);
    try std.testing.expect(child.stderr == null);
}

test "Child - tryWait returns null without process" {
    var child = Child{};
    try std.testing.expect(child.tryWait() == null);
}

test "Child - tryWait returns cached status" {
    var child = Child{};
    child.exit_status = ExitStatus.fromRaw(0);
    const status = child.tryWait();
    try std.testing.expect(status != null);
    try std.testing.expect(status.?.isSuccess());
}

test "Child - waitBlocking returns cached status" {
    var child = Child{};
    child.exit_status = ExitStatus.fromRaw(42 << 8);
    const status = try child.waitBlocking();
    try std.testing.expectEqual(@as(?u8, 42), status.code());
}

test "Child - waitBlocking without process returns error" {
    var child = Child{};
    try std.testing.expectError(error.NoProcess, child.waitBlocking());
}

test "ChildStderr - pipe roundtrip" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const pipe_fds = try posix.pipe();
    var write_file = std.fs.File{ .handle = pipe_fds[1] };
    var stderr_handle = ChildStderr.init(pipe_fds[0]);

    try write_file.writeAll("error msg");
    write_file.close();

    var buf: [20]u8 = undefined;
    const n = try stderr_handle.read(&buf);
    try std.testing.expectEqualStrings("error msg", buf[0..n]);

    stderr_handle.close();
}

test "ChildStdin - double close is safe" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const pipe_fds = try posix.pipe();
    posix.close(pipe_fds[0]); // close read end

    var stdin_handle = ChildStdin.init(pipe_fds[1]);
    stdin_handle.close();
    // Second close should be a no-op
    stdin_handle.close();
    try std.testing.expect(stdin_handle.closed);
}

test "ChildStdin - write after close returns error" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const pipe_fds = try posix.pipe();
    posix.close(pipe_fds[0]);

    var stdin_handle = ChildStdin.init(pipe_fds[1]);
    stdin_handle.close();

    try std.testing.expectError(error.Closed, stdin_handle.write("test"));
    try std.testing.expectError(error.Closed, stdin_handle.writeAll("test"));
}

test "ChildStdout - read after close returns error" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    const pipe_fds = try posix.pipe();
    posix.close(pipe_fds[1]);

    var stdout_handle = ChildStdout.init(pipe_fds[0]);
    stdout_handle.close();

    var buf: [10]u8 = undefined;
    try std.testing.expectError(error.Closed, stdout_handle.read(&buf));
    try std.testing.expectError(error.Closed, stdout_handle.readAll(std.testing.allocator));
}
