//! Signal Handling for Async I/O
//!
//! Provides signal notification integrated with the I/O event loop.
//! Signals are tricky in async contexts because:
//! 1. Signal handlers run in interrupt context (limited operations)
//! 2. They can interrupt any code at any point
//! 3. Need to wake the event loop safely
//!
//! Strategy per platform:
//! - Linux: signalfd (receives signals as file descriptors)
//! - macOS/BSD: kqueue EVFILT_SIGNAL (native signal events)
//! - Windows: ConsoleCtrlHandler (CTRL_C_EVENT, CTRL_BREAK_EVENT, etc.)
//! - Fallback: self-pipe trick (signal handler writes to pipe)
//!
//! Common signals:
//! - SIGTERM: Graceful shutdown request (CTRL_CLOSE_EVENT on Windows)
//! - SIGINT: Ctrl+C interrupt (CTRL_C_EVENT on Windows)
//! - SIGHUP: Reload configuration
//! - SIGPIPE: Broken pipe (usually ignored)
//!

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

// Windows-specific imports
const windows = if (builtin.os.tag == .windows) std.os.windows else undefined;

/// Signal numbers we care about.
/// On Windows, these map to console control events:
/// - SIGINT -> CTRL_C_EVENT
/// - SIGTERM -> CTRL_CLOSE_EVENT
/// - SIGHUP -> CTRL_LOGOFF_EVENT (approximately)
/// - SIGQUIT -> CTRL_BREAK_EVENT
pub const Signal = enum(u6) {
    SIGHUP = 1,
    SIGINT = 2,
    SIGQUIT = 3,
    SIGTERM = 15,
    SIGUSR1 = 10,
    SIGUSR2 = 12,
    SIGPIPE = 13,
    SIGCHLD = 17,

    pub fn toInt(self: Signal) u6 {
        return @intFromEnum(self);
    }

    pub fn fromInt(sig: u6) ?Signal {
        return std.meta.intToEnum(Signal, sig) catch null;
    }
};

/// Signal set for blocking/masking
pub const SignalSet = struct {
    mask: u64 = 0,

    pub fn empty() SignalSet {
        return .{ .mask = 0 };
    }

    pub fn all() SignalSet {
        return .{ .mask = std.math.maxInt(u64) };
    }

    pub fn add(self: *SignalSet, sig: Signal) void {
        self.mask |= @as(u64, 1) << sig.toInt();
    }

    pub fn remove(self: *SignalSet, sig: Signal) void {
        self.mask &= ~(@as(u64, 1) << sig.toInt());
    }

    pub fn contains(self: SignalSet, sig: Signal) bool {
        return (self.mask & (@as(u64, 1) << sig.toInt())) != 0;
    }

    /// Convert to platform sigset_t (Unix only)
    pub fn toSigset(self: SignalSet) posix.sigset_t {
        if (builtin.os.tag == .windows) {
            @compileError("toSigset is not available on Windows");
        }
        var sigset = posix.sigemptyset();
        inline for (std.meta.fields(Signal)) |field| {
            const sig: Signal = @enumFromInt(field.value);
            if (self.contains(sig)) {
                posix.sigaddset(&sigset, sig.toInt());
            }
        }
        return sigset;
    }

    /// Convert Windows console control event to Signal (Windows only)
    pub fn fromWindowsCtrlType(ctrl_type: u32) ?Signal {
        if (builtin.os.tag != .windows) {
            @compileError("fromWindowsCtrlType is only available on Windows");
        }
        // Windows console control event types
        const CTRL_C_EVENT: u32 = 0;
        const CTRL_BREAK_EVENT: u32 = 1;
        const CTRL_CLOSE_EVENT: u32 = 2;
        const CTRL_LOGOFF_EVENT: u32 = 5;
        const CTRL_SHUTDOWN_EVENT: u32 = 6;

        return switch (ctrl_type) {
            CTRL_C_EVENT => .SIGINT,
            CTRL_BREAK_EVENT => .SIGQUIT,
            CTRL_CLOSE_EVENT, CTRL_SHUTDOWN_EVENT => .SIGTERM,
            CTRL_LOGOFF_EVENT => .SIGHUP,
            else => null,
        };
    }
};

/// Signal handler that integrates with the event loop.
pub const SignalHandler = struct {
    /// File descriptor to poll for signal readiness (Unix) or event handle (Windows)
    fd: if (builtin.os.tag == .windows) windows.HANDLE else posix.fd_t,

    /// How this handler was created
    kind: Kind,

    /// Signals being watched
    signals: SignalSet,

    /// Write fd stored per-instance for cleanup (only used by self_pipe on Unix)
    write_fd: if (builtin.os.tag == .windows) void else posix.fd_t = if (builtin.os.tag == .windows) {} else -1,

    const Kind = enum {
        signalfd, // Linux signalfd
        kqueue, // macOS/BSD (fd is kqueue)
        self_pipe, // Fallback (fd is read end of pipe)
        windows_console, // Windows ConsoleCtrlHandler
    };

    const Self = @This();

    /// Create a signal handler for the given signals.
    pub fn init(signals: SignalSet) !Self {
        if (builtin.os.tag == .windows) {
            return initWindowsConsole(signals);
        } else if (builtin.os.tag == .linux) {
            return initSignalfd(signals);
        } else if (builtin.os.tag == .macos or
            builtin.os.tag == .freebsd or
            builtin.os.tag == .openbsd or
            builtin.os.tag == .netbsd)
        {
            return initKqueue(signals);
        } else {
            return initSelfPipe(signals);
        }
    }

    /// Clean up resources.
    pub fn deinit(self: *Self) void {
        switch (self.kind) {
            .signalfd, .kqueue => posix.close(self.fd),
            .self_pipe => self.deinitSelfPipe(),
            .windows_console => self.deinitWindowsConsole(),
        }
    }

    /// Get the file descriptor to poll (Unix) or event handle (Windows).
    /// Register this with your event loop for READABLE.
    pub fn getFd(self: Self) if (builtin.os.tag == .windows) windows.HANDLE else posix.fd_t {
        return self.fd;
    }

    /// Read pending signals (call when fd/handle is readable/signaled).
    /// Returns the signals that fired.
    pub fn read(self: *Self) !SignalSet {
        return switch (self.kind) {
            .signalfd => self.readSignalfd(),
            .kqueue => self.readKqueue(),
            .self_pipe => self.readSelfPipe(),
            .windows_console => self.readWindowsConsole(),
        };
    }

    // ─────────────────────────────────────────────────────────────────────
    // Linux: signalfd
    // ─────────────────────────────────────────────────────────────────────

    fn initSignalfd(signals: SignalSet) !Self {
        if (builtin.os.tag != .linux) return error.UnsupportedPlatform;

        const sigset = signals.toSigset();

        // Block signals so they go to signalfd instead of default handler
        _ = posix.sigprocmask(.{ .BLOCK = true }, &sigset, null);

        // Create signalfd
        const fd = std.os.linux.signalfd(-1, &sigset, std.os.linux.SFD.NONBLOCK | std.os.linux.SFD.CLOEXEC);

        if (@as(isize, @bitCast(fd)) < 0) {
            return error.SignalfdFailed;
        }

        return Self{
            .fd = @intCast(fd),
            .kind = .signalfd,
            .signals = signals,
        };
    }

    fn readSignalfd(self: *Self) !SignalSet {
        if (builtin.os.tag != .linux) return error.UnsupportedPlatform;

        var result = SignalSet.empty();
        var info: std.os.linux.signalfd_siginfo = undefined;

        while (true) {
            const n = posix.read(self.fd, std.mem.asBytes(&info)) catch |err| {
                if (err == error.WouldBlock) break;
                return err;
            };

            if (n == 0) break;

            if (Signal.fromInt(@intCast(info.signo))) |sig| {
                result.add(sig);
            }
        }

        return result;
    }

    // ─────────────────────────────────────────────────────────────────────
    // macOS/BSD: kqueue EVFILT_SIGNAL
    // ─────────────────────────────────────────────────────────────────────

    fn initKqueue(signals: SignalSet) !Self {
        const kq = posix.kqueue() catch return error.KqueueFailed;
        errdefer posix.close(kq);

        // Register for each signal
        var changelist: [8]posix.Kevent = undefined;
        var change_count: usize = 0;

        inline for (std.meta.fields(Signal)) |field| {
            const sig: Signal = @enumFromInt(field.value);
            if (signals.contains(sig)) {
                if (change_count >= changelist.len) break;

                changelist[change_count] = .{
                    .ident = sig.toInt(),
                    .filter = posix.system.EVFILT.SIGNAL,
                    .flags = posix.system.EV.ADD,
                    .fflags = 0,
                    .data = 0,
                    .udata = 0,
                };
                change_count += 1;

                // Ignore default handler
                const act = posix.Sigaction{
                    .handler = .{ .handler = posix.SIG.IGN },
                    .mask = posix.sigemptyset(),
                    .flags = 0,
                };
                posix.sigaction(sig.toInt(), &act, null);
            }
        }

        if (change_count > 0) {
            _ = posix.kevent(kq, changelist[0..change_count], &.{}, null) catch |err| {
                return err;
            };
        }

        return Self{
            .fd = kq,
            .kind = .kqueue,
            .signals = signals,
        };
    }

    fn readKqueue(self: *Self) !SignalSet {
        var result = SignalSet.empty();
        var events: [8]posix.Kevent = undefined;

        const count = posix.kevent(self.fd, &.{}, &events, &posix.timespec{ .sec = 0, .nsec = 0 }) catch |err| {
            return err;
        };

        for (events[0..count]) |ev| {
            if (ev.filter == posix.system.EVFILT.SIGNAL) {
                if (Signal.fromInt(@intCast(ev.ident))) |sig| {
                    result.add(sig);
                }
            }
        }

        return result;
    }

    // ─────────────────────────────────────────────────────────────────────
    // Fallback: self-pipe trick
    // ─────────────────────────────────────────────────────────────────────

    // Global state for signal handler (must be static for signal handler access).
    // Uses atomic operations for thread safety.
    // Reference counted to properly manage lifecycle across multiple handlers.
    var self_pipe_write_fd: std.atomic.Value(i32) = std.atomic.Value(i32).init(-1);
    var self_pipe_ref_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);
    var pending_signals: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);

    fn initSelfPipe(signals: SignalSet) !Self {
        // Create pipe
        const pipe_fds = posix.pipe2(.{ .NONBLOCK = true, .CLOEXEC = true }) catch
            return error.PipeFailed;

        const read_fd = pipe_fds[0];
        const write_fd = pipe_fds[1];
        var owns_write_fd = true;

        // Ensure we clean up on any early exit
        errdefer {
            posix.close(read_fd);
            if (owns_write_fd) posix.close(write_fd);
        }

        // Atomically try to set the global write fd using compare-exchange.
        // This prevents the race where:
        // 1. Thread A: loads old_write_fd = 5
        // 2. Thread B: deinit swaps to -1 and closes fd 5
        // 3. Thread A: stores write_fd = 7, but TWO threads had the same idea
        while (true) {
            const old_write_fd = self_pipe_write_fd.load(.acquire);
            if (old_write_fd < 0) {
                // Try to be the first handler - use cmpxchg to avoid race
                const result = self_pipe_write_fd.cmpxchgWeak(
                    old_write_fd,
                    write_fd,
                    .acq_rel,
                    .acquire,
                );
                if (result == null) {
                    // Success! We installed our write_fd
                    owns_write_fd = false; // Global now owns it
                    break;
                }
                // Another thread beat us - retry the loop
            } else {
                // Another handler exists - close our duplicate write fd
                // (we'll share the existing one)
                posix.close(write_fd);
                owns_write_fd = false;
                break;
            }
        }

        // Successfully registered - now increment ref count
        _ = self_pipe_ref_count.fetchAdd(1, .acq_rel);

        // Install signal handlers
        const act = posix.Sigaction{
            .handler = .{ .handler = selfPipeSignalHandler },
            .mask = posix.sigemptyset(),
            .flags = posix.SA.RESTART,
        };

        inline for (std.meta.fields(Signal)) |field| {
            const sig: Signal = @enumFromInt(field.value);
            if (signals.contains(sig)) {
                posix.sigaction(sig.toInt(), &act, null);
            }
        }

        return Self{
            .fd = read_fd,
            .kind = .self_pipe,
            .signals = signals,
            .write_fd = write_fd,
        };
    }

    /// Clean up self-pipe resources.
    fn deinitSelfPipe(self: *Self) void {
        // Close read end
        posix.close(self.fd);

        // Decrement ref count and close write fd if last user
        const remaining = self_pipe_ref_count.fetchSub(1, .acq_rel);
        if (remaining == 1) {
            // We were the last user - close global write fd
            const write_fd = self_pipe_write_fd.swap(-1, .acq_rel);
            if (write_fd >= 0) {
                posix.close(write_fd);
            }
            // Clear pending signals
            _ = pending_signals.swap(0, .release);
        }
    }

    fn selfPipeSignalHandler(sig: c_int) callconv(.C) void {
        // Set bit for signal (async-signal-safe atomic operation)
        const mask = @as(u64, 1) << @as(u6, @intCast(sig));
        _ = pending_signals.fetchOr(mask, .monotonic);

        // Wake up event loop by writing to pipe
        // Load atomically to avoid TOCTOU race
        const write_fd = self_pipe_write_fd.load(.acquire);
        if (write_fd >= 0) {
            const byte = [_]u8{1};
            // Best-effort write to wake event loop. Errors are non-critical:
            // - EAGAIN: pipe full, event loop will wake anyway
            // - EBADF: pipe closed, shutdown in progress
            _ = posix.write(write_fd, &byte) catch {};
        }
    }

    fn readSelfPipe(self: *Self) !SignalSet {
        // Drain the pipe
        var buf: [64]u8 = undefined;
        while (true) {
            _ = posix.read(self.fd, &buf) catch |err| {
                if (err == error.WouldBlock) break;
                return err;
            };
        }

        // Get and clear pending signals
        const mask = pending_signals.swap(0, .acq_rel);

        var result = SignalSet.empty();
        inline for (std.meta.fields(Signal)) |field| {
            const sig: Signal = @enumFromInt(field.value);
            if ((mask & (@as(u64, 1) << sig.toInt())) != 0) {
                if (self.signals.contains(sig)) {
                    result.add(sig);
                }
            }
        }

        return result;
    }

    // ─────────────────────────────────────────────────────────────────────
    // Windows: ConsoleCtrlHandler
    // ─────────────────────────────────────────────────────────────────────

    // Global state for Windows console handler
    var windows_event_handle: std.atomic.Value(usize) = std.atomic.Value(usize).init(0);
    var windows_pending_signals: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);
    var windows_handler_ref_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);

    fn initWindowsConsole(signals: SignalSet) !Self {
        if (builtin.os.tag != .windows) return error.UnsupportedPlatform;

        // Create an event for signaling
        const event = windows.kernel32.CreateEventW(
            null, // default security
            1, // manual reset
            0, // initial state: non-signaled
            null, // unnamed
        ) orelse return error.WindowsEventFailed;
        errdefer _ = windows.kernel32.CloseHandle(event);

        // Try to install the console handler (once globally)
        const old_count = windows_handler_ref_count.fetchAdd(1, .acq_rel);
        if (old_count == 0) {
            // First handler - install the console ctrl handler
            windows_event_handle.store(@intFromPtr(event), .release);
            if (windows.kernel32.SetConsoleCtrlHandler(windowsCtrlHandler, 1) == 0) {
                _ = windows_handler_ref_count.fetchSub(1, .acq_rel);
                return error.WindowsHandlerFailed;
            }
        } else {
            // Handler already installed, just update event handle
            windows_event_handle.store(@intFromPtr(event), .release);
        }

        return Self{
            .fd = event,
            .kind = .windows_console,
            .signals = signals,
            .write_fd = {},
        };
    }

    fn deinitWindowsConsole(self: *Self) void {
        if (builtin.os.tag != .windows) return;

        const remaining = windows_handler_ref_count.fetchSub(1, .acq_rel);
        if (remaining == 1) {
            // Last handler - remove console ctrl handler
            _ = windows.kernel32.SetConsoleCtrlHandler(windowsCtrlHandler, 0);
            _ = windows_event_handle.swap(0, .acq_rel);
            _ = windows_pending_signals.swap(0, .release);
        }

        // Close the event handle
        _ = windows.kernel32.CloseHandle(self.fd);
    }

    fn windowsCtrlHandler(ctrl_type: windows.DWORD) callconv(windows.WINAPI) windows.BOOL {
        if (builtin.os.tag != .windows) return 0;

        // Map Windows control type to our Signal enum
        const sig = SignalSet.fromWindowsCtrlType(ctrl_type) orelse return 0;

        // Set pending signal
        const mask = @as(u64, 1) << sig.toInt();
        _ = windows_pending_signals.fetchOr(mask, .monotonic);

        // Signal the event to wake up waiters
        const event_ptr = windows_event_handle.load(.acquire);
        if (event_ptr != 0) {
            const event: windows.HANDLE = @ptrFromInt(event_ptr);
            _ = windows.kernel32.SetEvent(event);
        }

        // Return TRUE to indicate we handled it (prevents default behavior)
        return 1;
    }

    fn readWindowsConsole(self: *Self) !SignalSet {
        if (builtin.os.tag != .windows) return error.UnsupportedPlatform;

        // Reset the event
        _ = windows.kernel32.ResetEvent(self.fd);

        // Get and clear pending signals
        const mask = windows_pending_signals.swap(0, .acq_rel);

        var result = SignalSet.empty();
        inline for (std.meta.fields(Signal)) |field| {
            const sig: Signal = @enumFromInt(field.value);
            if ((mask & (@as(u64, 1) << sig.toInt())) != 0) {
                if (self.signals.contains(sig)) {
                    result.add(sig);
                }
            }
        }

        return result;
    }
};

/// Convenience: wait for shutdown signal (SIGTERM or SIGINT)
pub fn waitForShutdown() !Signal {
    var signals = SignalSet.empty();
    signals.add(.SIGTERM);
    signals.add(.SIGINT);

    var handler = try SignalHandler.init(signals);
    defer handler.deinit();

    // Simple blocking read
    while (true) {
        const fired = try handler.read();
        if (fired.contains(.SIGTERM)) return .SIGTERM;
        if (fired.contains(.SIGINT)) return .SIGINT;

        if (builtin.os.tag == .windows) {
            // Wait on the Windows event handle
            _ = windows.kernel32.WaitForSingleObject(handler.getFd(), windows.INFINITE);
        } else {
            // Poll for readiness (Unix)
            var pfd = [_]posix.pollfd{.{
                .fd = handler.getFd(),
                .events = posix.POLL.IN,
                .revents = 0,
            }};
            _ = posix.poll(&pfd, -1) catch continue;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "SignalSet - basic operations" {
    var set = SignalSet.empty();
    try std.testing.expect(!set.contains(.SIGINT));

    set.add(.SIGINT);
    try std.testing.expect(set.contains(.SIGINT));
    try std.testing.expect(!set.contains(.SIGTERM));

    set.add(.SIGTERM);
    try std.testing.expect(set.contains(.SIGINT));
    try std.testing.expect(set.contains(.SIGTERM));

    set.remove(.SIGINT);
    try std.testing.expect(!set.contains(.SIGINT));
    try std.testing.expect(set.contains(.SIGTERM));
}

test "Signal - conversion" {
    try std.testing.expectEqual(@as(u6, 2), Signal.SIGINT.toInt());
    try std.testing.expectEqual(@as(u6, 15), Signal.SIGTERM.toInt());

    try std.testing.expectEqual(Signal.SIGINT, Signal.fromInt(2).?);
    try std.testing.expect(Signal.fromInt(63) == null); // 63 is valid u6 but not a defined signal
}

test "SignalHandler - init and deinit" {
    var signals = SignalSet.empty();
    // SIGUSR1 doesn't exist on Windows, use SIGINT instead
    if (builtin.os.tag == .windows) {
        signals.add(.SIGINT);
    } else {
        signals.add(.SIGUSR1);
    }

    var handler = SignalHandler.init(signals) catch |err| {
        // May fail in restricted environments
        if (err == error.SignalfdFailed or err == error.KqueueFailed or
            err == error.WindowsEventFailed or err == error.WindowsHandlerFailed)
        {
            return;
        }
        return err;
    };
    defer handler.deinit();

    if (builtin.os.tag == .windows) {
        try std.testing.expect(handler.getFd() != std.os.windows.INVALID_HANDLE_VALUE);
    } else {
        try std.testing.expect(handler.getFd() >= 0);
    }
}

test "SignalSet - Windows ctrl type conversion" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;

    // CTRL_C_EVENT -> SIGINT
    try std.testing.expectEqual(Signal.SIGINT, SignalSet.fromWindowsCtrlType(0).?);
    // CTRL_BREAK_EVENT -> SIGQUIT
    try std.testing.expectEqual(Signal.SIGQUIT, SignalSet.fromWindowsCtrlType(1).?);
    // CTRL_CLOSE_EVENT -> SIGTERM
    try std.testing.expectEqual(Signal.SIGTERM, SignalSet.fromWindowsCtrlType(2).?);
    // Unknown event -> null
    try std.testing.expect(SignalSet.fromWindowsCtrlType(99) == null);
}
