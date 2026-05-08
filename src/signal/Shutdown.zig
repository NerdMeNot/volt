//! `volt.signal` — async signal handling for graceful shutdown.
//!
//! Wraps the cross-platform `internal.util.signal.SignalHandler`
//! (signalfd on Linux, EVFILT_SIGNAL on Darwin/BSD, ConsoleCtrlHandler
//! on Windows) and integrates it with the reactor: the SignalHandler's
//! fd is registered as a readable wait, the calling coroutine parks,
//! and the reactor wakes us when a signal arrives.
//!
//! ## Usage
//!
//! ```zig
//! var shutdown = try volt.signal.shutdown();
//! defer shutdown.deinit();
//!
//! // ... server code ...
//!
//! try shutdown.wait();    // suspends until SIGINT or SIGTERM
//! // graceful drain here
//! ```

const std = @import("std");
const builtin = @import("builtin");
const sig_util = @import("../internal/util/signal.zig");
const Park = @import("../scheduler/park.zig").Park;
const runtime_mod = @import("../runtime.zig");
const current = @import("../scheduler/current.zig");

pub const Signal = sig_util.Signal;
pub const SignalSet = sig_util.SignalSet;

/// A handle that fires when any of its registered signals arrives.
/// Multi-fire — call `wait()` repeatedly to wait for successive signals.
pub const SignalListener = struct {
    handler: sig_util.SignalHandler,

    pub fn init(signals: SignalSet) !SignalListener {
        return .{ .handler = try sig_util.SignalHandler.init(signals) };
    }

    pub fn deinit(self: *SignalListener) void {
        self.handler.deinit();
    }

    /// Suspend the calling coroutine until one of the registered
    /// signals arrives. Returns the SignalSet that fired.
    pub fn wait(self: *SignalListener) !SignalSet {
        if (builtin.os.tag == .windows) {
            @compileError("volt.signal on Windows ships at v0.7.x with IOCP integration");
        }
        const rt = runtime_mod.currentRuntime() orelse
            @panic("volt.signal.wait called outside a runtime");
        const coro = current.currentCoroutine() orelse
            @panic("volt.signal.wait called outside a coroutine");

        if (coro.isCancelled()) return error.Cancelled;

        var park: Park = .{};
        const fd = self.handler.getFd();
        try rt.reactor.registerWait(fd, .readable, @ptrCast(&park));
        park.parkCurrent() catch |err| switch (err) {
            error.Cancelled => {
                // Same kqueue-leak story as `volt.io.wait*`: cancelled
                // before signal arrives → kevent stays armed and
                // pending counter inflated, wedging idle workers.
                rt.reactor.unregisterWait(fd, .readable);
                return error.Cancelled;
            },
        };

        return try self.handler.read();
    }
};

/// Convenience: a SignalListener watching SIGINT + SIGTERM.
/// `wait()` returns when either is received.
pub fn shutdown() !SignalListener {
    var set = SignalSet.empty();
    set.add(.SIGINT);
    set.add(.SIGTERM);
    return try SignalListener.init(set);
}

/// Convenience: a SignalListener watching SIGINT (Ctrl+C) only.
pub fn ctrlC() !SignalListener {
    var set = SignalSet.empty();
    set.add(.SIGINT);
    return try SignalListener.init(set);
}
