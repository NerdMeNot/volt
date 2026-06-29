//! SIGINT graceful-shutdown plumbing for Runtime.runWithSignals.
//! Extracted from runtime.zig: the POSIX self-pipe, the async-
//! signal-safe handler, and the run-with-cancel driver. Runtime
//! keeps a thin `runWithSignals` method that delegates here. Tests
//! remain in runtime.zig. The public POSIX signal API is
//! src/signal.zig — a separate concern, not merged here.

const std = @import("std");
const builtin = @import("builtin");
const runtime = @import("runtime.zig");
const Runtime = runtime.Runtime;
const reactor_mod = @import("reactor.zig");
const signal_mod = @import("signal.zig");
const current = @import("current.zig");

const is_windows_rt = builtin.os.tag == .windows;
const posix_pipe = if (is_windows_rt) {} else @extern(
    *const fn (fds: *[2]c_int) callconv(.c) c_int,
    .{ .name = "pipe" },
);
const posix_close = if (is_windows_rt) {} else @extern(
    *const fn (fd: c_int) callconv(.c) c_int,
    .{ .name = "close" },
);
const posix_write = if (is_windows_rt) {} else @extern(
    *const fn (fd: c_int, buf: [*]const u8, count: usize) callconv(.c) isize,
    .{ .name = "write" },
);

var sigint_pipe_write_fd: std.atomic.Value(c_int) = std.atomic.Value(c_int).init(-1);

fn sigintHandler(sig: c_int, info: *anyopaque, ctx: ?*anyopaque) callconv(.c) void {
    _ = sig;
    _ = info;
    _ = ctx;
    const fd = sigint_pipe_write_fd.load(.acquire);
    if (fd < 0) return;
    const byte: u8 = 1;
    _ = posix_write(fd, @ptrCast(&byte), 1);
}

pub fn runWithSignals(
    self: *Runtime,
    comptime body: anytype,
    args: anytype,
) !@typeInfo(@typeInfo(@TypeOf(body)).@"fn".return_type.?).error_union.payload {
    if (comptime is_windows_rt) {
        return runWithSignalsNoOpWindows(self, body, args);
    }

    var pipe_fds: [2]c_int = undefined;
    if (posix_pipe(&pipe_fds) != 0) return error.PipeCreateFailed;
    defer _ = posix_close(pipe_fds[0]);
    defer _ = posix_close(pipe_fds[1]);

    // Non-blocking on both ends. The handler's `write` is in a
    // signal context and must not block; the poller drives reads
    // through the reactor's wait-readable path.
    reactor_mod.setNonblock(pipe_fds[0]) catch {};
    reactor_mod.setNonblock(pipe_fds[1]) catch {};

    sigint_pipe_write_fd.store(pipe_fds[1], .release);
    defer sigint_pipe_write_fd.store(-1, .release);

    const SIGINT: c_int = 2;
    var old_action: signal_mod.Sigaction = undefined;
    var new_action: signal_mod.Sigaction = std.mem.zeroes(signal_mod.Sigaction);
    new_action.sa_sigaction = @ptrCast(&sigintHandler);
    new_action.sa_flags = signal_mod.SA_SIGINFO;
    _ = signal_mod.sigaction_fn(SIGINT, &new_action, &old_action);
    defer _ = signal_mod.sigaction_fn(SIGINT, &old_action, null);

    const ArgsT = @TypeOf(args);
    const Internal = struct {
        fn run(read_fd: c_int, user_args: ArgsT) !@typeInfo(@typeInfo(@TypeOf(body)).@"fn".return_type.?).error_union.payload {
            var c = @import("cancel.zig").Cancel.init(@ptrCast(@alignCast(current.require().runtime)));
            defer c.deinit();

            const Poller = struct {
                fn run(cn: *@import("cancel.zig").Cancel, fd: c_int) void {
                    const rt: *Runtime = @ptrCast(@alignCast(current.require().runtime));
                    // Lazy-init a PollDesc for the self-pipe.
                    // pd_handle.release tears it down before the
                    // outer scope libc-closes the pipe fds.
                    var pd_slot: @import("pd_handle.zig").Atomic = .{};
                    defer @import("pd_handle.zig").release(&pd_slot, fd);
                    const pd = @import("pd_handle.zig").ensure(&pd_slot, rt, fd) catch return;
                    defer pd.decref();
                    rt.reactor.waitFdCancel(fd, pd, .read, cn) catch return;
                    cn.fire();
                }
            };
            var poller = try (@import("lib.zig").spawn(Poller.run, .{ &c, read_fd }));

            const body_args = .{&c} ++ user_args;
            const result = @call(.auto, body, body_args);

            // Body done — fire cancel (idempotent if already
            // fired by the signal handler) so the poller wakes
            // and exits.
            c.fire();
            _ = poller.join();
            return result;
        }
    };

    return try self.run(Internal.run, .{ pipe_fds[0], args });
}

/// Windows fallback for `runWithSignals` — provides the Cancel
/// to the body but doesn't install any signal handler. The body
/// must fire the cancel itself (e.g. via custom Win32 console
/// control handler) for graceful shutdown.
fn runWithSignalsNoOpWindows(
    self: *Runtime,
    comptime body: anytype,
    args: anytype,
) !@typeInfo(@typeInfo(@TypeOf(body)).@"fn".return_type.?).error_union.payload {
    const ArgsT = @TypeOf(args);
    const Internal = struct {
        fn run(user_args: ArgsT) !@typeInfo(@typeInfo(@TypeOf(body)).@"fn".return_type.?).error_union.payload {
            var c = @import("cancel.zig").Cancel.init(@ptrCast(@alignCast(current.require().runtime)));
            defer c.deinit();
            const body_args = .{&c} ++ user_args;
            return @call(.auto, body, body_args);
        }
    };
    return try self.run(Internal.run, .{args});
}
