//! `volt.process.Command` — async-friendly subprocess spawn + wait.
//!
//! v1.0 first cut: minimal builder over raw `fork + execve + waitpid +
//! pipe`, with the wait + I/O drain done on the blocking pool so the
//! calling coroutine parks instead of blocking a worker. Captures
//! stdout/stderr.
//!
//! ## Usage
//!
//! ```zig
//! var cmd = try volt.process.Command.init(allocator, "/bin/echo");
//! defer cmd.deinit();
//! try cmd.arg("hello");
//! try cmd.arg("world");
//! const result = try cmd.output();
//! defer result.deinit(allocator);
//! ```
//!
//! ## Limitations
//!
//! - Output is captured into memory (1 MiB cap per stream). Large-output
//!   streaming is a v1.x add-on (would need pipe-based async drain
//!   instead of read-to-end on the blocking thread).
//! - Custom env/cwd not yet supported. Parent's env is inherited.
//! - Async stdin not supported (would need a writeAll-to-pipe API in
//!   the same shape).
//!
//! v1.x adds: pipe-based async stdin/stdout/stderr, env/cwd builder
//! methods, sigchld-driven async wait (no blocking-pool thread per
//! child).

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const syscall = @import("../internal/syscall.zig");
const spawnBlocking = @import("../api/spawn_blocking.zig").spawnBlocking;

const MAX_OUTPUT_BYTES: usize = 1 * 1024 * 1024;

pub const Term = union(enum) {
    Exited: u8,
    Signal: u8,
    /// Stopped or unknown disposition.
    Unknown: u32,
};

pub const Output = struct {
    stdout: []u8,
    stderr: []u8,
    term: Term,

    pub fn deinit(self: *const Output, allocator: std.mem.Allocator) void {
        allocator.free(self.stdout);
        allocator.free(self.stderr);
    }
};

pub const Command = struct {
    allocator: std.mem.Allocator,
    argv: std.array_list.Managed([]const u8),

    pub fn init(allocator: std.mem.Allocator, program: []const u8) !Command {
        var c = Command{
            .allocator = allocator,
            .argv = std.array_list.Managed([]const u8).init(allocator),
        };
        try c.argv.append(program);
        return c;
    }

    pub fn deinit(self: *Command) void {
        self.argv.deinit();
    }

    pub fn arg(self: *Command, value: []const u8) !void {
        try self.argv.append(value);
    }

    pub fn output(self: *Command) !Output {
        var args = OutputArgs{ .cmd = self };
        return try spawnBlocking(outputBlocking, .{&args});
    }
};

const OutputArgs = struct { cmd: *Command };

fn outputBlocking(args: *OutputArgs) !Output {
    const cmd = args.cmd;
    const allocator = cmd.allocator;

    // Build null-terminated argv (libc execve form).
    const argv_z = try allocator.alloc(?[*:0]const u8, cmd.argv.items.len + 1);
    defer {
        // Free the dup'd C strings we own (everything except the
        // sentinel null at the end).
        for (argv_z[0..cmd.argv.items.len]) |slot| {
            if (slot) |p| allocator.free(std.mem.span(p));
        }
        allocator.free(argv_z);
    }
    for (cmd.argv.items, 0..) |a, i| {
        const z = try allocator.dupeZ(u8, a);
        argv_z[i] = z.ptr;
    }
    argv_z[cmd.argv.items.len] = null;

    // Pipes for stdout + stderr.
    const out_pipe = try syscall.pipe();
    errdefer {
        syscall.close(out_pipe[0]);
        syscall.close(out_pipe[1]);
    }
    const err_pipe = try syscall.pipe();
    errdefer {
        syscall.close(err_pipe[0]);
        syscall.close(err_pipe[1]);
    }

    const child_pid = std.c.fork();
    if (child_pid < 0) return error.ForkFailed;

    if (child_pid == 0) {
        // Child: redirect stdout / stderr, exec.
        _ = std.c.dup2(out_pipe[1], 1);
        _ = std.c.dup2(err_pipe[1], 2);
        syscall.close(out_pipe[0]);
        syscall.close(out_pipe[1]);
        syscall.close(err_pipe[0]);
        syscall.close(err_pipe[1]);

        // Inherit parent env via libc's `environ`.
        const envp: [*:null]const ?[*:0]const u8 = @ptrCast(std.c.environ);
        _ = std.c.execve(argv_z[0].?, @ptrCast(argv_z.ptr), envp);
        // execve returned → failure. Use _exit to skip atexit handlers.
        std.c._exit(127);
    }

    // Parent: close write ends; read both pipes to EOF.
    syscall.close(out_pipe[1]);
    syscall.close(err_pipe[1]);
    defer syscall.close(out_pipe[0]);
    defer syscall.close(err_pipe[0]);

    var stdout_buf: std.array_list.Managed(u8) = .init(allocator);
    errdefer stdout_buf.deinit();
    var stderr_buf: std.array_list.Managed(u8) = .init(allocator);
    errdefer stderr_buf.deinit();
    try drainFd(out_pipe[0], &stdout_buf);
    try drainFd(err_pipe[0], &stderr_buf);

    // Wait for child.
    var status: c_int = 0;
    while (true) {
        const r = std.c.waitpid(child_pid, &status, 0);
        if (r == child_pid) break;
        // EINTR; retry.
    }

    return .{
        .stdout = try stdout_buf.toOwnedSlice(),
        .stderr = try stderr_buf.toOwnedSlice(),
        .term = decodeTerm(status),
    };
}

fn drainFd(fd: i32, buf: *std.array_list.Managed(u8)) !void {
    var chunk: [64 * 1024]u8 = undefined;
    while (true) {
        const n = syscall.read(fd, &chunk) catch |err| switch (err) {
            error.WouldBlock => continue,
            else => return err,
        };
        if (n == 0) return;
        if (buf.items.len + n > MAX_OUTPUT_BYTES) return error.OutputTooLarge;
        try buf.appendSlice(chunk[0..n]);
    }
}

fn decodeTerm(status: c_int) Term {
    // POSIX W*() macros aren't directly callable from Zig; we decode
    // the status word inline. Layout (all standard POSIX):
    //   bits 0..7   = signal number (if signaled) or 0 (if exited)
    //   bits 8..15  = exit code (if exited)
    //   bit 7 of bits 0..7 = "core dumped" flag
    const lo: u32 = @bitCast(status);
    const term_sig: u8 = @intCast(lo & 0x7F);
    if (term_sig == 0) {
        return .{ .Exited = @intCast((lo >> 8) & 0xFF) };
    }
    if (term_sig != 0x7F) {
        return .{ .Signal = term_sig };
    }
    return .{ .Unknown = lo };
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

fn echoRoot() !void {
    var cmd = try Command.init(std.testing.allocator, "/bin/echo");
    defer cmd.deinit();
    try cmd.arg("volt-process");

    const result = try cmd.output();
    defer result.deinit(std.testing.allocator);

    try std.testing.expect(result.term == .Exited);
    try std.testing.expectEqual(@as(u8, 0), result.term.Exited);
    try std.testing.expectEqualStrings("volt-process\n", result.stdout);
}

test "process: /bin/echo via Command captures stdout + exit 0" {
    try volt.run(.{ .allocator = std.testing.allocator }, echoRoot, .{});
}

fn falseRoot() !void {
    const false_path: []const u8 = if (builtin.os.tag == .macos)
        "/usr/bin/false"
    else
        "/bin/false";
    var cmd = try Command.init(std.testing.allocator, false_path);
    defer cmd.deinit();

    const result = try cmd.output();
    defer result.deinit(std.testing.allocator);

    try std.testing.expect(result.term == .Exited);
    try std.testing.expectEqual(@as(u8, 1), result.term.Exited);
}

test "process: false captures non-zero exit" {
    try volt.run(.{ .allocator = std.testing.allocator }, falseRoot, .{});
}
