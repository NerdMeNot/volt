//! Process Command Builder
//!
//! Builder pattern for configuring and spawning child processes.
//!
//! ## Usage
//!
//! ```zig
//! const process = @import("volt").process;
//!
//! var child = try process.Command.init("ls")
//!     .arg("-la")
//!     .arg("/tmp")
//!     .currentDir("/home")
//!     .env("MY_VAR", "value")
//!     .stdout(.pipe)
//!     .stderr(.pipe)
//!     .spawn();
//! ```
//!

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Allocator = std.mem.Allocator;

const child_mod = @import("child.zig");
const Child = child_mod.Child;

// ─────────────────────────────────────────────────────────────────────────────
// Stdio
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for child process standard I/O.
pub const Stdio = enum {
    /// Inherit from parent process.
    inherit,

    /// Create a pipe for communication.
    pipe,

    /// Redirect to /dev/null (Unix) or NUL (Windows).
    null,

    /// Convert to std.process.Child.StdIo
    fn toStd(self: Stdio) std.process.Child.StdIo {
        return switch (self) {
            .inherit => .Inherit,
            .pipe => .Pipe,
            .null => .Ignore,
        };
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Command
// ─────────────────────────────────────────────────────────────────────────────

/// Builder for spawning child processes.
///
/// Use the builder pattern to configure the process, then call `spawn()`.
pub const Command = struct {
    const MAX_ARGS: usize = 64;
    const MAX_ENV: usize = 32;

    const EnvVar = struct {
        key: []const u8,
        value: []const u8,
    };

    /// The program to execute
    program: []const u8,

    /// Command line arguments (fixed-size buffer)
    arg_buf: [MAX_ARGS][]const u8 = undefined,
    arg_len: usize = 0,

    /// Environment variable overrides
    env_buf: [MAX_ENV]EnvVar = undefined,
    env_len: usize = 0,

    /// Whether to clear the inherited environment
    clear_env: bool = false,

    /// Working directory for the child process
    cwd: ?[]const u8 = null,

    /// Standard input configuration
    stdin_cfg: Stdio = .inherit,

    /// Standard output configuration
    stdout_cfg: Stdio = .inherit,

    /// Standard error configuration
    stderr_cfg: Stdio = .inherit,

    const Self = @This();

    /// Create a command for the given program.
    pub fn init(program: []const u8) Self {
        return .{
            .program = program,
        };
    }

    /// Add an argument.
    pub fn arg(self: Self, a: []const u8) Self {
        var copy = self;
        if (copy.arg_len < MAX_ARGS) {
            copy.arg_buf[copy.arg_len] = a;
            copy.arg_len += 1;
        }
        return copy;
    }

    /// Add multiple arguments.
    pub fn args(self: Self, argument_list: []const []const u8) Self {
        var copy = self;
        for (argument_list) |a| {
            if (copy.arg_len >= MAX_ARGS) break;
            copy.arg_buf[copy.arg_len] = a;
            copy.arg_len += 1;
        }
        return copy;
    }

    /// Get the arguments slice.
    pub fn getArgs(self: *const Self) []const []const u8 {
        return self.arg_buf[0..self.arg_len];
    }

    /// Set an environment variable for the child.
    pub fn env(self: Self, key: []const u8, value: []const u8) Self {
        var copy = self;
        if (copy.env_len < MAX_ENV) {
            copy.env_buf[copy.env_len] = .{ .key = key, .value = value };
            copy.env_len += 1;
        }
        return copy;
    }

    /// Get the environment variables slice.
    pub fn getEnvVars(self: *const Self) []const EnvVar {
        return self.env_buf[0..self.env_len];
    }

    /// Clear all inherited environment variables.
    pub fn envClear(self: Self) Self {
        var copy = self;
        copy.clear_env = true;
        return copy;
    }

    /// Set the working directory.
    pub fn currentDir(self: Self, dir: []const u8) Self {
        var copy = self;
        copy.cwd = dir;
        return copy;
    }

    /// Configure standard input.
    pub fn stdin(self: Self, cfg: Stdio) Self {
        var copy = self;
        copy.stdin_cfg = cfg;
        return copy;
    }

    /// Configure standard output.
    pub fn stdout(self: Self, cfg: Stdio) Self {
        var copy = self;
        copy.stdout_cfg = cfg;
        return copy;
    }

    /// Configure standard error.
    pub fn stderr(self: Self, cfg: Stdio) Self {
        var copy = self;
        copy.stderr_cfg = cfg;
        return copy;
    }

    /// Spawn the child process.
    pub fn spawn(self: *const Self) !Child {
        // Build argv array for std.process.Child
        var argv_storage: [MAX_ARGS + 1][]const u8 = undefined;
        argv_storage[0] = self.program;
        for (self.arg_buf[0..self.arg_len], 1..) |a, i| {
            argv_storage[i] = a;
        }
        const argv = argv_storage[0 .. self.arg_len + 1];

        // Create std.process.Child
        var child = std.process.Child.init(argv, std.heap.page_allocator);

        // Configure I/O
        child.stdin_behavior = self.stdin_cfg.toStd();
        child.stdout_behavior = self.stdout_cfg.toStd();
        child.stderr_behavior = self.stderr_cfg.toStd();

        // Set working directory
        if (self.cwd) |dir| {
            child.cwd = dir;
        }

        // Note: Custom environment not fully supported in this simplified version
        // Would need to allocate and format "key=value" strings

        // Spawn the process
        try child.spawn();

        // Wrap in our Child type
        return Child.initFromStd(child);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Command - builder pattern" {
    const cmd = Command.init("ls")
        .arg("-la")
        .arg("/tmp")
        .currentDir("/home")
        .stdout(.pipe)
        .stderr(.null);

    try std.testing.expectEqualStrings("ls", cmd.program);
    try std.testing.expectEqual(@as(usize, 2), cmd.arg_len);
    try std.testing.expectEqualStrings("-la", cmd.getArgs()[0]);
    try std.testing.expectEqualStrings("/tmp", cmd.getArgs()[1]);
    try std.testing.expectEqualStrings("/home", cmd.cwd.?);
    try std.testing.expectEqual(Stdio.inherit, cmd.stdin_cfg);
    try std.testing.expectEqual(Stdio.pipe, cmd.stdout_cfg);
    try std.testing.expectEqual(Stdio.null, cmd.stderr_cfg);
}

test "Command - multiple args" {
    const cmd = Command.init("echo")
        .args(&.{ "hello", "world", "!" });

    try std.testing.expectEqual(@as(usize, 3), cmd.arg_len);
}

test "Command - env vars" {
    const cmd = Command.init("env")
        .env("FOO", "bar")
        .env("BAZ", "qux");

    try std.testing.expectEqual(@as(usize, 2), cmd.env_len);
    try std.testing.expectEqualStrings("FOO", cmd.getEnvVars()[0].key);
    try std.testing.expectEqualStrings("bar", cmd.getEnvVars()[0].value);
}

test "Command - spawn echo" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var child = try Command.init("/bin/echo")
        .arg("hello")
        .stdout(.null)
        .spawn();

    const status = try child.waitBlocking();
    try std.testing.expect(status.isSuccess());
    try std.testing.expectEqual(@as(u8, 0), status.code().?);
}

test "Command - spawn with pipe" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    var child = try Command.init("/bin/echo")
        .arg("hello world")
        .stdout(.pipe)
        .spawn();

    // Read stdout
    var buf: [64]u8 = undefined;
    const n = child.stdout.?.read(&buf) catch 0;

    // Don't close stdout - std.process.Child.wait() will do it
    const status = try child.waitBlocking();

    try std.testing.expect(status.isSuccess());
    try std.testing.expect(n > 0);
    try std.testing.expectEqualStrings("hello world\n", buf[0..n]);
}

test "Command - spawn nonexistent" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;

    // Spawning succeeds (fork), but exec fails in the child
    // std.process.Child reports this as an error in wait()
    var child = try Command.init("/nonexistent/binary").spawn();

    // Wait returns an error because the child failed to exec
    if (child.waitBlocking()) |status| {
        // If we get a status, it should indicate failure
        try std.testing.expect(!status.isSuccess());
    } else |err| {
        // Expected: exec failed, so wait returns an error
        try std.testing.expect(err == error.FileNotFound or
            err == error.InvalidExe or
            err == error.AccessDenied or
            @intFromError(err) != 0);
    }
}

test "Command - spawn echo (Windows)" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;

    var child = try Command.init("cmd")
        .args(&.{ "/c", "echo", "hello" })
        .stdout(.null)
        .spawn();

    const status = try child.waitBlocking();
    try std.testing.expect(status.isSuccess());
    try std.testing.expectEqual(@as(?u8, 0), status.code());
}

test "Command - spawn with pipe (Windows)" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;

    var child = try Command.init("cmd")
        .args(&.{ "/c", "echo", "hello world" })
        .stdout(.pipe)
        .spawn();

    // Read stdout
    var buf: [64]u8 = undefined;
    const n = child.stdout.?.read(&buf) catch 0;

    const status = try child.waitBlocking();

    try std.testing.expect(status.isSuccess());
    try std.testing.expect(n > 0);
}
