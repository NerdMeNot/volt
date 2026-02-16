//! # Process Module
//!
//! Async-aware process spawning and management for the volt runtime.
//!
//! ## Overview
//!
//! | Type | Description |
//! |------|-------------|
//! | `Command` | Command builder for spawning processes |
//! | `Child` | Running child process |
//! | `ExitStatus` | Process exit status |
//! | `Stdio` | Stdin/stdout/stderr configuration |
//! | `Output` | Collected stdout/stderr output |
//! | `WaitWaiter` | Waiter for process completion |
//!
//! | Function | Description |
//! |----------|-------------|
//! | `run` | Execute command and wait for completion |
//! | `output` | Execute command and collect output |
//! | `shell` | Execute shell command string |
//! | `shellOutput` | Execute shell command and collect output |
//!
//! ## Quick Start
//!
//! ```zig
//! const process = @import("volt").process;
//!
//! // Simple command execution
//! var child = try process.Command.new("ls")
//!     .arg("-la")
//!     .spawn();
//!
//! const status = try child.wait();
//! if (status.isSuccess()) {
//!     std.debug.print("Command succeeded\n", .{});
//! }
//! ```
//!
//! ## Piped I/O
//!
//! ```zig
//! var child = try process.Command.new("cat")
//!     .stdin(.pipe)
//!     .stdout(.pipe)
//!     .spawn();
//!
//! // Write to stdin
//! try child.stdin.?.writeAll("Hello, World!\n");
//! child.stdin.?.close();
//!
//! // Read from stdout
//! var buf: [1024]u8 = undefined;
//! const n = try child.stdout.?.readAll(&buf);
//! std.debug.print("Output: {s}\n", .{buf[0..n]});
//!
//! _ = try child.wait();
//! ```
//!
//! ## Async Waiting
//!
//! ```zig
//! var child = try process.Command.new("sleep").arg("10").spawn();
//!
//! // Non-blocking check
//! if (child.tryWait()) |status| {
//!     // Already finished
//! } else {
//!     // Still running, use waiter pattern
//!     var waiter = process.WaitWaiter.init();
//!     if (!child.wait(&waiter)) {
//!         // Yield to scheduler...
//!     }
//! }
//! ```
//!
//! ## Design
//!
//! - Uses waiter pattern consistent with sync primitives
//! - Platform-specific implementations (POSIX fork/exec, Windows CreateProcess)
//! - Non-blocking wait with WNOHANG
//! - Piped I/O integration with async streams
//!

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Allocator = std.mem.Allocator;

// Re-export main types
pub const command_mod = @import("process/command.zig");
pub const child_mod = @import("process/child.zig");

// Command builder
pub const Command = command_mod.Command;
pub const Stdio = command_mod.Stdio;

// Child process
pub const Child = child_mod.Child;
pub const ChildStdin = child_mod.ChildStdin;
pub const ChildStdout = child_mod.ChildStdout;
pub const ChildStderr = child_mod.ChildStderr;
pub const ExitStatus = child_mod.ExitStatus;

// Waiters
pub const WaitWaiter = child_mod.WaitWaiter;
pub const WaitFuture = child_mod.WaitFuture;
pub const OutputFuture = child_mod.OutputFuture;

// Convenience types
pub const Output = child_mod.Output;

// ─────────────────────────────────────────────────────────────────────────────
// Convenience Functions
// ─────────────────────────────────────────────────────────────────────────────

/// Execute a command and collect its output.
/// Blocks until the process completes.
pub fn output(allocator: Allocator, argv: []const []const u8) !Output {
    if (argv.len == 0) return error.EmptyCommand;

    var cmd = Command.new(argv[0]);
    for (argv[1..]) |arg| {
        cmd = cmd.arg(arg);
    }

    var c = try cmd.stdout(.pipe).stderr(.pipe).spawn();
    return c.waitWithOutput(allocator);
}

/// Execute a command and wait for it to complete.
/// Returns the exit status.
pub fn run(argv: []const []const u8) !ExitStatus {
    if (argv.len == 0) return error.EmptyCommand;

    var cmd = Command.new(argv[0]);
    for (argv[1..]) |arg| {
        cmd = cmd.arg(arg);
    }

    var c = try cmd.spawn();
    return c.waitBlocking();
}

/// Execute a shell command.
/// On Unix, runs via /bin/sh -c.
/// On Windows, runs via cmd.exe /C.
pub fn shell(cmd_str: []const u8) !ExitStatus {
    if (builtin.os.tag == .windows) {
        var cmd = Command.new("cmd.exe");
        var c = try cmd.arg("/C").arg(cmd_str).spawn();
        return c.waitBlocking();
    } else {
        var cmd = Command.new("/bin/sh");
        var c = try cmd.arg("-c").arg(cmd_str).spawn();
        return c.waitBlocking();
    }
}

/// Execute a shell command and collect its output.
pub fn shellOutput(allocator: Allocator, cmd_str: []const u8) !Output {
    if (builtin.os.tag == .windows) {
        var cmd = Command.new("cmd.exe");
        var c = try cmd.arg("/C").arg(cmd_str).stdout(.pipe).stderr(.pipe).spawn();
        return c.waitWithOutput(allocator);
    } else {
        var cmd = Command.new("/bin/sh");
        var c = try cmd.arg("-c").arg(cmd_str).stdout(.pipe).stderr(.pipe).spawn();
        return c.waitWithOutput(allocator);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test {
    _ = @import("process/command.zig");
    _ = @import("process/child.zig");
}
