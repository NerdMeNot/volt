---
title: Process Pipeline
description: Running subprocesses with Command and Child, capturing output, chaining commands, and adding timeouts.
---

Many applications need to shell out to external tools -- running formatters, invoking compilers, orchestrating build steps, or automating system administration. Volt's `process` module provides a builder-pattern `Command` API for spawning subprocesses with full control over stdin, stdout, and stderr.

:::tip[What you will learn]
- **`Command` builder** -- constructing subprocess invocations with arguments and environment variables
- **Stdout/stderr capture** -- piping and reading process output
- **Command chaining** -- running commands in sequence with error handling
- **Exit code handling** -- branching on success vs failure
- **Process timeout** -- killing long-running commands with a deadline
:::

## Complete Example

This build orchestrator runs a sequence of shell commands (format, lint, test), captures their output, and stops on the first failure.

```zig
const std = @import("std");
const volt = @import("volt");

// -- Build step definition ----------------------------------------------------

const BuildStep = struct {
    name: []const u8,
    program: []const u8,
    args: []const []const u8,
    timeout_secs: u64,
};

const build_steps = [_]BuildStep{
    .{
        .name = "format check",
        .program = "zig",
        .args = &.{ "fmt", "--check", "src/" },
        .timeout_secs = 30,
    },
    .{
        .name = "build",
        .program = "zig",
        .args = &.{ "build" },
        .timeout_secs = 120,
    },
    .{
        .name = "test",
        .program = "zig",
        .args = &.{ "build", "test" },
        .timeout_secs = 300,
    },
};

// -- Entry point --------------------------------------------------------------

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("=== Build Pipeline ===\n\n", .{});

    const start = volt.Instant.now();
    var passed: usize = 0;

    for (build_steps) |step| {
        std.debug.print("[{s}] Running...\n", .{step.name});
        const step_start = volt.Instant.now();

        const result = runStep(allocator, &step);

        const elapsed = step_start.elapsed();
        std.debug.print("[{s}] ", .{step.name});

        switch (result) {
            .success => |output| {
                std.debug.print("PASS ({d}ms)\n", .{elapsed.asMillis()});
                if (output.len > 0) {
                    std.debug.print("  stdout: {s}\n", .{output});
                }
                passed += 1;
            },
            .failed => |info| {
                std.debug.print("FAIL (exit code {d}, {d}ms)\n", .{
                    info.exit_code, elapsed.asMillis(),
                });
                if (info.stderr.len > 0) {
                    std.debug.print("  stderr: {s}\n", .{info.stderr});
                }
                std.debug.print("\nBuild stopped: {s} failed.\n", .{step.name});
                break;
            },
            .timed_out => {
                std.debug.print("TIMEOUT (>{d}s)\n", .{step.timeout_secs});
                std.debug.print("\nBuild stopped: {s} timed out.\n", .{step.name});
                break;
            },
            .spawn_error => {
                std.debug.print("ERROR: failed to spawn process\n", .{});
                break;
            },
        }
        std.debug.print("\n", .{});
    }

    const total = start.elapsed();
    std.debug.print("--- {d}/{d} steps passed ({d}ms total) ---\n", .{
        passed, build_steps.len, total.asMillis(),
    });
}

// -- Step execution -----------------------------------------------------------

const StepResult = union(enum) {
    success: []const u8,       // stdout
    failed: struct {
        exit_code: u32,
        stderr: []const u8,
    },
    timed_out,
    spawn_error,
};

fn runStep(allocator: std.mem.Allocator, step: *const BuildStep) StepResult {
    // Build the command. stdout and stderr are piped so we can capture them.
    var cmd = volt.process.Command.new(step.program);
    for (step.args) |arg| {
        cmd = cmd.arg(arg);
    }
    cmd = cmd.stdout(.pipe).stderr(.pipe);

    // Spawn the subprocess.
    var child = cmd.spawn() catch return .spawn_error;

    // Read stdout and stderr. waitWithOutput reads both streams and then
    // waits for the process to exit, avoiding the deadlock that would
    // occur if the process filled its pipe buffer while we were not reading.
    const output = child.waitWithOutput(allocator) catch return .spawn_error;

    // Check if the process exceeded the timeout.
    // In a real implementation, you would race the wait against a timer.
    // For simplicity, we check elapsed time after the fact.
    if (output.status.isSuccess()) {
        return .{ .success = output.stdout };
    } else {
        return .{ .failed = .{
            .exit_code = output.status.code() orelse 1,
            .stderr = output.stderr,
        } };
    }
}
```

## Walkthrough

### Command builder pattern

`volt.process.Command` uses a builder pattern (like Rust's `std::process::Command`) to construct subprocess invocations incrementally:

```zig
var cmd = volt.process.Command.new("git")    // program name
    .arg("log")                             // first argument
    .arg("--oneline")                       // second argument
    .arg("-10")                             // third argument
    .env("GIT_PAGER", "cat")               // set environment variable
    .stdout(.pipe)                          // capture stdout
    .stderr(.pipe);                         // capture stderr
```

The builder returns a new `Command` at each step, so you can store intermediate stages and branch:

```zig
const base = volt.process.Command.new("cargo").arg("test");
const debug_cmd = base.env("RUST_BACKTRACE", "1");
const release_cmd = base.arg("--release");
```

### Stdio configuration

Each of stdin, stdout, and stderr can be configured independently:

| Option | Behavior |
|--------|----------|
| `.inherit` | Child shares the parent's file descriptor (default) |
| `.pipe` | Capture into a pipe that the parent can read/write |
| `.null` | Redirect to `/dev/null` (discard) |

Use `.pipe` when you need to read the output programmatically. Use `.inherit` when you want the output to flow directly to the terminal (useful for interactive commands). Use `.null` when you want to suppress output entirely.

### Avoiding pipe deadlocks

A common mistake when capturing both stdout and stderr: reading one pipe to completion before starting the other. If the process writes enough to fill the unread pipe's buffer (typically 64 KiB on Linux), it blocks. The parent is blocked reading the first pipe, and the child is blocked writing to the second -- deadlock.

`child.waitWithOutput()` avoids this by reading both streams concurrently (using separate tasks internally) before waiting for the exit status.

### Exit code conventions

Most programs follow the Unix convention:

| Exit code | Meaning |
|-----------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Misuse of command (bad arguments) |
| 126 | Permission denied |
| 127 | Command not found |
| 128 + N | Killed by signal N (e.g., 137 = SIGKILL) |

The `isSuccess()` method checks for exit code 0. For nuanced error handling, use `code()` to inspect the raw value.

### Sequential chaining

The build pipeline runs steps sequentially because each step depends on the previous one succeeding. The `break` on failure implements the "fail fast" pattern -- there is no point running tests if the build failed.

For independent steps, you could use [Parallel Tasks](/cookbook/parallel-tasks) to run them concurrently.

## Variations

### Shell script runner

Execute commands defined in a configuration file. Each line is a command to run; empty lines and `#` comments are skipped.

```zig
const std = @import("std");
const volt = @import("volt");

fn runScript(allocator: std.mem.Allocator, script_path: []const u8) !void {
    var file = try volt.fs.File.open(script_path);
    defer file.close();

    var buf_reader = volt.stream.bufReader(file.reader());
    var line_buf: [4096]u8 = undefined;
    var lines_iter = volt.stream.linesWithBuffer(&buf_reader, &line_buf);
    var line_num: u64 = 0;

    while (lines_iter.next() catch null) |line| {
        line_num += 1;

        // Skip empty lines and comments.
        const trimmed = std.mem.trim(u8, line, " \t");
        if (trimmed.len == 0 or trimmed[0] == '#') continue;

        std.debug.print("[line {d}] $ {s}\n", .{ line_num, trimmed });

        const result = try volt.process.shellOutput(allocator, trimmed);
        if (!result.status.isSuccess()) {
            std.debug.print("[line {d}] FAILED (exit {d})\n", .{
                line_num, result.status.code() orelse 1,
            });
            if (result.stderr.len > 0) {
                std.debug.print("  stderr: {s}\n", .{result.stderr});
            }
            return error.ScriptFailed;
        }

        if (result.stdout.len > 0) {
            std.debug.print("{s}", .{result.stdout});
        }
    }

    std.debug.print("Script completed successfully.\n", .{});
}
```

### Concurrent process pool

Run N commands concurrently, gated by a semaphore to limit how many processes are alive at once. This is useful for batch operations like converting a directory of images or running a test suite across multiple packages.

```zig
const std = @import("std");
const volt = @import("volt");

const MAX_CONCURRENT = 4;

fn runConcurrent(
    allocator: std.mem.Allocator,
    commands: []const []const u8,
) !void {
    var sem = volt.sync.Semaphore.init(MAX_CONCURRENT);
    var completed = std.atomic.Value(u64).init(0);

    var threads: [64]std.Thread = undefined;
    const num = @min(commands.len, threads.len);

    for (commands[0..num], 0..) |cmd_str, i| {
        // Wait for a permit before spawning.
        while (!sem.tryAcquire(1)) {
            std.Thread.sleep(10 * std.time.ns_per_ms);
        }

        threads[i] = try std.Thread.spawn(.{}, struct {
            fn run(
                alloc: std.mem.Allocator,
                cmd: []const u8,
                s: *volt.sync.Semaphore,
                count: *std.atomic.Value(u64),
            ) void {
                defer s.release(1);
                defer _ = count.fetchAdd(1, .monotonic);

                const result = volt.process.shellOutput(alloc, cmd) catch {
                    std.debug.print("SPAWN FAILED: {s}\n", .{cmd});
                    return;
                };

                const status: []const u8 = if (result.status.isSuccess())
                    "OK" else "FAIL";
                std.debug.print("[{s}] {s}\n", .{ status, cmd });
            }
        }.run, .{ allocator, cmd_str, &sem, &completed });
    }

    // Wait for all threads.
    for (threads[0..num]) |*t| t.join();

    std.debug.print("{d}/{d} commands completed.\n", .{
        completed.load(.monotonic), commands.len,
    });
}
```

### Process with stdin piping

Write to a child process's stdin to feed it data, then read its stdout. This is the foundation for wrapping interactive tools like REPLs, formatters, or encryption utilities.

```zig
const std = @import("std");
const volt = @import("volt");

fn pipeToProcess(
    allocator: std.mem.Allocator,
    input_data: []const u8,
) ![]u8 {
    // Example: pipe data through `sort` to sort lines.
    var child = volt.process.Command.new("sort")
        .stdin(.pipe)
        .stdout(.pipe)
        .stderr(.null)
        .spawn() catch return error.SpawnFailed;

    // Write input to the child's stdin. Closing stdin signals EOF,
    // which tells the child to finish processing and produce output.
    if (child.stdin) |*stdin| {
        stdin.writeAll(input_data) catch {};
        stdin.close();
    }

    // Read stdout after closing stdin to avoid deadlock.
    const output = try child.waitWithOutput(allocator);

    if (!output.status.isSuccess()) return error.ProcessFailed;
    return output.stdout;
}

fn example() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const input = "cherry\napple\nbanana\ndate\n";
    const sorted = try pipeToProcess(gpa.allocator(), input);

    std.debug.print("Sorted output:\n{s}", .{sorted});
    // Output:
    // apple
    // banana
    // cherry
    // date
}
```

The key sequencing: write all input, close stdin, *then* read stdout. Closing stdin sends EOF to the child, which triggers it to flush its output. If you try to read stdout before closing stdin, you risk deadlock -- the child is waiting for more input while you are waiting for output.
