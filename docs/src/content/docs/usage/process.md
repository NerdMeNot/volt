---
title: Subprocess Management
description: volt.process.Command — spawn external programs and capture their output without blocking a worker.
---

`volt.process.Command` is a builder for spawning external programs.
v1.0 ships a minimal first cut: argv, capture stdout, wait for exit.

## Basic usage

```zig
var cmd = try volt.process.Command.init(allocator, "git");
defer cmd.deinit();

try cmd.arg("rev-parse");
try cmd.arg("HEAD");

const result = try cmd.output();
defer result.deinit(allocator);

switch (result.term) {
    .Exited => |code| std.debug.print("exit code: {}\n", .{code}),
    .Signal => |sig| std.debug.print("killed by signal: {}\n", .{sig}),
    .Stopped, .Unknown => {},
}
std.debug.print("stdout: {s}\n", .{ result.stdout });
```

## API

### `Command.init(allocator, program)`

Creates a Command builder for the named program. Looks up `program`
on `PATH` (uses `execve`'s search behavior internally).

### `Command.arg(value)`

Appends one argument. Each call adds one positional arg.

### `Command.output()`

Spawns the process, reads its stdout to EOF, waits for exit, and
returns an `Output`:

```zig
pub const Output = struct {
    term: Term,            // Exited | Signal | Stopped | Unknown
    stdout: []const u8,    // captured to EOF; owner: caller
};
```

`output()` runs on the **blocking thread pool** — `fork` / `execve`
/ `waitpid` are blocking calls, so doing them on a regular Volt
worker would block all other coroutines on that worker. The
calling coroutine parks; a pool thread does the work; the coroutine
resumes when the child exits.

This means `output()` is fully concurrent-safe: launching N
subprocesses simultaneously from N coroutines runs them in parallel
because each is on its own pool thread.

### `Output.deinit(allocator)`

Frees the captured stdout buffer. Always pair with `output()`.

### `Term` variants

```zig
pub const Term = union(enum) {
    Exited: u8,        // normal exit; field is exit code
    Signal: u8,        // killed by signal; field is signal number
    Stopped: u8,       // SIGSTOP'd
    Unknown: u32,      // raw waitpid status bits we couldn't classify
};
```

## Patterns

### Run-and-capture pipeline

```zig
fn captureGitInfo(alloc: std.mem.Allocator) ![]const u8 {
    var cmd = try volt.process.Command.init(alloc, "git");
    defer cmd.deinit();
    try cmd.arg("log");
    try cmd.arg("--oneline");
    try cmd.arg("-n");
    try cmd.arg("10");

    const r = try cmd.output();
    if (r.term != .Exited or r.term.Exited != 0) {
        r.deinit(alloc);
        return error.GitFailed;
    }
    return r.stdout;  // caller owns; remember to free
}
```

### Concurrent subprocesses

```zig
fn runEach(scope: *volt.Scope, items: []const []const u8) !void {
    for (items) |item| {
        try scope.spawn(runOne, .{item});
    }
}

fn runOne(item: []const u8) !void {
    const alloc = volt.currentRuntime().?.allocator;
    var cmd = try volt.process.Command.init(alloc, "process-item");
    defer cmd.deinit();
    try cmd.arg(item);
    const r = try cmd.output();
    defer r.deinit(alloc);
    // ... handle r ...
}
```

Each `runOne` parks on its own subprocess; the blocking pool
services them in parallel.

### Timeout on subprocess

```zig
const r = volt.withTimeout(
    volt.Duration.fromSecs(30),
    runIt,
    .{cmd_path, args},
) catch |err| switch (err) {
    error.Timeout => return error.SubprocessHung,
    else => return err,
};
```

Cancelling the calling coroutine cancels its park on the blocking
pool thread — but the subprocess itself keeps running until it
exits. v1.0 doesn't kill the child on cancel; that's a v1.x add.
For now, time-bound subprocess work at the application level.

## What's NOT here yet

The v1.0 `Command` is intentionally minimal — `output()` is the
only execution method. Planned additions:

- `Command.spawn() !Child` — async wait + per-stream pipe access.
- `Command.stdin.write(...)` — feed data to a child via stdin.
- `Command.kill()` — send a signal to the child on cancel.
- Environment variable inheritance / override.
- Working directory override.
- Async wait via `signalfd(SIGCHLD)` — no blocking-pool thread per
  subprocess, lets you have thousands of children if you really
  want.

If your use case needs any of these, drop down to `std.posix.fork`
+ `std.posix.execve` and use `volt.spawnBlocking(waitpid, .{pid})`
to wait — it's not pretty but it's a tractable workaround until
the API expands.
