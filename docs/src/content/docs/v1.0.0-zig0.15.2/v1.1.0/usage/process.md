---
title: Process Management
description: Spawning and managing child processes with Volt's process module.
slug: v1.0.0-zig0.15.2/v110/usage/process
---

Volt provides subprocess management through a builder pattern. All APIs are under `volt.process`.

:::note[No runtime needed]
Process spawning and I/O are synchronous operations. They work without starting a runtime. For non-blocking process management inside the runtime, wrap long-running process waits in `io.concurrent()`.
:::

## Quick Start

### Run a command and wait

```zig
const volt = @import("volt");

// Run and wait for completion
const status = try volt.process.run(&.{ "ls", "-la" });
if (!status.isSuccess()) {
    // handle failure
}
```

### Collect output

```zig
const result = try volt.process.output(allocator, &.{ "git", "status", "--short" });
defer allocator.free(result.stdout);
defer allocator.free(result.stderr);

if (result.status.isSuccess()) {
    // result.stdout contains the output
}
```

### Shell commands

```zig
// Run a shell command (uses /bin/sh -c on Unix)
const status = try volt.process.shell("echo hello && date");

// Collect shell output
const result = try volt.process.shellOutput(allocator, "ls -la | wc -l");
defer allocator.free(result.stdout);
defer allocator.free(result.stderr);
```

## Command Builder

For full control, use the `Command` builder:

```zig
var cmd = volt.process.Command.init("cargo");
cmd = cmd.arg("build").arg("--release");
cmd = cmd.currentDir("/path/to/project");
cmd = cmd.env("RUST_LOG", "debug");
cmd = cmd.stdout(.pipe);
cmd = cmd.stderr(.pipe);

var child = try cmd.spawn();
```

### Adding arguments

```zig
var cmd = volt.process.Command.init("gcc");

// One at a time
cmd = cmd.arg("-Wall").arg("-O2").arg("-o").arg("output");

// Multiple at once
cmd = cmd.args(&.{ "main.c", "util.c" });
```

### Environment variables

```zig
var cmd = volt.process.Command.init("node");
cmd = cmd.arg("server.js");
cmd = cmd.env("PORT", "3000");
cmd = cmd.env("NODE_ENV", "production");

// Or clear inherited environment entirely
cmd = cmd.envClear();
cmd = cmd.env("PATH", "/usr/bin");
```

### Standard I/O configuration

Each of stdin, stdout, and stderr can be set to:

| Value | Behavior |
|-------|----------|
| `.inherit` | Shares parent's file descriptor (default) |
| `.pipe` | Creates a pipe for programmatic I/O |
| `.null` | Discards output / provides empty input |

```zig
var cmd = volt.process.Command.init("my-tool");
cmd = cmd.stdin(.pipe);    // we'll write to it
cmd = cmd.stdout(.pipe);   // we'll read from it
cmd = cmd.stderr(.null);   // discard stderr
```

## Working with Child Processes

### Reading output

```zig
var cmd = volt.process.Command.init("echo");
cmd = cmd.arg("hello world").stdout(.pipe);

var child = try cmd.spawn();

// Read all stdout
const stdout_data = try child.stdout.?.readAll(allocator);
defer allocator.free(stdout_data);

const status = try child.waitBlocking();
```

### Writing to stdin

```zig
var cmd = volt.process.Command.init("sort");
cmd = cmd.stdin(.pipe).stdout(.pipe);

var child = try cmd.spawn();

// Write data to the process
try child.stdin.?.writeAll("banana\napple\ncherry\n");
child.stdin.?.close();

// Read sorted output
const sorted = try child.stdout.?.readAll(allocator);
defer allocator.free(sorted);

const status = try child.waitBlocking();
```

### Waiting and collecting output

```zig
var child = try cmd.spawn();

// Blocking wait (returns ExitStatus)
const status = try child.waitBlocking();

// Or collect all output at once
var child2 = try cmd.spawn();
const output = try child2.waitWithOutput(allocator);
defer allocator.free(output.stdout);
defer allocator.free(output.stderr);
```

### Non-blocking check

```zig
var child = try cmd.spawn();

// Poll without blocking
if (try child.tryWait()) |status| {
    // Process has exited
    if (status.isSuccess()) {
        // ...
    }
} else {
    // Still running
}
```

## Signals and Termination

```zig
var child = try cmd.spawn();

// Graceful termination (SIGTERM on Unix)
try child.terminate();

// Forceful kill (SIGKILL on Unix)
try child.kill();

// Send specific signal (Unix only)
try child.signal(std.posix.SIG.USR1);
```

## Exit Status

```zig
const status = try child.waitBlocking();

if (status.isSuccess()) {
    // exited with code 0
}

if (status.code()) |exit_code| {
    // exited normally with this code
}

if (status.wasSignaled()) {
    if (status.signal()) |sig| {
        // terminated by this signal
    }
}
```

## Process Pipelines

Chain processes together by piping stdout to stdin:

```zig
// ls -la | grep ".zig" | wc -l
// Easiest with shell():
const result = try volt.process.shellOutput(allocator, "ls -la | grep '.zig' | wc -l");
defer allocator.free(result.stdout);
defer allocator.free(result.stderr);
```

For programmatic pipelines, spawn processes with `.pipe` I/O and connect them manually, or use the shell approach above.

## Async Process Management

Inside the runtime, wrap blocking waits with `io.concurrent()`:

```zig
fn runBuild(io: volt.Io) !volt.process.ExitStatus {
    var f = try io.concurrent(struct {
        fn run() !volt.process.ExitStatus {
            return volt.process.run(&.{ "cargo", "build", "--release" });
        }
    }.run, .{});
    return try f.@"await"(io);
}
```

## API Summary

| Function | Description |
|----------|-------------|
| `process.run(argv)` | Run command and wait |
| `process.output(alloc, argv)` | Run and collect output |
| `process.shell(cmd)` | Run shell command |
| `process.shellOutput(alloc, cmd)` | Run shell command and collect output |
| `Command.init(program)` | Create command builder |
| `cmd.arg(a)` / `cmd.args(list)` | Add arguments |
| `cmd.env(k, v)` | Set environment variable |
| `cmd.stdin/stdout/stderr(cfg)` | Configure I/O |
| `cmd.spawn()` | Start the process |
| `child.waitBlocking()` | Wait for exit |
| `child.tryWait()` | Non-blocking poll |
| `child.kill()` / `child.terminate()` | Stop the process |
| `child.waitWithOutput(alloc)` | Wait and collect output |
