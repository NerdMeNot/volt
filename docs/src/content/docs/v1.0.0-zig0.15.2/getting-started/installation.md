---
title: Installation
description: How to add Volt to your Zig project using the package manager, or
  build from source.
sidebar:
  order: 1
slug: v1.0.0-zig0.15.2/getting-started/installation
---

## Requirements

| Requirement | Version |
|-------------|---------|
| **Zig** | 0.15.0 or later (0.15.2 recommended) |
| **Linux** | Kernel 5.1+ for io\_uring, 4.x+ for epoll fallback |
| **macOS** | 10.12+ (x86\_64 and Apple Silicon) |
| **Windows** | Windows 10+ for IOCP |

Volt auto-detects the best I/O backend for your platform at startup. No compile-time flags are needed.

***

## Using the Zig Package Manager

### Step 1: Add the dependency

Add Volt to your `build.zig.zon`:

```zig
.{
    .name = .my_project,
    .version = "0.1.0",
    .minimum_zig_version = "0.15.0",

    .dependencies = .{
        .volt = .{
            .url = "https://github.com/NerdMeNot/volt/archive/refs/tags/v1.0.0-zig0.15.2.tar.gz",
            .hash = "volt-1.0.0-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            // Run `zig build` once to get the correct hash from the error message
        },
    },

    .paths = .{
        "build.zig",
        "build.zig.zon",
        "src",
    },
}
```

On first build, Zig will print the correct hash value if the placeholder does not match. Copy the real hash from the error message and replace the placeholder.

### Step 2: Wire up `build.zig`

Import the Volt module in your `build.zig`:

```zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Fetch the Volt dependency
    const volt_dep = b.dependency("volt", .{
        .target = target,
        .optimize = optimize,
    });

    // Create your executable
    const exe = b.addExecutable(.{
        .name = "my_app",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Add the Volt module so you can @import("volt")
    exe.root_module.addImport("volt", volt_dep.module("volt"));

    b.installArtifact(exe);

    // Optional: add a run step
    const run_cmd = b.addRunArtifact(exe);
    const run_step = b.step("run", "Run the application");
    run_step.dependOn(&run_cmd.step);
}
```

### Step 3: Verify it works

Create a minimal `src/main.zig`:

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    std.debug.print("Volt v{s} loaded\n", .{volt.version.string});
    std.debug.print("Runtime, sync, channel, net, time modules available.\n", .{});
}
```

Build and run:

```bash
zig build run
```

Expected output:

```
Volt v1.0.0 loaded
Runtime, sync, channel, net, time modules available.
```

If you see this, Volt is correctly installed and linked.

***

## Building from Source

Clone the repository and build directly:

```bash
git clone https://github.com/NerdMeNot/volt.git
cd volt
```

### Build the library

```bash
zig build
```

### Run the test suites

Volt ships with extensive tests:

| Command | What it runs |
|---------|-------------|
| `zig build test` | Unit tests (588+ inline tests across all modules) |
| `zig build test-concurrency` | Loom-style concurrency tests (83 tests) |
| `zig build test-robustness` | Edge case and robustness tests (35+ tests) |
| `zig build test-stress` | Stress tests with real threads |
| `zig build test-all` | All of the above |

```bash
# Quick validation
zig build test

# Full suite
zig build test-all
```

### Run benchmarks

```bash
zig build bench               # Run Volt benchmarks
zig build compare             # Side-by-side comparison with Tokio (requires Rust)
```

### Generate API documentation

```bash
zig build docs
# Output in zig-out/docs/
```

***

## Optional: Blitz Integration

Volt pairs with [Blitz](https://github.com/NerdMeNot/blitz) for CPU parallelism -- see the [Blitz integration guide](/v1.0.0-zig0.15.2/guides/blitz-integration/). Most users do not need this; the blocking pool (`io.concurrent`) handles CPU-intensive work without Blitz.

***

## Project Structure

When building from source, here is the repository layout:

```
volt/
├── build.zig           # Build configuration
├── build.zig.zon       # Package metadata and dependencies
├── src/
│   ├── lib.zig         # Public API entry point (@import("volt"))
│   ├── runtime.zig     # Runtime lifecycle (init, run, deinit)
│   ├── task.zig        # Task spawning (async, concurrent, Future, Group)
│   ├── sync.zig        # Sync primitives (Mutex, RwLock, Semaphore, ...)
│   ├── channel.zig     # Channels (Channel, Oneshot, Broadcast, Watch)
│   ├── future.zig      # Future/Poll/Waker types
│   ├── net.zig         # Networking (TCP, UDP, Unix)
│   ├── fs.zig          # Filesystem
│   ├── time.zig        # Duration, Instant, Sleep, Interval
│   ├── signal.zig      # Signal handling
│   ├── process.zig     # Process spawning
│   ├── shutdown.zig    # Graceful shutdown
│   ├── async.zig       # Async operations namespace
│   ├── stream.zig      # I/O streams
│   └── internal/       # Scheduler, backends, blocking pool
├── tests/              # Integration, stress, concurrency tests
└── bench/              # Benchmarks
```

***

## Troubleshooting

### Hash mismatch on first build

This is expected. Run `zig build` once, copy the correct hash from the error message, and paste it into your `build.zig.zon`.

### "Not running inside a Volt runtime"

This error comes from attempting to use runtime features outside a runtime context. All async operations go through the `io` handle, which prevents this error at compile time -- you cannot call `io.@"async"()` without an `io` handle:

```zig
// Preferred: io.@"async"() is compile-time safe
pub fn main() !void {
    try volt.run(myApp);
}

fn myApp(io: volt.Io) !void {
    var f = try io.@"async"(myFunc, .{});  // can't be called without io
    const result = f.@"await"(io);
}
```

If you need an explicit runtime handle:

```zig
pub fn main() !void {
    var io = try volt.Io.init(allocator, .{});
    defer io.deinit();
    try io.run(myApp);
}
```

### Linux: io\_uring not available

On kernels older than 5.1, Volt falls back to epoll automatically. No code changes are needed. To check your kernel version:

```bash
uname -r
```

### Build errors with Zig version

Volt requires Zig 0.15.0 or later. Key 0.15.x API differences from earlier versions:

* Atomics use `std.atomic.Value(T)`, not `std.atomic.Atomic`
* POSIX calls use `std.posix`, not `std.os`
* No `@fence` builtin -- use `fetchAdd(0, .seq_cst)` for fences
* No async/await keywords -- Volt uses explicit `Io` handle and manual state machines (similar to Zig's upcoming `std.Io` approach)

Check your version with:

```bash
zig version
```
