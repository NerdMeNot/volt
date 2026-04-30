---
title: Installation
description: Add Volt to your Zig project and verify the install with a one-line program.
---

## Requirements

- **Zig 0.16.0**. Volt pins to one Zig minor per release; the version
  is part of the tag (`vX.Y.Z-zigA.B.C`).
- **macOS** (arm64 or x86_64) or **Linux** (x86_64 or arm64). Windows
  cross-compiles cleanly today; runtime port is pending.
- **libc**. Volt uses `sigsetjmp` / `siglongjmp` / `mprotect` /
  `signalfd` directly; the build helper links libc automatically.

## Add the dependency

In your `build.zig.zon`:

```zig
.{
    .name = .my_project,
    .version = "0.1.0",
    .minimum_zig_version = "0.16.0",

    .dependencies = .{
        .volt = .{
            .url = "https://github.com/NerdMeNot/volt/archive/refs/tags/v1.0.0-zig0.16.0.tar.gz",
            .hash = "...", // run `zig build` once and paste the hash Zig prints
        },
    },

    .paths = .{ "build.zig", "build.zig.zon", "src" },
}
```

In your `build.zig`:

```zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const volt_dep = b.dependency("volt", .{
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "my_app",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("volt", volt_dep.module("volt"));
    exe.root_module.link_libc = true; // sigsetjmp/mprotect/signalfd

    b.installArtifact(exe);
}
```

## Verify

`src/main.zig`:

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    try volt.run(.{ .allocator = gpa.allocator() }, hello, .{});
}

fn hello() !void {
    try volt.sleep(volt.Duration.fromMillis(50));
    std.debug.print("hello from a coroutine\n", .{});
}
```

```sh
zig build run
# hello from a coroutine
```

If that prints, you're set up. The `volt.sleep(50ms)` call suspended
the coroutine, the reactor woke it, and it continued. No `async`,
no callback, no state machine.

## Building from source

If you're hacking on Volt itself:

```sh
git clone https://github.com/NerdMeNot/volt.git
cd volt
zig build              # build the library
zig build test         # run the test suite (~200+ tests)
zig build bench        # core benchmarks (ReleaseFast)

zig build run-echo            # cookbook examples
zig build run-fan-out
zig build run-work-offload
zig build run-timeout-retry
```

## Cross-compile sanity check

Volt's CI matrix runs `zig build-lib` for six target triples on every
commit. You can do the same locally:

```sh
zig build-lib src/lib.zig -target x86_64-linux-gnu     -lc -fno-emit-bin
zig build-lib src/lib.zig -target aarch64-linux-gnu    -lc -fno-emit-bin
zig build-lib src/lib.zig -target x86_64-windows-gnu   -lc -fno-emit-bin
zig build-lib src/lib.zig -target aarch64-windows-gnu  -lc -fno-emit-bin
zig build-lib src/lib.zig -target x86_64-macos         -lc -fno-emit-bin
zig build-lib src/lib.zig -target aarch64-macos        -lc -fno-emit-bin
```

All six should exit `0`. Windows compiles but does not yet run — see
the platform-status table in the [introduction](/).
