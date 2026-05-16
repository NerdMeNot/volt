---
title: Installation
description: Add Volt to your Zig project and verify the install with a one-line program.
---

## Requirements

- **Zig 0.16.0.** Volt pins to one Zig minor per release; the version
  is part of the tag (`vX.Y.Z-zigA.B.C`).
- **Darwin arm64.** Today this is the only platform with a working
  reactor (kqueue). Linux (epoll / io_uring) and Windows (IOCP) are
  cross-compile-clean but not runtime-ready — see [Roadmap](/appendix/roadmap/).
- **libc.** Volt uses `mmap` / `mprotect` / `kqueue` / `__ulock_wait`
  via `@extern`; the build helper links libc automatically.

## Add the dependency

`build.zig.zon`:

```zig
.{
    .name = .my_project,
    .version = "0.1.0",
    .minimum_zig_version = "0.16.0",

    .dependencies = .{
        .volt = .{
            .url = "https://github.com/NerdMeNot/volt/archive/refs/tags/v1.0.0-zig0.16.0.tar.gz",
            // .hash = ... — run `zig build` once and paste the hash Zig prints
        },
    },

    .paths = .{ "build.zig", "build.zig.zon", "src" },
}
```

`build.zig`:

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
    exe.root_module.link_libc = true; // mmap / kqueue / __ulock_wait

    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    b.step("run", "Run the app").dependOn(&run.step);
}
```

## Verify

`src/main.zig`:

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(hello, .{}));
}

fn hello() !void {
    volt.sleep(50 * std.time.ns_per_ms);
    std.debug.print("hello from a coroutine\n", .{});
}
```

```sh
zig build run
# hello from a coroutine
```

If that prints, you're set up. `volt.sleep` parked the coroutine on
the kqueue reactor's timer; ~50 ms later the kernel delivered the
timer event, the reactor woke the coroutine, and it continued. No
`async`, no callback, no state machine. The whole program is six
lines of executable code.

## Building from source

If you're hacking on Volt itself:

```sh
git clone https://github.com/NerdMeNot/volt.git
cd volt
zig build              # build the volt module
zig build test         # ~47 tests, leak-detecting
zig build stress       # 45 s multi-primitive stress test
zig build bench-yield  # one bench at a time — see build.zig for the full list
```

Pre-commit hook (`zig fmt --check` + `zig build-lib` type-check):

```sh
git config core.hooksPath .githooks
```

Tests run in CI on every push; the pre-commit hook is fast (~2 s).

## Cross-compile sanity check (optional)

Volt's CI compiles `zig build-lib` for Linux + Windows targets even
though the reactor doesn't ship for them yet — this catches type
errors that would block the eventual port:

```sh
zig build-lib src/lib.zig -target x86_64-linux-gnu  -lc -fno-emit-bin
zig build-lib src/lib.zig -target aarch64-linux-gnu -lc -fno-emit-bin
```

Both should exit `0`. Neither produces a working binary today — the
kqueue reactor is Darwin-specific.

## Next

- [Your first program](/getting-started/first-program/) — the
  one-pager walkthrough of what just happened.
- [Spawning and joining](/getting-started/spawn-join/) — spawn a
  child coroutine and collect its result.
