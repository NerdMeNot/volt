---
title: Filesystem
description: volt.fs — coroutine-aware file I/O via the blocking pool.
---

The v1.0 filesystem surface is intentionally tiny:

```zig
pub fn readFile(allocator: std.mem.Allocator, path: [:0]const u8) ![]u8;
pub fn writeFile(path: [:0]const u8, data: []const u8) !void;
```

Both run on the **blocking thread pool** — file syscalls are
blocking on every platform Volt supports today (kqueue and epoll
don't deliver readiness for regular files; that's an
io_uring-only feature). The calling coroutine parks; a pool thread
does the read or write; the coroutine resumes with the result.

## readFile

```zig
const data = try volt.fs.readFile(allocator, "/etc/hostname");
defer allocator.free(data);
```

Reads the entire file into a heap-allocated buffer. Returns
`error.FileNotFound`, `error.AccessDenied`, etc. on failure.

## writeFile

```zig
try volt.fs.writeFile("/tmp/output.txt", "hello\n");
```

Opens (creating if needed), writes, closes. Truncates existing
files.

## Concurrent file I/O

Each `readFile` / `writeFile` call parks the *calling* coroutine on
its own pool worker. To read N files in parallel, spawn N coroutines:

```zig
fn loadAll(paths: []const [:0]const u8) !void {
    const alloc = volt.currentRuntime().?.allocator;
    try volt.scope(struct {
        fn body(s: *volt.Scope) !void {
            const args_paths = scope_arg.?;
            for (args_paths) |path| {
                try s.spawn(loadOne, .{path});
            }
        }
    }.body);
}

fn loadOne(path: [:0]const u8) !void {
    const alloc = volt.currentRuntime().?.allocator;
    const data = try volt.fs.readFile(alloc, path);
    defer alloc.free(data);
    // ... process ...
}
```

The blocking pool services them in parallel up to its thread cap
(currently large enough that you won't hit it for normal file I/O).

## What's NOT here

v1.0 does not yet ship:

- **`File` handle type**: open once, do many reads/writes, close.
- **Streaming reads**: read in chunks rather than all-at-once.
- **`mkdir` / `readDir` / `stat` / etc.**
- **`mmapFile`** — zero-copy file access via mmap.
- **Direct io_uring file ops on Linux** — would skip the blocking
  pool entirely. The infrastructure is in place
  (`reactor_iouring.zig`); the file API hasn't been wired up yet.

For anything beyond `readFile` / `writeFile`, drop to `std.posix`
or `std.fs.cwd().openFile` and use `volt.spawnBlocking` for the
syscalls:

```zig
const result = try volt.spawnBlocking(struct {
    fn body() ![]u8 {
        const f = try std.fs.cwd().openFile("path", .{});
        defer f.close();
        // ... read ...
    }
}.body, .{});
```

That gets you the full `std.fs` surface with coroutine-friendly
parking. The dedicated `volt.fs` API will grow as it gets
real-world use; if you have a specific need, the simplest path is
to file an issue describing the operation.

## Why no streaming readers

A stackful runtime has a tempting "just write blocking code"
shortcut for streaming I/O — call `readFile` in a loop, line by
line, like you would with `BufReader` in std. The catch: each
syscall parks on the blocking pool, which means each line costs
one round-trip through the pool. For a 100MB log file with 1M
lines, that's a million pool round-trips.

A proper streaming reader needs either:
- io_uring (real async file I/O on Linux ≥ 5.1), so the reactor
  drives the I/O directly; or
- A buffered reader that does fewer, larger pool round-trips and
  parses lines from the buffer.

Both are planned. Until then, slurp full files with `readFile` and
parse in memory, or use `volt.spawnBlocking` for the whole
file-processing function so the parsing happens off-loop.
