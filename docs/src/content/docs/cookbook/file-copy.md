---
title: File copy + stat
description: Copy a file with volt.fs.copyFile. The copy bridges through the spawnBlocking pool so the calling coroutine parks rather than pinning a worker.
---

`volt.fs.copyFile` streams `src` to `dst` in 4 KiB chunks and
mirrors `src`'s mode bits to `dst`. Bridges through `spawnBlocking`
under the hood — the calling coroutine parks during each chunk
read/write so other tasks make progress.

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}

fn root() !void {
    try volt.fs.writeFile("/tmp/src.txt", "hello copy\n");
    defer volt.fs.unlink("/tmp/src.txt") catch {};

    try volt.fs.copyFile("/tmp/src.txt", "/tmp/dst.txt");
    defer volt.fs.unlink("/tmp/dst.txt") catch {};

    const m = try volt.fs.stat("/tmp/dst.txt");
    std.debug.print("copied {d} bytes, mode 0o{o}\n", .{ m.size(), m.permissions().getMode() });
}
```

## Related convenience helpers

The `volt.fs` facade exposes a small grab-bag of "do the obvious
thing" functions for everyday file work:

| Function | What it does |
|---|---|
| `readFile(allocator, path)` | Read whole file into an owned slice |
| `readFileString(allocator, path)` | Same + validate UTF-8 |
| `writeFile(path, data)` | Create-or-truncate + write |
| `appendFile(path, data)` | Open append + write |
| `copyFile(src, dst)` | Stream src to dst, preserve mode |
| `rename(old, new)` | Atomic if same filesystem |
| `symlink(target, link)` | Create symbolic link |
| `readLink(allocator, link)` | Resolve a symbolic link |
| `hardLink(target, link)` | Hard link (same fs required) |
| `unlink(path)` | Delete a file or symlink |
| `tempDir(allocator)` | System temp dir (TMPDIR / /tmp) |
| `tempFile(allocator, prefix)` | mkstemp-backed temp file |

All of these bridge through `spawnBlocking` when called from a
coroutine, so they never pin a worker.

## See also

- [`examples/file_copy.zig`](https://github.com/NerdMeNot/blitz-io/tree/main/examples/file_copy.zig)
- [`volt.fs.File`](/cookbook/file-watcher/) for the lower-level handle
