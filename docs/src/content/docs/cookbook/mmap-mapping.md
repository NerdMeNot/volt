---
title: Memory-mapped files
description: Zero-copy file access via volt.fs.mapFile and volt.fs.mapAnonymous. The kernel pages in regions on demand; you walk the slice.
---

`volt.fs.mapFile` maps an open file into the process address space.
The slice you get is the kernel's view of the file — reads page in
on demand, writes are flushed back via `msync`.

## Read-only file → slice

```zig
const std = @import("std");
const volt = @import("volt");

fn count_newlines(path: []const u8) !usize {
    var f = try volt.fs.File.open(path);
    defer f.close();

    var m = try volt.fs.mapFile(f.fd, .{});
    defer m.deinit();

    var n: usize = 0;
    for (m.asBytes()) |b| if (b == '\n') {
        n += 1;
    };
    return n;
}
```

## Writable shared mapping

```zig
var f = try volt.fs.File.openOptions(path, .{ .read = true, .write = true });
defer f.close();

var m = try volt.fs.mapFile(f.fd, .{
    .protection = .read_write,
    .sharing = .shared, // visible to other mappers + flushed to disk
    .length = 4096,
});
defer m.deinit();

const buf = try m.asBytesMut();
@memcpy(buf[0..5], "HELLO");
try m.flush(); // synchronous msync
```

## Anonymous scratch buffer

For a large zeroed buffer that the kernel can swap (vs `malloc` which
goes to the page tables eagerly):

```zig
var scratch = try volt.fs.mapAnonymous(.{
    .length = 64 * 1024 * 1024, // 64 MiB
});
defer scratch.deinit();
const buf = try scratch.asBytesMut();
// ... use buf
```

## Access hints — `advise`

Tell the kernel about your access pattern so it can prefetch
intelligently:

```zig
try m.advise(.sequential); // linear scan
try m.advise(.random);     // skipping around
try m.advise(.will_need);  // please page in soon
try m.advise(.dont_need);  // can evict from cache
```

## Truncation hazard

If the file is truncated *while* mapped, accessing the now-missing
pages raises `SIGBUS`. v1 documents this honestly: don't truncate
live mappings. A follow-up wave adds a `SIGBUS` handler + setjmp-
based recovery, after which:

```zig
try m.accessProtected(struct { ... }); // catches truncation
```

becomes the recommended pattern. For now, control truncation at the
application level.

## See also

- [`examples/mmap_count.zig`](https://github.com/NerdMeNot/blitz-io/tree/main/examples/mmap_count.zig)
