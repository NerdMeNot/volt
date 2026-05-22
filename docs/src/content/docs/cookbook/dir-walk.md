---
title: Directory walk
description: Stream a directory tree with volt.fs.Dir.walk. A visitor receives every entry with depth; return .continue / .skip / .stop to drive traversal.
---

`volt.fs.Dir.walk` is a streaming, iterative tree walk. You provide
a visitor — any struct with a `pub fn visit(entry, depth) WalkAction`
method — and `walk` recurses on your behalf, calling back per entry.

```zig
const std = @import("std");
const volt = @import("volt");

const Counter = struct {
    files: u32 = 0,
    dirs: u32 = 0,

    pub fn visit(self: *Counter, entry: volt.fs.DirEntry, depth: u32) volt.fs.WalkAction {
        _ = depth;
        switch (entry.kind) {
            .file => self.files += 1,
            .directory => self.dirs += 1,
            else => {},
        }
        return .@"continue";
    }
};

fn count(allocator: std.mem.Allocator, root: []const u8) !void {
    var d = try volt.fs.Dir.open(root);
    defer d.close();
    var c = Counter{};
    try d.walk(root, allocator, &c, .{ .max_depth = 3 });
    std.debug.print("{d} files, {d} dirs\n", .{ c.files, c.dirs });
}
```

## WalkAction

The visitor's return value drives traversal:

| Action | Effect |
|---|---|
| `.continue` | Recurse into this entry if it's a directory |
| `.skip` | Don't recurse into this subtree (still visits the entry) |
| `.stop` | Abort the entire walk |

## WalkOptions

```zig
.{
    .max_depth = 3,         // root = 0; default = no cap
    .include_hidden = false, // skip names starting with '.'
    .follow_symlinks = false,// don't descend through symlinks
}
```

## Cancellation

`walk` is a CPU loop with reasonable bounded work per iteration — for
cancellation, check inside your visitor:

```zig
const CancelVisitor = struct {
    cancel: *volt.Cancel,

    pub fn visit(self: *@This(), entry: volt.fs.DirEntry, depth: u32) volt.fs.WalkAction {
        _ = entry; _ = depth;
        if (self.cancel.isFired()) return .stop;
        return .@"continue";
    }
};
```

## See also

- [`examples/dir_walk.zig`](https://github.com/NerdMeNot/blitz-io/tree/main/examples/dir_walk.zig)
- `volt.fs.glob` — Node-style pattern matching for one-shot lookups
