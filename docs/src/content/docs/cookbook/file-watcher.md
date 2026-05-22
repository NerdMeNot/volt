---
title: File watcher
description: Watch a directory for changes. volt.fs.Watcher polls (today) and will swap to inotify / FSEvents / RDC backends without API change.
---

`volt.fs.Watcher` blocks the calling coroutine until something
changes under a watched path. Today the backend is polling (every
`poll_interval`, default 200 ms); native inotify / FSEvents /
ReadDirectoryChangesW backends land as a follow-up without changing
this API.

```zig
const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try rt.runWithSignals(root, .{});
}

fn root(c: *volt.Cancel) !void {
    const allocator = std.heap.smp_allocator;
    var w = volt.fs.Watcher.init(allocator, .{
        .poll_interval = volt.Duration.fromMillis(100),
    });
    defer w.deinit();
    try w.watch("/tmp/myproject", .{ .recursive = true });

    while (true) {
        const event = w.nextCancel(c) catch |e| switch (e) {
            error.Cancelled => return,
            else => return e,
        };
        defer allocator.free(event.path);

        const kind: []const u8 = switch (event.kind) {
            .created => "+",
            .modified => "~",
            .removed => "-",
        };
        std.debug.print("{s} {s}\n", .{ kind, event.path });
    }
}
```

## Event kinds

| Kind | When |
|---|---|
| `.created` | A new entry appeared at the path |
| `.modified` | An existing entry's content/size/mtime changed |
| `.removed` | A previously-seen entry no longer exists |

The current poll-diff backend doesn't surface `.renamed` — a rename
fires as `.removed` + `.created`. Native backends will distinguish.

## Trade-offs of the polling backend

- **Resolution = `poll_interval`**. Very short-lived changes
  (created + deleted within one interval) may be missed.
- **CPU cost** ≈ one stat per watched entry per interval. Fine for
  hundreds; if you need thousands, throttle `poll_interval` or wait
  for the native backends.
- **Cross-platform identical** — no per-OS quirks to debug.

## Pull-mode drain

For caller-driven loops:

```zig
const events = try w.drain(allocator, 16); // non-blocking, up to 16
for (events) |e| { ... allocator.free(e.path); }
allocator.free(events);
```

## See also

- [`examples/file_watcher.zig`](https://github.com/NerdMeNot/blitz-io/tree/main/examples/file_watcher.zig)
- [Config hot-reload recipe](/cookbook/config-hot-reload/) — pairs nicely with Watcher
