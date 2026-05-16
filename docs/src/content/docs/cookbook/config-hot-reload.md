---
title: Config Hot-Reload
description: Watch(T) for propagating config changes to every consumer without restart. Latest-value semantics; slow consumers don't backpressure producers.
---

A common operational requirement: change a config value at runtime
(via SIGHUP, an admin endpoint, a file watcher) and have every
running coroutine pick up the new value within a tick.
`volt.Watch(T)` makes this clean.

## The pattern

```zig
const std = @import("std");
const volt = @import("volt");

const Config = struct {
    log_level: u8,
    max_connections: u32,
    feature_flags: u64,
};

const WorkerCtx = struct {
    name: []const u8,
    rx: volt.Watch(Config).Receiver,
};

fn worker(ctx: *WorkerCtx) void {
    // Initial config is whatever the Watch was created with.
    var current = ctx.rx.borrow();
    std.debug.print("[{s}] starting with log_level={d} max_conn={d}\n", .{
        ctx.name, current.log_level, current.max_connections,
    });

    while (true) {
        ctx.rx.changed() catch return;       // park until version advances
        current = ctx.rx.borrow();           // snapshot the new value
        std.debug.print("[{s}] config changed: log_level={d} max_conn={d}\n", .{
            ctx.name, current.log_level, current.max_connections,
        });
    }
}

fn reloader(w: *volt.Watch(Config)) void {
    var levels = [_]u8{ 0, 1, 2, 3 };
    for (levels) |level| {
        volt.sleep(500 * std.time.ns_per_ms);
        w.send(.{
            .log_level = level,
            .max_connections = 1000,
            .feature_flags = @as(u64, level) * 100,
        });
    }
    volt.sleep(500 * std.time.ns_per_ms);
    w.close();
}

fn root() !void {
    var w = volt.Watch(Config).init(.{
        .log_level = 0,
        .max_connections = 1000,
        .feature_flags = 0,
    });
    defer w.deinit();

    var ctx_a = WorkerCtx{ .name = "A", .rx = w.receiver() };
    var ctx_b = WorkerCtx{ .name = "B", .rx = w.receiver() };

    const t_a = try volt.spawn(worker, .{&ctx_a});
    const t_b = try volt.spawn(worker, .{&ctx_b});
    reloader(&w);

    t_a.join();
    t_b.join();
}

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}
```

## Why Watch and not Broadcast

- Workers always want the **latest** config, not history. If 3
  reloads fired before a worker checked, the worker only cares
  about the most recent one. Broadcast would deliver all 3.
- Slow workers must not backpressure config updates. Watch's
  `send` never blocks; Broadcast's `send` doesn't either, but
  Broadcast can return `error.Lagged` to consumers (you don't
  want that on config).
- `borrow()` returns a snapshot copy, so the worker can read it
  without worrying about it changing mid-use.

If your "config" was actually a stream of events that consumers
should never miss (audit log, financial trades), use `Broadcast`
instead.

## Reading the value

```zig
const cfg = ctx.rx.borrow();
// cfg is a copy of the current Config; safe to use.
```

`borrow()` reads the underlying seqlock and returns a coherent
snapshot. If a writer is mid-write, `borrow` spins on the
seqlock until the write completes, then reads. You won't see
half of an old config and half of a new one even for structs
with multiple fields.

If `Config` contains slices or pointers, `borrow()` returns the
slice headers unchanged — they still point at the producer's
memory. The producer should ensure those allocations outlive any
receiver that might be holding the snapshot. Typical pattern:
produce a fresh `Config` each reload, hold the old one until
all in-flight workers have moved past it, free old ones lazily.

## Detecting changes

`rx.changed()` parks until the Watch's version advances past the
receiver's cursor. Returns `error.Closed` when the Watch closes.

The receiver's cursor advances every time `changed()` returns OK.
If the producer fires 5 updates between the receiver's
`changed()` calls, the receiver sees 1 wake (latest-value
semantics).

## Watching a file

The reloader above just emits synthetic updates. For real config
hot-reload, watch a file with `kqueue`'s `EVFILT_VNODE` (Darwin)
or `inotify` (Linux). Both can be wired into a coroutine via
`std.posix.kqueue` directly; the Volt reactor doesn't expose a
generic VNODE registration today.

Sketch:

```zig
fn fileWatcher(path: []const u8, w: *volt.Watch(Config)) !void {
    // Open file, register kevent with EVFILT_VNODE | NOTE_WRITE.
    // On wake: reload config from disk; w.send(new_config).
    // (Implementation requires direct kqueue calls; out of scope here.)
}
```

For a quick demo, an `inotify`-style poller in a coroutine works:
`volt.sleep` for N ms, stat the file, compare mtime, reload if
changed.

## Closing

`w.close()` wakes every parked `changed()` call with
`error.Closed`. Receivers interpret that as "no more config
updates coming; shut down." Useful at runtime teardown so
workers exit cleanly.

## What's NOT here (today)

- `Watch.Receiver.hasChanged()` (non-blocking poll) — not in the
  current API. Workaround: spawn a coroutine that loops on
  `changed()` and updates a shared atomic, poll the atomic from
  the worker that wanted non-blocking.
- `Watch.Receiver.markSeen()` (acknowledge without waiting) — not
  in the current API.

These may land in a future Watch revision. For now, the
`changed()` + `borrow()` loop is the canonical shape.

## See also

- [Channels: Watch](/usage/channels/) — the API.
- [Channels internals](/architecture/channels-internals/) — how the seqlock works.
- [Pub/Sub](/cookbook/pub-sub/) — Broadcast for "every event seen" semantics.
