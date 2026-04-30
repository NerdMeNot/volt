---
title: Config Hot-Reload
description: Watch(T) for propagating config changes to every consumer without restart.
---

A common operational requirement: change a config value at runtime
(via SIGHUP, an admin endpoint, a file watcher) and have every
running coroutine pick up the new value within a tick.
`volt.channel.Watch(T)` makes this clean.

## The pattern

```zig
const std = @import("std");
const volt = @import("volt");

const Config = struct {
    log_level: u8,
    max_connections: u32,
    feature_flags: u64,
};

fn worker(name: []const u8, rx: *volt.channel.Watch(Config).Receiver) void {
    while (true) {
        // Snapshot the current config.
        const cfg = rx.current();
        std.debug.print("[{s}] running with log_level={d} max_conn={d}\n", .{
            name, cfg.log_level, cfg.max_connections,
        });
        // Do some work that uses cfg ...
        volt.sleep(volt.Duration.fromMillis(100)) catch return;
        // Pick up changes.
        rx.changed() catch return;
    }
}

fn reloader(w: *volt.channel.Watch(Config)) !void {
    var sigs = try volt.signal.SignalListener.init(blk: {
        var s = volt.signal.SignalSet.empty();
        s.add(.SIGHUP);
        break :blk s;
    });
    defer sigs.deinit();

    while (true) {
        _ = try sigs.wait();
        const new_cfg = loadConfig();   // your config loader
        w.send(new_cfg);
    }
}

fn loadConfig() Config {
    return .{ .log_level = 2, .max_connections = 1000, .feature_flags = 0 };
}

fn root() !void {
    const initial = loadConfig();
    var w = volt.channel.Watch(Config).init(initial);
    defer w.deinit();

    try volt.scope(struct {
        fn body(s: *volt.Scope) !void {
            const args = body_args.?;
            try s.spawn(reloader, .{args.w});
            var rx_a = args.w.subscribe();
            var rx_b = args.w.subscribe();
            try s.spawn(worker, .{ "A", &rx_a });
            try s.spawn(worker, .{ "B", &rx_b });
        }
    }.body);
}
```

## Why Watch and not Broadcast

- Workers always want the **latest** config, not history. If 3
  reloads fired before a worker checked, the worker only cares
  about the most recent one.
- Slow workers must not backpressure config updates. Watch silently
  overwrites; producers never block.
- `current()` returns a snapshot, so the worker can read it without
  worrying about it changing mid-use.

If your "config" was actually a stream of events that consumers
should never miss (audit log, financial trades), use `Broadcast(T)`
instead.

## Avoiding torn reads

`Watch(T).Receiver.current()` returns `T` by value, holding the
internal mutex for the copy. So if `T` is a struct, the whole
struct is read coherently — you can't see half of an old config
and half of a new one.

If `T` contains slices or pointers, `current()` returns the slices
unchanged — they still point at the producer's memory. The
producer should ensure those allocations outlive the receiver. The
typical pattern: produce a fresh `Config` each reload, hold the
old one until you're sure no receiver still has a copy in flight,
free old ones lazily.

## Detecting changes without polling

`worker` above busy-loops with a 100ms sleep. If you want to
**only** wake on config change (no periodic check):

```zig
fn worker(name: []const u8, rx: *volt.channel.Watch(Config).Receiver) !void {
    while (true) {
        try rx.changed();      // suspend until next config update
        const cfg = rx.current();
        applyConfig(cfg);
    }
}
```

`rx.changed()` parks the worker until `w.send(new_cfg)` fires.
`applyConfig` runs once per change, never re-runs on the same
config.

## Mixed: tick + change

Sometimes you want both — periodic work, AND immediate response to
config changes:

```zig
fn worker(rx: *volt.channel.Watch(Config).Receiver) !void {
    var tick = volt.Interval.start(volt.Duration.fromSecs(1));
    while (true) {
        // Race tick vs change. Without a unified select for both
        // (Watch isn't a Channel), poll one and check the other:
        try tick.tick();
        if (rx.hasChanged()) {
            const cfg = rx.current();
            applyConfig(cfg);
            rx.markSeen();
        }
        // ... do periodic work ...
    }
}
```

`hasChanged()` is non-blocking — true iff the config has updated
since the last `subscribe` / `changed` / `markSeen`. `markSeen()`
acks without parking.

## Closing

`w.close()` wakes every parked `changed()` with `error.Closed`.
Receivers can interpret that as "no more config updates coming;
shut down." Useful at runtime teardown so workers exit cleanly.
