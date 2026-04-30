---
title: Signals and Shutdown
description: volt.signal — async-aware signal handling and graceful shutdown patterns.
---

## Signal listening

```zig
var sigs = try volt.signal.SignalListener.init(blk: {
    var set = volt.signal.SignalSet.empty();
    set.add(.SIGINT);
    set.add(.SIGTERM);
    set.add(.SIGUSR1);
    break :blk set;
});
defer sigs.deinit();

const fired = try sigs.wait();   // suspends; returns the SignalSet that fired
if (fired.contains(.SIGUSR1)) {
    // handle SIGUSR1
}
```

`SignalListener` builds on the kernel's `signalfd` (Linux) or
`EVFILT_SIGNAL` (Darwin/BSD). It gives you a coroutine-friendly
interface: `wait()` suspends until a signal in the set arrives.

Common subsets have shortcuts:

```zig
var s = try volt.signal.shutdown();   // SIGINT + SIGTERM
defer s.deinit();

var c = try volt.signal.ctrlC();      // SIGINT only
defer c.deinit();
```

`SignalListener.handler.read()` is non-blocking — returns the
fired SignalSet or `error.WouldBlock`. Useful when you want to
poll for shutdown without a dedicated coroutine.

## Graceful shutdown pattern

The canonical TCP server with graceful shutdown:

```zig
fn serve() !void {
    var listener = try volt.io.TcpListener.bind(volt.io.Address.any4(8080));
    defer listener.close();

    var shutdown = try volt.signal.shutdown();
    defer shutdown.deinit();

    while (true) {
        // Non-blocking shutdown check at the top of every iteration.
        if (shutdown.handler.read()) |_| {
            std.debug.print("shutdown signal received; draining\n", .{});
            return;
        } else |_| {}

        const conn = listener.accept() catch |err| switch (err) {
            error.Cancelled => return,
            else => return err,
        };
        _ = try volt.launch(handle, .{conn});
    }
}
```

When `volt.run` returns from `serve`, the runtime tears down the
worker pool. Coroutines spawned via `volt.launch` (the per-connection
handlers) get cancelled. Their current parks (on `read`, `writeAll`,
etc.) surface `error.Cancelled` and they unwind cleanly.

## Drain-then-exit pattern

If you want spawned handlers to **finish their current request** but
not accept new ones, wrap the spawn region in a `volt.scope`:

```zig
fn serve() !void {
    var listener = try volt.io.TcpListener.bind(volt.io.Address.any4(8080));
    defer listener.close();

    try volt.scope(struct {
        fn body(s: *volt.Scope) !void {
            var shutdown = try volt.signal.shutdown();
            defer shutdown.deinit();

            while (true) {
                if (shutdown.handler.read()) |_| return else |_| {}
                const conn = listener.accept() catch |err| switch (err) {
                    error.Cancelled => return,
                    else => return err,
                };
                try s.spawn(handle, .{conn});
            }
        }
    }.body);
    // After the scope returns: every active handler has finished
    // (or been cancelled and joined). The accept loop is gone.
}
```

The scope joins every handler before returning. Existing connections
finish their work; new ones don't get accepted because the loop has
exited. This is the "graceful drain" semantic without manual
WorkGuard tracking.

## Per-coroutine signal handling

`SignalListener` is a per-coroutine handle, not a process-global
hook. You can have multiple listeners with different signal sets in
different coroutines, and each gets its own signalfd. They don't
interfere.

The kernel's signal mask is set once per process. Volt blocks the
listened-for signals from interrupting normal code paths so
`signalfd` is the only way they're delivered. This is correct
behavior for an async-aware program; the signals don't go anywhere
else.

## What about real-time signals?

`SignalSet` covers the standard POSIX signals (`SIGINT`, `SIGTERM`,
`SIGHUP`, `SIGUSR1`, `SIGUSR2`, etc.). Real-time signals
(`SIGRTMIN..SIGRTMAX`) aren't exposed in v1.0. If you need them for
a specific use case, file an issue with the use case.

## Windows

`volt.signal` is currently POSIX-only. The Windows equivalent
(`SetConsoleCtrlHandler` + named events for service signals) ships
with the broader Windows runtime port — see the platform-status
note in the [introduction](/).

## Patterns to avoid

```zig
// DON'T install a regular signal handler with signal()/sigaction()
// alongside SignalListener — Volt's listener takes ownership of the
// signal mask and you'll miss deliveries.
std.posix.sigaction(...);  // ← BAD

// DON'T expect SignalListener to fire from an OS thread that's not
// a Volt worker. The wait() call requires a coroutine context.
```

Both of these will appear to work in casual testing and break in
hard-to-reproduce ways under load. Use `SignalListener` exclusively
inside Volt; if you need to bridge to a non-Volt thread, send the
signal info through a `Channel` to a coroutine that can react.
