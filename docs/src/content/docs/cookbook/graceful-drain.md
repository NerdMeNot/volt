---
title: Graceful Drain on Shutdown
description: Stop accepting new work, let active work finish, exit cleanly. Built from a Notify and a join list.
---

The "right" shutdown semantic for a server: stop accepting new
work, let active work finish, then exit. Volt makes this clean
with a `Notify` for the shutdown signal and explicit join of
in-flight handlers.

`volt.scope` does almost what you want — it auto-fires a Cancel
on body error — but for a clean drain you usually want to **wait
for handlers, not cancel them.** So we manage the join list
manually.

## The pattern

```zig
const std = @import("std");
const volt = @import("volt");

const Server = struct {
    shutdown: volt.Notify,
    handlers: std.ArrayList(*volt.Task(void)),
    handlers_mu: volt.Mutex,
    allocator: std.mem.Allocator,
};

fn handle(conn: volt.net.TcpStream) void {
    var s = conn;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = s.read(&buf) catch return;
        if (n == 0) return;
        s.writeAll(buf[0..n]) catch return;
    }
}

fn serve(srv: *Server) !void {
    var listener = try volt.net.TcpListener.bind(.any4(8080));
    defer listener.close();
    std.debug.print("listening :8080\n", .{});

    // Spawn a watcher that closes the listener fd when shutdown fires.
    // Closing the fd makes any in-flight accept() error out, breaking
    // the accept loop.
    const watcher = try volt.spawn(struct {
        fn run(n: *volt.Notify, l: *volt.net.TcpListener) void {
            n.wait();
            l.close();   // ← in-flight accept() will fail
        }
    }.run, .{ &srv.shutdown, &listener });

    // Accept loop. Exits when listener.accept() errors (fd closed).
    while (true) {
        const conn = listener.accept() catch break;

        const t = try volt.spawn(handle, .{conn});
        srv.handlers_mu.lock();
        try srv.handlers.append(t);
        srv.handlers_mu.unlock();
    }

    std.debug.print("draining {d} handlers...\n", .{srv.handlers.items.len});

    // Wait for every in-flight handler to complete.
    // Each handler's read() will return 0 when its peer closes, or
    // error out — either way, the handler returns and `join`
    // observes it.
    for (srv.handlers.items) |t| t.join();

    watcher.join();   // the watcher already ran .wait() + close(); it's done

    std.debug.print("drained; bye\n", .{});
}

fn fireShutdownLater(srv: *Server) void {
    volt.sleep(2 * std.time.ns_per_s);
    std.debug.print("firing shutdown\n", .{});
    srv.shutdown.notifyAll();
}

fn root(srv: *Server) !void {
    // Demo: fire shutdown after 2 seconds.
    const sched_shutdown = try volt.spawn(fireShutdownLater, .{srv});

    try serve(srv);
    sched_shutdown.join();
}

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();

    var srv = Server{
        .shutdown = volt.Notify.init(),
        .handlers = std.ArrayList(*volt.Task(void)).init(std.heap.smp_allocator),
        .handlers_mu = volt.Mutex.init(),
        .allocator = std.heap.smp_allocator,
    };
    defer {
        srv.shutdown.deinit();
        srv.handlers.deinit();
        srv.handlers_mu.deinit();
    }

    try (try rt.run(root, .{&srv}));
}
```

## Why it works

**The Notify is the shutdown signal.** Any code in the program can
call `srv.shutdown.notifyAll()` — from a signal handler, from an
admin RPC, from a timer. The watcher coroutine is parked on
`srv.shutdown.wait()`; `notifyAll` wakes it.

**The watcher closes the listener fd.** When the listener is
closed mid-accept, the in-flight `accept()` syscall returns an
error. Our accept loop exits.

**The handlers list is the explicit drain set.** Every spawned
handler's Task is recorded. After the accept loop exits, we walk
the list and `join` each — which parks the driver coroutine until
each handler's read+writeAll loop terminates naturally (peer
close) or errors.

The handlers themselves don't see the shutdown — they keep
running their normal read/write loop. The peer can hang up;
or the read times out; or the handler errors. Whichever
happens, the handler returns, `join` observes it, drain
proceeds to the next.

## Why Cancel-and-cleanup isn't right here

`volt.scope` fires a Cancel on body error. That's the *abort*
shape, not the *drain* shape:

```zig
// This DOESN'T drain — it CANCELS:
try volt.scope(struct {
    fn body(c: *volt.Cancel) anyerror!void {
        // spawn handlers...
        // wait for shutdown signal...
        // body returns OK → scope cleans up via... what?
        // body returns error → scope fires Cancel
    }
}.body);
```

`scope`'s OK-path leaves Cancel un-fired — it assumes the body
already joined its children. If you spawn handlers without joining
them and return OK, you leak Tasks (and possibly the children
outlive the scope). If you return an error, Cancel fires, which
wakes handlers from any cancel-aware blocking op with
`error.Cancelled` — that's abort, not drain.

For drain, you want: stop new work → wait for existing → exit.
That's the explicit join-list pattern. `Cancel` and `scope` aren't
the right tool.

## Adding a max-drain time

If you want to bound the drain time (a buggy client shouldn't
keep the server alive forever), add a deadline:

```zig
fn serveWithDrainDeadline(srv: *Server, drain_ns: u64) !void {
    // ... accept loop as before ...

    std.debug.print("draining (up to {d}ms)...\n", .{drain_ns / std.time.ns_per_ms});

    // Race join-each-handler against a deadline.
    const deadline_watcher = try volt.spawn(struct {
        fn run(handlers: *std.ArrayList(*volt.Task(void)),
               mu: *volt.Mutex,
               ns: u64) void {
            volt.sleep(ns);
            // Force-close every handler's connection.
            // (Real code: keep the TcpStream pointers in the Server
            //  struct alongside the Tasks, close each here.)
            _ = handlers; _ = mu;
        }
    }.run, .{ &srv.handlers, &srv.handlers_mu, drain_ns });

    for (srv.handlers.items) |t| t.join();
    deadline_watcher.join();
}
```

After `drain_ns`, the watcher would close every in-flight
TcpStream, forcing any parked I/O to error and unblocking the
handlers.

## Variant: drain via Cancel-then-abort

If "abort after timeout" is acceptable rather than "force-close
fds", the timeout watcher can fire a Cancel that's held by every
handler:

```zig
// Each handler runs with a Cancel and uses cancel-aware variants:
fn handleCancelAware(conn: volt.net.TcpStream, c: *volt.Cancel) void {
    var s = conn;
    defer s.close();
    var buf: [4096]u8 = undefined;
    while (true) {
        // (Future: readCancel; for now, periodic c.checkpoint().)
        _ = c;
        const n = s.read(&buf) catch return;
        if (n == 0) return;
        s.writeAll(buf[0..n]) catch return;
    }
}
```

When the drain deadline fires, `c.fire()` would wake any
cancel-aware blocking op a handler is parked on. Today's I/O ops
aren't cancel-aware, so the immediate workaround is the
close-the-fd approach.

## Wiring to Ctrl-C

Volt's signal handler is internal (used for SIGSEGV stack-growth);
user-facing signal handling lives outside core. Wire `sigaction`
directly:

```zig
var sig_srv: *Server = undefined;   // set in main before sigaction

fn handleSigInt(_: c_int, _: ?*anyopaque, _: ?*anyopaque) callconv(.c) void {
    sig_srv.shutdown.notifyAll();
}

// In main(), before rt.run:
sig_srv = &srv;
var act: std.posix.Sigaction = .{
    .handler = .{ .sigaction = &handleSigInt },
    .mask = std.posix.empty_sigset,
    .flags = 0,
};
_ = std.posix.sigaction(std.posix.SIG.INT, &act, null);
```

The signal handler is async-signal-safe enough — `Notify.notifyAll`
calls into the parking lot, which takes a pthread mutex; not
strictly AS-safe by POSIX letter, but practically safe on Darwin
and Linux because the mutex isn't held by the interrupted code.
For a strict-AS-safe variant, write a flag from the handler and
have a coroutine poll-and-notify it (`signalfd` on Linux or
kqueue's `EVFILT_SIGNAL` on Darwin).

## See also

- [Echo server](/cookbook/echo-server/) — the unprotected version of this loop.
- [Structured Concurrency](/usage/structured-concurrency/) — why scope isn't the right tool for drain.
- [Sync primitives](/usage/sync/) — `Notify` and `Mutex` API.
