//! # Volt — A stackful coroutine runtime for Zig
//!
//! Status: v0.1 — bootstrap + minimal scheduler. Public API:
//!   - volt.run(allocator, fn, args)  — entry point; runs root coroutine
//!   - volt.launch(fn, args)            — fire-and-forget; returns *Job
//!   - volt.spawn(fn, args)             — value-returning; returns *Task(T)
//!   - volt.yield()                     — explicit reschedule + cancel check
//!   - Job/Task: cancel(), isActive(), isCompleted(), join()
//!
//! Single-threaded scheduler in v0.1; multi-worker work-stealing in v0.9.
//! No I/O integration yet (v0.2). No channels (v0.3). No structured
//! concurrency (v0.5). See `docs/design/api-design.md` for the full roadmap.

const std = @import("std");

// ─────────────────────────────────────────────────────────────────────────────
// Public API — the user-facing surface
// ─────────────────────────────────────────────────────────────────────────────

pub const run = @import("api/run.zig").run;
pub const launch = @import("api/launch.zig").launch;
pub const destroyJob = @import("api/launch.zig").destroyJob;
pub const spawn = @import("api/spawn.zig").spawn;
pub const destroyTask = @import("api/spawn.zig").destroyTask;
pub const yield = @import("api/yield.zig").yield;

pub const Runtime = @import("runtime.zig").Runtime;
pub const Config = @import("runtime.zig").Config;
pub const Job = @import("task/job.zig").Job;
pub const Task = @import("task/task.zig").Task;

/// The currently-active runtime, if any. Returns null if not inside `volt.run`.
pub const currentRuntime = @import("runtime.zig").currentRuntime;

// ─────────────────────────────────────────────────────────────────────────────
// Time primitives — model-agnostic types
// ─────────────────────────────────────────────────────────────────────────────

pub const time = @import("time.zig");
pub const Duration = time.Duration;
pub const Instant = time.Instant;

// ─────────────────────────────────────────────────────────────────────────────
// Internal building blocks — exposed for benchmarks/tests, not for users
// ─────────────────────────────────────────────────────────────────────────────

pub const coroutine = struct {
    pub const Coroutine = @import("coroutine/coroutine.zig").Coroutine;
    pub const State = @import("coroutine/coroutine.zig").State;
    pub const ClosureBase = @import("coroutine/coroutine.zig").ClosureBase;
    pub const stack = @import("coroutine/stack.zig");
    pub const context = @import("coroutine/context_arm64.zig");
    pub const spawn_helper = @import("coroutine/spawn.zig");
};

pub const scheduler = struct {
    pub const tls = @import("scheduler/tls.zig");
    pub const Worker = @import("scheduler/worker.zig").Worker;
    pub const Injection = @import("scheduler/injection.zig").Injection;
    pub const park = @import("scheduler/park.zig");
    pub const Deque = @import("scheduler/deque.zig").Deque;
};

pub const io = struct {
    pub const Reactor = @import("io/reactor.zig").Reactor;
    pub const EventKind = @import("io/reactor.zig").EventKind;

    pub const waitReadable = @import("io/wait.zig").waitReadable;
    pub const waitWritable = @import("io/wait.zig").waitWritable;

    pub const read = @import("io/io.zig").read;
    pub const write = @import("io/io.zig").write;
    pub const writeAll = @import("io/io.zig").writeAll;
    pub const setNonblock = @import("io/io.zig").setNonblock;

    pub const TcpListener = @import("io/net.zig").TcpListener;
    pub const TcpStream = @import("io/net.zig").TcpStream;
    pub const Address = @import("io/net.zig").Address;
};

pub const internal = struct {
    pub const thread = @import("internal/thread.zig");
    pub const syscall = @import("internal/syscall.zig");
    pub const util = struct {
        pub const linked_list = @import("internal/util/linked_list.zig");
        pub const slab = @import("internal/util/slab.zig");
        pub const pool = @import("internal/util/pool.zig");
        pub const stack_guard = @import("internal/util/stack_guard.zig");
        pub const cacheline = @import("internal/util/cacheline.zig");
        pub const bit = @import("internal/util/bit.zig");
        pub const invocation_id = @import("internal/util/invocation_id.zig");
        pub const signal = @import("internal/util/signal.zig");
    };
};

// ─────────────────────────────────────────────────────────────────────────────
// Version
// ─────────────────────────────────────────────────────────────────────────────

pub const version = struct {
    pub const major = 0;
    pub const minor = 1;
    pub const patch = 0;
    pub const string = "0.1.0-zig0.16.0";
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pull in inline tests from every module so `zig build test` covers
// the whole tree.
// ─────────────────────────────────────────────────────────────────────────────

test {
    _ = time;
    _ = coroutine.Coroutine;
    _ = coroutine.stack;
    _ = coroutine.context;
    _ = coroutine.spawn_helper;
    _ = scheduler.tls;
    _ = scheduler.Worker;
    _ = scheduler.Injection;
    _ = scheduler.park;
    _ = @import("scheduler/deque.zig");
    _ = @import("scheduler/injection.zig");
    _ = io.Reactor;
    _ = @import("io/wait.zig");
    _ = @import("io/io.zig");
    _ = @import("io/net.zig");
    _ = Runtime;
    _ = Job;
    _ = @import("task/task.zig");
    _ = @import("api/yield.zig");
    _ = @import("api/launch.zig");
    _ = @import("api/spawn.zig");
    _ = @import("api/run.zig");
    _ = @import("test/integration_test.zig");
    _ = @import("test/io_integration_test.zig");
    _ = @import("test/tcp_integration_test.zig");
    _ = @import("test/multi_worker_test.zig");
    _ = internal.thread;
    _ = internal.syscall;
    _ = internal.util.linked_list;
    _ = internal.util.slab;
    _ = internal.util.pool;
    _ = internal.util.stack_guard;
    _ = internal.util.cacheline;
    _ = internal.util.bit;
    _ = internal.util.invocation_id;
    _ = internal.util.signal;
}
