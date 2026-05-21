//! Volt — stackful coroutine runtime for Zig.
//!
//! Multi-threaded by design: N OS threads each run a Worker dispatch
//! loop. Coroutines are scheduled on per-worker FIFO queues with
//! work-stealing, plus a per-P mailbox for cross-thread pushes.
//! Single-worker (workers = 1) is a configuration, not a special
//! code path; the same fences and queues run in both modes.
//!
//! Headline pieces:
//!   * AAPCS64 wide-save context switch
//!   * `__ulock_wait` (Darwin) / `futex` (Linux planned) Parker
//!   * Per-worker fixed-256 work-stealing queue (overflow → mailbox)
//!   * Single-slot LIFO cache on each Worker for spawn-chain locality
//!   * Comptime-specialized channels (`Spsc`, `Mpmc`, `Oneshot`,
//!     `Watch`, `Broadcast`)
//!   * kqueue reactor with single-poller-claim and inline poll
//!   * Parking-lot wait/wake (one mechanism for every sync primitive)
//!   * Grow-on-demand stacks with PROT_NONE guard pages
//!
//! ## Quick start
//!
//! ```zig
//! var rt = try volt.Runtime.init(.{ .allocator = gpa });
//! defer rt.deinit();
//! const result = try rt.run(myFn, .{ arg1, arg2 });
//! ```
//!
//! Inside a coroutine, use `volt.spawn(fn, args)` (which infers the
//! runtime) instead of `rt.spawn(fn, args)`. The returned `*Task(T)`
//! gives you `.join()` for the result.

// ─── Public namespaces ───────────────────────────────────────────────

pub const channel = @import("channel.zig");
pub const sync = @import("sync.zig");
pub const net = @import("net.zig");

/// Categorical I/O error vocabulary. Every `readAsync` / `writeAsync`
/// / `readFull` / `writeAll` call returns an `IoError`; the
/// reactor's wait primitives return a subset (`ReactorWaitError`).
/// Library authors `catch err switch (err) { ... }` against these.
pub const IoError = @import("reactor.zig").IoError;
pub const ReactorWaitError = @import("reactor.zig").ReactorWaitError;
pub const ReactorSetupError = @import("reactor.zig").ReactorSetupError;

/// Typed time vocabulary — `Duration` (a length of time) and
/// `Instant` (a point on the monotonic clock). Library APIs that
/// take timeouts / deadlines should accept `Duration`; the raw
/// `u64` nanosecond form is still available via `sleepNanos` for
/// bench loops that need the unboxed integer.
pub const time = @import("time.zig");
pub const Duration = time.Duration;
pub const Instant = time.Instant;

/// Threadlocal "current coroutine" lookup.
pub const current = @import("current.zig");

/// Parking-lot — coroutine wait/wake by address. For advanced users
/// implementing custom sync primitives. The built-in sync types
/// (`Mutex`, `Notify`, `Semaphore`) and channels already use this.
/// See `docs/internals/parking-lot.md`.
pub const parking = @import("park.zig");

/// Test-allocator namespace. `volt.testing.allocator` is a leak-
/// detecting allocator that's safe under multi-worker Runtimes
/// (unlike `std.testing.allocator`, whose stack-trace capture races
/// with worker stack writes). Use this for any test that constructs
/// a Runtime with `workers >= 2`.
pub const testing = @import("testing.zig");

// ─── Bootstrap & types ───────────────────────────────────────────────

pub const Runtime = @import("runtime.zig").Runtime;
pub const Config = @import("runtime.zig").Config;
pub const Task = @import("task.zig").Task;
pub const DetachedHandle = @import("runtime.zig").DetachedHandle;
pub const MAX_WORKERS = @import("runtime.zig").MAX_WORKERS;
pub const SpawnError = @import("runtime.zig").SpawnError;
// `runWithSignals` is a method on `Runtime` — see runtime.zig.

// ─── Inside-coroutine helpers ────────────────────────────────────────

/// Return the `Runtime` driving the current coroutine. Panics if
/// called outside one. Replaces the
/// `@ptrCast(@alignCast(volt.current.require().runtime))` ritual.
pub fn runtime() *Runtime {
    return @ptrCast(@alignCast(current.require().runtime));
}

/// Spawn a child coroutine on the running coroutine's runtime.
/// Returns `*Task(T)` whose `.join()` parks until the child completes.
/// **Must be called from inside a coroutine** — panics otherwise.
///
/// The error set is `SpawnError` (heap exhaustion + stack-arena
/// exhaustion) — explicit so callers and helpers like `scope`/
/// `withTimeout` can compose typed unions.
pub fn spawn(
    comptime user_fn: anytype,
    args: anytype,
) SpawnError!*Task(@typeInfo(@TypeOf(user_fn)).@"fn".return_type.?) {
    comptime {
        const FnInfo = @typeInfo(@TypeOf(user_fn));
        if (FnInfo != .@"fn") @compileError(
            "spawn's first argument must be a function; got " ++ @typeName(@TypeOf(user_fn)),
        );
        const ArgsT = @TypeOf(args);
        const ArgsInfo = @typeInfo(ArgsT);
        if (ArgsInfo != .@"struct" or !ArgsInfo.@"struct".is_tuple) @compileError(
            "spawn's second argument must be a tuple of args; got " ++ @typeName(ArgsT) ++
                " — wrap your args in .{...}",
        );
        const expected = FnInfo.@"fn".params.len;
        const got = ArgsInfo.@"struct".fields.len;
        if (expected != got) @compileError(std.fmt.comptimePrint(
            "spawn arg count mismatch: function expects {d} args, got {d}",
            .{ expected, got },
        ));
    }
    return runtime().spawn(user_fn, args);
}

/// Run `user_fn(args)` on the runtime's OS-thread pool. Parks the
/// calling coroutine until the function returns; the result is the
/// function's return value. Use this for sync / CPU-bound work that
/// would otherwise pin a worker (long syscalls, blocking C library
/// calls, gzip on a megabyte). Same shape as Tokio's
/// `spawn_blocking().await`.
///
/// **Must be called from inside a coroutine** — parks via the
/// parking-lot.
///
/// Backpressure: if every pool thread is busy and the pool is at
/// `Runtime.Config.blocking.max_threads`, the job queues. No error.
/// If you need to bound concurrent sync work, wrap calls in a
/// `Semaphore.acquire` at the boundary.
///
/// Errors: `error.SystemResources` if the pool needed to spawn a new
/// OS thread and the kernel refused (RLIMIT_NPROC, ENOMEM).
/// `error.PoolShuttingDown` if `Runtime.deinit` already started.
pub fn spawnBlocking(
    comptime user_fn: anytype,
    args: anytype,
) !@typeInfo(@TypeOf(user_fn)).@"fn".return_type.? {
    const Ret = @typeInfo(@TypeOf(user_fn)).@"fn".return_type.?;
    const Args = @TypeOf(args);

    const Closure = struct {
        rt: *Runtime,
        args: Args,
        result: Ret = undefined,
        done: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

        fn run(opaque_ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(opaque_ptr));
            self.result = @call(.auto, user_fn, self.args);
            self.done.store(1, .release);
            // Wake the parked coroutine. unparkOne is thread-safe;
            // the pool thread isn't the coroutine's owning worker.
            _ = parking.unparkOne(self.rt, &self.done);
        }
    };

    const rt = runtime();
    var closure = Closure{ .rt = rt, .args = args };
    var job = @import("blocking_pool.zig").Job{
        .run_fn = &Closure.run,
        .arg_ptr = &closure,
    };
    try rt.blocking_pool.submit(&job);

    // Park until the pool thread sets done. Validator rechecks under
    // the parking-lot bucket lock — if the pool thread happened to
    // complete + signal between our submit and our parkOn, the
    // validator returns false and parkOn returns immediately without
    // suspending.
    parking.parkOn(&closure.done, struct {
        fn validator(addr: *const anyopaque) bool {
            const done: *const std.atomic.Value(u32) = @ptrCast(@alignCast(addr));
            return done.load(.acquire) == 0;
        }
    }.validator);

    return closure.result;
}

/// Wait for every task in `tasks` to complete and return a struct
/// of their results. `tasks` is a struct literal of `*Task(T)` values
/// — fields may have different payload types; the returned struct's
/// fields carry the same names with the corresponding payloads.
///
/// ```zig
/// const t_a = try volt.spawn(fetchUser,  .{user_id});  // *Task(User)
/// const t_b = try volt.spawn(fetchPrefs, .{user_id});  // *Task(Prefs)
/// const r = volt.joinAll(.{ .user = t_a, .prefs = t_b });
/// // r.user: User, r.prefs: Prefs
/// ```
///
/// Zero allocation, comptime-generated heterogeneous struct. For tasks
/// whose body returns `E!T`, the corresponding result field is `E!T`
/// and the caller unwraps with `try`. Each task is consumed by its
/// `.join()` (frame freed); don't reuse handles after.
///
/// **Must be called from inside a coroutine** (uses `Task.join`).
pub fn joinAll(tasks: anytype) JoinAllResult(@TypeOf(tasks)) {
    var results: JoinAllResult(@TypeOf(tasks)) = undefined;
    inline for (@typeInfo(@TypeOf(tasks)).@"struct".fields) |f| {
        @field(results, f.name) = @field(tasks, f.name).join();
    }
    return results;
}

fn JoinAllResult(comptime TasksT: type) type {
    const fields = @typeInfo(TasksT).@"struct".fields;
    comptime var names: [fields.len][]const u8 = undefined;
    comptime var types: [fields.len]type = undefined;
    comptime var attrs: [fields.len]std.builtin.Type.StructField.Attributes = undefined;
    inline for (fields, 0..) |f, i| {
        names[i] = f.name;
        // f.type is `*Task(P)` — pluck the public `Payload` const off
        // the pointed-to Task to get P. Errors and all.
        types[i] = @typeInfo(f.type).pointer.child.Payload;
        attrs[i] = .{};
    }
    return @Struct(.auto, null, &names, &types, &attrs);
}

/// Wait for the FIRST task in `tasks` to complete; cancel the rest
/// and return a tagged union variant indicating which won + its
/// result. Mirrors `select`'s mental model — same field-named shape,
/// same "first wins" semantic — but races task *completions* instead
/// of channel/timer ops.
///
/// ```zig
/// var c = volt.Cancel.init(rt);
/// defer c.deinit();
/// const t_a = try volt.spawn(fetchPrimary,   .{&c, args});
/// const t_b = try volt.spawn(fetchFallback,  .{&c, args});
/// const winner = volt.joinFirst(&c, .{ .primary = t_a, .fallback = t_b });
/// switch (winner) {
///     .primary  => |r| return try r,
///     .fallback => |r| return try r,
/// }
/// ```
///
/// **Caller contract**: every task must share `c` and propagate it
/// through cancel-aware blocking calls (`sleepCancel`, `recvCancel`,
/// `Mutex.lockCancel`, etc.). When the first task wins, `c` fires
/// and the losers wake with `error.Cancelled`. Tasks that ignore the
/// cancel still complete naturally — `joinFirst` waits for all of
/// them to settle before returning. If a task ignores `c` and runs
/// forever, `joinFirst` runs forever; that's a caller bug.
///
/// **Must be called from inside a coroutine** (uses `spawn` + `join`).
pub fn joinFirst(
    c: *Cancel,
    tasks: anytype,
) SpawnError!JoinFirstResult(@TypeOf(tasks)) {
    const TasksT = @TypeOf(tasks);
    const fields = @typeInfo(TasksT).@"struct".fields;
    if (comptime fields.len == 0) @compileError("joinFirst needs at least one task");

    const race = @import("race.zig");
    const SlotsT = JoinFirstSlots(TasksT);
    var slots: SlotsT = undefined;
    var winner = race.WinnerSlot(fields.len){};

    // Spawn one watcher per task. Watcher joins its target, then
    // CAS-claims the winner slot and fires the shared Cancel — the
    // losers' cancel-aware blocking calls wake with error.Cancelled,
    // their bodies return, their tasks complete, their watchers
    // observe done and exit.
    var watchers: [fields.len]*Task(void) = undefined;
    inline for (fields, 0..) |f, i| {
        const target = @field(tasks, f.name);
        const Watcher = struct {
            target: f.type,
            slots: *SlotsT,
            winner: *race.WinnerSlot(fields.len),
            cancel: *Cancel,

            fn entry(self: *@This()) void {
                const r = self.target.join();
                if (self.winner.claim(i)) {
                    @field(self.slots, f.name) = r;
                    self.cancel.fire();
                }
            }
        };
        var ctx = Watcher{
            .target = target,
            .slots = &slots,
            .winner = &winner,
            .cancel = c,
        };
        watchers[i] = try spawn(Watcher.entry, .{&ctx});
    }

    // Wait for all watchers. The winner already fired the cancel,
    // so the losers' tasks settle and their watchers exit promptly.
    for (watchers) |w| _ = w.join();

    const winning_idx = winner.winner() orelse unreachable;
    var result: JoinFirstResult(TasksT) = undefined;
    inline for (fields, 0..) |f, i| {
        if (winning_idx == i) {
            result = @unionInit(JoinFirstResult(TasksT), f.name, @field(slots, f.name));
        }
    }
    return result;
}

fn JoinFirstSlots(comptime TasksT: type) type {
    // Heterogeneous slots — same shape as JoinAllResult, but each
    // field is initialised only when its task is the winner. The
    // post-switch only reads the winning slot.
    return JoinAllResult(TasksT);
}

fn JoinFirstResult(comptime TasksT: type) type {
    const fields = @typeInfo(TasksT).@"struct".fields;
    comptime var names: [fields.len][]const u8 = undefined;
    comptime var tag_vals: [fields.len]std.math.IntFittingRange(0, fields.len) = undefined;
    comptime var union_types: [fields.len]type = undefined;
    comptime var union_attrs: [fields.len]std.builtin.Type.UnionField.Attributes = undefined;
    inline for (fields, 0..) |f, i| {
        names[i] = f.name;
        tag_vals[i] = @intCast(i);
        union_types[i] = @typeInfo(f.type).pointer.child.Payload;
        union_attrs[i] = .{};
    }
    const Tag = @Enum(
        std.math.IntFittingRange(0, fields.len),
        .exhaustive,
        &names,
        &tag_vals,
    );
    return @Union(.auto, Tag, &names, &union_types, &union_attrs);
}

/// Run `f` over every element of `items` in parallel; write each
/// result into the matching slot of `results`. Returns when every
/// element has been processed.
///
/// ```zig
/// const inputs = [_]u32{ 1, 2, 3, 4, 5 };
/// var outputs: [5]u64 = undefined;
/// try volt.collect(&inputs, doubleU64, &outputs);
/// // outputs == [_]u64{ 2, 4, 6, 8, 10 }, all computed in parallel
/// ```
///
/// `items` and `results` must be slices / arrays of the same length;
/// `f` must have shape `fn(ItemT) ResultT`. Spawns one coroutine per
/// item (cheap — pool-allocated Coroutine + Frame). Allocates a
/// single `[N]*Task(void)` buffer on the runtime's allocator to
/// track the spawned coros; the per-task closure rides in the
/// Frame's args (no separate Ctx allocation).
///
/// For `f` shapes that can fail (`fn(ItemT) E!ResultT`), have
/// `ResultT` itself be an error union: each slot carries the per-
/// item result-or-error, and the caller unwraps with `try`.
///
/// **Must be called from inside a coroutine** (uses `spawn`).
pub fn collect(
    items: anytype,
    comptime f: anytype,
    results: anytype,
) (SpawnError || std.mem.Allocator.Error)!void {
    std.debug.assert(items.len == results.len);
    const ItemT = std.meta.Elem(@TypeOf(items));
    const ResultT = std.meta.Elem(@TypeOf(results));

    const Ctx = struct {
        item: ItemT,
        slot: *ResultT,

        // Args are copied into each Task's Frame by `spawn`, so a
        // `ctx` value on the loop's stack is stable for that single
        // spawn call — no aliasing issue when the loop variable
        // is reused next iteration.
        fn run(ctx: @This()) void {
            ctx.slot.* = f(ctx.item);
        }
    };

    const rt = runtime();
    const tasks = try rt.allocator.alloc(*Task(void), items.len);
    defer rt.allocator.free(tasks);

    for (items, results, 0..) |item, *slot, i| {
        const ctx = Ctx{ .item = item, .slot = slot };
        tasks[i] = try spawn(Ctx.run, .{ctx});
    }
    for (tasks) |t| _ = t.join();
}

/// Cooperative yield. Re-queues the current coroutine to the running
/// worker's tail (FIFO — yields don't bounce via lifo_slot). Must be
/// called from inside a coroutine.
pub const yield = @import("runtime.zig").yield;

/// Sleep for at least `d`. Routes through the reactor's timer
/// (`EVFILT_TIMER` on kqueue, `timerfd` on epoll,
/// `IORING_OP_TIMEOUT` on io_uring, `CreateThreadpoolTimer` on
/// IOCP) — uses zero worker-thread time, just like a parked socket
/// read. Must be called from inside a coroutine.
///
/// Returns `ReactorWaitError` if the underlying timer registration
/// fails (e.g., kernel out of resources). For normal kernels this
/// effectively never errors.
///
/// Kernel timer resolution is bounded below by ~1 µs on Darwin arm64;
/// shorter sleeps round up.
pub fn sleep(d: Duration) ReactorWaitError!void {
    const rt = runtime();
    return rt.reactor.waitTimer(d.ns);
}

/// Raw-nanosecond variant of `sleep`. Use this only for bench
/// loops where the `Duration` constructor's instructions show up
/// in measurement noise; library code should prefer `sleep`.
pub fn sleepNanos(ns: u64) ReactorWaitError!void {
    const rt = runtime();
    return rt.reactor.waitTimer(ns);
}

/// Cancel-aware variant of `sleep`. Returns `error.Cancelled` if
/// `c` fires before the duration elapses; the kernel-side timer
/// is deregistered eagerly so the resource isn't held past the
/// cancel.
pub fn sleepCancel(d: Duration, c: *Cancel) (ReactorWaitError || cancel.Error)!void {
    const rt = runtime();
    return rt.reactor.waitTimerCancel(d.ns, c);
}

/// Run `body` with a fresh `*Cancel`. If `body` returns an error,
/// the Cancel fires automatically — every coroutine that was given
/// `&cancel` and is mid-blocking-op wakes with `error.Cancelled`.
/// If `body` returns OK, the Cancel does NOT fire (the spawned
/// children, if any, must have completed already — `scope` does
/// not auto-await; that's the body's responsibility).
///
/// This is the minimum-viable "structured concurrency" primitive
/// for v1: it ties cancel lifetime to a lexical scope so errors
/// propagate without manual `defer cancel.fire()` boilerplate.
/// Auto-await of child tasks is library territory and may land in
/// a v1.x extension.
///
/// Return type mirrors `body`'s — if `body` returns `E!void`,
/// `scope` returns `E!void`. No `anyerror` widening.
///
/// Must be called from inside a coroutine (needs `runtime()`).
pub fn scope(comptime body: anytype) ScopeReturn(@TypeOf(body)) {
    var c = Cancel.init(runtime());
    defer c.deinit();
    body(&c) catch |e| {
        c.fire();
        return e;
    };
}

/// `scope`'s return type — preserves body's error set, no widening.
/// If body returns `E!void`, returns `E!void`. If body's return type
/// has no error union (rare; body is infallible), returns it as-is.
fn ScopeReturn(comptime BodyT: type) type {
    const ret = @typeInfo(BodyT).@"fn".return_type.?;
    const info = @typeInfo(ret);
    if (info == .error_union) {
        return info.error_union.error_set!void;
    }
    return ret;
}

/// Race `body(args..., *Cancel)` against `d`. Returns `body`'s
/// result if it completes first; returns `error.Timeout` if the
/// duration elapses first.
///
/// **Body shape**: the body must accept `*Cancel` as its last
/// argument and propagate it through cancel-aware blocking calls
/// (`sleepCancel`, `Mutex.lockCancel`, `Spsc.recvCancel`, etc.).
/// Bodies that ignore the cancel will still race the timer, but
/// won't be interrupted mid-blocking-op.
///
/// **Mechanism**: spawn a timer-firer child coroutine that calls
/// `sleepCancel(d, &c)`. If the sleep returns naturally (timer
/// expired), the firer CAS-claims the "winner = timer" slot and
/// calls `c.fire()`. If the body returns first, it CAS-claims
/// "winner = body" and calls `c.fire()` — the firer's sleepCancel
/// returns `error.Cancelled` and it exits cleanly.
///
/// The CAS arbitration is what determines the outcome: whichever
/// side flips the winner slot first wins the race, even if the
/// other side completes a few cycles later.
///
/// No background threads — the timer-firer is a coroutine on the
/// existing work-stealing fabric.
pub fn withTimeout(
    d: Duration,
    comptime body: anytype,
    args: anytype,
) WithTimeoutReturn(@TypeOf(body)) {
    // Two racers — body (idx 0) and timer (idx 1).
    const race = @import("race.zig");

    const Ctx = struct {
        cancel: *Cancel,
        dur: Duration,
        winner: *race.WinnerSlot(2),

        fn timerFirer(self: *@This()) void {
            sleepCancel(self.dur, self.cancel) catch return;
            // Slept the full duration — claim the win, fire the cancel.
            if (self.winner.claim(1)) self.cancel.fire();
        }
    };

    var c = Cancel.init(runtime());
    defer c.deinit();

    var winner = race.WinnerSlot(2){};
    var ctx = Ctx{ .cancel = &c, .dur = d, .winner = &winner };

    var firer = try spawn(Ctx.timerFirer, .{&ctx});

    const args_with_cancel = args ++ .{&c};
    const result = @call(.auto, body, args_with_cancel);

    const body_won = winner.claim(0);
    c.fire(); // wake the firer if it's still sleeping; no-op otherwise.
    _ = firer.join();

    if (!body_won) return error.Timeout;
    return result;
}

/// `withTimeout`'s return type — body's payload, with the typed
/// union `BodyError || SpawnError || error{Timeout}` on the error
/// side. No `anyerror` widening; callers see exactly which errors
/// can surface and can `catch err switch` exhaustively.
///
/// `SpawnError` enters because the timer-firer child is spawned;
/// `error.Timeout` enters because the duration may elapse before
/// the body completes; the body's own error set enters because
/// the body's error propagates unchanged on the non-timeout path.
fn WithTimeoutReturn(comptime BodyT: type) type {
    const fn_info = @typeInfo(BodyT).@"fn";
    const inner = fn_info.return_type.?;
    const inner_info = @typeInfo(inner);
    const Payload = if (inner_info == .error_union) inner_info.error_union.payload else inner;
    const BodyError = if (inner_info == .error_union) inner_info.error_union.error_set else error{};
    return (BodyError || SpawnError || error{Timeout})!Payload;
}

// ─── Channel & sync convenience re-exports ───────────────────────────

pub const Spsc = channel.Spsc;
pub const Mpmc = channel.Mpmc;
pub const Oneshot = channel.Oneshot;
pub const Watch = channel.Watch;
pub const Broadcast = channel.Broadcast;

pub const Mutex = sync.Mutex;
pub const Notify = sync.Notify;
pub const Semaphore = sync.Semaphore;
pub const RwLock = sync.RwLock;
pub const OnceCell = sync.OnceCell;
pub const Barrier = sync.Barrier;

/// Go-style cancellation handle. Caller-owned; fire to wake every
/// cancel-aware blocking op holding `*Cancel`. See `src/cancel.zig`.
pub const cancel = @import("cancel.zig");
pub const Cancel = cancel.Cancel;

/// First-of-N event multiplexer over channels and timers. See
/// `src/select.zig` for the full API; the common shape is:
///
/// ```zig
/// const r = try volt.select(.{
///     .data    = volt.selectRecv(&ch),
///     .timeout = volt.selectTimer(volt.Duration.fromMillis(100)),
/// });
/// switch (r) { .data => |v| ..., .timeout => ... }
/// ```
pub const select = @import("select.zig").select;
pub const selectRecv = @import("select.zig").selectRecv;
pub const selectSend = @import("select.zig").selectSend;
pub const selectTimer = @import("select.zig").selectTimer;

// ─── Internal (not for user code) ────────────────────────────────────

/// Internal-use namespaces. Stable enough that volt's tests + benches
/// can reach them, but NOT part of the public API contract. Names
/// and shapes here may change without notice between minor versions.
pub const internal = struct {
    pub const Coroutine = @import("coroutine.zig").Coroutine;
    pub const PendingKind = @import("coroutine.zig").PendingKind;
    pub const Frame = @import("runtime.zig").Frame;
    pub const STACK_SIZE = @import("runtime.zig").STACK_SIZE;
    pub const reactor = @import("reactor.zig");
    pub const context = @import("context.zig");
    pub const park = @import("runtime.zig").park;
    pub const unpark = @import("runtime.zig").unpark;
    pub const tryDispatchInline = @import("runtime.zig").tryDispatchInline;
};

// Ensure every sub-module's tests are reachable from the test root.
test {
    _ = @import("runtime.zig");
    _ = @import("current.zig");
    _ = @import("task.zig");
    _ = @import("coroutine.zig");
    _ = @import("channel.zig");
    _ = @import("sync.zig");
    _ = @import("net.zig");
    _ = @import("park.zig");
    _ = @import("reactor.zig");
    _ = @import("work_steal_queue.zig");
    _ = @import("p.zig");
    _ = @import("worker.zig");
    _ = @import("parker.zig");
    _ = @import("stack.zig");
    _ = @import("signal.zig");
    _ = @import("context.zig");
    _ = @import("cancel.zig");
    _ = @import("time.zig");
    _ = @import("select.zig");
}

// ─── Tests ───────────────────────────────────────────────────────────

const std = @import("std");

// `volt.testing.allocator` — leak-detecting + multi-worker-safe.
// See src/testing.zig for why std.testing.allocator doesn't fit.
const test_allocator = testing.allocator;

const SleepCtx = struct { slept_ns: i128 = 0 };

// Windows kernel32 perf-counter externs. Declared locally — Zig
// 0.16's `std.os.windows.kernel32` doesn't expose them.
extern "kernel32" fn QueryPerformanceCounter(lpPerformanceCount: *i64) callconv(.winapi) i32;
extern "kernel32" fn QueryPerformanceFrequency(lpFrequency: *i64) callconv(.winapi) i32;

fn nanosNow() i128 {
    // Portable across Windows / Darwin / Linux. POSIX uses
    // `clock_gettime(MONOTONIC)`; Windows uses
    // `QueryPerformanceCounter` (the canonical Windows monotonic
    // clock). The POSIX `timespec` doesn't compile on Windows in
    // Zig 0.16 — `std.c.timespec` is empty on win and the extern
    // signature rejects it under the Win64 calling convention.
    if (@import("builtin").os.tag == .windows) {
        var counter: i64 = 0;
        var freq: i64 = 0;
        _ = QueryPerformanceCounter(&counter);
        _ = QueryPerformanceFrequency(&freq);
        // counter * 1e9 / freq, with intermediate i128 to dodge overflow.
        return @divTrunc(@as(i128, counter) * std.time.ns_per_s, @as(i128, freq));
    }
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

fn sleepTestRoot(ctx: *SleepCtx) !void {
    const start = nanosNow();
    try sleep(Duration.fromMillis(10));
    ctx.slept_ns = nanosNow() - start;
}

test "sleep parks for ~10ms via kqueue timer" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();
    var ctx = SleepCtx{};
    try (try rt.run(sleepTestRoot, .{&ctx}));
    try std.testing.expect(ctx.slept_ns >= 9 * std.time.ns_per_ms);
    try std.testing.expect(ctx.slept_ns < 100 * std.time.ns_per_ms);
}

// ─── scope tests ─────────────────────────────────────────────────────

fn scopeOkBody(c: *Cancel) anyerror!void {
    _ = c;
}

fn scopeErrBody(c: *Cancel) anyerror!void {
    _ = c;
    return error.Boom;
}

// Records whether the cancel was fired post-body. Exposed via a
// global slot because the body fn only sees *Cancel, no user ctx.
var test_scope_seen_fired: bool = false;

fn scopeFireRecordingBody(c: *Cancel) anyerror!void {
    // Snapshot pre-error: cancel should NOT be fired yet.
    if (c.isFired()) test_scope_seen_fired = true;
    return error.Boom;
}

fn scopeRoot(_: *void) !void {
    // (1) ok body → scope returns OK
    try scope(scopeOkBody);

    // (2) error body → scope propagates the error
    const r = scope(scopeErrBody);
    try std.testing.expectError(error.Boom, r);

    // (3) cancel only fires AFTER the body returns the error, not
    //     during. The body samples `c.isFired()` and sets a flag
    //     if pre-mature firing occurred.
    test_scope_seen_fired = false;
    const r2 = scope(scopeFireRecordingBody);
    try std.testing.expectError(error.Boom, r2);
    try std.testing.expect(!test_scope_seen_fired);
}

test "scope: ok body returns OK; error body propagates error" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();
    var dummy: void = {};
    try (try rt.run(scopeRoot, .{&dummy}));
}

// ─── withTimeout tests ───────────────────────────────────────────────

fn fastBody(c: *Cancel) !u32 {
    try sleepCancel(Duration.fromMillis(5), c);
    return 0x42;
}

fn slowBody(c: *Cancel) !u32 {
    try sleepCancel(Duration.fromSecs(10), c);
    return 0x99;
}

fn withTimeoutBodyWinsRoot() !void {
    const r = try withTimeout(Duration.fromMillis(100), fastBody, .{});
    try std.testing.expectEqual(@as(u32, 0x42), r);
}

fn withTimeoutTimerWinsRoot() !void {
    const r = withTimeout(Duration.fromMillis(10), slowBody, .{});
    try std.testing.expectError(error.Timeout, r);
}

// Note: pinned to workers = 1, matching the existing TCP-echo /
// reactor-cancel tests. With workers ≥ 2, Runtime.deinit can hang
// because a worker parked in `reactor.poll(blocking=true)` has no
// external wake mechanism — see Phase 6 (reactor.interrupt()) for
// the fix; once that lands these tests should move to workers=2.
test "withTimeout: body finishes within budget — returns body's result" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    try (try rt.run(withTimeoutBodyWinsRoot, .{}));
}

test "withTimeout: body exceeds budget — returns error.Timeout" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    try (try rt.run(withTimeoutTimerWinsRoot, .{}));
}

// ─── typed-error story for scope + withTimeout ───────────────────────
//
// Asserts the comptime return type is the user's body error set unioned
// with SpawnError + error{Timeout}, NOT anyerror. The test is mostly
// at compile-time: if `WithTimeoutReturn` widens to anyerror, the
// `catch err switch` against a closed set fails to compile.

const TypedBodyError = error{ Boom, Squish };

fn typedScopeBody(c: *Cancel) TypedBodyError!void {
    _ = c;
    return error.Boom;
}

fn typedScopeRoot(_: *void) !void {
    // scope's return type is `TypedBodyError!void` — the exhaustive
    // switch compiles only if the set isn't widened to anyerror.
    const r = scope(typedScopeBody);
    if (r) |_| return error.UnexpectedOk else |e| switch (e) {
        error.Boom => {}, // expected
        error.Squish => return error.UnexpectedSquish,
    }
}

test "scope: body error set propagates, no anyerror widening" {
    var rt = try Runtime.init(.{ .allocator = test_allocator });
    defer rt.deinit();
    var dummy: void = {};
    try (try rt.run(typedScopeRoot, .{&dummy}));
}

fn typedTimeoutBody(c: *Cancel) TypedBodyError!u32 {
    _ = c;
    return error.Squish;
}

fn typedTimeoutRoot() !void {
    // withTimeout's return type is `(TypedBodyError || SpawnError ||
    // error{Timeout})!u32`. Exhaustive switch confirms no anyerror.
    const r = withTimeout(Duration.fromMillis(100), typedTimeoutBody, .{});
    if (r) |_| return error.UnexpectedOk else |e| switch (e) {
        error.Squish => {},
        error.Boom => return error.UnexpectedBoom,
        error.Timeout => return error.UnexpectedTimeout,
        error.OutOfMemory, error.ArenaExhausted, error.MmapFailed, error.MprotectFailed => return error.UnexpectedSpawnError,
    }
}

test "withTimeout: body error set + SpawnError + Timeout propagate, no anyerror widening" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 1 });
    defer rt.deinit();
    try (try rt.run(typedTimeoutRoot, .{}));
}

// ─── spawnBlocking tests ─────────────────────────────────────────────

fn doubleSync(x: u32) u32 {
    return x * 2;
}

fn spawnBlockingRoot() !void {
    const result = try spawnBlocking(doubleSync, .{@as(u32, 21)});
    try std.testing.expectEqual(@as(u32, 42), result);
}

test "spawnBlocking: sync fn runs on pool, result delivered to coroutine" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();
    try (try rt.run(spawnBlockingRoot, .{}));
}

const ManyCtx = struct { counter: *std.atomic.Value(u32) };

fn incOnce(c: *std.atomic.Value(u32)) u32 {
    return c.fetchAdd(1, .acq_rel);
}

fn spawnBlockingManyRoot(ctx: *ManyCtx) !void {
    // 64 concurrent spawnBlockings against a pool sized to 8 — the
    // queue absorbs the overflow and every job eventually runs.
    var tasks: [64]*Task(anyerror!u32) = undefined;
    for (&tasks) |*t| {
        t.* = try spawn(struct {
            fn body(counter: *std.atomic.Value(u32)) anyerror!u32 {
                return try spawnBlocking(incOnce, .{counter});
            }
        }.body, .{ctx.counter});
    }
    for (tasks) |t| _ = try (t.join());
    try std.testing.expectEqual(@as(u32, 64), ctx.counter.load(.acquire));
}

test "spawnBlocking: queue overflows past max_threads, all jobs run" {
    var rt = try Runtime.init(.{
        .allocator = test_allocator,
        .workers = 2,
        .blocking = .{ .max_threads = 8 },
    });
    defer rt.deinit();
    var counter = std.atomic.Value(u32).init(0);
    var ctx = ManyCtx{ .counter = &counter };
    try (try rt.run(spawnBlockingManyRoot, .{&ctx}));
}

// ─── joinAll / joinFirst tests ──────────────────────────────────────

fn produceU32() u32 {
    return 0x42;
}
fn produceBytes() []const u8 {
    return "hello";
}

fn joinAllRoot() !void {
    const t_u = try spawn(produceU32, .{});
    const t_b = try spawn(produceBytes, .{});
    const r = joinAll(.{ .num = t_u, .text = t_b });
    // Heterogeneous result struct — num is u32, text is []const u8.
    // If joinAll widened to anyopaque or anything erased, this
    // wouldn't compile.
    try std.testing.expectEqual(@as(u32, 0x42), r.num);
    try std.testing.expectEqualStrings("hello", r.text);
}

test "joinAll: heterogeneous results, typed struct return" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();
    try (try rt.run(joinAllRoot, .{}));
}

fn quickWinner(c: *Cancel) u32 {
    _ = c;
    return 1;
}

fn slowLoser(c: *Cancel) u32 {
    // Park on the shared cancel via a long sleep. When the winner
    // fires c, sleepCancel returns error.Cancelled — we map back to a
    // sentinel value the test recognises.
    sleepCancel(Duration.fromSecs(10), c) catch return 0xCA;
    return 0xFF;
}

fn joinFirstRoot(c: *Cancel) !void {
    const t_w = try spawn(quickWinner, .{c});
    const t_l = try spawn(slowLoser, .{c});
    const winner = try joinFirst(c, .{ .fast = t_w, .slow = t_l });
    switch (winner) {
        .fast => |v| try std.testing.expectEqual(@as(u32, 1), v),
        .slow => return error.SlowShouldNotWin,
    }
}

fn joinFirstOuter() !void {
    try scope(joinFirstRoot);
}

test "joinFirst: fast task wins, slow task observes cancel" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 2 });
    defer rt.deinit();
    try (try rt.run(joinFirstOuter, .{}));
}

// ─── collect tests ───────────────────────────────────────────────────

fn doubleU64(x: u32) u64 {
    return @as(u64, x) * 2;
}

fn collectRoot() !void {
    const inputs = [_]u32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    var outputs: [8]u64 = undefined;
    try collect(&inputs, doubleU64, &outputs);
    inline for (inputs, 0..) |x, i| {
        try std.testing.expectEqual(@as(u64, x) * 2, outputs[i]);
    }
}

test "collect: parallel map over slice, results in order" {
    var rt = try Runtime.init(.{ .allocator = test_allocator, .workers = 4 });
    defer rt.deinit();
    try (try rt.run(collectRoot, .{}));
}
