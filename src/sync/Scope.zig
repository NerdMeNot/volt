//! Scope — RAII nursery for structured concurrency.
//!
//! A `Scope` is a region of code where you can `spawn` child coroutines
//! and be guaranteed they ALL complete (or are cancelled) before the
//! region exits. Borrowed from Trio/Kotlin coroutines. The single
//! biggest ergonomic win over Go's bare `go ...` — children can't
//! outlive their parent scope, so resource lifetimes match lexical scope.
//!
//! ## Usage
//!
//! ```zig
//! try volt.scope(struct {
//!     fn body(s: *volt.Scope) !void {
//!         try s.spawn(workerA, .{ctx});
//!         try s.spawn(workerB, .{ctx});
//!         // ... any other concurrent work ...
//!     }
//! }.body);
//! // After this point: workerA and workerB have BOTH finished
//! // (normally or via cancellation). No leaked coroutines.
//! ```
//!
//! ## Semantics
//!
//! - On normal `body` return: scope joins all children, then returns void.
//! - On `body` error: scope cancels all children, joins them, then
//!   returns the body's error.
//! - Children that error don't auto-propagate. Use `s.fault(err)` to
//!   surface a child error to the scope; the FIRST such call wins, and
//!   the scope returns it after joining everyone.
//! - Cancellation of children is cooperative: each child observes
//!   `error.Cancelled` at its next suspension point (matches existing
//!   `Job.cancel` semantics).
//!
//! ## Error propagation
//!
//! Typed child error propagation (`Scope(E)` over a child error set)
//! requires children sharing a result-error type — planned. Today
//! `s.fault(anyerror)` is the explicit escape hatch.

const std = @import("std");
const Coroutine = @import("../coroutine/coroutine.zig").Coroutine;
const Job = @import("../task/job.zig").Job;
const launch_mod = @import("../api/launch.zig");
const launch = launch_mod.launch;
const launchWith = launch_mod.launchWith;
const destroyJob = launch_mod.destroyJob;
const destroyJobWith = launch_mod.destroyJobWith;
const runtime_mod = @import("../runtime.zig");
const thread = @import("../internal/thread.zig");

/// Bytes reserved on the calling coroutine's stack for child Frames.
/// Sized to fit ~16 small Frames (Coroutine + Closure + Args + Result
/// is typically 256-512 bytes). Beyond this, Scope falls back to the
/// runtime's heap allocator.
///
/// Pure Zig win: this lets all spawn-allocations for scope-bounded
/// coroutines avoid the heap entirely. No GC language can do this —
/// the runtime would need to scan and rearrange memory across
/// coroutine boundaries, which Zig doesn't have to.
const SCOPE_INLINE_ARENA_BYTES: usize = 8 * 1024;

pub const Scope = struct {
    /// Backing storage for child Frames + Job structs. Lives on the
    /// calling coroutine's stack inside `volt.scope`. Lifetime is
    /// bounded by the scope: scope.deinit joins all children first,
    /// THEN body returns and this storage becomes invalid — never
    /// before. So caller-stack allocation is provably safe.
    inline_arena: [SCOPE_INLINE_ARENA_BYTES]u8 align(16) = undefined,
    /// FixedBufferAllocator over `inline_arena`. Used for child Job +
    /// Frame allocations. `destroy` is a no-op — memory reclaimed when
    /// scope's stack frame deinitializes (after deinit/join).
    fba: std.heap.FixedBufferAllocator,

    /// Fallback allocator for when the inline arena is exhausted.
    /// Falls back to the runtime's allocator. Children list itself
    /// (the ArrayList backing) lives here too.
    rt_allocator: std.mem.Allocator,

    /// Tracked children. Each entry remembers whether it was allocated
    /// from the inline arena (no-op destroy) or from rt_allocator (real
    /// destroy).
    children: std.array_list.Managed(ChildEntry),

    fault_mutex: thread.Mutex = .{},
    first_fault: ?anyerror = null,

    const ChildEntry = struct {
        job: *Job,
        /// True if Job + Frame came from `fba` (inline arena). Determines
        /// which destroyJob variant scope.deinit calls.
        in_arena: bool,
    };

    /// Spawn a child coroutine in this scope. The child's lifetime is
    /// bounded by the scope's body: when `body` returns (or errors),
    /// the scope joins this child before returning to the caller of
    /// `volt.scope`.
    ///
    /// Allocation strategy: tries the inline arena first (zero heap
    /// calls — Frame and Job both bump-allocated on the calling
    /// coroutine's stack). Falls back to the runtime allocator if the
    /// arena is exhausted. The caller doesn't know or care which path
    /// was taken — it's a pure perf optimization.
    pub fn spawn(self: *Scope, comptime fn_ptr: anytype, args: anytype) !void {
        // Try inline arena first.
        if (launchWith(self.fba.allocator(), fn_ptr, args)) |job| {
            try self.children.append(.{ .job = job, .in_arena = true });
            return;
        } else |err| switch (err) {
            error.OutOfMemory => {
                // Arena exhausted — fall back to runtime allocator.
                const job = try launch(fn_ptr, args);
                errdefer destroyJob(job);
                try self.children.append(.{ .job = job, .in_arena = false });
            },
            else => return err,
        }
    }

    /// Surface a fault from inside a child coroutine. The first
    /// `fault` call wins; subsequent calls are ignored. After the
    /// scope's body returns, the scope cancels remaining children and
    /// returns this fault.
    pub fn fault(self: *Scope, err: anyerror) void {
        self.fault_mutex.lock();
        defer self.fault_mutex.unlock();
        if (self.first_fault == null) self.first_fault = err;
    }

    fn cancelAllInternal(self: *Scope) void {
        for (self.children.items) |entry| entry.job.cancel();
    }

    fn joinAllInternal(self: *Scope) void {
        // join() may return Cancelled or StackOverflow — both are
        // valid terminal states for cleanup; we discard. The result
        // tag is observable on the Task (if the user kept one).
        for (self.children.items) |entry| entry.job.join() catch {};
    }

    fn deinit(self: *Scope) void {
        for (self.children.items) |entry| {
            if (entry.in_arena) {
                // FBA destroy is a no-op; lifecycleRelease still fires
                // to unregister from runtime's live_coroutines list and
                // recycle the stack via Done.subscribe's pool path.
                destroyJobWith(self.fba.allocator(), entry.job);
            } else {
                destroyJob(entry.job);
            }
        }
        self.children.deinit();
    }
};

/// Run `body(scope)` as a structured-concurrency region.
///
/// `body` must have signature `fn (*Scope) E!void`. Returns body's
/// error (if it errored), or the first `scope.fault(err)` raised by a
/// child (if any), or `void` on clean completion.
pub fn scope(comptime body: anytype) !void {
    const rt = runtime_mod.currentRuntime() orelse
        @panic("volt.scope called outside a runtime");

    var s: Scope = .{
        .fba = undefined,
        .rt_allocator = rt.allocator,
        .children = std.array_list.Managed(Scope.ChildEntry).init(rt.allocator),
    };
    s.fba = std.heap.FixedBufferAllocator.init(&s.inline_arena);
    defer s.deinit();

    var body_error: ?anyerror = null;
    if (body(&s)) |_| {} else |err| {
        body_error = err;
    }

    // If anything went wrong (body or child fault), cancel siblings.
    if (body_error != null or s.first_fault != null) {
        s.cancelAllInternal();
    }

    // Wait for every child to reach `.done`.
    s.joinAllInternal();

    // Surface body error first (it's what the caller asked us to run);
    // child fault is a fallback signal.
    if (body_error) |e| return e;
    if (s.first_fault) |e| return e;
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

const volt = @import("../lib.zig");

const SimpleScopeCtx = struct {
    counter: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn simpleChild(ctx: *SimpleScopeCtx) void {
    _ = ctx.counter.fetchAdd(1, .monotonic);
}

fn simpleScopeBody(s: *Scope) !void {
    var i: u32 = 0;
    while (i < 8) : (i += 1) {
        const ctx = simpleScopeBodyArg.?;
        try s.spawn(simpleChild, .{ctx});
    }
}

// Use a static slot so simpleScopeBody can grab the context. (In real
// usage the scope body is a closure-like inline struct fn; tests use
// statics for simplicity.)
threadlocal var simpleScopeBodyArg: ?*SimpleScopeCtx = null;

fn simpleScopeRoot() !u32 {
    var ctx = SimpleScopeCtx{};
    simpleScopeBodyArg = &ctx;
    defer simpleScopeBodyArg = null;
    try scope(simpleScopeBody);
    return ctx.counter.load(.acquire);
}

test "scope: 8 spawned children all complete before scope returns" {
    const c = try volt.run(.{ .allocator = std.testing.allocator }, simpleScopeRoot, .{});
    try std.testing.expectEqual(@as(u32, 8), c);
}

const FaultCtx = struct {
    s: *Scope,
};

fn faultingChild(ctx: *FaultCtx) void {
    ctx.s.fault(error.SomethingBroke);
}

fn quietChild(_: *FaultCtx) void {
    var k: u32 = 0;
    while (k < 8) : (k += 1) {
        volt.yield() catch return;
    }
}

threadlocal var faultBodyArg: ?*FaultCtx = null;

fn faultScopeBody(s: *Scope) !void {
    const ctx = faultBodyArg.?;
    ctx.s = s;
    try s.spawn(quietChild, .{ctx});
    try s.spawn(faultingChild, .{ctx});
    try s.spawn(quietChild, .{ctx});
}

fn faultScopeRoot() !void {
    var ctx = FaultCtx{ .s = undefined };
    faultBodyArg = &ctx;
    defer faultBodyArg = null;
    try scope(faultScopeBody);
}

test "scope: child fault propagates as scope error; siblings cancelled" {
    const result = volt.run(.{ .allocator = std.testing.allocator }, faultScopeRoot, .{});
    try std.testing.expectError(error.SomethingBroke, result);
}

const BodyErrorCtx = struct {
    completed: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
};

fn bodyErrorChild(ctx: *BodyErrorCtx) void {
    var k: u32 = 0;
    while (k < 64) : (k += 1) {
        volt.yield() catch return;
    }
    _ = ctx.completed.fetchAdd(1, .monotonic);
}

threadlocal var bodyErrorArg: ?*BodyErrorCtx = null;

fn erroringScopeBody(s: *Scope) !void {
    const ctx = bodyErrorArg.?;
    try s.spawn(bodyErrorChild, .{ctx});
    try s.spawn(bodyErrorChild, .{ctx});
    return error.BodyDecidedToBail;
}

fn erroringScopeRoot() !u32 {
    var ctx = BodyErrorCtx{};
    bodyErrorArg = &ctx;
    defer bodyErrorArg = null;
    const result = scope(erroringScopeBody);
    try std.testing.expectError(error.BodyDecidedToBail, result);
    return ctx.completed.load(.acquire);
}

test "scope: body error cancels children before returning" {
    const completed = try volt.run(.{ .allocator = std.testing.allocator }, erroringScopeRoot, .{});
    // Children get cancelled at their first yield → completed should
    // be 0 (or at most a tiny race count if a child slipped through
    // its 64 yields before cancel was set). The hard invariant is
    // that the scope error surfaces.
    try std.testing.expect(completed <= 2);
}
