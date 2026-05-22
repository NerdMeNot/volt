//! Coroutine — the minimum stackful unit of work.
//!
//! Each Coroutine carries:
//!   - Its own callee-save register context (`ctx`)
//!   - A pointer back to its dispatching worker's `main_ctx` (where
//!     the swap-back lands on yield / park / done)
//!   - Its stack (allocator-managed)
//!   - A pointer to its Frame (comptime-specialized closure)
//!   - A destroy function for the Frame (set by the Frame factory)
//!   - An intrusive `next` pointer for the run queue
//!   - An intrusive `wait_next` pointer for sync-primitive wait queues
//!
//! No per-coroutine join park, no event-source vtable: completion is
//! signalled via the coroutine's optional `task_done` flag, woken
//! through the parking lot (see `park.zig`). Dispatch reads
//! `pending: PendingKind` after
//! swap-back to decide whether to re-queue, leave parked, or free.

const std = @import("std");
const context = @import("context.zig");
const stack_mod = @import("stack.zig");

/// User functions passed to `spawn` get monomorphized into a Frame
/// (comptime-generated). The frame's FIRST field is always a
/// `*const fn(*anyopaque) callconv(.c) void` (the trampoline that
/// `voltCoroEntry` calls). After the trampoline runs the user fn, it
/// must `context.swap(&coro.ctx, coro.main_ctx)`.
pub const TrampolineFn = *const fn (*anyopaque) callconv(.c) void;

/// Destroy a Frame allocation. Erased to *anyopaque so the Coroutine
/// doesn't carry a generic type parameter. Set by the spawn site to
/// the Frame type's destroy function.
pub const FrameDestroyFn = *const fn (frame_ptr: *anyopaque, allocator: std.mem.Allocator) void;

/// What does a swap-back from a coroutine mean?
///   `done`  — trampoline returned. Worker frees stack + Coroutine
///             (and Frame, if no Task owns it).
///   `yield` — coroutine called `yield()`. Worker re-pushes to queue.
///   `park`  — coroutine called `park()` for sync/IO. Dispatcher
///             transitions `park_state` to PARKED here; some
///             external event (reactor, sync) will re-queue via
///             `runtime.unpark`.
pub const PendingKind = enum(u8) {
    done,
    yield,
    park,
};

/// Coroutine park-state machine. Used to close the race window
/// between a primitive's "register me as waiter" step and the
/// coroutine's actual `context.swap` out of the running state.
/// Without this state, an unpark fired during that window would
/// push the coroutine onto the run queue while it is still being
/// executed by its owning worker — two workers would then race to
/// run the same stack.
///
/// Transitions:
///   RUNNING  -[dispatch sees `.park`]→ PARKED
///   RUNNING  -[unpark while running]→ NOTIFIED
///   PARKED   -[unpark]→ RUNNING + enqueue
///   NOTIFIED -[dispatch sees `.park`]→ RUNNING + immediate re-queue
pub const ParkState = struct {
    pub const RUNNING: u32 = 0;
    pub const PARKED: u32 = 1;
    pub const NOTIFIED: u32 = 2;
};

/// Layout note: HOT fields the dispatcher reads on every cycle
/// (`pending`, `park_state`, `main_ctx`, `next`, `stack`, `frame_*`,
/// `task_done`, `task_thread_parker`, `has_task`) live in the first
/// 64 bytes. `ctx` (168 B — the context-switch payload) follows.
/// COLD fields (`runtime`, `wait_next`, `cancel_in_flight`,
/// `reactor_wait_op`) trail at the bottom. The asm context switch
/// reads/writes `&coro.ctx` directly — its offset within Coroutine
/// doesn't matter to the asm (only `Context`'s internal layout
/// does, see context_arm64.zig).
///
/// Why this matters: `ctx` is 168 B (spans ≥ 2 cache lines). The
/// original layout had `pending` + `park_state` on a 3rd line at
/// offset 256+. Every dispatch's "read pending after swap-back"
/// then incurred a cold-line load. Reordering puts those small
/// hot fields in the SAME cache line as the start of `ctx` (which
/// is already live in cache after the swap), eliminating that miss.
pub const Coroutine = extern struct {
    // ── HOT (first 64 B — read on every dispatch / park / unpark) ──
    // `extern struct` because Zig's default-struct field auto-reorder
    // (for minimal padding) sorts the small `pending`/`park_state`
    // fields to the end of the struct, putting them on a 3rd cache
    // line. Extern preserves declaration order, keeping the hot
    // fields next to the start of `ctx` (which the swap-back leaves
    // hot in L1).
    /// Park-state machine (see `ParkState`). Initialised to RUNNING
    /// at spawn; transitions are owned by `runtime.park` / `runtime.unpark`
    /// / `dispatch` and documented at those sites.
    park_state: std.atomic.Value(u32) = std.atomic.Value(u32).init(ParkState.RUNNING),
    /// Why did the last swap-back happen? Worker reads this after the
    /// swap-back returns from `ctx.swap`. The trampoline sets it to
    /// `.done` before the terminal swap. `yield()` and `park()`
    /// helpers set it to `.yield` / `.park` before swapping.
    pending: PendingKind = .done,
    /// Non-null when a Task handle exists.
    has_task: bool = false,
    _hot_pad: [2]u8 = @splat(0),
    /// Worker's main context.
    main_ctx: *context.Context = undefined,
    /// Intrusive next pointer for the run queue (FIFO linked list).
    next: ?*Coroutine = null,
    /// Base of the slot handed out by the runtime's stack arena.
    /// SP top is `stack + stack.slotSize()`. The bottom `guardSize()`
    /// bytes are PROT_NONE — any access SIGSEGVs.
    stack: stack_mod.StackPtr = undefined,
    /// Pointer to the Frame (comptime-generated).
    frame_ptr: *anyopaque = undefined,
    /// Frees the Frame allocation.
    frame_destroy: ?FrameDestroyFn = null,
    /// Optional task completion flag. Dispatch's `.done` branch
    /// stores 1 here and calls `parking_lot.unparkOne(&done)` to
    /// wake any joiner. Null for coroutines without a Task handle.
    task_done: ?*std.atomic.Value(u32) = null,
    /// Optional OS-thread Parker to unpark when this coroutine
    /// completes. Set by `Runtime.run` on the root task so the
    /// driver thread (worker 0) wakes when the root finishes —
    /// the driver isn't a coroutine, so the parking-lot path
    /// doesn't apply. Null for non-root tasks.
    task_thread_parker: ?*@import("parker.zig").Parker = null,

    // ── Context-switch payload (168 B — dominates struct size) ────
    ctx: context.Context = .{},

    // ── COLD (read on rare paths — cancel, cross-runtime ops) ─────
    /// Owning runtime, type-erased to avoid the Coroutine ↔ Runtime
    /// import cycle. Cast back to `*Runtime` via @ptrCast/@alignCast
    /// when needed (e.g. `unpark` to push back to the queue).
    runtime: *anyopaque = undefined,
    /// Intrusive next pointer for synchronization-primitive wait
    /// queues (Mutex, Notify, Semaphore). The bespoke WaitQueue in
    /// `src/sync.zig` uses this. **Tracked for removal in #168**:
    /// migrating sync to the parking lot would let this field go
    /// (saving 8 bytes per Coroutine + simplifying the struct), but
    /// the migration involves a real fairness-vs-throughput design
    /// choice that's separate from this field's existence.
    wait_next: ?*Coroutine = null,
    /// Set by cancel-aware blocking ops (`Mutex.lockCancel`,
    /// `Spsc.recvCancel`, etc.) immediately before they call
    /// `park.parkOn`. The op's validator (which runs under the
    /// parking-lot bucket lock) reads this slot to close the
    /// register-then-park race: if the cancel fires between
    /// `c.register` and `parkOn`, the validator sees `fired` and
    /// bails without parking. Cleared after wake.
    ///
    /// Type-erased to avoid the coroutine → cancel → runtime →
    /// coroutine import cycle. Cast back to `*cancel.Cancel` at
    /// validator time.
    cancel_in_flight: ?*anyopaque = null,

    /// Set by cancel-aware reactor waits (`waitReadableCancel`
    /// etc.) before submitting to the kernel; cleared on resume.
    /// Points to a per-backend `WaitOp` struct that lives on this
    /// coroutine's stack and carries the info `cancelCoro` needs
    /// to deregister via syscall (fd + filter for kqueue, fd for
    /// epoll, socket + OVERLAPPED for IOCP; io_uring needs no extra
    /// state because `user_data == @intFromPtr(coro)`).
    ///
    /// Type-erased for the same import-cycle reason as
    /// `cancel_in_flight`.
    reactor_wait_op: ?*anyopaque = null,
};

// ─────────────────────────────────────────────────────────────────────
// Frame factory — comptime-monomorphized per (user_fn, ArgsT, RetT)
// ─────────────────────────────────────────────────────────────────────

/// The Frame is the per-spawn closure: it stores run_fn (trampoline
/// entry), back-pointer to its Coroutine, user fn's args, and a slot
/// for the result.
///
/// Layout: run_fn at offset 0 (voltCoroEntry reads it). Followed by
/// args / result / coro back-pointer.
pub fn Frame(comptime user_fn: anytype, comptime ArgsT: type) type {
    const FnT = @TypeOf(user_fn);
    const fn_info = @typeInfo(FnT).@"fn";
    const Ret = fn_info.return_type.?;

    return struct {
        const Self = @This();
        // MUST be first field — voltCoroEntry reads *(x19) as run_fn.
        run_fn: TrampolineFn = &trampoline,
        coro: ?*Coroutine = null,
        args: ArgsT,
        result: Ret = undefined,

        pub const Result = Ret;

        fn trampoline(opaque_ptr: *anyopaque) callconv(.c) void {
            const self: *Self = @ptrCast(@alignCast(opaque_ptr));
            self.result = @call(.auto, user_fn, self.args);
            // Terminal swap. Worker will see pending == .done.
            const c = self.coro.?;
            c.pending = .done;
            context.swap(&c.ctx, c.main_ctx);
            unreachable;
        }

        pub fn destroy(frame_ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *Self = @ptrCast(@alignCast(frame_ptr));
            allocator.destroy(self);
        }
    };
}
