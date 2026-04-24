//! Task - Scheduler-Integrated Future Wrapper
//!
//! A Task wraps a Future with:
//! - Packed atomic state for lock-free transitions
//! - Reference counting for safe memory management
//! - Intrusive list pointers for scheduler queues
//! - Waker integration for async operations
//!
//! State Machine:
//! ```
//!   IDLE ──spawn──> SCHEDULED ──run──> RUNNING ──yield──> IDLE
//!     │                                    │                │
//!     │                                    ▼                │
//!     └────────────────────────────── COMPLETE <────────────┘
//! ```
//!
//! ## Stackless Design
//!
//! Unlike stackful coroutines that allocate 16-64KB per task, futures are
//! small state machines (~256-512 bytes) that explicitly track their progress.
//! This enables millions of concurrent tasks instead of thousands.

const std = @import("std");
const thr = @import("../thread.zig");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

// Debug flag - set to true to trace task state transitions
const debug_task = false;

const future_mod = @import("../../future.zig");
const Poll = future_mod.Poll;
const PollResult = future_mod.PollResult;
const Waker = future_mod.Waker;
const RawWaker = future_mod.RawWaker;
const Context = future_mod.Context;

// ═══════════════════════════════════════════════════════════════════════════════
// Task State (Packed Atomic)
// ═══════════════════════════════════════════════════════════════════════════════

/// Task lifecycle state
pub const Lifecycle = enum(u8) {
    /// Not scheduled, waiting for work or I/O
    idle = 0,
    /// In a run queue, waiting to execute
    scheduled = 1,
    /// Currently executing on a worker
    running = 2,
    /// Finished execution
    complete = 3,
};

/// Packed task state for atomic operations
/// Layout: | ref_count (24 bits) | shield_depth (8 bits) | flags (8 bits) | lifecycle (8 bits) | unused (16 bits) |
///
/// ## Wakeup Protocol
///
/// The `notified` flag is the ONLY signal for "task needs to be polled again".
/// When a waker fires:
/// - If task is IDLE: transition to SCHEDULED and queue it
/// - If task is RUNNING or SCHEDULED: set `notified` bit
///
/// After poll returns Pending:
/// - Transition RUNNING -> IDLE atomically, capturing `notified`
/// - If `notified` was set, immediately reschedule
///
/// This prevents lost wakeups when a waker fires while the task is still running.
///
/// Shield-based cancellation:
/// When shield_depth > 0, cancellation is deferred. The CANCELLED flag
/// can still be set, but isCancelled() returns false until all shields
/// are dropped. This protects critical sections from partial execution.
pub const State = packed struct(u64) {
    /// Reserved for future use
    _reserved: u16 = 0,

    /// Current lifecycle state
    lifecycle: Lifecycle = .idle,

    /// Flags
    /// NOTIFIED: A waker fired while task was Running or Scheduled.
    /// This is the ONLY signal for "poll me again" - no separate "waiting" flag.
    notified: bool = false,
    cancelled: bool = false,
    join_interest: bool = false,
    detached: bool = false,
    _flag_reserved: u4 = 0,

    /// Shield depth (0-255) - cancellation deferred while > 0
    shield_depth: u8 = 0,

    /// Reference count (24 bits = 16 million refs, plenty)
    ref_count: u24 = 1,

    const REF_ONE: u64 = 1 << 40;
    const SHIELD_ONE: u64 = 1 << 32;

    pub fn toU64(self: State) u64 {
        return @bitCast(self);
    }

    pub fn fromU64(val: u64) State {
        return @bitCast(val);
    }

    pub fn addRef(self: State) State {
        var s = self;
        s.ref_count += 1;
        return s;
    }

    pub fn decRef(self: State) State {
        var s = self;
        if (builtin.mode == .Debug) {
            if (s.ref_count == 0) {
                @panic("refcount underflow: decRef called with ref_count already at 0");
            }
        }
        s.ref_count -= 1;
        return s;
    }

    /// Check if cancellation is currently effective (not shielded)
    pub fn isCancellationEffective(self: State) bool {
        return self.cancelled and self.shield_depth == 0;
    }

    /// Add a shield level
    pub fn addShield(self: State) State {
        var s = self;
        if (s.shield_depth < 255) {
            s.shield_depth += 1;
        }
        return s;
    }

    /// Remove a shield level
    pub fn removeShield(self: State) State {
        var s = self;
        if (s.shield_depth > 0) {
            s.shield_depth -= 1;
        }
        return s;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Task Header (Type-Erased)
// ═══════════════════════════════════════════════════════════════════════════════

/// Type-erased task header for scheduler queues
pub const Header = struct {
    /// Packed atomic state
    state: std.atomic.Value(u64),

    /// Intrusive list pointers (for scheduler queues)
    next: ?*Header = null,
    prev: ?*Header = null,

    /// VTable for type-erased operations
    vtable: *const VTable,

    /// Waker for JoinHandle notification.
    /// Set by JoinFuture.poll() so the joiner gets woken when this task completes.
    /// Protected by the happens-before from setJoinInterest CAS → transitionToComplete CAS.
    join_waker: ?Waker = null,

    pub const VTable = struct {
        /// Run one step of the task (poll the future)
        poll: *const fn (*Header) PollResult_,
        /// Clean up and free the task
        drop: *const fn (*Header) void,
        /// Schedule the task (adds ref, for I/O wakers)
        schedule: *const fn (*Header) void,
        /// Reschedule the task (no ref change, for sync primitives)
        reschedule: *const fn (*Header) void,
    };

    /// Result of polling a task
    pub const PollResult_ = enum {
        /// Task yielded, needs more work
        pending,
        /// Task completed
        complete,
        /// Task yielded due to cooperative budget exhaustion.
        /// Scheduler should route to global queue, not LIFO slot,
        /// to prevent the yielding task from starving others.
        yield,
    };

    /// Initialize header with vtable
    pub fn init(vtable: *const VTable) Header {
        return .{
            .state = std.atomic.Value(u64).init((State{}).toU64()),
            .vtable = vtable,
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // State Transitions
    // ─────────────────────────────────────────────────────────────────────────

    /// Transition to scheduled state. Returns true if task should be queued.
    ///
    /// Wakeup protocol:
    /// - IDLE -> SCHEDULED: returns true, caller must queue the task
    /// - RUNNING -> sets notified, returns false (task will check on yield)
    /// - SCHEDULED -> sets notified, returns false (already queued)
    /// - COMPLETE -> returns false
    pub fn transitionToScheduled(self: *Header) bool {
        var current = State.fromU64(self.state.load(.acquire));

        while (true) {
            if (current.lifecycle == .complete or current.cancelled) {
                if (debug_task) {
                    std.debug.print("[TASK] transitionToScheduled: task={x} is complete/cancelled\n", .{@intFromPtr(self)});
                }
                return false;
            }

            if (current.lifecycle == .idle) {
                // IDLE -> SCHEDULED: queue the task
                var new = current;
                new.lifecycle = .scheduled;
                new.notified = false; // Consumed by queuing

                if (self.state.cmpxchgWeak(current.toU64(), new.toU64(), .acq_rel, .acquire)) |updated| {
                    current = State.fromU64(updated);
                    continue;
                }
                if (debug_task) {
                    std.debug.print("[TASK] transitionToScheduled: task={x} IDLE->SCHEDULED, returning true (queue it)\n", .{@intFromPtr(self)});
                }
                return true; // Caller must queue
            }

            // RUNNING or SCHEDULED: just set notified bit
            if (current.notified) {
                if (debug_task) {
                    std.debug.print("[TASK] transitionToScheduled: task={x} already notified in {s}\n", .{ @intFromPtr(self), @tagName(current.lifecycle) });
                }
                return false; // Already notified
            }

            var new = current;
            new.notified = true;

            if (self.state.cmpxchgWeak(current.toU64(), new.toU64(), .acq_rel, .acquire)) |updated| {
                current = State.fromU64(updated);
                continue;
            }
            if (debug_task) {
                std.debug.print("[TASK] transitionToScheduled: task={x} set notified in {s}, returning false\n", .{ @intFromPtr(self), @tagName(current.lifecycle) });
            }
            return false; // Notified instead of scheduled
        }
    }

    /// Transition to running state. Returns true if successful.
    pub fn transitionToRunning(self: *Header) bool {
        var current = State.fromU64(self.state.load(.acquire));

        while (true) {
            if (current.lifecycle != .scheduled) {
                return false;
            }

            var new = current;
            new.lifecycle = .running;

            if (self.state.cmpxchgWeak(current.toU64(), new.toU64(), .acq_rel, .acquire)) |updated| {
                current = State.fromU64(updated);
                continue;
            }
            return true;
        }
    }

    /// Transition RUNNING -> IDLE atomically. Returns previous state.
    ///
    /// CRITICAL: This atomically transitions AND clears the notified bit.
    /// The caller MUST check if `prev.notified` was true and reschedule if so.
    /// This is the key to preventing lost wakeups.
    pub fn transitionToIdle(self: *Header) State {
        var current = State.fromU64(self.state.load(.acquire));

        while (true) {
            if (current.lifecycle != .running) {
                return current;
            }

            var new = current;
            new.lifecycle = .idle;
            new.notified = false; // Clear atomically - caller checks prev.notified

            if (self.state.cmpxchgWeak(current.toU64(), new.toU64(), .acq_rel, .acquire)) |updated| {
                current = State.fromU64(updated);
                continue;
            }
            return current; // Return PREVIOUS state so caller can check notified
        }
    }

    /// Mark task as complete. Returns the previous state so the caller
    /// can check join_interest and notify the joiner if needed.
    pub fn transitionToComplete(self: *Header) State {
        var current = State.fromU64(self.state.load(.acquire));

        while (true) {
            var new = current;
            new.lifecycle = .complete;

            if (self.state.cmpxchgWeak(current.toU64(), new.toU64(), .acq_rel, .acquire)) |updated| {
                current = State.fromU64(updated);
                continue;
            }
            return current; // Return previous state
        }
    }

    /// Set the join_interest flag atomically. Called by JoinFuture.poll()
    /// after storing the join_waker. The acq_rel CAS establishes a
    /// happens-before with transitionToComplete's acq_rel CAS.
    pub fn setJoinInterest(self: *Header) void {
        var current = State.fromU64(self.state.load(.acquire));
        while (true) {
            if (current.join_interest) return; // Already set
            var new = current;
            new.join_interest = true;
            if (self.state.cmpxchgWeak(current.toU64(), new.toU64(), .acq_rel, .acquire)) |updated| {
                current = State.fromU64(updated);
                continue;
            }
            return;
        }
    }

    /// Wake the joiner (if any). Called after transitionToComplete when
    /// the previous state had join_interest set. Consumes the join_waker.
    pub fn notifyJoiner(self: *Header) void {
        if (self.join_waker) |w| {
            self.join_waker = null;
            w.wake(); // Consumes waker, schedules JoinFuture's parent task
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Reference Counting
    // ─────────────────────────────────────────────────────────────────────────

    /// Increment reference count
    pub fn ref(self: *Header) void {
        _ = self.state.fetchAdd(State.REF_ONE, .acq_rel);
    }

    /// Decrement reference count. Returns true if this was the last reference.
    pub fn unref(self: *Header) bool {
        const prev = self.state.fetchSub(State.REF_ONE, .acq_rel);
        const prev_state = State.fromU64(prev);
        return prev_state.ref_count == 1;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // State Queries
    // ─────────────────────────────────────────────────────────────────────────

    pub fn isComplete(self: *Header) bool {
        return State.fromU64(self.state.load(.acquire)).lifecycle == .complete;
    }

    /// Check if task is effectively cancelled (cancelled AND not shielded)
    pub fn isCancelled(self: *Header) bool {
        const state = State.fromU64(self.state.load(.acquire));
        return state.isCancellationEffective();
    }

    /// Check if cancellation was requested (ignoring shield)
    pub fn isCancellationRequested(self: *Header) bool {
        return State.fromU64(self.state.load(.acquire)).cancelled;
    }

    pub fn isNotified(self: *Header) bool {
        return State.fromU64(self.state.load(.acquire)).notified;
    }

    pub fn clearNotified(self: *Header) bool {
        var current = State.fromU64(self.state.load(.acquire));
        while (current.notified) {
            var new = current;
            new.notified = false;
            if (self.state.cmpxchgWeak(current.toU64(), new.toU64(), .acq_rel, .acquire)) |updated| {
                current = State.fromU64(updated);
                continue;
            }
            return true;
        }
        return false;
    }

    /// Request cancellation
    pub fn cancel(self: *Header) bool {
        var current = State.fromU64(self.state.load(.acquire));
        while (true) {
            if (current.cancelled or current.lifecycle == .complete) {
                return false;
            }

            var new = current;
            new.cancelled = true;

            if (self.state.cmpxchgWeak(current.toU64(), new.toU64(), .acq_rel, .acquire)) |updated| {
                current = State.fromU64(updated);
                continue;
            }
            return true;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Shield-based Cancellation
    // ─────────────────────────────────────────────────────────────────────────

    pub fn beginShield(self: *Header) void {
        _ = self.state.fetchAdd(State.SHIELD_ONE, .acq_rel);
    }

    pub fn endShield(self: *Header) void {
        _ = self.state.fetchSub(State.SHIELD_ONE, .acq_rel);
    }

    pub fn shieldDepth(self: *Header) u8 {
        return State.fromU64(self.state.load(.acquire)).shield_depth;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Operations
    // ─────────────────────────────────────────────────────────────────────────

    /// Poll the task (run one step)
    pub fn poll(self: *Header) PollResult_ {
        return self.vtable.poll(self);
    }

    /// Schedule the task for execution (adds ref, for I/O wakers)
    pub fn schedule(self: *Header) void {
        self.vtable.schedule(self);
    }

    /// Reschedule the task (no ref change, for sync primitives waking)
    pub fn reschedule(self: *Header) void {
        self.vtable.reschedule(self);
    }

    /// Drop the task (clean up resources)
    pub fn drop(self: *Header) void {
        self.vtable.drop(self);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Waker Creation
    // ─────────────────────────────────────────────────────────────────────────

    /// Create a waker for this task
    pub fn waker(self: *Header) Waker {
        return .{
            .raw = .{
                .data = self,
                .vtable = &task_waker_vtable,
            },
        };
    }

    const task_waker_vtable = RawWaker.VTable{
        .wake = taskWake,
        .wake_by_ref = taskWakeByRef,
        .clone = taskWakerClone,
        .drop = taskWakerDrop,
    };

    fn taskWake(data: *anyopaque) void {
        const header: *Header = @ptrCast(@alignCast(data));
        header.schedule();
        // wake consumes, so drop the waker's ref
        if (header.unref()) {
            header.drop();
        }
    }

    fn taskWakeByRef(data: *anyopaque) void {
        const header: *Header = @ptrCast(@alignCast(data));
        header.schedule();
    }

    fn taskWakerClone(data: *anyopaque) RawWaker {
        const header: *Header = @ptrCast(@alignCast(data));
        header.ref();
        return .{ .data = data, .vtable = &task_waker_vtable };
    }

    fn taskWakerDrop(data: *anyopaque) void {
        const header: *Header = @ptrCast(@alignCast(data));
        if (header.unref()) {
            header.drop();
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// FutureTask - THE Task Type for Async Work
// ═══════════════════════════════════════════════════════════════════════════════

/// Task that wraps and polls a Future.
///
/// This is THE task type for async work. It properly polls the Future,
/// yielding when pending and resuming when its waker is called.
///
/// For blocking/CPU-intensive work, use BlockingPool.runBlocking() instead.
///
/// Usage:
/// ```zig
/// var my_future = mutex.lock();
/// var task = try FutureTask(LockFuture).create(allocator, my_future);
/// scheduler.spawn(&task.header);
/// ```
pub fn FutureTask(comptime F: type) type {
    const Output = F.Output;

    return struct {
        const Self = @This();

        /// Task header (must be first for pointer casting)
        header: Header,

        /// The future being polled
        future: F,

        /// Result storage (for non-void outputs)
        result: ?Output = null,

        /// Allocator for cleanup
        allocator: Allocator,

        /// Scheduler reference
        scheduler: ?*anyopaque = null,
        /// Callback for scheduling (adds ref)
        schedule_fn: ?*const fn (*anyopaque, *Header) void = null,
        /// Callback for rescheduling (no ref change)
        reschedule_fn: ?*const fn (*anyopaque, *Header) void = null,

        const vtable = Header.VTable{
            .poll = pollImpl,
            .drop = dropImpl,
            .schedule = scheduleImpl,
            .reschedule = rescheduleImpl,
        };

        /// Create a new future task
        pub fn create(allocator: Allocator, future: F) !*Self {
            const self = try allocator.create(Self);
            errdefer allocator.destroy(self);

            self.* = .{
                .header = Header.init(&vtable),
                .future = future,
                .allocator = allocator,
            };

            return self;
        }

        /// Set scheduler callbacks
        pub fn setScheduler(
            self: *Self,
            scheduler: *anyopaque,
            schedule_fn: *const fn (*anyopaque, *Header) void,
            reschedule_fn: *const fn (*anyopaque, *Header) void,
        ) void {
            self.scheduler = scheduler;
            self.schedule_fn = schedule_fn;
            self.reschedule_fn = reschedule_fn;
        }

        // ─────────────────────────────────────────────────────────────────────
        // VTable Implementations
        // ─────────────────────────────────────────────────────────────────────

        fn pollImpl(header: *Header) Header.PollResult_ {
            const self: *Self = @fieldParentPtr("header", header);

            // Check cancellation
            if (header.isCancelled()) {
                return .complete;
            }

            // Create waker and context for this poll
            const task_waker = header.waker();
            var ctx = Context{ .waker = &task_waker };

            // Poll the future
            const poll_result = self.future.poll(&ctx);

            if (poll_result.isReady()) {
                // Store result for non-void types
                if (Output != void) {
                    self.result = poll_result.unwrap();
                }
                return .complete;
            } else if (ctx.budget_exhausted) {
                // Cooperative budget exhausted - signal yield so the scheduler
                // routes this task to the global queue instead of the LIFO slot.
                // This prevents a budget-yielding task from monopolizing the worker.
                return .yield;
            } else {
                // Future is pending.
                // No need to set any flag - the scheduler will:
                // 1. Transition RUNNING -> IDLE atomically
                // 2. Check if notified was set during poll
                // 3. Reschedule if notified, otherwise task waits for waker
                return .pending;
            }
        }

        fn dropImpl(header: *Header) void {
            const self: *Self = @fieldParentPtr("header", header);
            // Deinit the future if it has a deinit
            if (@hasDecl(F, "deinit")) {
                self.future.deinit();
            }
            self.allocator.destroy(self);
        }

        /// Schedule callback - invoked by waker.
        /// Protocol:
        /// - If IDLE: transitions to SCHEDULED and queues task
        /// - If RUNNING/SCHEDULED: sets notified bit
        ///
        /// NOTE: Does NOT add ref. The waker clone already holds a ref
        /// that keeps the task alive. The waker's ref is released when
        /// the future's stored_waker is deinited (on completion).
        fn scheduleImpl(header: *Header) void {
            const self: *Self = @fieldParentPtr("header", header);
            if (debug_task) {
                const sched_ptr: usize = if (self.scheduler) |s| @intFromPtr(s) else 0;
                const fn_ptr: usize = if (self.schedule_fn) |f| @intFromPtr(f) else 0;
                std.debug.print("[TASK] scheduleImpl: header={x}, scheduler={x}, schedule_fn={x}\n", .{
                    @intFromPtr(header),
                    sched_ptr,
                    fn_ptr,
                });
            }
            if (self.scheduler) |sched| {
                if (self.schedule_fn) |schedule_fn| {
                    // transitionToScheduled handles the state machine:
                    // - Returns true if task should be queued (IDLE -> SCHEDULED)
                    // - Returns false if just set notified (RUNNING/SCHEDULED)
                    const should_queue = header.transitionToScheduled();
                    if (debug_task) {
                        std.debug.print("[TASK] scheduleImpl: transitionToScheduled returned {}\n", .{should_queue});
                    }
                    if (should_queue) {
                        // No ref increment - waker clone already holds a ref
                        if (debug_task) {
                            std.debug.print("[TASK] scheduleImpl: calling schedule_fn to enqueue task\n", .{});
                        }
                        schedule_fn(sched, header);
                        if (debug_task) {
                            std.debug.print("[TASK] scheduleImpl: schedule_fn returned\n", .{});
                        }
                    }
                }
            } else if (debug_task) {
                std.debug.print("[TASK] scheduleImpl: ERROR - no scheduler!\n", .{});
            }
        }

        /// Reschedule callback - same as schedule but doesn't add ref.
        /// Used when task is already referenced (e.g., sync primitives).
        fn rescheduleImpl(header: *Header) void {
            const self: *Self = @fieldParentPtr("header", header);
            if (self.scheduler) |sched| {
                if (self.reschedule_fn) |reschedule_fn| {
                    if (header.transitionToScheduled()) {
                        // No ref increment - task already has ref
                        reschedule_fn(sched, header);
                    }
                }
            }
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task Queue (Intrusive MPSC)
// ═══════════════════════════════════════════════════════════════════════════════

/// Simple single-producer single-consumer queue (for local worker queues)
pub const TaskQueue = struct {
    head: ?*Header = null,
    tail: ?*Header = null,
    len: usize = 0,

    pub fn init() TaskQueue {
        return .{};
    }

    /// Push a task to the back
    pub fn push(self: *TaskQueue, task: *Header) void {
        task.next = null;
        task.prev = self.tail;

        if (self.tail) |t| {
            t.next = task;
        } else {
            self.head = task;
        }
        self.tail = task;
        self.len += 1;
    }

    /// Pop a task from the front
    pub fn pop(self: *TaskQueue) ?*Header {
        const head = self.head orelse return null;

        self.head = head.next;
        if (self.head) |h| {
            h.prev = null;
        } else {
            self.tail = null;
        }

        head.next = null;
        head.prev = null;
        self.len -= 1;

        return head;
    }

    /// Check if empty
    pub fn isEmpty(self: *const TaskQueue) bool {
        return self.head == null;
    }

    /// Get length
    pub fn length(self: *const TaskQueue) usize {
        return self.len;
    }
};

/// Thread-safe task queue (for global queue).
/// Enhanced with batch operations and lock-free length for fast emptiness checks.
pub const GlobalTaskQueue = struct {
    queue: TaskQueue = .{},
    mutex: thr.Mutex = .{},
    /// Atomic length for lock-free emptiness/length checks.
    /// Updated inside the lock, but readable without locking.
    atomic_len: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    pub fn init() GlobalTaskQueue {
        return .{};
    }

    pub fn push(self: *GlobalTaskQueue, task: *Header) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.queue.push(task);
        _ = self.atomic_len.fetchAdd(1, .release);
    }

    pub fn pushBatch(self: *GlobalTaskQueue, tasks: []const *Header) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (tasks) |task| {
            self.queue.push(task);
        }
        _ = self.atomic_len.fetchAdd(tasks.len, .release);
    }

    pub fn pop(self: *GlobalTaskQueue) ?*Header {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.queue.pop()) |task| {
            _ = self.atomic_len.fetchSub(1, .release);
            return task;
        }
        return null;
    }

    /// Pop up to n tasks in a SINGLE lock acquisition.
    /// Returns number of tasks popped into `out`.
    pub fn popBatch(self: *GlobalTaskQueue, out: []*Header, n: usize) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        var count: usize = 0;
        while (count < n) {
            if (self.queue.pop()) |task| {
                out[count] = task;
                count += 1;
            } else break;
        }
        if (count > 0) {
            _ = self.atomic_len.fetchSub(count, .release);
        }
        return count;
    }

    /// Lock-free emptiness check (may be slightly stale).
    pub fn isEmptyFast(self: *const GlobalTaskQueue) bool {
        return self.atomic_len.load(.acquire) == 0;
    }

    /// Lock-free length (may be slightly stale).
    pub fn lengthFast(self: *const GlobalTaskQueue) usize {
        return self.atomic_len.load(.acquire);
    }

    /// Locked emptiness check (exact).
    pub fn isEmpty(self: *GlobalTaskQueue) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.queue.isEmpty();
    }

    /// Locked length (exact).
    pub fn length(self: *GlobalTaskQueue) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.queue.length();
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// WorkStealQueue - Lock-Free Ring Buffer
// ═══════════════════════════════════════════════════════════════════════════════

/// Lock-free single-producer, multi-consumer work-stealing queue.
///
/// Design:
/// - Fixed 256-slot ring buffer (no dynamic growth)
/// - Owner pushes to tail (back), pops from head (front) -- FIFO
/// - Stealers steal half from head into their own queue
/// - Overflow goes to global queue when full
/// - LIFO behavior comes from the separate lifo_slot in Worker, not this queue
///
/// The head is packed as two u32s into a single u64 for atomic CAS:
///   [steal_head (upper 32) | real_head (lower 32)]
/// When steal == real: no stealer active.
/// When steal != real: steal in progress, producer must not overflow past steal_head.
pub const WorkStealQueue = struct {
    const Self = @This();

    pub const CAPACITY: u32 = 256;
    const MASK: u32 = CAPACITY - 1;

    const CACHE_LINE_SIZE = @import("../util/cacheline.zig").CACHE_LINE_SIZE;

    /// Packed head: [steal_head (upper 32) | real_head (lower 32)]
    head: std.atomic.Value(u64) align(CACHE_LINE_SIZE) = std.atomic.Value(u64).init(pack(0, 0)),

    /// Tail: only the owning worker writes this
    tail: std.atomic.Value(u32) align(CACHE_LINE_SIZE) = std.atomic.Value(u32).init(0),

    /// Fixed-size ring buffer of task pointers (cache-line aligned to avoid false sharing with tail)
    buffer: [CAPACITY]std.atomic.Value(?*Header) align(CACHE_LINE_SIZE) = init_buffer(),

    fn init_buffer() [CAPACITY]std.atomic.Value(?*Header) {
        var buf: [CAPACITY]std.atomic.Value(?*Header) = undefined;
        for (&buf) |*slot| {
            slot.* = std.atomic.Value(?*Header).init(null);
        }
        return buf;
    }

    pub fn init() Self {
        return .{};
    }

    pub inline fn pack(steal: u32, real: u32) u64 {
        return (@as(u64, steal) << 32) | @as(u64, real);
    }

    pub inline fn unpack(val: u64) struct { steal: u32, real: u32 } {
        return .{
            .steal = @truncate(val >> 32),
            .real = @truncate(val),
        };
    }

    /// Approximate length (tail - steal_head). May be stale.
    pub fn len(self: *const Self) u32 {
        const head = unpack(self.head.load(.acquire));
        const tail = self.tail.load(.acquire);
        return tail -% head.steal;
    }

    /// Approximate emptiness check.
    pub fn isEmpty(self: *const Self) bool {
        return self.len() == 0;
    }

    /// Push a task to the tail of the queue (owner thread only).
    /// If the queue is full, overflows half to global_queue then pushes.
    pub fn push(self: *Self, task: *Header, global_queue: *GlobalTaskQueue) void {
        var tail = self.tail.load(.monotonic);

        while (true) {
            const head_packed = self.head.load(.acquire);
            const head = unpack(head_packed);

            if (tail -% head.steal < CAPACITY) {
                // Room available
                self.buffer[tail & MASK].store(task, .release);
                self.tail.store(tail +% 1, .release);
                return;
            }

            if (head.steal != head.real) {
                // Stealer active — can't overflow, push to global
                global_queue.push(task);
                return;
            }

            // No stealer — overflow half to global, then retry
            const half = CAPACITY / 2;
            const new_head = pack(head.steal +% half, head.real +% half);
            if (self.head.cmpxchgWeak(head_packed, new_head, .release, .acquire)) |_| {
                tail = self.tail.load(.monotonic);
                continue;
            }

            // Push overflowed tasks to global queue
            var i: u32 = 0;
            while (i < half) : (i += 1) {
                const idx = (head.real +% i) & MASK;
                const t = self.buffer[idx].load(.acquire) orelse continue;
                global_queue.push(t);
            }

            self.buffer[tail & MASK].store(task, .release);
            self.tail.store(tail +% 1, .release);
            return;
        }
    }

    /// Pop a task from the head of the queue (owner thread only, FIFO).
    /// Pops from the front for fairness.
    /// LIFO behavior comes from the separate lifo_slot, not this queue.
    pub fn pop(self: *Self) ?*Header {
        var head_packed = self.head.load(.acquire);

        while (true) {
            const head = unpack(head_packed);
            const tail = self.tail.load(.monotonic);

            if (head.real == tail) {
                // Queue is empty
                return null;
            }

            const idx = head.real & MASK;

            // Advance real_head (and steal_head if no active stealer)
            const next_real = head.real +% 1;
            const new_head = if (head.steal == head.real)
                pack(next_real, next_real)
            else
                pack(head.steal, next_real);

            if (self.head.cmpxchgWeak(head_packed, new_head, .release, .acquire)) |actual| {
                head_packed = actual;
                continue;
            }

            return self.buffer[idx].load(.acquire);
        }
    }

    /// Steal half of src's tasks into dst queue.
    /// Returns one task directly (the first stolen), rest go into dst.
    /// Returns null if src is empty or steal fails.
    ///
    /// Three-phase steal protocol:
    /// 1. CAS src.head to claim half (advance real_head, keep steal_head)
    /// 2. Copy tasks from src buffer to dst buffer
    /// 3. CAS src.head to release (advance steal_head to match real_head)
    pub fn stealInto(src: *Self, dst: *Self) ?*Header {
        const dst_tail = dst.tail.load(.monotonic);

        const src_head_packed = src.head.load(.acquire);
        const src_head = unpack(src_head_packed);

        // A steal is already in progress
        if (src_head.steal != src_head.real) return null;

        const src_tail = src.tail.load(.acquire);

        // Calculate available tasks (wrapping subtraction)
        const available = src_tail -% src_head.real;
        if (available == 0) return null;

        // Steal half (ceiling), capped to half capacity
        var to_steal = available -% (available / 2);
        if (to_steal > CAPACITY / 2) to_steal = CAPACITY / 2;

        // Check dst has room (need to_steal - 1 slots since first goes to caller)
        const dst_head_packed = dst.head.load(.acquire);
        const dst_head = unpack(dst_head_packed);
        if (to_steal > 1) {
            const dst_space = CAPACITY -% (dst_tail -% dst_head.steal);
            const dst_needed = to_steal - 1;
            if (dst_needed > dst_space) {
                to_steal = dst_space + 1;
            }
        }
        if (to_steal == 0) return null;

        // Phase 1: CAS to claim — advance real_head, keep steal_head
        const new_src_head = pack(src_head.steal, src_head.real +% to_steal);
        if (src.head.cmpxchgWeak(src_head_packed, new_src_head, .acq_rel, .acquire)) |_| {
            return null; // Lost race
        }

        // Phase 2: Copy tasks
        // First task goes directly to caller
        const first_task = src.buffer[src_head.real & MASK].load(.acquire);

        // Remaining tasks go into dst's buffer starting at dst_tail
        var i: u32 = 1;
        while (i < to_steal) : (i += 1) {
            const src_idx = (src_head.real +% i) & MASK;
            const dst_idx = (dst_tail +% (i - 1)) & MASK;
            const task = src.buffer[src_idx].load(.acquire);
            dst.buffer[dst_idx].store(task, .release);
        }

        // Update dst tail
        if (to_steal > 1) {
            dst.tail.store(dst_tail +% (to_steal - 1), .release);
        }

        // Phase 3: Release — advance steal_head to match real_head
        var release_head_packed = src.head.load(.acquire);
        while (true) {
            const release_head = unpack(release_head_packed);
            const final_head = pack(release_head.real, release_head.real);
            if (src.head.cmpxchgWeak(release_head_packed, final_head, .acq_rel, .acquire)) |actual| {
                release_head_packed = actual;
                continue;
            }
            break;
        }

        return first_task;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test "State - pack/unpack" {
    var state = State{};
    state.lifecycle = .running;
    state.notified = true;
    state.ref_count = 5;

    const packed_val = state.toU64();
    const unpacked = State.fromU64(packed_val);

    try std.testing.expectEqual(Lifecycle.running, unpacked.lifecycle);
    try std.testing.expect(unpacked.notified);
    try std.testing.expectEqual(@as(u32, 5), unpacked.ref_count);
}

test "Header - state transitions" {
    const vtable = Header.VTable{
        .poll = struct {
            fn f(_: *Header) Header.PollResult_ {
                return .pending;
            }
        }.f,
        .drop = struct {
            fn f(_: *Header) void {}
        }.f,
        .schedule = struct {
            fn f(_: *Header) void {}
        }.f,
        .reschedule = struct {
            fn f(_: *Header) void {}
        }.f,
    };

    var header = Header.init(&vtable);

    // idle -> scheduled
    try std.testing.expect(header.transitionToScheduled());
    try std.testing.expectEqual(Lifecycle.scheduled, State.fromU64(header.state.load(.acquire)).lifecycle);

    // scheduled -> running
    try std.testing.expect(header.transitionToRunning());
    try std.testing.expectEqual(Lifecycle.running, State.fromU64(header.state.load(.acquire)).lifecycle);

    // running -> idle
    _ = header.transitionToIdle();
    try std.testing.expectEqual(Lifecycle.idle, State.fromU64(header.state.load(.acquire)).lifecycle);
}

test "Header - reference counting" {
    const vtable = Header.VTable{
        .poll = struct {
            fn f(_: *Header) Header.PollResult_ {
                return .pending;
            }
        }.f,
        .drop = struct {
            fn f(_: *Header) void {}
        }.f,
        .schedule = struct {
            fn f(_: *Header) void {}
        }.f,
        .reschedule = struct {
            fn f(_: *Header) void {}
        }.f,
    };

    var header = Header.init(&vtable);

    // Initial ref count is 1
    try std.testing.expectEqual(@as(u32, 1), State.fromU64(header.state.load(.acquire)).ref_count);

    header.ref();
    try std.testing.expectEqual(@as(u32, 2), State.fromU64(header.state.load(.acquire)).ref_count);

    try std.testing.expect(!header.unref()); // Not last ref
    try std.testing.expectEqual(@as(u32, 1), State.fromU64(header.state.load(.acquire)).ref_count);

    try std.testing.expect(header.unref()); // Last ref
}

test "Header - waker" {
    const vtable = Header.VTable{
        .poll = struct {
            fn f(_: *Header) Header.PollResult_ {
                return .pending;
            }
        }.f,
        .drop = struct {
            fn f(_: *Header) void {}
        }.f,
        .schedule = struct {
            fn f(_: *Header) void {}
        }.f,
        .reschedule = struct {
            fn f(_: *Header) void {}
        }.f,
    };

    var header = Header.init(&vtable);

    const w = header.waker();
    try std.testing.expect(State.fromU64(header.state.load(.acquire)).ref_count == 1);

    // Clone should increment ref
    const w2 = w.clone();
    try std.testing.expect(State.fromU64(header.state.load(.acquire)).ref_count == 2);

    // Wake by ref should not change ref count
    w2.wakeByRef();
    try std.testing.expect(State.fromU64(header.state.load(.acquire)).ref_count == 2);
}

test "TaskQueue - push/pop" {
    var queue = TaskQueue.init();

    const vtable = Header.VTable{
        .poll = struct {
            fn f(_: *Header) Header.PollResult_ {
                return .pending;
            }
        }.f,
        .drop = struct {
            fn f(_: *Header) void {}
        }.f,
        .schedule = struct {
            fn f(_: *Header) void {}
        }.f,
        .reschedule = struct {
            fn f(_: *Header) void {}
        }.f,
    };

    var h1 = Header.init(&vtable);
    var h2 = Header.init(&vtable);
    var h3 = Header.init(&vtable);

    queue.push(&h1);
    queue.push(&h2);
    queue.push(&h3);

    try std.testing.expectEqual(&h1, queue.pop());
    try std.testing.expectEqual(&h2, queue.pop());
    try std.testing.expectEqual(&h3, queue.pop());
    try std.testing.expectEqual(@as(?*Header, null), queue.pop());
}

test "WorkStealQueue - push/pop FIFO" {
    var global = GlobalTaskQueue.init();
    var q = WorkStealQueue.init();

    const vtable = Header.VTable{
        .poll = struct {
            fn f(_: *Header) Header.PollResult_ {
                return .pending;
            }
        }.f,
        .drop = struct {
            fn f(_: *Header) void {}
        }.f,
        .schedule = struct {
            fn f(_: *Header) void {}
        }.f,
        .reschedule = struct {
            fn f(_: *Header) void {}
        }.f,
    };

    var headers: [4]Header = undefined;
    for (&headers) |*h| {
        h.* = Header.init(&vtable);
    }

    // Push 4 tasks
    for (&headers) |*h| {
        q.push(h, &global);
    }
    try std.testing.expectEqual(@as(u32, 4), q.len());

    // Pop returns FIFO (oldest first — LIFO behavior comes from lifo_slot)
    try std.testing.expectEqual(&headers[0], q.pop().?);
    try std.testing.expectEqual(&headers[1], q.pop().?);
    try std.testing.expectEqual(&headers[2], q.pop().?);
    try std.testing.expectEqual(&headers[3], q.pop().?);
    try std.testing.expectEqual(@as(?*Header, null), q.pop());
}

test "WorkStealQueue - stealInto" {
    var global = GlobalTaskQueue.init();
    var src = WorkStealQueue.init();
    var dst = WorkStealQueue.init();

    const vtable = Header.VTable{
        .poll = struct {
            fn f(_: *Header) Header.PollResult_ {
                return .pending;
            }
        }.f,
        .drop = struct {
            fn f(_: *Header) void {}
        }.f,
        .schedule = struct {
            fn f(_: *Header) void {}
        }.f,
        .reschedule = struct {
            fn f(_: *Header) void {}
        }.f,
    };

    // Push 8 tasks to src
    var headers: [8]Header = undefined;
    for (&headers) |*h| {
        h.* = Header.init(&vtable);
    }
    for (&headers) |*h| {
        src.push(h, &global);
    }

    // Steal half into dst
    const stolen = WorkStealQueue.stealInto(&src, &dst);
    try std.testing.expect(stolen != null); // Got one task directly

    // src should have ~half remaining
    try std.testing.expect(src.len() <= 4);
    // dst should have stolen - 1 tasks
    try std.testing.expect(dst.len() >= 1);
}

test "WorkStealQueue - empty steal" {
    var src = WorkStealQueue.init();
    var dst = WorkStealQueue.init();

    try std.testing.expectEqual(@as(?*Header, null), WorkStealQueue.stealInto(&src, &dst));
}

test "WorkStealQueue - pack/unpack roundtrip" {
    const packed_val = WorkStealQueue.pack(42, 99);
    const unpacked = WorkStealQueue.unpack(packed_val);
    try std.testing.expectEqual(@as(u32, 42), unpacked.steal);
    try std.testing.expectEqual(@as(u32, 99), unpacked.real);
}

// Note: Task(F, Args) was removed - blocking functions use BlockingPool.runBlocking()
// FutureTask is the only task type for async work on the scheduler
