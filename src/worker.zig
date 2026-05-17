//! M — OS thread (M:N scheduler, Phase 1).
//!
//! Owns the OS-thread-specific state: pthread handle, parker for
//! sleep/wake, main_ctx for ctx-swap return. The per-worker
//! scheduler state (run queue, lifo_slot, etc.) lives on `P` — see
//! `p.zig`.
//!
//! In Phase 1 of the M:N restructure, each M is 1:1-bound to a P
//! at startup and never detaches. Later phases introduce M ↔ P
//! detach/attach so syscalls don't stall a P's queue.
//!
//! This file also hosts the global `InjectionQueue` and the
//! threadlocal current-M tracking. Both will move out as the
//! restructure progresses (mailbox per P replaces injection in
//! Phase 2).

const std = @import("std");
const coroutine = @import("coroutine.zig");
const parker_mod = @import("parker.zig");
const context = @import("context.zig");
const p_mod = @import("p.zig");

pub const Coroutine = coroutine.Coroutine;
pub const Parker = parker_mod.Parker;
pub const P = p_mod.P;

/// Lock-free MPMC mailbox — Treiber stack threaded through
/// `Coroutine.next`. One per P (each P owns its mailbox; multiple
/// producers push to it, multiple consumers — owner P during
/// dispatch + sibling P's during stealing — pop from it).
///
/// Replaces the single global `InjectionQueue` from before Phase 2.
/// Now cross-P pushes target a specific P's mailbox instead of one
/// shared atomic head — reduces cache-line bouncing under high
/// cross-P traffic.
///
/// Where work lands:
///   * Spawn-from-coroutine, local-queue overflow → owner P's mailbox.
///   * Spawn-from-non-coroutine (driver pre-`run()`) → P[0]'s mailbox.
///   * `runtime.unpark` cross-thread wake → current M's P's mailbox.
///   * Dispatch's `.park` rescue path → current M's P's mailbox.
pub const Mailbox = struct {
    head: std.atomic.Value(?*Coroutine) = std.atomic.Value(?*Coroutine).init(null),

    pub fn push(self: *Mailbox, c: *Coroutine) void {
        var cur = self.head.load(.monotonic);
        while (true) {
            c.next = cur;
            if (self.head.cmpxchgWeak(cur, c, .release, .monotonic)) |observed| {
                cur = observed;
            } else return;
        }
    }

    pub fn pop(self: *Mailbox) ?*Coroutine {
        var cur = self.head.load(.acquire);
        while (cur) |c| {
            const next = c.next;
            if (self.head.cmpxchgWeak(cur, next, .acq_rel, .acquire)) |observed| {
                cur = observed;
            } else {
                c.next = null;
                return c;
            }
        }
        return null;
    }

    pub fn isEmpty(self: *const Mailbox) bool {
        return self.head.load(.acquire) == null;
    }
};

/// Threadlocal: the M currently running on this OS thread.
/// Set by `workerThreadEntry` / `Runtime.run` at startup; never
/// cleared during M's lifetime in Phase 1.
threadlocal var current_m: ?*anyopaque = null;

pub fn currentM() ?*anyopaque {
    return current_m;
}

pub fn currentMSet(m: ?*anyopaque) void {
    current_m = m;
}

pub const M = struct {
    /// The P this M is bound to. Phase 1: set once at init, never
    /// changes. Later phases will allow this to be `?*P` and
    /// support detach/attach.
    p: *P,
    main_ctx: context.Context = .{},
    parker: Parker = .{},
    thread: std.Thread = undefined,

    pub fn init(self: *M, p: *P) void {
        self.* = .{ .p = p };
        self.parker.init();
    }

    pub fn deinit(self: *M) void {
        self.parker.deinit();
    }
};
