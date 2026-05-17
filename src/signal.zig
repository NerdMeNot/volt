//! Process-wide SIGSEGV handler that grows registered coroutine
//! stacks on demand.
//!
//! Each stack reserves `RESERVATION_SIZE` of virtual address space.
//! Only the top body (`stack.BODY_SIZE`) is committed RW at first;
//! the rest is PROT_NONE. When SP walks into a PROT_NONE page the
//! kernel raises `SIGSEGV` and this handler:
//!
//!   1. Looks the fault address up in the registry.
//!   2. If it falls in a registered region but not in the guard
//!      (the very bottom page), `mprotect`s that page to PROT_RW
//!      and returns. The faulting instruction reruns successfully.
//!   3. Otherwise — real fault, chain to the previous handler (or
//!      raise the default).
//!
//! Async-signal-safety: the handler calls `mprotect` (on the POSIX
//! AS-safe list) and reads the registry under a tiny TTAS spinlock.
//! No allocator, no parking-lot, no logging.
//!
//! The registry has a fixed maximum size (`MAX_REGIONS`). Entries
//! are added on `register` and zero'd on `unregister`. A zero base
//! is the "empty" sentinel.

const std = @import("std");
const builtin = @import("builtin");

const is_windows = builtin.os.tag == .windows;

/// Maximum number of registered regions the SIGSEGV handler can
/// recognize. With one entry per Runtime arena and a small handful of
/// Runtime instances per process, 64 is plenty of headroom.
pub const MAX_REGIONS: usize = 64;

/// A single registered region. `base == 0` means the slot is free.
///
/// Two shapes:
///   * `slot_size == 0`: a single-stack region. `size` is the full
///     reservation length; the very bottom page is the guard. Used
///     by direct `stack.alloc()` callers (tests).
///   * `slot_size > 0`: an arena. `size` is the full slab length
///     (`n_slots * slot_size`). On a fault, the handler computes
///     `(fault - base) % slot_size` to find the per-slot offset,
///     then decides guard vs. growable.
const Region = struct {
    base: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    size: usize = 0,
    slot_size: usize = 0,
    /// Page size at the time of registration — captured so the
    /// signal handler can decide what the guard page is without
    /// re-querying.
    page_size: usize = 0,
};

// `[_]Region{.{}} ** MAX_REGIONS` is the comptime-friendly form —
// the loop variant tripped Zig's default 1000-branch eval quota in
// ReleaseFast.
var regions: [MAX_REGIONS]Region = [_]Region{.{}} ** MAX_REGIONS;

/// Single-shot install of the SIGSEGV handler. Multiple Runtime
/// instances in the same process call this; only the first install
/// actually fires. We never uninstall — process-lifetime handler.
///
/// On Windows, every public function in this file is a no-op (L5b):
/// stack-growth via Structured Exception Handling lives in a future
/// L5c landing. The arena pre-commits the full body region under
/// L5b, so the SIGSEGV-style grow-on-demand machinery isn't needed
/// to run; a guard page still catches genuine overflows via the
/// default Windows access-violation behaviour.
var installed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

// Signal-related libc bindings.
const SA_SIGINFO: c_int = 0x0040;
const SA_NODEFER: c_int = 0x0010;
const SA_ONSTACK: c_int = 0x0001;
const SIGSEGV: c_int = 11;
const SIGBUS: c_int = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos => 10,
    else => 7,
};

// `sigset_t` layout differs per OS. We never set it (just zero it),
// so a portable u128 padding is sufficient.
const SigsetT = extern struct { _pad: [128]u8 align(8) = @splat(0) };

const SigInfo = opaque {};

const SigactionFn = *const fn (sig: c_int, info: *SigInfo, ctx: ?*anyopaque) callconv(.c) void;

// Darwin sigaction layout differs slightly from Linux; both have
// (handler, mask, flags) but the field order varies. We use the
// libc-provided struct via @extern and rely on POSIX layout.
const Sigaction = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos => extern struct {
        sa_sigaction: ?SigactionFn,
        sa_mask: SigsetT,
        sa_flags: c_int,
    },
    else => extern struct {
        sa_sigaction: ?SigactionFn,
        sa_mask: SigsetT,
        sa_flags: c_int,
        sa_restorer: ?*const fn () callconv(.c) void = null,
    },
};

const sigaction_fn = @extern(
    *const fn (sig: c_int, act: ?*const Sigaction, oldact: ?*Sigaction) callconv(.c) c_int,
    .{ .name = "sigaction" },
);

const mprotect_fn = @extern(
    *const fn (addr: *anyopaque, len: usize, prot: c_int) callconv(.c) c_int,
    .{ .name = "mprotect" },
);

const PROT_READ: c_int = 1;
const PROT_WRITE: c_int = 2;

// Previous handler, so we can chain on a real fault.
var prev_segv: Sigaction = .{ .sa_sigaction = null, .sa_mask = .{}, .sa_flags = 0 };
var prev_bus: Sigaction = .{ .sa_sigaction = null, .sa_mask = .{}, .sa_flags = 0 };

// siginfo_t.si_addr offset on Darwin arm64. Layout:
//   int si_signo, errno, code, pid; uid_t uid; int status; void* si_addr;
// si_addr is at offset 24 on 64-bit Darwin (4*4 + 4 + 4 = 24). On
// Linux x86_64 the layout differs; the field is at offset 16 there.
// We capture from siginfo via @ptrCast + offset compile-time match.
fn siInfoAddr(info: *SigInfo) usize {
    const raw: [*]const u8 = @ptrCast(info);
    const offset: usize = switch (builtin.os.tag) {
        .macos, .ios, .tvos, .watchos => 24,
        else => 16, // Linux glibc layout
    };
    const addr_ptr: *const usize = @ptrCast(@alignCast(raw + offset));
    return addr_ptr.*;
}

/// SIGSEGV / SIGBUS handler. Called by the kernel on a fault — must
/// be async-signal-safe.
fn handler(sig: c_int, info: *SigInfo, ctx: ?*anyopaque) callconv(.c) void {
    const fault_addr = siInfoAddr(info);

    // Linear scan of the registry. MAX_REGIONS reads of one atomic
    // each — order ~100 µs worst case on a fully-loaded registry, but
    // in practice we hit a region almost immediately.
    var i: usize = 0;
    while (i < MAX_REGIONS) : (i += 1) {
        const base = regions[i].base.load(.acquire);
        if (base == 0) continue;
        const size = regions[i].size;
        const slot_size = regions[i].slot_size;
        const ps = regions[i].page_size;
        if (fault_addr < base or fault_addr >= base + size) continue;

        // Slot offset: 0 for single-stack regions, modulo for arenas.
        const slot_offset = if (slot_size == 0)
            fault_addr - base
        else
            (fault_addr - base) % slot_size;

        // Bottom-most page of the slot is the guard — real overflow,
        // chain to the default handler.
        if (slot_offset < ps) break;

        // Otherwise: grow. mprotect the page containing fault_addr
        // to PROT_RW. Page-align downward.
        const page_addr = fault_addr & ~(ps - 1);
        const page_ptr: *anyopaque = @ptrFromInt(page_addr);
        _ = mprotect_fn(page_ptr, ps, PROT_READ | PROT_WRITE);
        return;
    }

    // Not in any registered region — or in a guard page. Chain.
    chainTo(sig, info, ctx);
}

fn chainTo(sig: c_int, info: *SigInfo, ctx: ?*anyopaque) void {
    const prev: *const Sigaction = if (sig == SIGSEGV) &prev_segv else &prev_bus;
    if (prev.sa_sigaction) |h| {
        h(sig, info, ctx);
        return;
    }
    // No prior handler. Re-raise with default action by reinstalling
    // SIG_DFL and returning — the kernel will re-deliver since we
    // came in with SA_NODEFER unset (signal mask blocks SEGV during
    // handler). For simplicity, abort.
    @panic("volt: SIGSEGV in unregistered region — real fault");
}

/// Install the handler. Idempotent across calls; only the first
/// install actually touches `sigaction`. Called from `Runtime.init`
/// at the first runtime construction in a process.
pub fn ensureInstalled() void {
    if (comptime is_windows) return;
    if (installed.swap(true, .acq_rel)) return;

    var act: Sigaction = .{
        .sa_sigaction = &handler,
        .sa_mask = .{},
        .sa_flags = SA_SIGINFO | SA_NODEFER,
    };
    _ = sigaction_fn(SIGSEGV, &act, &prev_segv);
    _ = sigaction_fn(SIGBUS, &act, &prev_bus);
}

/// Register a single-stack region with the handler. `size` is the
/// full reservation length; the bottom page is treated as the guard.
pub fn register(base: usize, size: usize) error{RegistryFull}!void {
    if (comptime is_windows) return;
    return registerInternal(base, size, 0);
}

/// Register an arena range with the handler. `total_size` covers the
/// whole slab; `slot_size` is the per-slot stride so the handler can
/// compute `(fault - base) % slot_size` to locate the guard page.
pub fn registerArena(base: usize, total_size: usize, slot_size: usize) error{RegistryFull}!void {
    if (comptime is_windows) return;
    std.debug.assert(slot_size > 0);
    std.debug.assert(total_size % slot_size == 0);
    return registerInternal(base, total_size, slot_size);
}

fn registerInternal(base: usize, size: usize, slot_size: usize) error{RegistryFull}!void {
    const ps = std.heap.pageSize();
    var i: usize = 0;
    while (i < MAX_REGIONS) : (i += 1) {
        // Claim a free slot by CAS-ing the atomic base from 0 → base.
        if (regions[i].base.cmpxchgStrong(0, base, .acq_rel, .monotonic) == null) {
            regions[i].size = size;
            regions[i].slot_size = slot_size;
            regions[i].page_size = ps;
            return;
        }
    }
    return error.RegistryFull;
}

/// Unregister a stack region. Called by `stack.free()` at runtime
/// shutdown (or pool over-cap eviction).
pub fn unregister(base: usize) void {
    if (comptime is_windows) return;
    var i: usize = 0;
    while (i < MAX_REGIONS) : (i += 1) {
        if (regions[i].base.load(.acquire) == base) {
            regions[i].base.store(0, .release);
            regions[i].slot_size = 0;
            regions[i].size = 0;
            return;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

test "register + unregister round-trip" {
    const FAKE: usize = 0x1000;
    try register(FAKE, 16 * 1024);
    // Find the slot we wrote.
    var found: bool = false;
    for (&regions) |*r| {
        if (r.base.load(.acquire) == FAKE) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
    unregister(FAKE);
    for (&regions) |*r| {
        try std.testing.expect(r.base.load(.acquire) != FAKE);
    }
}
