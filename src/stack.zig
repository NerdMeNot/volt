//! Coroutine stack allocation backed by `mmap`, with a PROT_NONE
//! guard page at the bottom and grow-on-demand pages above it.
//!
//! Layout (low → high address):
//!
//! ```
//!   base ── guard (1 page, PROT_NONE) ── growable (PROT_NONE) ── body (PROT_RW)
//!           |<─── guardSize() ───>|<──── growable region ────>|<── BODY_SIZE ──>|
//!           base                                                                top
//! ```
//!
//! Total reservation = `RESERVATION_SIZE`. Only the top `BODY_SIZE`
//! is committed at allocation. SP starts at the very top and grows
//! down. When SP walks into a PROT_NONE page (anywhere except the
//! bottom guard), the SIGSEGV handler in `signal.zig` mprotects
//! that page to PROT_RW and resumes — auto-grow without explicit
//! reallocation. If SP hits the bottom guard, that's real overflow:
//! the handler chains to the default and the process aborts with
//! a stack trace.
//!
//! ## Why mmap + signal handler
//!
//! Pure alignedAlloc + per-spawn mprotect (the original Phase 4a
//! attempt) regressed multi-worker bench-spawn-hot ~8× because
//! mprotect serializes on the process VM lock. This design:
//!
//!   - allocates each stack with one `mmap` + one `mprotect` (top
//!     body region) — twice the syscall cost up front, amortized
//!     into the pool, never touched on pool-hit/pool-push.
//!   - grows under SIGSEGV via mprotect (POSIX async-signal-safe).
//!     This is per-page lazy growth; only fires when a coroutine
//!     actually needs more stack than 16 KiB.
//!
//! ## Memory cost
//!
//! Per stack:
//!   - Virtual address space: `RESERVATION_SIZE` (256 KiB default)
//!   - Committed RSS at idle: `BODY_SIZE` (16 KiB), growing in
//!     page increments as the coroutine uses more stack.
//!
//! With 16 KiB pages on Darwin, the initial body is one page.
//! On Linux (4 KiB pages) it's four. Either way the growable region
//! above the guard is 256 KiB - guard - BODY_SIZE; that's the
//! recursion budget before clean abort.

const std = @import("std");
const signal = @import("signal.zig");

/// Opaque base pointer for a stack reservation. Carries enough
/// alignment to satisfy `mmap` page alignment. Coroutine and pool
/// both hold stacks as `StackPtr` — no slice, the size is implicit
/// from `totalSize()`.
pub const StackPtr = [*]align(std.heap.page_size_max) u8;

/// Top-of-stack usable size — pages at the top of the reservation
/// committed RW at allocation time. SP starts at `base + totalSize()`
/// and grows down through this region first.
pub const BODY_SIZE: usize = 16 * 1024;

/// Total per-stack virtual reservation. The growable region between
/// the bottom guard and the initial body is `RESERVATION_SIZE -
/// guardSize() - BODY_SIZE` bytes (≈ 224 KiB on Darwin). Setting
/// this large gives more recursion budget; setting it small saves
/// virtual address space per stack.
pub const RESERVATION_SIZE: usize = 256 * 1024;

/// Guard-page size — one page at the runtime's actual page size.
pub inline fn guardSize() usize {
    return std.heap.pageSize();
}

/// Total mmap reservation length per stack.
pub inline fn totalSize() usize {
    return RESERVATION_SIZE;
}

/// Where the usable region begins relative to the mapping base.
/// The pool's intrusive next-pointer lives at this offset because
/// the bottom page is PROT_NONE (writing there would SIGSEGV).
///
/// **IMPORTANT**: this is *not* where SP starts. SP starts at
/// `base + totalSize()`. `usableOffset()` is just the lowest byte
/// that could ever be touched by the coroutine — used to place the
/// pool's intrusive next-pointer in a page that the signal handler
/// will already have grown if anyone has used the stack.
///
/// We store the pool's next-pointer in the top page (initial body),
/// not in this very first usable page, because the very first usable
/// page above the guard is uncommitted (PROT_NONE) on a fresh
/// alloc.
pub inline fn usableOffset() usize {
    return totalSize() - BODY_SIZE;
}

// Direct libc bindings. `std.posix.{mmap,munmap,mprotect}` were
// removed in Zig 0.16; we go to libc.
const mmap_fn = @extern(
    *const fn (
        addr: ?*anyopaque,
        len: usize,
        prot: c_int,
        flags: c_int,
        fd: c_int,
        offset: i64,
    ) callconv(.c) ?*anyopaque,
    .{ .name = "mmap" },
);

const munmap_fn = @extern(
    *const fn (addr: *anyopaque, len: usize) callconv(.c) c_int,
    .{ .name = "munmap" },
);

const mprotect_fn = @extern(
    *const fn (addr: *anyopaque, len: usize, prot: c_int) callconv(.c) c_int,
    .{ .name = "mprotect" },
);

const PROT_NONE: c_int = 0;
const PROT_READ: c_int = 1;
const PROT_WRITE: c_int = 2;
const MAP_PRIVATE: c_int = 0x0002;
const MAP_ANON: c_int = switch (@import("builtin").os.tag) {
    .macos, .ios, .tvos, .watchos => 0x1000,
    else => 0x20, // Linux, FreeBSD, etc.
};

const MAP_FAILED: ?*anyopaque = @ptrFromInt(@as(usize, @bitCast(@as(isize, -1))));

pub const Error = error{
    MmapFailed,
    MprotectFailed,
};

/// Allocate a guarded grow-on-demand stack. Reserves
/// `RESERVATION_SIZE` virtual bytes as PROT_NONE, then commits the
/// top `BODY_SIZE` to PROT_READ|PROT_WRITE. The bottom page stays
/// PROT_NONE (the guard); the middle region is PROT_NONE and grown
/// lazily by the SIGSEGV handler in `signal.zig`.
///
/// Registers the region with the signal handler. The signal handler
/// is itself installed lazily on first call — every `Runtime.init`
/// ensures it's in place before spawning anything.
pub fn alloc() Error!StackPtr {
    signal.ensureInstalled();

    const total = totalSize();
    const ret = mmap_fn(null, total, PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (ret == MAP_FAILED or ret == null) return error.MmapFailed;
    const base: [*]u8 = @ptrCast(ret.?);

    // Initial body — top BODY_SIZE bytes, PROT_RW.
    const body_offset = total - BODY_SIZE;
    const body_ptr: *anyopaque = @ptrCast(base + body_offset);
    const rc = mprotect_fn(body_ptr, BODY_SIZE, PROT_READ | PROT_WRITE);
    if (rc != 0) {
        _ = munmap_fn(ret.?, total);
        return error.MprotectFailed;
    }

    // Register with the signal handler so on-demand growth works
    // for this stack. Registry-full is non-fatal — the stack still
    // works for up to BODY_SIZE; deeper recursion will SIGSEGV
    // cleanly instead of growing.
    signal.register(@intFromPtr(base), total) catch {};

    return @alignCast(base);
}

/// Release a guarded stack back to the OS. Called from
/// `P.drainPools` at runtime shutdown and pool-cap eviction — never
/// on the spawn/free hot path.
pub fn free(base: StackPtr) void {
    signal.unregister(@intFromPtr(base));
    _ = munmap_fn(@ptrCast(base), totalSize());
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

test "stack.alloc returns page-aligned base" {
    const s = try alloc();
    defer free(s);
    try std.testing.expect(@intFromPtr(s) % std.heap.pageSize() == 0);
}

test "stack body (top BODY_SIZE) is writable" {
    const s = try alloc();
    defer free(s);
    const body_start = s + usableOffset();
    body_start[0] = 0xab;
    body_start[BODY_SIZE - 1] = 0xcd;
    try std.testing.expectEqual(@as(u8, 0xab), body_start[0]);
    try std.testing.expectEqual(@as(u8, 0xcd), body_start[BODY_SIZE - 1]);
}

test "stack grows on access to a PROT_NONE growable page" {
    const s = try alloc();
    defer free(s);
    const ps = std.heap.pageSize();
    // A page below the initial body — uncommitted, PROT_NONE on
    // alloc. Writing should trigger the SIGSEGV handler which
    // mprotects it RW and resumes.
    const grow_addr = s + usableOffset() - ps;
    grow_addr[0] = 0xee;
    try std.testing.expectEqual(@as(u8, 0xee), grow_addr[0]);
}

test "two stacks have distinct reservations" {
    const a = try alloc();
    defer free(a);
    const b = try alloc();
    defer free(b);
    try std.testing.expect(a != b);
    // Reservations don't overlap.
    const a_end = @intFromPtr(a) + totalSize();
    try std.testing.expect(@intFromPtr(b) >= a_end or @intFromPtr(b) + totalSize() <= @intFromPtr(a));
}
