//! Coroutine stack allocation backed by `mmap`, with a PROT_NONE
//! guard page below the usable region.
//!
//! Layout (low → high address):
//!
//! ```
//!   base ── guard page (1 page, PROT_NONE) ── usable body (BODY_SIZE)
//!           |<──── guardSize() bytes ────>|<──── BODY_SIZE ────>|
//!           base (returned)                                     top (SP starts here)
//! ```
//!
//! Stack overflow into the guard page raises `SIGSEGV` instead of
//! silently corrupting whatever lives next to the stack on the heap.
//!
//! ## Why mmap, not `alignedAlloc`
//!
//! An earlier attempt used `alignedAlloc(page_size_max)` + per-spawn
//! `mprotect(PROT_NONE)` on the bottom page. That regressed multi-
//! worker `bench-spawn-hot` ~8× on 11 workers — `mprotect` serializes
//! on Darwin's process-wide `vm_map_lock` (Linux: `mmap_sem`), and the
//! spawn/free churn fired it constantly. See
//! `docs/internals/phase-4-postmortem.md` for the full story.
//!
//! This design pays the `mprotect` cost **once per stack at first
//! allocation**. After that the stack lives in P's pool forever (it's
//! reused, never munmap'd on free). The hot path — pool pop / pool
//! push — is zero syscalls.
//!
//! `Runtime.deinit` calls `P.drainPools` which munmaps everything.
//!
//! ## Memory cost
//!
//! Per stack: `guardSize() + BODY_SIZE` of virtual address space.
//! Committed RSS: only what the coroutine actually touches in the
//! body region. The guard page is PROT_NONE so it's never backed by
//! a physical page — zero RSS impact on Darwin.

const std = @import("std");

/// Opaque base pointer for a guarded stack. Carries enough alignment
/// to satisfy `mmap` page alignment. Coroutine and pool both hold
/// stacks as `StackPtr` — no slice, the size is implicit from
/// `totalSize()`.
pub const StackPtr = [*]align(std.heap.page_size_max) u8;

/// Usable stack region size — the SP-addressable bytes after the
/// guard. Equal to the old `STACK_SIZE` constant; was the entire
/// allocation pre-guard-page.
pub const BODY_SIZE: usize = 16 * 1024;

/// Guard-page size — one page at the runtime's actual page size.
pub inline fn guardSize() usize {
    return std.heap.pageSize();
}

/// Total mapping size: guard page + usable body.
pub inline fn totalSize() usize {
    return guardSize() + BODY_SIZE;
}

/// Where the usable region begins relative to the mapping base.
/// The pool's intrusive next-pointer lives at this offset because
/// the bottom page is PROT_NONE (writing there would itself SIGSEGV).
pub inline fn usableOffset() usize {
    return guardSize();
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

// POSIX constants — values from Darwin's <sys/mman.h>. The same
// values work on Linux except MAP_ANON which differs (0x1000 vs
// 0x20). We branch.
const PROT_NONE: c_int = 0;
const PROT_READ: c_int = 1;
const PROT_WRITE: c_int = 2;
const MAP_PRIVATE: c_int = 0x0002;
const MAP_ANON: c_int = switch (@import("builtin").os.tag) {
    .macos, .ios, .tvos, .watchos => 0x1000,
    else => 0x20, // Linux, FreeBSD, etc.
};

// `mmap` returns this on failure rather than NULL. Cast through a
// usize to be portable across address-space sizes.
const MAP_FAILED: ?*anyopaque = @ptrFromInt(@as(usize, @bitCast(@as(isize, -1))));

pub const Error = error{
    MmapFailed,
    MprotectFailed,
};

/// Allocate a guarded stack. Reserves `totalSize()` bytes via a
/// single `mmap` (initially PROT_NONE for the whole range), then
/// `mprotect`s the body to PROT_READ|PROT_WRITE. The bottom page
/// stays PROT_NONE — that's the guard.
///
/// One `mmap` + one `mprotect` per call. Both take the VM lock, so
/// keep this off the hot path — `P.allocStack` should pool-hit
/// almost always; this only fires on cold start or when the pool
/// empties.
pub fn alloc() Error!StackPtr {
    const total = totalSize();
    const ret = mmap_fn(null, total, PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (ret == MAP_FAILED or ret == null) return error.MmapFailed;
    const base: [*]u8 = @ptrCast(ret.?);

    // Body region — everything above the guard. Make it RW.
    const body_ptr: *anyopaque = @ptrCast(base + guardSize());
    const rc = mprotect_fn(body_ptr, BODY_SIZE, PROT_READ | PROT_WRITE);
    if (rc != 0) {
        _ = munmap_fn(ret.?, total);
        return error.MprotectFailed;
    }

    return @alignCast(base);
}

/// Release a guarded stack back to the OS. Only called from
/// `P.drainPools` at runtime shutdown and pool-cap eviction — never
/// on the spawn/free hot path.
pub fn free(base: StackPtr) void {
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

test "stack body is writable, guard page lives at base" {
    const s = try alloc();
    defer free(s);

    // Body region is writable.
    const body_start = s + usableOffset();
    body_start[0] = 0xab;
    body_start[BODY_SIZE - 1] = 0xcd;
    try std.testing.expectEqual(@as(u8, 0xab), body_start[0]);
    try std.testing.expectEqual(@as(u8, 0xcd), body_start[BODY_SIZE - 1]);

    // We do NOT test writing the guard region here — that would
    // SIGSEGV the test process. The runtime-level overflow test
    // (search "stack overflow" in this tree) covers that path
    // separately by forking and catching the signal.
}

test "two stacks have distinct mappings" {
    const a = try alloc();
    defer free(a);
    const b = try alloc();
    defer free(b);
    try std.testing.expect(a != b);
}
