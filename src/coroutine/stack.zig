//! Growable stacks for stackful coroutines, via virtual-memory reserve +
//! commit-on-demand.
//!
//! ## Why this works without compiler help
//!
//! Go grows goroutine stacks by allocating a bigger stack, *copying* the
//! contents, and *fixing up* every interior pointer (e.g., `&local`).
//! That fixup needs compiler-emitted stackmaps to know which slots hold
//! pointers. Zig's compiler doesn't emit stackmaps, so copy-grow is
//! unsafe.
//!
//! Instead, we grow the stack *in place* using virtual memory:
//!   1. Reserve a large virtual range up-front (default 1 MiB) with
//!      `mmap PROT_NONE`. This costs *only* address space — zero
//!      physical memory.
//!   2. Commit just a small initial portion at the top via `mprotect`
//!      to `PROT_READ | PROT_WRITE` (default 1 page).
//!   3. The page just below the committed region is the guard — still
//!      `PROT_NONE` because the whole reservation is. Touching it
//!      raises `SIGSEGV`.
//!   4. The SIGSEGV handler `mprotect`s the guard page to R|W. The page
//!      below it is *already* `PROT_NONE` (still part of the reserved
//!      range), so it becomes the new guard automatically. One syscall
//!      per growth step. **Existing pointers stay valid** — the virtual
//!      addresses don't move, we just commit more pages of the same
//!      range.
//!   5. The bottom-most page of the reservation is the *permanent*
//!      floor. Hitting it means the coroutine has truly run out of its
//!      reserved range — the handler marks the coroutine
//!      `error.StackOverflow` and `siglongjmp`s back to the scheduler.
//!      No process crash; only that coroutine dies.
//!
//! ## Memory cost
//!
//! Per coroutine:
//!   - Virtual: `default_reserved` (1 MiB). Address space — abundant on
//!     64-bit systems (~128 TiB user space on Linux/macOS arm64).
//!   - Physical: starts at `default_initial_commit` (1 page = 4 KiB or
//!     16 KiB depending on system). Grows in 1-page increments only as
//!     the coroutine actually consumes stack.
//!
//! Compared to fixed 64 KiB stacks, the resident set per idle coroutine
//! drops 16-64×.
//!
//! ## Layout (low to high address)
//!
//! ```
//!   reserved_base
//!     ├── permanent floor (1 page, PROT_NONE forever)         ← terminal
//!     ├── uncommitted region (PROT_NONE, available for growth)
//!     ├── current guard (1 page, PROT_NONE, slides down)
//!     └── committed usable region (PROT_READ|PROT_WRITE)
//!   reserved_base + reserved_size                              ← sp starts
//! ```

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

/// AAPCS64 SP alignment.
pub const stack_alignment: std.mem.Alignment = .@"16";

/// Runtime page size (system-dependent: 4 KiB on Linux/Intel, 16 KiB on
/// Apple Silicon). Looked up once.
pub fn pageSize() usize {
    return std.heap.pageSize();
}

/// Default reserved range per coroutine — pure virtual address space
/// until pages are committed on demand.
///
/// 8 MiB is the cap on per-coroutine stack growth. Picked because:
///   - It costs ~zero physical memory (PROT_NONE virtual until written).
///   - 1M coroutines × 8 MiB = 8 TiB virtual, fits the 128 TiB user-space
///     ceiling on 64-bit Linux/macOS with room to spare.
///   - Big enough for recursive-descent parsers, regex backtracking,
///     deep AST walks — the workloads that exhaust a 1 MiB cap.
///   - Small enough that hitting it is a real signal of runaway
///     recursion (vs. a normal workload that just needs more headroom).
///
/// A coroutine that exhausts 8 MiB surfaces `error.StackOverflow` to
/// joiners; the runtime keeps running. To support workloads that
/// genuinely need more, raise this constant.
pub const default_reserved: usize = 8 * 1024 * 1024;

/// Default initial committed region — exactly one page. The smallest
/// possible footprint; subsequent pages are committed on guard-page
/// hits via the SIGSEGV handler.
pub fn defaultInitialCommit() usize {
    return pageSize();
}

/// Backwards-compatible name. `Coroutine.stack` is still a `[]u8` slice
/// covering the entire reserved range; the runtime tracks growth via
/// `committed_bottom` (managed externally — see `Coroutine`).
pub const default_size: usize = default_reserved;

/// Allocate a growable stack. Reserves `reserved_size` bytes of virtual
/// space, commits `initial_commit` bytes at the top (read+write), and
/// leaves the rest as `PROT_NONE`. The lowest page is the permanent
/// floor.
///
/// `initial_commit == 0` is legal: the entire reservation stays
/// `PROT_NONE` and the very first stack write traps into the SIGSEGV
/// handler, which commits one page on demand. Cost: ~µs signal trap on
/// first dispatch. Win: zero physical pages held by an unstarted coro.
/// Use this for spawn-heavy workloads with short-lived coroutines.
///
/// `initial_commit` and `reserved_size` are rounded up to the system
/// page size. `reserved_size` must be at least 2 pages when
/// `initial_commit` is 0 (floor + 1 growable), or at least 3 pages
/// when `initial_commit > 0` (floor + 1 guard + 1 usable).
pub fn allocGrowable(
    initial_commit: usize,
    reserved_size: usize,
) !GrowableStack {
    const page = pageSize();
    const reserved_aligned = std.mem.alignForward(usize, reserved_size, page);
    const initial_aligned = std.mem.alignForward(usize, initial_commit, page);

    const min_reserved: usize = if (initial_aligned == 0) 2 * page else 3 * page;
    if (reserved_aligned < min_reserved) return error.ReservedTooSmall;
    if (initial_aligned + 2 * page > reserved_aligned and initial_aligned != 0) {
        return error.InitialCommitTooLarge;
    }

    // Reserve the whole range. On POSIX this is mmap PROT_NONE; on
    // Windows it's VirtualAlloc(MEM_RESERVE).
    const base: usize = if (comptime builtin.os.tag == .windows) blk: {
        const W = struct {
            extern "kernel32" fn VirtualAlloc(
                lpAddress: ?*anyopaque,
                dwSize: usize,
                flAllocationType: u32,
                flProtect: u32,
            ) callconv(.winapi) ?*anyopaque;
        };
        const MEM_RESERVE: u32 = 0x2000;
        const PAGE_NOACCESS: u32 = 0x01;
        const ptr = W.VirtualAlloc(null, reserved_aligned, MEM_RESERVE, PAGE_NOACCESS) orelse
            return error.OutOfMemory;
        break :blk @intFromPtr(ptr);
    } else blk: {
        const map = try posix.mmap(
            null,
            reserved_aligned,
            .{},
            .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
            -1,
            0,
        );
        break :blk @intFromPtr(map.ptr);
    };
    errdefer freeRange(base, reserved_aligned);

    const top: usize = base + reserved_aligned;
    const committed_bottom: usize = top - initial_aligned;

    // Commit the initial usable region (skip when initial_commit == 0).
    if (initial_aligned > 0) {
        if (mprotectRW(committed_bottom, initial_aligned) != 0) {
            return error.GuardPageProtectionFailed;
        }
    }

    return .{
        .base = base,
        .top = top,
        .reserved_size = reserved_aligned,
        .committed_bottom = committed_bottom,
        .floor = base + page,
    };
}

/// Free the entire reserved range.
pub fn freeGrowable(gs: GrowableStack) void {
    freeRange(gs.base, gs.reserved_size);
}

fn freeRange(base: usize, reserved_size: usize) void {
    if (comptime builtin.os.tag == .windows) {
        const W = struct {
            extern "kernel32" fn VirtualFree(
                lpAddress: ?*anyopaque,
                dwSize: usize,
                dwFreeType: u32,
            ) callconv(.winapi) i32;
        };
        const MEM_RELEASE: u32 = 0x8000;
        _ = W.VirtualFree(@ptrFromInt(base), 0, MEM_RELEASE);
        return;
    }
    const ptr: [*]align(std.heap.page_size_min) const u8 = @ptrFromInt(base);
    const slice: []align(std.heap.page_size_min) const u8 = ptr[0..reserved_size];
    posix.munmap(slice);
}

pub const GrowableStack = struct {
    /// Lowest address of the reserved range.
    base: usize,
    /// One past the highest address (= base + reserved_size). SP starts
    /// here.
    top: usize,
    /// Total reserved virtual size.
    reserved_size: usize,
    /// Lowest currently-committed address. The page below this is the
    /// guard. Updated atomically by the SIGSEGV handler when it grows.
    committed_bottom: usize,
    /// The page just above this is the permanent floor — the SIGSEGV
    /// handler refuses to grow into it. (= base + page_size.)
    floor: usize,
};

/// Try to grow the committed region of `gs` by one page in response to
/// a guard-page fault at `fault_addr`. Returns:
///   - .grew  — committed region extended; caller (signal handler) can
///              return and let the faulting instruction retry.
///   - .terminal — fault is at the permanent floor; the coroutine has
///              truly exhausted its reservation. Caller must longjmp.
///   - .not_my_stack — fault is outside the reserved range; unrelated
///              SIGSEGV. Caller chains to the default handler.
///
/// **Async-signal-safe**: only calls `mprotect`, no allocation, no
/// locking. `mprotect` is on the POSIX async-signal-safe list.
pub fn tryGrow(gs: *GrowableStack, fault_addr: usize) GrowResult {
    if (fault_addr < gs.base or fault_addr >= gs.top) {
        return .not_my_stack;
    }
    const page = pageSize();
    const new_bottom = gs.committed_bottom - page;
    if (new_bottom < gs.floor) {
        return .terminal;
    }
    // The guard page is at [committed_bottom - page, committed_bottom).
    // Its address is `new_bottom`. Promote it to R|W in place — the
    // page below it is already PROT_NONE (whole range is) and becomes
    // the new guard automatically.
    if (mprotectRW(new_bottom, page) != 0) {
        return .terminal;
    }
    gs.committed_bottom = new_bottom;
    return .grew;
}

/// Cross-platform mprotect-to-RW shim. Darwin's libc-typed signature
/// takes `*anyopaque`; Linux's raw syscall takes `[*]const u8`. On
/// Windows, this is a VirtualAlloc(MEM_COMMIT, PAGE_READWRITE) call —
/// uncommitted pages must transition to committed before write access.
/// Returns 0 on success, non-zero on failure (errno is platform-specific).
fn mprotectRW(addr: usize, len: usize) usize {
    if (comptime builtin.os.tag == .windows) {
        const W = struct {
            extern "kernel32" fn VirtualAlloc(
                lpAddress: ?*anyopaque,
                dwSize: usize,
                flAllocationType: u32,
                flProtect: u32,
            ) callconv(.winapi) ?*anyopaque;
        };
        const MEM_COMMIT: u32 = 0x1000;
        const PAGE_READWRITE: u32 = 0x04;
        const got = W.VirtualAlloc(@ptrFromInt(addr), len, MEM_COMMIT, PAGE_READWRITE);
        return if (got == null) 1 else 0;
    }
    if (comptime builtin.os.tag == .linux) {
        const ptr: [*]const u8 = @ptrFromInt(addr);
        const rc = std.os.linux.mprotect(ptr, len, .{ .READ = true, .WRITE = true });
        // Linux raw syscall returns 0 success; non-zero on err.
        return rc;
    } else {
        const ptr: *align(std.heap.page_size_min) anyopaque = @ptrFromInt(addr);
        const rc = std.c.mprotect(@alignCast(ptr), len, .{ .READ = true, .WRITE = true });
        return if (rc < 0) 1 else 0;
    }
}

pub const GrowResult = enum { grew, terminal, not_my_stack };

// ─── Compat wrappers — preserve the existing Coroutine.stack: []u8 API.
//
// Coroutine.stack is still a slice over the reserved range. The growable
// metadata lives alongside it (see Coroutine). The old `alloc`/`free`/
// `topOf` API is kept for now but routes through the growable
// implementation.

pub fn alloc(allocator: std.mem.Allocator, size: usize) ![]align(16) u8 {
    _ = allocator;
    _ = size; // The legacy `size` arg is the *initial commit* request;
    //         we ignore it and use the default 1-page initial commit
    //         with `default_reserved` reservation. Callers needing a
    //         specific initial commit should use `allocGrowable`.
    const gs = try allocGrowable(defaultInitialCommit(), default_reserved);
    const ptr: [*]align(16) u8 = @ptrFromInt(gs.base);
    return ptr[0..gs.reserved_size];
}

pub fn free(allocator: std.mem.Allocator, stack: []align(16) u8) void {
    _ = allocator;
    const gs = GrowableStack{
        .base = @intFromPtr(stack.ptr),
        .top = @intFromPtr(stack.ptr) + stack.len,
        .reserved_size = stack.len,
        .committed_bottom = 0, // unused for free
        .floor = 0, // unused for free
    };
    freeGrowable(gs);
}

pub fn topOf(stack: []align(16) u8) [*]u8 {
    return stack.ptr + stack.len;
}

// ─── Legacy guard-page constant — kept for tests/docs. Not used by the
//     growable allocator (which uses the runtime page size).
pub const page_size: usize = std.heap.page_size_min;

test "stack: growable alloc + free" {
    const gs = try allocGrowable(pageSize(), 1024 * 1024);
    defer freeGrowable(gs);
    try std.testing.expect(gs.reserved_size >= 1024 * 1024);
    try std.testing.expect(gs.committed_bottom < gs.top);
    try std.testing.expectEqual(gs.base + pageSize(), gs.floor);
}

test "stack: writing the committed region is OK" {
    const gs = try allocGrowable(pageSize(), 1024 * 1024);
    defer freeGrowable(gs);
    const top_minus_one: *u8 = @ptrFromInt(gs.top - 1);
    top_minus_one.* = 0xAB;
    try std.testing.expectEqual(@as(u8, 0xAB), top_minus_one.*);
}

test "stack: tryGrow — fault inside guard region grows" {
    var gs = try allocGrowable(pageSize(), 1024 * 1024);
    defer freeGrowable(gs);
    const old_bottom = gs.committed_bottom;
    const guard_addr = gs.committed_bottom - pageSize();
    const result = tryGrow(&gs, guard_addr);
    try std.testing.expectEqual(GrowResult.grew, result);
    try std.testing.expectEqual(old_bottom - pageSize(), gs.committed_bottom);
    // After grow, writing at guard_addr should now succeed.
    const ptr: *u8 = @ptrFromInt(guard_addr);
    ptr.* = 0xCD;
    try std.testing.expectEqual(@as(u8, 0xCD), ptr.*);
}

test "stack: tryGrow — fault outside reserved range returns not_my_stack" {
    var gs = try allocGrowable(pageSize(), 1024 * 1024);
    defer freeGrowable(gs);
    try std.testing.expectEqual(GrowResult.not_my_stack, tryGrow(&gs, 0x12345));
    try std.testing.expectEqual(GrowResult.not_my_stack, tryGrow(&gs, gs.top + 1));
}

test "stack: tryGrow — eventually returns terminal at the floor" {
    // Smallest reservation: 3 pages = [permanent floor | growable page |
    // initial committed]. allocGrowable enforces reserved >= 3 pages
    // and (initial + 2 pages) <= reserved.
    const page = pageSize();
    var gs = try allocGrowable(page, 3 * page);
    defer freeGrowable(gs);

    // First grow: committed_bottom drops to `floor`. Still valid because
    // the page from floor up is committed — the permanent floor page
    // itself stays PROT_NONE as the new guard.
    const r1 = tryGrow(&gs, gs.committed_bottom - page);
    try std.testing.expectEqual(GrowResult.grew, r1);

    // Second grow attempt would drop new_bottom below the floor — terminal.
    const r2 = tryGrow(&gs, gs.committed_bottom - page);
    try std.testing.expectEqual(GrowResult.terminal, r2);
}
