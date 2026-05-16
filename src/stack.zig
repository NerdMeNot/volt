//! Coroutine stack slab arena, backed by a single `mmap` carved into
//! fixed-size slots with PROT_NONE guard pages and grow-on-demand body
//! pages.
//!
//! ## Per-slot layout (low → high address)
//!
//! ```
//!   base ── guard (1 page, PROT_NONE)
//!        ── growable region (PROT_NONE, grown on SIGSEGV)
//!        ── body (top BODY_SIZE bytes, PROT_RW once committed)
//!           |<─────────── RESERVATION_SIZE ────────────>|
//!           base                                       top
//! ```
//!
//! SP starts at `base + RESERVATION_SIZE` and grows down. The bottom
//! page is the guard — SP hitting it raises SIGSEGV which chains to
//! the default handler and aborts (real overflow). Pages above the
//! guard but below the initial body are PROT_NONE until the SIGSEGV
//! handler in `signal.zig` mprotects them RW on first touch.
//!
//! ## Why an arena instead of per-spawn mmap
//!
//! The earlier "mmap-per-stack + per-P pool of 64" design (commit
//! `081094d`) regressed `bench-spawn-hot` ~30× when batch size
//! exceeded POOL_CAP: every overflow free issued an `munmap` and
//! every overflow alloc an `mmap`, both serialising on the Darwin
//! `vm_map_lock` / Linux `mmap_sem`. The arena removes that cliff:
//!
//!   - One `mmap` at runtime startup for the full slab.
//!   - Per-slot `mprotect` happens lazily, once, on first allocation
//!     of that slot. Tracked via a small bitmap. After warm-up,
//!     hot-path alloc/free is one mutex round-trip on the slab's
//!     free list (often skipped — P-local pools absorb most ops).
//!   - Free never `munmap`s; it pushes the slot's index back onto the
//!     arena's free list.
//!
//! Grow-on-demand under SIGSEGV is unchanged — that fires only when
//! a coroutine actually uses more than `BODY_SIZE` of stack, which
//! is rare and per-coroutine-once.
//!
//! ## RSS cost
//!
//! Per active coroutine: one body page (16 KiB Darwin, 4 KiB Linux),
//! plus any growable pages the coroutine has touched. The arena's
//! virtual reservation is `n_slots * RESERVATION_SIZE` but only
//! committed slots contribute RSS. Idle, never-allocated slots stay
//! PROT_NONE and cost zero RSS — `bench-rss`'s headline number is
//! preserved.

const std = @import("std");
const signal = @import("signal.zig");

/// Opaque base pointer for a slot. Page-aligned by construction (the
/// arena's `mmap` returns a page-aligned base and slot size is a
/// multiple of the page size).
pub const StackPtr = [*]align(std.heap.page_size_max) u8;

/// Initial committed body region — top `BODY_SIZE` bytes of each
/// slot are `mprotect`'d RW on first allocation. SP starts at the
/// top of the slot and grows down into this region first.
pub const BODY_SIZE: usize = 16 * 1024;

/// Per-slot virtual reservation. Growable budget (between guard and
/// initial body) is `RESERVATION_SIZE - guardSize() - BODY_SIZE`
/// ≈ 224 KiB on Darwin. Raise this to widen the recursion budget;
/// lower it to fit more concurrent stacks in the same VA budget.
pub const RESERVATION_SIZE: usize = 256 * 1024;

/// Default arena size — `Runtime.Config.max_concurrent_stacks` falls
/// back to this if the caller doesn't set it. 16384 slots × 256 KiB
/// = 4 GiB virtual reservation, no RSS until coroutines actually use
/// the slots.
pub const DEFAULT_MAX_STACKS: usize = 16 * 1024;

/// Guard-page size — one page at the runtime's actual page size
/// (16 KiB Darwin arm64, 4 KiB Linux x86_64/arm64).
pub inline fn guardSize() usize {
    return std.heap.pageSize();
}

/// Per-slot total size used by the arena. Must be a multiple of the
/// page size so each slot's guard sits on a page boundary.
pub inline fn slotSize() usize {
    return RESERVATION_SIZE;
}

/// Offset within a slot of the first byte that's ever usable. The
/// arena's free-list "next" pointer lives at this offset, inside the
/// body region (which is committed once the slot has been used).
pub inline fn usableOffset() usize {
    return slotSize() - BODY_SIZE;
}

// libc bindings — `std.posix.{mmap,munmap,mprotect}` were removed in
// Zig 0.16.
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
    else => 0x20,
};
const MAP_FAILED: ?*anyopaque = @ptrFromInt(@as(usize, @bitCast(@as(isize, -1))));

pub const Error = error{
    MmapFailed,
    MprotectFailed,
    ArenaExhausted,
};

/// Slab arena of fixed-size coroutine stack slots.
///
/// One `mmap` at construction, one `munmap` at destruction. Per-slot
/// top-page commit (`mprotect`) is lazy — fires at most once per slot
/// over the arena's lifetime. The hot-path `alloc`/`free` is a
/// spinlock-protected pop/push on a stack of free slot indices; most
/// callers route through a P-local LIFO pool first so the arena's
/// spinlock is only hit on pool miss / driver-thread spawn.
pub const Arena = struct {
    mapping_base: StackPtr,
    mapping_total: usize,
    n_slots: usize,

    /// Side-array stack of free slot indices. Capacity is `n_slots`.
    /// `free_top` is the count of entries currently free (= the next
    /// push index). Pop reads `free_indices[free_top - 1]` then
    /// decrements; push increments then writes.
    free_indices: []u32,
    free_top: usize,

    /// One bit per slot: 1 once the slot's top body region has been
    /// `mprotect`'d to PROT_RW. Avoids re-syscalling on slot reuse.
    committed: []u8,

    /// TTAS spinlock protecting `free_indices`/`free_top`/`committed`.
    /// Critical section is a few cache-line accesses — no syscalls
    /// held under the lock (the lazy `mprotect` runs after release).
    /// Contention is bounded by per-P pool miss rate, which after
    /// warm-up is essentially zero on steady-state workloads.
    lock: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// Construct the arena. Reserves `n_slots * slotSize()` virtual
    /// bytes as PROT_NONE, allocates the free-index stack + commit
    /// bitmap on the heap, and registers the arena range with the
    /// SIGSEGV handler so grow-on-demand works for any slot.
    ///
    /// Bookkeeping memory (free_indices + committed) is owned by
    /// `allocator`; the slab itself is owned by `mmap`.
    pub fn init(allocator: std.mem.Allocator, n_slots: usize) Error!Arena {
        std.debug.assert(n_slots > 0);
        std.debug.assert(n_slots <= std.math.maxInt(u32));

        signal.ensureInstalled();

        const slot_size = slotSize();
        const total = n_slots * slot_size;
        const ret = mmap_fn(null, total, PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0);
        if (ret == MAP_FAILED or ret == null) return error.MmapFailed;
        const base: StackPtr = @ptrCast(@alignCast(ret.?));

        // Free-index stack: initialise full, ordered so pop yields
        // index 0 first (lowest VA — irrelevant to perf, helpful for
        // tests that want predictable layouts).
        const free_indices = allocator.alloc(u32, n_slots) catch {
            _ = munmap_fn(ret.?, total);
            return error.MmapFailed;
        };
        var i: usize = 0;
        while (i < n_slots) : (i += 1) {
            free_indices[i] = @intCast(n_slots - 1 - i);
        }

        const committed = allocator.alloc(u8, (n_slots + 7) / 8) catch {
            allocator.free(free_indices);
            _ = munmap_fn(ret.?, total);
            return error.MmapFailed;
        };
        @memset(committed, 0);

        // Single arena registration with the SIGSEGV handler. Handler
        // computes per-slot offset on fault.
        signal.registerArena(@intFromPtr(base), total, slot_size) catch {
            allocator.free(committed);
            allocator.free(free_indices);
            _ = munmap_fn(ret.?, total);
            return error.MmapFailed;
        };

        return .{
            .mapping_base = base,
            .mapping_total = total,
            .n_slots = n_slots,
            .free_indices = free_indices,
            .free_top = n_slots,
            .committed = committed,
        };
    }

    /// Tear down the arena: unregister with the signal handler,
    /// release the bookkeeping, `munmap` the slab.
    pub fn deinit(self: *Arena, allocator: std.mem.Allocator) void {
        signal.unregister(@intFromPtr(self.mapping_base));
        allocator.free(self.committed);
        allocator.free(self.free_indices);
        _ = munmap_fn(@ptrCast(self.mapping_base), self.mapping_total);
        self.* = undefined;
    }

    /// Pop one slot off the free list. Lazy-mprotect its top body
    /// page on first use. Returns `error.ArenaExhausted` if every
    /// slot is currently allocated.
    pub fn alloc(self: *Arena) Error!StackPtr {
        self.acquire();

        if (self.free_top == 0) {
            self.release();
            return error.ArenaExhausted;
        }
        self.free_top -= 1;
        const idx = self.free_indices[self.free_top];

        const slot_size = slotSize();
        const base: StackPtr = @alignCast(self.mapping_base + @as(usize, idx) * slot_size);

        const byte_idx = idx / 8;
        const bit_mask: u8 = @as(u8, 1) << @intCast(idx & 7);
        const already = (self.committed[byte_idx] & bit_mask) != 0;
        if (!already) self.committed[byte_idx] |= bit_mask;

        // Release the spinlock before issuing mprotect — the syscall
        // itself serialises on the kernel VM lock and we don't need
        // our own lock held across it.
        self.release();

        if (!already) {
            const body_offset = slot_size - BODY_SIZE;
            const body_ptr: *anyopaque = @ptrCast(base + body_offset);
            const rc = mprotect_fn(body_ptr, BODY_SIZE, PROT_READ | PROT_WRITE);
            if (rc != 0) {
                // Roll back: clear the commit bit and return the slot.
                self.acquire();
                self.committed[byte_idx] &= ~bit_mask;
                self.free_indices[self.free_top] = idx;
                self.free_top += 1;
                self.release();
                return error.MprotectFailed;
            }
        }
        return base;
    }

    /// Return a slot to the arena. No syscall — just pushes the
    /// slot's index back onto the free stack.
    pub fn free(self: *Arena, slot: StackPtr) void {
        const offset = @intFromPtr(slot) - @intFromPtr(self.mapping_base);
        const idx: u32 = @intCast(offset / slotSize());
        std.debug.assert(idx < self.n_slots);

        self.acquire();
        defer self.release();
        self.free_indices[self.free_top] = idx;
        self.free_top += 1;
    }

    /// Number of slots currently free. Snapshot; can change between
    /// the call and any subsequent action. Used by stats / tests.
    pub fn freeCount(self: *Arena) usize {
        self.acquire();
        defer self.release();
        return self.free_top;
    }

    inline fn acquire(self: *Arena) void {
        while (true) {
            // TTAS: spin on the (cheap) load until it looks free,
            // then try one CAS — avoids the cache-line ping-pong of
            // a hot CAS loop under contention.
            while (self.lock.load(.monotonic) != 0) std.atomic.spinLoopHint();
            if (self.lock.cmpxchgWeak(0, 1, .acquire, .monotonic) == null) return;
        }
    }

    inline fn release(self: *Arena) void {
        self.lock.store(0, .release);
    }
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

test "Arena.init/deinit round-trip" {
    var a = try Arena.init(std.testing.allocator, 8);
    defer a.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 8), a.n_slots);
    try std.testing.expectEqual(@as(usize, 8), a.freeCount());
}

test "Arena.alloc returns distinct page-aligned slots" {
    var a = try Arena.init(std.testing.allocator, 4);
    defer a.deinit(std.testing.allocator);

    const s0 = try a.alloc();
    const s1 = try a.alloc();
    try std.testing.expect(s0 != s1);
    try std.testing.expect(@intFromPtr(s0) % std.heap.pageSize() == 0);
    try std.testing.expect(@intFromPtr(s1) % std.heap.pageSize() == 0);

    a.free(s0);
    a.free(s1);
}

test "Arena: body region writable after alloc (mprotect fired)" {
    var a = try Arena.init(std.testing.allocator, 4);
    defer a.deinit(std.testing.allocator);

    const s = try a.alloc();
    defer a.free(s);

    const body_start = s + usableOffset();
    body_start[0] = 0xab;
    body_start[BODY_SIZE - 1] = 0xcd;
    try std.testing.expectEqual(@as(u8, 0xab), body_start[0]);
    try std.testing.expectEqual(@as(u8, 0xcd), body_start[BODY_SIZE - 1]);
}

test "Arena: slot reuse skips re-mprotect (commit bit sticky)" {
    var a = try Arena.init(std.testing.allocator, 2);
    defer a.deinit(std.testing.allocator);

    const s1 = try a.alloc();
    const body_start1 = s1 + usableOffset();
    body_start1[0] = 0x42;
    a.free(s1);

    // Second alloc — same slot, body should still be RW. We *can't*
    // assert the value is preserved (the runtime is free to zero it
    // out for security; we don't), but writes must still succeed.
    const s2 = try a.alloc();
    defer a.free(s2);
    const body_start2 = s2 + usableOffset();
    body_start2[0] = 0x99;
    try std.testing.expectEqual(@as(u8, 0x99), body_start2[0]);
}

test "Arena: grow-on-demand mprotects a deeper page on access" {
    var a = try Arena.init(std.testing.allocator, 2);
    defer a.deinit(std.testing.allocator);

    const s = try a.alloc();
    defer a.free(s);

    const ps = std.heap.pageSize();
    // Page one below the initial body — PROT_NONE on alloc. Writing
    // triggers the SIGSEGV handler which mprotects it RW.
    const grow_addr = s + usableOffset() - ps;
    grow_addr[0] = 0xee;
    try std.testing.expectEqual(@as(u8, 0xee), grow_addr[0]);
}

test "Arena.alloc returns ArenaExhausted when full" {
    var a = try Arena.init(std.testing.allocator, 2);
    defer a.deinit(std.testing.allocator);

    const s0 = try a.alloc();
    const s1 = try a.alloc();
    try std.testing.expectError(error.ArenaExhausted, a.alloc());

    a.free(s0);
    a.free(s1);
}

test "Arena: free returns slots to the free list (alloc succeeds again)" {
    var a = try Arena.init(std.testing.allocator, 2);
    defer a.deinit(std.testing.allocator);

    const s0 = try a.alloc();
    const s1 = try a.alloc();
    try std.testing.expectError(error.ArenaExhausted, a.alloc());

    a.free(s0);
    const s2 = try a.alloc();
    try std.testing.expectEqual(s0, s2);

    a.free(s1);
    a.free(s2);
}
