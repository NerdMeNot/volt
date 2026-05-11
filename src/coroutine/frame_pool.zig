//! Per-worker frame pool — type-erased slab allocator with comptime size
//! classes for the spawn-Frame hot path.
//!
//! ## Why
//!
//! Every `volt.spawn` / `volt.launch` calls `allocator.create(Frame)` where
//! Frame is the per-spawn metadata struct (Coroutine + Closure + Args +
//! Result). With `std.heap.smp_allocator` that's ~80 ns of lock-free
//! bin-allocator overhead. Across burst-spawn workloads (~10k spawns in a
//! tight loop) this adds up to ~800 µs of pure allocator traffic.
//!
//! A per-worker slab pool eliminates that cost:
//!
//! - **One mmap at worker init** for the backing region (1 MB per worker).
//!   No syscalls in the spawn hot path.
//! - **Comptime size classes**. `Frame(user_fn, Args)` has a comptime-known
//!   size, so `classFor()` is a comptime function call — the compiler picks
//!   the right size-class index at the call site. No runtime size lookup.
//! - **Lock-free freelist push** for cross-worker frees. A coroutine
//!   spawned on worker A may be destroyed on worker B (if B was the one
//!   that joined the Task and the second-arrival-of-rendezvous fired
//!   there). B pushes onto A's freelist via single-CAS — no mutex.
//! - **Owner-only pop**. Worker A's spawn path pops from its own freelist
//!   with no synchronization (single-owner reader). On miss, falls through
//!   to bump-alloc from the backing region. On exhaustion, falls back to
//!   the user allocator.
//!
//! ## Memory cost
//!
//! Per worker: `BACKING_BYTES` (1 MB) of virtual memory, lazily committed
//! (mmap PROT_NONE → mprotect-on-write via the kernel's normal demand-
//! paging on first touch).
//!
//! At rest, only the pages actually touched by allocations are resident.
//! For a worker that's served 1000 spawns of 512-byte Frames, resident is
//! ~512 KB. Far below the cost of the smp_allocator overhead saved.
//!
//! ## Size classes
//!
//! Powers of 2 from 128 to 4096 bytes. Frames are typically 256-1024
//! bytes (Coroutine ~512 bytes + Closure + Args + Result). Frames larger
//! than 4096 fall back to the user allocator — rare for realistic
//! user_fn signatures.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

/// Backing region per worker. mmap'd once at Worker.init.
pub const BACKING_BYTES: usize = 1 * 1024 * 1024;

/// Slab size classes. A Frame allocation picks the smallest class that
/// fits its `@sizeOf(F)`. Comptime resolution.
pub const SIZE_CLASSES = [_]usize{ 128, 256, 512, 1024, 2048, 4096 };

/// Pick a size class for a given comptime size. `@compileError` if the
/// frame is too large — caller falls back to user allocator.
pub fn classFor(comptime size: usize) ?usize {
    inline for (SIZE_CLASSES, 0..) |sz, i| {
        if (size <= sz) return i;
    }
    return null; // too large; caller falls back
}

const FreeNode = extern struct {
    next: ?*FreeNode,
};

pub const FramePool = struct {
    /// Owning worker id — for diagnostics and to identify which pool
    /// to return a slab to on cross-worker free.
    worker_id: usize,

    /// Backing region. mmap'd PROT_READ|PROT_WRITE at init; pages only
    /// become resident as they're touched. Alignment matches the
    /// system's minimum page alignment so we can pass it straight to
    /// `posix.munmap` at deinit.
    backing: []align(std.heap.page_size_min) u8,

    /// Bump pointer for fresh allocations (after freelists are empty).
    /// Owner-only access — no synchronization needed for bump_offset.
    bump_offset: usize,

    /// Per-size-class freelists. Owner pushes onto freelist on alloc-
    /// miss-fallback... no, owner POPS from freelist on alloc. Cross-
    /// worker free PUSHES via CAS.
    ///
    /// Atomic because cross-worker free can happen concurrently with
    /// the owner's pop. Pop is a single load+store (owner-only), push
    /// is a CAS loop.
    freelists: [SIZE_CLASSES.len]std.atomic.Value(?*FreeNode),

    /// Diagnostics.
    hits: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    misses: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    fallbacks: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn init(worker_id: usize) !FramePool {
        const page = std.heap.pageSize();
        const size = std.mem.alignForward(usize, BACKING_BYTES, page);
        const region = if (comptime builtin.os.tag == .windows) blk: {
            const W = struct {
                extern "kernel32" fn VirtualAlloc(
                    lpAddress: ?*anyopaque,
                    dwSize: usize,
                    flAllocationType: u32,
                    flProtect: u32,
                ) callconv(.winapi) ?*anyopaque;
            };
            const MEM_RESERVE_COMMIT: u32 = 0x3000;
            const PAGE_READWRITE: u32 = 0x04;
            const ptr = W.VirtualAlloc(null, size, MEM_RESERVE_COMMIT, PAGE_READWRITE) orelse
                return error.OutOfMemory;
            const p: [*]align(std.heap.page_size_min) u8 = @ptrCast(@alignCast(ptr));
            break :blk p[0..size];
        } else blk: {
            const map = try posix.mmap(
                null,
                size,
                .{ .READ = true, .WRITE = true },
                .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
                -1,
                0,
            );
            break :blk map;
        };

        var freelists: [SIZE_CLASSES.len]std.atomic.Value(?*FreeNode) = undefined;
        for (&freelists) |*f| f.* = std.atomic.Value(?*FreeNode).init(null);

        return .{
            .worker_id = worker_id,
            .backing = region,
            .bump_offset = 0,
            .freelists = freelists,
        };
    }

    pub fn deinit(self: *FramePool) void {
        if (comptime builtin.os.tag == .windows) {
            const W = struct {
                extern "kernel32" fn VirtualFree(
                    lpAddress: ?*anyopaque,
                    dwSize: usize,
                    dwFreeType: u32,
                ) callconv(.winapi) i32;
            };
            const MEM_RELEASE: u32 = 0x8000;
            _ = W.VirtualFree(self.backing.ptr, 0, MEM_RELEASE);
        } else {
            posix.munmap(self.backing);
        }
    }

    /// Try to allocate `@sizeOf(T)` bytes aligned to `@alignOf(T)`.
    /// Returns null if the frame is too large for any size class OR if
    /// the backing region is exhausted — caller falls back to user
    /// allocator. Owner-only.
    pub fn alloc(self: *FramePool, comptime T: type) ?*T {
        const class_idx = comptime classFor(@sizeOf(T)) orelse return null;
        const slot_size = SIZE_CLASSES[class_idx];

        // Freelist pop — owner-only, but the freelist is atomic because
        // cross-worker free pushes via CAS. We do a relaxed swap.
        var head = self.freelists[class_idx].load(.acquire);
        while (head) |node| {
            const next = node.next;
            if (self.freelists[class_idx].cmpxchgWeak(head, next, .acq_rel, .acquire)) |observed| {
                head = observed;
                continue;
            }
            _ = self.hits.fetchAdd(1, .monotonic);
            return @ptrCast(@alignCast(node));
        }

        // Freelist empty — bump-allocate.
        const align_bytes = @alignOf(T);
        const aligned = std.mem.alignForward(usize, self.bump_offset, align_bytes);
        if (aligned + slot_size > self.backing.len) {
            _ = self.fallbacks.fetchAdd(1, .monotonic);
            return null; // exhausted; caller falls back
        }
        self.bump_offset = aligned + slot_size;
        _ = self.misses.fetchAdd(1, .monotonic);
        return @ptrCast(@alignCast(self.backing.ptr + aligned));
    }

    /// Free a previously-allocated slot back to its pool. Safe to call
    /// from any thread (lock-free push).
    pub fn free(self: *FramePool, comptime T: type, ptr: *T) void {
        const class_idx = comptime classFor(@sizeOf(T)) orelse {
            @compileError("FramePool.free called with too-large type — should not happen");
        };
        const node: *FreeNode = @ptrCast(@alignCast(ptr));
        var head = self.freelists[class_idx].load(.acquire);
        while (true) {
            node.next = head;
            if (self.freelists[class_idx].cmpxchgWeak(head, node, .release, .acquire)) |observed| {
                head = observed;
                continue;
            }
            return;
        }
    }

    pub const Stats = struct {
        worker_id: usize,
        hits: u64,
        misses: u64,
        fallbacks: u64,
    };

    pub fn stats(self: *const FramePool) Stats {
        return .{
            .worker_id = self.worker_id,
            .hits = self.hits.load(.acquire),
            .misses = self.misses.load(.acquire),
            .fallbacks = self.fallbacks.load(.acquire),
        };
    }
};

test "frame_pool: alloc + free round-trip" {
    var pool = try FramePool.init(0);
    defer pool.deinit();

    const T = struct {
        x: u64,
        y: u64,
        z: u64,
    };

    const p1 = pool.alloc(T) orelse return error.AllocFailed;
    p1.* = .{ .x = 1, .y = 2, .z = 3 };
    try std.testing.expectEqual(@as(u64, 1), p1.x);

    pool.free(T, p1);

    const p2 = pool.alloc(T) orelse return error.AllocFailed;
    try std.testing.expectEqual(p1, p2); // LIFO recycle
}

test "frame_pool: bump exhaustion returns null" {
    var pool = try FramePool.init(0);
    defer pool.deinit();

    const Big = [4096]u8;
    const max_allocs = BACKING_BYTES / 4096;
    var got: u64 = 0;
    var i: usize = 0;
    while (i < max_allocs + 4) : (i += 1) {
        if (pool.alloc(Big)) |_| got += 1 else break;
    }
    try std.testing.expect(got >= max_allocs - 1); // ~ at capacity
    // Next alloc fails
    try std.testing.expect(pool.alloc(Big) == null);
}

test "frame_pool: classFor selects smallest fitting class" {
    try std.testing.expectEqual(@as(?usize, 0), classFor(64));
    try std.testing.expectEqual(@as(?usize, 0), classFor(128));
    try std.testing.expectEqual(@as(?usize, 1), classFor(129));
    try std.testing.expectEqual(@as(?usize, 1), classFor(256));
    try std.testing.expectEqual(@as(?usize, 2), classFor(257));
    try std.testing.expectEqual(@as(?usize, 5), classFor(4096));
    try std.testing.expectEqual(@as(?usize, null), classFor(4097));
}
