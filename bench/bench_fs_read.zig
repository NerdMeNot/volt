//! Bench — fs.File random reads. Honest A/B between the Phase 2/3
//! io_uring path and the spawnBlocking fallback, on a SINGLE
//! machine (same kernel, same filesystem, same scheduler), so the
//! only varying axis is the routing.
//!
//! Workload:
//!   * Pre-written 64 MiB file in /tmp with REAL data (not sparse).
//!   * N coroutines × M random 4 KiB `readAt(buf, offset)` ops.
//!   * Reads uniformly across the file, page-aligned.
//!
//! Two cache states, each run as an io_uring-vs-spawnBlocking A/B:
//!   * WARM — page-cache-resident. Measures pure runtime overhead
//!     (submit syscall vs thread-hop); the read itself is a memcpy
//!     from cache, no block I/O.
//!   * COLD — `posix_fadvise(DONTNEED)` evicts the page cache before
//!     each timed rep, so every read misses cache and hits the block
//!     layer. This is where io_uring's async submit (no worker thread
//!     blocked on the read) is supposed to beat spawnBlocking's
//!     thread-per-op. Linux-only (fadvise has no Darwin equivalent).
//!
//! Report per-op ns/op (median of 5 runs after 2 warmups), total
//! ops/sec, and the speedup ratio (or regression).
//!
//! ## METHODOLOGY FIX — 2026-05-30
//!
//! The pre-2026-05-30 version of this bench created its test file
//! with `ftruncate(fd, 64 MiB)` and NO writes — a fully sparse file
//! (verified: 0 disk blocks allocated). Every "read" hit a hole, so
//! the kernel zero-filled a page with ZERO block I/O on both paths.
//! All earlier results below were therefore measured on a *zero-I/O*
//! workload: they capture only runtime-overhead deltas, never real
//! fs performance. io_uring's whole value is hiding I/O latency, and
//! a hole read has none to hide — which is the real reason
//! spawnBlocking looked competitive. The old header also claimed a
//! "tmpfs-backed file"; the container `/tmp` is actually overlayfs on
//! the VM disk (verified via /proc/mounts).
//!
//! ## Historical results (ON THE FLAWED SPARSE WORKLOAD — overhead only)
//!
//!   Pre-Phase-6A (spawnBlocking-fallback landing):
//!     N=8   ops=64:   io_uring 1.88× slower
//!     N=64  ops=128:  io_uring 1.68× slower
//!     N=256 ops=128:  io_uring 1.19× slower
//!   Post-Phase-6A (inline-completion fast path):
//!     N=64  ops=128:  io_uring 1.43× slower
//!     N=256 ops=128:  io_uring 1.09× faster
//!
//! Phase 6A (still valid): saves the park/unpark futex round-trip
//! when the kernel completes the SQE inline during `io_uring_enter`
//! — the common case for page-cache-resident reads. After flush we
//! peek the CQ; if our CQE is already there we return without
//! parking. That win is real and shows on the WARM matrix below;
//! the COLD matrix is the one that tells us whether io_uring's
//! async-submit advantage materialises when reads actually block.
//!
//! ## Results on the FIXED bench (real data; 2026-05-31)
//!
//!   N=64 coros, 128 ops/coro, 4 KiB random reads:
//!     WARM:  io_uring 5295 ns  vs  spawnBlocking 4419 ns  → 1.20× slower
//!     COLD:  io_uring 41764 ns vs  spawnBlocking 21939 ns → 1.90× slower
//!
//! The headline: io_uring loses to spawnBlocking even COLD here.
//! spawnBlocking gets cheap I/O parallelism from ~64 warm pool
//! threads each doing a synchronous pread; our io_uring path pays a
//! per-op `io_uring_enter` plus the eventfd → epoll → unpark wake
//! round-trip per CQE, with no cross-coro SQE batching. Caveat on
//! "cold": `fadvise(DONTNEED)` evicts the GUEST page cache, but the
//! macOS host still caches the VM disk image, so physical latency is
//! ~22 µs (warm-ish). On genuinely cold storage (NVMe ~100 µs,
//! spinning ~ms) spawnBlocking's fixed per-op thread cost stays put
//! while I/O wait dominates and io_uring's no-thread-per-op advantage
//! grows — so this result is a floor for io_uring's relative
//! standing, not a verdict on real cold storage. Feeds the Phase 7
//! "is io_uring the right call" question.
//!
//! NOTE: building this honest cold A/B uncovered a pre-existing
//! use-after-free in `spawnBlocking` (closure freed before the pool
//! thread's `unparkOne` finished touching it) that deadlocked the
//! scheduler under bursty blocking I/O. Fixed in lib.zig; guarded by
//! `bench/repro_blocking_deadlock.zig`. Without that fix the COLD
//! spawnBlocking column above hangs.

const std = @import("std");
const builtin = @import("builtin");
const volt = @import("volt");

fn nanosNow() i128 {
    var ts: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + @as(i128, ts.nsec);
}

const FILE_BYTES: usize = 64 * 1024 * 1024; // 64 MiB
const READ_BYTES: usize = 4096; // 4 KiB
const N_COROS: u32 = 64;
const OPS_PER_CORO: u32 = 128;
const TOTAL_OPS: u64 = @as(u64, N_COROS) * @as(u64, OPS_PER_CORO);

const ReaderCtx = struct {
    path: [:0]const u8,
    ops: u32,
    seed: u64,
};

fn readerCoro(ctx: *ReaderCtx) !void {
    var f = try volt.fs.File.open(ctx.path);
    defer f.close();

    // Per-coro PRNG so offsets are deterministic given the seed.
    var prng = std.Random.DefaultPrng.init(ctx.seed);
    const rng = prng.random();
    // Number of 4-KiB-aligned slots in the file.
    const max_slot: u64 = @as(u64, FILE_BYTES / READ_BYTES);

    var buf: [READ_BYTES]u8 = undefined;
    var i: u32 = 0;
    while (i < ctx.ops) : (i += 1) {
        const slot = rng.uintLessThan(u64, max_slot);
        const offset = slot * @as(u64, READ_BYTES);
        const n = try f.readAt(&buf, offset);
        std.debug.assert(n == READ_BYTES);
    }
}

const RootCtx = struct {
    path: [:0]const u8,
    wall_ns: i128 = 0,
};

fn benchRoot(ctx: *RootCtx) !void {
    const rt = volt.runtime();
    // The spawned coroutine's return type is the inferred error
    // set of `readerCoro` — use @TypeOf so we match exactly.
    const ReaderTask = @TypeOf(try rt.spawn(readerCoro, .{@as(*ReaderCtx, undefined)}));
    const tasks = try rt.allocator.alloc(ReaderTask, N_COROS);
    defer rt.allocator.free(tasks);
    var reader_ctxs: [N_COROS]ReaderCtx = undefined;

    // Seed each reader differently so they hit different offsets
    // (worst case for the disk's readahead heuristics — keeps
    // the bench dominated by per-op syscall cost rather than
    // page cache hits).
    for (&reader_ctxs, 0..) |*rc, i| {
        rc.* = .{
            .path = ctx.path,
            .ops = OPS_PER_CORO,
            .seed = 0xC0FFEE +% i,
        };
    }

    const start = nanosNow();
    for (tasks, 0..) |*t, i| {
        t.* = try rt.spawn(readerCoro, .{&reader_ctxs[i]});
    }
    for (tasks) |t| {
        _ = t.join() catch unreachable;
    }
    const end = nanosNow();
    ctx.wall_ns = end - start;
}

/// One full bench run: fresh Runtime, fresh workload. Returns
/// per-op ns plus whether the io_uring path was taken.
const RunResult = struct {
    ns_per_op: i128,
    ops_via_ring: u64,
};

const Mode = enum { auto, force_spawn_blocking };
const Cache = enum { warm, cold };

fn benchOnce(
    allocator: std.mem.Allocator,
    path: [:0]const u8,
    path_ptr: [*:0]const u8,
    mode: Mode,
    cache: Cache,
) !RunResult {
    var rt = try volt.Runtime.init(.{ .allocator = allocator });
    defer rt.deinit();

    // Honest A/B: in `force_spawn_blocking` mode we keep the
    // fs_rings allocated (so Runtime.deinit can free them) but
    // NULL the slice so `currentPRing` returns null and every
    // Reactor.fsX falls through to the spawnBlocking proxy.
    // Restore before deinit. This way both modes hit the SAME
    // process, SAME kernel, SAME filesystem, SAME everything
    // except the routing — true apples-to-apples on a single
    // machine.
    const saved_rings = rt.fs_rings;
    if (mode == .force_spawn_blocking) rt.fs_rings = null;
    defer rt.fs_rings = saved_rings;

    // COLD: evict the page cache immediately before the timed
    // region so every read misses cache and hits the block layer.
    // (No-op / unreachable on non-Linux — the cold matrix is
    // Linux-gated in main.)
    if (cache == .cold) evictPageCache(path_ptr);

    var ctx = RootCtx{ .path = path };
    try (try rt.run(benchRoot, .{&ctx}));

    return .{
        .ns_per_op = @divTrunc(ctx.wall_ns, @as(i128, TOTAL_OPS)),
        .ops_via_ring = rt.fs_ops_via_ring.load(.monotonic),
    };
}

// libc externs for file setup. open(2) is variadic — declare it
// that way so the mode arg actually rides the stack on Darwin
// x86_64 SysV ABI. (Same gotcha caught earlier in fs_ring.zig.)
extern "c" fn open(p: [*:0]const u8, flags: c_int, ...) c_int;
extern "c" fn close(fd: c_int) c_int;
extern "c" fn write(fd: c_int, buf: [*]const u8, count: usize) isize;
extern "c" fn fsync(fd: c_int) c_int;
extern "c" fn unlink(p: [*:0]const u8) c_int;
// posix_fadvise is Linux-only here; the sole call site is inside an
// `if (builtin.os.tag == .linux)` branch, so on Darwin the comptime
// condition prunes the branch and this symbol is never referenced —
// no link error against a symbol macOS libc doesn't export.
extern "c" fn posix_fadvise(fd: c_int, offset: i64, len: i64, advice: c_int) c_int;

const O_RDONLY: c_int = 0;
const O_WRONLY: c_int = 1;
const O_CREAT: c_int = 0o100;
const O_TRUNC: c_int = 0o1000;

// Linux <linux/fadvise.h>: arch-independent on our targets
// (arm64 / x86_64; only s390 historically swapped these).
const POSIX_FADV_RANDOM: c_int = 1;
const POSIX_FADV_DONTNEED: c_int = 4;

/// Create `path` and write `FILE_BYTES` of REAL (non-zero) data,
/// then fsync so the blocks are physically allocated on disk. This
/// is the load-bearing fix over the old `ftruncate`-only version:
/// a sparse file reads as instant zero-fill with no block I/O, which
/// makes the io_uring-vs-spawnBlocking comparison meaningless (see
/// the METHODOLOGY FIX note in the file header).
fn createBenchFile(path_ptr: [*:0]const u8) !void {
    const fd = open(path_ptr, O_WRONLY | O_CREAT | O_TRUNC, @as(c_uint, 0o644));
    if (fd < 0) return error.OpenFailed;
    defer _ = close(fd);

    // 1 MiB chunk with an index-derived pattern. Non-zero so no
    // filesystem hole-punch / zero-page optimisation can sneak the
    // data back into "no real block" territory.
    var chunk: [1024 * 1024]u8 = undefined;
    for (&chunk, 0..) |*b, i| b.* = @truncate(i *% 31 +% 7);

    var written: usize = 0;
    while (written < FILE_BYTES) : (written += chunk.len) {
        var off: usize = 0;
        while (off < chunk.len) {
            const n = write(fd, chunk[off..].ptr, chunk.len - off);
            if (n <= 0) return error.WriteFailed;
            off += @intCast(n);
        }
    }
    if (fsync(fd) != 0) return error.FsyncFailed;
}

/// Drop the file's pages from the page cache so the next reads hit
/// the block layer. Linux-only by construction (see the extern note
/// above). fsync first: DONTNEED can't evict dirty pages, and though
/// createBenchFile already fsync'd, warm reps may have re-dirtied
/// nothing (reads don't dirty) — the fsync here is cheap insurance.
/// POSIX_FADV_RANDOM additionally disables readahead so each 4 KiB
/// read stays a standalone cold block fetch rather than dragging in
/// neighbours.
fn evictPageCache(path_ptr: [*:0]const u8) void {
    if (builtin.os.tag == .linux) {
        const fd = open(path_ptr, O_RDONLY, @as(c_uint, 0));
        if (fd < 0) return;
        defer _ = close(fd);
        _ = fsync(fd);
        _ = posix_fadvise(fd, 0, @intCast(FILE_BYTES), POSIX_FADV_DONTNEED);
        _ = posix_fadvise(fd, 0, @intCast(FILE_BYTES), POSIX_FADV_RANDOM);
    }
}

fn removeFile(path_ptr: [*:0]const u8) void {
    _ = unlink(path_ptr);
}

const REPS = 5;
const WARMUPS = 2;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    const path_z = "/tmp/volt-bench-fs-read.dat\x00";
    const path_ptr: [*:0]const u8 = path_z[0 .. path_z.len - 1 :0];
    const path: [:0]const u8 = path_z[0 .. path_z.len - 1 :0];

    try createBenchFile(path_ptr);
    defer removeFile(path_ptr);

    std.debug.print("\n=== fs.File random read bench ===\n", .{});
    std.debug.print(
        "Platform: ReleaseFast, file={d} MiB (REAL data), read={d} B, coros={d}, ops/coro={d}\n",
        .{ FILE_BYTES / (1024 * 1024), READ_BYTES, N_COROS, OPS_PER_CORO },
    );
    std.debug.print("Total ops per run: {d}\n", .{TOTAL_OPS});

    // WARM matrix — page-cache-resident; pure runtime-overhead A/B.
    std.debug.print("\n--- WARM (page-cache-resident) ---\n", .{});
    const warm_auto = try medianOf(smp, path, path_ptr, .auto, .warm);
    const warm_force = try medianOf(smp, path, path_ptr, .force_spawn_blocking, .warm);
    printVerdict(warm_auto, warm_force);

    // COLD matrix — Linux-only (fadvise DONTNEED has no Darwin peer).
    if (builtin.os.tag == .linux) {
        std.debug.print("\n--- COLD (page cache evicted per rep via fadvise DONTNEED) ---\n", .{});
        const cold_auto = try medianOf(smp, path, path_ptr, .auto, .cold);
        const cold_force = try medianOf(smp, path, path_ptr, .force_spawn_blocking, .cold);
        printVerdict(cold_auto, cold_force);
    } else {
        std.debug.print("\n--- COLD matrix skipped: fadvise(DONTNEED) is Linux-only ---\n", .{});
    }
}

fn printVerdict(auto_median: i128, force_median: i128) void {
    const auto_ns_f: f64 = @floatFromInt(auto_median);
    const force_ns_f: f64 = @floatFromInt(force_median);
    const auto_ops_per_sec = (1.0e9 / auto_ns_f) * @as(f64, N_COROS);
    const force_ops_per_sec = (1.0e9 / force_ns_f) * @as(f64, N_COROS);

    std.debug.print("                ns/op (median of {d})   ops/sec\n", .{REPS});
    std.debug.print("  auto:         {d:>10}                {d:>10.0}\n", .{ auto_median, auto_ops_per_sec });
    std.debug.print("  spawnBlocking:{d:>10}                {d:>10.0}\n", .{ force_median, force_ops_per_sec });

    if (force_median > auto_median) {
        const speedup = force_ns_f / auto_ns_f;
        std.debug.print("  => {d:.2}× FASTER on the auto (io_uring) path\n", .{speedup});
    } else if (auto_median > force_median) {
        const slowdown = auto_ns_f / force_ns_f;
        std.debug.print("  => auto path is {d:.2}× SLOWER\n", .{slowdown});
    } else {
        std.debug.print("  => even (both within rounding)\n", .{});
    }
}

fn medianOf(
    allocator: std.mem.Allocator,
    path: [:0]const u8,
    path_ptr: [*:0]const u8,
    mode: Mode,
    cache: Cache,
) !i128 {
    const label = switch (mode) {
        .auto => "AUTO (io_uring on Linux, spawnBlocking elsewhere)",
        .force_spawn_blocking => "FORCED spawnBlocking",
    };
    std.debug.print("  {s}\n", .{label});

    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchOnce(allocator, path, path_ptr, mode, cache);
    var first_via_ring: u64 = 0;
    for (&samples, 0..) |*s, i| {
        const r = try benchOnce(allocator, path, path_ptr, mode, cache);
        s.* = r.ns_per_op;
        if (i == 0) first_via_ring = r.ops_via_ring;
    }
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));
    const med = samples[REPS / 2];
    const went_via_ring = first_via_ring > 0;
    std.debug.print(
        "    per-op: {d} ns (samples: {any})  via_ring: {}\n",
        .{ med, samples, went_via_ring },
    );
    return med;
}
