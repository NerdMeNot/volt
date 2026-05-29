//! Bench — fs.File random reads. Honest A/B between the Phase 2/3
//! io_uring path and the spawnBlocking fallback, on a SINGLE
//! machine (same kernel, same filesystem, same scheduler), so the
//! only varying axis is the routing.
//!
//! Workload:
//!   * Pre-allocated 64 MiB file in /tmp.
//!   * N coroutines × M random 4 KiB `readAt(buf, offset)` ops.
//!   * Reads uniformly across the file, page-aligned.
//!
//! Report per-op ns/op (median of 5 runs after 2 warmups), total
//! ops/sec, and the speedup ratio (or regression).
//!
//! ## Results timeline
//!
//! podman-machine arm64 VM, tmpfs-backed file (fully cached
//! after warmups), 4 KiB random reads.
//!
//! Pre-Phase-6A (only the spawnBlocking-fallback path landing):
//!   N=8   ops=64:   io_uring 1.88× slower
//!   N=64  ops=128:  io_uring 1.68× slower
//!   N=256 ops=128:  io_uring 1.19× slower
//!
//! Post-Phase-6A (inline-completion fast path):
//!   N=64  ops=128:  io_uring 1.43× slower  (still slower, but
//!                                            ~273 ns/op better)
//!   N=256 ops=128:  io_uring 1.09× FASTER  (trend flipped)
//!
//! Phase 6A saves the park/unpark futex round-trip when the
//! kernel completes the SQE inline (during `io_uring_enter`),
//! which is the common case for page-cached reads. After flush,
//! we peek the CQ; if our CQE is already there, we return
//! without parking. Other CQEs in the batch are dispatched
//! normally.
//!
//! Why N=64 still loses: at low concurrency the spawnBlocking
//! pool is fast (pool threads are warm and abundant; futex
//! wake is cheap), and our remaining per-op overhead in
//! io_uring (mostly the `io_uring_enter` syscall, ~1µs) still
//! exceeds the thread-hop cost.
//!
//! Remaining Phase 6 optimisations that would close more of
//! the gap:
//!   * Batch SQE submission — defer flush to a worker block
//!     boundary so multiple coro submits amortise one
//!     io_uring_enter.
//!   * IORING_REGISTER_BUFFERS for hot buffers (avoids per-op
//!     get_user_pages on the kernel side).
//!   * IORING_SETUP_SUBMIT_ALL — already on.
//!
//! Updated memo §8 reflects reality.

const std = @import("std");
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

fn benchOnce(allocator: std.mem.Allocator, path: [:0]const u8, mode: Mode) !RunResult {
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

    var ctx = RootCtx{ .path = path };
    try (try rt.run(benchRoot, .{&ctx}));

    return .{
        .ns_per_op = @divTrunc(ctx.wall_ns, @as(i128, TOTAL_OPS)),
        .ops_via_ring = rt.fs_ops_via_ring.load(.monotonic),
    };
}

// libc externs for the setup file. open(2) is variadic — declare
// it that way so the mode arg actually rides the stack on Darwin
// x86_64 SysV ABI. (Same gotcha caught earlier in fs_ring.zig.)
extern "c" fn open(p: [*:0]const u8, flags: c_int, ...) c_int;
extern "c" fn close(fd: c_int) c_int;
extern "c" fn ftruncate(fd: c_int, len: i64) c_int;
extern "c" fn unlink(p: [*:0]const u8) c_int;

const O_WRONLY: c_int = 1;
const O_CREAT: c_int = 0o100;
const O_TRUNC: c_int = 0o1000;

/// Allocate a freshly-created `path` of `FILE_BYTES` zeros.
/// Removes any prior file at the same path.
fn createBenchFile(path: [*:0]const u8) !void {
    // Open + truncate to 0; then ftruncate up to FILE_BYTES so
    // the page table has the right size. Writing zeros byte by
    // byte would dirty cache before the bench even starts.
    const fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, @as(c_uint, 0o644));
    if (fd < 0) return error.OpenFailed;
    defer _ = close(fd);
    if (ftruncate(fd, @intCast(FILE_BYTES)) != 0) return error.TruncateFailed;
}

fn removeFile(path: [*:0]const u8) void {
    _ = unlink(path);
}

const REPS = 5;
const WARMUPS = 2;

pub fn main() !void {
    const smp = std.heap.smp_allocator;

    // Build the bench file. We use /tmp/volt-bench-fs-read.dat.
    const path_z = "/tmp/volt-bench-fs-read.dat\x00";
    const path_ptr: [*:0]const u8 = path_z[0 .. path_z.len - 1 :0];
    const path: [:0]const u8 = path_z[0 .. path_z.len - 1 :0];

    try createBenchFile(path_ptr);
    defer removeFile(path_ptr);

    std.debug.print("\n=== fs.File random read bench ===\n", .{});
    std.debug.print(
        "Platform: ReleaseFast, file={d} MiB, read={d} B, coros={d}, ops/coro={d}\n",
        .{ FILE_BYTES / (1024 * 1024), READ_BYTES, N_COROS, OPS_PER_CORO },
    );
    std.debug.print("Total ops per run: {d}\n\n", .{TOTAL_OPS});

    // Run both modes back-to-back: same process, same
    // filesystem, same page cache, same scheduler. The only
    // varying axis is the routing — proper A/B.
    const auto_median = try medianOf(smp, path, .auto);
    const force_median = try medianOf(smp, path, .force_spawn_blocking);

    const auto_ns_f: f64 = @floatFromInt(auto_median);
    const force_ns_f: f64 = @floatFromInt(force_median);
    const auto_ops_per_sec = (1.0e9 / auto_ns_f) * @as(f64, N_COROS);
    const force_ops_per_sec = (1.0e9 / force_ns_f) * @as(f64, N_COROS);

    std.debug.print("                ns/op (median of {d})   ops/sec\n", .{REPS});
    std.debug.print("  auto:         {d:>10}                {d:>10.0}\n", .{ auto_median, auto_ops_per_sec });
    std.debug.print("  spawnBlocking:{d:>10}                {d:>10.0}\n", .{ force_median, force_ops_per_sec });

    // If io_uring is available on this host, `auto` should be
    // faster (lower ns/op). Otherwise both modes hit the same
    // spawnBlocking path and the numbers should be within noise.
    if (force_median > auto_median) {
        const speedup = force_ns_f / auto_ns_f;
        std.debug.print("\n  Speedup:     {d:.2}× faster on the auto path\n", .{speedup});
    } else if (auto_median > force_median) {
        const slowdown = auto_ns_f / force_ns_f;
        std.debug.print("\n  REGRESSION:  auto path is {d:.2}× SLOWER\n", .{slowdown});
    } else {
        std.debug.print("\n  Even:        both paths within rounding\n", .{});
    }
}

fn medianOf(allocator: std.mem.Allocator, path: [:0]const u8, mode: Mode) !i128 {
    // Print mode header
    const label = switch (mode) {
        .auto => "AUTO (io_uring on Linux, spawnBlocking elsewhere)",
        .force_spawn_blocking => "FORCED spawnBlocking",
    };
    std.debug.print("=== {s} ===\n", .{label});

    var samples: [REPS]i128 = undefined;
    var w: u32 = 0;
    while (w < WARMUPS) : (w += 1) _ = try benchOnce(allocator, path, mode);
    var first_via_ring: u64 = 0;
    for (&samples, 0..) |*s, i| {
        const r = try benchOnce(allocator, path, mode);
        s.* = r.ns_per_op;
        if (i == 0) first_via_ring = r.ops_via_ring;
    }
    std.mem.sort(i128, &samples, {}, std.sort.asc(i128));
    const med = samples[REPS / 2];
    const went_via_ring = first_via_ring > 0;
    std.debug.print(
        "  per-op: {d} ns (samples: {any})  via_ring: {}\n\n",
        .{ med, samples, went_via_ring },
    );
    return med;
}
