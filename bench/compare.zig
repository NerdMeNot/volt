//! Volt vs Tokio Comparative Benchmark
//!
//! Builds and runs Volt (Zig) and Tokio (Rust) benchmarks,
//! then displays a formatted comparison table with resource usage.
//!
//! Usage: zig build compare

const std = @import("std");
const builtin = @import("builtin");

// ============================================================================
// ANSI Colors
// ============================================================================

const Color = struct {
    const red = "\x1b[0;31m";
    const green = "\x1b[0;32m";
    const yellow = "\x1b[1;33m";
    const blue = "\x1b[0;34m";
    const cyan = "\x1b[0;36m";
    const dim = "\x1b[2m";
    const bold = "\x1b[1m";
    const reset = "\x1b[0m";
};

// ============================================================================
// Benchmark Entry
// ============================================================================

const BenchEntry = struct {
    ns_per_op: f64 = 0,
    ops_per_sec: f64 = 0,
    bytes_per_op: f64 = 0,
    allocs_per_op: f64 = 0,
    peak_bytes: usize = 0,
};

const BenchmarkResults = struct {
    // Metadata (keys match JSON output from both benchmarks)
    sync_ops: u64 = 0,
    channel_ops: u64 = 0,
    async_ops: u64 = 0,
    iterations: u64 = 0,
    warmup: u64 = 0,
    workers: u64 = 0,

    // Tier 1: Sync
    mutex: BenchEntry = .{},
    rwlock_read: BenchEntry = .{},
    rwlock_write: BenchEntry = .{},
    semaphore: BenchEntry = .{},
    oncecell_get: BenchEntry = .{},
    oncecell_set: BenchEntry = .{},
    notify: BenchEntry = .{},
    barrier: BenchEntry = .{},

    // Tier 2: Channels
    channel_send: BenchEntry = .{},
    channel_recv: BenchEntry = .{},
    channel_roundtrip: BenchEntry = .{},
    oneshot: BenchEntry = .{},
    broadcast: BenchEntry = .{},
    watch: BenchEntry = .{},

    // Tier 3: Async contended
    channel_mpmc: BenchEntry = .{},
    mutex_contended: BenchEntry = .{},
    rwlock_contended: BenchEntry = .{},
    semaphore_contended: BenchEntry = .{},

    // Tier 4: Task scheduling
    task_spawn: BenchEntry = .{},
    task_spawn_batch: BenchEntry = .{},
    blocking_spawn: BenchEntry = .{},
};

// ============================================================================
// JSON Parsing
// ============================================================================

fn parseJsonResults(json_str: []const u8) BenchmarkResults {
    var results = BenchmarkResults{};
    var current_bench: ?*BenchEntry = null;

    var lines = std.mem.splitScalar(u8, json_str, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0 or trimmed[0] == '{' or trimmed[0] == '}') continue;

        if (std.mem.indexOf(u8, trimmed, ":")) |colon_idx| {
            const key = std.mem.trim(u8, trimmed[0..colon_idx], " \t\"");
            const value_str = std.mem.trim(u8, trimmed[colon_idx + 1 ..], " \t,");

            // Remove trailing brace if present
            if (std.mem.endsWith(u8, value_str, "{")) {
                // Start of a benchmark entry
                current_bench = getBenchEntry(&results, key);
                continue;
            }

            // Metadata (keys match JSON output from volt_bench.zig and main.rs)
            if (std.mem.eql(u8, key, "sync_ops")) {
                results.sync_ops = std.fmt.parseInt(u64, value_str, 10) catch 0;
            } else if (std.mem.eql(u8, key, "channel_ops")) {
                results.channel_ops = std.fmt.parseInt(u64, value_str, 10) catch 0;
            } else if (std.mem.eql(u8, key, "async_ops")) {
                results.async_ops = std.fmt.parseInt(u64, value_str, 10) catch 0;
            } else if (std.mem.eql(u8, key, "iterations")) {
                results.iterations = std.fmt.parseInt(u64, value_str, 10) catch 0;
            } else if (std.mem.eql(u8, key, "warmup")) {
                results.warmup = std.fmt.parseInt(u64, value_str, 10) catch 0;
            } else if (std.mem.eql(u8, key, "workers")) {
                results.workers = std.fmt.parseInt(u64, value_str, 10) catch 0;
            }
            // Benchmark fields
            else if (current_bench) |bench| {
                if (std.mem.eql(u8, key, "ns_per_op")) {
                    bench.ns_per_op = std.fmt.parseFloat(f64, value_str) catch 0;
                } else if (std.mem.eql(u8, key, "ops_per_sec")) {
                    bench.ops_per_sec = std.fmt.parseFloat(f64, value_str) catch 0;
                } else if (std.mem.eql(u8, key, "bytes_per_op")) {
                    bench.bytes_per_op = std.fmt.parseFloat(f64, value_str) catch 0;
                } else if (std.mem.eql(u8, key, "allocs_per_op")) {
                    bench.allocs_per_op = std.fmt.parseFloat(f64, value_str) catch 0;
                } else if (std.mem.eql(u8, key, "peak_bytes")) {
                    bench.peak_bytes = std.fmt.parseInt(usize, value_str, 10) catch 0;
                }
            }
        }
    }

    return results;
}

fn getBenchEntry(results: *BenchmarkResults, name: []const u8) ?*BenchEntry {
    // Tier 1: Sync
    if (std.mem.eql(u8, name, "mutex")) return &results.mutex;
    if (std.mem.eql(u8, name, "rwlock_read")) return &results.rwlock_read;
    if (std.mem.eql(u8, name, "rwlock_write")) return &results.rwlock_write;
    if (std.mem.eql(u8, name, "semaphore")) return &results.semaphore;
    if (std.mem.eql(u8, name, "oncecell_get")) return &results.oncecell_get;
    if (std.mem.eql(u8, name, "oncecell_set")) return &results.oncecell_set;
    if (std.mem.eql(u8, name, "notify")) return &results.notify;
    if (std.mem.eql(u8, name, "barrier")) return &results.barrier;
    // Tier 2: Channels
    if (std.mem.eql(u8, name, "channel_send")) return &results.channel_send;
    if (std.mem.eql(u8, name, "channel_recv")) return &results.channel_recv;
    if (std.mem.eql(u8, name, "channel_roundtrip")) return &results.channel_roundtrip;
    if (std.mem.eql(u8, name, "oneshot")) return &results.oneshot;
    if (std.mem.eql(u8, name, "broadcast")) return &results.broadcast;
    if (std.mem.eql(u8, name, "watch")) return &results.watch;
    // Tier 3: Async contended
    if (std.mem.eql(u8, name, "channel_mpmc")) return &results.channel_mpmc;
    if (std.mem.eql(u8, name, "mutex_contended")) return &results.mutex_contended;
    if (std.mem.eql(u8, name, "rwlock_contended")) return &results.rwlock_contended;
    if (std.mem.eql(u8, name, "semaphore_contended")) return &results.semaphore_contended;
    // Tier 4: Task scheduling
    if (std.mem.eql(u8, name, "task_spawn")) return &results.task_spawn;
    if (std.mem.eql(u8, name, "task_spawn_batch")) return &results.task_spawn_batch;
    if (std.mem.eql(u8, name, "blocking_spawn")) return &results.blocking_spawn;
    return null;
}

// ============================================================================
// Table Drawing
// ============================================================================

const TableWriter = struct {
    const w1 = 22; // Benchmark name
    const w2 = 10; // Volt ns
    const w3 = 10; // Tokio ns
    const w4 = 10; // Volt B/op
    const w5 = 10; // Tokio B/op
    const w6 = 14; // Winner

    fn printHeader(title: []const u8) void {
        std.debug.print("\n{s}{s}{s}\n", .{ Color.bold, title, Color.reset });
    }

    fn printTopBorder() void {
        std.debug.print("{s}", .{"\u{250C}"});
        printDash(w1 + 2);
        std.debug.print("{s}", .{"\u{252C}"});
        printDash(w2 + 2);
        std.debug.print("{s}", .{"\u{252C}"});
        printDash(w3 + 2);
        std.debug.print("{s}", .{"\u{252C}"});
        printDash(w4 + 2);
        std.debug.print("{s}", .{"\u{252C}"});
        printDash(w5 + 2);
        std.debug.print("{s}", .{"\u{252C}"});
        printDash(w6 + 2);
        std.debug.print("{s}\n", .{"\u{2510}"});
    }

    fn printMidBorder() void {
        std.debug.print("{s}", .{"\u{251C}"});
        printDash(w1 + 2);
        std.debug.print("{s}", .{"\u{253C}"});
        printDash(w2 + 2);
        std.debug.print("{s}", .{"\u{253C}"});
        printDash(w3 + 2);
        std.debug.print("{s}", .{"\u{253C}"});
        printDash(w4 + 2);
        std.debug.print("{s}", .{"\u{253C}"});
        printDash(w5 + 2);
        std.debug.print("{s}", .{"\u{253C}"});
        printDash(w6 + 2);
        std.debug.print("{s}\n", .{"\u{2524}"});
    }

    fn printBottomBorder() void {
        std.debug.print("{s}", .{"\u{2514}"});
        printDash(w1 + 2);
        std.debug.print("{s}", .{"\u{2534}"});
        printDash(w2 + 2);
        std.debug.print("{s}", .{"\u{2534}"});
        printDash(w3 + 2);
        std.debug.print("{s}", .{"\u{2534}"});
        printDash(w4 + 2);
        std.debug.print("{s}", .{"\u{2534}"});
        printDash(w5 + 2);
        std.debug.print("{s}", .{"\u{2534}"});
        printDash(w6 + 2);
        std.debug.print("{s}\n", .{"\u{2518}"});
    }

    fn printDash(count: usize) void {
        for (0..count) |_| {
            std.debug.print("{s}", .{"\u{2500}"});
        }
    }

    fn printTableHeader() void {
        std.debug.print("\u{2502} {s:<" ++ std.fmt.comptimePrint("{d}", .{w1}) ++
            "} \u{2502} {s:>" ++ std.fmt.comptimePrint("{d}", .{w2}) ++
            "} \u{2502} {s:>" ++ std.fmt.comptimePrint("{d}", .{w3}) ++
            "} \u{2502} {s:>" ++ std.fmt.comptimePrint("{d}", .{w4}) ++
            "} \u{2502} {s:>" ++ std.fmt.comptimePrint("{d}", .{w5}) ++
            "} \u{2502} {s:<" ++ std.fmt.comptimePrint("{d}", .{w6}) ++ "} \u{2502}\n", .{
            "Benchmark", "Volt ns", "Tokio ns", "Volt B/op", "Tokio B/op", "Winner",
        });
    }

    fn printRow(name: []const u8, volt: BenchEntry, tokio: BenchEntry, bufs: *Bufs) void {
        const volt_ns = std.fmt.bufPrint(&bufs.b1, "{d:.1}", .{volt.ns_per_op}) catch "N/A";
        const tokio_ns = std.fmt.bufPrint(&bufs.b2, "{d:.1}", .{tokio.ns_per_op}) catch "N/A";
        const volt_bytes = std.fmt.bufPrint(&bufs.b3, "{d:.1}", .{volt.bytes_per_op}) catch "N/A";
        const tokio_bytes = std.fmt.bufPrint(&bufs.b4, "{d:.1}", .{tokio.bytes_per_op}) catch "N/A";
        const winner = findWinner(volt.ns_per_op, tokio.ns_per_op, &bufs.b5);

        std.debug.print("\u{2502} {s:<" ++ std.fmt.comptimePrint("{d}", .{w1}) ++
            "} \u{2502} {s:>" ++ std.fmt.comptimePrint("{d}", .{w2}) ++
            "} \u{2502} {s:>" ++ std.fmt.comptimePrint("{d}", .{w3}) ++
            "} \u{2502} {s:>" ++ std.fmt.comptimePrint("{d}", .{w4}) ++
            "} \u{2502} {s:>" ++ std.fmt.comptimePrint("{d}", .{w5}) ++
            "} \u{2502} {s:<" ++ std.fmt.comptimePrint("{d}", .{w6}) ++ "} \u{2502}\n", .{
            name, volt_ns, tokio_ns, volt_bytes, tokio_bytes, winner,
        });
    }

    fn findWinner(volt_ns: f64, tokio_ns: f64, buf: *[48]u8) []const u8 {
        if (volt_ns == 0 and tokio_ns == 0) return "N/A";
        if (volt_ns == 0) return std.fmt.bufPrint(buf, "{s}Tokio{s}", .{ Color.yellow, Color.reset }) catch "Tokio";
        if (tokio_ns == 0) return std.fmt.bufPrint(buf, "{s}Volt{s}", .{ Color.green, Color.reset }) catch "Volt";

        const ratio = tokio_ns / volt_ns;

        if (@abs(ratio - 1.0) < 0.05) {
            return "Tie";
        }

        if (volt_ns < tokio_ns) {
            return std.fmt.bufPrint(buf, "{s}Volt +{d:.1}x{s}", .{ Color.green, ratio, Color.reset }) catch "Volt";
        } else {
            const inv_ratio = volt_ns / tokio_ns;
            return std.fmt.bufPrint(buf, "{s}Tokio +{d:.1}x{s}", .{ Color.yellow, inv_ratio, Color.reset }) catch "Tokio";
        }
    }

    const Bufs = struct {
        b1: [32]u8 = undefined,
        b2: [32]u8 = undefined,
        b3: [32]u8 = undefined,
        b4: [32]u8 = undefined,
        b5: [48]u8 = undefined,
    };
};

// ============================================================================
// Subprocess Execution
// ============================================================================

fn runCommand(allocator: std.mem.Allocator, argv: []const []const u8, cwd: ?[]const u8) !struct { stdout: []u8, stderr: []u8, success: bool } {
    var child = std.process.Child.init(argv, allocator);
    if (cwd) |dir| {
        child.cwd = dir;
    }
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;

    try child.spawn();

    var stdout_buf: [4 * 1024 * 1024]u8 = undefined;
    var stderr_buf: [4 * 1024 * 1024]u8 = undefined;

    const stdout_len = child.stdout.?.readAll(&stdout_buf) catch 0;
    const stderr_len = child.stderr.?.readAll(&stderr_buf) catch 0;

    const term = try child.wait();
    const success = term.Exited == 0;

    const stdout = try allocator.dupe(u8, stdout_buf[0..stdout_len]);
    const stderr = try allocator.dupe(u8, stderr_buf[0..stderr_len]);

    return .{ .stdout = stdout, .stderr = stderr, .success = success };
}

fn getProjectDir(allocator: std.mem.Allocator) ![]const u8 {
    const exe_path = try std.fs.selfExeDirPathAlloc(allocator);
    defer allocator.free(exe_path);

    if (std.mem.indexOf(u8, exe_path, ".zig-cache")) |_| {
        const project_dir = try std.fs.path.resolve(allocator, &.{ exe_path, "..", "..", ".." });
        return project_dir;
    } else {
        const project_dir = try std.fs.path.resolve(allocator, &.{ exe_path, "..", ".." });
        return project_dir;
    }
}

// ============================================================================
// Summary Stats
// ============================================================================

const CompareResult = struct {
    volt_wins: u32 = 0,
    tokio_wins: u32 = 0,
    ties: u32 = 0,
    volt_total_bytes: f64 = 0,
    tokio_total_bytes: f64 = 0,
    volt_total_allocs: f64 = 0,
    tokio_total_allocs: f64 = 0,

    fn compare(self: *CompareResult, volt: BenchEntry, tokio: BenchEntry) void {
        if (volt.ns_per_op > 0 and tokio.ns_per_op > 0) {
            const ratio = tokio.ns_per_op / volt.ns_per_op;
            if (@abs(ratio - 1.0) < 0.05) {
                self.ties += 1;
            } else if (volt.ns_per_op < tokio.ns_per_op) {
                self.volt_wins += 1;
            } else {
                self.tokio_wins += 1;
            }
        }

        self.volt_total_bytes += volt.bytes_per_op;
        self.tokio_total_bytes += tokio.bytes_per_op;
        self.volt_total_allocs += volt.allocs_per_op;
        self.tokio_total_allocs += tokio.allocs_per_op;
    }
};

// ============================================================================
// Main
// ============================================================================

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const project_dir = try getProjectDir(allocator);
    defer allocator.free(project_dir);

    const bench_dir = try std.fs.path.join(allocator, &.{ project_dir, "bench" });
    defer allocator.free(bench_dir);

    const rust_dir = try std.fs.path.join(allocator, &.{ bench_dir, "rust_bench" });
    defer allocator.free(rust_dir);

    // Print header
    std.debug.print("{s}══════════════════════════════════════════════════════════════════════════════════════{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("{s}                    VOLT vs TOKIO COMPARATIVE BENCHMARK{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("{s}══════════════════════════════════════════════════════════════════════════════════════{s}\n\n", .{ Color.bold, Color.reset });

    // Build Rust/Tokio benchmark
    std.debug.print("{s}Building Tokio benchmark...{s}\n", .{ Color.blue, Color.reset });
    const cargo_result = try runCommand(allocator, &.{ "cargo", "build", "--release" }, rust_dir);
    defer allocator.free(cargo_result.stdout);
    defer allocator.free(cargo_result.stderr);

    if (!cargo_result.success) {
        std.debug.print("{s}Failed to build Tokio benchmark{s}\n", .{ Color.red, Color.reset });
        std.debug.print("{s}\n", .{cargo_result.stderr});
        return;
    }
    std.debug.print("{s}✓ Tokio build complete{s}\n", .{ Color.green, Color.reset });

    // Run Volt benchmark
    std.debug.print("{s}Running Volt benchmark...{s}\n", .{ Color.blue, Color.reset });

    const zig_out_bench = try std.fs.path.join(allocator, &.{ project_dir, "zig-out", "bench" });
    defer allocator.free(zig_out_bench);
    const volt_bench_path = try std.fs.path.join(allocator, &.{ zig_out_bench, "volt_bench" });
    defer allocator.free(volt_bench_path);

    const volt_result = try runCommand(allocator, &.{ volt_bench_path, "--json" }, bench_dir);
    defer allocator.free(volt_result.stdout);
    defer allocator.free(volt_result.stderr);

    if (!volt_result.success) {
        std.debug.print("{s}Failed to run Volt benchmark{s}\n", .{ Color.red, Color.reset });
        std.debug.print("stdout: {s}\n", .{volt_result.stdout});
        std.debug.print("stderr: {s}\n", .{volt_result.stderr});
        return;
    }
    std.debug.print("{s}✓ Volt benchmark complete{s}\n", .{ Color.green, Color.reset });

    // Run Tokio benchmark
    std.debug.print("{s}Running Tokio benchmark...{s}\n", .{ Color.blue, Color.reset });

    const tokio_bench_path = try std.fs.path.join(allocator, &.{ rust_dir, "target", "release", "volt_rust_bench" });
    defer allocator.free(tokio_bench_path);

    const tokio_result = try runCommand(allocator, &.{ tokio_bench_path, "--json" }, rust_dir);
    defer allocator.free(tokio_result.stdout);
    defer allocator.free(tokio_result.stderr);

    if (!tokio_result.success) {
        std.debug.print("{s}Failed to run Tokio benchmark{s}\n", .{ Color.red, Color.reset });
        std.debug.print("{s}\n", .{tokio_result.stderr});
        return;
    }
    std.debug.print("{s}✓ Tokio benchmark complete{s}\n\n", .{ Color.green, Color.reset });

    // Parse results
    const volt = parseJsonResults(volt_result.stdout);
    const tokio = parseJsonResults(tokio_result.stdout);

    var bufs = TableWriter.Bufs{};
    var compare = CompareResult{};

    // Display comparison
    std.debug.print("{s}══════════════════════════════════════════════════════════════════════════════════════{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("{s}                         VOLT vs TOKIO COMPARISON{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("{s}══════════════════════════════════════════════════════════════════════════════════════{s}\n", .{ Color.bold, Color.reset });

    // 1. Synchronization (uncontended)
    TableWriter.printHeader("1. SYNCHRONIZATION - Uncontended (lower is better)");
    TableWriter.printTopBorder();
    TableWriter.printTableHeader();
    TableWriter.printMidBorder();
    TableWriter.printRow("Mutex", volt.mutex, tokio.mutex, &bufs);
    compare.compare(volt.mutex, tokio.mutex);
    TableWriter.printRow("RwLock (read)", volt.rwlock_read, tokio.rwlock_read, &bufs);
    compare.compare(volt.rwlock_read, tokio.rwlock_read);
    TableWriter.printRow("RwLock (write)", volt.rwlock_write, tokio.rwlock_write, &bufs);
    compare.compare(volt.rwlock_write, tokio.rwlock_write);
    TableWriter.printRow("Semaphore", volt.semaphore, tokio.semaphore, &bufs);
    compare.compare(volt.semaphore, tokio.semaphore);
    TableWriter.printBottomBorder();

    // 2. Channels
    TableWriter.printHeader("2. CHANNELS (lower is better)");
    TableWriter.printTopBorder();
    TableWriter.printTableHeader();
    TableWriter.printMidBorder();
    TableWriter.printRow("Channel send", volt.channel_send, tokio.channel_send, &bufs);
    compare.compare(volt.channel_send, tokio.channel_send);
    TableWriter.printRow("Channel recv", volt.channel_recv, tokio.channel_recv, &bufs);
    compare.compare(volt.channel_recv, tokio.channel_recv);
    TableWriter.printRow("Channel roundtrip", volt.channel_roundtrip, tokio.channel_roundtrip, &bufs);
    compare.compare(volt.channel_roundtrip, tokio.channel_roundtrip);
    TableWriter.printRow("Oneshot", volt.oneshot, tokio.oneshot, &bufs);
    compare.compare(volt.oneshot, tokio.oneshot);
    TableWriter.printRow("Broadcast (4 recv)", volt.broadcast, tokio.broadcast, &bufs);
    compare.compare(volt.broadcast, tokio.broadcast);
    TableWriter.printRow("Watch", volt.watch, tokio.watch, &bufs);
    compare.compare(volt.watch, tokio.watch);
    TableWriter.printBottomBorder();

    // 3. Once Cell & Coordination
    TableWriter.printHeader("3. ONCE CELL & COORDINATION (lower is better)");
    TableWriter.printTopBorder();
    TableWriter.printTableHeader();
    TableWriter.printMidBorder();
    TableWriter.printRow("OnceCell get (hot)", volt.oncecell_get, tokio.oncecell_get, &bufs);
    compare.compare(volt.oncecell_get, tokio.oncecell_get);
    TableWriter.printRow("OnceCell set", volt.oncecell_set, tokio.oncecell_set, &bufs);
    compare.compare(volt.oncecell_set, tokio.oncecell_set);
    TableWriter.printRow("Barrier", volt.barrier, tokio.barrier, &bufs);
    compare.compare(volt.barrier, tokio.barrier);
    TableWriter.printRow("Notify", volt.notify, tokio.notify, &bufs);
    compare.compare(volt.notify, tokio.notify);
    TableWriter.printBottomBorder();

    // 4. Async Contended
    TableWriter.printHeader("4. ASYNC CONTENDED (lower is better)");
    TableWriter.printTopBorder();
    TableWriter.printTableHeader();
    TableWriter.printMidBorder();
    TableWriter.printRow("Mutex (4 tasks)", volt.mutex_contended, tokio.mutex_contended, &bufs);
    compare.compare(volt.mutex_contended, tokio.mutex_contended);
    TableWriter.printRow("RwLock (4R + 2W)", volt.rwlock_contended, tokio.rwlock_contended, &bufs);
    compare.compare(volt.rwlock_contended, tokio.rwlock_contended);
    TableWriter.printRow("Semaphore (8T, 2 perm)", volt.semaphore_contended, tokio.semaphore_contended, &bufs);
    compare.compare(volt.semaphore_contended, tokio.semaphore_contended);
    TableWriter.printRow("Channel MPMC (4P + 4C)", volt.channel_mpmc, tokio.channel_mpmc, &bufs);
    compare.compare(volt.channel_mpmc, tokio.channel_mpmc);
    TableWriter.printBottomBorder();

    // 5. Task Scheduling
    TableWriter.printHeader("5. TASK SCHEDULING (lower is better)");
    TableWriter.printTopBorder();
    TableWriter.printTableHeader();
    TableWriter.printMidBorder();
    TableWriter.printRow("Spawn+await (noop)", volt.task_spawn, tokio.task_spawn, &bufs);
    compare.compare(volt.task_spawn, tokio.task_spawn);
    TableWriter.printRow("Spawn batch (100)", volt.task_spawn_batch, tokio.task_spawn_batch, &bufs);
    compare.compare(volt.task_spawn_batch, tokio.task_spawn_batch);
    TableWriter.printRow("Blocking spawn+wait", volt.blocking_spawn, tokio.blocking_spawn, &bufs);
    compare.compare(volt.blocking_spawn, tokio.blocking_spawn);
    TableWriter.printBottomBorder();

    // Legend
    std.debug.print("\n{s}══════════════════════════════════════════════════════════════════════════════════════{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("{s}                                     LEGEND{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("{s}══════════════════════════════════════════════════════════════════════════════════════{s}\n\n", .{ Color.bold, Color.reset });
    std.debug.print("  {s}Volt{s}       = Volt async runtime (Zig)\n", .{ Color.green, Color.reset });
    std.debug.print("  {s}Tokio{s}      = Tokio async runtime (Rust)\n\n", .{ Color.yellow, Color.reset });
    std.debug.print("  Sync ops: {d}  |  Channel ops: {d}  |  Async ops: {d}\n", .{ volt.sync_ops, volt.channel_ops, volt.async_ops });
    std.debug.print("  Iterations: {d} (after {d} warmup)\n", .{ volt.iterations, volt.warmup });
    std.debug.print("  Workers: {d}\n", .{volt.workers});

    // Summary
    std.debug.print("\n{s}══════════════════════════════════════════════════════════════════════════════════════{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("{s}                                    SUMMARY{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("{s}══════════════════════════════════════════════════════════════════════════════════════{s}\n\n", .{ Color.bold, Color.reset });

    const total = compare.volt_wins + compare.tokio_wins + compare.ties;
    std.debug.print("  {s}Performance:{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("    Benchmarks compared:   {d}\n", .{total});
    std.debug.print("    {s}Volt wins:{s}             {d}\n", .{ Color.green, Color.reset, compare.volt_wins });
    std.debug.print("    {s}Tokio wins:{s}            {d}\n", .{ Color.yellow, Color.reset, compare.tokio_wins });
    std.debug.print("    Ties:                  {d}\n\n", .{compare.ties});

    std.debug.print("  {s}Resource Usage (total B/op across all benchmarks):{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("    {s}Volt:{s}                  {d:.1} bytes/op\n", .{ Color.green, Color.reset, compare.volt_total_bytes });
    std.debug.print("    {s}Tokio:{s}                 {d:.1} bytes/op\n\n", .{ Color.yellow, Color.reset, compare.tokio_total_bytes });

    std.debug.print("  {s}Allocations (total allocs/op across all benchmarks):{s}\n", .{ Color.bold, Color.reset });
    std.debug.print("    {s}Volt:{s}                  {d:.4} allocs/op\n", .{ Color.green, Color.reset, compare.volt_total_allocs });
    std.debug.print("    {s}Tokio:{s}                 {d:.4} allocs/op\n\n", .{ Color.yellow, Color.reset, compare.tokio_total_allocs });

    if (compare.volt_wins > compare.tokio_wins) {
        std.debug.print("  {s}{s}Volt leads overall!{s}\n\n", .{ Color.green, Color.bold, Color.reset });
    } else if (compare.tokio_wins > compare.volt_wins) {
        std.debug.print("  {s}{s}Tokio leads overall{s}\n\n", .{ Color.yellow, Color.bold, Color.reset });
    } else {
        std.debug.print("  {s}{s}It's a tie!{s}\n\n", .{ Color.cyan, Color.bold, Color.reset });
    }

    std.debug.print("  {s}Note:{s} Volt is inspired by Tokio's design. Thanks to the\n", .{ Color.bold, Color.reset });
    std.debug.print("  Tokio team for their excellent work and documentation!\n\n", .{});
}
