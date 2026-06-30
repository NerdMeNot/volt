//! io_uring environment probe.
//!
//! Run inside the Linux dev container (see scripts/probe-linux.sh)
//! to verify io_uring is actually usable for the async file I/O
//! work (issue #17 / docs/internals/async-fs-io.md):
//!
//!   * basic `io_uring_setup` works (kernel new enough, sysctl /
//!     seccomp not blocking)
//!   * the features we depend on are present (`IORING_FEAT_*`)
//!   * the modern setup flags we plan to use are accepted
//!     (`SINGLE_ISSUER`, `DEFER_TASKRUN`, `SUBMIT_ALL`)
//!   * the opcodes we depend on are supported (`READ`, `WRITE`,
//!     `FSYNC`, `OPENAT`, `CLOSE`, `ASYNC_CANCEL`)
//!
//! Output is human-readable on stderr; exits 0 if everything we
//! need works, non-zero otherwise. The non-zero path tells you
//! Phase 2 is blocked on environment fixes (e.g.
//! kernel.io_uring_disabled or a seccomp profile that needs
//! adjusting) before any code is written.

const std = @import("std");
const linux = std.os.linux;
const print = std.debug.print;

/// Tiny libc-free /proc file read — std.fs is gone in Zig 0.16
/// and the runtime convention is to use the raw linux syscalls.
/// Returns the trimmed contents (NUL/newline-stripped) or null on
/// any I/O error (file missing, permission denied, etc).
fn readProcFile(path: [*:0]const u8, buf: []u8) ?[]const u8 {
    const open_rc = linux.open(path, .{ .ACCMODE = .RDONLY }, 0);
    if (linux.errno(open_rc) != .SUCCESS) return null;
    const fd: i32 = @intCast(open_rc);
    defer _ = linux.close(fd);

    const read_rc = linux.read(fd, buf.ptr, buf.len);
    if (linux.errno(read_rc) != .SUCCESS) return null;
    const n: usize = @intCast(read_rc);
    return std.mem.trimEnd(u8, buf[0..n], "\n ");
}

pub fn main() !u8 {
    print("\n=== Volt io_uring probe ===\n", .{});

    // 1. Kernel version. /proc/version is most informative.
    var kver_buf: [256]u8 = undefined;
    if (readProcFile("/proc/version", &kver_buf)) |s| {
        print("Kernel: {s}\n", .{s});
    } else {
        print("Kernel: (/proc/version unreadable)\n", .{});
    }

    // 2. sysctl: kernel.io_uring_disabled. Ubuntu 23.04+ ships
    //    this set to 1 by default; that's the silent killer.
    //    Fedora doesn't set it. Either way, surface the value.
    var sysctl_buf: [16]u8 = undefined;
    if (readProcFile("/proc/sys/kernel/io_uring_disabled", &sysctl_buf)) |s| {
        print("kernel.io_uring_disabled = {s} ", .{s});
        if (std.mem.eql(u8, s, "0")) {
            print("(ok — io_uring permitted)\n", .{});
        } else {
            print("(BLOCKED — kernel sysctl forbids io_uring)\n", .{});
        }
    } else {
        print("kernel.io_uring_disabled: sysctl not present (older kernel — should be fine)\n", .{});
    }

    print("\n", .{});

    // 3. Basic setup. If this fails the rest is moot.
    var basic_ok = false;
    var params = std.mem.zeroes(linux.io_uring_params);
    if (linux.IoUring.init_params(8, &params)) |ring_const| {
        var ring = ring_const;
        defer ring.deinit();
        basic_ok = true;
        print("io_uring_setup(8, flags=0): OK\n", .{});
        print("  ring features bitmap: 0x{x}\n", .{params.features});

        const FeatCheck = struct { name: []const u8, bit: u32 };
        const feats = [_]FeatCheck{
            .{ .name = "SINGLE_MMAP    (5.4+)", .bit = linux.IORING_FEAT_SINGLE_MMAP },
            .{ .name = "NODROP         (5.5+)", .bit = linux.IORING_FEAT_NODROP },
            .{ .name = "SUBMIT_STABLE  (5.5+)", .bit = linux.IORING_FEAT_SUBMIT_STABLE },
            .{ .name = "RW_CUR_POS     (5.6+)", .bit = linux.IORING_FEAT_RW_CUR_POS },
            .{ .name = "FAST_POLL      (5.7+)", .bit = linux.IORING_FEAT_FAST_POLL },
            .{ .name = "EXT_ARG        (5.11+)", .bit = linux.IORING_FEAT_EXT_ARG },
            .{ .name = "NATIVE_WORKERS (5.12+)", .bit = linux.IORING_FEAT_NATIVE_WORKERS },
            .{ .name = "CQE_SKIP       (5.17+)", .bit = linux.IORING_FEAT_CQE_SKIP },
        };
        for (feats) |f| {
            const present = (params.features & f.bit) != 0;
            const mark: []const u8 = if (present) "X" else " ";
            print("  [{s}] {s}\n", .{ mark, f.name });
        }
    } else |err| {
        print("io_uring_setup(8, flags=0): FAIL ({s})\n", .{@errorName(err)});
        print("\nNot usable. Likely causes:\n", .{});
        print("  * kernel.io_uring_disabled=1 (see sysctl above)\n", .{});
        print("  * container seccomp profile blocks the syscall\n", .{});
        print("  * kernel too old (need >= 5.1 at the floor; 5.15+ for Volt)\n", .{});
        return 2;
    }

    print("\n", .{});

    // 5. Modern setup flag combos.
    const FlagCheck = struct { name: []const u8, flags: u32 };
    const setups = [_]FlagCheck{
        .{ .name = "SUBMIT_ALL                                  (5.18+)", .flags = linux.IORING_SETUP_SUBMIT_ALL },
        .{ .name = "SINGLE_ISSUER                               (6.0+)", .flags = linux.IORING_SETUP_SINGLE_ISSUER },
        .{ .name = "SINGLE_ISSUER | DEFER_TASKRUN               (6.1+)", .flags = linux.IORING_SETUP_SINGLE_ISSUER | linux.IORING_SETUP_DEFER_TASKRUN },
        .{ .name = "SINGLE_ISSUER | DEFER_TASKRUN | SUBMIT_ALL  (target)", .flags = linux.IORING_SETUP_SINGLE_ISSUER | linux.IORING_SETUP_DEFER_TASKRUN | linux.IORING_SETUP_SUBMIT_ALL },
    };
    var target_ok = false;
    for (setups, 0..) |s, i| {
        if (linux.IoUring.init(8, s.flags)) |r_const| {
            var r = r_const;
            defer r.deinit();
            print("setup flags = {s}: OK\n", .{s.name});
            if (i == setups.len - 1) target_ok = true;
        } else |err| {
            print("setup flags = {s}: FAIL ({s})\n", .{ s.name, @errorName(err) });
        }
    }

    print("\n", .{});

    // 6. Opcode probe.
    var probe_ring = linux.IoUring.init(8, 0) catch |err| {
        print("opcode probe: setup failed ({s})\n", .{@errorName(err)});
        return 2;
    };
    defer probe_ring.deinit();
    const probe = probe_ring.get_probe() catch |err| {
        print("opcode probe: get_probe failed ({s})\n", .{@errorName(err)});
        return 2;
    };
    print("opcode probe: last_op = {s} (ops_len = {d})\n", .{ @tagName(probe.last_op), probe.ops_len });

    // Opcodes Volt's Phase 2 fs path depends on. fdatasync is
    // not a separate opcode — io_uring uses FSYNC with the
    // IORING_FSYNC_DATASYNC flag.
    const ops_we_need = [_]linux.IORING_OP{
        .READ,
        .WRITE,
        .FSYNC,
        .OPENAT,
        .CLOSE,
        .ASYNC_CANCEL,
    };
    var ops_ok = true;
    for (ops_we_need) |op| {
        const supported = probe.is_supported(op);
        const mark: []const u8 = if (supported) "X" else " ";
        print("  [{s}] {s}\n", .{ mark, @tagName(op) });
        if (!supported) ops_ok = false;
    }

    print("\n=== Verdict ===\n", .{});
    if (basic_ok and ops_ok) {
        if (target_ok) {
            print("READY — io_uring usable; target flag stack accepted.\n", .{});
            print("Phase 2 (per-worker io_uring rings on Linux) can proceed.\n", .{});
            return 0;
        } else {
            print("USABLE — io_uring works, all needed ops supported,\n", .{});
            print("but the target flag stack (SINGLE_ISSUER+DEFER_TASKRUN+SUBMIT_ALL)\n", .{});
            print("was rejected. Kernel is likely < 6.1; Phase 2 will need a\n", .{});
            print("fallback to flags=0. Still proceedable.\n", .{});
            return 0;
        }
    } else {
        print("BLOCKED — io_uring not usable in this environment.\n", .{});
        return 2;
    }
}
