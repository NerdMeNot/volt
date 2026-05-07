//! P3.B — Dir + Walker integration tests.

const std = @import("std");
const volt = @import("../lib.zig");

fn unique_dir(buf: []u8, prefix: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}-{d}", .{ prefix, std.posix.system.getpid() });
}

fn rmRf(path: []const u8) void {
    var z_buf: [256]u8 = undefined;
    const path_z = std.fmt.bufPrintZ(&z_buf, "{s}", .{path}) catch return;
    // Best effort — try unlink (file) then rmdir.
    _ = std.posix.system.unlink(path_z.ptr);
    _ = std.posix.system.rmdir(path_z.ptr);
}

// ─────────────────────────────────────────────────────────────────────
// Test 1 — Dir.iterate yields each entry exactly once
// ─────────────────────────────────────────────────────────────────────

const IterCtx = struct {
    found_a: bool = false,
    found_b: bool = false,
    found_dot: bool = false,
    found_dotdot: bool = false,
    count: u32 = 0,
};

fn iterRoot(dir_path: []const u8) !IterCtx {
    rmRf(dir_path);

    // Create the directory + two files.
    var z_buf: [256]u8 = undefined;
    const dir_z = try std.fmt.bufPrintZ(&z_buf, "{s}", .{dir_path});
    _ = std.posix.system.mkdir(dir_z.ptr, 0o755);
    defer _ = std.posix.system.rmdir(dir_z.ptr);

    var path_buf: [256]u8 = undefined;
    const file_a = try std.fmt.bufPrint(&path_buf, "{s}/a.txt", .{dir_path});
    var fa = try volt.fs.File.create(file_a);
    fa.close();
    defer rmRf(file_a);

    var path_buf2: [256]u8 = undefined;
    const file_b = try std.fmt.bufPrint(&path_buf2, "{s}/b.txt", .{dir_path});
    var fb = try volt.fs.File.create(file_b);
    fb.close();
    defer rmRf(file_b);

    var dir = try volt.fs.Dir.open(dir_path);
    defer dir.close();

    var it = try dir.iterate();
    defer it.deinit();

    var ctx = IterCtx{};
    while (it.next()) |entry| {
        ctx.count += 1;
        if (std.mem.eql(u8, entry.name, "a.txt")) ctx.found_a = true;
        if (std.mem.eql(u8, entry.name, "b.txt")) ctx.found_b = true;
        if (std.mem.eql(u8, entry.name, ".")) ctx.found_dot = true;
        if (std.mem.eql(u8, entry.name, "..")) ctx.found_dotdot = true;
    }
    return ctx;
}

test "P3.B: Dir.iterate yields entries, skips '.' and '..'" {
    var path_buf: [128]u8 = undefined;
    const dir_path = try unique_dir(&path_buf, "/tmp/volt-fs-dir");
    const ctx = try volt.run(.{ .allocator = std.testing.allocator }, iterRoot, .{dir_path});
    try std.testing.expectEqual(@as(u32, 2), ctx.count);
    try std.testing.expect(ctx.found_a);
    try std.testing.expect(ctx.found_b);
    try std.testing.expect(!ctx.found_dot);
    try std.testing.expect(!ctx.found_dotdot);
}

// ─────────────────────────────────────────────────────────────────────
// Test 2 — Walker descends into subdirectories
// ─────────────────────────────────────────────────────────────────────

const WalkCtx = struct {
    paths: std.array_list.Managed([]u8),
};

fn walkRoot(root: []const u8) !WalkCtx {
    // Build a tree:
    //   root/
    //     top.txt
    //     sub/
    //       inner.txt
    rmRf(root);
    var path_z_buf: [256]u8 = undefined;
    const root_z = try std.fmt.bufPrintZ(&path_z_buf, "{s}", .{root});
    _ = std.posix.system.mkdir(root_z.ptr, 0o755);

    var sub_buf: [256]u8 = undefined;
    const sub = try std.fmt.bufPrint(&sub_buf, "{s}/sub", .{root});
    var sub_z_buf: [256]u8 = undefined;
    const sub_z = try std.fmt.bufPrintZ(&sub_z_buf, "{s}", .{sub});
    _ = std.posix.system.mkdir(sub_z.ptr, 0o755);

    var top_buf: [256]u8 = undefined;
    const top = try std.fmt.bufPrint(&top_buf, "{s}/top.txt", .{root});
    var ft = try volt.fs.File.create(top);
    ft.close();

    var inner_buf: [256]u8 = undefined;
    const inner = try std.fmt.bufPrint(&inner_buf, "{s}/inner.txt", .{sub});
    var fi = try volt.fs.File.create(inner);
    fi.close();

    // Walk.
    var ctx = WalkCtx{
        .paths = std.array_list.Managed([]u8).init(std.testing.allocator),
    };
    {
        var w = try volt.fs.Walker.open(std.testing.allocator, root, .{});
        defer w.deinit();
        while (try w.next()) |entry| {
            try ctx.paths.append(try std.testing.allocator.dupe(u8, entry.path));
        }
    }

    // Cleanup. Order matters: files first, then dirs from leaf to root.
    rmRf(inner);
    _ = std.posix.system.rmdir(sub_z.ptr);
    rmRf(top);
    _ = std.posix.system.rmdir(root_z.ptr);
    return ctx;
}

test "P3.B: Walker descends into subdirectories yielding all entries" {
    var path_buf: [128]u8 = undefined;
    const root = try unique_dir(&path_buf, "/tmp/volt-fs-walk");
    var ctx = try volt.run(.{ .allocator = std.testing.allocator }, walkRoot, .{root});
    defer {
        for (ctx.paths.items) |p| std.testing.allocator.free(p);
        ctx.paths.deinit();
    }

    // Expect: top.txt, sub, sub/inner.txt — three entries (order
    // depends on filesystem; `top.txt` and `sub` may be either order).
    try std.testing.expectEqual(@as(usize, 3), ctx.paths.items.len);
    var saw_top = false;
    var saw_sub = false;
    var saw_inner = false;
    for (ctx.paths.items) |p| {
        if (std.mem.eql(u8, p, "top.txt")) saw_top = true;
        if (std.mem.eql(u8, p, "sub")) saw_sub = true;
        if (std.mem.eql(u8, p, "sub/inner.txt")) saw_inner = true;
    }
    try std.testing.expect(saw_top);
    try std.testing.expect(saw_sub);
    try std.testing.expect(saw_inner);
}

// ─────────────────────────────────────────────────────────────────────
// Test 3 — Walker.skipSubtree prunes descent
// ─────────────────────────────────────────────────────────────────────

fn walkSkipRoot(root: []const u8) !u32 {
    // Build the same tree as test 2.
    rmRf(root);
    var path_z_buf: [256]u8 = undefined;
    const root_z = try std.fmt.bufPrintZ(&path_z_buf, "{s}", .{root});
    _ = std.posix.system.mkdir(root_z.ptr, 0o755);

    var sub_buf: [256]u8 = undefined;
    const sub = try std.fmt.bufPrint(&sub_buf, "{s}/sub", .{root});
    var sub_z_buf: [256]u8 = undefined;
    const sub_z = try std.fmt.bufPrintZ(&sub_z_buf, "{s}", .{sub});
    _ = std.posix.system.mkdir(sub_z.ptr, 0o755);

    var top_buf: [256]u8 = undefined;
    const top = try std.fmt.bufPrint(&top_buf, "{s}/top.txt", .{root});
    var ft = try volt.fs.File.create(top);
    ft.close();

    var inner_buf: [256]u8 = undefined;
    const inner = try std.fmt.bufPrint(&inner_buf, "{s}/inner.txt", .{sub});
    var fi = try volt.fs.File.create(inner);
    fi.close();

    var count: u32 = 0;
    {
        var w = try volt.fs.Walker.open(std.testing.allocator, root, .{});
        defer w.deinit();
        while (try w.next()) |entry| {
            count += 1;
            if (entry.kind == .directory and std.mem.eql(u8, entry.name, "sub")) {
                w.skipSubtree();
            }
        }
    }

    rmRf(inner);
    _ = std.posix.system.rmdir(sub_z.ptr);
    rmRf(top);
    _ = std.posix.system.rmdir(root_z.ptr);
    return count;
}

test "P3.B: Walker.skipSubtree skips descent into a directory" {
    var path_buf: [128]u8 = undefined;
    const root = try unique_dir(&path_buf, "/tmp/volt-fs-skip");
    const count = try volt.run(.{ .allocator = std.testing.allocator }, walkSkipRoot, .{root});
    // Expect 2 entries: `top.txt` and `sub` (no descent into sub →
    // `sub/inner.txt` not yielded).
    try std.testing.expectEqual(@as(u32, 2), count);
}

// ─────────────────────────────────────────────────────────────────────
// P3.x.6 — Walker.max_depth surfaces error.MaxDepthExceeded
// ─────────────────────────────────────────────────────────────────────

fn maxDepthRoot(root: []const u8) !bool {
    rmRf(root);
    var path_z_buf: [256]u8 = undefined;
    const root_z = try std.fmt.bufPrintZ(&path_z_buf, "{s}", .{root});
    _ = std.posix.system.mkdir(root_z.ptr, 0o755);

    var sub_buf: [256]u8 = undefined;
    const sub = try std.fmt.bufPrint(&sub_buf, "{s}/sub", .{root});
    var sub_z_buf: [256]u8 = undefined;
    const sub_z = try std.fmt.bufPrintZ(&sub_z_buf, "{s}", .{sub});
    _ = std.posix.system.mkdir(sub_z.ptr, 0o755);

    var inner_buf: [256]u8 = undefined;
    const inner = try std.fmt.bufPrint(&inner_buf, "{s}/inner.txt", .{sub});
    var fi = try volt.fs.File.create(inner);
    fi.close();

    // max_depth = 1 means we yield root contents but NEVER descend.
    // The walker should error.MaxDepthExceeded when it tries to
    // descend into "sub".
    var saw_error = false;
    {
        var w = try volt.fs.Walker.open(std.testing.allocator, root, .{ .max_depth = 1 });
        defer w.deinit();
        while (true) {
            const r = w.next() catch |err| {
                if (err == error.MaxDepthExceeded) saw_error = true;
                break;
            };
            if (r == null) break;
        }
    }

    rmRf(inner);
    _ = std.posix.system.rmdir(sub_z.ptr);
    _ = std.posix.system.rmdir(root_z.ptr);
    return saw_error;
}

test "P3.x.6: Walker max_depth = 1 → error.MaxDepthExceeded on descent" {
    var path_buf: [128]u8 = undefined;
    const root = try unique_dir(&path_buf, "/tmp/volt-fs-md");
    const saw = try volt.run(.{ .allocator = std.testing.allocator }, maxDepthRoot, .{root});
    try std.testing.expect(saw);
}
