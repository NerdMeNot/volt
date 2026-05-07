//! P3.C — tree operation integration tests.

const std = @import("std");
const volt = @import("../lib.zig");

fn unique_dir(buf: []u8, prefix: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}-{d}", .{ prefix, std.posix.system.getpid() });
}

// ─────────────────────────────────────────────────────────────────────
// makeDirAll + removeTree round-trip
// ─────────────────────────────────────────────────────────────────────

fn mkRmRoot(root: []const u8) !void {
    // Belt-and-suspenders: nuke any leftover from a prior failed run.
    volt.fs.tree.removeTree(std.testing.allocator, root) catch {};

    // Create root/sub/inner/leaf with intermediate directories.
    var path_buf: [256]u8 = undefined;
    const deep = try std.fmt.bufPrint(&path_buf, "{s}/sub/inner/leaf", .{root});
    try volt.fs.tree.makeDirAll(deep, 0o755);

    // Drop a couple of files so removeTree exercises both file +
    // dir cleanup paths.
    var file_buf: [256]u8 = undefined;
    const top_file = try std.fmt.bufPrint(&file_buf, "{s}/top.txt", .{root});
    {
        var f = try volt.fs.File.create(top_file);
        f.close();
    }
    var inner_buf: [256]u8 = undefined;
    const inner_file = try std.fmt.bufPrint(&inner_buf, "{s}/sub/inner.txt", .{root});
    {
        var f = try volt.fs.File.create(inner_file);
        f.close();
    }

    // Tear it all down.
    try volt.fs.tree.removeTree(std.testing.allocator, root);
}

test "P3.C: makeDirAll + removeTree round-trip" {
    var path_buf: [128]u8 = undefined;
    const root = try unique_dir(&path_buf, "/tmp/volt-fs-tree");
    try volt.run(.{ .allocator = std.testing.allocator }, mkRmRoot, .{root});
}

// ─────────────────────────────────────────────────────────────────────
// rename
// ─────────────────────────────────────────────────────────────────────

fn renameRoot(old: []const u8, new: []const u8) !void {
    volt.fs.tree.removeFile(old) catch {};
    volt.fs.tree.removeFile(new) catch {};

    {
        var f = try volt.fs.File.create(old);
        try f.writeAll("hello");
        f.close();
    }
    try volt.fs.tree.rename(old, new);

    // Old gone, new exists.
    if (volt.fs.File.openRead(old)) |_| {
        return error.OldStillExists;
    } else |_| {}
    var f = try volt.fs.File.openRead(new);
    defer f.close();
    var buf: [16]u8 = undefined;
    const n = try f.read(&buf);
    try std.testing.expectEqualStrings("hello", buf[0..n]);

    try volt.fs.tree.removeFile(new);
}

test "P3.C: rename moves a file" {
    var old_buf: [128]u8 = undefined;
    var new_buf: [128]u8 = undefined;
    const old = try std.fmt.bufPrint(&old_buf, "/tmp/volt-rn-{d}-old.txt", .{std.posix.system.getpid()});
    const new = try std.fmt.bufPrint(&new_buf, "/tmp/volt-rn-{d}-new.txt", .{std.posix.system.getpid()});
    try volt.run(.{ .allocator = std.testing.allocator }, renameRoot, .{ old, new });
}

// ─────────────────────────────────────────────────────────────────────
// copy + symlink + readlinkAlloc
// ─────────────────────────────────────────────────────────────────────

const CopyArgs = struct {
    src: []const u8,
    dst: []const u8,
    link: []const u8,
};

fn copySymlinkRoot(args: CopyArgs) !void {
    volt.fs.tree.removeFile(args.src) catch {};
    volt.fs.tree.removeFile(args.dst) catch {};
    volt.fs.tree.removeFile(args.link) catch {};

    {
        var f = try volt.fs.File.create(args.src);
        try f.writeAll("payload");
        f.close();
    }

    const n = try volt.fs.tree.copy(args.src, args.dst);
    try std.testing.expectEqual(@as(u64, 7), n);

    {
        var f = try volt.fs.File.openRead(args.dst);
        defer f.close();
        var buf: [16]u8 = undefined;
        const r = try f.read(&buf);
        try std.testing.expectEqualStrings("payload", buf[0..r]);
    }

    try volt.fs.tree.symlink(args.dst, args.link);
    const target = try volt.fs.tree.readlinkAlloc(std.testing.allocator, args.link);
    defer std.testing.allocator.free(target);
    try std.testing.expectEqualStrings(args.dst, target);

    try volt.fs.tree.removeFile(args.src);
    try volt.fs.tree.removeFile(args.dst);
    try volt.fs.tree.removeFile(args.link);
}

test "P3.C: copy + symlink + readlinkAlloc" {
    var src_buf: [128]u8 = undefined;
    var dst_buf: [128]u8 = undefined;
    var link_buf: [128]u8 = undefined;
    const pid = std.posix.system.getpid();
    const src = try std.fmt.bufPrint(&src_buf, "/tmp/volt-cp-{d}-src.txt", .{pid});
    const dst = try std.fmt.bufPrint(&dst_buf, "/tmp/volt-cp-{d}-dst.txt", .{pid});
    const link = try std.fmt.bufPrint(&link_buf, "/tmp/volt-cp-{d}-link", .{pid});
    try volt.run(.{ .allocator = std.testing.allocator }, copySymlinkRoot, .{
        CopyArgs{ .src = src, .dst = dst, .link = link },
    });
}

// ─────────────────────────────────────────────────────────────────────
// path utilities
// ─────────────────────────────────────────────────────────────────────

test "P3.C: path.basename / dirname / extension" {
    try std.testing.expectEqualStrings("c.txt", volt.fs.path.basename("a/b/c.txt"));
    try std.testing.expectEqualStrings("a/b", volt.fs.path.dirname("a/b/c.txt").?);
    try std.testing.expectEqualStrings(".txt", volt.fs.path.extension("foo.txt"));
}

test "P3.C: path.join concatenates with /" {
    const joined = try volt.fs.path.join(std.testing.allocator, &.{ "a", "b", "c" });
    defer std.testing.allocator.free(joined);
    try std.testing.expectEqualStrings("a/b/c", joined);
}

// ─────────────────────────────────────────────────────────────────────
// P3.x.1 — stat / exists / access / chmod / canonicalize
// ─────────────────────────────────────────────────────────────────────

const StatFillsArgs = struct {
    path: []const u8,
};

fn statFillsRoot(args: StatFillsArgs) !void {
    volt.fs.tree.removeFile(args.path) catch {};

    // exists() is false when nothing is there.
    try std.testing.expectEqual(false, try volt.fs.tree.exists(args.path));

    // Create a file with some content; stat reports its size.
    {
        var f = try volt.fs.File.create(args.path);
        try f.writeAll("hello stat");
        f.close();
    }
    defer volt.fs.tree.removeFile(args.path) catch {};

    try std.testing.expectEqual(true, try volt.fs.tree.exists(args.path));

    const meta = try volt.fs.tree.stat(args.path);
    try std.testing.expectEqual(@as(u64, 10), meta.size);
    try std.testing.expectEqual(volt.fs.Kind.file, meta.kind);

    // access — readable should pass.
    try volt.fs.tree.access(args.path, .{ .read = true });

    // chmod 0o600 then verify mode.
    try volt.fs.tree.chmod(args.path, 0o600);
    const meta2 = try volt.fs.tree.stat(args.path);
    try std.testing.expectEqual(@as(u32, 0o600), @as(u32, @intCast(meta2.mode)) & 0o777);

    // canonicalize: returns absolute path with symlinks resolved.
    // On macOS, `/tmp` is a symlink to `/private/tmp`, so the result
    // ends with our basename but the prefix may differ. Just verify
    // it's absolute and ends with the file name.
    const canon = try volt.fs.tree.canonicalize(std.testing.allocator, args.path);
    defer std.testing.allocator.free(canon);
    try std.testing.expect(canon.len > 0 and canon[0] == '/');
    try std.testing.expect(std.mem.endsWith(u8, canon, volt.fs.path.basename(args.path)));
}

test "P3.x.1: tree.stat / exists / access / chmod / canonicalize" {
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/volt-fills-{d}.txt", .{std.posix.system.getpid()});
    try volt.run(.{ .allocator = std.testing.allocator }, statFillsRoot, .{StatFillsArgs{ .path = path }});
}

// ─────────────────────────────────────────────────────────────────────
// lstat sees the link, stat follows it
// ─────────────────────────────────────────────────────────────────────

const LStatArgs = struct {
    target: []const u8,
    link: []const u8,
};

fn lstatRoot(args: LStatArgs) !void {
    volt.fs.tree.removeFile(args.target) catch {};
    volt.fs.tree.removeFile(args.link) catch {};

    {
        var f = try volt.fs.File.create(args.target);
        try f.writeAll("target");
        f.close();
    }
    defer volt.fs.tree.removeFile(args.target) catch {};
    try volt.fs.tree.symlink(args.target, args.link);
    defer volt.fs.tree.removeFile(args.link) catch {};

    // lstat sees a symlink.
    const lmeta = try volt.fs.tree.lstat(args.link);
    try std.testing.expectEqual(volt.fs.Kind.symlink, lmeta.kind);

    // stat follows it — sees the target file.
    const meta = try volt.fs.tree.stat(args.link);
    try std.testing.expectEqual(volt.fs.Kind.file, meta.kind);
    try std.testing.expectEqual(@as(u64, 6), meta.size);
}

test "P3.x.1: lstat vs stat on a symlink" {
    var t_buf: [128]u8 = undefined;
    var l_buf: [128]u8 = undefined;
    const pid = std.posix.system.getpid();
    const target = try std.fmt.bufPrint(&t_buf, "/tmp/volt-ls-{d}-target.txt", .{pid});
    const link = try std.fmt.bufPrint(&l_buf, "/tmp/volt-ls-{d}-link", .{pid});
    try volt.run(.{ .allocator = std.testing.allocator }, lstatRoot, .{LStatArgs{ .target = target, .link = link }});
}
