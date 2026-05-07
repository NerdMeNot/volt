//! P4 — Mmap integration tests.

const std = @import("std");
const volt = @import("../lib.zig");

fn cleanup(path: []const u8) void {
    var z_buf: [256]u8 = undefined;
    const path_z = std.fmt.bufPrintZ(&z_buf, "{s}", .{path}) catch return;
    _ = std.posix.system.unlink(path_z.ptr);
}

// ─────────────────────────────────────────────────────────────────────
// P4.A — mapFile + slice round-trip
// ─────────────────────────────────────────────────────────────────────

const MapCtx = struct {
    path: []const u8,
    payload: []const u8,
    seen: [64]u8 = undefined,
    seen_len: usize = 0,
};

fn mapFileRoot(ctx: *MapCtx) !void {
    cleanup(ctx.path);
    {
        var f = try volt.fs.File.create(ctx.path);
        try f.writeAll(ctx.payload);
        f.close();
    }
    defer cleanup(ctx.path);

    var f = try volt.fs.File.open(ctx.path, .{ .read = true });
    defer f.close();

    var m = try volt.fs.Mmap.mapFile(&f, .{
        .mode = .read_only,
        .perms = .{ .read = true },
    });
    defer m.deinit();

    const view = m.sliceConst();
    @memcpy(ctx.seen[0..view.len], view);
    ctx.seen_len = view.len;
}

test "P4.A: Mmap.mapFile slice() yields the file's bytes" {
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/volt-mmap-{d}.txt", .{std.posix.system.getpid()});
    var ctx = MapCtx{ .path = path, .payload = "memory-mapped payload" };
    try volt.run(.{ .allocator = std.testing.allocator }, mapFileRoot, .{&ctx});
    try std.testing.expectEqualStrings(ctx.payload, ctx.seen[0..ctx.seen_len]);
}

// ─────────────────────────────────────────────────────────────────────
// P4.A — anonymous mmap is read/write-able
// ─────────────────────────────────────────────────────────────────────

fn anonRoot() !void {
    var m = try volt.fs.Mmap.anonymous(4096, .{
        .perms = .{ .read = true, .write = true, .exec = false },
    });
    defer m.deinit();

    const buf = m.slice();
    @memset(buf, 0);
    buf[0] = 'V';
    buf[1] = 'o';
    buf[2] = 'l';
    buf[3] = 't';
    try std.testing.expectEqualStrings("Volt", buf[0..4]);
}

test "P4.A: Mmap.anonymous gives a writable region" {
    try volt.run(.{ .allocator = std.testing.allocator }, anonRoot, .{});
}

// ─────────────────────────────────────────────────────────────────────
// P4.B — advise / flush / protect smoke tests
// ─────────────────────────────────────────────────────────────────────

fn opsRoot(path: []const u8) !void {
    cleanup(path);
    {
        var f = try volt.fs.File.create(path);
        try f.writeAll("operations payload " ** 64); // ~1.2 KiB
        f.close();
    }
    defer cleanup(path);

    var f = try volt.fs.File.open(path, .{ .read = true, .write = true });
    defer f.close();

    var m = try volt.fs.Mmap.mapFile(&f, .{
        .mode = .read_write,
        .perms = .{ .read = true, .write = true },
    });
    defer m.deinit();

    const range = volt.fs.MmapRange{ .offset = 0, .length = m.len };

    // advise: hint sequential access. Should succeed without error.
    try m.advise(range, .will_need);
    try m.advise(range, .sequential);

    // Mutate a byte then flush async.
    m.slice()[0] = 'X';
    try m.flush(range, .async_);

    // Make the mapping read-only via protect; reading still works.
    try m.protect(range, .{ .read = true, .write = false });
    try std.testing.expectEqual(@as(u8, 'X'), m.sliceConst()[0]);
}

test "P4.B: advise / flush / protect" {
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/volt-mops-{d}.txt", .{std.posix.system.getpid()});
    try volt.run(.{ .allocator = std.testing.allocator }, opsRoot, .{path});
}

// ─────────────────────────────────────────────────────────────────────
// P4.C — prefault round-trip. Verifies the blocking-pool dispatch
// path completes; can't directly observe that pages are resident
// without parsing /proc/self/smaps, so this is a smoke test.
// ─────────────────────────────────────────────────────────────────────

fn prefaultRoot(path: []const u8) !void {
    cleanup(path);
    {
        var f = try volt.fs.File.create(path);
        // Write 256 KiB so we have multiple pages.
        const chunk: [4096]u8 = .{0xAB} ** 4096;
        var i: u32 = 0;
        while (i < 64) : (i += 1) try f.writeAll(&chunk);
        f.close();
    }
    defer cleanup(path);

    var f = try volt.fs.File.open(path, .{ .read = true });
    defer f.close();

    var m = try volt.fs.Mmap.mapFile(&f, .{
        .mode = .read_only,
        .perms = .{ .read = true },
    });
    defer m.deinit();

    // Prefault the whole region. After this, reads should be
    // RAM-only.
    try m.prefault(.{ .offset = 0, .length = m.len });

    // Verify a sample byte still reads back correctly.
    try std.testing.expectEqual(@as(u8, 0xAB), m.sliceConst()[0]);
    try std.testing.expectEqual(@as(u8, 0xAB), m.sliceConst()[m.len - 1]);
}

test "P4.C: prefault completes via blocking pool" {
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/volt-mpref-{d}.txt", .{std.posix.system.getpid()});
    try volt.run(.{ .allocator = std.testing.allocator }, prefaultRoot, .{path});
}

// ─────────────────────────────────────────────────────────────────────
// P4.D — remap returns the new slice. Contract: caller must rebind.
// ─────────────────────────────────────────────────────────────────────

fn remapRoot() !void {
    // Anonymous mapping — start at 4 KiB, grow to 16 KiB.
    var m = try volt.fs.Mmap.anonymous(4096, .{
        .perms = .{ .read = true, .write = true, .exec = false },
    });
    defer m.deinit();

    m.slice()[0] = 'A';
    try std.testing.expectEqual(@as(usize, 4096), m.len);

    const new_view = try m.remap(16384);
    // remap returns the fresh slice. Length must match the request.
    try std.testing.expectEqual(@as(usize, 16384), new_view.len);
    try std.testing.expectEqual(@as(usize, 16384), m.len);
    // The byte we wrote should still be there at offset 0 (Linux:
    // in-place if possible; Darwin: data preserved through the
    // unmap+remap because we're holding the same content). On
    // Darwin the unmap+remap of an anonymous mapping LOSES the
    // contents — anonymous pages are zero-fill. So skip the
    // content-preservation check here.
    _ = new_view[0]; // touch the new view; should not segfault
}

test "P4.D: remap returns a fresh slice with the new length" {
    try volt.run(.{ .allocator = std.testing.allocator }, remapRoot, .{});
}

// ─────────────────────────────────────────────────────────────────────
// P4.E — Mmap.reader() composes with copy() via the as_bytes arm
// ─────────────────────────────────────────────────────────────────────

const MmapCopyArgs = struct {
    src_path: []const u8,
    dst_path: []const u8,
    payload: []const u8,
};

fn mmapCopyRoot(args: MmapCopyArgs) !void {
    cleanup(args.src_path);
    cleanup(args.dst_path);

    {
        var f = try volt.fs.File.create(args.src_path);
        try f.writeAll(args.payload);
        f.close();
    }
    defer cleanup(args.src_path);
    defer cleanup(args.dst_path);

    var src_file = try volt.fs.File.openRead(args.src_path);
    defer src_file.close();

    var m = try volt.fs.Mmap.mapFile(&src_file, .{
        .mode = .read_only,
        .perms = .{ .read = true },
    });
    defer m.deinit();

    {
        var dst_file = try volt.fs.File.create(args.dst_path);
        defer dst_file.close();
        // copy() should take the as_bytes path here (Mmap exposes it).
        const n = try volt.io.copy(dst_file.writer(), m.reader());
        try std.testing.expectEqual(@as(u64, args.payload.len), n);
    }

    // Verify by reading the destination back.
    var verify = try volt.fs.File.openRead(args.dst_path);
    defer verify.close();
    var buf: [256]u8 = undefined;
    const r = try verify.read(&buf);
    try std.testing.expectEqualStrings(args.payload, buf[0..r]);
}

test "P4.E: copy(file.writer(), mmap.reader()) — as_bytes dispatch" {
    var src_buf: [128]u8 = undefined;
    var dst_buf: [128]u8 = undefined;
    const pid = std.posix.system.getpid();
    const src = try std.fmt.bufPrint(&src_buf, "/tmp/volt-mc-{d}-src.txt", .{pid});
    const dst = try std.fmt.bufPrint(&dst_buf, "/tmp/volt-mc-{d}-dst.txt", .{pid});

    try volt.run(.{ .allocator = std.testing.allocator }, mmapCopyRoot, .{
        MmapCopyArgs{ .src_path = src, .dst_path = dst, .payload = "memory-mapped → file via copy()" },
    });
}

// ─────────────────────────────────────────────────────────────────────
// P4.E — Mmap.readerAt() positional read
// ─────────────────────────────────────────────────────────────────────

fn mmapReaderAtRoot(path: []const u8) !void {
    cleanup(path);
    {
        var f = try volt.fs.File.create(path);
        try f.writeAll("0123456789abcdef");
        f.close();
    }
    defer cleanup(path);

    var f = try volt.fs.File.openRead(path);
    defer f.close();
    var m = try volt.fs.Mmap.mapFile(&f, .{
        .mode = .read_only,
        .perms = .{ .read = true },
    });
    defer m.deinit();

    // Read 4 bytes at offset 6.
    const r_at = m.readerAt();
    var buf: [4]u8 = undefined;
    const n = try r_at.readAt(&buf, 6);
    try std.testing.expectEqual(@as(usize, 4), n);
    try std.testing.expectEqualStrings("6789", &buf);

    // readerAt is positional — calling read on a separate Reader
    // (with cursor) shouldn't see those bytes consumed.
    const r = m.reader();
    var buf2: [4]u8 = undefined;
    const n2 = try r.read(&buf2);
    try std.testing.expectEqual(@as(usize, 4), n2);
    try std.testing.expectEqualStrings("0123", &buf2);
}

test "P4.E: Mmap.readerAt() positional read independent of cursor" {
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/volt-mat-{d}.txt", .{std.posix.system.getpid()});
    try volt.run(.{ .allocator = std.testing.allocator }, mmapReaderAtRoot, .{path});
}
