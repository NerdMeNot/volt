//! Windows fs cross-compile gate (issue #6).
//!
//! The default-target build of `volt` does NOT catch the
//! `if (is_windows) @compileError(...)` stubs in `src/fs*` because
//! nothing in `lib.zig` *calls* the windows-gated paths, so semantic
//! analysis never reaches them. This program references the full
//! `volt.fs.*` surface so that compiling it for a Windows target
//! instantiates every gated path — any surviving `@compileError`
//! fails the compile.
//!
//! Build-only: `zig build check-windows` compiles (and cross-links)
//! this exe but never runs it. It exists purely so CI and local devs
//! get an honest "does Windows fs compile?" signal. See issue #6.
//!
//! As Windows backends land, the set of firing `@compileError`s
//! shrinks; the gate is green when none remain.

const std = @import("std");
const volt = @import("volt");
const fs = volt.fs;

// Each `touchX` references one slice of the surface. `main` calls them
// all; the exe is never executed, so the calls only need to type-check
// (and thereby force analysis of the gated branches).
pub fn main() !void {
    const alloc = std.heap.page_allocator;
    try touchFile();
    try touchFileMeta();
    try touchPathOps(alloc);
    try touchDir(alloc);
    try touchMmap();
}

fn touchFile() !void {
    var f = try fs.File.openOptions("x", .{ .read = true, .write = true, .create = true });
    defer f.close();
    var buf: [16]u8 = undefined;
    _ = try f.read(&buf);
    _ = try f.write(&buf);
    _ = try f.readAt(&buf, 0);
    _ = try f.writeAt(&buf, 0);
    _ = try f.seek(0, .start);
    try f.sync();
    try f.dataSync();
    try f.setLen(0);

    var f2 = try fs.File.open("y");
    defer f2.close();
    var f3 = try fs.File.create("z");
    defer f3.close();
}

fn touchFileMeta() !void {
    var f = try fs.File.open("x");
    defer f.close();
    _ = try f.metadata();
    try f.setPermissions(.{ .mode = 0o644 });
}

fn touchPathOps(alloc: std.mem.Allocator) !void {
    _ = try fs.stat("x");
    _ = try fs.lstat("x");
    _ = fs.exists("x");
    _ = try fs.tryExists("x");
    _ = fs.access("x", .{ .read = true });
    try fs.chmod("x", .{ .mode = 0o644 });
    try fs.chown("x", 0, 0);
    try fs.setTimes("x", fs.SystemTime.fromSecs(0), fs.SystemTime.fromSecs(0));
    const canon = try fs.canonicalize(alloc, "x");
    alloc.free(canon);
    try fs.rename("a", "b");
    try fs.hardLink("a", "b");
    try fs.symlink("a", "b");
    const link = try fs.readLink(alloc, "a");
    alloc.free(link);
    try fs.unlink("x");
    const tmp = try fs.tempDir(alloc);
    alloc.free(tmp);
    const tf = try fs.tempFile(alloc, "pre");
    alloc.free(tf.path);
}

fn touchDir(alloc: std.mem.Allocator) !void {
    try fs.makeDir("d", .{});
    try fs.removeDir(alloc, "d", .{});
    const ents = try fs.dirEntries(alloc, ".");
    fs.freeDirEntries(alloc, ents);
    var d = try fs.Dir.open(".");
    defer d.close();
    _ = try d.next();
}

fn touchMmap() !void {
    var f = try fs.File.open("x");
    defer f.close();
    var m = try fs.mapFile(f.fd, .{});
    defer m.deinit();
    try m.flush();
    try m.advise(.sequential);
    try m.lock();
    try m.unlock();
    try m.protect(.read_only);

    var anon = try fs.mapAnonymous(.{ .length = 4096 });
    defer anon.deinit();
}
