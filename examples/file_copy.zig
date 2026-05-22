//! `volt.fs.copyFile` — clone a file byte-for-byte under a Volt
//! runtime. The copy goes through the spawnBlocking pool so the
//! coroutine doesn't pin a worker on slow I/O.
//!
//! Run: zig build run-file-copy

const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}

fn root() !void {
    // Build a small source file in /tmp.
    const src_path = "/tmp/volt-file-copy-src.txt";
    const dst_path = "/tmp/volt-file-copy-dst.txt";
    defer volt.fs.unlink(src_path) catch {};
    defer volt.fs.unlink(dst_path) catch {};

    try volt.fs.writeFile(src_path, "Hello from Volt's volt.fs.copyFile!\n");
    try volt.fs.copyFile(src_path, dst_path);

    const src_meta = try volt.fs.stat(src_path);
    const dst_meta = try volt.fs.stat(dst_path);
    std.debug.print("src {d} bytes mode 0o{o}\n", .{ src_meta.size(), src_meta.permissions().getMode() });
    std.debug.print("dst {d} bytes mode 0o{o}\n", .{ dst_meta.size(), dst_meta.permissions().getMode() });

    if (src_meta.size() != dst_meta.size()) return error.SizeMismatch;
    std.debug.print("OK\n", .{});
}
