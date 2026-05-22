//! `volt.fs.mapFile` — count newlines in a file via mmap, zero
//! copy. The kernel pages in regions on demand; we just walk the
//! slice.
//!
//! Run: zig build run-mmap-count

const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}

fn root() !void {
    const path = "/tmp/volt-mmap-count.txt";
    defer volt.fs.unlink(path) catch {};

    // Make a tiny test file with known line count.
    try volt.fs.writeFile(path, "line one\nline two\nline three\nlast line\n");

    var f = try volt.fs.File.open(path);
    defer f.close();

    var mapped = try volt.fs.mapFile(f.fd, .{});
    defer mapped.deinit();

    const bytes = mapped.asBytes();
    var newlines: usize = 0;
    for (bytes) |b| if (b == '\n') {
        newlines += 1;
    };
    std.debug.print("{s}: {d} bytes, {d} newlines\n", .{ path, bytes.len, newlines });
}
