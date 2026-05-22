//! `volt.fs.Dir.walk` — recursive tree walk with a visitor that
//! prints each entry's depth + kind.
//!
//! Run: zig build run-dir-walk

const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(root, .{}));
}

fn root() !void {
    const allocator = std.heap.smp_allocator;
    // Walk the project's `src` directory if we're in the repo,
    // otherwise fall back to /tmp.
    const candidates = [_][]const u8{ "src", "/tmp" };
    var target: []const u8 = "/tmp";
    for (candidates) |c| if (volt.fs.exists(c)) {
        target = c;
        break;
    };

    std.debug.print("walking: {s}\n", .{target});
    var d = try volt.fs.Dir.open(target);
    defer d.close();

    var visitor = PrintVisitor{};
    try d.walk(target, allocator, &visitor, .{ .max_depth = 3 });
    std.debug.print("\n{d} files / {d} dirs\n", .{ visitor.files, visitor.dirs });
}

const PrintVisitor = struct {
    files: u32 = 0,
    dirs: u32 = 0,

    pub fn visit(self: *PrintVisitor, entry: volt.fs.DirEntry, depth: u32) volt.fs.WalkAction {
        const kind: u8 = switch (entry.kind) {
            .file => 'f',
            .directory => 'd',
            .sym_link => 'l',
            else => '?',
        };
        var i: u32 = 0;
        while (i < depth) : (i += 1) std.debug.print("  ", .{});
        std.debug.print("{c} {s}\n", .{ kind, entry.name });
        switch (entry.kind) {
            .file => self.files += 1,
            .directory => self.dirs += 1,
            else => {},
        }
        return .@"continue";
    }
};
