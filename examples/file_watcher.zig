//! `volt.fs.Watcher` — watch a directory and print events as they
//! arrive. The watcher polls every 100ms; create / touch / delete
//! files under /tmp/volt-watch-demo to see events.
//!
//! Run: zig build run-file-watcher (Ctrl-C to stop)

const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try rt.runWithSignals(root, .{});
}

fn root(c: *volt.Cancel) !void {
    const allocator = std.heap.smp_allocator;
    const dir = "/tmp/volt-watch-demo";
    volt.fs.makeDir(dir, .{ .recursive = true }) catch |e| switch (e) {
        error.AlreadyExists => {},
        else => return e,
    };

    var w = volt.fs.Watcher.init(allocator, .{
        .poll_interval = volt.Duration.fromMillis(100),
    });
    defer w.deinit();
    try w.watch(dir, .{});

    std.debug.print("watching {s} — touch / rm files under it to see events\n", .{dir});
    std.debug.print("Ctrl-C to stop.\n", .{});

    while (true) {
        const event = w.nextCancel(c) catch |e| switch (e) {
            error.Cancelled => {
                std.debug.print("\nstopped.\n", .{});
                return;
            },
            else => return e,
        };
        defer allocator.free(event.path);
        const kind: []const u8 = switch (event.kind) {
            .created => "CREATED",
            .modified => "MODIFIED",
            .removed => "REMOVED",
        };
        std.debug.print("  {s:9} {s}\n", .{ kind, event.path });
    }
}
