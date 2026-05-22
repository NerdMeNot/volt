//! DNS lookup — resolve a hostname and print all returned
//! addresses.
//!
//! Demonstrates: volt.net.lookupHost via spawnBlocking. Calling
//! coro parks on the OS-thread pool while libc getaddrinfo runs.
//!
//! Run: zig build run-dns-lookup

const std = @import("std");
const volt = @import("volt");

pub fn main() !void {
    var rt = try volt.Runtime.init(.{ .allocator = std.heap.smp_allocator });
    defer rt.deinit();
    try (try rt.run(resolve, .{}));
}

fn resolve() !void {
    const allocator = std.heap.smp_allocator;
    const hosts = [_][]const u8{ "localhost", "127.0.0.1", "::1" };
    for (hosts) |h| {
        const addrs = volt.net.lookupHost(allocator, h, 443) catch |err| {
            std.debug.print("lookup {s}: {s}\n", .{ h, @errorName(err) });
            continue;
        };
        defer allocator.free(addrs);
        std.debug.print("lookup {s}: {d} address(es)\n", .{ h, addrs.len });
        for (addrs, 0..) |addr, i| {
            std.debug.print("  [{d}] {f}\n", .{ i, addr });
        }
    }
}
