//! Minimal copy of Volt's Future protocol so the spike can run standalone
//! without crossing module boundaries. If the spike clears, the production
//! version uses src/future/{Poll,Waker}.zig directly.

const std = @import("std");

pub const Poll = enum { pending, ready };

pub fn PollResult(comptime T: type) type {
    return union(enum) {
        pending: void,
        ready: T,

        const Self = @This();

        pub fn isReady(self: Self) bool {
            return self == .ready;
        }

        pub fn isPending(self: Self) bool {
            return self == .pending;
        }

        pub fn unwrap(self: Self) T {
            return switch (self) {
                .pending => @panic("unwrap on pending"),
                .ready => |v| v,
            };
        }
    };
}

pub const Waker = struct {
    wake_fn: *const fn (*anyopaque) void = noopWake,
    ctx: *anyopaque = undefined,

    fn noopWake(_: *anyopaque) void {}
};

pub const Context = struct {
    waker: *const Waker,
};

pub var noop_waker: Waker = .{};

/// Compile-time check that a type follows the Future protocol.
pub fn isFuture(comptime T: type) bool {
    // @hasDecl only works on container types
    switch (@typeInfo(T)) {
        .@"struct", .@"enum", .@"union", .@"opaque" => {},
        else => return false,
    }
    if (!@hasDecl(T, "Output") or !@hasDecl(T, "poll")) return false;
    const poll_info = @typeInfo(@TypeOf(T.poll));
    if (poll_info != .@"fn") return false;
    const params = poll_info.@"fn".params;
    if (params.len != 2) return false;
    if (params[0].type != *T) return false;
    if (params[1].type != *Context) return false;
    return true;
}
