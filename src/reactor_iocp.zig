//! Windows IOCP reactor — **stub**. Real implementation lands in L3
//! per `~/.claude/plans/create-a-proper-plan-giggly-cherny.md`.
//!
//! IOCP is completion-based; the L3 design polyfills it as
//! readiness (Go's netpoll pattern: zero-byte `WSARecv` / `WSASend`
//! to learn "socket is readable/writable") so `net.zig` stays
//! uniform across all platforms.
//!
//! This file exists so `src/reactor.zig`'s comptime switch type-
//! checks on every target. Building for Windows today produces a
//! library whose Reactor methods panic on use — the cross-compile
//! sanity check (`zig build-lib -target x86_64-windows-gnu -lc
//! -fno-emit-bin`) passes; running on Windows does not.

const std = @import("std");
const coroutine = @import("coroutine.zig");

const STUB_MSG = "Volt: Windows IOCP reactor not yet implemented (L3). " ++
    "Build cross-compiles cleanly; running requires the L3 impl.";

pub const Reactor = struct {
    pub fn init() !Reactor {
        @panic(STUB_MSG);
    }

    pub fn deinit(self: *Reactor) void {
        _ = self;
    }

    pub fn waitReadable(self: *Reactor, fd: i32) void {
        _ = .{ self, fd };
        @panic(STUB_MSG);
    }

    pub fn waitWritable(self: *Reactor, fd: i32) void {
        _ = .{ self, fd };
        @panic(STUB_MSG);
    }

    pub fn waitTimer(self: *Reactor, ns: u64) void {
        _ = .{ self, ns };
        @panic(STUB_MSG);
    }

    pub fn pendingCount(self: *const Reactor) u32 {
        _ = self;
        return 0;
    }

    pub fn poll(self: *Reactor, blocking: bool) usize {
        _ = .{ self, blocking };
        @panic(STUB_MSG);
    }
};

pub fn setNonblock(fd: i32) !void {
    _ = fd;
    @panic(STUB_MSG);
}

pub fn readAsync(rx: *Reactor, fd: i32, buf: []u8) !usize {
    _ = .{ rx, fd, buf };
    @panic(STUB_MSG);
}

pub fn writeAsync(rx: *Reactor, fd: i32, buf: []const u8) !usize {
    _ = .{ rx, fd, buf };
    @panic(STUB_MSG);
}

pub fn readFull(rx: *Reactor, fd: i32, buf: []u8) !usize {
    _ = .{ rx, fd, buf };
    @panic(STUB_MSG);
}

pub fn writeAll(rx: *Reactor, fd: i32, buf: []const u8) !void {
    _ = .{ rx, fd, buf };
    @panic(STUB_MSG);
}

comptime {
    _ = coroutine;
}
