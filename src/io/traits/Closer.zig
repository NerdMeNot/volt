//! `volt.io.Closer` — vtable for resources that release at close.
//!
//! `close` returns void (Posix close-error rarely actionable; matches
//! Go's `io.Closer` shape and Volt's existing `TcpStream.close`).
//! Implementations should be idempotent — calling close on an already-
//! closed resource is a no-op, not a panic.

const std = @import("std");

pub const Closer = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        close: *const fn (ctx: *anyopaque) void,
    };

    pub fn close(self: Closer) void {
        self.vtable.close(self.ctx);
    }
};
