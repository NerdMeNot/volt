//! `volt.io.Seeker` — vtable for byte streams that support random
//! access by offset.
//!
//! Mirrors `lseek` semantics: `seekTo` is absolute, `seekBy` is
//! relative. `getEndPos` returns the byte length (file size) without
//! moving the cursor — useful for `Reader.readAll(stream, alloc)` and
//! similar routines that want to size their buffer up front.

const std = @import("std");
const io_errors = @import("../errors.zig");

pub const SeekError = io_errors.SeekError;

pub const Seeker = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        seekTo: *const fn (ctx: *anyopaque, pos: u64) SeekError!void,
        seekBy: *const fn (ctx: *anyopaque, delta: i64) SeekError!void,
        getPos: *const fn (ctx: *anyopaque) SeekError!u64,
        getEndPos: *const fn (ctx: *anyopaque) SeekError!u64,
    };

    pub fn seekTo(self: Seeker, pos: u64) SeekError!void {
        return self.vtable.seekTo(self.ctx, pos);
    }

    pub fn seekBy(self: Seeker, delta: i64) SeekError!void {
        return self.vtable.seekBy(self.ctx, delta);
    }

    pub fn getPos(self: Seeker) SeekError!u64 {
        return self.vtable.getPos(self.ctx);
    }

    /// Byte length of the underlying resource. Doesn't move the cursor.
    pub fn getEndPos(self: Seeker) SeekError!u64 {
        return self.vtable.getEndPos(self.ctx);
    }
};
