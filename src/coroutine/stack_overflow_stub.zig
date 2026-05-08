//! No-op stack-overflow handler — for OS targets without a dedicated
//! POSIX or Windows arm. Compiles cleanly so cross-compile targets
//! stay healthy. A stack overflow on these targets crashes the
//! process; recovery isn't supported.

pub const supported = false;

pub const SigJmpBuf = extern struct { bytes: [16]u8 = [_]u8{0} ** 16 };

pub const DispatchCheckpoint = struct { jmp_buf: SigJmpBuf = .{} };

pub fn installPerThread() !void {}

pub fn beginDispatch(_: *DispatchCheckpoint) void {}

pub fn endDispatch() void {}

pub fn currentCheckpoint() ?*DispatchCheckpoint {
    return null;
}

pub fn longjmpDispatch(_: *DispatchCheckpoint) noreturn {
    @panic("stack overflow recovery not supported on this OS");
}

pub inline fn setjmpDispatch(_: *DispatchCheckpoint) bool {
    return false;
}
