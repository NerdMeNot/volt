//! Raw syscall wrappers — replaces the medium-level std.posix.* functions
//! that Zig 0.16 removed.
//!
//! Zig 0.16 directed code to either std.Io (high-level, requires Io handle)
//! or std.posix.system (raw syscalls, manual errno). A runtime's backends
//! belong on the latter; this module puts the removed wrappers back under a
//! Volt-owned namespace.
//!
//! Usage:
//!   const posix = std.posix;                 // types & constants
//!   const syscall = @import(".../syscall.zig"); // removed wrappers
//!
//!   const fd = try syscall.socket(posix.AF.INET, posix.SOCK.STREAM, 0);

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

/// Re-export the raw syscall layer so callers can reach down when needed.
pub const system = std.posix.system;

/// `posix.errno(rc)` shim that accepts a signed `rc` (negative-on-error
/// convention) on both Darwin and Linux. On Linux `posix.errno` takes
/// `usize`; we bit-cast. On Darwin/BSD it accepts the signed value
/// directly (libc-typed return).
inline fn errnoOf(signed: isize) std.posix.E {
    return switch (builtin.os.tag) {
        .linux => posix.errno(@as(usize, @bitCast(signed))),
        else => posix.errno(signed),
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Sockets
// ─────────────────────────────────────────────────────────────────────────────

pub const SocketError = error{
    AccessDenied,
    AddressFamilyNotSupported,
    ProtocolFamilyNotAvailable,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    ProtocolNotSupported,
    SocketTypeNotSupported,
} || std.posix.UnexpectedError;

pub fn socket(domain: u32, sock_type: u32, protocol: u32) SocketError!posix.socket_t {
    // On Darwin (and several BSDs), CLOEXEC/NONBLOCK aren't real socket-type
    // bits — they're Zig shims that need post-create fcntl. Linux's accept4-
    // style flags ARE real, so we keep them passthrough on Linux.
    const has_native_flags = builtin.os.tag == .linux;
    const flag_mask: u32 = posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK;
    const native_type = if (has_native_flags) sock_type else sock_type & ~flag_mask;
    const want_cloexec = !has_native_flags and (sock_type & posix.SOCK.CLOEXEC) != 0;
    const want_nonblock = !has_native_flags and (sock_type & posix.SOCK.NONBLOCK) != 0;

    const rc = system.socket(domain, native_type, protocol);
    switch (posix.errno(rc)) {
        .SUCCESS => {},
        .ACCES => return error.AccessDenied,
        .AFNOSUPPORT => return error.AddressFamilyNotSupported,
        .INVAL => return error.ProtocolFamilyNotAvailable,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOBUFS, .NOMEM => return error.SystemResources,
        .PROTONOSUPPORT => return error.ProtocolNotSupported,
        .PROTOTYPE => return error.SocketTypeNotSupported,
        else => |err| return posix.unexpectedErrno(err),
    }
    const fd: posix.socket_t = @intCast(rc);
    errdefer close(fd);

    if (want_cloexec) {
        const F = posix.F;
        _ = fcntl(fd, F.SETFD, @intCast(@as(c_int, 1))) catch {}; // FD_CLOEXEC
    }
    if (want_nonblock) {
        const F = posix.F;
        const cur = fcntl(fd, F.GETFL, 0) catch 0;
        const nb: u32 = @bitCast(posix.O{ .NONBLOCK = true });
        _ = fcntl(fd, F.SETFL, cur | nb) catch {};
    }
    return fd;
}

// ─────────────────────────────────────────────────────────────────────────────
// close
// ─────────────────────────────────────────────────────────────────────────────

/// Close a file descriptor. Logs (in debug) if close fails unexpectedly.
/// close() is not retried on EINTR — per POSIX, retrying may close someone else's fd.
pub fn close(fd: posix.fd_t) void {
    switch (posix.errno(system.close(fd))) {
        .SUCCESS, .INTR, .IO => return,
        .BADF => if (std.debug.runtime_safety) @panic("close on invalid fd"),
        else => return,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipes
// ─────────────────────────────────────────────────────────────────────────────

pub const PipeError = error{
    SystemFdQuotaExceeded,
    ProcessFdQuotaExceeded,
} || std.posix.UnexpectedError;

pub fn pipe() PipeError![2]posix.fd_t {
    var fds: [2]posix.fd_t = undefined;
    switch (posix.errno(system.pipe(&fds))) {
        .SUCCESS => return fds,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        else => |err| return posix.unexpectedErrno(err),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// fcntl
// ─────────────────────────────────────────────────────────────────────────────

pub const FcntlError = error{
    AccessDenied,
    FileBusy,
    ProcessFdQuotaExceeded,
    Locked,
    DeadLock,
    LockedRegionLimitExceeded,
} || std.posix.UnexpectedError;

pub fn fcntl(fd: posix.fd_t, cmd: i32, arg: usize) FcntlError!usize {
    while (true) {
        // With libc linked, `system.fcntl` returns `c_int` on both
        // Linux and Darwin (libc-typed return). Promote to isize for
        // the negative-error / success-bitcast unification.
        const raw = system.fcntl(fd, cmd, arg);
        const signed: isize = @as(isize, raw);
        const rc: usize = @bitCast(signed);
        switch (errnoOf(signed)) {
            .SUCCESS => return rc,
            .INTR => continue,
            .ACCES => return error.Locked,
            .BADF => unreachable,
            .BUSY => return error.FileBusy,
            .INVAL => unreachable,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NOTDIR => unreachable,
            .PERM => return error.AccessDenied,
            .SRCH => unreachable,
            .DEADLK => return error.DeadLock,
            .NOLCK => return error.LockedRegionLimitExceeded,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// write / writev
// ─────────────────────────────────────────────────────────────────────────────

pub const WriteError = error{
    DiskQuota,
    FileTooBig,
    InputOutput,
    NoSpaceLeft,
    DeviceBusy,
    InvalidArgument,
    AccessDenied,
    BrokenPipe,
    WouldBlock,
    ConnectionResetByPeer,
    OperationAborted,
    NotOpenForWriting,
    LockViolation,
    ProcessNotFound,
} || std.posix.UnexpectedError;

pub fn write(fd: posix.fd_t, bytes: []const u8) WriteError!usize {
    const max = if (builtin.os.tag == .linux) 0x7ffff000 else std.math.maxInt(isize);
    const count = @min(bytes.len, max);
    while (true) {
        const rc = system.write(fd, bytes.ptr, count);
        const signed: isize = @bitCast(rc);
        if (signed >= 0) return @intCast(signed);
        switch (errnoOf(signed)) {
            .INTR => continue,
            .INVAL => return error.InvalidArgument,
            .FAULT => unreachable,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.NotOpenForWriting,
            .DESTADDRREQ => unreachable,
            .DQUOT => return error.DiskQuota,
            .FBIG => return error.FileTooBig,
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .PERM => return error.AccessDenied,
            .PIPE => return error.BrokenPipe,
            .CONNRESET => return error.ConnectionResetByPeer,
            .BUSY => return error.DeviceBusy,
            .SRCH => return error.ProcessNotFound,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub fn writev(fd: posix.fd_t, iov: []const posix.iovec_const) WriteError!usize {
    const iov_count = std.math.cast(u31, iov.len) orelse std.math.maxInt(u31);
    while (true) {
        const rc = system.writev(fd, iov.ptr, iov_count);
        const signed: isize = @bitCast(rc);
        if (signed >= 0) return @intCast(signed);
        switch (errnoOf(signed)) {
            .INTR => continue,
            .INVAL => return error.InvalidArgument,
            .FAULT => unreachable,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.NotOpenForWriting,
            .DESTADDRREQ => unreachable,
            .DQUOT => return error.DiskQuota,
            .FBIG => return error.FileTooBig,
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .PERM => return error.AccessDenied,
            .PIPE => return error.BrokenPipe,
            .CONNRESET => return error.ConnectionResetByPeer,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// read
// ─────────────────────────────────────────────────────────────────────────────

pub fn read(fd: posix.fd_t, bytes: []u8) ReadError!usize {
    const max = if (builtin.os.tag == .linux) 0x7ffff000 else std.math.maxInt(isize);
    const count = @min(bytes.len, max);
    while (true) {
        const rc = system.read(fd, bytes.ptr, count);
        const signed: isize = @bitCast(rc);
        if (signed >= 0) return @intCast(signed);
        switch (errnoOf(signed)) {
            .INTR => continue,
            .INVAL => unreachable,
            .FAULT => unreachable,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.NotOpenForReading,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .NOBUFS, .NOMEM => unreachable,
            .CONNRESET => return error.ConnectionResetByPeer,
            .TIMEDOUT => return error.ConnectionTimedOut,
            .NOTCONN => return error.SocketNotConnected,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// readv
// ─────────────────────────────────────────────────────────────────────────────

pub const ReadError = error{
    InputOutput,
    IsDir,
    NotOpenForReading,
    WouldBlock,
    ConnectionResetByPeer,
    ConnectionTimedOut,
    SocketNotConnected,
    AccessDenied,
    BrokenPipe,
} || std.posix.UnexpectedError;

pub fn readv(fd: posix.fd_t, iov: []const posix.iovec) ReadError!usize {
    const iov_count = std.math.cast(u31, iov.len) orelse std.math.maxInt(u31);
    while (true) {
        const rc = system.readv(fd, iov.ptr, iov_count);
        const signed: isize = @bitCast(rc);
        if (signed >= 0) return @intCast(signed);
        switch (errnoOf(signed)) {
            .INTR => continue,
            .INVAL => unreachable,
            .FAULT => unreachable,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.NotOpenForReading,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .NOBUFS, .NOMEM => unreachable, // ReadError doesn't model SystemResources
            .CONNRESET => return error.ConnectionResetByPeer,
            .TIMEDOUT => return error.ConnectionTimedOut,
            .NOTCONN => return error.SocketNotConnected,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// bind / listen / accept / connect / shutdown
// ─────────────────────────────────────────────────────────────────────────────

pub const BindError = error{
    AccessDenied,
    AddressInUse,
    AddressNotAvailable,
    AlreadyBound,
    AddressFamilyNotSupported,
    SymLinkLoop,
    NameTooLong,
    FileNotFound,
    SystemResources,
    NotDir,
    ReadOnlyFileSystem,
    NetworkSubsystemFailed,
    FileDescriptorNotASocket,
} || std.posix.UnexpectedError;

pub fn bind(fd: posix.socket_t, addr: *const posix.sockaddr, len: posix.socklen_t) BindError!void {
    switch (posix.errno(system.bind(fd, addr, len))) {
        .SUCCESS => return,
        .ACCES, .PERM => return error.AccessDenied,
        .ADDRINUSE => return error.AddressInUse,
        .BADF => unreachable,
        .INVAL => return error.AlreadyBound,
        .NOTSOCK => return error.FileDescriptorNotASocket,
        .ADDRNOTAVAIL => return error.AddressNotAvailable,
        .FAULT => unreachable,
        .LOOP => return error.SymLinkLoop,
        .NAMETOOLONG => return error.NameTooLong,
        .NOENT => return error.FileNotFound,
        .NOMEM => return error.SystemResources,
        .NOTDIR => return error.NotDir,
        .ROFS => return error.ReadOnlyFileSystem,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub const ListenError = error{
    AddressInUse,
    FileDescriptorNotASocket,
    OperationNotSupported,
    NetworkSubsystemFailed,
    SystemResources,
} || std.posix.UnexpectedError;

pub fn listen(fd: posix.socket_t, backlog: u31) ListenError!void {
    switch (posix.errno(system.listen(fd, backlog))) {
        .SUCCESS => return,
        .ADDRINUSE => return error.AddressInUse,
        .BADF => unreachable,
        .NOTSOCK => return error.FileDescriptorNotASocket,
        .OPNOTSUPP => return error.OperationNotSupported,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub const AcceptError = error{
    ConnectionAborted,
    FileDescriptorNotASocket,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    SocketNotListening,
    OperationNotSupported,
    WouldBlock,
    ProtocolFailure,
    BlockedByFirewall,
    ConnectionResetByPeer,
} || std.posix.UnexpectedError;

pub fn accept(
    fd: posix.socket_t,
    addr: ?*posix.sockaddr,
    addr_size: ?*posix.socklen_t,
    flags: u32,
) AcceptError!posix.socket_t {
    // accept4 is Linux-only. Darwin/BSD has plain accept and emulates
    // SOCK_CLOEXEC / SOCK_NONBLOCK via post-accept fcntl.
    const has_accept4 = comptime switch (builtin.os.tag) {
        .linux => true,
        else => false,
    };
    while (true) {
        const rc = if (comptime has_accept4)
            system.accept4(fd, addr, addr_size, flags)
        else
            system.accept(fd, addr, addr_size);
        // With libc linked, both accept (Darwin) and accept4 (Linux)
        // return `c_int`. Promote to isize for unified handling.
        const signed: isize = @as(isize, rc);
        if (comptime !has_accept4) {
            if (signed >= 0) {
                const accepted: posix.socket_t = @intCast(signed);
                const flag_mask: u32 = posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK;
                if ((flags & flag_mask) != 0) {
                    if ((flags & posix.SOCK.CLOEXEC) != 0) {
                        _ = fcntl(accepted, posix.F.SETFD, 1) catch {};
                    }
                    if ((flags & posix.SOCK.NONBLOCK) != 0) {
                        const cur = fcntl(accepted, posix.F.GETFL, 0) catch 0;
                        const nb: u32 = @bitCast(posix.O{ .NONBLOCK = true });
                        _ = fcntl(accepted, posix.F.SETFL, cur | nb) catch {};
                    }
                }
                return accepted;
            }
        }
        if (signed >= 0) return @intCast(signed);
        switch (errnoOf(signed)) {
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .BADF => unreachable,
            .CONNABORTED => return error.ConnectionAborted,
            .FAULT => unreachable,
            .INVAL => return error.SocketNotListening,
            .NOTSOCK => return error.FileDescriptorNotASocket,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NOBUFS, .NOMEM => return error.SystemResources,
            .OPNOTSUPP => return error.OperationNotSupported,
            .PROTO => return error.ProtocolFailure,
            .PERM => return error.BlockedByFirewall,
            .CONNRESET => return error.ConnectionResetByPeer,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub const ConnectError = error{
    AccessDenied,
    AddressInUse,
    AddressNotAvailable,
    AddressFamilyNotSupported,
    SystemResources,
    ConnectionPending,
    ConnectionRefused,
    ConnectionResetByPeer,
    NetworkUnreachable,
    FileNotFound,
    FileDescriptorNotASocket,
    ConnectionTimedOut,
    WouldBlock,
} || std.posix.UnexpectedError;

pub fn connect(fd: posix.socket_t, addr: *const posix.sockaddr, len: posix.socklen_t) ConnectError!void {
    while (true) {
        switch (posix.errno(system.connect(fd, addr, len))) {
            .SUCCESS => return,
            .ACCES, .PERM => return error.AccessDenied,
            .ADDRINUSE => return error.AddressInUse,
            .ADDRNOTAVAIL => return error.AddressNotAvailable,
            .AFNOSUPPORT => return error.AddressFamilyNotSupported,
            .AGAIN, .INPROGRESS => return error.WouldBlock,
            .ALREADY => return error.ConnectionPending,
            .BADF => unreachable,
            .CONNREFUSED => return error.ConnectionRefused,
            .CONNRESET => return error.ConnectionResetByPeer,
            .FAULT => unreachable,
            .INTR => continue,
            .ISCONN => unreachable,
            .HOSTUNREACH, .NETUNREACH => return error.NetworkUnreachable,
            .NOENT => return error.FileNotFound,
            .NOTSOCK => return error.FileDescriptorNotASocket,
            .PROTOTYPE => unreachable,
            .TIMEDOUT => return error.ConnectionTimedOut,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub const ShutdownError = error{
    ConnectionAborted,
    ConnectionResetByPeer,
    BlockingOperationInProgress,
    FileDescriptorNotASocket,
    SocketNotConnected,
    SystemResources,
} || std.posix.UnexpectedError;

pub const ShutdownHow = enum(u32) { recv = 0, send = 1, both = 2 };

pub fn shutdown(fd: posix.socket_t, how: ShutdownHow) ShutdownError!void {
    const how_int: c_int = @intCast(@intFromEnum(how));
    switch (posix.errno(system.shutdown(fd, how_int))) {
        .SUCCESS => return,
        .BADF => unreachable,
        .INVAL => unreachable,
        .NOTCONN => return error.SocketNotConnected,
        .NOTSOCK => return error.FileDescriptorNotASocket,
        .NOBUFS => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// send / recv / sendto / recvfrom
// ─────────────────────────────────────────────────────────────────────────────

pub const SendError = error{
    AccessDenied,
    WouldBlock,
    FastOpenAlreadyInProgress,
    ConnectionResetByPeer,
    ConnectionRefused,
    MessageTooBig,
    SystemResources,
    BrokenPipe,
    NetworkUnreachable,
    NetworkSubsystemFailed,
    NotConnected,
    AddressFamilyNotSupported,
} || std.posix.UnexpectedError;

pub fn send(fd: posix.socket_t, buf: []const u8, flags: u32) SendError!usize {
    return sendto(fd, buf, flags, null, 0);
}

pub fn sendto(
    fd: posix.socket_t,
    buf: []const u8,
    flags: u32,
    dest_addr: ?*const posix.sockaddr,
    addrlen: posix.socklen_t,
) SendError!usize {
    while (true) {
        const rc = system.sendto(fd, buf.ptr, buf.len, flags, dest_addr, addrlen);
        const signed: isize = @bitCast(rc);
        if (signed >= 0) return @intCast(signed);
        switch (errnoOf(signed)) {
            .ACCES => return error.AccessDenied,
            .AGAIN => return error.WouldBlock,
            .ALREADY => return error.FastOpenAlreadyInProgress,
            .BADF => unreachable,
            .CONNRESET => return error.ConnectionResetByPeer,
            .DESTADDRREQ => unreachable,
            .FAULT => unreachable,
            .INTR => continue,
            .INVAL => unreachable,
            .ISCONN => unreachable,
            .MSGSIZE => return error.MessageTooBig,
            .NOBUFS, .NOMEM => return error.SystemResources,
            .NOTCONN => return error.NotConnected,
            .NOTSOCK => unreachable,
            .OPNOTSUPP => unreachable,
            .PIPE => return error.BrokenPipe,
            .AFNOSUPPORT => return error.AddressFamilyNotSupported,
            .CONNREFUSED => return error.ConnectionRefused,
            .LOOP => unreachable,
            .NAMETOOLONG => unreachable,
            .NOENT => return error.NetworkUnreachable,
            .NOTDIR => unreachable,
            .HOSTUNREACH, .NETUNREACH => return error.NetworkUnreachable,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub const RecvFromError = error{
    WouldBlock,
    SystemResources,
    SocketNotConnected,
    ConnectionResetByPeer,
    ConnectionRefused,
    ConnectionTimedOut,
    MessageTooBig,
} || std.posix.UnexpectedError;

pub fn recv(fd: posix.socket_t, buf: []u8, flags: u32) RecvFromError!usize {
    return recvfrom(fd, buf, flags, null, null);
}

pub fn recvfrom(
    fd: posix.socket_t,
    buf: []u8,
    flags: u32,
    src_addr: ?*posix.sockaddr,
    addrlen: ?*posix.socklen_t,
) RecvFromError!usize {
    while (true) {
        const rc = system.recvfrom(fd, buf.ptr, buf.len, flags, src_addr, addrlen);
        const signed: isize = @bitCast(rc);
        if (signed >= 0) return @intCast(signed);
        switch (errnoOf(signed)) {
            .AGAIN => return error.WouldBlock,
            .BADF => unreachable,
            .CONNREFUSED => return error.ConnectionRefused,
            .CONNRESET => return error.ConnectionResetByPeer,
            .FAULT => unreachable,
            .INTR => continue,
            .INVAL => unreachable,
            .NOMEM, .NOBUFS => return error.SystemResources,
            .NOTCONN => return error.SocketNotConnected,
            .NOTSOCK => unreachable,
            .TIMEDOUT => return error.ConnectionTimedOut,
            .MSGSIZE => return error.MessageTooBig,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// getsockname
// ─────────────────────────────────────────────────────────────────────────────

pub const GetSockNameError = error{
    SystemResources,
    FileDescriptorNotASocket,
    SocketNotBound,
} || std.posix.UnexpectedError;

pub fn getsockname(fd: posix.socket_t, addr: *posix.sockaddr, addrlen: *posix.socklen_t) GetSockNameError!void {
    switch (posix.errno(system.getsockname(fd, addr, addrlen))) {
        .SUCCESS => return,
        .BADF => unreachable,
        .FAULT => unreachable,
        .INVAL => unreachable,
        .NOTSOCK => return error.FileDescriptorNotASocket,
        .NOBUFS => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// getsockopt
// ─────────────────────────────────────────────────────────────────────────────

pub const GetSockOptError = error{
    NoDevice,
    SystemResources,
    InvalidProtocolOption,
    TimeoutTooBig,
    AccessDenied,
} || std.posix.UnexpectedError;

pub fn getsockopt(
    fd: posix.socket_t,
    level: i32,
    optname: u32,
    opt: []u8,
) GetSockOptError!void {
    var len: posix.socklen_t = @intCast(opt.len);
    switch (posix.errno(system.getsockopt(fd, level, @intCast(optname), opt.ptr, &len))) {
        .SUCCESS => return,
        .BADF => unreachable,
        .FAULT => unreachable,
        .INVAL => return error.InvalidProtocolOption,
        .NOMEM => return error.SystemResources,
        .NOBUFS => return error.SystemResources,
        .NOPROTOOPT => return error.InvalidProtocolOption,
        .NOTSOCK => unreachable,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub const SetSockOptError = error{
    SystemResources,
    InvalidProtocolOption,
    AccessDenied,
} || std.posix.UnexpectedError;

pub fn setsockopt(
    fd: posix.socket_t,
    level: i32,
    optname: u32,
    opt: []const u8,
) SetSockOptError!void {
    const len: posix.socklen_t = @intCast(opt.len);
    switch (posix.errno(system.setsockopt(fd, level, @intCast(optname), opt.ptr, len))) {
        .SUCCESS => return,
        .BADF => unreachable,
        .FAULT => unreachable,
        .INVAL => return error.InvalidProtocolOption,
        .NOMEM => return error.SystemResources,
        .NOBUFS => return error.SystemResources,
        .NOPROTOOPT => return error.InvalidProtocolOption,
        .NOTSOCK => unreachable,
        .ACCES, .PERM => return error.AccessDenied,
        else => |err| return posix.unexpectedErrno(err),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// unlinkatZ / fchmodat
// ─────────────────────────────────────────────────────────────────────────────

pub const UnlinkatError = error{
    DirNotEmpty,
    AccessDenied,
    FileBusy,
    FileNotFound,
    InvalidUtf8,
    NameTooLong,
    SystemResources,
    ReadOnlyFileSystem,
    NotDir,
    IsDir,
    BadPathName,
    SymLinkLoop,
} || std.posix.UnexpectedError;

pub fn unlinkatZ(dirfd: posix.fd_t, sub_path: [*:0]const u8, flags: u32) UnlinkatError!void {
    switch (posix.errno(system.unlinkat(dirfd, sub_path, flags))) {
        .SUCCESS => return,
        .ACCES, .PERM => return error.AccessDenied,
        .BUSY => return error.FileBusy,
        .FAULT => unreachable,
        .IO => return error.FileNotFound,
        .ISDIR => return error.IsDir,
        .LOOP => return error.SymLinkLoop,
        .NAMETOOLONG => return error.NameTooLong,
        .NOENT => return error.FileNotFound,
        .NOTDIR => return error.NotDir,
        .NOMEM => return error.SystemResources,
        .ROFS => return error.ReadOnlyFileSystem,
        .EXIST, .NOTEMPTY => return error.DirNotEmpty,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub const UnlinkError = error{
    FileNotFound,
    AccessDenied,
    IsDir,
    FileBusy,
    SymLinkLoop,
    NameTooLong,
    NotDir,
    SystemResources,
    ReadOnlyFileSystem,
} || std.posix.UnexpectedError;

pub fn unlink(path: []const u8) UnlinkError!void {
    const path_c = posix.toPosixPath(path) catch return error.NameTooLong;
    switch (posix.errno(system.unlink(&path_c))) {
        .SUCCESS => return,
        .ACCES, .PERM => return error.AccessDenied,
        .BUSY => return error.FileBusy,
        .FAULT => unreachable,
        .ISDIR => return error.IsDir,
        .LOOP => return error.SymLinkLoop,
        .NAMETOOLONG => return error.NameTooLong,
        .NOENT => return error.FileNotFound,
        .NOTDIR => return error.NotDir,
        .NOMEM => return error.SystemResources,
        .ROFS => return error.ReadOnlyFileSystem,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub const FChmodAtError = error{
    AccessDenied,
    FileNotFound,
    InvalidUtf8,
    NameTooLong,
    SymLinkLoop,
    ReadOnlyFileSystem,
    NotDir,
} || std.posix.UnexpectedError;

pub fn fchmodat(dirfd: posix.fd_t, sub_path: []const u8, mode: u32, flags: u32) FChmodAtError!void {
    const sub_path_c = posix.toPosixPath(sub_path) catch return error.NameTooLong;
    const m: posix.mode_t = @intCast(mode);
    switch (posix.errno(system.fchmodat(dirfd, &sub_path_c, m, flags))) {
        .SUCCESS => return,
        .ACCES, .PERM => return error.AccessDenied,
        .BADF => unreachable,
        .FAULT => unreachable,
        .LOOP => return error.SymLinkLoop,
        .NAMETOOLONG => return error.NameTooLong,
        .NOENT => return error.FileNotFound,
        .NOTDIR => return error.NotDir,
        .ROFS => return error.ReadOnlyFileSystem,
        else => |err| return posix.unexpectedErrno(err),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// lseek wrappers
// ─────────────────────────────────────────────────────────────────────────────

pub const SeekError = error{
    Unseekable,
} || std.posix.UnexpectedError;

const SEEK_SET: i32 = 0;
const SEEK_CUR: i32 = 1;
const SEEK_END: i32 = 2;

fn lseek(fd: posix.fd_t, offset: i64, whence: i32) SeekError!u64 {
    const rc = system.lseek(fd, @intCast(offset), whence);
    const signed: i64 = @bitCast(@as(u64, @bitCast(@as(i64, rc))));
    if (signed >= 0) return @intCast(signed);
    switch (errnoOf(signed)) {
        .BADF, .INVAL, .OVERFLOW, .NXIO, .SPIPE => return error.Unseekable,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn lseekSet(fd: posix.fd_t, offset: u64) SeekError!void {
    _ = try lseek(fd, @intCast(offset), SEEK_SET);
}

pub fn lseekCur(fd: posix.fd_t, offset: i64) SeekError!void {
    _ = try lseek(fd, offset, SEEK_CUR);
}

pub fn lseekEnd(fd: posix.fd_t, offset: i64) SeekError!u64 {
    return lseek(fd, offset, SEEK_END);
}

pub fn lseekCurPos(fd: posix.fd_t) SeekError!u64 {
    return lseek(fd, 0, SEEK_CUR);
}

// ─────────────────────────────────────────────────────────────────────────────
// fchmod
// ─────────────────────────────────────────────────────────────────────────────

pub fn fchmod(fd: posix.fd_t, mode: u32) !void {
    const m: posix.mode_t = @intCast(mode);
    switch (posix.errno(system.fchmod(fd, m))) {
        .SUCCESS => return,
        .ACCES, .PERM => return error.AccessDenied,
        .BADF, .INVAL, .ROFS, .IO => unreachable,
        else => |err| return posix.unexpectedErrno(err),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// readlinkatZ
// ─────────────────────────────────────────────────────────────────────────────

pub fn readlinkatZ(dirfd: posix.fd_t, path: [*:0]const u8, buf: []u8) ![]u8 {
    const rc = system.readlinkat(dirfd, path, buf.ptr, buf.len);
    const signed: isize = @bitCast(rc);
    if (signed >= 0) return buf[0..@intCast(signed)];
    switch (errnoOf(signed)) {
        .ACCES, .PERM => return error.AccessDenied,
        .FAULT => unreachable,
        .INVAL => return error.NotLink,
        .IO => return error.InputOutput,
        .LOOP => return error.SymLinkLoop,
        .NAMETOOLONG => return error.NameTooLong,
        .NOENT => return error.FileNotFound,
        .NOTDIR => return error.NotDir,
        .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// pwrite / fstat / ftruncate / renameZ / symlinkatZ
// ─────────────────────────────────────────────────────────────────────────────

pub fn pread(fd: posix.fd_t, bytes: []u8, offset: u64) ReadError!usize {
    while (true) {
        const rc = system.pread(fd, bytes.ptr, bytes.len, @intCast(offset));
        const signed: isize = @bitCast(rc);
        if (signed >= 0) return @intCast(signed);
        switch (errnoOf(signed)) {
            .INTR => continue,
            .INVAL => unreachable,
            .FAULT => unreachable,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.NotOpenForReading,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .NOBUFS, .NOMEM => unreachable, // ReadError doesn't model SystemResources
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub fn pwrite(fd: posix.fd_t, bytes: []const u8, offset: u64) WriteError!usize {
    while (true) {
        const rc = system.pwrite(fd, bytes.ptr, bytes.len, @intCast(offset));
        const signed: isize = @bitCast(rc);
        if (signed >= 0) return @intCast(signed);
        switch (errnoOf(signed)) {
            .INTR => continue,
            .INVAL => return error.InvalidArgument,
            .FAULT => unreachable,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.NotOpenForWriting,
            .DQUOT => return error.DiskQuota,
            .FBIG => return error.FileTooBig,
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .PERM => return error.AccessDenied,
            .PIPE => return error.BrokenPipe,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub fn fstat(fd: posix.fd_t) !posix.Stat {
    var stat: posix.Stat = undefined;
    switch (posix.errno(system.fstat(fd, &stat))) {
        .SUCCESS => return stat,
        .BADF => unreachable,
        .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn ftruncate(fd: posix.fd_t, length: u64) !void {
    while (true) {
        switch (posix.errno(system.ftruncate(fd, @intCast(length)))) {
            .SUCCESS => return,
            .INTR => continue,
            .FBIG => return error.FileTooBig,
            .IO => return error.InputOutput,
            .PERM => return error.AccessDenied,
            .BADF, .INVAL => unreachable,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub fn renameZ(old_path: [*:0]const u8, new_path: [*:0]const u8) !void {
    switch (posix.errno(system.rename(old_path, new_path))) {
        .SUCCESS => return,
        .ACCES, .PERM => return error.AccessDenied,
        .BUSY => return error.FileBusy,
        .DQUOT => return error.DiskQuota,
        .FAULT => unreachable,
        .ISDIR => return error.IsDir,
        .LOOP => return error.SymLinkLoop,
        .MLINK => return error.LinkQuotaExceeded,
        .NAMETOOLONG => return error.NameTooLong,
        .NOENT => return error.FileNotFound,
        .NOTDIR => return error.NotDir,
        .NOMEM => return error.SystemResources,
        .NOSPC => return error.NoSpaceLeft,
        .EXIST, .NOTEMPTY => return error.PathAlreadyExists,
        .ROFS => return error.ReadOnlyFileSystem,
        .XDEV => return error.CrossDevice,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn symlinkatZ(target_path: [*:0]const u8, dirfd: posix.fd_t, sym_link_path: [*:0]const u8) !void {
    switch (posix.errno(system.symlinkat(target_path, dirfd, sym_link_path))) {
        .SUCCESS => return,
        .ACCES, .PERM => return error.AccessDenied,
        .DQUOT => return error.DiskQuota,
        .EXIST => return error.PathAlreadyExists,
        .FAULT => unreachable,
        .IO => return error.InputOutput,
        .LOOP => return error.SymLinkLoop,
        .NAMETOOLONG => return error.NameTooLong,
        .NOENT => return error.FileNotFound,
        .NOSPC => return error.NoSpaceLeft,
        .NOTDIR => return error.NotDir,
        .ROFS => return error.ReadOnlyFileSystem,
        .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// open / openat (Z variants)
// ─────────────────────────────────────────────────────────────────────────────

pub const OpenError = std.posix.OpenError;

pub fn openZ(path: [*:0]const u8, flags: posix.O, mode: posix.mode_t) OpenError!posix.fd_t {
    return openatZ(posix.AT.FDCWD, path, flags, mode);
}

pub fn openatZ(dirfd: posix.fd_t, path: [*:0]const u8, flags: posix.O, mode: posix.mode_t) OpenError!posix.fd_t {
    return posix.openatZ(dirfd, path, flags, mode);
}

// ─────────────────────────────────────────────────────────────────────────────
// waitpid
// ─────────────────────────────────────────────────────────────────────────────

pub const WaitPidResult = struct { pid: posix.pid_t, status: u32 };

pub fn waitpid(pid: posix.pid_t, flags: u32) WaitPidResult {
    var status: c_int = undefined;
    while (true) {
        const rc = system.waitpid(pid, &status, @intCast(flags));
        const signed: isize = @bitCast(@as(usize, @bitCast(@as(isize, rc))));
        switch (errnoOf(signed)) {
            .SUCCESS => return .{ .pid = @intCast(rc), .status = @bitCast(status) },
            .INTR => continue,
            .CHILD => unreachable, // PID doesn't exist
            .INVAL => unreachable, // bad flags
            else => |err| {
                _ = err;
                return .{ .pid = -1, .status = 0 };
            },
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// fsync
// ─────────────────────────────────────────────────────────────────────────────

pub const SyncError = error{
    InputOutput,
    NoSpaceLeft,
    DiskQuota,
} || std.posix.UnexpectedError;

// ─────────────────────────────────────────────────────────────────────────────
// sendfile / copy_file_range — kernel zero-copy
// ─────────────────────────────────────────────────────────────────────────────

pub const SendfileError = error{
    WouldBlock,
    InputOutput,
    BrokenPipe,
    ConnectionResetByPeer,
    OperationNotSupported,
    InvalidArgument,
    AccessDenied,
    SystemResources,
} || std.posix.UnexpectedError;

/// Kernel sendfile wrapper. Transfers up to `count` bytes from
/// `in_fd` (must be a regular file on Darwin; any seekable fd on
/// Linux 2.6.33+) to `out_fd` (any fd on Linux; must be a socket
/// on Darwin). `offset` advances the source's read cursor when
/// non-null; pass null to read from / advance the existing cursor.
/// Returns bytes sent.
pub fn sendfile(
    out_fd: posix.fd_t,
    in_fd: posix.fd_t,
    offset: ?*u64,
    count: usize,
) SendfileError!usize {
    return switch (builtin.os.tag) {
        .linux => sendfileLinux(out_fd, in_fd, offset, count),
        .macos, .ios, .tvos, .watchos, .visionos, .driverkit => sendfileDarwin(out_fd, in_fd, offset, count),
        else => error.OperationNotSupported,
    };
}

fn sendfileLinux(out_fd: posix.fd_t, in_fd: posix.fd_t, offset: ?*u64, count: usize) SendfileError!usize {
    var off_local: i64 = if (offset) |o| @intCast(o.*) else 0;
    const off_ptr: ?*i64 = if (offset != null) &off_local else null;
    while (true) {
        const rc = std.c.sendfile(out_fd, in_fd, off_ptr, count);
        if (rc >= 0) {
            if (offset) |o| o.* = @intCast(off_local);
            return @intCast(rc);
        }
        switch (posix.errno(rc)) {
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .IO => return error.InputOutput,
            .PIPE => return error.BrokenPipe,
            .CONNRESET => return error.ConnectionResetByPeer,
            .OPNOTSUPP, .NOSYS => return error.OperationNotSupported,
            .INVAL => return error.InvalidArgument,
            .NOMEM => return error.SystemResources,
            .ACCES, .PERM => return error.AccessDenied,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn sendfileDarwin(out_fd: posix.fd_t, in_fd: posix.fd_t, offset: ?*u64, count: usize) SendfileError!usize {
    // Darwin's sendfile takes (in, out) — note the param order is
    // SWAPPED relative to Linux. Returns 0 on success with *len
    // updated; -1 on error, with *len reflecting bytes already
    // transferred. EAGAIN is reported with *len > 0 if some bytes
    // were sent before would-block.
    while (true) {
        const start_off: i64 = if (offset) |o| @intCast(o.*) else 0;
        var len_inout: i64 = @intCast(count);
        const rc = std.c.sendfile(in_fd, out_fd, start_off, &len_inout, null, 0);
        if (rc == 0) {
            if (offset) |o| o.* = @intCast(start_off + len_inout);
            return @intCast(len_inout);
        }
        switch (posix.errno(rc)) {
            .INTR => {
                if (len_inout > 0) {
                    if (offset) |o| o.* = @intCast(start_off + len_inout);
                    return @intCast(len_inout);
                }
                continue;
            },
            .AGAIN => {
                if (len_inout > 0) {
                    // Partial transfer before would-block — surface
                    // those bytes; caller's loop will park on
                    // writable for the next batch.
                    if (offset) |o| o.* = @intCast(start_off + len_inout);
                    return @intCast(len_inout);
                }
                return error.WouldBlock;
            },
            .IO => return error.InputOutput,
            .PIPE => return error.BrokenPipe,
            .CONNRESET => return error.ConnectionResetByPeer,
            .OPNOTSUPP => return error.OperationNotSupported,
            .INVAL => return error.InvalidArgument,
            .NOTSOCK => return error.OperationNotSupported,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub const CopyFileRangeError = error{
    WouldBlock,
    InputOutput,
    OperationNotSupported,
    CrossDevice,
    InvalidArgument,
    SystemResources,
    AccessDenied,
    NoSpaceLeft,
} || std.posix.UnexpectedError;

/// Linux-only `copy_file_range` — kernel-level file→file copy.
/// Returns bytes copied. `error.OperationNotSupported` /
/// `error.CrossDevice` mean "fall back to userspace loop." On
/// non-Linux platforms returns `error.OperationNotSupported`.
pub fn copyFileRange(
    fd_in: posix.fd_t,
    fd_out: posix.fd_t,
    len: usize,
) CopyFileRangeError!usize {
    if (builtin.os.tag != .linux) return error.OperationNotSupported;
    while (true) {
        const rc = std.c.copy_file_range(fd_in, null, fd_out, null, len, 0);
        if (rc >= 0) return @intCast(rc);
        switch (posix.errno(rc)) {
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .XDEV => return error.CrossDevice,
            .NOSYS, .OPNOTSUPP => return error.OperationNotSupported,
            .INVAL => return error.InvalidArgument,
            .IO => return error.InputOutput,
            .NOMEM => return error.SystemResources,
            .NOSPC => return error.NoSpaceLeft,
            .ACCES, .PERM => return error.AccessDenied,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

pub fn fsync(fd: posix.fd_t) SyncError!void {
    while (true) {
        switch (posix.errno(system.fsync(fd))) {
            .SUCCESS => return,
            .BADF, .INVAL, .ROFS => unreachable,
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .DQUOT => return error.DiskQuota,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// kevent (Darwin / BSD)
// ─────────────────────────────────────────────────────────────────────────────

pub const KeventError = error{
    EventNotFound,
    AccessDenied,
    SystemResources,
} || std.posix.UnexpectedError;

pub fn kevent(
    kq: i32,
    changelist: []const posix.Kevent,
    eventlist: []posix.Kevent,
    timeout: ?*const posix.timespec,
) KeventError!usize {
    while (true) {
        const rc = system.kevent(
            kq,
            changelist.ptr,
            std.math.cast(c_int, changelist.len) orelse std.math.maxInt(c_int),
            eventlist.ptr,
            std.math.cast(c_int, eventlist.len) orelse std.math.maxInt(c_int),
            timeout,
        );
        const signed: isize = @bitCast(@as(usize, @bitCast(@as(isize, rc))));
        if (signed >= 0) return @intCast(signed);
        switch (errnoOf(signed)) {
            .INTR => continue,
            .ACCES => return error.AccessDenied,
            .FAULT => unreachable,
            .BADF => unreachable,
            .INVAL => unreachable,
            .NOENT => return error.EventNotFound,
            .NOMEM => return error.SystemResources,
            .SRCH => return error.EventNotFound,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// kqueue (Darwin / BSD)
// ─────────────────────────────────────────────────────────────────────────────

pub const KqueueError = error{
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
} || std.posix.UnexpectedError;

pub fn kqueue() KqueueError!i32 {
    const rc: isize = @bitCast(@as(usize, @bitCast(@as(isize, system.kqueue()))));
    if (rc >= 0) return @intCast(rc);
    switch (posix.errno(rc)) {
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        else => |err| return posix.unexpectedErrno(err),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// mkdirat / fstatat (with null-terminated paths)
// ─────────────────────────────────────────────────────────────────────────────

pub const MkdirError = error{
    AccessDenied,
    DiskQuota,
    PathAlreadyExists,
    SymLinkLoop,
    LinkQuotaExceeded,
    NameTooLong,
    FileNotFound,
    SystemResources,
    NoSpaceLeft,
    NotDir,
    ReadOnlyFileSystem,
    InvalidUtf8,
    BadPathName,
    NoDevice,
} || std.posix.UnexpectedError;

pub fn mkdiratZ(dirfd: posix.fd_t, sub_path: [*:0]const u8, mode: u32) MkdirError!void {
    // mkdirat's mode_t is platform-specific: u16 on Darwin, u32 on Linux.
    const m: posix.mode_t = @intCast(mode);
    switch (posix.errno(system.mkdirat(dirfd, sub_path, m))) {
        .SUCCESS => return,
        .ACCES => return error.AccessDenied,
        .BADF => unreachable,
        .PERM => return error.AccessDenied,
        .DQUOT => return error.DiskQuota,
        .EXIST => return error.PathAlreadyExists,
        .FAULT => unreachable,
        .LOOP => return error.SymLinkLoop,
        .MLINK => return error.LinkQuotaExceeded,
        .NAMETOOLONG => return error.NameTooLong,
        .NOENT => return error.FileNotFound,
        .NOMEM => return error.SystemResources,
        .NOSPC => return error.NoSpaceLeft,
        .NOTDIR => return error.NotDir,
        .ROFS => return error.ReadOnlyFileSystem,
        .NODEV => return error.NoDevice,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub const FStatAtError = error{
    AccessDenied,
    FileNotFound,
    NameTooLong,
    SymLinkLoop,
    SystemResources,
    InvalidUtf8,
} || std.posix.UnexpectedError;

pub fn fstatatZ(dirfd: posix.fd_t, sub_path: [*:0]const u8, flags: u32) FStatAtError!posix.Stat {
    var stat: posix.Stat = undefined;
    switch (posix.errno(system.fstatat(dirfd, sub_path, &stat, flags))) {
        .SUCCESS => return stat,
        .ACCES => return error.AccessDenied,
        .PERM => return error.AccessDenied,
        .BADF => unreachable,
        .FAULT => unreachable,
        .INVAL => unreachable,
        .LOOP => return error.SymLinkLoop,
        .NAMETOOLONG => return error.NameTooLong,
        .NOENT => return error.FileNotFound,
        .NOMEM => return error.SystemResources,
        .NOTDIR => return error.FileNotFound,
        else => |err| return posix.unexpectedErrno(err),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "syscall.socket - creates TCP socket" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const fd = try socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    defer close(fd);
    try std.testing.expect(fd >= 0);
}

test "syscall.pipe - creates pipe" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const fds = try pipe();
    defer close(fds[0]);
    defer close(fds[1]);
    try std.testing.expect(fds[0] >= 0);
    try std.testing.expect(fds[1] >= 0);
}

test "syscall.kqueue - creates kqueue on BSD/Darwin" {
    const is_bsd = switch (builtin.os.tag) {
        .macos, .ios, .tvos, .watchos, .visionos, .freebsd, .netbsd, .openbsd, .dragonfly => true,
        else => false,
    };
    if (!is_bsd) return error.SkipZigTest;
    const fd = try kqueue();
    defer close(fd);
    try std.testing.expect(fd >= 0);
}
