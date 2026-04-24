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

// ─────────────────────────────────────────────────────────────────────────────
// Sockets
// ─────────────────────────────────────────────────────────────────────────────

pub const SocketError = error{
    PermissionDenied,
    AddressFamilyNotSupported,
    ProtocolFamilyNotAvailable,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    ProtocolNotSupported,
    SocketTypeNotSupported,
} || std.posix.UnexpectedError;

pub fn socket(domain: u32, sock_type: u32, protocol: u32) SocketError!posix.socket_t {
    const rc = system.socket(domain, sock_type, protocol);
    switch (posix.errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .ACCES => return error.PermissionDenied,
        .AFNOSUPPORT => return error.AddressFamilyNotSupported,
        .INVAL => return error.ProtocolFamilyNotAvailable,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOBUFS, .NOMEM => return error.SystemResources,
        .PROTONOSUPPORT => return error.ProtocolNotSupported,
        .PROTOTYPE => return error.SocketTypeNotSupported,
        else => |err| return posix.unexpectedErrno(err),
    }
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
    PermissionDenied,
    FileBusy,
    ProcessFdQuotaExceeded,
    Locked,
    DeadLock,
    LockedRegionLimitExceeded,
} || std.posix.UnexpectedError;

pub fn fcntl(fd: posix.fd_t, cmd: i32, arg: usize) FcntlError!usize {
    while (true) {
        const rc: usize = @bitCast(@as(isize, system.fcntl(fd, cmd, arg)));
        switch (posix.errno(@as(isize, @bitCast(rc)))) {
            .SUCCESS => return rc,
            .INTR => continue,
            .ACCES => return error.Locked,
            .BADF => unreachable,
            .BUSY => return error.FileBusy,
            .INVAL => unreachable,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NOTDIR => unreachable,
            .PERM => return error.PermissionDenied,
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
        switch (posix.errno(signed)) {
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
        switch (posix.errno(signed)) {
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
        switch (posix.errno(signed)) {
            .INTR => continue,
            .INVAL => unreachable,
            .FAULT => unreachable,
            .AGAIN => return error.WouldBlock,
            .BADF => return error.NotOpenForReading,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .NOBUFS, .NOMEM => return error.SystemResources,
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
    while (true) {
        const rc = if (comptime @hasDecl(system, "accept4"))
            system.accept4(fd, addr, addr_size, flags)
        else
            system.accept(fd, addr, addr_size);
        const signed: isize = @bitCast(@as(usize, @bitCast(@as(isize, rc))));
        if (signed >= 0) return @intCast(signed);
        switch (posix.errno(signed)) {
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
    PermissionDenied,
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
            .ACCES, .PERM => return error.PermissionDenied,
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
        switch (posix.errno(signed)) {
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
        switch (posix.errno(signed)) {
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
    PermissionDenied,
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
