//! Risk #1 mitigation gate — verify the v1.1 IoError taxonomy.
//!
//! Two assertions:
//!   1. `fromErrno` produces the expected `IoError` for every errno
//!      we care about. New errno mappings get a line here.
//!   2. A representative `catch |err| switch (err)` over historical
//!      error names (the ones users were already writing against the
//!      pre-v1.1 surface) still compiles. If any of these names
//!      vanished from the public sub-sets, the build fails here
//!      before any consumer notices.

const std = @import("std");
const io_errors = @import("../io/errors.zig");
const io = @import("../io/io.zig");
const wait = @import("../io/wait.zig");
const net = @import("../io/net.zig");
const IoError = io_errors.IoError;
const fromErrno = io_errors.fromErrno;

// ─────────────────────────────────────────────────────────────────────────────
// 1. fromErrno: every errno of interest hits a named member.
// ─────────────────────────────────────────────────────────────────────────────

test "fromErrno: I/O fundamentals" {
    try std.testing.expectEqual(IoError.WouldBlock, fromErrno(.AGAIN));
    try std.testing.expectEqual(IoError.WouldBlock, fromErrno(.INPROGRESS));
    try std.testing.expectEqual(IoError.Interrupted, fromErrno(.INTR));
    try std.testing.expectEqual(IoError.BrokenPipe, fromErrno(.PIPE));
    try std.testing.expectEqual(IoError.InputOutput, fromErrno(.IO));
}

test "fromErrno: permissions / paths" {
    try std.testing.expectEqual(IoError.AccessDenied, fromErrno(.ACCES));
    try std.testing.expectEqual(IoError.AccessDenied, fromErrno(.PERM));
    try std.testing.expectEqual(IoError.ReadOnlyFileSystem, fromErrno(.ROFS));
    try std.testing.expectEqual(IoError.FileNotFound, fromErrno(.NOENT));
    try std.testing.expectEqual(IoError.PathAlreadyExists, fromErrno(.EXIST));
    try std.testing.expectEqual(IoError.IsDir, fromErrno(.ISDIR));
    try std.testing.expectEqual(IoError.NotDir, fromErrno(.NOTDIR));
    try std.testing.expectEqual(IoError.SymLinkLoop, fromErrno(.LOOP));
    try std.testing.expectEqual(IoError.NameTooLong, fromErrno(.NAMETOOLONG));
    try std.testing.expectEqual(IoError.DirNotEmpty, fromErrno(.NOTEMPTY));
    try std.testing.expectEqual(IoError.CrossDevice, fromErrno(.XDEV));
}

test "fromErrno: connection state" {
    try std.testing.expectEqual(IoError.AddressInUse, fromErrno(.ADDRINUSE));
    try std.testing.expectEqual(IoError.AddressNotAvailable, fromErrno(.ADDRNOTAVAIL));
    try std.testing.expectEqual(IoError.ConnectionRefused, fromErrno(.CONNREFUSED));
    try std.testing.expectEqual(IoError.ConnectionResetByPeer, fromErrno(.CONNRESET));
    try std.testing.expectEqual(IoError.ConnectionAborted, fromErrno(.CONNABORTED));
    try std.testing.expectEqual(IoError.ConnectionTimedOut, fromErrno(.TIMEDOUT));
    try std.testing.expectEqual(IoError.ConnectionPending, fromErrno(.ALREADY));
    try std.testing.expectEqual(IoError.NetworkUnreachable, fromErrno(.NETUNREACH));
    try std.testing.expectEqual(IoError.HostUnreachable, fromErrno(.HOSTUNREACH));
    try std.testing.expectEqual(IoError.SocketNotConnected, fromErrno(.NOTCONN));
    try std.testing.expectEqual(IoError.FileDescriptorNotASocket, fromErrno(.NOTSOCK));
}

test "fromErrno: socket layer" {
    try std.testing.expectEqual(IoError.AddressFamilyNotSupported, fromErrno(.AFNOSUPPORT));
    try std.testing.expectEqual(IoError.ProtocolNotSupported, fromErrno(.PROTONOSUPPORT));
    try std.testing.expectEqual(IoError.SocketTypeNotSupported, fromErrno(.PROTOTYPE));
    try std.testing.expectEqual(IoError.OperationNotSupported, fromErrno(.OPNOTSUPP));
    try std.testing.expectEqual(IoError.MessageTooBig, fromErrno(.MSGSIZE));
    try std.testing.expectEqual(IoError.ProtocolFailure, fromErrno(.PROTO));
    try std.testing.expectEqual(IoError.InvalidProtocolOption, fromErrno(.NOPROTOOPT));
}

test "fromErrno: resource limits" {
    try std.testing.expectEqual(IoError.ProcessFdQuotaExceeded, fromErrno(.MFILE));
    try std.testing.expectEqual(IoError.SystemFdQuotaExceeded, fromErrno(.NFILE));
    try std.testing.expectEqual(IoError.NoSpaceLeft, fromErrno(.NOSPC));
    try std.testing.expectEqual(IoError.DiskQuota, fromErrno(.DQUOT));
    try std.testing.expectEqual(IoError.FileTooBig, fromErrno(.FBIG));
    try std.testing.expectEqual(IoError.LinkQuotaExceeded, fromErrno(.MLINK));
    try std.testing.expectEqual(IoError.SystemResources, fromErrno(.NOMEM));
    try std.testing.expectEqual(IoError.SystemResources, fromErrno(.NOBUFS));
}

test "fromErrno: locking / busy" {
    try std.testing.expectEqual(IoError.DeviceBusy, fromErrno(.BUSY));
    try std.testing.expectEqual(IoError.DeadLock, fromErrno(.DEADLK));
    try std.testing.expectEqual(IoError.LockedRegionLimitExceeded, fromErrno(.NOLCK));
}

test "fromErrno: misc" {
    try std.testing.expectEqual(IoError.NoDevice, fromErrno(.NODEV));
    try std.testing.expectEqual(IoError.InvalidArgument, fromErrno(.INVAL));
    try std.testing.expectEqual(IoError.ProcessNotFound, fromErrno(.SRCH));
    try std.testing.expectEqual(IoError.Unseekable, fromErrno(.SPIPE));
    try std.testing.expectEqual(IoError.Unseekable, fromErrno(.NXIO));
    try std.testing.expectEqual(IoError.Unseekable, fromErrno(.OVERFLOW));
}

test "fromErrno: unknown errno collapses to Unexpected" {
    // Use .SUCCESS as a stand-in for "kernel returned something we don't model."
    // The else arm of fromErrno catches it.
    try std.testing.expectEqual(IoError.Unexpected, fromErrno(.SUCCESS));
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Historical-name compile-fence — every name a v1.0 user could
//    `catch |err| switch (err)` against must still be a valid arm in the
//    new sub-sets. If a future commit deletes one of these from
//    `IoError`, this test fails to compile.
//
// These functions never run; their existence is the assertion.
// ─────────────────────────────────────────────────────────────────────────────

fn _historicalReadErrors(e: io.ReadError) void {
    switch (e) {
        // Names a v1.0 user would have caught against syscall.ReadError + wait.WaitError.
        error.WouldBlock,
        error.BrokenPipe,
        error.InputOutput,
        error.IsDir,
        error.NotOpenForReading,
        error.ConnectionResetByPeer,
        error.ConnectionTimedOut,
        error.SocketNotConnected,
        error.AccessDenied,
        error.Cancelled,
        error.OutOfMemory,
        error.Unexpected,
        => {},
        else => {},
    }
}

fn _historicalWriteErrors(e: io.WriteError) void {
    switch (e) {
        error.WouldBlock,
        error.BrokenPipe,
        error.ConnectionResetByPeer,
        error.InputOutput,
        error.NotOpenForWriting,
        error.AccessDenied,
        error.DiskQuota,
        error.FileTooBig,
        error.NoSpaceLeft,
        error.DeviceBusy,
        error.InvalidArgument,
        error.OperationAborted,
        error.LockViolation,
        error.ProcessNotFound,
        error.Cancelled,
        error.OutOfMemory,
        error.Unexpected,
        => {},
        else => {},
    }
}

fn _historicalConnectErrors(e: net.ConnectError) void {
    switch (e) {
        error.AccessDenied,
        error.AddressInUse,
        error.AddressNotAvailable,
        error.AddressFamilyNotSupported,
        error.ConnectionPending,
        error.ConnectionRefused,
        error.ConnectionResetByPeer,
        error.NetworkUnreachable,
        error.FileNotFound,
        error.FileDescriptorNotASocket,
        error.ConnectionTimedOut,
        error.WouldBlock,
        error.Cancelled,
        error.OutOfMemory,
        error.Unexpected,
        => {},
        else => {},
    }
}

fn _historicalAcceptErrors(e: net.AcceptError) void {
    switch (e) {
        error.ConnectionAborted,
        error.FileDescriptorNotASocket,
        error.ProcessFdQuotaExceeded,
        error.SystemFdQuotaExceeded,
        error.SystemResources,
        error.SocketNotListening,
        error.OperationNotSupported,
        error.WouldBlock,
        error.ProtocolFailure,
        error.BlockedByFirewall,
        error.ConnectionResetByPeer,
        error.Cancelled,
        error.OutOfMemory,
        error.Unexpected,
        => {},
        else => {},
    }
}

fn _historicalWaitErrors(e: wait.WaitError) void {
    switch (e) {
        // Historical names from the v1.0 wait.WaitError. Platform-specific
        // names (EpollCtlFailed, EventNotFound) are intentionally gone —
        // they collapsed to error.WaitRegistrationFailed, caught by `else`.
        error.Cancelled,
        error.OutOfMemory,
        error.AccessDenied,
        error.SystemResources,
        error.Unexpected,
        => {},
        else => {},
    }
}

test "historical-name compile-fence" {
    // Witness functions are static; merely referencing them is enough.
    _ = &_historicalReadErrors;
    _ = &_historicalWriteErrors;
    _ = &_historicalConnectErrors;
    _ = &_historicalAcceptErrors;
    _ = &_historicalWaitErrors;
}
