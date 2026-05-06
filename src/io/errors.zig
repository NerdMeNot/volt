//! Volt-owned I/O error taxonomy.
//!
//! `IoError` is the master closed set; per-operation sub-sets list the
//! errors each public operation can return. Every backend translates to
//! these names at its own boundary — `volt.io`, `volt.net`, `volt.fs`
//! never leak `syscall.*Error` or `posix.E.*` through their public types.
//!
//! ## Why a master set + sub-sets
//!
//! Zig error tags are global: `error.WouldBlock` declared in two sets
//! refers to the same tag. So sub-sets are nominal subsets of `IoError`
//! and can be widened automatically — `try syscall.read(...)` returning
//! `syscall.ReadError` flows up as `ReadError` here without manual
//! translation, provided every name in the syscall set also appears in
//! our subset. That's the migration contract.
//!
//! ## Adding a new error
//!
//! Add to `IoError` first, then to whichever operation sub-set(s) can
//! return it. `fromErrno` covers the kernel-side mapping; per-call
//! translation in `syscall.zig` covers context-sensitive errnos
//! (`.BADF` means `NotOpenForReading` in read, `NotOpenForWriting` in
//! write — `fromErrno` can't disambiguate, so it returns `Unexpected`
//! and callers narrow at their own boundary).

const std = @import("std");
const posix = std.posix;

// ─────────────────────────────────────────────────────────────────────────────
// Master set
// ─────────────────────────────────────────────────────────────────────────────

pub const IoError = error{
    // I/O fundamentals
    WouldBlock,
    Interrupted,
    BrokenPipe,
    InputOutput,
    Unseekable,

    // Permissions
    AccessDenied,
    ReadOnlyFileSystem,
    BlockedByFirewall,

    // Path / filesystem state
    FileNotFound,
    PathAlreadyExists,
    IsDir,
    NotDir,
    NotLink,
    DirNotEmpty,
    SymLinkLoop,
    NameTooLong,
    BadPathName,
    InvalidUtf8,
    CrossDevice,

    // Resource limits
    SystemResources,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    NoSpaceLeft,
    DiskQuota,
    FileTooBig,
    LinkQuotaExceeded,

    // Connections / sockets
    NotConnected,
    SocketNotConnected,
    SocketNotBound,
    SocketNotListening,
    AlreadyBound,
    ConnectionRefused,
    ConnectionAborted,
    ConnectionResetByPeer,
    ConnectionTimedOut,
    ConnectionPending,
    NetworkUnreachable,
    HostUnreachable,
    AddressInUse,
    AddressNotAvailable,
    NetworkSubsystemFailed,

    // Socket layer
    FileDescriptorNotASocket,
    AddressFamilyNotSupported,
    ProtocolFamilyNotAvailable,
    ProtocolNotSupported,
    SocketTypeNotSupported,
    OperationNotSupported,
    ProtocolFailure,
    InvalidProtocolOption,
    MessageTooBig,
    FastOpenAlreadyInProgress,

    // File handle state
    NotOpenForReading,
    NotOpenForWriting,

    // Locking / busy
    DeviceBusy,
    FileBusy,
    Locked,
    LockViolation,
    DeadLock,
    LockedRegionLimitExceeded,
    OperationAborted,
    BlockingOperationInProgress,

    // Misc
    NoDevice,
    InvalidArgument,
    ProcessNotFound,
    TimeoutTooBig,

    // Coroutine / runtime layer
    Cancelled,
    OutOfMemory,

    // Reactor — platform-neutral name; backends translate kqueue/epoll/iouring
    // failures into this at their boundary.
    WaitRegistrationFailed,

    // Catch-all for errnos we don't model.
    Unexpected,
};

// ─────────────────────────────────────────────────────────────────────────────
// Per-operation sub-sets — what each public call can return.
//
// Each set is a curated subset of IoError. Zig narrows automatically when
// a wider set flows into a function whose return type is one of these.
// ─────────────────────────────────────────────────────────────────────────────

pub const ReadError = error{
    WouldBlock,
    Interrupted,
    BrokenPipe,
    InputOutput,
    IsDir,
    NotOpenForReading,
    ConnectionResetByPeer,
    ConnectionTimedOut,
    SocketNotConnected,
    AccessDenied,
    Cancelled,
    OutOfMemory,
    SystemResources,
    WaitRegistrationFailed,
    Unexpected,
};

pub const WriteError = error{
    WouldBlock,
    Interrupted,
    BrokenPipe,
    ConnectionResetByPeer,
    InputOutput,
    NotOpenForWriting,
    AccessDenied,
    DiskQuota,
    FileTooBig,
    NoSpaceLeft,
    DeviceBusy,
    InvalidArgument,
    OperationAborted,
    LockViolation,
    ProcessNotFound,
    Cancelled,
    OutOfMemory,
    SystemResources,
    WaitRegistrationFailed,
    Unexpected,
};

pub const SeekError = error{
    Unseekable,
    Unexpected,
};

pub const SyncError = error{
    InputOutput,
    NoSpaceLeft,
    DiskQuota,
    Unexpected,
};

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
    HostUnreachable,
    FileNotFound,
    FileDescriptorNotASocket,
    ConnectionTimedOut,
    WouldBlock,
    Cancelled,
    OutOfMemory,
    WaitRegistrationFailed,
    Unexpected,
};

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
    Unexpected,
};

pub const ListenError = error{
    AddressInUse,
    FileDescriptorNotASocket,
    OperationNotSupported,
    NetworkSubsystemFailed,
    SystemResources,
    Unexpected,
};

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
    Cancelled,
    OutOfMemory,
    WaitRegistrationFailed,
    Unexpected,
};

pub const ShutdownError = error{
    ConnectionAborted,
    ConnectionResetByPeer,
    BlockingOperationInProgress,
    FileDescriptorNotASocket,
    SocketNotConnected,
    SystemResources,
    Unexpected,
};

pub const SocketError = error{
    AccessDenied,
    AddressFamilyNotSupported,
    ProtocolFamilyNotAvailable,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    ProtocolNotSupported,
    SocketTypeNotSupported,
    Unexpected,
};

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
    Cancelled,
    OutOfMemory,
    WaitRegistrationFailed,
    Unexpected,
};

pub const RecvError = error{
    WouldBlock,
    SystemResources,
    SocketNotConnected,
    ConnectionResetByPeer,
    ConnectionRefused,
    ConnectionTimedOut,
    MessageTooBig,
    Cancelled,
    OutOfMemory,
    WaitRegistrationFailed,
    Unexpected,
};

pub const OpenError = error{
    AccessDenied,
    FileNotFound,
    PathAlreadyExists,
    IsDir,
    NotDir,
    NameTooLong,
    SymLinkLoop,
    BadPathName,
    InvalidUtf8,
    ReadOnlyFileSystem,
    NoSpaceLeft,
    DiskQuota,
    FileTooBig,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    DeviceBusy,
    FileBusy,
    NoDevice,
    OperationNotSupported,
    Unexpected,
};

pub const StatError = error{
    AccessDenied,
    FileNotFound,
    NameTooLong,
    NotDir,
    SymLinkLoop,
    SystemResources,
    Unexpected,
};

pub const FcntlError = error{
    AccessDenied,
    FileBusy,
    ProcessFdQuotaExceeded,
    Locked,
    DeadLock,
    LockedRegionLimitExceeded,
    Unexpected,
};

pub const GetSockOptError = error{
    NoDevice,
    SystemResources,
    InvalidProtocolOption,
    TimeoutTooBig,
    AccessDenied,
    Unexpected,
};

pub const WaitError = error{
    Cancelled,
    OutOfMemory,
    AccessDenied,
    SystemResources,
    WaitRegistrationFailed,
    Unexpected,
};

// ─────────────────────────────────────────────────────────────────────────────
// Errno → IoError translation.
//
// Generic mapping for new I/O sites that don't go through syscall.zig's
// already-narrowed wrappers. Context-sensitive errnos (.BADF, .ISCONN,
// .DESTADDRREQ) collapse to Unexpected — caller-side switch must
// disambiguate before invoking this. Mirrors what syscall.zig already
// does per call-site.
// ─────────────────────────────────────────────────────────────────────────────

pub fn fromErrno(e: posix.E) IoError {
    return switch (e) {
        .ACCES, .PERM => error.AccessDenied,
        .ADDRINUSE => error.AddressInUse,
        .ADDRNOTAVAIL => error.AddressNotAvailable,
        .AFNOSUPPORT => error.AddressFamilyNotSupported,
        .AGAIN => error.WouldBlock,
        .ALREADY => error.ConnectionPending,
        .BUSY => error.DeviceBusy,
        .CONNABORTED => error.ConnectionAborted,
        .CONNREFUSED => error.ConnectionRefused,
        .CONNRESET => error.ConnectionResetByPeer,
        .DEADLK => error.DeadLock,
        .DQUOT => error.DiskQuota,
        .EXIST => error.PathAlreadyExists,
        .FBIG => error.FileTooBig,
        .HOSTUNREACH => error.HostUnreachable,
        .INPROGRESS => error.WouldBlock,
        .INTR => error.Interrupted,
        .INVAL => error.InvalidArgument,
        .IO => error.InputOutput,
        .ISDIR => error.IsDir,
        .LOOP => error.SymLinkLoop,
        .MFILE => error.ProcessFdQuotaExceeded,
        .MLINK => error.LinkQuotaExceeded,
        .MSGSIZE => error.MessageTooBig,
        .NAMETOOLONG => error.NameTooLong,
        .NETUNREACH => error.NetworkUnreachable,
        .NFILE => error.SystemFdQuotaExceeded,
        .NOBUFS, .NOMEM => error.SystemResources,
        .NODEV => error.NoDevice,
        .NOENT => error.FileNotFound,
        .NOLCK => error.LockedRegionLimitExceeded,
        .NOPROTOOPT => error.InvalidProtocolOption,
        .NOSPC => error.NoSpaceLeft,
        .NOTCONN => error.SocketNotConnected,
        .NOTDIR => error.NotDir,
        .NOTEMPTY => error.DirNotEmpty,
        .NOTSOCK => error.FileDescriptorNotASocket,
        .NXIO => error.Unseekable,
        .OPNOTSUPP => error.OperationNotSupported,
        .OVERFLOW => error.Unseekable,
        .PIPE => error.BrokenPipe,
        .PROTO => error.ProtocolFailure,
        .PROTONOSUPPORT => error.ProtocolNotSupported,
        .PROTOTYPE => error.SocketTypeNotSupported,
        .ROFS => error.ReadOnlyFileSystem,
        .SPIPE => error.Unseekable,
        .SRCH => error.ProcessNotFound,
        .TIMEDOUT => error.ConnectionTimedOut,
        .XDEV => error.CrossDevice,
        else => error.Unexpected,
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "fromErrno: common cases hit named members" {
    try std.testing.expectEqual(IoError.WouldBlock, fromErrno(.AGAIN));
    try std.testing.expectEqual(IoError.BrokenPipe, fromErrno(.PIPE));
    try std.testing.expectEqual(IoError.ConnectionRefused, fromErrno(.CONNREFUSED));
    try std.testing.expectEqual(IoError.FileNotFound, fromErrno(.NOENT));
    try std.testing.expectEqual(IoError.IsDir, fromErrno(.ISDIR));
    try std.testing.expectEqual(IoError.AddressInUse, fromErrno(.ADDRINUSE));
}

test "fromErrno: unknown errno collapses to Unexpected" {
    // .SUCCESS isn't a real error — ensures the switch's else arm is the catch-all.
    try std.testing.expectEqual(IoError.Unexpected, fromErrno(.SUCCESS));
}

// Sub-set / master-set consistency is verified by the migration itself:
// every public type in `volt.io.{io,wait,net}` and `volt.fs` widens its
// callees' error sets into `IoError.*`. If any sub-set is missing a
// member, the build fails at the call site.
