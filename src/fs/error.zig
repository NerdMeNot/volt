//! Categorical filesystem error vocabulary. Parallel to
//! `reactor.IoError` (which covers socket / pipe transport errors)
//! — `FsError` covers the path-name semantic errors that come from
//! `open` / `stat` / `mkdir` / `rename` / friends.
//!
//! `FileError = FsError || IoError` is the union used by `File`'s
//! read/write/seek/sync methods, since an opened file can hit
//! either category.

const std = @import("std");
const builtin = @import("builtin");
const IoError = @import("../reactor.zig").IoError;

pub const FsError = error{
    /// `ENOENT` / `ERROR_FILE_NOT_FOUND` — the path doesn't exist.
    NotFound,
    /// `EEXIST` / `ERROR_FILE_EXISTS` — the path exists when
    /// `.create_new` was requested.
    AlreadyExists,
    /// `EACCES` / `EPERM` / `ERROR_ACCESS_DENIED` — permission
    /// denied. The classic "fix your umask" case.
    AccessDenied,
    /// `EISDIR` — operation requires a regular file but got a
    /// directory.
    IsDirectory,
    /// `ENOTDIR` — a path component is not a directory.
    NotDirectory,
    /// `EROFS` — read-only filesystem.
    ReadOnlyFs,
    /// `ENAMETOOLONG` — path or component exceeds limits.
    NameTooLong,
    /// `ENOSPC` — disk full / quota exhausted.
    NoSpace,
    /// `ENOTEMPTY` / `EEXIST` on remove — directory not empty.
    DirNotEmpty,
    /// `EMLINK` — too many hard links to the target.
    TooManyLinks,
    /// `ELOOP` — symbolic-link loop encountered during resolution.
    SymlinkLoop,
    /// `EINVAL` / `ENAMETOOLONG` from a path with embedded NULs,
    /// or otherwise malformed.
    InvalidPath,
    /// `EBADF` — file descriptor closed / never valid.
    BadDescriptor,
    /// `EBUSY` / `ETXTBSY` — the resource is in use; the kernel
    /// won't let us touch it. Rare on user fs paths.
    Busy,
    /// `EXDEV` — cross-device link / rename. `volt.fs.rename`
    /// surfaces this so callers can fall back to copy + delete.
    CrossDevice,
    /// `ENOMEM` — kernel-side OOM.
    SystemResources,
    /// `EOPNOTSUPP` / `ENOSYS` — the operation isn't supported by
    /// this filesystem or platform.
    NotSupported,
    /// `EINTR` — interrupted syscall. Caller may retry; we
    /// surface it because some callers want to know.
    Interrupted,
    /// Catch-all for errnos we haven't categorised yet.
    Unexpected,
};

/// Combined error set used by `File` read/write/sync methods —
/// the file may be open and read fail for transport reasons
/// (`IoError`) or path / fs reasons (`FsError`).
pub const FileError = FsError || IoError;

/// Decode the current errno into a typed `FsError`. The caller
/// passes the freshly-captured errno; mapping is platform-aware
/// where the same name maps to different values.
pub fn fromErrno(errno: c_int) FsError {
    return switch (errno) {
        @intFromEnum(std.c.E.NOENT) => error.NotFound,
        @intFromEnum(std.c.E.EXIST) => error.AlreadyExists,
        @intFromEnum(std.c.E.ACCES) => error.AccessDenied,
        @intFromEnum(std.c.E.PERM) => error.AccessDenied,
        @intFromEnum(std.c.E.ISDIR) => error.IsDirectory,
        @intFromEnum(std.c.E.NOTDIR) => error.NotDirectory,
        @intFromEnum(std.c.E.ROFS) => error.ReadOnlyFs,
        @intFromEnum(std.c.E.NAMETOOLONG) => error.NameTooLong,
        @intFromEnum(std.c.E.NOSPC) => error.NoSpace,
        @intFromEnum(std.c.E.NOTEMPTY) => error.DirNotEmpty,
        @intFromEnum(std.c.E.MLINK) => error.TooManyLinks,
        @intFromEnum(std.c.E.LOOP) => error.SymlinkLoop,
        @intFromEnum(std.c.E.INVAL) => error.InvalidPath,
        @intFromEnum(std.c.E.BADF) => error.BadDescriptor,
        @intFromEnum(std.c.E.BUSY) => error.Busy,
        @intFromEnum(std.c.E.XDEV) => error.CrossDevice,
        @intFromEnum(std.c.E.NOMEM) => error.SystemResources,
        @intFromEnum(std.c.E.OPNOTSUPP) => error.NotSupported,
        @intFromEnum(std.c.E.NOSYS) => error.NotSupported,
        @intFromEnum(std.c.E.INTR) => error.Interrupted,
        else => error.Unexpected,
    };
}

/// Snapshot the current errno. POSIX exposes errno as a
/// thread-local int; std.c has the per-platform indirection.
pub fn currentErrno() c_int {
    return std.c._errno().*;
}
