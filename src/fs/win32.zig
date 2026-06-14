//! Win32 filesystem bindings — the Windows analog of `syscall.zig`.
//!
//! Thin `@extern` declarations for the kernel32 calls the Windows fs
//! backend needs, plus the shared support every fs module reuses:
//! UTF-8 ↔ UTF-16 path conversion, `GetLastError` → `FsError`
//! mapping, and `FILETIME` → Unix-time conversion.
//!
//! Like `syscall.zig`, every extern is gated behind `is_windows` so
//! this module is importable from any platform (the POSIX build pulls
//! it in transitively via `metadata.zig`) — the externs only
//! materialise when compiling for Windows.
//!
//! Calling convention is `.winapi` (Microsoft x64 on our only Windows
//! target). Symbols resolve against kernel32, which mingw links by
//! default under `-lc`.

const std = @import("std");
const builtin = @import("builtin");
const fs_error = @import("error.zig");

const is_windows = builtin.os.tag == .windows;

pub const FsError = fs_error.FsError;

// ─── Core Win32 types ────────────────────────────────────────────

pub const HANDLE = *anyopaque;
pub const DWORD = u32;
pub const BOOL = i32;
/// `CreateSymbolicLinkW` returns `BOOLEAN` (a byte), not `BOOL`.
pub const BOOLEAN = u8;
pub const WCHAR = u16;
pub const LARGE_INTEGER = i64;
pub const ULONGLONG = u64;

pub const TRUE: BOOL = 1;
pub const FALSE: BOOL = 0;

/// `(HANDLE)-1` — what `CreateFileW` returns on failure.
pub const INVALID_HANDLE_VALUE: HANDLE = @ptrFromInt(std.math.maxInt(usize));

// ─── dwDesiredAccess ─────────────────────────────────────────────
pub const GENERIC_READ: DWORD = 0x80000000;
pub const GENERIC_WRITE: DWORD = 0x40000000;
/// Append-only write access — pair with no GENERIC_WRITE so the
/// kernel forces every write to EOF (the O_APPEND analog).
pub const FILE_APPEND_DATA: DWORD = 0x0004;

// ─── dwShareMode ─────────────────────────────────────────────────
pub const FILE_SHARE_READ: DWORD = 0x00000001;
pub const FILE_SHARE_WRITE: DWORD = 0x00000002;
pub const FILE_SHARE_DELETE: DWORD = 0x00000004;

// ─── dwCreationDisposition ───────────────────────────────────────
pub const CREATE_NEW: DWORD = 1;
pub const CREATE_ALWAYS: DWORD = 2;
pub const OPEN_EXISTING: DWORD = 3;
pub const OPEN_ALWAYS: DWORD = 4;
pub const TRUNCATE_EXISTING: DWORD = 5;

// ─── dwDesiredAccess (attribute-only handles) ────────────────────
pub const FILE_READ_ATTRIBUTES: DWORD = 0x0080;
pub const FILE_WRITE_ATTRIBUTES: DWORD = 0x0100;

// ─── dwFlagsAndAttributes ────────────────────────────────────────
pub const FILE_ATTRIBUTE_READONLY: DWORD = 0x00000001;
pub const FILE_ATTRIBUTE_DIRECTORY: DWORD = 0x00000010;
pub const FILE_ATTRIBUTE_NORMAL: DWORD = 0x00000080;
pub const FILE_ATTRIBUTE_REPARSE_POINT: DWORD = 0x00000400;
/// `GetFileAttributesW` sentinel for "failed / does not exist".
pub const INVALID_FILE_ATTRIBUTES: DWORD = 0xFFFFFFFF;
/// Required to open a directory handle (for fstat / fsync on dirs).
pub const FILE_FLAG_BACKUP_SEMANTICS: DWORD = 0x02000000;
/// Open the reparse point itself rather than its target (the lstat /
/// readlink analog).
pub const FILE_FLAG_OPEN_REPARSE_POINT: DWORD = 0x00200000;

pub const FILE_SHARE_ALL: DWORD = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;

// ─── MoveFileExW flags ───────────────────────────────────────────
pub const MOVEFILE_REPLACE_EXISTING: DWORD = 0x1;
pub const MOVEFILE_COPY_ALLOWED: DWORD = 0x2;

// ─── CreateSymbolicLinkW flags ───────────────────────────────────
pub const SYMBOLIC_LINK_FLAG_DIRECTORY: DWORD = 0x1;
pub const SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE: DWORD = 0x2;

// ─── Reparse points (readlink) ───────────────────────────────────
pub const IO_REPARSE_TAG_SYMLINK: DWORD = 0xA000000C;
pub const IO_REPARSE_TAG_MOUNT_POINT: DWORD = 0xA0000003;
pub const FSCTL_GET_REPARSE_POINT: DWORD = 0x000900A8;
pub const MAXIMUM_REPARSE_DATA_BUFFER_SIZE: usize = 16384;

// ─── SetFilePointerEx dwMoveMethod ───────────────────────────────
pub const FILE_BEGIN: DWORD = 0;
pub const FILE_CURRENT: DWORD = 1;
pub const FILE_END: DWORD = 2;

// ─── GetLastError codes we categorise ────────────────────────────
pub const ERROR_FILE_NOT_FOUND: DWORD = 2;
pub const ERROR_PATH_NOT_FOUND: DWORD = 3;
pub const ERROR_ACCESS_DENIED: DWORD = 5;
pub const ERROR_INVALID_HANDLE: DWORD = 6;
pub const ERROR_NOT_ENOUGH_MEMORY: DWORD = 8;
pub const ERROR_OUTOFMEMORY: DWORD = 14;
pub const ERROR_NO_MORE_FILES: DWORD = 18;
pub const ERROR_WRITE_PROTECT: DWORD = 19;
pub const ERROR_SHARING_VIOLATION: DWORD = 32;
pub const ERROR_HANDLE_EOF: DWORD = 38;
pub const ERROR_HANDLE_DISK_FULL: DWORD = 39;
pub const ERROR_NOT_SUPPORTED: DWORD = 50;
pub const ERROR_FILE_EXISTS: DWORD = 80;
pub const ERROR_INVALID_PARAMETER: DWORD = 87;
pub const ERROR_INVALID_NAME: DWORD = 123;
pub const ERROR_DIR_NOT_EMPTY: DWORD = 145;
pub const ERROR_ALREADY_EXISTS: DWORD = 183;
pub const ERROR_DISK_FULL: DWORD = 112;
pub const ERROR_NOT_SAME_DEVICE: DWORD = 17;
pub const ERROR_TOO_MANY_LINKS: DWORD = 1142;
pub const ERROR_NOT_A_REPARSE_POINT: DWORD = 4390;
/// Returned by CreateSymbolicLinkW when the caller lacks the
/// create-symlink right (no Developer Mode / not elevated).
pub const ERROR_PRIVILEGE_NOT_HELD: DWORD = 1314;

// ─── Structs ─────────────────────────────────────────────────────

pub const FILETIME = extern struct {
    dwLowDateTime: DWORD,
    dwHighDateTime: DWORD,
};

pub const BY_HANDLE_FILE_INFORMATION = extern struct {
    dwFileAttributes: DWORD,
    ftCreationTime: FILETIME,
    ftLastAccessTime: FILETIME,
    ftLastWriteTime: FILETIME,
    dwVolumeSerialNumber: DWORD,
    nFileSizeHigh: DWORD,
    nFileSizeLow: DWORD,
    nNumberOfLinks: DWORD,
    nFileIndexHigh: DWORD,
    nFileIndexLow: DWORD,
};

pub const MAX_PATH: usize = 260;

pub const WIN32_FIND_DATAW = extern struct {
    dwFileAttributes: DWORD,
    ftCreationTime: FILETIME,
    ftLastAccessTime: FILETIME,
    ftLastWriteTime: FILETIME,
    nFileSizeHigh: DWORD,
    nFileSizeLow: DWORD,
    dwReserved0: DWORD,
    dwReserved1: DWORD,
    cFileName: [MAX_PATH]u16,
    cAlternateFileName: [14]u16,
};

pub const OVERLAPPED = extern struct {
    Internal: usize = 0,
    InternalHigh: usize = 0,
    DUMMYUNIONNAME: extern union {
        DUMMYSTRUCTNAME: extern struct { Offset: DWORD, OffsetHigh: DWORD },
        Pointer: ?*anyopaque,
    } = .{ .Pointer = null },
    hEvent: ?HANDLE = null,
};

// ─── Externs (kernel32) ──────────────────────────────────────────

pub const CreateFileW = if (is_windows) @extern(*const fn (
    lpFileName: [*:0]const u16,
    dwDesiredAccess: DWORD,
    dwShareMode: DWORD,
    lpSecurityAttributes: ?*anyopaque,
    dwCreationDisposition: DWORD,
    dwFlagsAndAttributes: DWORD,
    hTemplateFile: ?HANDLE,
) callconv(.winapi) HANDLE, .{ .name = "CreateFileW" }) else {};

pub const CloseHandle = if (is_windows) @extern(
    *const fn (hObject: HANDLE) callconv(.winapi) BOOL,
    .{ .name = "CloseHandle" },
) else {};

pub const ReadFile = if (is_windows) @extern(*const fn (
    hFile: HANDLE,
    lpBuffer: [*]u8,
    nNumberOfBytesToRead: DWORD,
    lpNumberOfBytesRead: ?*DWORD,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) BOOL, .{ .name = "ReadFile" }) else {};

pub const WriteFile = if (is_windows) @extern(*const fn (
    hFile: HANDLE,
    lpBuffer: [*]const u8,
    nNumberOfBytesToWrite: DWORD,
    lpNumberOfBytesWritten: ?*DWORD,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) BOOL, .{ .name = "WriteFile" }) else {};

pub const SetFilePointerEx = if (is_windows) @extern(*const fn (
    hFile: HANDLE,
    liDistanceToMove: LARGE_INTEGER,
    lpNewFilePointer: ?*LARGE_INTEGER,
    dwMoveMethod: DWORD,
) callconv(.winapi) BOOL, .{ .name = "SetFilePointerEx" }) else {};

pub const FlushFileBuffers = if (is_windows) @extern(
    *const fn (hFile: HANDLE) callconv(.winapi) BOOL,
    .{ .name = "FlushFileBuffers" },
) else {};

pub const SetEndOfFile = if (is_windows) @extern(
    *const fn (hFile: HANDLE) callconv(.winapi) BOOL,
    .{ .name = "SetEndOfFile" },
) else {};

pub const GetFileInformationByHandle = if (is_windows) @extern(*const fn (
    hFile: HANDLE,
    lpFileInformation: *BY_HANDLE_FILE_INFORMATION,
) callconv(.winapi) BOOL, .{ .name = "GetFileInformationByHandle" }) else {};

pub const GetLastError = if (is_windows) @extern(
    *const fn () callconv(.winapi) DWORD,
    .{ .name = "GetLastError" },
) else {};

/// `FILE_INFO_BY_HANDLE_CLASS.FileBasicInfo` — the class selector for
/// `SetFileInformationByHandle` when changing attributes/timestamps.
pub const FileBasicInfo: i32 = 0;

pub const FILE_BASIC_INFO = extern struct {
    /// 0 in any timestamp field means "leave unchanged".
    CreationTime: LARGE_INTEGER = 0,
    LastAccessTime: LARGE_INTEGER = 0,
    LastWriteTime: LARGE_INTEGER = 0,
    ChangeTime: LARGE_INTEGER = 0,
    FileAttributes: DWORD = 0,
};

pub const SetFileInformationByHandle = if (is_windows) @extern(*const fn (
    hFile: HANDLE,
    FileInformationClass: i32,
    lpFileInformation: *anyopaque,
    dwBufferSize: DWORD,
) callconv(.winapi) BOOL, .{ .name = "SetFileInformationByHandle" }) else {};

// ─── Path conversion ─────────────────────────────────────────────

/// Largest path we'll convert. Windows' classic `MAX_PATH` is 260,
/// but `\\?\`-prefixed paths reach ~32767; we size to the POSIX
/// `PATH_MAX` (4096) like the rest of the fs layer and leave the
/// long-path-prefix story to a follow-up.
pub const WPATH_MAX: usize = 4096;

/// NUL-terminated UTF-16LE path, stack-allocated. Mirrors
/// `file.zig`'s `PathZ` for the POSIX side.
pub const WPathZ = struct {
    buf: [WPATH_MAX:0]u16 = undefined,
    len: usize = 0,

    pub fn fromUtf8(path: []const u8) FsError!WPathZ {
        if (std.mem.indexOfScalar(u8, path, 0) != null) return error.InvalidPath;
        if (path.len >= WPATH_MAX) return error.NameTooLong;
        var self: WPathZ = .{};
        // UTF-16 unit count ≤ UTF-8 byte count, so `path.len < WPATH_MAX`
        // guarantees the result + NUL fit.
        const n = std.unicode.utf8ToUtf16Le(self.buf[0..], path) catch return error.InvalidPath;
        self.buf[n] = 0;
        self.len = n;
        return self;
    }

    pub fn ptr(self: *const WPathZ) [*:0]const u16 {
        return @ptrCast(&self.buf);
    }
};

// ─── Error mapping ───────────────────────────────────────────────

/// Map a `GetLastError` code to the categorical `FsError`. Parallel
/// to `error.fromErrno` for the POSIX side.
pub fn fromLastError(code: DWORD) FsError {
    return switch (code) {
        ERROR_FILE_NOT_FOUND, ERROR_PATH_NOT_FOUND => error.NotFound,
        ERROR_FILE_EXISTS, ERROR_ALREADY_EXISTS => error.AlreadyExists,
        ERROR_ACCESS_DENIED, ERROR_SHARING_VIOLATION, ERROR_PRIVILEGE_NOT_HELD => error.AccessDenied,
        ERROR_WRITE_PROTECT => error.ReadOnlyFs,
        ERROR_INVALID_NAME => error.InvalidPath,
        ERROR_INVALID_HANDLE => error.BadDescriptor,
        ERROR_DISK_FULL, ERROR_HANDLE_DISK_FULL => error.NoSpace,
        ERROR_DIR_NOT_EMPTY => error.DirNotEmpty,
        ERROR_TOO_MANY_LINKS => error.TooManyLinks,
        ERROR_NOT_SAME_DEVICE => error.CrossDevice,
        ERROR_NOT_ENOUGH_MEMORY, ERROR_OUTOFMEMORY => error.SystemResources,
        ERROR_NOT_SUPPORTED, ERROR_NOT_A_REPARSE_POINT => error.NotSupported,
        else => error.Unexpected,
    };
}

/// Snapshot `GetLastError`. Lives here so callers never touch the
/// extern directly.
pub fn lastError() DWORD {
    if (!is_windows) return 0;
    return GetLastError();
}

// ─── FILETIME → Unix time ────────────────────────────────────────

pub const UnixTime = struct { secs: i64, nsecs: u32 };

/// 100-ns ticks between 1601-01-01 (FILETIME epoch) and 1970-01-01
/// (Unix epoch).
const FILETIME_UNIX_DIFF: u64 = 116_444_736_000_000_000;

pub fn filetimeToUnix(ft: FILETIME) UnixTime {
    const ticks = (@as(u64, ft.dwHighDateTime) << 32) | @as(u64, ft.dwLowDateTime);
    if (ticks < FILETIME_UNIX_DIFF) return .{ .secs = 0, .nsecs = 0 };
    const unix_ticks = ticks - FILETIME_UNIX_DIFF;
    return .{
        .secs = @intCast(unix_ticks / 10_000_000),
        .nsecs = @intCast((unix_ticks % 10_000_000) * 100),
    };
}

pub fn unixToFiletime(secs: i64, nsecs: u32) FILETIME {
    const ticks_i: i128 = (@as(i128, secs) * 10_000_000) +
        @divFloor(@as(i128, nsecs), 100) +
        @as(i128, FILETIME_UNIX_DIFF);
    const ticks: u64 = if (ticks_i < 0) 0 else @intCast(ticks_i);
    return .{ .dwLowDateTime = @truncate(ticks), .dwHighDateTime = @truncate(ticks >> 32) };
}

// ─── Externs (path/metadata ops) ─────────────────────────────────

pub const GetFileAttributesW = if (is_windows) @extern(
    *const fn (lpFileName: [*:0]const u16) callconv(.winapi) DWORD,
    .{ .name = "GetFileAttributesW" },
) else {};

pub const SetFileAttributesW = if (is_windows) @extern(
    *const fn (lpFileName: [*:0]const u16, dwFileAttributes: DWORD) callconv(.winapi) BOOL,
    .{ .name = "SetFileAttributesW" },
) else {};

pub const SetFileTime = if (is_windows) @extern(*const fn (
    hFile: HANDLE,
    lpCreationTime: ?*const FILETIME,
    lpLastAccessTime: ?*const FILETIME,
    lpLastWriteTime: ?*const FILETIME,
) callconv(.winapi) BOOL, .{ .name = "SetFileTime" }) else {};

pub const MoveFileExW = if (is_windows) @extern(*const fn (
    lpExistingFileName: [*:0]const u16,
    lpNewFileName: ?[*:0]const u16,
    dwFlags: DWORD,
) callconv(.winapi) BOOL, .{ .name = "MoveFileExW" }) else {};

pub const CreateHardLinkW = if (is_windows) @extern(*const fn (
    lpFileName: [*:0]const u16,
    lpExistingFileName: [*:0]const u16,
    lpSecurityAttributes: ?*anyopaque,
) callconv(.winapi) BOOL, .{ .name = "CreateHardLinkW" }) else {};

pub const CreateSymbolicLinkW = if (is_windows) @extern(*const fn (
    lpSymlinkFileName: [*:0]const u16,
    lpTargetFileName: [*:0]const u16,
    dwFlags: DWORD,
) callconv(.winapi) BOOLEAN, .{ .name = "CreateSymbolicLinkW" }) else {};

pub const DeleteFileW = if (is_windows) @extern(
    *const fn (lpFileName: [*:0]const u16) callconv(.winapi) BOOL,
    .{ .name = "DeleteFileW" },
) else {};

pub const GetFinalPathNameByHandleW = if (is_windows) @extern(*const fn (
    hFile: HANDLE,
    lpszFilePath: [*]u16,
    cchFilePath: DWORD,
    dwFlags: DWORD,
) callconv(.winapi) DWORD, .{ .name = "GetFinalPathNameByHandleW" }) else {};

pub const GetTempPathW = if (is_windows) @extern(
    *const fn (nBufferLength: DWORD, lpBuffer: [*]u16) callconv(.winapi) DWORD,
    .{ .name = "GetTempPathW" },
) else {};

pub const GetTempFileNameW = if (is_windows) @extern(*const fn (
    lpPathName: [*:0]const u16,
    lpPrefixString: [*:0]const u16,
    uUnique: u32,
    lpTempFileName: [*]u16,
) callconv(.winapi) u32, .{ .name = "GetTempFileNameW" }) else {};

pub const DeviceIoControl = if (is_windows) @extern(*const fn (
    hDevice: HANDLE,
    dwIoControlCode: DWORD,
    lpInBuffer: ?*anyopaque,
    nInBufferSize: DWORD,
    lpOutBuffer: ?*anyopaque,
    nOutBufferSize: DWORD,
    lpBytesReturned: ?*DWORD,
    lpOverlapped: ?*OVERLAPPED,
) callconv(.winapi) BOOL, .{ .name = "DeviceIoControl" }) else {};

// ─── Externs (directory ops) ─────────────────────────────────────

pub const CreateDirectoryW = if (is_windows) @extern(*const fn (
    lpPathName: [*:0]const u16,
    lpSecurityAttributes: ?*anyopaque,
) callconv(.winapi) BOOL, .{ .name = "CreateDirectoryW" }) else {};

pub const RemoveDirectoryW = if (is_windows) @extern(
    *const fn (lpPathName: [*:0]const u16) callconv(.winapi) BOOL,
    .{ .name = "RemoveDirectoryW" },
) else {};

pub const FindFirstFileW = if (is_windows) @extern(*const fn (
    lpFileName: [*:0]const u16,
    lpFindFileData: *WIN32_FIND_DATAW,
) callconv(.winapi) HANDLE, .{ .name = "FindFirstFileW" }) else {};

pub const FindNextFileW = if (is_windows) @extern(*const fn (
    hFindFile: HANDLE,
    lpFindFileData: *WIN32_FIND_DATAW,
) callconv(.winapi) BOOL, .{ .name = "FindNextFileW" }) else {};

pub const FindClose = if (is_windows) @extern(
    *const fn (hFindFile: HANDLE) callconv(.winapi) BOOL,
    .{ .name = "FindClose" },
) else {};

// ─── Memory mapping ──────────────────────────────────────────────

// CreateFileMappingW flProtect:
pub const PAGE_READONLY: DWORD = 0x02;
pub const PAGE_READWRITE: DWORD = 0x04;
pub const PAGE_WRITECOPY: DWORD = 0x08;
// MapViewOfFile dwDesiredAccess:
pub const FILE_MAP_COPY: DWORD = 0x0001;
pub const FILE_MAP_WRITE: DWORD = 0x0002;
pub const FILE_MAP_READ: DWORD = 0x0004;

pub const CreateFileMappingW = if (is_windows) @extern(*const fn (
    hFile: HANDLE,
    lpFileMappingAttributes: ?*anyopaque,
    flProtect: DWORD,
    dwMaximumSizeHigh: DWORD,
    dwMaximumSizeLow: DWORD,
    lpName: ?[*:0]const u16,
) callconv(.winapi) ?HANDLE, .{ .name = "CreateFileMappingW" }) else {};

pub const MapViewOfFile = if (is_windows) @extern(*const fn (
    hFileMappingObject: HANDLE,
    dwDesiredAccess: DWORD,
    dwFileOffsetHigh: DWORD,
    dwFileOffsetLow: DWORD,
    dwNumberOfBytesToMap: usize,
) callconv(.winapi) ?*anyopaque, .{ .name = "MapViewOfFile" }) else {};

pub const UnmapViewOfFile = if (is_windows) @extern(
    *const fn (lpBaseAddress: *const anyopaque) callconv(.winapi) BOOL,
    .{ .name = "UnmapViewOfFile" },
) else {};

pub const FlushViewOfFile = if (is_windows) @extern(*const fn (
    lpBaseAddress: *const anyopaque,
    dwNumberOfBytesToFlush: usize,
) callconv(.winapi) BOOL, .{ .name = "FlushViewOfFile" }) else {};

pub const VirtualProtect = if (is_windows) @extern(*const fn (
    lpAddress: *anyopaque,
    dwSize: usize,
    flNewProtect: DWORD,
    lpflOldProtect: *DWORD,
) callconv(.winapi) BOOL, .{ .name = "VirtualProtect" }) else {};

pub const VirtualLock = if (is_windows) @extern(
    *const fn (lpAddress: *anyopaque, dwSize: usize) callconv(.winapi) BOOL,
    .{ .name = "VirtualLock" },
) else {};

pub const VirtualUnlock = if (is_windows) @extern(
    *const fn (lpAddress: *anyopaque, dwSize: usize) callconv(.winapi) BOOL,
    .{ .name = "VirtualUnlock" },
) else {};
