//! `volt.fs.Dir` — async directory handle.
//!
//! Wraps a directory fd. All path-taking operations go through
//! `*at` syscalls rooted at the dir's fd, so they're TOCTOU-safe:
//! `dir.openFile("config.json")` won't get redirected if a parent
//! directory is renamed/deleted between the lookup and the open.
//!
//! `Dir.cwd()` returns a special handle bound to `AT_FDCWD` —
//! useful when you don't have (or don't want) a long-lived dirfd.
//! `cwd().close()` is a no-op.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

const syscall = @import("../internal/syscall.zig");
const spawnBlocking = @import("../api/spawn_blocking.zig").spawnBlocking;
const File = @import("File.zig").File;
const OpenOptionsT = @import("OpenOptions.zig").OpenOptions;
const DirEntry = @import("DirEntry.zig").DirEntry;
const Kind = @import("Metadata.zig").Kind;

pub const Dir = struct {
    fd: posix.fd_t,

    /// Special handle bound to AT_FDCWD. Path-relative operations
    /// resolve from the process's current working directory.
    pub fn cwd() Dir {
        return .{ .fd = posix.AT.FDCWD };
    }

    /// Open `path` as a directory. Routes through the blocking pool.
    pub fn open(path: []const u8) !Dir {
        var path_buf: [4096]u8 = undefined;
        if (path.len >= path_buf.len) return error.NameTooLong;
        const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

        const Args = struct { path_z: [:0]const u8 };
        var args = Args{ .path_z = path_z };
        const fd = try spawnBlocking(struct {
            fn run(a: *Args) !posix.fd_t {
                const flags = posix.O{
                    .ACCMODE = .RDONLY,
                    .DIRECTORY = true,
                    .CLOEXEC = true,
                };
                return posix.openatZ(posix.AT.FDCWD, a.path_z.ptr, flags, 0);
            }
        }.run, .{&args});
        return .{ .fd = fd };
    }

    /// Open a sub-directory. Path is rooted at this dir's fd
    /// (TOCTOU-safe vs path-relative open).
    pub fn openDir(self: *Dir, path: []const u8) !Dir {
        var path_buf: [4096]u8 = undefined;
        if (path.len >= path_buf.len) return error.NameTooLong;
        const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

        const Args = struct { dirfd: posix.fd_t, path_z: [:0]const u8 };
        var args = Args{ .dirfd = self.fd, .path_z = path_z };
        const fd = try spawnBlocking(struct {
            fn run(a: *Args) !posix.fd_t {
                const flags = posix.O{
                    .ACCMODE = .RDONLY,
                    .DIRECTORY = true,
                    .CLOEXEC = true,
                };
                return posix.openatZ(a.dirfd, a.path_z.ptr, flags, 0);
            }
        }.run, .{&args});
        return .{ .fd = fd };
    }

    /// Open a file relative to this directory.
    pub fn openFile(self: *Dir, path: []const u8, opts: OpenOptionsT) !File {
        var path_buf: [4096]u8 = undefined;
        if (path.len >= path_buf.len) return error.NameTooLong;
        const path_z = std.fmt.bufPrintZ(&path_buf, "{s}", .{path}) catch return error.NameTooLong;

        // OpenOptions.toPosix() is private to OpenOptions; replicate
        // the flag construction here. Kept consistent by the inline
        // tests in OpenOptions.zig.
        const accmode: posix.ACCMODE = if (opts.read and opts.write)
            .RDWR
        else if (opts.write or opts.append)
            .WRONLY
        else
            .RDONLY;
        const flags = posix.O{
            .ACCMODE = accmode,
            .APPEND = opts.append,
            .CREAT = opts.create or opts.exclusive,
            .EXCL = opts.exclusive,
            .TRUNC = opts.truncate,
            .CLOEXEC = true,
        };

        const Args = struct { dirfd: posix.fd_t, path_z: [:0]const u8, flags: posix.O, mode: posix.mode_t };
        var args = Args{ .dirfd = self.fd, .path_z = path_z, .flags = flags, .mode = opts.mode };
        const fd = try spawnBlocking(struct {
            fn run(a: *Args) !posix.fd_t {
                return posix.openatZ(a.dirfd, a.path_z.ptr, a.flags, a.mode);
            }
        }.run, .{&args});
        return File{ .fd = fd };
    }

    pub fn close(self: *Dir) void {
        // AT_FDCWD is a sentinel — never close it.
        if (self.fd == posix.AT.FDCWD) return;
        syscall.close(self.fd);
    }

    /// Iterate the directory's entries. The returned iterator owns
    /// a duplicated fd (libc `readdir` machinery requires a DIR*).
    pub fn iterate(self: *Dir) !Iterator {
        // dup the fd because fdopendir takes ownership; closedir on
        // the iterator end will close the dup, leaving our fd intact.
        const dup_fd = std.c.dup(self.fd);
        if (dup_fd < 0) return error.SystemResources;
        const dir_p = std.c.fdopendir(dup_fd) orelse {
            _ = std.c.close(dup_fd);
            return error.SystemResources;
        };
        return Iterator{ .dir_p = dir_p };
    }

    pub const Iterator = struct {
        dir_p: *std.c.DIR,

        pub fn next(self: *Iterator) ?DirEntry {
            while (true) {
                const ent = std.c.readdir(self.dir_p) orelse return null;
                const name_slice = nameSlice(ent);
                // Skip `.` and `..`.
                if (std.mem.eql(u8, name_slice, ".") or std.mem.eql(u8, name_slice, "..")) {
                    continue;
                }
                return DirEntry{
                    .name = name_slice,
                    .kind = kindFromDt(ent.type),
                    .inode = inodeOf(ent),
                };
            }
        }

        pub fn deinit(self: *Iterator) void {
            _ = std.c.closedir(self.dir_p);
        }
    };
};

fn nameSlice(ent: *const std.c.dirent) []const u8 {
    // Linux dirent has no namlen — must use strlen.
    // Darwin / BSD dirent has namlen.
    if (@hasField(std.c.dirent, "namlen")) {
        return ent.name[0..ent.namlen];
    }
    return std.mem.sliceTo(@as([*:0]const u8, @ptrCast(&ent.name)), 0);
}

fn inodeOf(ent: *const std.c.dirent) u64 {
    if (@hasField(std.c.dirent, "ino")) return @intCast(ent.ino);
    if (@hasField(std.c.dirent, "fileno")) return @intCast(ent.fileno);
    return 0;
}

fn kindFromDt(d_type: u8) Kind {
    return switch (d_type) {
        std.c.DT.REG => .file,
        std.c.DT.DIR => .directory,
        std.c.DT.LNK => .symlink,
        std.c.DT.BLK => .block_device,
        std.c.DT.CHR => .character_device,
        std.c.DT.FIFO => .named_pipe,
        std.c.DT.SOCK => .unix_domain_socket,
        else => .unknown,
    };
}
