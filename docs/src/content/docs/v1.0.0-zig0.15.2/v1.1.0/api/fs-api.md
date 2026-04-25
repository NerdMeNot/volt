---
title: Filesystem API
description: File I/O, directory operations, metadata, and async file access in Volt.
slug: v1.0.0-zig0.15.2/v110/api/fs-api
---

Reading config files at startup, writing logs, processing data pipelines, managing temp directories -- filesystem operations are everywhere. The `fs` module gives you two ways to work with files: **synchronous** (`File`) for simple scripts and startup logic, and **async** (`AsyncFile`) for high-concurrency servers where blocking the event loop is not an option.

Most of the time you'll reach for the convenience functions -- `readFile`, `writeFile`, `copy` -- which handle opening, reading/writing, and closing in a single call. When you need fine-grained control (seeking, partial reads, metadata inspection), drop down to `File` or `AsyncFile` handles.

### At a Glance

```zig
const fs = @import("volt").fs;

// One-liner: read an entire file into memory
const data = try fs.readFile(allocator, "config.json");
defer allocator.free(data);

// One-liner: write a string to a file (creates or truncates)
try fs.writeFile("output.txt", "Hello, world!");

// File handle for more control
var file = try fs.File.open("data.bin");
defer file.close();

var buf: [4096]u8 = undefined;
const n = try file.read(&buf);
```

```zig
// Directory operations
try fs.createDirAll("logs/2026/02");

var iter = try fs.readDir("logs/2026/02");
defer iter.close();

while (try iter.next()) |entry| {
    std.debug.print("{s} ({} bytes)\n", .{ entry.name, entry.size });
}
```

```zig
// Async file I/O (requires Io handle -- uses io_uring on Linux, blocking pool elsewhere)
var async_file = try fs.AsyncFile.open(io, "large_dataset.bin");
defer async_file.close();

const chunk = try async_file.read(&buf);
```

## Module Import

```zig
const fs = @import("volt").fs;
```

All types and functions are accessed through the `fs` namespace: `fs.File`, `fs.readFile`, `fs.createDir`, etc.

***

## Convenience Functions

These handle the full open-read/write-close lifecycle in one call. Prefer these for simple operations.

### `fs.readFile`

```zig
pub fn readFile(allocator: Allocator, path: []const u8) ![]u8
```

Read the entire contents of a file into a newly allocated buffer. Caller owns the returned slice.

```zig
const config = try fs.readFile(allocator, "settings.json");
defer allocator.free(config);

const parsed = try std.json.parseFromSlice(Config, allocator, config, .{});
```

### `fs.readFileString`

```zig
pub fn readFileString(allocator: Allocator, path: []const u8) ![]u8
```

Like `readFile`, but validates that the contents are valid UTF-8. Returns `error.InvalidUtf8` if validation fails.

```zig
const template = try fs.readFileString(allocator, "email_template.txt");
defer allocator.free(template);
```

### `fs.writeFile`

```zig
pub fn writeFile(path: []const u8, data: []const u8) !void
```

Write data to a file, creating it if it doesn't exist, truncating it if it does.

```zig
const output = try std.json.stringifyAlloc(allocator, result, .{});
defer allocator.free(output);
try fs.writeFile("results.json", output);
```

### `fs.appendFile`

```zig
pub fn appendFile(path: []const u8, data: []const u8) !void
```

Append data to a file, creating it if it doesn't exist. Useful for logs and audit trails.

```zig
const timestamp = std.time.timestamp();
var buf: [128]u8 = undefined;
const line = std.fmt.bufPrint(&buf, "[{d}] Request processed\n", .{timestamp}) catch return;
try fs.appendFile("app.log", line);
```

### `fs.copy`

```zig
pub fn copy(src: []const u8, dst: []const u8) !void
```

Copy a file from `src` to `dst`. Creates the destination if it doesn't exist, truncates if it does.

```zig
// Backup before overwriting
try fs.copy("database.db", "database.db.bak");
```

### `fs.rename`

```zig
pub fn rename(old: []const u8, new: []const u8) !void
```

Rename or move a file or directory. Atomic on most filesystems when source and destination are on the same mount.

```zig
// Atomic write pattern: write to temp, then rename into place
try fs.writeFile("config.json.tmp", new_config);
try fs.rename("config.json.tmp", "config.json");
```

### `fs.fileSize`

```zig
pub fn fileSize(path: []const u8) !u64
```

Get the size of a file in bytes without opening a handle. Convenience wrapper around `metadata(path).size()`.

```zig
const size = try fs.fileSize("dataset.csv");
const buf = try allocator.alloc(u8, size);
defer allocator.free(buf);
```

### `fs.exists`

```zig
pub fn exists(path: []const u8) !bool
```

Check if a path exists. Returns an error if the check itself fails (e.g., permission denied on parent directory).

```zig
if (try fs.exists("config.toml")) {
    const config = try fs.readFile(allocator, "config.toml");
    defer allocator.free(config);
    // ...
}
```

### `fs.tryExists`

```zig
pub fn tryExists(path: []const u8) bool
```

Check if a path exists, returning `false` on any error. Convenient when you don't care about the failure reason.

```zig
const config_path = if (fs.tryExists("config.local.toml"))
    "config.local.toml"
else
    "config.toml";
```

***

## File

### `fs.File`

A handle to an open file. Provides read, write, seek, and metadata operations. Works without a runtime -- no async machinery needed.

#### Construction

| Method | Description |
|--------|-------------|
| `File.open(path)` | Open for reading |
| `File.create(path)` | Create or truncate for writing |
| `File.createNew(path)` | Create new file, fail if exists |
| `File.openWithOptions(path, opts)` | Open with custom `OpenOptions` |

```zig
// Read an existing file
var input = try fs.File.open("data.csv");
defer input.close();

// Create (or overwrite) an output file
var output = try fs.File.create("report.csv");
defer output.close();

// Fail if the file already exists (safe for lock files, temp files)
var lock = try fs.File.createNew("/tmp/myapp.lock");
defer lock.close();
```

#### Reading

| Method | Signature | Description |
|--------|-----------|-------------|
| `read` | `fn read(buf: []u8) !usize` | Read up to `buf.len` bytes, returns bytes read |
| `readAll` | `fn readAll(buf: []u8) !usize` | Fill the buffer completely (errors on EOF) |
| `readFull` | `fn readFull(buf: []u8) !usize` | Fill the buffer, returns count (no error on EOF) |
| `pread` | `fn pread(buf: []u8, offset: u64) !usize` | Read at a specific offset without seeking |
| `readToEnd` | `fn readToEnd(allocator: Allocator) ![]u8` | Read entire remaining contents |
| `readVectored` | `fn readVectored(iovecs: []posix.iovec) !usize` | Scatter read into multiple buffers |

```zig
var file = try fs.File.open("binary.dat");
defer file.close();

// Read a fixed-size header (errors if file is too short)
var header: [64]u8 = undefined;
const n = try file.readAll(&header);
if (n < 64) return error.UnexpectedEof;

// Read the rest into a dynamic buffer
const body = try file.readToEnd(allocator);
defer allocator.free(body);
```

`readFull` is like `readAll` but returns the byte count instead of erroring on EOF -- useful for streaming parsers that process partial chunks:

```zig
var buf: [256 * 1024]u8 = undefined;
while (true) {
    const n = try file.readFull(&buf);
    if (n == 0) break;
    try processChunk(buf[0..n]);
}
```

#### Writing

| Method | Signature | Description |
|--------|-----------|-------------|
| `write` | `fn write(data: []const u8) !usize` | Write up to `data.len` bytes, returns bytes written |
| `writeAll` | `fn writeAll(data: []const u8) !void` | Write all bytes (retries on partial writes) |
| `pwrite` | `fn pwrite(data: []const u8, offset: u64) !usize` | Write at a specific offset without seeking |
| `writeVectored` | `fn writeVectored(iovecs: []const posix.iovec_const) !usize` | Gather write from multiple buffers |

```zig
var file = try fs.File.create("output.bin");
defer file.close();

try file.writeAll(&header_bytes);
try file.writeAll(payload);
try file.syncAll(); // Flush to disk
```

#### Seeking

| Method | Signature | Description |
|--------|-----------|-------------|
| `seekTo` | `fn seekTo(offset: u64) !void` | Seek to absolute position |
| `seekBy` | `fn seekBy(offset: i64) !void` | Seek relative to current position |
| `rewind` | `fn rewind() !void` | Seek to the beginning |
| `getPos` | `fn getPos() !u64` | Get current position |
| `getLen` | `fn getLen() !u64` | Get file length |

```zig
var file = try fs.File.open("index.bin");
defer file.close();

// Jump to a record by index
const record_size: u64 = 256;
try file.seekTo(record_idx * record_size);

var record: [256]u8 = undefined;
_ = try file.readAll(&record);
```

#### Sync and Metadata

| Method | Signature | Description |
|--------|-----------|-------------|
| `syncAll` | `fn syncAll() !void` | Flush data + metadata to disk |
| `syncData` | `fn syncData() !void` | Flush data only (faster, skips metadata) |
| `metadata` | `fn metadata() !Metadata` | Get file metadata |
| `setLen` | `fn setLen(len: u64) !void` | Truncate or extend the file |
| `setPermissions` | `fn setPermissions(perms: Permissions) !void` | Set file permissions |

```zig
var file = try fs.File.create("important.dat");
defer file.close();

try file.writeAll(critical_data);
try file.syncAll(); // Ensure data survives a crash
```

#### Advisory Hints

Tell the kernel how you plan to access a file. On Linux this uses `posix_fadvise`; on macOS it uses `fcntl(F_RDAHEAD)` for sequential/random hints.

| Method | Signature | Description |
|--------|-----------|-------------|
| `advise` | `fn advise(advice: FileAdvice) !void` | Hint for the entire file |
| `adviseRange` | `fn adviseRange(offset: u64, len: u64, advice: FileAdvice) !void` | Hint for a byte range |

#### `File.FileAdvice`

| Value | Description |
|-------|-------------|
| `.normal` | Default behavior |
| `.sequential` | Reading front-to-back -- increase read-ahead |
| `.random` | Random access -- disable read-ahead |
| `.will_need` | Will need this data soon -- start prefetching |
| `.dont_need` | Done with this data -- can evict from cache |

```zig
var file = try fs.File.open("data.parquet");
defer file.close();

// Sequential scan
try file.advise(.sequential);

// Targeted prefetch for a specific row group
try file.adviseRange(row_group_offset, row_group_size, .will_need);
```

:::note[macOS limitations]
On macOS, only `.sequential` and `.random` have effect on regular files. `.will_need`, `.dont_need`, and `.normal` are no-ops. Range parameters are ignored -- the hint applies to the whole file. For range-level control, use memory-mapped files with `MappedFile.advise()`.
:::

#### Reader/Writer Interfaces

Files support standard `io.Reader` and `io.Writer` interfaces for composition with buffered I/O, compression, serialization, etc.

```zig
var file = try fs.File.open("data.txt");
defer file.close();

var reader = file.reader();
// Use with any code that accepts an io.Reader
```

#### `close`

```zig
pub fn close(self: *File) void
```

Close the file handle. Always use `defer file.close()` immediately after opening.

***

## Memory-Mapped Files

### `fs.mmapFile`

```zig
pub fn mmapFile(path: []const u8, options: MmapOptions) !MappedFile
```

Memory-map a file by path. The file is opened, mapped, and the file descriptor is closed -- the mapping survives the close per POSIX. Returns `error.EmptyFile` for zero-length files.

```zig
var mapped = try fs.mmapFile("data.csv", .{});
defer mapped.unmap();

const data = mapped.slice(); // []const u8 backed by kernel page cache
```

### `fs.mmapHandle`

```zig
pub fn mmapHandle(file: *File, options: MmapOptions) !MappedFile
```

Memory-map an open file handle. Does NOT take ownership of the file -- the caller is still responsible for closing it. The mapping remains valid even after the file is closed.

```zig
var file = try fs.File.open("data.parquet");
defer file.close();

var mapped = try fs.mmapHandle(&file, .{});
defer mapped.unmap();
```

### `fs.MmapOptions`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `protection` | `MmapProtection` | `.read_only` | `.read_only` or `.read_write` |
| `sequential` | `bool` | `false` | Hint: sequential access (enables read-ahead) |
| `random` | `bool` | `false` | Hint: random access (disables read-ahead) |
| `populate` | `bool` | `false` | Pre-fault pages eagerly (`MAP_POPULATE` on Linux, `MADV_WILLNEED` on macOS) |

### `fs.MmapProtection`

| Value | MAP flags | Description |
|-------|-----------|-------------|
| `.read_only` | `MAP_PRIVATE`, `PROT_READ` | Pages can be read but not written |
| `.read_write` | `MAP_SHARED`, `PROT_READ \| PROT_WRITE` | Pages can be read and written; changes flush to disk |

### `fs.MappedFile`

A memory-mapped file region. Read-only mappings are safe for concurrent reads from multiple threads. Read-write mappings require external synchronization.

| Method | Signature | Description |
|--------|-----------|-------------|
| `slice` | `fn slice() []const u8` | Immutable view of the mapped region |
| `sliceMut` | `fn sliceMut() ![]u8` | Mutable view (errors on read-only mapping) |
| `sync` | `fn sync() !void` | Flush changes to disk (`msync`) |
| `advise` | `fn advise(offset, len, hint) !void` | Advise kernel about a region |
| `unmap` | `fn unmap() void` | Release the mapping (`munmap`) |

```zig
// Read-write mapping with modification
var mapped = try fs.mmapFile("output.dat", .{ .protection = .read_write });
defer mapped.unmap();

const buf = try mapped.sliceMut();
@memcpy(buf[0..8], "HEADER!!");
try mapped.sync();
```

### `fs.MadviceHint`

Advisory hints for mapped memory regions (passed to `MappedFile.advise`):

| Value | `madvise` flag | Description |
|-------|----------------|-------------|
| `.normal` | `MADV_NORMAL` | Default behavior |
| `.sequential` | `MADV_SEQUENTIAL` | Sequential access -- increase read-ahead |
| `.random` | `MADV_RANDOM` | Random access -- disable read-ahead |
| `.will_need` | `MADV_WILLNEED` | Prefetch these pages into memory |
| `.dont_need` | `MADV_DONTNEED` | Evict these pages from memory |

```zig
var mapped = try fs.mmapFile("large_file.parquet", .{});
defer mapped.unmap();

// Prefetch the row group we need
try mapped.advise(rg_offset, rg_len, .will_need);

// Release pages from the previous row group
try mapped.advise(prev_offset, prev_len, .dont_need);
```

***

## AsyncFile

### `fs.AsyncFile`

An async file handle that integrates with the Volt runtime. On Linux with io\_uring, file operations are truly asynchronous -- submitted directly to the kernel's completion ring. On other platforms (macOS, BSD, Windows), operations run on the blocking thread pool so they don't stall the event loop.

:::tip[When to use AsyncFile vs File]
Use **`File`** for startup/shutdown logic, CLI tools, or anywhere you're not inside the async runtime. Use **`AsyncFile`** inside the runtime when you're handling many concurrent connections and can't afford to block I/O workers with disk reads.
:::

#### Construction

| Method | Description |
|--------|-------------|
| `AsyncFile.open(io, path)` | Open for reading |
| `AsyncFile.create(io, path)` | Create or truncate for writing |
| `AsyncFile.createNew(io, path)` | Create new file, fail if exists |

```zig
var file = try fs.AsyncFile.open(io, "large_dataset.csv");
defer file.close();

var buf: [8192]u8 = undefined;
const n = try file.read(&buf);
```

#### Async Convenience Functions

These are the async equivalents of the top-level convenience functions. They require an `Io` handle.

```zig
// Read entire file without blocking the event loop
const data = try fs.readFileAsync(io, allocator, "big_file.json");
defer allocator.free(data);

// Write without blocking
try fs.writeFileAsync(io, "output.dat", processed_data);

// Append without blocking
try fs.appendFileAsync(io, "events.log", log_line);
```

| Function | Signature | Description |
|----------|-----------|-------------|
| `readFileAsync` | `fn(io, allocator, path) ![]u8` | Read entire file asynchronously |
| `writeFileAsync` | `fn(io, path, data) !void` | Write file asynchronously |
| `appendFileAsync` | `fn(io, path, data) !void` | Append to file asynchronously |

***

## OpenOptions

### `fs.OpenOptions`

Builder pattern for fine-grained control over how files are opened. Every setter returns a new `OpenOptions`, so calls can be chained.

| Field | Default | Description |
|-------|---------|-------------|
| `read` | `false` | Open for reading |
| `write` | `false` | Open for writing |
| `append` | `false` | Writes go to end of file |
| `truncate` | `false` | Truncate file to 0 bytes on open |
| `create` | `false` | Create file if it doesn't exist |
| `create_new` | `false` | Create file, fail if it already exists |
| `mode` | `0o666` | Unix permissions for new files (modified by umask) |
| `custom_flags` | `0` | Platform-specific open flags |

```zig
// Read + write (update in place)
var file = try fs.OpenOptions.new()
    .setRead(true)
    .setWrite(true)
    .open("database.db");
defer file.close();

// Append-only log file
var log = try fs.OpenOptions.new()
    .setWrite(true)
    .setCreate(true)
    .setAppend(true)
    .open("audit.log");
defer log.close();

// Create with restricted permissions (Unix)
var secret = try fs.OpenOptions.new()
    .setWrite(true)
    .setCreateNew(true)
    .setMode(0o600) // owner read/write only
    .open("/etc/myapp/secret.key");
defer secret.close();
```

***

## Directory Operations

### `fs.createDir`

```zig
pub fn createDir(path: []const u8) !void
```

Create a single directory. Fails if the parent doesn't exist or the directory already exists.

### `fs.createDirAll`

```zig
pub fn createDirAll(path: []const u8) !void
```

Create a directory and all missing parent directories. Succeeds silently if the directory already exists.

```zig
// Ensure the full path exists before writing
try fs.createDirAll("data/exports/2026/02");
try fs.writeFile("data/exports/2026/02/report.csv", csv_data);
```

### `fs.createDirMode`

```zig
pub fn createDirMode(path: []const u8, mode: u32) !void
```

Create a directory with specific Unix permissions.

```zig
try fs.createDirMode("/var/run/myapp", 0o755);
```

### `fs.removeDir`

```zig
pub fn removeDir(path: []const u8) !void
```

Remove an empty directory. Fails if the directory is not empty.

### `fs.removeDirAll`

```zig
pub fn removeDirAll(allocator: Allocator, path: []const u8) !void
```

Remove a directory and all its contents recursively. Use with caution.

```zig
// Clean up temp directory after processing
try fs.removeDirAll(allocator, "/tmp/myapp-work");
```

### `fs.removeFile`

```zig
pub fn removeFile(path: []const u8) !void
```

Remove a single file.

### `fs.readDir`

```zig
pub fn readDir(path: []const u8) !ReadDir
```

Open a directory for iteration. Returns a `ReadDir` iterator.

```zig
var iter = try fs.readDir("uploads/");
defer iter.close();

while (try iter.next()) |entry| {
    if (entry.isFile()) {
        std.debug.print("File: {s}\n", .{entry.name});
    }
}
```

### `fs.ReadDir`

Iterator over directory entries. Call `next()` to get the next `DirEntry`, or `null` when done.

| Method | Description |
|--------|-------------|
| `next() !?DirEntry` | Get next entry, or `null` at end |
| `close()` | Close the directory handle |

### `fs.DirEntry`

A single entry from a directory listing.

| Field/Method | Description |
|--------------|-------------|
| `name` | Entry name (filename only, not full path) |
| `isFile()` | Returns `true` if this is a regular file |
| `isDir()` | Returns `true` if this is a directory |
| `isSymlink()` | Returns `true` if this is a symbolic link |

### `fs.DirBuilder`

Builder for creating directories with options.

```zig
var builder = fs.DirBuilder.new();
builder.setRecursive(true);
builder.setMode(0o750);
try builder.create("path/to/dir");
```

***

## Metadata

### `fs.metadata`

```zig
pub fn metadata(path: []const u8) !Metadata
```

Get metadata for a file or directory. Follows symlinks.

```zig
const meta = try fs.metadata("data.db");
std.debug.print("Size: {} bytes\n", .{meta.size()});
std.debug.print("Type: {s}\n", .{if (meta.isFile()) "file" else "directory"});
```

### `fs.symlinkMetadata`

```zig
pub fn symlinkMetadata(path: []const u8) !Metadata
```

Get metadata for a path without following symlinks. If the path is a symlink, returns metadata about the symlink itself.

### `fs.Metadata`

| Method | Return Type | Description |
|--------|-------------|-------------|
| `fileType()` | `FileType` | File, directory, symlink, etc. |
| `isFile()` | `bool` | Is a regular file? |
| `isDir()` | `bool` | Is a directory? |
| `isSymlink()` | `bool` | Is a symbolic link? |
| `size()` | `u64` | Size in bytes |
| `permissions()` | `Permissions` | File permissions |
| `modified()` | `i64` | Last modified (Unix epoch seconds) |
| `accessed()` | `i64` | Last accessed (Unix epoch seconds) |
| `created()` | `?i64` | Creation time (platform-dependent, may be `null`) |
| `modifiedTime()` | `SystemTime` | Last modified as `SystemTime` |
| `accessedTime()` | `SystemTime` | Last accessed as `SystemTime` |

```zig
const meta = try fs.metadata("report.pdf");

// Check age
const mtime = meta.modifiedTime();
if (mtime.elapsed().asSecs() > 86400) {
    std.debug.print("File is more than a day old\n", .{});
}

// Check size
if (meta.size() > 100 * 1024 * 1024) {
    std.debug.print("Warning: file is over 100 MB\n", .{});
}
```

### `fs.Permissions`

File permission bits. Provides methods for querying Unix permission modes.

```zig
const meta = try fs.metadata("script.sh");
const perms = meta.permissions();

if (!perms.isExecutable()) {
    try fs.setPermissions("script.sh", perms.withExecutable(true));
}
```

### `fs.setPermissions`

```zig
pub fn setPermissions(path: []const u8, perms: Permissions) !void
```

Set permissions on a file or directory.

### `fs.FileType`

Enum representing the type of a filesystem entry:

| Variant | Description |
|---------|-------------|
| `.file` | Regular file |
| `.directory` | Directory |
| `.sym_link` | Symbolic link |
| `.block_device` | Block device |
| `.character_device` | Character device |
| `.named_pipe` | Named pipe (FIFO) |
| `.unix_domain_socket` | Unix domain socket |
| `.unknown` | Unknown type |

### `fs.SystemTime`

A point in time from the filesystem. Provides ergonomic methods for working with timestamps.

| Method | Description |
|--------|-------------|
| `elapsed()` | Duration since this time (returns `Duration`) |
| `durationSince(other)` | Duration between two system times |

***

## Link Operations

### `fs.hardLink`

```zig
pub fn hardLink(src: []const u8, dst: []const u8) !void
```

Create a hard link. Both paths will point to the same underlying data.

### `fs.symlink`

```zig
pub fn symlink(target: []const u8, link_path: []const u8) !void
```

Create a symbolic link at `link_path` pointing to `target`.

```zig
try fs.symlink("config.production.toml", "config.toml");
```

### `fs.readLink`

```zig
pub fn readLink(allocator: Allocator, path: []const u8) ![]u8
```

Read the target of a symbolic link. Caller owns the returned path.

```zig
const target = try fs.readLink(allocator, "config.toml");
defer allocator.free(target);
std.debug.print("config.toml -> {s}\n", .{target});
```

***

## Path Operations

### `fs.canonicalize`

```zig
pub fn canonicalize(allocator: Allocator, path: []const u8) ![]u8
```

Resolve a path to its canonical, absolute form. Resolves all symlinks and removes `.` and `..` components.

```zig
const abs = try fs.canonicalize(allocator, "../data/input.csv");
defer allocator.free(abs);
// e.g., "/home/user/project/data/input.csv"
```

***

## Common Patterns

### Atomic File Write

Write to a temporary file, then atomically rename it into place. This prevents readers from ever seeing a partially-written file:

```zig
const fs = @import("volt").fs;

fn atomicWrite(path: []const u8, data: []const u8) !void {
    const tmp_path = path ++ ".tmp";
    try fs.writeFile(tmp_path, data);
    try fs.rename(tmp_path, path);
}
```

### Config File Loading with Fallback

```zig
const fs = @import("volt").fs;

fn loadConfig(allocator: std.mem.Allocator) ![]u8 {
    // Try local override first, then fall back to default
    const path = if (fs.tryExists("config.local.toml"))
        "config.local.toml"
    else if (fs.tryExists("config.toml"))
        "config.toml"
    else
        return error.NoConfigFile;

    return fs.readFile(allocator, path);
}
```

### Log Rotation

```zig
const fs = @import("volt").fs;

fn rotateLogIfNeeded(log_path: []const u8, max_size: u64) !void {
    const meta = fs.metadata(log_path) catch return; // No file = nothing to rotate
    if (meta.size() < max_size) return;

    // Rotate: current -> .1, .1 -> .2, etc.
    fs.rename(log_path ++ ".1", log_path ++ ".2") catch {};
    try fs.rename(log_path, log_path ++ ".1");
}
```

### Directory Walk

Process all files in a directory tree:

```zig
const std = @import("std");
const fs = @import("volt").fs;

fn processDirectory(allocator: std.mem.Allocator, path: []const u8) !void {
    var iter = try fs.readDir(path);
    defer iter.close();

    while (try iter.next()) |entry| {
        if (entry.isDir()) {
            // Recurse into subdirectories
            const subpath = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ path, entry.name });
            defer allocator.free(subpath);
            try processDirectory(allocator, subpath);
        } else if (entry.isFile()) {
            std.debug.print("Processing: {s}/{s}\n", .{ path, entry.name });
        }
    }
}
```

### High-Concurrency File Processing

Use `AsyncFile` inside the runtime to process many files without blocking I/O workers:

```zig
const volt = @import("volt");
const fs = volt.fs;

fn processFiles(io: volt.Io, paths: []const []const u8) void {
    for (paths) |path| {
        const f = io.@"async"(struct {
            fn run(io_inner: volt.Io, p: []const u8) void {
                // AsyncFile uses io_uring on Linux, blocking pool elsewhere
                const data = fs.readFileAsync(io_inner, std.heap.page_allocator, p) catch return;
                defer std.heap.page_allocator.free(data);

                // Process the data...
            }
        }.run, .{ io, path });
        f.detach(); // fire-and-forget: each file processes independently
    }
}
```
