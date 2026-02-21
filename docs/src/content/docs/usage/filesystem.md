---
title: Filesystem
description: Reading, writing, and managing files and directories with Volt's filesystem module.
---

Volt provides synchronous filesystem operations that work **without a runtime**, plus async variants for use inside the runtime. All APIs are under `volt.fs`.

:::note[No runtime needed for sync I/O]
`volt.fs.readFile()`, `File.open()`, `readDir()`, and all other synchronous operations work without starting a runtime. They are thin wrappers around OS calls. For non-blocking I/O inside the runtime, wrap them in `io.concurrent()` or use `AsyncFile`.
:::

## Reading and Writing Files

### One-shot convenience functions

```zig
const volt = @import("volt");

// Read an entire file into an allocated buffer
const data = try volt.fs.readFile(allocator, "config.json");
defer allocator.free(data);

// Read as validated UTF-8 string
const text = try volt.fs.readFileString(allocator, "README.md");
defer allocator.free(text);

// Write a file (creates or truncates)
try volt.fs.writeFile("output.txt", "Hello, Volt!");

// Append to a file (creates if needed)
try volt.fs.appendFile("log.txt", "new entry\n");
```

### File handle

For more control, open a `File` handle:

```zig
// Open for reading
var file = try volt.fs.File.open("data.bin");
defer file.close();

var buf: [4096]u8 = undefined;
const n = try file.read(&buf);
// buf[0..n] contains the data

// Positioned read (doesn't change file position)
const m = try file.pread(&buf, 1024); // read from offset 1024

// Create for writing
var out = try volt.fs.File.create("output.bin");
defer out.close();

try out.writeAll("binary data here");
try out.syncAll(); // flush to disk
```

### OpenOptions builder

```zig
var file = try volt.fs.File.getOptions()
    .setRead(true)
    .setWrite(true)
    .setCreate(true)
    .setTruncate(false)
    .open("state.db");
defer file.close();
```

### Seeking

```zig
var file = try volt.fs.File.open("data.bin");
defer file.close();

try file.seekTo(100);         // absolute position
try file.seekBy(-10);         // relative offset
try file.rewind();            // back to start
const pos = try file.getPos(); // current position
```

## Directory Operations

### Reading directories

```zig
var dir = try volt.fs.readDir("/tmp");
defer dir.close();

while (try dir.next()) |entry| {
    if (entry.isFile()) {
        // process file
    } else if (entry.isDir()) {
        // process subdirectory
    }
}
```

### Creating and removing directories

```zig
// Create a single directory
try volt.fs.createDir("output");

// Create nested directories (like mkdir -p)
try volt.fs.createDirAll("output/reports/2024");

// Remove empty directory
try volt.fs.removeDir("output/old");

// Remove directory and all contents recursively
try volt.fs.removeDirAll(allocator, "output/temp");

// Remove a file
try volt.fs.removeFile("output/stale.txt");
```

## File Metadata

```zig
const meta = try volt.fs.metadata("config.json");

const size = meta.size();
const file_type = meta.fileType();
const modified = meta.modified();
const perms = meta.permissions();

// Check existence
if (volt.fs.exists("config.json")) {
    // file exists
}
```

## File Copy and Move

```zig
// Copy a file (returns bytes copied)
const bytes = try volt.fs.copy("source.txt", "dest.txt");

// Rename/move a file or directory
try volt.fs.rename("old_name.txt", "new_name.txt");

// Symbolic links
try volt.fs.symlink("target.txt", "link.txt");
const target = try volt.fs.readLink(allocator, "link.txt");
defer allocator.free(target);

// Hard links
try volt.fs.hardLink("original.txt", "hardlink.txt");
```

## Async File I/O

Inside the runtime, use `AsyncFile` for non-blocking operations (uses io_uring on Linux, blocking pool elsewhere):

```zig
fn processFiles(io: volt.Io) !void {
    // Async read
    const data = try volt.fs.readFileAsync(io.runtime, allocator, "large_file.bin");
    defer allocator.free(data);

    // Async write
    try volt.fs.writeFileAsync(io.runtime, "output.bin", data);
}
```

Or wrap synchronous operations with `io.concurrent()` to avoid blocking worker threads:

```zig
fn readConfig(io: volt.Io) ![]const u8 {
    var f = try io.concurrent(struct {
        fn run() ![]const u8 {
            return volt.fs.readFile(std.heap.page_allocator, "config.json");
        }
    }.run, .{});
    return try f.@"await"(io);
}
```

## API Summary

| Function | Runtime needed? | Notes |
|----------|:-:|-------|
| `readFile(allocator, path)` | No | Read entire file |
| `writeFile(path, data)` | No | Create/truncate and write |
| `appendFile(path, data)` | No | Append to file |
| `File.open(path)` | No | Open file handle |
| `readDir(path)` | No | Iterate directory entries |
| `createDirAll(path)` | No | Create nested directories |
| `copy(src, dst)` | No | Copy file |
| `rename(old, new)` | No | Move/rename |
| `metadata(path)` | No | Get file metadata |
| `exists(path)` | No | Check existence |
| `readFileAsync(rt, alloc, path)` | **Yes** | Async read (io_uring or pool) |
| `writeFileAsync(rt, path, data)` | **Yes** | Async write |
| `AsyncFile.open(rt, path)` | **Yes** | Async file handle |
