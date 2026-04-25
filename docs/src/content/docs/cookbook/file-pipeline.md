---
title: File Processing Pipeline
description: Scanning directories, line-by-line stream processing with BufReader and Lines, and parallelizing file I/O with io.concurrent.
---

File processing is one of the most common real-world tasks: scan a directory, read each file, parse or transform its contents, and write a summary. This recipe builds a log analyzer that demonstrates the filesystem and stream APIs -- `readDir`, `File`, `BufReader`, `Lines`, and `io.concurrent` for parallel file reads.

:::tip[What you will learn]
- **Directory iteration** -- using `readDir` to discover files
- **Buffered line-by-line reading** -- `BufReader` and `Lines` for memory-efficient processing
- **Blocking pool for file I/O** -- keeping disk reads off the event loop with `io.concurrent`
- **Atomic writes** -- write to a temp file, then rename for crash safety
- **File metadata** -- filtering by size and modification time
:::

## Complete Example

This log analyzer scans a directory of `.log` files, counts lines matching `ERROR`, `WARN`, and `INFO`, and writes a summary report.

```zig
const std = @import("std");
const volt = @import("volt");

// -- Report data --------------------------------------------------------------

const FileReport = struct {
    filename: [256]u8,
    filename_len: usize,
    total_lines: u64,
    error_count: u64,
    warn_count: u64,
    info_count: u64,
    size_bytes: u64,

    pub fn name(self: *const FileReport) []const u8 {
        return self.filename[0..self.filename_len];
    }
};

// -- Entry point --------------------------------------------------------------

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const log_dir = "logs";
    const report_path = "log_report.txt";

    // Phase 1: Discover log files.
    std.debug.print("Scanning {s}/ for log files...\n", .{log_dir});
    var files = discoverLogFiles(log_dir) orelse {
        std.debug.print("No log files found in {s}/\n", .{log_dir});
        return;
    };

    std.debug.print("Found {d} log files.\n\n", .{files.count});

    // Phase 2: Analyze each file. In a real application you would use
    // io.concurrent to parallelize this across the blocking pool.
    var reports_buf: [128]FileReport = undefined;
    var report_count: usize = 0;

    for (files.entries[0..files.count]) |entry| {
        if (report_count >= reports_buf.len) break;

        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{
            log_dir, entry.name(),
        }) catch continue;

        if (analyzeFile(path, entry.name())) |report| {
            reports_buf[report_count] = report;
            report_count += 1;
        }
    }

    // Phase 3: Write the summary report atomically.
    writeReport(allocator, report_path, reports_buf[0..report_count]);
    std.debug.print("Report written to {s}\n", .{report_path});
}

// -- Phase 1: Directory scanning ----------------------------------------------

const FileList = struct {
    entries: [128]FileEntry,
    count: usize,
};

const FileEntry = struct {
    name_buf: [256]u8,
    name_len: usize,
    size: u64,

    pub fn name(self: *const FileEntry) []const u8 {
        return self.name_buf[0..self.name_len];
    }
};

fn discoverLogFiles(dir_path: []const u8) ?FileList {
    var dir = volt.fs.readDir(dir_path) catch return null;
    defer dir.close();

    var list: FileList = undefined;
    list.count = 0;

    while (dir.next() catch null) |entry| {
        // Only process .log files.
        const entry_name = entry.name();
        if (!std.mem.endsWith(u8, entry_name, ".log")) continue;

        if (list.count >= list.entries.len) break;

        var file_entry: FileEntry = undefined;
        const len = @min(entry_name.len, file_entry.name_buf.len);
        @memcpy(file_entry.name_buf[0..len], entry_name[0..len]);
        file_entry.name_len = len;
        file_entry.size = entry.size() catch 0;

        list.entries[list.count] = file_entry;
        list.count += 1;
    }

    if (list.count == 0) return null;
    return list;
}

// -- Phase 2: Line-by-line analysis -------------------------------------------

fn analyzeFile(path: []const u8, filename: []const u8) ?FileReport {
    var file = volt.fs.File.open(path) catch return null;
    defer file.close();

    // Wrap the file in a BufReader for efficient buffered reads.
    // BufReader reads in 8 KiB chunks internally, reducing the number
    // of syscalls compared to reading one byte at a time.
    var buf_reader = volt.stream.bufReader(file.reader());

    // Lines gives us an iterator that yields one line at a time,
    // stripping the trailing newline. This is the most memory-efficient
    // way to process a file -- only one line is in memory at a time.
    var line_buf: [4096]u8 = undefined;
    var lines_iter = volt.stream.linesWithBuffer(&buf_reader, &line_buf);

    var report: FileReport = undefined;
    const name_len = @min(filename.len, report.filename.len);
    @memcpy(report.filename[0..name_len], filename[0..name_len]);
    report.filename_len = name_len;
    report.total_lines = 0;
    report.error_count = 0;
    report.warn_count = 0;
    report.info_count = 0;
    report.size_bytes = 0;

    while (lines_iter.next() catch null) |line| {
        report.total_lines += 1;

        // Simple pattern matching: check if the line contains a log level.
        // A production analyzer would use structured log parsing.
        if (std.mem.indexOf(u8, line, "ERROR") != null) {
            report.error_count += 1;
        } else if (std.mem.indexOf(u8, line, "WARN") != null) {
            report.warn_count += 1;
        } else if (std.mem.indexOf(u8, line, "INFO") != null) {
            report.info_count += 1;
        }
    }

    return report;
}

// -- Phase 3: Atomic report writing -------------------------------------------

fn writeReport(
    allocator: std.mem.Allocator,
    output_path: []const u8,
    reports: []const FileReport,
) void {
    _ = allocator;

    // Write to a temporary file first, then rename. This ensures the
    // output file is never in a partial state -- either it has the old
    // content or the complete new content.
    const tmp_path = "log_report.tmp";

    var tmp = volt.fs.File.create(tmp_path) catch return;

    // Write the header.
    tmp.writeAll("=== Log Analysis Report ===\n\n") catch {
        tmp.close();
        return;
    };

    // Write per-file summaries.
    var total_errors: u64 = 0;
    var total_warns: u64 = 0;
    var total_lines: u64 = 0;

    for (reports) |r| {
        var line_buf: [512]u8 = undefined;
        const line = std.fmt.bufPrint(&line_buf,
            "{s}: {d} lines, {d} errors, {d} warnings, {d} info\n",
            .{ r.name(), r.total_lines, r.error_count, r.warn_count, r.info_count },
        ) catch continue;

        tmp.writeAll(line) catch continue;

        total_errors += r.error_count;
        total_warns += r.warn_count;
        total_lines += r.total_lines;
    }

    // Write the totals.
    var summary_buf: [256]u8 = undefined;
    const summary = std.fmt.bufPrint(&summary_buf,
        "\n--- Totals ---\nFiles: {d}\nLines: {d}\nErrors: {d}\nWarnings: {d}\n",
        .{ reports.len, total_lines, total_errors, total_warns },
    ) catch "";

    tmp.writeAll(summary) catch {};

    // Flush and close before renaming.
    tmp.sync() catch {};
    tmp.close();

    // Atomic rename: replaces the old report in a single filesystem operation.
    volt.fs.rename(tmp_path, output_path) catch {
        std.debug.print("Warning: rename failed, report left as {s}\n", .{tmp_path});
    };
}
```

## Walkthrough

### Directory iteration with readDir

`volt.fs.readDir` returns an iterator over directory entries. Each entry exposes:

- `name()` -- the file or directory name (not the full path)
- `size()` -- file size in bytes
- `isDir()` -- whether the entry is a directory

The iterator must be closed with `defer dir.close()` to release the underlying file descriptor. Failing to close it leaks a descriptor, which matters in long-running servers.

We filter for `.log` files using `std.mem.endsWith`. For more complex patterns (e.g., `app-*.log`), you could use glob matching or regex.

### Buffered reading with BufReader

Raw `file.read()` calls issue one syscall per invocation. For line-by-line processing where each "read" returns just a few bytes until a newline, this creates excessive syscall overhead. `BufReader` solves this by reading large chunks (typically 8 KiB) into an internal buffer and serving small reads from that buffer.

The performance difference is dramatic: reading a 10 MB file one byte at a time requires ~10 million syscalls. With BufReader, it takes ~1,250 (10 MB / 8 KiB).

### Lines iterator

`volt.stream.linesWithBuffer` wraps a `BufReader` and yields one line at a time, stripping the trailing `\n` (or `\r\n` on Windows). The caller provides a fixed buffer for the current line, so there is no allocation per line.

If a line exceeds the buffer size, it is truncated. For log files this is rarely an issue; for files with unbounded line lengths, use `readToEnd` with an allocator instead.

### Atomic writes

Writing directly to the output file risks leaving it in a corrupted state if the process crashes mid-write. The write-to-temp-then-rename pattern avoids this:

1. Write to `report.tmp`
2. Call `sync()` to flush to disk
3. `rename("report.tmp", "report.txt")` -- atomic on all major filesystems

After the rename, readers see either the old file or the complete new file, never a partial write.

### Why io.concurrent for file I/O?

Disk reads can block for milliseconds (or longer on spinning disks or networked filesystems). Running them on an I/O worker would stall all async tasks on that worker. The blocking pool has dedicated threads for exactly this purpose. See [Work Offloading](/cookbook/work-offloading) for details.

## Variations

### Streaming CSV processor

Read a CSV file line by line, transform each row, and write the output. This pattern works for files of any size because only one line is in memory at a time.

```zig
const std = @import("std");
const volt = @import("volt");

fn processCsv(input_path: []const u8, output_path: []const u8) !void {
    var input = try volt.fs.File.open(input_path);
    defer input.close();

    var output = try volt.fs.File.create(output_path);
    defer output.close();

    var buf_reader = volt.stream.bufReader(input.reader());
    var line_buf: [4096]u8 = undefined;
    var lines_iter = volt.stream.linesWithBuffer(&buf_reader, &line_buf);
    var line_num: u64 = 0;

    while (lines_iter.next() catch null) |line| {
        line_num += 1;

        // Skip the header row.
        if (line_num == 1) {
            try output.writeAll("id,name,score,grade\n");
            continue;
        }

        // Parse CSV fields (simple split, no quoting support).
        var fields = std.mem.splitScalar(u8, line, ',');
        const id = fields.next() orelse continue;
        const name = fields.next() orelse continue;
        const score_str = fields.next() orelse continue;

        // Transform: compute a letter grade from the numeric score.
        const score = std.fmt.parseInt(u32, score_str, 10) catch continue;
        const grade: []const u8 = if (score >= 90) "A"
            else if (score >= 80) "B"
            else if (score >= 70) "C"
            else "F";

        var out_buf: [512]u8 = undefined;
        const out_line = std.fmt.bufPrint(&out_buf, "{s},{s},{s},{s}\n", .{
            id, name, score_str, grade,
        }) catch continue;

        try output.writeAll(out_line);
    }
}
```

### File watcher

Poll a directory for new or modified files by tracking modification timestamps. Process any file whose timestamp is newer than the last scan.

```zig
const std = @import("std");
const volt = @import("volt");

const WatchState = struct {
    last_scan: i128, // nanosecond timestamp of last scan
};

fn watchDirectory(dir_path: []const u8) void {
    var state = WatchState{ .last_scan = std.time.nanoTimestamp() };

    while (true) {
        std.Thread.sleep(2 * std.time.ns_per_s); // Poll every 2 seconds.

        var dir = volt.fs.readDir(dir_path) catch continue;
        defer dir.close();

        const scan_time = std.time.nanoTimestamp();

        while (dir.next() catch null) |entry| {
            const entry_name = entry.name();
            if (!std.mem.endsWith(u8, entry_name, ".log")) continue;

            // Check if this file was modified since our last scan.
            const mtime = entry.modificationTime() catch continue;
            if (mtime <= state.last_scan) continue;

            std.debug.print("New/modified: {s}\n", .{entry_name});

            // Process the file (analyze, copy, etc.)
            var path_buf: [512]u8 = undefined;
            const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{
                dir_path, entry_name,
            }) catch continue;

            if (analyzeFile(path, entry_name)) |report| {
                std.debug.print("  {d} errors, {d} warnings\n", .{
                    report.error_count, report.warn_count,
                });
            }
        }

        state.last_scan = scan_time;
    }
}
```

This is a polling-based watcher. For production use, consider platform-specific APIs (inotify on Linux, FSEvents on macOS) which deliver notifications without polling.

### Parallel file processing with io.concurrent

When you have many files to process, use `io.concurrent` to read them concurrently on the blocking pool. A semaphore limits the number of concurrent file reads to avoid exhausting file descriptors.

```zig
const std = @import("std");
const volt = @import("volt");

const MAX_CONCURRENT_READS = 8;

fn processFilesParallel(
    io: volt.Runtime,
    allocator: std.mem.Allocator,
    paths: []const []const u8,
) !void {
    // Semaphore gates concurrent file reads to MAX_CONCURRENT_READS.
    var sem = volt.sync.Semaphore.init(MAX_CONCURRENT_READS);

    var result_ch = try volt.channel.bounded(FileReport, allocator, 16);
    defer result_ch.deinit();

    // Spawn a blocking task for each file.
    for (paths) |path| {
        // Acquire a permit before starting work. If all permits are
        // taken, this blocks until one is released.
        while (!sem.tryAcquire(1)) {
            std.Thread.sleep(1 * std.time.ns_per_ms);
        }

        var f = io.@"async"(struct {
            fn run(
                p: []const u8,
                s: *volt.sync.Semaphore,
                ch: *volt.channel.Channel(FileReport),
            ) void {
                defer s.release(1);

                const filename = std.fs.path.basename(p);
                if (analyzeFile(p, filename)) |report| {
                    while (true) {
                        switch (ch.trySend(report)) {
                            .ok => break,
                            .full => std.Thread.sleep(1 * std.time.ns_per_ms),
                            .closed => return,
                        }
                    }
                }
            }
        }.run, .{ path, &sem, &result_ch }) catch {
            sem.release(1);
            continue;
        };
        f.detach();
    }

    // Collect results.
    result_ch.close();
    while (true) {
        switch (result_ch.tryRecv()) {
            .value => |report| {
                std.debug.print("{s}: {d} errors\n", .{
                    report.name(), report.error_count,
                });
            },
            .empty => {
                if (result_ch.isClosed()) break;
                std.Thread.sleep(1 * std.time.ns_per_ms);
            },
            .closed => break,
        }
    }
}
```

The semaphore pattern here is the same one used in [Connection Pool](/cookbook/connection-pool) -- a bounded resource (file descriptors) is protected by a counting semaphore.
