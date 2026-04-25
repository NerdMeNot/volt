---
title: Work Offloading
description: Offloading CPU-bound work to the blocking pool and Blitz, mixing
  I/O and compute without starving the event loop.
slug: v1.0.0-zig0.15.2/v110/cookbook/work-offloading
---

:::tip[What you'll learn]
* The difference between I/O workers and the blocking pool, and when each is used
* How to use `io.concurrent` to run CPU-bound functions without starving the event loop
* How to build a channel-based pipeline that separates I/O stages from compute stages
* How to size the blocking pool for your workload
:::

The Volt runtime has two pools of threads with different purposes:

| Pool | Purpose | Scheduling |
|------|---------|------------|
| **I/O workers** | Polling sockets, driving timers, running async tasks | Work-stealing, cooperative |
| **Blocking pool** | CPU-intensive or truly blocking operations | On-demand threads, up to 512 |

CPU-intensive work (hashing, compression, parsing large payloads) should run on the blocking pool so it does not starve I/O tasks of CPU time. The key threshold: if a function takes more than about 1 millisecond of CPU time without yielding, it belongs on the blocking pool.

## Pattern 1: io.concurrent for CPU Work

Use `io.concurrent` to move a function onto the blocking pool. The calling task yields until the result is ready, so other I/O work continues uninterrupted.

This example reads an upload from a TCP connection, computes its SHA-256 hash on the blocking pool, and sends the hex digest back to the client:

```zig
const std = @import("std");
const volt = @import("volt");

fn handleUpload(stream: volt.net.TcpStream) void {
    var conn = stream;
    defer conn.close();

    // Stage 1: Read the upload on an I/O worker.
    // This is non-blocking -- the worker can service other connections
    // between reads.
    var buf: [1 << 20]u8 = undefined; // 1 MiB receive buffer
    var total: usize = 0;
    while (total < buf.len) {
        const n = conn.tryRead(buf[total..]) catch return orelse {
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        };
        if (n == 0) break; // client closed connection
        total += n;
    }
    const payload = buf[0..total];

    // Stage 2: Hash the payload on the blocking pool.
    // SHA-256 over 1 MiB takes several milliseconds of uninterrupted CPU
    // time. Running it here keeps the I/O worker free for other tasks.
    const digest = computeSha256Blocking(payload);

    // Stage 3: Send the hex digest back on the I/O worker.
    var hex_buf: [64]u8 = undefined;
    const hex = std.fmt.bufPrint(&hex_buf, "{s}", .{
        std.fmt.fmtSliceHexLower(&digest),
    }) catch return;
    _ = conn.writeAll(hex) catch return;
}

fn computeSha256Blocking(data: []const u8) [32]u8 {
    // std.crypto.hash.sha2.Sha256 is CPU-bound with no yield points.
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    // Process in 8 KiB chunks to keep cache hot.
    var offset: usize = 0;
    while (offset < data.len) {
        const end = @min(offset + 8192, data.len);
        hasher.update(data[offset..end]);
        offset = end;
    }
    return hasher.finalResult();
}
```

To set up the runtime and dispatch the blocking call explicitly:

```zig
var io = try volt.Io.init(allocator, .{
    .num_workers = 4,            // I/O workers (usually = CPU count)
    .max_blocking_threads = 64,  // blocking pool ceiling
});
defer io.deinit();

// io.concurrent moves computeSha256Blocking onto the blocking pool
// and returns a future you can await.
var f = try io.concurrent(computeSha256Blocking, .{payload});
const digest = try f.@"await"(io);
```

:::note
`io.concurrent` creates a real OS thread (if one is not already idle in the pool). Avoid spawning thousands of blocking tasks simultaneously -- use a bounded channel to limit concurrency instead.
:::

## Pattern 2: Mixing I/O and Compute in a Pipeline

When your application has a steady flow of data that must be read, transformed, and written, a three-stage pipeline is a natural fit. Each stage runs independently and communicates through bounded channels. The bounded capacity provides backpressure: if the compute stage falls behind, the reader pauses automatically.

```
  [Network Read]  -->  raw_ch  -->  [CPU Transform]  -->  result_ch  -->  [Network Write]
   (I/O worker)      (bounded)     (blocking pool)       (bounded)       (I/O worker)
```

```zig
const std = @import("std");
const volt = @import("volt");

const RawRecord = struct {
    buf: [4096]u8,
    len: usize,

    pub fn data(self: *const RawRecord) []const u8 {
        return self.buf[0..self.len];
    }
};

const ParsedRecord = struct {
    checksum: u64,
    field_count: u32,
    source_len: usize,
};

fn pipeline(allocator: std.mem.Allocator) !void {
    // Bounded channels between stages. Capacity of 16 keeps memory bounded
    // while allowing the reader to stay ahead of the parser.
    var raw_ch = try volt.channel.bounded(RawRecord, allocator, 16);
    defer raw_ch.deinit();

    var result_ch = try volt.channel.bounded(ParsedRecord, allocator, 16);
    defer result_ch.deinit();

    // Stage 1 -- "ingest": reads records from the network (I/O-bound).
    const ingest = try std.Thread.spawn(.{}, stageIngest, .{&raw_ch});

    // Stage 2 -- "transform": parses and checksums each record (CPU-bound).
    // Runs on the blocking pool so it does not block I/O workers.
    const transform = try std.Thread.spawn(.{}, stageTransform, .{
        &raw_ch,
        &result_ch,
    });

    // Stage 3 -- "persist": writes results to a downstream socket (I/O-bound).
    const persist = try std.Thread.spawn(.{}, stagePersist, .{&result_ch});

    // Tear down in order: close each channel after its producer finishes.
    ingest.join();
    raw_ch.close();

    transform.join();
    result_ch.close();

    persist.join();
}

/// Stage 1: reads raw records from a network source and pushes them
/// into raw_ch. Runs on an I/O worker.
fn stageIngest(raw_ch: *volt.channel.Channel(RawRecord)) void {
    for (0..1000) |i| {
        var record: RawRecord = undefined;
        // Simulate a network read that fills the buffer.
        const payload = std.fmt.bufPrint(
            record.buf[0..],
            "record-{d}:field1,field2,field3,field4",
            .{i},
        ) catch return;
        record.len = payload.len;

        while (true) {
            switch (raw_ch.trySend(record)) {
                .ok => break,
                .full => std.Thread.sleep(1 * std.time.ns_per_ms),
                .closed => return,
            }
        }
    }
}

/// Stage 2: pulls raw records, parses them, computes a checksum,
/// and pushes the result downstream. CPU-bound -- runs on the blocking pool.
fn stageTransform(
    raw_ch: *volt.channel.Channel(RawRecord),
    out_ch: *volt.channel.Channel(ParsedRecord),
) void {
    while (true) {
        switch (raw_ch.tryRecv()) {
            .value => |raw| {
                // CPU work: parse fields and compute a rolling checksum.
                const payload = raw.buf[0..raw.len];
                var field_count: u32 = 1;
                var checksum: u64 = 0;
                for (payload) |byte| {
                    if (byte == ',') field_count += 1;
                    checksum = checksum *% 31 +% byte;
                }

                const result = ParsedRecord{
                    .checksum = checksum,
                    .field_count = field_count,
                    .source_len = raw.len,
                };

                while (true) {
                    switch (out_ch.trySend(result)) {
                        .ok => break,
                        .full => std.Thread.sleep(1 * std.time.ns_per_ms),
                        .closed => return,
                    }
                }
            },
            .empty => {
                if (raw_ch.isClosed()) return;
                std.Thread.sleep(1 * std.time.ns_per_ms);
            },
            .closed => return,
        }
    }
}

/// Stage 3: pulls parsed results and writes them to a downstream destination.
/// I/O-bound -- runs on an I/O worker.
fn stagePersist(result_ch: *volt.channel.Channel(ParsedRecord)) void {
    var records_written: u64 = 0;
    while (true) {
        switch (result_ch.tryRecv()) {
            .value => |item| {
                // Write result to a socket, file, database, etc.
                _ = item;
                records_written += 1;
            },
            .empty => {
                if (result_ch.isClosed()) return;
                std.Thread.sleep(1 * std.time.ns_per_ms);
            },
            .closed => return,
        }
    }
    _ = records_written;
}
```

The key property of this pipeline is that each stage can be scaled independently. If the transform stage is the bottleneck, you can spawn multiple transform threads that all read from `raw_ch` and write to `result_ch` -- the bounded channel handles synchronization.

## Blitz Integration

:::caution[Planned / Future API]
The Blitz integration described in this section is **not yet available**. It represents the planned API for bridging Volt's async I/O runtime with [Blitz](https://github.com/srini-code/blitz), a CPU parallelism library for Zig (analogous to how Tokio complements Rayon in the Rust ecosystem). **Use `io.concurrent` for all CPU-bound work today.**
:::

**Available now:**

* `io.concurrent` offloads CPU work to the blocking thread pool. This is the recommended approach for all CPU-bound work today.
* Channel-based pipelines let you structure I/O and compute as separate stages.

**Planned (not yet available):**

* `io.blitz.computeOnBlitz(func, args)` -- submit a function to Blitz's work-stealing thread pool and suspend the calling I/O task until the result is ready. Under the hood, a oneshot channel delivers the result back.
* `io.blitz.parallelMap(items, func)` -- distribute a slice across Blitz workers for parallel processing, then return results to the I/O task.

```zig
// Planned API -- not yet implemented.
// I/O stays on Volt workers, CPU goes to Blitz workers.

// Single function offload:
const digest = try io.blitz.computeOnBlitz(computeSha256, .{data});

// Parallel map over chunks:
const hashes = try io.blitz.parallelMap([]const u8, [32]u8, chunks, computeSha256);
```

The integration requires a bridge that manages task suspension on the Volt side and job injection on the Blitz side. Until the bridge ships, `io.concurrent` is the correct way to handle CPU work.

## Thread Pool Sizing

| Parameter | Default | Guidance |
|-----------|---------|----------|
| `num_workers` | CPU count | I/O-bound work; rarely needs tuning |
| `max_blocking_threads` | 512 | Cap based on memory. Each thread uses ~8 MiB stack. |
| `blocking_keep_alive_ns` | 10s | Idle threads exit after this. Lower for bursty workloads. |

For a server that processes uploads (I/O) and compresses them (CPU):

```zig
var io = try volt.Io.init(allocator, .{
    .num_workers = 4,              // 4 I/O workers
    .max_blocking_threads = 16,    // 16 CPU threads (match core count)
    .blocking_keep_alive_ns = 5 * std.time.ns_per_s, // shorter idle timeout
});
```

Setting `max_blocking_threads` equal to your physical core count is a reasonable starting point for CPU-bound offloading. If your blocking work is itself I/O-bound (e.g., calling a blocking C library that waits on a socket), a higher value is appropriate.

## When to Offload

| Operation | Where | Why |
|-----------|-------|-----|
| Network I/O | I/O workers | Non-blocking, event-driven |
| File read/write | Blocking pool | May block on disk |
| JSON/CSV parsing | Blocking pool | CPU-bound |
| Compression (zstd, gzip) | Blocking pool | CPU-bound |
| Hashing (SHA-256, etc.) | Blocking pool | CPU-bound |
| DNS resolution | Blocking pool | Uses blocking `getaddrinfo` |
| Database queries | I/O workers | Network I/O (non-blocking driver) |
| Database queries | Blocking pool | Blocking driver (e.g., C library) |

The rule of thumb: if the operation takes more than 1 millisecond of CPU time without yielding, move it to the blocking pool.

## Try It Yourself

### Exercise 1: Compression Pipeline

Build a pipeline that reads files from disk, compresses them on the blocking pool, and writes the compressed output. This combines I/O and CPU offloading in a realistic workflow.

```zig
const std = @import("std");
const volt = @import("volt");

const Chunk = struct {
    data: [16384]u8,
    len: usize,
    sequence: u64,

    pub fn payload(self: *const Chunk) []const u8 {
        return self.data[0..self.len];
    }
};

const CompressedChunk = struct {
    data: [16384]u8,
    len: usize,
    original_len: usize,
    sequence: u64,

    pub fn payload(self: *const CompressedChunk) []const u8 {
        return self.data[0..self.len];
    }
};

fn compressionPipeline(allocator: std.mem.Allocator) !void {
    var input_ch = try volt.channel.bounded(Chunk, allocator, 8);
    defer input_ch.deinit();

    var output_ch = try volt.channel.bounded(CompressedChunk, allocator, 8);
    defer output_ch.deinit();

    // Reader: reads 16 KiB chunks from a file (I/O-bound).
    const reader = try std.Thread.spawn(.{}, fileReader, .{&input_ch});

    // Compressor: compresses each chunk (CPU-bound, blocking pool).
    const compressor = try std.Thread.spawn(.{}, compressWorker, .{
        &input_ch,
        &output_ch,
    });

    // Writer: writes compressed chunks to output (I/O-bound).
    const writer = try std.Thread.spawn(.{}, compressedWriter, .{&output_ch});

    reader.join();
    input_ch.close();
    compressor.join();
    output_ch.close();
    writer.join();
}

fn fileReader(ch: *volt.channel.Channel(Chunk)) void {
    // Read a file in 16 KiB chunks and push into the channel.
    var seq: u64 = 0;
    // In a real application, open a file and read from it.
    for (0..64) |_| {
        var chunk: Chunk = undefined;
        // Simulate file read: fill with sample data.
        @memset(chunk.data[0..], 0x42);
        chunk.len = chunk.data.len;
        chunk.sequence = seq;
        seq += 1;

        while (true) {
            switch (ch.trySend(chunk)) {
                .ok => break,
                .full => std.Thread.sleep(1 * std.time.ns_per_ms),
                .closed => return,
            }
        }
    }
}

fn compressWorker(
    in_ch: *volt.channel.Channel(Chunk),
    out_ch: *volt.channel.Channel(CompressedChunk),
) void {
    while (true) {
        switch (in_ch.tryRecv()) {
            .value => |chunk| {
                // CPU-bound: compress the chunk.
                // Replace this with real compression (e.g., zstd, deflate).
                var compressed: CompressedChunk = undefined;
                compressed.sequence = chunk.sequence;
                compressed.original_len = chunk.len;

                // Simulate compression by copying (a real implementation
                // would call into zstd or deflate here).
                const out_len = @min(chunk.len, compressed.data.len);
                @memcpy(compressed.data[0..out_len], chunk.data[0..out_len]);
                compressed.len = out_len;

                while (true) {
                    switch (out_ch.trySend(compressed)) {
                        .ok => break,
                        .full => std.Thread.sleep(1 * std.time.ns_per_ms),
                        .closed => return,
                    }
                }
            },
            .empty => {
                if (in_ch.isClosed()) return;
                std.Thread.sleep(1 * std.time.ns_per_ms);
            },
            .closed => return,
        }
    }
}

fn compressedWriter(ch: *volt.channel.Channel(CompressedChunk)) void {
    var total_original: usize = 0;
    var total_compressed: usize = 0;

    while (true) {
        switch (ch.tryRecv()) {
            .value => |chunk| {
                total_original += chunk.original_len;
                total_compressed += chunk.len;
                // Write chunk.payload() to output file.
            },
            .empty => {
                if (ch.isClosed()) break;
                std.Thread.sleep(1 * std.time.ns_per_ms);
            },
            .closed => break,
        }
    }

    std.debug.print(
        "Compression complete: {d} bytes -> {d} bytes ({d:.1}% ratio)\n",
        .{ total_original, total_compressed,
           @as(f64, @floatFromInt(total_compressed)) /
           @as(f64, @floatFromInt(@max(total_original, 1))) * 100.0 },
    );
}
```

### Exercise 2: Batch Processor

Accumulate items from a stream and process them in bulk on the blocking pool. Batching amortizes per-item overhead and improves cache locality for CPU work.

```zig
const std = @import("std");
const volt = @import("volt");

const Event = struct {
    timestamp: i64,
    payload: [256]u8,
    payload_len: u16,

    pub fn data(self: *const Event) []const u8 {
        return self.payload[0..self.payload_len];
    }
};

const BatchResult = struct {
    event_count: u32,
    total_bytes: u64,
    checksum: u64,
};

const BATCH_SIZE = 32;

fn batchProcessor(allocator: std.mem.Allocator) !void {
    var event_ch = try volt.channel.bounded(Event, allocator, 64);
    defer event_ch.deinit();

    var result_ch = try volt.channel.bounded(BatchResult, allocator, 8);
    defer result_ch.deinit();

    // Producer: generates events from a stream (I/O-bound).
    const producer = try std.Thread.spawn(.{}, eventProducer, .{&event_ch});

    // Batch consumer: accumulates BATCH_SIZE events, then processes
    // the batch on the blocking pool (CPU-bound).
    const consumer = try std.Thread.spawn(.{}, batchConsumer, .{
        &event_ch,
        &result_ch,
    });

    // Result handler: logs or forwards batch results (I/O-bound).
    const handler = try std.Thread.spawn(.{}, resultHandler, .{&result_ch});

    producer.join();
    event_ch.close();
    consumer.join();
    result_ch.close();
    handler.join();
}

fn eventProducer(ch: *volt.channel.Channel(Event)) void {
    for (0..256) |i| {
        var event: Event = undefined;
        event.timestamp = std.time.milliTimestamp();
        const msg = std.fmt.bufPrint(
            event.payload[0..],
            "event-{d}",
            .{i},
        ) catch return;
        event.payload_len = @intCast(msg.len);

        while (true) {
            switch (ch.trySend(event)) {
                .ok => break,
                .full => std.Thread.sleep(1 * std.time.ns_per_ms),
                .closed => return,
            }
        }
    }
}

fn batchConsumer(
    in_ch: *volt.channel.Channel(Event),
    out_ch: *volt.channel.Channel(BatchResult),
) void {
    var batch: [BATCH_SIZE]Event = undefined;
    var batch_len: usize = 0;

    while (true) {
        switch (in_ch.tryRecv()) {
            .value => |event| {
                batch[batch_len] = event;
                batch_len += 1;

                // When we have a full batch, process it.
                if (batch_len == BATCH_SIZE) {
                    const result = processBatch(batch[0..batch_len]);
                    batch_len = 0;

                    while (true) {
                        switch (out_ch.trySend(result)) {
                            .ok => break,
                            .full => std.Thread.sleep(1 * std.time.ns_per_ms),
                            .closed => return,
                        }
                    }
                }
            },
            .empty => {
                if (in_ch.isClosed()) {
                    // Flush any remaining partial batch.
                    if (batch_len > 0) {
                        const result = processBatch(batch[0..batch_len]);
                        while (true) {
                            switch (out_ch.trySend(result)) {
                                .ok => break,
                                .full => std.Thread.sleep(1 * std.time.ns_per_ms),
                                .closed => return,
                            }
                        }
                    }
                    return;
                }
                std.Thread.sleep(1 * std.time.ns_per_ms);
            },
            .closed => return,
        }
    }
}

/// CPU-bound batch processing. This runs on the blocking pool.
/// Processing a batch together is more cache-friendly than
/// processing items one at a time.
fn processBatch(events: []const Event) BatchResult {
    var total_bytes: u64 = 0;
    var checksum: u64 = 0;

    for (events) |event| {
        const payload = event.data();
        total_bytes += payload.len;
        for (payload) |byte| {
            checksum = checksum *% 31 +% byte;
        }
    }

    return BatchResult{
        .event_count = @intCast(events.len),
        .total_bytes = total_bytes,
        .checksum = checksum,
    };
}

fn resultHandler(ch: *volt.channel.Channel(BatchResult)) void {
    var total_batches: u64 = 0;
    var total_events: u64 = 0;

    while (true) {
        switch (ch.tryRecv()) {
            .value => |result| {
                total_batches += 1;
                total_events += result.event_count;
                // Forward to monitoring, write to log, etc.
            },
            .empty => {
                if (ch.isClosed()) break;
                std.Thread.sleep(1 * std.time.ns_per_ms);
            },
            .closed => break,
        }
    }

    std.debug.print(
        "Processed {d} events in {d} batches ({d} events/batch avg)\n",
        .{ total_events, total_batches,
           if (total_batches > 0) total_events / total_batches else 0 },
    );
}
```

### Exercise 3: Blocking Pool Utilization Metrics

Track how much time your blocking pool threads spend doing useful work versus sitting idle. This helps you tune `max_blocking_threads` and `blocking_keep_alive_ns`.

```zig
const std = @import("std");
const volt = @import("volt");

const BlockingMetrics = struct {
    tasks_submitted: std.atomic.Value(u64),
    tasks_completed: std.atomic.Value(u64),
    active_threads: std.atomic.Value(u32),
    peak_threads: std.atomic.Value(u32),
    total_busy_ns: std.atomic.Value(u64),

    pub fn init() BlockingMetrics {
        return .{
            .tasks_submitted = std.atomic.Value(u64).init(0),
            .tasks_completed = std.atomic.Value(u64).init(0),
            .active_threads = std.atomic.Value(u32).init(0),
            .peak_threads = std.atomic.Value(u32).init(0),
            .total_busy_ns = std.atomic.Value(u64).init(0),
        };
    }

    /// Call this before dispatching work to the blocking pool.
    pub fn taskStarted(self: *BlockingMetrics) void {
        _ = self.tasks_submitted.fetchAdd(1, .monotonic);
        const active = self.active_threads.fetchAdd(1, .monotonic) + 1;

        // Update peak if we have a new high-water mark.
        var peak = self.peak_threads.load(.monotonic);
        while (active > peak) {
            peak = self.peak_threads.cmpxchgWeak(
                peak, active, .monotonic, .monotonic,
            ) orelse break;
        }
    }

    /// Call this after blocking pool work finishes.
    pub fn taskCompleted(self: *BlockingMetrics, elapsed_ns: u64) void {
        _ = self.tasks_completed.fetchAdd(1, .monotonic);
        _ = self.active_threads.fetchSub(1, .monotonic);
        _ = self.total_busy_ns.fetchAdd(elapsed_ns, .monotonic);
    }

    pub fn report(self: *const BlockingMetrics) void {
        const submitted = self.tasks_submitted.load(.monotonic);
        const completed = self.tasks_completed.load(.monotonic);
        const peak = self.peak_threads.load(.monotonic);
        const busy_ns = self.total_busy_ns.load(.monotonic);
        const busy_ms = busy_ns / std.time.ns_per_ms;

        std.debug.print(
            \\--- Blocking Pool Metrics ---
            \\  Tasks submitted:  {d}
            \\  Tasks completed:  {d}
            \\  Peak threads:     {d}
            \\  Total busy time:  {d} ms
            \\
        , .{ submitted, completed, peak, busy_ms });
    }
};

/// Wrapper that instruments a blocking function with metrics.
fn instrumentedBlocking(
    metrics: *BlockingMetrics,
    comptime func: anytype,
    args: anytype,
) @TypeOf(@call(.auto, func, args)) {
    metrics.taskStarted();
    const start = std.time.nanoTimestamp();
    defer {
        const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
        metrics.taskCompleted(elapsed);
    }
    return @call(.auto, func, args);
}
```

Use these metrics to answer practical questions:

* **Peak threads close to `max_blocking_threads`?** -- Raise the cap or add backpressure.
* **Peak threads much lower than `max_blocking_threads`?** -- Lower the cap to reduce memory reservation.
* **High busy time per task?** -- Consider batching to amortize overhead.
* **Many tasks submitted but few completed?** -- Your blocking work may itself be blocking on I/O; check for accidental synchronous calls.
