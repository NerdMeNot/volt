---
title: Producer-Consumer
description: Worker pool with bounded Channel for backpressure, multiple
  producers and consumers, and poison-pill shutdown.
slug: v1.0.0-zig0.15.2/v110/cookbook/producer-consumer
---

The producer-consumer pattern is the backbone of most background processing systems. Image thumbnail generation, email delivery queues, log ingestion pipelines, payment processing -- any workflow where work arrives faster than it can be handled benefits from decoupling production from consumption.

Producers push jobs onto a bounded channel; consumers pull and process them. The bounded capacity provides natural backpressure -- when the channel is full, producers must wait, giving consumers time to catch up.

:::tip[What you'll learn]
* **Bounded Channel for backpressure** -- how a fixed-capacity channel throttles producers automatically
* **Multiple producers and consumers** -- MPMC work distribution across threads
* **Clean shutdown via channel close** -- producers finish, close the channel, consumers drain and exit
* **Work distribution patterns** -- how to route different job types to the right processing logic
:::

## Complete Example

This example models a job queue where producers submit different kinds of background work (sending emails, resizing images, generating reports) and a pool of consumers processes each job according to its type.

```zig
const std = @import("std");
const volt = @import("volt");

// -- Job definition ---------------------------------------------------------

const JobType = enum {
    email_send,
    image_resize,
    report_generate,
};

const Job = struct {
    id: u64,
    job_type: JobType,
    payload: [128]u8,
    payload_len: u16,

    pub fn create(id: u64, job_type: JobType, data: []const u8) Job {
        var job: Job = undefined;
        job.id = id;
        job.job_type = job_type;
        const len = @min(data.len, 128);
        @memcpy(job.payload[0..len], data[0..len]);
        job.payload_len = @intCast(len);
        return job;
    }

    pub fn data(self: *const Job) []const u8 {
        return self.payload[0..self.payload_len];
    }
};

// -- Configuration ----------------------------------------------------------

const NUM_PRODUCERS = 3;
const NUM_CONSUMERS = 4;
const JOBS_PER_PRODUCER = 100;
const CHANNEL_CAPACITY = 32;

// -- Entry point ------------------------------------------------------------

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // A bounded channel acts as our job queue. The capacity cap (32) means
    // producers will block when consumers fall behind -- this is backpressure.
    var job_queue = try volt.channel.bounded(Job, allocator, CHANNEL_CAPACITY);
    defer job_queue.deinit();

    // Shared counter so we can verify every job was handled.
    var jobs_processed = std.atomic.Value(u64).init(0);

    // Start consumers FIRST so they are ready when jobs arrive.
    var consumers: [NUM_CONSUMERS]std.Thread = undefined;
    for (&consumers, 0..) |*t, i| {
        t.* = try std.Thread.spawn(.{}, consumer, .{
            &job_queue,
            &jobs_processed,
            i,
        });
    }

    // Start producers. Each submits JOBS_PER_PRODUCER jobs.
    var producers: [NUM_PRODUCERS]std.Thread = undefined;
    for (&producers, 0..) |*t, i| {
        t.* = try std.Thread.spawn(.{}, producer, .{
            &job_queue,
            i,
        });
    }

    // Wait for every producer to finish submitting.
    for (&producers) |*t| t.join();

    // Close the channel. This is the shutdown signal:
    //   - No new sends will succeed (trySend returns .closed).
    //   - Consumers drain whatever is left, then see .closed from tryRecv.
    job_queue.close();

    // Wait for consumers to finish draining.
    for (&consumers) |*t| t.join();

    const total = jobs_processed.load(.acquire);
    std.debug.print("Done. Processed {d} / {d} jobs.\n", .{
        total,
        NUM_PRODUCERS * JOBS_PER_PRODUCER,
    });
}

// -- Producer ---------------------------------------------------------------

fn producer(
    queue: *volt.channel.Channel(Job),
    producer_id: usize,
) void {
    // Each producer cycles through job types to simulate varied workloads.
    const types = [_]JobType{ .email_send, .image_resize, .report_generate };
    const payloads = [_][]const u8{
        "to=user@example.com subject=Welcome",
        "src=/uploads/photo.jpg width=800",
        "report=monthly format=pdf",
    };

    for (0..JOBS_PER_PRODUCER) |i| {
        const id = producer_id * JOBS_PER_PRODUCER + i;
        const kind = types[i % types.len];
        const job = Job.create(id, kind, payloads[i % payloads.len]);

        // Retry loop: if the channel is full, wait briefly and try again.
        // This is where backpressure becomes visible -- the producer slows
        // down instead of overwhelming the system.
        while (true) {
            switch (queue.trySend(job)) {
                .ok => break,
                .full => std.Thread.sleep(1 * std.time.ns_per_ms),
                .closed => return,
            }
        }
    }
}

// -- Consumer ---------------------------------------------------------------

fn consumer(
    queue: *volt.channel.Channel(Job),
    processed: *std.atomic.Value(u64),
    consumer_id: usize,
) void {
    _ = consumer_id;
    while (true) {
        switch (queue.tryRecv()) {
            .value => |job| {
                processJob(&job);
                _ = processed.fetchAdd(1, .acq_rel);
            },
            .empty => {
                // The buffer is momentarily empty. Check if the channel has
                // been closed -- if so, there will never be more jobs.
                if (queue.isClosed()) return;
                std.Thread.sleep(1 * std.time.ns_per_ms);
            },
            .closed => return, // Channel closed AND buffer drained.
        }
    }
}

fn processJob(job: *const Job) void {
    // Route to the right handler based on job type.
    switch (job.job_type) {
        .email_send => {
            // In production: connect to SMTP, send the message.
            _ = job.data();
            std.Thread.sleep(5 * std.time.ns_per_ms);
        },
        .image_resize => {
            // In production: decode image, resize, write to storage.
            _ = job.data();
            std.Thread.sleep(10 * std.time.ns_per_ms);
        },
        .report_generate => {
            // In production: query database, render PDF.
            _ = job.data();
            std.Thread.sleep(20 * std.time.ns_per_ms);
        },
    }
}
```

## Walkthrough

### Bounded capacity as backpressure

The channel has `CHANNEL_CAPACITY = 32` slots. When all 32 are occupied, `trySend` returns `.full` and the producer must wait. This is **backpressure by design** -- without it, a fast producer would allocate unbounded memory while a slow consumer falls further and further behind.

The producer's response to `.full` is a policy decision. Three common choices:

* **Wait and retry** (as shown above) -- guarantees no data loss, appropriate when every job matters.
* **Drop the item** -- appropriate for telemetry or metrics where freshness matters more than completeness.
* **Apply backpressure upstream** -- reject incoming requests at the API layer (e.g., return HTTP 503), pushing the backpressure signal to the caller.

### Why close the channel instead of stopping producers?

`job_queue.close()` solves a coordination problem that would otherwise require careful bookkeeping. After calling close:

1. Any future `trySend` immediately returns `.closed` -- no producer can sneak in more work.
2. Consumers continue to receive buffered items normally via `tryRecv`.
3. Once the buffer is empty, `tryRecv` returns `.closed` -- the consumer knows it is safe to exit.

This three-phase sequence (stop accepting, drain, exit) is the same pattern used by production message brokers. It guarantees that no job is silently lost and no consumer hangs waiting for work that will never arrive.

### Why start consumers before producers?

If producers start first and the channel fills before any consumer is running, the producers spin-wait on `.full` until a consumer thread is scheduled. Starting consumers first means there is always someone ready to pull work as soon as it appears, avoiding an unnecessary initial stall.

### MPMC: how jobs get distributed

`Channel(T)` is a multi-producer, multi-consumer queue. Internally it uses a Vyukov ring buffer with per-slot sequence numbers and CAS on head/tail indices, so the data path is lock-free. A mutex is only taken when registering async Future waiters -- the `trySend`/`tryRecv` hot path never locks.

Because any consumer can pull any job, work is distributed roughly evenly. If one consumer is busy with a slow `report_generate` job, the others continue pulling from the queue without blocking.

### Why route by job type inside the consumer?

The `processJob` function dispatches on `job.job_type` so that each kind of work gets the right handler. This keeps the architecture simple (one queue, one consumer pool) while still supporting heterogeneous workloads. If certain job types need dedicated resources (e.g., a GPU for image processing), the variant patterns below show how to split into multiple queues.

## Poison-Pill Variant

Channel close shuts down all consumers at once. If you need to shut down consumers **one at a time** -- for example, to scale down during low traffic -- use a poison pill: a special sentinel value that tells one consumer to exit.

```zig
const MaybeJob = union(enum) {
    job: Job,
    shutdown,
};

var queue = try volt.channel.bounded(MaybeJob, allocator, 32);

// Send one shutdown sentinel per consumer.
for (0..NUM_CONSUMERS) |_| {
    while (true) {
        switch (queue.trySend(.shutdown)) {
            .ok => break,
            .full => std.Thread.sleep(1 * std.time.ns_per_ms),
            .closed => return,
        }
    }
}

// Each consumer exits when it receives its own poison pill.
fn consumer(queue: *volt.channel.Channel(MaybeJob)) void {
    while (true) {
        switch (queue.tryRecv()) {
            .value => |item| switch (item) {
                .job => |j| processJob(&j),
                .shutdown => return,
            },
            .empty => std.Thread.sleep(1 * std.time.ns_per_ms),
            .closed => return,
        }
    }
}
```

Each `.shutdown` value is consumed by exactly one consumer (since the channel is MPMC and each value is delivered once), so sending `NUM_CONSUMERS` sentinels ensures every consumer eventually exits.

## Variations

### Priority Queue

Use two channels -- one for high-priority work and one for normal. Consumers always check the high-priority channel first, so urgent jobs jump the queue without requiring a sorted data structure.

```zig
fn consumer(hi: *Channel(Job), lo: *Channel(Job)) void {
    while (true) {
        // Always drain high-priority first.
        switch (hi.tryRecv()) {
            .value => |job| { processJob(&job); continue; },
            else => {},
        }
        // Fall through to normal priority.
        switch (lo.tryRecv()) {
            .value => |job| processJob(&job),
            .empty => {
                if (hi.isClosed() and lo.isClosed()) return;
                std.Thread.sleep(1 * std.time.ns_per_ms);
            },
            .closed => {
                // Normal channel is closed. Drain remaining high-priority
                // work before exiting.
                while (true) {
                    switch (hi.tryRecv()) {
                        .value => |job| processJob(&job),
                        else => return,
                    }
                }
            },
        }
    }
}
```

### Fan-Out then Fan-In with Oneshot

Combine the job queue with `Oneshot` channels for result collection. Each request carries a oneshot sender; the consumer sends the result back to the specific producer that submitted the work. This turns the fire-and-forget queue into a request/response system.

```zig
const Request = struct {
    job: Job,
    reply: *volt.channel.Oneshot(Result).Sender,
};

// Producer: submit work and wait for the result.
var pair = volt.channel.oneshot(Result);
queue.trySend(Request{ .job = job, .reply = &pair.sender });

// Block until the consumer sends a result back.
while (pair.receiver.tryRecv() == null) {
    std.Thread.sleep(1 * std.time.ns_per_ms);
}

// Consumer: process the job and reply.
fn consumer(queue: *Channel(Request)) void {
    while (true) {
        switch (queue.tryRecv()) {
            .value => |req| {
                const result = processJob(&req.job);
                req.reply.send(result);
            },
            .empty => std.Thread.sleep(1 * std.time.ns_per_ms),
            .closed => return,
        }
    }
}
```

## Sizing the Channel

| Capacity | Trade-off |
|----------|-----------|
| Small (1-8) | Low latency, high backpressure. Producers block often. Good when you want tight feedback between producers and consumers. |
| Medium (32-256) | Good default. Absorbs short bursts without excessive memory. |
| Large (1024+) | High throughput, absorbs long stalls. Uses more memory and increases maximum latency for any single item sitting in the queue. |

A good starting point is `num_consumers * 4` -- enough to keep all consumers busy through brief producer pauses without buffering so much that latency becomes unpredictable.

## Try It Yourself

These exercises build on the complete example above. Each one adds a production concern that real job queues need.

### Per-consumer stats

Add tracking to each consumer: how many jobs it processed, and the average processing time. Print a summary after shutdown. Hints:

* Pass a mutable stats struct (or use a per-consumer index into a shared array) alongside the consumer ID.
* Use `std.time.Timer` to measure elapsed time per job.
* Print stats after joining all consumer threads.

### Dead letter queue

Some jobs fail. Add a second channel that acts as a dead letter queue (DLQ). When `processJob` returns an error, the consumer pushes the failed job onto the DLQ instead of dropping it. After shutdown, print how many jobs ended up in the DLQ. This is the foundation for retry logic in production systems.

### Job priority with dual channels

Combine the priority queue variant with the main example. Create a `hi_queue` and a `lo_queue`. Producers assign `report_generate` jobs to the high-priority channel and everything else to the normal channel. Measure whether high-priority jobs complete faster on average than they did with a single queue.
