---
title: Observability
description: snapshot, metrics, tracing — see what your runtime is doing while it runs.
---

Volt ships three observability surfaces, all under the
`volt.observability` and `volt.tracing` namespaces:

| API | What it gives you |
|---|---|
| `volt.observability.snapshot` | Per-task list with state, name, spawn site |
| `volt.observability.metrics` | Per-worker counters: queue depth, steals, parks, ctx switches |
| `volt.tracing.span` | OTel-shaped JSON event stream around a code region |

## Task snapshots

```zig
const rt = volt.currentRuntime().?;
const snaps = try volt.observability.snapshot(allocator, rt);
defer allocator.free(snaps);

for (snaps) |s| {
    std.debug.print("task {d} [{s}] state={s} on_cpu_ns={d}\n", .{
        s.id, s.name orelse "<unnamed>", @tagName(s.state), s.cpu_time_ns,
    });
}
```

Each `TaskSnapshot` has:

- `id: usize` — invocation-id assigned at spawn.
- `name: ?[]const u8` — set via `Job.setName` / `Task.setName`.
- `spawn_site: ?SourceLocation` — set via `setSpawnSite(@src())`.
- `state: TaskState` — `.running`, `.parked_io`, `.parked_channel`,
  `.parked_sync`, `.parked_sleep`, `.parked_join`, `.completed`.
- `cpu_time_ns: u64` — total time spent on-CPU since spawn.

Call from anywhere inside a coroutine. The snapshot is point-in-time;
states change as soon as you've read them.

For a quick count:

```zig
const total = volt.observability.count(rt);       // all tasks ever spawned
const live = volt.observability.liveCount(rt);    // currently running or parked
```

## Runtime metrics

```zig
const metrics = try volt.observability.metrics(allocator, rt);
defer metrics.deinit(allocator);

for (metrics.workers) |w| {
    std.debug.print("worker {d}: pushed={d} stolen={d} parked={d} ctx_switches={d}\n", .{
        w.id, w.pushes, w.steals, w.parks, w.context_switches,
    });
}

std.debug.print("stack pool: hits={d} misses={d}\n", .{
    metrics.stack_pool_hits, metrics.stack_pool_misses,
});
```

`WorkerMetrics`:

- `id: usize` — worker index.
- `pushes: u64` — coroutines pushed onto this worker's deque.
- `steals: u64` — coroutines stolen from another worker.
- `parks: u64` — times this worker condvar-parked (idle).
- `context_switches: u64` — assembly switches into a coroutine.

`RuntimeMetrics`:

- `workers: []WorkerMetrics`
- `stack_pool_hits: u64` — coroutine spawns that got a recycled stack.
- `stack_pool_misses: u64` — spawns that had to mmap fresh.
- `injection_pushes: u64` — work routed through the global queue.

Counters are monotonic; they reset only when the runtime
deinits. For rate metrics (per-second steal rate, etc.), poll twice
and subtract.

## Tracing spans

```zig
const result = try volt.tracing.span(.{
    .name = "handle_request",
    .attributes = &.{
        .{ .key = "user_id", .value = .{ .string = user_id } },
        .{ .key = "method", .value = .{ .string = "GET" } },
    },
}, struct {
    fn body() !Response {
        // ... actual handler ...
    }
}.body);
```

`volt.tracing.span(opts, body)` runs `body()` between an
`event_start` and `event_end` pair, emitting an OTel-shaped JSON
event for each. Returns whatever `body` returned.

Default sink prints to stderr as JSON lines:

```
{"ts":1714579500123456,"event":"start","span":"handle_request","tid":2,"attributes":{"user_id":"u_42","method":"GET"}}
{"ts":1714579500128976,"event":"end","span":"handle_request","tid":2,"duration_ns":5520}
```

Pipe to a collector (`vector`, `fluent-bit`, or anything that
parses JSON Lines) for ingestion. Or replace the sink:

```zig
fn myCustomSink(event: volt.tracing.Event) void {
    // forward to OTLP, statsd, datadog, whatever
}
volt.tracing.setSink(&myCustomSink);
```

The sink fn must be thread-safe — it can be called from any
worker.

## What's NOT here yet

- **`tokio-console`-equivalent UI**: an interactive TUI (or HTTP
  endpoint) showing live task state. Scaffolding is in
  `observability/snapshot.zig`; the UI on top hasn't shipped.
- **Async backtraces**: capturing the chain of suspension points
  that led to a panic. Coroutines store spawn-site info; deeper
  trace-walking is planned.
- **Per-coroutine memory budget**: tracking allocations against a
  quota. Useful for plugin systems / multi-tenant; planned.

Today's stack is enough to answer the typical questions: "how many
coroutines do I have? what are they doing? which workers are
oversubscribed? how often is the stack pool missing?"

## Naming tasks

Always name long-lived coroutines so they show up identifiable in
snapshots:

```zig
const j = try volt.launch(handle, .{conn});
j.setName("conn-handler");
j.setSpawnSite(@src());
```

Or from inside the coroutine:

```zig
fn handle(conn: TcpStream) void {
    volt.coroutine.setCurrentName("conn-handler");
    volt.coroutine.setCurrentSpawnSite(@src());
    // ...
}
```

The `Job.setName` / `Task.setName` form is preferred when you have
the handle (you can name the task before it even starts running).
The `setCurrent*` form is for when you're inside the coroutine and
don't have its own handle.
