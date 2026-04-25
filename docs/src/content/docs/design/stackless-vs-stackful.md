---
title: "Stackless vs Stackful Coroutines"
description: "Why Volt uses stackless futures instead of stackful coroutines, with detailed memory, performance, and architectural comparisons."
---

Async I/O runtimes need to manage thousands or millions of concurrent tasks.
A web server handling 100K connections, a database proxy routing queries, a
message broker dispatching events -- all multiplex many logical operations
onto a small number of OS threads. The fundamental question is: **how do we
represent a suspended task?**

There are two approaches: stackful coroutines and stackless futures. Volt
chose stackless. This page explains why.

## Stackful Coroutines

A stackful coroutine is a function with its own stack. When it suspends, the
entire register set and stack pointer are saved. When it resumes, they are
restored. The coroutine has a complete call stack, so it can suspend from any
depth in the call chain.

### How Stackful Works

```
                   OS Thread Stack                Coroutine Stack
                   +-----------+                  +-----------+
                   | main()    |                  | handler() |
                   | scheduler |     switch       | parse()   |
                   | resume()  |  ------------>   | read()    |
                   |           |  <------------   | (suspend) |
                   | ...       |     switch       |           |
                   +-----------+                  +-----------+
                                                  16-64 KB each
```

Suspension saves registers and swaps the stack pointer:

```
save:   rsp -> coroutine.saved_sp
        push rbx, rbp, r12-r15     (callee-saved registers)
resume: pop r15-r12, rbp, rbx
        rsp <- coroutine.saved_sp
        ret
```

### Stackful Runtimes in the Wild

| Runtime | Stack Model | Initial Stack Size |
|---------|-------------|-------------------|
| Go goroutines | Stackful, growable | 2 KB (grows to 1 MB) |
| Lua coroutines | Stackful, fixed | ~2 KB |
| Java virtual threads (Loom) | Stackful, growable | ~1 KB |
| Zig async (pre-0.11) | Stackless via `@Frame` | Frame-sized (compiler-generated state machine) |
| Boost.Fiber (C++) | Stackful, fixed | 64 KB default |
| Ruby Fiber | Stackful, fixed | 128 KB |

### Advantages of Stackful

**Natural programming model.** Code reads like synchronous code. There is
no visible difference between a blocking call and a suspending call:

```zig
// Stackful: looks identical to synchronous code
fn handleConnection(conn: TcpStream) void {
    const request = conn.read(&buf);      // suspends here
    const response = processRequest(request);
    conn.write(response);                  // suspends here
}
```

**Can suspend from any call depth.** A function 10 levels deep in the call
stack can suspend without the intermediate frames knowing or caring.

**Simpler state management.** Local variables live on the coroutine stack.
No need to manually pack them into a struct.

### Disadvantages of Stackful

**Memory overhead.** Each coroutine needs its own stack:

```
10,000 coroutines  x  16 KB  =  160 MB
100,000 coroutines x  16 KB  =  1.6 GB
1,000,000 coroutines x 16 KB = 16 GB
```

Go mitigates this with 2 KB initial stacks that grow, but growth requires
copying the entire stack (and fixing up pointers), and the minimum 2 KB
still adds up at scale.

**Stack sizing dilemma.** Too small and you get stack overflow. Too large and
you waste memory. Growable stacks add runtime overhead and complexity.

**Context switch overhead.** Saving and restoring the full register set
costs 200-500ns per switch:

```
Context switch cost breakdown (approximate, x86_64):
  Save 6 callee-saved registers:   ~5ns
  Save SIMD state (if used):       ~50-100ns
  Stack pointer swap:               ~2ns
  Pipeline flush + refill:          ~100-200ns
  Cache miss on new stack:          ~50-200ns
  Total:                            ~200-500ns
```

**Cache pollution.** When switching between coroutines, the CPU cache is
polluted by the new stack's memory. With thousands of coroutines cycling
through, cache hit rates drop significantly.

**Platform-specific assembly.** Stack switching requires per-architecture code:

```
Architecture    Registers to Save    Stack Switch Mechanism
x86_64          rbx, rbp, r12-r15   mov rsp
aarch64         x19-x28, x29, lr    mov sp
RISC-V          s0-s11, ra          mv sp
WASM            N/A                  Not supported
```

---

## Stackless Futures

A stackless future is a state machine. Each suspend point becomes a state.
The future's `poll()` method advances through states, returning `.pending`
when it needs to wait and `.ready` when it has a result. Only the data
needed to continue from the current state is stored -- not an entire
call stack.

### How Stackless Works

```
                   OS Thread Stack
                   +-----------+
                   | main()    |
                   | scheduler |
                   | poll()    | ----> future.poll(&ctx)
                   |           | <---- return .pending  (or .ready)
                   | ...       |
                   +-----------+

                   Future (on heap or embedded)
                   +-----------+
                   | state: u8 |
                   | buf: [N]u8|     100-512 bytes
                   | partial: T|
                   +-----------+
```

No stack switching. Suspension is a function return. Resumption is a function
call. The scheduler calls `poll()` again when the waker fires.

### Stackless Runtimes in the Wild

| Runtime | Stack Model | Per-Task Size |
|---------|-------------|---------------|
| Rust async/await | Stackless state machines | Compiler-computed |
| JavaScript Promises | Stackless closures | ~100-200 bytes |
| C# async/await | Stackless state machines | Compiler-computed |
| Python asyncio | Stackless generators | ~500 bytes |
| Volt (Zig) | Stackless futures | ~256-512 bytes |
| Tokio (Rust) | Stackless futures | ~256-512 bytes |

### Advantages of Stackless

**Tiny memory footprint.** A future stores only the state enum and the
variables live across the current suspend point:

```
10,000 futures  x  256 B  =  2.5 MB
100,000 futures x  256 B  =  25 MB
1,000,000 futures x 256 B = 256 MB
```

That is 64x less memory than stackful at 16 KB per stack.

**Cache-friendly.** Small futures fit in a few cache lines:

```
Cache line:  128 bytes (x86_64 with spatial prefetcher)
Future size: ~256 bytes = 2 cache lines
Stack size:  ~16 KB = 128 cache lines
```

Polling a future touches 2 cache lines. Switching to a coroutine stack
touches potentially hundreds.

**No platform-specific assembly.** Suspension is a function return.
Resumption is a function call. Both are standard calling convention
operations that work on every platform.

**Zero-allocation waiters.** With intrusive linked lists, the waiter
struct is embedded directly in the future (see the
[zero-allocation waiters](/design/zero-allocation-waiters) page). No heap
allocation on the wait path.

**Compiler optimization.** Because the state machine is visible to the
compiler, it can optimize across suspend points -- inlining state
transitions, eliminating dead states, computing sizes at comptime.

### Disadvantages of Stackless

**More complex programming model.** In Zig 0.15, without language-level
async/await, futures must be written as explicit state machines:

```zig
const MyFuture = struct {
    pub const Output = void;
    state: enum { init, waiting_for_lock, waiting_for_read, done },
    mutex_waiter: Mutex.Waiter,
    read_buf: [4096]u8,

    pub fn poll(self: *@This(), ctx: *Context) PollResult(void) {
        switch (self.state) {
            .init => { ... },
            .waiting_for_lock => { ... },
            .waiting_for_read => { ... },
            .done => return .{ .ready = {} },
        }
    }
};
```

**Cannot suspend from arbitrary call depth.** Every function in the call
chain that might suspend must return a future. You cannot call a synchronous
library function that internally does I/O -- it will block the worker thread.

---

## Why Volt Chose Stackless

### 1. Scale

The target workload is millions of concurrent I/O tasks. At 256 bytes per
task:

```
1,000,000 tasks x 256 bytes = 256 MB
```

This is manageable on any modern server. At 16 KB per stackful coroutine:

```
1,000,000 tasks x 16 KB = 16 GB
```

That requires a large-memory machine and leaves little room for actual data.

### 2. Performance

**No stack switching overhead.** Suspending a future is a function return
(~5ns). Resuming is a function call (~5ns). Total: ~10ns per
suspend/resume cycle.

A stackful context switch costs 200-500ns: register save/restore, pipeline
flush, and likely cache miss on the new stack. This is a 20-50x difference
per suspend point.

**Better cache utilization.** Consider a scheduler polling 1000 tasks per tick:

```
Stackless: 1000 tasks x 2 cache lines = 2000 cache line accesses
           Working set: ~250 KB (fits in L2)

Stackful:  1000 stacks x 128+ cache lines (active portion)
           Working set: ~16 MB (exceeds L2, thrashes L3)
```

**Zero-allocation waiters.** Volt embeds waiter structs directly in
futures using intrusive linked lists. No `malloc` or `free` on the wait
path. Tokio uses the same pattern. This is critical for sync primitives
that may be acquired and released millions of times per second.

### 3. Memory Predictability

Zig's comptime type system knows the exact size of every future at compile
time:

```zig
const LockFuture = Mutex.LockFuture;
// @sizeOf(LockFuture) is known at comptime -- no runtime surprises

const Task = FutureTask(LockFuture);
// @sizeOf(Task) is known at comptime -- exact allocation size
```

There are no guard pages, no stack growth, no `mmap` overhead, no
fragmentation from variable-sized stack allocations.

With stackful coroutines, stack sizing is a runtime guess. Too small means
stack overflow (a crash). Too large means wasted memory.

### 4. Cross-Platform Simplicity

Volt targets Linux (x86_64, aarch64), macOS (x86_64, aarch64), and
Windows (x86_64). Stackful coroutines would require:

```
Platform-specific code for stackful:
  - x86_64 Linux:   setjmp/longjmp or custom assembly
  - aarch64 Linux:  custom assembly (different register ABI)
  - x86_64 macOS:   makecontext/swapcontext or custom assembly
  - aarch64 macOS:  custom assembly + Apple ABI quirks
  - x86_64 Windows: Fiber API or custom assembly + SEH unwinding
  - WASM:           Not possible (no stack switching)
```

With stackless futures, none of this exists. The same code works on every
platform and every architecture, including WASM where stack switching is
fundamentally impossible.

### 5. Alignment with Zig's Philosophy

Zig values explicitness: no hidden control flow, no hidden allocations, no
hidden state. Stackless futures are explicit state machines where:

- Every suspend point is visible (the `.pending` return)
- Every piece of state is a named field in the struct
- Every allocation is explicit (the `FutureTask.create` call)
- Sizes are known at comptime (`@sizeOf(MyFuture)`)

Stackful coroutines hide the suspend mechanism, hide the stack allocation,
and hide the state. Zig's [`std.Io`](https://github.com/ziglang/zig/tree/master/lib/std/Io)
(in development, expected in 0.16) takes the same explicit-handle approach
Volt uses today — passing an I/O context to every async operation, with no
magic keywords or hidden global state.

---

## The Trade-off in Practice

### Stackful (Hypothetical)

```zig
fn handleConnection(conn: TcpStream) void {
    const request = conn.read(&buf);
    const db_result = db.query("SELECT ...");
    const response = render(request, db_result);
    conn.write(response);
}
```

Clean, readable, sequential. But each call allocates a hidden stack frame,
and the coroutine's 16 KB stack sits in memory for the entire duration.

### Stackless with Combinators (Volt API)

In practice, most users do not write raw futures. Volt provides
higher-level APIs that compose futures:

```zig
const volt = @import("volt");

fn handleRequest(io: volt.Runtime, user_id: u64, key: []const u8) !void {
    // Spawn async tasks and await results
    var user_f = try io.@"async"(fetchUser, .{user_id});
    var posts_f = try io.@"async"(fetchPosts, .{user_id});

    // Wait for both concurrently
    const user, const posts = try io.joinAll(.{ user_f, posts_f });

    // Race two operations
    const winner = try io.race(.{
        try io.@"async"(fetchFromPrimary, .{key}),
        try io.@"async"(fetchFromReplica, .{key}),
    });
}
```

The `FnFuture` wrapper converts any regular function into a single-poll
future, so spawning "just works" for simple cases:

```zig
fn processRequest(data: []const u8) !Response {
    return Response.from(data);
}

var f = try io.@"async"(processRequest, .{data});
const response = try f.@"await"(io);
```

---

## Full Comparison

| Criterion | Stackful | Stackless (Volt) |
|-----------|----------|---------------------|
| Memory per task | 8-64 KB | 100-512 bytes |
| 1M tasks | 8-64 GB | 100-512 MB |
| Context switch cost | 200-500 ns | 5-20 ns |
| Cache behavior | Poor (stack pollution) | Excellent (compact) |
| Platform portability | Assembly per arch | Zero platform code |
| Wait allocation | 0 (on stack) | 0 (intrusive) |
| Programming model | Natural (sync-looking) | Explicit (state machine) |
| Size at comptime | No (runtime growth) | Yes (`@sizeOf`) |
| Guard pages | Required | Not needed |
| WASM support | Impossible | Works |
| Zig `std.Io` compat | No (different model) | Same explicit-handle philosophy |

### Cross-Runtime Comparison

| Runtime | Model | Per-Task | Switch Cost | Allocator-Free Wait |
|---------|-------|----------|-------------|---------------------|
| Go | Stackful, growable | 2 KB min | ~300 ns | No |
| Java Loom | Stackful, growable | ~1 KB min | ~200 ns | No |
| Rust/Tokio | Stackless | ~256-512 B | ~10 ns | Yes (intrusive) |
| Zig/Volt | Stackless | ~256-512 B | ~10 ns | Yes (intrusive) |
| C# | Stackless | Compiler-sized | ~15 ns | No |
| Python asyncio | Stackless (generator) | ~500 B | ~50 ns | No |

---

## Zig's std.Io and How Volt Relates

Zig's [`std.Io`](https://github.com/ziglang/zig/tree/master/lib/std/Io) (in development, expected in 0.16) is an explicit I/O interface that works like `Allocator`. There are no new `async`/`await` keywords. Instead, asynchrony is expressed through method calls:

- `io.async(func, .{args})` — launch an asynchronous operation, returns a `Future`
- `future.await(io)` — block until the result is ready
- `io.concurrent(func, .{args})` — explicitly request true concurrent execution (errors if unavailable)
- `future.cancel(io)` — request cancellation (idempotent with `await`)

Multiple backends exist in the Zig master branch: `Io.Threaded` (production-ready), plus proof-of-concept `Io.IoUring` (Linux) and `Io.Kqueue` (macOS/BSD) backends that use stackful coroutines and depend on future language enhancements.

This design shares Volt's core philosophy: **explicit I/O handles, no hidden global state, no magic keywords**. Both Volt and `std.Io` pass an I/O context explicitly to every operation. The key difference is scope — `std.Io` is a minimal standard-library primitive, while Volt provides a full runtime with work-stealing scheduling, sync primitives, channels, and cooperative budgeting.

### Volt Today vs Zig's std.Io

| Aspect | Zig `std.Io` | Volt |
|--------|-------------|------|
| I/O handle | `std.Io` (vtable-based interface) | `volt.Runtime` (runtime handle) |
| Async call | `io.async(func, .{args})` | `io.@"async"(func, .{args})` |
| Await | `future.await(io)` | `handle.@"await"(io)` |
| Concurrency | Explicit `io.concurrent()` vs `io.async()` | All spawns are concurrent (work-stealing) |
| Cancellation | `future.cancel(io)` (idempotent with await) | `handle.cancel()` |
| Scheduler | OS threads (`Threaded`) | Work-stealing with LIFO slot, cooperative budget |
| I/O backends | Threaded + proof-of-concept io_uring, kqueue | io_uring, kqueue, IOCP, epoll |
| Sync primitives | Mutex, Condition | Mutex, RwLock, Semaphore, Barrier, Notify, OnceCell |
| Channels | `Queue(T)` (MPMC bounded) | Channel, Oneshot, Broadcast, Watch, Select |

Volt complements `std.Io` the same way Tokio complements Rust's `std::future` — providing the scheduler, sync primitives, and higher-level abstractions that a minimal standard-library interface does not include.

:::note
For more on Zig's I/O direction, see Andrew Kelley's [explanation of the new async I/O design](https://andrewkelley.me/post/zig-new-async-io-text-version.html).
:::
