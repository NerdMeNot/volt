---
title: Stackless vs Stackful
description: Why Volt is built on stackful coroutines instead of stackless futures, what each approach actually costs, and which workloads each is right for.
---

Async runtimes come in two flavors. Tokio, Rust async, JavaScript
promises, C# `Task<T>` — all stackless. Go, Erlang, Lua, Volt — all
stackful. The difference is concrete and worth understanding before
you commit to either model.

## What "stackless" actually means

A stackless coroutine is a state machine. The compiler (or a macro)
takes async code that looks like:

```rust
async fn fetch_user(id: u64) -> User {
    let row = db.query(id).await;
    let posts = api.get_posts(id).await;
    User::from(row, posts)
}
```

…and rewrites it as a struct with one variant per `await` point:

```rust
enum FetchUserState {
    Start { id: u64 },
    AwaitingDb { id: u64, db_future: DbQuery },
    AwaitingApi { row: Row, api_future: ApiCall },
    Done,
}
impl Future for FetchUser { fn poll(&mut self, cx: &mut Context) -> Poll<User> { ... } }
```

The state machine is ~100-500 bytes regardless of how many locals
you have. There's no runtime stack — the locals at each `await`
point get baked into the corresponding enum variant. You hand that
struct to a runtime that calls `poll(&mut self)` until it returns
`Ready`.

That's the price: every async function has a *different type*, the
compiler has to do nontrivial work to generate the state machine,
the language needs `async`/`await` keywords, and the borrow checker
needs `Pin` to handle the self-referential intermediate state.

The win is real though: 1M concurrent waiters costs ~256 MiB of
state-machine memory. That's the regime where stackless dominates.

## What "stackful" actually means

A stackful coroutine is a function that owns a real call stack. When
it suspends, the runtime saves its registers + stack pointer to a
context struct and switches to a different coroutine. When it
resumes, the runtime restores those registers and the function
continues.

```zig
fn fetchUser(id: u64) !User {
    const row = try db.query(id);          // suspends here; resumes here
    const posts = try api.getPosts(id);    // suspends here; resumes here
    return User.from(row, posts);
}
```

No state machine, no `async fn`, no `Pin`. Locals live on the stack;
they survive suspension because the stack itself is preserved. The
function looks identical to a synchronous version.

The price: each coroutine needs its own stack. Volt commits 1 page
(4-16 KiB) up front and grows in place via guard-page faults to a
cap of 8 MiB. For 10K concurrent coroutines that's 40-160 MiB
resident — heavier than 10K stackless futures, but Volt-cheap relative
to OS threads (which start at 1 MiB resident on most systems).

## The Zig-specific argument

In Rust, "function coloring" — the rule that you can only call
`async fn` from another `async fn` — is mostly absorbed by the
language. You write `await` and the compiler handles it. The cost is
language-level; the type system makes it manageable.

Zig has no `async`/`await` and (per maintainer statement) won't add
one again after the 0.10 attempt. That changes the math:

- A stackless Volt would have to expose Future/Poll types, force
  every async function to have a different return type, and require
  manual state-machine plumbing for non-trivial control flow. This
  was Volt's pre-pivot design and it generated dramatically more
  code than equivalent Tokio Rust.
- A stackful Volt looks and reads like blocking Zig. No keyword
  changes, no syntax tax. Function signatures don't carry async-ness
  in their types.

Stackful is the right choice for *Zig specifically*. In Rust,
stackless wins because the language ergonomics absorb the type
complexity. In Zig, that absorption isn't available, so the
ergonomics math flips.

## The memory shape

```
  STACKLESS (Tokio):                  STACKFUL (Volt):

  Each task ≈ 256-512 B               Each coroutine reserves 8 MiB virtual
                                      address space, commits 1 page (4-16 KiB)

  ┌──────────────────────┐            ┌──────────────────────────┐
  │ enum FetchUserState  │            │  reserved range (8 MiB)  │
  │  ┌────────────────┐  │            │  ┌────────────────────┐  │
  │  │ Start          │  │            │  │ permanent floor    │  │  PROT_NONE
  │  │ AwaitingDb     │  │            │  ├────────────────────┤  │
  │  │ AwaitingApi    │  │            │  │                    │  │
  │  │ Done           │  │            │  │   uncommitted      │  │  PROT_NONE
  │  └────────────────┘  │            │  │   (~7.99 MiB)      │  │  (virtual only)
  │  current locals      │            │  │                    │  │
  └──────────────────────┘            │  ├────────────────────┤  │
                                      │  │ guard page         │  │  PROT_NONE
   Compiler-generated.                │  ├────────────────────┤  │
   Locals at each await                │  │  committed         │  │  PROT_RW
   point baked into                    │  │  (1 page initial,  │  │
   matching enum variant.              │  │   grows on fault)  │  │
   1M waiters ≈ 256 MiB.              │  └────────────────────┘  │
                                      │           ▲              │
                                      │     SP grows down        │
                                      └──────────────────────────┘

                                       Real call stack. Locals live
                                       on it; pointers stay valid
                                       across suspension.
                                       1M waiters ≈ 4-16 GiB resident
                                       (more virtual, mostly uncommitted).
```

## What this trade actually costs

You pay:

- **~4-16 KiB of resident memory per coroutine** (vs ~256-512 bytes
  for a stackless Future).
- **A small per-suspend overhead** for the context switch (Volt
  measures ~10-15 ns on Apple Silicon for the assembly switch,
  plus park/unpark bookkeeping). Stackless polling is roughly
  comparable; the dominant cost in either case is the futex / parker
  underneath.
- **Cache footprint**: stackless tasks pack denser. If your workload
  is "1M waiting tasks, hot path picks one, runs ~100 ns of work,
  parks again," stackless's tighter cache footprint matters.

You buy:

- **Synchronous-shape code**. The biggest single win. Recursive
  parsers, nested error handling, complex control flow all "just
  work."
- **No function coloring**. Library functions don't get a stickier
  type because they internally suspend. Composability stays simple.
- **Stack-allocated locals across suspensions**. Take an address of
  a stack local before suspending; the address is still valid
  after.
- **Cancellation through arbitrary blocking calls**. Because there's
  no `Pin` constraint and no `async fn` boundary, a single Park
  primitive cancels uniformly through any wait point.

## When you'd pick stackless instead

The honest answer: if you're building a workload where you genuinely
expect 100K+ concurrent *parked* coroutines and per-task state is
small (e.g., a backend that holds 1M open WebSocket connections,
each waiting on infrequent traffic), stackless's tighter memory
footprint is a real advantage. That regime is roughly 1-2 orders of
magnitude past where stackful starts to feel cramped.

For everything else — HTTP servers handling requests, data pipelines,
CLI tools, system services, service meshes, anything where you have
"thousands of concurrent active operations" rather than "millions of
parked ones" — stackful's ergonomics dominate the memory cost. Volt
is targeting the second category.

## The decision summary

| Axis | Stackless | Stackful (Volt) |
|---|---|---|
| Per-task memory | ~256-512 B | ~4-16 KiB resident |
| Source ergonomics | Requires `async`/`await` | Looks like blocking Zig |
| Function coloring | Yes — `async fn` is a different type | No |
| Stack locals across suspension | Compiler-managed | Native (the stack is real) |
| Cancellation | Per-Future, with `Pin` constraints | One uniform Park-cancel for everything |
| Best at | 1M+ parked, tiny per-task state | Thousands active, complex per-task work |
| Language fit | Languages with `async`/`await` syntax | Languages without |

Volt is the second column. Pick it when its tradeoffs match your
workload. If they don't — for instance, if you're building a million-
connection edge proxy on a machine where memory is the bottleneck —
that's a legitimate stackless use case and Tokio (in Rust) is
probably the right answer.
