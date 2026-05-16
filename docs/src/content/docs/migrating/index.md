---
title: Migrating to Volt
description: If you're coming from Go, Tokio, or Node.js — what changes, what stays the same, and where to look first.
---

You already know how to write concurrent code. The question
isn't "what is a coroutine?" — it's "what's different here?"

These pages answer that question for the three runtimes whose
users most often ask. Each one starts with the mental-model
shift, shows side-by-side translations of common patterns,
calls out the gotchas specific to that background, and lists
what's not yet in Volt that you may be looking for.

## Pick your starting point

- **[Coming from Go](/migrating/from-go/)** — goroutines map
  almost 1:1 to coroutines; the differences are the spawn-handle
  shape, explicit `*Cancel` instead of `context.Context`, and
  the absence of `select`.
- **[Coming from Tokio (Rust)](/migrating/from-tokio/)** — the
  biggest conceptual shift: stackful vs stackless. No `async`,
  no `await`, no `Future`, no `Pin`. Same scheduler shape; very
  different programming model.
- **[Coming from Node.js](/migrating/from-nodejs/)** — single-
  threaded event loop → true M:N. Promise → `Task(T)`. Callback
  → coroutine. The biggest shift: you can actually have data
  races now.

## Tabular reference

The [Tokio + Go comparison](/appendix/tokio-go-comparison/) page
has the dense feature-by-feature mapping. The pages here are the
narrative version — pick the prose for first reading, the table
for quick reference later.

## Common across all three

A few mental-model shifts apply regardless of where you're
coming from:

- **There is no implicit runtime.** You construct one with
  `Runtime.init` and feed it a root function. The runtime owns
  the worker pool, the reactor, the parking lot, the slab
  arena.
- **Stackful coroutines** have real stacks. Pointers to
  stack-locals stay valid across suspensions. No `Pin`, no
  state-machine generation, no "every async fn has a different
  type."
- **Cancellation is data**, not a method on a handle.
  `volt.Cancel` flows through code as an explicit parameter,
  like Go's `context.Context`.
- **The scheduler is M:N work-stealing.** Your coroutine can
  resume on a different OS thread than it suspended on. Don't
  cache threadlocals across yield points.
- **No `async` / `await` keyword.** Code reads like blocking
  I/O. The runtime suspends at every wait point — `read`,
  `recv`, `lock`, `sleep` — automatically.

## See also

- [Architecture](/architecture/) — how Volt is built. Useful to
  read in parallel with migration; the mental model lands faster
  when you know what's underneath.
- [Choosing a primitive](/guides/choosing-primitive/) — the
  decision tree for picking the right Volt primitive once
  you've mentally mapped the source-language concept.
- [Roadmap](/appendix/roadmap/) — what's not yet in Volt that
  you may be looking for.
