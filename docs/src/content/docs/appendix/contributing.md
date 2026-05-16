---
title: Contributing
description: Bench-gate protocol, commit conventions, where to start. The rules that keep the runtime small and the regressions caught early.
---

Volt is a small project with strict conventions. The conventions
exist because the runtime is small enough that one bad landing
can regress a load-bearing benchmark by 30× (see [Slab arena
postmortem](/performance/slab-arena-postmortem/)). The phase-
landing protocol is what catches this.

## The bench gate

**Mandatory** for any landing that touches:

- The scheduler (`src/runtime.zig`, `src/worker.zig`, `src/p.zig`,
  `src/work_steal_queue.zig`).
- The coroutine struct (`src/coroutine.zig`).
- The allocation path (`src/stack.zig`, `src/signal.zig`, slab
  arena interaction in `src/p.zig`).
- Sync primitives (`src/sync.zig`, `src/cancel.zig`,
  `src/park.zig`, `src/parker.zig`).

Run before merge, must be green vs the prior measured baseline:

```sh
zig build bench-yield                 # ~9 ns/op
zig build bench-spsc                  # 12 ns/op
zig build bench-mpmc                  # 54-157 ns/op
zig build bench-mutex                 # 15 ns/op
zig build bench-spawn-hot             # 101-575 ns/op
zig build bench-fanout-scaling
zig build bench-scaling
zig build bench-parallel-compute
zig build bench-tcp-echo              # ~8,500 ns/RTT
zig build bench-rss                   # 16.6 KiB/coro at N=10k
zig build stress                       # 45s, ~840M ops, 3 runs
```

A change that moves the bench it claims to improve by <20%
doesn't land — under the noise floor.

A regression on *any* bench is a non-starter. "Follow up in a
later PR" is not acceptable for a regressed bench — the bench
gate exists exactly because that's the failure mode.

### When to take 5-run medians

Spawn-heavy benches (`bench-spawn-hot`, `bench-fanout-scaling`,
`bench-scaling`) have 15-40% run-to-run variance. Single-shot
numbers are misleading. Take 5-run medians for any spawn-heavy
claim. Micro benches (`yield`, `spsc`, `mutex`) are stable
within ~5%; single-shot is fine.

Document the system load average alongside the numbers. Loaded
hosts inflate spawn-heavy numbers by 20%+; the comparison only
holds when load is comparable.

## Phase-landing protocol violations on record

Both of these caused real regressions:

1. **Phase 4** (reverted 2026-05-15). Tried per-spawn `mprotect`
   for stack guard pages. Ship-skip-the-bench-gate landing.
   `bench-spawn-hot` regressed ~8× under multi-worker. Reverted
   after debugging. See [Phase 4
   postmortem](/performance/phase-4-postmortem/).

2. **Stack guard pages via pool of 64** (slab-arena postmortem,
   2026-05-16). Landed with the bench gate skipped. Three weeks
   later: `bench-spawn-hot` was 30× off baseline; no one noticed
   because the unit tests passed. See [Slab arena
   postmortem](/performance/slab-arena-postmortem/).

The lesson: the bench gate is not optional. CI catching this
automatically (a cron running the gate against the latest commit)
is roadmapped; until then, the protocol is "the committer runs
it before merging."

## Commit conventions

### Format

Conventional commits, terse subject, opinionated body:

```
feat(channel): Mpmc(T, cap) — Vyukov bounded MPMC with parking-lot blocking

Each ring cell carries a `seq` counter. Enqueue at position P
succeeds when `cell.seq == P`; the CAS is on the shared
`enqueue_pos` counter, then `cell.seq = P + 1` publishes...
```

Conventional commit types Volt uses:

- `feat(scope): ...` — new functionality
- `fix(scope): ...` — bug fix
- `perf(scope): ...` — performance improvement (must include
  bench receipts in the body)
- `refactor(scope): ...` — no behaviour change
- `docs: ...` or `docs(scope): ...` — docs only
- `test(scope): ...` — tests only
- `bench: ...` — bench file only
- `chore: ...` — tooling, build, infra

### What goes in the body

- The **why** of the change. The diff already shows the what.
- For perf changes: numbers before and after, methodology
  (5-run median, system load).
- For correctness fixes: the race / invariant violation in
  prose, and how the fix closes it.
- For refactors: what the new shape buys.
- Links to issues, postmortems, prior commits where relevant.

### What stays out

- **No "Co-Authored-By" lines.** Single-author commits.
- **No emojis.** Plain text.
- **No version bump bullet points.** Use the changelog file.
- **No marketing.** "X is the right way" without receipts is
  noise. Show the receipt or skip the claim.

## Pre-commit hook

```sh
git config core.hooksPath .githooks
```

The hook (`./.githooks/pre-commit`) runs:

1. `zig fmt --check` on `src/`, `bench/`, `build.zig`.
2. `zig build-lib src/lib.zig -lc -fno-emit-bin` — type-check
   only, no test run.

This is fast (~2s). Real tests run in CI on every push.

Why not run tests in the hook: `zig test`'s `--listen=-` IPC
handshake hangs on loaded hosts. Type-check is fast and catches
syntax/type errors; CI catches behaviour regressions.

## Testing conventions

### Allocator

Production multi-worker tests: `std.heap.smp_allocator`.

Single-worker / single-thread tests: `std.testing.allocator` for
leak detection.

`std.testing.allocator` is **not thread-safe** on all paths
(it captures stack traces via a process-global hash map for
double-free detection). Tests that use it under multi-worker
crash with `EXC_BAD_ACCESS`. See `692b659` for the test
infrastructure fix.

### Leak detection

`std.testing.allocator` fails the test if any allocation leaks.
This is the leak gate. Any new allocation path in the runtime
must have a test exercising both alloc and free of that path —
the leak detector validates both happened.

### Where to put tests

Inline `test "..."` blocks at the bottom of the `src/` file that
implements the thing being tested. Two reasons:

1. Tests near implementation are read alongside the code.
2. Zig's test discovery (`zig build test`) sees them automatically.

Cross-cutting integration tests can live in a separate file under
`src/`. Avoid `test/` as a top-level dir.

## Where to start

For a first contribution:

1. **Read the architecture chapter.** Start with
   [Overview](/architecture/), pick a topic that interests you,
   read the source file alongside.
2. **Run the bench gate locally.** Establish a baseline on your
   hardware. Run `zig build test` to confirm everything passes.
3. **Pick a small thing.** The roadmap mentions tens of items;
   pick one that's well-scoped. Examples:
   - A cancel-aware `volt.sleep` variant (currently absent —
     see [Time](/usage/time/)).
   - A `volt.select` primitive (currently absent — see
     [Tokio + Go comparison](/appendix/tokio-go-comparison/)).
   - A periodic-RSS-tracking script that runs in CI to catch
     regressions early.
4. **Open an issue first.** Describe the design. Get feedback
   on the shape before writing 500 lines of code that won't
   land because the design isn't right.

For a perf contribution:

1. **Profile first.** `samply record` is the recommended tool on
   Darwin. The multi-worker investigation
   ([profile](/performance/multi-worker-profile/)) shows the
   methodology.
2. **Form a hypothesis.** "I think X is slow because Y."
3. **Measure before changing.** Capture the bench numbers for
   the affected gates.
4. **Make the change. Re-measure.** If it doesn't move by 20%+,
   it's noise — don't land.
5. **Commit with receipts.** Numbers before / after, system
   load, methodology in the body.

## Documentation

Every architectural change updates the architecture chapter. The
pattern:

- **Mental model opener** with a real-world analogy.
- **Source-line walkthroughs** of load-bearing code paths, not
  just pointers.
- **Diagrams at every concept boundary** — Mermaid sequence for
  flows, ASCII for layouts.
- **"Tried & rejected" sidebar** where the design has rejected
  alternatives on record.
- **"Further reading" footer** linking to relevant
  prior-art (Tokio, Go runtime, papers).

Docs that don't compile are worse than no docs — every code
sample in `docs/` must compile against `src/lib.zig`. Verify
via the cookbook recipes or by pasting into a scratch project.

## Code principles

These show up across the source tree:

1. **Correctness first.** Port well-known algorithms (Tokio WSQ,
   parking_lot mutex, Vyukov ring) rather than invent.
2. **Comments explain WHY, not WHAT.** Ordering constraints,
   hidden races, platform behaviour. Identifier names should
   explain what.
3. **Atomic ops need ordering justification.** Every `.acq_rel`
   / `.release` / `.acquire` should be paired against a matching
   read or write in the [memory-model](/architecture/memory-model/)
   doc.
4. **No raw pointers across yield points.** A coroutine can
   resume on a different worker thread. Re-read any threadlocal-
   cached pointers after every potential yield.
5. **Stackful means stack contents preserved across suspension.**
   Heap pointers stashed on a coroutine's stack live as long as
   the coroutine. This is what makes the synchronous-shape API
   possible.
6. **Explicit allocators.** Zig idiom. One allocator in
   `Runtime.Config`; the runtime hands it to primitives that
   need it.

## Reporting bugs

Open a GitHub issue with:

- Zig version (`zig version`).
- Platform (`uname -a`).
- A minimal reproducer (compileable Zig program).
- Expected vs actual behaviour.

For runtime hangs, `Runtime.dumpState()` writes the scheduler's
atomic state to stderr — include that in the report.

## Further reading

- [Architecture](/architecture/) — full chapter on how the runtime is built.
- [Performance](/performance/) — bench methodology, postmortems, profile receipts.
- [Roadmap](/appendix/roadmap/) — what's planned, what's out of scope.
