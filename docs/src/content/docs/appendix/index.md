---
title: Appendix
description: Reference material that doesn't fit elsewhere — comparisons, glossary, roadmap, contributor notes.
---

The pages here are reference-shaped: short, fact-dense, no
narrative.

- [Tokio + Go comparison](/appendix/tokio-go-comparison/) — feature-by-feature mapping. Useful if you're coming from one runtime to the other.
- [Zig 0.16 notes](/appendix/zig-0.16-notes/) — what `std` removed in 0.16 (`std.Thread.Mutex`, `std.posix.{mmap, fcntl, ...}` mid-level) and what Volt uses instead.
- [Roadmap](/appendix/roadmap/) — what's not yet implemented, and what explicitly stays out of Volt core (lives in downstream libraries).
- [Contributing](/appendix/contributing/) — bench-gate protocol, commit conventions, where to start.
- [Glossary](/getting-started/glossary/) — terminology (P, M, Parker, Park, slab, scope, etc.).
