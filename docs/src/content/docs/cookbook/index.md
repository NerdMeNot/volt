---
title: Cookbook
description: Practical recipes and patterns for building real applications with Volt.
---

You learn a runtime by building things with it, not by reading API signatures. These recipes are real programs -- each one solves a problem you will actually encounter when building networked services in Zig.

Welcome to the Volt cookbook. This is a collection of self-contained, practical recipes that show you how to solve the problems you will actually encounter when building networked services, concurrent pipelines, and production systems with Volt. Each recipe combines the core APIs -- networking, channels, sync primitives, and timers -- into patterns you can adapt and drop into your own projects.

:::tip[Where to Start]
If you are new to Volt, begin with **[Echo Server](/cookbook/echo-server)** to learn the networking fundamentals, then move to **[Timeout Patterns](/cookbook/timeout-patterns)** to see how timers and deadlines work. Together, these two recipes cover the building blocks used by every other recipe in the cookbook.
:::

## Recipes at a Glance

The recipes are ordered from foundational concepts to more involved production patterns. Work through the beginner recipes first -- the intermediate ones build directly on the ideas introduced there.

### Getting Started

These recipes cover the core APIs and are the best entry points into the library.

| Recipe | Difficulty | What You Will Build |
|--------|-----------|---------------------|
| [Echo Server](/cookbook/echo-server) | **Beginner** | A TCP server that accepts connections and echoes data back. Covers `TcpListener`, `TcpStream`, and spawning a task per connection. |
| [HTTP Server](/cookbook/http-server) | **Beginner/Intermediate** | An HTTP/1.1 server from raw TCP -- request parsing, path routing, static file serving, and JSON responses. |
| [Timeout Patterns](/cookbook/timeout-patterns) | **Beginner** | Deadline checks around operations, retry with exponential backoff, and deadline propagation across nested calls. |
| [Parallel Tasks](/cookbook/parallel-tasks) | **Beginner** | Health-check dashboard using `io.@"async"`, `f.@"await"(io)`, and the Group pattern to probe services concurrently. |
| [Producer/Consumer](/cookbook/producer-consumer) | **Beginner** | A job queue built on bounded `Channel` with multiple producers, multiple consumers, backpressure, and clean shutdown via poison pill. |

### Networking and Messaging

Patterns for real-time communication and event distribution between clients and services.

| Recipe | Difficulty | What You Will Build |
|--------|-----------|---------------------|
| [Chat Server](/cookbook/chat-server) | **Intermediate** | A multi-client chat server using `BroadcastChannel` for message fan-out, with per-client tasks and disconnect handling. |
| [Pub/Sub System](/cookbook/pub-sub) | **Intermediate** | An event bus with typed events and topic-based routing, built on `BroadcastChannel` with subscriber lifecycle management and backpressure handling. |

### Resource Management

Techniques for controlling access to shared resources -- connection pools, rate limits, and semaphore-gated concurrency.

| Recipe | Difficulty | What You Will Build |
|--------|-----------|---------------------|
| [Connection Pool](/cookbook/connection-pool) | **Intermediate** | A semaphore-gated resource pool with acquire/release, health checks on checkout, timeout on acquire, and pool sizing guidance. |
| [Rate Limiter](/cookbook/rate-limiter) | **Intermediate** | A token bucket rate limiter built on `Semaphore` and `Interval`, with per-client limiting and a sliding window variant. |

### Data Processing

File and process pipelines for reading, transforming, and writing data.

| Recipe | Difficulty | What You Will Build |
|--------|-----------|---------------------|
| [File Pipeline](/cookbook/file-pipeline) | **Intermediate** | A log analyzer using `readDir`, `BufReader`, `Lines` for line-by-line processing, atomic writes, and parallel file I/O with `io.concurrent`. |
| [Process Pipeline](/cookbook/process-pipeline) | **Intermediate** | A build orchestrator that runs subprocesses with `Command`, captures stdout/stderr, chains commands, and enforces timeouts. |

### Lifecycle and Operations

Production concerns: shutdown sequencing, configuration changes, and keeping the event loop responsive under mixed workloads.

| Recipe | Difficulty | What You Will Build |
|--------|-----------|---------------------|
| [Work Offloading](/cookbook/work-offloading) | **Intermediate** | CPU-bound vs I/O-bound separation using the blocking pool (and, in the future, Blitz). Keeps compute-heavy work from starving the event loop. |
| [Config Hot-Reload](/cookbook/config-hot-reload) | **Intermediate** | Live configuration updates via `Watch` channel. File-change detection triggers an atomic swap that notifies all consumers without restarts. |
| [Graceful Drain](/cookbook/graceful-drain) | **Intermediate** | Signal-triggered shutdown using `Shutdown` and `WorkGuard`. Drains in-flight connections, tracks outstanding requests, and performs ordered cleanup. |

### Advanced

Capstone recipes that combine multiple patterns into realistic applications.

| Recipe | Difficulty | What You Will Build |
|--------|-----------|---------------------|
| [Service Mesh](/cookbook/service-mesh) | **Advanced** | A reverse proxy combining connection pooling, rate limiting, health checks, graceful shutdown, and backend routing into one application. |

## Prerequisites

All recipes assume you have:

1. **Zig 0.15.2+** installed
2. **Volt** added as a dependency in `build.zig.zon`
3. Familiarity with basic Zig syntax (error unions, optionals, slices)

If any recipe uses a feature beyond these basics, it will call that out at the top.

## How to Read the Recipes

Every recipe follows a consistent structure so you can quickly find what you need:

- **Problem statement** -- what we are building and why it matters
- **Complete code** -- a working example you can copy directly into your project
- **Walkthrough** -- step-by-step explanation of the key design decisions
- **Variations** -- alternative approaches, extensions, and trade-offs to consider

Code examples use the `io` import alias throughout:

```zig
const volt = @import("volt");
```

All examples target the non-blocking polling API (`tryAccept`, `tryRead`, `trySend`, `tryRecv`), which works today without requiring the full async runtime scheduler. When the scheduler-integrated Future API is used, it is called out explicitly.
