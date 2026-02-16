---
title: Layered Architecture
description: Structuring a Volt application with dependency injection, layered services, and a clean separation between container and context.
---

Real applications are not a single function. They have layers -- handlers that parse requests, services that enforce business rules, repositories that talk to databases. Dependencies like a DB pool or logger live for the lifetime of the app, while things like the `io` handle or a request ID are scoped to a single request.

This recipe shows how to structure a Volt application the same way you would in Go, Java, or TypeScript: a **container** for long-lived dependencies, a **context** for per-request state, and clean layer boundaries.

:::tip[What you will learn]
- **Container vs Context** -- what goes where and why
- **Layered call flow** -- handler → service → repository with clear boundaries
- **Parallel queries in the repo layer** -- using `io` from the context without polluting every function signature
- **Initialization in main** -- wiring the container once, creating contexts per request
:::

## The Pattern

```
┌─────────────────────────────────────────────────────────┐
│  main()                                                 │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Container (app-level, lives forever)             │  │
│  │  • db_pool, logger, config, cache                 │  │
│  └───────────────────────────────────────────────────┘  │
│                          │                              │
│                    volt.run(serve)                       │
│                          │                              │
│  ┌───────────────────────▼───────────────────────────┐  │
│  │  Per-request Context                              │  │
│  │  • io: volt.Io                                    │  │
│  │  • request_id: u64                                │  │
│  └───────────────────────┬───────────────────────────┘  │
│                          │                              │
│            handler → service → repository               │
└─────────────────────────────────────────────────────────┘
```

**Container** holds things created once at startup: DB pool, logger, config. It outlives every request.

**Context** holds things scoped to a unit of work: the `io` handle, a request ID, a trace ID. It is created per request and discarded when the request ends.

The distinction matters: the container is shared and read-only during request handling. The context is unique to each request and carries the async runtime handle.

## Complete Example

```zig
const std = @import("std");
const volt = @import("volt");

// ── Container ────────────────────────────────────────────────────────────────
// Created once in main(). Shared across all requests. Read-only after init.

const Container = struct {
    logger: Logger,
    db: DbPool,
    cache: Cache,
    config: Config,
};

// ── Context ──────────────────────────────────────────────────────────────────
// Created per request. Carries the runtime handle and request-scoped data.

const Ctx = struct {
    io: volt.Io,
    request_id: u64,
    container: *const Container,

    // Convenience accessors so layers don't reach through two levels.
    pub fn logger(self: *const Ctx) *const Logger {
        return &self.container.logger;
    }

    pub fn db(self: *const Ctx) *const DbPool {
        return &self.container.db;
    }

    pub fn cache(self: *const Ctx) *const Cache {
        return &self.container.cache;
    }
};

// ── Entry point ──────────────────────────────────────────────────────────────

pub fn main() !void {
    // Wire the container once. Everything here lives for the process lifetime.
    var container = Container{
        .logger = Logger.init(.info),
        .db = DbPool.init("postgres://localhost:5432/myapp", 20),
        .cache = Cache.init("redis://localhost:6379"),
        .config = Config{
            .port = 8080,
            .max_connections = 10_000,
        },
    };

    container.logger.log(.info, "starting server on :{d}", container.config.port);

    // Start the runtime. The container pointer is captured by the closure
    // and passed into the async world.
    try volt.runWith(std.heap.page_allocator, .{
        .num_workers = 4,
    }, struct {
        fn entry(io: volt.Io) void {
            serve(io, &container);
        }
    }.entry);
}

// ── Accept loop ──────────────────────────────────────────────────────────────

var next_request_id: u64 = 0;

fn serve(io: volt.Io, container: *const Container) void {
    var listener = volt.net.listen("0.0.0.0:8080") catch return;
    defer listener.close();

    while (listener.tryAccept() catch null) |result| {
        next_request_id += 1;

        // Each request gets its own context.
        var f = io.@"async"(handleRequest, .{
            Ctx{
                .io = io,
                .request_id = next_request_id,
                .container = container,
            },
            result.stream,
        }) catch continue;
        f.detach();
    }
}

// ── Handler layer ────────────────────────────────────────────────────────────
// Parses HTTP, delegates to the service, writes the response.
// Knows about HTTP. Does NOT know about SQL or cache keys.

fn handleRequest(ctx: Ctx, conn: volt.net.TcpStream) void {
    var stream = conn;
    defer stream.close();

    ctx.logger().log(.info, "[req:{d}] incoming request", ctx.request_id);

    // Parse the request (simplified -- see HTTP Server recipe for full parser).
    var buf: [4096]u8 = undefined;
    const n = stream.tryRead(&buf) catch return orelse return;
    const path = parsePath(buf[0..n]);

    // Route
    if (std.mem.startsWith(u8, path, "/users/")) {
        const id = parseUserId(path) orelse {
            stream.writeAll(httpResponse(400, "bad user id")) catch return;
            return;
        };

        // Call the service layer. Pass ctx, not io.
        const profile = UserService.getProfile(&ctx, id) catch |err| {
            ctx.logger().log(.err, "[req:{d}] service error: {}", ctx.request_id, err);
            stream.writeAll(httpResponse(500, "internal error")) catch return;
            return;
        };

        ctx.logger().log(.info, "[req:{d}] found user: {s}", ctx.request_id, profile.name);
        stream.writeAll(httpResponse(200, profile.name)) catch return;
    } else {
        stream.writeAll(httpResponse(404, "not found")) catch return;
    }
}

// ── Service layer ────────────────────────────────────────────────────────────
// Business logic. Orchestrates calls to repositories.
// Knows about domain rules. Does NOT know about HTTP or SQL.

const UserService = struct {
    pub fn getProfile(ctx: *const Ctx, user_id: u64) !UserProfile {
        // Business rule: check cache first, fall back to DB.
        if (UserCache.get(ctx, user_id)) |cached| {
            return cached;
        }

        // Fetch user and posts in parallel -- this is where io is used.
        var user_f = try ctx.io.@"async"(UserRepo.findById, .{ ctx, user_id });
        var posts_f = try ctx.io.@"async"(PostRepo.countByUser, .{ ctx, user_id });
        const user = try user_f.@"await"(ctx.io);
        const post_count = try posts_f.@"await"(ctx.io);

        const profile = UserProfile{
            .id = user.id,
            .name = user.name,
            .email = user.email,
            .post_count = post_count,
        };

        // Cache for next time.
        UserCache.put(ctx, user_id, profile);

        return profile;
    }
};

// ── Repository layer ─────────────────────────────────────────────────────────
// Data access. Talks to databases, caches, external APIs.
// Knows about SQL and cache keys. Does NOT know about business rules.

const UserRepo = struct {
    pub fn findById(ctx: *const Ctx, user_id: u64) !User {
        ctx.logger().log(.debug, "[req:{d}] querying user {d}", ctx.request_id, user_id);
        // In production: ctx.db().query("SELECT * FROM users WHERE id = $1", user_id)
        return User{ .id = user_id, .name = "Alice", .email = "alice@example.com" };
    }
};

const PostRepo = struct {
    pub fn countByUser(ctx: *const Ctx, user_id: u64) !u32 {
        ctx.logger().log(.debug, "[req:{d}] counting posts for user {d}", ctx.request_id, user_id);
        // In production: ctx.db().queryScalar("SELECT COUNT(*) FROM posts WHERE user_id = $1", user_id)
        return 42;
    }
};

const UserCache = struct {
    pub fn get(ctx: *const Ctx, user_id: u64) ?UserProfile {
        _ = ctx;
        _ = user_id;
        // In production: ctx.cache().get("user:{d}", user_id)
        return null; // Cache miss
    }

    pub fn put(ctx: *const Ctx, user_id: u64, profile: UserProfile) void {
        _ = ctx;
        _ = user_id;
        _ = profile;
        // In production: ctx.cache().set("user:{d}", user_id, serialize(profile))
    }
};

// ── Domain types ─────────────────────────────────────────────────────────────

const User = struct {
    id: u64,
    name: []const u8,
    email: []const u8,
};

const UserProfile = struct {
    id: u64,
    name: []const u8,
    email: []const u8,
    post_count: u32,
};

const Config = struct {
    port: u16,
    max_connections: u32,
};

// ── Infrastructure stubs ─────────────────────────────────────────────────────
// Replace these with real implementations.

const Logger = struct {
    level: Level,
    const Level = enum { debug, info, warn, err };

    pub fn init(level: Level) Logger {
        return .{ .level = level };
    }

    pub fn log(self: *const Logger, level: Level, comptime fmt: []const u8, args: anytype) void {
        if (@intFromEnum(level) >= @intFromEnum(self.level)) {
            std.debug.print(fmt ++ "\n", args);
        }
    }
};

const DbPool = struct {
    url: []const u8,
    max_conns: u32,

    pub fn init(url: []const u8, max_conns: u32) DbPool {
        return .{ .url = url, .max_conns = max_conns };
    }
};

const Cache = struct {
    url: []const u8,

    pub fn init(url: []const u8) Cache {
        return .{ .url = url };
    }
};

// ── HTTP helpers (minimal) ───────────────────────────────────────────────────

fn parsePath(raw: []const u8) []const u8 {
    // "GET /users/42 HTTP/1.1\r\n..." → "/users/42"
    var it = std.mem.splitScalar(u8, raw, ' ');
    _ = it.next(); // skip method
    return it.next() orelse "/";
}

fn parseUserId(path: []const u8) ?u64 {
    // "/users/42" → 42
    const prefix = "/users/";
    if (!std.mem.startsWith(u8, path, prefix)) return null;
    return std.fmt.parseInt(u64, path[prefix.len..], 10) catch null;
}

fn httpResponse(status: u16, body: []const u8) []const u8 {
    _ = status;
    return body;
}
```

## How It Flows

```
Request arrives
  │
  ▼
handleRequest(ctx, stream)          ← Handler: parses HTTP, routes
  │
  ▼
UserService.getProfile(ctx, 42)     ← Service: business logic, orchestration
  │
  ├──► UserRepo.findById(ctx, 42)   ← Repo: DB query (spawned as task)
  │
  ├──► PostRepo.countByUser(ctx, 42) ← Repo: DB query (spawned as task)
  │
  ▼  (await both futures)
  │
  ▼
UserCache.put(ctx, 42, profile)     ← Repo: cache write
  │
  ▼
Response written
```

The `io` handle travels inside `ctx`. The handler doesn't use `io` directly -- it calls the service, which uses `ctx.io` to spawn parallel queries. The repos receive `ctx` for logging and DB access but don't spawn tasks themselves.

## Why This Split

| | Container | Context |
|---|---|---|
| **Lifetime** | Entire process | Single request |
| **Created in** | `main()` | Accept loop |
| **Contains** | DB pool, logger, config, cache | `io`, request ID, trace ID |
| **Shared across** | All requests | One request only |
| **Mutability** | Read-only after init | Unique per request |

Putting `io` in the container would be wrong -- `io` is the handle to the runtime that was started by `volt.run`. It belongs to the scope where the runtime is active. The DB pool, logger, and config exist independently of the runtime.

## Testing

This structure makes testing straightforward. Each layer can be tested in isolation:

```zig
// Test the repo without a runtime -- just call it with a test context
test "UserRepo.findById returns user" {
    // Tier 1 tests: no runtime needed, no io needed
    const user = try UserRepo.findById(&test_ctx, 42);
    try std.testing.expectEqualStrings("Alice", user.name);
}

// Test the service with a runtime -- it needs io for parallel queries
test "UserService.getProfile fetches in parallel" {
    try volt.run(struct {
        fn entry(io: volt.Io) !void {
            var ctx = Ctx{
                .io = io,
                .request_id = 1,
                .container = &test_container,
            };
            const profile = try UserService.getProfile(&ctx, 42);
            try std.testing.expectEqual(@as(u32, 42), profile.post_count);
        }
    }.entry);
}
```

The handler layer is tested with integration tests (full HTTP round-trip). The service layer is tested with the runtime. The repo layer is tested without any runtime at all.
