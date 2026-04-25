---
title: Config Hot Reload
description: Live configuration updates using Watch channel with file-change
  detection, atomic swap, and consumer notification.
slug: v1.0.0-zig0.15.2/cookbook/config-hot-reload
---

Many servers need to update configuration at runtime without restarting -- feature flags, rate limits, logging levels, database connection strings. The `Watch` channel is designed for exactly this pattern: it holds a single value, and all subscribers are notified when it changes.

:::tip[What you'll learn]
* **Watch channel** for single-value broadcast to many consumers
* **File change detection** with modification time comparison
* **Atomic config swap** so readers never see a half-updated struct
* **Version-based change tracking** so workers skip redundant processing
* **SIGHUP-triggered reload** for operator-friendly config updates
:::

## Architecture

```
┌──────────────┐      send()      ┌──────────────────┐
│ Config Loader │ ──────────────> │   Watch(Config)   │
│ (file watcher)│                 │ (single value)    │
└──────────────┘                  └────────┬─────────┘
                                    subscribe()
                              ┌──────────┼──────────┐
                              ▼          ▼          ▼
                          Worker 1   Worker 2   Worker 3
                         (borrow)   (borrow)   (borrow)
```

## Complete Example

```zig
const std = @import("std");
const volt = @import("volt");

/// Application configuration that can be reloaded at runtime.
/// All fields have safe defaults so the application can start
/// before the first config file is loaded.
pub const AppConfig = struct {
    // -- Server limits --
    max_connections: u32 = 100,
    rate_limit_rps: u32 = 1000,
    request_timeout_ms: u32 = 30_000,

    // -- Logging --
    log_level: LogLevel = .info,

    // -- Caching --
    enable_cache: bool = true,
    cache_ttl_secs: u32 = 300,
    cache_max_entries: u32 = 10_000,

    // -- Database --
    db_url: []const u8 = "postgresql://localhost:5432/app",
    db_pool_size: u16 = 10,
    db_statement_timeout_ms: u32 = 5_000,

    // -- Feature flags --
    enable_beta_api: bool = false,
    enable_request_logging: bool = true,
    enable_rate_limiting: bool = true,
    maintenance_mode: bool = false,

    pub const LogLevel = enum { debug, info, warn, err };
};

/// Global config watch channel.
var config_watch = volt.channel.watch(AppConfig, .{});

/// Load configuration from a file.
/// In a real application this would parse JSON, TOML, or YAML.
fn loadConfig(path: []const u8) !AppConfig {
    _ = path;
    // Real implementation: read file, parse, return struct.
    // The key contract is that this function either returns a
    // fully-valid AppConfig or an error -- never a partial config.
    return AppConfig{
        .max_connections = 200,
        .rate_limit_rps = 500,
        .log_level = .debug,
        .enable_beta_api = true,
        .db_pool_size = 20,
    };
}

/// Return the last-modified time of a file, or null if the file
/// cannot be stat'd (permissions, file deleted, etc.).
fn getFileModTime(path: []const u8) ?i128 {
    const file = std.fs.cwd().openFile(path, .{}) catch return null;
    defer file.close();
    const stat = file.stat() catch return null;
    return stat.mtime;
}

/// Config reloader -- polls for file changes and updates the watch channel.
fn configReloader(path: []const u8) void {
    var last_modified: ?i128 = null;

    while (true) {
        const current_modified = getFileModTime(path);

        // Reload when:
        //   - We get a valid mtime that differs from the last one, or
        //   - We have never successfully loaded (last_modified is null).
        const should_reload = if (current_modified) |cur| blk: {
            if (last_modified) |prev| {
                break :blk cur != prev;
            }
            break :blk true; // First successful stat.
        } else false;

        if (should_reload) {
            if (loadConfig(path)) |new_config| {
                last_modified = current_modified;

                // Atomic swap -- all subscribers see the new value.
                config_watch.send(new_config);
                std.debug.print(
                    "Config reloaded: max_conn={}, rps={}, beta={}\n",
                    .{
                        new_config.max_connections,
                        new_config.rate_limit_rps,
                        new_config.enable_beta_api,
                    },
                );
            } else |err| {
                // Keep the old config. A typo in the config file
                // must not break running workers.
                std.debug.print("Config load failed: {}\n", .{err});
            }
        }

        // Poll every 5 seconds.
        std.Thread.sleep(5 * std.time.ns_per_s);
    }
}

/// Worker that reads the current config.
fn worker(worker_id: usize) void {
    var rx = config_watch.subscribe();

    while (true) {
        // Check if config has changed since we last looked.
        if (rx.hasChanged()) {
            const cfg = rx.borrow();
            std.debug.print("Worker {}: config updated, max_conn={}, beta={}\n", .{
                worker_id,
                cfg.max_connections,
                cfg.enable_beta_api,
            });
            rx.markSeen();
        }

        // Do work using current config.
        doWork(&rx);

        std.Thread.sleep(100 * std.time.ns_per_ms);
    }
}

fn doWork(rx: *volt.channel.Watch(AppConfig).Receiver) void {
    const cfg = rx.borrow();

    if (cfg.maintenance_mode) {
        // Reject requests, return 503.
        return;
    }

    if (cfg.enable_cache) {
        // Use cache with cfg.cache_ttl_secs...
    }

    if (cfg.enable_rate_limiting) {
        // Enforce cfg.rate_limit_rps...
    }
}

pub fn main() !void {
    // Start config reloader in background.
    const reloader = try std.Thread.spawn(.{}, configReloader, .{"config.toml"});
    _ = reloader;

    // Start workers.
    var workers: [4]std.Thread = undefined;
    for (&workers, 0..) |*t, i| {
        t.* = try std.Thread.spawn(.{}, worker, .{i});
    }

    // Wait for workers (in practice, run until shutdown).
    for (&workers) |*t| t.join();
}
```

## Walkthrough

### Why Watch instead of Mutex + shared pointer?

| Approach | Pros | Cons |
|----------|------|------|
| `Mutex` + shared config | Simple | Every reader takes the lock; no change notification |
| `Watch(Config)` | Lock-free reads via `borrow()`; change notification via `hasChanged()` | Slightly more complex setup |
| `RwLock` + shared config | Multiple concurrent readers | No change notification; readers still acquire lock |

`Watch` is purpose-built for this use case. `borrow()` returns a pointer to the current value with no locking on the hot path. The version counter lets consumers skip work when nothing has changed.

### Atomic config swap

`config_watch.send(new_config)` atomically replaces the stored value and bumps the version counter. All existing subscribers will see `hasChanged() == true` on their next check. The old value is overwritten -- there is no history.

### Error handling on reload

If `loadConfig` fails, the old configuration remains in effect. This is the safe default -- a typo in the config file should not crash running workers. The reloader also guards against a missing or unreadable file by returning `null` from `getFileModTime`, which prevents a reload attempt on a file that cannot be read.

## Reactive Pattern with tryRecv

Instead of polling `hasChanged()`, use `tryRecv()` to get the new value directly:

```zig
fn reactiveWorker() void {
    var rx = config_watch.subscribe();

    while (true) {
        switch (rx.tryRecv()) {
            .value => |cfg| {
                std.debug.print("New config received: {}\n", .{cfg.max_connections});
                applyConfig(cfg);
            },
            .empty => {
                // No change -- use current config.
            },
            .closed => return,
        }

        doWork();
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }
}
```

## Layered Configuration

Combine multiple watch channels for configuration that comes from different sources:

```zig
// File-based config (reloaded from disk).
var file_config = volt.channel.watch(FileConfig, default_file_config);

// Environment overrides (set once at startup, or from admin API).
var env_overrides = volt.channel.watch(EnvOverrides, .{});

/// Merge file config with environment overrides.
fn getEffectiveConfig() EffectiveConfig {
    var file_rx = file_config.subscribe();
    var env_rx = env_overrides.subscribe();

    const base = file_rx.borrow();
    const overrides = env_rx.borrow();

    return EffectiveConfig{
        .max_connections = overrides.max_connections orelse base.max_connections,
        .log_level = overrides.log_level orelse base.log_level,
        .rate_limit = base.rate_limit, // Not overridable.
    };
}
```

## Triggering Reload via Signal

Combine with `AsyncSignal` to reload config on `SIGHUP`:

```zig
fn configReloaderWithSignal(path: []const u8) void {
    var sighup = volt.signal.AsyncSignal.hangup() catch return;
    defer sighup.deinit();

    while (true) {
        // Block until SIGHUP is received.
        if (sighup.tryRecv() catch null) |_| {
            if (loadConfig(path)) |cfg| {
                config_watch.send(cfg);
                std.debug.print("Config reloaded via SIGHUP\n", .{});
            } else |err| {
                std.debug.print("Reload failed: {}\n", .{err});
            }
        }
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }
}
```

Send `kill -HUP <pid>` to trigger a reload without restarting the process.

## Try It Yourself

The example above covers the core pattern. Here are three extensions that production systems typically need.

### 1. Config Validation Before Applying

Never push an invalid config to subscribers. Validate before calling `send()`:

```zig
const ConfigError = error{
    MaxConnectionsTooLow,
    RateLimitZero,
    InvalidDbUrl,
    PoolSizeTooLarge,
    TimeoutTooShort,
};

/// Validate a config before it is applied. Returns an error
/// describing the first problem found, or void on success.
fn validateConfig(cfg: AppConfig) ConfigError!void {
    if (cfg.max_connections < 1) return error.MaxConnectionsTooLow;
    if (cfg.rate_limit_rps == 0 and cfg.enable_rate_limiting)
        return error.RateLimitZero;
    if (cfg.db_url.len == 0) return error.InvalidDbUrl;
    if (cfg.db_pool_size > 500) return error.PoolSizeTooLarge;
    if (cfg.request_timeout_ms < 100) return error.TimeoutTooShort;
}

fn configReloaderValidated(path: []const u8) void {
    var last_modified: ?i128 = null;

    while (true) {
        const current_modified = getFileModTime(path);

        const should_reload = if (current_modified) |cur| blk: {
            if (last_modified) |prev| break :blk cur != prev;
            break :blk true;
        } else false;

        if (should_reload) {
            if (loadConfig(path)) |new_config| {
                // Gate: reject invalid configs before they reach subscribers.
                validateConfig(new_config) catch |err| {
                    std.debug.print(
                        "Config validation failed: {} -- keeping old config\n",
                        .{err},
                    );
                    // Do NOT update last_modified so we retry on the next
                    // cycle in case the operator fixes the file quickly.
                    std.Thread.sleep(5 * std.time.ns_per_s);
                    continue;
                };

                last_modified = current_modified;
                config_watch.send(new_config);
                std.debug.print("Config reloaded (validated)\n", .{});
            } else |err| {
                std.debug.print("Config load failed: {}\n", .{err});
            }
        }

        std.Thread.sleep(5 * std.time.ns_per_s);
    }
}
```

### 2. Config Change Log

Track what changed between versions so operators can see a clear audit trail:

```zig
/// Compare two configs and print every field that differs.
fn logConfigDiff(old: AppConfig, new: AppConfig) void {
    if (old.max_connections != new.max_connections)
        std.debug.print("  max_connections: {} -> {}\n", .{ old.max_connections, new.max_connections });
    if (old.rate_limit_rps != new.rate_limit_rps)
        std.debug.print("  rate_limit_rps: {} -> {}\n", .{ old.rate_limit_rps, new.rate_limit_rps });
    if (old.log_level != new.log_level)
        std.debug.print("  log_level: {} -> {}\n", .{ @intFromEnum(old.log_level), @intFromEnum(new.log_level) });
    if (old.enable_cache != new.enable_cache)
        std.debug.print("  enable_cache: {} -> {}\n", .{ old.enable_cache, new.enable_cache });
    if (old.cache_ttl_secs != new.cache_ttl_secs)
        std.debug.print("  cache_ttl_secs: {} -> {}\n", .{ old.cache_ttl_secs, new.cache_ttl_secs });
    if (old.db_pool_size != new.db_pool_size)
        std.debug.print("  db_pool_size: {} -> {}\n", .{ old.db_pool_size, new.db_pool_size });
    if (old.enable_beta_api != new.enable_beta_api)
        std.debug.print("  enable_beta_api: {} -> {}\n", .{ old.enable_beta_api, new.enable_beta_api });
    if (old.maintenance_mode != new.maintenance_mode)
        std.debug.print("  maintenance_mode: {} -> {}\n", .{ old.maintenance_mode, new.maintenance_mode });
}

/// Use the diff in the reload loop:
fn reloadWithChangeLog(path: []const u8) void {
    var last_modified: ?i128 = null;
    var current_config = AppConfig{}; // Start with defaults.

    while (true) {
        const mtime = getFileModTime(path);
        const should_reload = if (mtime) |cur| blk: {
            if (last_modified) |prev| break :blk cur != prev;
            break :blk true;
        } else false;

        if (should_reload) {
            if (loadConfig(path)) |new_config| {
                std.debug.print("Config change detected:\n", .{});
                logConfigDiff(current_config, new_config);

                current_config = new_config;
                last_modified = mtime;
                config_watch.send(new_config);
            } else |err| {
                std.debug.print("Config load failed: {}\n", .{err});
            }
        }

        std.Thread.sleep(5 * std.time.ns_per_s);
    }
}
```

### 3. Config Rollback on Error

Keep the previous config around so you can revert if the new config causes problems at the application layer (for example, a database URL that parses correctly but the server is unreachable):

```zig
/// Attempt to apply config to live subsystems. Returns an error
/// if any subsystem rejects the new values.
fn tryApplyConfig(cfg: AppConfig) !void {
    // Example: verify the database is reachable with the new URL
    // and pool size before committing.
    try verifyDbConnection(cfg.db_url, cfg.db_pool_size);

    // Example: verify the cache can be resized.
    try resizeCachePool(cfg.cache_max_entries);
}

fn reloadWithRollback(path: []const u8) void {
    var last_modified: ?i128 = null;
    var previous_config = AppConfig{}; // Safe fallback.

    while (true) {
        const mtime = getFileModTime(path);
        const should_reload = if (mtime) |cur| blk: {
            if (last_modified) |prev| break :blk cur != prev;
            break :blk true;
        } else false;

        if (should_reload) {
            if (loadConfig(path)) |new_config| {
                validateConfig(new_config) catch |err| {
                    std.debug.print("Validation failed: {}\n", .{err});
                    std.Thread.sleep(5 * std.time.ns_per_s);
                    continue;
                };

                // Snapshot the current config before applying.
                const rollback_config = previous_config;

                // Try to apply the new config to live subsystems.
                tryApplyConfig(new_config) catch |err| {
                    std.debug.print(
                        "Apply failed: {} -- rolling back\n",
                        .{err},
                    );
                    // Restore the previous known-good config.
                    config_watch.send(rollback_config);
                    std.Thread.sleep(5 * std.time.ns_per_s);
                    continue;
                };

                // Success -- commit.
                previous_config = new_config;
                last_modified = mtime;
                config_watch.send(new_config);
                std.debug.print("Config applied successfully\n", .{});
            } else |err| {
                std.debug.print("Config load failed: {}\n", .{err});
            }
        }

        std.Thread.sleep(5 * std.time.ns_per_s);
    }
}
```

The rollback pattern ensures that subscribers never see a config that causes runtime failures. The sequence is: parse, validate, apply to subsystems, and only then broadcast via `send()`. If any step fails, the previous config remains in effect.
