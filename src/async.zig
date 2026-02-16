//! # Async Combinators and Utilities
//!
//! Provides composable async operations for coordinating futures:
//!
//! ## Combinators
//!
//! | Combinator | Description |
//! |------------|-------------|
//! | `Timeout` | Wrap a future with a deadline |
//! | `Select2`, `Select3` | Race 2-3 heterogeneous futures |
//! | `Join2`, `Join3` | Wait for 2-3 heterogeneous futures |
//! | `SelectN` | Race N homogeneous futures |
//! | `JoinN` | Wait for N homogeneous futures |
//! | `Race` | Like Promise.race() - returns first value |
//! | `All` | Like Promise.all() - returns all values |
//!
//! ## Quick Start
//!
//! ```zig
//! const volt = @import("volt");
//!
//! // Timeout: Cancel if operation takes too long
//! var timeout = volt.async_ops.Timeout(ReadFuture).init(reader.read(&buf), volt.Duration.fromSecs(5));
//!
//! // Select: Race two different types
//! var select = volt.async_ops.Select2(AcceptFuture, ShutdownFuture).init(accept, shutdown);
//!
//! // Join: Wait for both (different types)
//! var join = volt.async_ops.Join2(UserFuture, PostsFuture).init(fetchUser, fetchPosts);
//!
//! // SelectN: Race many of same type
//! var futures = [_]FetchFuture{ fetch(url1), fetch(url2), fetch(url3) };
//! var selectN = volt.async_ops.SelectN(FetchFuture, 3).init(&futures);
//!
//! // Race: Simpler API - just get the first value
//! var racer = volt.async_ops.Race(FetchFuture, 3).init(&futures);
//!
//! // All: Wait for all, get array of results
//! var joiner = volt.async_ops.All(FetchFuture, 3).init(&futures);
//! ```
//!
//! ## Pattern: Server with Graceful Shutdown
//!
//! ```zig
//! fn runServer(listener: *TcpListener, shutdown: *ShutdownSignal) !void {
//!     while (true) {
//!         var select = async_ops.Select2(AcceptFuture, ShutdownFuture).init(
//!             listener.accept(),
//!             shutdown.wait(),
//!         );
//!
//!         switch (select.poll(waker)) {
//!             .first => |conn| spawnHandler(conn),
//!             .second => |_| {
//!                 log.info("Shutdown signal received", .{});
//!                 break;
//!             },
//!             .pending => suspend(),
//!         }
//!     }
//! }
//! ```
//!
//! ## Pattern: Parallel Fetch with Timeout
//!
//! ```zig
//! fn fetchUserData(id: u64) !UserData {
//!     // Fetch user and posts concurrently
//!     var join = async_ops.Join2(UserFuture, PostsFuture).init(
//!         fetchUser(id),
//!         fetchPosts(id),
//!     );
//!
//!     // With 5 second timeout
//!     var timeout = async_ops.Timeout(@TypeOf(join)).init(
//!         join,
//!         Duration.fromSecs(5),
//!     );
//!
//!     // Poll until complete
//!     while (true) {
//!         switch (timeout.poll(waker)) {
//!             .ready => |results| return .{
//!                 .user = results[0],
//!                 .posts = results[1],
//!             },
//!             .timeout => return error.TimedOut,
//!             .pending => suspend(),
//!             .err => |e| return e,
//!         }
//!     }
//! }
//! ```

const combinators = @import("async/combinators.zig");

// ═══════════════════════════════════════════════════════════════════════════════
// Core Types
// ═══════════════════════════════════════════════════════════════════════════════

/// Generic poll result for futures.
pub const PollResult = combinators.PollResult;

/// Timeout-aware poll result.
pub const TimeoutResult = combinators.TimeoutResult;

/// Simple waker type.
pub const Waker = combinators.Waker;

// ═══════════════════════════════════════════════════════════════════════════════
// Timeout
// ═══════════════════════════════════════════════════════════════════════════════

/// Wrap a future with a timeout.
///
/// If the inner future doesn't complete before the deadline, `.timeout`
/// is returned and the future is effectively cancelled.
pub const Timeout = combinators.Timeout;

/// Convenience alias for timeout with select semantics.
pub const SelectWithTimeout = combinators.SelectWithTimeout;

// ═══════════════════════════════════════════════════════════════════════════════
// Select (Race)
// ═══════════════════════════════════════════════════════════════════════════════

/// Race two futures - returns whichever completes first.
pub const Select2 = combinators.Select2;

/// Race three futures.
pub const Select3 = combinators.Select3;

/// Result type for Select2.
pub const Select2Result = combinators.Select2Result;

/// Result type for Select3.
pub const Select3Result = combinators.Select3Result;

// ═══════════════════════════════════════════════════════════════════════════════
// Join (Parallel)
// ═══════════════════════════════════════════════════════════════════════════════

/// Wait for two futures to complete concurrently.
pub const Join2 = combinators.Join2;

/// Wait for three futures to complete concurrently.
pub const Join3 = combinators.Join3;

/// Result type for Join2.
pub const Join2Result = combinators.Join2Result;

/// Result type for Join3.
pub const Join3Result = combinators.Join3Result;

// ═══════════════════════════════════════════════════════════════════════════════
// SelectN/JoinN (Homogeneous, arbitrary count)
// ═══════════════════════════════════════════════════════════════════════════════

/// Race N futures of the same type.
///
/// ## Example
///
/// ```zig
/// var futures = [_]FetchFuture{ fetch(url1), fetch(url2), fetch(url3) };
/// var select = io.SelectN(FetchFuture, 3).init(&futures);
///
/// switch (select.poll(waker)) {
///     .ready => |result| {
///         std.debug.print("Future {} won with {}\n", .{result.index, result.value});
///     },
///     .pending => {},
///     .err => |e| return e.@"error",
/// }
/// ```
pub const SelectN = combinators.SelectN;

/// Wait for N futures of the same type to complete.
///
/// ## Example
///
/// ```zig
/// var futures = [_]FetchFuture{ fetch(url1), fetch(url2), fetch(url3) };
/// var join = io.JoinN(FetchFuture, 3).init(&futures);
///
/// switch (join.poll(waker)) {
///     .ready => |results| {
///         for (results) |r| process(r);
///     },
///     .pending => {},
///     .err => |e| return e.@"error",
/// }
/// ```
pub const JoinN = combinators.JoinN;

/// Result type for SelectN.
pub const SelectNResult = combinators.SelectNResult;

/// Result type for JoinN.
pub const JoinNResult = combinators.JoinNResult;

// ═══════════════════════════════════════════════════════════════════════════════
// Race and All (Promise.race / Promise.all style)
// ═══════════════════════════════════════════════════════════════════════════════

/// Race N futures, returning the first value (like Promise.race).
///
/// Simpler than SelectN when you don't care which future won.
///
/// ## Example
///
/// ```zig
/// var futures = [_]FetchFuture{ fetch(url1), fetch(url2) };
/// var racer = io.Race(FetchFuture, 2).init(&futures);
///
/// switch (racer.poll(waker)) {
///     .ok => |value| return value,
///     .err => |e| return e,
///     .pending => {},
/// }
/// ```
pub const Race = combinators.Race;

/// Wait for all N futures (like Promise.all).
///
/// Simpler than JoinN when you don't need error index information.
///
/// ## Example
///
/// ```zig
/// var futures = [_]FetchFuture{ fetch(url1), fetch(url2) };
/// var joiner = io.All(FetchFuture, 2).init(&futures);
///
/// switch (joiner.poll(waker)) {
///     .ok => |values| {
///         for (values) |v| process(v);
///     },
///     .err => |e| return e,
///     .pending => {},
/// }
/// ```
pub const All = combinators.All;

/// Result type for Race.
pub const RaceResult = combinators.RaceResult;

/// Result type for All.
pub const AllResult = combinators.AllResult;

// ═══════════════════════════════════════════════════════════════════════════════
// Convenience Functions
// ═══════════════════════════════════════════════════════════════════════════════

const time = @import("time.zig");

/// Create a timeout wrapper for a future.
pub fn timeout(comptime FutureType: type, future: FutureType, duration: time.Duration) Timeout(FutureType) {
    return Timeout(FutureType).init(future, duration);
}

/// Create a select2 combinator.
pub fn select2(comptime F1: type, comptime F2: type, f1: F1, f2: F2) Select2(F1, F2) {
    return Select2(F1, F2).init(f1, f2);
}

/// Create a join2 combinator.
pub fn join2(comptime F1: type, comptime F2: type, f1: F1, f2: F2) Join2(F1, F2) {
    return Join2(F1, F2).init(f1, f2);
}

/// Create a SelectN combinator (race N homogeneous futures).
pub fn selectN(comptime FutureType: type, comptime N: usize, futures: *[N]FutureType) SelectN(FutureType, N) {
    return SelectN(FutureType, N).init(futures);
}

/// Create a JoinN combinator (wait for all N homogeneous futures).
pub fn joinN(comptime FutureType: type, comptime N: usize, futures: *[N]FutureType) JoinN(FutureType, N) {
    return JoinN(FutureType, N).init(futures);
}

/// Create a Race combinator (like Promise.race).
pub fn race(comptime FutureType: type, comptime N: usize, futures: *[N]FutureType) Race(FutureType, N) {
    return Race(FutureType, N).init(futures);
}

/// Create an All combinator (like Promise.all).
pub fn all(comptime FutureType: type, comptime N: usize, futures: *[N]FutureType) All(FutureType, N) {
    return All(FutureType, N).init(futures);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

test {
    _ = combinators;
}
