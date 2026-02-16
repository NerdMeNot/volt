//! Test Common Utilities
//!
//! Shared test infrastructure for concurrency testing, loom-style
//! model checking, and custom assertions.
//!
//! Usage:
//! ```zig
//! const common = @import("../common.zig");
//! const Barrier = common.Barrier;
//! const loomRun = common.loomRun;
//! ```

pub const concurrency = @import("common/concurrency.zig");
pub const model = @import("common/model.zig");
pub const assertions = @import("common/assertions.zig");

// Re-export commonly used types for convenience
pub const ConcurrentCounter = concurrency.ConcurrentCounter;
pub const Latch = concurrency.Latch;
pub const Barrier = concurrency.Barrier;
pub const ThreadRng = concurrency.ThreadRng;
pub const ContentionTracker = concurrency.ContentionTracker;
pub const runConcurrent = concurrency.runConcurrent;
pub const runConcurrentDynamic = concurrency.runConcurrentDynamic;
pub const yieldThread = concurrency.yieldThread;
pub const spinBriefly = concurrency.spinBriefly;

pub const Model = model.Model;
pub const loomRun = model.loomRun;
pub const loomQuick = model.loomQuick;
pub const loomThorough = model.loomThorough;
pub const loomModel = model.loomModel;
pub const runWithModel = model.runWithModel;

pub const assertPending = assertions.assertPending;
pub const assertReady = assertions.assertReady;
pub const assertOk = assertions.assertOk;
pub const assertErr = assertions.assertErr;
pub const assertRecv = assertions.assertRecv;
pub const assertEmpty = assertions.assertEmpty;
pub const assertClosed = assertions.assertClosed;
pub const assertLagged = assertions.assertLagged;
pub const assertTrue = assertions.assertTrue;
pub const assertFalse = assertions.assertFalse;
pub const assertEqual = assertions.assertEqual;
pub const TimeoutChecker = assertions.TimeoutChecker;

// Get test config
pub const config = @import("test_config.zig");
pub const getConfig = config.getConfig;
pub const autoConfig = config.autoConfig;
pub const StandardConfigs = config.StandardConfigs;

test {
    // Run all submodule tests
    @import("std").testing.refAllDecls(@This());
}
