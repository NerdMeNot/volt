//! Architecture-dispatched context-switch primitive.
//!
//! Volt supports two architectures:
//!   - aarch64 (Apple Silicon, Linux/arm64, Windows/arm64) — see context_arm64.zig
//!   - x86_64  (Linux/x86_64, Darwin/Intel, Windows/x86_64) — see context_x86_64.zig
//!
//! This module re-exports the per-arch types and asm symbols under
//! arch-neutral names so the rest of Volt depends on `coroutine/context`,
//! not on a specific arch file.
//!
//! ## Adding a new arch
//!
//! 1. Write `context_<arch>.zig` with the same public surface (Context,
//!    FullContext, voltCtxSwap, voltCoroEntry, initContext, etc.).
//! 2. Add an arm in the `impl` switch below.
//! 3. CI matrix: add the new triple.

const std = @import("std");
const builtin = @import("builtin");

pub const impl = switch (builtin.cpu.arch) {
    .aarch64 => @import("context_arm64.zig"),
    .x86_64 => @import("context_x86_64.zig"),
    else => @compileError("Volt: unsupported CPU arch '" ++ @tagName(builtin.cpu.arch) ++
        "' — supported: aarch64, x86_64."),
};

pub const Context = impl.Context;
pub const FullContext = impl.FullContext;
pub const voltCtxSwap = impl.voltCtxSwap;
pub const voltCoroEntry = impl.voltCoroEntry;
pub const voltCtxResumeFull = impl.voltCtxResumeFull;
pub const voltCtxSwapFull = impl.voltCtxSwapFull;
pub const swap = impl.swap;
pub const initContext = impl.initContext;

test {
    _ = impl;
}
