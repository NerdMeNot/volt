//! Context-switch dispatch shim.
//!
//! Picks the per-arch context-switch backend at comptime based on
//! `builtin.cpu.arch`. Re-exports the public surface (`Context`
//! type, `swap`, `initContext`, `voltCoroEntry`) so the rest of the
//! codebase imports a single file regardless of which CPU is
//! underneath.
//!
//! Each backend lives in its own file:
//!   * `context_arm64.zig`  — AAPCS64 wide-save (Darwin/Linux/Windows)
//!   * `context_x86_64.zig` — System V x86-64 (Linux + macOS) and
//!                            Microsoft x64 (Windows); per-OS branch
//!                            inside the file
//!
//! Adding an arch is a self-contained file + an entry in the
//! switch below. The interface (`Context`, `swap(*Context,
//! *Context) void`, `initContext(*Context, stack_top, closure)`,
//! `voltCoroEntry()`) stays stable; backends differ only in
//! register set + asm.

const builtin = @import("builtin");

const backend = switch (builtin.cpu.arch) {
    .aarch64 => @import("context_arm64.zig"),
    .x86_64 => @import("context_x86_64.zig"),
    else => @compileError("Volt: no context switch for this arch — see src/context.zig"),
};

pub const Context = backend.Context;
pub const swap = backend.swap;
pub const initContext = backend.initContext;
pub const voltCoroEntry = backend.voltCoroEntry;
