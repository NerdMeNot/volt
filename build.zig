//! Volt build — minimal scaffold while the stackful core is rebuilt.
//!
//! Exposes the `volt` module + a single `test` step. Stress / robustness /
//! concurrency / integration / benchmark targets are gone with the stackless
//! tree; new ones come back as the new core stabilizes.

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Public module
    _ = b.addModule("volt", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Unit tests — runs every inline test in src/.
    const unit_test_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    const unit_tests = b.addTest(.{ .root_module = unit_test_mod });

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&b.addRunArtifact(unit_tests).step);

    // Documentation
    const docs_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    const docs_obj = b.addObject(.{
        .name = "volt-docs",
        .root_module = docs_mod,
    });
    const install_docs = b.addInstallDirectory(.{
        .source_dir = docs_obj.getEmittedDocs(),
        .install_dir = .{ .custom = "docs/api-reference" },
        .install_subdir = "",
    });
    b.step("docs", "Generate documentation").dependOn(&install_docs.step);
}
