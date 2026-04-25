//! PROBE: can Zig 0.16 comptime parse a Zig source file via std.zig.Ast?
//!
//! If yes, we have a path to source-level async/await emulation:
//!   1. User writes a normal-looking async fn in a separate file
//!   2. @embedFile gives us the source string at comptime
//!   3. std.zig.Ast.parse builds the AST
//!   4. We walk the AST, find suspension points, generate state machines
//!
//! If no, the @embedFile path is closed. We fall back to:
//!   - linear() spec-of-methods (works today)
//!   - or a build.zig preprocessor as out-of-process codegen
//!
//! This file is a diagnostic — it doesn't generate state machines yet.
//! It just answers the prerequisite: can we parse Zig at comptime at all?

const std = @import("std");

// Try to parse a small Zig source string at comptime.
// Probe 1: Ast.parse at comptime — BLOCKED by @intFromPtr in
// FixedBufferAllocator (pointer alignment isn't comptime-evaluable).
// Documented for the record. Not retried.

// Probe 2: std.zig.Tokenizer at comptime. Tokenizer is iterator-style;
// if it doesn't allocate, we can walk tokens at comptime even though
// we can't build the AST.
test "comptime probe: std.zig.Tokenizer" {
    comptime {
        const source: [:0]const u8 =
            \\pub fn fetch(rt: Rt, url: []const u8) ![]const u8 {
            \\    var stream = try connect(rt, url);
            \\    defer stream.close();
            \\    var buf: [4096]u8 = undefined;
            \\    const n = try stream.read(rt, &buf);
            \\    return buf[0..n];
            \\}
        ;

        var tokenizer = std.zig.Tokenizer.init(source);
        var token_count: usize = 0;
        var has_fn = false;
        var has_try = false;
        var has_return = false;

        while (true) {
            const tok = tokenizer.next();
            token_count += 1;
            if (tok.tag == .eof) break;
            if (tok.tag == .keyword_fn) has_fn = true;
            if (tok.tag == .keyword_try) has_try = true;
            if (tok.tag == .keyword_return) has_return = true;
        }

        if (token_count == 0) @compileError("zero tokens");
        if (!has_fn) @compileError("expected to find `fn`");
        if (!has_try) @compileError("expected to find `try`");
        if (!has_return) @compileError("expected to find `return`");
    }

    std.debug.print("PROBE: comptime Tokenizer.next() works\n", .{});
}
