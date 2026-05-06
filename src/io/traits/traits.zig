//! Trait surface namespace — re-exports the byte-level I/O vtables.
//!
//! Lifted into `volt.io.{Reader,Writer,...}` from `lib.zig`. Each
//! trait lives in its own `*.zig` file for cohesion; this file is the
//! single import point used by adapters (`BufReader`, `copy`, …) and
//! by `lib.zig` for public re-export.
//!
//! ## Vtable marker policy
//!
//! Reader and Writer expose nullable function-pointer "markers"
//! (`as_fd`, `as_bytes`) that fast-path consumers (`copy`) query for
//! kernel-level zero-copy or memory-direct dispatch. The pattern is
//! well-precedented (Go's `io.Copy` runtime type assertions, Rust's
//! unstable specialisation) — this is the lowest-overhead version.
//!
//! **Hard cap until v2.0:** the current 2 markers + 1 op slot
//! (`readv` / `writev` / `flush`). Each Reader/Writer vtable is
//! comptime-asserted ≤ 64 bytes (see `Reader.zig` / `Writer.zig`).
//! Any new optimisation hook beyond that requires a v2.0 trait
//! redesign with a tagged-union `as_kind: ?KindTag` shape — adding
//! `as_compressed`, `as_encrypted`, `as_chunked`, etc. ad-hoc would
//! bloat every vtable for every consumer and tempt wrappers to lie
//! about what they expose.

pub const Reader = @import("Reader.zig").Reader;
pub const ReadError = @import("Reader.zig").ReadError;
pub const SliceReader = @import("Reader.zig").SliceReader;

pub const Writer = @import("Writer.zig").Writer;
pub const WriteError = @import("Writer.zig").WriteError;
pub const BufferWriter = @import("Writer.zig").BufferWriter;

pub const Closer = @import("Closer.zig").Closer;

pub const Seeker = @import("Seeker.zig").Seeker;
pub const SeekError = @import("Seeker.zig").SeekError;

pub const ReaderAt = @import("ReaderAt.zig").ReaderAt;
pub const WriterAt = @import("WriterAt.zig").WriterAt;

test {
    _ = @import("Reader.zig");
    _ = @import("Writer.zig");
    _ = @import("Closer.zig");
    _ = @import("Seeker.zig");
    _ = @import("ReaderAt.zig");
    _ = @import("WriterAt.zig");
}
