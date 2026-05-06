//! Trait surface namespace — re-exports the byte-level I/O vtables.
//!
//! Lifted into `volt.io.{Reader,Writer,...}` from `lib.zig`. Each
//! trait lives in its own `*.zig` file for cohesion; this file is the
//! single import point used by adapters (`BufReader`, `copy`, …) and
//! by `lib.zig` for public re-export.

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
