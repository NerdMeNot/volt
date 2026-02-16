//! I/O Utilities
//!
//! Provides buffered I/O, copy utilities, and iterators for streams.
//!
//! ## Buffered I/O
//!
//! ```zig
//! const io = @import("volt").io;
//!
//! // Buffered reading
//! var buf_reader = io.BufReader(FileType).init(file);
//! while (try buf_reader.readLine(&line_buf)) |line| {
//!     process(line);
//! }
//!
//! // Buffered writing
//! var buf_writer = io.BufWriter(FileType).init(file);
//! try buf_writer.writeAll("data");
//! try buf_writer.flush();
//! ```
//!
//! ## Copy
//!
//! ```zig
//! // Copy all data between streams
//! const copied = try io.copy(reader, writer);
//!
//! // Copy exactly N bytes
//! try io.copyN(reader, writer, 1024);
//! ```
//!
//! ## Line Iteration
//!
//! ```zig
//! var lines_iter = io.lines(&reader, allocator);
//! defer lines_iter.deinit();
//!
//! while (try lines_iter.next()) |line| {
//!     std.debug.print("{s}\n", .{line});
//! }
//! ```

// Buffered I/O
pub const buf_reader = @import("buf_reader.zig");
pub const buf_writer = @import("buf_writer.zig");

pub const BufReader = buf_reader.BufReader;
pub const bufReader = buf_reader.bufReader;

pub const BufWriter = buf_writer.BufWriter;
pub const bufWriter = buf_writer.bufWriter;

// Copy utilities
pub const copy_util = @import("copy.zig");

pub const copy = copy_util.copy;
pub const copyBuf = copy_util.copyBuf;
pub const copyN = copy_util.copyN;
pub const copyNBuf = copy_util.copyNBuf;
pub const BidirectionalResult = copy_util.BidirectionalResult;
pub const tryBidirectionalCopy = copy_util.tryBidirectionalCopy;
pub const DEFAULT_COPY_BUF_SIZE = copy_util.DEFAULT_BUF_SIZE;

// Lines and split iterators
pub const lines_util = @import("lines.zig");

pub const Lines = lines_util.Lines;
pub const lines = lines_util.lines;
pub const linesWithBuffer = lines_util.linesWithBuffer;
pub const stripNewline = lines_util.stripNewline;

pub const Split = lines_util.Split;
pub const split = lines_util.split;
pub const DEFAULT_MAX_LINE_LEN = lines_util.DEFAULT_MAX_LINE_LEN;

// Tests
test {
    _ = buf_reader;
    _ = buf_writer;
    _ = copy_util;
    _ = lines_util;
}
