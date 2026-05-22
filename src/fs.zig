//! Filesystem namespace facade — `volt.fs.{path, ...}`.
//!
//! Today (Phase A): exposes `volt.fs.path` for pure-string path
//! utilities, which are needed by Unix-socket addresses and useful
//! for any path-touching code.
//!
//! Phase B adds: `File`, `Dir`, `Walker`, `Metadata`, `MappedFile`,
//! `Watcher`, plus convenience free functions (`readFile`,
//! `writeFile`, `copyFile`, `stat`, etc.) — see the roadmap.

pub const path = @import("fs/path.zig");

// Force test-runner to discover tests in transitively-imported files.
// Zig's runner walks declarations from the root file; explicit
// `_ = @import` references inside a `test` block guarantee the
// file's tests are pulled into the run regardless of whether other
// code paths touch them.
test {
    _ = @import("fs/path.zig");
}
