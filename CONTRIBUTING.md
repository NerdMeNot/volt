# Contributing to Volt

Thank you for your interest in contributing to Volt. This document covers the process
for reporting bugs, submitting changes, and the standards we hold contributions to.

## Reporting Bugs

Open a GitHub issue with:

1. **Platform details** -- OS, architecture, Zig version, kernel version (for Linux)
2. **Minimal reproduction** -- the smallest code that triggers the bug
3. **Expected vs actual behavior** -- what you expected and what happened
4. **Relevant logs or stack traces** -- include any error output

For concurrency bugs, note whether the issue reproduces reliably or intermittently, and
how many threads are involved.

## Submitting Pull Requests

1. Fork the repository and create a branch from `main`.
2. Make your changes following the code style and testing requirements below.
3. Ensure all tests pass locally before opening the PR.
4. Open a pull request with a clear description of what the change does and why.
5. Link any related issues.

Keep pull requests focused. One logical change per PR is easier to review than a large
batch of unrelated modifications.

## Code Style

Follow Zig conventions and the project guidelines in `CLAUDE.md`:

- **File naming**: `PascalCase.zig` for files that export a primary type, `snake_case.zig`
  for namespace modules.
- **Identifiers**: types in `PascalCase`, functions in `camelCase`, variables in `snake_case`.
- **Comments**: explain *why*, not *what*. If the code does something non-obvious for
  platform-specific or correctness reasons, document the rationale.
- **Error handling**: never swallow errors. Distinguish recoverable errors (e.g., `WouldBlock`)
  from fatal ones (e.g., `EBADF`). Include context in error paths.
- **Concurrency**: document thread safety. Justify memory ordering on every atomic operation.
  Document lock ordering to prevent deadlocks.

## Testing Requirements

All changes must pass the existing test suites before merging:

```bash
# Unit tests (588+ tests)
zig build test

# Loom-style concurrency tests (83 tests)
zig build test-concurrency

# Edge case and robustness tests (35+ tests)
zig build test-robustness

# Stress tests with real threads
zig build test-stress

# Run everything
zig build test-all
```

### Running a Single Test

Use `--test-filter` to run a specific test by name:

```bash
# Run only tests matching "Mutex"
zig build test -- --test-filter "Mutex"

# Run a specific concurrency test
zig build test-concurrency -- --test-filter "channel_close"
```

### Test Intensity

Some test suites support intensity configuration via environment variables:

```bash
# Increase loom-style iteration count (default varies by test)
LOOM_ITERATIONS=10000 zig build test-concurrency

# Increase stress test intensity
VOLT_TEST_INTENSITY=high zig build test-stress
```

### New Test Guidelines

New code should include tests covering:

- Happy path
- Error conditions
- Edge cases (empty input, maximum values, zero-length buffers)
- Platform-specific behavior where applicable

For lock-free or concurrent data structures, add loom-style concurrency tests in
`tests/concurrency/` that exercise interleavings systematically.

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/) style:

```
feat(sync): add async-aware condition variable
fix(channel): prevent lost wakeup in MPMC recv path
docs(readme): update build instructions for Zig 0.15
test(barrier): add stress test for concurrent reset
refactor(scheduler): extract idle worker bitmap
```

Keep the first line under 72 characters. Use the body for additional context when the
change is non-trivial.

Do **not** add `Co-Authored-By` lines to commit messages.

## Build Requirements

- **Zig 0.15.2** (minimum 0.15.0)
- Linux, macOS, or Windows (see `CLAUDE.md` for platform-specific details)

## License

By contributing to Volt, you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE).
