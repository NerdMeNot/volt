# Volt audit + test infrastructure

Scripts for keeping Volt's runtime quality high. Run before committing
or on every PR.

## Quick reference

| Script | Purpose | Default behavior |
|---|---|---|
| `audit_all.sh` | Run every audit below in sequence | Prints findings; exits 1 if any issues |
| `audit_park_sites.sh` | Verify every `parkCurrent()` call has a `catch` arm | Flags sites with no catch handler at all |
| `audit_registrations.sh` | Verify `reactor.register*` calls are paired with `reactor.unregister*` | Flags orphan registrations |
| `audit_yield_sync.sh` | Surface tests using `volt.yield()` loops as synchronization barriers | Informational; tests should migrate to Barrier/Latch/WaitGroup |
| `cross_compile_all.sh` | Build-lib check across every supported target × reactor backend | Fails if any target stops compiling |
| `stress_test.sh` | Run the test suite N times with different seeds; surface flakes | Per-iteration log; flake-rate roll-up |
| `test_with_timeout.sh` | Wrap `zig build test` in a hard timeout; capture stack traces on hang | Default 90s; tmp/hang-*.txt on hang |
| `zombie_check.sh` | Detect orphan zig-test processes still alive from prior runs | Reports; `--kill` to terminate |

## Suggested CI usage

```yaml
# Pre-commit: cheap audits
- run: scripts/audit_all.sh --strict

# Per-PR: regression gate
- run: TIMEOUT_SECS=120 scripts/test_with_timeout.sh

# Daily on main: catch flakes
- run: N=200 scripts/stress_test.sh
```

## Per-script details

### `audit_park_sites.sh`

Looks at every `parkCurrent()` call site outside the test tree. Each
park typically registers external state (a reactor wait, a waiter list
entry, a pool closure pointer); the corresponding cancel-cleanup must
live in a `catch` arm. The script surfaces any park site that has NO
catch arm in the next 16 lines — those are potential leaks.

Does NOT verify the catch body is the *correct* cleanup (would need
semantic analysis); pair this with `audit_registrations.sh` to cover
the reactor-registration case explicitly.

### `audit_registrations.sh`

Every `reactor.registerWait` / `registerTimer` call must be matched
with an `unregisterWait` / `unregisterTimer` reachable from the cancel
arm. This was the v1.0 bug class — kqueue timer leak masked as v1.2
follow-up that turned out to be a 100-LoC fix. The script enforces the
pattern.

### `audit_yield_sync.sh`

`volt.yield()` is a reschedule hint, NOT a synchronization barrier. A
test that does `while (i < N) yield()` to "give children a chance to
park" works on quiet systems but fails under load — yields don't
guarantee the launched coroutines actually run. The fix is a real
synchronization primitive (see `volt.test.Barrier` / `Latch` /
`WaitGroup`). This script surfaces the offending patterns.

### `cross_compile_all.sh`

Compile-checks the lib (or full test binary with `--tests`) against
every supported (target, reactor backend) tuple. Useful for catching
"works on my Mac" regressions that would only surface on CI.

### `stress_test.sh`

Runs `zig build test` N times with different seeds. Each iteration
runs under `test_with_timeout.sh`. At the end, prints a per-test
flake-rate roll-up. Use with `N=200` to catch flakes that don't
reproduce in single runs.

### `test_with_timeout.sh`

Drop-in replacement for `zig build test` with a wall-clock budget.
On hang: captures `sample <pid>` (Darwin) or `gdb thread bt all`
(Linux) for every test process to `tmp/hang-<timestamp>.txt`, then
SIGKILLs the process tree. This is the discipline that catches the
"all 11 threads parked on a futex with infinite timeout" pattern.

### `zombie_check.sh`

Lists orphan `zig build test` and test-binary processes still alive
from prior runs. A process is flagged as a zombie if its elapsed time
is > 60s and CPU time is < 10s (i.e., parked on a futex / select
forever). `--kill` SIGKILLs all detected zombies; `--strict` exits 1
if any are found (CI gate).
