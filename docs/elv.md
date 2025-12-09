# Early Lock Violation (ELV) in Mako

ELV relaxes the default two‑phase lock behavior for write intents on leader shards. Once a transaction has chosen its commit vector clock, intent locks are treated as visibility fences rather than strict reader blockers. Reads can bypass write intents when a stable version exists, while installs still serialize and retain rollback safety.

## Correctness sketch
- **Serialization point**: Transactions still serialize at the commit vector clock generated after locking. Validation checks detect any version change or in‑flight installs (modifying/intent bits), so two conflicting writers cannot both certify.
- **Visibility**: Readers bypass intent locks only when a stable version exists; installing writers mark `modifying` so readers spin until the version is fully installed. Inserts using `MAX_TID` remain protected.
- **Rollback safety**: Versions are appended as before; old versions remain available for rollback/GC. Unlock still bumps the header version on abort/install to invalidate speculative readers.
- **Dependencies**: Validation relies on the existing vector‑clock monotonicity and per‑key install ordering enforced by the install lock.

## Using the feature flag
- Enable ELV via `--enable_elv` on `dbtest` (default is off).
- The global flag can also be toggled programmatically with `set_elv_enabled(bool)`.

## Metrics
- `elv_lock_conflicts`: aborts caused by lock contention in the write phase.
- `elv_validation_aborts`: aborts triggered during validation/read stability checks.
- `elv_intent_reads`: reads that bypassed a write‑intent lock under ELV.

## Testing and benchmarking
- Unit tests: `ctest -R ElvTests` (or `ninja test_elv` to build/run directly).
- Full suite: `ninja run_tests` runs ELV and existing Silo/STO tests.
- Benchmarks: run any workload with `--enable_elv` to compare against the baseline; ELV is disabled otherwise for compatibility.
