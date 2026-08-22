# Collector V2 Phase 7 integration rehearsal

Status: completed on 2026-08-22 against isolated databases only. The real
`pump_research` database was read-only throughout. This document does not authorize a live
migration, Epoch 3 creation, or collector start.

## Result

The component-level V2 implementation is internally coherent, but the deployment is **NO-GO**.
The rehearsal found and fixed two integration defects: one in live-population scheduler
reconstruction and one in archive-scope claim concurrency. Three
deployment gates remain open:

1. the Mac has only about 5.4–6.1 GiB free (2.35–2.6%), so a full 20.28 GiB restore and archive
   staging cannot be performed safely;
2. the verified final dump begins at Alembic `e4b7a9c1d203`, not the present live revision
   `f2c8d4a6197e`; restore drills must include that missing pre-V2 step;
3. no indexed Phase 6 provider is configured, and the default public Solana RPC returned HTTP
   429 for `getTokenLargestAccounts` during acceptance testing.

## Safety boundary and clone

The verified dump was inspected with `pg_restore --list`. Its recorded facts are:

- artifact: `epoch2-final.dump`;
- bytes: 4,862,381,881;
- SHA-256: `21acd1e0e46421250cb4ce6c302e15066e66d52c12f285b92f7acd3491584846`;
- verification: custom-format dump catalog was readable;
- verified at: 2026-08-17T18:24:11Z;
- independent-copy assertion: true.

The full restore was refused before allocation because the host had only 6.1 GiB free while the
source database was 21,778,521,111 bytes. A safe full rehearsal needs 36–45 GiB free: about
20.3 GiB restored data/indexes, 5–8 GiB dump/WAL/migration and archive staging, and at least
10–15 GiB safety headroom.

The fallback clone was `pump_research_v2_rehearsal_test`. It contained the complete Epoch 2
operational scheduling cohort from the dump: 58,614 tokens, 58,589 poll schedules, 5,224 capacity
decisions, and all three epoch declarations. The 58,614 matching immutable NEW-admission events
were copied read-only from live because the final dump predates the final live rows. The 233 live
tokens newer than the dump were deliberately not fabricated. High-volume observations were not
restored; therefore this is a faithful scheduler/migration rehearsal, not a full data-equivalence
restore. The real observation count was checked separately and remained 9,202,662.

## Migration rehearsal

The dump first required `e4b7a9c1d203 -> f2c8d4a6197e`. The complete V2 chain then ran in order.
Times below are wall-clock times on the 58.6k operational clone with empty high-volume fact
partitions; they are not claims about a full 20 GB restore.

| Step | Time | DB growth | WAL | Row preservation | Rewrite finding |
|---|---:|---:|---:|---|---|
| `e4b7a9c1d203 -> f2c8d4a6197e` | 0.58 s | 90 KiB | not isolated | yes | operational metadata only |
| `f2c8d4a6197e -> 7c31a8e4d5f2` | 0.77 s | 552 KiB | 711 KiB | yes | nullable/constant-default operational columns; no fact backfill |
| `7c31a8e4d5f2 -> b184a7d2e903` | 0.63 s | 520 KiB | 1.01 MiB | yes | nullable columns on observation partition tree; metadata-only on PostgreSQL 16 |
| `b184a7d2e903 -> c61e29d841af` | 0.57 s | 216 KiB | 218 KiB | yes | create archive catalog tables |
| `c61e29d841af -> e52a1c9d704f` | 0.73 s | 296 KiB | 268 KiB | yes | create candidate tables plus nullable schedule projection |
| `e52a1c9d704f -> f63b7d9a20ce` | 0.75 s | 784 KiB | 669 KiB | yes | create Phase 6 evidence tables |

Upgrade scripts contain no `UPDATE`, `DELETE`, `TRUNCATE`, or observation backfill. PostgreSQL
still takes brief `ACCESS EXCLUSIVE` locks for `ALTER TABLE`, including propagation through the
observation partition tree. A full-size lock-duration rehearsal was impossible on this Mac, so
live operational safety is not yet proven. Apply each revision separately in a maintenance window
and stop if its lock/elapsed-time guard is exceeded.

An independent empty database rehearsed every boundary as upgrade, downgrade, then re-upgrade.
All five V2 boundaries passed. Downgrade is a compatibility test, not the recommended production
rollback: it drops new evidence tables and can rewrite projections. Restore the verified
pre-migration backup into a new database instead.

## Legacy coverage reconstruction

The first full-cohort attempt failed before commit with asyncpg's 32,767 query-argument ceiling.
The initializer constructed one `IN` predicate containing all 58,589 token UUIDs. Small tests had
not exercised this PostgreSQL driver limit. The fix resolves admissions relationally by joining
`lifecycle_events` to the locked, unmapped `poll_schedules`; the bind count is constant. Coverage
policy, lifecycle thresholds, and capacity mathematics were unchanged.

The post-fix run produced:

- runtime: 409.886 seconds;
- mapped schedules: 58,589; unmapped: 0;
- immutable coverage decisions: 58,589;
- classes: 34,393 `LONG_TAIL_WEEK`, 24,074 `RETIRED_CONTROL`, 99
  `PROTECTED_ACTIVE`, 23 `PROTECTED_WATCH`;
- requested/effective demand: 1,329.768 observations/minute (`NORMAL`);
- core requests: 44.326/minute;
- due in first minute: 168;
- leases after graceful stop: 0;
- observations before/after: 0/0 in the bounded clone;
- lifecycle events before/after: 58,614/58,614;
- reconstruction storage growth: about 303 MiB;
- reconstruction plus subsequent rehearsal/catalog WAL: about 496 MiB.

The audit write is intentionally expensive and held schedule locks for about 6.8 minutes. It must
run while collection is stopped. The immediate restart took 0.154 seconds and did not add a second
coverage decision. This proves restart idempotency and bounded initial phasing; it also establishes
a maintenance-window allowance of at least 15 minutes for the current population.

## Integrated collector and recovery

The Phase 7 integration regression starts one real `CollectorRuntime` and `CollectorWorker` with
controlled providers and all optional loops enabled: discovery, DEX availability, four scheduled
observation workers, heartbeat, storage telemetry, both boost feeds, token security, market
context, and four selective-security workers. Every loop ran and every component finished as
`stopped` after intentional shutdown.

The complete suite also covers SIGINT, SIGTERM, a hard-killed process, abandoned-run recovery,
lease expiry, candidate/security task restart, four-worker schedule claims, boost races, archive
interruption/retry, and semantic idempotency. Expected provider failures are durable degraded or
failed evidence; they are not converted to safe/negative facts.

The archive concurrency regression exposed a second deterministic-identity race: concurrent
workers could collide on the archive-scope primary key while the insert arbitrated only on the
separate identity constraint. Persistence now uses targetless conflict arbitration followed by a
locked semantic readback, so identical work is idempotent and different content fails explicitly.
Claim-specific staging directories also prevent an expired worker from removing its successor's
files. The race test passed ten consecutive repetitions and the complete suite passed afterward.

## Resource stress model

The model keeps the Phase 1 core/universal DEX load at 147.9 requests/minute before candidate
coverage. Candidate work is capped at 100 concurrent 15-second-coverage tokens and 12 tasks/minute.

| Pressure | Candidate tasks requested/admitted/deferred per min | Candidate coverage observations/min | Maximum DEX requests/min |
|---:|---:|---:|---:|
| 1x | 2 / 2 / 0 | 120 | 151.900 |
| 2x | 4 / 4 / 0 | 240 | 155.900 |
| 5x | 10 / 10 / 0 | 400 | 161.233 |
| 10x | 20 / 12 / 8 | 400 | 161.233 |

All are below the 192/minute safe DEX budget; core displacement is zero. Phase 6 provider work is
independently capped at 6 requests/minute. At 1x it admits 4.5/4.5; at 2x, 5x, 10x, and a 25x
mania case it admits 6 while deferring 3, 16.5, 39, and 106.5 respectively. Wallet/deep work loses
capacity first. These are deterministic closed-form stress results, not a measured production CPU
benchmark.

CPU, RSS, DB write p95, IOPS, and WAL under a full 20 GB integrated replay remain unmeasured because
the safe full clone could not be built. That is a deployment blocker, not a value to estimate away.

## Archive, cold access, and retention

The closed rehearsal epoch exported all 48 registered archive families:

- 175,776 source/exported rows;
- 48 Parquet files;
- 14,961,025 Parquet bytes;
- 85.11 bytes/row;
- 11.408:1 logical compression for this audit-heavy scope;
- manifest and aggregate file checksums verified;
- full streaming readback and DuckDB integrity passed;
- no duplicate primary-key groups and no referential gaps in implemented checks.

A primary filesystem copy (50 objects, 15,109,425 bytes) passed readback. A secondary local path
was rejected because no physical/provider independence could honestly be asserted. Retention
eligibility remained false for both missing independent-copy verification and minimum hot age;
`deletion_performed` was false. Phase 6 hot/cold equivalence is additionally covered by the
integration suite with holder/security facts and identity equality.

## Final quality gates

- full pytest: 196 passed in 119.21 seconds;
- Ruff: passed;
- mypy strict: passed for 107 source files;
- `git diff --check`: passed;
- Alembic metadata parity on the isolated migrated test database: passed;
- isolated migration boundary upgrade/downgrade/re-upgrade: passed;
- archive claim race: passed ten consecutive repetitions before the final full-suite run.

An earlier sandboxed pytest invocation could not open localhost and produced integration-fixture
setup errors; the identical suite was rerun with access to the guarded `_test` database and is the
passing result reported above.

## Disk and status

`collector status` on the migrated clone reports operational state, epoch/run, components,
lifecycle and coverage counts, capacity, Phase 2 streams, candidate/Tier 2/Tier 3 queues, provider
configuration, DB/storage/archive/backup state, and host filesystem free bytes/percent. No routine
monitoring SQL is required after migration.

The Mac reached about 5.37 GiB free (2.35%) before cleanup and recovered to about 5.84 GiB
(roughly 3% reported filesystem capacity) after the two disposable databases and temporary
archive copies were removed. The thresholds in
`docs/epoch3-readiness.md` make less than 20% free an emergency stop. No further full restore,
archive benchmark, or Epoch 3 activity is safe on this volume.

## Limits and blockers

- Full 9,202,662-observation restore/migration, full-table lock measurement, and full hot/cold
  equivalence were not run for lack of safe disk.
- The operational clone came from a dump 233 tokens behind live and was supplemented only with
  matching immutable admission evidence; it cannot certify all final dump provenance.
- Default public Solana RPC is not a production SLA and rejected holder enumeration with 429.
- Advanced trader, creator, pool decoder, wallet, and funding providers are unconfigured.
- There is no measured integrated host CPU/RSS/IOPS result for sizing down from the conservative
  infrastructure recommendation.
- The one-time 6.8-minute scheduler initialization should be optimized later, but is safe while
  stopped and is not on every restart.
