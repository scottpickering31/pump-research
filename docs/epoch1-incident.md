# Epoch 1 scheduler-capacity incident

## Incident declaration

Epoch 1 stopped collecting at approximately `2026-08-16T03:35:00Z`, after
about six hours, when the scheduled-observation component encountered:

```text
asyncpg.exceptions.UniqueViolationError:
duplicate key value violates unique constraint
"scheduler_capacity_decisions_pkey"

Key (id)=(2079f6c4-a74c-50a6-8b8b-4ab37fb66a52) already exists.
```

Collection remains stopped. Existing Epoch 1 facts are preserved. Epoch 1 is
engineering evidence only after the resulting observation continuity gap and
must be explicitly invalidated before another epoch is prepared.

## Phase A: exact root-cause audit

### Deterministic identity

Both identities are deterministic:

- `idempotency_key` is SHA-256 of canonical JSON containing the UTC 30-second
  bucket, scheduler-policy SHA-256, and the complete capacity-plan snapshot.
- `id` is UUIDv5 in `uuid.NAMESPACE_URL` over that idempotency key.

Normal generation therefore has these possibilities:

- Same ID and same idempotency key: yes, whenever the same logical decision is
  reconstructed or reused.
- Same ID and different idempotency key: no, except a cryptographic UUIDv5
  collision or externally corrupted/manually inserted data.
- Different ID and same idempotency key: no, except externally
  corrupted/manually inserted data, because the UUID is derived from the key.

The live row has idempotency key
`f950579886d6c87dd23cf9e3b10389be13a32caefa85e5b4bb9d5f42c9600ecc`.
Recomputing UUIDv5 from that key produces
`2079f6c4-a74c-50a6-8b8b-4ab37fb66a52`, exactly the failed primary key.

### Live-row comparison

The existing row was persisted at `2026-08-16T03:35:00.164562Z` for bucket
`2026-08-16T03:35:00Z`. It records `DEGRADED`, policy
`fbddfad7471f421d8ad7583b61caaabfe45ed4b00e123ecd20aa14be1c10d2c2`,
requested load `9814.066667` token observations/minute, and safe capacity
`5760`/minute.

The durable component failure contains the failed bind parameters. They contain
the same ID, idempotency key, bucket, mode, and policy. Because the idempotency
key hashes the complete canonical plan snapshot, the stored and attempted rows
were the same logical decision (subject only to the ordinary cryptographic hash
collision assumption). This was not evidence of two different capacity plans
being assigned one identity.

### Why the original conflict clause failed

Persistence used:

```sql
INSERT ...
ON CONFLICT (idempotency_key) DO NOTHING
```

That clause names only the unique index on `idempotency_key` as its conflict
arbiter. The concurrently inserted row also conflicts with the independent
primary-key index. PostgreSQL detected the non-arbiter primary-key conflict, so
the targeted clause was not permitted to suppress it and the statement raised
`UniqueViolationError`.

The defect is not that PostgreSQL failed to notice the idempotency key. The
defect is that two database uniqueness constraints encode one logical identity,
while the insert made only one of them safe under a concurrent replay.

### Concurrency and cache scope

The collector constructs one `AdaptiveScheduler` and shares it among:

- four scheduled-observation claim/execute loops;
- DEX-availability admission; and
- lifecycle classification performed while completing observations.

The in-memory capacity cache is shared within that process, but it is not
protected by an asyncio lock. A bucket refresh awaits population queries between
the cache check and cache assignment, so tasks can interleave and construct the
same decision. Even after one task fills the cache, every caller executes the
capacity-decision insert again.

`claim_next_batch` takes a PostgreSQL transaction advisory lock, which
serializes claim transactions. Admission and lifecycle-transition transactions
also call `_capacity_decision`, but do not take that claim lock. Poll execution
and its lifecycle work occur after the claim transaction. Consequently a claim
worker can race a lifecycle/admission transaction even though claims themselves
are serialized. The cache is neither cross-process coordination nor a durable
idempotency mechanism.

The per-process `_validated_capacity_decision_ids` set does not fix this. It is
checked only after the insert, is not synchronized, is not shared across
processes or restarts, and causes later calls in the same process to skip
semantic readback entirely.

### Frequency and DEGRADED mode

The failure is not specific to the transition into `DEGRADED`. It can occur on
the first concurrent persistence race in any capacity refresh bucket, in any
mode. High load made overlapping claims, completions, lifecycle transitions,
and admissions more likely. The live database contains one decision for each of
735 refresh buckets from `21:28:00Z` through `03:35:00Z`; the race happened in
the final bucket rather than being a one-time state-transition operation.

## Fix

Capacity-decision persistence now uses untargeted `ON CONFLICT DO NOTHING`, so
either uniqueness constraint can arbitrate an identical concurrent insert. A
no-op is never assumed safe: every call reads rows matching either the expected
UUID or idempotency key and verifies exactly one row with equal ID, key, bucket,
mode, policy digest, and complete decision snapshot. A cardinality or field
mismatch emits `scheduler_capacity_decision_integrity_error` at critical
severity and raises `SchedulerCapacityDecisionIntegrityError`.

The old process-local set that skipped repeated readback was removed. Identical
races continue normally; semantic identity corruption still fails loudly.
Lifecycle thresholds, capacity mathematics, configured request ceiling, and
polling priorities were not changed.

## Concurrency and sustained verification

The regression test deliberately synchronizes one claim writer and three
lifecycle writers inside a shared 30-second `DEGRADED` refresh. Four claim tasks
are active concurrently, exactly as in production. The test would raise the
observed primary-key violation with the old insert. With the fix it proves:

- all four claim workers return work and continue;
- one logical capacity row exists;
- the decision is `DEGRADED`;
- all claimed batches can complete; and
- a later batch can still be claimed.

Separate cases verify identical replay, reconstruction by a new scheduler
process, same ID with different key/content, and same key with different
ID/content. Only semantically equal content is accepted.

An accelerated persistence test ran 800 consecutive capacity windows—more than
the 735 windows reached before the live failure—with four concurrent scheduler
instances per window. It produced exactly 800 rows and 800 unique idempotency
keys with no escaped duplicate-key error.

A separate 12-hour scheduling simulation used 120 ACTIVE, 3,000 mature NEW,
4,700 FADING, 50 DORMANT, four-worker-equivalent bounded service, and a
conservative 30 NEW arrivals/minute. Initial requested demand is approximately
9,793 observations/minute. After 12 hours, with all arrivals conservatively
remaining NEW, requested demand is approximately 53,113/minute; adaptation
still bounds effective demand to 5,756.09/minute and actual service to 192
requests/minute. Average occupancy is 29.94/30. ACTIVE remains at five seconds
with zero p95 lateness. All initial populations receive observations; only 6 of
29,470 schedules are due at the exact terminal instant, so overdue work does not
grow as an impossible FIFO backlog.

Final effective intervals in that conservative terminal population were:

| Tier | Effective seconds | p95 lateness seconds |
|---|---:|---:|
| ACTIVE | 5 | 0.000 |
| NEW initial | 176 | 0.563 |
| NEW | 351 | 0.563 |
| FADING | 2,806 | 89.813 |
| DORMANT | 11,222 | 1,437.250 |

The lower-tier values reflect the deliberately severe assumption that 21,600
arrivals receive no lifecycle promotion during the simulation. They are
explicit capacity degradation, not hidden scheduler lateness.

## Failure-state and research-validity handling

`collector status` now derives an operational state independently from the
epoch declaration:

- a failed latest run reports `FAILED` immediately;
- a nominally running run whose heartbeat is older than the greater of 30
  seconds or three heartbeat intervals reports `STALE`;
- `actively_collecting` is false in either case; and
- a running epoch paired with a failed/stale run emits a continuity warning.

Effective epoch validity is an audited mutable projection rebuilt from immutable
epoch events. This avoids rewriting the immutable epoch declaration while
allowing an explicit later invalidation. Invalid epochs are rejected by research
reports by default. Engineering analysis requires `--include-invalid`.

Epoch 1 (`b058dd97-4c55-4045-b4b1-1234562836a8`) was explicitly closed as
`invalid` at `2026-08-16T06:59:18.324563Z`, with `data_valid=false` and this
reason:

> Scheduler capacity-decision primary-key race terminated scheduled observation
> at approximately 2026-08-16T03:35:00Z, causing an unrecoverable observation
> continuity gap; preserve for engineering analysis only and exclude from
> research/backtesting.

Its 1,632,909 observations, 7,896 tokens, 7,984 pairs, capacity decisions, batch
facts, and all other historical rows remain in PostgreSQL. No historical data
was deleted or rewritten.

## Epoch 2 clean-start semantics

Epoch 2 (`16d0e72b-feae-4744-b1ef-a9259c3e6511`) exists in `planned` state and
has no start timestamp. Collection has not been restarted.

On its first collector start, in the same transaction as the epoch start and
collector-run creation:

1. Existing token/pair metadata and lifecycle state are retained. Tokens are not
   reset to NEW and historical facts are untouched.
2. Every mutable poll schedule is locked and its stale Epoch 1 due time is
   replaced with a deterministic SHA-256 token/epoch phase within that tier's
   current effective interval.
3. Old leases are cleared only from the mutable projection. Old batches and
   memberships remain immutable Epoch 1 evidence.
4. One append-only `epoch_start_rebase` schedule decision per token records old
   and new due times, effective cadence, capacity decision, configuration, and
   the Epoch 2 foreign key.
5. The first Epoch 2 poll therefore measures lateness from the new phase, not
   from the hours-old Epoch 1 obligation.
6. A restart while Epoch 2 is already running reconstructs schedules and does
   not perform the one-time rebase again.

The 14 current PENDING_DEX identities remain eligible for ordinary future
availability checks; they are not discarded. New requests and observations are
linked to an Epoch 2 collector run, so provenance is unambiguous. Researchers
must treat the carried lifecycle state as the documented initial condition of
Epoch 2, not as an Epoch 2-derived transition.

## Backup evidence

The failed Epoch 1 database dump was re-read after the fix:

- path: `/Users/scottpickering/Desktop/pump-research-backups/epoch1-failed-6h.dump`
- bytes: `856180260`
- catalog entries: `797`
- SHA-256: `a47df354dc48846779fb6a744aea2a6058e5fea0df7ac575889718946214ea80`

A fresh independent full backup was then created for planned Epoch 2 and
verified through Docker Compose `pg_restore --list` plus a complete SHA-256
read:

- path: `/Users/scottpickering/Desktop/pump-research-backups/epoch2-prestart-20260816T0703Z.dump`
- bytes: `856184769`
- catalog entries: `799`
- SHA-256: `e8512f6c0dbc207403cf4caa8ae2e0e3a4f4e854a5ffef2e0afded27669589d2`

## Quality gate

- 117 tests passed: 43 unit and 74 integration.
- The exact four-worker race regression fails under the old insert semantics and
  passes with the fix.
- The 800-window sustained `DEGRADED` persistence test passed.
- The 12-hour bounded scheduling simulation passed.
- Physical SIGKILL/restart recovery passed.
- Invalid-epoch default filtering and stale-heartbeat reporting passed.
- Alembic additive migration upgrade/downgrade/upgrade passed on
  `pump_research_capacity_test`.
- Alembic model/schema parity reports no pending operations.
- Ruff, mypy, and `git diff --check` pass.
- Destructive fixtures ran only against `pump_research_capacity_test`.
- The live collector has no advisory lock and was not restarted.
- No trading, wallet, signing, ML, retention deletion, or lifecycle-threshold
  change was introduced.

## Exact controlled Epoch 2 procedure

Set the database URL for each application command:

```bash
export PUMP_RESEARCH_DATABASE_URL='postgresql+asyncpg://pump_research:pump_research@localhost:5433/pump_research'
```

1. Check the Epoch 1 backup (already completed; safe to repeat):

   ```bash
   python -m pump_research backup status --epoch 1
   python -m pump_research backup verify \
     /Users/scottpickering/Desktop/pump-research-backups/epoch1-failed-6h.dump \
     --epoch 1 --independent-copy
   ```

2. Apply and verify the migration (already applied; the upgrade is idempotent):

   ```bash
   python -m alembic upgrade head
   python -m alembic current
   python -m alembic check
   ```

3. Invalidate Epoch 1 (already completed; do not rerun the close command). Verify:

   ```bash
   python -m pump_research epoch status --number 1
   ```

   The one-time command that was executed was:

   ```bash
   python -m pump_research epoch close --number 1 --status invalid \
     --reason 'Scheduler capacity-decision primary-key race terminated scheduled observation at approximately 2026-08-16T03:35:00Z, causing an unrecoverable observation continuity gap; preserve for engineering analysis only and exclude from research/backtesting.'
   ```

4. Create Epoch 2 (already completed; do not rerun). Verify it is planned and
   `started_at` is null:

   ```bash
   python -m pump_research epoch status --number 2
   ```

5. Verify the fresh independent pre-start backup (already completed; safe to
   repeat):

   ```bash
   python -m pump_research backup verify \
     /Users/scottpickering/Desktop/pump-research-backups/epoch2-prestart-20260816T0703Z.dump \
     --epoch 2 --independent-copy
   python -m pump_research backup status --epoch 2
   ```

6. Start Epoch 2 only when the operator is ready for the uninterrupted 24-hour
   window:

   ```bash
   mkdir -p logs/epoch2
   caffeinate -dimsu env \
     PUMP_RESEARCH_DATABASE_URL="$PUMP_RESEARCH_DATABASE_URL" \
     .venv/bin/python -m pump_research collector run --epoch 2 \
     2>&1 | tee -a logs/epoch2/collector.log
   ```

7. In a second terminal, confirm health immediately and after at least three
   heartbeat intervals:

   ```bash
   PUMP_RESEARCH_DATABASE_URL="$PUMP_RESEARCH_DATABASE_URL" \
     .venv/bin/python -m pump_research collector status
   PUMP_RESEARCH_DATABASE_URL="$PUMP_RESEARCH_DATABASE_URL" \
     .venv/bin/python -m pump_research epoch status --number 2
   ```

Require `operational_state=HEALTHY`, `actively_collecting=true`, Epoch 2
`status=running`, a recent heartbeat, request usage at or below 192/minute safe
capacity, and no component failure. Stop with `Ctrl-C`/SIGTERM; do not close the
epoch until the intended validation disposition is known.

## GO / NO-GO

**GO for a controlled Epoch 2 start.** The exact live failure has a deterministic
pre-fix regression, concurrent identical persistence is semantically verified,
the capacity policy remains bounded and unchanged, the live schema migration is
applied, Epoch 1 is invalid and preserved, Epoch 2 is planned but not started,
and a fresh independently located full backup is verified. GO does not authorize
automatic retention, deletion, trading, or any scheduler/lifecycle-policy
change.
