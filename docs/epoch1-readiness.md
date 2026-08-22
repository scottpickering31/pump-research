# Epoch 1 readiness

> Historical document: Epoch 1 failed at approximately
> `2026-08-16T03:35:00Z` and is permanently marked invalid. Do not restart or
> reuse it for research. See [Epoch 1 scheduler-capacity incident](epoch1-incident.md)
> for the fix, preserved-data status, and controlled Epoch 2 procedure.

## Recommendation

**NO-GO until the preflight independent backup step below has produced a real
artifact outside the project/PostgreSQL volume and `backup verify` reports it.**

The database collision guard, epoch provenance, capacity-bounded scheduler,
storage telemetry, Parquet/Zstandard exporter, independent readback, DuckDB
queries, and backup-evidence commands are implemented and tested. No independent
backup artifact exists merely because those commands exist. After the preflight
artifact is created and verified, the remaining recommendation is GO for one
controlled 24-hour Epoch 1 run—not for unattended long-term retention.

## 1. Epoch 0 incident declaration

Epoch 0 began on 2026-08-15 and was engineering burn-in only. An integration
fixture ran `TRUNCATE ... CASCADE` against `pump_research`. PostgreSQL
`archive_mode` was off and there was no local dump. The dataset is permanently
lost, has research validity **NONE**, will not be recovered, and must never be
spliced into Epoch 1. The durable Epoch 0 database declaration is seeded as
`invalid`; [data-epochs.md](data-epochs.md) is the authoritative incident record.

## 2. Database safety architecture

A destructive integration operation is permitted only when all of these are
true:

1. `PUMP_RESEARCH_TEST_DATABASE_URL` is explicitly present.
2. `PUMP_RESEARCH_ENVIRONMENT=test` is explicit.
3. PostgreSQL itself reports an approved `current_database()` name: exact
   `pump_research_test`, a `test_` prefix, an `_test` suffix, or an `_test_`
   segment.

The integration migration fixture checks the connected database before invoking
Alembic. The fixture checks again on the exact connection immediately before
every `TRUNCATE`. Alembic also repeats the connected-database assertion when
`PUMP_RESEARCH_MIGRATION_DESTRUCTIVE_TEST=1`. A URL alias cannot override what
`current_database()` reports.

Read-only proof commands:

```bash
PUMP_RESEARCH_ENVIRONMENT=test \
PUMP_RESEARCH_TEST_DATABASE_URL='postgresql+asyncpg://pump_research:pump_research@localhost:5433/pump_research_test' \
PUMP_RESEARCH_DATABASE_URL='postgresql+asyncpg://pump_research:pump_research@localhost:5433/pump_research_test' \
python -m pump_research db safety-check
```

The same command pointed at `.../pump_research` must report
`destructive_test_operations_permitted: false`. The integration suite must never
be run with the live URL in `PUMP_RESEARCH_TEST_DATABASE_URL`.

## 3. Exact Epoch 1 start procedure

Do not start the collector until every step succeeds.

1. Stop any collector and confirm `collector status` shows no running process.
2. Apply the additive readiness migration to the already-declared-lost/empty
   `pump_research` schema:

   ```bash
   PUMP_RESEARCH_DATABASE_URL='postgresql+asyncpg://pump_research:pump_research@localhost:5433/pump_research' \
   python -m alembic upgrade head
   ```

3. Confirm Epoch 0 is present and invalid:

   ```bash
   python -m pump_research epoch list
   ```

4. Declare Epoch 1 exactly once:

   ```bash
   python -m pump_research epoch create \
     --number 1 \
     --purpose 'first valid 24-hour research collection'
   python -m pump_research epoch status --number 1
   ```

5. Create and verify the independent preflight backup described in section 8.
   This dump now includes the planned Epoch 1 declaration.
6. Capture the configuration and safety status in the run log:

   ```bash
   python -m pump_research db safety-check
   python -m pump_research collector status
   python -m pump_research backup status --epoch 1
   ```

   A live database correctly reports destructive test operations as forbidden.
   Backup status must show an independently verified artifact.

7. Start the collector. This transaction writes Epoch 1's exact UTC `running`
   event and its first collector run together:

   ```bash
   python -m pump_research collector run --epoch 1
   ```

8. In a second terminal, save `collector status` immediately, after 10 minutes,
   after one hour, and periodically through 24 hours. Do not alter lifecycle or
   scheduler settings during the epoch.

## 4. Exact controlled stop procedure

1. Send one `SIGTERM` (or press Ctrl-C once) and wait for
   `collector_stopped`. Do not close the epoch while a collector run is still
   `running`; the command refuses this.
2. Confirm durable status:

   ```bash
   python -m pump_research collector status
   ```

3. Close the epoch explicitly:

   ```bash
   python -m pump_research epoch close \
     --number 1 --status completed --reason 'controlled 24-hour stop'
   python -m pump_research epoch status --number 1
   ```

Use `aborted` with an exact reason if collection ended early. Do not mark an
incomplete run `completed`.

## 5. Capacity-aware scheduler configuration

The prior capacity-aware algorithm and lifecycle thresholds are unchanged.
Configured targets are:

| State | Target |
| --- | ---: |
| ACTIVE | 5 seconds |
| RESURRECTED | 5 seconds |
| NEW, first two minutes after DEX admission | 15 seconds |
| NEW, subsequently | 30 seconds |
| WATCH | 15 seconds |
| FADING | 120 seconds |
| DORMANT | 900 seconds |

The request ceiling remains 240/minute, batch size remains at most 30, and the
20% safety headroom gives a planning budget of 192 requests/minute or 5,760 token
observations/minute. Capacity decisions are immutable and include requested,
available, and effective rates and target/effective intervals. ACTIVE and
RESURRECTED are protected first; overload deterministically stretches lower
tiers, and critical overload still assigns every populated tier a finite fair
rate. See [capacity-aware-scheduler.md](capacity-aware-scheduler.md) for the load
simulation and bounded-demand proof.

## 6. Storage telemetry

The collector writes one compact sample every 600 seconds by default. Database,
table, index, and TOAST/auxiliary byte readings use PostgreSQL size functions.
PostgreSQL `reltuples` is intentionally recorded as an inexpensive row-count
estimate at this frequency; the final report performs fact-table aggregates for
actual epoch rows.

Tracked relations include the current monthly partitions for observations,
lifecycle evidence, and batch membership, plus API requests, lifecycle events,
batches, outcomes, schedule decisions, discovery, pairs, and tokens. Status
labels GiB/day and 30/90/365-day figures as extrapolations and lists recent top
growth contributors. Telemetry adds roughly 12 relation rows every 10 minutes,
which is immaterial beside collection facts.

## 7. Archival process

Only an immutable closed range may be exported. A running epoch requires a
10-minute closed-range lag; a completed Epoch 1 may be exported end-to-end.

```bash
python -m pump_research archive export \
  --epoch 1 \
  --from 'EPOCH_START_ISO' \
  --to 'EPOCH_END_ISO' \
  --output '/path/to/archive'

python -m pump_research archive verify '/path/to/archive/manifests/epoch=1/MANIFEST'
python -m pump_research archive stats '/path/to/archive/manifests/epoch=1/MANIFEST'
python -m pump_research archive analyze '/path/to/archive/manifests/epoch=1/MANIFEST'
```

Exports are streamed in configurable 25,000-row chunks, partitioned by epoch,
table, and UTC year/month/day, and written with Parquet Zstandard compression.
UUIDs remain lossless strings, timestamps are typed UTC timestamps, decimals use
fixed-precision Parquet decimals, and JSON/JSONB uses canonical sorted compact
JSON. The manifest records source queries and watermarks, row counts, schemas,
logical bytes, Parquet bytes, compression ratio, time/ID bounds, per-file SHA256,
canonical row-content SHA256, code revision, and creation time.

Verification reads every Parquet row and compares file hashes, content hashes,
counts, and timestamp bounds. An identical rerun resolves to the verified
manifest; a conflicting artifact for the same range is refused. Export and
verification contain no PostgreSQL `DELETE`, `TRUNCATE`, `DROP`, detach, or
partition-removal operation.

DuckDB reads the files directly and measures observation count, unique tokens,
one token's price/liquidity/volume series, observations-to-pairs-to-tokens join,
lifecycle reconstruction, time-window scan, and buy/sell ratio. This validates
Parquet as a cold analytical store while PostgreSQL remains the hot operational
authority.

## 8. Backup process

For this MacBook run, make one preflight dump and then dumps every six hours,
plus a final dump after the controlled stop. Put them on an external volume or a
separately synchronized location—not the repository, Docker volume, or archive
staging directory. A six-hour cadence avoids the write and I/O cost of frequent
full dumps while bounding loss during this validation run.

Example plain-format dump to an external volume:

```bash
docker compose exec -T postgres \
  pg_dump -U pump_research -d pump_research -Fp \
  > /Volumes/INDEPENDENT_BACKUP/pump-research/epoch1-preflight.sql

python -m pump_research backup verify \
  /Volumes/INDEPENDENT_BACKUP/pump-research/epoch1-preflight.sql \
  --epoch 1 --independent-copy

python -m pump_research backup status --epoch 1
```

Plain SQL verification reads the PostgreSQL dump header and completed trailer and
hashes the entire file. Custom `.dump` verification uses `pg_restore --list` when
`pg_restore` is installed on the host. Verified archive manifests can also be
recorded as backup evidence, but the primary database dump and archive copy
should be distinct artifacts. Copy manifests and checksum files to the same
independent destination after archive verification.

The application reports no backup until it has read and recorded the artifact.
The `--independent-copy` flag is an operator assertion; code also refuses to call
a path inside the project workspace independent.

## 9. Emergency recovery

1. Stop the collector; do not repeatedly restart a failing process.
2. Record `collector status`, `epoch status --number 1`, database logs, disk
   capacity, and the last verified backup status.
3. Never reset schedules, truncate tables, recreate Epoch 1, or import partial
   Epoch 0 material.
4. If the database is readable, take a new emergency dump before repair.
5. Restore only into a new database with an approved `_test`/restore-test name.
   Validate migration revision, epoch/run counts, manifest checksums, and report
   bounds there before any production cutover.
6. Mark Epoch 1 `aborted` or `invalid` with the exact reason if completeness or
   provenance cannot be demonstrated.

## 10. Twenty-four-hour validation commands

After stop/close and archive verification:

```bash
python -m pump_research report 24h \
  --epoch 1 \
  --archive-manifest '/path/to/MANIFEST' \
  --output-directory reports/epoch1
```

Outputs are Markdown, JSON, and hourly CSV. The epoch-filtered report includes
collection/run provenance, hourly discovery/DEX/pending/observation/request
facts, capacity decisions, target/effective cadence and degradation, batch
occupancy, API outcomes and latency, lifecycle transitions, dataset null and
duplicate evidence, largest durable polling gaps, PostgreSQL samples, and the
measured archive/DuckDB result when a manifest is supplied.

Also retain:

```bash
python -m pump_research collector status > reports/epoch1/final-status.json
python -m pump_research backup status --epoch 1 > reports/epoch1/backup-status.json
python -m pump_research archive stats '/path/to/MANIFEST' > reports/epoch1/archive-stats.json
python -m pump_research archive analyze '/path/to/MANIFEST' > reports/epoch1/archive-analytics.json
```

## 11. Hot/cold design — not activated

Future archive eligibility begins only after a UTC partition/range is closed and
no operational schedule needs it. The future sequence is: export, canonical and
file checksum verification, independent second copy, direct analytical reads,
explicit human review, then a separately implemented and approved PostgreSQL
retention action. Researchers would query recent PostgreSQL facts plus manifest-
enumerated Parquet ranges and reject overlaps or gaps.

Rollback retains PostgreSQL until both archive copies and analytical validation
are proven. The code's retention gate requires a verified archive, an independent
second copy, successful analytical reads, and explicit human approval. There is
currently no cleanup command, so passing that gate still cannot delete anything.

## 12. Unresolved risks

- No independent Epoch 1 backup exists until the operator runs and verifies the
  preflight procedure; this is the current NO-GO item.
- The MacBook and its Docker volume remain a single operational failure domain.
- Frequent `pg_dump` of the full high-volume database may contend with collection;
  the six-hour cadence must be observed during Epoch 1 and adjusted only after
  measurement.
- Ten-minute row counts use PostgreSQL estimates. Final epoch counts are computed
  from facts and can be expensive at much larger scale.
- Archive exports temporarily require space for Parquet staging in addition to
  hot PostgreSQL and the independent copy.
- DuckDB validates analytical usability, not a cross-hot/cold query federation
  layer. That layer remains future work.
- The realistic-looking PumpPortal key previously present in `.env.example` was
  removed and replaced with a placeholder. Git history search found no committed
  occurrence under that variable assignment, but the credential owner must still
  rotate/revoke it because repository exposure cannot be ruled out.

No trading, wallet, signing, order execution, scoring, machine learning, data
deletion, automatic retention, request-ceiling increase, or lifecycle threshold
change is part of this work.
