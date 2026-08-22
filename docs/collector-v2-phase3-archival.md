# Collector V2 Phase 3: production archival

## Pre-implementation audit (2026-08-21)

This section records the repository state inspected before Phase 3 implementation.
No live database migration, collector start, Epoch 3 creation, or historical-data
mutation was part of the audit.

### Existing exporter

`pump_research.archival` exports a closed epoch time range one UTC day at a time. It
streams SQLAlchemy mapping partitions (default 25,000 rows), builds one Arrow table per
chunk, and writes Parquet with Zstandard, dictionary encoding, and statistics. Memory is
therefore bounded approximately by one database/Arrow chunk plus encoder buffers, not by
the full range. Each table/day currently produces one file, including an empty-schema
file when no rows exist. Row groups follow export chunks; compression level and target
file size are not explicit.

The existing path is `epoch=N/<family>/year=YYYY/month=MM/day=DD/part-<range-id>.parquet`.
That is usable for epoch and time scans, but places epoch before family, is awkward for a
multi-epoch family query, and does not carry archive schema in the path. A range-wide
manifest lives below `manifests/epoch=N`.

The current archive schema is inferred directly from SQLAlchemy models. UUIDs become
strings, UTC timestamps use Arrow microsecond timestamps, PostgreSQL `NUMERIC` remains
Arrow decimal128, JSONB becomes canonical sorted compact JSON text, and nulls are
preserved. This correctly avoids lossy float conversion. It does not yet publish a
stable per-family contract independently of the current ORM, and its manifest lacks the
source Alembic revision.

### Existing family coverage

The exporter already includes full range-scoped rows for:

- observations, API request logs, discovery events;
- lifecycle events and lifecycle evidence evaluations;
- poll batches, outcomes, and members;
- coverage decisions;
- tokens and pairs needed by the selected epoch scope;
- Phase 2 pair facts, boost observations/events, metadata events, security snapshots,
  and market-context snapshots.

Important omissions are collection epoch declarations/events, collector runs/events,
discovery connectivity events, lifecycle/coverage/scheduler policy documents,
scheduler-capacity decisions, poll-schedule decisions, and deduplication conflicts.
Those omissions prevent a cold archive alone from fully explaining configuration,
runtime boundaries, source gaps, and every scheduling decision. Mutable projections and
queues (`poll_schedules`, availability/security tasks, checkpoints, component-health and
current-epoch projections) are appropriately absent, but no explicit classification is
recorded.

The current token query selects only tokens with discovery events inside the exported
range. A partial mid-epoch export can therefore contain observations/pairs for a token
discovered before the range without the token dimension row. Pair selection is based on
observations and is less restrictive. This is a referential-completeness defect for
partial ranges.

### Existing manifest and verification

The JSON manifest records scope identity, epoch, time watermark, table entries, row
counts, logical canonical bytes, Parquet bytes, compression ratio, per-file SHA-256,
canonical content SHA-256, timestamp/identifier bounds, Arrow field descriptions,
exporter revision, and a hard-coded `deletion_permitted=false`. Verification reads every
Parquet row in bounded batches and recomputes file/content digests, counts, time bounds,
and identifier bounds.

Missing production gates are:

- a checksum sidecar/self-verifiable manifest envelope;
- manifest schema validation, supported-version rejection, and aggregate file digest;
- source Alembic revision and explicit per-family archive schema versions;
- durable pending/exporting/exported/verified/copy/eligibility state;
- independent source-count/readback verification against the same closed source scope;
- DuckDB readback as part of durable verification;
- cross-family referential checks;
- an immutable record of failures and state transitions.

### Existing idempotency, interruption, and concurrency

Archive identity is deterministic over schema version, epoch, range, and the table list.
If the expected manifest exists it is verified and reused; a different manifest for the
same range is rejected. Files use temporary names and `os.replace`, and a manifest is
atomically replaced only after all files finish.

The incomplete-state behaviour is not retry-safe: a crash after a final Parquet rename
but before manifest publication leaves a file that makes retry fail as a conflict.
Temporary files have random names and no durable cleanup/recovery record. Two workers
can pass the same existence check concurrently and race on final replacement. There is
no database claim/lease, no compare-after-conflict semantic verification, and no source
snapshot/recheck protecting against a source scope changing during export.

### Existing analytical, backup, retention, and CLI support

DuckDB analytics already prove observation count, unique-token count, token time series,
observations/pairs/tokens joins, lifecycle reconstruction, a time-window scan, and a
buy/sell ratio. They require a single manifest and hard-require four core families; there
is no reusable query catalog for boost/security/context joins or multi-manifest cold
scans.

Backup verification can verify a manifest outside the project and records an operator
assertion that it is independent. It does not model primary versus secondary archive
objects, copy every manifest member, or verify a copied archive as a complete unit.
Retention has a pure fail-closed deletion authorization helper requiring a verified
archive, second copy, analytical read, and explicit approval. There is no eligibility
catalog or calculation. No deletion implementation exists.

The CLI supports `archive export`, `verify`, `stats`, and `analyze`; it lacks archive
catalog status, copy/verify-copy, retention status, and disk preflight output. Collector
status reports backups but not archive lag, verified coverage, archive bytes,
retention-eligible ranges, or the latest archive failure.

### Pre-implementation family classification

| Family | Phase 3 classification | Reason |
|---|---|---|
| tokens, pairs | full dimension snapshot per source scope | cold joins must be self-contained, including pre-range discoveries |
| observations, API request logs, discovery/lifecycle/connectivity events, lifecycle evidence | full immutable archive | required for source truth and as-of reconstruction |
| collector runs/events, epoch declarations/events | full immutable archive | epoch/runtime provenance and failure boundaries |
| lifecycle, coverage, scheduler policies and capacity/coverage/poll-schedule decisions | full immutable archive | explain classification and scheduling without hot PostgreSQL |
| poll batches, outcomes, members | full immutable archive | expected-poll outcome, lateness, and request provenance |
| Phase 2 pair/boost/metadata/security/context facts | full immutable archive | as-of research inputs |
| deduplication conflicts | full immutable archive | data-integrity evidence |
| storage samples and backup verification | compact full metadata archive/catalog retention | small operational evidence; not high-volume research facts |
| poll schedules, availability/security tasks, discovery checkpoint, component health, current epoch projection | hot-only mutable projection | restart state, not historical source truth; reconstructed/explained by immutable evidence |

The implementation below must close these gaps while retaining the existing rule that
archive and eligibility operations never delete PostgreSQL data.

## Implemented production contract

Phase 3 archive schema 2 uses one deterministic identity over the source Alembic
revision, epoch UUID/number, inclusive/exclusive range, complete family contract,
partition policy, precision/null representation, and compression policy. The catalog
derives a stable UUID from that identity. A collision whose semantic source document
differs is an integrity failure.

Exports require a closed epoch. This keeps collector-run and epoch provenance stable;
support for publishing facts from a still-running multi-month epoch remains a future
partition-closure enhancement. Export streams in configurable chunks (25,000 by
default), emits at most 1,000,000 rows/file, uses 25,000-row groups, Zstandard level 6,
dictionary encoding, and Parquet statistics. Local preflight reserves at least 2 GiB
plus a conservative staging estimate. It fails before writing Parquet when unsafe.

### Family classification

Fully archived families are:

- `tokens`, `pairs`, `observations`, `api_request_log`, `discovery_events`,
  `discovery_connectivity_events`, and `deduplication_conflicts`;
- `collection_epochs`, `collection_epoch_events`, `collector_runs`, and
  `collector_run_events`;
- lifecycle evidence/events/policies;
- coverage decisions/policies, scheduler policies/capacity decisions, and poll-schedule
  decisions;
- poll batches, batch outcomes, and batch members;
- Phase 2 pair facts, boost observations/events, token metadata events, token-security
  snapshots, and market-context snapshots;
- compact storage samples/relation samples and backup-verification evidence.

No lossy summary replaces a research/provenance family. Mutable operational projections
remain hot-only: poll schedules, DEX/security tasks and leases, discovery checkpoints,
component-health projections, and current-epoch projection. Archive catalog rows also
remain compact hot metadata and belong in DB backups; including a catalog inside the
archive it is currently constructing would be recursive.

The partial-range token query now includes identities referenced by discovery,
observations, lifecycle/coverage facts, boosts, metadata, or security snapshots, including
tokens admitted before the range. Pair dimensions likewise include pair-fact and boost-only
references. DuckDB verification rejects missing core and Phase 2 token/pair dependencies.

## Parquet layout and field contract

```text
<root>/
  schema=v2/
    family=observations/
      year=2026/month=08/day=16/epoch=2/
        scope=<64-char-identity>/part-00000.parquet
    family=boost_observations/...
    manifests/epoch=2/scope=<identity>/
      manifest.json
      manifest.sha256
```

Family-first Hive partitions make multi-epoch family/time scans natural while retaining
epoch and scope pruning. Scope dimensions/policies are exported once per archive unit;
time facts are partitioned by UTC day. File splitting avoids tiny per-token files and
multi-hundred-GB objects.

UUIDs are canonical strings. UTC timestamps are microsecond Arrow timestamps and retain
separate source/received/persisted columns. PostgreSQL `NUMERIC` is decimal128 at its
declared precision/scale, including tiny prices and reserve quantities. JSONB is sorted,
compact deterministic UTF-8 JSON text. Null remains null and is counted per column;
unknown never becomes zero or false.

Each family contract carries stable field names/types/nullability, a family schema
version, primary-key columns, source table, scope mode, and source-query digest. A source
schema change therefore creates a different archive identity rather than silently
reinterpreting an existing file.

## Manifest and verification

The immutable manifest records archive/source schema versions, scope/epoch and validity,
watermark, family contracts, layout/sort/compression, row and logical/Parquet bytes,
per-file paths/bytes/SHA-256/content SHA-256, aggregate digest, null counts, schemas,
identifier/timestamp bounds, exporter revision, started/completed/verified times, source
row-count recheck, and `deletion_permitted=false`. `manifest.sha256` independently
protects the manifest bytes.

Verification fails closed unless all of the following pass:

1. manifest schema is supported and its sidecar digest matches;
2. every Parquet object exists, opens, and is read completely in bounded batches;
3. Arrow schema, row count, null counts, identifiers and timestamp bounds match;
4. file SHA-256 and canonical row-content SHA-256 match;
5. aggregate file digest and manifest/source totals match;
6. PostgreSQL counts still match immediately after export;
7. DuckDB reads every family, finds no duplicate primary identities, and passes core
   referential checks;
8. primary/copy content length and full read-after-upload SHA-256 match.

Legacy schema-1 archives remain readable but intentionally report no schema-2 manifest,
source-coverage, or DuckDB eligibility proof and can never satisfy Phase 3 retention
eligibility.

## Catalog, idempotency, and recovery

Migration `c61e29d841af` adds `archive_scopes` plus immutable
`archive_scope_events`, `archive_copy_verifications`, and
`archive_retention_evaluations`. Catalog states are `pending`, `exporting`, `exported`,
`verified`, `independently_copied`, `retention_eligible`, and `failed`.

A row lock and expiring claim token serialize workers. Files are produced below the
scope's `.incomplete` directory, then promoted with atomic same-filesystem renames.
Manifest publication is last. After a crash, stale staging is removed only inside the
validated incomplete scope; already promoted objects are reused only when their hashes
match. Different content under an existing key raises an integrity error. A retry after
files but before manifest, after an interrupted copy, or after lease expiry is therefore
safe and deterministic.

The filesystem object store atomically uploads and reads back objects. The provider-
neutral protocol also has an S3-compatible adapter over a minimal SDK client contract;
it sends content length and SHA-256, then HEADs and fully reads the uploaded object.
Provider credentials are owned by the injected SDK/environment and never enter a
manifest. No AWS, Hetzner, Cloudflare, or other vendor is hard-coded.

`secondary` verification requires an explicit operator assertion plus explanation of
the independent failure domain. Merely choosing another directory is not treated as
proof of physical independence.

## Retention eligibility (metadata only)

`retention status` creates an immutable evaluation and marks a scope eligible only when:

- archive and manifest verification passed under supported schema 2;
- full source-scope coverage and analytical reads passed;
- a primary copy and an explicitly independent verified secondary copy match the
  canonical manifest/file digest;
- there is no unresolved integrity failure;
- the epoch is closed and effective epoch validity is true;
- the configured minimum hot age (14 days by default) passed.

There is no delete, detach, drop, compaction, or space-reclamation command. Eligibility
is metadata; the older explicit-human-approval deletion gate also remains in force for a
future separately approved phase.

## DuckDB usage

`ColdArchiveQuery` opens one or more manifests and creates family views directly over
Parquet. It supports token history, time windows, epoch token dimensions, lifecycle
chronology, first/peak market cap, and boost/security/context as-of reads. It never
restores data into PostgreSQL.

```python
from pathlib import Path
from pump_research.archive_analytics import ColdArchiveQuery

with ColdArchiveQuery([Path(".../manifest.json")]) as cold:
    history = cold.observations_for_token("<mint>")
    context = cold.enrichment_as_of("<mint>", "2026-08-16T09:00:00Z")
```

Operational commands are:

```text
python -m pump_research archive export --epoch N --from ... --to ... --output ...
python -m pump_research archive verify <manifest>
python -m pump_research archive analyze <manifest>
python -m pump_research archive status --epoch N
python -m pump_research archive copy <manifest> --output ... --role secondary \
  --independent-copy --independence-detail "separate provider/device"
python -m pump_research archive verify-copy <manifest> --output ... --role secondary \
  --independent-copy --independence-detail "separate provider/device"
python -m pump_research retention status --scope-id ... --minimum-hot-days 14
```

## Epoch 2 benchmark and disk decision

The Mac had only 5.84 GB decimal free before the benchmark. A complete 20+ GB Epoch 2
export could not safely coexist with source, staging, and safety space, so it was not
attempted. The production disk guard made the intended fail-closed decision.

A new read-only benchmark used the busiest Epoch 2 hour, 2026-08-16 08:00–09:00 UTC:

| Metric | Measured result |
|---|---:|
| rows | 329,583 |
| PostgreSQL logical heap bytes | 84,676,584 |
| Parquet/ZSTD bytes | 21,742,062 |
| logical/Parquet ratio | 3.895:1 |
| export throughput | 42,733 rows/s |
| full readback throughput | 3,480,081 rows/s |
| DuckDB aggregate | 13.176 ms |
| maximum process RSS after run | 407,355,392 bytes |

The file checksum was
`0e13348828e5e9e596bc16e1d06d0b422fea70d3fa682874cbbeb95197e5e68d`.
This benchmark is explicitly labelled non-canonical because the live DB remains at
`f2c8d4a6197e`; it exports the existing core observation columns without Phase 3 catalog
writes. The earlier 10-minute all-family Epoch 2 benchmark measured 115,750 rows,
15,241,303 Parquet bytes, 5.586:1 logical compression, and a 2.044 GiB/day same-load
extrapolation. Together they support a conservative 1.8–3.0 GiB/day full-archive planning
range until a complete schema-2 validation export is possible on adequately sized disk.

## Failure and concurrency verification

Tests inject mid-export crashes, publication interruption after final files, source-row
changes, corrupt Parquet, file and manifest checksum changes, wrong schema, row-count
mismatch, insufficient disk, interrupted/duplicate uploads, missing/mismatched remote
objects, absent secondary independence, and concurrent workers. Every case fails closed;
retry reuses only checksum-identical content. The complete suite passed 155 tests.

## Current limitation

Phase 3 deliberately requires a closed epoch. Long-running production epochs will need
an explicit immutable partition/range-closure contract before same-epoch daily scopes
can become retention-eligible. Phase 3 implements no retention execution and does not
make the current unpartitioned API log or other legacy tables independently droppable.
