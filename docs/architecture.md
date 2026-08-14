# Architecture constraints for a long-running research collection

This document records Phase 0 constraints, not an implemented schema. It exists to prevent later implementation choices from undermining a 3–12 month experiment or making a 100M+ observation corpus scientifically ambiguous.

## Data boundaries

The design separates four kinds of persisted data:

1. **Source evidence** — append-only discovery events, provider responses, explicit empty responses, and enough request/provenance metadata to interpret them.
2. **Normalized facts** — append-only typed values extracted from source evidence without lifecycle interpretation.
3. **Operational state and evidence** — mutable future schedule/lease projections plus append-only attempts, batch membership, errors, and checkpoints.
4. **Derived research state** — append-only lifecycle transitions and other reproducible calculations, linked to input cutoffs and configuration versions.

Raw observations must never contain current lifecycle labels. Derived state may reference raw/normalized facts, but it may not rewrite them.

## Identity and provenance

- The canonical token identity is `(chain, token_address)`. Provider record IDs are aliases/provenance, not token identity.
- A discovery occurrence is an immutable provider event linked to a token. Multiple providers or duplicate deliveries can therefore be retained without creating duplicate tokens.
- DEX pairs are distinct entities identified by chain plus pair address. A token can participate in multiple pairs; provider IDs remain provenance rather than canonical identity. Pair discovery and each pair observation remain facts; any “primary pair” is a versioned derived selection.
- Each external response needs a durable request/attempt identity, provider and endpoint identity, response status, parser/schema version, collector version, and payload provenance.
- Raw payload retention, compression, terms-of-use, and redaction rules must be decided before collection. Full payloads or a losslessly reversible durable encoding are preferred; a normalized projection alone cannot silently substitute for source evidence.

## Time model and as-of reconstruction

At minimum, distinguish:

- `source_event_at`: time asserted by the source, nullable;
- `requested_at`: request start;
- `received_at`: response completion/collector receipt;
- `persisted_at`: successful durable commit;
- `due_at`: when the scheduler intended the poll to begin;
- `decided_at`: when a lifecycle or schedule decision was made; and
- `input_watermark`: latest evidence permitted as input to that decision.

All persisted timestamps are UTC-aware. Provider times retain documented precision. A collector timestamp must never be presented as a provider/source timestamp.

Two historical questions must remain answerable:

- Event-time: what does the source say was true or occurred by time T?
- Knowledge-time: what had this collector durably learned by time T?

Lifecycle and reports must declare which axis and cutoff they use. Backfilled events retain their original source time and later receipt/persistence times, preventing them from leaking into earlier knowledge-time analyses.

## Completeness and collection gaps

An observation table alone cannot measure missingness. The system will persist append-only poll obligations/schedule decisions and an attempt ledger containing due time, token/pair or batch membership, claim/lease, request start/end, endpoint, outcome, retry lineage, and completion time. A mutable “next due” row may accelerate scheduling but cannot be the only evidence that work was expected. Outcomes distinguish at least success-with-data, success-empty, partial response, malformed response, throttled, transport failure, persistence failure, cancelled, and expired/unattempted work.

Discovery adapters must expose their coverage semantics: cursor/sequence where available, checkpoint time, reconnect boundaries, backfill support, and detected discontinuities. A checkpoint cannot advance past an event until the event is durably stored. If a provider cannot prove completeness, reports must state that limitation rather than imply full discovery coverage.

Daily data-quality reports can then compare due work with completed outcomes, quantify lateness and scheduler lag, report API and database failures, identify sequence/cursor gaps, and distinguish provider-empty results from collector gaps.

## Restart and idempotency model

PostgreSQL is the system of record. In-memory work queues are accelerators only.

- Future work is claimed with bounded leases. A crashed worker's lease expires and another worker may retry it.
- Durable idempotency keys cover discovery delivery, request attempts, per-member observations, and lifecycle transitions.
- A retry does not overwrite its predecessor; attempts are linked in a retry lineage.
- Repeated identical values from separate successful polls remain separate observations.
- Transactions must ensure that source evidence and required checkpoint changes cannot diverge.
- Startup reconciles expired leases and overdue work rather than constructing state from logs.

Exact transaction boundaries and uniqueness keys will be decided with the schema. They must be tested against crashes before commit, after external response but before commit, and after commit but before acknowledgement.

## API budget and adaptive polling

One shared DEX Screener budget governs all request paths and concurrent workers. Batch planning deduplicates due addresses, observes provider batch-size limits, records batch membership, and reserves conservative rate-limit headroom for retries and operational variance. Retries pass through the same limiter and obey provider feedback. The effective limit and safety-margin configuration are versioned so historical request policy is reconstructable.

Polling tiers are collection policies, not token quality labels. Transitions use only evidence available at `decided_at`, record their configuration version and input watermark, and affect only future `due_at` values. Every token retains a low-frequency liveness probe so dormant assets can reactivate.

Before cadence is approved, calculate:

```text
requests/day = sum(tier_token_count × polls_per_day) / effective_batch_size
observations/day = sum(tier_token_count × polls_per_day × mean_pairs_returned)
```

Both estimates need safety factors for retries, partial batches, pair growth, discovery bursts, and provider limit changes.

## PostgreSQL at 100M+ observations

The persistence design must be validated with a volume model and representative load test before long-running collection begins.

- Partition large append-only fact tables by a bounded time interval appropriate to actual volume; avoid one partition per token.
- Keep common filter/join fields in narrow typed columns. Provider payloads may use JSONB for provenance, but hot analytical predicates must not depend on repeatedly scanning large JSON documents.
- Minimize indexes on write-heavy partitions and justify each by a concrete query, idempotency, or archival need.
- Use batch/bulk insert paths. Do not instantiate 100M observations as long-lived ORM object graphs.
- Separate small mutable scheduling projections from large append-only facts to limit update churn and vacuum pressure.
- Decide idempotency and partition keys together because PostgreSQL unique constraints on partitioned tables constrain the allowed key shape.
- Define partition creation, index maintenance, backup/restore, vacuum/analyze, disk alerting, and query-timeout procedures before unattended operation.

Capacity planning must include raw table bytes, JSON/payload bytes, indexes, WAL, temporary headroom, backups, archive staging, expected compression, and growth during recovery from downtime.

## Archival correctness

Parquet archival preserves data; it is not permission to discard inconvenient tokens. Each archive unit requires a durable manifest with schema version, source partition/range, row count, minimum/maximum keys and timestamps, file checksum, creation time, and verification state.

Files are published atomically and independently read back before a manifest becomes verified. Any later PostgreSQL partition removal requires a separately approved retention policy and a verified archive. Cross-tier queries and reports must detect missing ranges, overlaps, duplicate rows, corrupt files, and incompatible schema versions.

## Decisions intentionally deferred

Phase 0 does not choose table definitions, partition interval, polling thresholds, archive retention, a discovery provider, or exact DEX Screener endpoints. Those decisions require verified provider contracts, expected volume, storage budget, and operational recovery objectives.
