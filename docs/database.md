# Database design

The initial schema is designed for source provenance and high-volume append-only observations. It now also contains a small mutable projection for the approved initial DEX-availability admission workflow; discovery adapters, market-data clients, and general polling policy remain outside the schema.

## Design principles

- All timestamps are `timestamptz`. Repository writes reject naïve datetimes and normalize aware input to UTC; async database sessions also set PostgreSQL's timezone to UTC.
- Tokens and pairs are stable identities; static identity attributes are not repeated in observations.
- `api_request_log`, `discovery_events`, `observations`, and `lifecycle_events` are immutable at the database level. PostgreSQL triggers reject row updates and deletes.
- `collector_runs` is intentionally mutable only to record the eventual end/status of a process invocation.
- `dex_availability_tasks` is an intentionally mutable, leased operational projection. Its append-only lifecycle events remain the state-history record.
- Source evidence, normalized market facts, and derived lifecycle decisions use separate tables and never overwrite one another.

## Tables

### `tokens`

One provider-neutral tracked token identity. `chain` plus `address` is unique; `id` is the internal UUID used by foreign keys. `first_discovered_at` is a nullable source-time value and `persisted_at` records database knowledge time. The repository has no delete operation, and dependent foreign keys use `RESTRICT`.

### `pairs`

One canonical pair per `chain` plus pair `address`, linked to its tracked token with `token_id`. A token can have any number of pairs. `dex_identifier` and first-seen data are pair-level static attributes, avoiding duplication in every observation. `ix_pairs_token_id` supports loading all pairs for one token.

### `collector_runs`

Operational provenance for a collector invocation. It records start/end time, status, collector version, and the full immutable configuration snapshot plus SHA-256 digest. `ix_collector_runs_started_at` supports run-history and incident queries. This is not a source-fact table and may be finalized from `running` to a terminal status.

### `api_request_log`

One immutable external API attempt/result, including request time, receipt time, outcome, HTTP status, request body, raw response body, response hash, failure detail, and optional collector run. It is the durable raw evidence for a batched response, so that response data is stored once rather than copied into every pair observation. `idempotency_key` is globally unique. Indexes support provider/time investigations and run-level audits.

### `discovery_events`

Immutable source evidence associated with a token. It preserves provider identity, provider event ID when available, source event time, collector receipt time, raw source payload, and payload hash. `idempotency_key` is globally unique, so repeated delivery does not create a duplicate event. Indexes support both token knowledge-time history and provider/source-time coverage analysis.

### `dex_availability_tasks`

One current-work projection per token for initial DEX availability. New discovery creates a `PENDING_DEX` row and an append-only matching lifecycle event in the same transaction. The row records its next check time, latest check, attempt count, and an expiring lease. An empty DEX result keeps the row in `PENDING_DEX` and advances `next_check_at`; a matching pair changes it to `NEW`. Neither operation deletes the token or its source evidence.

The partial index `ix_dex_availability_tasks_due_pending` contains only `PENDING_DEX` rows ordered by `next_check_at`, keeping due-work claims efficient even after many tokens become `NEW`. Leases are intentionally reclaimable after expiry, so a reboot after a claim cannot strand a token. This table does not contain raw DEX facts: request/response evidence remains in `api_request_log` and every state decision is separately append-only in `lifecycle_events`.

### `observations`

Immutable normalized market measurements for one pair from one API request. It includes source observation time when supplied, a locator into the parent raw response, source-record hash, and typed decimal market values. `received_at` is the collector knowledge-time and partition key; metrics never carry lifecycle state or static token metadata.

The primary key is `(received_at, id)` because PostgreSQL requires the partition key in a partitioned-table primary key. Idempotency is `(received_at, api_request_log_id, pair_id)`: one request can create at most one observation per pair, while unchanged metrics from later successful requests remain distinct observations. `ix_observations_pair_received_at` is the primary time-series access path.

### `lifecycle_events`

Immutable derived state transitions, isolated from source facts. It stores prior/new state, decision time, input watermark, reason, full configuration snapshot and digest, and optional structured reason detail. `idempotency_key` is globally unique. `ix_lifecycle_events_token_decided_at` supports reconstructing state history as known at a decision time.

## Expected query patterns

- Find a token by `(chain, address)`; then load its pairs via `ix_pairs_token_id`.
- Read a pair's observation series over a bounded receipt-time range via `ix_observations_pair_received_at` and only the relevant monthly partitions.
- Join an observation to `api_request_log` to recover request/receipt timing and the original batch payload/response.
- Reconstruct discovery knowledge by token/receipt time, or source coverage by provider/source event time.
- Claim the earliest due `PENDING_DEX` tasks with `FOR UPDATE SKIP LOCKED`, while allowing expired leases to be recovered.
- Reconstruct lifecycle decisions by token/decision time, retaining their exact input watermark and configuration snapshot.
- Investigate failures or gaps by provider/request time and collector run.

## Scalability and partitioning

`observations` is the only initial table expected to grow beyond 100M rows. It uses monthly range partitions on `received_at`, which keeps receipt-time queries and retention/archival operations bounded without creating a partition per token. The initial migration creates monthly partitions from January 2026 through December 2027. There is intentionally no default partition: an uncovered month fails loudly instead of silently placing high-volume data into an unbounded catch-all table.

Before the final provisioned month, an approved operational migration must create additional partitions. Partition interval, index count, and retention timing must be revalidated with actual observations/day, pair multiplicity, row/index size, WAL volume, and query workload. The observation index set is intentionally limited to the pair/time series path plus the partition-compatible idempotency constraint. Bulk inserts should use the repository's multi-row path, not ORM object graphs.

Raw response JSON is stored per API request rather than per observation. This relies on request batch membership being preserved in the raw request payload and on each observation retaining a source-record locator/hash. Future Parquet archival must retain the same request-to-observation provenance relationship.
