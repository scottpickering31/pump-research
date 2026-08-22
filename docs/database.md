# Database design

The schema is designed for source provenance and high-volume append-only observations. It contains small mutable projections for initial DEX admission and recurring adaptive polls; discovery adapters and market-data clients remain outside the schema. Lifecycle classification writes immutable evidence plus the separate schedule projection.

## Design principles

- All timestamps are `timestamptz`. Repository writes reject naïve datetimes and normalize aware input to UTC; async database sessions also set PostgreSQL's timezone to UTC.
- Tokens and pairs are stable identities; static identity attributes are not repeated in observations.
- `api_request_log`, `discovery_events`, `discovery_connectivity_events`, `observations`, `lifecycle_policies`, `lifecycle_evidence_evaluations`, `lifecycle_events`, scheduler decisions, poll batches, memberships, and outcomes are immutable at the database level. PostgreSQL triggers reject row updates and deletes.
- `collector_runs` is intentionally mutable only to record the eventual end/status of a process invocation.
- `dex_availability_tasks` is an intentionally mutable, leased operational projection. Its append-only lifecycle events remain the state-history record.
- `discovery_checkpoint_states` is a mutable provider-neutral cursor projection advanced only in the transaction that persists its discovery batch.
- `deduplication_conflicts` is append-only evidence of deliveries rejected by an idempotency constraint; it makes duplicate rates measurable without duplicating every accepted fact.
- `poll_schedules` is the recurring-poll projection. Scheduler decisions, batch claims, memberships, and outcomes remain immutable evidence.
- Source evidence, normalized market facts, and derived lifecycle decisions use separate tables and never overwrite one another.

## Tables

### `tokens`

One provider-neutral tracked token identity. `chain` plus `address` is unique; `id` is the internal UUID used by foreign keys. `first_discovered_at` is a nullable source-time value and `persisted_at` records database knowledge time. The repository has no delete operation, and dependent foreign keys use `RESTRICT`.

### `pairs`

One canonical pair per `chain` plus pair `address`, linked to its tracked token with `token_id`. A token can have any number of pairs. `dex_identifier` and first-seen data are pair-level static attributes, avoiding duplication in every observation. `ix_pairs_token_id` supports loading all pairs for one token.

### `collector_runs`

Operational provenance for a collector invocation. It records start/end time, status, collector version, and the full immutable configuration snapshot plus SHA-256 digest. `ix_collector_runs_started_at` supports run-history and incident queries. This is not a source-fact table and may be finalized from `running` to a terminal status.

### `collector_run_events`

Immutable evidence for graceful stops, failures, and lock-protected stale-run
reconciliation. Its unique terminal idempotency key permits one semantic terminal
event per run; contradictory repeat finalization is rejected.

### `api_request_log`

One immutable external API attempt/result, including request time, receipt time, outcome, HTTP status, request body, raw response body, response hash, failure detail, and optional collector run. It is the durable raw evidence for a batched response, so that response data is stored once rather than copied into every pair observation. `idempotency_key` is globally unique. Indexes support provider/time investigations and run-level audits.

### `discovery_events`

Immutable source evidence associated with a token. It preserves provider identity, provider event ID when available, source event time, collector receipt time, raw source payload, and payload hash. `idempotency_key` is globally unique, so repeated delivery does not create a duplicate event. Indexes support both token knowledge-time history and provider/source-time coverage analysis.

### `discovery_checkpoint_states`

One current opaque checkpoint per cursor-capable discovery source. It stores the source namespace, opaque cursor or validator, last durable batch receipt time, and coverage semantics. The coordinator advances it only after every event in the batch has been admitted in the same transaction. A disconnect leaves the prior value intact, and a restarted process supplies that exact value back to the replaceable source adapter. PumpPortal has no replay cursor and therefore does not create this projection; its live batches use post-commit acknowledgement plus separate connectivity-gap evidence.

### `discovery_connectivity_events`

Append-only disconnect and successful-resubscription boundaries for live discovery providers.
Both boundaries share a provider-neutral `gap_id`; `source_name`, UTC `observed_at`, `reason`, and
bounded diagnostic `detail` preserve what the collector observed without leaking provider payloads
into other domains. A unique idempotency key prevents duplicate gap boundaries. The
`(source_name, observed_at)` index supports coverage reports, while `gap_id` pairs boundaries.
An unmatched disconnect is an explicitly open gap, not an assertion of complete coverage.

### `deduplication_conflicts`

One append-only row for each duplicate delivery rejected by discovery, request,
observation, or lifecycle idempotency. Accepted records remain in their source
fact table, so this table grows only with duplicate traffic. Its
`record_type, occurred_at` index supports time-bounded duplicate-rate reporting.
It is operational evidence; it does not replace or mutate the retained source
record.

### `dex_availability_tasks`

One current-work projection per token for initial DEX availability. New discovery creates a `PENDING_DEX` row and an append-only matching lifecycle event in the same transaction. The row records its next check time, latest check, attempt count, and an expiring lease. An empty DEX result keeps the row in `PENDING_DEX` and advances `next_check_at`; a matching pair changes it to `NEW`. Neither operation deletes the token or its source evidence.

The partial index `ix_dex_availability_tasks_due_pending` contains only `PENDING_DEX` rows ordered by `next_check_at`, keeping due-work claims efficient even after many tokens become `NEW`. Leases are intentionally reclaimable after expiry, so a reboot after a claim cannot strand a token. This table does not contain raw DEX facts: request/response evidence remains in `api_request_log` and every state decision is separately append-only in `lifecycle_events`.

### `observations`

Immutable normalized market measurements for one pair from one API request. It includes source observation time when supplied, a locator into the parent raw response, source-record hash, and typed decimal market values. `received_at` is the collector knowledge-time and partition key; metrics never carry lifecycle state or static token metadata.

The primary key is `(received_at, id)` because PostgreSQL requires the partition key in a partitioned-table primary key. Idempotency is `(received_at, api_request_log_id, pair_id)`: one request can create at most one observation per pair, while unchanged metrics from later successful requests remain distinct observations. `ix_observations_pair_received_at` is the primary time-series access path.

### `lifecycle_policies`

One immutable JSON policy document per `policy_sha256`. The primary-key digest
is the stable identity used by lifecycle-evidence evaluations; `created_at`
records when that policy was first persisted. An immutability trigger rejects
updates and deletes. Inserting an already-known digest is accepted only when
the supplied JSON document is identical.

### `lifecycle_evidence_evaluations`

Immutable derived evidence describing how one token's pair observations from
one API response were reduced to one lifecycle input. It records `selected` or
`failed`, the selected observation/pair when successful, a canonical candidate
snapshot and reason, and the exact normalized selection-policy digest. The
partition-compatible unique key is input watermark plus token, API request, and
policy digest. The table is monthly partitioned by `input_watermark`; the
token/watermark index supports historical selection reconstruction. It neither
replaces nor mutates the underlying observations. Historical rows retain their
original inline `policy_snapshot`; new rows leave that compatibility column
null and resolve the document through the restricted foreign key to
`lifecycle_policies`.

### `lifecycle_events`

Immutable derived state transitions, isolated from source facts. It stores prior/new state, decision time, input watermark, rule reason, full configuration snapshot and digest, and structured reason detail. Observation-driven transitions reference the exact lifecycle-evidence evaluation; admission events have no evidence-selection reference. The lifecycle classifier records the selected normalized values and responsible thresholds; source facts remain in `observations` and `api_request_log`. `idempotency_key` is globally unique. `ix_lifecycle_events_token_decided_at` supports reconstructing state history as known at a decision time.

### `poll_schedules`

One mutable current projection per recurring token. It contains lifecycle state, priority, next due time, latest due/start/completion times, attempt count, policy version, effective-capacity decision, target/effective interval, and an expiring batch lease. Claims use lifecycle priority followed by due time and token UUID; the capacity planner bounds each tier's aggregate renewal demand before strict priority is applied.

### `poll_schedule_decisions`

Immutable evidence of initial scheduling, lifecycle-driven cadence changes, and configuration changes. It preserves previous/new state, previous/new due time, reason, decision time, policy digest, capacity-decision reference, and target/effective interval. Same-state delivery under an unchanged policy is idempotent and does not postpone an existing obligation.

### `scheduler_policies` and `scheduler_capacity_decisions`

`scheduler_policies` stores one immutable reconstructable policy document per SHA-256 digest. `scheduler_capacity_decisions` stores one immutable time-bucket population/demand calculation and its effective intervals. This normalizes static configuration instead of copying it into every dynamic decision. Database triggers reject updates and deletes.

### `poll_batches`

Immutable evidence that a worker claimed one single-chain batch. It records claim and lease-expiry times, one normal-request reservation, and the capacity decision used. The provider/time index supports the safe rolling request-budget calculation and request-rate audits. Retry attempts remain subject to the process-wide HTTP limiter and safety headroom.

### `poll_batch_members`

Immutable per-token membership for each claimed batch: original due time, claim time, lifecycle state, priority, claim lateness, target/effective interval, capacity-decision reference, and any expired predecessor batch. This makes late, degraded, retried, and abandoned obligations measurable after restart. The table is monthly partitioned on `claimed_at` because it grows at approximately one row per scheduled token poll. `ix_poll_batch_members_token_claimed_at` supports bounded predecessor lookup for per-token cadence and largest-gap reporting.

### `poll_batch_outcomes`

At most one immutable completion per batch. It records outcome, completion time, optional API-request provenance, member count, failure detail, completion policy, and min/max/mean observation lateness. A claimed batch without an outcome is explicit evidence of interrupted work rather than a silent gap.

## Expected query patterns

- Find a token by `(chain, address)`; then load its pairs via `ix_pairs_token_id`.
- Read a pair's observation series over a bounded receipt-time range via `ix_observations_pair_received_at` and only the relevant monthly partitions.
- Join an observation to `api_request_log` to recover request/receipt timing and the original batch payload/response.
- Reconstruct discovery knowledge by token/receipt time, or source coverage by provider/source event time.
- Claim the earliest due `PENDING_DEX` tasks with `FOR UPDATE SKIP LOCKED`, while allowing expired leases to be recovered.
- Claim one chain-specific recurring poll batch, audit its members and request reservation, and reject stale completion after lease recovery.
- Measure due-to-claim and due-to-completion lateness over bounded receipt-time ranges.
- Reconstruct lifecycle decisions by token/decision time, retaining their exact input watermark and configuration snapshot.
- Reconstruct pair selection by token/input watermark, join `policy_sha256` to
  `lifecycle_policies`, then join candidate and selected identifiers back to
  immutable observations and the raw API response.
- Investigate failures or gaps by provider/request time and collector run.
- Produce the 24-hour report through time-bounded aggregates; it uses historical lifecycle events rather than mutable task/schedule projections, and uses the token/claim-time index to obtain only the immediately preceding claim for each token in the window.

## Scalability and partitioning

`observations`, `lifecycle_evidence_evaluations`, and `poll_batch_members` may grow beyond 100M rows. All use monthly range partitions on their collector timestamps, which keeps time-bounded queries and retention/archival operations bounded without creating a partition per token. Their migrations create monthly partitions from January 2026 through December 2027. There is intentionally no default partition: an uncovered month fails loudly instead of silently placing high-volume data into an unbounded catch-all table.

Before the final provisioned month, an approved operational migration must create additional partitions. Partition interval, index count, and retention timing must be revalidated with actual observations/day, pair multiplicity, row/index size, WAL volume, and query workload. The observation index set is intentionally limited to the pair/time series path plus the partition-compatible idempotency constraint. Bulk inserts should use the repository's multi-row path, not ORM object graphs.

Raw response JSON is stored per API request rather than per observation. This relies on request batch membership being preserved in the raw request payload and on each observation retaining a source-record locator/hash. Future Parquet archival must retain the same request-to-observation provenance relationship.
