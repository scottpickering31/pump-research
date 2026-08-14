# Adaptive scheduler

The adaptive scheduler is a bounded PostgreSQL-backed planner for recurring
DEX market observations. It does not classify tokens and does not interpret
market values. A lifecycle transition supplies one of six states; the policy
uses that state only to choose the next interval and a tie-breaking priority.

| Lifecycle state | Default interval |
| --- | ---: |
| `NEW` | 5 seconds |
| `ACTIVE` | 5 seconds |
| `WATCH` | 15 seconds |
| `FADING` | 60 seconds |
| `DORMANT` | 15 minutes |
| `RESURRECTED` | 5 seconds |

All intervals are configurable through the corresponding
`PUMP_RESEARCH_SCHEDULER_*_INTERVAL_SECONDS` environment variables. Every
schedule decision, claimed batch, and completion stores the complete policy
snapshot and SHA-256 digest used at that time.

## Durable scheduling and restart behavior

`poll_schedules` is the small mutable projection used to find due work. The
authoritative evidence remains append-only in schedule decisions, claimed
batches, batch memberships, batch outcomes, lifecycle events, and API request
logs. A new process reads the same due times and leases directly from
PostgreSQL; it does not reconstruct an in-memory queue. Expired leases are
reclaimable, and the replacement membership records the abandoned prior batch
ID. A stale worker is rejected if it later tries to complete a reclaimed batch.

Applying a lifecycle transition changes only future work. Reapplying the same
state under the same policy is idempotent and cannot move the existing due time
forward. This prevents a stream of duplicate state evidence from starving a
token.

## Batching, priority, and fairness

One claim returns one chain-specific batch containing no more than 30 token
addresses. PostgreSQL row locks and a transaction-scoped advisory lock prevent
concurrent workers from claiming the same schedule. The scheduler creates no
background or unbounded in-memory queue; `scheduler_max_in_flight_batches`
bounds outstanding leased batches.

Selection is earliest-due-first. Lifecycle priority breaks equal due times in
this order: `NEW`, `RESURRECTED`, `ACTIVE`, `WATCH`, `FADING`, `DORMANT`.
Keeping due time as the primary key means sufficiently overdue dormant work
cannot be starved by a continuous stream of newly due high-priority work.

Before a batch is claimed, it reserves the DEX client's configured maximum
attempt count against the rolling one-minute request budget. Claims are
serialized in PostgreSQL, so multiple scheduler processes cannot collectively
reserve more request capacity than configured. The HTTP client still applies
its limiter to every actual attempt and retry.

## Lateness measurements

Every membership persists its original `due_at`, actual `claimed_at`, and
`claim_lateness_ms`. Completion persists the minimum, maximum, and mean
observation lateness across the batch, measured from each member's original due
time. Exact per-token completion lateness remains reconstructable by joining a
batch outcome's completion time to its immutable memberships.

`poll_batch_members` is monthly partitioned because it grows once per token per
poll and may reach observation-scale volume. Missing future partitions fail
loudly and must be provisioned before the current horizon ends.

## Network-free load simulation

The integration suite bulk-loads 3,007 auditable token schedules and runs two
saturated rolling-minute windows using PostgreSQL and a fake clock. It never
constructs or invokes the DEX Screener client. The scenario reserves three
possible attempts per 30-address batch against a configured 240-request ceiling.

The deterministic reference result is 160 batches at 100% mean occupancy,
240 maximum reserved requests per rolling minute, 607 observations still
overdue, and claim-lateness p50/p95/p99 of 0/60,000/60,000 ms. The overdue count
is intentional evidence that this token population and cadence can exceed
available request capacity; it must remain visible to operations and later
data-quality reporting.

Separate adversarial tests race concurrent claimers, verify that completed
batches do not release their rolling-window reservation early, and exercise the
exact 60-second boundary. The HTTP limiter is also driven through two simulated
minutes of saturated demand to prove that request starts remain within its
configured rolling ceiling without issuing network traffic.
