# Adaptive scheduler

The adaptive scheduler is a bounded PostgreSQL-backed planner for recurring
DEX market observations. It does not classify tokens and does not interpret
market values. A lifecycle transition supplies one of six states; the policy
uses that state only to choose the next interval and a tie-breaking priority.

| Lifecycle state | Default interval |
| --- | ---: |
| `NEW` | 15 seconds for 2 minutes, then 30 seconds |
| `ACTIVE` | 5 seconds |
| `WATCH` | 15 seconds |
| `FADING` | 120 seconds |
| `DORMANT` | 15 minutes |
| `RESURRECTED` | 5 seconds |

All intervals and capacity headroom are configurable. The scheduler computes
requested and effective rates every 30 seconds. Static policy documents are
normalized by SHA-256; immutable capacity decisions store population, demand,
mode, and target/effective intervals. Schedules, decisions, batches, and members
reference the responsible capacity decision. See
[`capacity-aware-scheduler.md`](capacity-aware-scheduler.md) for the algorithm,
live-population mathematics, simulation, and restart gate.

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

Selection is lifecycle-priority-first in this order: `ACTIVE`, `RESURRECTED`,
`NEW`, `WATCH`, `FADING`, `DORMANT`; due time and token UUID provide deterministic
within-tier fairness. Capacity allocation gives every populated tier a positive
finite rate, so strict priority does not depend on an impossible high-priority
queue eventually emptying.

Before a batch is claimed, it reserves one normal request against the safe
rolling budget (192/minute at the default 240 ceiling and 20% headroom). Claims
are serialized in PostgreSQL, so workers cannot collectively overshoot that
reservation. Retries and admission requests use the reserved headroom, and the
HTTP client applies the shared hard limiter to every actual attempt.

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

Unit simulation runs four hours at the supplied 11,585-token population and a
separate 30-admissions/minute stress assumption. PostgreSQL integration tests
also race concurrent claimers and exercise rolling-window boundaries. All use a
fake clock and no DEX network calls. Measured results are in
[`capacity-aware-scheduler.md`](capacity-aware-scheduler.md).
