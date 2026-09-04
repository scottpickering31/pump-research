# 24-hour collection report

`python -m pump_research report 24h --epoch 1` creates Markdown, JSON, and hourly
CSV output. The default destination is the ignored `reports/` directory. Epoch
number is the primary provenance filter; use `--end-at` for a reproducible cutoff.

The report derives hourly counts from append-only facts: discovery events, API
request logs, observations, lifecycle events, poll batches and memberships,
batch outcomes, and schedule decisions. It includes discovered tokens, DEX
admissions (`PENDING_DEX → NEW`), pending tokens as-of each hour end, requests,
429s, request latency, batch occupancy, expected and actual polling cadence,
state transitions, resurrections, null rates, rows written, and the largest
durable poll-claim gaps. Database size is a measurement at report generation,
not a historical reconstruction.

Pending-token counts are reconstructed from lifecycle history rather than the
mutable DEX task projection. Actual cadence and gaps are based on durable
poll-claim times, not on assumed request completion. Expected cadence is read
from the immutable scheduler configuration snapshot attached to each claimed
batch.

For an epoch-scoped report, the window begins at the earliest known run
`collection_started_at`, not at epoch lifecycle `started_at`. Validation output lists each run's
clipped live interval, sums only their union, and exposes gaps between restarts. Every run must have
a known live boundary; historical NULL values fail the epoch report rather than silently counting
startup as coverage. The boundary means the worker was permitted to start. PumpPortal discovery and
DEX/API coverage still require their own durable events and attempts, whose first timestamps may be
slightly later.

Duplicate rate is based on an append-only collision ledger: a duplicate is a
delivery rejected by a durable idempotency constraint. The denominator is
accepted discovery/request/observation facts plus recorded duplicate deliveries.
This ledger begins with this migration; reports for periods before it exists
cannot retrospectively infer rejected deliveries.
