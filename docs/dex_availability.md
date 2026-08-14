# Initial DEX availability admission

This narrow workflow admits a discovered token to the research dataset before
it has a DEX Screener pair. It is not the general polling scheduler and makes
no financial or quality judgement.

```text
discovery event → PENDING_DEX → one ≤30-address DEX batch
                                     ├─ no matching pair → retain + schedule retry
                                     └─ matching pair    → NEW
```

`admit_discovery()` persists the provider-neutral discovery event, canonical
token, initial lifecycle event, and `dex_availability_tasks` projection in one
database transaction. A duplicate discovery delivery does not create another
token or pending task.

`check_due()` leases at most 30 due pending tokens, groups them by chain, and
calls the injected DEX client once per chain group. Each successful response,
including an empty response, is recorded in `api_request_log` before task state
is updated. A no-match creates an append-only `PENDING_DEX → PENDING_DEX`
decision with the next check time; a matching pair creates `PENDING_DEX → NEW`.
The raw response remains separate from this derived state.

The retry interval and lease duration are configured through
`PUMP_RESEARCH_DEX_AVAILABILITY_RETRY_SECONDS` and
`PUMP_RESEARCH_DEX_AVAILABILITY_LEASE_SECONDS`. Their values and schema version
are stored with every lifecycle decision. A worker crash can leave a lease, but
another process can reclaim it after expiry from PostgreSQL; no in-memory queue
is authoritative.

An API failure is recorded as a failed/throttled `api_request_log` row, leaves
the token `PENDING_DEX`, and schedules a retry. The workflow intentionally does
not create market observations or select a primary pair; those remain later
collection responsibilities.
