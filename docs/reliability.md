# Reliability and restart verification

The reliability suite uses mock HTTP transports, fake discovery sources, a
test-owned PostgreSQL backend connection, and real local subprocess signals. It
does not call Pump.fun or DEX Screener.

| Failure | Verified behavior |
| --- | --- |
| HTTP timeout | Retries the configured attempts through the limiter, records failure metrics, then surfaces the timeout. |
| HTTP 429 | Uses the retry path and records throttling and retry evidence. |
| HTTP 500 | Retries through the same limiter and records the failed and successful attempts. |
| Bad JSON | Raises an explicit parse error and does not retry a malformed successful response. |
| Discovery disconnect | Leaves the durable opaque checkpoint unchanged and resumes from it after reconstruction. |
| PostgreSQL interruption | Terminates one test-owned backend connection, observes a loud exception, then verifies a new pre-ping connection succeeds. |
| Duplicate discovery | Preserves one token, source event, initial lifecycle event, and pending task. |
| Duplicate API response | Collapses an exact duplicate while retaining unchanged facts from a genuinely later request. |
| SIGKILL/process restart | Physically kills a real `python -m pump_research collector run` process and leaves its run unfinished. |
| Restart reconstruction | Replacement recovers the abandoned run and reconstructs tokens, DEX work, schedules, leases, and discovery checkpoint state from PostgreSQL. |
| Slow or failed startup | Invocation `started_at` precedes reconstruction. A separate `collection_started_at` commits only after successful startup, before any worker task is created; failed initialization leaves no run/live boundary. |
| SIGINT/SIGTERM | The worker stops accepting new claims, allows bounded in-flight work to finish or cancels after the configured grace period, marks components stopped, appends immutable terminal evidence, and finalizes its run as `stopped`. |
| Closed `tee` output pipe during Ctrl+C | Structured logging absorbs `EPIPE`; a closed log consumer cannot turn an intentional stop into a component or collector failure. |
| Stale-running reconciliation | The repair command must both observe a stale heartbeat and acquire the collector's singleton advisory lock. It preserves the last heartbeat, snapshots prior component evidence into an immutable event, and never closes the epoch implicitly. |
| Unexpected pipeline error | The failing fixed worker loop records durable `failed` component health and terminates the collector run rather than leaving a partially dead pipeline appearing healthy. |
| Persisted retryable failure | Reconciliation/polling failures mark their component `degraded` even though the workflow safely persisted the failure and returned; the last successful timestamp is retained. |
| Lifecycle failure during polling | Immutable API evidence and observations commit, the batch is marked `partial` with durable classifier failure detail, derived writes roll back to a savepoint, and the unexpected error terminates the supervised worker loudly. |
| Partial DEX batch | Mixed present/absent members are recorded as `partial`, with every unmatched address in durable failure detail; observation locators retain the raw response index. |
| Multiple token pairs | Every pair observation is retained; lifecycle evidence is selected by versioned highest-liquidity policy with a canonical address tie-break, independent of response order. |
| Incomplete pair evidence | Missing liquidity in a multi-pair candidate records an immutable failed evidence evaluation, leaves lifecycle state unchanged, and marks the poll `partial`. |
| Accidental second collector | A PostgreSQL session advisory lock rejects the second process before it can create a run or multiply the process-local API ceiling. The lock disappears automatically if the owning database session/process dies. |
| End-to-end synthetic flow | Fake discovery, absent/present DEX responses, durable admission, scheduler claim, immutable observation, lifecycle transition, and next due time are verified without third-party requests. |

The real collector command owns process lifecycle and coordinates four fixed
tasks—discovery, initial DEX reconciliation, scheduled observation collection,
and heartbeat. It has no per-token asyncio task or authoritative in-memory
queue; durable leases make due work reclaimable after restart.

Only one collector process may run against a database at a time. This is a
deliberate reliability boundary: discovery reconciliation and scheduled polling
share one in-process DEX Screener limiter, while PostgreSQL enforces singleton
ownership for the process lifetime.

`collector status` separates `HEALTHY_RUNNING`, `GRACEFULLY_STOPPED`,
`STALE_OR_CRASHED`, and `FAILED`. An epoch may intentionally remain `running`
after a graceful collector stop until the operator explicitly closes it.

Status exposes both the invocation and live-work boundaries. Historical runs with a NULL live
boundary remain explicitly unknown. Continuity calculations use distinct run intervals and never
count startup/reconstruction time or restart gaps as live-worker uptime.
