# Pump Research — Agent Guide

## Project objective

Pump Research is a long-running, research-only data collection system for Pump.fun-originated Solana tokens. It discovers relevant token addresses, tracks their availability and market behaviour on DEX Screener, and preserves a complete historical dataset for later statistical analysis.

It is not a trading system. Its scope is discovery, collection, persistence, lifecycle tracking, archival, reporting, and operational data-quality monitoring.

## Architectural rules

- Keep provider-neutral domain concepts separate from provider-specific payloads and adapters.
- Keep token discovery, market-data access, batch planning/rate limiting, scheduling, lifecycle classification, persistence, archival, reporting, and health monitoring as separate concerns.
- Treat discovery providers as replaceable adapters. A token's durable identity is its chain plus token address; provider IDs and metadata belong to source-specific discovery records.
- Treat DEX Screener as a market-data/enrichment source after a token address is known. Do not make its identifiers or response shape the canonical token model.
- Model DEX pairs separately from tokens. One token can have zero, one, or many pairs; the canonical pair identity is chain plus pair address, while provider IDs remain provenance. Pair-selection policy is derived, versioned logic—not a raw fact.
- Use explicit interfaces/protocols for external clients so tests can replace them with fakes.
- Prefer a single, maintainable Python application over distributed systems or premature infrastructure.
- Keep append-only source observations and collection-attempt records separate from mutable operational projections and derived lifecycle state.
- Persist the immutable contents and digest of every configuration version whose thresholds affect collection coverage; a digest without recoverable contents is insufficient.
- Critical collection evidence belongs in PostgreSQL. Logs are supplementary and must not be the sole record of attempts, gaps, or failures.

## Research-data invariants

- Preserve every discovered token, including failures, rugs, dumps, and tokens that never become interesting.
- Never permanently delete a token because it appears inactive or dead.
- Do not overwrite historical discovery events, market observations, collection attempts, or lifecycle transitions.
- Preserve distinct time semantics: source-event time when supplied, request start and completion time, collector receipt time, and persistence time. Never manufacture a source timestamp when the provider supplies none.
- Preserve raw provider evidence, or a losslessly reproducible representation of it, separately from normalized and derived values. Record provider, endpoint/schema version, request identity, and collector version.
- Record explicit empty/no-match responses. Absence of an observation must never ambiguously mean either “the provider returned nothing” or “no request completed.”
- Repeated unchanged responses are valid time-series observations and must not be collapsed merely because their values match.
- Avoid survivorship bias, look-ahead bias, and retrospective mutation of what was known at a given point in time.
- Compute lifecycle transitions using only observations available by the decision time. Record the decision time, input watermark/cutoff, previous and new state, reason, and configuration version.
- Lifecycle state must never be written into or used to mutate raw observations.
- Prevent duplicate source events, observations, attempts, and lifecycle transitions with explicit, durable idempotency keys and database constraints—not in-memory assumptions.
- Make historical queries capable of answering both “what did the source say happened?” and “what had this collector successfully learned by time T?”

## Scheduling and API-use rules

- Use one shared API budget per external service across all concurrent workers/processes, with conservative configurable headroom below documented limits.
- Batch and deduplicate due token addresses before requests whenever the API contract permits it; retain per-token membership in each batch for auditability.
- Persist append-only poll obligations/schedule decisions as well as the current scheduling projection: due time, claim/lease state, attempt start/end, outcome, retry classification, and next due time. Leases must expire so work is recoverable after a crash.
- Distinguish retryable failures, terminal request failures, provider-empty responses, partial/malformed responses, and successful observations.
- Respect `Retry-After` and provider feedback, use bounded backoff with jitter, and prevent retries from bypassing the shared rate limiter.
- Adaptive polling may change only future schedules. Preserve every schedule/lifecycle decision and never backfill an observation as if it had been collected earlier.
- Dormant tokens remain scheduled at low frequency and can return to active polling based only on newly collected evidence.

## PostgreSQL and archival rules

- Design high-volume fact tables for 100M+ rows: time partitioning, narrow typed columns for common queries, restrained indexing, and batched/bulk writes.
- Keep high-churn operational scheduling tables separate from append-only observation partitions.
- Avoid indexes on every payload field and avoid loading large result sets as ORM object graphs.
- Choose partition keys and uniqueness/idempotency constraints together; document any PostgreSQL constraint limitation and how global duplicates are prevented.
- Capacity planning must estimate observations/day, bytes/row including indexes, write amplification, partition count, and retention horizon before polling cadence is approved.
- Archival is a verified state transition, not a file export. Parquet archives require schema version, row count, key/time bounds, checksum, creation metadata, and a durable manifest.
- Do not remove PostgreSQL partitions until archived files have been atomically published, independently verified, and recorded in the manifest. Deletion must be an explicit approved retention policy, never an inactivity rule.
- Reports spanning PostgreSQL and archives must detect overlaps and gaps and use explicit as-of cutoffs.

## Reliability and observability requirements

- Recover all authoritative operational state from PostgreSQL after a crash or reboot; in-memory queues and caches are disposable.
- Advance discovery cursors/checkpoints only in coordination with durable persistence of the corresponding discovery events. Reconnect logic must expose unrecoverable source gaps.
- Persist collection-attempt and batch membership records sufficient to explain every expected poll as succeeded, empty, failed, partial, late, or never attempted.
- Record worker heartbeats and scheduler lag, API request counts/statuses/latency, database write failures/latency, due-versus-completed counts, and the oldest overdue work.
- Use UTC-aware timestamps and document clock-skew assumptions. Monotonic clocks may govern elapsed time inside a process but cannot replace persisted UTC event times.
- Never silently drop malformed, unrecognized, oversized, or partially parsed provider data. Quarantine or persist failure evidence with bounded payload handling.
- Test restart, lease expiry, duplicate delivery, partial batch responses, transaction rollback, provider throttling, and archival verification once those components exist.

## Development rules

- Work incrementally, completing and verifying one approved phase at a time.
- Use Python 3.12+, asyncio, PostgreSQL, SQLAlchemy 2.x async, asyncpg, Alembic, httpx, Pydantic v2, pydantic-settings, tenacity where appropriate, pytest, Ruff, mypy, structured logging, and Docker Compose for local PostgreSQL.
- Keep dependencies conservative. Do not add a dependency without a clear current need.
- Write tests alongside behaviour once implementation begins; external service clients must be mockable.
- Inspect the existing worktree before changing files. Preserve unrelated user changes.
- Do not suppress errors. Surface, log, and persist enough context to diagnose discovery, API, database, and scheduler failures.
- Do not implement unapproved future phases merely because their architecture is documented here.

## Never implement

- Wallet management, private-key handling, transaction signing, token purchases, token sales, or automated trading.
- Automated financial decisions, opportunity scoring, predictive trading models, or winner/loser classifications during collection.
- Kubernetes, Redis, Kafka, Celery, microservices, a frontend, or machine learning unless a future explicit requirement supersedes this guide.
- Destructive historical-data cleanup based solely on inactivity.
