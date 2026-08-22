# Collector V2 Phase 1 — coverage scheduler implementation

Status: Phase A audit completed before implementation. This document will be extended with the implemented schema, policy, simulations and quality results. This phase does not change lifecycle thresholds, the configured DEX ceiling, historical observations, or Epoch 1/2 facts, and it does not create Epoch 3.

## Phase A — pre-change scheduler audit

Repository baseline: Alembic `f2c8d4a6197e`, collector stopped, Epoch 2 completed and valid.

### 1. How `poll_schedules.next_due_at` is determined

There are three write paths in `scheduling/scheduler.py`:

1. Initial scheduling or a lifecycle transition calls `set_lifecycle_state_in_session()`. It calculates the current capacity tier from lifecycle state and `state_decided_at`, obtains the current capacity decision, and sets `next_due_at = decided_at + effective_interval`.
2. Batch completion calls `complete_batch_in_session()`. It reads the effective intervals from the capacity decision referenced by the claimed batch, re-evaluates the token's current lifecycle state, and sets `next_due_at = completed_at + effective_interval`. Scheduling from completion rather than the old due time prevents an impossible cadence from accumulating an automatically growing chain of missed obligations.
3. When an epoch transitions from planned to running, `initialize_epoch_in_session()` rebases every schedule once. It hashes `(epoch_id, token_id)` into a phase within the current effective interval and sets `next_due_at = epoch_started_at + phase`. This prevents a simultaneous restart avalanche at a clean epoch boundary.

Reapplying the same lifecycle state under the same scheduler policy returns the existing projection without moving its due time. That prevents repeated evidence from postponing a poll indefinitely. A state change while a batch is in flight is respected at completion because the completion path locks and reads the current schedule.

### 2. Lifecycle-to-target mapping

`AdaptivePollingPolicy` currently owns one interval per lifecycle state plus a special early NEW tier:

| Lifecycle state / tier | Current target |
|---|---:|
| NEW_INITIAL | 15s |
| NEW after two minutes | 30s |
| ACTIVE | 5s |
| RESURRECTED | 5s |
| WATCH | 15s |
| FADING | 120s |
| DORMANT | 900s |

`CapacityTier` is effectively a second spelling of lifecycle state (`ACTIVE`, `RESURRECTED`, `WATCH`, `FADING`, `DORMANT`) with NEW split into `NEW_INITIAL` and `NEW`. `interval_for()`, `priority_for()`, `capacity_tier_for()` and `target_intervals` all accept or return lifecycle-derived values. This is the primary policy coupling V2 must remove.

### 3. How age since admission is represented

There is no explicit DEX-admission timestamp on `poll_schedules`. For NEW only, `state_decided_at` doubles as the admission/age origin because the DEX availability transaction records `PENDING_DEX -> NEW` and creates the schedule with the same `received_at`. The current capacity count treats NEW rows newer than `now - new_initial_duration` as `NEW_INITIAL`.

This works only while the state remains NEW. `state_decided_at` is a lifecycle transition timestamp and is overwritten on every later transition, so it is not a stable token admission clock. `tokens.first_discovered_at` is deliberately nullable source time and is not suitable. The stable existing fact is the first `lifecycle_events.new_state = 'NEW'` decision/input watermark; Phase 1 needs that value materialized in the operational projection without changing its authoritative event.

### 4. How capacity degradation changes intervals

Every refresh bucket (30s by default) counts schedules by lifecycle-derived capacity tier. Requested token observations/minute are `count * 60 / target_interval`. Safe requests are `floor(configured ceiling * (1 - headroom))`; with 240, 20% and batch size 30 this is 192 requests/min or 5,760 token observations/min.

When requested demand fits, target equals effective. Otherwise:

- ACTIVE and RESURRECTED are protected first.
- If protected demand fits, lower tiers share the remainder through deterministic weighted max-min allocation.
- If protected demand alone fills/exceeds capacity, mode is CRITICAL; protected tiers share 95% fairly and populated lower tiers receive 5%.
- Effective intervals are rounded upward so calculated effective demand cannot exceed the safe token budget.

The capacity decision affects only the effective interval written at transition, claim and completion. Target interval remains separately persisted. Headroom and the configured 240/min ceiling are not bypassed.

### 5. Per-state handling before V2

- ACTIVE: strict highest claim priority, 5s target, protected capacity tier.
- RESURRECTED: second strict claim priority, 5s target, protected capacity tier.
- NEW: third claim priority, 15s for two minutes measured from NEW `state_decided_at`, then 30s forever while NEW.
- WATCH: fourth claim priority despite `_PRIORITIES` storing WATCH after NEW; 15s target, lower/non-protected capacity allocation.
- FADING: fifth claim priority, 120s forever while FADING.
- DORMANT: final claim priority, 900s forever while DORMANT.

Lifecycle conditions themselves live in `lifecycle/policy.py` and `lifecycle/classifier.py`. On a transition, the classifier persists immutable lifecycle evidence/event and calls the scheduler in the same transaction. Phase 1 must not change those conditions.

### 6. Restart reconstruction

PostgreSQL is authoritative. Process startup counts tokens, pending tasks, schedules and leases; it does not rebuild schedules from logs. A hard-killed run is marked failed on the next start. Unexpired leases remain owned until expiry; expired leases are claimable and the new batch member records the previous batch ID. The collector process advisory lock prevents two collector runtimes.

Ordinary restart uses the existing `poll_schedules` projection as-is. Only the first start of a planned epoch invokes the epoch initializer and rephases all schedules. Capacity policy documents and decisions are durable by digest, so a batch completion can replay its claimed effective interval after restart.

### 7. How claims are persisted

`claim_next_batch()` takes a PostgreSQL transaction advisory lock for budget/claim serialization, checks the rolling one-minute reserved request count and maximum in-flight batches, then selects eligible schedules (`next_due_at <= now` and no live lease). It chooses one chain, locks up to 30 rows with `FOR UPDATE SKIP LOCKED`, inserts one immutable `poll_batches` row and one immutable `poll_batch_members` row per token, and updates each mutable schedule lease. The batch/member reference the capacity decision and target/effective interval.

Completion locks the batch and schedules, rejects an expired/reclaimed lease, inserts the immutable outcome, and advances projections atomically. A duplicate completion returns the existing outcome.

### 8. How overdue schedules are selected

Selection is deterministic: SQL lifecycle priority, then oldest `next_due_at`, then token UUID. A head query selects a chain with the globally highest-ranked eligible row; a second query fills that chain's batch using the same order. This maintains API-valid single-chain batches but can leave capacity unused when chains are fragmented.

### 9. Current anti-starvation behavior

Within one lifecycle tier, oldest due time and token UUID provide deterministic FIFO fairness. Across tiers, the capacity planner assigns every populated lower tier a positive effective rate, including a 5% reserve in CRITICAL mode. However claim selection is strict lifecycle priority, not a persisted per-tier service reservation. Anti-starvation therefore relies on effective intervals making higher tiers cease being due often enough. Epoch 2 demonstrated that continual higher-tier arrivals can still leave lower-tier schedules many hours overdue. There is no bounded retired population or durable rotation cursor.

### 10. Capacity decision persistence

The refresh bucket, scheduler policy digest and complete capacity-plan snapshot form a SHA-256 idempotency key; UUIDv5 of that key is the primary key. Scheduler policy JSON is stored once by digest. Capacity insert uses untargeted `ON CONFLICT DO NOTHING`, then reads by ID or idempotency key and verifies exact semantic equality for ID, key, bucket, mode, policy and snapshot. Any same identity/different content raises `SchedulerCapacityDecisionIntegrityError`. Each scheduler instance caches only the current bucket/decision; persistence makes concurrent workers and restart idempotent.

## Complete lifecycle/cadence coupling inventory

| Location | Coupling before Phase 1 |
|---|---|
| `scheduling/policy.py` | `CapacityTier` mirrors lifecycle; priorities map lifecycle directly; NEW age uses lifecycle `state_decided_at`; target intervals are lifecycle settings. |
| `scheduling/capacity.py` | protected and weighted allocation sets are lifecycle-named tiers. |
| `scheduling/scheduler.py` | schedule creation, epoch rebase, claims, completion, population counts and SQL priority all derive cadence from lifecycle. |
| `persistence/models.py` | `poll_schedules` has lifecycle and state time but no admission time/coverage class; member/decision evidence has lifecycle but no coverage. |
| `collection/dex_availability.py` | NEW admission calls the lifecycle scheduler; no separate coverage initialization. |
| `lifecycle/classifier.py` | lifecycle transition immediately reschedules cadence through the same method. |
| `monitoring/status.py` | capacity counts, overdue groups and interval display are lifecycle-only; NEW_INITIAL is inferred from `state_decided_at`. |
| `scheduling/simulation.py` | simulated token identity and queues are capacity/lifecycle tiers; arrivals remain NEW forever. |
| `reporting/twenty_four_hour.py` | schedule decisions and lateness are reported by lifecycle only. |
| scheduler integration/unit tests | fixtures seed lifecycle state to obtain cadence and assert the lifecycle-named capacity tiers. |
| configuration | one interval setting per lifecycle plus a two-minute NEW special case; no age bands, FADING tail or fixed scan budget. |

## Audit conclusion and implementation constraints

The authoritative admission fact already exists, so Phase 1 can add a stable `admitted_at` projection with an unambiguous migration from each token's earliest NEW lifecycle event. A row without that event is ambiguous and migration/runtime initialization must fail rather than infer from `tokens.first_discovered_at` or persistence time.

Coverage must become a durable projection with immutable transition evidence and a content-addressed policy. Capacity tiers should be coverage classes, not lifecycle aliases. Lifecycle changes may override coverage (ACTIVE/WATCH/RESURRECTED/FADING) but cannot redefine admission age or erase earlier coverage decisions. Retired population selection needs its own fixed token-observation budget and durable per-token last-control-scan/ordinal state so restart and concurrency cannot create an avalanche.

## Implemented coverage model

Phase 1 adds coverage as an independent operational and historical concept. `poll_schedules.lifecycle_state` still describes observed market behaviour; `coverage_class` describes collection intensity. Lifecycle thresholds and transition rules are unchanged.

| Coverage class | Derivation | Target cadence |
|---|---|---:|
| `PROTECTED_ACTIVE` | lifecycle ACTIVE | 5s |
| `PROTECTED_RESURRECTED` | lifecycle RESURRECTED | 5s |
| `PROTECTED_WATCH` | lifecycle WATCH | 15s |
| `INITIAL` | ordinary admission age 0–2m | 15s |
| `EARLY` | age 2–10m | 30s |
| `MATURE` | age 10–60m | 300s |
| `COOLED` | age 1–6h | 1,800s |
| `LONG_TAIL_DAY` | age 6–24h | 7,200s |
| `LONG_TAIL_WEEK` | age 1–7d | 43,200s |
| `FADING_TAIL` | first 30m after entering FADING | 120s |
| `FADING_COOL` | 30m–6h after entering FADING | 1,800s |
| `RETIRED_CONTROL` | ordinary age >=7d, FADING age >=6h, or DORMANT | fixed-budget rotation |

The ordinary path contains exactly `8 + 16 + 10 + 10 + 9 + 12 = 65` nominal observations. It is finite. ACTIVE, RESURRECTED, and WATCH override age while those lifecycle states apply. A lifecycle resurrection promotes a retired token immediately to `PROTECTED_RESURRECTED`; all earlier coverage evidence remains immutable.

Admission age is the materialized earliest DEX admission fact. New admissions use the timestamp of the durable `PENDING_DEX -> NEW` transaction. Legacy schedules are mapped only from the earliest immutable `lifecycle_events.new_state = 'NEW'` input watermark. `tokens.first_discovered_at`, row persistence time, and current wall time are never substituted.

## Durable schema and audit behaviour

Migration `7c31a8e4d5f2` adds:

- immutable `coverage_policies`, keyed by SHA-256 of the complete coverage policy;
- immutable `coverage_decisions`, including lifecycle state, admission time, previous/new class, decision/effective times, target/effective cadence, next due time, policy, capacity decision, epoch/run provenance, and reason;
- the current coverage projection, next deterministic boundary, policy digest, and control-scan state on `poll_schedules`;
- coverage class on immutable batch membership;
- ordinary/control batch kind and a unique UTC control window on `poll_batches`.

Historical Epoch 1/2 schedules are intentionally left `LEGACY_UNMAPPED` by the migration. There is no bulk guess and no observation/lifecycle rewrite. On the first start of a future planned epoch, the existing epoch-start transaction locks schedules, reconstructs admission from lifecycle evidence, derives coverage under the immutable policy, clears stale operational leases, and deterministically phases ordinary work. Any schedule without unambiguous admission evidence aborts the transaction with `CoverageReconstructionError`. No Epoch 3 is created by this implementation.

Once mapped, a restart loads the durable projection directly. Expired ordinary leases retain the existing reclaim semantics. Retired rows have `next_due_at = NULL`, so restart cannot make the retired population simultaneously due. Coverage boundaries never postpone an obligation that was already due when the boundary was processed.

## FADING collapse tail

FADING classification is unchanged. Only its coverage is finite:

1. 120-second observations for the first 30 minutes after the recorded FADING transition (about 15 nominal observations);
2. 30-minute observations from 30 minutes through six hours (about 11 nominal observations);
3. entry into `RETIRED_CONTROL` after six hours.

This preserves a conservative collapse tail without charging 120-second demand forever. DORMANT enters control rotation immediately. A control observation can still feed the existing lifecycle classifier; existing DORMANT resurrection evidence can therefore restore protected 5-second coverage.

## Fixed-budget retirement/control algorithm

The default hard ceiling is two retired token observations per UTC minute. It is independent of whether the retired population is 10,000 or 10 million.

For each UTC minute:

1. PostgreSQL permits at most one `retired_control` batch through a unique `control_window_start` constraint.
2. The scheduler transaction advisory lock serializes budget and window arbitration across all four workers.
3. It selects null/oldest `last_control_scan_at`, then stable admission time, then token UUID. There is no random seed or in-memory cursor.
4. The batch contains at most the configured token budget (and never more than 30).
5. Completion durably updates `last_control_scan_at` and `control_scan_count`; a crash leaves an expiring lease and the normal reclaim path.

This is deterministic round-robin-by-oldest-service. Every retained token is eventually selected while the set is finite, but rotation time grows with retired population: at two/minute, one million retired tokens take about 347 days for a complete sweep. That sparse sensitivity is the explicit research tradeoff required to bound aggregate cost. Control batches preserve a stable negative/control sample; no historical token or fact is deleted.

## Capacity precedence and reserve

The configured DEX ceiling remains 240 requests/minute. Twenty percent headroom yields 192 safe requests/minute. Phase 1 reserves another configurable 14 safe requests/minute for availability reconciliation, retries, and future integrations, leaving 178 requests/minute (5,340 packed token observations/minute) for scheduled market/control work.

Allocation order and weights are:

1. ACTIVE and RESURRECTED are fully protected whenever their combined demand fits;
2. WATCH (`64`), INITIAL (`32`), EARLY (`16`), MATURE (`8`);
3. FADING_TAIL (`4`), FADING_COOL and COOLED (`2`);
4. LONG_TAIL_DAY (`1`), LONG_TAIL_WEEK (`0.5`);
5. RETIRED_CONTROL (`0.25`), with its tiny fixed no-starvation reservation.

If ACTIVE/RESURRECTED alone exceed scheduled capacity, mode is CRITICAL: they share 95% fairly and lower classes share 5%. WATCH can degrade only after ACTIVE/RESURRECTED have been protected. Target and effective intervals remain separate in every capacity decision. The scheduler still enforces the rolling database-backed request budget; neither control work nor retries can raise the configured ceiling.

## Deterministic demand simulation

Assumptions: 28 admitted tokens/minute (Epoch 2 rate), 120 protected ACTIVE/RESURRECTED, 100 WATCH, a conservative FADING transition cohort equal to 40% of admission flow, two control observations/minute, 30 addresses/request, and the 14-request reserve. Every simulated token occupies exactly one coverage class.

| Cumulative tokens | Requested/effective obs/min | Core requests/min | Core + reserve requests/min | Capacity use of 5,340 | Result |
|---:|---:|---:|---:|---:|---|
| 10,000 | 3,218.8 | 107.3 | 121.3 | 60.3% | NORMAL, bounded |
| 50,000 | 3,622.7 | 120.8 | 134.8 | 67.8% | NORMAL, bounded |
| 100,000 | 3,692.2 | 123.1 | 137.1 | 69.1% | NORMAL, bounded |
| 500,000 | 3,953.2 | 132.7 | 146.7 | 74.0% | NORMAL, bounded |
| 1,000,000 | 3,953.2 | 132.7 | 146.7 | 74.0% | NORMAL, bounded |

The control batch is modeled as one actual request/minute once retired tokens exist, rather than incorrectly assuming its two addresses always share an ordinary 30-address batch. The 500k and 1m rows therefore have identical observation demand, request rate, and effective intervals even though retired population grows by 500k. This is the acceptance proof: ordinary population saturates after seven days and retirement demand is a constant, so cumulative history cannot create an unbounded due queue.

The maximum modeled steady-state rate is about 132.7 core requests/minute, or 146.7 including all 14 reserved requests. That leaves about 45.3 requests/minute below the 192 safe ceiling and 93.3 below the configured ceiling.

## Concurrency and restart guarantees

- Ordinary claims retain `FOR UPDATE SKIP LOCKED`, transaction advisory serialization, expiring leases, single-chain batches, and <=30 members.
- The control-window unique constraint plus the same advisory lock prevents two workers claiming one retirement window.
- Coverage transitions lock the token and schedule, preserve immutable evidence, and fail on conflicting equal-time lifecycle state.
- Capacity decision persistence retains untargeted conflict arbitration plus semantic readback, so the Epoch 1 primary-key race remains covered.
- Semantically different population snapshots in one wall-clock capacity bucket receive different deterministic IDs; identical snapshots converge on one durable row.
- Epoch initialization uses `(epoch_id, token_id)` deterministic phasing and does not reset lifecycle state, attempt count, observations, or history.

Tests cover four concurrent workers, NORMAL and DEGRADED operation, same-token double claims, fixed-window control concurrency, fair control rotation, restart without a retired avalanche, lifecycle resurrection, finite FADING, ambiguous legacy admission failure, capacity persistence replay, and the million-token plateau.

## Status fields

`collector status` retains all lifecycle metrics and adds:

- `tokens_by_lifecycle_state` and `tokens_by_coverage_class`/`coverage_counts`;
- retired and legacy-unmapped population;
- target/effective interval by coverage class;
- requested/effective coverage observations per minute and utilization;
- control budget and recent control scans;
- overdue count and p50/p95/max lateness by coverage class;
- safe, reserved, and scheduled request budgets.

An unmapped legacy population is shown explicitly and the running scheduler refuses to plan it. Status therefore cannot make old lifecycle-only rows look like an achievable V2 schedule.

## Limitations and future integration points

- Rotation detects resurrection only as quickly as its population-dependent sweep. Candidate/security/boost signals may later promote coverage, but none are implemented here.
- DEX batches remain single-chain. The present research cohort is Solana; a future multi-chain cohort needs per-chain control fairness.
- Capacity planning assumes efficient ordinary batching; status exposes actual occupancy and requests/minute. The simulation separately charges one request for sparse control work.
- A very large legacy reconstruction is intentionally a one-time, auditable epoch-start transaction. It should be timed on an isolated restored backup before a future live epoch.
- No automatic archive deletion, lifecycle threshold change, API-ceiling increase, trading, boost, holder, wallet, execution, or market-regime functionality is part of Phase 1.

## Quality gate record

Completed on 2026-08-17:

- full pytest: **128 passed** in 115.89s against only `pump_research_capacity_test`;
- post-status-change focused integration: **1 passed**;
- Ruff: passed repository-wide;
- mypy: passed across 67 source/test files;
- `git diff --check`: passed;
- Alembic metadata/schema parity: no new operations detected;
- isolated `f2c8d4a6197e -> 7c31a8e4d5f2 -> f2c8d4a6197e -> 7c31a8e4d5f2` migration cycle: passed;
- four-worker concurrency and both NORMAL/DEGRADED sustained tests: passed;
- deterministic 1m-token simulation: NORMAL at 3,953.2 observations/min and about 146.7 requests/min including reserve;
- read-only live check: live schema remains `f2c8d4a6197e`, Epoch 2 remains completed/valid, exactly 9,202,662 observations remain, latest run is stopped, no advisory locks are held, and no Epoch 3 exists.

A Phase 2 GO does not authorize applying the migration to live PostgreSQL, creating an epoch, or starting collection.
