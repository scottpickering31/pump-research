# Capacity-aware scheduler

## Outcome and scope

The collector now converts impossible requested cadences into explicit,
deterministic effective cadences before work is claimed. It retains every token,
does not change DEX admission or lifecycle thresholds, does not delete research
facts, does not add trading or prediction, and keeps the configured DEX Screener
ceiling at 240 requests/minute.

The scheduler uses 20% configurable headroom by default. At a 240 request ceiling
and 30 addresses/request, scheduled observation capacity is therefore:

```text
safe_requests_per_minute = floor(240 * (1 - 0.20)) = 192
available_token_observations_per_minute = 192 * 30 = 5,760
```

The remaining 48 requests/minute are headroom for initial DEX admission, retries,
latency variation, and provider feedback. The process-wide HTTP limiter still
enforces the full configured 240 request ceiling on every actual attempt.

## Audit findings

### Admission is correct and unchanged

PumpPortal `subscribeNewToken` creation events create a canonical Solana token,
immutable discovery evidence, and a `PENDING_DEX` availability task. They do not
create a recurring poll schedule and do not enter `NEW`.

An availability lease batches at most 30 addresses into DEX Screener. A token is
promoted `PENDING_DEX -> NEW` only when a successful response contains a Solana
pair whose base or quote token address equals the discovered mint. Promotion and
the first recurring schedule commit in the same transaction. Empty, failed, and
unmatched responses retain the token in `PENDING_DEX` for retry.

Admission expresses this cohort: a PumpPortal-discovered, Pump.fun-originated
Solana mint that has become DEX Screener-queryable. It does not require
`dexId == pumpswap`; Pump.fun origin is source provenance, while venue-specific
pair identities remain provider evidence. That is the pre-existing documented
meaning, and no admission change was justified by the audit.

### Why requests stayed near 100/minute while lateness grew

The prior scheduler had no population-demand calculation. Every completion simply
set `next_due_at = completed_at + target_interval`. When total requested demand was
impossible, one durable obligation per token remained overdue and lateness grew.

Two implementation constraints also held actual request use below the configured
ceiling:

1. Every normal scheduled batch reserved all three possible retry attempts, so a
   240 request budget admitted only 80 batches/minute even when responses succeeded
   on the first attempt. At full batches that is 2,400, not 7,200, addresses/minute.
2. The worker executed one scheduled batch at a time despite a configured
   four-batch in-flight bound. Network latency, database writes, lifecycle
   evaluation, and a 250 ms loop delay were serialized.

The new scheduler reserves one normal request inside a 192 request safe budget.
Retries continue through the shared HTTP limiter and consume headroom. Four fixed
batch workers overlap I/O and persistence; no task or queue is created per token.

### Lifecycle rules are unchanged

The evidence-only transitions remain:

- `NEW -> ACTIVE`: 5-minute volume is at least $100.
- `NEW -> WATCH`: volume is below the ACTIVE threshold and liquidity is at least
  $1,000.
- `ACTIVE -> FADING`: 5-minute volume is at most $25.
- `WATCH -> FADING`: 5-minute volume is at most $10.
- `FADING -> DORMANT`: hourly volume is at most $10 and liquidity is at most $100.
- `DORMANT -> RESURRECTED`: 5-minute volume is at least $100 and liquidity is at
  least $500.

Missing evidence satisfies no rule. There is no fixed or minimum NEW residence
time, and low-volume/low-liquidity tokens can remain NEW indefinitely. The large
NEW population was not caused by a ten-minute hold; it was caused by the absence
of an outgoing rule for that evidence shape. This change deliberately makes such
tokens affordable to retain instead of inventing a classification threshold.
`RESURRECTED` still has no outgoing lifecycle rule.

## Target cadence policy

| Lifecycle tier | Target interval | Scheduling priority |
|---|---:|---:|
| ACTIVE | 5 seconds | 1 |
| RESURRECTED | 5 seconds | 2 |
| NEW, first 120 seconds after DEX admission | 15 seconds | 3 |
| NEW, after 120 seconds | 30 seconds | 3 |
| WATCH | 15 seconds | 4 |
| FADING | 120 seconds | 5 |
| DORMANT | 900 seconds | 6 |

WATCH is retained because it is part of the existing lifecycle classifier. Its
15-second target is unchanged and it is placed between NEW and FADING.

The NEW age window is based on the durable `state_decided_at` of DEX admission.
Restart cannot reset it. At completion, a NEW token older than 120 seconds uses
the 30-second target without a lifecycle mutation.

## Capacity algorithm

For tier `i`, requested token observations/minute are:

```text
demand_i = token_count_i * 60 / target_interval_seconds_i
```

The planner is recalculated in deterministic 30-second UTC buckets. If total
demand fits the safe capacity, target and effective intervals are identical and
mode is `NORMAL`.

If it does not fit:

1. ACTIVE and RESURRECTED receive their full 5-second targets whenever their
   combined demand fits.
2. Remaining capacity is assigned to populated lower tiers by weighted max-min
   allocation. Per-token effective rate is
   `min(target_rate_i, lambda * weight_i)`, where `lambda` is the unique level
   whose aggregate demand fits the remaining capacity.
3. Weights are NEW-initial 16, mature NEW 8, WATCH 4, FADING 1, and DORMANT 0.25.
   Thus early NEW retains a twofold advantage over mature NEW, while FADING and
   DORMANT absorb most shedding.
4. If ACTIVE/RESURRECTED alone exceed capacity, mode is `CRITICAL`. They share 95%
   fairly at equal per-token rates; 5% is shared among populated lower tiers to
   prevent starvation. A high-severity structured event reports the effective
   protected cadence.
5. Effective intervals are rounded upward to whole seconds. Consequently
   calculated effective demand is never above safe capacity; rounding can leave a
   small unused fraction but cannot oversubscribe it.

This allocation has no random choice. Inside a lifecycle priority, claims order by
`next_due_at` and token UUID. Completion creates exactly one next obligation at
the effective interval. Because aggregate renewal demand is at or below service
capacity, the scheduler does not generate an ever-growing count of missed target
slots. A policy change may leave a bounded one-cycle transient while existing due
work completes under the newly recorded cadence.

## Live-population mathematics

Using the exact supplied counts and treating all 4,711 NEW tokens as older than
two minutes:

| Tier | Count | Old demand/min | New target demand/min | Effective interval | Effective demand/min |
|---|---:|---:|---:|---:|---:|
| NEW | 4,711 | 56,532 | 9,422 | 76 s | 3,719.2 |
| ACTIVE | 111 | 1,332 | 1,332 | 5 s | 1,332.0 |
| FADING | 6,710 | 6,710 | 3,355 | 602 s | 668.8 |
| DORMANT | 73 | 4.9 | 4.9 | 2,408 s | 1.8 |
| **Total** | **11,605** | **64,579** | **14,113.9** | — | **5,721.8** |

The baseline cadence reduces target demand by 78%, but still requests 470.46
full-batch requests/minute and does not fit. Adaptation produces 5,721.8 token
observations/minute, or 190.73 full-batch requests/minute. This is below the 192
safe scheduled budget and 240 configured ceiling. ACTIVE remains at 5 seconds;
mature NEW is observed about 7.9 times as often as FADING and 31.7 times as often
as DORMANT.

The integer rounding margin is 38.2 token observations/minute. Therefore the
steady-state renewal equation has negative, not positive, drift:

```text
effective demand 5,721.8 < safe service capacity 5,760
```

Target lateness cannot grow indefinitely solely because the old requested policy
was impossible: the impossible target is now represented as an explicit effective
cadence before the next obligation is created.

## Synthetic performance simulation

`tests/unit/test_scheduler_capacity.py` runs the production planner and a
network-free deterministic batch simulation. Request slots are evenly constrained
to the safe rolling rate, batch size is at most 30, lifecycle priority is strict,
and each completion creates one next due time using the current effective plan.

### Exact supplied population, no arrivals, four hours

| Measurement | Result |
|---|---:|
| Requests/minute | 192.0 |
| Average addresses/request | 29.963 |
| Token observations | 1,380,689 |
| All initial tokens observed | yes |
| ACTIVE p95 lateness | 0 s |
| mature NEW p95 lateness | 0 s |
| FADING p95 lateness | 0.375 s |
| DORMANT p95 lateness | 4.453 s |
| Overdue at end | 1 FADING schedule |

### Arrival stress, four hours

The prompt did not supply a measured admission rate. The second simulation
therefore labels 30 NEW admissions/minute as a conservative stress assumption,
not a live measurement. Arrivals conservatively remain NEW; no favorable lifecycle
promotion is assumed.

| Measurement | Result |
|---|---:|
| New arrivals | 7,200 |
| Final population | 18,785 |
| Requests/minute | 192.0 |
| Average addresses/request | 29.926 |
| Token observations | 1,379,008 |
| All initial tokens observed | yes |
| ACTIVE p95 lateness | 0 s |
| NEW-initial / mature NEW p95 lateness | 0.563 s / 0.563 s |
| FADING p95 lateness | 51.0 s |
| DORMANT p95 lateness | 246.625 s |
| End overdue count | 259 (1.38% of population) |

Final effective intervals are ACTIVE 5 s, NEW-initial 87 s, mature NEW 173 s,
FADING 1,384 s, and DORMANT 5,536 s. Final calculated demand is 5,759.0 token
observations/minute, still below 5,760. Every populated tier receives observations,
and ACTIVE retains its intended high-resolution advantage. Lateness remains bounded
relative to the explicit effective intervals rather than increasing without limit
against impossible targets.

## Fairness and batching guarantees

- No token is removed, randomly sampled, or silently dropped.
- Every populated tier receives a positive finite effective rate, including in
  CRITICAL mode.
- Deterministic due-time/UUID order rotates completion-driven obligations within a
  tier; repeatedly polling one fixed subset cannot satisfy the projection while
  leaving another subset unscheduled.
- Claims contain one chain and at most 30 unique addresses.
- A transaction advisory lock, row locks, leases, and the rolling safe-request
  reservation prevent duplicate claims and multi-worker budget overshoot.
- Expired leases remain reclaimable after restart. Historical membership records
  point to abandoned predecessor batches.
- Four fixed batch tasks are the maximum default concurrency; there is never one
  asyncio task per token.

## Historical reproducibility

Migration `84d1f0c2a6be` is additive:

- `scheduler_policies` stores one immutable policy document per SHA-256 digest.
- `scheduler_capacity_decisions` stores immutable time bucket, population counts,
  target/effective intervals, demand, capacity, degradation totals, and mode while
  referencing the normalized policy digest.
- Future poll schedules, initial/lifecycle schedule decisions, batches, and batch
  members reference the capacity decision. Members also retain target and effective
  interval seconds directly for efficient audits.
- Existing rows remain compatible because new references are nullable. They are
  not rewritten or deleted.

The large static policy JSON is stored once in the normalized policy table for
dynamic decisions. A capacity document is written at most once per 30-second
population bucket, not once per token poll. Raw observations and API response
evidence are unchanged.

## Status output

`collector status` now includes:

- requested, available, and effective token observations/minute;
- requested, available, effective, configured-ceiling, and current requests/minute;
- requested/effective capacity utilization;
- `NORMAL`, `DEGRADED`, or `CRITICAL` mode;
- target and effective intervals for every lifecycle tier, including both NEW age
  windows;
- population and degraded count/percentage;
- current overdue count and maximum overdue seconds by lifecycle state;
- recent-hour p50/p95 claim lateness by lifecycle state;
- the existing recent batch occupancy, HTTP errors, and whole-scheduler lateness.

The output distinguishes the 240 hard ceiling from the 192 safe scheduled budget.
An operator can no longer mistake a target interval for an achieved/effective one.

## Storage impact

At the exact current-population effective rate, projected daily rows are:

| Relation/evidence | Rows/day |
|---|---:|
| Poll batch members | about 8.239 million |
| Normalized pair observations | about 8.197 million |
| Lifecycle evidence evaluations | about 8.214 million |
| API request logs | about 274,646 |
| Capacity decisions at 30-second refresh | at most 2,880 |

Observation and lifecycle ratios use the measured burn-in ratios from
`docs/storage-audit.md`; pair multiplicity or empty responses can change them. API
requests assume the measured near-full 30-address batches.

Using measured total bytes/row (424 observation, 7,713 API request, and the
post-normalization lifecycle estimate of 1,158) plus an estimated 308 bytes for a
poll member with the new cadence fields gives this four-table projection:

| Relation | Approximate GiB/day |
|---|---:|
| Observations | 3.24 |
| Lifecycle evidence | 8.86 |
| Poll members | 2.36 |
| API requests | 1.97 |
| **Subtotal** | **16.43 GiB/day** |

Allow roughly 17–18 GiB/day for these facts plus poll batches/outcomes, tokens,
pairs, scheduler decisions, and normal relation variation, before WAL, backups,
and free-space headroom. This is about twice the prior measured write rate because
the corrected worker can use roughly 191 rather than 98 requests/minute. It is far
below the roughly 93 million poll-member rows/day implied by the impossible old
targets, but capacity planning must use the new achievable write rate before a live
restart. No compaction or retention deletion is part of this change.

## Controlled restart instructions

### Data-loss gate

During this implementation, the repository's pre-existing integration fixture was
run against its default `pump_research` database URL. That fixture executes
`TRUNCATE ... CASCADE`; the local burn-in database was cleared. A status check
confirmed zero tokens, observations, schedules, and collector runs. PostgreSQL has
`archive_mode=off`, no WAL archive command, and no workspace or `/tmp` dump was
found. Epoch 0 has since been explicitly accepted as permanently lost, with
research validity NONE. Recovery and partial-data splicing are forbidden.

The fixture now requires an explicit test URL and test environment and checks the
actual connected PostgreSQL database name immediately before destructive SQL.
All subsequent tests use the isolated `pump_research_capacity_test` database.
The next run is the explicitly declared Epoch 1; see
[epoch1-readiness.md](epoch1-readiness.md) for the authoritative start gate.

### Restart after that decision

1. Confirm no collector process is running and capture an external database backup.
2. Set and review these values:

   ```dotenv
   PUMP_RESEARCH_DEX_SCREENER_REQUESTS_PER_MINUTE=240
   PUMP_RESEARCH_SCHEDULER_CAPACITY_HEADROOM_RATIO=0.20
   PUMP_RESEARCH_SCHEDULER_CAPACITY_REFRESH_SECONDS=30
   PUMP_RESEARCH_SCHEDULER_NEW_INITIAL_INTERVAL_SECONDS=15
   PUMP_RESEARCH_SCHEDULER_NEW_INITIAL_DURATION_SECONDS=120
   PUMP_RESEARCH_SCHEDULER_NEW_INTERVAL_SECONDS=30
   PUMP_RESEARCH_SCHEDULER_ACTIVE_INTERVAL_SECONDS=5
   PUMP_RESEARCH_SCHEDULER_WATCH_INTERVAL_SECONDS=15
   PUMP_RESEARCH_SCHEDULER_FADING_INTERVAL_SECONDS=120
   PUMP_RESEARCH_SCHEDULER_DORMANT_INTERVAL_SECONDS=900
   PUMP_RESEARCH_SCHEDULER_RESURRECTED_INTERVAL_SECONDS=5
   PUMP_RESEARCH_SCHEDULER_BATCH_SIZE=30
   PUMP_RESEARCH_SCHEDULER_MAX_IN_FLIGHT_BATCHES=4
   ```

3. Apply and verify schema without starting collection:

   ```bash
   .venv/bin/python -m alembic upgrade head
   .venv/bin/python -m alembic check
   .venv/bin/python -m pump_research collector status
   ```

4. Confirm status reports a safe 192 request budget, coherent populations, and an
   effective demand no greater than 5,760 token observations/minute. On a restored
   database, verify lifecycle counts match the pre-restart backup; no state should
   mass-reset to NEW.
5. Start one supervised collector process:

   ```bash
   .venv/bin/python -m pump_research collector run --epoch 1
   ```

6. After 2, 5, and 15 minutes, capture `collector status`. Require actual requests
   at or below 240/minute, effective demand at or below safe capacity, batch
   occupancy near the modeled value, ACTIVE/RESURRECTED effective cadence visible,
   no repeated CRITICAL warning, and bounded per-state p95/max lateness. Stop the
   collector if occupancy collapses, headroom is exhausted by admission/retries, or
   effective demand exceeds capacity.

Integration tests must always use an isolated URL:

```bash
PUMP_RESEARCH_TEST_DATABASE_URL='postgresql+asyncpg://pump_research:pump_research@localhost:5433/pump_research_capacity_test' \
  PUMP_RESEARCH_ENVIRONMENT=test \
  .venv/bin/python -m pytest
```

## Unresolved limitations

- The prompt did not include an admission-rate measurement. The 30/minute arrival
  simulation is a labeled stress assumption and must be replaced with a measurement
  from a restored dataset or a fresh burn-in.
- The capacity equation assumes full 30-address batches. Status exposes occupancy;
  materially lower occupancy reduces realized token capacity and requires more
  headroom or lower effective rates.
- The 20% headroom is a policy choice, not proof that every future burst of DEX
  admission and retries fits. The shared HTTP limiter remains the hard safety net.
- Integer-second intervals can leave small safe capacity unused in CRITICAL mode.
- Existing unleased schedules adopt a changed effective interval when next claimed
  and completed, producing a bounded rollout transient rather than a destructive
  mass rewrite of due times.
- Lifecycle rules can leave NEW and RESURRECTED indefinitely in those states. This
  task intentionally does not invent age or trading-quality transitions.
- Storage growth is now likely higher than the original burn-in's observed rate;
  validate six-hour and 24-hour deltas before unattended operation.
- The local burn-in data-loss incident must be resolved or formally accepted before
  any live restart is represented as continuous research coverage.
