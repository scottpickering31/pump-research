# Collector V2 Phase 5: candidate orchestration

## Pre-implementation audit

This audit was completed before Phase 5 code was changed. Four concepts already
exist, but only two have durable runtime representations:

| Concept | Existing meaning | Durable state before Phase 5 | Coupling found |
|---|---|---|---|
| lifecycle `WATCH` | Market-behaviour classification produced when the existing NEW evidence is below the ACTIVE volume threshold but meets the existing liquidity threshold | `lifecycle_events` plus `poll_schedules.lifecycle_state` | The coverage policy maps WATCH directly to `PROTECTED_WATCH`; the lifecycle thresholds themselves are otherwise isolated in `lifecycle/policy.py` |
| coverage priority | Operational observation cadence, independently derived from lifecycle, admission time, and FADING age | `coverage_decisions`, `coverage_policies`, and the `poll_schedules` projection | ACTIVE, RESURRECTED, and WATCH deliberately select protected coverage classes; no candidate concept is consulted |
| Phase 4 candidate timestamp | An offline, deterministic research reference row at a fixed age or other dataset-builder rule | Dataset manifest/Parquet artifact only | It is not an online nomination and must not be reused as one |
| enrichment candidate/tier | “Additional analysis is justified using facts known now” | None | No candidate, tier, promotion, demotion, or enrichment-task entity existed |

The word `candidate` in the existing lifecycle evidence selector refers to a
candidate DEX pair. It is unrelated to token enrichment candidacy.

### Evidence and work patterns already available

- Boost observations and boost events are append-only and distinguish missing,
  null, and numeric zero. They did not wake retired tokens before Phase 5.
- Metadata events and token-security snapshots are append-only. Security uses a
  compact operational task projection with PostgreSQL leases.
- Poll work is claimed transactionally with `FOR UPDATE SKIP LOCKED`; leases are
  recoverable. Four fixed workers are used, never one task per token.
- Coverage is reconstructed from durable admission/lifecycle facts at restart.
  Capacity decisions are deterministic, persisted idempotently, and lower
  priorities stretch before protected ACTIVE/RESURRECTED work.
- Retired scans use a fixed token/minute budget and deterministic rotation. Their
  work does not grow with the retired population.

### Separation required by Phase 5

`WATCH` remains a lifecycle fact. Candidate eligibility is a versioned
orchestration decision. Enrichment tier is the current extra-analysis projection.
Candidate coverage is a finite operational override. A token may therefore be:

- WATCH but not a candidate (for example, missing the rule's contemporaneous
  activity evidence);
- a Tier 1 candidate while lifecycle remains NEW, FADING, or DORMANT;
- temporarily in candidate coverage without becoming ACTIVE; or
- demoted to Tier 0 while its immutable candidate and tier events remain.

No existing lifecycle threshold is changed by this design.

## Implemented contract

### Candidate identity and evidence

`candidate_events` is append-only. Its UUIDv5 and idempotency key are derived
from epoch, token, input watermark, evidence digest, policy digest, and trigger.
An insert conflict is followed by semantic readback; different content under the
same identity raises `CandidateIntegrityError`. The compact evidence snapshot
contains the selected observation identity and received-at watermark, lifecycle
and coverage projections, numeric market inputs, and boost/security source fact
identities. It references `market-v1` but does not duplicate its full feature
vector. `ORCHESTRATION_RULE_V1_NOT_TRADING_SIGNAL` is explicitly operational,
not a prediction or trade recommendation.

### Tier model and transitions

| Tier | Phase 5 meaning | Work created |
|---|---|---|
| `TIER_0_UNIVERSAL` | Universal collector only | none |
| `TIER_1_INTERESTING` | Transparent contemporaneous activity, WATCH+activity, new boost, or retired-control evidence justified modest enrichment | security refresh and metadata refresh obligations |
| `TIER_2_INVESTIGATE` | A current Tier 1 token also meets the stronger activity envelope and has a fresh basic security snapshot | the Tier 1 obligations plus a Tier 2 eligibility review |
| `TIER_3_DEEP_REVIEW` | Reserved for Phase 6 | never entered by Phase 5 |
| `TIER_4_PRETRADE` | Reserved for a separately approved future phase | never entered by Phase 5 |

The market envelope is deliberately simple and configurable: liquidity at least
$10,000, at least 20 known 5-minute transactions, and known 5-minute
volume/liquidity at least 0.05. Tier 2 uses $50,000, 50 transactions, ratio 0.10,
and a security snapshot no older than six hours. Unknown inputs do not pass.
WATCH without the activity envelope does not nominate a token. No future return,
outcome label, or later lifecycle event is an input.

Promotion, demotion, and TTL re-evaluation are immutable
`candidate_tier_events`; `candidate_current_state` is only a rebuildable
projection. Tier 1 coverage lasts 30 minutes and Tier 2 lasts 60 minutes. At
expiry, new eligible evidence creates an explicit refresh candidate; cooling
evidence demotes to Tier 0. Old evidence cannot move the projection backwards.

### Coverage and boost wake-up

Candidate tier, lifecycle, and coverage remain separate. An admitted candidate
may receive a durable 15-second scheduling override, but it never becomes ACTIVE
without the unchanged lifecycle classifier. The override expires back to the
age/lifecycle-derived class; completion and restart preserve the expiry. A
database-serialized hard cap of 100 simultaneous overrides bounds extra work to
400 observations/minute (13.34 full batches/minute), within the existing
14-request scheduler reserve. A tier nomination can still be retained when the
coverage slot is deferred; that denial is recorded in tier-event detail.

Newly received boost facts for already tracked tokens may nominate/wake a token.
The numeric boost fact remains canonical, missing is never zero, and wake-up does
not imply ACTIVE or tradability. A PostgreSQL advisory-serialized limit of five
wake-ups/minute prevents a feed burst from creating an avalanche. Overflow is
not silently interpreted as zero and the ordinary fixed-budget retired scan
remains available.

### Enrichment task queue and freshness

`candidate_enrichment_tasks` has a deterministic semantic key, bounded attempts,
`not_before`, owner/lease/expiry, result identity/digest, evidence generated and
received timestamps, and `fresh_until`. Claims use `FOR UPDATE SKIP LOCKED` plus
a database-serialized 12-task/minute gate. Expired claims are recoverable after
restart. Completion is idempotent only for identical results; poison work reaches
terminal `failed` after four attempts without blocking other tasks. Phase 5 does
not execute an external holder, indexer, or wallet call.

### Capacity stress model

Core plus universal V2 work remains 147.9 DEX requests/minute in the conservative
Phase 2 model. Candidate coverage is capped separately and task work is deferred
before core collection.

| Load | Requested tasks/min | Admitted tasks/min | Candidate coverage tokens | Extra DEX req/min | Total DEX req/min |
|---:|---:|---:|---:|---:|---:|
| normal | 1.50 | 1.50 | 49 measured peak | 6.53 | 154.43 |
| 2x | 2.99 | 2.99 | 98 modeled | 13.07 | 160.97 |
| 5x | 7.48 | 7.48 | 100 capped | 13.33 | 161.23 |
| 10x | 14.96 | 12.00 | 100 capped | 13.33 | 161.23 |

All remain below the existing safe ceiling of 192/minute. The configured hard
ceiling remains 240/minute. The candidate queue, boost wake-up gate, and coverage
cap are independent budgets; none can bypass the shared DEX limiter.

### Epoch 2 read-only simulation

The rule was evaluated from lifecycle-selected Epoch 2 observations only, using
received-at 30-minute TTL windows. The query was executed in a read-only
transaction over the completed valid epoch.

- evaluated universe: 58,535 tokens with selected observations;
- unique tokens ever eligible: 672 (1.148%);
- candidate refresh events: 1,593 (0.748/minute; 2.37/eligible token);
- Tier 0→1 promotions/re-promotions: 679;
- Tier 1→0 cooling transitions: 537;
- Tier 2 transitions: zero, as expected because Epoch 2 predates the Phase 2
  security-snapshot stream;
- measured peak concurrent 30-minute coverage: 49 tokens.

This operational selectivity was not tuned against future returns.

Qenis first qualified at `2026-08-16T17:41:57.755382Z`, about 90 seconds after
DEX admission while lifecycle was still NEW. Contemporaneous facts were
liquidity $196,623.87, 5-minute volume $93,298.70, 169 buys, 16 sells, and a
volume/liquidity ratio of 0.474503. It entered Tier 1 for MARKET_ACTIVITY. The
later collapse was not read by the rule. Five deterministic ordinary controls
(`Hqd5…`, `CVAk…`, `BmFz…`, `EAMh…`, `CgPa…`) never met the envelope and remained
Tier 0 despite having 27–274 observations each.

### Phase 6 interface (design only)

`candidates/phase6_contract.py` defines non-executable contracts for
`HOLDER_SNAPSHOT`, `TRADER_DISTRIBUTION`, `CREATOR_HISTORY`,
`LIQUIDITY_EVENT_ANALYSIS`, `WALLET_CLUSTER_ANALYSIS`, and
`FUNDING_GRAPH_ANALYSIS`. Each states minimum tier, bounded inputs, expected raw
fact outputs, TTL, and cache identity. No provider client or task consumer for
these types exists in Phase 5.

### Restart, concurrency, research, and archive behavior

Token-row locks serialize first nomination; global advisory locks serialize the
two budgets; tier and task identities make retry safe; task leases expire. Four
concurrent evaluators produce one semantic candidate and one task set. Candidate
facts are exposed through Phase 4 as-of state and `research candidate-history`;
both candidate and tier input watermarks must be at/before T. Candidate events,
tier events, tasks, and policies are included in future verified archives.

### Migration chain and limitations

The eventual isolated migration order is:

`f2c8d4a6197e → 7c31a8e4d5f2 → b184a7d2e903 → c61e29d841af → e52a1c9d704f`.

Apply it only during a separately approved Epoch 3 readiness procedure after a
verified backup, migration rehearsal, schema parity check, and collector-lock
check. Phase 5 does not apply it live. Existing Phase 1/2/3 dependencies are
linear; the only material risk is attempting to run current code against the
old live schema before the whole chain is applied.

Limitations: the placeholder rule is not alpha, Epoch 2 cannot exercise Tier 2,
candidate tasks await Phase 6 consumers, social/holder/wallet/execution data are
absent, and old archives created before Phase 5 naturally contain no candidate
families.

## Phase 5 verification and live safety

The final isolated gate on 2026-08-22 produced:

- full pytest suite: 180 passed;
- Ruff: passed;
- mypy strict mode: passed for 117 source files;
- `git diff --check`: passed;
- Alembic model/schema parity: no pending upgrade operations;
- isolated `e52a1c9d704f → c61e29d841af → e52a1c9d704f` migration cycle: passed;
- four-worker nomination/task-claim concurrency, lease-expiry restart,
  promotion/demotion TTL, poison-task isolation, global coverage cap, as-of
  visibility, future-fact rejection, simultaneous boost-wake idempotency,
  deterministic boost selection, and
  1x/2x/5x/10x budget stress tests: passed.

Read-only live checks confirmed the live database remained at `f2c8d4a6197e`,
Epoch 2 remained `completed` and valid, the observation count remained exactly
9,202,662, the only epochs were 0/1/2, the latest collector run remained
`stopped`, and the live database held zero advisory locks. An OS-process check
found no collector. No live migration, collection start, historical mutation,
or Epoch 3 creation occurred.
