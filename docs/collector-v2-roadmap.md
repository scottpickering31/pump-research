# Collector V2 implementation roadmap

Status: Phases 1–6 are implemented in source and Phase 7 integration rehearsal is complete.
Their migrations remain unapplied to live; Epoch 3 does not exist and the collector remains
stopped. The numbered historical plan below is retained as design provenance even where delivery
was reordered.

## Decision

**NO-GO to migrate live, create Epoch 3, or start Epoch 3 today.**

The integrated rehearsal found and fixed a live-population reconstruction bind-limit defect and an
archive-scope claim race, then mapped 58,589 schedules without an avalanche. Deployment remains
blocked by insufficient local disk, absence of a full 20 GB restored-clone lock/resource rehearsal,
and provider readiness. See
`docs/collector-v2-integration-readiness.md` and `docs/epoch3-readiness.md`.

## Non-negotiable boundaries

- No mutation, deletion, relabelling or cross-epoch splice of Epoch 1/2 facts.
- No lifecycle threshold changes in the initial V2 implementation.
- No DEX request-ceiling increase.
- No trading, wallet management, signing, transaction submission or ML.
- New expensive integrations are disabled until they have explicit budgets, provenance and test fixtures.
- Every migration runs upgrade/downgrade/upgrade against an isolated test database and is compatibility-tested against a restored copy, never the live collection database.
- Each phase is reviewed and verified before the next one changes behavior.

## Phase 0 — freeze the reviewed contract

Deliverables:

- approve the six Collector V2 design documents and assign schema/policy versions;
- record the measured Epoch 2 baseline queries/results in a machine-readable design fixture;
- decide the proposed age bands, seven-day direct horizon and fixed scan budgets for simulation—not production;
- choose provider contracts for Solana RPC/indexed data without adding credentials yet;
- define explicit acceptance budgets for API, DB GiB/day, archive GiB/day and query latency.

Gate: reviewers can trace every proposed normalized field to an existing raw field or named source contract; unresolved assumptions remain labelled.

## Phase 1 — schema and data-contract foundations

Implement only forward-compatible structures:

- collection/coverage policy documents and immutable coverage decisions;
- normalized pair creation, h6/h24 transactions and base/quote liquidity fields;
- source-attributed token/pair metadata snapshots;
- boost snapshots/feed requests;
- token security/authority snapshots;
- shared market-context observations;
- candidate eligibility/evaluation envelope without scores or trading actions;
- archive catalog/raw-body state needed for hot/cold placement;
- compact lifecycle-evidence format for new rows plus exception detail, leaving old partitions unchanged.

Design all timestamps and idempotency keys before migration. Add epoch/run/request references and parser/configuration digests. Extend archive schemas and as-of readers in the same phase so no new table becomes a PostgreSQL-only historical island.

Tests:

- schema constraints and semantic collision checks;
- nullable source time versus first-received knowledge time;
- late-arriving event cannot enter an earlier as-of row;
- JSON/raw-normalized hash round-trip;
- old Epoch 1/2 rows remain readable without backfill;
- migration upgrade/downgrade/upgrade in a dedicated test DB and schema parity.

Gate: restored Epoch 2 opens and reports identically; no data rewrite is generated.

## Phase 2 — cheap universal signals

Implement adapters/normalizers for:

- DEX fields already paid for in every response;
- pair-response boost active plus global latest/top boost feeds at a conservative shared cadence;
- PumpPortal/DEX/on-chain metadata snapshots and changes;
- admission-time SPL/Token-2022 mint, authority, supply and extension state;
- one-minute SOL/USD and launch/admission/aggregate market context.

Every external client has a protocol and fake. Absence from a feed remains unknown/not-returned, never zero. Provider response bodies and failures use the existing request ledger. No candidate scoring is added.

Tests:

- representative optional/malformed raw payloads;
- boost feed coverage and numeric change events;
- classic SPL and Token-2022 security fixtures;
- mutable metadata changes and off-chain failure;
- shared-service budget interaction with DEX market polling;
- restart/idempotency and archive round-trip.

Gate: incremental API and storage cost are measured under synthetic live rates and fit their allocated reserve.

## Phase 3 — bounded long-tail scheduler

Implement the coverage state machine separately from lifecycle:

- proposed six age bands and finite 65-observation universal path;
- ACTIVE/RESURRECTED and WATCH overrides using existing lifecycle decisions;
- bounded FADING tail;
- retired/pending fixed-budget deterministic rotation;
- event-driven resurrection eligibility;
- per-class allocations, virtual deadlines and deterministic fairness;
- epoch initialization/rebase decisions that do not inherit old lateness;
- complete status/report metrics for lifecycle, coverage, age band and realized cadence.

Keep the existing capacity adapter as the final configured-ceiling guard. Do not change lifecycle classification rules.

Tests:

- fake-clock exact-band boundaries;
- 10k through 1m cumulative populations;
- at least 30 simulated days at 28 admissions/min;
- arrival burst and protected ACTIVE overload;
- four-worker concurrent claims and process restart;
- no starvation in allocated classes and complete rotation accounting;
- request attempts/retries/availability/boost remain below safe budget;
- batch occupancy threshold and partial-batch stress;
- old schedules become explicit epoch initialization decisions, not mass-overdue work.

Gate: demand plateaus near the documented 4,082 observations/min scenario at 500k and 1m cumulative identities, with bounded virtual lateness and honest degradation.

## Phase 4 — archive, catalog and retention readiness

Extend non-destructive archive support to all immutable provenance/policy/coverage/security/context families. Add:

- primary object-store interface and immutable publish/readback;
- independent copy verification;
- hot/cold range catalog with overlap/gap/schema checks;
- content-addressed raw-body export/reference;
- DuckDB/Polars as-of queries across hot and cold;
- isolated restore drill and measured full-day compression.

Do **not** add or enable automatic partition deletion. Retention eligibility may be calculated, but execution remains fail-closed and human-approved in a future task.

Gate: a full synthetic/closed-day archive has equal rows/content bounds, two verified copies, successful analytical reads and a restore/query drill.

## Phase 5 — candidate-trigger framework

Implement budgeting and orchestration, not predictive selection:

- Tier 2 entry may initially use existing ACTIVE/WATCH/RESURRECTED lifecycle evidence;
- immutable eligibility decision with input watermark and deterministic policy;
- per-provider cost reservation and maximum concurrent candidates;
- TTL/cache semantics and explicit unavailable/stale outcomes;
- demotion affecting only future enrichment;
- no model score and no trade action.

Tests prove no circular dependency: a feature collected because of promotion cannot be used to cause that same earlier promotion. Reconstructed decisions after restart are identical.

Gate: candidate workload cannot steal ACTIVE core-market budget or exceed any provider budget.

## Phase 6 — holder and security integrations

Add candidate-triggered top-N/account-owner resolution, supply/holder snapshots and shallow creator/authority relationships. Prefer standard Solana facts; evaluate an indexed provider for exact holder counts only with documented coverage, rate and cost.

Deliver concentration metrics as versioned derivations with explicit excluded account roles. Snapshot raw facts, slot and receipt time. Validate on fixtures for LP accounts, duplicate owner accounts, frozen accounts and Token-2022 mechanics.

Gate: top-N and owner concentration reproduce from archived raw facts; provider gaps remain visible.

## Phase 7 — deep wallet, trader and liquidity analysis

For strong candidates only:

- immutable decoded swap facts and transaction-size distributions;
- creator/funder and related-wallet graph facts;
- synchronized-flow evaluations;
- AMM-specific LP add/remove/migration facts;
- cached closed-range graph analyses with decoder versions.

Start with offline/replay analysis before live enrichment. This phase has the highest provider, CPU, storage and correctness risk.

Gate: Qenis-like and 24iu engineering fixtures can express unique-trader, concentration, liquidity-event and funding evidence without token-specific rules; false certainty is not introduced.

## Phase 8 — regime and execution context

Expand the Phase 2 shared context and add candidate-only execution research:

- SOL return/volatility and versioned cross-sectional universes;
- quote size ladders, route/no-route, impact, fees and context slot;
- priority-fee context and optional simulation evidence for later paper research;
- executable-versus-theoretical outcome contract.

This remains data collection. Do not build or submit a swap transaction.

Gate: a historical quote row cannot be reconstructed using future route state, and no-route/failure is persisted as data.

## Phase 9 — reporting and research validation

Extend status and reports:

- counts/demand/target/effective/realized cadence by lifecycle, coverage and age;
- fixed-scan rotation age and event-trigger recall;
- source connectivity and completeness;
- new stream freshness, missingness and provider budgets;
- hot/archive growth, archive lag and backup age;
- as-of dataset manifest with valid epochs, code/config/schema versions;
- Qenis and 24iu contract reports with explicit epoch validity boundaries;
- theoretical and execution-adjusted metrics kept separate.

Run leakage tests with deliberately late events and corrupted/overlapping archive fixtures.

Gate: one command can produce a reproducible as-of dataset manifest and rerunning it yields identical content hashes.

## Phase 10 — Epoch 3 readiness, then a short validation

Epoch 3 should be a 24–72 hour Collector V2 validation, not a declaration that the annual architecture is finished. It is created only after all prior applicable gates pass.

Pre-create GO criteria:

- full pytest, Ruff, mypy and `git diff --check` pass;
- isolated migration upgrade/downgrade/upgrade and restored-Epoch-2 compatibility pass;
- no test can target the live database;
- 30-day simulation proves bounded demand at cumulative populations through 1m;
- four-worker and restart tests pass;
- ACTIVE cadence protection and overload truthfulness pass;
- every new stream has immutable provenance and explicit missingness;
- closed-day archive and independent copy verify and query;
- fresh pre-start DB backup verifies;
- storage/API projections fit provisioned headroom;
- service shutdown/recovery remains graceful;
- no trading, signing, wallet or automatic deletion path exists.

Validation observations:

- actual admission rate and per-band populations;
- target/effective/realized cadence, cohort completeness and starvation;
- requests/min, occupancy, retries, endpoint budgets and headroom;
- PostgreSQL and per-family GiB/day;
- Parquet/Zstandard GiB/day and query performance;
- boost/security/metadata/context coverage and freshness;
- archive/backup verification and recovery time.

Stop and mark the validation invalid on an unrecoverable continuity/provenance gap. Controlled restarts remain allowed only when report semantics account for them.

## Genuine long-term epoch GO criteria

A successful short Epoch 3 is necessary, not sufficient. Before a genuine long-term epoch:

- no collector crash or unexplained gap in the validation window;
- API demand remains bounded as cumulative token count grows;
- ACTIVE/RESURRECTED realized cadence meets the protected objective under measured load;
- first-ten-minute NEW resolution meets the coverage target;
- retired/dead population cannot create unbounded due work;
- fixed scans and event triggers measure resurrection recall;
- all data streams meet provenance/as-of/missingness contracts;
- archive verifies, hot+cold queries detect gaps/overlaps, and independent backup restores;
- measured hot and cold 30/90/365-day projections fit the selected infrastructure/budget;
- evidence shows no future-data leakage in dataset fixtures;
- explicit human review approves the epoch configuration and purpose.

## Exact implementation order

1. Review/freeze schema and as-of contracts.
2. Add forward-only schema, normalized existing DEX fields and archive schemas.
3. Add cheap universal boost/metadata/token-security/context streams.
4. Implement and simulate coverage-class long-tail scheduling.
5. Complete object archive/catalog/independent-copy verification.
6. Add deterministic candidate-trigger orchestration and budgets.
7. Add holder/security snapshots.
8. Add deep trader/wallet/liquidity analysis offline, then candidate live collection.
9. Add shared regime expansion and candidate execution quotes.
10. Complete reports, leakage tests, load/storage tests and Epoch 3 readiness review.

## Known risks

- PumpPortal remains best-effort and cannot prove complete discovery during disconnects.
- DEX latest/top boost feeds do not document exhaustive state; absence is unknown.
- Exact holders/traders and wallet graphs likely require a paid indexed source or substantial RPC decoding.
- The 28/min admission stress rate uses discoveries as a conservative proxy and needs direct measurement.
- The seven-day horizon and age curve may miss delayed winners; bounded controls must quantify this rather than assume it away.
- A 10-minute Parquet sample is not a full-day/full-epoch compression measurement.
- Candidate streams can overwhelm the system unless they have independent budgets and backpressure.
- Historical execution feasibility cannot be inferred from DEX displayed price alone.

These risks are compatible with beginning incremental implementation. They are not compatible with starting Epoch 3 before the corresponding gates are tested.
# Phase 5 completion boundary

Phase 5 implements deterministic nomination, Tier 0–2 orchestration, finite
candidate coverage, boost wake-up, bounded leased tasks, as-of visibility, and
future Phase 6 task contracts. It deliberately stops before holder/trader
providers, creator-history collection, liquidity-event decoding, wallet/funding
graphs, opportunity models, execution, or trading. Phase 6 requires separate
approval and should consume the versioned task contracts rather than altering
`ORCHESTRATION_RULE_V1` into a trading score.

# Phase 6 completion boundary

Phase 6 implements the provider-neutral selective-security contract, finalized
top-20 Solana holder adapter, bounded page/traversal budgets, immutable deep
evidence, explainable clusters, Tier 3 orchestration, security-v1 as-of access,
archive coverage and failure/concurrency handling. Indexed trader, creator,
pool-program and funding providers remain explicit deployment integrations; an
unconfigured source records unavailable evidence. Phase 6 stops before Tier 4,
execution quotes, opportunity models, trading, signing or wallet management.
