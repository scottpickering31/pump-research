# Collector V2 Phase 4: strict as-of research contract

Status: timestamp/source audit complete; Phase 4 implements research artifacts only. It
does not create an epoch, start collection, migrate the live database, alter historical
facts, train a production model, or produce a trading decision.

## Canonical knowledge rule

For a decision timestamp `T`, a fact is visible only when its system-availability time is
less than or equal to `T`. A provider timestamp in the past never bypasses that test.

- External/source facts: availability is `received_at`.
- Lifecycle and scheduler-derived facts: availability is `decided_at`, provided every
  input watermark is also `<= decided_at <= T`.
- Coverage facts are operationally effective only when both `decided_at <= T` and
  `coverage_effective_at <= T`; knowing a future effective time does not make the future
  state active early.
- Fixed context buckets require `bucket_end <= T` and `received_at <= T`. A closed bucket
  is never exposed while it is still open.
- Epoch/run terminal state is visible through immutable events at `occurred_at`, not by
  applying the eventual mutable status retrospectively.
- `persisted_at` is audit evidence for durable write timing. It does not replace an
  explicit earlier receipt/decision timestamp, but an impossible `persisted_at` ordering
  is reported as a quality anomaly.

All timestamps must be timezone-aware UTC. Queries use inclusive knowledge cutoffs
(`availability_at <= T`) and deterministic tie-breaking by availability time then durable
identifier.

## Timestamp audit

| Family / field | Meaning | Availability governing as-of use |
|---|---|---|
| `tokens.first_discovered_at` | provider source event time, when supplied | never alone; earliest qualifying discovery receipt or DEX receipt |
| `tokens.persisted_at` | database write time | provenance/quality only |
| `pairs.first_discovered_at` | optional pair identity source/discovery time | never alone; first observation or pair-fact receipt |
| `discovery_events.source_event_at` | provider event time | descriptive only |
| `discovery_events.received_at` | collector receipt | canonical discovery availability |
| API `requested_at` / `received_at` | request start / response or failure receipt | response facts use request `received_at` |
| observations `source_observed_at` | provider observation time, often absent | descriptive only |
| observations `received_at` | completed response receipt shared by normalized pair rows | canonical market-fact availability |
| pair facts `pair_created_at` | provider assertion about pair creation | visible only at pair-fact `received_at` |
| pair facts `received_at` | receipt of source record | canonical availability |
| boost/metadata/security `source_observed_at` | provider/RPC source time, possibly absent | descriptive only |
| boost/metadata/security `received_at` | collector receipt | canonical availability |
| boost events `decided_at` | local change/crossing derivation | `max(decided_at, referenced boost received_at)` |
| lifecycle events `input_watermark` | newest evidence allowed into decision | validation constraint, never availability by itself |
| lifecycle events `decided_at` | local classification decision | canonical availability when watermark is not future |
| capacity decisions `decided_at` | bucketed scheduler-plan decision | canonical availability |
| coverage decisions `decided_at` | time plan was known | knowledge time; effective state additionally needs `coverage_effective_at <= T` |
| context `bucket_start` / `bucket_end` | fixed UTC source window | window semantics; not availability |
| context `source_observed_at` | closed-bucket cutoff (`bucket_end`) | descriptive/validation |
| context `received_at` | derivation completion/receipt | canonical availability, also require closed bucket |
| epoch `created_at`; epoch-current `started_at` | declaration / lifecycle transition to running | provenance only; not proof of live source coverage |
| run `started_at` | collector invocation/startup began | invocation provenance only |
| run `collection_started_at` | startup committed; runtime may now start its live worker | earliest possible live work for that run; not source-connection proof |
| epoch/run events `occurred_at` | immutable operational transition | canonical transition availability |
| all `persisted_at` | database insertion time | durable audit and anomaly check |

### Run/epoch boundary consumer audit

| Code path | Classification and rule |
|---|---|
| epoch create/start/list/status/close | lifecycle provenance; keep epoch-current `started_at` |
| scheduler epoch initialization | operational lifecycle anchor for deterministic rebase; keep epoch `started_at`, with no coverage claim |
| stale-run recovery and latest-run ordering | invocation/process provenance; keep run `started_at` |
| collector status | latest-run ordering and invocation uptime use `started_at`; display `collection_started_at` and live uptime separately |
| archive eligibility, sizing, and `collector_runs` export | epoch/run provenance scope; keep lifecycle/run `started_at`; archived run rows carry the separate live boundary |
| epoch-scoped 24-hour report and continuity diagnostics | actual live-window calculation; use each run's `collection_started_at`, fail on NULL, and retain inter-run gaps |
| candidate epoch simulation rate denominator | actual live-window calculation; sum known run intervals and retain gaps; fail on NULL |
| PostgreSQL/Parquet research sources and dataset descriptors | actual live-window provenance; require known run boundaries and preserve every interval |
| source-specific discovery/API coverage | use immutable source events, connectivity evidence, attempts, and responses; neither start field proves coverage |

## Identity, admission, and pair selection

Research admission is the first valid-epoch lifecycle transition to `NEW`, using that
event's `decided_at`. This is the collector's stable first-DEX-visible decision. Source
launch time, token identity time, and pair creation time are retained separately and
must not substitute for admission.

At `T`, the canonical pair is selected only from pairs having observations available by
`T`: take each pair's latest available observation, then choose greatest known liquidity,
latest receipt, and pair UUID as deterministic tie-breakers. Lookback and outcome paths
stay on that selected pair; they never splice prices across venues.

## Hot/cold behavior

PostgreSQL and verified Parquet adapters normalize into one provider-neutral history
contract. Both apply the same valid-epoch filter, availability predicates, ordering, pair
selection, feature, and label code. Cold files may contain future rows; filtering occurs
inside the as-of layer rather than trusting archive range boundaries.

No automatic overlap resolution is permitted. A combined source must have explicit,
non-overlapping hot/cold watermarks or prove duplicate rows byte/semantically identical.

## History windows and irregular sampling

The current point is the latest selected-pair observation at or before `T`. A lookback
baseline is the latest observation at or before `T-horizon`; it is accepted only within
the versioned backward tolerance. Actual elapsed seconds and current observation age are
features. There is no forward interpolation, backfill, resampling, or use of a future
point. Missing baselines yield null features.

V1 backward tolerances are 5, 10, 15, 30, 60, 120, and 300 seconds for 5s, 15s, 30s,
1m, 2m, 5m, and 15m horizons respectively.

## Dataset universe and validity

Canonical datasets include every valid-epoch admitted token that reaches a configured
reference timestamp, including dead, retired, failed, and feature-poor tokens. Outcomes
never decide whether the canonical candidate row exists. Outcome-based strata are
research-only views over an already fixed candidate universe and carry their own digest.

Invalid epochs are excluded by default and cannot silently combine with valid epochs.
Qenis is available as a valid Epoch 2 case study. The explosive 24iu segment is in invalid
Epoch 1; Epoch 2 has 68 later observations around $261–266 market cap and $507–515
liquidity. Phase 4 uses that as a negative validity/leakage test and does not splice the
invalid move into Epoch 2 labels.

Canonical PostgreSQL and archive research adapters require a known
`collection_started_at` for every run in the selected epoch. A historical NULL fails closed as an
unknown coverage boundary; it is never replaced with run or epoch `started_at`. Dataset source
descriptors retain each distinct run interval. Restarts therefore do not collapse intervening gaps
into continuous coverage. The first durable discovery event or API request may be slightly later
than `collection_started_at`, and source-specific completeness remains governed by durable source
events, requests, checkpoints, and connectivity evidence—not by the live-work boundary alone.

## Leakage threats

Tests attack late metadata/security/boost/lifecycle updates, backdated source timestamps,
future observation rows, open context buckets, cross-pair price splicing, invalid epochs,
split-boundary labels, and accidental forward interpolation. Any violation is an
integrity error or a missing value, never silent repair.

## Implemented interfaces

The provider-neutral `pump_research.research` package contains immutable fact contracts,
the as-of state resolver, market-v1 features, outcomes-v1 labels, candidate generation,
chronological splits, source adapters, and the dataset builder/verifier. Commands are:

```text
pump-research research as-of --epoch N --token ADDRESS --at TIMESTAMP
pump-research research token-history --epoch N --token ADDRESS --at TIMESTAMP
pump-research research build --epoch N --from TIMESTAMP --to TIMESTAMP --output PATH
pump-research research inspect MANIFEST
pump-research research verify MANIFEST
```

`--archive-manifest` selects verified cold Parquet. Supplying it with an explicit
`--hot-from` cutoff joins cold facts before the cutoff to PostgreSQL facts at/after it.
The ranges do not overlap; duplicate durable IDs are accepted only when content is equal.
The cold adapter reruns the Phase 3 checksum/schema/content verification before querying.

## Epoch 2 case studies

### Qenis

The read-only benchmark scanned all 810 valid Epoch 2 observations. Fixed-age snapshots
before collapse show market cap rising from about $7.46m at +2m to $15.94m at +1h,
liquidity rising from about $197.8k to $289.6k, m5 buy/sell imbalance of 0.75–0.82,
positive one-minute returns, five-minute path monotonicity of 0.70–1.00, and low realized
short-window volatility. These are descriptive facts, not a Qenis-specific rule.

The same rows' future-only labels record an observed >=80% price decline, an >=80%
liquidity decline, and `time_to_collapse` between roughly 20 and 78 minutes after the
respective snapshots. Those facts are absent from feature columns. Missing 6h endpoints
remain unavailable even though an actually observed threshold crossing is irrevocably
`TRUE`; an unobserved crossing remains `UNKNOWN` when the path is incomplete.

### 24iu

The requested explosive segment is not in valid Epoch 2. Epoch 2 contains 68 late,
nearly flat observations and no Epoch 2 `NEW` admission event. Therefore fixed-age v1
correctly generates no 24iu candidate rows. Importing its earlier explosive move from
invalid Epoch 1 would violate both epoch validity and as-of provenance, so Phase 4 uses
this case as a leakage-boundary test rather than manufacturing an “early” case study.
The available Epoch 2 facts remain queryable through observation-driven inspection.

## Read-only benchmark and quality findings

Free disk before the build was only 6.4 GiB, so the full 9.2m-observation Epoch 2 build
was rejected operationally in favour of the fixed Qenis/24iu scope. The final benchmark:

| Measure | Result |
|---|---:|
| source observation rows scanned | 878 |
| source tokens | 2 |
| fixed-age candidates emitted | 7 (Qenis only) |
| feature + label columns | 377 |
| Parquet/ZSTD bytes | 124,334 |
| feature/label materialization time | 0.104 s |
| materialization throughput | 67.2 candidates/s |
| DuckDB verification scan | 0.0098 s |
| process peak RSS | 117,374,976 bytes |

Dataset identity was
`b70dc62f5d3f07cfd6bb66d7c6f3d22e64f0a832b35196ecf76959a901e4e4dd`.
An exact second build reused that identity; manifest SHA-256 remained
`d7c27d790b797d3249374e8e90bd1366f7d62ab62af644db90cc73c6f6b1fbc0`
and Parquet SHA-256 remained
`c403b36a553c76e07ce5253f84f61cd6af9776722dd14ee4fbedcdd8208a4b8b`.

The two-token source load and complete command took about ten seconds; the manifest
separates source rows from materialization timing. Diagnostics found no duplicate
candidate IDs, impossible non-positive prices/negative liquidity, or availability
watermark anomalies. Median source cadence was 6.18s, p95 490.9s, and maximum 7,050s,
showing why elapsed-time/tolerance columns are necessary. Of seven Qenis candidates,
5 had 30s/1m/5m/15m endpoint labels, 3 had 1h labels, and none had a sufficiently near
6h/24h/7d endpoint; unavailable labels remain null/`UNKNOWN`.

The tiny file is dominated by Parquet schema/footer overhead and must not be used as a
direct per-row production estimate. A planning range of 0.8–2.0 KiB per candidate for
large row groups gives roughly 0.8–2.0 GiB per million rows. At seven fixed snapshots
and 28 admissions/minute this is approximately 7–17 GiB/30d, 20–51 GiB/90d, and
82–206 GiB/year. Measure a larger future valid-epoch artifact before procurement.

## Current limitations

- Pre-Phase-2 epochs truthfully expose boost/security/context as unknown; Phase 4 does
  not backfill them.
- Chart outcomes are theoretical. Execution-adjusted return stays unavailable until
  historical quote/route evidence exists.
- Tokens admitted before a valid epoch but lacking a valid admission event are queryable
  but absent from fixed-age candidates. An epoch-boundary cohort rule needs a new contract.
- The builder currently materializes candidate rows in memory. Disk fails closed, but a
  larger benchmark and chunked materialization are required before full-cohort builds.

## Verification and safety result

- Full pytest: 168 passed.
- Ruff, mypy (60 source files), `git diff --check`, and Alembic metadata parity: passed.
- Phase 4 has no schema migration; no live or isolated migration cycle was required.
- Leakage attacks, split/purge, deterministic rebuild, insufficient disk, Qenis/24iu
  separation, verified archive readback, and hot/cold equivalence: passed.
- Live remained at Alembic `f2c8d4a6197e`; observations remained 9,202,662; Epoch 2
  remained `completed`/valid; no Epoch 3 or advisory lock exists; latest run is stopped;
  no collector process exists.

The current code-level `collector status` expects the unapplied Phase 1 coverage migration
and therefore cannot query the intentionally old live schema. Live safety was verified
with direct read-only catalog queries instead; no migration was applied to conceal this
expected version mismatch.
