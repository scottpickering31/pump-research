# Collector V2 factual audit

Status: architecture and data-contract audit only. This document does not authorize an Epoch 3, a collector start, a lifecycle-policy change, an API-budget change, or mutation of Epoch 1/2 data. Repository state was inspected at Alembic head `f2c8d4a6197e`; Epoch 2 measurements are read-only observations made on 2026-08-17.

## Executive finding

The current collector is strong at immutable market/request provenance, lifecycle and scheduling auditability, epoch/run isolation, storage telemetry, backup evidence, and fail-closed retention. It is not yet a sufficient long-term trading-research contract. The main gaps are normalized DEX fields already present in raw payloads, promotion/boost history, on-chain token and holder state, creator/trader/wallet facts, execution quotes, shared market context, and a polling policy whose steady-state demand depends on recent arrivals rather than all tokens ever discovered.

The raw-response model is doing important work: every successful DEX batch stores the original pair list in `api_request_log.response_payload`, and each normalized observation identifies its record by locator and hash. A physical sample of 3,141 Epoch 2 DEX responses found `pairCreatedAt` and 6h/24h transaction windows in 2,945 responses, `info` in 716, and pair-level `boosts` in 198. Those fields are retained but mostly not normalized. The pair payload's boost object is only `active`; DEX Screener's dedicated boost endpoints are required for `amount` and `totalAmount`.

## Classification legend

| Code | Meaning |
|---|---|
| A | Already collected durably in normalized or explicit audit fields |
| B | Present only in immutable raw provider payloads |
| C | Derivable later from existing immutable facts without another source call |
| D | Missing, but cheap enough for continuous or shared low-frequency collection |
| E | Missing and expensive; candidate-triggered or on-demand |
| F | Unnecessary, redundant, or low-value for the stated research goal |

“Derivable” does not mean “safe to derive without an as-of cutoff.” All derived features must use knowledge available by the requested historical time.

## Architecture inventory

| Family / table | Durable contents and role | Class | V2 disposition |
|---|---|---:|---|
| `tokens` | Provider-neutral `(chain,address)` identity, nullable provider source time, persistence time | A | Keep. Add/derive a distinct first-received knowledge time; do not repurpose nullable source time. |
| `pairs` | Token relationship, `(chain,address)`, DEX identifier, nullable first-discovered time | A | Keep. Normalize first seen and provider `pairCreatedAt`; retain one-to-many pairs. |
| `collection_epochs`, events, current projection | Immutable declaration/configuration plus append-only status transitions and rebuildable projection | A | Keep as the top-level validity/provenance boundary. |
| `collector_runs`, run events, component health | Run configuration/version, heartbeats, terminal and reconciliation evidence, operational health | A | Keep. Archive immutable run/event evidence; component health is a mutable projection. |
| `discovery_events` | Full PumpPortal payload, hash, provider/event identity, nullable source time, receipt/persistence and run | A | Keep. This is the authoritative discovery evidence. |
| discovery checkpoints/connectivity | Best-effort coverage declaration and durable disconnect/reconnect gaps | A | Keep. PumpPortal has no replay, so completeness remains explicitly best-effort. |
| `dex_availability_tasks` | Mutable PENDING_DEX admission work, lease and attempts | A | Keep as operational projection; lifecycle/request ledgers carry history. |
| `api_request_log` | Immutable request/response JSON, status/outcome, timestamps, hash, failure and run | A | Keep the envelope hot; archive large raw bodies early after verification. |
| `observations` | Immutable normalized pair-market facts linked to raw request/record | A | Keep, extend narrowly with high-value existing DEX fields. |
| lifecycle policies/evidence/events | Immutable policy by digest, selected-pair evidence per token/request, transitions with watermark and configuration | A | Preserve semantics. Redesign evidence row width; it is currently the largest relation family. |
| scheduler policies/capacity decisions | Immutable policy and population/effective-cadence decisions | A | Keep. V2 adds coverage-class/age-band facts without changing lifecycle evidence thresholds initially. |
| `poll_schedules` | Mutable next-due/lease projection | A | Keep hot only; rebuild from immutable decisions plus latest completions. |
| poll schedule decisions/batches/members/outcomes | Immutable due, claim, membership, capacity, target/effective interval, lateness and outcome evidence | A | Keep complete enough for gap analysis; archive old high-volume rows. |
| storage sample tables | DB and per-relation bytes, row estimates, rates | A | Keep compact long-term telemetry. |
| backup verification | Artifact path/hash/size/read verification/independent-copy evidence | A | Keep long term; a path alone is not a backup. |
| Parquet manifests | JSON manifests with range, rows, bounds, hashes, schema/revision and `deletion_permitted=false` | A | Keep and copy independently. No Epoch 2 Parquet archive exists in the repository at audit time. |
| retention gate | Requires verified archive, second copy, analytical read and explicit human approval | A | Keep fail closed. It performs no deletion. |
| deduplication conflicts | Append-only collision instrumentation | A | Keep; low volume and high diagnostic value. |

## Module inventory

| Module | Verified responsibility | Assessment |
|---|---|---|
| `discovery/contracts.py`, `discovery/pumpportal.py` | Provider-neutral discovery protocol; PumpPortal live WebSocket adapter, raw payload preservation, acknowledgement and best-effort coverage | Strong separation. PumpPortal-specific metadata is raw-only until normalized. |
| `collection/discovery.py` | Coordinates fetch, transactional admission and provider acknowledgement | Keep; source checkpoint/ack follows durable persistence. |
| `collection/dex_availability.py` | PENDING_DEX leases, <=30-address matching, request logging and NEW admission | Correct admission boundary; preserve. |
| `market_data/dexscreener.py`, `dexscreener_models.py`, `rate_limiter.py` | Shared in-process budget, retry/Retry-After, typed plus raw pair responses | Extend typed normalization; retain raw. A future multi-process deployment still needs one shared durable/service budget. |
| `collection/polling.py` | Executes a claimed batch, persists raw request/observations, selects lifecycle evidence and completes outcome | Correct orchestration boundary; new enrichments should not be folded into this universal hot path indiscriminately. |
| `scheduling/policy.py`, `capacity.py`, `scheduler.py` | Versioned cadence, deterministic capacity allocation, PostgreSQL claims/leases, batches/members/decisions and fair conflict handling | Mathematically bounded to safe capacity, but eligibility is cumulative; V2 needs coverage classes and finite horizon. |
| `scheduling/simulation.py` | Fake-clock load simulation | Extend for arrivals, coverage aging, fixed scans and 1m cumulative identities. |
| `lifecycle/classifier.py`, `policy.py`, `evidence_selection.py` | Threshold transitions, immutable policy and versioned primary-pair evidence | Preserve thresholds. Narrow future evidence physical representation only. |
| `persistence/models.py`, `repositories.py` | PostgreSQL system of record, idempotent immutable facts and mutable projections | Strong base; add source-attributed V2 families rather than mutable token columns. |
| `collection/runtime.py`, `worker.py`, `recovery.py` | Advisory-lock singleton runtime, heartbeats/component state, graceful stop and stale reconciliation | Keep; V2 enrichment workers must participate in the same run/health contract. |
| `epochs.py` | Planned/running/completed/invalid epoch transitions and valid-data boundary | Keep; all V2 reports/datasets default to valid epochs. |
| `monitoring/status.py`, `monitoring/storage.py` | Operational state, capacity/lateness, storage sampling and extrapolation | Extend by coverage class, enrichment freshness/budget and archive lag. |
| `reporting/twenty_four_hour.py` | Epoch-filtered collection, scheduler, API, lifecycle, dataset, storage, archive and backup report | Extend rather than create an unversioned parallel report. |
| `archival.py`, `archive_analytics.py` | Streaming Parquet/Zstd export, manifest/idempotency/verification and DuckDB queries | Sound prototype; expand family coverage and object-store catalog. |
| `backup.py`, `retention.py` | Read verification/backup evidence and fail-closed future retention gate | Preserve; no deletion exists or is proposed for this task. |
| `cli.py`, `config.py` | Explicit operational commands and validated/versioned settings | Add future commands only with dry-run/safety semantics; no V2 behavior in this audit. |

## Admission and discovery facts

PumpPortal discovery creates the canonical token, raw `discovery_event`, `PENDING_DEX` lifecycle event, and DEX availability projection in one transaction. It does **not** create `NEW`. The availability worker batches at most 30 addresses; `NEW` is entered only when `GET /tokens/v1/solana/{addresses}` returns a matching pair. The successful or explicit empty response is logged before the task is advanced. This is correct admission semantics and must not be changed as part of V2.

PumpPortal's typed contract uses only `mint` and optional `signature`, while allowing and preserving all extra fields. Epoch 2 payloads include, among other keys, `name`, `symbol`, `uri`, `traderPublicKey`, `pool`, `marketCapSol`, bonding-curve values, initial buy and transaction signature. These are B until normalized. `traderPublicKey` is a provider assertion; it must not be relabelled “creator” without an on-chain/source-specific definition.

`tokens.first_discovered_at` is nullable because the collector does not manufacture a provider timestamp. Both case-study tokens have it null, even though their discovery `received_at` is known. V2 should expose `first_received_at = min(discovery_events.received_at)` as the canonical knowledge-time origin while retaining the nullable source time separately.

## Current observation contract

Every observation is pair-scoped, immutable and linked to one immutable API request. Its durable normalized fields are:

| Group | Fields | Class |
|---|---|---:|
| Identity/provenance | composite partition key `(received_at,id)`, `pair_id`, `api_request_log_id`, `source_record_locator`, `source_record_sha256`, `persisted_at` | A |
| Time | collector `received_at`; nullable `source_observed_at` | A |
| Price/value | `price_usd`, `price_native`, `liquidity_usd`, `market_cap_usd`, `fully_diluted_valuation_usd` | A |
| Volume | `volume_m5_usd`, `volume_h1_usd`, `volume_h6_usd`, `volume_h24_usd` | A |
| Price change | `price_change_m5_pct`, `price_change_h1_pct`, `price_change_h6_pct`, `price_change_h24_pct` | A |
| Transactions | `buys_m5`, `sells_m5`, `buys_h1`, `sells_h1` | A |

DEX Screener's pair response also contains the following. They are preserved in the raw response, not normalized:

| Raw pair field | Current state | Class / recommendation |
|---|---|---|
| `txns.h6`, `txns.h24` buys/sells | Raw and observed in Epoch 2 | B; normalize because it is small and repeatedly useful. |
| `liquidity.base`, `liquidity.quote` | Raw | B; normalize for reserve/composition checks. |
| `pairCreatedAt` | Raw and common | B; normalize into a source-attributed pair fact. |
| base/quote name, symbol, address | Raw; token/pair address partly normalized | B; normalize as versioned metadata/pair composition, not mutable token columns. |
| `labels`, pair URL | Raw | B; labels are useful low-frequency facts; URL is derivable/display-only. |
| `info.imageUrl`, websites, socials | Raw when present | B; normalize as immutable metadata snapshots/events. |
| `boosts.active` | Raw when present | B; normalize as pair-response boost snapshot. |
| dedicated boost `amount`, `totalAmount` | Not in pair contract | D; collect from the dedicated shared endpoints. |
| provider-defined extra windows/fields | Raw survives typed-model `extra=ignore` | B; keep raw-first and promote fields only after a versioned contract decision. |

Price/liquidity velocity, returns, liquidity-to-market-cap/FDV ratios, buy/sell ratios, rolling volatility, observation gaps and threshold crossings are C. They should not be duplicated into universal raw observations.

The current source does not assert a market observation timestamp, so `source_observed_at` remains null. `received_at` is the knowledge-time boundary; `pairCreatedAt` is a separate provider assertion and is not an observation time.

## Lifecycle and scheduler audit

Current lifecycle thresholds are versioned and are not changed by this design:

- NEW to ACTIVE: 5m volume at least 100.
- NEW to WATCH: below ACTIVE volume and liquidity at least 1,000.
- ACTIVE to FADING: 5m volume at most 25.
- WATCH to FADING: 5m volume at most 10.
- FADING to DORMANT: 1h volume at most 10 and liquidity at most 100.
- DORMANT to RESURRECTED: 5m volume at least 100 and liquidity at least 500.

The scheduling targets are NEW 15s for the first two minutes in NEW, then 30s; ACTIVE and RESURRECTED 5s; WATCH 15s; FADING 120s; DORMANT 900s. A deterministic capacity plan uses the configured 240 request/min ceiling, 20% headroom, and 30-address batches: 192 safe requests/min or 5,760 token observations/min. ACTIVE/RESURRECTED are protected; lower tiers share residual capacity by weighted max-min allocation. Completion schedules the next due time from completion plus the effective interval, so the mathematical plan is bounded. Epoch 2 nevertheless showed operationally unacceptable long-tail latency because a continually growing low-information population remains perpetually eligible and strict priority plus ongoing arrivals can leave already-due low tiers waiting for hours. V2 needs a finite direct-polling horizon and fixed-budget resurrection/control scans, not another change to the API ceiling.

Lifecycle evidence is scientifically sound but physically expensive. There are 9,202,662 observations and exactly 9,202,662 lifecycle evidence evaluations. The evidence partition is about 9.86 GB total versus 3.98 GB for observations. V2 should keep the selected observation/pair, input watermark, outcome, reason code and policy digest for every evaluation, while storing bulky candidate/ranking JSON only for failures, ambiguity, multi-pair cases or policy diagnostics. That is a forward schema optimization, never a rewrite of Epoch 1/2.

## Archive and reporting audit

The exporter streams rows in chunks, writes date/epoch/data-family Parquet with Zstandard, canonicalizes JSON, publishes atomically, and verifies row counts, time/ID bounds, content and file hashes. DuckDB usability queries cover counts, time series, observation/pair/token joins, lifecycle reconstruction, windows and buy/sell ratio. The retention module is a safety gate only.

Current export families are observations, lifecycle evidence/events, poll batch members/batches/outcomes, API logs, discovery events, pairs and tokens. Missing from the archival contract are epoch/run events and policies, discovery connectivity, deduplication conflicts, scheduler policies/capacity decisions, poll schedule decisions, storage telemetry and backup evidence. Mutable projections (`poll_schedules`, component health, checkpoints, DEX tasks) should be snapshotted only for recovery/debugging; their immutable ledgers and policies are the research record.

A read-only 10-minute Epoch 2 export (17:00–17:10 UTC) measured 115,750 exported rows, 15,241,303 Parquet bytes, 131.67 bytes/exported row and 5.586:1 logical-to-Parquet compression. Its naive same-load extrapolation is 2.044 GiB/day. It is a short-window measurement, not an Epoch 2 full-export claim.

## Epoch 2 measured storage inventory

The final database was about 21.78 GB decimal (20.28 GiB). Epoch 2 ran from 2026-08-16 07:14:28 to 2026-08-17 18:44:15 UTC and durably linked 50,983 distinct discovered tokens and 50,965 distinct NEW admissions to its collector runs; the roughly 58.9k final token identity population also includes prior engineering history. From the first to last Epoch 2 telemetry sample, 2026-08-16 07:14:30 to 2026-08-17 18:24:44 UTC, the database grew 17,929,469,952 bytes in about 35.17 hours: approximately 12.24 GB/day or 11.40 GiB/day.

| Relation family | Approx. final total bytes | Main cause |
|---|---:|---|
| lifecycle evidence current partition | 9.86 GB | one wide evaluation per observation; 2.32 GB indexes |
| observations current partition | 3.98 GB | 9.2m normalized facts and indexes |
| API request log | 3.69 GB | raw JSON; about 2.95 GB TOAST |
| poll batch members current partition | 2.50 GB | per-token claim/due evidence and indexes |
| poll batch outcomes | 0.51 GB | one outcome per batch |
| poll batches | 0.50 GB | batch configuration/evidence |
| lifecycle events | 0.23 GB | transition history |
| poll schedule decisions | 0.18 GB | scheduling history |
| poll schedules | 0.16 GB | mutable projection/index churn |
| discovery events | 0.08 GB | full PumpPortal payloads |
| tokens + pairs | 0.03 GB | identities |

The largest Epoch 2 growth contributors were lifecycle evidence (+8.13 GB), observations (+3.27 GB), API request log (+3.05 GB), poll members (+2.08 GB), batch outcomes (+0.41 GB), and batches (+0.40 GB). Storage work should target evidence width and hot/cold placement, not discard observations or chaff.

## Case-study evidence

### Qenis — `76is6tHSLhCyRr3kQYpT9P3KVd4bVDgLsVh8mjjDpump`

Epoch 2 contains 810 selected observations. Market cap ranged from about $1,325 to $18.96m and liquidity from about $1,344 to $316,109. Before the peak, the aggregate 5m buy/sell-count ratio was about 8.62; 95% of absolute pre-peak price steps were below about 0.65%, consistent with the visually smooth rise. From the peak it fell below 50% in about 42 seconds and below 10%/1% in about 134 seconds.

Current facts make the trajectory, buy/sell imbalance and liquidity collapse visible. They cannot establish whether distinct humans bought, whether wallets were related, who funded them, whether trades were self-coordinated, how holdings concentrated, whether LP tokens/positions were controlled or removed, or whether a quoted route could execute the displayed return. V2 needs candidate-triggered swap/trader distribution, holder snapshots, creator/funding graph, liquidity events and execution quotes to diagnose the abnormality before collapse. Boost history and token authority/metadata changes are useful contextual facts, not sufficient classifiers.

### 24iu — `24iuWxHS71FePsDAkmxypASu3gu8HDLdwehDbavVpump`

The explosive section is in invalid Epoch 1, not valid Epoch 2. It must never be spliced into an Epoch 2 research dataset. As engineering-only evidence, Epoch 1 has 173 observations: market cap rose from about $268 to $572k, then crossed below 50%, 10%, and 1% of peak in roughly 28, 33, and 51 seconds. Peak liquidity was only about $23.9k. Valid Epoch 2 contains 68 later observations around $261–266 market cap, $507–515 liquidity and no trades.

This is an important contract test: price return alone implies an extraordinary winner, while low depth, route availability, fees, latency, price impact and fill failure determine whether the return was executable. Unique traders, size distribution, wallet relationships and holder/creator facts help separate broad momentum from manipulation. The data builder must keep the invalid engineering epoch labelled and excluded by default.

## External contract verification

- DEX Screener's official [API reference](https://docs.dexscreener.com/api/reference) documents up to 30 token addresses and 300 requests/min for `/tokens/v1`, pair `txns`, liquidity base/quote, `pairCreatedAt`, `info`, and `boosts.active`. The dedicated latest/top boost endpoints are 60 requests/min and expose numeric `amount` and optional `totalAmount`.
- DEX Screener's [boost documentation](https://docs.dexscreener.com/boosting) currently describes 500 active boosts as its “Golden Ticker” display rule. V2 must store numeric facts and source/version, not encode 500 as an immutable research truth.
- Solana documents that mint accounts expose decimals, mint authority and freeze authority, and that freeze authority can block transfers. [Token Extensions](https://solana.com/docs/tokens/extensions) add mechanisms such as transfer fees, non-transferability, permanent delegate, transfer hooks, default state, pausing and confidential transfer. Security collection must record the token program and decoded extensions, not apply Ethereum contract assumptions.
- Standard Solana [`getTokenLargestAccounts`](https://solana.com/docs/rpc/http/gettokenlargestaccounts) returns only the 20 largest token accounts. It supports top-holder concentration but not exact holder count or wallet clustering; those require more RPC work or an indexed provider.
- Metaplex metadata can change when the update authority exists and the token is mutable; see the official [metadata update contract](https://www.metaplex.com/docs/tokens/update-token). Metadata must therefore be snapshotted/versioned.
- Jupiter's official [quote API](https://developers.jup.ag/docs/api-reference/swap/v1/quote) returns input/output amount, minimum threshold, slippage, price impact, route plan, AMM fees, context slot and quote time. Historical quote snapshots are necessary for executable backtests; future route state cannot reconstruct a past quote reliably.

## Audit conclusions

1. Admission semantics are correct and should remain unchanged.
2. Existing raw evidence prevents permanent loss of several omitted DEX fields, but repeatedly parsing multi-gigabyte JSONB is not a durable analytical strategy.
3. The collector can reconstruct “what request returned and when,” lifecycle decisions, expected polls, failures and capacity decisions unusually well.
4. It cannot yet reconstruct holder, trader, creator, security, promotion or execution state at time T.
5. The current scheduler is mathematically capped but the eligible long tail grows without a stopping rule, so useful cadence does not scale with cumulative population.
6. The main hot-storage problems are one-wide-evidence-row-per-observation and raw payload residency, not token identity or lifecycle events.
7. Epoch 2 is valid and remains untouched. The invalid Epoch 1 portion of 24iu is engineering evidence only.
