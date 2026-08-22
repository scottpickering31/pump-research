# Collector V2 long-term data contract

Status: Phase 1 scheduling and Phase 2 cheap universal enrichment are implemented in
code behind unapplied migrations. Candidate/holder/wallet/execution/model layers remain
design-only. No trading, signing, lifecycle-threshold, API-ceiling, or Epoch 3 behavior
is introduced.

## Implemented Phase 2 subset

Phase 2 implements the low-cost portion of this target contract:

- future observations normalize h6/h24 buys/sells and base/quote liquidity;
- `pair_fact_events` records change-deduplicated pair creation/composition/labels;
- `boost_observations` and `boost_events` retain pair-active plus shared latest/top
  amount/total facts and neutral numeric crossings;
- `token_metadata_events` versions PumpPortal and DEX metadata without overwrites;
- `token_security_snapshots` records a finite admission/1h/24h/7d mint-account path;
- `market_context_snapshots` stores shared five-minute as-of context;
- all six immutable families are included in the verified Parquet export catalog.

Metaplex metadata mutability, decoded settings for every Token-2022 extension, holders,
wallet graphs, swaps/trader identities, LP instructions, quotes, social metrics, models,
and decisions remain deferred. Existing Epoch 1/2 facts are not backfilled or rewritten.

## Contract objective

For a token and historical knowledge time `T`, a dataset builder must be able to answer:

> What facts, source assertions, collection gaps, derived state, and policy decisions had the collector received and durably made available by T?

It must then be able to attach future outcomes in a separate label phase without allowing those outcomes, later metadata, later holder state or later tier membership into the feature row.

The contract separates five layers:

1. immutable provider/source evidence;
2. immutable normalized facts;
3. mutable operational projections rebuilt from durable ledgers;
4. immutable derived evaluations with input watermarks and versioned policy;
5. future labels and model/decision outputs, which never mutate layers 1–4.

## Common envelope for every new fact family

Every external snapshot/event should carry or reference:

- canonical subject identity: token, pair, wallet, market or route;
- provider and endpoint/method, provider schema version when supplied;
- immutable request/stream-event identity and raw record locator/hash;
- `source_event_at` or chain `slot`/`block_time` when the source supplies it;
- `requested_at`, `received_at`, and `persisted_at` as applicable;
- observation outcome: data, explicit empty, partial, malformed, failed, not applicable;
- collector run, epoch, code revision and configuration/policy digest;
- parser/normalizer schema version;
- content idempotency key and collision verification;
- completeness/coverage semantics: complete, top-N, sampled, best-effort, or unknown.

Large immutable configuration and schema documents should be content-addressed once and referenced by SHA-256. Large raw provider bodies may move to verified object storage after the retention gate; the hot request envelope retains the content hash and immutable archive reference.

## Proposed data families

### 1. Core market time series

Keep the current observation fields and raw DEX response. Normalize these additional fields already present in the pair contract:

- buys/sells for 6h and 24h;
- base and quote liquidity quantities;
- pair creation time as a provider assertion;
- pair labels and base/quote identity;
- pair-response `boosts.active`;
- optional provider schema version.

Metadata/social arrays belong in change snapshots, not every observation. DEX URL is display data and can remain raw/derived. Retain all provider-defined extra fields in raw evidence before deciding to normalize them. Derived returns, velocity, volatility, buy/sell ratios, liquidity/market-cap ratios and gaps are computed offline as-of a cutoff.

### 2. Token origin and lifecycle

Add or expose immutable facts for:

- source discovery time, nullable;
- collector first-received discovery time;
- first DEX-visible receipt time and request;
- each pair's provider creation time and collector first-seen time;
- Pump.fun pool/migration/graduation facts when a source or on-chain instruction can define them;
- creator/deployer claims with a source-specific role, never an unqualified address guess;
- exact first NEW, ACTIVE, WATCH, FADING, DORMANT and RESURRECTED events from `lifecycle_events`;
- scheduling coverage-class transitions separately from market lifecycle.

Token age must state its origin: source creation age, first-received age, pair-created age, or first-DEX-visible age. A null source time must not be replaced by receipt time.

### 3. Boost and promotion history

Use two evidence streams:

1. normalize `boosts.active` from ordinary batched pair responses at the market cadence;
2. poll DEX Screener's global latest/top boost feeds conservatively under a separate configured route budget, retaining the full returned list, list rank, amount, total amount, request times and coverage semantics.

The latest/top endpoints are feeds, not documented complete token-by-token state. A token absent from a response is `NOT_IN_RETURNED_FEED`, not zero boost. Changes are derived from successive observed values. Store immutable `boost_snapshots` and optional normalized `boost_change_events` with token, provider, observed/received times, `active`, `amount`, `total_amount`, feed kind/rank and request reference. First seen and threshold crossings are derived, versioned events. Do not encode a permanent “gold” category; even the currently documented 500-active display rule can change.

This global-feed approach is the cheapest useful signal. It needs no per-token universal request. Candidate-specific paid-order checks may enrich promotion provenance, but orders expose product/status/payment time rather than complete boost quantities.

### 4. Liquidity behavior

Universally retain normalized USD/base/quote liquidity and pair identity. Derive velocity, ratio to market cap/FDV, drawdown and abrupt removal from market snapshots. For Tier 2/3 candidates, add on-chain liquidity-event facts:

- pool/position/program identity;
- transaction signature, slot/block time and decoded instruction;
- asset amounts before/after;
- add/remove/migrate classification and decoder version;
- LP token/position owner facts where the AMM design exposes them;
- lock/burn/authority evidence with source and interpretation version.

“Locked liquidity” is not a universal Solana field and differs by pool program. Store raw account/instruction facts first and a program-specific evaluation separately.

### 5. Trader activity

DEX aggregate transaction counts cannot provide unique traders or size distribution. For Tier 2/3, collect or decode swaps over closed slot/time ranges and retain transaction signature, wallet/account role, pair/pool, input/output mint and raw amounts, direction, slot/block time, receipt time, source and decoder version. From those immutable swaps derive:

- unique buyers, sellers and traders;
- count and notional distribution, median and percentiles;
- repeat-buyer frequency;
- top-wallet and large-trade share;
- burst/synchronization measures.

If an indexed provider returns only aggregates, persist its definition and coverage. Do not combine provider “trader” counts with locally decoded wallet counts without a namespace.

### 6. Holder distribution

Use infrequent, candidate-triggered snapshots. A compact snapshot contains supply, holder-count method, top-N completeness, excluded accounts/roles, and concentration metrics. A child table may retain the top-N token accounts and resolved owners with balances and source slot.

Standard Solana RPC provides only the 20 largest token accounts in one method; exact holder count and complete top-20 owner aggregation may require resolving token accounts and/or an indexed provider. Record token-account concentration and owner concentration separately. LP/pool/bonding-curve/treasury/creator exclusions must be explicit and versioned. Proposed metrics include holder count/growth, top 10/20 owner percentage, largest non-system/non-LP owner, creator percentage, recent-wallet percentage and concentration change.

### 7. Creator/deployer history

Create source-attributed `token_actor_relationships` rather than a mutable `creator` column. Fields include token, address, role (`pumpportal_trader`, mint authority, metadata update authority, funding wallet, launch transaction signer, etc.), evidence request/transaction, source time, received time and confidence/method.

Previous launches, survival, collapse and winner counts are derived by joining immutable relationships to separately versioned future outcome labels. Reputation scores are future model features, not raw facts. Wallet relationships discovered later have their own availability time and cannot leak into earlier feature rows.

### 8. Wallet and on-chain security analysis

Deep analysis is Tier 3 or Tier 4. Preserve the input address set, closed slot/time range, source transactions/accounts, graph-extraction version and result availability time. Candidate outputs may include:

- common funding sources and funding depth;
- related-wallet clusters with typed edges and evidence;
- synchronized buys and repeated timing;
- wallet first-seen/age definition;
- creator transfers and insider concentration;
- top-buyer concentration and coordinated flow.

Persist raw graph facts separately from versioned `security_evaluations`. An evaluation contains findings and uncertainty, not a permanent “scam” truth. Cache immutable closed-range graph facts forever; refresh open/current results on new slots or a policy TTL.

### 9. Token and contract security state

At first DEX admission, and later on candidate promotion or a slow refresh, snapshot:

- token program ID (classic SPL Token or Token-2022);
- decimals, raw supply and source slot;
- mint and freeze authorities, including revoked/null state;
- metadata program/account, update authority, mutability and URI;
- Token-2022 extension types and decoded settings;
- applicable transfer fee, transfer hook, default frozen state, permanent delegate, pausable/non-transferable/confidential or close-mint mechanics;
- account owner/executable/lamport facts needed to verify parsing.

Authority or supply changes become immutable events derived from snapshots. A security flag must include program/decoder version and underlying facts. Do not import EVM concepts such as honeypot taxes without a Solana-specific mechanism and evidence.

### 10. Metadata and social state

Normalize low-frequency immutable snapshots for name, symbol, metadata URI, image URI/hash where fetched, website, X/Twitter, Telegram and other typed links. Preserve the raw PumpPortal event, DEX `info`, on-chain metadata account and fetched off-chain JSON as separate sources. Record HTTP content hash/status for off-chain metadata. Derive change events; never overwrite the original name or links.

Follower counts, sentiment and engagement are not must-have collection. They are provider-expensive, definition-unstable and susceptible to manipulation; defer them until a concrete research question justifies the cost.

### 11. Shared market-regime context

Create a shared `market_context_observations` series rather than copying context into token rows. Initial low-cost series:

- SOL/USD spot from a versioned source, with request provenance;
- derived SOL returns and realized volatility;
- PumpPortal launches/minute and discovery connectivity state;
- tracked-token admissions/minute, ACTIVE share, aggregate volume/liquidity and transition rates;
- broad tracked meme-token activity/success-rate definitions;
- UTC calendar features derived offline.

One-minute shared resolution is adequate initially; 5–15 seconds is useful only if execution research proves it necessary. Cross-sectional aggregates must record their membership universe and input watermark.

### 12. Execution and tradability context

Displayed price is not an executable price. Historical route state cannot be reconstructed reliably from current pools, so Tier 3/4 needs immutable quote snapshots for fixed hypothetical sizes in both directions. Store:

- input/output mint and raw amount, direction and size-ladder version;
- quoted output and minimum threshold;
- slippage setting, price impact and route/no-route outcome;
- route plan, pool/AMM identities and fee amounts/mints;
- quote context slot, request/receipt latency and provider response;
- current Solana priority-fee context and compute/unit assumptions;
- optional transaction simulation result for paper/pre-trade checks;
- no-route, timeout and malformed responses explicitly.

Quotes estimate route availability and price impact; they do not prove fill. Future paper execution should separately record decision-to-quote latency, simulated/attempted result and realized failure. Universal 5-second quoting is rejected. Candidate-triggered quote ladders and a small shared control sample are sufficient.

### 13. Future candidate/model/decision history

Schema only, for a later approved phase:

- candidate/evaluation timestamp and knowledge cutoff;
- eligibility-policy and feature-set version/digest;
- exact feature availability watermark and missingness mask;
- model ID/version/training-data cutoff;
- opportunity, manipulation and execution scores;
- trade/no-trade or paper decision, reasons and hypothetical size;
- quote/security snapshot references;
- eventual label-set version and outcomes.

No model score belongs in immutable market facts. No live or paper trade action is part of Collector V2 implementation until separately approved.

## Universal versus candidate-triggered collection

Collection tier is an operational coverage class, not lifecycle, quality or label.

| Tier | Subjects and data | Entry | Exit / freshness |
|---|---|---|---|
| 0 DISCOVERED/PENDING | identity, raw discovery, connectivity, DEX availability attempts | every valid discovery event | DEX admission moves to Tier 1; unresolved availability uses age-based backoff and eventually fixed-budget rescan |
| 1 UNIVERSAL MARKET | finite early-life core DEX observations, raw responses, pair/metadata facts, pair boost active, admission security snapshot | every DEX-admitted token | remains represented forever; direct polling cools to retired coverage after finite horizon unless promoted |
| 2 WATCH/INTERESTING | richer trader aggregates, holder/security snapshots, metadata refresh, execution-presence probe | existing ACTIVE/WATCH/RESURRECTED evidence or an explicit versioned deterministic eligibility policy | demote after inactivity/TTL; immutable evidence remains |
| 3 STRONG CANDIDATE | deep swaps/wallet graph, creator history, LP events, holder history, quote ladders | future explicitly approved deterministic candidate rule; no ML required | refresh while candidate; cache closed-range facts and expire open analyses |
| 4 PRE-TRADE | freshest security, supply/authority, route and fee checks | future paper/live decision workflow only | seconds/minutes TTL; not implemented in V2 collector phase |

Initial V2 must reuse current lifecycle evidence for Tier 2 entry where practical and must not invent “good token” thresholds. A new eligibility policy, if later needed, is immutable, versioned and records each decision with its input watermark. Demotion changes future collection only.

Suggested freshness, to validate rather than silently hardcode:

| Fact | Tier 1 | Tier 2 | Tier 3/4 |
|---|---|---|---|
| market pair snapshot | age-decay schedule | 5–15s when active | 5s plus quotes |
| metadata | at admission + 6–24h while young | 1–6h or change-triggered | before evaluation if stale |
| mint/authority/extensions | admission | 6–24h or signature/account change | fresh within 1–5m pre-decision |
| holder snapshot | none beyond optional control sample | 5–15m | 1–5m during candidate burst |
| swap/trader window | aggregate DEX counts only | 1–5m indexed/decoded aggregates | near-real-time transaction stream |
| wallet graph | none | shallow cached result | 6–24h closed facts; open window refresh on new data |
| execution quote | none | optional route-exists probe | 5–15s for a live paper decision; size ladder 30–60s otherwise |

## As-of and leakage rules

Define `available_at` for normalized facts as the durable receipt/commit boundary specified by the table, normally `received_at` plus a committed row. A source event with `source_event_at < T` but `received_at > T` is unavailable at T. A correction or later-discovered relationship is unavailable before its own receipt time even when it describes an older transaction.

Dataset construction follows this contract:

1. choose one valid epoch set and a candidate timestamp T;
2. select source/normalized facts with `available_at <= T` and source event-time constraints required by the feature definition;
3. select the latest derived evaluation whose `decided_at <= T` and `input_watermark <= T`;
4. resolve all configuration, parser, feature and schema digests immutably;
5. emit explicit missingness reason: not requested, requested-empty, failed, partial, not yet available, not applicable or outside coverage;
6. freeze the feature row and its maximum availability timestamp;
7. compute labels only in a separate future window after T.

Epoch validity defaults to valid-only. Invalid Epoch 1 is opt-in engineering data with a prominent validity column and cannot be concatenated silently with Epoch 2. Cross-epoch token identity is canonical, but facts retain their epoch/run provenance.

## Future outcome-label contract

Do not calculate all labels during collection. Define a versioned label job with candidate time T, pair-selection policy, price/liquidity source, gap policy, fee/execution assumption and horizon. Candidate labels include:

- first passage and terminal return at +25%, +50%, +100%, +500%, 2x, 5x, 10x and 50x;
- drawdown thresholds -20%, -50%, -80% and -95%;
- maximum favorable and adverse excursion;
- time to peak, threshold, collapse and liquidity loss;
- liquidity survival and route availability;
- horizons 30s, 1m, 5m, 15m, 1h, 6h, 24h and 7d;
- theoretical market-price and execution-adjusted variants kept separate.

Gaps cannot default to “no move.” Labels should be `observed`, `censored`, `insufficient_coverage` or `not_executable`, with evidence references. Multi-pair labels require a versioned pair/route rule.

## Prioritization matrix

API cost is relative to the current system; storage and CPU refer to incremental V2 cost.

| Data family | Research value | API / CPU / storage / complexity | Collection mode | Recommendation |
|---|---|---|---|---|
| Current normalized market observations and raw provenance | HIGH | existing / existing / HIGH / existing | universal age-decayed + high-frequency active | MUST HAVE BEFORE LONG-TERM EPOCH |
| h6/h24 buys/sells, base/quote liquidity, pair creation/labels | HIGH | none / LOW / LOW / LOW | normalize universal response | MUST HAVE BEFORE LONG-TERM EPOCH |
| First-received and first-DEX-visible knowledge times | HIGH | none / LOW / negligible / LOW | event-driven/derive | MUST HAVE BEFORE LONG-TERM EPOCH |
| Boost active + latest/top amount/total history | HIGH | LOW shared / LOW / LOW / MEDIUM | universal response + shared feed | MUST HAVE BEFORE LONG-TERM EPOCH |
| Versioned metadata/social snapshots | MEDIUM | LOW / LOW / LOW-MEDIUM / MEDIUM | admission + low-frequency/event-driven | MUST HAVE BEFORE LONG-TERM EPOCH |
| Token program, authorities, supply, metadata mutability/extensions | HIGH | LOW-MEDIUM RPC / LOW / LOW / MEDIUM | admission + slow/event/candidate refresh | MUST HAVE BEFORE LONG-TERM EPOCH |
| Shared SOL/launch/cross-sectional regime | HIGH | LOW shared / LOW-MEDIUM / LOW / MEDIUM | universal shared low-frequency | MUST HAVE BEFORE LONG-TERM EPOCH |
| Finite age-decay coverage and retired scan ledger | HIGH | reduces cost / LOW / LOW / MEDIUM-HIGH | universal scheduling | MUST HAVE BEFORE LONG-TERM EPOCH |
| Narrow lifecycle evidence representation | HIGH operational | none / lower / large saving / MEDIUM | universal derived | MUST HAVE BEFORE LONG-TERM EPOCH |
| Holder top-N/concentration snapshots | HIGH | MEDIUM / MEDIUM / MEDIUM / HIGH | candidate-triggered; sparse control | SHOULD HAVE SOON |
| Exact holder count/history | MEDIUM-HIGH | HIGH or indexed / MEDIUM / MEDIUM / HIGH | candidate-triggered | SHOULD HAVE SOON |
| Unique trader and size distribution | HIGH | HIGH / HIGH / HIGH / HIGH | Tier 2/3 transaction/indexer | SHOULD HAVE SOON |
| Creator/deployer source relationships and launch history | HIGH | MEDIUM / MEDIUM / LOW-MEDIUM / HIGH | event-driven + candidate-triggered | SHOULD HAVE SOON |
| LP add/remove/migration facts | HIGH | MEDIUM-HIGH / HIGH / MEDIUM / HIGH | candidate-triggered/event-driven | SHOULD HAVE SOON |
| Execution quote size ladders | HIGH | MEDIUM-HIGH / LOW / MEDIUM / MEDIUM | candidate-triggered/pre-trade | SHOULD HAVE SOON |
| Deep wallet/funding/coordination graph | HIGH | HIGH / HIGH / HIGH / VERY HIGH | strong-candidate only | LATER |
| Priority-fee and simulation/fill evidence | HIGH for trading | MEDIUM / MEDIUM / MEDIUM / HIGH | Tier 4/paper execution | LATER |
| Social follower/sentiment metrics | LOW-MEDIUM | HIGH/unstable / MEDIUM / MEDIUM / HIGH | candidate/on-demand | LATER, only with hypothesis |
| DEX URL copied per observation | LOW | none / none / waste / LOW | derive | REJECT |
| Universal per-token holder/wallet scans at market cadence | LOW marginal | prohibitive all dimensions | universal high-frequency | REJECT |
| Permanent provider UI labels such as “gold” | LOW | none / LOW / redundant / misleading | derive from numeric/source facts | REJECT |
| Universal historical execution quotes for all chaff | LOW marginal | prohibitive / LOW / HIGH / MEDIUM | universal high-frequency | REJECT |

## Data-contract acceptance tests

- Raw and normalized record hashes round-trip and idempotency conflicts compare semantic content.
- Every new snapshot can be traced to an epoch/run/request and parser/configuration digest.
- An as-of fixture with late-arriving old events proves they do not enter earlier features.
- Invalid epochs are excluded by default.
- Metadata, holder, authority and boost absence distinguish unknown/not-returned from zero/null.
- Multi-pair observations and selections remain reproducible.
- Candidate promotion uses only facts available by its decision watermark.
- Quote/no-route outcomes persist at fixed hypothetical sizes.
- A reconstructed feature row is byte-for-byte stable for a fixed contract version.
# Phase 5 candidate orchestration addition

Phase 5 adds immutable `candidate_events` and `candidate_tier_events`, immutable
content-addressed `candidate_policies`, a rebuildable `candidate_current_state`,
and leased `candidate_enrichment_tasks`. Candidate availability is governed by
`candidate_at` plus `input_watermark`; tier availability is governed by
`decided_at` plus `input_watermark`. Both must be at or before an as-of timestamp.
The evidence snapshot is compact and references source fact identities; it does
not convert a candidate into a trading label. Lifecycle, candidate tier, and
coverage remain three independent dimensions.

## Phase 6 selective-security addition

Candidate-only deep evidence is stored in distinct holder, trader, creator,
liquidity, wallet-edge, funding, cluster and security-feature families. Every row
has local availability time and acquisition mode. Canonical as-of datasets include
only historically available rows received by T; retrospective reconstruction
never masquerades as live knowledge. Completeness and provider outcome are
independent, so top-20, partial, unavailable, failed and full distribution remain
distinguishable. See `security-feature-contract-v1.md`.
