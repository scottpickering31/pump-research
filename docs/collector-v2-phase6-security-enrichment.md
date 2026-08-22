# Collector V2 Phase 6: selective security enrichment

## Pre-implementation provider and repository audit

This audit was completed before Phase 6 implementation. Phase 5 has immutable
candidate/tier events and a leased task queue, but its Phase 6 task contracts are
deliberately non-executable. The only existing chain adapter is finalized Solana
`getMultipleAccounts`, used for cheap mint-security facts. DEX Screener market
responses contain aggregate transaction counts and reserves, but no wallet-level
holder, trader, funding, or LP-owner history. Raw DEX responses therefore cannot
reconstruct these families.

### Source matrix

| Family | Source | Class | Completeness and history | Pagination / rate / cost | Phase 6 decision |
|---|---|---|---|---|---|
| top holders | Solana `getTokenLargestAccounts` plus parsed account-owner lookup and the already known mint supply | standard RPC | exactly the 20 largest token accounts at the queried finalized slot; it is **not** a full holder distribution and token-account count is not wallet count | no pagination; two RPC calls when supply is cached; provider limits vary | implement as the default bounded holder source; persist `TOP_20_TOKEN_ACCOUNTS` completeness |
| full holder distribution | token-program `getProgramAccounts`, mint-filtered | standard RPC, expensive | complete only at one provider slot if the provider returns every matching account; owner aggregation and Token-2022 decoding are required | potentially very large response; provider-specific caps/timeouts; Helius documents a plan-dependent complex-RPC limit | disabled by default; eligible only as an explicitly configured Tier 3 source |
| holder count/history | indexed token-account provider | new third-party indexer | provider-dependent; historical snapshots usually cannot be assumed | paid-plan pagination and retention vary | provider-neutral contract implemented; no vendor asserted canonical and no provider enabled by default |
| signatures | `getSignaturesForAddress` | standard RPC | only transactions whose account keys reference the queried address; associated token-account activity can be missed; old history may be pruned by a non-archive node | newest-first, `before`/`until`, up to provider limits | usable for bounded evidence discovery, never described as complete without a closed-range proof |
| transaction details | finalized `getTransaction` | standard RPC | raw transaction and balance changes when retained; `null` means not found/not retained, not “no activity” | one logical lookup per signature, although some providers support constrained batches | compact signature/slot/source identities retained; no redundant full payload in normalized evidence |
| parsed swaps/traders | indexed transaction history, for example Helius `getTransactionsForAddress` | third-party indexer | parser coverage and address/token-account semantics are provider-specific; Helius documents unlimited mainnet retention but a pre-slot-111,491,819 token-owner limitation | plan-dependent; Helius documents 2 requests/s on its free DAS/enhanced tier and no batching for address history | provider-neutral paged contract implemented; operator must configure and accept a provider before live use |
| creator/deployer | mint/metadata authorities, creation transaction, Pump.fun creation evidence, tracked prior tokens | existing facts + standard/indexed chain history | authority is factual but not always the economic creator; fee payer is not automatically creator | cheap when already stored; creation-transaction lookup may be historical/pruned | persist typed relationship plus provenance and confidence basis; never collapse to one opaque score |
| liquidity reserves | Phase 2 pair reserve observations | already retained | complete only at observation cadence for the selected pair | no extra call | derive reserve-discontinuity evidence as-of and retain source observation identities |
| LP add/remove/migration | pool-program transactions or decoded indexed transactions | standard RPC or indexer | decoder/program coverage must be explicit; lock/burn semantics differ by pool design | transaction-history intensive | provider-neutral event contract; unsupported decoder is `unavailable`, never safe/zero |
| funding / wallet graph | finalized native/token transfers from bounded transaction histories | standard RPC or indexer | bounded depth/breadth is intentionally incomplete | expensive per wallet; pagination and history retention matter | Tier 3 only, one-to-two hops, hard wallet/page/edge limits |

Official Solana documentation states that `getTokenLargestAccounts` returns only
20 token accounts, `getProgramAccounts` returns program-owned accounts with
filters, `getSignaturesForAddress` returns signatures that reference the supplied
address, and `getTransaction` may return null. These constraints are encoded in
the data model rather than hidden. Relevant references:

- <https://solana.com/docs/rpc/http/gettokenlargestaccounts>
- <https://solana.com/docs/rpc/http/getprogramaccounts>
- <https://solana.com/docs/rpc/http/getsignaturesforaddress>
- <https://solana.com/docs/rpc/http/gettransaction>
- <https://www.helius.dev/docs/billing/rate-limits>
- <https://www.helius.dev/docs/enhanced-transactions/transaction-history>

No third-party completeness or terms claim is baked into canonical data. API
keys remain environment-only, and provider-specific adapters terminate at a
provider-neutral evidence contract.

### Existing semantic boundaries

- Lifecycle, coverage class, candidate tier, and security evidence are separate.
- Phase 4 research availability is governed by local `received_at`, not a source
  timestamp or slot.
- Phase 5 task leases and `SKIP LOCKED` claims are reusable, but require a Phase 6
  provider budget and result persistence layer.
- Existing archive manifests are table-family based and can include new
  append-only families without changing retention eligibility semantics.
- Epoch 2 contains no Phase 6 evidence. Any present-day chain reconstruction for
  Qenis must be marked `RETROSPECTIVELY_RECONSTRUCTED` and excluded from default
  historical as-of features.

## Implemented evidence contract

Phase 6 adds content-addressed `security_enrichment_policies`, durable pre-I/O
`security_provider_budget_reservations`, and immutable `security_provider_requests`.
The request row records provider/method/schema, request and receive times, source
slot/time, cursor, next cursor, HTTP status where available, raw bounded response,
content hash, availability, completeness, acquisition mode and failure detail.

Normalized append-only families are `holder_snapshots` with ranked
`holder_balance_facts`, `trader_distribution_snapshots`,
`creator_relationship_events`, `creator_history_snapshots`,
`liquidity_event_evidence`, factual `wallet_relationship_edges`, bounded
`funding_relationship_evidence`, explainable `wallet_cluster_snapshots`, and
versioned `security_feature_snapshots`.

Every semantic identity has a database uniqueness constraint and conflict
readback verification. Update/delete triggers protect every Phase 6 fact and
policy table. Mutable work remains only in the leased candidate task projection.
Provider outcomes are available, partial, unavailable, or failed; completeness
is independent and includes top-20, full-distribution, closed-range,
partial-pagination, bounded-graph and unknown. Unknown is never zero.

## Holder snapshots

The default adapter uses finalized `getTokenLargestAccounts`, then a bounded
parsed-account owner lookup. Supply is reused from the latest Phase 2 mint
snapshot available at the task watermark. Token accounts are aggregated by owner;
owner lookup failures retain distinct unknown-token-account identities. Known
pool accounts are excluded only from the separately reported largest non-pool
holder metric, with the exclusion reason retained.

Top-1/5/10/20 and covered-supply percentages are valid top-list facts. HHI is
null for the top-20 source and computed only when a provider asserts a full
distribution. Holder count remains null for the RPC top-20 path. Creator holding
is populated only from a separately evidenced creator relationship.

## Trader, creator and liquidity evidence

A trader page supplies compact facts: signature, slot, source event time, local
receive time, wallet when decoded, side and USD notional when known. Pages are
bounded to four. Each page consumes a durable budget reservation before I/O.
Duplicate signatures with different content are integrity failures; identical
signatures deduplicate. The aggregate preserves its exact window, page count,
completeness and source signatures and reports unique actors, counts, size
percentiles, concentration, repeat ratio and buy/sell overlap. Dependent metrics
are null when wallet/notional decoding is incomplete.

Creator relationships are typed facts rather than identity guesses. Creator
history stores only launches/outcomes already received by its as-of time; a later
launch cannot rewrite an earlier reputation snapshot. No opaque score is stored.

Liquidity evidence stores pair, typed event, signature/slot, source and receive
times, reserve deltas, before/after liquidity, removal percentage and LP wallet
when decoded. It distinguishes add/remove/mint/burn/transfer/migration/reserve
discontinuity. A missing decoder is unavailable. A displayed reserve break is not
falsely labeled as a decoded LP removal.

## Wallet/funding graph and clustering

Graphs are candidate-local: at most 40 wallets, 500 factual edges and two funding
hops by default. Factual edge types include common funder, direct transfer,
co-trade timing, repeated size, creator, LP and shared recent funding. Each edge
cites source facts and never claims two wallets share an identity.

Clusters are deterministic connected components over persisted factual edges.
The identity hashes sorted members, algorithm version and the input-edge hash;
the explanation lists relationship types. Reversing input order produces the
same clusters. Funding facts retain wallet, source, timestamp, amount, signature,
hop and bounded completeness.

## Tier 2/3 and concurrency

Tier 2 queues holder, trader, creator and liquidity tasks. Tier 3 queues wallet
edge/clustering and funding tasks only. Tier 3 means deep review, never pre-trade
or opportunity. Promotion uses contemporaneous evidence when any configured
operational-selectivity condition holds: top-10 holders at least 60%, at least
100 trades from at most 20 known traders, common-funder share at least 25%, or a
recent removal of at least 30%. These select expensive work; they are not fraud
or profit thresholds. Tier 4 remains inactive. Tier 3 is capped at 20 tokens.

Four fixed workers claim with `SKIP LOCKED`. The canonical token is locked before
the Phase 6 evidence read, so same-watermark completions use one committed view.
Equal-watermark evidence can promote/add facts but cannot undo a higher tier;
demotion requires a later watermark. This prevents duplicate deep tasks and
partial-visibility oscillation.

## Freshness and budgets

| Evidence | Default freshness |
|---|---:|
| holder | 10 minutes |
| trader distribution | 5 minutes |
| creator history | 24 hours |
| liquidity analysis | 5 minutes |
| wallet graph/cluster | 1 hour or input change |
| funding relationship | 24 hours or new signature |

Freshness, input hash and `fresh_until` are durable. The default logical-provider
budget is six operations/minute, with sub-ceilings of one holder snapshot, four
transaction-history operations and one wallet-graph operation/minute. A standard
holder snapshot consumes two raw Solana calls (largest accounts plus parsed-owner
lookup), so one snapshot/minute also respects the existing two-raw-RPC/minute
Solana ceiling. It is independent of the DEX limiter.
Precedence is core DEX, universal enrichment, Tier 2, then Tier 3.
Before reserving either raw call, a standard-RPC holder task checks the durable
universal mint-security queue. Work due within the coming minute makes the
candidate task defer without provider I/O or attempt consumption. The shared
raw limiter remains the final hard ceiling.

| Load | Requested/min | Admitted/min | Deferred/min | Core DEX displaced |
|---|---:|---:|---:|---:|
| normal (1 Tier 2, 0.25 Tier 3/min) | 4.5 | 4.5 | 0 | 0 |
| 2x | 9 | 6 | 3 | 0 |
| 5x | 22.5 | 6 | 16.5 | 0 |
| 10x | 45 | 6 | 39 | 0 |
| mania 25x | 112.5 | 6 | 106.5 | 0 |

## As-of, archival and storage

Availability is local `received_at`. Source time never moves it backward. Phase 4
selects Phase 6 facts only when historically available and received by T.
Retrospective rows are queryable for forensics but excluded by default. All Phase
6 facts, request provenance, reservations and policies are Phase 3 archive
families and inherit verified Parquet/ZSTD, checksums, independent-copy and
no-deletion rules. PostgreSQL numeric fields through 76 digits use Arrow
decimal128/256; wider raw integer balances use canonical decimal text because
Arrow decimal256 has a 76-digit limit. This is lossless and explicitly typed in
the manifest rather than coerced to float.

At the six-operation/minute ceiling there are at most 8,640 provider-attempt rows
per day. The holder sub-budget allows at most 1,440 snapshots and 28,800 ranked
facts/day. If all transaction-history capacity were used by one family, its
aggregate ceiling would be 5,760 trader/liquidity rows/day; creator refreshes are
at most 1,440/day; and wallet/funding requests are at most 1,440/day. The absolute
500-edge graph bound implies a deliberately pessimistic 720,000 factual edges/day,
although the 20-token Tier 3 cap and content idempotency make sustained operation
at that edge ceiling unlikely. Planning allowance is 0.15–1.2 GiB/day hot and
0.04–0.35 GiB/day Parquet, with the upper end reserved for pathological maximum-
edge graphs and bounded raw pages. Actual selective rates must be measured in a
future validation epoch; no lower figure is presented as measured fact.

## Qenis and migration boundary

Epoch 2 has market observations but no Phase 6 holder/wallet/creator/decoded LP
evidence. It supports contemporaneous price, aggregate flow and displayed
liquidity discontinuities, not historical holder or funding claims. Any present
chain reconstruction is `retrospectively_reconstructed` and cannot enter Epoch 2
historical-as-of features. A read-only audit found 810 Qenis observations from
2026-08-16 17:41:57Z through 22:25:25Z, maximum displayed liquidity about
$316,109, and a worst consecutive displayed-liquidity ratio about 0.0194 (a
roughly 98.1% discontinuity). These are existing market facts, not wallet/LP
attribution. No Qenis-specific rule exists.

The eventual chain is:

`f2c8d4a6197e -> 7c31a8e4d5f2 -> b184a7d2e903 -> c61e29d841af -> e52a1c9d704f -> f63b7d9a20ce`.

Phase 6 does not apply it live. Advanced indexed trader/creator/pool/funding
sources remain provider-neutral deployment integrations and record unavailable
until configured. The finalized top-20 holder adapter is the only default deep
source. No Tier 4, execution, trading, signing, wallet management or ML exists.
