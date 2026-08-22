# Collector V2 Phase 2 — cheap universal enrichment

Status: implementation audit and contract, prepared before Phase 2 schema/code changes.
This phase does not create Epoch 3, change lifecycle thresholds or API ceilings, or add
holder, wallet-graph, execution, model, signing, or trading behavior.

## Pre-implementation inventory

The authoritative market request is DEX Screener
`GET /tokens/v1/{chainId}/{tokenAddresses}` with at most 30 addresses. Every successful
response is retained losslessly in `api_request_log.response_payload`; normalized
`observations` reference both that request and a `pairs[index]` source locator/hash. The
typed adapter deliberately ignores unknown keys, so a raw field can survive while being
unavailable to normalized queries.

PumpPortal discovery similarly keeps every complete source payload in
`discovery_events.source_payload`. Its typed admission contract consumes only `mint`,
optional `signature`, and a provider timestamp when one is supplied. Discovery creates
`PENDING_DEX`; only a matching DEX pair response admits a token to `NEW`. Phase 2 does
not change that boundary.

| Requested fact | Before Phase 2 | Class | Request consequence |
|---|---|---:|---|
| price USD/native, USD liquidity, market cap, FDV | normalized observation + raw | A | none |
| m5/h1/h6/h24 volume and price change | normalized observation + raw | A | none |
| m5/h1 buys/sells | normalized observation + raw | A | none |
| h6/h24 buys/sells | typed `txns` map and raw only | B/D | zero extra request |
| liquidity base/quote quantities | typed `liquidity` and raw only | B/D | zero extra request |
| `pairCreatedAt` | typed and raw only | B/D | zero extra request |
| base/quote name, symbol and address; labels | partly typed and raw only | B/D | zero extra request |
| pair `info.imageUrl`, websites and socials | raw only; omitted by typed model | B/D | zero extra request |
| pair `boosts.active` | raw only; omitted by typed model | B/D | zero extra request |
| boost `amount` / `totalAmount` | absent from pair response | C/E | shared dedicated feed request |
| PumpPortal name/symbol/URI and extra launch facts | raw discovery only | B/D | zero extra request |
| mint/freeze authority, supply, decimals, program | missing | C/E | batched Solana RPC |
| Token-2022 extensions | missing | C/E | same batched mint-account RPC |
| metadata mutability | missing | C/E | separate Metaplex/Token-2022 metadata account work; deferred |
| SOL/USD/context aggregates | derivable from immutable observations/events | C/D | zero external request |

Classes are: A already normalized durably; B raw-only; C missing; D obtainable from an
existing call or immutable facts without an external request; E requires a separate
request/source. `api_request_log` is already provider-neutral and can provenance DEX
feed and Solana RPC requests without weakening raw evidence.

## Official-source contract audit

DEX Screener's official [API reference](https://docs.dexscreener.com/api/reference)
documents `pairCreatedAt`, liquidity `base`/`quote`, arbitrary transaction windows,
`info`, and `boosts.active` on token-pair results. It separately documents global
`/token-boosts/latest/v1` and `/token-boosts/top/v1` feeds at 60 requests/minute, with
`amount` and `totalAmount`. “Latest” and “top” are bounded feeds, not documented complete
per-token state. Omission therefore means only *not present in this returned feed*, never
zero.

Solana's official [`getMultipleAccounts`](https://solana.com/docs/rpc/http/getmultipleaccounts)
contract returns accounts in request order and permits up to 100 public keys per call.
The mint account directly supplies the classic mint/freeze authorities, raw supply,
decimals, initialized state, owning token program, and Token-2022 extension bytes. This
supports a finite admission/young-life snapshot schedule without a wallet graph.

## Phase 2 collection contract

### Existing pair requests

Future observations normalize h6/h24 transaction counts and base/quote reserve
quantities. Pair creation and composition are stored in a change-deduplicated immutable
pair-fact stream, not rewritten onto old pairs. Pair `info` becomes source-specific
metadata history. Pair boost-active is captured only when the `boosts` object and
`active` member are actually present; omitted and null remain unknown.

### Boost feeds

Poll `latest` every 60 seconds and `top` every 300 seconds. This costs at most 1.2 DEX
requests/minute and uses the same application limiter plus the route's 60/minute guard.
Persist the full raw returned feed first, then source-attributed numeric change facts for
already tracked Solana tokens. Do not admit unrelated feed tokens. Feed absence is not a
negative fact. Neutral power-of-ten crossings are audit events for query acceleration;
they carry no UI status or quality meaning and 500 is not encoded as a category.

### Metadata

Normalize PumpPortal name/symbol/URI at discovery and DEX pair token identity/image/link
facts when present. Snapshots are source-namespaced and appended only on a content
change, under a per-token transaction lock. Returning to an earlier value is still a
new event because the source request/event is part of its identity. Social platforms are
not scraped and no follower/engagement metrics are collected.

### Token security

At DEX admission, then approximately 1 hour, 24 hours, and 7 days later, query mint
accounts through `getMultipleAccounts` in batches of up to 100. This finite four-snapshot
path plateaus with arrival rate and does not scan the cumulative retired population.
Persist available, unavailable, and malformed outcomes explicitly, with RPC slot,
account owner, raw-account hash, authorities/supply/decimals, token-program class, and
decoded extension type names. Unsupported extension bodies remain hashed raw evidence;
no opaque risk score is created. Metaplex metadata mutability is deferred because it is
not present in the mint account and needs additional account derivation/fetching.

### Shared context

One five-minute, versioned snapshot is derived using only rows received by the bucket
cutoff. SOL/USD is a robust median of positive `price_usd / price_native` assertions,
with returns/volatility derived only from earlier context rows. The same snapshot records
admissions, transitions, and the latest-per-pair aggregate activity universe. A matured
cohort conversion fraction is evaluated at the current cutoff; it is contemporaneous
context, not a future success label. Context is stored once, never copied into every
token observation.

## As-of rules

`received_at` is the research availability boundary. A source timestamp older than T is
not visible at T when its row was received after T. As-of reads select only
`received_at <= T`; `source_observed_at`, pair creation time, and RPC slot describe the
source fact but cannot override that rule. Explicit nulls mean the source supplied no
value or the fact was unavailable according to the row's status; numeric zero and false
remain ordinary known values.

## Backfill decision

No Epoch 1/2 rows are changed. The raw DEX and discovery bodies make portions of the new
contract theoretically recoverable, but multi-gigabyte historical replay would create
new facts at a later knowledge time and cannot recreate the original normalization
availability boundary. Phase 2 is future-only. Any later engineering backfill must be a
separate derived dataset explicitly labelled with its backfill time and invalid for
historical as-of availability.

## Cost model

At 28 admissions/minute, four finite security snapshots create about 112 security rows/minute.
With 100-account RPC batches the theoretical lower bound is 1.12 RPC calls/minute; a
two-call/minute service cap provides queue headroom without depending on cumulative token
count. Boost feeds add 1.2 DEX requests/minute. Shared context adds 288 rows/day and no
external calls. Change-deduplicated metadata/pair/boost rows depend on source changes and
are bounded far below observation volume in normal operation.

Phase 1's modeled 146.7 DEX requests/minute already includes a 14-request reserve. The
1.2-request boost feed consumes part of that reserve: modeled known traffic rises by 1.2,
the unallocated reserve falls to 12.8, and the total including reserve remains 146.7.
That is 45.3 requests/minute below the 192/minute safe ceiling. If reserve were displayed
without netting the new feed, the conservative sum would be 147.9/minute, still below
the ceiling. Solana RPC has a distinct explicit 2/minute budget. Neither stream changes
Phase 1 cadence mathematics.

## Incremental storage estimate

At the Phase 1 plateau of about 3,953 market observations/minute, six additional nullable
normalized values add roughly 0.30–0.40 GiB/day to hot observation storage before real
compression/TOAST measurements. At 28 admissions/minute, steady-state family volumes are
approximately:

| Family | Expected rows/day | Planning bytes/day |
|---|---:|---:|
| wider observations (no new rows) | 5.69m touched rows | 0.30–0.40 GiB incremental |
| pair fact changes | about 40,320 initial facts | 20–40 MiB |
| metadata changes | up to about 80,640 initial source versions | 50–100 MiB |
| pair/feed boost changes + events | about 80,000 initial facts/events, change-dependent | 30–70 MiB |
| security snapshots | about 161,280 at four finite snapshots/token | 45–80 MiB |
| security raw API envelopes | about 1,613–2,880 batched calls | 30–80 MiB |
| boost raw API envelopes | 1,728 calls | 10–50 MiB, feed-size dependent |
| market context | 288 | negligible |

The total planning range is about 0.45–0.75 GiB/day hot PostgreSQL and roughly
0.08–0.20 GiB/day additional Parquet/Zstandard. These are engineering estimates, not
measurements; the first post-migration validation epoch must use storage telemetry and a
verified archive to replace them. Change deduplication lowers metadata/boost fact rows,
while raw request envelopes remain complete.

## Field disposition

| Data family | Field | Source | Mode | Extra request cost | Historical value | Backfillable? | Null semantics | Recommendation |
|---|---|---|---|---:|---|---|---|---|
| market | h6/h24 buys/sells | DEX pair | universal market cadence | 0 | high | raw-derived only, not as-of safe | absent != 0 | add future-only |
| market | liquidity base/quote | DEX pair | universal market cadence | 0 | high | raw-derived only, not as-of safe | absent != 0 | add future-only |
| pair facts | creation/composition/labels | DEX pair | event/change | 0 | high | possible derived backfill | null unknown | add future-only |
| boosts | active count | DEX pair | event/change | 0 | high | possible derived backfill | missing object != 0 | add future-only |
| boosts | amount/total | DEX latest/top | shared event feed | 1.2 DEX rpm | high | no | feed omission != 0 | add |
| metadata | name/symbol/URI/image/links | PumpPortal + DEX | event/change | 0 | medium-high | raw-derived only | null unknown | add future-only |
| security | authority/supply/decimals/program/extensions | Solana mint RPC | finite low-frequency | <=2 RPC rpm | high | no | unavailable distinct | add |
| security | Metaplex mutability | metadata account | deferred | additional RPC + PDA parsing | high | no | unknown distinct | defer |
| context | SOL ratio/return/volatility | existing observations | shared 5m | 0 | high | derivable | insufficient sample = null | add |
| context | admissions/activity/ACTIVE conversion | existing facts | shared 5m | 0 | high | derivable | no cohort = null | add |
| social | followers/engagement/sentiment | social providers | candidate/later | high/unstable | low-medium | no | provider-specific | reject Phase 2 |

## Retired-token interaction

Boost facts for a retired token are retained with the same immutable provenance as any
other tracked token, so a later, separately approved coverage policy can use a newly
received boost as deterministic wake-up evidence. Phase 2 deliberately does not change
coverage classes, lifecycle thresholds, or Phase 1 scheduler mathematics; consequently
it does not automatically wake a retired token.

## Verification result

The complete test suite passed with 143 tests, including parsing, missing-versus-zero,
as-of availability, four-writer deduplication, restart idempotency, finite security-task
reconstruction, context idempotency, feed cohort filtering, and raw-response retention.
Ruff, mypy (74 source files), `git diff --check`, and Alembic metadata parity all passed.
The Phase 2 migration completed upgrade/downgrade/upgrade against an isolated test
database; it was not applied to the live database.

The final read-only live check found Alembic head `f2c8d4a6197e`, Epoch 2
`completed` with `data_valid=true`, exactly 9,202,662 observations, no Epoch 3, a stopped
latest collector run, no matching collector OS process, and zero live collector advisory
locks. Epoch 1/2 facts were not rewritten or deleted.
