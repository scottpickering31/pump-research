# Collector V2 provider readiness

Status: acceptance audit on 2026-08-22. No provider was purchased or provisioned and no collector
was started.

## Matrix

| Data source | Configured path | Readiness | Acceptance evidence | Epoch 3 behavior |
|---|---|---|---|---|
| DEX Screener token/pair batches | public HTTPS, 240 application ceiling | LIVE-READY for existing market path | HTTP 200 in 0.30–0.36 s; valid empty arrays for two retired tokens | normal; empty remains explicit provider-empty |
| DEX Screener latest/top boosts | public HTTPS, 1.2 modeled requests/min | LIVE-READY | latest feed HTTP 200 in 0.081 s; 30 records, 28 Solana; numeric `amount`/`totalAmount` present | normal bounded feeds |
| PumpPortal discovery WebSocket | API key configured outside tracked files | PARTIAL | proven by Epoch 2, not re-authenticated in Phase 7; provider documents message charging/eligibility | require prestart reconnect acceptance; gaps remain explicit |
| Solana mint-account security | public mainnet RPC, <=2 requests/min | PARTIAL | `getMultipleAccounts` HTTP 200 in 0.336 s | basic mint facts may run; failures remain unavailable/failed |
| Standard-RPC top holders | same public RPC, top-20 only | UNAVAILABLE for validation acceptance | one `getTokenLargestAccounts` returned HTTP 429/error 429 in 0.374 s | queue/retry; never interpret as low concentration |
| Indexed trader distribution | none | UNCONFIGURED | no URL/key/adapter | unavailable/null evidence |
| Indexed creator history | none | UNCONFIGURED | no URL/key/adapter | unavailable/null evidence |
| Pool/liquidity decoder | none | UNCONFIGURED | no decoder/provider | unavailable/null evidence |
| Wallet/funding history | none | UNCONFIGURED | no URL/key/adapter | unavailable/null evidence |

DEX Screener documents up to 30 addresses per token request and a public 300 requests/minute route
limit; this application deliberately retains its lower 240 ceiling and 192 scheduled-safe budget:
<https://docs.dexscreener.com/api/reference>.

Solana explicitly says public RPC endpoints are rate-limited, limits may change, and public
endpoints are not intended for production applications:
<https://solana.com/docs/references/clusters> and <https://solana.com/rpc>.

PumpPortal documents its WebSocket API-key and account/funding conditions here:
<https://www.pumpportal.net/data-api/pump-swap>. Configuration must be reviewed against the
operator's account and current terms before Epoch 3.

## Recommended validation mode

Use **MODE A: standard-RPC/basic evidence only, advanced indexed evidence unavailable** for a short
validation, but only after a private/dedicated Solana RPC endpoint passes the acceptance suite.
Mode A is safer than wiring an untested indexer immediately before an integration epoch. It is not
a claim that advanced evidence exists.

The current runtime always instantiates `StandardSolanaHolderProvider`. Supplying
`PUMP_RESEARCH_SECURITY_INDEXER_URL` does not activate an indexer adapter; status reports
`UNAVAILABLE_ADAPTER_NOT_IMPLEMENTED`. MODE B therefore requires a separately approved provider
adapter phase and is outside Phase 7.

Advanced task results under MODE A use explicit `unavailable` envelopes:

- trader: `indexed_trader_history_not_configured`;
- creator: `creator_history_source_not_configured`;
- liquidity: `pool_program_decoder_not_configured`;
- wallet: `wallet_history_source_not_configured`;
- funding: `funding_history_source_not_configured`.

Unknown, unavailable, partial, failed, and zero remain distinct. Provider failure never becomes a
security-safe fact.

## Acceptance suite required before Epoch 3

For each configured endpoint, record provider name, endpoint product/tier, schema/adapter version,
account/project identity hash, acceptance time, and configuration hash without storing credentials.
Then verify:

1. authentication and read-only permissions;
2. 30 minutes at configured request rate plus 2x burst;
3. p50/p95/p99 latency, HTTP/RPC error and 429 behavior;
4. pagination cursor stability, duplicate-page handling, terminal page, and maximum-page guard;
5. known SPL and Token-2022 mints;
6. one active, one retired, one missing, and one malformed-account case;
7. documented versus observed completeness and historical range;
8. bounded retry respecting `Retry-After` and the shared provider limiter;
9. provider outage persisted as failed/unavailable evidence;
10. explicit monthly request/storage cost at normal, 2x, 5x, and 10x candidate rates.

The public Solana endpoint fails this gate for holder work. DEX and boost paths passed a basic
connectivity/schema check but still need the sustained prestart test from the target host/IP.

