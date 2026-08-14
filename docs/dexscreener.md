# DEX Screener token-pairs client

The implementation follows DEX Screener's official [API reference](https://docs.dexscreener.com/api/reference), checked on 2026-08-14. The applicable public endpoint is `GET /tokens/v1/{chainId}/{tokenAddresses}`. Its documented contract accepts up to 30 comma-separated token addresses and has a 300-requests-per-minute limit.

## Client policy

- The client sends at most 30 unique, non-empty addresses in one eligible batch request.
- It defaults to 240 requests/minute, retaining 20% safety headroom beneath the documented public limit.
- All default clients in one process share the same async rate limiter; all request attempts, including retries, pass through it. A future multi-process scheduler must coordinate this provider budget durably.
- Retryable failures are transport errors, HTTP 429, and HTTP 5xx. `Retry-After` is honored when supplied; other retries use bounded exponential backoff.
- Other HTTP failures and typed-parse failures raise explicit errors. No failed batch is silently returned as an empty result.
- Returned results contain both typed pair records and the raw response list. Persistence is intentionally deferred to the later collection orchestration phase.

## Metrics

`DexScreenerMetrics` exposes in-process counters for batches/addresses, outbound HTTP requests, success/failure/throttle counts, retries, parser failures, pairs returned, limiter wait time, and request latency. These are designed for later integration with durable operational metrics; they are not a substitute for `api_request_log` persistence.

## Test contract

Unit tests use `httpx.MockTransport`, so no live API data is fetched. The primary batching test passes 30 distinct Solana-shaped addresses and asserts exactly one eligible HTTP request to `/tokens/v1/solana/{comma-separated-addresses}`. A companion test asserts that 31 addresses yield two requests.
