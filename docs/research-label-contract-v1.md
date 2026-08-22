# Research label contract: outcomes-v1

Labels use the `outcomes-v1.1.0` derivation revision and are computed in a separate pass
after features are frozen. They may inspect
observations after decision time `T`; no label field is an input to `market-v1`.

## Horizons and endpoint coverage

V1 defines 30s, 1m, 5m, 15m, 1h, 6h, 24h, and 7d horizons. The endpoint is the first
same-pair observation at or after `T+horizon` within a versioned forward tolerance. The
actual elapsed duration is recorded. Missing endpoint coverage produces null numeric
labels and `UNKNOWN` categorical labels, not false.

Path-dependent false labels require a complete-enough observed path. A true crossing is
known when observed; false is emitted only when the endpoint exists and maximum internal
gap stays within the versioned path-gap allowance. Otherwise it is `UNKNOWN`.

An observed threshold crossing remains `TRUE` even if the requested horizon endpoint is
missing: the event itself is known. Endpoint returns, extrema, and unobserved crossings
remain null/`UNKNOWN`. This records Qenis' observed collapse without claiming complete
6h endpoint coverage.

## Labels

Per horizon:

- theoretical market return, maximum favorable/adverse observed excursion;
- `+25/+50/+100/+200/+500%`, `2x/5x/10x/50x` and
  `-20/-30/-50/-80/-95%` crossings as `TRUE/FALSE/UNKNOWN`;
- minimum future liquidity, major relative liquidity collapse, relative liquidity
  survival, endpoint lifecycle state, and observed continuity diagnostics;
- liquidity at `T` and exit, price discontinuity/rug proxies, while explicitly leaving
  execution-adjusted P&L unavailable.

Across the longest requested horizon:

- `+50% before -25%`, `+100% before -30%`, `+200% before -40%`, and `5x before -50%`;
- time to +50%, 2x, 5x, -50%, and -80%; time to observed peak and collapse.

Theoretical chart return is named `theoretical_market_return`. The future
`execution_adjusted_return` remains null with an availability reason until historical
route, impact, fee, priority-fee, latency, and fill evidence exists.

Conservative executability proxies include entry and available-exit
`notional / liquidity_usd` ratios for fixed $100, $1,000, and $5,000 reference notionals,
plus observed discontinuity and liquidity-collapse facts. These are diagnostic ratios,
not price-impact estimates or executable P&L.

## Split boundaries

Labels never cross a split's allowed label boundary. Rows whose selected maximum horizon
extends past that boundary are purged from supervised materialization (or retained only
as explicitly unlabeled inspection rows). Invalid epochs never supply labels to valid
epochs.

The builder rejects a split contract whose purge horizon is shorter than the label set's
maximum 7-day horizon. The default purge horizon is therefore 604,800 seconds.
