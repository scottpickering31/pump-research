# Research feature contract: market-v1

`market-v1` (`market-v1.0.0` derivation revision) is a small deterministic feature family,
not a model or score. The immutable
contract contains its schema, availability rules, input families, horizon tolerances,
derivation revision, and SHA-256 digest.

## Inputs and missingness

Inputs are valid-epoch token admission, observations, lifecycle events, pair facts,
boosts, metadata, security snapshots, and shared context. Every input must satisfy the
Phase 4 availability rule. Null means unknown/unavailable; it is never converted to zero
or false. Ratios are null for null or zero denominators. Numeric calculations use source
decimal values and emit finite derived doubles plus explicit actual-elapsed/freshness
columns.

## Feature groups

- Identity/time: epoch, token, decision time, admission time, age, selected pair, current
  observation receipt/age, lifecycle state.
- Market: price, native price, market cap, FDV, liquidity, rolling provider volumes,
  liquidity/market-cap and volume/liquidity ratios.
- Returns: 5s, 15s, 30s, 1m, 2m, 5m, and 15m with actual elapsed seconds.
- Dynamics: one-minute return velocity and acceleration; market-cap, volume, and liquidity
  slope/acceleration using available historical points.
- Flow: m5/h1/h6/h24 buy ratios and imbalance, m5 imbalance change, and transaction-count
  acceleration.
- Path: five-minute realized log-return volatility, recent drawdown/recovery, consecutive
  positive/negative steps, monotonicity, and smoothness.
- Liquidity: changes over 1m/5m/15m, liquidity/volume, relative decline and an explicit
  recent abrupt-decline indicator.
- Boost: knowledge state, active state/count, amount, total amount, first-seen age, latest
  change, and freshness. Absence of any observation remains unknown.
- Security: snapshot status/freshness, token program, and three-state mint/freeze authority
  facts (`present`, `none`, `unknown`).
- Context: snapshot freshness, SOL price/returns/volatility, launch rate, ACTIVE conversion
  fraction, and aggregate tracked flow/activity.

Pair metadata and display metadata are returned by the as-of state API but are not turned
into speculative alpha features in v1.

## Path formulas

Returns are `current / baseline - 1` for positive prices. Realized volatility is the
population standard deviation of consecutive log returns. Drawdown is current/maximum
recent price minus one; recovery is current/minimum recent price minus one. Monotonicity
is the absolute signed-step sum divided by non-zero step count. Smoothness is mean
absolute log return divided by root-mean-square log return; it is descriptive and has no
token-specific threshold.

A combined v1 research artifact currently has 377 identity, provenance, feature, label,
and diagnostic columns. A feature addition or formula change requires a new derivation
revision and contract digest; existing dataset identities are never reinterpreted.
