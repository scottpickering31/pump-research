# Security feature contract v1

`security-v1` version `1.0.0` is a transparent, non-trading research feature set.
Each immutable snapshot records generated/received time, acquisition mode,
candidate/epoch provenance, policy hash, exact input fact IDs and hashes, schema
hash, and nullable values. Availability requires `received_at <= T`;
retrospective reconstruction is excluded by default.

| Feature | Definition | Missingness |
|---|---|---|
| `holder_top10_pct` | ten largest owner-aggregated balances / supply, percent | null without valid supply/holder evidence |
| `holder_hhi` | squared wallet supply shares | null unless full distribution |
| `creator_hold_pct` | evidenced creator balances / supply | null without creator link/balance |
| `creator_prior_collapse_rate` | known prior collapses / tracked prior launches | null without denominator |
| `unique_trader_count` | distinct decoded wallets in the closed window | null when required wallet decoding is incomplete |
| `trades_per_unique_trader` | trades / unique traders | null without positive unique count |
| `top10_trader_volume_share` | top-ten wallet notional / known notional | null without decoded notionals |
| `repeat_trade_ratio` | trades from repeat wallets / decoded trades | null without decoded wallets |
| `wallet_cluster_count` | connected components for the input edge set | null if graph unavailable |
| `largest_cluster_trade_share` | cluster volume / decoded volume | reserved/null pending attribution contract |
| `common_funder_share` | wallets sharing most common known funder / funded wallets | null without funding facts |
| `synchronized_trade_score` | recent co-trade evidence strength in prior 15m | null without timing edges |
| `liquidity_removal_recent_pct` | largest evidenced removal in prior 15m | null without events |
| `liquidity_change_velocity` | normalized liquidity delta/time | reserved/null pending decoder |
| `creator_transfer_activity` | evidenced recent creator transfers | null without applicable facts |
| `security_snapshot_age_seconds` | T minus Phase 2 security receipt | null without snapshot |

Percentages use decimal arithmetic on 0–100; HHI is 0–1. Provider failure,
unconfigured source, partial pagination and true empty result are distinct.
Security-v1 is structural evidence. It is not a classifier, risk score,
opportunity score, BUY/SELL output or executable-P&L claim.

Synthetic regressions cover 700-wallet distributed activity, 15-wallet/common-
funder coordination, three-wallet repeated volume, creator concentration with
liquidity removal, and organic high-volume activity. They compare structure only
and never inspect future outcomes.
