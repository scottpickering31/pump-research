"""Deterministic first-generation market feature derivation."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from pump_research.research.asof import TokenStateAsOf
from pump_research.research.contracts import (
    LOOKBACK_TOLERANCE_SECONDS,
    FeatureSetContract,
    ObservationFact,
    SecurityFact,
    utc,
)

_HORIZON_NAMES = {5: "5s", 15: "15s", 30: "30s", 60: "1m", 120: "2m", 300: "5m", 900: "15m"}


@dataclass(frozen=True, slots=True)
class FeatureResult:
    values: dict[str, object]
    input_observation_ids: tuple[str, ...]
    availability_watermark: datetime | None


def build_market_features(
    state: TokenStateAsOf, contract: FeatureSetContract | None = None
) -> FeatureResult:
    feature_set = contract or FeatureSetContract()
    if state.feature_set_sha256 != feature_set.sha256:
        raise ValueError("token state was reconstructed with a different feature contract")
    current = state.current_observation
    values: dict[str, object] = {
        "epoch_number": state.epoch_number,
        "epoch_id": state.epoch_id,
        "token_id": state.token_id,
        "token_address": state.address,
        "decision_at": state.decision_at,
        "admission_at": state.admission_at,
        "token_age_seconds": _elapsed(state.admission_at, state.decision_at),
        "identity_known": state.identity_known,
        "lifecycle_state": state.lifecycle.new_state if state.lifecycle else None,
        "coverage_class": state.coverage.coverage_class if state.coverage else None,
        "selected_pair_id": state.selected_pair_id,
        "observation_received_at": current.received_at if current else None,
        "observation_age_seconds": _elapsed(current.received_at, state.decision_at)
        if current
        else None,
        "price_usd": _number(current.price_usd) if current else None,
        "price_native": _number(current.price_native) if current else None,
        "market_cap_usd": _number(current.market_cap_usd) if current else None,
        "fdv_usd": _number(current.fully_diluted_valuation_usd) if current else None,
        "liquidity_usd": _number(current.liquidity_usd) if current else None,
        "volume_m5_usd": _number(current.volume_m5_usd) if current else None,
        "volume_h1_usd": _number(current.volume_h1_usd) if current else None,
        "volume_h6_usd": _number(current.volume_h6_usd) if current else None,
        "volume_h24_usd": _number(current.volume_h24_usd) if current else None,
    }
    values["liquidity_market_cap_ratio"] = _ratio(
        current.liquidity_usd if current else None,
        current.market_cap_usd if current else None,
    )
    values["volume_m5_liquidity_ratio"] = _ratio(
        current.volume_m5_usd if current else None,
        current.liquidity_usd if current else None,
    )
    values["liquidity_volume_m5_ratio"] = _ratio(
        current.liquidity_usd if current else None,
        current.volume_m5_usd if current else None,
    )
    inputs: set[str] = {current.id} if current else set()
    baselines: dict[int, ObservationFact | None] = {}
    for horizon in feature_set.lookback_horizons_seconds:
        baseline = _lookback(
            state.observation_history,
            state.decision_at,
            horizon,
            LOOKBACK_TOLERANCE_SECONDS[horizon],
        )
        baselines[horizon] = baseline
        name = _HORIZON_NAMES[horizon]
        if current is not None and baseline is not None:
            inputs.add(baseline.id)
        values[f"return_{name}"] = _return(current, baseline, "price_usd")
        values[f"return_{name}_actual_seconds"] = (
            _elapsed(baseline.received_at, current.received_at)
            if current is not None and baseline is not None
            else None
        )
        values[f"liquidity_change_{name}"] = _return(current, baseline, "liquidity_usd")
    one_minute = values["return_1m"]
    one_minute_elapsed = values["return_1m_actual_seconds"]
    values["return_velocity_1m_per_second"] = _per_second(one_minute, one_minute_elapsed)
    prior_end = baselines[60]
    prior_start = (
        _lookback(
            state.observation_history,
            state.decision_at - timedelta(seconds=60),
            60,
            LOOKBACK_TOLERANCE_SECONDS[60],
        )
        if prior_end is not None
        else None
    )
    prior_return = _return(prior_end, prior_start, "price_usd")
    prior_elapsed = (
        _elapsed(prior_start.received_at, prior_end.received_at)
        if prior_end is not None and prior_start is not None
        else None
    )
    values["return_acceleration_1m"] = _difference(
        values["return_velocity_1m_per_second"], _per_second(prior_return, prior_elapsed)
    )
    for field_name, output_name in (
        ("market_cap_usd", "market_cap"),
        ("volume_m5_usd", "volume_m5"),
        ("liquidity_usd", "liquidity"),
    ):
        current_slope = _slope(current, baselines[60], field_name)
        prior_slope = _slope(prior_end, prior_start, field_name)
        values[f"{output_name}_velocity_1m"] = current_slope
        values[f"{output_name}_acceleration_1m"] = _difference(current_slope, prior_slope)
    _flow_features(values, current, baselines[60])
    _path_features(values, state.observation_history, state.decision_at)
    _boost_features(values, state)
    _security_features(values, state)
    _context_features(values, state)
    values["metadata_known"] = state.metadata is not None
    values["pair_created_at_known"] = (
        state.pair_fact is not None and state.pair_fact.pair_created_at is not None
    )
    return FeatureResult(
        values=values,
        input_observation_ids=tuple(sorted(inputs)),
        availability_watermark=state.availability_watermark,
    )


def _flow_features(
    values: dict[str, object], current: ObservationFact | None, baseline: ObservationFact | None
) -> None:
    for suffix, buys_name, sells_name in (
        ("m5", "buys_m5", "sells_m5"),
        ("h1", "buys_h1", "sells_h1"),
        ("h6", "buys_h6", "sells_h6"),
        ("h24", "buys_h24", "sells_h24"),
    ):
        buys = getattr(current, buys_name) if current else None
        sells = getattr(current, sells_name) if current else None
        values[f"buys_{suffix}"] = buys
        values[f"sells_{suffix}"] = sells
        values[f"buy_ratio_{suffix}"] = _count_ratio(buys, sells)
        values[f"buy_sell_imbalance_{suffix}"] = _imbalance(buys, sells)
    previous_imbalance = _imbalance(
        baseline.buys_m5 if baseline else None, baseline.sells_m5 if baseline else None
    )
    values["buy_sell_imbalance_m5_change_1m"] = _difference(
        values["buy_sell_imbalance_m5"], previous_imbalance
    )
    current_transactions = _sum_optional(current.buys_m5, current.sells_m5) if current else None
    prior_transactions = _sum_optional(baseline.buys_m5, baseline.sells_m5) if baseline else None
    values["transactions_m5"] = current_transactions
    values["transaction_acceleration_1m"] = _difference(current_transactions, prior_transactions)


def _path_features(
    values: dict[str, object], history: tuple[ObservationFact, ...], decision_at: datetime
) -> None:
    window = [
        item
        for item in history
        if utc(item.received_at) >= utc(decision_at) - timedelta(minutes=5)
        and item.price_usd is not None
        and item.price_usd > 0
    ]
    prices = [float(item.price_usd) for item in window if item.price_usd is not None]
    log_returns = [math.log(right / left) for left, right in zip(prices, prices[1:], strict=False)]
    values["path_observation_count_5m"] = len(prices)
    values["realized_volatility_5m"] = (
        statistics.pstdev(log_returns) if len(log_returns) >= 2 else None
    )
    current_price = prices[-1] if prices else None
    values["drawdown_from_local_peak_5m"] = (
        current_price / max(prices) - 1 if current_price is not None else None
    )
    values["recovery_from_local_low_5m"] = (
        current_price / min(prices) - 1 if current_price is not None else None
    )
    signs = [1 if value > 0 else -1 if value < 0 else 0 for value in log_returns]
    positive, negative = _consecutive(signs)
    values["consecutive_positive_observations"] = positive
    values["consecutive_negative_observations"] = negative
    nonzero = [sign for sign in signs if sign]
    values["path_monotonicity_5m"] = abs(sum(nonzero)) / len(nonzero) if nonzero else None
    rms = (
        math.sqrt(sum(value * value for value in log_returns) / len(log_returns))
        if log_returns
        else 0
    )
    values["path_smoothness_5m"] = (
        sum(abs(value) for value in log_returns) / len(log_returns) / rms if rms else None
    )
    liquidity_change = values.get("liquidity_change_1m")
    values["abrupt_liquidity_decline_1m"] = (
        liquidity_change < -0.5 if isinstance(liquidity_change, float) else None
    )


def _boost_features(values: dict[str, object], state: TokenStateAsOf) -> None:
    boost = state.boost
    values["boost_known"] = boost is not None
    values["boost_active"] = (
        boost.active_boost_count > 0
        if boost is not None and boost.active_boost_count is not None
        else None
    )
    values["boost_active_count"] = boost.active_boost_count if boost else None
    values["boost_amount"] = _number(boost.amount) if boost else None
    values["boost_total_amount"] = _number(boost.total_amount) if boost else None
    values["boost_freshness_seconds"] = (
        _elapsed(boost.received_at, state.decision_at) if boost else None
    )
    values["time_since_first_boost_seconds"] = (
        _elapsed(state.first_boost_received_at, state.decision_at)
        if state.first_boost_received_at
        else None
    )
    previous = state.previous_boost
    values["boost_amount_change"] = _difference(
        _number(boost.amount) if boost else None,
        _number(previous.amount) if previous else None,
    )
    values["boost_total_amount_change"] = _difference(
        _number(boost.total_amount) if boost else None,
        _number(previous.total_amount) if previous else None,
    )


def _security_features(values: dict[str, object], state: TokenStateAsOf) -> None:
    security = state.security
    values["security_known"] = security is not None
    values["security_status"] = security.status if security else "unknown"
    values["security_snapshot_freshness_seconds"] = (
        _elapsed(security.received_at, state.decision_at) if security else None
    )
    values["token_program"] = security.token_program if security else "unknown"
    values["mint_authority_state"] = _authority_state(security, "mint_authority")
    values["freeze_authority_state"] = _authority_state(security, "freeze_authority")


def _context_features(values: dict[str, object], state: TokenStateAsOf) -> None:
    context = state.context
    values["context_known"] = context is not None
    values["context_freshness_seconds"] = (
        _elapsed(context.received_at, state.decision_at) if context else None
    )
    values["sol_usd_price"] = _number(context.sol_usd_price) if context else None
    values["sol_return_5m"] = _number(context.sol_return_5m) if context else None
    values["sol_realized_volatility_1h"] = (
        _number(context.sol_realized_volatility_1h) if context else None
    )
    values["tracked_admission_rate_per_minute"] = (
        context.admitted_tokens
        / max(_elapsed(context.bucket_start, context.bucket_end) or 0, 1)
        * 60
        if context is not None and context.admitted_tokens is not None
        else None
    )
    values["recent_active_conversion_fraction"] = (
        _number(context.mature_cohort_active_fraction) if context else None
    )
    values["aggregate_tracked_volume_m5_usd"] = (
        _number(context.aggregate_volume_m5_usd) if context else None
    )
    values["aggregate_tracked_buy_ratio_m5"] = (
        _count_ratio(context.aggregate_buys_m5, context.aggregate_sells_m5) if context else None
    )


def _lookback(
    history: tuple[ObservationFact, ...],
    end_at: datetime,
    horizon_seconds: int,
    tolerance_seconds: int,
) -> ObservationFact | None:
    target = utc(end_at) - timedelta(seconds=horizon_seconds)
    candidates = [item for item in history if utc(item.received_at) <= target]
    if not candidates:
        return None
    candidate = max(candidates, key=lambda item: (utc(item.received_at), item.id))
    if (target - utc(candidate.received_at)).total_seconds() > tolerance_seconds:
        return None
    return candidate


def _return(
    current: ObservationFact | None, baseline: ObservationFact | None, field_name: str
) -> float | None:
    if current is None or baseline is None:
        return None
    current_value = getattr(current, field_name)
    baseline_value = getattr(baseline, field_name)
    if current_value is None or baseline_value is None or baseline_value <= 0:
        return None
    return float(current_value / baseline_value - 1)


def _slope(
    current: ObservationFact | None, baseline: ObservationFact | None, field_name: str
) -> float | None:
    if current is None or baseline is None:
        return None
    current_value = getattr(current, field_name)
    baseline_value = getattr(baseline, field_name)
    seconds = _elapsed(baseline.received_at, current.received_at)
    if current_value is None or baseline_value is None or seconds is None or seconds <= 0:
        return None
    return float(current_value - baseline_value) / seconds


def _authority_state(security: SecurityFact | None, field_name: str) -> str:
    if security is None or security.status != "available":
        return "unknown"
    return "present" if getattr(security, field_name) is not None else "none"


def _number(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _ratio(numerator: Decimal | None, denominator: Decimal | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return float(numerator / denominator)


def _count_ratio(buys: int | None, sells: int | None) -> float | None:
    if buys is None or sells is None or buys + sells == 0:
        return None
    return buys / (buys + sells)


def _imbalance(buys: int | None, sells: int | None) -> float | None:
    if buys is None or sells is None or buys + sells == 0:
        return None
    return (buys - sells) / (buys + sells)


def _sum_optional(left: int | None, right: int | None) -> int | None:
    return left + right if left is not None and right is not None else None


def _elapsed(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return (utc(end) - utc(start)).total_seconds()


def _per_second(value: object, seconds: object) -> float | None:
    if not isinstance(value, (int, float)) or not isinstance(seconds, (int, float)) or seconds <= 0:
        return None
    return float(value) / float(seconds)


def _difference(left: object, right: object) -> float | None:
    if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
        return None
    return float(left) - float(right)


def _consecutive(signs: list[int]) -> tuple[int, int]:
    if not signs:
        return 0, 0
    final = signs[-1]
    count = 0
    for sign in reversed(signs):
        if sign != final or sign == 0:
            break
        count += 1
    return (count if final > 0 else 0, count if final < 0 else 0)
