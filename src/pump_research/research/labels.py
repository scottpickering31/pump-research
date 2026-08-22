"""Future-only outcome labels, kept separate from as-of feature derivation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from pump_research.research.asof import TokenStateAsOf
from pump_research.research.contracts import (
    LABEL_FORWARD_TOLERANCE_SECONDS,
    LabelSetContract,
    LifecycleFact,
    ObservationFact,
    TokenHistory,
    TriState,
    utc,
)

_HORIZON_NAMES = {
    30: "30s",
    60: "1m",
    300: "5m",
    900: "15m",
    3600: "1h",
    21600: "6h",
    86400: "24h",
    604800: "7d",
}
_UPSIDE = {
    "plus_25pct": Decimal("1.25"),
    "plus_50pct": Decimal("1.5"),
    "plus_100pct": Decimal("2"),
    "plus_200pct": Decimal("3"),
    "plus_500pct": Decimal("6"),
    "2x": Decimal("2"),
    "5x": Decimal("5"),
    "10x": Decimal("10"),
    "50x": Decimal("50"),
}
_DOWNSIDE = {
    "minus_20pct": Decimal("0.8"),
    "minus_30pct": Decimal("0.7"),
    "minus_50pct": Decimal("0.5"),
    "minus_80pct": Decimal("0.2"),
    "minus_95pct": Decimal("0.05"),
}


@dataclass(frozen=True, slots=True)
class LabelResult:
    values: dict[str, object]
    future_observation_ids: tuple[str, ...]
    maximum_future_at: datetime | None


def build_outcome_labels(
    history: TokenHistory,
    state: TokenStateAsOf,
    contract: LabelSetContract | None = None,
) -> LabelResult:
    label_set = contract or LabelSetContract()
    current = state.current_observation
    values: dict[str, object] = {
        "label_set": label_set.identifier,
        "label_set_sha256": label_set.sha256,
        "theoretical_entry_price_usd": _float(current.price_usd) if current else None,
        "entry_liquidity_usd": _float(current.liquidity_usd) if current else None,
        "execution_adjusted_return": None,
        "execution_adjusted_return_availability": "UNAVAILABLE_NO_HISTORICAL_QUOTES",
    }
    for notional in label_set.proxy_notionals_usd:
        values[f"entry_notional_{notional}_liquidity_ratio"] = _notional_ratio(
            notional, current.liquidity_usd if current else None
        )
    future = tuple(
        sorted(
            (
                item
                for item in history.observations
                if item.pair_id == state.selected_pair_id
                and utc(item.received_at) > state.decision_at
            ),
            key=lambda item: (utc(item.received_at), item.id),
        )
    )
    used: set[str] = set()
    horizon_paths: dict[int, tuple[ObservationFact, ...]] = {}
    horizon_complete: dict[int, bool] = {}
    for horizon in label_set.horizons_seconds:
        name = _HORIZON_NAMES[horizon]
        endpoint = _endpoint(future, state.decision_at, horizon)
        path_limit = state.decision_at + timedelta(
            seconds=horizon + LABEL_FORWARD_TOLERANCE_SECONDS[horizon]
        )
        # Even without a sufficiently near endpoint, a crossing that was actually
        # observed is known TRUE. Non-crossings and extrema remain UNKNOWN because
        # incomplete future coverage cannot prove they never happened.
        path = tuple(item for item in future if utc(item.received_at) <= path_limit)
        complete = endpoint is not None and _path_complete(current, path, horizon)
        horizon_paths[horizon] = path
        horizon_complete[horizon] = complete
        if endpoint is not None:
            used.add(endpoint.id)
        used.update(item.id for item in path)
        values[f"horizon_{name}_available"] = endpoint is not None
        values[f"horizon_{name}_path_complete"] = complete
        values[f"horizon_{name}_actual_seconds"] = (
            (utc(endpoint.received_at) - state.decision_at).total_seconds()
            if endpoint is not None
            else None
        )
        values[f"theoretical_market_return_{name}"] = _price_return(current, endpoint)
        values[f"exit_liquidity_usd_{name}"] = (
            _float(endpoint.liquidity_usd) if endpoint is not None else None
        )
        for notional in label_set.proxy_notionals_usd:
            values[f"exit_notional_{notional}_liquidity_ratio_{name}"] = _notional_ratio(
                notional, endpoint.liquidity_usd if endpoint else None
            )
        _path_labels(values, name, current, path, complete)
        values[f"future_lifecycle_state_{name}"] = _lifecycle_at(
            history.lifecycle, utc(endpoint.received_at) if endpoint is not None else None
        )
    available_horizons = [horizon for horizon, path in horizon_paths.items() if path]
    maximum_horizon = (
        max(available_horizons) if available_horizons else max(label_set.horizons_seconds)
    )
    _ordered_and_timing_labels(
        values,
        current,
        horizon_paths[maximum_horizon],
        horizon_complete[maximum_horizon],
        state.decision_at,
    )
    return LabelResult(
        values=values,
        future_observation_ids=tuple(sorted(used)),
        maximum_future_at=(max((item.received_at for item in future), default=None)),
    )


def _path_labels(
    values: dict[str, object],
    name: str,
    current: ObservationFact | None,
    path: tuple[ObservationFact, ...],
    complete: bool,
) -> None:
    base = current.price_usd if current else None
    returns: list[float] = []
    if base is not None and base > 0:
        for item in path:
            if item.price_usd is not None:
                returns.append(float(item.price_usd / base - 1))
    values[f"maximum_favorable_excursion_{name}"] = max(returns) if returns and complete else None
    values[f"maximum_adverse_excursion_{name}"] = min(returns) if returns and complete else None
    for label, multiplier in _UPSIDE.items():
        crossed = bool(
            base and any(item.price_usd and item.price_usd >= base * multiplier for item in path)
        )
        values[f"crossed_{label}_{name}"] = _tri(crossed, complete)
    for label, multiplier in _DOWNSIDE.items():
        crossed = bool(
            base and any(item.price_usd and item.price_usd <= base * multiplier for item in path)
        )
        values[f"crossed_{label}_{name}"] = _tri(crossed, complete)
    liquidity = [item.liquidity_usd for item in path if item.liquidity_usd is not None]
    values[f"minimum_future_liquidity_usd_{name}"] = (
        _float(min(liquidity)) if liquidity and complete else None
    )
    entry_liquidity = current.liquidity_usd if current else None
    collapsed = bool(
        entry_liquidity and any(value <= entry_liquidity * Decimal("0.2") for value in liquidity)
    )
    values[f"major_liquidity_collapse_{name}"] = _tri(collapsed, complete)
    endpoint_liquidity = path[-1].liquidity_usd if path else None
    survived = bool(
        entry_liquidity
        and endpoint_liquidity is not None
        and endpoint_liquidity >= entry_liquidity * Decimal("0.5")
    )
    values[f"liquidity_survival_{name}"] = _tri(survived, complete)
    discontinuities = [
        abs(float(right.price_usd / left.price_usd - 1))
        for left, right in zip(path, path[1:], strict=False)
        if left.price_usd and right.price_usd and left.price_usd > 0
    ]
    values[f"maximum_observed_price_discontinuity_{name}"] = (
        max(discontinuities) if discontinuities and complete else None
    )
    values[f"extreme_gap_or_rug_proxy_{name}"] = _tri(
        bool(discontinuities and max(discontinuities) >= 0.95), complete
    )


def _ordered_and_timing_labels(
    values: dict[str, object],
    current: ObservationFact | None,
    path: tuple[ObservationFact, ...],
    complete: bool,
    decision_at: datetime,
) -> None:
    base = current.price_usd if current else None
    pairs = {
        "plus_50_before_minus_25": (Decimal("1.5"), Decimal("0.75")),
        "plus_100_before_minus_30": (Decimal("2"), Decimal("0.7")),
        "plus_200_before_minus_40": (Decimal("3"), Decimal("0.6")),
        "5x_before_minus_50": (Decimal("5"), Decimal("0.5")),
    }
    for name, (up, down) in pairs.items():
        up_at = _first_cross(path, base, up, upward=True)
        down_at = _first_cross(path, base, down, upward=False)
        if up_at is not None:
            values[name] = (
                "TRUE"
                if down_at is None or utc(up_at.received_at) < utc(down_at.received_at)
                else "FALSE"
            )
        elif down_at is not None:
            values[name] = "FALSE"
        else:
            values[name] = "FALSE" if complete else "UNKNOWN"
    for name, multiplier, upward in (
        ("time_to_plus_50_seconds", Decimal("1.5"), True),
        ("time_to_2x_seconds", Decimal("2"), True),
        ("time_to_5x_seconds", Decimal("5"), True),
        ("time_to_minus_50_seconds", Decimal("0.5"), False),
        ("time_to_minus_80_seconds", Decimal("0.2"), False),
    ):
        crossing = _first_cross(path, base, multiplier, upward=upward)
        values[name] = (
            (utc(crossing.received_at) - decision_at).total_seconds()
            if crossing is not None
            else None
        )
    priced = [item for item in path if item.price_usd is not None]
    peak = max(priced, key=lambda item: item.price_usd or Decimal(0), default=None)
    values["time_to_peak_seconds"] = (
        (utc(peak.received_at) - decision_at).total_seconds() if peak and complete else None
    )
    collapse = _first_cross(path, base, Decimal("0.2"), upward=False)
    values["time_to_collapse_seconds"] = (
        (utc(collapse.received_at) - decision_at).total_seconds() if collapse else None
    )


def _endpoint(
    future: tuple[ObservationFact, ...], decision_at: datetime, horizon: int
) -> ObservationFact | None:
    target = utc(decision_at) + timedelta(seconds=horizon)
    limit = target + timedelta(seconds=LABEL_FORWARD_TOLERANCE_SECONDS[horizon])
    return next(
        (item for item in future if target <= utc(item.received_at) <= limit),
        None,
    )


def _path_complete(
    current: ObservationFact | None, path: tuple[ObservationFact, ...], horizon: int
) -> bool:
    if current is None or not path:
        return False
    times = [utc(current.received_at), *(utc(item.received_at) for item in path)]
    maximum_gap = max(
        (right - left).total_seconds() for left, right in zip(times, times[1:], strict=False)
    )
    allowed_gap = min(max(60.0, horizon * 0.1), 1800.0)
    return maximum_gap <= allowed_gap


def _first_cross(
    path: tuple[ObservationFact, ...],
    base: Decimal | None,
    multiplier: Decimal,
    *,
    upward: bool,
) -> ObservationFact | None:
    if base is None or base <= 0:
        return None
    threshold = base * multiplier
    return next(
        (
            item
            for item in path
            if item.price_usd is not None
            and (item.price_usd >= threshold if upward else item.price_usd <= threshold)
        ),
        None,
    )


def _lifecycle_at(items: tuple[LifecycleFact, ...], timestamp: datetime | None) -> str | None:
    if timestamp is None:
        return None
    candidates = [
        item
        for item in items
        if utc(item.decided_at) <= timestamp and utc(item.input_watermark) <= utc(item.decided_at)
    ]
    return (
        max(candidates, key=lambda item: (utc(item.decided_at), item.id)).new_state
        if candidates
        else None
    )


def _price_return(
    current: ObservationFact | None, endpoint: ObservationFact | None
) -> float | None:
    if (
        current is None
        or endpoint is None
        or current.price_usd is None
        or endpoint.price_usd is None
        or current.price_usd <= 0
    ):
        return None
    return float(endpoint.price_usd / current.price_usd - 1)


def _tri(value: bool, complete: bool) -> TriState:
    if value:
        return "TRUE"
    return "FALSE" if complete else "UNKNOWN"


def _float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _notional_ratio(notional: int, liquidity: Decimal | None) -> float | None:
    if liquidity is None or liquidity <= 0:
        return None
    return float(Decimal(notional) / liquidity)
