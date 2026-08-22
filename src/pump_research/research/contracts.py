"""Provider-neutral, versioned research contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal

TriState = Literal["TRUE", "FALSE", "UNKNOWN"]

LOOKBACK_HORIZONS_SECONDS: tuple[int, ...] = (5, 15, 30, 60, 120, 300, 900)
LOOKBACK_TOLERANCE_SECONDS: dict[int, int] = {
    5: 5,
    15: 10,
    30: 15,
    60: 30,
    120: 60,
    300: 120,
    900: 300,
}
LABEL_HORIZONS_SECONDS: tuple[int, ...] = (30, 60, 300, 900, 3600, 21600, 86400, 604800)
LABEL_FORWARD_TOLERANCE_SECONDS: dict[int, int] = {
    30: 15,
    60: 30,
    300: 120,
    900: 300,
    3600: 900,
    21600: 3600,
    86400: 10800,
    604800: 43200,
}


def utc(value: datetime, field_name: str = "timestamp") -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def canonical_digest(value: object) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=_json_default
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return utc(value).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"cannot canonicalize {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class DiscoveryFact:
    id: str
    received_at: datetime
    source_event_at: datetime | None = None
    event_type: str = "discovered"


@dataclass(frozen=True, slots=True)
class ObservationFact:
    id: str
    pair_id: str
    pair_address: str
    received_at: datetime
    source_observed_at: datetime | None = None
    price_usd: Decimal | None = None
    price_native: Decimal | None = None
    liquidity_usd: Decimal | None = None
    market_cap_usd: Decimal | None = None
    fully_diluted_valuation_usd: Decimal | None = None
    volume_m5_usd: Decimal | None = None
    volume_h1_usd: Decimal | None = None
    volume_h6_usd: Decimal | None = None
    volume_h24_usd: Decimal | None = None
    buys_m5: int | None = None
    sells_m5: int | None = None
    buys_h1: int | None = None
    sells_h1: int | None = None
    buys_h6: int | None = None
    sells_h6: int | None = None
    buys_h24: int | None = None
    sells_h24: int | None = None
    liquidity_base: Decimal | None = None
    liquidity_quote: Decimal | None = None


@dataclass(frozen=True, slots=True)
class LifecycleFact:
    id: str
    decided_at: datetime
    input_watermark: datetime
    previous_state: str | None
    new_state: str
    reason_code: str


@dataclass(frozen=True, slots=True)
class PairFact:
    id: str
    pair_id: str
    received_at: datetime
    pair_created_at: datetime | None = None
    dex_identifier: str | None = None
    base_token_address: str | None = None
    quote_token_address: str | None = None


@dataclass(frozen=True, slots=True)
class BoostFact:
    id: str
    received_at: datetime
    source_observed_at: datetime | None = None
    active_boost_count: int | None = None
    amount: Decimal | None = None
    total_amount: Decimal | None = None


@dataclass(frozen=True, slots=True)
class MetadataFact:
    id: str
    received_at: datetime
    source_observed_at: datetime | None = None
    name: str | None = None
    symbol: str | None = None
    metadata_uri: str | None = None
    image_url: str | None = None
    website_url: str | None = None
    twitter: str | None = None
    telegram: str | None = None


@dataclass(frozen=True, slots=True)
class SecurityFact:
    id: str
    received_at: datetime
    source_observed_at: datetime | None = None
    status: str = "unavailable"
    token_program: str = "unknown"
    mint_authority: str | None = None
    freeze_authority: str | None = None
    raw_supply: Decimal | None = None
    decimals: int | None = None
    extension_types: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class MarketContextFact:
    id: str
    bucket_start: datetime
    bucket_end: datetime
    received_at: datetime
    sol_usd_price: Decimal | None = None
    sol_return_5m: Decimal | None = None
    sol_realized_volatility_1h: Decimal | None = None
    admitted_tokens: int | None = None
    mature_cohort_active_fraction: Decimal | None = None
    pair_sample_count: int | None = None
    aggregate_volume_m5_usd: Decimal | None = None
    aggregate_buys_m5: int | None = None
    aggregate_sells_m5: int | None = None


@dataclass(frozen=True, slots=True)
class CoverageFact:
    id: str
    decided_at: datetime
    effective_at: datetime
    coverage_class: str
    lifecycle_state: str


@dataclass(frozen=True, slots=True)
class CandidateFact:
    id: str
    candidate_at: datetime
    input_watermark: datetime
    trigger_type: str
    evidence_sha256: str


@dataclass(frozen=True, slots=True)
class CandidateTierFact:
    id: str
    decided_at: datetime
    input_watermark: datetime
    previous_tier: str
    new_tier: str
    reason_code: str


@dataclass(frozen=True, slots=True)
class SelectiveSecurityFact:
    """One Phase 6 fact with explicit availability and reconstruction semantics."""

    id: str
    family: str
    received_at: datetime
    acquisition_mode: str
    availability: str | None = None
    completeness: str | None = None
    values: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TokenHistory:
    epoch_id: str
    epoch_number: int
    epoch_data_valid: bool
    token_id: str
    chain: str
    address: str
    discoveries: tuple[DiscoveryFact, ...] = ()
    observations: tuple[ObservationFact, ...] = ()
    lifecycle: tuple[LifecycleFact, ...] = ()
    pair_facts: tuple[PairFact, ...] = ()
    boosts: tuple[BoostFact, ...] = ()
    metadata: tuple[MetadataFact, ...] = ()
    security: tuple[SecurityFact, ...] = ()
    context: tuple[MarketContextFact, ...] = ()
    coverage: tuple[CoverageFact, ...] = ()
    candidates: tuple[CandidateFact, ...] = ()
    candidate_tiers: tuple[CandidateTierFact, ...] = ()
    selective_security: tuple[SelectiveSecurityFact, ...] = ()
    source_descriptor: dict[str, object] = field(default_factory=dict)

    @property
    def admission_at(self) -> datetime | None:
        candidates = [
            utc(item.decided_at)
            for item in self.lifecycle
            if item.new_state == "NEW" and item.input_watermark <= item.decided_at
        ]
        return min(candidates) if candidates else None


@dataclass(frozen=True, slots=True)
class FeatureSetContract:
    name: str = "market"
    version: str = "1"
    required_inputs: tuple[str, ...] = ("observations", "lifecycle_events")
    optional_inputs: tuple[str, ...] = (
        "pair_fact_events",
        "boost_observations",
        "token_metadata_events",
        "token_security_snapshots",
        "market_context_snapshots",
    )
    availability_rule: str = "availability_at <= decision_at; no forward interpolation"
    lookback_horizons_seconds: tuple[int, ...] = LOOKBACK_HORIZONS_SECONDS
    lookback_tolerance_seconds: tuple[tuple[int, int], ...] = tuple(
        LOOKBACK_TOLERANCE_SECONDS.items()
    )
    derivation_revision: str = "market-v1.0.0"

    @property
    def identifier(self) -> str:
        return f"{self.name}-v{self.version}"

    @property
    def sha256(self) -> str:
        return canonical_digest(asdict(self))


@dataclass(frozen=True, slots=True)
class LabelSetContract:
    name: str = "outcomes"
    version: str = "1"
    horizons_seconds: tuple[int, ...] = LABEL_HORIZONS_SECONDS
    forward_tolerance_seconds: tuple[tuple[int, int], ...] = tuple(
        LABEL_FORWARD_TOLERANCE_SECONDS.items()
    )
    proxy_notionals_usd: tuple[int, ...] = (100, 1000, 5000)
    derivation_revision: str = "outcomes-v1.1.0"
    execution_adjusted_return_available: bool = False

    @property
    def identifier(self) -> str:
        return f"{self.name}-v{self.version}"

    @property
    def sha256(self) -> str:
        return canonical_digest(asdict(self))


@dataclass(frozen=True, slots=True)
class CandidateTimestampPolicy:
    mode: Literal["fixed_age", "observation", "lifecycle"] = "fixed_age"
    offsets_seconds: tuple[int, ...] = (30, 60, 120, 300, 600, 1800, 3600)
    lifecycle_states: tuple[str, ...] = ("ACTIVE", "FADING", "RESURRECTED")
    version: str = "1"

    @property
    def sha256(self) -> str:
        return canonical_digest(asdict(self))


@dataclass(frozen=True, slots=True)
class SplitWindow:
    name: Literal["train", "validation", "test"]
    start_at: datetime
    end_at: datetime
    label_end_at: datetime

    def __post_init__(self) -> None:
        start = utc(self.start_at, f"{self.name}.start_at")
        end = utc(self.end_at, f"{self.name}.end_at")
        label_end = utc(self.label_end_at, f"{self.name}.label_end_at")
        if not start < end <= label_end:
            raise ValueError("split requires start_at < end_at <= label_end_at")


@dataclass(frozen=True, slots=True)
class ChronologicalSplitContract:
    windows: tuple[SplitWindow, ...]
    token_exclusive_by_admission: bool = True
    maximum_label_horizon_seconds: int = max(LABEL_HORIZONS_SECONDS)
    version: str = "1"

    def __post_init__(self) -> None:
        if tuple(window.name for window in self.windows) != (
            "train",
            "validation",
            "test",
        ):
            raise ValueError("canonical split windows must be train, validation, test")
        for previous, current in zip(self.windows, self.windows[1:], strict=False):
            if previous.end_at > current.start_at:
                raise ValueError("chronological split windows cannot overlap")

    @property
    def sha256(self) -> str:
        return canonical_digest(asdict(self))

    def assign(self, admission_at: datetime, decision_at: datetime) -> str | None:
        admission = utc(admission_at)
        decision = utc(decision_at)
        for window in self.windows:
            assignment_time = admission if self.token_exclusive_by_admission else decision
            if window.start_at <= assignment_time < window.end_at:
                if not window.start_at <= decision < window.end_at:
                    return None
                if decision + timedelta(seconds=self.maximum_label_horizon_seconds) > (
                    window.label_end_at
                ):
                    return None
                return window.name
        return None
