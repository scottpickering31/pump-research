"""Provider-neutral Phase 6 evidence contracts with explicit incompleteness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum


class EvidenceAvailability(StrEnum):
    AVAILABLE = "available"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"


class EvidenceCompleteness(StrEnum):
    FULL_DISTRIBUTION = "full_distribution"
    TOP_20_TOKEN_ACCOUNTS = "top_20_token_accounts"
    CLOSED_TIME_RANGE = "closed_time_range"
    PARTIAL_PAGINATION = "partial_pagination"
    BOUNDED_GRAPH = "bounded_graph"
    UNKNOWN = "unknown"


class AcquisitionMode(StrEnum):
    HISTORICALLY_AVAILABLE = "historically_available"
    RETROSPECTIVELY_RECONSTRUCTED = "retrospectively_reconstructed"


class TradeSide(StrEnum):
    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class WalletRelationshipType(StrEnum):
    COMMON_FUNDER = "COMMON_FUNDER"
    DIRECT_TRANSFER = "DIRECT_TRANSFER"
    CO_TRADE_TIMING = "CO_TRADE_TIMING"
    REPEATED_SIZE_PATTERN = "REPEATED_SIZE_PATTERN"
    CREATOR_LINK = "CREATOR_LINK"
    LP_LINK = "LP_LINK"
    SHARED_RECENT_FUNDING = "SHARED_RECENT_FUNDING"


class LiquidityEventType(StrEnum):
    ADD = "LIQUIDITY_ADD"
    REMOVE = "LIQUIDITY_REMOVE"
    RESERVE_DISCONTINUITY = "RESERVE_DISCONTINUITY"
    LP_MINT = "LP_MINT"
    LP_BURN = "LP_BURN"
    LP_TRANSFER = "LP_TRANSFER"
    POOL_MIGRATION = "POOL_MIGRATION"


def utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class EvidenceEnvelope:
    provider: str
    provider_schema_version: str
    source_observed_at: datetime | None
    received_at: datetime
    availability: EvidenceAvailability
    completeness: EvidenceCompleteness
    acquisition_mode: AcquisitionMode
    source_slot: int | None = None
    page_cursor: str | None = None
    next_cursor: str | None = None
    http_status_code: int | None = None
    failure_code: str | None = None
    raw_payload: object | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "received_at", utc(self.received_at, "received_at"))
        if self.source_observed_at is not None:
            object.__setattr__(
                self,
                "source_observed_at",
                utc(self.source_observed_at, "source_observed_at"),
            )
        if self.availability is EvidenceAvailability.FAILED and self.failure_code is None:
            raise ValueError("failed evidence requires failure_code")
        if self.http_status_code is not None and not 100 <= self.http_status_code <= 599:
            raise ValueError("http_status_code must be a valid three-digit HTTP status")


@dataclass(frozen=True, slots=True)
class HolderAccountFact:
    token_account: str
    owner_wallet: str | None
    raw_balance: Decimal
    is_known_pool: bool = False
    is_creator: bool = False
    exclusion_reason: str | None = None

    def __post_init__(self) -> None:
        if self.raw_balance < 0:
            raise ValueError("holder balance cannot be negative")
        if self.exclusion_reason is not None and not self.is_known_pool:
            raise ValueError("only explicitly known pool accounts may be excluded")


@dataclass(frozen=True, slots=True)
class HolderEvidencePage:
    envelope: EvidenceEnvelope
    mint_supply_raw: Decimal | None
    holder_count: int | None
    accounts: tuple[HolderAccountFact, ...]


@dataclass(frozen=True, slots=True)
class TradeFact:
    signature: str
    source_slot: int | None
    source_event_at: datetime | None
    received_at: datetime
    wallet: str | None
    side: TradeSide
    notional_usd: Decimal | None
    sequence: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "received_at", utc(self.received_at, "received_at"))
        if self.source_event_at is not None:
            object.__setattr__(
                self, "source_event_at", utc(self.source_event_at, "source_event_at")
            )
        if self.notional_usd is not None and self.notional_usd < 0:
            raise ValueError("trade notional cannot be negative")


@dataclass(frozen=True, slots=True)
class TraderEvidencePage:
    envelope: EvidenceEnvelope
    window_start: datetime
    window_end: datetime
    trades: tuple[TradeFact, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "window_start", utc(self.window_start, "window_start"))
        object.__setattr__(self, "window_end", utc(self.window_end, "window_end"))
        if self.window_end <= self.window_start:
            raise ValueError("trader window must be positive")


@dataclass(frozen=True, slots=True)
class CreatorRelationshipFact:
    creator_wallet: str
    relationship_type: str
    first_linked_at: datetime | None
    source_fact_identity: str


@dataclass(frozen=True, slots=True)
class CreatorHistoryFact:
    creator_wallet: str
    as_of: datetime
    prior_token_count: int | None
    prior_tracked_launches: int | None
    prior_collapse_count: int | None
    prior_large_winner_count: int | None
    median_survival_seconds: Decimal | None
    mean_survival_seconds: Decimal | None
    launches_last_30d: int | None
    creator_hold_pct: Decimal | None
    source_token_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class LiquidityEventFact:
    pair_address: str
    event_type: LiquidityEventType
    source_event_at: datetime | None
    received_at: datetime
    signature: str | None
    base_delta: Decimal | None
    quote_delta: Decimal | None
    liquidity_usd_before: Decimal | None
    liquidity_usd_after: Decimal | None
    removal_pct: Decimal | None
    lp_wallet: str | None = None


@dataclass(frozen=True, slots=True)
class WalletEdgeFact:
    wallet_a: str
    wallet_b: str
    relationship_type: WalletRelationshipType
    first_observed_at: datetime
    evidence_received_at: datetime
    strength_count: int
    source_fact_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.wallet_a == self.wallet_b:
            raise ValueError("wallet edge endpoints must differ")
        if self.strength_count <= 0:
            raise ValueError("wallet edge strength must be positive")


@dataclass(frozen=True, slots=True)
class FundingFact:
    wallet: str
    funding_source: str
    funding_at: datetime | None
    received_at: datetime
    amount_lamports: int | None
    hop_depth: int
    source_signature: str
    completeness: EvidenceCompleteness

    def __post_init__(self) -> None:
        if not 1 <= self.hop_depth <= 2:
            raise ValueError("funding graph depth must be one or two")
        if self.amount_lamports is not None and self.amount_lamports < 0:
            raise ValueError("funding amount cannot be negative")


@dataclass(frozen=True, slots=True)
class ProviderPageRequest:
    token_address: str
    candidate_id: str
    input_watermark: datetime
    cursor: str | None
    limit: int
    window_start: datetime | None = None
    window_end: datetime | None = None
    wallet_addresses: tuple[str, ...] = ()
    maximum_hops: int = 1
    mint_supply_raw: Decimal | None = None
    known_pool_accounts: tuple[str, ...] = ()
    creator_wallet: str | None = None


@dataclass(frozen=True, slots=True)
class CreatorEvidencePage:
    envelope: EvidenceEnvelope
    relationships: tuple[CreatorRelationshipFact, ...]
    history: CreatorHistoryFact | None


@dataclass(frozen=True, slots=True)
class LiquidityEvidencePage:
    envelope: EvidenceEnvelope
    events: tuple[LiquidityEventFact, ...]


@dataclass(frozen=True, slots=True)
class WalletEdgeEvidencePage:
    envelope: EvidenceEnvelope
    edges: tuple[WalletEdgeFact, ...]


@dataclass(frozen=True, slots=True)
class FundingEvidencePage:
    envelope: EvidenceEnvelope
    relationships: tuple[FundingFact, ...]
