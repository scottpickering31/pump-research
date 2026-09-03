"""Bounded shared DEX boost feeds with immutable numeric/change evidence."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.market_data.dexscreener import (
    DEX_SCREENER_PROVIDER,
    DexScreenerBoostFeedResult,
    DexScreenerHttpError,
)
from pump_research.market_data.dexscreener_models import DexScreenerBoostFeedRecord
from pump_research.persistence.enrichment import (
    BoostCreate,
    BoostRepository,
    MetadataCreate,
    TokenMetadataRepository,
    canonical_digest,
)
from pump_research.persistence.models import Token
from pump_research.persistence.repositories import ApiRequestLogRepository


class BoostFeedSource(Protocol):
    """Mockable global boost-feed boundary."""

    async def fetch_boost_feed(self, *, feed_kind: str) -> DexScreenerBoostFeedResult:
        """Return a typed and lossless latest/top feed response."""


class BoostWakeupHandler(Protocol):
    """Bounded candidate wake-up hook for a newly persisted boost fact."""

    async def evaluate_boost_observation_in_session(
        self,
        session: AsyncSession,
        *,
        boost_observation_id: uuid.UUID,
        collector_run_id: uuid.UUID,
    ) -> object:
        """Evaluate one tracked boost fact without implying lifecycle ACTIVE."""


@dataclass(frozen=True, slots=True)
class BoostCollectionResult:
    """One shared-feed collection summary."""

    feed_kind: str
    source_records: int
    tracked_records: int
    boost_changes: int
    metadata_changes: int


class BoostCollectionWorkflow:
    """Retain complete bounded feeds and normalize only existing cohort tokens."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        source: BoostFeedSource,
        *,
        wakeup_handler: BoostWakeupHandler | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._source = source
        self._requests = ApiRequestLogRepository()
        self._boosts = BoostRepository()
        self._metadata = TokenMetadataRepository()
        self._wakeup_handler = wakeup_handler

    async def collect(
        self,
        *,
        feed_kind: str,
        collector_run_id: uuid.UUID,
        requested_at: datetime | None = None,
    ) -> BoostCollectionResult:
        """Fetch and persist one feed; omission is never materialized as zero."""
        if feed_kind not in {"latest", "top"}:
            raise ValueError("feed_kind must be 'latest' or 'top'")
        requested_at = requested_at or datetime.now(UTC)
        request_identity = uuid.uuid4()
        endpoint = f"/token-boosts/{feed_kind}/v1"
        try:
            result = await self._source.fetch_boost_feed(feed_kind=feed_kind)
        except Exception as error:
            received_at = datetime.now(UTC)
            async with self._session_factory() as session, session.begin():
                await self._requests.record(
                    session,
                    collector_run_id=collector_run_id,
                    idempotency_key=_key("boost-request-failed", request_identity),
                    provider=DEX_SCREENER_PROVIDER,
                    endpoint=endpoint,
                    requested_at=requested_at,
                    received_at=received_at,
                    outcome=(
                        "throttled"
                        if isinstance(error, DexScreenerHttpError) and error.status_code == 429
                        else "failed"
                    ),
                    http_status_code=(
                        error.status_code if isinstance(error, DexScreenerHttpError) else None
                    ),
                    request_payload={"feed_kind": feed_kind},
                    response_payload=None,
                    response_payload_sha256=None,
                    failure_detail={"error_type": type(error).__name__, "message": str(error)},
                )
            raise

        payload = {"records": list(result.raw_response)}
        # The provider request is complete evidence in its own right. Commit it
        # before normalization so a later per-record failure cannot erase the raw
        # feed that explains both persisted and not-yet-persisted enrichment.
        async with self._session_factory() as session, session.begin():
            request = await self._requests.record(
                session,
                collector_run_id=collector_run_id,
                idempotency_key=_key("boost-request", request_identity),
                provider=DEX_SCREENER_PROVIDER,
                endpoint=endpoint,
                requested_at=requested_at,
                received_at=result.received_at,
                outcome="empty" if not result.records else "succeeded",
                http_status_code=200,
                request_payload={
                    "feed_kind": feed_kind,
                    "coverage_semantics": "bounded_global_feed_not_complete_token_state",
                    "normalization_semantics": "raw_feed_then_per_tracked_record_transactions",
                    "provider_attempt_count": result.attempt_count,
                },
                response_payload=payload,
                response_payload_sha256=canonical_digest(payload),
                failure_detail=None,
            )
            addresses = {
                record.token_address
                for record in result.records
                if record.chain_id == "solana" and record.token_address
            }
            token_rows = (
                (
                    await session.execute(
                        select(Token.address, Token.id).where(
                            Token.chain == "solana", Token.address.in_(addresses)
                        )
                    )
                ).all()
                if addresses
                else []
            )
            tokens_by_address = {address: token_id for address, token_id in token_rows}
            request_id = request.id

        boost_changes = 0
        metadata_changes = 0
        tracked_records = 0
        records = zip(result.records, result.raw_response, strict=True)
        for index, (record, raw) in enumerate(records):
            token_id = tokens_by_address.get(record.token_address or "")
            if token_id is None:
                continue
            # A feed can overlap another feed in arbitrary token order. Keeping
            # each tracked record atomic avoids retaining one token's advisory
            # locks while acquiring another token's locks. If a later record
            # fails, earlier records remain valid immutable enrichment and the
            # already-committed raw feed remains available for audit or replay.
            async with self._session_factory() as session, session.begin():
                locator = f"records[{index}]"
                record_sha256 = canonical_digest(raw)
                boost = await self._boosts.record_if_changed(
                    session,
                    token_id=token_id,
                    pair_id=None,
                    collector_run_id=collector_run_id,
                    api_request_log_id=request_id,
                    provider=DEX_SCREENER_PROVIDER,
                    source_kind=f"{feed_kind}_feed",
                    feed_rank=index + 1,
                    source_observed_at=None,
                    received_at=result.received_at,
                    source_record_locator=locator,
                    source_record_sha256=record_sha256,
                    fact=BoostCreate(amount=record.amount, total_amount=record.total_amount),
                )
                if boost is not None and self._wakeup_handler is not None:
                    await self._wakeup_handler.evaluate_boost_observation_in_session(
                        session,
                        boost_observation_id=boost.id,
                        collector_run_id=collector_run_id,
                    )
                metadata = await self._metadata.record_if_changed(
                    session,
                    token_id=token_id,
                    pair_id=None,
                    collector_run_id=collector_run_id,
                    api_request_log_id=request_id,
                    discovery_event_id=None,
                    provider=DEX_SCREENER_PROVIDER,
                    source_kind="boost_feed",
                    source_observed_at=None,
                    received_at=result.received_at,
                    source_record_locator=locator,
                    source_record_sha256=record_sha256,
                    metadata=_feed_metadata(record),
                )
            tracked_records += 1
            boost_changes += boost is not None
            metadata_changes += metadata is not None
        return BoostCollectionResult(
            feed_kind=feed_kind,
            source_records=len(result.records),
            tracked_records=tracked_records,
            boost_changes=boost_changes,
            metadata_changes=metadata_changes,
        )


def _feed_metadata(record: DexScreenerBoostFeedRecord) -> MetadataCreate:
    website: str | None = None
    twitter: str | None = None
    telegram: str | None = None
    other: list[object] = []
    for link in record.links or []:
        kind = (link.type or link.label or "").lower()
        if "twitter" in kind or kind == "x":
            twitter = twitter or link.url
        elif "telegram" in kind:
            telegram = telegram or link.url
        elif website is None:
            website = link.url
        if link.url:
            other.append(link.model_dump(mode="json"))
    return MetadataCreate(
        image_url=record.icon,
        header_url=record.header,
        website_url=website,
        twitter=twitter,
        telegram=telegram,
        other_links=other or None,
    )


def _key(prefix: str, identity: uuid.UUID) -> str:
    return hashlib.sha256(f"{prefix}:{identity}".encode()).hexdigest()
