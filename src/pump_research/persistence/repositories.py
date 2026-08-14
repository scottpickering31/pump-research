"""Repository abstractions with PostgreSQL-backed idempotent writes."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from pump_research.persistence.models import (
    ApiRequestLog,
    CollectorRun,
    DiscoveryEvent,
    LifecycleEvent,
    Observation,
    Pair,
    Token,
)


class PairTokenMismatchError(ValueError):
    """Raised when a canonical pair is associated with another tracked token."""


def _normalize_utc(value: datetime | None, field_name: str) -> datetime | None:
    """Reject naïve timestamps and normalize aware values to UTC."""
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _normalize_timestamp_values(
    values: dict[str, object], field_names: tuple[str, ...]
) -> dict[str, object]:
    """Normalize known timestamp fields in flexible repository payloads."""
    normalized = dict(values)
    for field_name in field_names:
        value = normalized.get(field_name)
        if value is not None:
            if not isinstance(value, datetime):
                msg = f"{field_name} must be a datetime or None"
                raise TypeError(msg)
            normalized[field_name] = _normalize_utc(value, field_name)
    return normalized


@dataclass(frozen=True, slots=True)
class ObservationCreate:
    """Normalized fields for one immutable pair observation."""

    pair_id: uuid.UUID
    source_observed_at: datetime | None = None
    source_record_locator: str | None = None
    source_record_sha256: str | None = None
    price_usd: Decimal | None = None
    price_native: Decimal | None = None
    liquidity_usd: Decimal | None = None
    market_cap_usd: Decimal | None = None
    fully_diluted_valuation_usd: Decimal | None = None
    volume_m5_usd: Decimal | None = None
    volume_h1_usd: Decimal | None = None
    volume_h6_usd: Decimal | None = None
    volume_h24_usd: Decimal | None = None
    price_change_m5_pct: Decimal | None = None
    price_change_h1_pct: Decimal | None = None
    price_change_h6_pct: Decimal | None = None
    price_change_h24_pct: Decimal | None = None
    buys_m5: int | None = None
    sells_m5: int | None = None
    buys_h1: int | None = None
    sells_h1: int | None = None


async def _load_by_id[T](session: AsyncSession, model: type[T], identifier: uuid.UUID) -> T:
    result = await session.execute(select(model).where(model.id == identifier))  # type: ignore[attr-defined]
    return result.scalar_one()


class TokenRepository:
    """Idempotent creation and retrieval of canonical token identities."""

    async def get_or_create(
        self,
        session: AsyncSession,
        *,
        chain: str,
        address: str,
        first_discovered_at: datetime | None,
    ) -> Token:
        first_discovered_at = _normalize_utc(first_discovered_at, "first_discovered_at")
        statement = (
            insert(Token)
            .values(chain=chain, address=address, first_discovered_at=first_discovered_at)
            .on_conflict_do_nothing(index_elements=[Token.chain, Token.address])
            .returning(Token.id)
        )
        identifier = (await session.execute(statement)).scalar_one_or_none()
        if identifier is not None:
            return await _load_by_id(session, Token, identifier)

        result = await session.execute(
            select(Token).where(Token.chain == chain, Token.address == address)
        )
        return result.scalar_one()


class PairRepository:
    """Idempotent creation of canonical pairs without assuming one pair per token."""

    async def get_or_create(
        self,
        session: AsyncSession,
        *,
        token_id: uuid.UUID,
        chain: str,
        address: str,
        dex_identifier: str | None,
        first_discovered_at: datetime | None,
    ) -> Pair:
        first_discovered_at = _normalize_utc(first_discovered_at, "first_discovered_at")
        statement = (
            insert(Pair)
            .values(
                token_id=token_id,
                chain=chain,
                address=address,
                dex_identifier=dex_identifier,
                first_discovered_at=first_discovered_at,
            )
            .on_conflict_do_nothing(index_elements=[Pair.chain, Pair.address])
            .returning(Pair.id)
        )
        identifier = (await session.execute(statement)).scalar_one_or_none()
        pair = (
            await _load_by_id(session, Pair, identifier)
            if identifier is not None
            else (
                await session.execute(
                    select(Pair).where(Pair.chain == chain, Pair.address == address)
                )
            ).scalar_one()
        )
        if pair.token_id != token_id:
            msg = "A canonical pair cannot be reassigned to a different token"
            raise PairTokenMismatchError(msg)
        return pair


class CollectorRunRepository:
    """Create and finish mutable operational collector-run records."""

    async def start(
        self,
        session: AsyncSession,
        *,
        started_at: datetime,
        collector_version: str,
        configuration_sha256: str,
        configuration_snapshot: dict[str, object],
    ) -> CollectorRun:
        normalized_started_at = _normalize_utc(started_at, "started_at")
        assert normalized_started_at is not None
        run = CollectorRun(
            started_at=normalized_started_at,
            status="running",
            collector_version=collector_version,
            configuration_sha256=configuration_sha256,
            configuration_snapshot=configuration_snapshot,
        )
        session.add(run)
        await session.flush()
        return run

    async def finish(
        self,
        session: AsyncSession,
        *,
        run_id: uuid.UUID,
        finished_at: datetime,
        status: str,
        failure_detail: dict[str, object] | None = None,
    ) -> None:
        normalized_finished_at = _normalize_utc(finished_at, "finished_at")
        assert normalized_finished_at is not None
        await session.execute(
            update(CollectorRun)
            .where(CollectorRun.id == run_id)
            .values(
                finished_at=normalized_finished_at,
                status=status,
                failure_detail=failure_detail,
            )
        )


class DiscoveryEventRepository:
    """Append immutable, idempotent discovery-source evidence."""

    async def record(
        self,
        session: AsyncSession,
        **values: object,
    ) -> DiscoveryEvent:
        normalized_values = _normalize_timestamp_values(
            values,
            ("source_event_at", "received_at"),
        )
        statement = (
            insert(DiscoveryEvent)
            .values(**normalized_values)
            .on_conflict_do_nothing(index_elements=[DiscoveryEvent.idempotency_key])
            .returning(DiscoveryEvent.id)
        )
        identifier = (await session.execute(statement)).scalar_one_or_none()
        if identifier is not None:
            return await _load_by_id(session, DiscoveryEvent, identifier)

        key = normalized_values["idempotency_key"]
        result = await session.execute(
            select(DiscoveryEvent).where(DiscoveryEvent.idempotency_key == key)
        )
        return result.scalar_one()


class ApiRequestLogRepository:
    """Append immutable, idempotent request evidence and raw batch responses."""

    async def record(self, session: AsyncSession, **values: object) -> ApiRequestLog:
        normalized_values = _normalize_timestamp_values(values, ("requested_at", "received_at"))
        statement = (
            insert(ApiRequestLog)
            .values(**normalized_values)
            .on_conflict_do_nothing(index_elements=[ApiRequestLog.idempotency_key])
            .returning(ApiRequestLog.id)
        )
        identifier = (await session.execute(statement)).scalar_one_or_none()
        if identifier is not None:
            return await _load_by_id(session, ApiRequestLog, identifier)

        key = normalized_values["idempotency_key"]
        result = await session.execute(
            select(ApiRequestLog).where(ApiRequestLog.idempotency_key == key)
        )
        return result.scalar_one()


class ObservationRepository:
    """Append normalized facts without deduplicating unchanged market values."""

    async def record_many(
        self,
        session: AsyncSession,
        *,
        api_request: ApiRequestLog,
        observations: list[ObservationCreate],
    ) -> int:
        if api_request.received_at is None:
            msg = "Observations require an API request with a received_at timestamp"
            raise ValueError(msg)
        if not observations:
            return 0

        values = [
            {
                "received_at": api_request.received_at,
                "api_request_log_id": api_request.id,
                **asdict(
                    replace(
                        observation,
                        source_observed_at=_normalize_utc(
                            observation.source_observed_at,
                            "source_observed_at",
                        ),
                    )
                ),
            }
            for observation in observations
        ]
        statement = (
            insert(Observation)
            .values(values)
            .on_conflict_do_nothing(
                index_elements=[
                    Observation.received_at,
                    Observation.api_request_log_id,
                    Observation.pair_id,
                ]
            )
            .returning(Observation.id)
        )
        return len((await session.execute(statement)).scalars().all())


class LifecycleEventRepository:
    """Append derived lifecycle transitions with durable idempotency."""

    async def record(self, session: AsyncSession, **values: object) -> LifecycleEvent:
        normalized_values = _normalize_timestamp_values(
            values,
            ("decided_at", "input_watermark"),
        )
        statement = (
            insert(LifecycleEvent)
            .values(**normalized_values)
            .on_conflict_do_nothing(index_elements=[LifecycleEvent.idempotency_key])
            .returning(LifecycleEvent.id)
        )
        identifier = (await session.execute(statement)).scalar_one_or_none()
        if identifier is not None:
            return await _load_by_id(session, LifecycleEvent, identifier)

        key = normalized_values["idempotency_key"]
        result = await session.execute(
            select(LifecycleEvent).where(LifecycleEvent.idempotency_key == key)
        )
        return result.scalar_one()
