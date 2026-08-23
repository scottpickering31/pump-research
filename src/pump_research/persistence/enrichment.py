"""Concurrency-safe persistence for append-only Phase 2 enrichment facts."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, localcontext

from sqlalchemy import func, nullsfirst, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from pump_research.persistence.models import (
    BoostEvent,
    BoostObservation,
    MarketContextSnapshot,
    PairFactEvent,
    Token,
    TokenMetadataEvent,
    TokenSecuritySnapshot,
    TokenSecurityTask,
)

_SCHEMA_VERSION = 1
_BOOST_THRESHOLDS = tuple(Decimal(value) for value in ("1", "10", "100", "1000"))
_BOOST_POLICY_SNAPSHOT: dict[str, object] = {
    "component": "boost_numeric_crossings",
    "schema_version": 1,
    "thresholds": [str(value) for value in _BOOST_THRESHOLDS],
    "semantics": "neutral_power_of_ten_query_bands_not_quality_categories",
}
_BOOST_POLICY_SHA256 = hashlib.sha256(
    json.dumps(_BOOST_POLICY_SNAPSHOT, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
_SECURITY_PHASE_DELAYS = (
    timedelta(hours=1),
    timedelta(hours=23),
    timedelta(days=6),
)


class EnrichmentIdentityConflictError(RuntimeError):
    """One idempotency identity resolved to semantically different content."""


def canonical_digest(value: object) -> str:
    """Hash JSON-compatible values with deterministic decimal/timestamp encoding."""
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _key(*parts: object) -> str:
    return hashlib.sha256(":".join(str(part) for part in parts).encode()).hexdigest()


def _lock_id(*parts: object) -> int:
    value = int.from_bytes(hashlib.sha256(":".join(map(str, parts)).encode()).digest()[:8], "big")
    return value - 2**64 if value >= 2**63 else value


async def _lock_scope(session: AsyncSession, *parts: object) -> None:
    await session.scalar(select(func.pg_advisory_xact_lock(_lock_id(*parts))))


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class PairFactCreate:
    """One normalized pair assertion embedded in a raw pair response."""

    pair_created_at: datetime | None
    dex_identifier: str | None
    labels: list[object] | None
    base_token_address: str | None
    base_token_name: str | None
    base_token_symbol: str | None
    quote_token_address: str | None
    quote_token_name: str | None
    quote_token_symbol: str | None


class PairFactRepository:
    """Append pair facts only when the source-attributed content changes."""

    async def record_if_changed(
        self,
        session: AsyncSession,
        *,
        pair_id: uuid.UUID,
        collector_run_id: uuid.UUID | None,
        api_request_log_id: uuid.UUID,
        provider: str,
        received_at: datetime,
        source_record_locator: str,
        source_record_sha256: str,
        fact: PairFactCreate,
    ) -> PairFactEvent | None:
        received_at = _utc(received_at, "received_at")
        content = asdict(fact)
        content_sha256 = canonical_digest(content)
        await _lock_scope(session, "pair-fact", pair_id, provider)
        previous = await session.scalar(
            select(PairFactEvent)
            .where(PairFactEvent.pair_id == pair_id, PairFactEvent.provider == provider)
            .order_by(PairFactEvent.received_at.desc(), PairFactEvent.id.desc())
            .limit(1)
        )
        if previous is not None and previous.content_sha256 == content_sha256:
            return None
        idempotency_key = _key("pair-fact", pair_id, api_request_log_id, source_record_locator)
        values = {
            "id": uuid.uuid5(uuid.NAMESPACE_URL, idempotency_key),
            "pair_id": pair_id,
            "collector_run_id": collector_run_id,
            "api_request_log_id": api_request_log_id,
            "idempotency_key": idempotency_key,
            "provider": provider,
            "received_at": received_at,
            "source_record_locator": source_record_locator,
            "source_record_sha256": source_record_sha256,
            "content_sha256": content_sha256,
            "schema_version": _SCHEMA_VERSION,
            **content,
        }
        await session.execute(insert(PairFactEvent).values(**values).on_conflict_do_nothing())
        stored = await session.scalar(
            select(PairFactEvent).where(
                or_(
                    PairFactEvent.id == values["id"],
                    PairFactEvent.idempotency_key == idempotency_key,
                )
            )
        )
        if stored is None or stored.content_sha256 != content_sha256:
            raise EnrichmentIdentityConflictError("pair fact identity maps to different content")
        return stored


@dataclass(frozen=True, slots=True)
class MetadataCreate:
    """Stable normalized metadata fields; absence is represented as None."""

    name: str | None = None
    symbol: str | None = None
    metadata_uri: str | None = None
    image_url: str | None = None
    header_url: str | None = None
    website_url: str | None = None
    twitter: str | None = None
    telegram: str | None = None
    other_links: list[object] | None = None


class TokenMetadataRepository:
    """Append source-specific metadata versions without repeated unchanged rows."""

    async def record_if_changed(
        self,
        session: AsyncSession,
        *,
        token_id: uuid.UUID,
        pair_id: uuid.UUID | None,
        collector_run_id: uuid.UUID | None,
        api_request_log_id: uuid.UUID | None,
        discovery_event_id: uuid.UUID | None,
        provider: str,
        source_kind: str,
        source_observed_at: datetime | None,
        received_at: datetime,
        source_record_locator: str,
        source_record_sha256: str,
        metadata: MetadataCreate,
    ) -> TokenMetadataEvent | None:
        if api_request_log_id is None and discovery_event_id is None:
            raise ValueError("metadata requires API-request or discovery-event provenance")
        received_at = _utc(received_at, "received_at")
        if source_observed_at is not None:
            source_observed_at = _utc(source_observed_at, "source_observed_at")
        content = asdict(metadata)
        if all(value is None for value in content.values()):
            return None
        content_sha256 = canonical_digest(content)
        scope_pair = pair_id or "token"
        await _lock_scope(session, "metadata", token_id, provider, source_kind, scope_pair)
        filters = [
            TokenMetadataEvent.token_id == token_id,
            TokenMetadataEvent.provider == provider,
            TokenMetadataEvent.source_kind == source_kind,
        ]
        filters.append(
            TokenMetadataEvent.pair_id.is_(None)
            if pair_id is None
            else TokenMetadataEvent.pair_id == pair_id
        )
        previous = await session.scalar(
            select(TokenMetadataEvent)
            .where(*filters)
            .order_by(TokenMetadataEvent.received_at.desc(), TokenMetadataEvent.id.desc())
            .limit(1)
        )
        if previous is not None and previous.content_sha256 == content_sha256:
            return None
        evidence_id = api_request_log_id or discovery_event_id
        idempotency_key = _key(
            "metadata",
            token_id,
            provider,
            source_kind,
            scope_pair,
            evidence_id,
            source_record_locator,
        )
        values = {
            "id": uuid.uuid5(uuid.NAMESPACE_URL, idempotency_key),
            "token_id": token_id,
            "pair_id": pair_id,
            "collector_run_id": collector_run_id,
            "api_request_log_id": api_request_log_id,
            "discovery_event_id": discovery_event_id,
            "idempotency_key": idempotency_key,
            "provider": provider,
            "source_kind": source_kind,
            "source_observed_at": source_observed_at,
            "received_at": received_at,
            "source_record_locator": source_record_locator,
            "source_record_sha256": source_record_sha256,
            "content_sha256": content_sha256,
            "schema_version": _SCHEMA_VERSION,
            **content,
        }
        await session.execute(insert(TokenMetadataEvent).values(**values).on_conflict_do_nothing())
        stored = await session.scalar(
            select(TokenMetadataEvent).where(
                or_(
                    TokenMetadataEvent.id == values["id"],
                    TokenMetadataEvent.idempotency_key == idempotency_key,
                )
            )
        )
        if stored is None or stored.content_sha256 != content_sha256:
            raise EnrichmentIdentityConflictError("metadata identity maps to different content")
        return stored


@dataclass(frozen=True, slots=True)
class BoostCreate:
    """Nullable boost facts; at least one value must be source-present."""

    active_boost_count: int | None = None
    amount: Decimal | None = None
    total_amount: Decimal | None = None


class BoostRepository:
    """Persist numeric boost changes and neutral threshold events transactionally."""

    async def record_if_changed(
        self,
        session: AsyncSession,
        *,
        token_id: uuid.UUID,
        pair_id: uuid.UUID | None,
        collector_run_id: uuid.UUID | None,
        api_request_log_id: uuid.UUID,
        provider: str,
        source_kind: str,
        feed_rank: int | None,
        source_observed_at: datetime | None,
        received_at: datetime,
        source_record_locator: str,
        source_record_sha256: str,
        fact: BoostCreate,
    ) -> BoostObservation | None:
        if all(value is None for value in asdict(fact).values()):
            return None
        received_at = _utc(received_at, "received_at")
        if source_observed_at is not None:
            source_observed_at = _utc(source_observed_at, "source_observed_at")
        scope_pair = pair_id or "feed"
        await _lock_scope(session, "boost", token_id, source_kind, scope_pair)
        filters = [
            BoostObservation.token_id == token_id,
            BoostObservation.source_kind == source_kind,
        ]
        filters.append(
            BoostObservation.pair_id.is_(None)
            if pair_id is None
            else BoostObservation.pair_id == pair_id
        )
        previous = await session.scalar(
            select(BoostObservation)
            .where(*filters)
            .order_by(BoostObservation.received_at.desc(), BoostObservation.id.desc())
            .limit(1)
        )
        content = asdict(fact)
        content_sha256 = canonical_digest(content)
        if previous is not None and previous.content_sha256 == content_sha256:
            return None
        idempotency_key = _key(
            "boost", token_id, source_kind, scope_pair, api_request_log_id, source_record_locator
        )
        identifier = uuid.uuid5(uuid.NAMESPACE_URL, idempotency_key)
        values = {
            "id": identifier,
            "token_id": token_id,
            "pair_id": pair_id,
            "collector_run_id": collector_run_id,
            "api_request_log_id": api_request_log_id,
            "idempotency_key": idempotency_key,
            "provider": provider,
            "source_kind": source_kind,
            "feed_rank": feed_rank,
            "source_observed_at": source_observed_at,
            "received_at": received_at,
            "source_record_locator": source_record_locator,
            "source_record_sha256": source_record_sha256,
            "content_sha256": content_sha256,
            "schema_version": _SCHEMA_VERSION,
            **content,
        }
        inserted = (
            await session.execute(
                insert(BoostObservation)
                .values(**values)
                .on_conflict_do_nothing()
                .returning(BoostObservation.id)
            )
        ).scalar_one_or_none()
        stored = await session.scalar(
            select(BoostObservation).where(
                or_(
                    BoostObservation.id == identifier,
                    BoostObservation.idempotency_key == idempotency_key,
                )
            )
        )
        if stored is None or stored.content_sha256 != content_sha256:
            raise EnrichmentIdentityConflictError("boost identity maps to different content")
        if inserted is not None:
            await self._record_events(session, observation=stored, previous=previous)
        return stored

    async def _record_events(
        self,
        session: AsyncSession,
        *,
        observation: BoostObservation,
        previous: BoostObservation | None,
    ) -> None:
        for metric in ("active_boost_count", "amount", "total_amount"):
            current_raw = getattr(observation, metric)
            if current_raw is None:
                continue
            current = Decimal(current_raw)
            previous_raw = getattr(previous, metric) if previous is not None else None
            previous_value = Decimal(previous_raw) if previous_raw is not None else None
            if previous_value is None:
                await self._event(
                    session,
                    observation,
                    event_type="first_seen",
                    metric=metric,
                    direction="none",
                    previous_value=None,
                    new_value=current,
                    threshold=None,
                )
                continue
            if previous_value == current:
                continue
            direction = "up" if current > previous_value else "down"
            await self._event(
                session,
                observation,
                event_type="state_change",
                metric=metric,
                direction=direction,
                previous_value=previous_value,
                new_value=current,
                threshold=None,
            )
            for threshold in _BOOST_THRESHOLDS:
                crossed_up = previous_value < threshold <= current
                crossed_down = previous_value >= threshold > current
                if crossed_up or crossed_down:
                    await self._event(
                        session,
                        observation,
                        event_type="threshold_crossing",
                        metric=metric,
                        direction="up" if crossed_up else "down",
                        previous_value=previous_value,
                        new_value=current,
                        threshold=threshold,
                    )

    async def _event(
        self,
        session: AsyncSession,
        observation: BoostObservation,
        *,
        event_type: str,
        metric: str,
        direction: str,
        previous_value: Decimal | None,
        new_value: Decimal,
        threshold: Decimal | None,
    ) -> None:
        key = _key(
            "boost-event", observation.id, event_type, metric, direction, threshold or "none"
        )
        await session.execute(
            insert(BoostEvent)
            .values(
                id=uuid.uuid5(uuid.NAMESPACE_URL, key),
                boost_observation_id=observation.id,
                token_id=observation.token_id,
                idempotency_key=key,
                event_type=event_type,
                metric=metric,
                direction=direction,
                previous_value=previous_value,
                new_value=new_value,
                threshold_value=threshold,
                decided_at=observation.received_at,
                policy_sha256=_BOOST_POLICY_SHA256,
                policy_snapshot=_BOOST_POLICY_SNAPSHOT,
            )
            .on_conflict_do_nothing()
        )


@dataclass(frozen=True, slots=True)
class SecurityClaim:
    """One leased finite security snapshot obligation."""

    token_id: uuid.UUID
    address: str
    phase: int
    lease_id: uuid.UUID


class TokenSecurityTaskRepository:
    """Create, claim and advance finite mint-security work."""

    async def create_if_absent(
        self, session: AsyncSession, *, token_id: uuid.UUID, due_at: datetime
    ) -> None:
        due_at = _utc(due_at, "due_at")
        await session.execute(
            insert(TokenSecurityTask)
            .values(token_id=token_id, phase=0, next_due_at=due_at, updated_at=due_at)
            .on_conflict_do_nothing(index_elements=[TokenSecurityTask.token_id])
        )

    async def claim_due(
        self,
        session: AsyncSession,
        *,
        now: datetime,
        limit: int,
        lease_duration: timedelta,
    ) -> tuple[SecurityClaim, ...]:
        now = _utc(now, "now")
        rows = (
            await session.execute(
                select(TokenSecurityTask, Token)
                .join(Token, Token.id == TokenSecurityTask.token_id)
                .where(
                    TokenSecurityTask.next_due_at <= now,
                    or_(
                        TokenSecurityTask.lease_id.is_(None),
                        TokenSecurityTask.lease_expires_at <= now,
                    ),
                )
                .order_by(
                    nullsfirst(TokenSecurityTask.next_due_at),
                    TokenSecurityTask.token_id,
                )
                .limit(limit)
                .with_for_update(skip_locked=True, of=TokenSecurityTask)
            )
        ).all()
        claims: list[SecurityClaim] = []
        for task, token in rows:
            lease_id = uuid.uuid4()
            task.lease_id = lease_id
            task.lease_expires_at = now + lease_duration
            task.updated_at = now
            claims.append(SecurityClaim(task.token_id, token.address, task.phase, lease_id))
        await session.flush()
        return tuple(claims)

    async def complete(
        self,
        session: AsyncSession,
        *,
        claims: tuple[SecurityClaim, ...],
        checked_at: datetime,
    ) -> None:
        checked_at = _utc(checked_at, "checked_at")
        for claim in claims:
            task = await session.scalar(
                select(TokenSecurityTask)
                .where(
                    TokenSecurityTask.token_id == claim.token_id,
                    TokenSecurityTask.lease_id == claim.lease_id,
                )
                .with_for_update()
            )
            if task is None:
                raise RuntimeError("token security lease is no longer owned")
            task.phase += 1
            task.last_checked_at = checked_at
            task.attempt_count += 1
            task.lease_id = None
            task.lease_expires_at = None
            task.next_due_at = (
                checked_at + _SECURITY_PHASE_DELAYS[task.phase - 1]
                if task.phase <= len(_SECURITY_PHASE_DELAYS)
                else None
            )
            task.updated_at = checked_at

    async def retry(
        self,
        session: AsyncSession,
        *,
        claims: tuple[SecurityClaim, ...],
        checked_at: datetime,
        retry_at: datetime,
    ) -> None:
        checked_at = _utc(checked_at, "checked_at")
        retry_at = _utc(retry_at, "retry_at")
        for claim in claims:
            task = await session.scalar(
                select(TokenSecurityTask)
                .where(
                    TokenSecurityTask.token_id == claim.token_id,
                    TokenSecurityTask.lease_id == claim.lease_id,
                )
                .with_for_update()
            )
            if task is None:
                raise RuntimeError("token security lease is no longer owned")
            task.last_checked_at = checked_at
            task.attempt_count += 1
            task.lease_id = None
            task.lease_expires_at = None
            task.next_due_at = retry_at
            task.updated_at = checked_at


class TokenSecuritySnapshotRepository:
    """Append one explicit snapshot outcome for each token/request/phase."""

    async def record(self, session: AsyncSession, **values: object) -> TokenSecuritySnapshot:
        received_at = values.get("received_at")
        if not isinstance(received_at, datetime):
            raise TypeError("received_at must be a datetime")
        values["received_at"] = _utc(received_at, "received_at")
        idempotency_key = str(values["idempotency_key"])
        identifier = uuid.uuid5(uuid.NAMESPACE_URL, idempotency_key)
        values["id"] = identifier
        values["schema_version"] = _SCHEMA_VERSION
        await session.execute(
            insert(TokenSecuritySnapshot).values(**values).on_conflict_do_nothing()
        )
        stored = await session.scalar(
            select(TokenSecuritySnapshot).where(
                or_(
                    TokenSecuritySnapshot.id == identifier,
                    TokenSecuritySnapshot.idempotency_key == idempotency_key,
                )
            )
        )
        if stored is None:
            raise RuntimeError("token security snapshot insert did not resolve")
        mismatches = [
            key
            for key, value in values.items()
            if key not in {"id", "schema_version"}
            and not _database_value_equal(getattr(stored, key), value)
        ]
        if mismatches:
            raise EnrichmentIdentityConflictError(
                "token security snapshot identity maps to different content: "
                + ", ".join(mismatches)
            )
        return stored


class MarketContextRepository:
    """Persist a single semantically verified context row per epoch/bucket/policy."""

    async def record(self, session: AsyncSession, **values: object) -> MarketContextSnapshot:
        identity = _key(
            "market-context",
            values["collection_epoch_id"],
            values["bucket_start"],
            values["policy_sha256"],
        )
        identifier = uuid.uuid5(uuid.NAMESPACE_URL, identity)
        values["id"] = identifier
        values["schema_version"] = _SCHEMA_VERSION
        await session.execute(
            insert(MarketContextSnapshot).values(**values).on_conflict_do_nothing()
        )
        stored = await session.scalar(
            select(MarketContextSnapshot).where(
                or_(
                    MarketContextSnapshot.id == identifier,
                    (
                        (MarketContextSnapshot.collection_epoch_id == values["collection_epoch_id"])
                        & (MarketContextSnapshot.bucket_start == values["bucket_start"])
                        & (MarketContextSnapshot.policy_sha256 == values["policy_sha256"])
                    ),
                )
            )
        )
        if stored is None:
            raise RuntimeError("market context insert did not resolve")
        semantic_fields = (
            "collection_epoch_id",
            "bucket_start",
            "bucket_end",
            "source_observed_at",
            "sol_usd_price",
            "sol_return_5m",
            "sol_realized_volatility_1h",
            "admitted_tokens",
            "active_transitions",
            "mature_cohort_tokens",
            "mature_cohort_active_tokens",
            "mature_cohort_active_fraction",
            "pair_sample_count",
            "aggregate_volume_m5_usd",
            "aggregate_buys_m5",
            "aggregate_sells_m5",
            "policy_sha256",
            "policy_snapshot",
        )
        mismatches = [
            field
            for field in semantic_fields
            if not _database_value_equal(getattr(stored, field), values[field])
        ]
        if mismatches:
            raise EnrichmentIdentityConflictError(
                "market context bucket identity maps to different content: "
                + ", ".join(mismatches)
            )
        return stored


async def latest_as_of[T](
    session: AsyncSession,
    model: type[T],
    *,
    token_id: uuid.UUID,
    as_of: datetime,
) -> T | None:
    """Return the latest token enrichment actually received by an as-of cutoff."""
    as_of = _utc(as_of, "as_of")
    statement = (
        select(model)
        .where(model.token_id == token_id, model.received_at <= as_of)  # type: ignore[attr-defined]
        .order_by(model.received_at.desc(), model.id.desc())  # type: ignore[attr-defined]
        .limit(1)
    )
    return (await session.execute(statement)).scalars().first()


def _database_value_equal(stored: object, proposed: object) -> bool:
    """Compare values after the harmless coercions PostgreSQL numeric/JSON performs."""
    if isinstance(stored, Decimal) or isinstance(proposed, Decimal):
        try:
            stored_decimal = Decimal(stored)  # type: ignore[arg-type]
            proposed_decimal = Decimal(proposed)  # type: ignore[arg-type]
        except (InvalidOperation, TypeError, ValueError):
            return False
        if stored_decimal == proposed_decimal:
            return True
        if not stored_decimal.is_finite() or not proposed_decimal.is_finite():
            return False
        stored_exponent = stored_decimal.as_tuple().exponent
        proposed_exponent = proposed_decimal.as_tuple().exponent
        assert isinstance(stored_exponent, int)
        assert isinstance(proposed_exponent, int)
        precision = max(
            len(stored_decimal.as_tuple().digits) + abs(stored_exponent),
            len(proposed_decimal.as_tuple().digits) + abs(proposed_exponent),
        )
        try:
            with localcontext() as context:
                context.prec = max(precision, 1)
                coerced = proposed_decimal.quantize(
                    stored_decimal,
                    rounding=ROUND_HALF_UP,
                )
        except InvalidOperation:
            return False
        return stored_decimal == coerced
    if isinstance(stored, datetime) and isinstance(proposed, datetime):
        return stored.astimezone(UTC) == proposed.astimezone(UTC)
    return stored == proposed
