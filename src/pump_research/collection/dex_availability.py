"""Durable admission from discovery to initial DEX Screener availability.

This is intentionally a narrow workflow, not the general polling scheduler.
It retains tokens without DEX results and uses database leases so a replacement
process can resume pending work after a crash.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

import httpx
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.config import Settings
from pump_research.discovery.contracts import DiscoveredToken
from pump_research.market_data.dexscreener import (
    DEX_SCREENER_PROVIDER,
    TOKEN_BATCH_LIMIT,
    DexScreenerHttpError,
    DexScreenerTokenPairsResult,
)
from pump_research.market_data.dexscreener_models import DexScreenerPair
from pump_research.persistence.models import ApiRequestLog
from pump_research.persistence.repositories import (
    ApiRequestLogRepository,
    DexAvailabilityClaim,
    DexAvailabilityTaskRepository,
    DiscoveryEventRepository,
    LifecycleEventRepository,
    TokenRepository,
)
from pump_research.scheduling.policy import LifecycleState
from pump_research.scheduling.scheduler import AdaptiveScheduler

_WORKFLOW_NAME = "dex_availability_admission"
_DEX_ENDPOINT = "/tokens/v1/{chain_id}/{token_addresses}"


class DexTokenPairsSource(Protocol):
    """Market-data boundary needed by the availability workflow and its tests."""

    async def fetch_token_pairs(
        self,
        *,
        chain_id: str,
        token_addresses: list[str],
    ) -> DexScreenerTokenPairsResult:
        """Return DEX pairs for a provider-permitted address batch."""


@dataclass(frozen=True, slots=True)
class DiscoveryAdmission:
    """Result of durably admitting one discovery event to DEX availability work."""

    token_id: uuid.UUID
    state: str
    pending_task_created: bool


@dataclass(frozen=True, slots=True)
class DexAvailabilityRunResult:
    """Auditable totals from one bounded, due-work availability pass."""

    claimed_tokens: int
    checked_tokens: int
    retained_pending_tokens: int
    promoted_new_tokens: int
    failed_tokens: int


class DexAvailabilityWorkflow:
    """Persist discovery, retain non-listed tokens, and promote DEX-present tokens."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        dex_source: DexTokenPairsSource,
        settings: Settings,
        *,
        logger: structlog.stdlib.BoundLogger | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._dex_source = dex_source
        self._retry_interval = timedelta(seconds=settings.dex_availability_retry_seconds)
        self._lease_duration = timedelta(seconds=settings.dex_availability_lease_seconds)
        self._configuration_snapshot = {
            "component": _WORKFLOW_NAME,
            "schema_version": 1,
            "retry_seconds": settings.dex_availability_retry_seconds,
            "lease_seconds": settings.dex_availability_lease_seconds,
        }
        self._configuration_sha256 = _payload_sha256(self._configuration_snapshot)
        self._logger = logger or structlog.get_logger("pump_research.collection.dex_availability")
        self._token_repository = TokenRepository()
        self._discovery_repository = DiscoveryEventRepository()
        self._task_repository = DexAvailabilityTaskRepository()
        self._request_repository = ApiRequestLogRepository()
        self._lifecycle_repository = LifecycleEventRepository()
        self._adaptive_scheduler = AdaptiveScheduler(session_factory, settings)

    async def admit_discovery(self, event: DiscoveredToken) -> DiscoveryAdmission:
        """Durably persist a discovery event and create its initial pending task."""
        async with self._session_factory() as session, session.begin():
            token = await self._token_repository.get_or_create(
                session,
                chain=event.chain,
                address=event.address,
                first_discovered_at=event.source_event_at,
            )
            await self._discovery_repository.record(
                session,
                token_id=token.id,
                idempotency_key=event.idempotency_key,
                provider=event.source_name,
                provider_event_id=event.source_event_id,
                event_type=event.event_type,
                source_event_at=event.source_event_at,
                received_at=event.received_at,
                source_payload=dict(event.source_payload),
                source_payload_sha256=event.source_payload_sha256,
            )
            task, task_created = await self._task_repository.create_pending_if_absent(
                session,
                token_id=token.id,
                due_at=event.received_at,
            )
            if task_created:
                await self._record_lifecycle(
                    session,
                    token_id=token.id,
                    idempotency_parts=("initial-pending", str(token.id)),
                    previous_state=None,
                    new_state="PENDING_DEX",
                    decided_at=event.received_at,
                    input_watermark=event.received_at,
                    reason_code="discovered_awaiting_dex",
                    reason_detail={"discovery_provider": event.source_name},
                )
        return DiscoveryAdmission(
            token_id=token.id,
            state=task.state,
            pending_task_created=task_created,
        )

    async def check_due(
        self,
        *,
        now: datetime | None = None,
        maximum_tokens: int = TOKEN_BATCH_LIMIT,
    ) -> DexAvailabilityRunResult:
        """Claim at most one DEX-eligible batch and persist every outcome."""
        if maximum_tokens < 1 or maximum_tokens > TOKEN_BATCH_LIMIT:
            msg = f"maximum_tokens must be between 1 and {TOKEN_BATCH_LIMIT}"
            raise ValueError(msg)
        check_started_at = now or datetime.now(UTC)
        async with self._session_factory() as session, session.begin():
            claims = await self._task_repository.claim_due(
                session,
                now=check_started_at,
                limit=maximum_tokens,
                lease_duration=self._lease_duration,
            )
        if not claims:
            return DexAvailabilityRunResult(0, 0, 0, 0, 0)

        claims_by_chain: dict[str, list[DexAvailabilityClaim]] = defaultdict(list)
        for claim in claims:
            claims_by_chain[claim.chain].append(claim)

        checked = 0
        retained = 0
        promoted = 0
        failed = 0
        for chain, chain_claims in claims_by_chain.items():
            outcome = await self._check_claim_batch(
                chain=chain,
                claims=chain_claims,
                requested_at=check_started_at,
            )
            checked += outcome.checked_tokens
            retained += outcome.retained_pending_tokens
            promoted += outcome.promoted_new_tokens
            failed += outcome.failed_tokens
        return DexAvailabilityRunResult(
            claimed_tokens=len(claims),
            checked_tokens=checked,
            retained_pending_tokens=retained,
            promoted_new_tokens=promoted,
            failed_tokens=failed,
        )

    async def _check_claim_batch(
        self,
        *,
        chain: str,
        claims: list[DexAvailabilityClaim],
        requested_at: datetime,
    ) -> DexAvailabilityRunResult:
        addresses = [claim.address for claim in claims]
        try:
            result = await self._dex_source.fetch_token_pairs(
                chain_id=chain,
                token_addresses=addresses,
            )
            _validate_batch_result(result, chain=chain, addresses=addresses)
        except Exception as error:
            await self._record_failed_batch(
                chain=chain,
                claims=claims,
                requested_at=requested_at,
                error=error,
            )
            self._logger.error(
                "dex_availability_batch_failed",
                chain=chain,
                address_count=len(addresses),
                error_type=type(error).__name__,
            )
            return DexAvailabilityRunResult(
                claimed_tokens=0,
                checked_tokens=0,
                retained_pending_tokens=0,
                promoted_new_tokens=0,
                failed_tokens=len(claims),
            )

        matched_pairs = {
            claim.token_id: _matching_pairs(result.pairs, chain=chain, address=claim.address)
            for claim in claims
        }
        received_at = result.batches[0].received_at
        present_claims = [claim for claim in claims if matched_pairs[claim.token_id]]
        absent_claims = [claim for claim in claims if not matched_pairs[claim.token_id]]
        async with self._session_factory() as session, session.begin():
            request = await self._record_successful_request(
                session,
                chain=chain,
                claims=claims,
                requested_at=requested_at,
                result=result,
            )
            if absent_claims:
                await self._task_repository.complete_with_retry(
                    session,
                    token_ids=[claim.token_id for claim in absent_claims],
                    lease_id=absent_claims[0].lease_id,
                    checked_at=received_at,
                    retry_at=received_at + self._retry_interval,
                )
                for claim in absent_claims:
                    await self._record_lifecycle(
                        session,
                        token_id=claim.token_id,
                        idempotency_parts=("retry", str(request.id), str(claim.token_id)),
                        previous_state="PENDING_DEX",
                        new_state="PENDING_DEX",
                        decided_at=received_at,
                        input_watermark=received_at,
                        reason_code="dex_not_present_retry_scheduled",
                        reason_detail={
                            "api_request_log_id": str(request.id),
                            "next_check_at": (received_at + self._retry_interval).isoformat(),
                        },
                    )
            if present_claims:
                await self._task_repository.complete_as_new(
                    session,
                    token_ids=[claim.token_id for claim in present_claims],
                    lease_id=present_claims[0].lease_id,
                    checked_at=received_at,
                )
                for claim in present_claims:
                    await self._record_lifecycle(
                        session,
                        token_id=claim.token_id,
                        idempotency_parts=("new", str(request.id), str(claim.token_id)),
                        previous_state="PENDING_DEX",
                        new_state="NEW",
                        decided_at=received_at,
                        input_watermark=received_at,
                        reason_code="dex_pair_present",
                        reason_detail={
                            "api_request_log_id": str(request.id),
                            "matching_pair_count": len(matched_pairs[claim.token_id]),
                        },
                    )
                    await self._adaptive_scheduler.set_lifecycle_state_in_session(
                        session,
                        token_id=claim.token_id,
                        state=LifecycleState.NEW,
                        decided_at=received_at,
                        reason_code="dex_pair_present",
                    )
        self._logger.info(
            "dex_availability_batch_completed",
            chain=chain,
            address_count=len(addresses),
            retained_pending_count=len(absent_claims),
            promoted_new_count=len(present_claims),
        )
        return DexAvailabilityRunResult(
            claimed_tokens=0,
            checked_tokens=len(claims),
            retained_pending_tokens=len(absent_claims),
            promoted_new_tokens=len(present_claims),
            failed_tokens=0,
        )

    async def _record_successful_request(
        self,
        session: AsyncSession,
        *,
        chain: str,
        claims: list[DexAvailabilityClaim],
        requested_at: datetime,
        result: DexScreenerTokenPairsResult,
    ) -> ApiRequestLog:
        batch = result.batches[0]
        response_payload = {"pairs": list(batch.raw_response)}
        return await self._request_repository.record(
            session,
            collector_run_id=None,
            idempotency_key=_idempotency_key("request", str(claims[0].lease_id), chain),
            provider=DEX_SCREENER_PROVIDER,
            endpoint=_DEX_ENDPOINT,
            requested_at=requested_at,
            received_at=batch.received_at,
            outcome="empty" if not batch.pairs else "succeeded",
            http_status_code=200,
            request_payload={
                "chain_id": chain,
                "token_addresses": [claim.address for claim in claims],
            },
            response_payload=response_payload,
            response_payload_sha256=_payload_sha256(response_payload),
            failure_detail=None,
        )

    async def _record_failed_batch(
        self,
        *,
        chain: str,
        claims: list[DexAvailabilityClaim],
        requested_at: datetime,
        error: Exception,
    ) -> None:
        received_at = datetime.now(UTC)
        status_code = error.status_code if isinstance(error, DexScreenerHttpError) else None
        outcome = "throttled" if status_code == httpx.codes.TOO_MANY_REQUESTS else "failed"
        async with self._session_factory() as session, session.begin():
            request = await self._request_repository.record(
                session,
                collector_run_id=None,
                idempotency_key=_idempotency_key("failed-request", str(claims[0].lease_id), chain),
                provider=DEX_SCREENER_PROVIDER,
                endpoint=_DEX_ENDPOINT,
                requested_at=requested_at,
                received_at=received_at,
                outcome=outcome,
                http_status_code=status_code,
                request_payload={
                    "chain_id": chain,
                    "token_addresses": [claim.address for claim in claims],
                },
                response_payload=None,
                response_payload_sha256=None,
                failure_detail={"error_type": type(error).__name__, "message": str(error)},
            )
            await self._task_repository.complete_with_retry(
                session,
                token_ids=[claim.token_id for claim in claims],
                lease_id=claims[0].lease_id,
                checked_at=received_at,
                retry_at=received_at + self._retry_interval,
            )
            for claim in claims:
                await self._record_lifecycle(
                    session,
                    token_id=claim.token_id,
                    idempotency_parts=("failed", str(request.id), str(claim.token_id)),
                    previous_state="PENDING_DEX",
                    new_state="PENDING_DEX",
                    decided_at=received_at,
                    input_watermark=received_at,
                    reason_code="dex_check_failed_retry_scheduled",
                    reason_detail={
                        "api_request_log_id": str(request.id),
                        "next_check_at": (received_at + self._retry_interval).isoformat(),
                    },
                )

    async def _record_lifecycle(
        self,
        session: AsyncSession,
        *,
        token_id: uuid.UUID,
        idempotency_parts: tuple[str, ...],
        previous_state: str | None,
        new_state: str,
        decided_at: datetime,
        input_watermark: datetime,
        reason_code: str,
        reason_detail: Mapping[str, object] | None,
    ) -> None:
        await self._lifecycle_repository.record(
            session,
            token_id=token_id,
            idempotency_key=_idempotency_key(*idempotency_parts),
            previous_state=previous_state,
            new_state=new_state,
            decided_at=decided_at,
            input_watermark=input_watermark,
            reason_code=reason_code,
            reason_detail=dict(reason_detail) if reason_detail is not None else None,
            configuration_sha256=self._configuration_sha256,
            configuration_snapshot=self._configuration_snapshot,
        )


def _validate_batch_result(
    result: DexScreenerTokenPairsResult,
    *,
    chain: str,
    addresses: list[str],
) -> None:
    if (
        result.chain_id != chain
        or result.requested_addresses != tuple(addresses)
        or len(result.batches) != 1
    ):
        msg = "DEX availability source returned a result that does not match the claimed batch"
        raise ValueError(msg)


def _matching_pairs(
    pairs: tuple[DexScreenerPair, ...],
    *,
    chain: str,
    address: str,
) -> tuple[DexScreenerPair, ...]:
    return tuple(
        pair
        for pair in pairs
        if pair.chain_id == chain
        and (
            (pair.base_token is not None and pair.base_token.address == address)
            or (pair.quote_token is not None and pair.quote_token.address == address)
        )
    )


def _idempotency_key(*parts: str) -> str:
    return hashlib.sha256(":".join((_WORKFLOW_NAME, *parts)).encode()).hexdigest()


def _payload_sha256(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(encoded).hexdigest()
