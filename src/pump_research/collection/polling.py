"""Durable market-observation execution for batches claimed by the scheduler."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import httpx
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.lifecycle.classifier import LifecycleRequestEvaluation
from pump_research.market_data.dexscreener import (
    DEX_SCREENER_PROVIDER,
    DexScreenerHttpError,
    DexScreenerTokenPairsResult,
)
from pump_research.market_data.dexscreener_models import DexScreenerPair
from pump_research.persistence.enrichment import (
    BoostCreate,
    BoostRepository,
    MetadataCreate,
    PairFactCreate,
    PairFactRepository,
    TokenMetadataRepository,
)
from pump_research.persistence.repositories import (
    ApiRequestLogRepository,
    ObservationCreate,
    ObservationRepository,
    PairRepository,
    PairTokenMismatchError,
)
from pump_research.scheduling.locks import lock_schedule_token_fk_path
from pump_research.scheduling.scheduler import AdaptiveScheduler, PollBatchClaim, PollOutcome

_ENDPOINT = "/tokens/v1/{chain_id}/{token_addresses}"


class ScheduledDexSource(Protocol):
    """The mockable DEX boundary used for scheduler-driven market observations."""

    async def fetch_token_pairs(
        self, *, chain_id: str, token_addresses: list[str]
    ) -> DexScreenerTokenPairsResult:
        """Return typed pair records for exactly one bounded scheduler batch."""


class LifecycleObservationHandler(Protocol):
    """Transactional lifecycle boundary required by scheduled polling."""

    async def evaluate_request_in_session(
        self,
        session: AsyncSession,
        *,
        api_request_log_id: uuid.UUID,
    ) -> LifecycleRequestEvaluation:
        """Select and evaluate one evidence record per represented token."""


class CandidateObservationHandler(Protocol):
    """Optional derived orchestration boundary; raw collection stays authoritative."""

    async def evaluate_request_in_session(
        self,
        session: AsyncSession,
        *,
        api_request_log_id: uuid.UUID,
        collector_run_id: uuid.UUID,
    ) -> object:
        """Evaluate only facts durably available in this request transaction."""


class LifecycleEvaluationError(RuntimeError):
    """Raw observations were preserved but their derived evaluation failed."""


@dataclass(frozen=True, slots=True)
class PollExecutionResult:
    """Durable result of attempting one scheduler batch."""

    batch_id: uuid.UUID
    observations_written: int
    outcome: PollOutcome


@dataclass(frozen=True, slots=True)
class ObservationBuildResult:
    """Normalized observations plus explicit per-member coverage evidence."""

    creates: tuple[ObservationCreate, ...]
    observed_token_ids: frozenset[uuid.UUID]
    issues: tuple[dict[str, object], ...]
    normalized_pairs: tuple[NormalizedPair, ...]


@dataclass(frozen=True, slots=True)
class NormalizedPair:
    """One canonicalized response pair available for auxiliary zero-cost facts."""

    token_id: uuid.UUID
    token_address: str
    pair_id: uuid.UUID
    pair: DexScreenerPair
    source_index: int
    source_record_sha256: str


class ScheduledObservationWorkflow:
    """Fetch, persist, and complete one claimed batch without in-memory authority."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        dex_source: ScheduledDexSource,
        scheduler: AdaptiveScheduler,
        lifecycle_classifier: LifecycleObservationHandler,
        *,
        logger: structlog.stdlib.BoundLogger | None = None,
        candidate_orchestrator: CandidateObservationHandler | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._dex_source = dex_source
        self._scheduler = scheduler
        self._lifecycle_classifier = lifecycle_classifier
        self._logger = logger or structlog.get_logger("pump_research.collection.polling")
        self._candidate_orchestrator = candidate_orchestrator
        self._requests = ApiRequestLogRepository()
        self._pairs = PairRepository()
        self._observations = ObservationRepository()
        self._pair_facts = PairFactRepository()
        self._metadata = TokenMetadataRepository()
        self._boosts = BoostRepository()

    async def execute(
        self, claim: PollBatchClaim, *, collector_run_id: uuid.UUID
    ) -> PollExecutionResult:
        """Execute one bounded claim; database facts and completion commit together."""
        requested_at = datetime.now(UTC)
        try:
            result = await self._dex_source.fetch_token_pairs(
                chain_id=claim.chain,
                token_addresses=list(claim.token_addresses),
            )
            self._validate(result, claim)
        except Exception as error:
            return await self._persist_failure(claim, collector_run_id, requested_at, error)

        batch = result.batches[0]
        classification_error: Exception | None = None
        async with self._session_factory() as session, session.begin():
            # This transaction writes token-referencing evidence before it locks
            # PollSchedule during completion. Enter the shared side of the Phase 6
            # fence protocol before either resource family is touched.
            await lock_schedule_token_fk_path(session, exclusive=False)
            build = await self._observation_creates(session, claim, result)
            empty_addresses = [
                member.address
                for member in claim.members
                if member.token_id not in build.observed_token_ids
            ]
            outcome = _coverage_outcome(
                member_count=len(claim.members),
                observed_count=len(build.observed_token_ids),
                has_issues=bool(build.issues),
            )
            failure_detail = (
                {
                    "empty_addresses": empty_addresses,
                    "normalization_issues": list(build.issues),
                }
                if outcome is PollOutcome.PARTIAL
                else None
            )
            request = await self._requests.record(
                session,
                collector_run_id=collector_run_id,
                idempotency_key=_key("poll-request", str(claim.batch_id)),
                provider=DEX_SCREENER_PROVIDER,
                endpoint=_ENDPOINT,
                requested_at=requested_at,
                received_at=batch.received_at,
                outcome=outcome.value,
                http_status_code=200,
                request_payload={
                    "chain_id": claim.chain,
                    "token_addresses": list(claim.token_addresses),
                    "provider_attempt_count": batch.attempt_count,
                },
                response_payload={"pairs": list(batch.raw_response)},
                response_payload_sha256=_digest({"pairs": list(batch.raw_response)}),
                failure_detail=failure_detail,
            )
            await self._record_zero_cost_enrichment(
                session,
                request_id=request.id,
                collector_run_id=collector_run_id,
                received_at=batch.received_at,
                normalized_pairs=build.normalized_pairs,
            )
            inserted = await self._observations.record_many(
                session, api_request=request, observations=list(build.creates)
            )
            lifecycle_result: LifecycleRequestEvaluation | None = None
            try:
                async with session.begin_nested():
                    lifecycle_result = await self._lifecycle_classifier.evaluate_request_in_session(
                        session,
                        api_request_log_id=request.id,
                    )
            except Exception as error:
                classification_error = error
                outcome = PollOutcome.PARTIAL
                failure_detail = {
                    **(failure_detail or {}),
                    "lifecycle_evaluation": {
                        "error_type": type(error).__name__,
                        "message": str(error)[:1_000],
                        "observation_count": len(build.creates),
                    },
                }
            if lifecycle_result is not None and lifecycle_result.selection_failures:
                outcome = PollOutcome.PARTIAL
                failure_detail = {
                    **(failure_detail or {}),
                    "lifecycle_evidence_selection": list(lifecycle_result.selection_failures),
                }
            if lifecycle_result is not None and self._candidate_orchestrator is not None:
                try:
                    async with session.begin_nested():
                        await self._candidate_orchestrator.evaluate_request_in_session(
                            session,
                            api_request_log_id=request.id,
                            collector_run_id=collector_run_id,
                        )
                except Exception as error:
                    classification_error = classification_error or error
                    outcome = PollOutcome.PARTIAL
                    failure_detail = {
                        **(failure_detail or {}),
                        "candidate_orchestration": {
                            "error_type": type(error).__name__,
                            "message": str(error)[:1_000],
                        },
                    }
            await self._scheduler.complete_batch_in_session(
                session,
                batch_id=claim.batch_id,
                outcome=outcome,
                api_request_log_id=request.id,
                failure_detail=failure_detail,
            )
        self._logger.info(
            "scheduled_poll_completed",
            batch_id=str(claim.batch_id),
            observation_count=inserted,
            outcome=outcome.value,
        )
        if classification_error is not None:
            raise LifecycleEvaluationError(
                "scheduled observations were preserved but lifecycle evaluation failed"
            ) from classification_error
        return PollExecutionResult(claim.batch_id, inserted, outcome)

    async def _observation_creates(
        self,
        session: AsyncSession,
        claim: PollBatchClaim,
        result: DexScreenerTokenPairsResult,
    ) -> ObservationBuildResult:
        creates: list[ObservationCreate] = []
        normalized_pairs: list[NormalizedPair] = []
        observed_token_ids: set[uuid.UUID] = set()
        issues: list[dict[str, object]] = []
        for member in claim.members:
            for index, pair in _pairs_for_address(result.pairs, claim.chain, member.address):
                if pair.pair_address is None:
                    issues.append(
                        {
                            "kind": "missing_pair_address",
                            "member_address": member.address,
                            "source_record_locator": f"pairs[{index}]",
                        }
                    )
                    continue
                try:
                    stored_pair = await self._pairs.get_or_create(
                        session,
                        token_id=member.token_id,
                        chain=claim.chain,
                        address=pair.pair_address,
                        dex_identifier=pair.dex_id,
                        first_discovered_at=None,
                    )
                except PairTokenMismatchError:
                    # A pair is canonical once linked; retain its observation through
                    # its owning token and make the skipped association explicit.
                    issues.append(
                        {
                            "kind": "pair_token_mismatch",
                            "member_address": member.address,
                            "pair_address": pair.pair_address,
                            "source_record_locator": f"pairs[{index}]",
                        }
                    )
                    continue
                source_record_sha256 = _digest(result.batches[0].raw_response[index])
                creates.append(
                    _observation_create(
                        stored_pair.id,
                        pair,
                        index,
                        source_record_sha256=source_record_sha256,
                    )
                )
                normalized_pairs.append(
                    NormalizedPair(
                        token_id=member.token_id,
                        token_address=member.address,
                        pair_id=stored_pair.id,
                        pair=pair,
                        source_index=index,
                        source_record_sha256=source_record_sha256,
                    )
                )
                observed_token_ids.add(member.token_id)
        return ObservationBuildResult(
            creates=tuple(creates),
            observed_token_ids=frozenset(observed_token_ids),
            issues=tuple(issues),
            normalized_pairs=tuple(normalized_pairs),
        )

    async def _record_zero_cost_enrichment(
        self,
        session: AsyncSession,
        *,
        request_id: uuid.UUID,
        collector_run_id: uuid.UUID,
        received_at: datetime,
        normalized_pairs: tuple[NormalizedPair, ...],
    ) -> None:
        """Persist facts embedded in the already-paid-for pair response."""
        for normalized in normalized_pairs:
            pair = normalized.pair
            locator = f"pairs[{normalized.source_index}]"
            await self._pair_facts.record_if_changed(
                session,
                pair_id=normalized.pair_id,
                collector_run_id=collector_run_id,
                api_request_log_id=request_id,
                provider=DEX_SCREENER_PROVIDER,
                received_at=received_at,
                source_record_locator=locator,
                source_record_sha256=normalized.source_record_sha256,
                fact=PairFactCreate(
                    pair_created_at=_dex_millis_timestamp(pair.pair_created_at),
                    dex_identifier=pair.dex_id,
                    labels=list(pair.labels) if pair.labels is not None else None,
                    base_token_address=pair.base_token.address if pair.base_token else None,
                    base_token_name=pair.base_token.name if pair.base_token else None,
                    base_token_symbol=pair.base_token.symbol if pair.base_token else None,
                    quote_token_address=pair.quote_token.address if pair.quote_token else None,
                    quote_token_name=pair.quote_token.name if pair.quote_token else None,
                    quote_token_symbol=pair.quote_token.symbol if pair.quote_token else None,
                ),
            )
            token_side = _token_side(pair, normalized.token_address)
            metadata = _dex_metadata(pair, token_side)
            await self._metadata.record_if_changed(
                session,
                token_id=normalized.token_id,
                pair_id=normalized.pair_id,
                collector_run_id=collector_run_id,
                api_request_log_id=request_id,
                discovery_event_id=None,
                provider=DEX_SCREENER_PROVIDER,
                source_kind="pair_response",
                source_observed_at=None,
                received_at=received_at,
                source_record_locator=locator,
                source_record_sha256=normalized.source_record_sha256,
                metadata=metadata,
            )
            if (
                pair.boosts is not None
                and "active" in pair.boosts.model_fields_set
                and pair.boosts.active is not None
            ):
                await self._boosts.record_if_changed(
                    session,
                    token_id=normalized.token_id,
                    pair_id=normalized.pair_id,
                    collector_run_id=collector_run_id,
                    api_request_log_id=request_id,
                    provider=DEX_SCREENER_PROVIDER,
                    source_kind="pair_response",
                    feed_rank=None,
                    source_observed_at=None,
                    received_at=received_at,
                    source_record_locator=locator,
                    source_record_sha256=normalized.source_record_sha256,
                    fact=BoostCreate(active_boost_count=pair.boosts.active),
                )

    async def _persist_failure(
        self,
        claim: PollBatchClaim,
        collector_run_id: uuid.UUID,
        requested_at: datetime,
        error: Exception,
    ) -> PollExecutionResult:
        received_at = datetime.now(UTC)
        status_code = error.status_code if isinstance(error, DexScreenerHttpError) else None
        outcome = (
            PollOutcome.THROTTLED
            if status_code == httpx.codes.TOO_MANY_REQUESTS
            else PollOutcome.FAILED
        )
        async with self._session_factory() as session, session.begin():
            await lock_schedule_token_fk_path(session, exclusive=False)
            request = await self._requests.record(
                session,
                collector_run_id=collector_run_id,
                idempotency_key=_key("poll-request-failure", str(claim.batch_id)),
                provider=DEX_SCREENER_PROVIDER,
                endpoint=_ENDPOINT,
                requested_at=requested_at,
                received_at=received_at,
                outcome="throttled" if outcome is PollOutcome.THROTTLED else "failed",
                http_status_code=status_code,
                request_payload={
                    "chain_id": claim.chain,
                    "token_addresses": list(claim.token_addresses),
                    "provider_attempt_count": getattr(error, "dexscreener_attempt_count", 1),
                },
                response_payload=None,
                response_payload_sha256=None,
                failure_detail={"error_type": type(error).__name__, "message": str(error)},
            )
            await self._scheduler.complete_batch_in_session(
                session,
                batch_id=claim.batch_id,
                outcome=outcome,
                api_request_log_id=request.id,
                failure_detail={"error_type": type(error).__name__},
            )
        self._logger.warning(
            "scheduled_poll_failed", batch_id=str(claim.batch_id), error_type=type(error).__name__
        )
        return PollExecutionResult(claim.batch_id, 0, outcome)

    @staticmethod
    def _validate(result: DexScreenerTokenPairsResult, claim: PollBatchClaim) -> None:
        if (
            result.chain_id != claim.chain
            or result.requested_addresses != claim.token_addresses
            or len(result.batches) != 1
        ):
            raise ValueError("scheduled DEX response does not match its durable batch claim")


def _pairs_for_address(
    pairs: tuple[DexScreenerPair, ...], chain: str, address: str
) -> tuple[tuple[int, DexScreenerPair], ...]:
    return tuple(
        (index, pair)
        for index, pair in enumerate(pairs)
        if pair.chain_id == chain
        and (
            (pair.base_token is not None and pair.base_token.address == address)
            or (pair.quote_token is not None and pair.quote_token.address == address)
        )
    )


def _coverage_outcome(*, member_count: int, observed_count: int, has_issues: bool) -> PollOutcome:
    if observed_count == 0 and not has_issues:
        return PollOutcome.EMPTY
    if observed_count == member_count and not has_issues:
        return PollOutcome.SUCCEEDED
    return PollOutcome.PARTIAL


def _observation_create(
    pair_id: uuid.UUID,
    pair: DexScreenerPair,
    index: int,
    *,
    source_record_sha256: str | None = None,
) -> ObservationCreate:
    txns_m5, txns_h1 = pair.txns.get("m5"), pair.txns.get("h1")
    txns_h6, txns_h24 = pair.txns.get("h6"), pair.txns.get("h24")
    return ObservationCreate(
        pair_id=pair_id,
        source_record_locator=f"pairs[{index}]",
        source_record_sha256=source_record_sha256
        or _digest(pair.model_dump(mode="json", by_alias=True)),
        price_usd=pair.price_usd,
        price_native=pair.price_native,
        liquidity_usd=pair.liquidity.usd if pair.liquidity else None,
        market_cap_usd=pair.market_cap,
        fully_diluted_valuation_usd=pair.fdv,
        volume_m5_usd=pair.volume.get("m5"),
        volume_h1_usd=pair.volume.get("h1"),
        volume_h6_usd=pair.volume.get("h6"),
        volume_h24_usd=pair.volume.get("h24"),
        price_change_m5_pct=pair.price_change.get("m5"),
        price_change_h1_pct=pair.price_change.get("h1"),
        price_change_h6_pct=pair.price_change.get("h6"),
        price_change_h24_pct=pair.price_change.get("h24"),
        buys_m5=txns_m5.buys if txns_m5 else None,
        sells_m5=txns_m5.sells if txns_m5 else None,
        buys_h1=txns_h1.buys if txns_h1 else None,
        sells_h1=txns_h1.sells if txns_h1 else None,
        buys_h6=txns_h6.buys if txns_h6 else None,
        sells_h6=txns_h6.sells if txns_h6 else None,
        buys_h24=txns_h24.buys if txns_h24 else None,
        sells_h24=txns_h24.sells if txns_h24 else None,
        liquidity_base=pair.liquidity.base if pair.liquidity else None,
        liquidity_quote=pair.liquidity.quote if pair.liquidity else None,
    )


def _dex_millis_timestamp(value: int | None) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(value / 1_000, tz=UTC)
    except (OverflowError, OSError, ValueError) as error:
        raise ValueError("DEX pairCreatedAt is outside the supported timestamp range") from error


def _token_side(
    pair: DexScreenerPair,
    token_address: str,
) -> object:
    if pair.base_token is not None and pair.base_token.address == token_address:
        return pair.base_token
    if pair.quote_token is not None and pair.quote_token.address == token_address:
        return pair.quote_token
    return None


def _dex_metadata(pair: DexScreenerPair, token_side: object) -> MetadataCreate:
    name = getattr(token_side, "name", None)
    symbol = getattr(token_side, "symbol", None)
    info = pair.info
    website: str | None = None
    twitter: str | None = None
    telegram: str | None = None
    other_links: list[object] = []
    if info is not None:
        for website_item in info.websites or []:
            if website is None and website_item.url:
                website = website_item.url
            if website_item.url:
                other_links.append(
                    {
                        "kind": "website",
                        "label": website_item.label,
                        "url": website_item.url,
                    }
                )
        for social_item in info.socials or []:
            platform = (social_item.platform or "").lower()
            if platform in {"twitter", "x"} and twitter is None:
                twitter = social_item.handle
            elif platform == "telegram" and telegram is None:
                telegram = social_item.handle
            if social_item.handle:
                other_links.append(
                    {
                        "kind": "social",
                        "platform": social_item.platform,
                        "handle": social_item.handle,
                    }
                )
    return MetadataCreate(
        name=name,
        symbol=symbol,
        image_url=info.image_url if info else None,
        header_url=info.header if info else None,
        website_url=website,
        twitter=twitter,
        telegram=telegram,
        other_links=other_links or None,
    )


def _key(*parts: str) -> str:
    return hashlib.sha256(":".join(("scheduled_observation", *parts)).encode()).hexdigest()


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
