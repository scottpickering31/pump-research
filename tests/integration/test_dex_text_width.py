"""Epoch 14 width regressions using synthetic provider evidence and real PostgreSQL."""

from __future__ import annotations

import asyncio
import uuid
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, insert, select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.collection.boosts import BoostCollectionWorkflow, _feed_metadata
from pump_research.collection.dex_availability import DexAvailabilityWorkflow
from pump_research.collection.polling import ScheduledObservationWorkflow, _dex_metadata
from pump_research.config import Settings
from pump_research.discovery.contracts import DiscoveredToken
from pump_research.lifecycle.classifier import LifecycleClassifier
from pump_research.market_data.dexscreener import (
    DexScreenerBatchResult,
    DexScreenerBoostFeedResult,
    DexScreenerTokenPairsResult,
)
from pump_research.market_data.dexscreener_models import (
    DexScreenerBoostFeedRecord,
    DexScreenerPair,
)
from pump_research.persistence.enrichment import (
    EnrichmentIdentityConflictError,
    MetadataCreate,
    PairFactCreate,
    PairFactRepository,
    TokenMetadataRepository,
    canonical_digest,
)
from pump_research.persistence.models import (
    ApiRequestLog,
    DiscoveryEvent,
    Observation,
    Pair,
    PairFactEvent,
    PollBatchOutcome,
    TokenMetadataEvent,
)
from pump_research.persistence.repositories import CollectorRunRepository, TokenRepository
from pump_research.scheduling.policy import LifecycleState
from pump_research.scheduling.scheduler import AdaptiveScheduler, PollBatchClaim, PollOutcome

pytestmark = pytest.mark.integration
NOW = datetime(2026, 9, 11, 18, 56, tzinfo=UTC)
# Production's exact sentence was not supplied; reproduce its shape and width.
LONG_SYMBOL = "This provider controlled token symbol is a sentence far beyond the limit. 🚀 " * 80


@dataclass
class WidthClock:
    current: datetime = NOW

    def now(self) -> datetime:
        return self.current


class PairSource:
    def __init__(self, records: list[dict[str, Any]], clock: WidthClock) -> None:
        self.records = records
        self.clock = clock

    async def fetch_token_pairs(
        self, *, chain_id: str, token_addresses: list[str]
    ) -> DexScreenerTokenPairsResult:
        addresses = tuple(token_addresses)
        raw = tuple(deepcopy(self.records))
        return DexScreenerTokenPairsResult(
            chain_id,
            addresses,
            (
                DexScreenerBatchResult(
                    chain_id=chain_id,
                    requested_addresses=addresses,
                    pairs=tuple(DexScreenerPair.model_validate(record) for record in raw),
                    received_at=self.clock.now(),
                    raw_response=raw,
                ),
            ),
        )


async def subject(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[AdaptiveScheduler, PollBatchClaim, uuid.UUID, WidthClock, Settings]:
    clock = WidthClock()
    settings = Settings(_env_file=None, database_url="postgresql+asyncpg://unused/unused_test")
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    async with session_factory() as session, session.begin():
        run = await CollectorRunRepository().start(
            session,
            started_at=NOW,
            collector_version="epoch14-width-test",
            configuration_sha256="a" * 64,
            configuration_snapshot={},
        )
        token = await TokenRepository().get_or_create(
            session,
            chain="solana",
            address="width-token",
            first_discovered_at=NOW,
        )
        await scheduler.set_lifecycle_state_in_session(
            session,
            token_id=token.id,
            state=LifecycleState.NEW,
            decided_at=NOW,
            reason_code="test_setup",
        )
    clock.current += timedelta(seconds=settings.scheduler_new_interval_seconds)
    claim = await scheduler.claim_next_batch()
    assert claim is not None
    return scheduler, claim, run.id, clock, settings


def pair_record(mode: str = "ordinary") -> dict[str, Any]:
    def display(value: str, width: int) -> str:
        if mode == "boundary":
            return "🚀" * width
        if mode == "all":
            return LONG_SYMBOL
        return value

    return {
        "chainId": "solana",
        "pairAddress": "width-pair",
        "dexId": display("pumpfun", 128),
        "url": LONG_SYMBOL,
        "baseToken": {
            "address": "width-token",
            "name": display(" Ordinary Name ", 512),
            "symbol": LONG_SYMBOL if mode == "symbol" else display("PUMP", 128),
        },
        "quoteToken": {
            "address": "SOL",
            "name": display("Solana", 512),
            "symbol": display("SOL", 128),
        },
        "labels": [LONG_SYMBOL],
        "info": {
            "imageUrl": display("https://example.test/image", 4096),
            "header": display("https://example.test/header", 4096),
            "openGraph": LONG_SYMBOL,
            "websites": [{"label": LONG_SYMBOL, "url": display("https://example.test", 4096)}],
            "socials": [
                {"platform": "twitter", "handle": display("@pump", 2048)},
                {"platform": "telegram", "handle": display("pump", 2048)},
                {"platform": LONG_SYMBOL, "handle": LONG_SYMBOL},
            ],
        },
        "priceUsd": "0.01",
        "liquidity": {"usd": "2000"},
        "volume": {"m5": "200", "h1": "300"},
    }


def pair_fact(raw: dict[str, Any]) -> PairFactCreate:
    return PairFactCreate(
        pair_created_at=None,
        dex_identifier=raw["dexId"],
        labels=raw["labels"],
        base_token_address=raw["baseToken"]["address"],
        base_token_name=raw["baseToken"]["name"],
        base_token_symbol=raw["baseToken"]["symbol"],
        quote_token_address=raw["quoteToken"]["address"],
        quote_token_name=raw["quoteToken"]["name"],
        quote_token_symbol=raw["quoteToken"]["symbol"],
    )


@pytest.mark.parametrize("mode", ["ordinary", "boundary", "symbol", "all"])
async def test_scheduled_text_width_preserves_raw_and_completes_task_group(
    session_factory: async_sessionmaker[AsyncSession],
    mode: str,
) -> None:
    scheduler, claim, run_id, clock, settings = await subject(session_factory)
    raw = pair_record(mode)
    workflow = ScheduledObservationWorkflow(
        session_factory,
        PairSource([raw], clock),
        scheduler,
        LifecycleClassifier(session_factory, settings, clock=clock),
    )
    sibling_completed = asyncio.Event()

    async def sibling() -> None:
        await asyncio.sleep(0)
        sibling_completed.set()

    # Exercise the real scheduled write path in a TaskGroup, with no DB exception catch.
    async with asyncio.TaskGroup() as group:
        task = group.create_task(workflow.execute(claim, collector_run_id=run_id))
        group.create_task(sibling())
    assert sibling_completed.is_set()
    assert task.result().outcome is PollOutcome.SUCCEEDED
    assert task.result().observations_written == 1
    expected_fact = asdict(pair_fact(raw))
    typed = DexScreenerPair.model_validate(raw)
    expected_metadata = asdict(_dex_metadata(typed, typed.base_token))
    async with session_factory() as session:
        fact = (await session.scalars(select(PairFactEvent))).one()
        metadata = (await session.scalars(select(TokenMetadataEvent))).one()
        pair = (await session.scalars(select(Pair))).one()
        request = await session.get(ApiRequestLog, fact.api_request_log_id)
        observation = (await session.scalars(select(Observation))).one()
        outcome = await session.get(PollBatchOutcome, claim.batch_id)
        for row, content in ((fact, expected_fact), (metadata, expected_metadata)):
            assert row.content_sha256 == canonical_digest(content)
            assert row.source_record_sha256 == canonical_digest(raw)
            assert row.source_record_locator == "pairs[0]"
            for key, value in content.items():
                column = row.__table__.c[key]
                width = getattr(column.type, "length", None)
                expected = value[:width] if isinstance(value, str) and width else value
                assert getattr(row, key) == expected
        assert pair.address == "width-pair"
        assert pair.dex_identifier == raw["dexId"][:128]
        assert request is not None
        assert request.response_payload == {"pairs": [raw]}
        assert request.response_payload_sha256 == canonical_digest({"pairs": [raw]})
        assert metadata.api_request_log_id == request.id == observation.api_request_log_id
        assert observation.source_record_sha256 == canonical_digest(raw)
        assert outcome is not None and outcome.outcome == "succeeded"

        if mode == "symbol":
            # The migrated schema still rejects the exact class of failing insert.
            # A savepoint isolates this deliberate reproduction from the fixed path.
            values = {column.name: getattr(fact, column.name) for column in fact.__table__.c}
            values.update(
                id=uuid.uuid4(), idempotency_key="unbounded-repro", base_token_symbol=LONG_SYMBOL
            )
            with pytest.raises(DBAPIError) as error:
                async with session.begin_nested():
                    await session.execute(insert(PairFactEvent).values(**values))
            assert getattr(error.value.orig, "sqlstate", None) == "22001"
            assert "character varying(128)" in str(error.value.orig)


@pytest.mark.parametrize("with_valid_pair", [True, False])
@pytest.mark.parametrize("field", ["chainId", "pairAddress", "baseToken", "quoteToken"])
async def test_oversized_pair_identity_is_partial_without_truncating_addresses(
    session_factory: async_sessionmaker[AsyncSession],
    field: str,
    with_valid_pair: bool,
) -> None:
    scheduler, claim, run_id, clock, settings = await subject(session_factory)
    invalid = pair_record()
    if field in {"baseToken", "quoteToken"}:
        invalid[field]["address"] = LONG_SYMBOL
    else:
        invalid[field] = LONG_SYMBOL
    records = [invalid, pair_record()] if with_valid_pair else [invalid]
    workflow = ScheduledObservationWorkflow(
        session_factory,
        PairSource(records, clock),
        scheduler,
        LifecycleClassifier(session_factory, settings, clock=clock),
    )
    result = await workflow.execute(claim, collector_run_id=run_id)
    assert result.outcome is PollOutcome.PARTIAL
    assert result.observations_written == int(with_valid_pair)
    async with session_factory() as session:
        request = (await session.scalars(select(ApiRequestLog))).one()
        pair = (await session.scalars(select(Pair))).one_or_none()
        outcome = await session.get(PollBatchOutcome, claim.batch_id)
        assert (pair is not None) is with_valid_pair
        if pair is not None:
            assert pair.address == "width-pair"
        assert request.response_payload == {"pairs": records}
        assert request.failure_detail is not None
        issue = request.failure_detail["normalization_issues"]
        assert issue == [
            {
                "kind": "oversized_pair_identity",
                "field": f"{field}.address" if field.endswith("Token") else field,
                "length": len(LONG_SYMBOL),
                "max_length": 32 if field == "chainId" else 128,
                "source_record_locator": "pairs[0]",
            }
        ]
        assert outcome is not None and outcome.failure_detail == request.failure_detail


async def test_full_value_tail_changes_preserve_content_hashing_and_idempotency(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    scheduler, claim, run_id, clock, settings = await subject(session_factory)
    raw = pair_record("symbol")
    source = PairSource([raw], clock)
    workflow = ScheduledObservationWorkflow(
        session_factory,
        source,
        scheduler,
        LifecycleClassifier(session_factory, settings, clock=clock),
    )
    for suffix in ("A", "B", "B"):
        raw["baseToken"]["symbol"] = LONG_SYMBOL + suffix
        await workflow.execute(claim, collector_run_id=run_id)
        clock.current += timedelta(hours=1)
        next_claim = await scheduler.claim_next_batch()
        assert next_claim is not None
        claim = next_claim
    async with session_factory() as session, session.begin():
        facts = list(
            (await session.scalars(select(PairFactEvent).order_by(PairFactEvent.received_at))).all()
        )
        metadata = list(
            (
                await session.scalars(
                    select(TokenMetadataEvent).order_by(TokenMetadataEvent.received_at)
                )
            ).all()
        )
        assert len(facts) == len(metadata) == 2
        assert facts[0].base_token_symbol == facts[1].base_token_symbol == LONG_SYMBOL[:128]
        assert facts[0].content_sha256 != facts[1].content_sha256
        assert metadata[0].content_sha256 != metadata[1].content_sha256
        assert await session.scalar(select(func.count()).select_from(Observation)) == 3
        assert await session.scalar(select(func.count()).select_from(ApiRequestLog)) == 3
        fact = facts[-1]

        async def record_fact(value: PairFactCreate) -> PairFactEvent | None:
            return await PairFactRepository().record_if_changed(
                session,
                pair_id=fact.pair_id,
                collector_run_id=run_id,
                api_request_log_id=fact.api_request_log_id,
                provider="dexscreener",
                received_at=fact.received_at,
                source_record_locator="pairs[0]",
                source_record_sha256=canonical_digest(raw),
                fact=value,
            )

        assert await record_fact(pair_fact(raw)) is None
        raw["baseToken"]["symbol"] = LONG_SYMBOL + "conflicting tail"
        with pytest.raises(EnrichmentIdentityConflictError):
            async with session.begin_nested():
                await record_fact(pair_fact(raw))


class FeedSource:
    def __init__(self, raw: dict[str, Any]) -> None:
        self.raw = raw

    async def fetch_boost_feed(self, *, feed_kind: str) -> DexScreenerBoostFeedResult:
        return DexScreenerBoostFeedResult(
            feed_kind=feed_kind,
            records=(DexScreenerBoostFeedRecord.model_validate(self.raw),),
            received_at=NOW,
            raw_response=(self.raw,),
        )


async def test_boost_metadata_width_preserves_full_links_and_evidence(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, _, run_id, _, _ = await subject(session_factory)
    raw = {
        "chainId": "solana",
        "tokenAddress": "width-token",
        "amount": 1,
        "url": LONG_SYMBOL,
        "description": LONG_SYMBOL,
        "icon": LONG_SYMBOL,
        "header": LONG_SYMBOL,
        "links": [
            {"type": kind, "label": LONG_SYMBOL, "url": LONG_SYMBOL}
            for kind in ("website", "twitter", "telegram")
        ],
    }
    result = await BoostCollectionWorkflow(session_factory, FeedSource(raw)).collect(
        feed_kind="latest",
        collector_run_id=run_id,
        requested_at=NOW,
    )
    assert result.metadata_changes == result.boost_changes == 1
    async with session_factory() as session:
        metadata = (await session.scalars(select(TokenMetadataEvent))).one()
        request = await session.get(ApiRequestLog, metadata.api_request_log_id)
        assert metadata.image_url == metadata.header_url == LONG_SYMBOL[:4096]
        assert metadata.website_url == LONG_SYMBOL[:4096]
        assert metadata.twitter == metadata.telegram == LONG_SYMBOL[:2048]
        assert metadata.other_links == raw["links"]
        assert metadata.content_sha256 == canonical_digest(
            asdict(_feed_metadata(DexScreenerBoostFeedRecord.model_validate(raw)))
        )
        assert request is not None and request.response_payload == {"records": [raw]}
        assert request.response_payload_sha256 == canonical_digest({"records": [raw]})


async def test_metadata_uri_width_and_database_errors_remain_visible(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    scheduler, claim, run_id, clock, settings = await subject(session_factory)
    raw = pair_record()
    raw["uri"] = LONG_SYMBOL
    await ScheduledObservationWorkflow(
        session_factory,
        PairSource([raw], clock),
        scheduler,
        LifecycleClassifier(session_factory, settings, clock=clock),
    ).execute(claim, collector_run_id=run_id)
    async with session_factory() as session, session.begin():
        request = (await session.scalars(select(ApiRequestLog))).one()
        metadata = MetadataCreate(metadata_uri=LONG_SYMBOL)
        stored = await TokenMetadataRepository().record_if_changed(
            session,
            token_id=claim.members[0].token_id,
            pair_id=None,
            collector_run_id=run_id,
            api_request_log_id=request.id,
            discovery_event_id=None,
            provider="dexscreener",
            source_kind="pair_response",
            source_observed_at=None,
            received_at=clock.now(),
            source_record_locator="pairs[0]",
            source_record_sha256=canonical_digest(raw),
            metadata=metadata,
        )
        assert request.response_payload == {"pairs": [raw]}
        assert stored is not None and stored.metadata_uri == LONG_SYMBOL[:4096]
        assert stored.content_sha256 == canonical_digest(asdict(metadata))
        # Foreign-key failures are still fatal to the caller, never swallowed.
        with pytest.raises(DBAPIError) as error:
            async with session.begin_nested():
                await TokenMetadataRepository().record_if_changed(
                    session,
                    token_id=uuid.uuid4(),
                    pair_id=None,
                    collector_run_id=run_id,
                    api_request_log_id=request.id,
                    discovery_event_id=None,
                    provider="dexscreener",
                    source_kind="pair_response",
                    source_observed_at=None,
                    received_at=clock.now(),
                    source_record_locator="pairs[0]",
                    source_record_sha256="a" * 64,
                    metadata=metadata,
                )
        assert getattr(error.value.orig, "sqlstate", None) == "23503"


async def test_discovery_metadata_width_preserves_payload_and_metadata_idempotency(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, _, run_id, clock, settings = await subject(session_factory)
    payload = {
        field: LONG_SYMBOL
        for field in (
            "name",
            "symbol",
            "uri",
            "image",
            "website",
            "twitter",
            "telegram",
        )
    }
    event = DiscoveredToken(
        chain="solana",
        address="discovered-width-token",
        source_name="pumpportal",
        source_event_id="width-event",
        event_type="token_created",
        source_event_at=None,
        received_at=clock.now(),
        source_payload=payload,
        source_payload_sha256=canonical_digest(payload),
        idempotency_key="width-event",
    )
    await DexAvailabilityWorkflow(session_factory, PairSource([], clock), settings).admit_discovery(
        event,
        collector_run_id=run_id,
    )
    async with session_factory() as session, session.begin():
        row = (await session.scalars(select(TokenMetadataEvent))).one()
        discovery = await session.get(DiscoveryEvent, row.discovery_event_id)
        assert discovery is not None and discovery.source_payload == payload
        assert discovery.source_payload_sha256 == canonical_digest(payload)
        assert row.source_record_sha256 == canonical_digest(payload)
        assert row.api_request_log_id is None
        assert row.name == LONG_SYMBOL[:512]
        assert row.symbol == LONG_SYMBOL[:128]
        assert row.metadata_uri == row.image_url == row.website_url == LONG_SYMBOL[:4096]
        assert row.twitter == row.telegram == LONG_SYMBOL[:2048]
        metadata = MetadataCreate(
            name=LONG_SYMBOL,
            symbol=LONG_SYMBOL,
            metadata_uri=LONG_SYMBOL,
            image_url=LONG_SYMBOL,
            website_url=LONG_SYMBOL,
            twitter=LONG_SYMBOL,
            telegram=LONG_SYMBOL,
        )
        assert row.content_sha256 == canonical_digest(asdict(metadata))

        async def record_metadata(value: MetadataCreate) -> TokenMetadataEvent | None:
            return await TokenMetadataRepository().record_if_changed(
                session,
                token_id=row.token_id,
                pair_id=None,
                collector_run_id=run_id,
                api_request_log_id=None,
                discovery_event_id=discovery.id,
                provider="pumpportal",
                source_kind="discovery",
                source_observed_at=None,
                received_at=clock.now(),
                source_record_locator="discovery_event",
                source_record_sha256=canonical_digest(payload),
                metadata=value,
            )

        assert await record_metadata(metadata) is None
        conflicting = MetadataCreate(**{**asdict(metadata), "symbol": LONG_SYMBOL + "new tail"})
        with pytest.raises(EnrichmentIdentityConflictError):
            async with session.begin_nested():
                await record_metadata(conflicting)
