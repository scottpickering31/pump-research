from __future__ import annotations

from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from pump_research.collection.dex_availability import DexAvailabilityWorkflow
from pump_research.collection.discovery import DiscoveryCoordinator
from pump_research.config import Settings
from pump_research.database import check_database_health, create_database_engine
from pump_research.discovery.contracts import (
    DiscoveredToken,
    DiscoveryBatch,
    DiscoveryCheckpoint,
    DiscoveryCoverage,
    DiscoveryCoverageStatus,
    DiscoverySourceError,
    TokenDiscoverySource,
)
from pump_research.market_data.dexscreener import DexScreenerTokenPairsResult
from pump_research.persistence.models import ApiRequestLog, Observation
from pump_research.persistence.repositories import (
    ApiRequestLogRepository,
    DiscoveryCheckpointRepository,
    ObservationCreate,
    ObservationRepository,
    PairRepository,
    TokenRepository,
)


def _settings(database_url: str) -> Settings:
    return Settings(database_url=database_url, database_connect_timeout_seconds=2)


class NeverCalledDexSource:
    async def fetch_token_pairs(
        self,
        *,
        chain_id: str,
        token_addresses: list[str],
    ) -> DexScreenerTokenPairsResult:
        raise AssertionError("Discovery checkpoint tests must not call DEX Screener")


class FakeDiscoverySource(TokenDiscoverySource):
    def __init__(
        self,
        *,
        result: DiscoveryBatch | None = None,
        error: DiscoverySourceError | None = None,
    ) -> None:
        self._result = result
        self._error = error
        self.seen_checkpoints: list[DiscoveryCheckpoint | None] = []

    @property
    def source_name(self) -> str:
        return "restartable-discovery"

    async def fetch(self, checkpoint: DiscoveryCheckpoint | None = None) -> DiscoveryBatch:
        self.seen_checkpoints.append(checkpoint)
        if self._error is not None:
            raise self._error
        assert self._result is not None
        return self._result

    async def aclose(self) -> None:
        return None


def _discovery_batch(
    *,
    received_at: datetime,
    checkpoint: str,
    events: tuple[DiscoveredToken, ...] = (),
) -> DiscoveryBatch:
    return DiscoveryBatch(
        events=events,
        received_at=received_at,
        coverage=DiscoveryCoverage(
            status=DiscoveryCoverageStatus.BEST_EFFORT,
            supports_replay=False,
            note="test feed",
        ),
        next_checkpoint=DiscoveryCheckpoint(checkpoint),
        not_modified=not events,
    )


@pytest.mark.integration
async def test_postgres_connection_interruption_is_loud_and_new_connection_recovers(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    engine = session_factory.kw["bind"]
    assert engine is not None
    database_url = engine.url.render_as_string(hide_password=False)
    resilient_engine = create_database_engine(_settings(database_url))
    killer_engine = create_async_engine(database_url)
    victim = await resilient_engine.connect()
    try:
        backend_pid = int((await victim.execute(text("SELECT pg_backend_pid()"))).scalar_one())
        async with killer_engine.connect() as killer:
            terminated = bool(
                (
                    await killer.execute(
                        text("SELECT pg_terminate_backend(:backend_pid)"),
                        {"backend_pid": backend_pid},
                    )
                ).scalar_one()
            )
        assert terminated is True
        with pytest.raises(DBAPIError):
            await victim.execute(text("SELECT 1"))
    finally:
        with suppress(DBAPIError):
            await victim.close()

    health = await check_database_health(resilient_engine)
    assert health.server_version
    await killer_engine.dispose()
    await resilient_engine.dispose()


@pytest.mark.integration
async def test_duplicate_api_response_is_idempotent_but_later_observation_is_retained(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    now = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
    tokens = TokenRepository()
    pairs = PairRepository()
    requests = ApiRequestLogRepository()
    observations = ObservationRepository()
    async with session_factory() as session, session.begin():
        token = await tokens.get_or_create(
            session,
            chain="solana",
            address="duplicate-api-token",
            first_discovered_at=now,
        )
        pair = await pairs.get_or_create(
            session,
            token_id=token.id,
            chain="solana",
            address="duplicate-api-pair",
            dex_identifier="test-dex",
            first_discovered_at=now,
        )
        request_values = {
            "idempotency_key": "duplicate-api-request",
            "provider": "test-market-data",
            "endpoint": "/tokens/v1/solana/duplicate-api-token",
            "requested_at": now,
            "received_at": now,
            "outcome": "succeeded",
            "http_status_code": 200,
            "request_payload": {"addresses": [token.address]},
            "response_payload": {"pairs": [{"address": pair.address}]},
            "response_payload_sha256": "a" * 64,
        }
        request = await requests.record(session, **request_values)
        duplicate_request = await requests.record(session, **request_values)
        observation = ObservationCreate(
            pair_id=pair.id,
            price_usd=Decimal("0.001"),
            volume_m5_usd=Decimal("12.5"),
        )
        assert request.id == duplicate_request.id
        assert (
            await observations.record_many(
                session,
                api_request=request,
                observations=[observation],
            )
            == 1
        )
        assert (
            await observations.record_many(
                session,
                api_request=duplicate_request,
                observations=[observation],
            )
            == 0
        )

        later_request = await requests.record(
            session,
            **{
                **request_values,
                "idempotency_key": "later-unchanged-api-request",
                "requested_at": now.replace(second=1),
                "received_at": now.replace(second=1),
            },
        )
        assert (
            await observations.record_many(
                session,
                api_request=later_request,
                observations=[observation],
            )
            == 1
        )

    async with session_factory() as session:
        request_count = await session.scalar(select(func.count()).select_from(ApiRequestLog))
        observation_count = await session.scalar(
            select(func.count()).select_from(Observation)
        )
    assert request_count == 2
    assert observation_count == 2


@pytest.mark.integration
async def test_discovery_disconnect_preserves_checkpoint_and_restart_resumes_from_it(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    now = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
    engine = session_factory.kw["bind"]
    assert engine is not None
    settings = _settings(engine.url.render_as_string(hide_password=False))
    admission = DexAvailabilityWorkflow(
        session_factory,
        NeverCalledDexSource(),
        settings,
    )
    event = DiscoveredToken(
        chain="solana",
        address="checkpointed-discovery-token",
        source_name="restartable-discovery",
        source_event_id="event-1",
        event_type="token_created",
        source_event_at=now,
        received_at=now,
        source_payload={"mint": "checkpointed-discovery-token"},
        source_payload_sha256="b" * 64,
        idempotency_key="checkpointed-event-1",
    )
    first_source = FakeDiscoverySource(
        result=_discovery_batch(
            received_at=now,
            checkpoint="checkpoint-1",
            events=(event,),
        )
    )
    await DiscoveryCoordinator(session_factory, first_source, admission).run_once()
    assert first_source.seen_checkpoints == [None]

    disconnected_source = FakeDiscoverySource(
        error=DiscoverySourceError("simulated discovery disconnect")
    )
    with pytest.raises(DiscoverySourceError, match="disconnect"):
        await DiscoveryCoordinator(
            session_factory,
            disconnected_source,
            admission,
        ).run_once()
    assert disconnected_source.seen_checkpoints == [DiscoveryCheckpoint("checkpoint-1")]

    checkpoints = DiscoveryCheckpointRepository()
    async with session_factory() as session:
        after_disconnect = await checkpoints.get(
            session,
            source_name="restartable-discovery",
        )
    assert after_disconnect is not None
    assert after_disconnect.checkpoint_value == "checkpoint-1"

    restarted_source = FakeDiscoverySource(
        result=_discovery_batch(
            received_at=now.replace(second=1),
            checkpoint="checkpoint-2",
        )
    )
    await DiscoveryCoordinator(session_factory, restarted_source, admission).run_once()
    assert restarted_source.seen_checkpoints == [DiscoveryCheckpoint("checkpoint-1")]
    async with session_factory() as session:
        after_restart = await checkpoints.get(
            session,
            source_name="restartable-discovery",
        )
    assert after_restart is not None
    assert after_restart.checkpoint_value == "checkpoint-2"
