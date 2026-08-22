from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.archival import export_epoch_range
from pump_research.config import Settings
from pump_research.epochs import close_epoch, create_epoch, start_epoch
from pump_research.persistence.models import (
    ApiRequestLog,
    DiscoveryEvent,
    LifecycleEvent,
    Observation,
    Pair,
    Token,
)
from pump_research.persistence.repositories import CollectorRunRepository
from pump_research.research.asof import get_token_state_as_of
from pump_research.research.features import build_market_features
from pump_research.research.labels import build_outcome_labels
from pump_research.research.sources import (
    DuckDBArchiveResearchSource,
    PostgresResearchSource,
)

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _settings() -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://unused:unused@localhost/unused",
        environment="test",
    )


@pytest.mark.asyncio
async def test_hot_postgres_and_cold_parquet_produce_identical_as_of_research(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    address = await _seed_closed_epoch(session_factory)
    hot = PostgresResearchSource(session_factory)
    hot_histories = await hot.load_histories(epoch_number=2, token_addresses=[address])
    assert len(hot_histories) == 1
    manifest = await export_epoch_range(
        session_factory,
        epoch_number=2,
        start_at=NOW,
        end_at=NOW + timedelta(minutes=5),
        output=tmp_path / "archive",
        chunk_rows=100,
        minimum_free_bytes=1,
    )
    cold = DuckDBArchiveResearchSource([manifest])
    cold_histories = await cold.load_histories(epoch_number=2, token_addresses=[address])
    assert len(cold_histories) == 1
    decision = NOW + timedelta(minutes=2)
    hot_state = get_token_state_as_of(hot_histories[0], decision)
    cold_state = get_token_state_as_of(cold_histories[0], decision)
    assert build_market_features(hot_state).values == build_market_features(cold_state).values
    assert (
        build_outcome_labels(hot_histories[0], hot_state).values
        == build_outcome_labels(cold_histories[0], cold_state).values
    )


async def _seed_closed_epoch(session_factory: async_sessionmaker[AsyncSession]) -> str:
    async with session_factory() as session, session.begin():
        epoch = await create_epoch(
            session,
            _settings(),
            epoch_number=2,
            purpose="Phase 4 isolated hot/cold equivalence",
            now=NOW,
        )
        await start_epoch(session, epoch_number=2, now=NOW)
        run = await CollectorRunRepository().start(
            session,
            started_at=NOW,
            collector_version="phase4-test",
            configuration_sha256="a" * 64,
            configuration_snapshot={"phase": 4},
            collection_epoch_id=epoch.id,
        )
        token = Token(chain="solana", address="phase4-token")
        session.add(token)
        await session.flush()
        pair = Pair(token_id=token.id, chain="solana", address="phase4-pair")
        session.add(pair)
        session.add(
            DiscoveryEvent(
                collector_run_id=run.id,
                token_id=token.id,
                idempotency_key="phase4-discovery",
                provider="fixture",
                provider_event_id="1",
                event_type="token_created",
                source_event_at=NOW - timedelta(days=1),
                received_at=NOW,
                source_payload={"mint": token.address},
                source_payload_sha256="b" * 64,
            )
        )
        session.add(
            LifecycleEvent(
                collector_run_id=run.id,
                token_id=token.id,
                idempotency_key="phase4-new",
                previous_state="PENDING_DEX",
                new_state="NEW",
                decided_at=NOW + timedelta(seconds=10),
                input_watermark=NOW + timedelta(seconds=10),
                reason_code="dex_pair_present",
                configuration_sha256="c" * 64,
                configuration_snapshot={"phase": 4},
            )
        )
        for index, minute in enumerate((1, 2, 3, 4)):
            received = NOW + timedelta(minutes=minute)
            request = ApiRequestLog(
                collector_run_id=run.id,
                idempotency_key=f"phase4-request-{index}",
                provider="dex_screener",
                endpoint="tokens/v1",
                requested_at=received - timedelta(seconds=1),
                received_at=received,
                outcome="succeeded",
                http_status_code=200,
                response_payload={"pairs": []},
                response_payload_sha256=f"{index + 1}" * 64,
            )
            session.add(request)
            await session.flush()
            session.add(
                Observation(
                    received_at=received,
                    pair_id=pair.id,
                    api_request_log_id=request.id,
                    source_observed_at=NOW - timedelta(days=1),
                    price_usd=Decimal(index + 1),
                    liquidity_usd=Decimal("10000") - Decimal(index * 100),
                    market_cap_usd=Decimal(index + 1) * Decimal("1000000"),
                    volume_m5_usd=Decimal(index + 1) * Decimal("100"),
                    buys_m5=10 + index,
                    sells_m5=2,
                )
            )
        run_id = run.id
        address = token.address
    async with session_factory() as session, session.begin():
        await CollectorRunRepository().finish(
            session,
            run_id=run_id,
            finished_at=NOW + timedelta(minutes=5),
            status="stopped",
        )
        await close_epoch(
            session,
            epoch_number=2,
            status="completed",
            reason="complete Phase 4 equivalence fixture",
            now=NOW + timedelta(minutes=5),
        )
    return address
