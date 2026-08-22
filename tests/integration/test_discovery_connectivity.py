from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.collection.dex_availability import DexAvailabilityWorkflow
from pump_research.collection.discovery import DiscoveryCoordinator
from pump_research.config import Settings
from pump_research.discovery.contracts import (
    DiscoveryBatch,
    DiscoveryCheckpoint,
    DiscoveryConnectivityEvent,
    DiscoveryConnectivityEventType,
    DiscoveryCoverage,
    DiscoveryCoverageStatus,
    TokenDiscoverySource,
)
from pump_research.market_data.dexscreener import DexScreenerTokenPairsResult
from pump_research.persistence.models import DiscoveryConnectivityEvent as PersistedEvent

NOW = datetime(2026, 8, 15, 12, 0, tzinfo=UTC)


class NeverCalledDexSource:
    async def fetch_token_pairs(
        self, *, chain_id: str, token_addresses: list[str]
    ) -> DexScreenerTokenPairsResult:
        del chain_id, token_addresses
        raise AssertionError("DEX source must not be called while persisting gap evidence")


class GapSource(TokenDiscoverySource):
    @property
    def source_name(self) -> str:
        return "pumpportal"

    async def fetch(self, checkpoint: DiscoveryCheckpoint | None = None) -> DiscoveryBatch:
        assert checkpoint is None
        gap_id = "3d42b4fa-a3c6-4df0-934c-5fbdcb55d715"
        events = tuple(
            DiscoveryConnectivityEvent(
                source_name=self.source_name,
                gap_id=gap_id,
                event_type=event_type,
                observed_at=NOW.replace(second=index),
                reason=reason,
                detail={"test": True},
                idempotency_key=hashlib.sha256(
                    f"{gap_id}:{event_type.value}".encode()
                ).hexdigest(),
            )
            for index, (event_type, reason) in enumerate(
                (
                    (DiscoveryConnectivityEventType.DISCONNECTED, "ConnectionError"),
                    (
                        DiscoveryConnectivityEventType.RECONNECTED,
                        "subscription_reestablished",
                    ),
                )
            )
        )
        return DiscoveryBatch(
            events=(),
            connectivity_events=events,
            received_at=NOW.replace(second=2),
            coverage=DiscoveryCoverage(
                status=DiscoveryCoverageStatus.BEST_EFFORT,
                supports_replay=False,
                note="live source; no replay",
            ),
            next_checkpoint=None,
        )

    async def aclose(self) -> None:
        return None


@pytest.mark.integration
async def test_gap_boundaries_are_persisted_idempotently(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    engine = session_factory.kw["bind"]
    assert engine is not None
    settings = Settings(
        database_url=engine.url.render_as_string(hide_password=False),
        pumpportal_api_key="unused-by-this-test",
    )
    admission = DexAvailabilityWorkflow(
        session_factory,
        NeverCalledDexSource(),
        settings,
    )
    coordinator = DiscoveryCoordinator(session_factory, GapSource(), admission)

    await coordinator.run_once()
    await coordinator.run_once()

    async with session_factory() as session:
        records = (
            await session.scalars(
                select(PersistedEvent).order_by(PersistedEvent.observed_at)
            )
        ).all()
        count = await session.scalar(select(func.count()).select_from(PersistedEvent))

    assert count == 2
    assert [record.event_type for record in records] == ["disconnected", "reconnected"]
    assert len({record.gap_id for record in records}) == 1
    assert all(isinstance(record.gap_id, uuid.UUID) for record in records)
