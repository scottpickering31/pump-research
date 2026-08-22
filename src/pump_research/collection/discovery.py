"""Atomic discovery ingestion and durable provider checkpoint coordination."""

from __future__ import annotations

import uuid
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.collection.dex_availability import DiscoveryAdmission
from pump_research.discovery.contracts import (
    DiscoveredToken,
    DiscoveryBatch,
    DiscoveryCheckpoint,
    TokenDiscoverySource,
)
from pump_research.persistence.repositories import (
    DiscoveryCheckpointRepository,
    DiscoveryConnectivityEventRepository,
)


class DiscoveryAdmissionSink(Protocol):
    """Transactional boundary required by provider-neutral discovery ingestion."""

    async def admit_discovery_in_session(
        self,
        session: AsyncSession,
        event: DiscoveredToken,
        collector_run_id: uuid.UUID | None = None,
    ) -> DiscoveryAdmission:
        """Persist an event and its initial durable work in the caller transaction."""


class DiscoveryCoordinator:
    """Persist a complete fetched batch before advancing its opaque checkpoint."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        source: TokenDiscoverySource,
        admission_sink: DiscoveryAdmissionSink,
    ) -> None:
        self._session_factory = session_factory
        self._source = source
        self._admission_sink = admission_sink
        self._checkpoints = DiscoveryCheckpointRepository()
        self._connectivity_events = DiscoveryConnectivityEventRepository()

    async def run_once(self, *, collector_run_id: uuid.UUID | None = None) -> DiscoveryBatch:
        """Fetch from the durable cursor and atomically commit events plus next cursor."""
        async with self._session_factory() as session:
            checkpoint_state = await self._checkpoints.get(
                session,
                source_name=self._source.source_name,
            )
        checkpoint = (
            DiscoveryCheckpoint(checkpoint_state.checkpoint_value)
            if checkpoint_state is not None
            else None
        )
        batch = await self._source.fetch(checkpoint)
        if any(event.source_name != self._source.source_name for event in batch.events):
            msg = "Discovery source returned an event under another provider namespace"
            raise ValueError(msg)
        if any(
            event.source_name != self._source.source_name for event in batch.connectivity_events
        ):
            msg = "Discovery source returned connectivity evidence under another namespace"
            raise ValueError(msg)

        async with self._session_factory() as session, session.begin():
            for discovery_event in batch.events:
                await self._admission_sink.admit_discovery_in_session(
                    session, discovery_event, collector_run_id
                )
            for connectivity_event in batch.connectivity_events:
                await self._connectivity_events.record(
                    session,
                    source_name=connectivity_event.source_name,
                    gap_id=connectivity_event.gap_id,
                    event_type=connectivity_event.event_type.value,
                    observed_at=connectivity_event.observed_at,
                    reason=connectivity_event.reason,
                    detail=dict(connectivity_event.detail),
                    idempotency_key=connectivity_event.idempotency_key,
                )
            if batch.next_checkpoint is not None:
                await self._checkpoints.advance(
                    session,
                    source_name=self._source.source_name,
                    checkpoint_value=batch.next_checkpoint.value,
                    batch_received_at=batch.received_at,
                    coverage_status=batch.coverage.status.value,
                    supports_replay=batch.coverage.supports_replay,
                    coverage_note=batch.coverage.note,
                )
        await self._source.acknowledge(batch)
        return batch
