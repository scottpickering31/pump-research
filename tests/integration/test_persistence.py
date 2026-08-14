from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from pump_research.persistence.models import DiscoveryEvent, LifecycleEvent, Observation
from pump_research.persistence.repositories import (
    ApiRequestLogRepository,
    CollectorRunRepository,
    DiscoveryEventRepository,
    LifecycleEventRepository,
    ObservationCreate,
    ObservationRepository,
    PairRepository,
    TokenRepository,
)


@pytest.mark.integration
async def test_persistence_is_idempotent_and_observations_are_immutable(
    session: AsyncSession,
) -> None:
    now = datetime.now(UTC)
    token_repository = TokenRepository()
    pair_repository = PairRepository()
    run_repository = CollectorRunRepository()
    discovery_repository = DiscoveryEventRepository()
    request_repository = ApiRequestLogRepository()
    observation_repository = ObservationRepository()
    lifecycle_repository = LifecycleEventRepository()

    async with session.begin():
        run = await run_repository.start(
            session,
            started_at=now,
            collector_version="test-version",
            configuration_sha256="a" * 64,
            configuration_snapshot={"mode": "integration-test"},
        )
        token = await token_repository.get_or_create(
            session,
            chain="solana",
            address="token-address",
            first_discovered_at=now,
        )
        same_token = await token_repository.get_or_create(
            session,
            chain="solana",
            address="token-address",
            first_discovered_at=now,
        )
        assert token.id == same_token.id

        first_pair = await pair_repository.get_or_create(
            session,
            token_id=token.id,
            chain="solana",
            address="pair-address-one",
            dex_identifier="test-dex",
            first_discovered_at=now,
        )
        second_pair = await pair_repository.get_or_create(
            session,
            token_id=token.id,
            chain="solana",
            address="pair-address-two",
            dex_identifier="test-dex",
            first_discovered_at=now,
        )
        assert first_pair.id != second_pair.id

        discovery_event = await discovery_repository.record(
            session,
            token_id=token.id,
            idempotency_key="discovery:1",
            provider="test-provider",
            provider_event_id="external-1",
            event_type="token_seen",
            source_event_at=now,
            received_at=now,
            source_payload={"address": "token-address"},
            source_payload_sha256="b" * 64,
        )
        duplicate_discovery_event = await discovery_repository.record(
            session,
            token_id=token.id,
            idempotency_key="discovery:1",
            provider="test-provider",
            provider_event_id="external-1",
            event_type="token_seen",
            source_event_at=now,
            received_at=now,
            source_payload={"address": "token-address"},
            source_payload_sha256="b" * 64,
        )
        assert discovery_event.id == duplicate_discovery_event.id

        request = await request_repository.record(
            session,
            collector_run_id=run.id,
            idempotency_key="request:1",
            provider="test-market-data",
            endpoint="/pairs/batch",
            requested_at=now,
            received_at=now,
            outcome="succeeded",
            http_status_code=200,
            request_payload={"addresses": ["token-address"]},
            response_payload={"pairs": [{"address": "pair-address-one"}]},
            response_payload_sha256="c" * 64,
            failure_detail=None,
        )
        inserted_count = await observation_repository.record_many(
            session,
            api_request=request,
            observations=[
                ObservationCreate(
                    pair_id=first_pair.id,
                    source_observed_at=now,
                    source_record_locator="pairs[0]",
                    source_record_sha256="d" * 64,
                    price_usd=Decimal("0.000012345678901234"),
                    liquidity_usd=Decimal("1234.50"),
                    volume_m5_usd=Decimal("12.25"),
                    buys_m5=3,
                    sells_m5=1,
                )
            ],
        )
        assert inserted_count == 1
        assert (
            await observation_repository.record_many(
                session,
                api_request=request,
                observations=[ObservationCreate(pair_id=first_pair.id)],
            )
            == 0
        )

        lifecycle_event = await lifecycle_repository.record(
            session,
            token_id=token.id,
            idempotency_key="lifecycle:1",
            previous_state=None,
            new_state="new",
            decided_at=now,
            input_watermark=now,
            reason_code="initial_state",
            reason_detail={"test": True},
            configuration_sha256="e" * 64,
            configuration_snapshot={"version": 1},
        )
        duplicate_lifecycle_event = await lifecycle_repository.record(
            session,
            token_id=token.id,
            idempotency_key="lifecycle:1",
            previous_state=None,
            new_state="new",
            decided_at=now,
            input_watermark=now,
            reason_code="initial_state",
            reason_detail={"test": True},
            configuration_sha256="e" * 64,
            configuration_snapshot={"version": 1},
        )
        assert lifecycle_event.id == duplicate_lifecycle_event.id
        first_pair_id = first_pair.id
        lifecycle_event_id = lifecycle_event.id

    discovery_count = await session.scalar(select(func.count()).select_from(DiscoveryEvent))
    observation_count = await session.scalar(select(func.count()).select_from(Observation))
    lifecycle_count = await session.scalar(select(func.count()).select_from(LifecycleEvent))
    assert discovery_count == 1
    assert observation_count == 1
    assert lifecycle_count == 1
    await session.rollback()

    with pytest.raises(DBAPIError, match="immutable"):
        async with session.begin():
                await session.execute(
                    update(Observation)
                .where(Observation.pair_id == first_pair_id)
                .values(price_usd=Decimal("99"))
            )

    with pytest.raises(DBAPIError, match="immutable"):
        async with session.begin():
                await session.execute(
                    update(LifecycleEvent)
                .where(LifecycleEvent.id == lifecycle_event_id)
                .values(new_state="incorrect")
            )
