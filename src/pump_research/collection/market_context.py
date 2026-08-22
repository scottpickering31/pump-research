"""Shared five-minute context derived only from facts received by each cutoff."""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.config import Settings
from pump_research.persistence.enrichment import MarketContextRepository
from pump_research.persistence.models import (
    ApiRequestLog,
    CollectorRun,
    LifecycleEvent,
    MarketContextSnapshot,
    Observation,
)


class MarketContextWorkflow:
    """Create one shared, versioned context row for each closed UTC bucket."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._interval = settings.market_context_interval_seconds
        self._policy_snapshot: dict[str, object] = {
            "component": "market_context",
            "schema_version": 1,
            "bucket_seconds": self._interval,
            "sol_price": "median positive price_usd/price_native across latest pair rows",
            "activity_universe": "latest observation per pair received within closed bucket",
            "mature_cohort": "NEW in [cutoff-2h, cutoff-1h), ACTIVE known by cutoff",
            "volatility": "root-mean-square of prior as-of bucket returns over one hour",
        }
        self._policy_sha256 = hashlib.sha256(
            json.dumps(self._policy_snapshot, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        self._repository = MarketContextRepository()

    async def record_closed_bucket(
        self,
        *,
        collector_run_id: uuid.UUID,
        now: datetime | None = None,
    ) -> MarketContextSnapshot:
        """Derive the last closed bucket using receipt time as the knowledge cutoff."""
        received_at = now or datetime.now(UTC)
        if received_at.tzinfo is None or received_at.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        received_at = received_at.astimezone(UTC)
        bucket_end = _floor_time(received_at, self._interval)
        bucket_start = bucket_end - timedelta(seconds=self._interval)
        async with self._session_factory() as session, session.begin():
            epoch_id = await session.scalar(
                select(CollectorRun.collection_epoch_id).where(CollectorRun.id == collector_run_id)
            )
            if epoch_id is None:
                raise ValueError("collector run does not exist")
            latest = (
                select(
                    Observation.pair_id,
                    Observation.price_usd,
                    Observation.price_native,
                    Observation.volume_m5_usd,
                    Observation.buys_m5,
                    Observation.sells_m5,
                    func.row_number()
                    .over(
                        partition_by=Observation.pair_id,
                        order_by=(Observation.received_at.desc(), Observation.id.desc()),
                    )
                    .label("row_number"),
                )
                .join(ApiRequestLog, ApiRequestLog.id == Observation.api_request_log_id)
                .join(CollectorRun, CollectorRun.id == ApiRequestLog.collector_run_id)
                .where(
                    CollectorRun.collection_epoch_id == epoch_id,
                    Observation.received_at >= bucket_start,
                    Observation.received_at < bucket_end,
                )
                .subquery()
            )
            ratio = latest.c.price_usd / latest.c.price_native
            aggregates = (
                await session.execute(
                    select(
                        func.count().label("pair_count"),
                        func.sum(latest.c.volume_m5_usd).label("volume"),
                        func.sum(latest.c.buys_m5).label("buys"),
                        func.sum(latest.c.sells_m5).label("sells"),
                        func.percentile_cont(0.5)
                        .within_group(ratio)
                        .filter(
                            latest.c.price_usd.is_not(None),
                            latest.c.price_native.is_not(None),
                            latest.c.price_usd > 0,
                            latest.c.price_native > 0,
                        )
                        .label("sol_price"),
                    ).where(latest.c.row_number == 1)
                )
            ).one()
            admitted_tokens = await _transition_count(
                session,
                epoch_id=epoch_id,
                state="NEW",
                start=bucket_start,
                end=bucket_end,
            )
            active_transitions = await _transition_count(
                session,
                epoch_id=epoch_id,
                state="ACTIVE",
                start=bucket_start,
                end=bucket_end,
            )
            cohort_start = bucket_end - timedelta(hours=2)
            cohort_end = bucket_end - timedelta(hours=1)
            cohort = (
                select(LifecycleEvent.token_id)
                .join(CollectorRun, CollectorRun.id == LifecycleEvent.collector_run_id)
                .where(
                    CollectorRun.collection_epoch_id == epoch_id,
                    LifecycleEvent.new_state == "NEW",
                    LifecycleEvent.decided_at >= cohort_start,
                    LifecycleEvent.decided_at < cohort_end,
                )
                .distinct()
                .subquery()
            )
            mature_count = int(await session.scalar(select(func.count()).select_from(cohort)) or 0)
            mature_active_count = int(
                await session.scalar(
                    select(func.count(func.distinct(LifecycleEvent.token_id)))
                    .join(CollectorRun, CollectorRun.id == LifecycleEvent.collector_run_id)
                    .where(
                        CollectorRun.collection_epoch_id == epoch_id,
                        LifecycleEvent.new_state == "ACTIVE",
                        LifecycleEvent.decided_at < bucket_end,
                        LifecycleEvent.token_id.in_(select(cohort.c.token_id)),
                    )
                )
                or 0
            )
            prior_prices = list(
                (
                    await session.execute(
                        select(MarketContextSnapshot.sol_usd_price)
                        .where(
                            MarketContextSnapshot.collection_epoch_id == epoch_id,
                            MarketContextSnapshot.policy_sha256 == self._policy_sha256,
                            MarketContextSnapshot.bucket_end <= bucket_start,
                            MarketContextSnapshot.bucket_end >= bucket_start - timedelta(hours=1),
                            MarketContextSnapshot.sol_usd_price.is_not(None),
                        )
                        .order_by(MarketContextSnapshot.bucket_end)
                    )
                ).scalars()
            )
            sol_price = Decimal(aggregates.sol_price) if aggregates.sol_price is not None else None
            sol_return, volatility = _returns(sol_price, prior_prices)
            return await self._repository.record(
                session,
                collection_epoch_id=epoch_id,
                collector_run_id=collector_run_id,
                bucket_start=bucket_start,
                bucket_end=bucket_end,
                source_observed_at=bucket_end,
                received_at=received_at,
                sol_usd_price=sol_price,
                sol_return_5m=sol_return,
                sol_realized_volatility_1h=volatility,
                admitted_tokens=admitted_tokens,
                active_transitions=active_transitions,
                mature_cohort_tokens=mature_count,
                mature_cohort_active_tokens=mature_active_count,
                mature_cohort_active_fraction=(
                    Decimal(mature_active_count) / Decimal(mature_count) if mature_count else None
                ),
                pair_sample_count=int(aggregates.pair_count or 0),
                aggregate_volume_m5_usd=(
                    Decimal(aggregates.volume) if aggregates.volume is not None else None
                ),
                aggregate_buys_m5=int(aggregates.buys) if aggregates.buys is not None else None,
                aggregate_sells_m5=(
                    int(aggregates.sells) if aggregates.sells is not None else None
                ),
                policy_sha256=self._policy_sha256,
                policy_snapshot=self._policy_snapshot,
            )


async def _transition_count(
    session: AsyncSession,
    *,
    epoch_id: uuid.UUID,
    state: str,
    start: datetime,
    end: datetime,
) -> int:
    return int(
        await session.scalar(
            select(func.count(func.distinct(LifecycleEvent.token_id)))
            .join(CollectorRun, CollectorRun.id == LifecycleEvent.collector_run_id)
            .where(
                CollectorRun.collection_epoch_id == epoch_id,
                LifecycleEvent.new_state == state,
                LifecycleEvent.decided_at >= start,
                LifecycleEvent.decided_at < end,
            )
        )
        or 0
    )


def _floor_time(value: datetime, seconds: int) -> datetime:
    timestamp = int(value.timestamp())
    return datetime.fromtimestamp(timestamp - timestamp % seconds, tz=UTC)


def _returns(
    current: Decimal | None,
    prior_prices: list[Decimal | None],
) -> tuple[Decimal | None, Decimal | None]:
    clean = [Decimal(value) for value in prior_prices if value is not None and value > 0]
    if current is None or current <= 0 or not clean:
        return None, None
    latest_return = current / clean[-1] - 1
    series = [*clean, current]
    returns = [float(series[index] / series[index - 1] - 1) for index in range(1, len(series))]
    volatility = math.sqrt(sum(value * value for value in returns) / len(returns))
    return latest_return, Decimal(str(volatility))
