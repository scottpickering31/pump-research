"""Read-only historical simulation of the transparent orchestration rule."""

from __future__ import annotations

import asyncio
import heapq
import json
from collections.abc import AsyncIterable
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.candidates.policy import CandidatePolicy
from pump_research.config import get_settings
from pump_research.database import create_database_engine


class EvidenceRow(Protocol):
    token_id: object
    received_at: datetime
    liquidity_usd: Decimal | None
    volume_m5_usd: Decimal | None
    buys_m5: int | None
    sells_m5: int | None


@dataclass(frozen=True, slots=True)
class HistoricalCandidateSimulation:
    epoch_number: int
    evaluated_tokens: int
    eligible_tokens: int
    candidate_events: int
    candidates_per_eligible_token: float
    candidates_per_minute: float
    promotions_to_tier1: int
    demotions_to_tier0: int
    promotions_to_tier2: int
    peak_candidate_coverage: int
    universe_escalated_pct: float
    methodology: str


async def simulate_epoch(
    session_factory: async_sessionmaker[AsyncSession],
    policy: CandidatePolicy,
    *,
    epoch_number: int,
) -> HistoricalCandidateSimulation:
    """Stream lifecycle-selected evidence without loading the epoch into RAM."""
    query = text("""
        SELECT le.token_id::text AS token_id, le.input_watermark AS received_at,
               o.liquidity_usd, o.volume_m5_usd, o.buys_m5, o.sells_m5
        FROM lifecycle_evidence_evaluations le
        JOIN observations o
          ON o.received_at=le.selected_observation_received_at
         AND o.id=le.selected_observation_id
        JOIN api_request_log ar ON ar.id=le.api_request_log_id
        JOIN collector_runs cr ON cr.id=ar.collector_run_id
        JOIN collection_epochs e ON e.id=cr.collection_epoch_id
        WHERE e.epoch_number=:epoch AND le.outcome='selected'
        ORDER BY le.token_id,le.input_watermark,le.id
    """)
    async with session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        bounds = (
            await session.execute(
                text("""
                SELECT ec.started_at,ec.ended_at
                FROM collection_epochs e JOIN collection_epoch_current ec
                  ON ec.collection_epoch_id=e.id
                WHERE e.epoch_number=:epoch
                """),
                {"epoch": epoch_number},
            )
        ).one()
        stream = await session.stream(query, {"epoch": epoch_number})
        simulation = await _simulate_rows(stream, policy, epoch_number=epoch_number)
    duration_minutes = max(1.0, (bounds.ended_at - bounds.started_at).total_seconds() / 60)
    return HistoricalCandidateSimulation(
        **{
            **asdict(simulation),
            "candidates_per_minute": round(simulation.candidate_events / duration_minutes, 6),
        }
    )


async def _simulate_rows(
    rows: AsyncIterable[EvidenceRow],
    policy: CandidatePolicy,
    *,
    epoch_number: int,
) -> HistoricalCandidateSimulation:
    current_token: str | None = None
    tier1 = False
    expires_at: datetime | None = None
    token_eligible = False
    evaluated_tokens = eligible_tokens = candidates = promotions = demotions = 0
    intervals: list[tuple[datetime, datetime]] = []
    async for row in rows:
        token_id = str(row.token_id)
        if token_id != current_token:
            if current_token is not None:
                evaluated_tokens += 1
                eligible_tokens += token_eligible
            current_token = token_id
            tier1 = False
            expires_at = None
            token_eligible = False
        eligible = _market_eligible(row, policy)
        token_eligible = token_eligible or eligible
        if not tier1 and eligible:
            tier1 = True
            promotions += 1
            candidates += 1
            expires_at = row.received_at + policy.tier1_ttl
            intervals.append((row.received_at, expires_at))
        elif tier1 and expires_at is not None and row.received_at >= expires_at:
            if eligible:
                candidates += 1
                expires_at = row.received_at + policy.tier1_ttl
                intervals.append((row.received_at, expires_at))
            else:
                tier1 = False
                expires_at = None
                demotions += 1
    if current_token is not None:
        evaluated_tokens += 1
        eligible_tokens += token_eligible
    peak = _peak_intervals(intervals)
    return HistoricalCandidateSimulation(
        epoch_number=epoch_number,
        evaluated_tokens=evaluated_tokens,
        eligible_tokens=eligible_tokens,
        candidate_events=candidates,
        candidates_per_eligible_token=round(candidates / max(1, eligible_tokens), 6),
        candidates_per_minute=0.0,
        promotions_to_tier1=promotions,
        demotions_to_tier0=demotions,
        promotions_to_tier2=0,
        peak_candidate_coverage=peak,
        universe_escalated_pct=round(100 * eligible_tokens / max(1, evaluated_tokens), 6),
        methodology=(
            "streamed lifecycle-selected observations; received_at ordering; "
            "30-minute Tier 1 TTL; no outcome facts; Epoch 2 has no Phase 2 security stream"
        ),
    )


def _market_eligible(row: EvidenceRow, policy: CandidatePolicy) -> bool:
    transactions = (
        None if row.buys_m5 is None or row.sells_m5 is None else row.buys_m5 + row.sells_m5
    )
    ratio = (
        None
        if row.volume_m5_usd is None or not row.liquidity_usd
        else row.volume_m5_usd / row.liquidity_usd
    )
    return bool(
        row.liquidity_usd is not None
        and row.liquidity_usd >= policy.minimum_liquidity_usd
        and transactions is not None
        and transactions >= policy.minimum_transactions_m5
        and ratio is not None
        and ratio >= policy.minimum_volume_liquidity_ratio
    )


def _peak_intervals(intervals: list[tuple[datetime, datetime]]) -> int:
    active: list[datetime] = []
    peak = 0
    for start, end in sorted(intervals):
        while active and active[0] <= start:
            heapq.heappop(active)
        heapq.heappush(active, end)
        peak = max(peak, len(active))
    return peak


async def _main() -> None:
    settings = get_settings()
    engine = create_database_engine(settings)
    try:
        result = await simulate_epoch(
            async_sessionmaker(engine, expire_on_commit=False),
            CandidatePolicy.from_settings(settings),
            epoch_number=2,
        )
        print(json.dumps(asdict(result), indent=2, sort_keys=True))
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(_main())
