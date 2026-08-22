from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from pump_research.persistence.models import (
    LifecycleEvidenceEvaluation,
    LifecyclePolicy,
)
from pump_research.persistence.repositories import (
    ApiRequestLogRepository,
    LifecycleEvidenceEvaluationRepository,
    LifecyclePolicyRepository,
    LifecyclePolicySnapshotMismatchError,
    TokenRepository,
)


def _policy_snapshot() -> dict[str, object]:
    return {
        "component": "lifecycle_evidence_selector",
        "schema_version": 1,
        "ranking": ["liquidity_usd DESC", "pair_address ASC"],
    }


def _policy_sha256(snapshot: dict[str, object]) -> str:
    encoded = json.dumps(
        snapshot,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


async def _request(
    session: AsyncSession,
    *,
    now: datetime,
    suffix: str,
) -> tuple[uuid.UUID, uuid.UUID]:
    token = await TokenRepository().get_or_create(
        session,
        chain="solana",
        address=f"storage-policy-{suffix}",
        first_discovered_at=now,
    )
    request = await ApiRequestLogRepository().record(
        session,
        collector_run_id=None,
        idempotency_key=f"storage-policy-request-{suffix}",
        provider="test-market-data",
        endpoint="/pairs/batch",
        requested_at=now,
        received_at=now,
        outcome="succeeded",
        http_status_code=200,
        request_payload={"addresses": [token.address]},
        response_payload={"pairs": []},
        response_payload_sha256="a" * 64,
        failure_detail=None,
    )
    return token.id, request.id


@pytest.mark.integration
async def test_future_evaluations_reference_one_exact_normalized_policy(
    session: AsyncSession,
) -> None:
    now = datetime(2026, 8, 15, 12, tzinfo=UTC)
    snapshot = _policy_snapshot()
    policy_sha256 = _policy_sha256(snapshot)
    repository = LifecycleEvidenceEvaluationRepository()

    async with session.begin():
        first_token_id, first_request_id = await _request(session, now=now, suffix="new")
        evaluation = await repository.record(
            session,
            input_watermark=now,
            token_id=first_token_id,
            api_request_log_id=first_request_id,
            outcome="failed",
            selected_pair_id=None,
            selected_observation_id=None,
            selected_observation_received_at=None,
            reason_code="no_candidate_pair_observations",
            reason_detail={"candidates": []},
            policy_sha256=policy_sha256,
            policy_snapshot=snapshot,
        )
        resolved = await repository.resolve_policy_snapshot(session, evaluation)

        historical_token_id, historical_request_id = await _request(
            session,
            now=now,
            suffix="historical",
        )
        historical = LifecycleEvidenceEvaluation(
            input_watermark=now,
            token_id=historical_token_id,
            api_request_log_id=historical_request_id,
            outcome="failed",
            selected_pair_id=None,
            selected_observation_id=None,
            selected_observation_received_at=None,
            reason_code="no_candidate_pair_observations",
            reason_detail={"candidates": []},
            policy_sha256=policy_sha256,
            policy_snapshot=snapshot,
        )
        session.add(historical)
        await session.flush()
        historical_resolved = await repository.resolve_policy_snapshot(session, historical)
        evaluation_is_sql_null = await session.scalar(
            select(LifecycleEvidenceEvaluation.policy_snapshot.is_(None)).where(
                LifecycleEvidenceEvaluation.id == evaluation.id,
                LifecycleEvidenceEvaluation.input_watermark == evaluation.input_watermark,
            )
        )
        historical_is_sql_null = await session.scalar(
            select(LifecycleEvidenceEvaluation.policy_snapshot.is_(None)).where(
                LifecycleEvidenceEvaluation.id == historical.id,
                LifecycleEvidenceEvaluation.input_watermark == historical.input_watermark,
            )
        )

    assert evaluation.policy_snapshot is None
    assert historical.policy_snapshot == snapshot
    assert evaluation_is_sql_null is True
    assert historical_is_sql_null is False
    assert resolved == snapshot
    assert historical_resolved == snapshot
    async with session.begin():
        policy_count = await session.scalar(select(func.count()).select_from(LifecyclePolicy))
        resolvable_count = await session.scalar(
            select(func.count())
            .select_from(LifecycleEvidenceEvaluation)
            .join(
                LifecyclePolicy,
                LifecyclePolicy.policy_sha256 == LifecycleEvidenceEvaluation.policy_sha256,
            )
        )
    assert policy_count == 1
    assert resolvable_count == 2


@pytest.mark.integration
async def test_one_policy_digest_cannot_resolve_to_different_snapshots(
    session: AsyncSession,
) -> None:
    repository = LifecyclePolicyRepository()
    snapshot = _policy_snapshot()
    policy_sha256 = _policy_sha256(snapshot)
    async with session.begin():
        await repository.ensure(
            session,
            policy_sha256=policy_sha256,
            policy_snapshot=snapshot,
        )

    with pytest.raises(LifecyclePolicySnapshotMismatchError):
        async with session.begin():
            await repository.ensure(
                session,
                policy_sha256=policy_sha256,
                policy_snapshot={**snapshot, "schema_version": 2},
            )


@pytest.mark.integration
async def test_lifecycle_policy_rows_are_immutable(session: AsyncSession) -> None:
    snapshot = _policy_snapshot()
    policy_sha256 = _policy_sha256(snapshot)
    async with session.begin():
        await LifecyclePolicyRepository().ensure(
            session,
            policy_sha256=policy_sha256,
            policy_snapshot=snapshot,
        )

    with pytest.raises(DBAPIError, match="immutable"):
        async with session.begin():
            await session.execute(
                update(LifecyclePolicy)
                .where(LifecyclePolicy.policy_sha256 == policy_sha256)
                .values(policy_snapshot={"changed": True})
            )

    with pytest.raises(DBAPIError, match="immutable"):
        async with session.begin():
            await session.execute(
                delete(LifecyclePolicy).where(
                    LifecyclePolicy.policy_sha256 == policy_sha256
                )
            )


@pytest.mark.integration
async def test_only_unused_poll_member_due_index_was_removed(
    session: AsyncSession,
) -> None:
    async with session.begin():
        due_index_count = await session.scalar(
            text(
                "SELECT count(*) FROM pg_indexes "
                "WHERE tablename LIKE 'poll_batch_members%' "
                "AND indexname LIKE '%token_id_due_at%'"
            )
        )
        claimed_index_count = await session.scalar(
            text(
                "SELECT count(*) FROM pg_indexes "
                "WHERE tablename LIKE 'poll_batch_members%' "
                "AND indexname LIKE '%token_id_claimed_at%'"
            )
        )
        due_column_count = await session.scalar(
            text(
                "SELECT count(*) FROM information_schema.columns "
                "WHERE table_schema = current_schema() "
                "AND table_name = 'poll_batch_members' "
                "AND column_name = 'due_at'"
            )
        )

    assert due_index_count == 0
    assert claimed_index_count is not None and claimed_index_count > 0
    assert due_column_count == 1
