from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from pump_research.database_safety import assert_destructive_test_database


def _test_database_url() -> str:
    value = os.environ.get("PUMP_RESEARCH_TEST_DATABASE_URL")
    if value is None:
        raise RuntimeError(
            "Integration tests require PUMP_RESEARCH_TEST_DATABASE_URL pointing to a "
            "disposable database whose name has an approved test marker; the collector database "
            "is never an acceptable test target"
        )
    if os.environ.get("PUMP_RESEARCH_ENVIRONMENT", "").lower() != "test":
        raise RuntimeError(
            "CRITICAL DATABASE SAFETY ABORT: integration tests require "
            "PUMP_RESEARCH_ENVIRONMENT=test"
        )
    return value


DATABASE_URL = _test_database_url()
PROJECT_ROOT = Path(__file__).parents[2]


@pytest_asyncio.fixture(scope="session", autouse=True)
async def apply_migrations() -> None:
    engine = create_async_engine(DATABASE_URL)
    try:
        async with engine.connect() as connection:
            await assert_destructive_test_database(
                connection,
                environment=os.environ["PUMP_RESEARCH_ENVIRONMENT"],
                explicit_test_database_url=True,
                operation="integration-test migration rebuild",
            )
    finally:
        await engine.dispose()
    environment = {
        **os.environ,
        "PUMP_RESEARCH_DATABASE_URL": DATABASE_URL,
        "PUMP_RESEARCH_ENVIRONMENT": "test",
        "PUMP_RESEARCH_MIGRATION_DESTRUCTIVE_TEST": "1",
    }
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
        cwd=PROJECT_ROOT,
        env=environment,
    )


@pytest_asyncio.fixture
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(DATABASE_URL)

    async def truncate_test_data() -> None:
        async with engine.begin() as connection:
            await assert_destructive_test_database(
                connection,
                environment=os.environ["PUMP_RESEARCH_ENVIRONMENT"],
                explicit_test_database_url=True,
                operation="integration fixture TRUNCATE CASCADE",
            )
            await connection.execute(
                text(
                    "TRUNCATE deduplication_conflicts, poll_batch_outcomes, poll_batch_members, "
                    "holder_balance_facts, security_feature_snapshots, wallet_cluster_snapshots, "
                    "funding_relationship_evidence, wallet_relationship_edges, "
                    "liquidity_event_evidence, creator_relationship_events, "
                    "creator_history_snapshots, trader_distribution_snapshots, holder_snapshots, "
                    "security_provider_requests, security_provider_budget_reservations, "
                    "security_enrichment_policies, "
                    "candidate_enrichment_tasks, candidate_current_state, "
                    "candidate_tier_events, candidate_events, candidate_policies, "
                    "archive_retention_evaluations, archive_copy_verifications, "
                    "archive_scope_events, archive_scopes, "
                    "boost_events, boost_observations, token_metadata_events, "
                    "token_security_snapshots, token_security_tasks, market_context_snapshots, "
                    "pair_fact_events, "
                    "coverage_decisions, poll_schedule_decisions, poll_schedules, poll_batches, "
                    "scheduler_capacity_decisions, scheduler_policies, "
                    "coverage_policies, "
                    "storage_relation_samples, storage_samples, backup_verifications, "
                    "dex_availability_tasks, discovery_checkpoint_states, "
                    "discovery_connectivity_events, "
                    "lifecycle_evidence_evaluations, lifecycle_policies, observations, "
                    "lifecycle_events, discovery_events, "
                    "api_request_log, pairs, tokens, collector_run_events, collector_runs, "
                    "collection_epoch_current, collection_epoch_events, collection_epochs CASCADE"
                )
            )
            for seed_statement in (
                """
                INSERT INTO collection_epochs
                  (id, epoch_number, name, purpose, data_valid, invalid_reason,
                   configuration_sha256, configuration_snapshot, code_revision, created_at)
                VALUES
                  ('00000000-0000-0000-0000-000000000000', 0, 'Epoch 0',
                   'engineering burn-in only', false,
                   'data destroyed by integration fixture using TRUNCATE CASCADE; '
                   'unrecoverable; research validity NONE',
                   '1e58165118b9b0c49744ae0a6a63ef3aa71704f870696223c2edf18b81696d96',
                   '{"component":"collection_epoch","epoch_number":0,'
                   '"purpose":"engineering burn-in","research_validity":"NONE",'
                   '"schema_version":1}'::jsonb,
                   NULL, TIMESTAMPTZ '2026-08-15 00:00:00+00')
                """,
                """
                INSERT INTO collection_epoch_events
                  (id, collection_epoch_id, status, occurred_at, reason, detail, idempotency_key)
                VALUES
                  ('00000000-0000-0000-0000-000000000001',
                   '00000000-0000-0000-0000-000000000000', 'invalid',
                   TIMESTAMPTZ '2026-08-15 00:00:00+00', 'Epoch 0 permanently lost',
                   '{"research_validity":"NONE","recovery":"forbidden"}'::jsonb,
                   'epoch:0:invalid:lost-2026-08-15')
                """,
                """
                INSERT INTO collection_epoch_current
                  (collection_epoch_id, status, data_valid, invalid_reason, started_at,
                   ended_at, latest_event_id, updated_at)
                VALUES
                  ('00000000-0000-0000-0000-000000000000', 'invalid', false,
                   'data destroyed by integration fixture using TRUNCATE CASCADE; '
                   'unrecoverable; research validity NONE', NULL,
                   TIMESTAMPTZ '2026-08-15 00:00:00+00',
                   '00000000-0000-0000-0000-000000000001',
                   TIMESTAMPTZ '2026-08-15 00:00:00+00')
                """,
            ):
                await connection.exec_driver_sql(seed_statement)

    await truncate_test_data()

    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await truncate_test_data()
    await engine.dispose()


@pytest_asyncio.fixture
async def session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """A convenience session for persistence tests using the clean database fixture."""
    async with session_factory() as database_session:
        yield database_session
