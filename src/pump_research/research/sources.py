"""Hot PostgreSQL and cold DuckDB adapters for one research history contract."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol, cast

import duckdb
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.archival import verify_archive
from pump_research.archive_analytics import ColdArchiveQuery
from pump_research.research.contracts import (
    BoostFact,
    CandidateFact,
    CandidateTierFact,
    CoverageFact,
    DiscoveryFact,
    LifecycleFact,
    MarketContextFact,
    MetadataFact,
    ObservationFact,
    PairFact,
    SecurityFact,
    SelectiveSecurityFact,
    TokenHistory,
    canonical_digest,
    utc,
)


class ResearchSource(Protocol):
    async def load_histories(
        self,
        *,
        epoch_number: int,
        token_addresses: Sequence[str] | None = None,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        allow_invalid_epoch: bool = False,
    ) -> tuple[TokenHistory, ...]: ...


class InMemoryResearchSource:
    """Deterministic test/fixture source using the same immutable fact contract."""

    def __init__(self, histories: Iterable[TokenHistory], *, name: str = "memory") -> None:
        self._histories = tuple(histories)
        self._name = name

    async def load_histories(
        self,
        *,
        epoch_number: int,
        token_addresses: Sequence[str] | None = None,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        allow_invalid_epoch: bool = False,
    ) -> tuple[TokenHistory, ...]:
        addresses = set(token_addresses or ())
        result = [
            history
            for history in self._histories
            if history.epoch_number == epoch_number
            and (not addresses or history.address in addresses)
            and (history.epoch_data_valid or allow_invalid_epoch)
        ]
        return tuple(
            _with_descriptor(
                _slice_history(history, start_at=start_at, end_at=end_at),
                {"kind": self._name, "sha256": _history_digest(history)},
            )
            for history in sorted(result, key=lambda item: item.address)
        )


class HotColdResearchSource:
    """Combine verified cold history and hot facts at one explicit non-overlap cutoff."""

    def __init__(self, cold: ResearchSource, hot: ResearchSource, *, hot_from: datetime) -> None:
        self._cold = cold
        self._hot = hot
        self._hot_from = utc(hot_from, "hot/cold cutoff")

    async def load_histories(
        self,
        *,
        epoch_number: int,
        token_addresses: Sequence[str] | None = None,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        allow_invalid_epoch: bool = False,
    ) -> tuple[TokenHistory, ...]:
        cold_end = min(utc(end_at), self._hot_from) if end_at else self._hot_from
        hot_start = max(utc(start_at), self._hot_from) if start_at else self._hot_from
        cold_histories, hot_histories = await _load_both(
            self._cold,
            self._hot,
            epoch_number=epoch_number,
            token_addresses=token_addresses,
            cold_start=start_at,
            cold_end=cold_end,
            hot_start=hot_start,
            hot_end=end_at,
            allow_invalid_epoch=allow_invalid_epoch,
        )
        by_token: dict[str, list[TokenHistory]] = defaultdict(list)
        for history in cold_histories:
            by_token[history.token_id].append(
                _slice_history(history, start_at=start_at, end_at=cold_end)
            )
        for history in hot_histories:
            by_token[history.token_id].append(
                _slice_history(history, start_at=hot_start, end_at=end_at)
            )
        return tuple(
            sorted(
                (_merge_histories(parts, self._hot_from) for parts in by_token.values()),
                key=lambda item: item.address,
            )
        )


class DuckDBArchiveResearchSource:
    """Strict cold adapter over verified archive manifests and Parquet views."""

    def __init__(self, manifests: Sequence[Path], *, require_verified: bool = True) -> None:
        self._manifest_paths = tuple(path.resolve() for path in manifests)
        self._require_verified = require_verified
        if not self._manifest_paths:
            raise ValueError("at least one archive manifest is required")

    async def load_histories(
        self,
        *,
        epoch_number: int,
        token_addresses: Sequence[str] | None = None,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        allow_invalid_epoch: bool = False,
    ) -> tuple[TokenHistory, ...]:
        if self._require_verified:
            for manifest_path in self._manifest_paths:
                await verify_archive(manifest_path)
        with ColdArchiveQuery(self._manifest_paths) as archive:
            manifests = [
                manifest for manifest in archive.manifests if manifest.get("epoch") == epoch_number
            ]
            if not manifests:
                return ()
            if len({str(item.get("epoch_id")) for item in manifests}) != 1:
                raise ValueError("cold research source has conflicting epoch identities")
            valid = all(bool(item.get("epoch_data_valid", True)) for item in manifests)
            if not valid and not allow_invalid_epoch:
                return ()
            descriptor: dict[str, object] = {
                "kind": "parquet",
                "archive_schema_revisions": sorted(
                    {
                        str(item.get("archive_schema_version", item.get("schema_version")))
                        for item in manifests
                    }
                ),
                "manifest_sha256": sorted(_sha256_file(path) for path in self._manifest_paths),
            }
            return _histories_from_duckdb(
                archive.connection,
                archive.files_by_family,
                epoch_number=epoch_number,
                epoch_id=str(manifests[0]["epoch_id"]),
                epoch_valid=valid,
                descriptor=descriptor,
                token_addresses=token_addresses,
                start_at=start_at,
                end_at=end_at,
            )


class PostgresResearchSource:
    """Read-only hot adapter that tolerates pre-Phase-2 optional columns/tables."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def load_histories(
        self,
        *,
        epoch_number: int,
        token_addresses: Sequence[str] | None = None,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        allow_invalid_epoch: bool = False,
    ) -> tuple[TokenHistory, ...]:
        async with self._session_factory() as session:
            schema = await _schema_inventory(session)
            epoch = (
                (
                    await session.execute(
                        text(
                            "SELECT e.id::text AS epoch_id, e.epoch_number, "
                            "COALESCE(ec.data_valid, e.data_valid) AS data_valid "
                            "FROM collection_epochs e LEFT JOIN collection_epoch_current ec "
                            "ON ec.collection_epoch_id=e.id WHERE e.epoch_number=:epoch"
                        ),
                        {"epoch": epoch_number},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if epoch is None:
                return ()
            if not bool(epoch["data_valid"]) and not allow_invalid_epoch:
                return ()
            descriptor = {
                "kind": "postgresql",
                "schema_revision": await session.scalar(
                    text("SELECT version_num FROM alembic_version")
                ),
                "epoch": epoch_number,
                "read_only": True,
            }
            rows = await _postgres_rows(
                session,
                schema,
                epoch_number=epoch_number,
                token_addresses=token_addresses,
                start_at=start_at,
                end_at=end_at,
            )
        return _assemble_histories(
            rows,
            epoch_id=cast(str, epoch["epoch_id"]),
            epoch_number=epoch_number,
            epoch_valid=bool(epoch["data_valid"]),
            descriptor=descriptor,
        )


async def _load_both(
    cold: ResearchSource,
    hot: ResearchSource,
    *,
    epoch_number: int,
    token_addresses: Sequence[str] | None,
    cold_start: datetime | None,
    cold_end: datetime,
    hot_start: datetime,
    hot_end: datetime | None,
    allow_invalid_epoch: bool,
) -> tuple[tuple[TokenHistory, ...], tuple[TokenHistory, ...]]:
    cold_result, hot_result = await asyncio.gather(
        cold.load_histories(
            epoch_number=epoch_number,
            token_addresses=token_addresses,
            start_at=cold_start,
            end_at=cold_end,
            allow_invalid_epoch=allow_invalid_epoch,
        ),
        hot.load_histories(
            epoch_number=epoch_number,
            token_addresses=token_addresses,
            start_at=hot_start,
            end_at=hot_end,
            allow_invalid_epoch=allow_invalid_epoch,
        ),
    )
    return cold_result, hot_result


def _slice_history(
    history: TokenHistory, *, start_at: datetime | None, end_at: datetime | None
) -> TokenHistory:
    start = utc(start_at) if start_at else None
    end = utc(end_at) if end_at else None

    def included(value: datetime) -> bool:
        timestamp = utc(value)
        return (start is None or timestamp >= start) and (end is None or timestamp < end)

    return replace(
        history,
        discoveries=tuple(item for item in history.discoveries if included(item.received_at)),
        observations=tuple(item for item in history.observations if included(item.received_at)),
        lifecycle=tuple(item for item in history.lifecycle if included(item.decided_at)),
        pair_facts=tuple(item for item in history.pair_facts if included(item.received_at)),
        boosts=tuple(item for item in history.boosts if included(item.received_at)),
        metadata=tuple(item for item in history.metadata if included(item.received_at)),
        security=tuple(item for item in history.security if included(item.received_at)),
        context=tuple(item for item in history.context if included(item.received_at)),
        coverage=tuple(item for item in history.coverage if included(item.decided_at)),
        candidates=tuple(item for item in history.candidates if included(item.candidate_at)),
        candidate_tiers=tuple(
            item for item in history.candidate_tiers if included(item.decided_at)
        ),
        selective_security=tuple(
            item for item in history.selective_security if included(item.received_at)
        ),
    )


class _Identified(Protocol):
    @property
    def id(self) -> str: ...


def _merge_facts[Fact: _Identified](
    parts: Iterable[tuple[Fact, ...]], key: Callable[[Fact], tuple[datetime, str]]
) -> tuple[Fact, ...]:
    by_id: dict[str, Fact] = {}
    for facts in parts:
        for fact in facts:
            identifier = fact.id
            existing = by_id.get(identifier)
            if existing is not None and existing != fact:
                raise ValueError(f"hot/cold fact {identifier} has conflicting content")
            by_id[identifier] = fact
    return tuple(sorted(by_id.values(), key=key))


def _merge_histories(parts: list[TokenHistory], cutoff: datetime) -> TokenHistory:
    first = parts[0]
    if any(
        (item.epoch_id, item.epoch_number, item.token_id, item.chain, item.address)
        != (first.epoch_id, first.epoch_number, first.token_id, first.chain, first.address)
        for item in parts[1:]
    ):
        raise ValueError("hot/cold histories have conflicting token or epoch identity")
    if len({item.epoch_data_valid for item in parts}) != 1:
        raise ValueError("hot/cold histories disagree about epoch validity")
    return TokenHistory(
        epoch_id=first.epoch_id,
        epoch_number=first.epoch_number,
        epoch_data_valid=first.epoch_data_valid,
        token_id=first.token_id,
        chain=first.chain,
        address=first.address,
        discoveries=_merge_facts(
            (item.discoveries for item in parts), lambda item: (item.received_at, item.id)
        ),
        observations=_merge_facts(
            (item.observations for item in parts), lambda item: (item.received_at, item.id)
        ),
        lifecycle=_merge_facts(
            (item.lifecycle for item in parts), lambda item: (item.decided_at, item.id)
        ),
        pair_facts=_merge_facts(
            (item.pair_facts for item in parts), lambda item: (item.received_at, item.id)
        ),
        boosts=_merge_facts(
            (item.boosts for item in parts), lambda item: (item.received_at, item.id)
        ),
        metadata=_merge_facts(
            (item.metadata for item in parts), lambda item: (item.received_at, item.id)
        ),
        security=_merge_facts(
            (item.security for item in parts), lambda item: (item.received_at, item.id)
        ),
        context=_merge_facts(
            (item.context for item in parts), lambda item: (item.received_at, item.id)
        ),
        coverage=_merge_facts(
            (item.coverage for item in parts), lambda item: (item.decided_at, item.id)
        ),
        candidates=_merge_facts(
            (item.candidates for item in parts), lambda item: (item.candidate_at, item.id)
        ),
        candidate_tiers=_merge_facts(
            (item.candidate_tiers for item in parts),
            lambda item: (item.decided_at, item.id),
        ),
        selective_security=_merge_facts(
            (item.selective_security for item in parts),
            lambda item: (item.received_at, item.id),
        ),
        source_descriptor={
            "kind": "hot_cold",
            "cutoff": utc(cutoff).isoformat(),
            "sources": sorted((item.source_descriptor for item in parts), key=canonical_digest),
        },
    )


async def _schema_inventory(session: AsyncSession) -> dict[str, set[str]]:
    result = await session.execute(
        text(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema=current_schema()"
        )
    )
    inventory: dict[str, set[str]] = defaultdict(set)
    for table_name, column_name in result:
        inventory[str(table_name)].add(str(column_name))
    return dict(inventory)


async def _postgres_rows(
    session: AsyncSession,
    schema: dict[str, set[str]],
    *,
    epoch_number: int,
    token_addresses: Sequence[str] | None,
    start_at: datetime | None,
    end_at: datetime | None,
) -> dict[str, list[dict[str, object]]]:
    address_clause = (
        "" if not token_addresses else " AND t.address = ANY(CAST(:addresses AS text[]))"
    )
    parameters: dict[str, object] = {
        "epoch": epoch_number,
        "addresses": list(token_addresses or ()),
        "start_at": utc(start_at) if start_at else None,
        "end_at": utc(end_at) if end_at else None,
    }
    candidate_exists = (
        " OR EXISTS (SELECT 1 FROM candidate_events ce "
        "WHERE ce.token_id=t.id AND ce.collection_epoch_id=(SELECT id "
        "FROM collection_epochs WHERE epoch_number=:epoch))"
        if "candidate_events" in schema
        else ""
    )
    token_sql = (
        "SELECT t.id::text AS token_id,t.chain,t.address FROM tokens t WHERE ("
        "EXISTS (SELECT 1 FROM lifecycle_events le JOIN collector_runs cr "
        "ON cr.id=le.collector_run_id JOIN collection_epochs e "
        "ON e.id=cr.collection_epoch_id WHERE le.token_id=t.id AND e.epoch_number=:epoch) "
        "OR EXISTS (SELECT 1 FROM pairs p JOIN observations o ON o.pair_id=p.id "
        "JOIN api_request_log ar ON ar.id=o.api_request_log_id JOIN collector_runs cr "
        "ON cr.id=ar.collector_run_id JOIN collection_epochs e ON e.id=cr.collection_epoch_id "
        "WHERE p.token_id=t.id AND e.epoch_number=:epoch)"
        + candidate_exists
        + ")"
        + address_clause
        + " ORDER BY t.address"
    )
    rows: dict[str, list[dict[str, object]]] = {
        "tokens": await _mapping_rows(session, token_sql, parameters)
    }
    time_filter = (
        " AND (CAST(:start_at AS timestamptz) IS NULL "
        "OR {column} >= CAST(:start_at AS timestamptz))"
        " AND (CAST(:end_at AS timestamptz) IS NULL "
        "OR {column} < CAST(:end_at AS timestamptz))"
    )
    observation_columns = [
        "source_observed_at",
        "price_usd",
        "price_native",
        "liquidity_usd",
        "market_cap_usd",
        "fully_diluted_valuation_usd",
        "volume_m5_usd",
        "volume_h1_usd",
        "volume_h6_usd",
        "volume_h24_usd",
        "buys_m5",
        "sells_m5",
        "buys_h1",
        "sells_h1",
        "buys_h6",
        "sells_h6",
        "buys_h24",
        "sells_h24",
        "liquidity_base",
        "liquidity_quote",
    ]
    projected = ",".join(
        f"o.{column}" if column in schema.get("observations", set()) else f"NULL AS {column}"
        for column in observation_columns
    )
    rows["observations"] = await _mapping_rows(
        session,
        "SELECT t.id::text AS token_id,o.id::text,p.id::text AS pair_id,"
        "p.address AS pair_address,o.received_at," + projected + " FROM observations o "
        "JOIN pairs p ON p.id=o.pair_id JOIN tokens t ON t.id=p.token_id "
        "JOIN api_request_log ar ON ar.id=o.api_request_log_id JOIN collector_runs cr "
        "ON cr.id=ar.collector_run_id JOIN collection_epochs e ON e.id=cr.collection_epoch_id "
        "WHERE e.epoch_number=:epoch"
        + address_clause
        + time_filter.format(column="o.received_at")
        + " ORDER BY t.id,o.received_at,o.id",
        parameters,
    )
    rows["lifecycle"] = await _mapping_rows(
        session,
        "SELECT t.id::text AS token_id,le.id::text,le.decided_at,le.input_watermark,"
        "le.previous_state,le.new_state,le.reason_code FROM lifecycle_events le "
        "JOIN tokens t ON t.id=le.token_id JOIN collector_runs cr ON cr.id=le.collector_run_id "
        "JOIN collection_epochs e ON e.id=cr.collection_epoch_id WHERE e.epoch_number=:epoch"
        + address_clause
        + " AND (CAST(:end_at AS timestamptz) IS NULL "
        "OR le.decided_at < CAST(:end_at AS timestamptz)) "
        "ORDER BY t.id,le.decided_at,le.id",
        parameters,
    )
    rows["discoveries"] = await _mapping_rows(
        session,
        "SELECT t.id::text AS token_id,de.id::text,de.received_at,de.source_event_at,de.event_type "
        "FROM discovery_events de JOIN tokens t ON t.id=de.token_id JOIN collector_runs cr "
        "ON cr.id=de.collector_run_id JOIN collection_epochs e ON e.id=cr.collection_epoch_id "
        "WHERE e.epoch_number=:epoch"
        + address_clause
        + time_filter.format(column="de.received_at")
        + " ORDER BY t.id,de.received_at,de.id",
        parameters,
    )
    optional_queries = _optional_postgres_queries(schema, address_clause, time_filter)
    for family, query in optional_queries.items():
        rows[family] = await _mapping_rows(session, query, parameters)
    return rows


def _optional_postgres_queries(
    schema: dict[str, set[str]], address_clause: str, time_filter: str
) -> dict[str, str]:
    queries: dict[str, str] = {}
    common = (
        " JOIN tokens t ON t.id={alias}.token_id JOIN collector_runs cr "
        "ON cr.id={alias}.collector_run_id JOIN collection_epochs e "
        "ON e.id=cr.collection_epoch_id WHERE e.epoch_number=:epoch"
    )
    if "pair_fact_events" in schema:
        queries["pair_facts"] = (
            "SELECT t.id::text AS token_id,pf.id::text,pf.pair_id::text,pf.received_at,"
            "pf.pair_created_at,pf.dex_identifier,pf.base_token_address,pf.quote_token_address "
            "FROM pair_fact_events pf JOIN pairs p ON p.id=pf.pair_id JOIN tokens t "
            "ON t.id=p.token_id JOIN collector_runs cr ON cr.id=pf.collector_run_id "
            "JOIN collection_epochs e ON e.id=cr.collection_epoch_id WHERE e.epoch_number=:epoch"
            + address_clause
            + time_filter.format(column="pf.received_at")
        )
    for family, table_name, alias, fields in (
        (
            "boosts",
            "boost_observations",
            "bo",
            "source_observed_at,active_boost_count,amount,total_amount",
        ),
        (
            "metadata",
            "token_metadata_events",
            "tm",
            "source_observed_at,name,symbol,metadata_uri,image_url,website_url,twitter,telegram",
        ),
        (
            "security",
            "token_security_snapshots",
            "ts",
            "source_observed_at,status,token_program,mint_authority,freeze_authority,raw_supply,decimals,extension_types",
        ),
    ):
        if table_name in schema:
            select_fields = ",".join(f"{alias}.{field}" for field in fields.split(","))
            queries[family] = (
                f"SELECT t.id::text AS token_id,{alias}.id::text,{alias}.received_at,"
                f"{select_fields} FROM {table_name} {alias}"
                + common.format(alias=alias)
                + address_clause
                + time_filter.format(column=f"{alias}.received_at")
            )
    if "market_context_snapshots" in schema:
        queries["context"] = (
            "SELECT mc.id::text,mc.bucket_start,mc.bucket_end,mc.received_at,mc.sol_usd_price,"
            "mc.sol_return_5m,mc.sol_realized_volatility_1h,mc.admitted_tokens,"
            "mc.mature_cohort_active_fraction,mc.pair_sample_count,mc.aggregate_volume_m5_usd,"
            "mc.aggregate_buys_m5,mc.aggregate_sells_m5 FROM market_context_snapshots mc "
            "JOIN collection_epochs e ON e.id=mc.collection_epoch_id WHERE e.epoch_number=:epoch"
            + time_filter.format(column="mc.received_at")
        )
    if "coverage_decisions" in schema:
        queries["coverage"] = (
            "SELECT t.id::text AS token_id,cd.id::text,cd.decided_at,"
            "cd.coverage_effective_at AS effective_at,cd.new_coverage_class AS coverage_class,"
            "cd.lifecycle_state FROM coverage_decisions cd JOIN tokens t ON t.id=cd.token_id "
            "WHERE cd.collection_epoch_id=(SELECT id FROM collection_epochs "
            "WHERE epoch_number=:epoch)"
            + address_clause
            + " AND (CAST(:end_at AS timestamptz) IS NULL "
            "OR cd.decided_at < CAST(:end_at AS timestamptz))"
        )
    if "candidate_events" in schema:
        queries["candidates"] = (
            "SELECT t.id::text AS token_id,ce.id::text,ce.candidate_at,"
            "ce.input_watermark,ce.trigger_type,ce.evidence_sha256 "
            "FROM candidate_events ce JOIN tokens t ON t.id=ce.token_id "
            "WHERE ce.collection_epoch_id=(SELECT id FROM collection_epochs "
            "WHERE epoch_number=:epoch)"
            + address_clause
            + time_filter.format(column="ce.candidate_at")
        )
    if "candidate_tier_events" in schema:
        queries["candidate_tiers"] = (
            "SELECT t.id::text AS token_id,ct.id::text,ct.decided_at,"
            "ct.input_watermark,ct.previous_tier,ct.new_tier,ct.reason_code "
            "FROM candidate_tier_events ct JOIN tokens t ON t.id=ct.token_id "
            "WHERE ct.collection_epoch_id=(SELECT id FROM collection_epochs "
            "WHERE epoch_number=:epoch)"
            + address_clause
            + time_filter.format(column="ct.decided_at")
        )
    for family, table_name, alias, received_column in (
        ("holder_snapshots", "holder_snapshots", "hs", "received_at"),
        (
            "trader_distribution_snapshots",
            "trader_distribution_snapshots",
            "td",
            "received_at",
        ),
        ("creator_history_snapshots", "creator_history_snapshots", "ch", "received_at"),
        ("liquidity_event_evidence", "liquidity_event_evidence", "li", "received_at"),
        (
            "wallet_relationship_edges",
            "wallet_relationship_edges",
            "we",
            "evidence_received_at",
        ),
        (
            "funding_relationship_evidence",
            "funding_relationship_evidence",
            "fr",
            "received_at",
        ),
        ("wallet_cluster_snapshots", "wallet_cluster_snapshots", "wc", "received_at"),
        ("security_feature_snapshots", "security_feature_snapshots", "sf", "received_at"),
    ):
        if table_name not in schema:
            continue
        availability = f"{alias}.availability" if "availability" in schema[table_name] else "NULL"
        completeness = f"{alias}.completeness" if "completeness" in schema[table_name] else "NULL"
        queries[family] = (
            f"SELECT t.id::text AS token_id,{alias}.id::text,"
            f"{alias}.{received_column} AS fact_received_at,{alias}.acquisition_mode,"
            f"{availability} AS availability,{completeness} AS completeness,"
            f"to_jsonb({alias}) AS values FROM {table_name} {alias} "
            f"JOIN tokens t ON t.id={alias}.token_id WHERE "
            f"{alias}.collection_epoch_id=(SELECT id FROM collection_epochs "
            "WHERE epoch_number=:epoch)"
            if "collection_epoch_id" in schema[table_name]
            else (
                f"SELECT t.id::text AS token_id,{alias}.id::text,"
                f"{alias}.{received_column} AS fact_received_at,{alias}.acquisition_mode,"
                f"{availability} AS availability,{completeness} AS completeness,"
                f"to_jsonb({alias}) AS values FROM {table_name} {alias} "
                f"JOIN tokens t ON t.id={alias}.token_id JOIN candidate_events ce "
                f"ON ce.id={alias}.candidate_id WHERE ce.collection_epoch_id="
                "(SELECT id FROM collection_epochs WHERE epoch_number=:epoch)"
            )
        )
        queries[family] += address_clause + time_filter.format(column=f"{alias}.{received_column}")
    return queries


async def _mapping_rows(
    session: AsyncSession, query: str, parameters: dict[str, object]
) -> list[dict[str, object]]:
    return [dict(row) for row in (await session.execute(text(query), parameters)).mappings()]


def _histories_from_duckdb(
    connection: duckdb.DuckDBPyConnection,
    families: Mapping[str, list[str]],
    *,
    epoch_number: int,
    epoch_id: str,
    epoch_valid: bool,
    descriptor: dict[str, object],
    token_addresses: Sequence[str] | None,
    start_at: datetime | None,
    end_at: datetime | None,
) -> tuple[TokenHistory, ...]:
    if (
        "tokens" not in families
        or "observations" not in families
        or "lifecycle_events" not in families
    ):
        raise ValueError("cold research source lacks required token/observation/lifecycle families")
    where, parameters = _duckdb_filters(token_addresses, start_at, end_at)
    observation_columns = _duck_columns(connection, "observations")
    optional_observation_projection = ",".join(
        f"o.{column}" if column in observation_columns else f"NULL AS {column}"
        for column in (
            "buys_h6",
            "sells_h6",
            "buys_h24",
            "sells_h24",
            "liquidity_base",
            "liquidity_quote",
        )
    )
    token_where = (
        ""
        if not token_addresses
        else f" WHERE address IN ({','.join('?' for _ in token_addresses)})"
    )
    rows: dict[str, list[dict[str, object]]] = {
        "tokens": _duck_rows(
            connection,
            "SELECT DISTINCT id AS token_id,chain,address FROM tokens"
            + token_where
            + " ORDER BY address",
            list(token_addresses or ()),
        ),
        "observations": _duck_rows(
            connection,
            "SELECT t.id AS token_id,o.id,o.pair_id,"
            "o.received_at::VARCHAR AS received_at,"
            "o.source_observed_at::VARCHAR AS source_observed_at,"
            "o.price_usd,o.price_native,o.liquidity_usd,o.market_cap_usd,"
            "o.fully_diluted_valuation_usd,o.volume_m5_usd,o.volume_h1_usd,"
            "o.volume_h6_usd,o.volume_h24_usd,o.buys_m5,o.sells_m5,"
            "o.buys_h1,o.sells_h1," + optional_observation_projection + ","
            "p.address AS pair_address FROM observations o "
            "JOIN pairs p ON p.id=o.pair_id JOIN tokens t ON t.id=p.token_id"
            + where.format(column="o.received_at")
            + " ORDER BY t.id,o.received_at,o.id",
            parameters,
        ),
        "lifecycle": _duck_rows(
            connection,
            "SELECT t.id AS token_id,le.id,le.previous_state,le.new_state,le.reason_code,"
            "le.decided_at::VARCHAR AS decided_at,"
            "le.input_watermark::VARCHAR AS input_watermark FROM lifecycle_events le "
            "JOIN tokens t ON t.id=le.token_id"
            + where.format(column="le.decided_at")
            + " ORDER BY t.id,le.decided_at,le.id",
            parameters,
        ),
    }
    optional = {
        "discoveries": (
            "discovery_events",
            "de",
            "de.received_at",
            "de.id,de.event_type,de.received_at::VARCHAR AS received_at,"
            "de.source_event_at::VARCHAR AS source_event_at",
        ),
        "pair_facts": (
            "pair_fact_events",
            "pf",
            "pf.received_at",
            "pf.id,pf.pair_id,pf.dex_identifier,pf.base_token_address,"
            "pf.quote_token_address,pf.received_at::VARCHAR AS received_at,"
            "pf.pair_created_at::VARCHAR AS pair_created_at",
        ),
        "boosts": (
            "boost_observations",
            "bo",
            "bo.received_at",
            "bo.id,bo.active_boost_count,bo.amount,bo.total_amount,"
            "bo.received_at::VARCHAR AS received_at,"
            "bo.source_observed_at::VARCHAR AS source_observed_at",
        ),
        "metadata": (
            "token_metadata_events",
            "tm",
            "tm.received_at",
            "tm.id,tm.name,tm.symbol,tm.metadata_uri,tm.image_url,tm.website_url,"
            "tm.twitter,tm.telegram,tm.received_at::VARCHAR AS received_at,"
            "tm.source_observed_at::VARCHAR AS source_observed_at",
        ),
        "security": (
            "token_security_snapshots",
            "ts",
            "ts.received_at",
            "ts.id,ts.status,ts.token_program,ts.mint_authority,ts.freeze_authority,"
            "ts.raw_supply,ts.decimals,ts.extension_types,"
            "ts.received_at::VARCHAR AS received_at,"
            "ts.source_observed_at::VARCHAR AS source_observed_at",
        ),
        "coverage": (
            "coverage_decisions",
            "cd",
            "cd.decided_at",
            "cd.id,cd.new_coverage_class AS coverage_class,cd.lifecycle_state,"
            "cd.decided_at::VARCHAR AS decided_at,"
            "cd.coverage_effective_at::VARCHAR AS effective_at",
        ),
        "candidates": (
            "candidate_events",
            "ce",
            "ce.candidate_at",
            "ce.id,ce.trigger_type,ce.evidence_sha256,"
            "ce.candidate_at::VARCHAR AS candidate_at,"
            "ce.input_watermark::VARCHAR AS input_watermark",
        ),
        "candidate_tiers": (
            "candidate_tier_events",
            "ct",
            "ct.decided_at",
            "ct.id,ct.previous_tier,ct.new_tier,ct.reason_code,"
            "ct.decided_at::VARCHAR AS decided_at,"
            "ct.input_watermark::VARCHAR AS input_watermark",
        ),
    }
    for output, (family, alias, column, projection) in optional.items():
        if family not in families:
            continue
        join = (
            f" JOIN pairs p ON p.id={alias}.pair_id JOIN tokens t ON t.id=p.token_id"
            if family == "pair_fact_events"
            else f" JOIN tokens t ON t.id={alias}.token_id"
        )
        rows[output] = _duck_rows(
            connection,
            f"SELECT t.id AS token_id,{projection} FROM {family} {alias}{join}"
            + where.format(column=column),
            parameters,
        )
    for family, alias, received_column in (
        ("holder_snapshots", "hs", "received_at"),
        ("trader_distribution_snapshots", "td", "received_at"),
        ("creator_history_snapshots", "ch", "received_at"),
        ("liquidity_event_evidence", "li", "received_at"),
        ("wallet_relationship_edges", "we", "evidence_received_at"),
        ("funding_relationship_evidence", "fr", "received_at"),
        ("wallet_cluster_snapshots", "wc", "received_at"),
        ("security_feature_snapshots", "sf", "received_at"),
    ):
        if family not in families:
            continue
        columns = _duck_columns(connection, family)
        availability = f"{alias}.availability" if "availability" in columns else "NULL"
        completeness = f"{alias}.completeness" if "completeness" in columns else "NULL"
        rows[family] = _duck_rows(
            connection,
            f"SELECT t.id AS token_id,{alias}.id,{alias}.acquisition_mode,"
            f"to_json({alias}) AS values,"
            f"{alias}.{received_column}::VARCHAR AS fact_received_at,"
            f"{availability} AS fact_availability,"
            f"{completeness} AS fact_completeness FROM {family} {alias} "
            f"JOIN tokens t ON t.id={alias}.token_id"
            + where.format(column=f"{alias}.{received_column}"),
            parameters,
        )
    if "market_context_snapshots" in families:
        context_filter, context_parameters = _time_only_filter(start_at, end_at, "received_at")
        rows["context"] = _duck_rows(
            connection,
            "SELECT id,sol_usd_price,sol_return_5m,sol_realized_volatility_1h,"
            "admitted_tokens,mature_cohort_active_fraction,pair_sample_count,"
            "aggregate_volume_m5_usd,aggregate_buys_m5,aggregate_sells_m5,"
            "bucket_start::VARCHAR AS bucket_start,bucket_end::VARCHAR AS bucket_end,"
            "received_at::VARCHAR AS received_at,"
            "source_observed_at::VARCHAR AS source_observed_at "
            "FROM market_context_snapshots" + context_filter,
            context_parameters,
        )
    return _assemble_histories(
        rows,
        epoch_id=epoch_id,
        epoch_number=epoch_number,
        epoch_valid=epoch_valid,
        descriptor=descriptor,
    )


def _duckdb_filters(
    addresses: Sequence[str] | None, start_at: datetime | None, end_at: datetime | None
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    parameters: list[object] = []
    if addresses:
        clauses.append(f"t.address IN ({','.join('?' for _ in addresses)})")
        parameters.extend(addresses)
    if start_at:
        clauses.append("{column} >= ?")
        parameters.append(utc(start_at))
    if end_at:
        clauses.append("{column} < ?")
        parameters.append(utc(end_at))
    return (" WHERE " + " AND ".join(clauses) if clauses else "", parameters)


def _time_only_filter(
    start_at: datetime | None, end_at: datetime | None, column: str
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    parameters: list[object] = []
    if start_at:
        clauses.append(f"{column} >= ?")
        parameters.append(utc(start_at))
    if end_at:
        clauses.append(f"{column} < ?")
        parameters.append(utc(end_at))
    return (" WHERE " + " AND ".join(clauses) if clauses else "", parameters)


def _duck_rows(
    connection: duckdb.DuckDBPyConnection, query: str, parameters: Sequence[object]
) -> list[dict[str, object]]:
    cursor = connection.execute(query, list(parameters))
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _duck_columns(connection: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {str(row[0]) for row in connection.execute(f'DESCRIBE "{relation}"').fetchall()}


def _assemble_histories(
    rows: dict[str, list[dict[str, object]]],
    *,
    epoch_id: str,
    epoch_number: int,
    epoch_valid: bool,
    descriptor: dict[str, object],
) -> tuple[TokenHistory, ...]:
    grouped: dict[str, dict[str, list[dict[str, object]]]] = defaultdict(lambda: defaultdict(list))
    for family, family_rows in rows.items():
        if family in {"tokens", "context"}:
            continue
        _reject_duplicate_ids(family, family_rows)
        for row in family_rows:
            grouped[str(row["token_id"])][family].append(row)
    contexts = tuple(_context(row) for row in rows.get("context", []))
    histories: list[TokenHistory] = []
    seen_tokens: set[str] = set()
    for token in rows.get("tokens", []):
        token_id = str(token["token_id"])
        if token_id in seen_tokens:
            continue
        seen_tokens.add(token_id)
        facts = grouped[token_id]
        histories.append(
            TokenHistory(
                epoch_id=epoch_id,
                epoch_number=epoch_number,
                epoch_data_valid=epoch_valid,
                token_id=token_id,
                chain=str(token["chain"]),
                address=str(token["address"]),
                discoveries=tuple(_discovery(row) for row in facts["discoveries"]),
                observations=tuple(_observation(row) for row in facts["observations"]),
                lifecycle=tuple(_lifecycle(row) for row in facts["lifecycle"]),
                pair_facts=tuple(_pair_fact(row) for row in facts["pair_facts"]),
                boosts=tuple(_boost(row) for row in facts["boosts"]),
                metadata=tuple(_metadata(row) for row in facts["metadata"]),
                security=tuple(_security(row) for row in facts["security"]),
                context=contexts,
                coverage=tuple(_coverage(row) for row in facts["coverage"]),
                candidates=tuple(_candidate(row) for row in facts["candidates"]),
                candidate_tiers=tuple(_candidate_tier(row) for row in facts["candidate_tiers"]),
                selective_security=tuple(
                    _selective_security(family, row)
                    for family in (
                        "holder_snapshots",
                        "trader_distribution_snapshots",
                        "creator_history_snapshots",
                        "liquidity_event_evidence",
                        "wallet_relationship_edges",
                        "funding_relationship_evidence",
                        "wallet_cluster_snapshots",
                        "security_feature_snapshots",
                    )
                    for row in facts[family]
                ),
                source_descriptor=descriptor,
            )
        )
    return tuple(sorted(histories, key=lambda item: item.address))


def _reject_duplicate_ids(family: str, rows: list[dict[str, object]]) -> None:
    identifiers = [str(row["id"]) for row in rows]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError(f"research source contains duplicate {family} identities")


def _discovery(row: Mapping[str, object]) -> DiscoveryFact:
    return DiscoveryFact(
        str(row["id"]),
        _dt(row["received_at"]),
        _maybe_dt(row.get("source_event_at")),
        str(row["event_type"]),
    )


def _observation(row: Mapping[str, object]) -> ObservationFact:
    return ObservationFact(
        id=str(row["id"]),
        pair_id=str(row["pair_id"]),
        pair_address=str(row["pair_address"]),
        received_at=_dt(row["received_at"]),
        source_observed_at=_maybe_dt(row.get("source_observed_at")),
        price_usd=_decimal(row.get("price_usd")),
        price_native=_decimal(row.get("price_native")),
        liquidity_usd=_decimal(row.get("liquidity_usd")),
        market_cap_usd=_decimal(row.get("market_cap_usd")),
        fully_diluted_valuation_usd=_decimal(row.get("fully_diluted_valuation_usd")),
        volume_m5_usd=_decimal(row.get("volume_m5_usd")),
        volume_h1_usd=_decimal(row.get("volume_h1_usd")),
        volume_h6_usd=_decimal(row.get("volume_h6_usd")),
        volume_h24_usd=_decimal(row.get("volume_h24_usd")),
        buys_m5=_int(row.get("buys_m5")),
        sells_m5=_int(row.get("sells_m5")),
        buys_h1=_int(row.get("buys_h1")),
        sells_h1=_int(row.get("sells_h1")),
        buys_h6=_int(row.get("buys_h6")),
        sells_h6=_int(row.get("sells_h6")),
        buys_h24=_int(row.get("buys_h24")),
        sells_h24=_int(row.get("sells_h24")),
        liquidity_base=_decimal(row.get("liquidity_base")),
        liquidity_quote=_decimal(row.get("liquidity_quote")),
    )


def _lifecycle(row: Mapping[str, object]) -> LifecycleFact:
    return LifecycleFact(
        str(row["id"]),
        _dt(row["decided_at"]),
        _dt(row["input_watermark"]),
        _str(row.get("previous_state")),
        str(row["new_state"]),
        str(row["reason_code"]),
    )


def _pair_fact(row: Mapping[str, object]) -> PairFact:
    return PairFact(
        str(row["id"]),
        str(row["pair_id"]),
        _dt(row["received_at"]),
        _maybe_dt(row.get("pair_created_at")),
        _str(row.get("dex_identifier")),
        _str(row.get("base_token_address")),
        _str(row.get("quote_token_address")),
    )


def _boost(row: Mapping[str, object]) -> BoostFact:
    return BoostFact(
        str(row["id"]),
        _dt(row["received_at"]),
        _maybe_dt(row.get("source_observed_at")),
        _int(row.get("active_boost_count")),
        _decimal(row.get("amount")),
        _decimal(row.get("total_amount")),
    )


def _metadata(row: Mapping[str, object]) -> MetadataFact:
    return MetadataFact(
        str(row["id"]),
        _dt(row["received_at"]),
        _maybe_dt(row.get("source_observed_at")),
        *(
            _str(row.get(field))
            for field in (
                "name",
                "symbol",
                "metadata_uri",
                "image_url",
                "website_url",
                "twitter",
                "telegram",
            )
        ),
    )


def _security(row: Mapping[str, object]) -> SecurityFact:
    extensions = row.get("extension_types")
    if isinstance(extensions, str):
        extensions = json.loads(extensions)
    return SecurityFact(
        str(row["id"]),
        _dt(row["received_at"]),
        _maybe_dt(row.get("source_observed_at")),
        str(row.get("status") or "unavailable"),
        str(row.get("token_program") or "unknown"),
        _str(row.get("mint_authority")),
        _str(row.get("freeze_authority")),
        _decimal(row.get("raw_supply")),
        _int(row.get("decimals")),
        tuple(str(item) for item in extensions) if isinstance(extensions, list) else None,
    )


def _context(row: Mapping[str, object]) -> MarketContextFact:
    return MarketContextFact(
        str(row["id"]),
        _dt(row["bucket_start"]),
        _dt(row["bucket_end"]),
        _dt(row["received_at"]),
        _decimal(row.get("sol_usd_price")),
        _decimal(row.get("sol_return_5m")),
        _decimal(row.get("sol_realized_volatility_1h")),
        _int(row.get("admitted_tokens")),
        _decimal(row.get("mature_cohort_active_fraction")),
        _int(row.get("pair_sample_count")),
        _decimal(row.get("aggregate_volume_m5_usd")),
        _int(row.get("aggregate_buys_m5")),
        _int(row.get("aggregate_sells_m5")),
    )


def _coverage(row: Mapping[str, object]) -> CoverageFact:
    return CoverageFact(
        str(row["id"]),
        _dt(row["decided_at"]),
        _dt(row.get("effective_at") or row["coverage_effective_at"]),
        str(row.get("coverage_class") or row["new_coverage_class"]),
        str(row["lifecycle_state"]),
    )


def _candidate(row: Mapping[str, object]) -> CandidateFact:
    return CandidateFact(
        str(row["id"]),
        _dt(row["candidate_at"]),
        _dt(row["input_watermark"]),
        str(row["trigger_type"]),
        str(row["evidence_sha256"]),
    )


def _candidate_tier(row: Mapping[str, object]) -> CandidateTierFact:
    return CandidateTierFact(
        str(row["id"]),
        _dt(row["decided_at"]),
        _dt(row["input_watermark"]),
        str(row["previous_tier"]),
        str(row["new_tier"]),
        str(row["reason_code"]),
    )


def _selective_security(family: str, row: Mapping[str, object]) -> SelectiveSecurityFact:
    raw_values = row.get("values")
    if isinstance(raw_values, str):
        raw_values = json.loads(raw_values)
    if isinstance(raw_values, dict):
        values = dict(raw_values)
    else:
        excluded = {
            "token_id",
            "id",
            "fact_received_at",
            "fact_availability",
            "fact_completeness",
        }
        values = {key: value for key, value in row.items() if key not in excluded}
    return SelectiveSecurityFact(
        id=str(row["id"]),
        family=family,
        received_at=_dt(row.get("fact_received_at") or row["received_at"]),
        acquisition_mode=str(row["acquisition_mode"]),
        availability=_str(row.get("fact_availability") or row.get("availability")),
        completeness=_str(row.get("fact_completeness") or row.get("completeness")),
        values=values,
    )


def _with_descriptor(history: TokenHistory, descriptor: dict[str, object]) -> TokenHistory:
    return replace(history, source_descriptor=descriptor)


def _history_digest(history: TokenHistory) -> str:
    return canonical_digest(
        {
            "epoch": history.epoch_number,
            "token": history.token_id,
            "counts": {
                "observations": len(history.observations),
                "lifecycle": len(history.lifecycle),
                "boosts": len(history.boosts),
            },
        }
    )


def _dt(value: object) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise ValueError(f"expected timestamp, got {type(value).__name__}")
    return utc(value)


def _maybe_dt(value: object) -> datetime | None:
    return _dt(value) if value is not None else None


def _decimal(value: object) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


def _int(value: object) -> int | None:
    if value is None:
        return None
    if not isinstance(value, (int, str, Decimal)):
        raise ValueError(f"expected integer-compatible value, got {type(value).__name__}")
    return int(value)


def _str(value: object) -> str | None:
    return str(value) if value is not None else None


def _sha256_file(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()
