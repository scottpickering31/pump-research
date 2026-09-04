from __future__ import annotations

import json
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from pump_research.research.asof import ResearchContractError, get_token_state_as_of
from pump_research.research.candidates import (
    deterministic_stratum,
    generate_candidate_timestamps,
)
from pump_research.research.contracts import (
    BoostFact,
    CandidateFact,
    CandidateTierFact,
    CandidateTimestampPolicy,
    ChronologicalSplitContract,
    DiscoveryFact,
    LifecycleFact,
    MarketContextFact,
    MetadataFact,
    ObservationFact,
    SecurityFact,
    SelectiveSecurityFact,
    SplitWindow,
    TokenHistory,
)
from pump_research.research.dataset import (
    DatasetBuildSpec,
    DatasetDiskSafetyError,
    build_dataset,
    verify_dataset,
)
from pump_research.research.features import build_market_features
from pump_research.research.labels import build_outcome_labels
from pump_research.research.sources import (
    DuckDBArchiveResearchSource,
    HotColdResearchSource,
    InMemoryResearchSource,
)

T0 = datetime(2026, 8, 16, 12, 0, tzinfo=UTC)
QENIS = "76is6tHSLhCyRr3kQYpT9P3KVd4bVDgLsVh8mjjDpump"
IU24 = "24iuWxHS71FePsDAkmxypASu3gu8HDLdwehDbavVpump"


def _history(*, address: str = QENIS, valid: bool = True) -> TokenHistory:
    observations: list[ObservationFact] = []
    for index in range(49):
        received = T0 + timedelta(seconds=index * 15)
        if index <= 24:
            price = Decimal("1") + Decimal(index) * Decimal("0.02")
            liquidity = Decimal("10000") + Decimal(index) * Decimal("100")
        else:
            price = max(Decimal("0.01"), Decimal("1.48") - Decimal(index - 24) * Decimal("0.35"))
            liquidity = max(
                Decimal("200"), Decimal("12400") - Decimal(index - 24) * Decimal("3000")
            )
        observations.append(
            ObservationFact(
                id=f"o-{index:03d}",
                pair_id="pair-1",
                pair_address="pair-address",
                received_at=received,
                source_observed_at=received - timedelta(minutes=1),
                price_usd=price,
                price_native=price / Decimal("150"),
                liquidity_usd=liquidity,
                market_cap_usd=price * Decimal("1000000"),
                fully_diluted_valuation_usd=price * Decimal("1000000"),
                volume_m5_usd=Decimal("1000") + index * Decimal("50"),
                volume_h1_usd=Decimal("5000") + index * Decimal("100"),
                buys_m5=30 + index,
                sells_m5=5 + index // 10,
                buys_h1=200 + index,
                sells_h1=50,
            )
        )
    return TokenHistory(
        epoch_id="epoch-2" if valid else "epoch-1",
        epoch_number=2 if valid else 1,
        epoch_data_valid=valid,
        token_id="token-1",
        chain="solana",
        address=address,
        discoveries=(DiscoveryFact("d-1", T0 - timedelta(seconds=5), T0 - timedelta(hours=1)),),
        observations=tuple(observations),
        lifecycle=(
            LifecycleFact("l-new", T0, T0, "PENDING_DEX", "NEW", "dex_visible"),
            LifecycleFact(
                "l-active",
                T0 + timedelta(minutes=3),
                T0 + timedelta(minutes=3),
                "NEW",
                "ACTIVE",
                "activity",
            ),
            LifecycleFact(
                "l-fading",
                T0 + timedelta(minutes=8),
                T0 + timedelta(minutes=8),
                "ACTIVE",
                "FADING",
                "collapse",
            ),
        ),
        boosts=(
            BoostFact(
                "b-late",
                T0 + timedelta(minutes=4),
                T0 - timedelta(hours=1),
                1,
                Decimal("10"),
                Decimal("10"),
            ),
        ),
        metadata=(
            MetadataFact("m-old", T0, name="Before"),
            MetadataFact(
                "m-late",
                T0 + timedelta(minutes=4),
                T0 - timedelta(days=1),
                name="After",
            ),
        ),
        security=(
            SecurityFact(
                "s-old",
                T0 + timedelta(seconds=5),
                status="available",
                token_program="spl_token",
                mint_authority="authority",
                freeze_authority="freeze",
            ),
            SecurityFact(
                "s-late",
                T0 + timedelta(minutes=4),
                T0 - timedelta(days=1),
                status="available",
                token_program="spl_token",
                mint_authority=None,
                freeze_authority=None,
            ),
        ),
        context=(
            MarketContextFact(
                "c-closed",
                T0,
                T0 + timedelta(minutes=5),
                T0 + timedelta(minutes=5, seconds=1),
                sol_usd_price=Decimal("150"),
                admitted_tokens=10,
            ),
        ),
        candidates=(
            CandidateFact(
                "candidate-late",
                T0 + timedelta(minutes=4),
                T0 + timedelta(minutes=4),
                "MARKET_ACTIVITY",
                "f" * 64,
            ),
        ),
        candidate_tiers=(
            CandidateTierFact(
                "tier-late",
                T0 + timedelta(minutes=4),
                T0 + timedelta(minutes=4),
                "TIER_0_UNIVERSAL",
                "TIER_1_INTERESTING",
                "MARKET_ACTIVITY",
            ),
        ),
        selective_security=(
            SelectiveSecurityFact(
                "holder-early",
                "holder_snapshots",
                T0 + timedelta(minutes=1),
                "historically_available",
                "partial",
                "top_20_token_accounts",
                {"top_10_pct": "25"},
            ),
            SelectiveSecurityFact(
                "holder-late",
                "holder_snapshots",
                T0 + timedelta(minutes=4),
                "historically_available",
                "available",
                "full_distribution",
                {"top_10_pct": "80"},
            ),
            SelectiveSecurityFact(
                "edge-reconstructed",
                "wallet_relationship_edges",
                T0 + timedelta(minutes=1),
                "retrospectively_reconstructed",
                values={"relationship_type": "COMMON_FUNDER"},
            ),
        ),
        source_descriptor={"kind": "fixture", "revision": "1"},
    )


def test_as_of_leakage_attacks_cannot_see_late_backdated_facts() -> None:
    history = _history()
    decision = T0 + timedelta(minutes=2)
    state = get_token_state_as_of(history, decision)
    assert state.lifecycle is not None and state.lifecycle.new_state == "NEW"
    assert state.boost is None
    assert state.metadata is not None and state.metadata.name == "Before"
    assert state.security is not None and state.security.mint_authority == "authority"
    assert state.context is None
    assert state.candidate is None
    assert state.candidate_tier is None
    assert state.holder_snapshot is not None
    assert state.holder_snapshot.id == "holder-early"
    assert state.wallet_edges == ()
    assert state.current_observation is not None
    assert state.current_observation.received_at == decision
    assert all(item.received_at <= decision for item in state.observation_history)

    later = get_token_state_as_of(history, T0 + timedelta(minutes=5, seconds=1))
    assert later.lifecycle is not None and later.lifecycle.new_state == "ACTIVE"
    assert later.boost is not None and later.boost.amount == Decimal("10")
    assert later.metadata is not None and later.metadata.name == "After"
    assert later.security is not None and later.security.mint_authority is None
    assert later.context is not None
    assert later.candidate is not None
    assert later.candidate_tier is not None
    assert later.holder_snapshot is not None
    assert later.holder_snapshot.id == "holder-late"
    assert later.wallet_edges == ()


def test_future_price_never_enters_rolling_features() -> None:
    history = _history()
    decision = T0 + timedelta(minutes=2)
    state = get_token_state_as_of(history, decision)
    features = build_market_features(state).values
    current = next(item for item in history.observations if item.received_at == decision)
    baseline = next(
        item for item in history.observations if item.received_at == decision - timedelta(minutes=1)
    )
    assert current.price_usd is not None
    assert baseline.price_usd is not None
    assert features["return_1m"] == pytest.approx(float(current.price_usd / baseline.price_usd - 1))
    mutated = replace(
        history,
        observations=history.observations
        + (
            replace(
                history.observations[-1],
                id="future-attack",
                received_at=decision + timedelta(microseconds=1),
                source_observed_at=T0 - timedelta(days=1),
                price_usd=Decimal("999999"),
            ),
        ),
    )
    attacked = build_market_features(get_token_state_as_of(mutated, decision)).values
    assert attacked == features


def test_labels_use_future_separately_and_unknown_is_not_false() -> None:
    history = _history()
    state = get_token_state_as_of(history, T0 + timedelta(minutes=2))
    labels = build_outcome_labels(history, state).values
    assert labels["crossed_minus_80pct_5m"] == "TRUE"
    assert labels["major_liquidity_collapse_5m"] == "TRUE"
    assert labels["theoretical_market_return_5m"] is not None
    assert labels["crossed_plus_50pct_24h"] == "UNKNOWN"
    assert labels["execution_adjusted_return"] is None
    assert labels["entry_notional_1000_liquidity_ratio"] is not None
    assert labels["exit_notional_1000_liquidity_ratio_5m"] is not None


def test_candidate_universe_and_strata_do_not_use_outcomes() -> None:
    policy = CandidateTimestampPolicy()
    original = generate_candidate_timestamps(_history(), policy)
    no_future = replace(_history(), observations=_history().observations[:10])
    truncated = generate_candidate_timestamps(no_future, policy)
    assert [item.id for item in original] == [item.id for item in truncated]
    assert deterministic_stratum(original[0].id, 10) == deterministic_stratum(original[0].id, 10)


def test_chronological_split_is_token_exclusive_and_purges_crossing_labels() -> None:
    contract = ChronologicalSplitContract(
        windows=(
            SplitWindow("train", T0, T0 + timedelta(days=10), T0 + timedelta(days=10)),
            SplitWindow(
                "validation",
                T0 + timedelta(days=11),
                T0 + timedelta(days=15),
                T0 + timedelta(days=15),
            ),
            SplitWindow(
                "test",
                T0 + timedelta(days=16),
                T0 + timedelta(days=20),
                T0 + timedelta(days=20),
            ),
        ),
        maximum_label_horizon_seconds=86400,
    )
    assert contract.assign(T0, T0 + timedelta(days=2)) == "train"
    assert contract.assign(T0, T0 + timedelta(days=9, hours=12)) is None
    assert contract.assign(T0, T0 + timedelta(days=12)) is None
    assert contract.assign(T0 + timedelta(days=11), T0 + timedelta(days=12)) == "validation"
    with pytest.raises(ValueError, match="purge horizon"):
        DatasetBuildSpec(
            epoch_numbers=(2,),
            scope_start_at=T0,
            scope_end_at=T0 + timedelta(days=20),
            split_contract=contract,
        )


@pytest.mark.asyncio
async def test_dataset_rebuild_is_deterministic_and_disk_guard_fails_closed(tmp_path: Path) -> None:
    history = _history()
    spec = DatasetBuildSpec(
        epoch_numbers=(2,),
        scope_start_at=T0,
        scope_end_at=T0 + timedelta(hours=2),
        token_addresses=(QENIS,),
        code_revision="test-revision",
        minimum_free_bytes=1,
    )
    manifest = await build_dataset((history,), spec=spec, output=tmp_path, now=T0)
    first = manifest.read_bytes()
    first_verification = verify_dataset(manifest)
    rebuilt = await build_dataset(
        (history,), spec=spec, output=tmp_path, now=T0 + timedelta(days=1)
    )
    assert rebuilt == manifest
    assert rebuilt.read_bytes() == first
    assert verify_dataset(rebuilt) == first_verification
    changed_history = replace(
        history,
        observations=(
            replace(history.observations[0], price_usd=Decimal("9")),
            *history.observations[1:],
        ),
    )
    changed_manifest = await build_dataset((changed_history,), spec=spec, output=tmp_path, now=T0)
    assert changed_manifest != manifest
    with pytest.raises(DatasetDiskSafetyError):
        await build_dataset(
            (history,),
            spec=replace(spec, code_revision="different"),
            output=tmp_path / "unsafe",
            free_bytes_override=1,
        )


def test_qenis_features_are_predecision_and_collapse_is_only_a_label() -> None:
    history = _history(address=QENIS)
    decision = T0 + timedelta(minutes=2)
    state = get_token_state_as_of(history, decision)
    features = build_market_features(state).values
    labels = build_outcome_labels(history, state).values
    assert features["path_smoothness_5m"] is not None
    assert cast_float(features["buy_ratio_m5"]) > 0.8
    assert cast_float(features["realized_volatility_5m"]) < 0.02
    assert "time_to_collapse_seconds" not in features
    assert labels["time_to_collapse_seconds"] is not None


def test_24iu_invalid_explosive_history_cannot_be_spliced_into_epoch2() -> None:
    invalid = _history(address=IU24, valid=False)
    with pytest.raises(ResearchContractError):
        get_token_state_as_of(invalid, T0 + timedelta(minutes=2))
    valid_flat = replace(
        _history(address=IU24),
        observations=tuple(
            replace(item, price_usd=Decimal("0.000000131"), market_cap_usd=Decimal("263"))
            for item in _history(address=IU24).observations[:8]
        ),
    )
    state = get_token_state_as_of(valid_flat, T0 + timedelta(minutes=2))
    labels = build_outcome_labels(valid_flat, state).values
    assert labels["crossed_plus_100pct_5m"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_cold_archive_with_future_rows_matches_in_memory_as_of(tmp_path: Path) -> None:
    history = _history()
    manifest = _write_cold_fixture(tmp_path, history)
    cold = DuckDBArchiveResearchSource([manifest], require_verified=False)
    loaded = await cold.load_histories(epoch_number=2, token_addresses=[QENIS])
    assert len(loaded) == 1
    decision = T0 + timedelta(minutes=2)
    expected = build_market_features(get_token_state_as_of(history, decision)).values
    actual = build_market_features(get_token_state_as_of(loaded[0], decision)).values
    assert actual == expected
    expected_labels = build_outcome_labels(history, get_token_state_as_of(history, decision)).values
    actual_labels = build_outcome_labels(
        loaded[0], get_token_state_as_of(loaded[0], decision)
    ).values
    assert actual_labels == expected_labels


@pytest.mark.asyncio
async def test_pre_phase2_archive_normalizes_absent_columns_to_unknown(tmp_path: Path) -> None:
    manifest = _write_cold_fixture(tmp_path, _history(), include_phase2_columns=False)
    cold = DuckDBArchiveResearchSource([manifest], require_verified=False)
    loaded = await cold.load_histories(epoch_number=2, token_addresses=[QENIS])
    observation = loaded[0].observations[0]
    assert observation.buys_h6 is None
    assert observation.liquidity_base is None


@pytest.mark.asyncio
async def test_explicit_hot_cold_cutoff_matches_single_immutable_history() -> None:
    history = _history()
    combined = HotColdResearchSource(
        InMemoryResearchSource((history,), name="cold"),
        InMemoryResearchSource((history,), name="hot"),
        hot_from=T0 + timedelta(minutes=4),
    )
    loaded = await combined.load_histories(epoch_number=2)
    assert len(loaded) == 1
    assert len(loaded[0].observations) == len(history.observations)
    decision = T0 + timedelta(minutes=5)
    expected = build_market_features(get_token_state_as_of(history, decision)).values
    actual = build_market_features(get_token_state_as_of(loaded[0], decision)).values
    assert actual == expected
    assert loaded[0].source_descriptor["kind"] == "hot_cold"


def _write_cold_fixture(
    root: Path, history: TokenHistory, *, include_phase2_columns: bool = True
) -> Path:
    observation_rows = [_observation_archive_row(item) for item in history.observations]
    if not include_phase2_columns:
        for row in observation_rows:
            for column in (
                "buys_h6",
                "sells_h6",
                "buys_h24",
                "sells_h24",
                "liquidity_base",
                "liquidity_quote",
            ):
                row.pop(column)
    files = {
        "collector_runs": [
            {
                "id": "00000000-0000-0000-0000-000000000002",
                "started_at": T0,
                "collection_started_at": T0,
                "finished_at": T0 + timedelta(days=30),
                "status": "stopped",
            }
        ],
        "tokens": [{"id": history.token_id, "chain": history.chain, "address": history.address}],
        "pairs": [
            {
                "id": "pair-1",
                "token_id": history.token_id,
                "chain": "solana",
                "address": "pair-address",
            }
        ],
        "observations": observation_rows,
        "lifecycle_events": [
            {
                "id": item.id,
                "token_id": history.token_id,
                "decided_at": item.decided_at,
                "input_watermark": item.input_watermark,
                "previous_state": item.previous_state,
                "new_state": item.new_state,
                "reason_code": item.reason_code,
            }
            for item in history.lifecycle
        ],
        "boost_observations": [
            {**asdict(item), "token_id": history.token_id} for item in history.boosts
        ],
        "token_metadata_events": [
            {**asdict(item), "token_id": history.token_id} for item in history.metadata
        ],
        "token_security_snapshots": [
            {**asdict(item), "token_id": history.token_id} for item in history.security
        ],
        "market_context_snapshots": [
            {**asdict(item), "source_observed_at": item.bucket_end} for item in history.context
        ],
    }
    entries = []
    for family, rows in files.items():
        relative = Path("schema=v2") / f"family={family}" / "part-00000.parquet"
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows), target)
        entries.append({"family": family, "table": family, "file": relative.as_posix()})
    manifest = root / "schema=v2" / "manifests" / "epoch=2" / "scope=fixture" / "manifest.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(
        json.dumps(
            {
                "archive_schema_version": 2,
                "epoch": 2,
                "epoch_id": history.epoch_id,
                "epoch_data_valid": True,
                "entries": entries,
            }
        ),
        encoding="utf-8",
    )
    return manifest


def cast_float(value: object) -> float:
    assert isinstance(value, float)
    return value


def _observation_archive_row(item: ObservationFact) -> dict[str, object]:
    row = asdict(item)
    row.pop("pair_address")
    return row
