"""Immutable deterministic research dataset builder and verifier."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import resource
import shutil
import statistics
import subprocess
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from pump_research.research.asof import ResearchContractError, get_token_state_as_of
from pump_research.research.candidates import generate_candidate_timestamps
from pump_research.research.contracts import (
    CandidateTimestampPolicy,
    ChronologicalSplitContract,
    FeatureSetContract,
    LabelSetContract,
    TokenHistory,
    canonical_digest,
    utc,
)
from pump_research.research.features import build_market_features
from pump_research.research.labels import build_outcome_labels

DATASET_SCHEMA_VERSION = 1


class DatasetIntegrityError(RuntimeError):
    """Raised when an immutable research artifact conflicts or fails verification."""


class DatasetDiskSafetyError(RuntimeError):
    """Raised before output when staging space is unsafe."""


def research_code_revision() -> str:
    """Content revision covering committed HEAD and the exact research implementation."""
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=False, capture_output=True, text=True
    )
    head = result.stdout.strip() if result.returncode == 0 else "unknown"
    digest = hashlib.sha256()
    digest.update(head.encode())
    for path in sorted(Path(__file__).resolve().parent.glob("*.py")):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return f"{head}+research.{digest.hexdigest()[:16]}"


@dataclass(frozen=True, slots=True)
class DatasetBuildSpec:
    epoch_numbers: tuple[int, ...]
    scope_start_at: datetime
    scope_end_at: datetime
    feature_set: FeatureSetContract = field(default_factory=FeatureSetContract)
    label_set: LabelSetContract = field(default_factory=LabelSetContract)
    candidate_policy: CandidateTimestampPolicy = field(default_factory=CandidateTimestampPolicy)
    split_contract: ChronologicalSplitContract | None = None
    token_addresses: tuple[str, ...] = ()
    cohort_rule: str = "all_valid_epoch_admitted_tokens"
    code_revision: str = "unknown"
    configuration_sha256: str = "research-default"
    minimum_free_bytes: int = 1_073_741_824

    def __post_init__(self) -> None:
        if not self.epoch_numbers:
            raise ValueError("dataset requires at least one epoch")
        if utc(self.scope_start_at) >= utc(self.scope_end_at):
            raise ValueError("dataset scope requires start_at < end_at")
        if (
            self.split_contract is not None
            and self.split_contract.maximum_label_horizon_seconds
            < max(self.label_set.horizons_seconds)
        ):
            raise ValueError("split purge horizon is shorter than the label-set horizon")


async def build_dataset(
    histories: tuple[TokenHistory, ...],
    *,
    spec: DatasetBuildSpec,
    output: Path,
    now: datetime | None = None,
    free_bytes_override: int | None = None,
) -> Path:
    """Build one immutable Parquet dataset without mutating source facts."""
    started = time.perf_counter()
    selected = tuple(
        history
        for history in histories
        if history.epoch_number in spec.epoch_numbers
        and (not spec.token_addresses or history.address in spec.token_addresses)
    )
    if any(not history.epoch_data_valid for history in selected):
        raise ResearchContractError("canonical dataset scope contains an invalid epoch")
    semantic = _semantic_snapshot(selected, spec)
    identity = canonical_digest(semantic)
    final_dir = output.resolve() / f"dataset={identity}"
    manifest_path = final_dir / "manifest.json"
    if manifest_path.exists():
        verification = verify_dataset(manifest_path)
        if verification["dataset_identity"] != identity:
            raise DatasetIntegrityError("existing dataset identity has conflicting semantics")
        return manifest_path
    candidates = [
        candidate
        for history in selected
        for candidate in generate_candidate_timestamps(
            history,
            spec.candidate_policy,
            scope_start_at=spec.scope_start_at,
            scope_end_at=spec.scope_end_at,
        )
    ]
    candidates.sort(key=lambda item: (item.decision_at, item.token_id, item.id))
    candidate_examples_generated = len(candidates)
    source_observation_rows_scanned = sum(len(history.observations) for history in selected)
    estimated_bytes = max(len(candidates) * 4096, 64 * 1024 * 1024)
    _disk_preflight(output, estimated_bytes, spec.minimum_free_bytes, free_bytes_override)
    history_by_key = {(item.epoch_number, item.token_id): item for item in selected}
    rows: list[dict[str, object]] = []
    for candidate in candidates:
        history = history_by_key[(candidate.epoch_number, candidate.token_id)]
        state = get_token_state_as_of(history, candidate.decision_at, spec.feature_set)
        features = build_market_features(state, spec.feature_set)
        labels = build_outcome_labels(history, state, spec.label_set)
        split = (
            spec.split_contract.assign(candidate.admission_at, candidate.decision_at)
            if spec.split_contract
            else "unsplit"
        )
        if spec.split_contract is not None and split is None:
            continue
        row = {
            "candidate_id": candidate.id,
            "candidate_reason": candidate.reason,
            "candidate_policy_sha256": candidate.policy_sha256,
            "feature_set": spec.feature_set.identifier,
            "feature_set_sha256": spec.feature_set.sha256,
            "label_set": spec.label_set.identifier,
            "label_set_sha256": spec.label_set.sha256,
            "split": split,
            "feature_availability_watermark": features.availability_watermark,
            "feature_input_ids_sha256": canonical_digest(features.input_observation_ids),
            "label_future_ids_sha256": canonical_digest(labels.future_observation_ids),
            **features.values,
            **labels.values,
        }
        if (
            features.availability_watermark
            and features.availability_watermark > candidate.decision_at
        ):
            raise DatasetIntegrityError("feature availability watermark exceeds decision time")
        rows.append(row)
    _reject_duplicate_candidates(rows)
    table = _arrow_table(rows)
    schema_sha256 = canonical_digest(
        [(field.name, str(field.type), field.nullable) for field in table.schema]
    )
    staging = output.resolve() / f".incomplete-{identity}-{uuid.uuid4().hex}"
    staging.mkdir(parents=True, exist_ok=False)
    data_path = staging / "data.parquet"
    try:
        pq.write_table(
            table,
            data_path,
            compression="zstd",
            compression_level=6,
            use_dictionary=True,
            write_statistics=True,
            row_group_size=25_000,
        )
        file_sha256 = _sha256_file(data_path)
        content_sha256 = _row_content_sha256(rows)
        quality = _quality_report(rows, selected)
        duckdb_validation_seconds = _duckdb_read_benchmark(data_path)
        generated_at = utc(now or datetime.now(UTC))
        elapsed = time.perf_counter() - started
        manifest: dict[str, object] = {
            "dataset_schema_version": DATASET_SCHEMA_VERSION,
            "dataset_identity": identity,
            "semantic_contract": semantic,
            "feature_set": asdict(spec.feature_set),
            "feature_set_sha256": spec.feature_set.sha256,
            "label_set": asdict(spec.label_set),
            "label_set_sha256": spec.label_set.sha256,
            "candidate_policy": asdict(spec.candidate_policy),
            "candidate_policy_sha256": spec.candidate_policy.sha256,
            "split_contract": asdict(spec.split_contract) if spec.split_contract else None,
            "source_descriptors": semantic["source_descriptors"],
            "epoch_scope": list(spec.epoch_numbers),
            "scope_start_at": utc(spec.scope_start_at).isoformat(),
            "scope_end_at": utc(spec.scope_end_at).isoformat(),
            "cohort_rule": spec.cohort_rule,
            "token_addresses": list(spec.token_addresses),
            "code_revision": spec.code_revision,
            "configuration_sha256": spec.configuration_sha256,
            "generated_at": generated_at.isoformat(),
            "row_count": table.num_rows,
            "candidate_examples_generated": candidate_examples_generated,
            "source_observation_rows_scanned": source_observation_rows_scanned,
            "unique_token_count": len({str(row["token_id"]) for row in rows}),
            "column_count": table.num_columns,
            "schema": [(field.name, str(field.type), field.nullable) for field in table.schema],
            "schema_sha256": schema_sha256,
            "file": "data.parquet",
            "file_bytes": data_path.stat().st_size,
            "file_sha256": file_sha256,
            "content_sha256": content_sha256,
            "quality": quality,
            "build_seconds": round(elapsed, 6),
            "build_rows_per_second": round(table.num_rows / elapsed, 3) if elapsed else None,
            "duckdb_validation_seconds": round(duckdb_validation_seconds, 6),
            "peak_rss_bytes": _peak_rss_bytes(),
            "verification_status": "verified",
            "production_model_trained": False,
            "trading_decisions_present": False,
        }
        _atomic_json(staging / "manifest.json", manifest)
        digest = _sha256_file(staging / "manifest.json")
        _atomic_text(staging / "manifest.sha256", f"{digest}  manifest.json\n")
        _verify_dataset_files(staging / "manifest.json")
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        if final_dir.exists():
            existing = verify_dataset(final_dir / "manifest.json")
            if existing["content_sha256"] != content_sha256:
                raise DatasetIntegrityError("concurrent dataset publication differs")
            shutil.rmtree(staging)
            return final_dir / "manifest.json"
        os.replace(staging, final_dir)
        verify_dataset(final_dir / "manifest.json")
        return final_dir / "manifest.json"
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def verify_dataset(manifest_path: Path) -> dict[str, object]:
    manifest = _verify_dataset_files(manifest_path)
    data_path = manifest_path.parent / cast(str, manifest["file"])
    connection = duckdb.connect(database=":memory:")
    try:
        row = connection.execute(
            "SELECT count(*),count(DISTINCT candidate_id),count(DISTINCT token_id),"
            "min(decision_at)::VARCHAR,max(decision_at)::VARCHAR FROM read_parquet(?)",
            [str(data_path)],
        ).fetchone()
        if row is None:
            raise DatasetIntegrityError("DuckDB dataset verification returned no result")
        if int(row[0]) != _manifest_int(manifest, "row_count") or int(row[1]) != int(row[0]):
            raise DatasetIntegrityError("dataset row count or candidate uniqueness differs")
        if int(row[2]) != _manifest_int(manifest, "unique_token_count"):
            raise DatasetIntegrityError("dataset unique-token count differs")
    finally:
        connection.close()
    return {
        "verified": True,
        "dataset_identity": manifest["dataset_identity"],
        "row_count": manifest["row_count"],
        "unique_token_count": manifest["unique_token_count"],
        "file_bytes": manifest["file_bytes"],
        "file_sha256": manifest["file_sha256"],
        "content_sha256": manifest["content_sha256"],
        "schema_sha256": manifest["schema_sha256"],
    }


def inspect_dataset(manifest_path: Path) -> dict[str, object]:
    verification = verify_dataset(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        **verification,
        "quality": manifest["quality"],
        "semantic_contract": manifest["semantic_contract"],
    }


def _semantic_snapshot(
    histories: tuple[TokenHistory, ...], spec: DatasetBuildSpec
) -> dict[str, object]:
    descriptors = sorted(
        {
            canonical_digest(history.source_descriptor): history.source_descriptor
            for history in histories
        }.values(),
        key=canonical_digest,
    )
    return {
        "dataset_schema_version": DATASET_SCHEMA_VERSION,
        "feature_set_sha256": spec.feature_set.sha256,
        "label_set_sha256": spec.label_set.sha256,
        "candidate_policy_sha256": spec.candidate_policy.sha256,
        "split_contract_sha256": spec.split_contract.sha256 if spec.split_contract else None,
        "source_descriptors": descriptors,
        "input_facts_sha256": _histories_content_sha256(histories),
        "epochs": list(spec.epoch_numbers),
        "scope_start_at": utc(spec.scope_start_at).isoformat(),
        "scope_end_at": utc(spec.scope_end_at).isoformat(),
        "token_addresses": list(spec.token_addresses),
        "cohort_rule": spec.cohort_rule,
        "code_revision": spec.code_revision,
        "configuration_sha256": spec.configuration_sha256,
    }


def _histories_content_sha256(histories: tuple[TokenHistory, ...]) -> str:
    digest = hashlib.sha256()
    for history in sorted(histories, key=lambda item: (item.epoch_number, item.token_id)):
        digest.update(
            canonical_digest(
                {
                    "epoch_id": history.epoch_id,
                    "epoch_number": history.epoch_number,
                    "epoch_data_valid": history.epoch_data_valid,
                    "token_id": history.token_id,
                    "chain": history.chain,
                    "address": history.address,
                }
            ).encode()
        )
        for family in (
            history.discoveries,
            history.observations,
            history.lifecycle,
            history.pair_facts,
            history.boosts,
            history.metadata,
            history.security,
            history.context,
            history.coverage,
        ):
            for fact in sorted(family, key=lambda item: item.id):
                digest.update(canonical_digest(asdict(cast(Any, fact))).encode())
    return digest.hexdigest()


def _arrow_table(rows: list[dict[str, object]]) -> pa.Table:
    if not rows:
        schema = pa.schema(
            [
                pa.field("candidate_id", pa.string(), False),
                pa.field("token_id", pa.string(), False),
                pa.field("decision_at", pa.timestamp("us", tz="UTC"), False),
            ]
        )
        return pa.Table.from_pylist([], schema=schema)
    columns = sorted({key for row in rows for key in row})
    arrays: list[pa.Array] = []
    fields: list[pa.Field] = []
    for column in columns:
        values = [row.get(column) for row in rows]
        data_type = _column_type(column, values)
        arrays.append(pa.array(values, type=data_type))
        fields.append(pa.field(column, data_type, nullable=any(value is None for value in values)))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _column_type(column: str, values: list[object]) -> pa.DataType:
    present = next((value for value in values if value is not None), None)
    if isinstance(present, datetime) or column.endswith("_at") or column.endswith("_watermark"):
        return pa.timestamp("us", tz="UTC")
    if (
        isinstance(present, bool)
        or column.endswith("_known")
        or column.endswith("_available")
        or column.endswith("_complete")
        or column.startswith("abrupt_")
    ):
        return pa.bool_()
    if isinstance(present, int) and not isinstance(present, bool):
        return pa.int64()
    if isinstance(present, (float, Decimal)):
        return pa.float64()
    if present is None and _numeric_column(column):
        return pa.float64()
    return pa.string()


def _numeric_column(column: str) -> bool:
    fragments = (
        "price",
        "return",
        "liquidity",
        "volume",
        "market_cap",
        "fdv",
        "ratio",
        "imbalance",
        "velocity",
        "acceleration",
        "volatility",
        "drawdown",
        "recovery",
        "smoothness",
        "monotonicity",
        "seconds",
        "amount",
        "count",
        "rate",
        "supply",
    )
    return any(fragment in column for fragment in fragments)


def _quality_report(
    rows: list[dict[str, object]], histories: tuple[TokenHistory, ...]
) -> dict[str, object]:
    row_count = len(rows)
    columns = sorted({key for row in rows for key in row})
    null_rates = {
        column: (sum(row.get(column) is None for row in rows) / row_count if row_count else None)
        for column in columns
    }
    label_distributions: dict[str, dict[str, int]] = {}
    for column in columns:
        values = [row.get(column) for row in rows]
        if any(value in {"TRUE", "FALSE", "UNKNOWN"} for value in values):
            label_distributions[column] = {
                state: sum(value == state for value in values)
                for state in ("TRUE", "FALSE", "UNKNOWN")
            }
    cadence = [
        (right.received_at - left.received_at).total_seconds()
        for history in histories
        for left, right in zip(history.observations, history.observations[1:], strict=False)
        if right.received_at >= left.received_at
    ]
    impossible = [
        row["candidate_id"]
        for row in rows
        if (isinstance(row.get("price_usd"), (int, float)) and cast(float, row["price_usd"]) <= 0)
        or (
            isinstance(row.get("liquidity_usd"), (int, float))
            and cast(float, row["liquidity_usd"]) < 0
        )
    ]
    token_candidates: dict[str, int] = {}
    for row in rows:
        token_id = str(row["token_id"])
        token_candidates[token_id] = token_candidates.get(token_id, 0) + 1
    horizon_availability = {
        column: {
            "known": sum(row.get(column) is not None for row in rows),
            "unknown": sum(row.get(column) is None for row in rows),
        }
        for column in columns
        if column.startswith("theoretical_market_return_")
    }
    extreme_values: dict[str, dict[str, float | None]] = {}
    for column in ("price_usd", "market_cap_usd", "liquidity_usd", "volume_m5_usd"):
        numeric_values: list[float] = []
        for row in rows:
            value = row.get(column)
            if isinstance(value, (int, float)):
                numeric_values.append(float(value))
        extreme_values[column] = {
            "minimum": min(numeric_values) if numeric_values else None,
            "p50": _percentile(numeric_values, 0.5),
            "p99": _percentile(numeric_values, 0.99),
            "maximum": max(numeric_values) if numeric_values else None,
        }
    return {
        "null_rates": null_rates,
        "token_coverage": {
            "source_tokens": len(histories),
            "tokens_with_candidates": len(token_candidates),
            "candidates_per_token_minimum": min(token_candidates.values())
            if token_candidates
            else None,
            "candidates_per_token_median": statistics.median(token_candidates.values())
            if token_candidates
            else None,
            "candidates_per_token_maximum": max(token_candidates.values())
            if token_candidates
            else None,
        },
        "horizon_label_availability": horizon_availability,
        "label_distributions": label_distributions,
        "extreme_value_distributions": extreme_values,
        "rows_by_split": {
            str(split): sum(row.get("split") == split for row in rows)
            for split in sorted({row.get("split") for row in rows}, key=str)
        },
        "observation_cadence_seconds": {
            "count": len(cadence),
            "median": statistics.median(cadence) if cadence else None,
            "p95": _percentile(cadence, 0.95),
            "maximum": max(cadence) if cadence else None,
        },
        "duplicate_candidate_count": row_count - len({row["candidate_id"] for row in rows}),
        "impossible_value_candidate_ids": impossible,
        "timestamp_anomaly_count": sum(_timestamp_anomaly(row) for row in rows),
    }


def _verify_dataset_files(manifest_path: Path) -> dict[str, object]:
    sidecar = manifest_path.with_name("manifest.sha256")
    if not manifest_path.is_file() or not sidecar.is_file():
        raise DatasetIntegrityError("dataset manifest or checksum sidecar is missing")
    expected = sidecar.read_text(encoding="ascii").split()[0]
    if _sha256_file(manifest_path) != expected:
        raise DatasetIntegrityError("dataset manifest checksum differs")
    manifest = cast(dict[str, object], json.loads(manifest_path.read_text(encoding="utf-8")))
    if manifest.get("dataset_schema_version") != DATASET_SCHEMA_VERSION:
        raise DatasetIntegrityError("unsupported dataset schema version")
    if canonical_digest(manifest["semantic_contract"]) != manifest["dataset_identity"]:
        raise DatasetIntegrityError("dataset semantic identity differs")
    data_path = manifest_path.parent / cast(str, manifest["file"])
    if not data_path.is_file() or _sha256_file(data_path) != manifest["file_sha256"]:
        raise DatasetIntegrityError("dataset Parquet checksum differs")
    table = pq.ParquetFile(data_path).read()
    if table.num_rows != manifest["row_count"] or table.num_columns != manifest["column_count"]:
        raise DatasetIntegrityError("dataset Parquet shape differs")
    if (
        canonical_digest([(field.name, str(field.type), field.nullable) for field in table.schema])
        != manifest["schema_sha256"]
    ):
        raise DatasetIntegrityError("dataset schema checksum differs")
    if _row_content_sha256(table.to_pylist()) != manifest["content_sha256"]:
        raise DatasetIntegrityError("dataset canonical content checksum differs")
    return manifest


def _disk_preflight(
    output: Path, estimated_bytes: int, minimum_free_bytes: int, override: int | None
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    free = override if override is not None else shutil.disk_usage(output).free
    required = minimum_free_bytes + estimated_bytes * 2
    if free < required:
        raise DatasetDiskSafetyError(
            f"insufficient dataset staging space: free={free}, required={required}"
        )


def _reject_duplicate_candidates(rows: list[dict[str, object]]) -> None:
    identifiers = [str(row["candidate_id"]) for row in rows]
    if len(identifiers) != len(set(identifiers)):
        raise DatasetIntegrityError("duplicate candidate identities were generated")


def _row_content_sha256(rows: list[dict[str, object]]) -> str:
    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda item: str(item["candidate_id"])):
        digest.update(
            json.dumps(row, sort_keys=True, separators=(",", ":"), default=_json_default).encode()
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return utc(value).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"unsupported research value: {type(value).__name__}")


def _atomic_json(path: Path, value: object) -> None:
    _atomic_text(path, json.dumps(value, indent=2, sort_keys=True, default=_json_default) + "\n")


def _atomic_text(path: Path, value: str) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(int((len(ordered) - 1) * fraction), len(ordered) - 1)]


def _manifest_int(manifest: dict[str, object], field_name: str) -> int:
    value = manifest[field_name]
    if not isinstance(value, int):
        raise DatasetIntegrityError(f"dataset manifest {field_name} is not an integer")
    return value


def _timestamp_anomaly(row: dict[str, object]) -> bool:
    watermark = row.get("feature_availability_watermark")
    decision_at = row.get("decision_at")
    return (
        isinstance(watermark, datetime)
        and isinstance(decision_at, datetime)
        and watermark > decision_at
    )


def _duckdb_read_benchmark(path: Path) -> float:
    connection = duckdb.connect(database=":memory:")
    started = time.perf_counter()
    try:
        connection.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()
    finally:
        connection.close()
    return time.perf_counter() - started


def _peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if platform.system() == "Darwin" else value * 1024
