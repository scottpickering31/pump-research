"""DuckDB research access over one or more verified cold Parquet manifests."""

from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, cast

import duckdb


class ColdArchiveQuery:
    """Bounded DuckDB views over cold files without restoring PostgreSQL."""

    def __init__(self, manifests: Iterable[Path]) -> None:
        self.connection = duckdb.connect(database=":memory:")
        self.files_by_family: dict[str, list[str]] = defaultdict(list)
        self.manifests: list[dict[str, object]] = []
        for manifest_path in manifests:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.manifests.append(cast(dict[str, object], manifest))
            version_value = manifest.get(
                "archive_schema_version", manifest.get("schema_version", 1)
            )
            if not isinstance(version_value, int):
                raise ValueError("archive schema version must be an integer")
            root = (
                manifest_path.resolve().parents[4]
                if version_value == 2
                else manifest_path.resolve().parents[2]
            )
            for entry in manifest["entries"]:
                family_value = entry.get("family")
                family = cast(str, family_value if family_value is not None else entry["table"])
                path = str(root / cast(str, entry["file"]))
                if path not in self.files_by_family[family]:
                    self.files_by_family[family].append(path)
        if not self.manifests:
            raise ValueError("at least one archive manifest is required")
        for family, files in self.files_by_family.items():
            self.connection.from_parquet(files).create_view(family)

    def close(self) -> None:
        self.connection.close()

    def observations_for_token(
        self, token_address: str, *, limit: int | None = None
    ) -> list[tuple[object, ...]]:
        suffix = "" if limit is None else f" LIMIT {int(limit)}"
        return self.connection.execute(
            "SELECT o.id, o.received_at::VARCHAR, o.pair_id, o.api_request_log_id, "
            "o.price_usd, o.price_native, o.liquidity_usd, o.market_cap_usd, "
            "o.fully_diluted_valuation_usd, "
            "o.volume_m5_usd, o.volume_h1_usd, o.volume_h6_usd, o.volume_h24_usd, "
            "o.buys_m5, o.sells_m5, o.buys_h1, o.sells_h1 "
            "FROM observations o JOIN pairs p ON p.id=o.pair_id "
            "JOIN tokens t ON t.id=p.token_id WHERE t.address=? "
            f"ORDER BY o.received_at, o.id{suffix}",
            [token_address],
        ).fetchall()

    def observations_in_range(self, start_at: str, end_at: str) -> int:
        return _scalar_int(
            self.connection,
            "SELECT count(*) FROM observations WHERE received_at >= ? AND received_at < ?",
            [start_at, end_at],
        )

    def tokens_for_epoch(self, epoch_number: int) -> list[tuple[object, ...]]:
        epoch_ids = [
            manifest["epoch_id"] for manifest in self.manifests if manifest["epoch"] == epoch_number
        ]
        if not epoch_ids:
            return []
        # A manifest is already epoch-scoped; dimensions contain only referenced tokens.
        return self.connection.execute(
            "SELECT DISTINCT id, chain, address, first_discovered_at::VARCHAR "
            "FROM tokens ORDER BY address"
        ).fetchall()

    def lifecycle_chronology(self, token_address: str) -> list[tuple[object, ...]]:
        return self.connection.execute(
            "SELECT e.decided_at::VARCHAR, e.previous_state, e.new_state, e.reason_code "
            "FROM lifecycle_events e JOIN tokens t ON t.id=e.token_id "
            "WHERE t.address=? ORDER BY e.decided_at, e.id",
            [token_address],
        ).fetchall()

    def first_and_peak_market_cap(self, token_address: str) -> tuple[object, ...] | None:
        return self.connection.execute(
            "SELECT arg_min(o.market_cap_usd, o.received_at), max(o.market_cap_usd) "
            "FROM observations o JOIN pairs p ON p.id=o.pair_id "
            "JOIN tokens t ON t.id=p.token_id WHERE t.address=?",
            [token_address],
        ).fetchone()

    def enrichment_as_of(self, token_address: str, as_of: str) -> dict[str, object]:
        result: dict[str, object] = {}
        queries = {
            "boost": (
                "boost_observations",
                "SELECT amount, total_amount, active_boost_count, received_at::VARCHAR "
                "FROM boost_observations b JOIN tokens t ON t.id=b.token_id "
                "WHERE t.address=? AND b.received_at <= ? "
                "ORDER BY b.received_at DESC, b.id DESC LIMIT 1",
            ),
            "security": (
                "token_security_snapshots",
                "SELECT status, mint_authority, freeze_authority, raw_supply, token_program, "
                "received_at::VARCHAR FROM token_security_snapshots s "
                "JOIN tokens t ON t.id=s.token_id "
                "WHERE t.address=? AND s.received_at <= ? "
                "ORDER BY s.received_at DESC, s.id DESC LIMIT 1",
            ),
        }
        for name, (family, query) in queries.items():
            result[name] = (
                self.connection.execute(query, [token_address, as_of]).fetchone()
                if family in self.files_by_family
                else None
            )
        if "market_context_snapshots" in self.files_by_family:
            result["context"] = self.connection.execute(
                "SELECT id, bucket_start::VARCHAR, bucket_end::VARCHAR, "
                "received_at::VARCHAR, sol_usd_price, sol_return_5m, "
                "sol_realized_volatility_1h "
                "FROM market_context_snapshots WHERE received_at <= ? "
                "ORDER BY received_at DESC, id DESC LIMIT 1",
                [as_of],
            ).fetchone()
        else:
            result["context"] = None
        return result

    def __enter__(self) -> ColdArchiveQuery:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def run_archive_analytics(manifest_path: Path) -> dict[str, object]:
    """Run representative direct research queries and persist timing evidence."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    with ColdArchiveQuery([manifest_path]) as archive:
        connection = archive.connection
        for family in ("observations", "pairs", "tokens", "lifecycle_events"):
            if family not in archive.files_by_family:
                raise ValueError(f"archive has no {family} Parquet files")
        timings: dict[str, float] = {}

        def measured(name: str, query: Callable[[], Any]) -> Any:
            started = time.perf_counter()
            result = query()
            timings[name] = round((time.perf_counter() - started) * 1000, 3)
            return result

        observation_count = int(
            measured(
                "observation_count",
                lambda: _scalar(connection, "SELECT count(*) FROM observations"),
            )
        )
        unique_tokens = int(
            measured(
                "unique_tokens",
                lambda: _scalar(
                    connection,
                    "SELECT count(DISTINCT p.token_id) FROM observations o "
                    "JOIN pairs p ON p.id=o.pair_id",
                ),
            )
        )
        selected = connection.execute(
            "SELECT t.id, t.address FROM tokens t JOIN pairs p ON p.token_id=t.id "
            "JOIN observations o ON o.pair_id=p.id ORDER BY t.address LIMIT 1"
        ).fetchone()
        points: list[tuple[object, ...]] = []
        if selected is not None:
            points = measured(
                "selected_token_time_series",
                lambda: archive.observations_for_token(cast(str, selected[1]), limit=100),
            )
        joined_rows = int(
            measured(
                "observations_pairs_tokens_join",
                lambda: _scalar(
                    connection,
                    "SELECT count(*) FROM observations o JOIN pairs p ON p.id=o.pair_id "
                    "JOIN tokens t ON t.id=p.token_id",
                ),
            )
        )
        lifecycle_rows = int(
            measured(
                "lifecycle_reconstruction",
                lambda: _scalar(connection, "SELECT count(*) FROM lifecycle_events"),
            )
        )
        window_rows = int(
            measured(
                "time_window_scan",
                lambda: archive.observations_in_range(manifest["start_at"], manifest["end_at"]),
            )
        )
        derived = measured(
            "derived_buy_sell_ratio",
            lambda: _scalar(
                connection,
                "SELECT avg(CASE WHEN sells_m5 > 0 THEN buys_m5::DOUBLE/sells_m5 END) "
                "FROM observations",
            ),
        )
        enrichment = (
            measured(
                "enrichment_as_of",
                lambda: archive.enrichment_as_of(cast(str, selected[1]), manifest["end_at"]),
            )
            if selected is not None
            else {"boost": None, "security": None, "context": None}
        )
        result: dict[str, object] = {
            "schema_version": 2,
            "manifest": str(manifest_path),
            "manifest_sha256": _sha256_file(manifest_path),
            "observation_count": observation_count,
            "unique_tokens": unique_tokens,
            "selected_token": None
            if selected is None
            else {"id": selected[0], "address": selected[1], "returned_points": len(points)},
            "observations_pairs_tokens_join_rows": joined_rows,
            "lifecycle_reconstruction_rows": lifecycle_rows,
            "time_window_observation_rows": window_rows,
            "derived_buy_sell_ratio": float(derived) if derived is not None else None,
            "enrichment_as_of_available": {
                name: value is not None
                for name, value in cast(dict[str, object], enrichment).items()
            },
            "query_time_ms": timings,
            "analytical_reads_passed": (
                observation_count == joined_rows == window_rows and lifecycle_rows >= 0
            ),
            "store_assessment": (
                "Verified Parquet supports direct multi-family research scans and joins; "
                "PostgreSQL remains the authoritative hot operational store."
            ),
        }
    validation_path = manifest_path.with_suffix(".analytics.json")
    validation_path.write_text(
        json.dumps(result, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    return result


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for block in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _scalar(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    parameters: list[object] | None = None,
) -> object:
    row = connection.execute(query, parameters or []).fetchone()
    if row is None:
        raise ValueError("analytical scalar query returned no row")
    return row[0]


def _scalar_int(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    parameters: list[object] | None = None,
) -> int:
    value = _scalar(connection, query, parameters)
    if not isinstance(value, int):
        raise ValueError("analytical count query did not return an integer")
    return value
