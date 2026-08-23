import asyncio
from pathlib import Path
from typing import Any

import pytest

from pump_research.archive_storage import S3CompatibleObjectStore
from pump_research.cli import build_parser, run_archive_command
from pump_research.config import Settings


def test_research_cli_exposes_build_as_of_and_hot_cold_cutoff() -> None:
    parser = build_parser()
    arguments = parser.parse_args(
        [
            "research",
            "build",
            "--epoch",
            "2",
            "--from",
            "2026-08-16T00:00:00Z",
            "--to",
            "2026-08-17T00:00:00Z",
            "--output",
            "/tmp/research",
            "--archive-manifest",
            "/tmp/archive/manifest.json",
            "--hot-from",
            "2026-08-16T12:00:00Z",
        ]
    )
    assert arguments.command == "research"
    assert arguments.research_command == "build"
    assert arguments.epoch == 2
    assert arguments.output == Path("/tmp/research")
    assert arguments.hot_from == "2026-08-16T12:00:00Z"


def test_archive_cli_exposes_s3_copy_verify_and_readiness_without_delete() -> None:
    parser = build_parser()
    copy_arguments = parser.parse_args(
        [
            "archive",
            "copy-s3",
            "/tmp/archive/manifest.json",
            "--role",
            "secondary",
            "--independent-copy",
            "--independence-detail",
            "separate provider and device",
        ]
    )
    assert copy_arguments.archive_command == "copy-s3"
    assert copy_arguments.independent_copy is True
    assert copy_arguments.independence_detail == "separate provider and device"
    assert parser.parse_args(
        ["archive", "verify-s3-copy", "/tmp/archive/manifest.json", "--role", "primary"]
    ).archive_command == "verify-s3-copy"
    assert parser.parse_args(["archive", "s3-readiness"]).archive_command == "s3-readiness"
    with pytest.raises(SystemExit):
        parser.parse_args(["archive", "delete-s3", "anything"])


def test_s3_readiness_cli_reports_credential_free_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class RejectedClient:
        def head_object(self, **kwargs: Any) -> dict[str, Any]:
            del kwargs
            raise RuntimeError("rejected leaked-cli-credential")

    settings = Settings(
        database_url="postgresql+asyncpg://unused:unused@localhost/unused"
    )
    monkeypatch.setattr("pump_research.cli.get_settings", lambda: settings)
    monkeypatch.setattr("pump_research.cli.configure_logging", lambda configured: None)
    monkeypatch.setattr(
        "pump_research.cli.create_s3_compatible_object_store",
        lambda configured: S3CompatibleObjectStore(
            client=RejectedClient(),  # type: ignore[arg-type]
            bucket="research",
        ),
    )

    assert asyncio.run(run_archive_command(command="s3-readiness")) == 1
    output = capsys.readouterr().out
    assert '"status": "FAIL"' in output
    assert '"delete_attempted": false' in output
    assert "leaked-cli-credential" not in output
