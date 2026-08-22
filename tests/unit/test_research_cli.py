from pathlib import Path

from pump_research.cli import build_parser


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
