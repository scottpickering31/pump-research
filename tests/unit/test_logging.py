from __future__ import annotations

from pump_research.logging import BrokenPipeSafeWriter


class ClosedPipe:
    def write(self, value: str) -> int:
        del value
        raise BrokenPipeError(32, "Broken pipe")

    def flush(self) -> None:
        raise BrokenPipeError(32, "Broken pipe")


def test_closed_log_pipeline_cannot_raise_into_collector_work() -> None:
    writer = BrokenPipeSafeWriter(ClosedPipe())  # type: ignore[arg-type]
    assert writer.write("shutdown log") == len("shutdown log")
    writer.flush()
