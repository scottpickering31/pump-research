"""Future retention safety gate; this module performs no deletion."""

from __future__ import annotations

from dataclasses import dataclass


class RetentionSafetyError(RuntimeError):
    """A mandatory human/data-integrity gate is incomplete."""


@dataclass(frozen=True, slots=True)
class RetentionEvidence:
    verified_archive: bool
    independent_second_copy: bool
    analytical_reads_passed: bool
    explicit_human_approval: bool


def assert_retention_deletion_authorized(evidence: RetentionEvidence) -> None:
    """Fail closed unless every future destructive-retention precondition exists."""
    missing = [
        name
        for name, present in (
            ("verified archive", evidence.verified_archive),
            ("independent second copy", evidence.independent_second_copy),
            ("successful analytical reads", evidence.analytical_reads_passed),
            ("explicit human approval", evidence.explicit_human_approval),
        )
        if not present
    ]
    if missing:
        raise RetentionSafetyError(
            "retention deletion is forbidden; missing: " + ", ".join(missing)
        )
