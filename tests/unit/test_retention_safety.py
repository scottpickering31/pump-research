from __future__ import annotations

import pytest

from pump_research.retention import (
    RetentionEvidence,
    RetentionSafetyError,
    assert_retention_deletion_authorized,
)


def test_retention_gate_fails_closed_for_every_missing_precondition() -> None:
    with pytest.raises(RetentionSafetyError, match="independent second copy"):
        assert_retention_deletion_authorized(
            RetentionEvidence(
                verified_archive=True,
                independent_second_copy=False,
                analytical_reads_passed=True,
                explicit_human_approval=True,
            )
        )


def test_retention_gate_requires_explicit_human_approval_even_with_verified_data() -> None:
    with pytest.raises(RetentionSafetyError, match="explicit human approval"):
        assert_retention_deletion_authorized(RetentionEvidence(True, True, True, False))
