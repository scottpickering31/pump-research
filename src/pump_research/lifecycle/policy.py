"""Versioned, evidence-only lifecycle transition policy."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from pump_research.config import Settings
from pump_research.lifecycle.evidence import RawObservationEvidence
from pump_research.scheduling.policy import LifecycleState


class LifecycleTransitionRule(StrEnum):
    """The supported forward lifecycle edges for market-observation scheduling."""

    NEW_TO_ACTIVE = "new_to_active"
    NEW_TO_WATCH = "new_to_watch"
    ACTIVE_TO_FADING = "active_to_fading"
    WATCH_TO_FADING = "watch_to_fading"
    FADING_TO_DORMANT = "fading_to_dormant"
    DORMANT_TO_RESURRECTED = "dormant_to_resurrected"


@dataclass(frozen=True, slots=True)
class TransitionEvaluation:
    """One satisfied rule and the exact normalized evidence it used."""

    rule: LifecycleTransitionRule
    previous_state: LifecycleState
    new_state: LifecycleState
    input_values: dict[str, str]
    thresholds: dict[str, str]


@dataclass(frozen=True, slots=True)
class LifecyclePolicy:
    """All lifecycle thresholds, supplied only through validated configuration."""

    new_to_active_min_volume_m5_usd: Decimal
    new_to_watch_min_liquidity_usd: Decimal
    active_to_fading_max_volume_m5_usd: Decimal
    watch_to_fading_max_volume_m5_usd: Decimal
    fading_to_dormant_max_volume_h1_usd: Decimal
    fading_to_dormant_max_liquidity_usd: Decimal
    dormant_to_resurrected_min_volume_m5_usd: Decimal
    dormant_to_resurrected_min_liquidity_usd: Decimal

    @classmethod
    def from_settings(cls, settings: Settings) -> LifecyclePolicy:
        """Create a policy from the complete validated runtime configuration."""
        return cls(
            new_to_active_min_volume_m5_usd=(settings.lifecycle_new_to_active_min_volume_m5_usd),
            new_to_watch_min_liquidity_usd=(settings.lifecycle_new_to_watch_min_liquidity_usd),
            active_to_fading_max_volume_m5_usd=(
                settings.lifecycle_active_to_fading_max_volume_m5_usd
            ),
            watch_to_fading_max_volume_m5_usd=(
                settings.lifecycle_watch_to_fading_max_volume_m5_usd
            ),
            fading_to_dormant_max_volume_h1_usd=(
                settings.lifecycle_fading_to_dormant_max_volume_h1_usd
            ),
            fading_to_dormant_max_liquidity_usd=(
                settings.lifecycle_fading_to_dormant_max_liquidity_usd
            ),
            dormant_to_resurrected_min_volume_m5_usd=(
                settings.lifecycle_dormant_to_resurrected_min_volume_m5_usd
            ),
            dormant_to_resurrected_min_liquidity_usd=(
                settings.lifecycle_dormant_to_resurrected_min_liquidity_usd
            ),
        )

    def evaluate(
        self,
        *,
        state: LifecycleState,
        observation: RawObservationEvidence,
    ) -> TransitionEvaluation | None:
        """Evaluate only the permitted outgoing transition from ``state``.

        Missing source values intentionally satisfy no rule.  This keeps an
        incomplete provider record from becoming invented evidence of inactivity.
        """
        volume_m5 = observation.volume_m5_usd
        volume_h1 = observation.volume_h1_usd
        liquidity = observation.liquidity_usd

        if state is LifecycleState.NEW and volume_m5 is not None:
            if volume_m5 >= self.new_to_active_min_volume_m5_usd:
                return TransitionEvaluation(
                    rule=LifecycleTransitionRule.NEW_TO_ACTIVE,
                    previous_state=state,
                    new_state=LifecycleState.ACTIVE,
                    input_values={"volume_m5_usd": _decimal_text(volume_m5)},
                    thresholds={
                        "min_volume_m5_usd": _decimal_text(self.new_to_active_min_volume_m5_usd)
                    },
                )
            if liquidity is not None and liquidity >= self.new_to_watch_min_liquidity_usd:
                return TransitionEvaluation(
                    rule=LifecycleTransitionRule.NEW_TO_WATCH,
                    previous_state=state,
                    new_state=LifecycleState.WATCH,
                    input_values={
                        "volume_m5_usd": _decimal_text(volume_m5),
                        "liquidity_usd": _decimal_text(liquidity),
                    },
                    thresholds={
                        "max_volume_m5_usd_exclusive": _decimal_text(
                            self.new_to_active_min_volume_m5_usd
                        ),
                        "min_liquidity_usd": _decimal_text(self.new_to_watch_min_liquidity_usd),
                    },
                )

        if (
            state is LifecycleState.ACTIVE
            and volume_m5 is not None
            and volume_m5 <= self.active_to_fading_max_volume_m5_usd
        ):
            return TransitionEvaluation(
                rule=LifecycleTransitionRule.ACTIVE_TO_FADING,
                previous_state=state,
                new_state=LifecycleState.FADING,
                input_values={"volume_m5_usd": _decimal_text(volume_m5)},
                thresholds={
                    "max_volume_m5_usd": _decimal_text(self.active_to_fading_max_volume_m5_usd)
                },
            )

        if (
            state is LifecycleState.WATCH
            and volume_m5 is not None
            and volume_m5 <= self.watch_to_fading_max_volume_m5_usd
        ):
            return TransitionEvaluation(
                rule=LifecycleTransitionRule.WATCH_TO_FADING,
                previous_state=state,
                new_state=LifecycleState.FADING,
                input_values={"volume_m5_usd": _decimal_text(volume_m5)},
                thresholds={
                    "max_volume_m5_usd": _decimal_text(self.watch_to_fading_max_volume_m5_usd)
                },
            )

        if (
            state is LifecycleState.FADING
            and volume_h1 is not None
            and liquidity is not None
            and volume_h1 <= self.fading_to_dormant_max_volume_h1_usd
            and liquidity <= self.fading_to_dormant_max_liquidity_usd
        ):
            return TransitionEvaluation(
                rule=LifecycleTransitionRule.FADING_TO_DORMANT,
                previous_state=state,
                new_state=LifecycleState.DORMANT,
                input_values={
                    "volume_h1_usd": _decimal_text(volume_h1),
                    "liquidity_usd": _decimal_text(liquidity),
                },
                thresholds={
                    "max_volume_h1_usd": _decimal_text(self.fading_to_dormant_max_volume_h1_usd),
                    "max_liquidity_usd": _decimal_text(self.fading_to_dormant_max_liquidity_usd),
                },
            )

        if (
            state is LifecycleState.DORMANT
            and volume_m5 is not None
            and liquidity is not None
            and volume_m5 >= self.dormant_to_resurrected_min_volume_m5_usd
            and liquidity >= self.dormant_to_resurrected_min_liquidity_usd
        ):
            return TransitionEvaluation(
                rule=LifecycleTransitionRule.DORMANT_TO_RESURRECTED,
                previous_state=state,
                new_state=LifecycleState.RESURRECTED,
                input_values={
                    "volume_m5_usd": _decimal_text(volume_m5),
                    "liquidity_usd": _decimal_text(liquidity),
                },
                thresholds={
                    "min_volume_m5_usd": _decimal_text(
                        self.dormant_to_resurrected_min_volume_m5_usd
                    ),
                    "min_liquidity_usd": _decimal_text(
                        self.dormant_to_resurrected_min_liquidity_usd
                    ),
                },
            )
        return None

    @property
    def snapshot(self) -> dict[str, object]:
        """Return the complete versioned policy needed to reconstruct a decision."""
        return {
            "component": "lifecycle_classifier",
            "schema_version": 1,
            "evaluation_order": [
                LifecycleTransitionRule.NEW_TO_ACTIVE.value,
                LifecycleTransitionRule.NEW_TO_WATCH.value,
                LifecycleTransitionRule.ACTIVE_TO_FADING.value,
                LifecycleTransitionRule.WATCH_TO_FADING.value,
                LifecycleTransitionRule.FADING_TO_DORMANT.value,
                LifecycleTransitionRule.DORMANT_TO_RESURRECTED.value,
            ],
            "thresholds": {
                "new_to_active_min_volume_m5_usd": _decimal_text(
                    self.new_to_active_min_volume_m5_usd
                ),
                "new_to_watch_min_liquidity_usd": _decimal_text(
                    self.new_to_watch_min_liquidity_usd
                ),
                "active_to_fading_max_volume_m5_usd": _decimal_text(
                    self.active_to_fading_max_volume_m5_usd
                ),
                "watch_to_fading_max_volume_m5_usd": _decimal_text(
                    self.watch_to_fading_max_volume_m5_usd
                ),
                "fading_to_dormant_max_volume_h1_usd": _decimal_text(
                    self.fading_to_dormant_max_volume_h1_usd
                ),
                "fading_to_dormant_max_liquidity_usd": _decimal_text(
                    self.fading_to_dormant_max_liquidity_usd
                ),
                "dormant_to_resurrected_min_volume_m5_usd": _decimal_text(
                    self.dormant_to_resurrected_min_volume_m5_usd
                ),
                "dormant_to_resurrected_min_liquidity_usd": _decimal_text(
                    self.dormant_to_resurrected_min_liquidity_usd
                ),
            },
        }

    @property
    def sha256(self) -> str:
        """Return a stable digest of the complete policy snapshot."""
        encoded = json.dumps(
            self.snapshot,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        return hashlib.sha256(encoded).hexdigest()


def _decimal_text(value: Decimal) -> str:
    """Encode a normalized decimal without database storage-scale padding."""
    normalized = value.normalize() if value else Decimal(0)
    return format(normalized, "f")
