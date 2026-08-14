"""Immutable raw-observation input boundary for lifecycle derivation."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class RawObservationEvidence:
    """The source-fact fields a lifecycle rule is permitted to inspect.

    This value object is deliberately separate from both the SQLAlchemy
    ``Observation`` model and lifecycle projections. It carries no state,
    score, recommendation, or mutable scheduling field.
    """

    observation_id: uuid.UUID
    received_at: datetime
    pair_id: uuid.UUID
    api_request_log_id: uuid.UUID
    volume_m5_usd: Decimal | None
    volume_h1_usd: Decimal | None
    liquidity_usd: Decimal | None
