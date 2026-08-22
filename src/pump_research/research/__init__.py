"""Strict as-of research and reproducible dataset construction."""

from pump_research.research.asof import TokenStateAsOf, get_token_state_as_of
from pump_research.research.contracts import (
    CandidateTimestampPolicy,
    ChronologicalSplitContract,
    FeatureSetContract,
    LabelSetContract,
    TokenHistory,
)
from pump_research.research.dataset import (
    DatasetBuildSpec,
    build_dataset,
    inspect_dataset,
    verify_dataset,
)
from pump_research.research.features import FeatureResult, build_market_features
from pump_research.research.labels import LabelResult, build_outcome_labels
from pump_research.research.sources import (
    DuckDBArchiveResearchSource,
    HotColdResearchSource,
    InMemoryResearchSource,
    PostgresResearchSource,
)

__all__ = [
    "CandidateTimestampPolicy",
    "ChronologicalSplitContract",
    "DatasetBuildSpec",
    "DuckDBArchiveResearchSource",
    "FeatureSetContract",
    "FeatureResult",
    "HotColdResearchSource",
    "InMemoryResearchSource",
    "LabelSetContract",
    "LabelResult",
    "PostgresResearchSource",
    "TokenHistory",
    "TokenStateAsOf",
    "build_dataset",
    "build_market_features",
    "build_outcome_labels",
    "get_token_state_as_of",
    "inspect_dataset",
    "verify_dataset",
]
