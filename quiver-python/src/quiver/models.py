"""Data classes for Quiver requests and responses."""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from ._types import EntityId, FeatureName, FeatureViewName, NullStrategy
from .exceptions import QuiverValidationError


@dataclass(frozen=True)
class FeatureRequest:
    """Request for features from a specific feature view."""

    feature_view: FeatureViewName
    entities: List[EntityId]
    features: List[FeatureName]
    as_of: Optional[datetime] = None
    null_strategy: Optional[NullStrategy] = None

    def __post_init__(self) -> None:
        """Validate inputs immediately on construction."""
        if not self.feature_view.strip():
            raise QuiverValidationError("feature_view cannot be empty")

        if not self.entities:
            raise QuiverValidationError("entities cannot be empty")

        if not self.features:
            raise QuiverValidationError("features cannot be empty")


# Export all data classes
__all__ = [
    "FeatureRequest",
]
