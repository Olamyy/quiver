from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import pyarrow as pa

from ._types import EntityId, FeatureName, FeatureViewName, NullStrategy
from .exceptions import QuiverValidationError


@dataclass(frozen=True)
class FeatureRequest:
    """Request for features from a specific feature view.

    Args:
        feature_view: Name of the feature view to query
        entities: List of entity IDs to get features for
        features: List of feature names to retrieve
        as_of: Point-in-time for temporal queries
        null_strategy: How to handle null/missing values

    Raises:
        QuiverValidationError: If inputs are invalid
    """

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

        if len(set(self.entities)) != len(self.entities):
            raise QuiverValidationError("entities must be unique")

        if len(set(self.features)) != len(self.features):
            raise QuiverValidationError("features must be unique")


@dataclass
class RequestContext:
    """Context information for feature requests.

    Attributes:
        request_id: Optional trace ID for this request. If provided, the server will
            use this instead of generating a UUID. Useful for correlating metrics
            and logs across multiple services. If not provided, server generates UUID.
        caller: Optional identifier for the service/system making the request
        environment: Optional environment identifier (e.g., "production", "staging")
    """

    request_id: Optional[str] = None
    caller: Optional[str] = None
    environment: Optional[str] = None


@dataclass
class OutputOptions:
    """Options for controlling feature request output format."""

    include_timestamps: bool = False
    include_freshness: bool = False
    null_strategy: Optional[NullStrategy] = None


@dataclass
class FeatureViewInfo:
    """Information about a feature view."""

    name: str
    schema: pa.Schema  # noqa
    backend: str
    entity_types: List[str]


@dataclass
class Entity:
    """Entity specification with type and ID."""

    entity_type: str
    entity_id: str


__all__ = [
    "FeatureRequest",
    "RequestContext",
    "OutputOptions",
    "FeatureViewInfo",
    "Entity",
]
