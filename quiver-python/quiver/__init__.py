"""Quiver Python Client - Feature serving client for ML inference."""

from .client import Client
from .table import FeatureTable
from .models import (
    FeatureRequest,
    RequestContext,
    OutputOptions,
    FeatureViewInfo,
    Entity,
)
from .exceptions import (
    QuiverError,
    QuiverConnectionError,
    QuiverValidationError,
    QuiverFeatureViewNotFound,
    QuiverFeatureNotFound,
    QuiverServerError,
    QuiverTimeoutError,
)
from ._types import EntityId, FeatureName, FeatureViewName, NullStrategy

__version__ = "0.1.0"

__all__ = [
    "Client",
    "FeatureTable",
    "FeatureRequest",
    "RequestContext",
    "OutputOptions",
    "FeatureViewInfo",
    "Entity",
    "QuiverError",
    "QuiverConnectionError",
    "QuiverValidationError",
    "QuiverFeatureViewNotFound",
    "QuiverFeatureNotFound",
    "QuiverServerError",
    "QuiverTimeoutError",
    "EntityId",
    "FeatureName",
    "FeatureViewName",
    "NullStrategy",
    "__version__",
]
