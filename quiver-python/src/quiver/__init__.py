"""Quiver Python Client - Feature serving client for ML inference."""

from .client import Client
from .table import FeatureTable
from .models import FeatureRequest
from .exceptions import (
    QuiverError,
    QuiverConnectionError,
    QuiverValidationError,
    QuiverFeatureViewNotFound,
    QuiverFeatureNotFound,
)
from ._types import EntityId, FeatureName, FeatureViewName, NullStrategy

# Backward compatibility aliases
ValidationError = QuiverValidationError
ConnectionError = QuiverConnectionError

__version__ = "0.1.0"

__all__ = [
    "Client",
    "FeatureTable",
    "FeatureRequest",
    "QuiverError",
    "QuiverConnectionError",
    "QuiverValidationError",
    "QuiverFeatureViewNotFound",
    "QuiverFeatureNotFound",
    "ValidationError",  # Alias for backward compatibility
    "ConnectionError",  # Alias for backward compatibility
    "EntityId",
    "FeatureName",
    "FeatureViewName",
    "NullStrategy",
    "__version__",
]
