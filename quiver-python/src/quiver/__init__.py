"""Quiver Python Client - Feature serving client for ML inference."""

from .client import Client
from .table import FeatureTable
from .models import FeatureRequest
from .exceptions import (
    QuiverError,
    ConnectionError,
    ValidationError, 
    FeatureViewNotFound,
    FeatureNotFound,
)
from ._types import EntityId, FeatureName, FeatureViewName, NullStrategy

__version__ = "0.1.0"

__all__ = [
    "Client",
    "FeatureTable",
    "FeatureRequest",
    "QuiverError",
    "ConnectionError",
    "ValidationError",
    "FeatureViewNotFound", 
    "FeatureNotFound",
    "EntityId",
    "FeatureName",
    "FeatureViewName", 
    "NullStrategy",
    "__version__",
]
