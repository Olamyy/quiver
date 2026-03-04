"""Custom exceptions for Quiver Python client."""

from typing import List, Optional


class QuiverError(Exception):
    """Base exception for all Quiver client errors."""

    pass


class QuiverConnectionError(QuiverError):
    """Failed to connect to Quiver server."""

    def __init__(self, message: str, address: Optional[str] = None) -> None:
        self.message = message
        self.address = address
        super().__init__(f"Connection failed: {message}")


class QuiverValidationError(QuiverError):
    """Invalid request parameters or client configuration."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(f"Validation error: {message}")


class QuiverFeatureViewNotFound(QuiverError):
    """Requested feature view does not exist."""

    def __init__(self, feature_view: str) -> None:
        self.feature_view = feature_view
        super().__init__(f"Feature view not found: {feature_view}")


class QuiverFeatureNotFound(QuiverError):
    """One or more requested features do not exist."""

    def __init__(
        self,
        missing_features: List[str],
        available_features: Optional[List[str]] = None,
    ) -> None:
        self.missing_features = missing_features
        self.available_features = available_features or []
        super().__init__(f"Features not found: {', '.join(missing_features)}")


class QuiverServerError(QuiverError):
    """Server-side error occurred."""

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(f"Server error: {message}")


class QuiverTimeoutError(QuiverError):
    """Request timed out."""

    def __init__(self, message: str, timeout: Optional[float] = None) -> None:
        self.message = message
        self.timeout = timeout
        super().__init__(f"Timeout error: {message}")


# Create aliases for easier import
FeatureViewNotFound = QuiverFeatureViewNotFound
FeatureNotFound = QuiverFeatureNotFound
ServerError = QuiverServerError
TimeoutError = QuiverTimeoutError

__all__ = [
    "QuiverError",
    "QuiverConnectionError",
    "QuiverValidationError",
    "QuiverFeatureViewNotFound",
    "QuiverFeatureNotFound",
    "QuiverServerError",
    "QuiverTimeoutError",
    # Aliases
    "FeatureViewNotFound",
    "FeatureNotFound",
    "ServerError",
    "TimeoutError",
]
