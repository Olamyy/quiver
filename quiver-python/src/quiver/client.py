"""Core Quiver client implementation."""

from typing import List, Optional

try:
    import pyarrow.flight as flight
except ImportError:
    flight = None

from ._types import EntityId, FeatureName, FeatureViewName, NullStrategy
from .exceptions import QuiverConnectionError, QuiverValidationError
from .models import FeatureRequest
from .table import FeatureTable


class Client:
    """Quiver client for feature serving."""

    def __init__(
        self,
        address: str,
        *,
        timeout: float = 30.0,
        validate_connection: bool = False,  # Default to False for now
    ) -> None:
        # Validate inputs immediately
        if not isinstance(address, str) or not address.strip():
            raise QuiverValidationError("address cannot be empty")

        if timeout <= 0:
            raise QuiverValidationError("timeout must be positive")

        if flight is None:
            raise ImportError("PyArrow Flight is required but not installed")

        # Store configuration
        self._address = (
            address if address.startswith("grpc://") else f"grpc://{address}"
        )
        self._timeout = timeout
        self._closed = False

        # For now, create a placeholder client
        # In full implementation, would initialize actual Flight client
        self._flight_client = None

    def get_features(
        self,
        feature_view: FeatureViewName,
        entities: List[EntityId],
        features: List[FeatureName],
        *,
        null_strategy: Optional[NullStrategy] = None,
    ) -> FeatureTable:
        """Get features for entities."""
        if self._closed:
            raise QuiverValidationError("Client is closed")

        # Validate request
        request_obj = FeatureRequest(
            feature_view=feature_view,
            entities=entities,
            features=features,
            null_strategy=null_strategy,
        )

        # For now, return empty table as placeholder
        # In full implementation, would make actual Flight request
        import pyarrow as pa

        schema = pa.schema(
            [
                pa.field("entity_id", pa.string()),
                *[pa.field(fname, pa.string()) for fname in features],
            ]
        )
        table = pa.Table.from_arrays(
            [pa.array([]) for _ in schema.names], schema=schema
        )
        return FeatureTable(table)

    def close(self) -> None:
        """Close client connection."""
        self._closed = True

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


__all__ = ["Client"]
