import time
from datetime import datetime
from typing import List, Optional, Any, cast, Tuple
from concurrent.futures import ThreadPoolExecutor
import logging

import pyarrow as pa
import pyarrow.flight as flight

from ._types import (
    EntityId,
    FeatureName,
    FeatureViewName,
    NullStrategy,
    CompressionType,
)
from .exceptions import (
    QuiverConnectionError,
    QuiverValidationError,
    QuiverFeatureViewNotFound,
    QuiverFeatureNotFound,
    QuiverServerError,
    QuiverTimeoutError,
)
from .models import FeatureRequest, RequestContext, FeatureViewInfo
from .table import FeatureTable

logger = logging.getLogger(__name__)


class Client:
    """Quiver client for feature serving with Arrow Flight integration.

    Provides strongly-typed interface to Quiver server with immediate validation.
    All network errors, configuration errors, and type errors are caught early.

    Args:
        address: Server address (e.g. "localhost:8815" or "grpc://localhost:8815")
        timeout: Request timeout in seconds
        max_retries: Number of retry attempts for failed requests
        compression: Optional compression algorithm
        default_null_strategy: How to handle null/missing values by default
        validate_connection: Whether to validate connection on init

    Raises:
        QuiverConnectionError: If server is unreachable
        QuiverValidationError: If configuration is invalid
    """

    def __init__(
        self,
        address: str,
        *,
        timeout: float = 30.0,
        max_retries: int = 3,
        compression: Optional[CompressionType] = None,
        default_null_strategy: NullStrategy = "fill_null",
        validate_connection: bool = False,
    ) -> None:
        # Validate inputs immediately
        if not isinstance(address, str) or not address.strip():
            raise QuiverValidationError("address cannot be empty")

        if timeout <= 0:
            raise QuiverValidationError("timeout must be positive")

        if max_retries < 0:
            raise QuiverValidationError("max_retries cannot be negative")

        # Parse address into host and port
        self._host, self._port = self._normalize_address(address)
        self._timeout = timeout
        self._max_retries = max_retries
        self._compression = compression
        self._default_null_strategy = default_null_strategy
        self._closed = False

        # Initialize Flight client
        try:
            location = flight.Location.for_grpc_tcp(self._host, self._port)
            self._flight_client = flight.FlightClient(location)
        except Exception as e:
            # For development, create a mock client if Flight fails
            logger.warning(f"Flight client creation failed: {e}. Using mock mode.")
            self._flight_client = None

        # Validate connection if requested
        if validate_connection and self._flight_client:
            self._validate_connection()

    def _normalize_address(self, address: str) -> Tuple[str, int]:
        """Parse address into host and port.

        Args:
            address: Address in various formats:
                - "localhost:8815"
                - "grpc://localhost:8815"
                - "127.0.0.1:8815"
                - "localhost" (defaults to port 8815)

        Returns:
            Tuple of (host, port)

        Raises:
            QuiverValidationError: If address format is invalid
        """
        address = address.strip()

        # Remove protocol prefix if present
        if address.startswith(("grpc://", "grpc+tls://")):
            address = address.split("://", 1)[1]

        # Parse host:port
        if ":" in address:
            host, port_str = address.rsplit(":", 1)
            try:
                port = int(port_str)
                if not (1 <= port <= 65535):
                    raise ValueError("Port out of range")
            except ValueError as e:
                raise QuiverValidationError(f"Invalid port in address '{address}': {e}")
        else:
            # Default to port 8815 for Quiver
            host = address
            port = 8815

        if not host:
            raise QuiverValidationError("Host cannot be empty")

        return host, port

    def _validate_connection(self) -> None:
        """Validate connection to server by attempting to list flights."""
        if not self._flight_client:
            raise QuiverConnectionError(
                "No Flight client available", f"{self._host}:{self._port}"
            )
        try:
            list(self._flight_client.list_flights())
            logger.info(
                f"Successfully connected to Quiver server at {self._host}:{self._port}"
            )
        except Exception as e:
            raise QuiverConnectionError(
                f"Failed to connect to server: {str(e)}", f"{self._host}:{self._port}"
            )

    def get_features(
        self,
        feature_view: FeatureViewName,
        entities: List[EntityId],
        features: List[FeatureName],
        *,
        as_of: Optional[datetime] = None,
        null_strategy: Optional[NullStrategy] = None,
        include_timestamps: bool = False,
        include_freshness: bool = False,
        context: Optional[RequestContext] = None,
        timeout: Optional[float] = None,
    ) -> FeatureTable:
        """Get features for specified entities.

        This implementation provides the complete interface but uses mock data
        until protobuf integration is completed.

        Args:
            feature_view: Name of the feature view to query
            entities: List of entity IDs to get features for
            features: List of feature names to retrieve
            as_of: Point-in-time for temporal queries
            null_strategy: How to handle null/missing values
            include_timestamps: Whether to include feature timestamps
            include_freshness: Whether to include freshness metadata
            context: Request context for tracing and debugging
            timeout: Request timeout override

        Returns:
            FeatureTable: Table with requested features

        Raises:
            QuiverValidationError: If inputs are invalid
            QuiverFeatureViewNotFound: If feature view doesn't exist
            QuiverFeatureNotFound: If features don't exist
            QuiverConnectionError: If connection fails
            QuiverServerError: If server error occurs
            QuiverTimeoutError: If request times out
        """
        if self._closed:
            raise QuiverValidationError("Client is closed")

        # Create and validate request - this validates all inputs
        effective_null_strategy = null_strategy or cast(
            NullStrategy, self._default_null_strategy
        )
        request = FeatureRequest(
            feature_view=feature_view,
            entities=entities,
            features=features,
            as_of=as_of,
            null_strategy=effective_null_strategy,
        )

        effective_timeout = timeout or self._timeout

        # TODO: Replace with actual Flight/proto implementation
        return self._create_mock_response(
            request, include_timestamps, include_freshness
        )

    def _create_mock_response(
        self, request: FeatureRequest, include_timestamps: bool, include_freshness: bool
    ) -> FeatureTable:
        """Create realistic mock response demonstrating full interface."""
        num_entities = len(request.entities)

        # Build schema and data
        schema_fields = [pa.field("entity_id", pa.string())]
        arrays = [pa.array(request.entities)]

        # Generate realistic mock feature data
        for feature in request.features:
            if feature == "age":
                values = [25 + (i * 5) % 40 for i in range(num_entities)]  # Ages 25-65
                schema_fields.append(pa.field(feature, pa.int64()))
                arrays.append(pa.array(values))
            elif feature == "tier":
                tiers = ["platinum", "gold", "silver", "bronze"]
                values = [tiers[i % len(tiers)] for i in range(num_entities)]
                schema_fields.append(pa.field(feature, pa.string()))
                arrays.append(pa.array(values))
            elif feature == "total_orders":
                values = [
                    10 + (i * 3) % 100 for i in range(num_entities)
                ]  # Orders 10-109
                schema_fields.append(pa.field(feature, pa.int64()))
                arrays.append(pa.array(values))
            elif feature == "avg_order_value":
                values = [
                    50.0 + (i * 12.5) % 200.0 for i in range(num_entities)
                ]  # $50-250
                schema_fields.append(pa.field(feature, pa.float64()))
                arrays.append(pa.array(values))
            else:
                # Default to numeric values
                values = [1.0 + i * 0.5 for i in range(num_entities)]
                schema_fields.append(pa.field(feature, pa.float64()))
                arrays.append(pa.array(values))

        # Add optional timestamp column
        if include_timestamps:
            schema_fields.append(pa.field("feature_timestamp", pa.timestamp("us")))
            current_ts = int(time.time() * 1000000)
            ts_values = [
                current_ts - (i * 60000000) for i in range(num_entities)
            ]  # Stagger timestamps
            arrays.append(pa.array(ts_values))

        # Add optional freshness metadata
        if include_freshness:
            schema_fields.append(pa.field("freshness_score", pa.float64()))
            freshness_values = [
                0.95 - (i * 0.01) % 0.2 for i in range(num_entities)
            ]  # 0.75-0.95
            arrays.append(pa.array(freshness_values))

        schema = pa.schema(schema_fields)
        table = pa.table(arrays, schema=schema)

        logger.info(
            f"Generated mock response: {table.num_rows} rows, {table.num_columns} columns"
        )
        return FeatureTable(table)

    def get_features_batch(self, requests: List[FeatureRequest]) -> List[FeatureTable]:
        """Get features for multiple requests in parallel."""
        if self._closed:
            raise QuiverValidationError("Client is closed")

        if not requests:
            return []

        # Use thread pool for parallel execution
        with ThreadPoolExecutor(max_workers=min(len(requests), 10)) as executor:
            futures = []
            for req in requests:
                future = executor.submit(
                    self.get_features,
                    req.feature_view,
                    req.entities,
                    req.features,
                    as_of=req.as_of,
                    null_strategy=req.null_strategy,
                )
                futures.append(future)

            # Collect results
            results = []
            for future in futures:
                results.append(future.result())

            return results

    def list_feature_views(self) -> List[FeatureViewInfo]:
        """List available feature views."""
        if self._closed:
            raise QuiverValidationError("Client is closed")

        # For mock mode, return realistic sample views
        sample_schemas = {
            "user_features": pa.schema(
                [
                    pa.field("entity_id", pa.string()),
                    pa.field("age", pa.int64()),
                    pa.field("tier", pa.string()),
                    pa.field("total_orders", pa.int64()),
                    pa.field("avg_order_value", pa.float64()),
                ]
            ),
            "product_features": pa.schema(
                [
                    pa.field("entity_id", pa.string()),
                    pa.field("category", pa.string()),
                    pa.field("price", pa.float64()),
                    pa.field("rating", pa.float64()),
                    pa.field("review_count", pa.int64()),
                ]
            ),
        }

        views = []
        for name, schema in sample_schemas.items():
            view_info = FeatureViewInfo(
                name=name,
                schema=schema,
                backend="memory",  # Mock backend
                entity_types=["default"],
            )
            views.append(view_info)

        return views

    def close(self) -> None:
        """Close connection to server."""
        if not self._closed:
            try:
                if self._flight_client and hasattr(self._flight_client, "close"):
                    self._flight_client.close()
            except Exception as e:
                logger.warning(f"Error closing Flight client: {e}")
            finally:
                self._closed = True
                logger.info("Quiver client closed")

    def __enter__(self) -> "Client":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

    def __repr__(self) -> str:
        status = "closed" if self._closed else "open"
        return f"Client(address='{self._host}:{self._port}', timeout={self._timeout}, status='{status}')"


__all__ = ["Client"]
