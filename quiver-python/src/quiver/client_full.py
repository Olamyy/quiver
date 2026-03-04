"""Core Quiver client implementation with Arrow Flight integration."""

import time
from datetime import datetime
from typing import List, Optional, Any
from concurrent.futures import ThreadPoolExecutor
import logging

import pyarrow as pa
import pyarrow.flight as flight
from google.protobuf.timestamp_pb2 import Timestamp

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
    FeatureViewNotFound,
    FeatureNotFound,
    ServerError,
    TimeoutError,
)
from .models import FeatureRequest, RequestContext, OutputOptions, FeatureViewInfo
from .table import FeatureTable
from .proto.serving_pb2 import (
    FeatureRequest as ProtoFeatureRequest,
    OutputOptions as ProtoOutputOptions,
)
from .proto.types_pb2 import (
    EntityKey,
    RequestContext as ProtoRequestContext,
    FreshnessPolicy,
)

logger = logging.getLogger(__name__)


class Client:
    """Quiver client for feature serving with full Arrow Flight integration.

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
        validate_connection: bool = True,
    ) -> None:
        # Validate inputs immediately
        if not isinstance(address, str) or not address.strip():
            raise QuiverValidationError("address cannot be empty")

        if timeout <= 0:
            raise QuiverValidationError("timeout must be positive")

        if max_retries < 0:
            raise QuiverValidationError("max_retries cannot be negative")

        # Parse and normalize address
        self._address = self._normalize_address(address)
        self._timeout = timeout
        self._max_retries = max_retries
        self._compression = compression
        self._default_null_strategy = default_null_strategy
        self._closed = False

        # Initialize Flight client
        try:
            location = flight.Location.for_grpc_uri(self._address)
            self._flight_client = flight.FlightClient(location)
        except Exception as e:
            raise QuiverConnectionError(
                f"Failed to create Flight client: {str(e)}", self._address
            )

        # Validate connection if requested
        if validate_connection:
            self._validate_connection()

    def _normalize_address(self, address: str) -> str:
        """Normalize address to proper gRPC URI format."""
        address = address.strip()
        if not address.startswith(("grpc://", "grpc+tls://")):
            # Default to grpc:// for local development
            if "localhost" in address or "127.0.0.1" in address:
                return f"grpc://{address}"
            else:
                return f"grpc+tls://{address}"
        return address

    def _validate_connection(self) -> None:
        """Validate connection to server by attempting to list flights."""
        try:
            # Try a simple operation to verify connectivity
            list(self._flight_client.list_flights())
            logger.info(f"Successfully connected to Quiver server at {self._address}")
        except Exception as e:
            raise QuiverConnectionError(
                f"Failed to connect to server: {str(e)}", self._address
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
            FeatureViewNotFound: If feature view doesn't exist
            FeatureNotFound: If features don't exist
            QuiverConnectionError: If connection fails
            ServerError: If server error occurs
            TimeoutError: If request times out
        """
        if self._closed:
            raise QuiverValidationError("Client is closed")

        # Create and validate request
        request = FeatureRequest(
            feature_view=feature_view,
            entities=entities,
            features=features,
            as_of=as_of,
            null_strategy=null_strategy or self._default_null_strategy,
        )

        # Build protobuf request
        proto_request = self._build_proto_request(
            request, include_timestamps, include_freshness, context
        )

        # Execute request with retries
        effective_timeout = timeout or self._timeout
        return self._execute_request(proto_request, effective_timeout)

    def _build_proto_request(
        self,
        request: FeatureRequest,
        include_timestamps: bool,
        include_freshness: bool,
        context: Optional[RequestContext],
    ) -> ProtoFeatureRequest:
        """Build protobuf FeatureRequest from Python request."""
        # Convert entities to EntityKey protos
        entity_keys = []
        for entity_id in request.entities:
            # For now, assume all entities are of type "default"
            # In future versions, could support typed entities
            entity_key = EntityKey()
            entity_key.entity_type = "default"
            entity_key.entity_id = entity_id
            entity_keys.append(entity_key)

        # Convert as_of datetime to protobuf Timestamp
        as_of_proto = None
        if request.as_of:
            as_of_proto = Timestamp()
            as_of_proto.FromDatetime(request.as_of)

        # Build output options
        output_options = ProtoOutputOptions()
        output_options.include_timestamps = include_timestamps
        output_options.include_freshness = include_freshness
        output_options.null_strategy = request.null_strategy or "fill_null"

        # Build request context if provided
        context_proto = None
        if context:
            context_proto = ProtoRequestContext()
            context_proto.request_id = context.request_id or ""
            context_proto.caller = context.caller or ""
            context_proto.environment = context.environment or ""

        # Build main request
        proto_request = ProtoFeatureRequest()
        proto_request.feature_view = request.feature_view
        proto_request.feature_names.extend(request.features)
        proto_request.entities.extend(entity_keys)

        if as_of_proto:
            proto_request.as_of.CopyFrom(as_of_proto)
        if context_proto:
            proto_request.context.CopyFrom(context_proto)

        proto_request.output.CopyFrom(output_options)

        return proto_request

    def _execute_request(
        self, proto_request: ProtoFeatureRequest, timeout: float
    ) -> FeatureTable:
        """Execute the Flight request with retries."""
        last_exception = None

        for attempt in range(self._max_retries + 1):
            try:
                # Encode request as ticket
                ticket_data = proto_request.SerializeToString()
                ticket = flight.Ticket(ticket_data)

                # Execute Flight request
                start_time = time.time()
                flight_reader = self._flight_client.do_get(ticket)

                # Read all batches from the stream
                batches = []
                schema = None

                for batch_reader in flight_reader:
                    if schema is None:
                        schema = batch_reader.schema
                    batches.extend(batch_reader.to_batches())

                    # Check timeout
                    if time.time() - start_time > timeout:
                        raise TimeoutError(f"Request timed out after {timeout}s")

                # Combine batches into table
                if batches:
                    table = pa.Table.from_batches(batches, schema)
                else:
                    # Empty result - create empty table with proper schema
                    table = pa.Table.from_arrays([], schema or pa.schema([]))

                logger.debug(
                    f"Retrieved {table.num_rows} rows in {time.time() - start_time:.2f}s"
                )
                return FeatureTable(table)

            except flight.FlightUnavailableError as e:
                last_exception = QuiverConnectionError(
                    f"Server unavailable: {str(e)}", self._address
                )
            except flight.FlightNotFoundError as e:
                raise FeatureViewNotFound(proto_request.feature_view)
            except flight.FlightInvalidArgumentError as e:
                if "feature" in str(e).lower():
                    raise FeatureNotFound(list(proto_request.feature_names))
                else:
                    raise QuiverValidationError(f"Invalid request: {str(e)}")
            except flight.FlightTimedOutError as e:
                raise TimeoutError(f"Request timed out: {str(e)}")
            except Exception as e:
                last_exception = ServerError(f"Unexpected error: {str(e)}")

            # Wait before retry (exponential backoff)
            if attempt < self._max_retries:
                wait_time = min(2.0**attempt, 10.0)  # Cap at 10 seconds
                time.sleep(wait_time)
                logger.warning(
                    f"Request failed, retrying in {wait_time}s (attempt {attempt + 1}/{self._max_retries + 1})"
                )

        # All retries exhausted
        raise last_exception or ServerError("Request failed after all retries")

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

        try:
            flight_infos = list(self._flight_client.list_flights())
            views = []

            for info in flight_infos:
                if info.descriptor and info.descriptor.path:
                    view_name = info.descriptor.path[
                        0
                    ]  # First path element is view name
                    view_info = FeatureViewInfo(
                        name=view_name,
                        schema=info.schema,
                        backend="unknown",  # Would need additional metadata call
                        entity_types=["default"],  # Would need additional metadata
                    )
                    views.append(view_info)

            return views

        except Exception as e:
            raise ServerError(f"Failed to list feature views: {str(e)}")

    def close(self) -> None:
        """Close connection to server."""
        if not self._closed:
            try:
                if hasattr(self._flight_client, "close"):
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
        return f"Client(address='{self._address}', timeout={self._timeout}, status='{status}')"


__all__ = ["Client"]
