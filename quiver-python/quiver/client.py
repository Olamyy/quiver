import time
from datetime import datetime
from typing import List, Optional, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
import logging

import pyarrow as pa
import pyarrow.flight as flight
from google.protobuf import timestamp_pb2

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
from .proto import serving_pb2, types_pb2

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
        self._host, self._port = self._normalize_address(address)
        self._timeout = timeout
        self._max_retries = max_retries
        self._compression = compression
        self._default_null_strategy = default_null_strategy
        self._closed = False

        try:
            location = flight.Location.for_grpc_tcp(self._host, self._port)
            self._flight_client = flight.FlightClient(location)
        except Exception:  # noqa
            self._flight_client = None

        if not self._flight_client:
            raise QuiverConnectionError(
                "Failed to create Flight client", f"{self._host}:{self._port}"
            )

        if validate_connection:
            self._validate_connection()

    @staticmethod
    def _normalize_address(address: str) -> Tuple[str, int]:
        """Parse address into host and port."""
        address = address.strip()

        if address.startswith(("grpc://", "grpc+tls://")):
            address = address.split("://", 1)[1]

        if ":" in address:
            host, port_str = address.rsplit(":", 1)
            port = int(port_str)
        else:
            host = address
            port = 8815

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

        effective_null_strategy = null_strategy or self._default_null_strategy

        request = FeatureRequest(
            feature_view=feature_view,
            entities=entities,
            features=features,
            as_of=as_of,
            null_strategy=effective_null_strategy,
        )

        effective_timeout = timeout or self._timeout

        proto_request = self._build_proto_request(
            request, include_timestamps, include_freshness, context
        )
        return self._execute_flight_request(proto_request, effective_timeout)

    @staticmethod
    def _build_proto_request(
        request: FeatureRequest,
        include_timestamps: bool,
        include_freshness: bool,
        context: Optional[RequestContext],
    ) -> serving_pb2.FeatureRequest:  # noqa
        """Build protobuf FeatureRequest from Python request."""
        proto_request = serving_pb2.FeatureRequest()  # noqa
        proto_request.feature_view = request.feature_view
        proto_request.feature_names.extend(request.features)

        for entity_id in request.entities:
            entity_key = types_pb2.EntityKey()  # noqa
            entity_key.entity_type = "default"
            entity_key.entity_id = entity_id
            proto_request.entities.append(entity_key)

        if request.as_of:
            timestamp = timestamp_pb2.Timestamp()  # noqa
            timestamp.FromDatetime(request.as_of)
            proto_request.as_of.CopyFrom(timestamp)

        output_options = serving_pb2.OutputOptions()  # noqa
        output_options.include_timestamps = include_timestamps
        output_options.include_freshness = include_freshness
        output_options.null_strategy = request.null_strategy or "fill_null"
        proto_request.output.CopyFrom(output_options)

        if context:
            context_proto = types_pb2.RequestContext()  # noqa
            context_proto.request_id = context.request_id or ""
            context_proto.caller = context.caller or ""
            context_proto.environment = context.environment or ""
            proto_request.context.CopyFrom(context_proto)

        return proto_request

    def _execute_flight_request(
        self,
        proto_request: serving_pb2.FeatureRequest,  # noqa
        timeout: float,  # noqa
    ) -> FeatureTable:
        """Execute the Flight request with retry logic."""
        if not self._flight_client:
            raise QuiverConnectionError("Flight client not connected")

        last_exception = None
        for attempt in range(self._max_retries + 1):
            try:
                ticket_data = proto_request.SerializeToString()
                ticket = flight.Ticket(ticket_data)

                start_time = time.time()
                flight_reader = self._flight_client.do_get(ticket)

                batches = []
                schema = None

                for batch in flight_reader:
                    if schema is None:
                        schema = batch.data.schema
                    batches.append(batch.data)

                    if time.time() - start_time > timeout:
                        raise QuiverTimeoutError(f"Request timed out after {timeout}s")

                if batches:
                    table = pa.Table.from_batches(batches, schema)  # noqa
                else:
                    table = pa.Table.from_arrays([], schema or pa.schema([]))  # noqa

                logger.debug(
                    f"Retrieved {table.num_rows} rows in {time.time() - start_time:.2f}s"
                )
                return FeatureTable(table)

            except flight.FlightUnavailableError as e:
                last_exception = QuiverConnectionError(
                    f"Server unavailable: {str(e)}", f"{self._host}:{self._port}"
                )
            except flight.FlightTimedOutError as e:
                raise QuiverTimeoutError(f"Request timed out: {str(e)}")
            except flight.FlightError as e:
                error_msg = str(e).lower()
                if "not found" in error_msg or proto_request.feature_view in error_msg:
                    raise QuiverFeatureViewNotFound(proto_request.feature_view)
                elif "feature" in error_msg:
                    raise QuiverFeatureNotFound(list(proto_request.feature_names))
                else:
                    raise QuiverValidationError(f"Invalid request: {str(e)}")

            except Exception as e:
                last_exception = QuiverServerError(f"Unexpected error: {str(e)}")

            if attempt < self._max_retries:
                wait_time = min(2.0**attempt, 10.0)
                time.sleep(wait_time)
                logger.warning(
                    f"Request failed, retrying in {wait_time}s (attempt {attempt + 1}/{self._max_retries + 1})"
                )

        raise last_exception or QuiverServerError("Request failed after all retries")

    def get_features_batch(self, requests: List[FeatureRequest]) -> List[FeatureTable]:
        """Get features for multiple requests in parallel."""
        if self._closed:
            raise QuiverValidationError("Client is closed")

        if not requests:
            return []

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
                    view_name = info.descriptor.path[0]
                    view_info = FeatureViewInfo(
                        name=view_name,
                        schema=info.schema,
                        backend="unknown",
                        entity_types=["default"],
                    )
                    views.append(view_info)

        except Exception as e:
            raise QuiverServerError(f"Failed to list feature views: {str(e)}")

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
