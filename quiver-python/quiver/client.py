import time
from datetime import datetime
from typing import List, Optional, Any, Tuple, Dict
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
from .observability import ObservabilityClient

logger = logging.getLogger(__name__)


class ResponseHeaderCapture(flight.ClientMiddleware):
    """Captures gRPC response headers from Flight calls."""

    def __init__(self):
        super().__init__()
        self.headers: Dict[str, str] = {}

    def received_headers(self, headers: Dict[str, str]) -> None:
        """Store response headers when they arrive from the server."""
        self.headers.update(headers)


class ResponseHeaderCaptureFactory(flight.ClientMiddlewareFactory):
    """Factory for creating ResponseHeaderCapture middleware instances."""

    def __init__(self):
        super().__init__()
        self.last_middleware: Optional[ResponseHeaderCapture] = None

    def start_call(self, info: Any) -> flight.ClientMiddleware:
        """Create a new middleware instance for this call."""
        self.last_middleware = ResponseHeaderCapture()
        return self.last_middleware


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
        observability_host: Optional[str] = None,
        observability_port: int = 8816,
        observability_timeout: float = 5.0,
    ) -> None:
        self._host, self._port = self._normalize_address(address)
        self._timeout = timeout
        self._max_retries = max_retries
        self._compression = compression
        self._default_null_strategy = default_null_strategy
        self._closed = False
        self._last_request_id: Optional[str] = None
        self._last_from_cache: Optional[bool] = None
        self._header_capture_factory = ResponseHeaderCaptureFactory()

        try:
            middleware = [self._header_capture_factory]
            self._flight_client = flight.connect(
                f"grpc://{self._host}:{self._port}",
                middleware=middleware,
            )
        except Exception:  # noqa
            self._flight_client = None

        if not self._flight_client:
            raise QuiverConnectionError(
                "Failed to create Flight client", f"{self._host}:{self._port}"
            )

        obs_host = observability_host or self._host
        try:
            self._obs_client: Optional[ObservabilityClient] = ObservabilityClient(
                host=obs_host,
                port=observability_port,
                timeout=observability_timeout,
            )
        except Exception as e:
            logger.warning(f"Observability service not available: {str(e)}")
            self._obs_client = None

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
            context: Request context for tracing and debugging. Can include a
                custom request_id for correlation with other services.
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

        Example:
            Pass a custom request_id for cross-service tracing:
                from quiver.models import RequestContext
                ctx = RequestContext(request_id="my-trace-id-12345")
                table = client.get_features(
                    "user_profile",
                    ["user:1000"],
                    ["score"],
                    context=ctx
                )
                # Server will use "my-trace-id-12345" instead of generating UUID
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

                if self._header_capture_factory.last_middleware:
                    headers = self._header_capture_factory.last_middleware.headers

                    request_id_value = headers.get("x-quiver-request-id")
                    if request_id_value:
                        if isinstance(request_id_value, bytes):
                            self._last_request_id = request_id_value.decode("utf-8")
                        elif isinstance(request_id_value, str):
                            self._last_request_id = request_id_value
                        elif (
                            isinstance(request_id_value, list)
                            and len(request_id_value) > 0
                        ):
                            first_val = request_id_value[0]
                            if isinstance(first_val, bytes):
                                self._last_request_id = first_val.decode("utf-8")
                            else:
                                self._last_request_id = str(first_val)

                    from_cache_value = headers.get("x-quiver-from-cache")
                    if from_cache_value:
                        cache_str = None
                        if isinstance(from_cache_value, bytes):
                            cache_str = from_cache_value.decode("utf-8")
                        elif isinstance(from_cache_value, str):
                            cache_str = from_cache_value
                        elif (
                            isinstance(from_cache_value, list)
                            and len(from_cache_value) > 0
                        ):
                            first_val = from_cache_value[0]
                            if isinstance(first_val, bytes):
                                cache_str = first_val.decode("utf-8")
                            else:
                                cache_str = str(first_val)

                        if cache_str:
                            self._last_from_cache = cache_str.lower() == "true"

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
                    f"Retrieved {len(table)} rows in {time.time() - start_time:.2f}s"
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

    def get_schema(self, feature_view: FeatureViewName) -> Optional[pa.Schema]:
        """Get schema for a feature view."""
        if self._closed:
            raise QuiverValidationError("Client is closed")

        try:
            descriptor = flight.FlightDescriptor.for_path(feature_view)
            schema_result = self._flight_client.get_schema(descriptor)
            return schema_result.schema
        except flight.FlightUnavailableError:
            raise QuiverFeatureViewNotFound(feature_view)
        except Exception as e:
            raise QuiverServerError(f"Failed to get schema: {str(e)}")

    def get_server_info(self) -> Dict[str, Any]:
        """Get server configuration and metadata.

        Returns:
            Dictionary containing server configuration including:
            - server: Server settings (host, port, etc.)
            - registry: Feature view definitions
            - adapters: Adapter configurations (credentials filtered)

        Raises:
            QuiverValidationError: If client is closed
            QuiverServerError: If server error occurs
        """
        if self._closed:
            raise QuiverValidationError("Client is closed")

        try:
            import json

            action = flight.Action("get_server_info", b"")

            action_stream = self._flight_client.do_action(action)

            for result in action_stream:
                server_info = json.loads(result.body.to_pybytes().decode("utf-8"))
                return server_info

            return {}

        except Exception as e:
            raise QuiverServerError(f"Failed to get server info: {str(e)}")

    def flush_cache(self) -> bool:
        """Flush the server cache.

        This operation clears any cached feature data on the server,
        forcing fresh data retrieval on subsequent requests.

        Returns:
            bool: True if cache was successfully flushed

        Raises:
            QuiverValidationError: If client is closed
            QuiverServerError: If server error occurs

        Note:
            This is currently a no-op operation on the server side.
            Future implementations may add actual cache clearing functionality.
        """
        if self._closed:
            raise QuiverValidationError("Client is closed")

        try:
            action = flight.Action("flush_cache", b"")

            action_stream = self._flight_client.do_action(action)

            for result in action_stream:
                response = result.body.to_pybytes().decode("utf-8")
                logger.info(f"Cache flush response: {response}")
                return True

            return False

        except Exception as e:
            raise QuiverServerError(f"Failed to flush cache: {str(e)}")

    def reload_registry(self) -> bool:
        """Reload the feature registry.

        This operation forces the server to reload its feature view
        definitions from the configuration, allowing for dynamic
        configuration updates without server restart.

        Returns:
            bool: True if registry was successfully reloaded

        Raises:
            QuiverValidationError: If client is closed
            QuiverServerError: If server error occurs

        Note:
            This is currently a no-op operation on the server side.
            Future implementations may add actual registry reloading functionality.
        """
        if self._closed:
            raise QuiverValidationError("Client is closed")

        try:
            action = flight.Action("reload_registry", b"")

            action_stream = self._flight_client.do_action(action)

            for result in action_stream:
                response = result.body.to_pybytes().decode("utf-8")
                logger.info(f"Registry reload response: {response}")
                return True

            return False

        except Exception as e:
            raise QuiverServerError(f"Failed to reload registry: {str(e)}")

    def list_server_actions(self) -> List[Dict[str, str]]:
        """List all available server management actions.

        Returns:
            List of action dictionaries containing:
            - type: Action type identifier
            - description: Human-readable description of the action

        Raises:
            QuiverValidationError: If client is closed
            QuiverServerError: If server error occurs
        """
        if self._closed:
            raise QuiverValidationError("Client is closed")

        try:
            actions = list(self._flight_client.list_actions())

            action_list = []
            for action in actions:
                action_list.append(
                    {"type": action.type, "description": action.description}
                )

            return action_list

        except Exception as e:
            raise QuiverServerError(f"Failed to list server actions: {str(e)}")

    def get_last_request_id(self) -> Optional[str]:
        """Get the request ID from the last feature request.

        The request ID can be used to retrieve detailed metrics about the request
        from the observability service via get_metrics().

        Returns:
            The request ID if available, None if no request has been made yet

        Example:
            table = client.get_features(...)
            request_id = client.get_last_request_id()
            if request_id:
                metrics = client.get_metrics(request_id)
                print(f"Total latency: {metrics['total_ms']:.2f}ms")
        """
        return self._last_request_id

    def get_last_from_cache(self) -> Optional[bool]:
        """Get the cache status from the last feature request.

        Returns:
            True if the last response was from cache, False if fresh, None if unknown
        """
        return self._last_from_cache

    def get_metrics(self, request_id: Optional[str] = None) -> Dict[str, Any]:
        """Retrieve detailed metrics for a feature request.

        Queries the observability service for latency metrics collected during
        feature resolution. Metrics include 11 instrumentation points covering
        registry lookup, dispatch, backend execution, merge, and more.

        Args:
            request_id: The request ID to retrieve metrics for. If None, uses the
                       request ID from the last feature request.

        Returns:
            Dictionary with latency metrics (all in milliseconds):
            - registry_lookup_ms: Time to lookup feature view in registry
            - cache_lookup_ms: Time to check metrics cache
            - partition_ms: Time to partition features by backend
            - dispatch_ms: Time to submit requests to backends
            - backend_redis_ms: Redis backend latency (0.0 if not used)
            - backend_delta_ms: Delta backend latency (0.0 if not used)
            - backend_postgres_ms: PostgreSQL backend latency (0.0 if not used)
            - backend_max_ms: Maximum latency across all backends
            - alignment_ms: Time to align entity results
            - merge_ms: Time to merge backend results via Arrow LEFT JOIN
            - validation_ms: Time to validate merged results
            - serialization_ms: Time to convert to Arrow IPC format
            - total_ms: Total request latency
            - critical_path_ms: Critical path latency
            - feature_view: Feature view that was queried
            - entity_count: Number of entities requested
            - request_timestamp: When the request was processed

        Raises:
            QuiverValidationError: If client is closed or no request_id available
            QuiverConnectionError: If observability service unavailable
            QuiverServerError: If request_id not found (metrics expired or invalid)
            QuiverTimeoutError: If observability service times out

        Example:
            table = client.get_features("user_features", ["user1", "user2"], ["score"])
            metrics = client.get_metrics()  # Uses last request ID
            print(f"Merge latency: {metrics['merge_ms']:.2f}ms")
        """
        if self._closed:
            raise QuiverValidationError("Client is closed")

        if not self._obs_client:
            raise QuiverConnectionError(
                "Observability service not available",
                "Check that observability service is running on port 8816",
            )

        effective_request_id = request_id or self._last_request_id
        if not effective_request_id:
            raise QuiverValidationError(
                "No request_id available; either pass one explicitly or call get_features() first"
            )

        return self._obs_client.get_metrics(effective_request_id)

    def close(self) -> None:
        """Close connection to server."""
        if not self._closed:
            try:
                if self._flight_client and hasattr(self._flight_client, "close"):
                    self._flight_client.close()
                if self._obs_client:
                    self._obs_client.close()
            except Exception as e:
                logger.warning(f"Error closing client: {e}")
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
