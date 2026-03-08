"""Observability client for querying request metrics from Quiver server.

Provides transparent access to the separate observability service on port 8816
for retrieving detailed latency metrics about feature requests.
"""

import logging
from typing import Optional, Dict, Any
import grpc

from .exceptions import QuiverConnectionError, QuiverServerError, QuiverTimeoutError
from .proto import observability_pb2, observability_pb2_grpc

logger = logging.getLogger(__name__)


class ObservabilityClient:
    """Client for querying request metrics from the observability service.

    Provides access to detailed fanout latency metrics (11 instrumentation points)
    for feature requests via the separate observability gRPC service.

    Args:
        host: Observability service host (default: "localhost")
        port: Observability service port (default: 8816)
        timeout: Request timeout in seconds (default: 5.0)

    Raises:
        QuiverConnectionError: If unable to connect to observability service
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8816,
        timeout: float = 5.0,
    ) -> None:
        self._host = host
        self._port = port
        self._timeout = timeout
        self._channel = None
        self._stub = None
        self._closed = False

        try:
            self._connect()
        except Exception as e:
            raise QuiverConnectionError(
                f"Failed to connect to observability service: {str(e)}",
                f"{host}:{port}",
            )

    def _connect(self) -> None:
        """Establish connection to observability service (sync)."""
        target = f"{self._host}:{self._port}"
        self._channel = grpc.insecure_channel(target)
        self._stub = observability_pb2_grpc.ObservabilityServiceStub(self._channel)

    def get_metrics(self, request_id: str) -> Dict[str, Any]:
        """Retrieve metrics for a feature request by ID (synchronous).

        Args:
            request_id: The request ID returned in the x-request-id header
                       from a previous feature request

        Returns:
            Dictionary with latency metrics:
            - registry_lookup_ms: Time to lookup feature view
            - cache_lookup_ms: Time to check metrics cache
            - partition_ms: Time to partition features by backend
            - dispatch_ms: Time to dispatch to backends
            - backend_redis_ms: Redis backend latency (if used, else 0.0)
            - backend_delta_ms: Delta backend latency (if used, else 0.0)
            - backend_postgres_ms: PostgreSQL backend latency (if used, else 0.0)
            - backend_max_ms: Maximum backend latency
            - alignment_ms: Time to align entity results
            - merge_ms: Time to merge backend results
            - validation_ms: Time to validate merged results
            - serialization_ms: Time to serialize to Arrow
            - total_ms: Total request latency
            - critical_path_ms: Critical path latency
            - feature_view: Feature view that was queried
            - entity_count: Number of entities requested
            - request_timestamp: When the request was processed

        Raises:
            QuiverConnectionError: If connection fails
            QuiverServerError: If request_id not found (expired or invalid)
            QuiverTimeoutError: If request times out
            ValueError: If request_id is empty
        """
        if self._closed:
            raise QuiverConnectionError("Observability client is closed", "")

        if not request_id or not request_id.strip():
            raise ValueError("request_id cannot be empty")

        try:
            request = observability_pb2.GetMetricsRequest(request_id=request_id)
            response = self._stub.GetMetrics(request, timeout=self._timeout)

            return {
                "registry_lookup_ms": response.metrics.registry_lookup_ms,
                "cache_lookup_ms": response.metrics.cache_lookup_ms,
                "partition_ms": response.metrics.partition_ms,
                "dispatch_ms": response.metrics.dispatch_ms,
                "backend_redis_ms": response.metrics.backend_redis_ms,
                "backend_delta_ms": response.metrics.backend_delta_ms,
                "backend_postgres_ms": response.metrics.backend_postgres_ms,
                "backend_max_ms": response.metrics.backend_max_ms,
                "alignment_ms": response.metrics.alignment_ms,
                "merge_ms": response.metrics.merge_ms,
                "validation_ms": response.metrics.validation_ms,
                "serialization_ms": response.metrics.serialization_ms,
                "total_ms": response.metrics.total_ms,
                "critical_path_ms": response.metrics.critical_path_ms,
                "feature_view": response.feature_view,
                "entity_count": response.entity_count,
                "request_timestamp": response.request_timestamp,
            }
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise QuiverServerError(
                    f"Metrics not found for request_id: {request_id} (expired or invalid)"
                )
            elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise QuiverTimeoutError(
                    f"Observability service timeout for request_id: {request_id}",
                    self._timeout,
                )
            else:
                raise QuiverServerError(f"Observability service error: {str(e)}")
        except Exception as e:
            raise QuiverServerError(f"Failed to retrieve metrics: {str(e)}")

    def close(self) -> None:
        """Close the connection to observability service."""
        if not self._closed and self._channel:
            self._channel.close()
            self._closed = True

    def __del__(self) -> None:
        """Cleanup on garbage collection."""
        try:
            self.close()
        except Exception:
            pass

    def __repr__(self) -> str:
        return f"ObservabilityClient({self._host}:{self._port})"
