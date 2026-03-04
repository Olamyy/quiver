"""HTTP client for benchmark testing"""

import time
from typing import Dict, List, Any
import httpx
from ..models import DataPoint


class HTTPBenchmarkClient:
    """HTTP client for testing FastAPI feature server"""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )

    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()

    async def ping(self) -> bool:
        """Test HTTP server connection"""
        try:
            response = await self.client.get(f"{self.base_url}/health")
            return response.status_code == 200
        except:
            return False

    async def get_features(
        self, entity_ids: List[str], feature_names: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Get features via HTTP API"""
        try:
            request_data = {"entity_ids": entity_ids, "feature_names": feature_names}

            response = await self.client.post(
                f"{self.base_url}/features", json=request_data
            )

            if response.status_code == 200:
                data = response.json()
                return data.get("features", {})
            else:
                raise Exception(f"HTTP {response.status_code}: {response.text}")

        except Exception as e:
            print(f"Error in HTTPBenchmarkClient: {e}")
            raise

    async def get_stats(self) -> Dict[str, Any]:
        """Get Redis stats via HTTP API"""
        try:
            response = await self.client.get(f"{self.base_url}/stats")
            if response.status_code == 200:
                return response.json()
            else:
                return {}
        except:
            return {}


async def benchmark_http_client(
    client: HTTPBenchmarkClient,
    entity_ids: List[str],
    feature_names: List[str],
    duration_seconds: int,
) -> List[DataPoint]:
    """Benchmark HTTP client implementation"""

    data_points = []
    end_time = time.time() + duration_seconds

    while time.time() < end_time:
        start = time.time()

        try:
            result = await client.get_features(entity_ids, feature_names)
            latency_ms = (time.time() - start) * 1000

            success = len(result) > 0 and any(
                any(v is not None for v in features.values())
                for features in result.values()
            )

            data_points.append(
                DataPoint(
                    timestamp=start,
                    latency_ms=latency_ms,
                    success=success,
                )
            )

        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            data_points.append(
                DataPoint(
                    timestamp=start,
                    latency_ms=latency_ms,
                    success=False,
                    error_type=type(e).__name__,
                )
            )

    return data_points
