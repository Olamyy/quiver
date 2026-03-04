"""Quiver client implementation for benchmarks"""

import time
from typing import Dict, List, Any

try:
    import quiver

    QUIVER_AVAILABLE = True
except ImportError:
    QUIVER_AVAILABLE = False

from ..models import DataPoint


class QuiverBenchmarkClient:
    """Quiver client for benchmark testing"""

    def __init__(self, host: str = "localhost", port: int = 8815):
        if not QUIVER_AVAILABLE:
            raise ImportError("Quiver client not available")

        self.host = host
        self.port = port
        self.client = None

    async def connect(self):
        """Connect to Quiver server"""
        try:
            self.client = quiver.Client(f"{self.host}:{self.port}")
            await self.ping()
            return True
        except Exception as e:
            print(f"Failed to connect to Quiver at {self.host}:{self.port}: {e}")
            return False

    async def ping(self) -> bool:
        """Test Quiver connection"""
        try:
            if self.client is None:
                return False
            views = self.client.list_feature_views()
            return True
        except Exception: # noqa
            return False

    async def close(self):
        """Close Quiver connection"""
        if self.client:
            self.client.close()
            self.client = None

    async def get_features(
        self,
        entity_ids: List[str],
        feature_names: List[str],
        feature_view: str = "user_features",
    ) -> Dict[str, Dict[str, Any]]:
        """Get features using Quiver client"""
        if not self.client:
            raise RuntimeError("Client not connected")

        try:
            feature_table = self.client.get_features(
                feature_view=feature_view, entities=entity_ids, features=feature_names
            )

            df = feature_table.to_pandas()

            results = {}
            for entity_id in entity_ids:
                entity_rows = (
                    df[df["entity_id"] == entity_id]
                    if "entity_id" in df.columns
                    else df
                )

                if len(entity_rows) > 0:
                    row = entity_rows.iloc[0]
                    entity_features = {}

                    for feature_name in feature_names:
                        if feature_name in row:
                            value = row[feature_name]
                            if hasattr(value, "isna") and value.isna():
                                entity_features[feature_name] = None
                            else:
                                entity_features[feature_name] = value
                        else:
                            entity_features[feature_name] = None

                    results[entity_id] = entity_features
                else:
                    results[entity_id] = {fname: None for fname in feature_names}

            return results

        except Exception as e:
            print(f"Error in QuiverBenchmarkClient: {e}")
            raise


async def benchmark_quiver_client(
    client: QuiverBenchmarkClient,
    entity_ids: List[str],
    feature_names: List[str],
    duration_seconds: int,
    feature_view: str = "user_features",
) -> List[DataPoint]:
    """Benchmark Quiver client implementation"""

    data_points = []
    end_time = time.time() + duration_seconds

    while time.time() < end_time:
        start = time.time()

        try:
            result = await client.get_features(entity_ids, feature_names, feature_view)
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
