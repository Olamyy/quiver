"""Direct Redis client implementations"""

import json
import struct
import time
from typing import Dict, List, Any
import redis.asyncio as redis
from ..models import DataPoint


class DirectRedisClient:
    """Base class for direct Redis access"""

    def __init__(self, host: str = "localhost", port: int = 6379):
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            decode_responses=False,
            socket_connect_timeout=5,
            socket_timeout=5,
        )

    async def close(self):
        """Close Redis connection"""
        await self.redis_client.aclose()

    async def ping(self) -> bool:
        """Test Redis connection"""
        try:
            await self.redis_client.ping()
            return True
        except:
            return False


class DirectRedisBinaryClient(DirectRedisClient):
    """Direct Redis client using binary format (same as Quiver internal storage)"""

    async def get_features(
        self, entity_ids: List[str], feature_names: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Get features using binary format for efficient storage/retrieval"""
        time.time()

        try:
            results = {}

            pipe = self.redis_client.pipeline()

            keys_map = {}
            for entity_id in entity_ids:
                for feature_name in feature_names:
                    redis_key = f"quiver:f:{feature_name}:e:{entity_id}"
                    keys_map[(entity_id, feature_name)] = redis_key
                    pipe.get(redis_key)

            values = await pipe.execute()

            value_idx = 0
            for entity_id in entity_ids:
                entity_features = {}
                for feature_name in feature_names:
                    raw_value = values[value_idx]
                    value_idx += 1

                    if raw_value is not None:
                        try:
                            entity_features[feature_name] = struct.unpack(
                                "d", raw_value
                            )[0]
                        except struct.error:
                            try:
                                entity_features[feature_name] = float(
                                    raw_value.decode()
                                )
                            except (UnicodeDecodeError, ValueError):
                                entity_features[feature_name] = None
                    else:
                        entity_features[feature_name] = None

                results[entity_id] = entity_features

            return results

        except Exception as e:
            print(f"Error in DirectRedisBinaryClient: {e}")
            raise


class DirectRedisJSONClient(DirectRedisClient):
    """Direct Redis client using JSON format (traditional approach)"""

    def __init__(self, host: str = "localhost", port: int = 6379):
        super().__init__(host, port)
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )

    async def get_features(
        self, entity_ids: List[str], feature_names: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Get features using JSON format (most common current approach)"""
        time.time()

        try:
            results = {}

            pipe = self.redis_client.pipeline()

            keys_map = {}
            for entity_id in entity_ids:
                for feature_name in feature_names:
                    redis_key = f"feature:{feature_name}:entity:{entity_id}"
                    keys_map[(entity_id, feature_name)] = redis_key
                    pipe.get(redis_key)

            values = await pipe.execute()

            value_idx = 0
            for entity_id in entity_ids:
                entity_features = {}
                for feature_name in feature_names:
                    raw_value = values[value_idx]
                    value_idx += 1

                    if raw_value is not None:
                        try:
                            entity_features[feature_name] = json.loads(raw_value)
                        except json.JSONDecodeError:
                            try:
                                entity_features[feature_name] = float(raw_value)
                            except ValueError:
                                entity_features[feature_name] = None
                    else:
                        entity_features[feature_name] = None

                results[entity_id] = entity_features

            return results

        except Exception as e:
            print(f"Error in DirectRedisJSONClient: {e}")
            raise


async def benchmark_redis_client(
    client: DirectRedisClient,
    entity_ids: List[str],
    feature_names: List[str],
    duration_seconds: int,
) -> List[DataPoint]:
    """Benchmark a Redis client implementation"""

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
