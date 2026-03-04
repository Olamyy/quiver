import asyncio
import json
import time
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


class FeatureRequest(BaseModel):
    entity_ids: List[str]
    feature_names: List[str]


class FeatureResponse(BaseModel):
    features: Dict[str, Dict[str, float]]


class FeatureServerState:
    redis_client: Optional[redis.Redis] = None


state = FeatureServerState()


@asynccontextmanager
async def lifespan(app: FastAPI):
    state.redis_client = redis.Redis(
        host="localhost",
        port=6379,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )

    try:
        await state.redis_client.ping()
        print("Connected to Redis successfully")
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        raise

    yield

    if state.redis_client:
        await state.redis_client.aclose()


app = FastAPI(
    title="Quiver Benchmark HTTP Server",
    description="HTTP API for benchmarking feature serving performance",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check():
    try:
        await state.redis_client.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis connection failed: {e}")


@app.post("/features", response_model=FeatureResponse)
async def get_features(request: FeatureRequest):
    start_time = time.time()

    try:
        features = {}

        for entity_id in request.entity_ids:
            entity_features = {}

            for feature_name in request.feature_names:
                redis_key = f"feature:{feature_name}:entity:{entity_id}"

                try:
                    value = await state.redis_client.get(redis_key)
                    if value is not None:
                        try:
                            entity_features[feature_name] = json.loads(value)
                        except json.JSONDecodeError:
                            entity_features[feature_name] = float(value)
                    else:
                        entity_features[feature_name] = None

                except Exception as e:
                    print(f"Error fetching {redis_key}: {e}")
                    entity_features[feature_name] = None

            features[entity_id] = entity_features

        duration_ms = (time.time() - start_time) * 1000

        return FeatureResponse(features=features)

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch features: {str(e)}"
        )


@app.post("/features/batch", response_model=List[FeatureResponse])
async def get_features_batch(requests: List[FeatureRequest]):
    tasks = [get_features(req) for req in requests]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    responses = []
    for result in results:
        if isinstance(result, Exception):
            raise HTTPException(status_code=500, detail=str(result))
        responses.append(result)

    return responses


@app.get("/stats")
async def get_redis_stats():
    try:
        info = await state.redis_client.info()
        return {
            "redis_version": info.get("redis_version"),
            "connected_clients": info.get("connected_clients"),
            "used_memory_human": info.get("used_memory_human"),
            "total_commands_processed": info.get("total_commands_processed"),
            "keyspace": {k: v for k, v in info.items() if k.startswith("db")},
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get Redis stats: {e}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "http_server:app", host="0.0.0.0", port=8000, reload=False, log_level="info"
    )
