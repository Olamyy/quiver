from dataclasses import dataclass
from typing import Optional
from enum import Enum


class LoadPattern(str, Enum):
    SUSTAINED = "sustained"
    BURST = "burst"
    RAMP_UP = "ramp_up"


class BenchmarkMethod(str, Enum):
    DIRECT_REDIS_BINARY = "direct_redis_binary"
    DIRECT_REDIS_JSON = "direct_redis_json"
    QUIVER_CLIENT = "quiver_client"
    HTTP_API = "http_api"


@dataclass
class BenchmarkConfig:
    duration_seconds: int = 60
    warmup_seconds: int = 10
    load_pattern: LoadPattern = LoadPattern.SUSTAINED
    concurrent_requests: int = 100
    requests_per_second: Optional[int] = None
    num_entities: int = 10_000
    features_per_request: int = 60
    feature_size_bytes: int = 8
    redis_host: str = "localhost"
    redis_port: int = 6379
    quiver_host: str = "localhost"
    quiver_port: int = 8815
    http_host: str = "localhost"
    http_port: int = 8000
    methods: Optional[list[str]] = None

    def __post_init__(self):
        if not self.methods:
            self.methods = list(BenchmarkMethod)


@dataclass
class BurstConfig:
    burst_duration_seconds: int = 5
    quiet_duration_seconds: int = 10
    burst_multiplier: float = 5.0


@dataclass
class RampUpConfig:
    start_rps: int = 10
    end_rps: int = 1000
    ramp_duration_seconds: int = 60


SCENARIOS = {
    "quick": BenchmarkConfig(
        duration_seconds=30,
        warmup_seconds=5,
        concurrent_requests=50,
        num_entities=1_000,
        features_per_request=30,
    ),
    "standard": BenchmarkConfig(
        duration_seconds=60,
        warmup_seconds=10,
        concurrent_requests=100,
        num_entities=10_000,
        features_per_request=60,
    ),
    "heavy": BenchmarkConfig(
        duration_seconds=120,
        warmup_seconds=15,
        concurrent_requests=200,
        num_entities=50_000,
        features_per_request=120,
    ),
    "burst_test": BenchmarkConfig(
        duration_seconds=300,
        load_pattern=LoadPattern.BURST,
        concurrent_requests=100,
        num_entities=10_000,
        features_per_request=60,
    ),
    "capacity_test": BenchmarkConfig(
        duration_seconds=180,
        load_pattern=LoadPattern.RAMP_UP,
        num_entities=25_000,
        features_per_request=80,
    ),
}
