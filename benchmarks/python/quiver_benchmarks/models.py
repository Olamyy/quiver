"""Data models for benchmark results and metrics"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional
from .config import BenchmarkConfig, BenchmarkMethod
from .environment import EnvironmentInfo


@dataclass
class PerformanceMetrics:
    """Performance metrics for a single benchmark run"""

    method: BenchmarkMethod
    total_requests: int
    successful_requests: int
    failed_requests: int

    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float

    throughput_rps: float
    success_rate: float

    avg_cpu_percent: float
    max_cpu_percent: float
    avg_memory_mb: float
    max_memory_mb: float

    total_bytes_sent: int = 0
    total_bytes_received: int = 0

    actual_duration_seconds: float = 0.0

    error_types: Optional[Dict[str, int]] = None

    def __post_init__(self):
        if self.error_types is None:
            self.error_types = {}


@dataclass
class BenchmarkResults:
    """Complete benchmark results for all methods tested"""

    config: BenchmarkConfig
    environment: EnvironmentInfo
    timestamp: datetime
    metrics: Dict[BenchmarkMethod, PerformanceMetrics]

    total_test_duration_seconds: float
    data_setup_time_seconds: float

    def get_baseline_method(self) -> Optional[BenchmarkMethod]:
        """Get the baseline method for comparison (typically direct Redis binary)"""
        if BenchmarkMethod.DIRECT_REDIS_BINARY in self.metrics:
            return BenchmarkMethod.DIRECT_REDIS_BINARY
        return next(iter(self.metrics.keys())) if self.metrics else None

    def calculate_improvements(self) -> Dict[BenchmarkMethod, Dict[str, float]]:
        """Calculate performance improvements relative to baseline"""
        baseline_method = self.get_baseline_method()
        if not baseline_method or baseline_method not in self.metrics:
            return {}

        baseline = self.metrics[baseline_method]
        improvements = {}

        for method, metrics in self.metrics.items():
            if method == baseline_method:
                continue

            improvements[method] = {
                "latency_improvement": baseline.avg_latency_ms / metrics.avg_latency_ms,
                "throughput_improvement": metrics.throughput_rps
                / baseline.throughput_rps,
                "memory_savings": (baseline.avg_memory_mb - metrics.avg_memory_mb)
                / baseline.avg_memory_mb,
                "latency_p95_improvement": baseline.p95_latency_ms
                / metrics.p95_latency_ms,
            }

        return improvements


@dataclass
class DataPoint:
    """Single measurement data point"""

    timestamp: float
    latency_ms: float
    success: bool
    error_type: Optional[str] = None
    memory_mb: float = 0.0
    cpu_percent: float = 0.0
