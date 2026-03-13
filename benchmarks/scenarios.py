from dataclasses import dataclass
from typing import Dict, List, Any
import time
import psutil


@dataclass
class ScenarioConfig:
    """Configuration for a benchmark scenario."""
    name: str
    backends: List[str]
    feature_views: List[str]
    entity_count: int = 100
    feature_count: int = 4
    batch_size: int = 1


SCENARIOS = {
    "redis-postgres": ScenarioConfig(
        name="redis-postgres",
        backends=["redis", "postgres"],
        feature_views=["ecommerce_view", "timeseries_view"],
        entity_count=100,
        batch_size=1,
    ),
    "redis-postgres-s3": ScenarioConfig(
        name="redis-postgres-s3",
        backends=["redis", "postgres", "s3"],
        feature_views=["ecommerce_view", "timeseries_view"],
        entity_count=100,
        batch_size=1,
    ),
}

FIXTURE_ENTITIES = {
    "ecommerce_view": ["user:1000", "user:1001", "user:1002"],
    "timeseries_view": ["sensor:1000", "sensor:1001", "sensor:1002"],
    "sessions_view": ["session:1000", "session:1001", "session:1002"],
}

FIXTURE_COLUMNS = {
    "ecommerce_view": {
        "spend_30d": "float64",
        "purchase_count": "int64",
        "last_purchase_date": "timestamp",
        "is_premium": "bool",
    },
    "timeseries_view": {
        "temperature": "float64",
        "humidity": "float64",
        "alert_triggered": "bool",
    },
    "sessions_view": {
        "active_user_id": "string",
        "request_count": "int64",
        "last_activity": "timestamp",
    },
}


def generate_entity_ids(view_name: str, count: int) -> List[str]:
    """Generate entity IDs for a feature view."""
    if view_name == "ecommerce_view":
        return [f"user:{1000 + i}" for i in range(count)]
    elif view_name == "timeseries_view":
        return [f"sensor:{1000 + i}" for i in range(count)]
    elif view_name == "sessions_view":
        return [f"session:{1000 + i}" for i in range(count)]
    else:
        raise ValueError(f"Unknown feature view: {view_name}")


def get_scenario_config(scenario_name: str, entity_count: int = 100) -> ScenarioConfig:
    """Get scenario config, optionally overriding entity count."""
    if scenario_name not in SCENARIOS:
        raise ValueError(f"Unknown scenario: {scenario_name}")

    config = SCENARIOS[scenario_name]
    config.entity_count = entity_count
    return config


@dataclass
class MetricsCollector:
    """Collects timing and resource metrics during benchmarks."""

    start_time: float = 0.0
    end_time: float = 0.0
    latencies: List[float] = None  # milliseconds
    peak_cpu: float = 0.0
    peak_memory: float = 0.0

    def __post_init__(self):
        if self.latencies is None:
            self.latencies = []

    def start(self):
        """Start timing and resource monitoring."""
        self.start_time = time.perf_counter()
        self.peak_cpu = psutil.cpu_percent(interval=0.1)
        self.peak_memory = psutil.virtual_memory().used / 1024 / 1024  # MB

    def end(self):
        """End timing and finalize metrics."""
        self.end_time = time.perf_counter()

    def record_latency(self, latency_ms: float):
        """Record a single request latency in milliseconds."""
        self.latencies.append(latency_ms)

    def update_resource_usage(self):
        """Update peak CPU and memory usage."""
        self.peak_cpu = max(self.peak_cpu, psutil.cpu_percent(interval=0.01))
        current_mem = psutil.virtual_memory().used / 1024 / 1024
        self.peak_memory = max(self.peak_memory, current_mem)

    def percentile(self, p: float) -> float:
        """Get latency percentile (e.g., 0.50 for P50)."""
        if not self.latencies:
            return 0.0
        sorted_lat = sorted(self.latencies)
        idx = int(len(sorted_lat) * p)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "latency_p50_ms": round(self.percentile(0.50), 2),
            "latency_p95_ms": round(self.percentile(0.95), 2),
            "latency_p99_ms": round(self.percentile(0.99), 2),
            "throughput_rps": round(len(self.latencies) / (self.end_time - self.start_time), 2),
            "peak_cpu_percent": round(self.peak_cpu, 2),
            "peak_memory_mb": round(self.peak_memory, 2),
            "request_count": len(self.latencies),
            "duration_seconds": round(self.end_time - self.start_time, 2),
        }


def format_metrics_report(
    scenario: str,
    baseline_metrics: Dict[str, Any],
    quiver_metrics: Dict[str, Any],
) -> str:
    """Format benchmark results into human-readable report."""

    baseline_p50 = baseline_metrics["latency_p50_ms"]
    quiver_p50 = quiver_metrics["latency_p50_ms"]

    if quiver_p50 > 0:
        speedup = baseline_p50 / quiver_p50
        improvement_pct = (1 - quiver_p50 / baseline_p50) * 100
    else:
        speedup = 0
        improvement_pct = 0

    baseline_cpu = baseline_metrics["peak_cpu_percent"]
    quiver_cpu = quiver_metrics["peak_cpu_percent"]
    cpu_reduction_pct = (1 - quiver_cpu / baseline_cpu) * 100 if baseline_cpu > 0 else 0

    report = f"""
COMPARATIVE BENCHMARK RESULTS
=============================

Scenario: {scenario}
─────────────────────────────────────────────────────────

Direct Backend Access (Application Fan-Out):
  Latency (P50):        {baseline_metrics['latency_p50_ms']:.2f} ms
  Latency (P95):        {baseline_metrics['latency_p95_ms']:.2f} ms
  Latency (P99):        {baseline_metrics['latency_p99_ms']:.2f} ms
  Throughput:           {baseline_metrics['throughput_rps']:.0f} req/s
  CPU Peak:             {baseline_metrics['peak_cpu_percent']:.1f}%
  Memory Peak:          {baseline_metrics['peak_memory_mb']:.0f} MB
  Requests:             {baseline_metrics['request_count']}

Quiver Server:
  Latency (P50):        {quiver_metrics['latency_p50_ms']:.2f} ms
  Latency (P95):        {quiver_metrics['latency_p95_ms']:.2f} ms
  Latency (P99):        {quiver_metrics['latency_p99_ms']:.2f} ms
  Throughput:           {quiver_metrics['throughput_rps']:.0f} req/s
  CPU Peak:             {quiver_metrics['peak_cpu_percent']:.1f}%
  Memory Peak:          {quiver_metrics['peak_memory_mb']:.0f} MB
  Requests:             {quiver_metrics['request_count']}

Performance Comparison:
  Latency Speedup:      {speedup:.2f}x faster
  Improvement:          {improvement_pct:+.1f}%
  CPU Efficiency:       {cpu_reduction_pct:+.1f}% (lower is better)
"""
    return report
