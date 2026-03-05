# Quiver Benchmarks

Performance benchmarks comparing Quiver against traditional feature serving methods.

## Quick Start

```bash
# Python benchmarks (currently available)
make bench-py              # Standard 60s benchmark (all methods)
make bench-py-quick        # Quick 30s verification  
make bench-py-heavy        # Heavy 120s load test
make bench-py-scenarios    # List available test configurations

# Start HTTP server for benchmarking (runs in foreground)
make bench-py-http

# Rust benchmarks (placeholder - not implemented)
make bench                 # Would run Rust benchmark suite
make bench-criterion       # Would run Criterion-based benchmarks
```

## Python Benchmark Implementation

### Test Methods

The Python benchmark suite compares four feature serving approaches:

1. **`direct_redis_binary`** - Direct Redis access using Quiver's binary format
2. **`direct_redis_json`** - Direct Redis access using JSON serialization  
3. **`quiver_client`** - Quiver gRPC client using Arrow Flight
4. **`http_api`** - HTTP REST API baseline

### Available Scenarios

| Name | Duration | Concurrency | Entities | Features |
|------|----------|-------------|----------|----------|
| `quick` | 30s | 50 | 1,000 | 30 |
| `standard` | 60s | 100 | 10,000 | 60 |
| `heavy` | 120s | 200 | 50,000 | 120 |
| `burst_test` | 300s | 100 | 10,000 | 60 |
| `capacity_test` | 180s | 100 | 25,000 | 80 |

### Prerequisites

The benchmarks require these services running:

1. **Redis server** - `make redis-start`
2. **Quiver server** - `make run` (or `make run-release` for production builds)
3. **HTTP server** - `make bench-py-http` (for HTTP API tests only)

### Full Benchmark Setup

```bash
# Terminal 1: Start Redis
make redis-start

# Terminal 2: Start Quiver server  
make run

# Terminal 3: Run benchmarks
make bench-py

# Optional Terminal 4: HTTP server (only needed for HTTP API tests)
make bench-py-http
```

### Individual Test Commands

```bash
cd benchmarks/python

# Run specific scenario
uv run python run_benchmark.py run --scenario standard

# Test specific methods only
uv run python run_benchmark.py run --scenario quick --methods quiver_client,direct_redis_json

# Custom configuration
uv run python run_benchmark.py run --scenario heavy --methods quiver_client
```

## Results Format

### Console Output
Real-time progress with final results table showing:
- Average, P50, P95, P99 latency (ms)
- Throughput (requests/second)
- Success rate
- CPU and memory usage

### JSON Export
Results exported to `reports/benchmark_report_TIMESTAMP.json` with structure:
```json
{
  "config": { "duration_seconds": 60, "concurrent_requests": 100, ... },
  "environment": { "redis_available": true, "quiver_available": true, ... },
  "metrics": {
    "direct_redis_binary": {
      "avg_latency_ms": 12.5,
      "p95_latency_ms": 25.1,
      "throughput_rps": 850.2,
      "success_rate": 0.998,
      ...
    }
  }
}
```

## Implementation Structure

```
benchmarks/
├── python/
│   ├── run_benchmark.py           # CLI entry point
│   └── quiver_benchmarks/
│       ├── benchmark_runner.py    # Main orchestrator  
│       ├── config.py              # Test scenarios and settings
│       ├── models.py              # Result data structures
│       ├── environment.py         # Infrastructure detection
│       └── clients/               # Test client implementations
│           ├── direct_redis.py    # Direct Redis clients
│           ├── quiver_client.py   # Quiver gRPC client
│           └── http_client.py     # HTTP API client
└── reports/                       # Generated result files
```

## Adding New Test Methods

1. Create client in `quiver_benchmarks/clients/new_method.py`:
```python
class NewMethodClient:
    async def get_features(self, entity_id: str, feature_names: list[str]) -> dict:
        # Implement feature retrieval
        pass

async def benchmark_new_method(config: BenchmarkConfig) -> PerformanceMetrics:
    # Implement benchmarking logic
    pass
```

2. Add to `BenchmarkMethod` enum in `config.py`
3. Import and register in `benchmark_runner.py`

## Adding New Scenarios

Add to `SCENARIOS` dict in `config.py`:
```python
"new_scenario": BenchmarkConfig(
    duration_seconds=90,
    concurrent_requests=150,
    num_entities=20_000,
    features_per_request=80,
)
```