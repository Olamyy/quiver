# Quiver Python Benchmarks

Technical benchmarks comparing Quiver feature serving performance against traditional approaches using Python implementations.

## Quick Start

```bash
# Run standard benchmark suite
make bench-py

# Run quick verification 
make bench-py-quick

# List available scenarios
make bench-py-scenarios
```

## Architecture Comparison

The benchmark tests 4 different approaches to feature serving:

### 1. Direct Redis (Binary)
**Path**: App → Python Redis Client → Redis  
**Storage**: Binary format (same as Quiver internal)  
**Purpose**: Isolate pure protocol/path overhead

### 2. Direct Redis (JSON)  
**Path**: App → Python Redis Client → Redis  
**Storage**: JSON format (traditional approach)  
**Purpose**: Real-world improvement vs current practices

### 3. Quiver Client
**Path**: App → Quiver Python Client → Quiver Server → Redis Adapter → Redis  
**Storage**: Binary format via Arrow Flight  
**Purpose**: Our proposed architecture

### 4. HTTP API
**Path**: App → HTTP Client → FastAPI Server → Redis → HTTP Response  
**Storage**: JSON format  
**Purpose**: Most common ML inference pattern

## Environment Detection

The benchmark automatically detects and reports:
- OS and version
- CPU model and core count  
- RAM size
- Python version
- Redis version (if accessible)
- Quiver version (if available)

## Configuration

### Predefined Scenarios

| Scenario | Duration | Entities | Features | Concurrent Requests |
|----------|----------|----------|----------|-------------------|
| quick | 30s | 1,000 | 30 | 50 |
| standard | 60s | 10,000 | 60 | 100 |
| heavy | 120s | 50,000 | 120 | 200 |
| burst_test | 300s | 10,000 | 60 | 100 (burst pattern) |
| capacity_test | 180s | 25,000 | 80 | ramp-up pattern |

### Custom Configuration

```bash
# Override duration
cd benchmarks/python && uv run python run_benchmark.py run --duration 120

# Test specific methods only
cd benchmarks/python && uv run python run_benchmark.py run --methods "direct_redis_json,quiver_client"

# Save results to file
cd benchmarks/python && uv run python run_benchmark.py run --output results.json
```

## Setup Requirements

### Infrastructure
1. **Redis server** running on localhost:6379
2. **Quiver server** running on localhost:8815 (for Quiver method)
3. **HTTP server** running on localhost:8000 (for HTTP method)

### Starting Services

```bash
# Start Redis (if not running)
redis-server

# Start Quiver server
make run

# Start HTTP server (in separate terminal)
make bench-py-http
```

## Report Format

The benchmark generates technical reports with:

### Environment Information
- System specifications (OS, CPU, RAM)
- Software versions (Python, Redis, Quiver)

### Methodology
- Test configuration (duration, load pattern, data size)
- Number of entities and features tested
- Concurrent request patterns

### Results
- Latency metrics (avg, P50, P95, P99, min, max)
- Throughput (requests per second)
- Success rate and error analysis
- Resource usage (when available)

### Sample Output

```
Environment Information:
  OS: Darwin 21.6.0
  CPU: Apple M2, 8 cores  
  RAM: 16GB
  Python: 3.10.12
  Redis: 7.0.5
  Quiver: 0.1.0

Performance Comparison:
┌─────────────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┐
│ Method              │ Avg Latency     │ P95 Latency     │ Throughput      │ Success Rate    │
├─────────────────────┼─────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ direct_redis_binary │ 2.3ms           │ 4.1ms           │ 4,247 rps       │ 99.8%           │
│ direct_redis_json   │ 3.8ms           │ 7.2ms           │ 2,630 rps       │ 99.5%           │
│ quiver_client       │ 4.2ms           │ 8.9ms           │ 2,380 rps       │ 99.1%           │
│ http_api            │ 12.5ms          │ 28.3ms          │ 800 rps         │ 99.9%           │
└─────────────────────┴─────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

## Implementation Details

### Load Patterns
- **Sustained**: Constant load throughout test duration
- **Burst**: Alternating high/low load periods  
- **Ramp-up**: Gradually increasing load over time

### Data Generation
- Realistic feature values (random floats)
- Configurable entity count and feature dimensions
- Both binary and JSON storage formats populated

### Error Handling
- Connection failures tracked and categorized
- Timeout handling with configurable limits
- Graceful degradation and cleanup

## Development

### Adding New Methods

1. Create client in `clients/new_method.py`
2. Implement async `get_features()` method
3. Add benchmark function following the pattern
4. Register in `BenchmarkMethod` enum
5. Add to `run_method_benchmark()` switch

### Adding New Scenarios

Add to `SCENARIOS` dict in `config.py`:

```python
"custom_scenario": BenchmarkConfig(
    duration_seconds=180,
    concurrent_requests=150,
    num_entities=20_000,
    features_per_request=80,
)
```

This benchmark suite provides objective, technical performance comparisons to inform architectural decisions about feature serving infrastructure.