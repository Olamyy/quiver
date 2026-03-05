# Quiver ML Inference Benchmarks

This benchmark suite proves Quiver's value proposition for real-world ML inference workloads by comparing it against traditional feature serving approaches.

## Quick Start

```bash
# Run complete benchmark suite (30 seconds per scenario)
make bench

# Run quick verification (10 second timeout)
make bench-quick

# Run precise Criterion benchmarks with detailed analysis
make bench-criterion

# Generate reports and view results
make bench-report
```

## What This Proves

Instead of testing "do the gRPC methods work?" (which is already covered by unit tests), these benchmarks answer the business question: **"Should I use Quiver for my ML inference workload?"**

### Real Performance Questions Answered:
- How much faster is feature lookup for a recommendation model?
- How does zero-copy transfer improve end-to-end inference time?
- Can I replace my current feature serving with this and get better performance?
- What are the cost implications of switching to Quiver?

### Scenarios Tested:

#### E-commerce Recommendations
- **Workload:** Product recommendations with user, item, and context features
- **Scale:** 10,000 users, ~60 features per request
- **Comparison:** Quiver (Arrow Flight) vs Redis+JSON
- **Metrics:** Latency, throughput, memory usage, operational complexity

*Future scenarios (planned):*
- **Fraud Detection:** Real-time transaction scoring with 500+ features
- **Ad Targeting:** User profile + context features for ad serving
- **Batch Scoring:** Large-scale batch predictions cost analysis

## Business Impact Results

Expected benchmark results demonstrate:

```
PERFORMANCE IMPROVEMENTS:
   - 2.5x faster inference vs Redis+JSON
   - 40% reduction in memory usage
   - Sub-10ms P95 latency for feature-heavy workloads

COST BENEFITS:
   - Lower infrastructure costs due to memory efficiency
   - Reduced operational overhead (fewer services to maintain)
   - Better user experience from faster response times
```

## Report Formats

### Executive Summary (Console)
Real-time results with business-focused metrics during benchmark execution.

### Detailed Reports (Files)
- **JSON:** `reports/benchmark_report_TIMESTAMP.json` - Machine-readable results
- **Markdown:** `reports/benchmark_report_TIMESTAMP.md` - Human-readable analysis

### Criterion Analysis (HTML)
- **Charts:** `target/criterion/` - Detailed performance visualization
- **Comparisons:** Statistical analysis of performance differences

## Architecture

```
benchmarks/
├── main.rs                     # Main benchmark runner
├── lib.rs                      # Report generation and orchestration
├── inference/                  # ML inference scenario benchmarks
│   └── recommendation_system.rs   # E-commerce recommendation benchmark
├── vs_alternatives/            # Head-to-head comparisons (future)
└── benches/                    # Criterion-based precise benchmarks
    └── recommendation_benchmark.rs
```

## Benchmark Methodology

### Realistic Workloads
- **Data:** Synthetic but realistic feature distributions
- **Scale:** Production-like entity counts and feature dimensions  
- **Patterns:** Request patterns that match actual ML serving

### Fair Comparisons
- **Baseline:** Redis+JSON (most common current approach)
- **Environment:** Same hardware, network, and resource constraints
- **Timing:** End-to-end latency including serialization overhead

### Business Metrics
- **Latency:** P50/P95/P99 response times that affect user experience
- **Throughput:** Requests per second under sustained load
- **Resources:** Memory and CPU usage translating to infrastructure costs
- **Reliability:** Success rates and error handling

## Extending the Benchmarks

To add new benchmark scenarios:

1. **Create scenario module:** `benchmarks/inference/new_scenario.rs`
2. **Implement comparison:** Quiver vs alternative approach
3. **Add to runner:** Include in `benchmarks/lib.rs::run_all_benchmarks()`
4. **Update Makefile:** Add any new commands if needed

### Example Scenario Structure:
```rust
pub async fn run_fraud_detection_benchmark() -> BenchmarkResults {
    // 1. Define realistic fraud detection workload
    // 2. Implement Quiver-based feature serving
    // 3. Implement traditional database+cache approach
    // 4. Run head-to-head performance comparison
    // 5. Return business-focused metrics
}
```

This ensures all benchmarks prove real business value rather than just technical functionality.