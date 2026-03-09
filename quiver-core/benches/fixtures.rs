/// Test data generation for benchmarks
/// Provides deterministic entity lists and batch size configurations for reproducible measurements.

/// Generates a deterministic list of entity IDs for benchmarking.
/// Entities follow the format "user:{id}" where id ranges from 1000 to 1000 + count - 1.
/// Order is consistent across calls to ensure reproducible merging behavior.
#[allow(dead_code)]
pub fn get_entities(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("user:{}", 1000 + i)).collect()
}

/// Predefined batch sizes for scaling benchmarks.
/// Tests throughput and latency across entity batch sizes from 1 to 10,000.
pub const DEFAULT_BATCH_SIZES: &[usize] = &[1, 10, 100, 1_000, 10_000];

/// Parses batch sizes from environment variable or returns default set.
/// Format: "BENCH_BATCH_SIZES=1,10,100" produces [1, 10, 100]
pub fn parse_batch_sizes() -> Vec<usize> {
    std::env::var("BENCH_BATCH_SIZES")
        .ok()
        .and_then(|s| {
            let sizes: Result<Vec<_>, _> = s
                .split(',')
                .map(|part| part.trim().parse::<usize>())
                .collect();
            sizes.ok().filter(|v| !v.is_empty())
        })
        .unwrap_or_else(|| DEFAULT_BATCH_SIZES.to_vec())
}
