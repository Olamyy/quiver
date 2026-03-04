//! Benchmark Runner
//!
//! Orchestrates benchmarks and generates business-focused reports
//! proving Quiver's value proposition for ML inference workloads.

use std::fs;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod inference {
    pub mod recommendation_system;
}

use inference::recommendation_system::{run_benchmark, print_results, BenchmarkResults};

#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub timestamp: DateTime<Utc>,
    pub version: String,
    pub environment: String,
    pub results: Vec<BenchmarkResults>,
    pub summary: BenchmarkSummary,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkSummary {
    pub total_scenarios: u32,
    pub avg_improvement_factor: f64,
    pub avg_memory_savings: f64,
    pub recommendation: String,
}

pub async fn run_all_benchmarks() -> BenchmarkReport {
    println!("Quiver ML Inference Benchmark Suite");
    println!("===================================");
    println!();

    let mut results = Vec::new();

    let rec_results = run_benchmark().await;
    print_results(&rec_results);
    results.push(rec_results);

    // Generate summary
    let summary = generate_summary(&results);
    
    let report = BenchmarkReport {
        timestamp: Utc::now(),
        version: "0.1.0".to_string(),
        environment: "local".to_string(),
        results,
        summary,
    };

    // Save report
    save_report(&report).await.expect("Failed to save report");

    report
}

/// Generate executive summary from all benchmark results
fn generate_summary(results: &[BenchmarkResults]) -> BenchmarkSummary {
    let total_scenarios = results.len() as u32;
    let avg_improvement_factor = results.iter()
        .map(|r| r.improvement_factor)
        .sum::<f64>() / total_scenarios as f64;
    let avg_memory_savings = results.iter()
        .map(|r| r.memory_savings)
        .sum::<f64>() / total_scenarios as f64;

    let recommendation = if avg_improvement_factor > 2.0 {
        "STRONG RECOMMEND: Quiver provides significant performance benefits for ML inference".to_string()
    } else if avg_improvement_factor > 1.5 {
        "RECOMMEND: Quiver shows meaningful improvements over traditional approaches".to_string()
    } else if avg_improvement_factor > 1.1 {
        "CONSIDER: Quiver provides modest benefits, evaluate based on your specific needs".to_string()
    } else {
        "EVALUATE: Performance benefits are marginal, consider other factors".to_string()
    };

    BenchmarkSummary {
        total_scenarios,
        avg_improvement_factor,
        avg_memory_savings,
        recommendation,
    }
}

/// Save benchmark report to file
async fn save_report(report: &BenchmarkReport) -> Result<(), Box<dyn std::error::Error>> {
    // Create reports directory if it doesn't exist
    fs::create_dir_all("reports")?;

    // Save JSON report for machine processing
    let json_report = serde_json::to_string_pretty(report)?;
    let timestamp = report.timestamp.format("%Y%m%d_%H%M%S");
    let json_filename = format!("reports/benchmark_report_{}.json", timestamp);
    fs::write(&json_filename, json_report)?;

    // Save markdown report for human consumption
    let markdown_report = generate_markdown_report(report);
    let md_filename = format!("reports/benchmark_report_{}.md", timestamp);
    fs::write(&md_filename, markdown_report)?;

    println!("📄 Reports saved:");
    println!("   • JSON: {}", json_filename);
    println!("   • Markdown: {}", md_filename);

    Ok(())
}

/// Generate markdown report for sharing
fn generate_markdown_report(report: &BenchmarkReport) -> String {
    let mut md = String::new();
    
    md.push_str("# Quiver Benchmark Report\n\n");
    md.push_str(&format!("**Generated:** {}\n", report.timestamp.format("%Y-%m-%d %H:%M:%S UTC")));
    md.push_str(&format!("**Version:** {}\n", report.version));
    md.push_str(&format!("**Environment:** {}\n\n", report.environment));

    for result in &report.results {
        md.push_str(&format!("## {}\n\n", result.scenario));
        
        md.push_str("### Environment\n");
        md.push_str(&format!("- Test duration: {} seconds\n", result.test_duration_seconds));
        md.push_str("- Platform: Simulated\n\n");
        
        md.push_str("### Dataset\n");
        md.push_str("- Users: 10,000\n");
        md.push_str("- Features per request: ~60\n");
        md.push_str("- Request pattern: Constant load\n\n");
        
        md.push_str("### Methodology\n");
        md.push_str("- Quiver: Simulated Arrow Flight protocol with 50μs overhead\n");
        md.push_str("- JSON: In-memory JSON serialization/deserialization with 200μs overhead\n");
        md.push_str("- Both approaches use identical synthetic data\n\n");
        
        md.push_str("### Results\n\n");
        md.push_str("| Metric | Quiver | JSON |\n");
        md.push_str("|--------|--------| ---- |\n");
        md.push_str(&format!("| Avg Latency | {:.2}ms | {:.2}ms |\n", 
                            result.quiver_results.avg_latency_ms,
                            result.json_results.avg_latency_ms));
        md.push_str(&format!("| P95 Latency | {:.2}ms | {:.2}ms |\n", 
                            result.quiver_results.p95_latency_ms,
                            result.json_results.p95_latency_ms));
        md.push_str(&format!("| Throughput | {:.1} QPS | {:.1} QPS |\n", 
                            result.quiver_results.throughput_qps,
                            result.json_results.throughput_qps));
        md.push_str(&format!("| Success Rate | {:.1}% | {:.1}% |\n\n", 
                            result.quiver_results.success_rate * 100.0,
                            result.json_results.success_rate * 100.0));
    }

    md
}

/// Print executive summary to console
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_benchmark_runner() {
        let results = vec![];
        let summary = generate_summary(&results);
        assert_eq!(summary.total_scenarios, 0);
    }

    #[test]
    fn test_markdown_generation() {
        use crate::inference::recommendation_system::{BenchmarkResults, PerformanceMetrics};
        
        let results = vec![BenchmarkResults {
            scenario: "Test".to_string(),
            quiver_results: PerformanceMetrics {
                avg_latency_ms: 5.0,
                p50_latency_ms: 4.0,
                p95_latency_ms: 8.0,
                p99_latency_ms: 12.0,
                throughput_qps: 200.0,
                memory_usage_mb: 50.0,
                cpu_utilization_percent: 30.0,
                success_rate: 1.0,
            },
            redis_json_results: PerformanceMetrics {
                avg_latency_ms: 10.0,
                p50_latency_ms: 8.0,
                p95_latency_ms: 15.0,
                p99_latency_ms: 25.0,
                throughput_qps: 150.0,
                memory_usage_mb: 80.0,
                cpu_utilization_percent: 50.0,
                success_rate: 1.0,
            },
            improvement_factor: 2.0,
            memory_savings: 0.375,
            test_duration_seconds: 30,
        }];

        let summary = generate_summary(&results);
        let report = BenchmarkReport {
            timestamp: Utc::now(),
            version: "test".to_string(),
            environment: "test".to_string(),
            results,
            summary,
        };

        let markdown = generate_markdown_report(&report);
        assert!(markdown.contains("# Quiver ML Inference Benchmark Report"));
        assert!(markdown.contains("2.0x faster"));
        assert!(markdown.contains("37% reduction"));
    }
}