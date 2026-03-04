//! Quiver Benchmark Suite

use quiver_benchmarks::run_all_benchmarks;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let _report = run_all_benchmarks().await;

    println!("Benchmark completed. Check reports/ directory for detailed results.");
    
    Ok(())
}