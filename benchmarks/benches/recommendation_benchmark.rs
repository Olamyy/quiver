//! Criterion-based performance benchmarks for recommendation system
//!
//! This provides precise microbenchmarks using the Criterion framework
//! for more rigorous performance analysis.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use tokio::runtime::Runtime;
use std::time::Duration;

use quiver_benchmarks::inference::recommendation_system::{
    QuiverRecommendationService, JsonSerializationService, 
    RecommendationRequest, RequestContext
};

fn generate_test_request(id: u64) -> RecommendationRequest {
    RecommendationRequest {
        user_id: format!("user_{}", id % 10_000),
        num_recommendations: 10,
        context: RequestContext {
            device_type: match id % 3 {
                0 => "mobile".to_string(),
                1 => "desktop".to_string(),
                _ => "tablet".to_string(),
            },
            time_of_day: match id % 4 {
                0 => "morning".to_string(),
                1 => "afternoon".to_string(),
                2 => "evening".to_string(),
                _ => "night".to_string(),
            },
            location: Some("San Francisco".to_string()),
            session_length: (id % 60) as u32 + 1,
        },
    }
}

fn benchmark_quiver_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let service = QuiverRecommendationService::new();
    
    c.bench_function("quiver_single_request", |b| {
        b.to_async(&rt).iter(|| async {
            let request = generate_test_request(black_box(0));
            let result = service.get_features(&request).await;
            black_box(result)
        });
    });
}

fn benchmark_json_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let service = JsonSerializationService::new();
    
    c.bench_function("json_single_request", |b| {
        b.to_async(&rt).iter(|| async {
            let request = generate_test_request(black_box(0));
            let result = service.get_features(&request).await;
            black_box(result)
        });
    });
}

fn benchmark_throughput_comparison(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("throughput_comparison");
    
    let quiver_service = QuiverRecommendationService::new();
    let json_service = JsonSerializationService::new();
    
    for batch_size in [1, 10, 50, 100].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        
        group.bench_with_input(
            BenchmarkId::new("quiver", batch_size), 
            batch_size, 
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let mut handles = Vec::new();
                    for i in 0..size {
                        let request = generate_test_request(i as u64);
                        let service = &quiver_service;
                        handles.push(tokio::spawn(async move {
                            service.get_features(&request).await
                        }));
                    }
                    
                    for handle in handles {
                        black_box(handle.await.unwrap());
                    }
                });
            }
        );
        
        group.bench_with_input(
            BenchmarkId::new("json", batch_size), 
            batch_size, 
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let mut handles = Vec::new();
                    for i in 0..size {
                        let request = generate_test_request(i as u64);
                        let service = &json_service;
                        handles.push(tokio::spawn(async move {
                            service.get_features(&request).await
                        }));
                    }
                    
                    for handle in handles {
                        black_box(handle.await.unwrap());
                    }
                });
            }
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    benchmark_quiver_latency,
    benchmark_json_latency, 
    benchmark_throughput_comparison
);

criterion_main!(benches);