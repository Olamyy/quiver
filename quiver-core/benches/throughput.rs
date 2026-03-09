use arrow_flight::Ticket;
/// Comprehensive benchmark suite for Quiver feature serving performance
///
/// Scenarios tested:
/// 1. Redis baseline - Single Redis backend reference
/// 2. PostgreSQL baseline - Single PostgreSQL backend reference
/// 3. Fanout 2x (Redis + PostgreSQL) - Parallel execution efficiency
/// 4. Fanout 3x (all adapters) - Full multi-backend scaling
/// 5. Batch scaling - Throughput across entity counts (1 to 10k)
///
/// Prerequisites: Backends running + Quiver server pre-spawned
/// Terminal 1: docker-compose up -d && uv run ingest.py postgres && uv run ingest.py redis
/// Terminal 2: cd quiver-core && cargo run --release -- --config ../examples/config/benchmark/redis-baseline.yaml
/// Terminal 3: make bench
use arrow_flight::flight_service_client::FlightServiceClient;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::StreamExt;
use prost::Message;
use quiver_core::proto::quiver::v1::{EntityKey, FeatureRequest};
use std::time::Duration;
use tonic::transport::Channel;

mod fixtures;

/// Real gRPC client for benchmarks. Measures latency of feature requests to Quiver Flight Service.
struct QuiverBenchClient {
    port: u16,
}

impl QuiverBenchClient {
    fn new(port: u16) -> Self {
        Self { port }
    }

    /// Measures latency of a feature request to a single backend via real gRPC.
    /// Constructs a FeatureRequest, sends it via FlightServiceClient, and consumes the Arrow response.
    async fn request_single_backend(
        &self,
        feature_view: &str,
        entities: &[String],
        feature_names: &[String],
        entity_type: &str,
    ) -> Result<Duration, Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();

        let channel = Channel::from_shared(format!("http://127.0.0.1:{}", self.port))?
            .connect()
            .await?;
        let mut client = FlightServiceClient::new(channel);

        let feature_request = FeatureRequest {
            feature_view: feature_view.to_string(),
            feature_names: feature_names.to_vec(),
            entities: entities
                .iter()
                .map(|e| EntityKey {
                    entity_type: entity_type.to_string(),
                    entity_id: e.clone(),
                })
                .collect(),
            as_of: None,
            freshness: None,
            context: None,
            output: None,
        };

        eprintln!(
            "[BENCH_REQ] feature_view={} entity_type={} entities={} features={}",
            feature_view,
            entity_type,
            entities.len(),
            feature_names.len()
        );

        let ticket = Ticket {
            ticket: feature_request.encode_to_vec().into(),
        };

        let response = client.do_get(ticket).await?;
        let stream = response.into_inner();

        let mut reader =
            arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(stream.map(
                |res| res.map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e))),
            ));

        let mut batch_count = 0;
        let mut total_rows = 0;
        while let Some(batch) = reader.next().await {
            batch_count += 1;
            if let Ok(batch) = batch {
                total_rows += batch.num_rows();
            }
        }

        let elapsed = start.elapsed();
        eprintln!(
            "[BENCH_RES] batches={} rows={} elapsed={:.2?}",
            batch_count, total_rows, elapsed
        );

        Ok(elapsed)
    }

    /// Measures latency of a fanout request across multiple backends via real gRPC.
    async fn request_fanout(
        &self,
        feature_view: &str,
        entities: &[String],
        feature_names: &[String],
        entity_type: &str,
    ) -> Result<Duration, Box<dyn std::error::Error>> {
        eprintln!(
            "[BENCH_FANOUT] Executing fanout request to {}",
            feature_view
        );
        self.request_single_backend(feature_view, entities, feature_names, entity_type)
            .await
    }
}

/// Scenario 1: Redis baseline (single real-time backend reference)
/// Single Redis backend establishes lowest latency baseline for comparison.
fn bench_redis_baseline(c: &mut Criterion) {
    eprintln!("[BENCHMARK] Starting redis_baseline scenario");
    eprintln!("[BENCHMARK] Connecting to server at 127.0.0.1:8815");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = QuiverBenchClient::new(8815);
    let entities: Vec<String> = (0..100).map(|i| format!("session:{}", 1000 + i)).collect();
    let entities = black_box(entities);
    let feature_names = vec![
        "active_user_id".to_string(),
        "request_count".to_string(),
        "last_activity".to_string(),
    ];

    let mut group = c.benchmark_group("scenario_1_redis_baseline");
    group.sample_size(50);
    group.throughput(Throughput::Elements(entities.len() as u64));

    group.bench_function("redis_baseline_100_entities", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .request_single_backend("sessions_view", &entities, &feature_names, "session")
                .await
        });
    });

    group.finish();
}

/// Scenario 2: PostgreSQL baseline (single historical backend reference)
/// Single PostgreSQL backend establishes database latency baseline for comparison.
fn bench_postgres_baseline(c: &mut Criterion) {
    eprintln!("[BENCHMARK] Starting postgres_baseline scenario");
    eprintln!("[BENCHMARK] Connecting to server at 127.0.0.1:8815");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = QuiverBenchClient::new(8815);
    let entities: Vec<String> = (0..100).map(|i| format!("user:{}", 1000 + i)).collect();
    let entities = black_box(entities);
    let feature_names = vec!["spend_30d".to_string(), "purchase_count".to_string()];

    let mut group = c.benchmark_group("scenario_2_postgres_baseline");
    group.sample_size(50);
    group.throughput(Throughput::Elements(entities.len() as u64));

    group.bench_function("postgres_baseline_100_entities", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .request_single_backend("ecommerce_features", &entities, &feature_names, "user")
                .await
        });
    });

    group.finish();
}

/// Scenario 3: Fanout 2x (Redis + PostgreSQL)
/// Parallel execution across two backends. Should be ~max(redis, postgres), not sum.
/// Validates parallelism: latency should be 2.5-3x single backend (not 2x).
fn bench_fanout_2x(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = QuiverBenchClient::new(8815);
    let entities: Vec<String> = (0..100).map(|i| format!("user:{}", 1000 + i)).collect();
    let entities = black_box(entities);
    let feature_names = vec![
        "active_user_id".to_string(),
        "request_count".to_string(),
        "spend_30d".to_string(),
        "purchase_count".to_string(),
    ];

    let mut group = c.benchmark_group("scenario_3_fanout_2x");
    group.sample_size(50);
    group.throughput(Throughput::Elements(entities.len() as u64));

    group.bench_function("fanout_2x_100_entities", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .request_fanout("hybrid_features", &entities, &feature_names, "user")
                .await
        });
    });

    group.finish();
}

/// Scenario 4: Fanout 3x (Redis + PostgreSQL ecommerce + PostgreSQL timeseries)
/// Full multi-backend coordination validates scaling across three adapters.
/// Should be ~4-5x single backend (demonstrates efficient merging with 3 sources).
fn bench_fanout_3x(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = QuiverBenchClient::new(8815);
    let entities: Vec<String> = (0..100).map(|i| format!("user:{}", 1000 + i)).collect();
    let entities = black_box(entities);
    let feature_names = vec![
        "active_user_id".to_string(),
        "spend_30d".to_string(),
        "temperature".to_string(),
    ];

    let mut group = c.benchmark_group("scenario_4_fanout_3x");
    group.sample_size(50);
    group.throughput(Throughput::Elements(entities.len() as u64));

    group.bench_function("fanout_3x_100_entities", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .request_fanout("extended_features", &entities, &feature_names, "user")
                .await
        });
    });

    group.finish();
}

/// Scenario 5: Batch scaling (fanout 2x with varying entity counts)
/// Tests throughput consistency and per-entity cost across batch sizes (1 to 10k).
/// Validates vectorization efficiency: per-entity latency should remain relatively constant.
fn bench_batch_scaling(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = QuiverBenchClient::new(8815);
    let batch_sizes = fixtures::parse_batch_sizes();
    let feature_names = vec!["active_user_id".to_string(), "spend_30d".to_string()];

    let mut group = c.benchmark_group("scenario_5_batch_scaling");
    group.sample_size(30);

    for size in &batch_sizes {
        let entities: Vec<String> = (0..*size).map(|i| format!("user:{}", 1000 + i)).collect();
        let entities = black_box(entities);
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.to_async(&rt).iter(|| async {
                client
                    .request_fanout("hybrid_features", &entities, &feature_names, "user")
                    .await
            });
        });
    }

    group.finish();
}

criterion_group!(scenario_1, bench_redis_baseline);
criterion_group!(scenario_2, bench_postgres_baseline);
criterion_group!(scenario_3, bench_fanout_2x);
criterion_group!(scenario_4, bench_fanout_3x);
criterion_group!(scenario_5, bench_batch_scaling);

criterion_main!(scenario_1, scenario_2, scenario_3, scenario_4, scenario_5);
