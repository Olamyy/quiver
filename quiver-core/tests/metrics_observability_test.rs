use quiver_core::fanout::metrics::{FanoutLatencies, Phase, Backend as MetricsBackend};
use quiver_core::metrics::MetricsStore;
use std::time::Duration;

/// Test metrics store creation and TTL configuration.
#[tokio::test]
async fn test_metrics_store_creation() {
    let store = MetricsStore::new();
    let mut latencies = FanoutLatencies::new();
    latencies.record_phase(Phase::Merge, 5.0);
    latencies.finalize();

    store
        .store(
            "test-123".to_string(),
            latencies.clone(),
            "user_features".to_string(),
            10,
        )
        .await;

    let retrieved = store.get("test-123").await;
    assert!(retrieved.is_some());
    let stored = retrieved.unwrap();
    assert_eq!(stored.feature_view, "user_features");
    assert_eq!(stored.entity_count, 10);
    assert_eq!(stored.latencies.merge_ms, 5.0);
}

/// Test that metrics store returns None for missing keys.
#[tokio::test]
async fn test_metrics_store_missing_key() {
    let store = MetricsStore::new();
    let retrieved = store.get("nonexistent").await;
    assert!(retrieved.is_none());
}

/// Test storing multiple metrics with different request IDs.
#[tokio::test]
async fn test_metrics_store_multiple_requests() {
    let store = MetricsStore::new();

    for i in 0..5 {
        let mut latencies = FanoutLatencies::new();
        latencies.record_phase(Phase::Merge, (i as f64) * 2.0);
        latencies.finalize();

        store
            .store(
                format!("req-{}", i),
                latencies,
                "view".to_string(),
                i as i32,
            )
            .await;
    }

    for i in 0..5 {
        let retrieved = store.get(&format!("req-{}", i)).await;
        assert!(retrieved.is_some());
        let stored = retrieved.unwrap();
        assert_eq!(stored.entity_count, i as i32);
        assert_eq!(stored.latencies.merge_ms, (i as f64) * 2.0);
    }
}

/// Test custom TTL configuration.
#[tokio::test]
async fn test_metrics_store_custom_ttl() {
    let store = MetricsStore::with_ttl(1, 100);
    let mut latencies = FanoutLatencies::new();
    latencies.record_phase(Phase::Merge, 5.0);
    latencies.finalize();

    store
        .store(
            "ttl-test".to_string(),
            latencies,
            "view".to_string(),
            1,
        )
        .await;

    let retrieved = store.get("ttl-test").await;
    assert!(retrieved.is_some());

    tokio::time::sleep(Duration::from_secs(2)).await;

    let expired = store.get("ttl-test").await;
    assert!(expired.is_none());
}

/// Test that latencies are correctly stored and retrieved.
#[tokio::test]
async fn test_metrics_store_latencies_roundtrip() {
    let store = MetricsStore::new();
    let mut latencies = FanoutLatencies::new();

    latencies.record_phase(Phase::RegistryLookup, 1.5);
    latencies.record_phase(Phase::Partition, 2.0);
    latencies.record_phase(Phase::Dispatch, 3.5);
    latencies.record_phase(Phase::Merge, 5.0);
    latencies.record_phase(Phase::Validation, 1.0);
    latencies.record_backend(MetricsBackend::Redis, 10.0);
    latencies.record_backend(MetricsBackend::Postgres, 15.0);
    latencies.finalize();

    store
        .store(
            "latencies-test".to_string(),
            latencies.clone(),
            "view".to_string(),
            5,
        )
        .await;

    let retrieved = store.get("latencies-test").await.unwrap();
    assert_eq!(retrieved.latencies.registry_lookup_ms, 1.5);
    assert_eq!(retrieved.latencies.partition_ms, 2.0);
    assert_eq!(retrieved.latencies.dispatch_ms, 3.5);
    assert_eq!(retrieved.latencies.merge_ms, 5.0);
    assert_eq!(retrieved.latencies.validation_ms, 1.0);
    assert_eq!(retrieved.latencies.backend_redis_ms, Some(10.0));
    assert_eq!(retrieved.latencies.backend_postgres_ms, Some(15.0));
    assert_eq!(retrieved.latencies.backend_max_ms, 15.0);
    assert!(retrieved.latencies.total_ms > 0.0);
}

/// Test that concurrent requests can store and retrieve metrics independently.
#[tokio::test]
async fn test_metrics_store_concurrent_access() {
    let store = std::sync::Arc::new(MetricsStore::new());

    let mut tasks = vec![];

    for i in 0..10 {
        let store_clone = store.clone();
        let task = tokio::spawn(async move {
            let mut latencies = FanoutLatencies::new();
            latencies.record_phase(Phase::Merge, i as f64);
            latencies.finalize();

            store_clone
                .store(
                    format!("concurrent-{}", i),
                    latencies,
                    "view".to_string(),
                    i as i32,
                )
                .await;

            let retrieved = store_clone.get(&format!("concurrent-{}", i)).await;
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().entity_count, i as i32);
        });
        tasks.push(task);
    }

    for task in tasks {
        task.await.unwrap();
    }
}

/// Test metrics store default initialization.
#[test]
fn test_metrics_store_default() {
    let store1 = MetricsStore::new();
    let store2 = MetricsStore::default();

    assert_eq!(std::mem::size_of_val(&store1), std::mem::size_of_val(&store2));
}
