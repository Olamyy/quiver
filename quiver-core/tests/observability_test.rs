/// Phase 3 observability and configuration tests.
///
/// Validates:
/// - Metrics instrumentation at all phases
/// - Ordering guarantees for different adapters
/// - Temporal routing with clock skew
/// - Fanout configuration validation

#[cfg(test)]
mod phase3_tests {
    use chrono::Duration;
    use quiver_core::adapters::{AdapterCapabilities, OrderingGuarantee, TemporalCapability};
    use quiver_core::config::Config;
    use quiver_core::fanout::metrics::{Backend as MetricsBackend, FanoutLatencies, Phase, Timer};
    use quiver_core::fanout::temporal::{TemporalCompatibility, TemporalRouter};
    use std::time::Duration as StdDuration;

    #[test]
    fn test_fanout_latencies_initialization() {
        let metrics = FanoutLatencies::new();
        assert_eq!(metrics.registry_lookup_ms, 0.0);
        assert_eq!(metrics.dispatch_ms, 0.0);
        assert_eq!(metrics.merge_ms, 0.0);
        assert_eq!(metrics.validation_ms, 0.0);
        assert_eq!(metrics.total_ms, 0.0);
        assert_eq!(metrics.critical_path_ms, 0.0);
    }

    #[test]
    fn test_metrics_recording_all_phases() {
        let mut metrics = FanoutLatencies::new();

        metrics.record_phase(Phase::RegistryLookup, 2.5);
        metrics.record_phase(Phase::Partition, 1.2);
        metrics.record_phase(Phase::Dispatch, 5.0);
        metrics.record_phase(Phase::Merge, 3.8);
        metrics.record_phase(Phase::Validation, 0.5);

        assert_eq!(metrics.registry_lookup_ms, 2.5);
        assert_eq!(metrics.partition_ms, 1.2);
        assert_eq!(metrics.dispatch_ms, 5.0);
        assert_eq!(metrics.merge_ms, 3.8);
        assert_eq!(metrics.validation_ms, 0.5);
    }

    #[test]
    fn test_metrics_per_backend_recording() {
        let mut metrics = FanoutLatencies::new();

        metrics.record_backend(MetricsBackend::Redis, 15.0);
        metrics.record_backend(MetricsBackend::Postgres, 25.0);

        assert_eq!(metrics.backend_redis_ms, Some(15.0));
        assert_eq!(metrics.backend_postgres_ms, Some(25.0));
        assert_eq!(metrics.backend_max_ms, 25.0);
    }

    #[test]
    fn test_metrics_finalize_calculates_totals() {
        let mut metrics = FanoutLatencies::new();

        metrics.record_phase(Phase::RegistryLookup, 1.0);
        metrics.record_phase(Phase::Partition, 2.0);
        metrics.record_phase(Phase::Dispatch, 3.0);
        metrics.record_phase(Phase::Merge, 4.0);
        metrics.record_phase(Phase::Validation, 5.0);
        metrics.record_backend(MetricsBackend::Redis, 10.0);

        metrics.finalize();

        assert_eq!(metrics.total_ms, 25.0);
        assert_eq!(metrics.critical_path_ms, 25.0);
    }

    #[test]
    fn test_timer_measures_elapsed_time() {
        let timer = Timer::start();
        std::thread::sleep(StdDuration::from_millis(10));
        let elapsed = timer.stop();

        assert!(elapsed >= 8.0);
        assert!(elapsed < 100.0);
    }

    #[test]
    fn test_ordering_guarantee_memory_adapter() {
        let caps = AdapterCapabilities {
            temporal: TemporalCapability::TimeTravel {
                typical_latency_ms: 1,
            },
            max_batch_size: Some(10_000),
            optimal_batch_size: Some(1_000),
            typical_latency_ms: 1,
            supports_parallel_requests: true,
            ordering_guarantee: OrderingGuarantee::OrderedByRequest,
        };

        assert_eq!(caps.ordering_guarantee, OrderingGuarantee::OrderedByRequest);
    }

    #[test]
    fn test_ordering_guarantee_redis_adapter() {
        let caps = AdapterCapabilities {
            temporal: TemporalCapability::CurrentOnly {
                typical_latency_ms: 5,
            },
            max_batch_size: Some(5_000),
            optimal_batch_size: Some(100),
            typical_latency_ms: 5,
            supports_parallel_requests: true,
            ordering_guarantee: OrderingGuarantee::Unordered,
        };

        assert_eq!(caps.ordering_guarantee, OrderingGuarantee::Unordered);
    }

    #[test]
    fn test_temporal_router_safe_margin() {
        let router = TemporalRouter::new();
        let now = chrono::Utc::now();
        let as_of = now - Duration::minutes(30);

        let result = router.check_compatibility(Some(as_of), now, Some(3600));
        assert_eq!(result, TemporalCompatibility::Safe);
    }

    #[test]
    fn test_temporal_router_at_risk_boundary() {
        let router = TemporalRouter::new();
        let now = chrono::Utc::now();
        let as_of = now - Duration::seconds(3585); // 59m 45s

        let result = router.check_compatibility(Some(as_of), now, Some(3600));
        assert_eq!(result, TemporalCompatibility::AtRisk);
    }

    #[test]
    fn test_temporal_router_unavailable() {
        let router = TemporalRouter::new();
        let now = chrono::Utc::now();
        let as_of = now - Duration::seconds(4000); // Beyond TTL

        let result = router.check_compatibility(Some(as_of), now, Some(3600));
        assert_eq!(result, TemporalCompatibility::Unavailable);
    }

    #[test]
    fn test_fanout_config_validation_enabled() {
        let config_yaml = r#"
server:
  host: localhost
  port: 8815
  fanout:
    enabled: true
    max_concurrent_backends: 10
    partial_failure_strategy: null_fill

registry:
  type: static
  views: []

adapters: {}
"#;

        let config: Config = serde_yaml::from_str(config_yaml).expect("valid config");
        let result = config.validate();

        assert!(result.is_ok(), "Config with fanout enabled should be valid");
    }

    #[test]
    fn test_fanout_config_validation_disabled() {
        let config_yaml = r#"
server:
  host: localhost
  port: 8815
  fanout:
    enabled: false
    max_concurrent_backends: 10
    partial_failure_strategy: null_fill

registry:
  type: static
  views: []

adapters: {}
"#;

        let config: Config = serde_yaml::from_str(config_yaml).expect("valid config");
        let result = config.validate();

        assert!(
            result.is_ok(),
            "Config with fanout disabled should be valid"
        );
    }

    #[test]
    fn test_fanout_config_validation_invalid_strategy() {
        let config_yaml = r#"
server:
  host: localhost
  port: 8815
  fanout:
    enabled: true
    max_concurrent_backends: 10
    partial_failure_strategy: invalid_strategy

registry:
  type: static
  views: []

adapters: {}
"#;

        let config: Config = serde_yaml::from_str(config_yaml).expect("valid config");
        let result = config.validate();

        assert!(
            result.is_err(),
            "Config with invalid partial_failure_strategy should fail"
        );
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("partial_failure_strategy")),
            "Error should mention invalid strategy"
        );
    }

    #[test]
    fn test_fanout_config_validation_zero_backends() {
        let config_yaml = r#"
server:
  host: localhost
  port: 8815
  fanout:
    enabled: true
    max_concurrent_backends: 0
    partial_failure_strategy: null_fill

registry:
  type: static
  views: []

adapters: {}
"#;

        let config: Config = serde_yaml::from_str(config_yaml).expect("valid config");
        let result = config.validate();

        assert!(
            result.is_err(),
            "Config with zero max_concurrent_backends should fail when fanout enabled"
        );
        let errors = result.unwrap_err();
        assert!(
            errors.iter().any(|e| e.contains("max_concurrent_backends")),
            "Error should mention max_concurrent_backends"
        );
    }

    #[test]
    fn test_fanout_config_valid_strategies() {
        for strategy in &["null_fill", "error", "forward_fill"] {
            let config_yaml = format!(
                r#"
server:
  host: localhost
  port: 8815
  fanout:
    enabled: true
    max_concurrent_backends: 10
    partial_failure_strategy: {}

registry:
  type: static
  views: []

adapters: {{}}
"#,
                strategy
            );

            let config: Config = serde_yaml::from_str(&config_yaml).expect("valid config");
            let result = config.validate();

            assert!(
                result.is_ok(),
                "Config with valid strategy '{}' should pass",
                strategy
            );
        }
    }

    #[test]
    fn test_fanout_config_defaults() {
        let config_yaml = r#"
server:
  host: localhost
  port: 8815

registry:
  type: static
  views: []

adapters: {}
"#;

        let config: Config = serde_yaml::from_str(config_yaml).expect("valid config");

        assert!(
            config.server.fanout.enabled,
            "Fanout enabled should default to true, got: {}",
            config.server.fanout.enabled
        );
        assert_eq!(
            config.server.fanout.max_concurrent_backends, 10,
            "Default max_concurrent_backends is 10, got: {}",
            config.server.fanout.max_concurrent_backends
        );
        assert_eq!(
            config.server.fanout.partial_failure_strategy, "null_fill",
            "Default strategy is null_fill, got: {}",
            config.server.fanout.partial_failure_strategy
        );
    }

    #[test]
    fn test_backend_priority_from_temporal_compatibility() {
        let router = TemporalRouter::new();

        let safe_priority = router.backend_priority(TemporalCompatibility::Safe);
        assert_eq!(safe_priority, Some(0), "Safe backends get priority 0");

        let at_risk_priority = router.backend_priority(TemporalCompatibility::AtRisk);
        assert_eq!(at_risk_priority, Some(1), "AtRisk backends get priority 1");

        let unavailable_priority = router.backend_priority(TemporalCompatibility::Unavailable);
        assert_eq!(
            unavailable_priority, None,
            "Unavailable backends are skipped"
        );
    }

    #[test]
    fn test_downtime_strategy_fail_default() {
        let config_yaml = r#"
server:
  host: localhost
  port: 8815

registry:
  type: static
  views: []

adapters: {}
"#;

        let config: Config = serde_yaml::from_str(config_yaml).expect("valid config");
        assert_eq!(
            config.server.fanout.downtime_strategy,
            quiver_core::config::DowntimeStrategy::Fail,
            "Default downtime_strategy is Fail"
        );
    }

    #[test]
    fn test_downtime_strategy_explicit_configuration() {
        let config_yaml = r#"
server:
  host: localhost
  port: 8815
  fanout:
    downtime_strategy: return_available

registry:
  type: static
  views: []

adapters: {}
"#;

        let config: Config = serde_yaml::from_str(config_yaml).expect("valid config");
        assert_eq!(
            config.server.fanout.downtime_strategy,
            quiver_core::config::DowntimeStrategy::ReturnAvailable,
            "downtime_strategy can be set to return_available"
        );
    }

    #[test]
    fn test_fallback_source_optional_with_fail_strategy() {
        let config_yaml = r#"
server:
  host: localhost
  port: 8815
  fanout:
    downtime_strategy: fail

registry:
  type: static
  views:
    - name: test_view
      entity_type: user
      entity_key: id
      columns:
        - name: score
          arrow_type: float64
          source: primary_adapter

adapters:
  primary_adapter:
    type: memory
"#;

        let config: Config = serde_yaml::from_str(config_yaml).expect("valid config");
        let result = config.validate();

        assert!(
            result.is_ok(),
            "fallback_source is optional when downtime_strategy=fail"
        );
    }

    #[test]
    fn test_fallback_source_required_with_use_fallback_strategy() {
        let config_yaml = r#"
server:
  host: localhost
  port: 8815
  fanout:
    downtime_strategy: use_fallback

registry:
  type: static
  views:
    - name: test_view
      entity_type: user
      entity_key: id
      columns:
        - name: score
          arrow_type: float64
          source: primary_adapter

adapters:
  primary_adapter:
    type: memory
  fallback_adapter:
    type: memory
"#;

        let config: Config = serde_yaml::from_str(config_yaml).expect("valid config");
        let result = config.validate();

        assert!(
            result.is_err(),
            "fallback_source is required when downtime_strategy=use_fallback"
        );
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("fallback_source") && e.contains("use_fallback")),
            "Error should mention fallback_source requirement"
        );
    }

    #[test]
    fn test_fallback_source_valid_with_use_fallback_strategy() {
        let config_yaml = r#"
server:
  host: localhost
  port: 8815
  fanout:
    downtime_strategy: use_fallback

registry:
  type: static
  views:
    - name: test_view
      entity_type: user
      entity_key: id
      columns:
        - name: score
          arrow_type: float64
          source: primary_adapter
          fallback_source: fallback_adapter

adapters:
  primary_adapter:
    type: memory
  fallback_adapter:
    type: memory
"#;

        let config: Config = serde_yaml::from_str(config_yaml).expect("valid config");
        let result = config.validate();

        assert!(
            result.is_ok(),
            "Config is valid when fallback_source is provided with use_fallback strategy"
        );
    }
}
