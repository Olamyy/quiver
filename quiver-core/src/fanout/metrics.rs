use std::time::Instant;

/// Fanout latency metrics tracking 11 instrumentation points.
///
/// Captures latency at each phase of the fanout pipeline:
/// 1. Registry lookup
/// 2. Cache lookup
/// 3. Partition (split hits/misses)
/// 4. Dispatch (submit to backends)
/// 5-7. Backend execution (parallel)
/// 8. Alignment (entity position index)
/// 9. Merge (assemble columns)
/// 10. Validation (verify completeness)
/// 11. Serialization (convert to Arrow IPC)
#[derive(Debug, Clone)]
pub struct FanoutLatencies {
    /// Phase 1: Registry lookup (find which backend has feature)
    pub registry_lookup_ms: f64,

    /// Phase 2: Cache lookup (check if cached)
    pub cache_lookup_ms: f64,

    /// Phase 3: Partition (split hits/misses by feature)
    pub partition_ms: f64,

    /// Phase 4: Dispatch (submit requests to backends)
    pub dispatch_ms: f64,

    /// Phase 5: Backend execution - Redis latency
    pub backend_redis_ms: Option<f64>,

    /// Phase 6: Backend execution - Delta/secondary latency
    pub backend_delta_ms: Option<f64>,

    /// Phase 7: Backend execution - PostgreSQL latency
    pub backend_postgres_ms: Option<f64>,

    /// Maximum latency across all backends (parallel phase)
    pub backend_max_ms: f64,

    /// Phase 8: Alignment (build entity position index)
    pub alignment_ms: f64,

    /// Phase 9: Merge (assemble columns)
    pub merge_ms: f64,

    /// Phase 10: Validation (verify completeness)
    pub validation_ms: f64,

    /// Phase 11: Serialization (convert to Arrow IPC)
    pub serialization_ms: f64,

    /// Total request latency
    pub total_ms: f64,

    /// Critical path latency (max of sequential phases)
    pub critical_path_ms: f64,
}

impl FanoutLatencies {
    /// Create a new latencies tracker with all zeros.
    pub fn new() -> Self {
        Self {
            registry_lookup_ms: 0.0,
            cache_lookup_ms: 0.0,
            partition_ms: 0.0,
            dispatch_ms: 0.0,
            backend_redis_ms: None,
            backend_delta_ms: None,
            backend_postgres_ms: None,
            backend_max_ms: 0.0,
            alignment_ms: 0.0,
            merge_ms: 0.0,
            validation_ms: 0.0,
            serialization_ms: 0.0,
            total_ms: 0.0,
            critical_path_ms: 0.0,
        }
    }

    /// Record latency for a specific phase.
    pub fn record_phase(&mut self, phase: Phase, duration_ms: f64) {
        match phase {
            Phase::RegistryLookup => self.registry_lookup_ms = duration_ms,
            Phase::CacheLookup => self.cache_lookup_ms = duration_ms,
            Phase::Partition => self.partition_ms = duration_ms,
            Phase::Dispatch => self.dispatch_ms = duration_ms,
            Phase::Alignment => self.alignment_ms = duration_ms,
            Phase::Merge => self.merge_ms = duration_ms,
            Phase::Validation => self.validation_ms = duration_ms,
            Phase::Serialization => self.serialization_ms = duration_ms,
        }
    }

    /// Record backend latency.
    pub fn record_backend(&mut self, backend: Backend, duration_ms: f64) {
        match backend {
            Backend::Redis => self.backend_redis_ms = Some(duration_ms),
            Backend::Delta => self.backend_delta_ms = Some(duration_ms),
            Backend::Postgres => self.backend_postgres_ms = Some(duration_ms),
            Backend::Custom(_) => {} // Ignore custom backends for now
        }

        // Update max backend latency
        let backends = vec![
            self.backend_redis_ms,
            self.backend_delta_ms,
            self.backend_postgres_ms,
        ];
        self.backend_max_ms = backends
            .iter()
            .filter_map(|&opt| opt)
            .fold(0.0, f64::max);
    }

    /// Finalize metrics: calculate totals and critical path.
    pub fn finalize(&mut self) {
        // Total is sum of all phases
        self.total_ms = self.registry_lookup_ms
            + self.cache_lookup_ms
            + self.partition_ms
            + self.dispatch_ms
            + self.backend_max_ms
            + self.alignment_ms
            + self.merge_ms
            + self.validation_ms
            + self.serialization_ms;

        // Critical path is max of sequential phases
        // Sequential: registry → cache → partition → dispatch → backend(parallel) → alignment → merge → validation → serialization
        self.critical_path_ms = [
            self.registry_lookup_ms,
            self.cache_lookup_ms,
            self.partition_ms,
            self.dispatch_ms,
            self.backend_max_ms,
            self.alignment_ms,
            self.merge_ms,
            self.validation_ms,
            self.serialization_ms,
        ]
        .iter()
        .fold(0.0, |a, &b| a + b);
    }

    /// Get latency for a specific phase.
    pub fn get_phase_latency(&self, phase: &Phase) -> f64 {
        match phase {
            Phase::RegistryLookup => self.registry_lookup_ms,
            Phase::CacheLookup => self.cache_lookup_ms,
            Phase::Partition => self.partition_ms,
            Phase::Dispatch => self.dispatch_ms,
            Phase::Alignment => self.alignment_ms,
            Phase::Merge => self.merge_ms,
            Phase::Validation => self.validation_ms,
            Phase::Serialization => self.serialization_ms,
        }
    }

    /// Get backend latency.
    pub fn get_backend_latency(&self, backend: &Backend) -> Option<f64> {
        match backend {
            Backend::Redis => self.backend_redis_ms,
            Backend::Delta => self.backend_delta_ms,
            Backend::Postgres => self.backend_postgres_ms,
            Backend::Custom(_) => None,
        }
    }
}

impl Default for FanoutLatencies {
    fn default() -> Self {
        Self::new()
    }
}

/// Fanout instrumentation phases per RFC v0.3 section 5.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Phase 1: Registry lookup (find which backend has feature)
    RegistryLookup,
    /// Phase 2: Cache lookup (check if cached)
    CacheLookup,
    /// Phase 3: Partition (split hits/misses by feature)
    Partition,
    /// Phase 4: Dispatch (submit requests to backends)
    Dispatch,
    /// Phase 8: Alignment (build entity position index)
    Alignment,
    /// Phase 9: Merge (assemble columns)
    Merge,
    /// Phase 10: Validation (verify completeness)
    Validation,
    /// Phase 11: Serialization (convert to Arrow IPC)
    Serialization,
}

/// Backend identifiers for metrics.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Backend {
    /// Redis real-time features
    Redis,
    /// Delta/secondary backend
    Delta,
    /// PostgreSQL historical data
    Postgres,
    /// Custom/unknown backend
    Custom(String),
}

impl Backend {
    /// Create backend from string identifier.
    pub fn from_name(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "redis" => Backend::Redis,
            "delta" => Backend::Delta,
            "postgres" | "postgresql" => Backend::Postgres,
            other => Backend::Custom(other.to_string()),
        }
    }

    /// Get backend name.
    pub fn name(&self) -> &str {
        match self {
            Backend::Redis => "redis",
            Backend::Delta => "delta",
            Backend::Postgres => "postgres",
            Backend::Custom(name) => name,
        }
    }
}

/// Simple timer for measuring phase latency.
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// Create a new timer that starts immediately.
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get elapsed time in milliseconds.
    pub fn elapsed_ms(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }

    /// Stop timer and return elapsed time in milliseconds.
    pub fn stop(self) -> f64 {
        self.elapsed_ms()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latencies_new() {
        let latencies = FanoutLatencies::new();
        assert_eq!(latencies.registry_lookup_ms, 0.0);
        assert_eq!(latencies.total_ms, 0.0);
    }

    #[test]
    fn test_latencies_record_phase() {
        let mut latencies = FanoutLatencies::new();
        latencies.record_phase(Phase::Merge, 5.0);
        assert_eq!(latencies.merge_ms, 5.0);
    }

    #[test]
    fn test_latencies_record_backend() {
        let mut latencies = FanoutLatencies::new();
        latencies.record_backend(Backend::Redis, 10.0);
        assert_eq!(latencies.backend_redis_ms, Some(10.0));
        assert_eq!(latencies.backend_max_ms, 10.0);
    }

    #[test]
    fn test_latencies_multiple_backends() {
        let mut latencies = FanoutLatencies::new();
        latencies.record_backend(Backend::Redis, 5.0);
        latencies.record_backend(Backend::Postgres, 15.0);
        assert_eq!(latencies.backend_max_ms, 15.0);
    }

    #[test]
    fn test_latencies_finalize() {
        let mut latencies = FanoutLatencies::new();
        latencies.record_phase(Phase::RegistryLookup, 2.0);
        latencies.record_phase(Phase::Merge, 5.0);
        latencies.record_phase(Phase::Validation, 1.0);
        latencies.record_backend(Backend::Redis, 10.0);
        latencies.finalize();

        assert!(latencies.total_ms > 0.0);
        assert!(latencies.critical_path_ms > 0.0);
    }

    #[test]
    fn test_backend_from_name() {
        assert_eq!(Backend::from_name("redis"), Backend::Redis);
        assert_eq!(Backend::from_name("postgres"), Backend::Postgres);
        assert_eq!(Backend::from_name("delta"), Backend::Delta);
    }

    #[test]
    fn test_timer() {
        let timer = Timer::start();
        let elapsed = timer.stop();
        assert!(elapsed >= 0.0);
    }
}
