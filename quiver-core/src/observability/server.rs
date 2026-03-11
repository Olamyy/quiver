use crate::metrics::MetricsStore;
use crate::proto::quiver::v1::{
    FanoutMetricsResponse, FlushMetricsStoreRequest, FlushMetricsStoreResponse, GetMetricsRequest,
    GetMetricsResponse,
};
use prost_types::Timestamp;
use std::sync::Arc;
use std::time::SystemTime;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

pub struct ObservabilityServer {
    metrics_store: Arc<MetricsStore>,
}

impl ObservabilityServer {
    pub fn new(metrics_store: Arc<MetricsStore>) -> Self {
        Self { metrics_store }
    }
}

#[tonic::async_trait]
impl crate::proto::quiver::v1::observability_service_server::ObservabilityService
    for ObservabilityServer
{
    async fn get_metrics(
        &self,
        request: Request<GetMetricsRequest>,
    ) -> Result<Response<GetMetricsResponse>, Status> {
        let req = request.into_inner();
        let request_id = &req.request_id;

        debug!("Retrieving metrics for request_id: {}", request_id);

        let stored = self.metrics_store.get(request_id).await.ok_or_else(|| {
            Status::not_found(format!("Metrics not found for request_id: {}", request_id))
        })?;

        let metrics_proto = FanoutMetricsResponse {
            registry_lookup_ms: stored.latencies.registry_lookup_ms,
            cache_lookup_ms: stored.latencies.cache_lookup_ms,
            partition_ms: stored.latencies.partition_ms,
            dispatch_ms: stored.latencies.dispatch_ms,
            backend_redis_ms: stored.latencies.backend_redis_ms.unwrap_or(0.0),
            backend_delta_ms: stored.latencies.backend_delta_ms.unwrap_or(0.0),
            backend_postgres_ms: stored.latencies.backend_postgres_ms.unwrap_or(0.0),
            backend_max_ms: stored.latencies.backend_max_ms,
            alignment_ms: stored.latencies.alignment_ms,
            merge_ms: stored.latencies.merge_ms,
            validation_ms: stored.latencies.validation_ms,
            serialization_ms: stored.latencies.serialization_ms,
            total_ms: stored.latencies.total_ms,
            critical_path_ms: stored.latencies.critical_path_ms,
        };

        let duration = stored
            .stored_at
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let timestamp = Timestamp {
            seconds: duration.as_secs() as i64,
            nanos: duration.subsec_nanos() as i32,
        };

        let response = GetMetricsResponse {
            metrics: Some(metrics_proto),
            request_timestamp: Some(timestamp),
            feature_view: stored.feature_view,
            entity_count: stored.entity_count,
        };

        Ok(Response::new(response))
    }

    async fn flush_metrics_store(
        &self,
        _request: Request<FlushMetricsStoreRequest>,
    ) -> Result<Response<FlushMetricsStoreResponse>, Status> {
        warn!("ADMIN: Flushing metrics store (benchmarking operation)");
        let entries_cleared = self.metrics_store.flush().await;
        Ok(Response::new(FlushMetricsStoreResponse { entries_cleared }))
    }
}
