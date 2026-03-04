use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::FlightService,
};
use futures::Stream;
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, debug};
use std::time::Instant;
use serde_json::json;

use crate::proto::quiver::v1::FeatureRequest;
use crate::resolver::Resolver;
use crate::config::AccessLogConfig;
use arrow_flight::encode::FlightDataEncoderBuilder;
use chrono::DateTime;
use futures::StreamExt;
use prost::Message;
use std::sync::Arc;

pub struct QuiverFlightServer {
    resolver: Arc<Resolver>,
    access_log_config: Option<AccessLogConfig>,
}

impl QuiverFlightServer {
    pub fn new(resolver: Arc<Resolver>, access_log_config: Option<AccessLogConfig>) -> Self {
        Self { 
            resolver,
            access_log_config,
        }
    }
    
    fn log_request(&self, endpoint: &str, feature_view: Option<&str>, entity_count: Option<usize>, 
                   feature_count: Option<usize>, duration: std::time::Duration, status: &str, error: Option<&str>) {
        if let Some(config) = &self.access_log_config {
            if !config.enabled {
                return;
            }
            
            match config.format.as_str() {
                "json" => {
                    let mut log_entry = json!({
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "endpoint": endpoint,
                        "duration_ms": duration.as_millis(),
                        "status": status
                    });
                    
                    if let Some(view) = feature_view {
                        log_entry["feature_view"] = json!(view);
                    }
                    if let Some(count) = entity_count {
                        log_entry["entity_count"] = json!(count);
                    }
                    if let Some(count) = feature_count {
                        log_entry["feature_count"] = json!(count);
                    }
                    if let Some(err) = error {
                        log_entry["error"] = json!(err);
                    }
                    
                    info!(target: "access", "{}", log_entry);
                }
                _ => {
                    let view_info = feature_view.map(|v| format!(" view={}", v)).unwrap_or_default();
                    let entity_info = entity_count.map(|c| format!(" entities={}", c)).unwrap_or_default();
                    let feature_info = feature_count.map(|c| format!(" features={}", c)).unwrap_or_default();
                    let error_info = error.map(|e| format!(" error={}", e)).unwrap_or_default();
                    
                    info!(target: "access", "{} {}ms{}{}{}{}", 
                          endpoint, duration.as_millis(), view_info, entity_info, feature_info, error_info);
                }
            }
        }
    }
}

#[tonic::async_trait]
impl FlightService for QuiverFlightServer {
    type HandshakeStream = Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let response = HandshakeResponse {
            payload: "authenticated".into(),
            protocol_version: 0,
        };
        let stream = futures::stream::once(async move { Ok(response) });
        Ok(Response::new(Box::pin(stream)))
    }
    type ListFlightsStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send>>;
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let views = self
            .resolver
            .list_views()
            .await
            .map_err(|e| Status::internal(format!("Failed to list views: {}", e)))?;

        let flights = views.into_iter().map(|name| FlightInfo {
            schema: prost::bytes::Bytes::new(),
            flight_descriptor: Some(FlightDescriptor {
                r#type: arrow_flight::flight_descriptor::DescriptorType::Path as i32,
                path: vec![name],
                cmd: prost::bytes::Bytes::new(),
            }),
            endpoint: vec![],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: vec![].into(),
        });

        let stream = futures::stream::iter(flights.map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let view_name = descriptor.path.first().ok_or_else(|| {
            Status::invalid_argument("FlightDescriptor must contain a path for the feature view")
        })?;

        let metadata = self
            .resolver
            .get_view_metadata(view_name)
            .await
            .map_err(|e| Status::internal(format!("Failed to resolve feature view: {}", e)))?;

        let schema = self
            .resolver
            .get_arrow_schema(view_name)
            .await
            .map_err(|e| Status::internal(format!("Failed to resolve schema: {}", e)))?;

        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema_result: SchemaResult = arrow_flight::SchemaAsIpc::new(&schema, &options)
            .try_into()
            .map_err(|e| Status::internal(format!("Failed to serialize schema: {}", e)))?;

        let endpoint = arrow_flight::FlightEndpoint {
            ticket: Some(Ticket {
                ticket: descriptor.cmd.clone(),
            }),
            location: vec![],
            expiration_time: None,
            app_metadata: vec![].into(),
        };

        let flight_info = FlightInfo {
            schema: schema_result.schema,
            flight_descriptor: Some(descriptor),
            endpoint: vec![endpoint],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: format!(
                "entity_type:{};schema_version:{};backends:{}",
                metadata.entity_type,
                metadata.schema_version,
                metadata
                    .backend_routing
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k, v))
                    .collect::<Vec<_>>()
                    .join(",")
            )
            .into_bytes()
            .into(),
        };

        Ok(Response::new(flight_info))
    }
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let view_name = descriptor.path.first().ok_or_else(|| {
            Status::invalid_argument("FlightDescriptor must contain a path for the feature view")
        })?;

        let schema = self
            .resolver
            .get_arrow_schema(view_name)
            .await
            .map_err(|e| Status::internal(format!("Failed to resolve schema: {}", e)))?;

        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema_result: SchemaResult = arrow_flight::SchemaAsIpc::new(&schema, &options)
            .try_into()
            .map_err(|e| Status::internal(format!("Failed to serialize schema: {}", e)))?;

        Ok(Response::new(schema_result))
    }

    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let start_time = Instant::now();
        let ticket = request.into_inner();
        
        let feature_request = match FeatureRequest::decode(ticket.ticket) {
            Ok(req) => req,
            Err(e) => {
                let duration = start_time.elapsed();
                self.log_request("do_get", None, None, None, duration, "error", 
                               Some(&format!("decode_failed: {}", e)));
                return Err(Status::invalid_argument(format!(
                    "Failed to decode Ticket into FeatureRequest: {}",
                    e
                )));
            }
        };

        let feature_view = &feature_request.feature_view;
        let entity_count = feature_request.entities.len();
        let feature_count = feature_request.feature_names.len();

        debug!("Processing feature request: view={}, entities={}, features={}", 
               feature_view, entity_count, feature_count);

        let batch_result = self
            .resolver
            .resolve(
                feature_view,
                &feature_request.feature_names,
                &feature_request
                    .entities
                    .iter()
                    .map(|e| e.entity_id.clone())
                    .collect::<Vec<_>>(),
                feature_request
                    .as_of
                    .map(|ts| {
                        DateTime::from_timestamp(ts.seconds, ts.nanos as u32).ok_or_else(|| {
                            Status::invalid_argument(format!(
                                "Invalid as_of timestamp: seconds={}, nanos={}",
                                ts.seconds, ts.nanos
                            ))
                        })
                    })
                    .transpose()?,
            )
            .await;

        let duration = start_time.elapsed();

        match batch_result {
            Ok(batch) => {
                self.log_request("do_get", Some(feature_view), Some(entity_count), 
                               Some(feature_count), duration, "success", None);

                let stream = FlightDataEncoderBuilder::new()
                    .build(futures::stream::once(async move { Ok(batch) }))
                    .map(
                        |res: Result<FlightData, arrow_flight::error::FlightError>| {
                            res.map_err(|e| Status::internal(format!("Flight encoding error: {}", e)))
                        },
                    );

                Ok(Response::new(Box::pin(stream)))
            }
            Err(e) => {
                let error_msg = format!("Resolution failed: {}", e);
                self.log_request("do_get", Some(feature_view), Some(entity_count), 
                               Some(feature_count), duration, "error", Some(&error_msg));
                Err(Status::internal(error_msg))
            }
        }
    }

    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send>>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();

        let first_msg = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Empty stream"))??;
        let descriptor = first_msg.flight_descriptor.as_ref().ok_or_else(|| {
            Status::invalid_argument("First FlightData message must contain a FlightDescriptor")
        })?;

        let backend_name = descriptor
            .path
            .first()
            .ok_or_else(|| {
                Status::invalid_argument(
                    "FlightDescriptor path must contain at least one element (backend name)",
                )
            })?
            .clone();

        let full_stream = futures::stream::once(async move { Ok(first_msg) })
            .chain(stream)
            .map(|res| {
                res.map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e)))
            });
        let mut reader =
            arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(full_stream);

        while let Some(batch_res) = reader.next().await {
            let batch = batch_res.map_err(|e| Status::internal(format!("Decode error: {}", e)))?;
            self.resolver
                .put(&backend_name, batch)
                .await
                .map_err(|e| Status::internal(format!("Put failed: {}", e)))?;
        }

        let res_stream = futures::stream::once(async move {
            Ok(PutResult {
                app_metadata: vec![].into(),
            })
        });
        Ok(Response::new(Box::pin(res_stream)))
    }

    type DoExchangeStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let mut stream = request.into_inner();
        let resolver = self.resolver.clone();

        let output_stream = async_stream::try_stream! {
            while let Some(data_res) = stream.next().await {
                let data = data_res?;

                let feature_request = FeatureRequest::decode(data.app_metadata).map_err(|e| {
                    Status::invalid_argument(format!("Failed to decode app_metadata: {}", e))
                })?;

                let entities: Vec<String> = feature_request
                    .entities
                    .iter()
                    .map(|e| e.entity_id.clone())
                    .collect();

                let as_of = feature_request.as_of.map(|ts| {
                    DateTime::from_timestamp(ts.seconds, ts.nanos as u32).expect("Invalid timestamp")
                });

                let batch = resolver
                    .resolve(
                        &feature_request.feature_view,
                        &feature_request.feature_names,
                        &entities,
                        as_of,
                    )
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;

                let mut encoder = FlightDataEncoderBuilder::new()
                    .build(futures::stream::once(async move { Ok(batch) }));
                while let Some(msg_res) = encoder.next().await {
                    let msg = msg_res.map_err(|e| Status::internal(format!("Flight encoding error: {}", e)))?;
                    yield msg;
                }
            }
        };

        Ok(Response::new(Box::pin(output_stream)))
    }

    type DoActionStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        tracing::info!("Received action: {}", action.r#type);

        match action.r#type.as_str() {
            "flush_cache" => {
                tracing::info!("Cache flush requested (noop)");
                let res = arrow_flight::Result {
                    body: "Cache flushed".into(),
                };
                let stream = futures::stream::once(async move { Ok(res) });
                Ok(Response::new(Box::pin(stream)))
            }
            "reload_registry" => {
                tracing::info!("Registry reload requested (noop)");
                let res = arrow_flight::Result {
                    body: "Registry reloaded".into(),
                };
                let stream = futures::stream::once(async move { Ok(res) });
                Ok(Response::new(Box::pin(stream)))
            }
            _ => Err(Status::unimplemented(format!(
                "Unknown action: {}",
                action.r#type
            ))),
        }
    }

    type ListActionsStream = Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send>>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            ActionType {
                r#type: "flush_cache".into(),
                description: "Flush the server cache".into(),
            },
            ActionType {
                r#type: "reload_registry".into(),
                description: "Reload the feature registry".into(),
            },
        ];
        let stream = futures::stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }
}
