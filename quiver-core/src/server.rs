use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::FlightService,
};
use futures::Stream;
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};

use crate::proto::quiver::v1::FeatureRequest;
use crate::resolver::Resolver;
use arrow_flight::encode::FlightDataEncoderBuilder;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use prost::Message;
use std::sync::Arc;

pub struct QuiverFlightServer {
    resolver: Arc<Resolver>,
}

impl tonic::server::NamedService for QuiverFlightServer {
    const NAME: &'static str = "arrow.flight.protocol.FlightService";
}

impl QuiverFlightServer {
    pub fn new(resolver: Arc<Resolver>) -> Self {
        Self { resolver }
    }
}

#[tonic::async_trait]
impl FlightService for QuiverFlightServer {
    type HandshakeStream = Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
    type ListFlightsStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send>>;
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not implemented"))
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
        let schema_result = arrow_flight::SchemaAsIpc::new(&schema, &options)
            .try_into()
            .map_err(|e| Status::internal(format!("Failed to serialize schema: {}", e)))?;

        Ok(Response::new(schema_result))
    }

    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let feature_request = FeatureRequest::decode(ticket.ticket).map_err(|e| {
            Status::invalid_argument(format!(
                "Failed to decode Ticket into FeatureRequest: {}",
                e
            ))
        })?;

        let batch = self
            .resolver
            .resolve(
                &feature_request.feature_view,
                &feature_request.feature_names,
                &feature_request
                    .entities
                    .iter()
                    .map(|e| e.entity_id.clone())
                    .collect::<Vec<_>>(),
                feature_request.as_of.map(|ts| {
                    DateTime::from_timestamp(ts.seconds, ts.nanos as u32).unwrap_or(Utc::now())
                }),
            )
            .await
            .map_err(|e| Status::internal(format!("Resolution failed: {}", e)))?;

        let stream = FlightDataEncoderBuilder::new()
            .build(futures::stream::once(async move { Ok(batch) }))
            .map(
                |res: Result<FlightData, arrow_flight::error::FlightError>| {
                    res.map_err(|e| Status::internal(format!("Flight encoding error: {}", e)))
                },
            );

        Ok(Response::new(Box::pin(stream)))
    }

    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send>>;

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    type DoExchangeStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    type DoActionStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    type ListActionsStream = Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send>>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
}
