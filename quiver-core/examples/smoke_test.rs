use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use prost::Message;
use tonic::transport::Channel;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FeatureRequest {
    #[prost(string, tag = "1")]
    pub feature_view: String,
    #[prost(string, repeated, tag = "2")]
    pub feature_names: Vec<String>,
    #[prost(message, repeated, tag = "3")]
    pub entities: Vec<EntityRequest>,
    #[prost(message, optional, tag = "4")]
    pub as_of: Option<prost_types::Timestamp>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntityRequest {
    #[prost(string, tag = "1")]
    pub entity_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:8815")
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);

    println!("--- Testing ListFlights ---");
    let flights = client.list_flights(arrow_flight::Criteria::default()).await?.into_inner();
    let views: Vec<_> = futures::StreamExt::collect::<Vec<_>>(flights).await;
    println!("Available views: {:?}", views);

    println!("--- Testing DoGet ---");
    let req = FeatureRequest {
        feature_view: "user_features".to_string(),
        feature_names: vec!["spend_30d".to_string(), "session_count".to_string()],
        entities: vec![
            EntityRequest { entity_id: "user:101".to_string() },
            EntityRequest { entity_id: "user:999".to_string() },
            EntityRequest { entity_id: "user:102".to_string() },
        ],
        as_of: None,
    };
    
    let mut buf = Vec::new();
    req.encode(&mut buf)?;
    
    let ticket = Ticket::new(buf);
    let mut stream = client.do_get(ticket).await?.into_inner();
    
    while let Some(batch) = futures::StreamExt::next(&mut stream).await {
        println!("Received FlightData from server! {:?}", batch);
    }

    Ok(())
}
