use std::{collections::HashMap, env, error::Error, net::Ipv4Addr, time::Instant};

use furink_proto::{
    discovery::{
        discovery_service_server::{DiscoveryService, DiscoveryServiceServer},
        lookup_response::LookupPayload,
        HeartbeatPayload, LookupRequest, LookupResponse, RegisterRequest, RegisterResponse,
        ServiceKind,
    },
    version::{
        version_service_server::{VersionService, VersionServiceServer},
        VersionRequest, VersionResponse,
    },
    VERSION,
};
use tokio::sync::RwLock;
use tonic::{async_trait, transport::Server, Code, Request, Response, Status};
use uuid::Uuid;

struct VersionServiceImpl {}

#[async_trait]
impl VersionService for VersionServiceImpl {
    async fn validate(
        &self,
        _: Request<VersionRequest>,
    ) -> Result<Response<VersionResponse>, Status> {
        Ok(Response::new(VersionResponse {
            version: VERSION.to_string(),
        }))
    }
}

#[derive(Debug, PartialEq)]
struct Service {
    address: Ipv4Addr,
    port: u16,
    last_heartbeat: Instant,
}

#[derive(Debug, Default)]
struct DiscoveryServiceImpl {
    services: RwLock<HashMap<ServiceKind, Vec<Service>>>,
}

#[async_trait]
impl DiscoveryService for DiscoveryServiceImpl {
    #[tracing::instrument]
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let mut services = self.services.write().await;
        let inner = request.into_inner();
        // create the service object
        let service = Service {
            address: inner
                .address
                .parse()
                .map_err(|_| Status::new(Code::InvalidArgument, "expected a valid ip address"))?,
            port: inner.port as u16,
            last_heartbeat: Instant::now(),
        };
        // check if the service is already registered, and create it if not
        let kind = ServiceKind::from_i32(inner.kind).unwrap();
        if services.contains_key(&kind) {
            services.get_mut(&kind).unwrap().push(service);
        } else {
            services.insert(kind, vec![service]);
        }
        // return response
        Ok(Response::new(RegisterResponse {
            id: Uuid::new_v4().to_string(),
            count: services.get(&kind).map(|v| v.len()).unwrap_or(0) as i64,
        }))
    }
    #[tracing::instrument]
    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        let inner = request.into_inner();
        let kind = ServiceKind::from_i32(inner.kind).unwrap();
        // read services
        let services = self.services.read().await;
        if let None = services.get(&kind) {
            return Ok(Response::new(LookupResponse { services: vec![] }));
        }
        // unwrap services
        let services = services.get(&kind).unwrap();
        // find services that have not had a heartbeat in the last 60 seconds
        let invalid_services: Vec<&Service> = services
            .iter()
            .filter(|s| {
                s.last_heartbeat.elapsed().as_secs()
                    < env::var("HEARTBEAT_TIMEOUT")
                        .expect("HEARTBEAT_TIMEOUT")
                        .parse::<u64>()
                        .unwrap()
            })
            .collect();
        // remove invalid services
        // this is done in a block to prevent locking the services map for too long
        {
            let mut services = self.services.write().await;
            for (_, services) in services.iter_mut() {
                services.retain(|s| !invalid_services.contains(&s));
            }
        }
        // return response
        Ok(Response::new(LookupResponse {
            services: services
                .iter()
                .map(|s| LookupPayload {
                    address: s.address.to_string(),
                    port: s.port as u32,
                })
                .collect(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatPayload>,
    ) -> Result<Response<HeartbeatPayload>, Status> {
        Ok(Response::new(request.into_inner()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	// load dotenv when in development
    if cfg!(debug_assertions) {
        dotenv::dotenv().unwrap();
    }
    // print splash
    println!(
        r#"
{} v{}
Authors: {}	
"#,
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        env!("CARGO_PKG_AUTHORS")
    );
    // initialize
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    //  setup server
    let addr = "[::1]:50051".parse().unwrap();
    let service = DiscoveryServiceImpl::default();
    tracing::info!(message = "Starting server...", %addr);

    // create server
    Server::builder()
        .trace_fn(|_| tracing::info_span!("service-discovery"))
        .add_service(DiscoveryServiceServer::new(service))
        .add_service(VersionServiceServer::new(VersionServiceImpl {}))
        .serve(addr)
        .await?;
    Ok(())
}
