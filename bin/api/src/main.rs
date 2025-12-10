mod service;
mod artifacts;

use std::net::SocketAddr;
use std::sync::Arc;

use alloy::primitives::Bytes;
use axum::{extract::DefaultBodyLimit, routing::{get, post}, Router};
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};
use service::ClusterServiceImpl;
use artifacts::ArtifactState;
use sp1_cluster_common::{logger, proto::cluster_service_server::ClusterServiceServer};
use sp1_cluster_artifact::redis::RedisArtifactClient;
use sqlx::{prelude::FromRow, types::time::OffsetDateTime};
use tonic::transport::Server;
use tracing::{info, warn};

#[derive(Serialize, Deserialize, FromRow)]
struct Request {
    id: Bytes,
    // Note: Ensure these fields exist in your database schema
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables
    if let Err(e) = dotenv::dotenv() {
        eprintln!("not loading .env file: {}", e);
    }
    logger::init(Resource::empty());
    info!("Loaded environment variables");

    // Connect to the database
    let database_url = std::env::var("API_DATABASE_URL").expect("API_DATABASE_URL must be set");
    let pool = sqlx::postgres::PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to database");

    if std::env::var("API_AUTO_MIGRATE").unwrap_or("false".to_string()) == "true" {
        info!("Running database migrations");
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");
    }

    let pool = Arc::new(pool);

    // Create the gRPC service
    let cluster_service = ClusterServiceImpl::new(pool.clone());
    let grpc_service = ClusterServiceServer::new(cluster_service);

    // Set up the gRPC server
    let grpc_addr = std::env::var("API_GRPC_ADDR").unwrap_or("127.0.0.1:50051".to_string());
    let grpc_addr = grpc_addr
        .parse::<SocketAddr>()
        .expect("Invalid gRPC address");
    info!("Starting gRPC server on {}", grpc_addr);

    // Start the gRPC server in a separate task
    tokio::spawn(async move {
        Server::builder()
            .accept_http1(true)
            .add_service(tonic_web::enable(grpc_service))
            .serve(grpc_addr)
            .await
            .unwrap_or_else(|e| {
                warn!("gRPC server error: {}", e);
            });
    });

    // Initialize artifact client (Redis)
    let redis_nodes = std::env::var("API_REDIS_NODES").ok();
    
    // Build the HTTP application with routes
    let app = if let Some(redis_nodes_str) = redis_nodes {
        info!("Initializing Redis artifact client: {}", redis_nodes_str);
        let nodes: Vec<String> = redis_nodes_str.split(',').map(|s| s.to_string()).collect();
        let pool_size = std::env::var("API_REDIS_POOL_SIZE")
            .unwrap_or("16".to_string())
            .parse()
            .unwrap_or(16);
        let artifact_client = Arc::new(RedisArtifactClient::new(nodes, pool_size));
        let artifact_state = ArtifactState { artifact_client };
        
        info!("Artifact upload/download endpoints enabled");
        Router::new()
            .route("/", get(|| async { "OK" }))
            .route("/healthz", get(|| async { "OK" }))
            .route("/artifacts/upload/{type}", post(artifacts::upload_artifact))
            .route("/artifacts/download/{type}/{id}", get(artifacts::download_artifact))
            .layer(DefaultBodyLimit::max(500 * 1024 * 1024)) // 500 MB limit
            .with_state(artifact_state)
    } else {
        warn!("API_REDIS_NODES not set, artifact upload/download endpoints will not be available");
        Router::new()
            .route("/", get(|| async { "OK" }))
            .route("/healthz", get(|| async { "OK" }))
    };

    // Run the HTTP server
    let http_addr = std::env::var("API_HTTP_ADDR").unwrap_or("127.0.0.1:3000".to_string());
    info!("Starting HTTP server on {}", http_addr);
    let listener = tokio::net::TcpListener::bind(http_addr).await?;
    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}
