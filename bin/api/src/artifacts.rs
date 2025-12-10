// Artifact upload/download HTTP handlers
// This allows clients to upload/download artifacts without直接连接 Redis

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use sp1_cluster_artifact::{ArtifactClient, ArtifactType};
use std::sync::Arc;
use tracing::{error, info};

#[derive(Clone)]
pub struct ArtifactState<A: ArtifactClient> {
    pub artifact_client: Arc<A>,
}

#[derive(Serialize, Deserialize)]
pub struct UploadResponse {
    pub artifact_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Upload an artifact (program, stdin, etc.)
/// POST /artifacts/upload
/// Body: raw bytes of the artifact
/// Query param: type=program|stdin|proof
pub async fn upload_artifact<A: ArtifactClient + Send + Sync + 'static>(
    State(state): State<ArtifactState<A>>,
    Path(artifact_type): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    let artifact_type = match artifact_type.as_str() {
        "program" => ArtifactType::Program,
        "stdin" => ArtifactType::Stdin,
        "proof" => ArtifactType::Proof,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Invalid artifact type. Must be 'program', 'stdin', or 'proof'".to_string(),
                }),
            )
                .into_response();
        }
    };
    
    // Create artifact ID
    let artifact_id = match state.artifact_client.create_artifact() {
        Ok(id) => id,
        Err(e) => {
            error!("Failed to create artifact ID: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Failed to create artifact ID: {}", e),
                }),
            )
                .into_response();
        }
    };
    
    // Get the ID string before moving artifact_id
    let artifact_id_str = artifact_id.clone().to_id();
    
    // Upload to storage
    // Note: For Program type, use upload_with_type which handles the data as-is
    // For other types, upload_raw is appropriate
    let upload_result = match artifact_type {
        ArtifactType::Program => {
            state
                .artifact_client
                .upload_with_type(&artifact_id, artifact_type, body.to_vec())
                .await
        }
        _ => {
            state
                .artifact_client
                .upload_raw(&artifact_id, artifact_type, body.to_vec())
                .await
        }
    };
    
    match upload_result {
        Ok(_) => {
            info!("Uploaded artifact: {} (type: {:?})", artifact_id_str, artifact_type);
            (
                StatusCode::OK,
                Json(UploadResponse {
                    artifact_id: artifact_id_str,
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!("Failed to upload artifact: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Failed to upload artifact: {}", e),
                }),
            )
                .into_response()
        }
    }
}

/// Download an artifact
/// GET /artifacts/{type}/{id}
pub async fn download_artifact<A: ArtifactClient + Send + Sync + 'static>(
    State(state): State<ArtifactState<A>>,
    Path((artifact_type, artifact_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let artifact_type = match artifact_type.as_str() {
        "program" => ArtifactType::Program,
        "stdin" => ArtifactType::Stdin,
        "proof" => ArtifactType::Proof,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Invalid artifact type".to_string(),
                }),
            )
                .into_response();
        }
    };
    
    // Download from storage
    match state
        .artifact_client
        .download_raw(&artifact_id, artifact_type)
        .await
    {
        Ok(bytes) => {
            info!("Downloaded artifact: {} (type: {:?}, size: {} bytes)", artifact_id, artifact_type, bytes.len());
            (StatusCode::OK, bytes).into_response()
        }
        Err(e) => {
            error!("Failed to download artifact {}: {:?}", artifact_id, e);
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Artifact not found: {}", e),
                }),
            )
                .into_response()
        }
    }
}

