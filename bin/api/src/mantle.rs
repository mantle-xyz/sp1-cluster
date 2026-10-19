//! Mantle-only `MantleClusterService` (see `sp1_cluster_common::mantle`).
//!
//! Kept apart from `service.rs` so upstream syncs of that file stay
//! conflict-free. The filter SQL mirrors `proof_request_list` on purpose. Keep
//! the two in step if upstream changes its filters.

use std::sync::Arc;

use sp1_cluster_common::mantle::{
    mantle_cluster_service_server::MantleClusterService, ProofRequestQueryRequest,
};
use sp1_cluster_common::proto::ProofRequestListResponse;
use sqlx::{PgPool, QueryBuilder};
use tonic::{Request, Response, Status};

use crate::service::DbProofRequest;

/// Same row cap as upstream `proof_request_list`.
const MAX_LIMIT: u32 = 1000;

pub struct MantleClusterServiceImpl {
    db_pool: Arc<PgPool>,
}

impl MantleClusterServiceImpl {
    pub fn new(db_pool: Arc<PgPool>) -> Self {
        Self { db_pool }
    }
}

#[async_trait::async_trait]
impl MantleClusterService for MantleClusterServiceImpl {
    async fn query_proof_requests(
        &self,
        request: Request<ProofRequestQueryRequest>,
    ) -> Result<Response<ProofRequestListResponse>, Status> {
        let req = request.into_inner();

        let mut query = QueryBuilder::new("SELECT * FROM proof_requests WHERE 1=1");

        if let Some(requester) = req.requester {
            query.push(" AND requester = ").push_bind(requester);
        }

        if !req.proof_status.is_empty() {
            let mut separated = query.push(" AND proof_status IN (").separated(",");
            for status in req.proof_status {
                separated.push_bind(status);
            }
            query.push(")");
        }

        if !req.execution_status.is_empty() {
            let mut separated = query.push(" AND execution_status IN (").separated(",");
            for status in req.execution_status {
                separated.push_bind(status);
            }
            query.push(")");
        }

        if let Some(minimum_deadline) = req.minimum_deadline {
            // Clamp like the offset below: a u64 above i64::MAX would wrap negative.
            query
                .push(" AND deadline >= ")
                .push_bind(minimum_deadline.min(i64::MAX as u64) as i64);
        }

        // `id` breaks `created_at` ties so pages are stable in both directions.
        query.push(if req.newest_first {
            " ORDER BY created_at DESC, id DESC"
        } else {
            " ORDER BY created_at ASC, id ASC"
        });

        let limit = req.limit.unwrap_or(10).min(MAX_LIMIT);
        query.push(" LIMIT ").push_bind(limit as i32);

        let offset = req.offset.unwrap_or(0).min(i32::MAX as u32);
        query.push(" OFFSET ").push_bind(offset as i32);

        let rows = query
            .build_query_as::<DbProofRequest>()
            .fetch_all(&*self.db_pool)
            .await
            .map_err(|e| Status::internal(format!("Failed to query proof requests: {e:?}")))?;

        Ok(Response::new(ProofRequestListResponse {
            proof_requests: rows.into_iter().map(|r| r.into_proto()).collect(),
        }))
    }
}
