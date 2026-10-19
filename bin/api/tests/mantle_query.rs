//! Postgres-backed tests for the Mantle `QueryProofRequests` extension.
//!
//! `#[ignore]`'d by default — run with:
//!
//!     cargo test --release -p sp1-cluster-api --tests -- --ignored --test-threads=1
//!
//! Requires a Postgres server at `DATABASE_URL`. Each test runs in its own
//! throwaway database created by `#[sqlx::test]`.

use sp1_cluster_api::{ClusterServiceImpl, MantleClusterServiceImpl};
use sp1_cluster_common::mantle::{
    mantle_cluster_service_server::MantleClusterService, ProofRequestQueryRequest,
};
use sp1_cluster_common::proto::{
    cluster_service_server::ClusterService, ProofRequestCreateRequest,
};
use sqlx::PgPool;
use std::sync::Arc;
use tonic::Request;

const MINE: u8 = 0xaa;
const OTHER: u8 = 0xbb;

/// `OTHER` owns the oldest rows (minutes 0..5), `MINE` the newer ones
/// (minutes 10..15). `created_at` is set explicitly so the order is exact.
async fn seed(pool: &PgPool) {
    let cluster = ClusterServiceImpl::new(Arc::new(pool.clone()));
    let rows = (0..5)
        .map(|i| (format!("req_other_{i}"), OTHER, i))
        .chain((0..5).map(|i| (format!("req_mine_{i}"), MINE, 10 + i)));
    for (id, requester, minute) in rows {
        cluster
            .proof_request_create(Request::new(ProofRequestCreateRequest {
                proof_id: id.clone(),
                requester: vec![requester; 20],
                stdin_artifact_id: "s".into(),
                program_artifact_id: "p".into(),
                deadline: u64::MAX >> 1,
                ..Default::default()
            }))
            .await
            .unwrap();
        sqlx::query(
            "UPDATE proof_requests SET created_at = TIMESTAMPTZ '2026-07-01 00:00:00+00' \
             + make_interval(mins => $2) WHERE id = $1",
        )
        .bind(&id)
        .bind(minute)
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn query(pool: &PgPool, req: ProofRequestQueryRequest) -> Vec<String> {
    MantleClusterServiceImpl::new(Arc::new(pool.clone()))
        .query_proof_requests(Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .proof_requests
        .into_iter()
        .map(|r| r.id)
        .collect()
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires Postgres at DATABASE_URL"]
async fn requester_filter_newest_first_pages(pool: PgPool) {
    seed(&pool).await;
    let page = |offset| ProofRequestQueryRequest {
        requester: Some(vec![MINE; 20]),
        newest_first: true,
        limit: Some(2),
        offset: Some(offset),
        ..Default::default()
    };
    assert_eq!(query(&pool, page(0)).await, ["req_mine_4", "req_mine_3"]);
    assert_eq!(query(&pool, page(2)).await, ["req_mine_2", "req_mine_1"]);
    assert_eq!(query(&pool, page(4)).await, ["req_mine_0"]);
}

/// Without the Mantle fields the extension behaves like upstream
/// `ProofRequestList`: every requester, oldest first.
#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires Postgres at DATABASE_URL"]
async fn defaults_match_upstream_order(pool: PgPool) {
    seed(&pool).await;
    let ids = query(
        &pool,
        ProofRequestQueryRequest {
            limit: Some(3),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(ids, ["req_other_0", "req_other_1", "req_other_2"]);
}
