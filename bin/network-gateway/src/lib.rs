pub mod admission;
pub mod artifact_http;
pub mod auth;
pub mod config;
pub mod ids;
pub mod program_store;
pub mod service;
pub mod status;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{routing::get, Router};
use sp1_cluster_artifact::{ArtifactClient, CompressedUpload};
use sp1_cluster_common::client::ClusterServiceClient;
use sp1_cluster_common::proto as cluster_pb;
use sp1_sdk::network::proto::artifact::artifact_store_server::ArtifactStoreServer;
use sp1_sdk::network::proto::base::network::prover_network_server::ProverNetworkServer;
use tokio::signal;
use tonic::transport::Server;
use tracing::{info, warn};

use crate::admission::{AdmissionController, Classifier};
use crate::artifact_http::ArtifactHttpState;
use crate::auth::{parse_allowlist, Auth, AuthMode};
use crate::config::Config;
use crate::program_store::{FilesystemProgramStore, InMemoryProgramStore, ProgramStore};
use crate::service::artifact_store::ArtifactStoreImpl;
use crate::service::prover_network::ProverNetworkImpl;

/// Resolve the auth config, connect to the cluster, and serve both gRPC and HTTP endpoints.
/// Blocks until the shutdown signal fires.
pub async fn run<A>(cfg: Config, client: A) -> Result<()>
where
    A: ArtifactClient + CompressedUpload + 'static,
{
    let cluster = ClusterServiceClient::new(cfg.cluster_rpc.clone())
        .await
        .map_err(|e| {
            anyhow::anyhow!("failed to connect to cluster RPC {}: {e}", cfg.cluster_rpc)
        })?;
    let auth = build_auth(&cfg)?;
    let program_store = build_program_store(&cfg)?;
    serve(
        cfg,
        client,
        cluster,
        auth,
        program_store,
        shutdown_signal(),
        shutdown_signal(),
    )
    .await
}

/// Serve both endpoints against pre-built cluster + auth state.
///
/// Broken out so integration tests can inject a fake ClusterServiceClient and
/// their own shutdown signals (e.g. `oneshot::Receiver`).
pub async fn serve<A, FG, FH>(
    cfg: Config,
    client: A,
    cluster: ClusterServiceClient,
    auth: Auth,
    program_store: Arc<dyn ProgramStore>,
    grpc_shutdown: FG,
    http_shutdown: FH,
) -> Result<()>
where
    A: ArtifactClient + CompressedUpload + 'static,
    FG: std::future::Future<Output = ()> + Send + 'static,
    FH: std::future::Future<Output = ()> + Send + 'static,
{
    let grpc_addr: SocketAddr = cfg
        .grpc_addr
        .parse()
        .with_context(|| format!("invalid GATEWAY_GRPC_ADDR: {}", cfg.grpc_addr))?;

    let balance_amount = cfg
        .balance_amount
        .clone()
        .unwrap_or_else(|| alloy_primitives::U256::MAX.to_string());

    // Tonic's 4 MiB default is too tight for SP1 verifying keys carried in
    // `create_program`; bump to 64 MiB so the gRPC path is never the
    // bottleneck. Artifact bodies go through the HTTP proxy, not gRPC.
    const MAX_GRPC_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

    let artifact_store = ArtifactStoreServer::new(ArtifactStoreImpl::new(
        client.clone(),
        cfg.public_http_url.clone(),
        auth.clone(),
    ))
    .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
    .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);
    let admission_metrics = Arc::new(crate::admission::AdmissionMetrics::new());
    let admission = Arc::new(build_admission(&cfg)?.with_metrics(admission_metrics.clone()));
    log_admission_config(&cfg);

    // Handles for the best-effort restart seed, spawned off the boot path
    // below (see the note at the spawn site).
    let seed_admission = admission.clone();
    let seed_cluster = cluster.clone();

    let prover_network = ProverNetworkServer::new(ProverNetworkImpl::new(
        client.clone(),
        cluster,
        cfg.public_http_url.clone(),
        balance_amount,
        auth,
        program_store,
        admission.clone(),
    ))
    .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
    .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);

    let grpc_task = tokio::spawn(async move {
        info!("gRPC server listening on {grpc_addr}");
        Server::builder()
            .accept_http1(true)
            .add_service(tonic_web::enable(prover_network))
            .add_service(tonic_web::enable(artifact_store))
            .serve_with_shutdown(grpc_addr, grpc_shutdown)
            .await
            .unwrap_or_else(|e| warn!("gRPC server error: {e}"));
    });

    // Reclaim admission slots whose terminal release was lost (client dropped,
    // crash) — the enforce path relies on this to not leak capacity.
    let reaper = admission.clone();
    let reap_period = std::time::Duration::from_secs(cfg.admission_reap_period_secs);
    let reaper_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(reap_period);
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            reaper.reap();
        }
    });

    let http_state = Arc::new(ArtifactHttpState { client });
    let app = Router::new()
        .route("/", get(|| async { "OK" }))
        .route("/healthz", get(|| async { "OK" }))
        .merge(artifact_http::router(http_state));

    let http_listener = tokio::net::TcpListener::bind(&cfg.http_addr)
        .await
        .with_context(|| format!("bind {}", cfg.http_addr))?;
    info!("HTTP server listening on {}", cfg.http_addr);

    // `/metrics` is served on its own loopback-bound listener (default
    // 127.0.0.1:9091), deliberately kept off the public artifact HTTP
    // surface so admission telemetry isn't exposed to SDK clients.
    let metrics_handle = admission_metrics.clone();
    let metrics_app = Router::new().route(
        "/metrics",
        get(move || {
            let m = metrics_handle.clone();
            async move { m.render_response() }
        }),
    );
    let metrics_listener = tokio::net::TcpListener::bind(&cfg.metrics_addr)
        .await
        .with_context(|| format!("bind metrics {}", cfg.metrics_addr))?;
    info!("metrics server listening on {}", cfg.metrics_addr);
    let metrics_task = tokio::spawn(async move {
        axum::serve(metrics_listener, metrics_app)
            .await
            .unwrap_or_else(|e| warn!("metrics server error: {e}"));
    });

    // Best-effort restart seed, deliberately off the boot path: the in-memory
    // counters reset to 0 on restart while the cluster may still be proving
    // requests from before this process started, so seed those to avoid a
    // brief over-admit until they drain. Spawned (not awaited) so a slow or
    // flaky cluster can never delay the gateway binding its listeners; it runs
    // once and exits. Any failure is logged and swallowed.
    tokio::spawn(async move {
        match seed_admission_from_cluster(&seed_admission, &seed_cluster).await {
            Ok(seeded) if seeded > 0 => info!(
                seeded,
                "admission: seeded in-flight proofs from cluster after restart"
            ),
            Ok(_) => {}
            Err(e) => warn!(
                error = %e,
                "admission: restart-seed failed (best-effort; brief over-admit possible)"
            ),
        }
    });

    let serve_result = axum::serve(http_listener, app)
        .with_graceful_shutdown(http_shutdown)
        .await;

    // Tear down the background tasks even if `serve` errored, so `serve()`
    // (driven directly by integration tests on a shared runtime) never leaks
    // the reaper/metrics loops or a still-running gRPC server.
    reaper_task.abort();
    metrics_task.abort();
    if serve_result.is_err() {
        grpc_task.abort();
        serve_result?;
    }

    grpc_task.await.ok();
    info!("network-gateway shut down cleanly");
    Ok(())
}

/// Log the effective admission policy at startup, and warn on the
/// enforce=true + cap=0 footgun (all requests for that pool would be shed).
fn log_admission_config(cfg: &Config) {
    info!(
        enforce = cfg.admission_enforce,
        range_cap = cfg.admission_range_max_inflight,
        agg_cap = cfg.admission_agg_max_inflight,
        global_cap = ?cfg.admission_global_max_inflight,
        "admission gate configured (single-instance authoritative; do not run >1 gateway replica)"
    );
    if cfg.admission_enforce
        && (cfg.admission_range_max_inflight == 0 || cfg.admission_agg_max_inflight == 0)
    {
        warn!(
            "admission enforce=true with a pool cap of 0 — all requests for that pool will be shed"
        );
    }
}

/// Best-effort: classify the cluster's currently-non-terminal (`Pending`)
/// proof requests and seed matching admission slots so the in-memory
/// counters reflect reality after a gateway restart.
///
/// The cluster's `ProofRequest` doesn't carry `vk_hash` (it's never
/// persisted past `request_proof`) — only `options_artifact_id`, which holds
/// the proof `mode` as a string (mirroring how `build_sdk_proof_request`
/// recovers `mode`). So seeded classification falls back to the mode-only
/// path of `Classifier::classify` (as if `vk_hash` were empty); requests that
/// were only classified by an explicit `*_VK_HASHES` override won't be
/// re-classified correctly by this path, but the default mode-based
/// classification (Compressed → Range, Plonk/Groth16 → Agg) still applies.
/// Upper bound on proofs pulled by the restart seed (see call site).
const SEED_MAX_PROOFS: u32 = 4096;

async fn seed_admission_from_cluster(
    admission: &AdmissionController,
    cluster: &ClusterServiceClient,
) -> Result<usize> {
    let proofs = cluster
        .get_proof_requests(cluster_pb::ProofRequestListRequest {
            proof_status: vec![cluster_pb::ProofRequestStatus::Pending as i32],
            execution_status: vec![],
            minimum_deadline: None,
            handled: None,
            // Bound the boot-time scan; in-flight (Pending) proofs against a
            // single self-hosted cluster are far below this. A backlog larger
            // than this only under-seeds slightly (brief over-admit), which the
            // reaper and normal admission converge back to correct.
            limit: Some(SEED_MAX_PROOFS),
            offset: None,
            scheduled_by: None,
        })
        .await
        .map_err(|e| anyhow::anyhow!("cluster get_proof_requests failed: {e}"))?;

    let mut seeded = 0usize;
    for proof in proofs {
        let mode: i32 = proof
            .options_artifact_id
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if let Some(pool) = admission.classify(mode, &[]) {
            admission.seed(&proof.id, pool);
            seeded += 1;
        }
    }
    Ok(seeded)
}

pub fn build_program_store(cfg: &Config) -> Result<Arc<dyn ProgramStore>> {
    match cfg.program_store.as_str() {
        "memory" => {
            info!("program store: in-memory (programs lost on gateway restart)");
            Ok(Arc::new(InMemoryProgramStore::new()))
        }
        "fs" => {
            let dir = cfg
                .program_store_dir
                .clone()
                .context("GATEWAY_PROGRAM_STORE_DIR is required when program_store=fs")?;
            let store = FilesystemProgramStore::new(dir.clone())?;
            info!(dir = %dir.display(), "program store: filesystem");
            Ok(Arc::new(store))
        }
        other => anyhow::bail!("unknown GATEWAY_PROGRAM_STORE={other} (expected memory or fs)"),
    }
}

/// Build the admission controller from `GATEWAY_ADMISSION_*` config.
/// This only constructs the counting/classifying core — `serve` attaches
/// metrics (`with_metrics`), spawns the reaper task, and mounts `/metrics`.
pub fn build_admission(cfg: &Config) -> Result<AdmissionController> {
    // A zero reap period would panic `tokio::time::interval`, silently killing
    // the reaper task (slots would then leak until never). A zero TTL would
    // reap live slots on the first sweep. Reject both at boot.
    if cfg.admission_reap_period_secs == 0 {
        anyhow::bail!("GATEWAY_ADMISSION_REAP_PERIOD_SECS must be > 0");
    }
    if cfg.admission_slot_ttl_secs == 0 {
        anyhow::bail!("GATEWAY_ADMISSION_SLOT_TTL_SECS must be > 0");
    }
    let range_vks = parse_vk_hashes(cfg.admission_range_vk_hashes.as_deref())
        .context("GATEWAY_ADMISSION_RANGE_VK_HASHES")?;
    let agg_vks = parse_vk_hashes(cfg.admission_agg_vk_hashes.as_deref())
        .context("GATEWAY_ADMISSION_AGG_VK_HASHES")?;
    let classifier = Classifier::new(range_vks, agg_vks);
    Ok(AdmissionController::new(
        classifier,
        cfg.admission_range_max_inflight,
        cfg.admission_agg_max_inflight,
        cfg.admission_global_max_inflight,
        cfg.admission_enforce,
        std::time::Duration::from_secs(cfg.admission_slot_ttl_secs),
    ))
}

fn parse_vk_hashes(input: Option<&[String]>) -> Result<std::collections::HashSet<Vec<u8>>> {
    let Some(entries) = input else {
        return Ok(Default::default());
    };
    entries
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| {
            hex::decode(s.trim_start_matches("0x")).with_context(|| format!("invalid vk_hash {s}"))
        })
        .collect()
}

pub fn build_auth(cfg: &Config) -> Result<Auth> {
    let allowlist = cfg
        .auth_allowlist
        .as_deref()
        .map(parse_allowlist)
        .transpose()
        .map_err(|e| anyhow::anyhow!("GATEWAY_AUTH_ALLOWLIST: {e}"))?
        .unwrap_or_default();
    if cfg.auth_mode == AuthMode::Allowlist && allowlist.is_empty() {
        anyhow::bail!("GATEWAY_AUTH_MODE=allowlist requires GATEWAY_AUTH_ALLOWLIST");
    }
    let auth = Auth {
        mode: cfg.auth_mode,
        allowlist,
    };
    info!(mode = ?auth.mode, allowlist_size = auth.allowlist.len(), "auth configured");
    Ok(auth)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .ok()
            .map(|mut s| async move { s.recv().await })
            .unwrap()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("shutdown signal received");
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn base_cfg() -> Config {
        Config::parse_from(["network-gateway", "--cluster-rpc", "http://localhost:50051"])
    }

    #[test]
    fn build_admission_accepts_defaults() {
        assert!(build_admission(&base_cfg()).is_ok());
    }

    #[test]
    fn build_admission_rejects_zero_reap_period() {
        // A zero period would panic tokio's interval and silently kill the reaper.
        let mut cfg = base_cfg();
        cfg.admission_reap_period_secs = 0;
        assert!(build_admission(&cfg).is_err());
    }

    #[test]
    fn build_admission_rejects_zero_ttl() {
        // A zero TTL would reap live slots on the first sweep.
        let mut cfg = base_cfg();
        cfg.admission_slot_ttl_secs = 0;
        assert!(build_admission(&cfg).is_err());
    }
}
