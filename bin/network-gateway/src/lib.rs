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

use crate::admission::{AdmissionController, Classifier, PendingObservation};
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
    // The reaper also reconciles against cluster truth each tick (see below).
    let reaper_cluster = cluster.clone();

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

    // Reclaim admission slots each tick two ways: (1) `reap()` ages out slots
    // whose TTL lapsed (a lost terminal release / a client that stopped polling)
    // — the last-resort backstop; (2) `reconcile()` against cluster truth
    // releases committed slots whose proof is no longer Pending (finished or
    // gone) promptly, so an abandoned-but-committed proof doesn't hold a scarce
    // pool for the whole TTL. The enforce path relies on this to not leak
    // capacity.
    let reaper = admission.clone();
    let reap_period = std::time::Duration::from_secs(cfg.admission_reap_period_secs);
    let reconcile_absent_observations = cfg.admission_reconcile_absent_observations;
    let reconcile_fetch_timeout =
        std::time::Duration::from_secs(cfg.admission_reconcile_fetch_timeout_secs);
    let reaper_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(reap_period);
        tick.tick().await; // skip the immediate first tick
                           // WARN only ONCE per "unimplemented episode" (below) so a backend that
                           // doesn't support the Pending query can't spam the log; cleared on any
                           // successful reply so a later regression re-warns.
        let mut unimplemented_warned = false;
        loop {
            tick.tick().await;
            // Reconcile FIRST (before reap): a reconcile that sees a proof still
            // Pending refreshes its reap deadline, so running it ahead of reap
            // prevents reap from reclaiming a slot on the very tick a prolonged
            // cluster-query outage recovers.
            //
            // Reconcile committed slots against the cluster's live (Pending) set —
            // the authority for a committed slot. `proof_request_list` is a
            // self-hosted ClusterService method (NOT an SP1 SDK RPC), so a normal
            // deployment always supports it. If a backend does not — or does so
            // only intermittently, e.g. an old replica briefly in rotation during a
            // rolling upgrade, or a misrouting proxy — we DON'T permanently disable
            // reconcile (a sticky latch would strand it until a gateway restart).
            // We treat the tick as a skip (`None`), keep querying each tick (cheap,
            // once per reap period), and WARN once, so a transient/rolling
            // Unimplemented recovers on its own. Meanwhile the TTL reaper +
            // terminal-poll release still bound every slot. Skip the round-trip
            // when no slots are tracked.
            //
            // Each outcome maps to exactly one `PendingObservation`, fed to a
            // SINGLE `reconcile` call so the "how to treat this snapshot" policy
            // lives in one place (in `reconcile`, not spread across these arms):
            //   * COMPLETE reply under the page limit (incl. empty) → `Complete`:
            //     the authoritative live set; absent committed slots advance their
            //     absence streak and are released after `absent_observations`
            //     consecutive absent reconciles, so a single empty page can't
            //     mass-release. (Classification of Complete-vs-Partial is the pure
            //     `classify_pending` helper.)
            //   * TRUNCATED reply (hit the page limit) → `Partial`: an incomplete
            //     view; present slots are refreshed but absence is NOT concluded
            //     (a missing slot may be on an unseen page).
            //   * query error / timeout / unimplemented → `None`: a gap in
            //     observation; every streak is left unchanged (the streak is an
            //     observation count, not a wall clock, so a gap neither advances
            //     nor resets it — absence accrues only across genuine consecutive
            //     absent replies).
            if reaper.has_tracked_slots() {
                let obs = match tokio::time::timeout(
                    reconcile_fetch_timeout,
                    fetch_pending_proofs(&reaper_cluster),
                )
                .await
                {
                    Ok(Ok(proofs)) => {
                        unimplemented_warned = false; // a successful reply ends any episode
                        let obs = classify_pending(proofs, PENDING_QUERY_LIMIT as usize);
                        if let PendingObservation::Partial(ref ids) = obs {
                            tracing::warn!(
                                count = ids.len(),
                                "admission reconcile got a truncated cluster Pending list (hit the query page limit); treating it as a partial view — present slots refreshed, absence not concluded"
                            );
                        }
                        obs
                    }
                    Ok(Err(e)) if pending_query_unimplemented(&e) => {
                        // The backend doesn't (currently) implement the Pending
                        // query. Do NOT latch reconcile off — keep retrying so a
                        // transient/rolling Unimplemented self-heals — but WARN only
                        // once per episode. Fall back to the TTL reaper +
                        // terminal-poll release meanwhile.
                        if !unimplemented_warned {
                            unimplemented_warned = true;
                            tracing::warn!(
                                error = %e,
                                "cluster does not implement proof_request_list; admission reconcile is a no-op until it does (still retrying each tick) — falling back to the TTL reaper and terminal-poll release (orphaned slots reclaimed more slowly)"
                            );
                        }
                        PendingObservation::None
                    }
                    Ok(Err(e)) => {
                        tracing::debug!(error = %e, "admission reconcile skipped (cluster query failed)");
                        PendingObservation::None
                    }
                    Err(_) => {
                        tracing::debug!(
                            "admission reconcile skipped (cluster query exceeded the reconcile fetch timeout)"
                        );
                        PendingObservation::None
                    }
                };
                // One INFO line per reap period summarizing what reconcile saw
                // and did. This is the operator's window into "why are slots not
                // being released" — it distinguishes a starved reconcile
                // (kind=skip: cluster query failing/timing out), a cluster that
                // still reports the proofs Pending (kind=complete, present>0,
                // released=0 → the proofs are genuinely/anomalously in the Pending
                // set, not a gateway bug), and a working release (released>0). Its
                // ABSENCE every tick means the reaper task itself is not running.
                // Bounded snapshot of the RAW cluster Pending ids this tick, so the
                // log shows exactly what the query returned — the ground truth to
                // compare our tracked (seeded) slot ids against. If the seeds are
                // absent here, they should be released; if present, the cluster is
                // still reporting them Pending.
                const PENDING_SAMPLE_CAP: usize = 64;
                let pending_ids: Vec<String> = match &obs {
                    PendingObservation::Complete(ids) | PendingObservation::Partial(ids) => {
                        ids.iter().take(PENDING_SAMPLE_CAP).cloned().collect()
                    }
                    PendingObservation::None => Vec::new(),
                };
                let report = reaper.reconcile(obs, reconcile_absent_observations);
                info!(
                    kind = report.kind,
                    live = report.live,
                    committed = report.committed_tracked,
                    present = report.present,
                    absent = report.absent,
                    released = report.released,
                    pending_ids = ?pending_ids,
                    present_ids = ?report.present_ids,
                    absent_ids = ?report.absent_ids,
                    "admission reconcile tick"
                );
            }
            // Reap runs every tick (even when reconcile is skipped) as the TTL
            // backstop for committed slots reconcile couldn't confirm.
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
    let seed_task = tokio::spawn(async move {
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
    // the reaper/metrics loops, the one-shot restart seed, or a still-running
    // gRPC server.
    reaper_task.abort();
    metrics_task.abort();
    seed_task.abort();
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
    info!(
        priority_enable = cfg.admission_priority_enable,
        ranked_proposers = cfg.admission_priority_order.as_ref().map_or(0, |v| v.len()),
        priority_ttl_secs = cfg.admission_priority_ttl_secs,
        "admission priority policy configured"
    );
    if cfg.admission_enforce
        && (cfg.admission_range_max_inflight == 0 || cfg.admission_agg_max_inflight == 0)
    {
        warn!(
            "admission enforce=true with a pool cap of 0 — all requests for that pool will be shed"
        );
    }
    if cfg.admission_priority_enable && !cfg.admission_enforce {
        warn!(
            "admission priority enabled but ENFORCE=false — priority is observed only (would_yield metrics); set GATEWAY_ADMISSION_ENFORCE=true to actually hold slots"
        );
    }
    if cfg.admission_priority_enable && cfg.auth_mode == AuthMode::None {
        warn!(
            "admission priority enabled but GATEWAY_AUTH_MODE=none — all requesters collapse to the zero address, so priority cannot distinguish proposers and is a no-op; set GATEWAY_AUTH_MODE=verify"
        );
    }
}

/// Page limit for the Pending query. Set to the cluster api's server-side clamp
/// (`bin/api/src/service.rs`: `limit.min(1000)`) so that a full page is
/// DETECTABLE: the reconciler treats `len >= PENDING_QUERY_LIMIT` as a possibly
/// truncated (incomplete) view — a `PendingObservation::Partial`, which refreshes
/// the slots it CAN see but never concludes absence for the ones it can't —
/// rather than mass-releasing live slots off an incomplete page. Requesting more
/// than the server clamp would make truncation undetectable (the reply silently
/// caps at the clamp). A single self-hosted cluster's in-flight Pending set is
/// far below this in practice.
const PENDING_QUERY_LIMIT: u32 = 1000;

/// Classify a successful Pending-set reply into a [`PendingObservation`]. A reply
/// AT OR ABOVE `limit` is treated as TRUNCATED (`Partial`) — an incomplete view
/// whose absent committed slots must NOT be concluded gone; a shorter reply is
/// the authoritative `Complete` set. Extracted with an injectable `limit` so the
/// truncation boundary is unit-testable without fabricating `limit` proofs (the
/// production caller passes `PENDING_QUERY_LIMIT`, which MUST equal the cluster
/// api's server-side clamp — see the const's doc).
fn classify_pending(proofs: Vec<cluster_pb::ProofRequest>, limit: usize) -> PendingObservation {
    let truncated = proofs.len() >= limit;
    let ids = proofs.into_iter().map(|p| p.id).collect();
    if truncated {
        PendingObservation::Partial(ids)
    } else {
        PendingObservation::Complete(ids)
    }
}

/// Fetch the cluster's current LIVE (runnable) proof requests: `Pending` AND
/// `deadline >= now`. Shared by the boot-time seed and the per-tick reconcile so
/// both see the same view.
///
/// The `minimum_deadline = now` filter is load-bearing, not cosmetic. `Pending`
/// alone conflates a proof the cluster is actually running/will run with a
/// past-deadline ZOMBIE that no cluster component will ever touch: the
/// coordinator's proof-claimer (`bin/coordinator/src/cluster.rs`) and every
/// fulfillment query (`crates/fulfillment/src/lib.rs`) all gate on
/// `minimum_deadline = now`, so an expired `Pending` row is claimed by no one and
/// consumes zero backend capacity. Admission slots exist to bound proofs that
/// actually consume the shared cluster, so the gate MUST define "occupying" the
/// same way the cluster defines "runnable" — otherwise a stale `Pending` row
/// (e.g. op-succinct abandoned a request_id and re-requested under a new one, and
/// nothing ever moved the old row to a terminal status) wedges a scarce pool
/// forever against a proof that isn't running. Filtering here makes such rows
/// drop out of the live set → reconcile treats them absent → their slots release.
///
/// Returns the cluster client's `eyre::Report` UNWRAPPED (no added context): the
/// client builds it via `?` from the RPC's `tonic::Status`, so the report's root
/// error IS the `Status` and [`pending_query_unimplemented`] can recover the
/// typed gRPC `Code`. Wrapping it here (`.wrap_err`/`anyhow!`) would bury the
/// `Status` and force brittle string matching, so don't.
async fn fetch_pending_proofs(
    cluster: &ClusterServiceClient,
) -> eyre::Result<Vec<cluster_pb::ProofRequest>> {
    // Absolute unix SECONDS, matching how the cluster stores/compares `deadline`
    // (coordinator + fulfillment both pass `now.as_secs()`).
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    cluster
        .get_proof_requests(cluster_pb::ProofRequestListRequest {
            proof_status: vec![cluster_pb::ProofRequestStatus::Pending as i32],
            execution_status: vec![],
            minimum_deadline: Some(now_secs),
            handled: None,
            limit: Some(PENDING_QUERY_LIMIT),
            offset: None,
            scheduled_by: None,
        })
        .await
}

/// Whether a `fetch_pending_proofs` error is a gRPC `Unimplemented` (the backend
/// doesn't support `proof_request_list`), vs any other failure. Keyed on the
/// TYPED `tonic::Code` recovered by downcasting the error chain — the cluster
/// client preserves the `tonic::Status` as the report's source (see
/// [`fetch_pending_proofs`]), so this is exact: a cluster-controlled message that
/// merely mentions the word "unimplemented" (e.g. `Status::internal("…
/// unimplemented feature …")`) can't trip the permanent reconcile-disable, and
/// there is no per-tick string allocation. `chain()` is walked (rather than a
/// bare top-level downcast) so an added context wrapper wouldn't silently break
/// the match.
fn pending_query_unimplemented(e: &eyre::Report) -> bool {
    e.chain().any(|src| {
        src.downcast_ref::<tonic::Status>()
            .is_some_and(|s| s.code() == tonic::Code::Unimplemented)
    })
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
async fn seed_admission_from_cluster(
    admission: &AdmissionController,
    cluster: &ClusterServiceClient,
) -> eyre::Result<usize> {
    let proofs = fetch_pending_proofs(cluster).await?;

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
    // A zero reconcile fetch timeout makes `tokio::time::timeout(0, fetch)` elapse
    // on every tick before the query can return, silently disabling reconcile
    // (the same silent-disable footgun guarded for reap_period==0).
    if cfg.admission_reconcile_fetch_timeout_secs == 0 {
        anyhow::bail!("GATEWAY_ADMISSION_RECONCILE_FETCH_TIMEOUT_SECS must be > 0 (0 makes the reconcile query time out every tick → reconcile never runs)");
    }
    // The reconcile fetch is awaited inline in the reaper loop, so a fetch
    // timeout longer than the reap period would let a slow/half-open cluster
    // stretch the reap (TTL-backstop) cadence to the timeout. Keep it within one
    // reap period.
    if cfg.admission_reconcile_fetch_timeout_secs > cfg.admission_reap_period_secs {
        anyhow::bail!(
            "GATEWAY_ADMISSION_RECONCILE_FETCH_TIMEOUT_SECS ({}) must be <= GATEWAY_ADMISSION_REAP_PERIOD_SECS ({}), else a slow cluster query stalls the reap cadence",
            cfg.admission_reconcile_fetch_timeout_secs,
            cfg.admission_reap_period_secs
        );
    }
    // The TTL is the backstop for committed slots when reconcile can't run; a
    // reap period at or above it means the backstop never fires before the slot
    // would already be considered expired on the sweep it's finally seen — keep
    // the reaper sweeping well inside the TTL.
    if cfg.admission_reap_period_secs >= cfg.admission_slot_ttl_secs {
        anyhow::bail!(
            "GATEWAY_ADMISSION_REAP_PERIOD_SECS ({}) must be < GATEWAY_ADMISSION_SLOT_TTL_SECS ({})",
            cfg.admission_reap_period_secs,
            cfg.admission_slot_ttl_secs
        );
    }
    // Upper-bound the TTL: `Instant::now() + Duration::from_secs(ttl)` panics on
    // overflow, and no realistic slot TTL exceeds a day. Reject an absurd value
    // at boot rather than panicking inside a request handler.
    const MAX_TTL_SECS: u64 = 86_400;
    if cfg.admission_slot_ttl_secs > MAX_TTL_SECS {
        anyhow::bail!(
            "GATEWAY_ADMISSION_SLOT_TTL_SECS must be <= {MAX_TTL_SECS} (got {})",
            cfg.admission_slot_ttl_secs
        );
    }
    // `absent_observations` is the CONSECUTIVE-absence threshold: reconcile
    // releases a committed slot only after it has been seen absent this many
    // times across consecutive COMPLETE reconciles (a present observation or a
    // client poll resets the streak; a skipped reconcile leaves it unchanged).
    // Require >= 2 so a single anomalous empty reply can never release a live
    // slot — the whole point of debouncing. (1 would release on the first absent
    // tick, which is the round-6 over-admit footgun this replaces.)
    if cfg.admission_reconcile_absent_observations < 2 {
        anyhow::bail!(
            "GATEWAY_ADMISSION_RECONCILE_ABSENT_OBSERVATIONS ({}) must be >= 2 (1 would release a slot on the first absent reconcile, defeating debounce)",
            cfg.admission_reconcile_absent_observations
        );
    }
    // The effective debounce window is `absent_observations × reap_period`. If it
    // reaches the slot TTL, the TTL backstop reaper fires before reconcile can
    // confirm sustained absence, making the (prompter, cheaper) reconcile release
    // path dead code. Keep the window strictly inside the TTL.
    if (cfg.admission_reconcile_absent_observations as u64) * cfg.admission_reap_period_secs
        >= cfg.admission_slot_ttl_secs
    {
        anyhow::bail!(
            "GATEWAY_ADMISSION_RECONCILE_ABSENT_OBSERVATIONS ({}) × GATEWAY_ADMISSION_REAP_PERIOD_SECS ({}) must be < GATEWAY_ADMISSION_SLOT_TTL_SECS ({}), else the TTL backstop fires before reconcile can release a gone proof",
            cfg.admission_reconcile_absent_observations,
            cfg.admission_reap_period_secs,
            cfg.admission_slot_ttl_secs
        );
    }
    // The post-commit grace spares a just-committed slot from reconcile until the
    // create leg has had time to register the proof (commit precedes create). If
    // it reaches the slot TTL, the TTL backstop reaps the slot before the grace
    // even elapses, so reconcile could never act on it — the same dead-code class
    // as the debounce-window bound above. Keep it strictly inside the TTL. (0 is
    // allowed: it disables the grace.)
    if cfg.admission_reconcile_commit_grace_secs >= cfg.admission_slot_ttl_secs {
        anyhow::bail!(
            "GATEWAY_ADMISSION_RECONCILE_COMMIT_GRACE_SECS ({}) must be < GATEWAY_ADMISSION_SLOT_TTL_SECS ({}), else the TTL backstop reaps a committed slot before the post-commit grace elapses",
            cfg.admission_reconcile_commit_grace_secs,
            cfg.admission_slot_ttl_secs
        );
    }
    if cfg.admission_priority_enable && cfg.admission_priority_ttl_secs == 0 {
        anyhow::bail!("GATEWAY_ADMISSION_PRIORITY_TTL_SECS must be > 0 when priority is enabled (0 makes every demand instantly stale → priority never fires)");
    }
    // Under enforce, a zero pool cap sheds that entire proof type forever, and a
    // zero global cap sheds EVERY classified proof — a silent proving outage
    // that only a warn() previously guarded (and global==0 wasn't guarded at
    // all). Reject at boot; disabling a proof type is not a realistic intent for
    // a protective throttle. (Dry-run admits regardless, so it is unaffected.)
    if cfg.admission_enforce {
        if cfg.admission_range_max_inflight == 0 {
            anyhow::bail!("GATEWAY_ADMISSION_RANGE_MAX_INFLIGHT must be > 0 when GATEWAY_ADMISSION_ENFORCE=true (0 sheds every range proof)");
        }
        if cfg.admission_agg_max_inflight == 0 {
            anyhow::bail!("GATEWAY_ADMISSION_AGG_MAX_INFLIGHT must be > 0 when GATEWAY_ADMISSION_ENFORCE=true (0 sheds every aggregation proof)");
        }
        if cfg.admission_global_max_inflight == Some(0) {
            anyhow::bail!("GATEWAY_ADMISSION_GLOBAL_MAX_INFLIGHT must be > 0 when set with GATEWAY_ADMISSION_ENFORCE=true (0 sheds every classified proof)");
        }
    }
    let range_vks = parse_vk_hashes(cfg.admission_range_vk_hashes.as_deref())
        .context("GATEWAY_ADMISSION_RANGE_VK_HASHES")?;
    let agg_vks = parse_vk_hashes(cfg.admission_agg_vk_hashes.as_deref())
        .context("GATEWAY_ADMISSION_AGG_VK_HASHES")?;
    let classifier = Classifier::new(range_vks, agg_vks);
    let priorities = parse_priority_order(cfg.admission_priority_order.as_deref())
        .context("GATEWAY_ADMISSION_PRIORITY_ORDER")?;
    Ok(AdmissionController::new(
        classifier,
        cfg.admission_range_max_inflight,
        cfg.admission_agg_max_inflight,
        cfg.admission_global_max_inflight,
        cfg.admission_enforce,
        std::time::Duration::from_secs(cfg.admission_slot_ttl_secs),
        priorities,
        cfg.admission_priority_enable,
        std::time::Duration::from_secs(cfg.admission_priority_ttl_secs),
    )
    .with_commit_grace(std::time::Duration::from_secs(
        cfg.admission_reconcile_commit_grace_secs,
    )))
}

/// Parse `0xADDR:RANK` pairs into a requester→rank map. Lower rank = higher
/// priority; missing entries default to lowest at lookup time.
fn parse_priority_order(
    input: Option<&[String]>,
) -> Result<std::collections::HashMap<Vec<u8>, u32>> {
    let Some(entries) = input else {
        return Ok(Default::default());
    };
    let mut map = std::collections::HashMap::new();
    for raw in entries {
        let s = raw.trim();
        if s.is_empty() {
            continue;
        }
        let (addr, rank) = s
            .split_once(':')
            .with_context(|| format!("PRIORITY_ORDER entry missing ':' rank: {s}"))?;
        let addr = hex::decode(addr.trim().trim_start_matches("0x"))
            .with_context(|| format!("PRIORITY_ORDER invalid address {addr}"))?;
        // Must match the recovered requester width (20-byte Ethereum address),
        // else a listed rank could never key against a real requester.
        if addr.len() != 20 {
            anyhow::bail!("PRIORITY_ORDER address must be 20 bytes: {}", s);
        }
        let rank: u32 = rank
            .trim()
            .parse()
            .with_context(|| format!("PRIORITY_ORDER invalid rank in {s}"))?;
        if map.insert(addr.clone(), rank).is_some() {
            anyhow::bail!("PRIORITY_ORDER duplicate address: {}", hex::encode(&addr));
        }
    }
    Ok(map)
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

    #[test]
    fn build_admission_rejects_absurd_ttl() {
        // An over-large TTL would overflow `Instant::now() + ttl` and panic in a
        // request handler; reject at boot.
        let mut cfg = base_cfg();
        cfg.admission_slot_ttl_secs = u64::MAX;
        assert!(build_admission(&cfg).is_err());
    }

    #[test]
    fn build_admission_rejects_absent_observations_below_two() {
        // 1 (or 0) would release a slot on the first absent reconcile — no
        // debounce, the round-6 over-admit footgun.
        let mut one = base_cfg();
        one.admission_reconcile_absent_observations = 1;
        assert!(build_admission(&one).is_err());
        let mut zero = base_cfg();
        zero.admission_reconcile_absent_observations = 0;
        assert!(build_admission(&zero).is_err());
    }

    #[test]
    fn build_admission_rejects_debounce_window_reaching_ttl() {
        // absent_observations × reap_period must stay below the slot TTL, else the
        // TTL backstop fires before reconcile can release a gone proof (making the
        // reconcile release path dead code).
        let mut cfg = base_cfg();
        cfg.admission_reap_period_secs = 60;
        cfg.admission_slot_ttl_secs = 120;
        cfg.admission_reconcile_absent_observations = 2; // 2×60 = 120 >= 120 → reject
        assert!(build_admission(&cfg).is_err());

        // A window comfortably inside the TTL passes.
        cfg.admission_slot_ttl_secs = 3600; // 2×60 = 120 < 3600
        assert!(build_admission(&cfg).is_ok());
    }

    #[test]
    fn build_admission_rejects_commit_grace_reaching_ttl() {
        // The post-commit grace must stay < ttl, else the TTL reaper fires before
        // the grace elapses (reconcile could never act). 0 is allowed (disabled).
        let mut cfg = base_cfg();
        cfg.admission_reap_period_secs = 10; // keep obs(3)×reap(30) < ttl below
        cfg.admission_slot_ttl_secs = 100;
        cfg.admission_reconcile_commit_grace_secs = 100; // == ttl → reject
        assert!(build_admission(&cfg).is_err());
        cfg.admission_reconcile_commit_grace_secs = 60; // < ttl → ok
        assert!(build_admission(&cfg).is_ok());
        cfg.admission_reconcile_commit_grace_secs = 0; // disabled → ok
        assert!(build_admission(&cfg).is_ok());
    }

    #[test]
    fn classify_pending_marks_at_or_above_limit_as_partial() {
        fn req(id: &str) -> cluster_pb::ProofRequest {
            cluster_pb::ProofRequest {
                id: id.to_string(),
                ..Default::default()
            }
        }
        // Below the limit → Complete (the authoritative full set).
        match classify_pending(vec![req("a")], 2) {
            PendingObservation::Complete(ids) => assert!(ids.contains("a")),
            _ => panic!("an under-limit reply must classify as Complete"),
        }
        // At the limit → Partial (possibly truncated, absence not conclusive).
        assert!(matches!(
            classify_pending(vec![req("a"), req("b")], 2),
            PendingObservation::Partial(_)
        ));
        // Above the limit → Partial.
        assert!(matches!(
            classify_pending(vec![req("a"), req("b"), req("c")], 2),
            PendingObservation::Partial(_)
        ));
    }

    #[test]
    fn pending_query_limit_matches_api_server_clamp() {
        // PENDING_QUERY_LIMIT MUST equal the cluster api's server-side clamp
        // (`bin/api/src/service.rs`: `limit.min(1000)`), else a full page is
        // indistinguishable from a truncated one and truncation goes undetected.
        // This pins the cross-crate coupling: changing one side alone fails here.
        assert_eq!(PENDING_QUERY_LIMIT, 1000);
    }

    #[test]
    fn build_admission_rejects_fetch_timeout_above_reap_period() {
        // The reconcile fetch is awaited inline in the reaper loop, so a fetch
        // timeout longer than the reap period would stretch the whole reaper
        // cadence past its configured value.
        let mut cfg = base_cfg();
        cfg.admission_reap_period_secs = 5;
        cfg.admission_reconcile_fetch_timeout_secs = 10;
        assert!(build_admission(&cfg).is_err());
    }

    #[test]
    fn pending_query_unimplemented_matches_typed_status_only() {
        use tonic::Status;
        // The permanent reconcile-disable must fire ONLY on a real gRPC
        // Unimplemented status, recovered by DOWNCAST of the error chain — not a
        // string match. `Status: Into<eyre::Report>` uses the exact same
        // conversion the cluster client applies (via `?`), so this faithfully
        // mirrors what `fetch_pending_proofs` returns, and doubles as the proof
        // that the typed `Code` survives into the report (plan §111).
        let unimpl: eyre::Report = Status::unimplemented("proof_request_list not supported").into();
        assert!(pending_query_unimplemented(&unimpl));

        // An Internal status whose MESSAGE merely mentions the word must not trip
        // it (the old string-prefix match risked this).
        let internal_word: eyre::Report = Status::internal("unimplemented feature path").into();
        assert!(!pending_query_unimplemented(&internal_word));

        // A transient Unavailable (e.g. transport) must not trip it.
        let unavailable: eyre::Report = Status::unavailable("tcp connect error").into();
        assert!(!pending_query_unimplemented(&unavailable));

        // A non-Status error must not trip it (and must not panic).
        let other: eyre::Report = eyre::eyre!("some other failure");
        assert!(!pending_query_unimplemented(&other));
    }

    #[test]
    fn build_admission_rejects_zero_fetch_timeout() {
        // fetch_timeout==0 makes tokio::time::timeout elapse every tick →
        // reconcile silently never runs.
        let mut cfg = base_cfg();
        cfg.admission_reconcile_fetch_timeout_secs = 0;
        assert!(build_admission(&cfg).is_err());
    }

    #[test]
    fn build_admission_rejects_reap_period_not_below_ttl() {
        // reap_period must sweep well inside the TTL backstop.
        let mut cfg = base_cfg();
        cfg.admission_reap_period_secs = 3600;
        cfg.admission_slot_ttl_secs = 3600;
        assert!(build_admission(&cfg).is_err());
    }

    #[test]
    fn parse_priority_order_pairs_and_rejects_malformed() {
        let a = format!("0x{}", "11".repeat(20)); // 20-byte address, 0x-prefixed
        let b = "22".repeat(20); // 20-byte address, bare hex tolerated
        let ok = parse_priority_order(Some(&[format!("{a}:0"), format!("{b}:1")])).expect("valid");
        assert_eq!(ok.get(&hex::decode("11".repeat(20)).unwrap()), Some(&0));
        assert_eq!(ok.get(&hex::decode("22".repeat(20)).unwrap()), Some(&1));

        assert!(parse_priority_order(Some(&["0xZZ:0".to_string()])).is_err()); // bad hex
                                                                               // wrong length: 20-byte addresses required, a short one is rejected
        assert!(parse_priority_order(Some(&["0x1111:0".to_string()])).is_err());
        assert!(parse_priority_order(Some(&[format!("{a}:notanum")])).is_err()); // bad rank
        assert!(parse_priority_order(Some(std::slice::from_ref(&a))).is_err()); // no rank
        assert!(parse_priority_order(Some(&[format!("{a}:0"), format!("{a}:1")])).is_err()); // duplicate address
        assert!(parse_priority_order(None).unwrap().is_empty());
    }

    #[test]
    fn build_admission_rejects_zero_cap_under_enforce() {
        // Under enforce, a zero pool/global cap sheds an entire proof type (or
        // everything) forever — reject at boot.
        let mut range0 = base_cfg();
        range0.admission_enforce = true;
        range0.admission_range_max_inflight = 0;
        assert!(build_admission(&range0).is_err());

        let mut agg0 = base_cfg();
        agg0.admission_enforce = true;
        agg0.admission_agg_max_inflight = 0;
        assert!(build_admission(&agg0).is_err());

        let mut global0 = base_cfg();
        global0.admission_enforce = true;
        global0.admission_global_max_inflight = Some(0);
        assert!(build_admission(&global0).is_err());

        // Dry-run admits regardless of caps, so a zero cap is harmless there.
        let mut dry = base_cfg();
        dry.admission_enforce = false;
        dry.admission_range_max_inflight = 0;
        dry.admission_global_max_inflight = Some(0);
        assert!(build_admission(&dry).is_ok());
    }

    #[test]
    fn build_admission_rejects_zero_priority_ttl_when_enabled() {
        // A zero priority TTL makes every demand instantly stale → priority
        // never fires. Reject at boot when priority is enabled.
        let mut cfg = base_cfg();
        cfg.admission_priority_enable = true;
        cfg.admission_priority_ttl_secs = 0;
        assert!(build_admission(&cfg).is_err());
        // ...but a zero TTL is harmless (ignored) while priority is disabled.
        let mut cfg_off = base_cfg();
        cfg_off.admission_priority_enable = false;
        cfg_off.admission_priority_ttl_secs = 0;
        assert!(build_admission(&cfg_off).is_ok());
    }
}
