use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use sp1_cluster_common::proto::{TaskType, WorkerType};
use spn_metrics::{
    metrics::{
        self, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
    },
    server::{MetricServer, MetricServerConfig},
    version::VersionInfo,
    Metrics,
};
use tokio::{sync::oneshot, task::JoinHandle};

use crate::BUILD_VERSION;

/// Coordinator metrics
#[derive(Clone, Metrics)]
#[metrics(scope = "cluster_coordinator")]
pub struct CoordinatorMetrics {}

impl CoordinatorMetrics {
    /// Create a new instance of CoordinatorMetrics
    pub fn new() -> Self {
        Default::default()
    }

    /// Increment counter when a worker is detected as dead (heartbeat timeout).
    /// Observability-only — no behavior change.
    pub fn increment_dead_workers(&self, worker_type: WorkerType) {
        describe_counter!(
            "coordinator_dead_workers_total",
            "Number of workers detected as dead via heartbeat timeout.",
        );
        counter!(
            "coordinator_dead_workers_total",
            "worker_type" => format!("{worker_type:?}"),
        )
        .increment(1);
    }

    /// Record the age of a worker's last heartbeat at the moment dead-worker cleanup
    /// removed it. By construction this is >= `WORKER_HEARTBEAT_TIMEOUT` (currently 30s)
    /// because the cleanup only fires once that threshold is crossed. The variation
    /// above the threshold reflects the cleanup poll cadence + scheduling jitter, NOT a
    /// pure "death-to-detection" lag (the worker may have stopped emitting heartbeats
    /// up to one heartbeat interval before its true death). Observability-only.
    pub fn record_dead_worker_heartbeat_age(&self, worker_type: WorkerType, secs: f64) {
        describe_histogram!(
            "coordinator_dead_worker_heartbeat_age_seconds",
            "Seconds between a worker's last heartbeat and dead-worker cleanup (>= WORKER_HEARTBEAT_TIMEOUT).",
        );
        histogram!(
            "coordinator_dead_worker_heartbeat_age_seconds",
            "worker_type" => format!("{worker_type:?}"),
        )
        .record(secs);
    }

    /// Increment counter when a task is requeued via the dead-worker cleanup path.
    /// Observability-only. Does NOT change retry budget semantics.
    pub fn increment_dead_worker_requeues(&self, worker_type: WorkerType, task_type: TaskType) {
        describe_counter!(
            "coordinator_dead_worker_requeues_total",
            "Number of task re-enqueues triggered by dead-worker cleanup.",
        );
        counter!(
            "coordinator_dead_worker_requeues_total",
            "worker_type" => format!("{worker_type:?}"),
            "task_type" => format!("{task_type:?}"),
        )
        .increment(1);
    }

    /// Increment counter when `remove_worker_internal` is called for a worker
    /// that's already gone from `state.workers`. Indicates a defensive early return
    /// rather than an `unwrap()` panic. Observability-only.
    pub fn increment_orphan_worker_removals(&self, path: &'static str) {
        describe_counter!(
            "coordinator_orphan_worker_removals_total",
            "remove_worker_internal called for a worker that's already absent from state.",
        );
        counter!(
            "coordinator_orphan_worker_removals_total",
            "path" => path,
        )
        .increment(1);
    }

    /// Increment counter when a dead-worker active-task entry references a missing proof.
    /// Observability-only. Indicates the proof was cleaned up concurrently.
    pub fn increment_dead_worker_missing_proof(&self) {
        describe_counter!(
            "coordinator_dead_worker_missing_proof_total",
            "Dead-worker cleanup found an active-task entry whose proof no longer exists.",
        );
        counter!("coordinator_dead_worker_missing_proof_total").increment(1);
    }

    /// Increment counter when a dead-worker active-task entry references a missing task
    /// within an existing proof. Observability-only. Indicates a state inconsistency.
    pub fn increment_dead_worker_missing_task(&self) {
        describe_counter!(
            "coordinator_dead_worker_missing_task_total",
            "Dead-worker cleanup found an active-task entry whose task no longer exists in the proof.",
        );
        counter!("coordinator_dead_worker_missing_task_total").increment(1);
    }

    /// Proofs failed because one execution submitted more shards than the per-proof limit.
    pub fn increment_proofs_over_shard_limit(&self) {
        describe_counter!(
            "coordinator_proofs_over_shard_limit_total",
            "Proofs failed as unexecutable for exceeding COORDINATOR_MAX_SHARDS_PER_PROOF.",
        );
        counter!("coordinator_proofs_over_shard_limit_total").increment(1);
    }

    /// Shard tasks that matched no CoreExecute of their proof, so the shard limit skipped them.
    /// Expected to stay 0; anything else means the task layout changed upstream.
    pub fn increment_unmatched_shards(&self) {
        describe_counter!(
            "coordinator_unmatched_shards_total",
            "Shard tasks the per-proof shard limit could not attribute to an execution.",
        );
        counter!("coordinator_unmatched_shards_total").increment(1);
    }

    /// A proof got a second CoreExecute task for the same run, from a submission retried
    /// after a coordinator stall. Expected rare; see shards.rs on its effect on the limit.
    pub fn increment_duplicate_executions(&self) {
        describe_counter!(
            "coordinator_duplicate_executions_total",
            "CoreExecute tasks created twice for one controller run (retried submissions).",
        );
        counter!("coordinator_duplicate_executions_total").increment(1);
    }

    /// Shards of the largest single execution of a proof, when it completes or fails. Use it
    /// to set COORDINATOR_MAX_SHARDS_PER_PROOF and see the headroom under it.
    pub fn record_proof_execution_shards(&self, outcome: &'static str, shards: u64) {
        if shards == 0 {
            return;
        }
        describe_histogram!(
            "coordinator_proof_execution_shards",
            "Shards submitted by the largest single execution of a proof, by outcome.",
        );
        histogram!("coordinator_proof_execution_shards", "outcome" => outcome)
            .record(shards as f64);
    }

    /// Artifact ids of failed proofs the coordinator sent for deletion. Counts ids in delete
    /// calls that succeeded, not keys Redis confirmed removing.
    pub fn increment_failed_proof_cleanup_ids(&self, count: u64) {
        describe_counter!(
            "coordinator_failed_proof_cleanup_ids_total",
            "Artifact ids of failed proofs sent for deletion in delete calls that succeeded.",
        );
        counter!("coordinator_failed_proof_cleanup_ids_total").increment(count);
    }

    /// Delete calls for a failed proof's artifacts that failed (those ids expire with the TTL).
    pub fn increment_failed_proof_cleanup_errors(&self) {
        describe_counter!(
            "coordinator_failed_proof_cleanup_errors_total",
            "Failed deletions of a failed proof's artifacts; those artifacts expire with the TTL.",
        );
        counter!("coordinator_failed_proof_cleanup_errors_total").increment(1);
    }

    /// Claimer re-issued a terminal status write that an earlier completion lost. A nonzero rate
    /// means completion writes are being dropped (e.g. a cluster-DB blip). Observability-only.
    pub fn increment_reissued_status_writes(&self) {
        describe_counter!(
            "coordinator_reissued_status_writes_total",
            "Terminal proof-status writes re-issued by the claimer after an earlier write was lost.",
        );
        counter!("coordinator_reissued_status_writes_total").increment(1);
    }

    /// Set the subscription gauges: handler entries held by the worker service
    /// (one per sub_id) and subscribers held by the coordinator. A handler can
    /// outlive its subscriber for ~20s while it finishes resending unacked
    /// messages, so handlers briefly above subscribers is normal; a gap that
    /// persists across many samples means handler entries are leaking. Sampled
    /// periodically, so values can lag by one sample interval. Observability-only.
    pub fn set_subscription_counts(&self, handlers: usize, subscribers: usize) {
        describe_gauge!(
            "coordinator_sub_handlers",
            "Subscriber handler entries held by the coordinator worker service.",
        );
        gauge!("coordinator_sub_handlers").set(handlers as f64);
        describe_gauge!(
            "coordinator_subscribers",
            "Subscribers tracked by the coordinator.",
        );
        gauge!("coordinator_subscribers").set(subscribers as f64);
    }
}

pub async fn initialize_metrics(
    addr: SocketAddr,
) -> Result<(
    Arc<CoordinatorMetrics>,
    Option<JoinHandle<()>>,
    tokio::sync::broadcast::Sender<()>,
)> {
    // Initialize metrics server first
    let version_info = VersionInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        build_timestamp: BUILD_VERSION.to_string(),
        cargo_features: option_env!("CARGO_FEATURES").unwrap_or("").to_string(),
        git_sha: option_env!("CARGO_GIT_SHA").unwrap_or("").to_string(),
        target_triple: option_env!("TARGET").unwrap_or("unknown").to_string(),
        build_profile: {
            #[cfg(debug_assertions)]
            let profile = "debug";
            #[cfg(not(debug_assertions))]
            let profile = "release";
            profile.to_string()
        },
    };

    let (ready_tx, ready_rx) = oneshot::channel();

    let metrics_server_config =
        MetricServerConfig::new(addr, version_info, "sp1-coordinator".to_string())
            .with_ready_signal(ready_tx);

    let metrics_server = MetricServer::new(metrics_server_config);

    // Spawn metrics server
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let metrics_server_handle = Some(tokio::spawn(async move {
        if let Err(e) = metrics_server.serve(shutdown_rx).await {
            tracing::error!("metrics server error: {:?}", e);
        }
    }));

    // Wait for metrics server to be ready
    match ready_rx.await {
        Ok(_) => tracing::info!("metrics server is ready"),
        Err(e) => tracing::warn!("failed to receive metrics server ready signal: {}", e),
    }

    // Initialize metrics after server is ready
    let metrics = Arc::new(CoordinatorMetrics::new());

    Ok((metrics, metrics_server_handle, shutdown_tx))
}
