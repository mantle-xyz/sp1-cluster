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
        counter!("coordinator_proofs_over_shard_limit_total").increment(1);
    }

    /// Shard tasks that matched no CoreExecute of their proof, so the shard limit skipped them.
    /// Expected to stay 0; anything else means the task layout changed upstream.
    pub fn increment_unmatched_shards(&self) {
        counter!("coordinator_unmatched_shards_total").increment(1);
    }

    /// A proof got a second CoreExecute task for the same run, from a submission retried
    /// after a coordinator stall. Expected rare; see shards.rs on its effect on the limit.
    pub fn increment_duplicate_executions(&self) {
        counter!("coordinator_duplicate_executions_total").increment(1);
    }

    /// Shards of the largest single execution of a proof, when it completes or fails. Use it
    /// to set COORDINATOR_MAX_SHARDS_PER_PROOF and see the headroom under it.
    ///
    /// Exported as a Prometheus histogram built from counters (`_bucket{le}`, `_sum`, `_count`),
    /// queried with `histogram_quantile`. Not a `histogram!`: the spn-metrics recorder renders
    /// those as summaries whose quantiles cover only the last minute or so, and proofs finish
    /// minutes apart, so the quantiles read 0 almost always.
    pub fn record_proof_execution_shards(&self, outcome: &'static str, shards: u64) {
        if shards == 0 {
            return;
        }
        for le in execution_shard_buckets_at_or_above(shards) {
            counter!(EXECUTION_SHARDS_BUCKET, "outcome" => outcome, "le" => le).increment(1);
        }
        counter!(EXECUTION_SHARDS_SUM, "outcome" => outcome).increment(shards);
        counter!(EXECUTION_SHARDS_COUNT, "outcome" => outcome).increment(1);
    }

    /// Describe and export every counter at 0, so a counter's first increment is a step from 0
    /// that `increase()`/`rate()` see (Prometheus does not count a series' first sample).
    pub fn register(&self) {
        for (name, help) in COUNTERS {
            describe_counter!(*name, *help);
            counter!(*name).increment(0);
        }
        describe_counter!(
            EXECUTION_SHARDS_BUCKET,
            "Proofs whose largest single execution submitted at most `le` shards, by outcome.",
        );
        describe_counter!(
            EXECUTION_SHARDS_SUM,
            "Shards of the largest single execution, summed over proofs, by outcome.",
        );
        describe_counter!(
            EXECUTION_SHARDS_COUNT,
            "Proofs recorded in coordinator_proof_execution_shards, by outcome.",
        );
        for outcome in ["completed", "failed"] {
            for (le, _) in EXECUTION_SHARD_BUCKETS {
                counter!(EXECUTION_SHARDS_BUCKET, "outcome" => outcome, "le" => *le).increment(0);
            }
            counter!(EXECUTION_SHARDS_SUM, "outcome" => outcome).increment(0);
            counter!(EXECUTION_SHARDS_COUNT, "outcome" => outcome).increment(0);
        }
    }

    /// Artifact ids of failed proofs the coordinator sent for deletion. Counts ids in delete
    /// calls that succeeded, not keys Redis confirmed removing.
    pub fn increment_failed_proof_cleanup_ids(&self, count: u64) {
        counter!("coordinator_failed_proof_cleanup_ids_total").increment(count);
    }

    /// Delete calls for a failed proof's artifacts that failed (those ids expire with the TTL).
    pub fn increment_failed_proof_cleanup_errors(&self) {
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

const EXECUTION_SHARDS_BUCKET: &str = "coordinator_proof_execution_shards_bucket";
const EXECUTION_SHARDS_SUM: &str = "coordinator_proof_execution_shards_sum";
const EXECUTION_SHARDS_COUNT: &str = "coordinator_proof_execution_shards_count";

/// Upper bounds of `coordinator_proof_execution_shards_bucket`, as (`le` label, bound). Finer
/// where a shard limit is likely to sit: ranges run around 3k shards, the planned limit is 52k.
const EXECUTION_SHARD_BUCKETS: &[(&str, u64)] = &[
    ("1000", 1_000),
    ("2000", 2_000),
    ("3000", 3_000),
    ("4000", 4_000),
    ("6000", 6_000),
    ("8000", 8_000),
    ("10000", 10_000),
    ("15000", 15_000),
    ("20000", 20_000),
    ("30000", 30_000),
    ("40000", 40_000),
    ("52000", 52_000),
    ("70000", 70_000),
    ("100000", 100_000),
    ("+Inf", u64::MAX),
];

/// The `le` labels a value of `shards` counts in: every bucket whose bound is at least it
/// (Prometheus buckets are cumulative).
fn execution_shard_buckets_at_or_above(shards: u64) -> impl Iterator<Item = &'static str> {
    EXECUTION_SHARD_BUCKETS
        .iter()
        .filter(move |(_, bound)| shards <= *bound)
        .map(|(le, _)| *le)
}

/// Counters exported at 0 from startup by [`CoordinatorMetrics::register`]. The alert rules on
/// these use `increase()`, which needs a sample before the first increment.
const COUNTERS: &[(&str, &str)] = &[
    (
        "coordinator_proofs_over_shard_limit_total",
        "Proofs failed as unexecutable for exceeding COORDINATOR_MAX_SHARDS_PER_PROOF.",
    ),
    (
        "coordinator_unmatched_shards_total",
        "Shard tasks the per-proof shard limit could not attribute to an execution.",
    ),
    (
        "coordinator_duplicate_executions_total",
        "CoreExecute tasks created twice for one controller run (retried submissions).",
    ),
    (
        "coordinator_failed_proof_cleanup_ids_total",
        "Artifact ids of failed proofs sent for deletion in delete calls that succeeded.",
    ),
    (
        "coordinator_failed_proof_cleanup_errors_total",
        "Failed deletions of a failed proof's artifacts; those artifacts expire with the TTL.",
    ),
];

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
    metrics.register();

    Ok((metrics, metrics_server_handle, shutdown_tx))
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_exporter_prometheus::PrometheusBuilder;

    /// Render what `f` records through the exporter spn-metrics installs.
    fn render(f: impl FnOnce(&CoordinatorMetrics)) -> String {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || f(&CoordinatorMetrics::new()));
        handle.render()
    }

    fn line<'a>(rendered: &'a str, prefix: &str) -> &'a str {
        rendered
            .lines()
            .find(|l| l.starts_with(prefix))
            .unwrap_or_else(|| panic!("no line {prefix} in\n{rendered}"))
    }

    #[test]
    fn buckets_are_cumulative() {
        let le: Vec<_> = execution_shard_buckets_at_or_above(10_001).collect();
        assert_eq!(le.first(), Some(&"15000"));
        assert_eq!(le.last(), Some(&"+Inf"));
        assert_eq!(
            execution_shard_buckets_at_or_above(10_000).next(),
            Some("10000"),
            "a value on a bound counts in that bucket"
        );
        assert_eq!(execution_shard_buckets_at_or_above(u64::MAX).count(), 1);
        assert!(EXECUTION_SHARD_BUCKETS.windows(2).all(|w| w[0].1 < w[1].1));
    }

    /// The shape `histogram_quantile` needs: `_bucket{le}` cumulative, `_sum`, `_count`, kept
    /// as counters (no rolling window).
    #[test]
    fn execution_shards_render_as_a_prometheus_histogram() {
        let out = render(|m| {
            m.register();
            m.record_proof_execution_shards("completed", 3_000);
            m.record_proof_execution_shards("completed", 10_001);
        });
        let bucket = |le: &str| {
            line(
                &out,
                &format!(
                    "coordinator_proof_execution_shards_bucket{{outcome=\"completed\",le=\"{le}\"}}"
                ),
            )
            .rsplit(' ')
            .next()
            .unwrap()
            .to_string()
        };
        assert_eq!(bucket("2000"), "0".to_string(), "{out}");
        assert_eq!(bucket("3000"), "1");
        assert_eq!(bucket("10000"), "1");
        assert_eq!(bucket("15000"), "2");
        assert_eq!(bucket("+Inf"), "2");
        // `register()` also exports the "failed" series, and the exporter's order is not fixed:
        // match the full label set.
        assert!(line(
            &out,
            "coordinator_proof_execution_shards_sum{outcome=\"completed\"}"
        )
        .ends_with(" 13001"));
        assert!(line(
            &out,
            "coordinator_proof_execution_shards_count{outcome=\"completed\"}"
        )
        .ends_with(" 2"));
        assert!(!out.contains("quantile="), "{out}");
    }

    /// Every alerted counter is exported at 0 before anything happens.
    #[test]
    fn registered_counters_start_at_zero() {
        let out = render(|m| m.register());
        for (name, _) in COUNTERS {
            assert!(
                out.lines().any(|l| l == format!("{name} 0")),
                "{name} missing in\n{out}"
            );
        }
        assert!(out.contains(
            "coordinator_proof_execution_shards_bucket{outcome=\"failed\",le=\"+Inf\"} 0"
        ));
    }
}
