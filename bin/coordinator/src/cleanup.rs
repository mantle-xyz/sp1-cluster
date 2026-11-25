//! Deleting a failed proof's artifacts.
//!
//! Workers delete artifacts only on the success path, so a proof that fails, expires or is
//! cancelled leaves its shard records, shard proofs and intermediate proofs in the artifact
//! store until their TTL. An oversized range that is rejected mid-proof can leave tens of GB in
//! Redis, and retries pile up on top of it.
//!
//! Only artifacts created during this run of the proof are deleted:
//! - The root task (`Controller`, or `ExecuteOnly`) carries the request's artifacts: program,
//!   stdin and the final proof output. Those come from outside the run and may be shared or
//!   still needed: every proof of a program uses the same program artifact, an SDK retry of
//!   `request_proof` reuses the same stdin for a second request, and a proof can be torn down
//!   after it completed. They are never deleted, wherever they appear (the program is also every
//!   shard's `inputs[0]`).
//! - Every other task was spawned by this run, and the artifacts it names beyond the root's were
//!   minted by this run's controller and workers (`create_artifact`): common input, execution
//!   output, shard records, shard proofs, reduce and wrap outputs. Those are deleted.
//! - Only ids named `artifact_*` are deleted, which also skips the non-artifact strings tasks
//!   carry as inputs (cycle limit, proof mode, metadata JSON, task ids).
//!
//! Not covered, left to the TTL:
//! - artifacts no task names: precompile event chunks (referenced from inside shard records),
//!   outputs written after the proof was torn down, and a shard record uploaded for a task the
//!   coordinator refused because the proof was already gone;
//! - cleanups still waiting out the delay when the coordinator stops, and proofs failed while
//!   it is shutting down (`fail_proof_internal` does nothing then);
//! - proofs that failed under a previous coordinator process;
//! - ids in a delete call that failed, and those not tried after the cleanup gave up on a
//!   stalled store;
//! - `refs:<id>` keys (the delete removes `<id>` and `<id>:chunks`).

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

use sp1_cluster_common::proto::TaskType;

use crate::{metrics::CoordinatorMetrics, store::ArtifactStore, AssignmentPolicy, Proof};

/// How long to wait after a proof fails before deleting its artifacts, so the tasks it
/// cancelled have stopped reading them.
pub const CLEANUP_DELAY: Duration = Duration::from_secs(30);

/// Ids per delete call. Each id is two keys (`<id>` and `<id>:chunks`), and Redis frees a
/// string value on its main thread even through UNLINK, so a batch of multi-MB shard records
/// holds Redis for a moment. Small batches with a pause keep that short.
const DELETE_BATCH: usize = 200;

/// Pause between delete calls.
const DELETE_PAUSE: Duration = Duration::from_millis(20);

/// Consecutive timed-out delete calls ([`crate::store::STORE_OP_TIMEOUT`]) after which a cleanup
/// gives up: the store is stalled, and every further call would wait out the timeout and leave
/// its UNLINKs running detached. A refused call fails fast and does not count.
const MAX_CONSECUTIVE_TIMEOUTS: usize = 3;

/// Prefix of the ids the artifact clients mint.
const ARTIFACT_PREFIX: &str = "artifact_";

fn is_root(task_type: TaskType) -> bool {
    matches!(task_type, TaskType::Controller | TaskType::ExecuteOnly)
}

/// The artifacts this run of the proof created, which are safe to delete when it fails. Empty
/// if the proof has no root task, since then nothing tells the run's artifacts apart.
pub fn failed_proof_artifacts<P: AssignmentPolicy>(proof: &Proof<P>) -> Vec<String> {
    fn io<P: AssignmentPolicy>(task: &crate::Task<P>) -> impl Iterator<Item = &str> {
        task.data
            .inputs
            .iter()
            .chain(task.data.outputs.iter())
            .map(String::as_str)
    }
    let roots: Vec<_> = proof
        .tasks
        .values()
        .filter(|t| is_root(t.data.task_type()))
        .collect();
    if roots.is_empty() {
        return vec![];
    }
    let request: HashSet<&str> = roots.iter().flat_map(|t| io(t)).collect();
    proof
        .tasks
        .values()
        .filter(|t| !is_root(t.data.task_type()))
        .flat_map(io)
        .filter(|id| id.starts_with(ARTIFACT_PREFIX) && !request.contains(id))
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// In the background: collect the failed proof's artifacts, release the proof, and delete them
/// after `delay`. Only the ids are kept through the delay; a large proof's task map is far
/// bigger than its id list.
pub fn spawn_cleanup<P: AssignmentPolicy>(
    cleaner: Arc<dyn ArtifactStore>,
    metrics: Option<Arc<CoordinatorMetrics>>,
    proof: Proof<P>,
    delay: Duration,
) {
    tokio::spawn(async move {
        let proof_id = proof.id.clone();
        // A large proof has hundreds of thousands of tasks; collect and drop it off the runtime.
        let ids = match tokio::task::spawn_blocking(move || failed_proof_artifacts(&proof)).await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!("collecting artifacts of failed proof {proof_id} failed: {e}");
                return;
            }
        };
        if ids.is_empty() {
            return;
        }
        tokio::time::sleep(delay).await;
        // A failed batch is skipped (its ids expire with the TTL) and the rest still go: a Redis
        // that refuses connections (restarting, failing over) fails each call fast, and the
        // batches after it recovers are deleted. Only a stalled Redis is given up on.
        let (mut sent, mut failed, mut timeouts) = (0usize, 0usize, 0usize);
        let mut last_error = None;
        for (n, batch) in ids.chunks(DELETE_BATCH).enumerate() {
            if n > 0 {
                tokio::time::sleep(DELETE_PAUSE).await;
            }
            match cleaner.delete(batch.to_vec()).await {
                Ok(()) => {
                    sent += batch.len();
                    timeouts = 0;
                }
                Err(e) => {
                    failed += batch.len();
                    timeouts = if crate::store::is_timeout(&e) {
                        timeouts + 1
                    } else {
                        0
                    };
                    last_error = Some(e);
                    if timeouts >= MAX_CONSECUTIVE_TIMEOUTS {
                        break;
                    }
                }
            }
        }
        // `sent` counts ids in delete calls that succeeded, not keys Redis removed: UNLINK of an
        // absent key also succeeds (e.g. COORDINATOR_REDIS_NODES ordered differently from the
        // workers' NODE_REDIS_NODES, which routes each id to the wrong node).
        match &last_error {
            None => tracing::info!(
                "cleanup of failed proof {proof_id}: {sent} artifact ids sent for deletion"
            ),
            // "failed to delete" is what the cleanup-errors alert tells operators to grep for.
            Some(e) => tracing::warn!(
                "failed to delete artifacts of failed proof {proof_id}: {e:#}; {sent} of {} ids \
                 sent, {failed} failed, {} not tried; those expire with the TTL",
                ids.len(),
                ids.len() - sent - failed
            ),
        }
        if let Some(m) = &metrics {
            m.increment_failed_proof_cleanup_ids(sent as u64);
            if last_error.is_some() {
                m.increment_failed_proof_cleanup_errors();
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{policy::default::DefaultPolicy, Task};
    use futures::future::BoxFuture;
    use sp1_cluster_common::proto::{TaskData, TaskStatus};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::SystemTime;

    fn task(
        id: &str,
        task_type: TaskType,
        inputs: &[&str],
        outputs: &[&str],
    ) -> Task<DefaultPolicy> {
        Task {
            id: id.into(),
            data: TaskData {
                task_type: task_type as i32,
                inputs: inputs.iter().map(|s| s.to_string()).collect(),
                outputs: outputs.iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            },
            created_at: SystemTime::now(),
            status: TaskStatus::Pending,
            retries: 0,
            subscribers: std::collections::HashSet::new(),
            worker: None,
            gpu_attempts: HashMap::new(),
            dead_worker_requeue_count: 0,
            extra: (),
        }
    }

    fn proof(tasks: Vec<Task<DefaultPolicy>>) -> Proof<DefaultPolicy> {
        let mut proof = Proof::new("p".into(), None, ());
        for t in tasks {
            proof.tasks.insert(t.id.clone(), t);
        }
        proof
    }

    /// CoreExecute's `inputs[3]`: sp1-prover passes the metadata JSON itself, not an artifact.
    const METADATA: &str = r#"{"num_deferred_proofs":0}"#;

    /// Task shapes as the claimer and sp1-prover build them. `program` is an `artifact_` id
    /// here, as the fulfiller names it, to show it is kept because the root names it, not
    /// because of its prefix.
    fn production_shaped(stdin: &str) -> Vec<Task<DefaultPolicy>> {
        let metadata = r#"{"stdin_private":false}"#;
        vec![
            task(
                "controller",
                TaskType::Controller,
                &["artifact_program", stdin, "2", "1000000", "", metadata],
                &["artifact_final_proof"],
            ),
            task(
                "exec",
                TaskType::CoreExecute,
                &["artifact_program", stdin, "artifact_ci", METADATA],
                &["artifact_exec_out"],
            ),
            task(
                "shard",
                TaskType::ProveShard,
                &[
                    "artifact_program",
                    "artifact_ci",
                    "artifact_rec",
                    "task_marker",
                ],
                &["artifact_shard_proof", "artifact_deferred"],
            ),
            task(
                "reduce",
                TaskType::RecursionReduce,
                &["artifact_shard_proof"],
                &["artifact_reduced"],
            ),
        ]
    }

    #[test]
    fn only_artifacts_created_by_this_run_are_collected() {
        assert_eq!(
            failed_proof_artifacts(&proof(production_shaped("artifact_stdin"))),
            vec![
                "artifact_ci",
                "artifact_deferred",
                "artifact_exec_out",
                "artifact_rec",
                "artifact_reduced",
                "artifact_shard_proof",
            ]
        );
    }

    /// An SDK retry of request_proof makes a second proof on the same stdin: failing one must
    /// not delete the other's input.
    #[test]
    fn the_request_artifacts_are_kept() {
        let ids = failed_proof_artifacts(&proof(production_shaped("artifact_shared_stdin")));
        assert!(!ids.contains(&"artifact_shared_stdin".to_string()));
        assert!(!ids.contains(&"artifact_program".to_string()));
        assert!(!ids.contains(&"artifact_final_proof".to_string()));
    }

    /// ExecuteOnly runs one task on the request's artifacts: nothing of its own to delete.
    #[test]
    fn an_execute_only_proof_deletes_nothing() {
        let p = proof(vec![task(
            "eo",
            TaskType::ExecuteOnly,
            &["artifact_program", "artifact_stdin", "2", "100", "", "{}"],
            &["artifact_final_proof"],
        )]);
        assert!(failed_proof_artifacts(&p).is_empty());
    }

    /// Without the root task nothing tells the run's artifacts from the request's.
    #[test]
    fn a_proof_without_a_root_task_deletes_nothing() {
        let mut tasks = production_shaped("artifact_stdin");
        tasks.retain(|t| t.id != "controller");
        assert!(failed_proof_artifacts(&proof(tasks)).is_empty());
    }

    struct Recorder(Mutex<Vec<Vec<String>>>);
    impl ArtifactStore for Recorder {
        fn delete(&self, ids: Vec<String>) -> BoxFuture<'_, anyhow::Result<()>> {
            self.0.lock().unwrap().push(ids);
            Box::pin(async { Ok(()) })
        }
        fn execution_cycles(&self, _: String) -> BoxFuture<'_, anyhow::Result<u64>> {
            Box::pin(async { anyhow::bail!("not recorded") })
        }
    }

    /// Counts delete calls; `fail(n)` decides call n (1-based): `Some(true)` times out,
    /// `Some(false)` is refused, `None` succeeds.
    struct Flaky {
        calls: Mutex<usize>,
        fail: fn(usize) -> Option<bool>,
    }
    impl Flaky {
        fn new(fail: fn(usize) -> Option<bool>) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(0),
                fail,
            })
        }
        fn calls(&self) -> usize {
            *self.calls.lock().unwrap()
        }
    }
    impl ArtifactStore for Flaky {
        fn delete(&self, _: Vec<String>) -> BoxFuture<'_, anyhow::Result<()>> {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            let outcome = (self.fail)(*calls);
            Box::pin(async move {
                match outcome {
                    Some(true) => Err(anyhow::Error::new(crate::store::StoreTimeout)),
                    Some(false) => anyhow::bail!("connection refused"),
                    None => Ok(()),
                }
            })
        }
        fn execution_cycles(&self, _: String) -> BoxFuture<'_, anyhow::Result<u64>> {
            Box::pin(async { anyhow::bail!("not recorded") })
        }
    }

    /// 2506 ids in batches of 200.
    const BATCHES: usize = 2506usize.div_ceil(DELETE_BATCH);

    async fn run(store: Arc<Flaky>, metrics: Option<Arc<CoordinatorMetrics>>) {
        spawn_cleanup(store, metrics, many_records(), CLEANUP_DELAY);
        tokio::time::sleep(CLEANUP_DELAY + Duration::from_secs(5)).await;
    }

    /// A stalled store is given up on instead of costing a timeout per remaining batch.
    #[tokio::test(start_paused = true)]
    async fn cleanup_gives_up_on_a_stalled_store() {
        let store = Flaky::new(|_| Some(true));
        run(store.clone(), None).await;
        assert_eq!(store.calls(), MAX_CONSECUTIVE_TIMEOUTS);
    }

    /// A store refusing connections (restarting, failing over) fails fast; every batch is still
    /// tried, so those after it recovers are deleted.
    #[tokio::test(start_paused = true)]
    async fn a_refusing_store_is_tried_for_every_batch() {
        let store = Flaky::new(|n| (n <= 5).then_some(false));
        run(store.clone(), None).await;
        assert_eq!(store.calls(), BATCHES);
    }

    /// Only consecutive timeouts give up: a success in between starts the count again.
    #[tokio::test(start_paused = true)]
    async fn timeouts_with_successes_between_do_not_give_up() {
        let store = Flaky::new(|n| (n % 3 != 0).then_some(true));
        run(store.clone(), None).await;
        assert_eq!(store.calls(), BATCHES);
    }

    /// However many batches fail, a proof's cleanup counts one error.
    #[tokio::test(start_paused = true)]
    async fn a_failed_cleanup_counts_one_error() {
        let recorder: &'static _ = Box::leak(Box::new(
            metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder(),
        ));
        let handle = recorder.handle();
        let _guard = spn_metrics::metrics::set_default_local_recorder(recorder);
        let store = Flaky::new(|_| Some(false));
        run(store.clone(), Some(Arc::new(CoordinatorMetrics::new()))).await;
        assert_eq!(store.calls(), BATCHES);
        assert!(handle
            .render()
            .lines()
            .any(|l| l == "coordinator_failed_proof_cleanup_errors_total 1"));
    }

    /// The production-shaped tasks plus one task naming 2500 records: 2506 ids.
    fn many_records() -> Proof<DefaultPolicy> {
        let mut tasks = production_shaped("artifact_stdin");
        let records: Vec<String> = (0..2500).map(|n| format!("artifact_rec_{n:05}")).collect();
        let refs: Vec<&str> = records.iter().map(String::as_str).collect();
        tasks.push(task("many", TaskType::ProveShard, &refs, &[]));
        proof(tasks)
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_waits_for_the_delay_and_deletes_in_batches() {
        let mut tasks = production_shaped("artifact_stdin");
        let records: Vec<String> = (0..2500).map(|n| format!("artifact_rec_{n:05}")).collect();
        let refs: Vec<&str> = records.iter().map(String::as_str).collect();
        tasks.push(task("many", TaskType::ProveShard, &refs, &[]));

        let recorder = Arc::new(Recorder(Mutex::new(vec![])));
        spawn_cleanup(recorder.clone(), None, proof(tasks), CLEANUP_DELAY);
        tokio::time::sleep(CLEANUP_DELAY - Duration::from_secs(1)).await;
        assert!(
            recorder.0.lock().unwrap().is_empty(),
            "deleted before the delay"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;

        // 2500 records plus the 6 run artifacts of the production-shaped tasks.
        let sizes: Vec<usize> = recorder.0.lock().unwrap().iter().map(Vec::len).collect();
        assert_eq!(sizes.iter().sum::<usize>(), 2506);
        assert!(sizes.iter().all(|&n| n <= DELETE_BATCH), "{sizes:?}");
        assert_eq!(sizes.len(), 2506usize.div_ceil(DELETE_BATCH));
    }
}
