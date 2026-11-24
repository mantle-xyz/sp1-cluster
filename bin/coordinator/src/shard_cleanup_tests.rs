//! Integration tests for the per-proof shard limit and failed-proof cleanup.
//!
//! They go through the real paths: workers register with `add_worker`, the policy assigns
//! tasks, heartbeats reconcile, a dead worker is removed by `cleanup_dead_workers`, a task is
//! retried by `fail_task`. Task shapes follow sp1-prover: CoreExecute inputs
//! [elf, stdin, common_input, metadata JSON], ProveShard inputs [elf, common_input, record,
//! marker]. Kept out of `lib.rs`'s test module, which upstream also appends to.

use super::*;
use crate::policy::{balanced::BalancedPolicy, default::DefaultPolicy};

const PROGRAM: &str = "program_ab";
const STDIN: &str = "artifact_stdin";
const FINAL_PROOF: &str = "artifact_final_proof";
const EXEC_WEIGHT: u32 = 10;

fn controller_task(proof_id: &str) -> TaskData {
    TaskData {
        task_type: TaskType::Controller as i32,
        proof_id: proof_id.into(),
        inputs: vec![
            PROGRAM.into(),
            STDIN.into(),
            "2".into(),
            "1000000".into(),
            String::new(),
            r#"{"stdin_private":false}"#.into(),
        ],
        outputs: vec![FINAL_PROOF.into()],
        ..Default::default()
    }
}

fn core_execute_task(proof_id: &str) -> TaskData {
    TaskData {
        task_type: TaskType::CoreExecute as i32,
        proof_id: proof_id.into(),
        inputs: vec![
            PROGRAM.into(),
            STDIN.into(),
            "artifact_ci".into(),
            r#"{"num_deferred_proofs":0}"#.into(),
        ],
        outputs: vec!["artifact_exec_out".into()],
        weight: EXEC_WEIGHT,
        ..Default::default()
    }
}

fn prove_shard_task(proof_id: &str, n: usize) -> TaskData {
    TaskData {
        task_type: TaskType::ProveShard as i32,
        proof_id: proof_id.into(),
        inputs: vec![
            PROGRAM.into(),
            "artifact_ci".into(),
            format!("artifact_record_{n}"),
            "task_marker".into(),
        ],
        outputs: vec![format!("artifact_shard_proof_{n}")],
        ..Default::default()
    }
}

async fn add_worker<P: AssignmentPolicy>(c: &Arc<Coordinator<P>>, id: &str, max_weight: u32) {
    let (tx, rx) = mpsc::unbounded_channel();
    std::mem::forget(rx); // keep the worker's channel open
    c.add_worker(
        id.into(),
        WorkerType::All,
        max_weight,
        tx,
        WorkerIdentity::default(),
    )
    .await
    .unwrap();
}

async fn holder<P: AssignmentPolicy>(c: &Arc<Coordinator<P>>, exec: &str) -> Option<String> {
    c.state.read().await.proofs["p1"].tasks[exec].worker.clone()
}

/// A proof with its controller and CoreExecute tasks, the execution assigned to `worker`.
/// Returns the CoreExecute task id.
async fn start_proof<P: AssignmentPolicy>(c: &Arc<Coordinator<P>>, worker: &str) -> String {
    add_worker(c, worker, 100).await;
    c.state.write().await.proofs.insert(
        "p1".into(),
        Proof::new("p1".into(), None, P::ProofState::default()),
    );
    c.create_task_from(None, controller_task("p1"))
        .await
        .unwrap();
    let exec = c
        .create_task_from(Some(worker), core_execute_task("p1"))
        .await
        .unwrap();
    assert_eq!(holder(c, &exec).await.as_deref(), Some(worker));
    exec
}

async fn submit_shards<P: AssignmentPolicy>(
    c: &Arc<Coordinator<P>>,
    from: &str,
    range: std::ops::Range<usize>,
) -> Result<(), Status> {
    for n in range {
        c.create_task_from(Some(from), prove_shard_task("p1", n))
            .await?;
    }
    Ok(())
}

async fn kill_worker<P: AssignmentPolicy>(c: &Arc<Coordinator<P>>, id: &str) {
    c.state
        .write()
        .await
        .workers
        .get_mut(id)
        .unwrap()
        .last_heartbeat = 0;
    c.cleanup_dead_workers().await;
    assert!(!c.state.read().await.workers.contains_key(id));
}

// ---- shard limit ----

/// The whole path: the shard over the limit is refused with a permanent code, the proof is
/// failed with the shard-limit payload that the status task records as unexecutable.
#[tokio::test]
async fn a_proof_over_the_shard_limit_fails_as_unexecutable() {
    let c = Arc::new(Coordinator::<DefaultPolicy>::new());
    let (tx, mut rx) = mpsc::unbounded_channel();
    c.set_proofs_tx(tx).await;
    c.set_max_shards_per_proof(2).await;
    start_proof(&c, "w1").await;

    submit_shards(&c, "w1", 0..2)
        .await
        .expect("shards up to the limit are accepted");
    let err = submit_shards(&c, "w1", 2..3)
        .await
        .expect_err("the shard over the limit must be refused");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    assert!(!c.state.read().await.proofs.contains_key("p1"));
    let result = rx
        .try_recv()
        .expect("the failure must reach the status task");
    assert_eq!(result.id, "p1");
    assert!(!result.success);
    assert!(sp1_cluster_common::failure::is_shard_limit(
        result.extra_data.as_deref().unwrap()
    ));
}

/// w1 misses heartbeats, is declared dead, re-registers under the same id and is handed the
/// execution again before it reports in. The node ignores that redelivery and the original
/// execution keeps submitting, so its count must carry on.
async fn dead_worker_back_to_the_same_worker_keeps_counting<P: AssignmentPolicy + Default>() {
    let c = Arc::new(Coordinator::<P>::new());
    c.set_max_shards_per_proof(3).await;
    let exec = start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..2).await.unwrap();

    kill_worker(&c, "w1").await;
    add_worker(&c, "w1", 100).await;
    assert_eq!(holder(&c, &exec).await.as_deref(), Some("w1"));

    submit_shards(&c, "w1", 2..3).await.unwrap();
    let err = submit_shards(&c, "w1", 3..4)
        .await
        .expect_err("the execution's 4th shard is over the limit of 3 across the requeue");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn a_dead_worker_requeue_back_to_the_same_worker_keeps_counting() {
    dead_worker_back_to_the_same_worker_keeps_counting::<DefaultPolicy>().await;
}

/// Production runs the balanced policy.
#[tokio::test]
async fn a_dead_worker_requeue_back_to_the_same_worker_keeps_counting_balanced() {
    dead_worker_back_to_the_same_worker_keeps_counting::<BalancedPolicy>().await;
}

/// w1 → w2 → w1 while w1's original execution never stopped: every shard it submits is
/// still one execution and must count together.
#[tokio::test]
async fn an_execution_that_never_stopped_counts_across_holder_changes() {
    let c = Arc::new(Coordinator::<DefaultPolicy>::new());
    c.set_max_shards_per_proof(3).await;
    let exec = start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..2).await.unwrap();

    kill_worker(&c, "w1").await;
    add_worker(&c, "w2", 100).await;
    assert_eq!(holder(&c, &exec).await.as_deref(), Some("w2"));
    kill_worker(&c, "w2").await;
    add_worker(&c, "w1", 100).await;
    assert_eq!(holder(&c, &exec).await.as_deref(), Some("w1"));

    submit_shards(&c, "w1", 2..3).await.unwrap();
    assert!(submit_shards(&c, "w1", 3..4).await.is_err());
}

/// The case the reviewer found: after the requeue w1 reports the task before it gets it
/// back, the coordinator cancels its execution, and the next delivery is a new execution.
/// A healthy range must not be pushed over the limit by the two executions adding up.
#[tokio::test]
async fn a_cancelled_execution_and_its_rerun_on_the_same_worker_count_apart() {
    let c = Arc::new(Coordinator::<DefaultPolicy>::new());
    c.set_max_shards_per_proof(3).await;
    let exec = start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..2).await.unwrap();

    kill_worker(&c, "w1").await;
    // Back without room for the execution, so it stays queued.
    add_worker(&c, "w1", EXEC_WEIGHT - 1).await;
    assert!(!c.state.read().await.workers["w1"]
        .active_tasks
        .contains(&("p1".to_string(), exec.clone())));
    // w1 still runs the old execution and says so; the coordinator cancels it.
    c.handle_heartbeat("w1", &["p1".into()], std::slice::from_ref(&exec), 0)
        .await
        .unwrap();
    // Room again: the execution is delivered to w1 as a new one.
    c.state
        .write()
        .await
        .workers
        .get_mut("w1")
        .unwrap()
        .max_weight = 100;
    let state = c.state.clone().write_owned().await;
    c.assign_tasks(state).await.unwrap();
    assert_eq!(holder(&c, &exec).await.as_deref(), Some("w1"));

    submit_shards(&c, "w1", 2..5)
        .await
        .expect("the rerun's 3 shards must not add to the cancelled run's 2");
    assert!(c.state.read().await.proofs.contains_key("p1"));
}

/// w1's execution keeps running after its requeue to w2, and w2 then reports a retryable
/// failure. That ends w2's execution only; w1's must keep counting as one.
#[tokio::test]
async fn another_worker_failing_does_not_split_a_running_execution() {
    let c = Arc::new(Coordinator::<DefaultPolicy>::new());
    c.set_max_shards_per_proof(3).await;
    let exec = start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..2).await.unwrap();

    kill_worker(&c, "w1").await;
    add_worker(&c, "w2", 100).await;
    assert_eq!(holder(&c, &exec).await.as_deref(), Some("w2"));
    c.fail_task("w2".into(), "p1".into(), exec.clone(), true)
        .await
        .unwrap();
    assert_eq!(c.state.read().await.proofs["p1"].tasks[&exec].retries, 1);

    submit_shards(&c, "w1", 2..3).await.unwrap();
    assert!(
        submit_shards(&c, "w1", 3..4).await.is_err(),
        "w1's execution is over the limit regardless of w2's retry"
    );
}

/// A CancelTask that could not be sent aborted nothing; the execution keeps its count.
#[tokio::test]
async fn an_undelivered_cancel_does_not_split_an_execution() {
    let c = Arc::new(Coordinator::<DefaultPolicy>::new());
    c.set_max_shards_per_proof(3).await;
    let exec = start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..2).await.unwrap();

    kill_worker(&c, "w1").await;
    // Back without room for the execution, on a stream that is already closed.
    let (tx, rx) = mpsc::unbounded_channel();
    drop(rx);
    c.add_worker(
        "w1".into(),
        WorkerType::All,
        EXEC_WEIGHT - 1,
        tx,
        WorkerIdentity::default(),
    )
    .await
    .unwrap();
    c.handle_heartbeat("w1", &["p1".into()], std::slice::from_ref(&exec), 0)
        .await
        .unwrap();

    submit_shards(&c, "w1", 2..3).await.unwrap();
    assert!(
        submit_shards(&c, "w1", 3..4).await.is_err(),
        "the cancel never reached w1, so its execution still counts as one"
    );
}

/// After its requeue, w1's execution fails on its own and w1 reports it before getting the
/// task back. The report is refused (w1 no longer holds the task), but the execution has
/// ended: the next one on w1 must count from zero.
#[tokio::test]
async fn a_refused_failure_report_still_ends_the_execution() {
    let c = Arc::new(Coordinator::<DefaultPolicy>::new());
    c.set_max_shards_per_proof(3).await;
    let exec = start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..2).await.unwrap();

    kill_worker(&c, "w1").await;
    add_worker(&c, "w1", EXEC_WEIGHT - 1).await;
    let refused = c
        .fail_task("w1".into(), "p1".into(), exec.clone(), true)
        .await
        .expect_err("w1 no longer holds the task");
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);

    c.state
        .write()
        .await
        .workers
        .get_mut("w1")
        .unwrap()
        .max_weight = 100;
    let state = c.state.clone().write_owned().await;
    c.assign_tasks(state).await.unwrap();
    assert_eq!(holder(&c, &exec).await.as_deref(), Some("w1"));

    submit_shards(&c, "w1", 2..5)
        .await
        .expect("the new execution's 3 shards must not add to the failed one's 2");
}

/// A CoreExecute submission retried after a stall creates a second task for the same run.
/// The shards already counted must survive it.
#[tokio::test]
async fn a_duplicate_execute_task_keeps_the_count() {
    let c = Arc::new(Coordinator::<DefaultPolicy>::new());
    c.set_max_shards_per_proof(3).await;
    start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..2).await.unwrap();

    c.create_task_from(Some("w1"), core_execute_task("p1"))
        .await
        .unwrap();
    submit_shards(&c, "w1", 2..3).await.unwrap();
    assert!(submit_shards(&c, "w1", 3..4).await.is_err());
}

/// After a requeue to another worker each worker's execution counts on its own.
#[tokio::test]
async fn executions_on_different_workers_count_apart() {
    let c = Arc::new(Coordinator::<DefaultPolicy>::new());
    c.set_max_shards_per_proof(3).await;
    let exec = start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..3).await.unwrap();

    kill_worker(&c, "w1").await;
    add_worker(&c, "w2", 100).await;
    assert_eq!(holder(&c, &exec).await.as_deref(), Some("w2"));

    submit_shards(&c, "w2", 3..6)
        .await
        .expect("the new execution counts from zero");
    assert!(submit_shards(&c, "w2", 6..7).await.is_err());
}

/// A retried execution re-submits every shard; the limit applies to one execution.
#[tokio::test]
async fn a_retried_execution_does_not_add_up_to_the_shard_limit() {
    let c = Arc::new(Coordinator::<DefaultPolicy>::new());
    c.set_max_shards_per_proof(2).await;
    let exec = start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..2).await.unwrap();

    c.fail_task("w1".into(), "p1".into(), exec.clone(), true)
        .await
        .unwrap();
    assert_eq!(c.state.read().await.proofs["p1"].tasks[&exec].retries, 1);
    assert_eq!(holder(&c, &exec).await.as_deref(), Some("w1"));

    submit_shards(&c, "w1", 2..4)
        .await
        .expect("the second execution's shards must not count the first's");
    assert!(c.state.read().await.proofs.contains_key("p1"));
}

// ---- failed-proof cleanup ----

struct RecordingCleaner(std::sync::Mutex<Vec<String>>);
impl store::ArtifactStore for RecordingCleaner {
    fn delete(&self, ids: Vec<String>) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        self.0.lock().unwrap().extend(ids);
        Box::pin(async { Ok(()) })
    }
    fn execution_cycles(&self, _: String) -> futures::future::BoxFuture<'_, anyhow::Result<u64>> {
        Box::pin(async { anyhow::bail!("not recorded") })
    }
}

fn with_cleaner() -> (Arc<Coordinator<DefaultPolicy>>, Arc<RecordingCleaner>) {
    let recorder = Arc::new(RecordingCleaner(std::sync::Mutex::new(vec![])));
    let mut c = Coordinator::<DefaultPolicy>::new();
    c.set_artifact_store(recorder.clone());
    (Arc::new(c), recorder)
}

async fn deleted_after_delay(recorder: &RecordingCleaner) -> Vec<String> {
    tokio::time::sleep(cleanup::CLEANUP_DELAY + Duration::from_secs(5)).await;
    let mut ids = recorder.0.lock().unwrap().clone();
    ids.sort();
    ids
}

/// What every failure path must delete for the proof built by `start_proof` plus shard 0:
/// the run's artifacts, never the program, stdin or final proof.
fn run_artifacts() -> Vec<String> {
    [
        "artifact_ci",
        "artifact_exec_out",
        "artifact_record_0",
        "artifact_shard_proof_0",
    ]
    .map(String::from)
    .to_vec()
}

#[tokio::test(start_paused = true)]
async fn a_failed_proof_s_run_artifacts_are_deleted_after_the_delay() {
    let (c, recorder) = with_cleaner();
    start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..1).await.unwrap();

    c.fail_proof("p1".into(), None, false, None).await.unwrap();
    tokio::time::sleep(cleanup::CLEANUP_DELAY - Duration::from_secs(1)).await;
    assert!(
        recorder.0.lock().unwrap().is_empty(),
        "deleted before the delay"
    );
    assert_eq!(deleted_after_delay(&recorder).await, run_artifacts());
}

/// The controller failing for good (`fail_task` with no retry) fails the proof there.
#[tokio::test(start_paused = true)]
async fn a_controller_failure_cleans_up() {
    let (c, recorder) = with_cleaner();
    let (tx, _rx) = mpsc::unbounded_channel();
    c.set_proofs_tx(tx).await;
    start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..1).await.unwrap();
    let controller = {
        let state = c.state.read().await;
        state.proofs["p1"]
            .tasks
            .values()
            .find(|t| t.data.task_type() == TaskType::Controller)
            .map(|t| t.id.clone())
            .unwrap()
    };

    c.fail_task("w1".into(), "p1".into(), controller, false)
        .await
        .unwrap();
    assert!(!c.state.read().await.proofs.contains_key("p1"));
    assert_eq!(deleted_after_delay(&recorder).await, run_artifacts());
}

#[tokio::test(start_paused = true)]
async fn an_expired_proof_cleans_up() {
    let (c, recorder) = with_cleaner();
    start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..1).await.unwrap();
    c.state
        .write()
        .await
        .proofs
        .get_mut("p1")
        .unwrap()
        .expires_at = Some(SystemTime::now() - Duration::from_secs(1));

    c.cleanup_cancel_expired_proofs().await;
    assert!(!c.state.read().await.proofs.contains_key("p1"));
    assert_eq!(deleted_after_delay(&recorder).await, run_artifacts());
}

/// The refused shard was already uploaded; it is deleted with the rest.
#[tokio::test(start_paused = true)]
async fn the_shard_over_the_limit_is_cleaned_up_too() {
    let (c, recorder) = with_cleaner();
    let (tx, _rx) = mpsc::unbounded_channel();
    c.set_proofs_tx(tx).await;
    c.set_max_shards_per_proof(1).await;
    start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..1).await.unwrap();
    assert!(submit_shards(&c, "w1", 1..2).await.is_err());

    let mut expected = run_artifacts();
    expected.extend(["artifact_record_1", "artifact_shard_proof_1"].map(String::from));
    expected.sort();
    assert_eq!(deleted_after_delay(&recorder).await, expected);
}

/// A proof can be torn down after it completed (e.g. a redelivered execution's duplicate
/// shards fail once the controller cleaned up). Its result is decided; keep everything.
#[tokio::test(start_paused = true)]
async fn a_proof_failed_after_completing_keeps_its_artifacts() {
    let (c, recorder) = with_cleaner();
    let (tx, _rx) = mpsc::unbounded_channel();
    c.set_proofs_tx(tx).await;
    start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..1).await.unwrap();

    c.complete_proof("p1".into(), None).await.unwrap();
    c.fail_proof("p1".into(), None, true, None).await.unwrap();
    assert!(deleted_after_delay(&recorder).await.is_empty());
}

/// The completion is the proof's result. A later teardown must not report a failure, or the
/// status task can replace a Completed write that is still waiting to be re-issued.
#[tokio::test]
async fn a_proof_failed_after_completing_reports_only_the_completion() {
    let c = Arc::new(Coordinator::<DefaultPolicy>::new());
    let (tx, mut rx) = mpsc::unbounded_channel();
    c.set_proofs_tx(tx).await;
    start_proof(&c, "w1").await;

    c.complete_proof("p1".into(), None).await.unwrap();
    c.fail_proof("p1".into(), None, true, None).await.unwrap();

    let first = rx.try_recv().expect("the completion is reported");
    assert!(first.success);
    assert!(
        rx.try_recv().is_err(),
        "no failure may follow the completion"
    );
    assert!(
        !c.state.read().await.proofs.contains_key("p1"),
        "the leftover tasks are still torn down"
    );
}

/// Without a cleaner (COORDINATOR_REDIS_NODES unset) failure behaves as before.
#[tokio::test]
async fn without_a_cleaner_failing_a_proof_deletes_nothing() {
    let c = Arc::new(Coordinator::<DefaultPolicy>::new());
    start_proof(&c, "w1").await;
    c.fail_proof("p1".into(), None, false, None).await.unwrap();
    assert!(!c.state.read().await.proofs.contains_key("p1"));
}

// ---- proofs running side by side ----

/// A second proof on the same program, with its own run (another range, or the aggregation
/// proof that runs while a range proves). Returns its CoreExecute task id.
async fn start_other_proof<P: AssignmentPolicy>(c: &Arc<Coordinator<P>>, worker: &str) -> String {
    c.state.write().await.proofs.insert(
        "p2".into(),
        Proof::new("p2".into(), None, P::ProofState::default()),
    );
    let mut controller = controller_task("p2");
    controller.inputs[1] = "artifact_stdin_p2".into();
    controller.outputs = vec!["artifact_final_proof_p2".into()];
    c.create_task_from(None, controller).await.unwrap();
    let mut exec = core_execute_task("p2");
    exec.inputs[1] = "artifact_stdin_p2".into();
    exec.inputs[2] = "artifact_ci_p2".into();
    exec.outputs = vec!["artifact_exec_out_p2".into()];
    let exec_id = c.create_task_from(Some(worker), exec).await.unwrap();
    let mut shard = prove_shard_task("p2", 0);
    shard.inputs[1] = "artifact_ci_p2".into();
    shard.inputs[2] = "artifact_record_p2_0".into();
    shard.outputs = vec!["artifact_shard_proof_p2_0".into()];
    c.create_task_from(Some(worker), shard).await.unwrap();
    exec_id
}

/// Failing one proof deletes only its own run: nothing the other proof created, and not the
/// program they share.
#[tokio::test(start_paused = true)]
async fn failing_a_proof_deletes_nothing_of_a_proof_running_alongside() {
    let (c, recorder) = with_cleaner();
    start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..1).await.unwrap();
    start_other_proof(&c, "w1").await;

    c.fail_proof("p1".into(), None, false, None).await.unwrap();

    let deleted = deleted_after_delay(&recorder).await;
    assert_eq!(deleted, run_artifacts(), "only p1's run artifacts");
    let p2: Vec<String> = {
        let state = c.state.read().await;
        state.proofs["p2"]
            .tasks
            .values()
            .flat_map(|t| t.data.inputs.iter().chain(t.data.outputs.iter()).cloned())
            .collect()
    };
    for id in &p2 {
        assert!(!deleted.contains(id), "deleted {id}, which p2 uses");
    }
}

// ---- what a completed proof records ----

/// An artifact store that knows the cycles of p1's execution output.
struct CyclesStore(u64);
impl store::ArtifactStore for CyclesStore {
    fn delete(&self, _: Vec<String>) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn execution_cycles(&self, id: String) -> futures::future::BoxFuture<'_, anyhow::Result<u64>> {
        let cycles = self.0;
        Box::pin(async move {
            anyhow::ensure!(id == "artifact_exec_out", "unexpected output {id}");
            Ok(cycles)
        })
    }
}

async fn wait_for_cycles<P: AssignmentPolicy>(c: &Arc<Coordinator<P>>) -> Option<u64> {
    for _ in 0..100 {
        if let Some(cycles) = c.state.read().await.proofs["p1"].execution_cycles {
            return Some(cycles);
        }
        tokio::task::yield_now().await;
    }
    None
}

/// The execution's cycles, read when its CoreExecute task completes, and its shards reach the
/// status task with the completion.
#[tokio::test]
async fn a_completed_proof_reports_its_cycles_and_shards() {
    let mut c = Coordinator::<DefaultPolicy>::new();
    c.set_artifact_store(Arc::new(CyclesStore(4_200_000_000)));
    let c = Arc::new(c);
    let (tx, mut rx) = mpsc::unbounded_channel();
    c.set_proofs_tx(tx).await;
    let exec = start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..3).await.unwrap();

    c.complete_task("w1".into(), "p1".into(), exec, Default::default())
        .await
        .unwrap();
    assert_eq!(wait_for_cycles(&c).await, Some(4_200_000_000));
    c.complete_proof("p1".into(), None).await.unwrap();

    let result = rx.try_recv().expect("the completion is reported");
    assert!(result.success);
    assert_eq!(result.execution_cycles, Some(4_200_000_000));
    assert_eq!(
        result.extra_data.as_deref(),
        Some(r#"{"execution_shards":3}"#)
    );
}

/// Without an artifact store there are no cycles, and the shards are still recorded.
#[tokio::test]
async fn without_a_store_a_completion_has_shards_but_no_cycles() {
    let c = Arc::new(Coordinator::<DefaultPolicy>::new());
    let (tx, mut rx) = mpsc::unbounded_channel();
    c.set_proofs_tx(tx).await;
    let exec = start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..2).await.unwrap();
    c.complete_task("w1".into(), "p1".into(), exec, Default::default())
        .await
        .unwrap();
    c.complete_proof("p1".into(), None).await.unwrap();

    let result = rx.try_recv().unwrap();
    assert_eq!(result.execution_cycles, None);
    assert_eq!(
        result.extra_data.as_deref(),
        Some(r#"{"execution_shards":2}"#)
    );
}

/// A completion that carries its own payload (execute-only) keeps it.
#[tokio::test]
async fn a_completion_payload_is_not_replaced() {
    let c = Arc::new(Coordinator::<DefaultPolicy>::new());
    let (tx, mut rx) = mpsc::unbounded_channel();
    c.set_proofs_tx(tx).await;
    start_proof(&c, "w1").await;
    submit_shards(&c, "w1", 0..2).await.unwrap();
    c.complete_proof("p1".into(), Some("{\"status\":2}".into()))
        .await
        .unwrap();
    assert_eq!(
        rx.try_recv().unwrap().extra_data.as_deref(),
        Some("{\"status\":2}")
    );
}
