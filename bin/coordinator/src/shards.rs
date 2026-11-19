//! Per-proof shard limit.
//!
//! Proving time grows with the number of shards, and the shards a range produces per
//! instruction vary by more than 3x with the transaction mix, so neither the request's cycle
//! limit nor its gas limit bounds it. The cluster caps the shards one execution of a proof may
//! submit; a proof over the cap fails as unexecutable and the caller splits the range.
//!
//! # What is counted
//!
//! Every shard (`ProveShard`) of a proof is submitted by the execution of its `CoreExecute`
//! task: core, global-memory and precompile shards all come from that task's executor. One
//! execution of a range always submits the same shards, so the limit is checked per
//! execution, never summed across executions (a retried or redelivered `CoreExecute` submits
//! the whole set again while the earlier set stays in the proof).
//!
//! The coordinator cannot see executions directly, so shards are counted in buckets that each
//! hold exactly one execution:
//! - stream: the controller run, keyed by its `common_input` artifact (`CoreExecute` takes it
//!   as `inputs[2]`, every shard as `inputs[1]`). A retried controller is a new stream.
//! - bucket: (submitting worker, how many times that worker reported the task failed, how
//!   many `CancelTask`s for it the coordinator delivered to that worker). Each part changes
//!   exactly when an execution ends and a new one may start on the same key:
//!   - a node runs at most one execution per task (`Already working on task` drops a
//!     redelivery) and keeps it across reconnects; a restarted process gets a new random
//!     worker id;
//!   - a worker reports a failure (`fail_task`) only after its execution ended;
//!   - a delivered `CancelTask` aborts that worker's execution and drops it from the node, so
//!     the next delivery to that worker is a new execution.
//!
//!   Failures and cancels are counted per worker, not per task. The task's `retries`, or a
//!   cancel sent to another worker, also move when some other worker's execution ends, and
//!   would split an execution still running elsewhere.
//!
//! Nothing is ever reset and the holder is not consulted: a worker still executing after its
//! task moved elsewhere keeps counting its own execution, which is still one execution of the
//! same range. Any bucket over the limit therefore means the range is over it.
//!
//! Remaining error. Each case needs an abnormal event (a coordinator stall over the client's
//! 60s timeout, a node reconnecting at the wrong moment); otherwise every bucket is exactly one
//! execution.
//! - Overcount (a healthy range may be split once, never an oversized proof):
//!   - an aborted execution can still submit the few shards already in flight in its
//!     pipeline; they land in the next bucket on that worker;
//!   - the coordinator re-sends a task a worker no longer reports (heartbeat reconcile) after
//!     that worker's execution ended without its report arriving; the new execution shares
//!     the old one's bucket. Not treated as a boundary, because the same re-send also covers
//!     a delivery still in flight, where a boundary would split one execution;
//!   - a shard submission retried after a stall is created twice.
//!   - a `CoreExecute` submission retried after a stall creates two `CoreExecute` tasks for one
//!     `common_input`; when both run on the same worker their shards share a bucket.
//! - Undercount (an oversized range may run longer before it trips, or finish untripped):
//!   - a `CancelTask` accepted by the worker's channel but lost while the node reconnects is
//!     counted as delivered; the execution it did not abort is split in two buckets;
//!   - a `CoreExecute` submission retried after a stall creates two `CoreExecute` tasks for one
//!     `common_input`. They share the stream (counts are kept), and a failure or cancel of one
//!     on a worker also splits the other's execution there. Counted in
//!     `coordinator_duplicate_executions_total`.
//! - Worker ids are random per process only outside ECS; on ECS the id is the task id, and an
//!   in-place container restart keeps it, merging the two executions (overcount).

//! # When it trips
//!
//! An execution can run ahead of proving only as far as the shard permit gate allows (about
//! half of Redis `maxmemory` in shard records, a few thousand shards), so the limit trips when
//! most of the over-limit execution's shards are already proven. What it saves is retrying an
//! oversized range until the caller's timeout, not the first attempt. Set it from the caller's
//! proving timeout minus a margin, and above the shards of any single block: op-succinct splits
//! an unexecutable range in half but resubmits a one-block range unchanged.

use std::collections::HashMap;

use sp1_cluster_common::proto::{TaskData, TaskType};

/// Position of the `common_input` artifact in a `CoreExecute` task's inputs
/// (`sp1_prover::worker::CoreExecuteTaskRequest::into_raw`).
const CORE_EXECUTE_COMMON_INPUT: usize = 2;
/// Position of the `common_input` artifact in a `ProveShard` task's inputs
/// (`sp1_prover::worker::ProveShardTaskRequest::into_raw`).
const PROVE_SHARD_COMMON_INPUT: usize = 1;

/// (submitting worker, its failure reports for the task, cancels delivered to it for the task).
type BucketKey = (String, u32, u32);

#[derive(Clone, Debug, Default)]
struct Stream {
    /// Failure reports for the `CoreExecute` task, per worker.
    failures: HashMap<String, u32>,
    /// `CancelTask`s the coordinator delivered for the `CoreExecute` task, per worker.
    cancellations: HashMap<String, u32>,
    buckets: HashMap<BucketKey, u64>,
}

/// Shard counts of one proof, per execution.
#[derive(Clone, Debug, Default)]
pub struct ShardStreams {
    /// Keyed by `common_input`.
    streams: HashMap<String, Stream>,
    /// `CoreExecute` task id to its stream's `common_input`.
    by_execute_task: HashMap<String, String>,
    /// Shards that matched no stream.
    unmatched: u64,
}

/// What [`ShardStreams::track`] decided about a new task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Tracked {
    /// Not a shard.
    Ignored,
    /// A shard whose `common_input` matches no `CoreExecute` task of the proof. Never expected:
    /// a non-zero count means the task layout changed and the limit is not being applied.
    /// `first` is set for the proof's first such shard.
    Unmatched { first: bool },
    /// A second `CoreExecute` task for a `common_input` already tracked (its submission was
    /// retried). The stream and its counts are kept.
    DuplicateExecution,
    /// Counted; its execution has submitted this many shards.
    Counted(u64),
    /// Counted, and its execution is now over the limit.
    OverLimit(u64),
}

impl ShardStreams {
    /// Record a new task of the proof. `submitter` is the worker that created it (`None` if
    /// unknown). `limit` 0 means no limit.
    pub fn track(
        &mut self,
        task_id: &str,
        task: &TaskData,
        submitter: Option<&str>,
        limit: u64,
    ) -> Tracked {
        match task.task_type() {
            TaskType::CoreExecute => {
                let Some(common_input) = task.inputs.get(CORE_EXECUTE_COMMON_INPUT) else {
                    return Tracked::Ignored;
                };
                self.by_execute_task
                    .insert(task_id.to_string(), common_input.clone());
                if self.streams.contains_key(common_input) {
                    return Tracked::DuplicateExecution;
                }
                self.streams.insert(common_input.clone(), Stream::default());
                Tracked::Ignored
            }
            TaskType::ProveShard => {
                let Some(stream) = task
                    .inputs
                    .get(PROVE_SHARD_COMMON_INPUT)
                    .and_then(|common_input| self.streams.get_mut(common_input))
                else {
                    self.unmatched += 1;
                    return Tracked::Unmatched {
                        first: self.unmatched == 1,
                    };
                };
                let worker = submitter.unwrap_or_default().to_string();
                let failures = stream.failures.get(&worker).copied().unwrap_or_default();
                let cancels = stream
                    .cancellations
                    .get(&worker)
                    .copied()
                    .unwrap_or_default();
                let shards = stream
                    .buckets
                    .entry((worker, failures, cancels))
                    .or_default();
                *shards += 1;
                if limit > 0 && *shards > limit {
                    Tracked::OverLimit(*shards)
                } else {
                    Tracked::Counted(*shards)
                }
            }
            _ => Tracked::Ignored,
        }
    }

    /// The coordinator delivered a `CancelTask` for `task_id` to `worker`: that worker's
    /// execution of it ended, and its next one is a new execution.
    pub fn note_cancelled(&mut self, task_id: &str, worker: &str) {
        if let Some(stream) = self.stream_of(task_id) {
            *stream.cancellations.entry(worker.to_string()).or_default() += 1;
        }
    }

    /// `worker` reported `task_id` failed: its execution of it ended, and its next one is a
    /// new execution.
    pub fn note_failed(&mut self, task_id: &str, worker: &str) {
        if let Some(stream) = self.stream_of(task_id) {
            *stream.failures.entry(worker.to_string()).or_default() += 1;
        }
    }

    /// The stream of a `CoreExecute` task, if `task_id` is one.
    fn stream_of(&mut self, task_id: &str) -> Option<&mut Stream> {
        let common_input = self.by_execute_task.get(task_id)?;
        self.streams.get_mut(common_input)
    }

    /// The most shards any single execution of the proof submitted.
    pub fn max_execution_shards(&self) -> u64 {
        self.streams
            .values()
            .flat_map(|s| s.buckets.values())
            .copied()
            .max()
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sp1_cluster_artifact::ArtifactId;
    use sp1_prover::worker::{ProofId, ProveShardTaskRequest, RequesterId, TaskContext};

    fn core_execute(common_input: &str) -> TaskData {
        TaskData {
            task_type: TaskType::CoreExecute as i32,
            inputs: vec![
                "program_x".into(),
                "artifact_stdin".into(),
                common_input.into(),
                r#"{"num_deferred_proofs":0}"#.into(),
            ],
            ..Default::default()
        }
    }

    /// A shard task as sp1-prover lays it out, so a layout change upstream fails these tests.
    fn shard(common_input: &str) -> TaskData {
        let raw = ProveShardTaskRequest {
            elf: "program_x".to_string().into(),
            common_input: common_input.to_string().into(),
            record: "artifact_record".to_string().into(),
            output: "artifact_shard_proof".to_string().into(),
            deferred_marker_task: "task_marker".to_string().into(),
            deferred_output: None,
            context: TaskContext {
                proof_id: ProofId::new("p"),
                parent_id: None,
                parent_context: None,
                requester_id: RequesterId::new("r"),
            },
        }
        .into_raw()
        .unwrap();
        TaskData {
            task_type: TaskType::ProveShard as i32,
            inputs: raw.inputs.iter().map(|a| a.id().to_string()).collect(),
            outputs: raw.outputs.iter().map(|a| a.id().to_string()).collect(),
            ..Default::default()
        }
    }

    fn streams_with(common_input: &str) -> ShardStreams {
        let mut streams = ShardStreams::default();
        streams.track("exec", &core_execute(common_input), None, 0);
        streams
    }

    fn submit(s: &mut ShardStreams, from: &str, n: u64, limit: u64) -> Tracked {
        let mut last = Tracked::Ignored;
        for _ in 0..n {
            last = s.track("shard", &shard("artifact_ci"), Some(from), limit);
        }
        last
    }

    #[test]
    fn an_execution_over_the_limit_trips() {
        let mut s = streams_with("artifact_ci");
        assert_eq!(submit(&mut s, "w1", 3, 3), Tracked::Counted(3));
        assert_eq!(submit(&mut s, "w1", 1, 3), Tracked::OverLimit(4));
        assert_eq!(s.max_execution_shards(), 4);
    }

    #[test]
    fn limit_zero_never_trips() {
        let mut s = streams_with("artifact_ci");
        assert_eq!(submit(&mut s, "w1", 100, 0), Tracked::Counted(100));
    }

    /// After a worker reports the execution failed, its next delivery is a new execution.
    #[test]
    fn a_failure_on_a_worker_starts_a_new_execution_there() {
        let mut s = streams_with("artifact_ci");
        assert_eq!(submit(&mut s, "w1", 3, 3), Tracked::Counted(3));
        s.note_failed("exec", "w1");
        assert_eq!(submit(&mut s, "w1", 3, 3), Tracked::Counted(3));
    }

    /// w2 failing must not split w1's execution, which is still running.
    #[test]
    fn a_failure_on_another_worker_does_not_split_an_execution() {
        let mut s = streams_with("artifact_ci");
        assert_eq!(submit(&mut s, "w1", 2, 3), Tracked::Counted(2));
        s.note_failed("exec", "w2");
        assert_eq!(submit(&mut s, "w1", 2, 3), Tracked::OverLimit(4));
    }

    /// After a cancel the worker's next delivery is a new execution.
    #[test]
    fn a_cancel_on_a_worker_starts_a_new_execution_there() {
        let mut s = streams_with("artifact_ci");
        assert_eq!(submit(&mut s, "w1", 3, 3), Tracked::Counted(3));
        s.note_cancelled("exec", "w1");
        assert_eq!(submit(&mut s, "w1", 3, 3), Tracked::Counted(3));
    }

    /// Cancelling a leftover execution on w1 must not split w2's live one.
    #[test]
    fn a_cancel_on_another_worker_does_not_split_an_execution() {
        let mut s = streams_with("artifact_ci");
        assert_eq!(submit(&mut s, "w2", 2, 3), Tracked::Counted(2));
        s.note_cancelled("exec", "w1");
        assert_eq!(submit(&mut s, "w2", 2, 3), Tracked::OverLimit(4));
    }

    /// Each worker's execution counts on its own; a leftover one that keeps running is still
    /// one execution of the range and trips like any other.
    #[test]
    fn executions_on_different_workers_count_separately() {
        let mut s = streams_with("artifact_ci");
        assert_eq!(submit(&mut s, "w1", 3, 3), Tracked::Counted(3));
        assert_eq!(submit(&mut s, "w2", 3, 3), Tracked::Counted(3));
        assert_eq!(submit(&mut s, "w1", 1, 3), Tracked::OverLimit(4));
    }

    /// A retried controller creates a new CoreExecute with a new common_input: a new stream.
    #[test]
    fn a_new_controller_run_is_a_separate_stream() {
        let mut s = streams_with("artifact_ci");
        s.track("exec2", &core_execute("artifact_ci_2"), None, 0);
        assert_eq!(submit(&mut s, "w1", 3, 3), Tracked::Counted(3));
        let other = s.track("s", &shard("artifact_ci_2"), Some("w1"), 3);
        assert_eq!(other, Tracked::Counted(1));
    }

    #[test]
    fn a_shard_of_no_known_execution_is_unmatched() {
        let mut s = streams_with("artifact_ci");
        let t = s.track("s", &shard("artifact_other"), Some("w1"), 1);
        assert_eq!(t, Tracked::Unmatched { first: true });
        let t = s.track("s2", &shard("artifact_other"), Some("w1"), 1);
        assert_eq!(t, Tracked::Unmatched { first: false });
    }

    /// A retried CoreExecute submission creates a second task for the same run; the counts
    /// already made must survive it.
    #[test]
    fn a_duplicate_execute_task_keeps_the_stream() {
        let mut s = streams_with("artifact_ci");
        assert_eq!(submit(&mut s, "w1", 2, 3), Tracked::Counted(2));
        let dup = s.track("exec_dup", &core_execute("artifact_ci"), None, 0);
        assert_eq!(dup, Tracked::DuplicateExecution);
        assert_eq!(submit(&mut s, "w1", 2, 3), Tracked::OverLimit(4));
    }

    #[test]
    fn other_tasks_are_ignored() {
        let mut s = streams_with("artifact_ci");
        let reduce = TaskData {
            task_type: TaskType::RecursionReduce as i32,
            inputs: vec!["a".into(), "artifact_ci".into()],
            ..Default::default()
        };
        assert_eq!(s.track("r", &reduce, Some("w1"), 1), Tracked::Ignored);
    }

    /// Pins the shard input layout this module reads to sp1-prover's own parser.
    #[test]
    fn the_shard_layout_matches_sp1_prover() {
        let task = shard("artifact_ci");
        assert_eq!(task.inputs[PROVE_SHARD_COMMON_INPUT], "artifact_ci");
    }
}
