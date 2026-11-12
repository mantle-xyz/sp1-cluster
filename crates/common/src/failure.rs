use serde::{Deserialize, Serialize};

use crate::proto::TaskType;

/// `extra_data` payload for post-execute task failures.
/// On the wire as `{"proving_failure": <ProvingFailure>}` to coexist with
/// the `ExecutionResult` shape written by execute_only.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProvingFailure {
    pub task_type: TaskType,
    pub reason: String,
}

impl ProvingFailure {
    /// Render as the `extra_data` wire string.
    pub fn to_extra_data(&self) -> String {
        serde_json::json!({ "proving_failure": self }).to_string()
    }

    /// Parse from the `extra_data` wire string. Returns `None` if the string
    /// isn't a proving-failure payload.
    pub fn from_extra_data(s: &str) -> Option<Self> {
        #[derive(Deserialize)]
        struct Envelope {
            proving_failure: ProvingFailure,
        }
        serde_json::from_str::<Envelope>(s)
            .ok()
            .map(|e| e.proving_failure)
    }

    /// Whether the prover's cycle-limit check stopped the proof.
    ///
    /// `reason` is the `Debug` rendering of the error, and the prover raises the limit in two
    /// shapes: per trace chunk as `ExecutionError::ExceededCycleLimit(limit)`, and once more after
    /// the run as an `anyhow` error "cycle limit exceeded: {cycles} > {limit}".
    pub fn exceeded_cycle_limit(&self) -> bool {
        self.reason.starts_with("ExceededCycleLimit(")
            || self.reason.starts_with("cycle limit exceeded:")
    }
}

/// `extra_data` written when a claimed proof passes its deadline before finishing, so the
/// terminal row records why it failed. On the wire as `{"expired": {"deadline": <secs>}}`.
pub fn expired_extra_data(deadline: u64) -> String {
    serde_json::json!({ "expired": { "deadline": deadline } }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(reason: &str) -> ProvingFailure {
        ProvingFailure {
            task_type: TaskType::CoreExecute,
            reason: reason.to_string(),
        }
    }

    #[test]
    fn both_cycle_limit_shapes_are_recognised() {
        assert!(failure("ExceededCycleLimit(1000)").exceeded_cycle_limit());
        assert!(failure("cycle limit exceeded: 1001 > 1000").exceeded_cycle_limit());
    }

    #[test]
    fn other_failures_are_not_cycle_limit() {
        assert!(!failure("UnconstrainedCycleLimitExceeded(1000)").exceeded_cycle_limit());
        assert!(!failure("Redis shard 0 admission wait > 120s").exceeded_cycle_limit());
        assert!(!failure("").exceeded_cycle_limit());
    }

    #[test]
    fn expired_payload_is_not_a_proving_failure() {
        let s = expired_extra_data(1_700_000_000);
        assert_eq!(s, r#"{"expired":{"deadline":1700000000}}"#);
        assert!(ProvingFailure::from_extra_data(&s).is_none());
    }
}
