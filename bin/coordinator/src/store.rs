//! The coordinator's optional access to the artifact store, set from `COORDINATOR_REDIS_NODES`.
//!
//! Two uses, both best effort (a failure is logged and changes nothing else):
//! - deleting a failed proof's artifacts ([`crate::cleanup`]);
//! - reading the cycles of a finished execution, recorded with the proof's completion
//!   ([`crate::Coordinator::complete_task`]). The proving path computes them but no component
//!   writes them: the worker drops the controller's `ExecutionOutput`.

use futures::future::BoxFuture;
use sp1_cluster_artifact::{redis::RedisArtifactClient, ArtifactClient, ArtifactType};
use sp1_core_machine::executor::ExecutionOutput;

/// The artifact store, as far as the coordinator uses it.
pub trait ArtifactStore: Send + Sync {
    /// Delete artifacts by id.
    fn delete(&self, ids: Vec<String>) -> BoxFuture<'_, anyhow::Result<()>>;

    /// The cycles of an execution, from the `ExecutionOutput` its `CoreExecute` task uploaded to
    /// `execution_output` (the task's only output) before completing.
    fn execution_cycles(&self, execution_output: String) -> BoxFuture<'_, anyhow::Result<u64>>;
}

impl ArtifactStore for RedisArtifactClient {
    fn delete(&self, ids: Vec<String>) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            self.delete_batch(&ids, ArtifactType::UnspecifiedArtifactType)
                .await
        })
    }

    fn execution_cycles(&self, execution_output: String) -> BoxFuture<'_, anyhow::Result<u64>> {
        Box::pin(read_execution_cycles(self, execution_output))
    }
}

/// Read an `ExecutionOutput` the way sp1-prover's controller does (`ArtifactClient::download`).
async fn read_execution_cycles(
    client: &impl ArtifactClient,
    execution_output: String,
) -> anyhow::Result<u64> {
    let output: ExecutionOutput = client.download(&execution_output).await?;
    Ok(output.cycles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sp1_cluster_artifact::InMemoryArtifactClient;

    /// Written as sp1-prover's CoreExecute task writes it (`controller/mod.rs`:
    /// `artifact_client.upload(&request.execution_output, &output)`). Both ends use this build's
    /// `ExecutionOutput`, so this pins the type and the read path, not compatibility with a
    /// worker image on another SP1 version: there a mismatch shows as the per-proof
    /// "could not read the execution cycles" warning.
    #[tokio::test]
    async fn reads_the_cycles_the_execute_task_uploaded() {
        let client = InMemoryArtifactClient::new();
        let id = "artifact_exec_out".to_string();
        let output = ExecutionOutput {
            cycles: 123_456_789,
            public_value_stream: vec![1, 2, 3],
        };
        client.upload(&id, &output).await.unwrap();
        assert_eq!(
            read_execution_cycles(&client, id).await.unwrap(),
            123_456_789
        );
    }

    #[tokio::test]
    async fn a_missing_output_is_an_error() {
        let client = InMemoryArtifactClient::new();
        assert!(read_execution_cycles(&client, "artifact_none".into())
            .await
            .is_err());
    }
}
