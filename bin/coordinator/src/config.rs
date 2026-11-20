// use crate::autoscaler::AutoscalerSettings;
use config::{Config, Environment};
use serde::Deserialize;
use sp1_cluster_common::auth::AuthConfig;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// Server bind address (`COORDINATOR_ADDR`).
    #[serde(default = "default_addr")]
    pub addr: String,
    /// ClusterService gRPC URL to claim proofs / push status (`COORDINATOR_CLUSTER_RPC`).
    #[serde(default = "default_cluster_rpc")]
    pub cluster_rpc: String,
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,
    #[serde(default)]
    pub disable_proof_status_update: bool,
    #[serde(default)]
    pub execute_only_mode: bool,
    /// Seconds a worker can be inactive before it is considered dead and its tasks
    /// requeue (`COORDINATOR_WORKER_HEARTBEAT_TIMEOUT_SECS`, default 30).
    #[serde(default = "default_worker_heartbeat_timeout_secs")]
    pub worker_heartbeat_timeout_secs: u64,
    /// Most shards one execution of a proof may submit before the proof fails as unexecutable
    /// (`COORDINATOR_MAX_SHARDS_PER_PROOF`, default 0 = no limit). Counted per execution: a
    /// retried execution starts again from zero instead of adding to the earlier one. See
    /// `shards.rs` for how to choose it.
    #[serde(default)]
    pub max_shards_per_proof: u64,
    /// Comma-separated Redis URLs of the artifact store, in the same order the workers use
    /// (`COORDINATOR_REDIS_NODES`). When set, a failed proof's artifacts are deleted instead of
    /// waiting for their TTL. Default empty = off.
    #[serde(default)]
    pub redis_nodes: String,
    /// Connection pool size per Redis node for that cleanup (`COORDINATOR_REDIS_POOL_MAX_SIZE`).
    #[serde(default = "default_redis_pool_max_size")]
    pub redis_pool_max_size: usize,
    /// Set in `Settings::new`, because the token falls back to unprefixed `CLUSTER_ACCESS_TOKEN`.
    #[serde(skip)]
    pub auth: AuthConfig,
}

fn default_redis_pool_max_size() -> usize {
    4
}

fn default_worker_heartbeat_timeout_secs() -> u64 {
    crate::DEFAULT_WORKER_HEARTBEAT_TIMEOUT
}

// Server
fn default_addr() -> String {
    "127.0.0.1:50051".to_string()
}

fn default_cluster_rpc() -> String {
    "http://127.0.0.1:50051".to_string()
}

fn default_metrics_addr() -> String {
    "0.0.0.0:9090".to_string()
}

impl Settings {
    pub fn new() -> Result<Self, config::ConfigError> {
        let builder = Config::builder().add_source(Environment::with_prefix("COORDINATOR"));
        let config = builder.build()?;
        let mut settings = config.try_deserialize::<Settings>()?;
        settings.auth = AuthConfig::from_env(Some("COORDINATOR"))
            .map_err(|e| config::ConfigError::Message(e.to_string()))?;
        Ok(settings)
    }
}
