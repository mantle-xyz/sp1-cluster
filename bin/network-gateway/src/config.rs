use std::path::PathBuf;

use clap::Parser;

use crate::auth::AuthMode;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "network-gateway",
    about = "SDK-compatible gateway for sp1-cluster"
)]
pub struct Config {
    /// gRPC server bind address.
    #[arg(long, env = "GATEWAY_GRPC_ADDR", default_value = "0.0.0.0:50061")]
    pub grpc_addr: String,

    /// HTTP server bind address (artifact proxy + health).
    #[arg(long, env = "GATEWAY_HTTP_ADDR", default_value = "0.0.0.0:8081")]
    pub http_addr: String,

    /// Operator metrics endpoint bind address (serves ONLY `/metrics`). Kept
    /// separate from GATEWAY_HTTP_ADDR (the public artifact surface) and bound
    /// to loopback by default so admission telemetry isn't exposed publicly.
    #[arg(long, env = "GATEWAY_METRICS_ADDR", default_value = "127.0.0.1:9091")]
    pub metrics_addr: String,

    /// Public base URL used to build artifact URIs returned to SDK clients.
    /// e.g. `http://gateway.internal:8081`.
    #[arg(
        long,
        env = "GATEWAY_PUBLIC_HTTP_URL",
        default_value = "http://localhost:8081"
    )]
    pub public_http_url: String,

    /// ClusterService gRPC URL (the `bin/api` endpoint).
    #[arg(long, env = "GATEWAY_CLUSTER_RPC")]
    pub cluster_rpc: String,

    /// Artifact store backend: "s3" or "redis".
    #[arg(long, env = "GATEWAY_ARTIFACT_STORE", default_value = "s3")]
    pub artifact_store: String,

    #[arg(long, env = "GATEWAY_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    #[arg(long, env = "GATEWAY_S3_REGION")]
    pub s3_region: Option<String>,

    #[arg(long, env = "GATEWAY_S3_CONCURRENCY", default_value_t = 32)]
    pub s3_concurrency: usize,

    #[arg(long, env = "GATEWAY_REDIS_NODES", value_delimiter = ',')]
    pub redis_nodes: Option<Vec<String>>,

    #[arg(long, env = "GATEWAY_REDIS_POOL_MAX_SIZE", default_value_t = 16)]
    pub redis_pool_max_size: usize,

    /// Balance reported for any address by `get_balance`. Decimal string.
    /// Defaults to U256::MAX at startup when left unset.
    #[arg(long, env = "GATEWAY_BALANCE_AMOUNT")]
    pub balance_amount: Option<String>,

    /// Authentication mode for signed requests: `none` (default), `verify`, or `allowlist`.
    #[arg(long, env = "GATEWAY_AUTH_MODE", default_value = "none", value_enum)]
    pub auth_mode: AuthMode,

    /// Comma-separated list of 0x-prefixed addresses. Required when `auth_mode=allowlist`.
    #[arg(long, env = "GATEWAY_AUTH_ALLOWLIST")]
    pub auth_allowlist: Option<String>,

    /// Program-store backend. `memory` (default) keeps registered programs
    /// in process memory — fine for single-user / self-hosted setups, but
    /// SDK clients will re-register their programs after a gateway restart.
    /// Use `fs` for durable on-disk storage.
    #[arg(long, env = "GATEWAY_PROGRAM_STORE", default_value = "memory")]
    pub program_store: String,

    /// Directory used by the `fs` program store. Required when
    /// `program_store=fs`; created on startup if missing.
    #[arg(long, env = "GATEWAY_PROGRAM_STORE_DIR")]
    pub program_store_dir: Option<PathBuf>,

    /// Enforce the admission gate. `false` (default) = dry-run: count + expose
    /// metrics but never reject; `true` = shed overflow with gRPC `Unavailable`.
    #[arg(long, env = "GATEWAY_ADMISSION_ENFORCE", default_value_t = false)]
    pub admission_enforce: bool,

    /// Range-pool (GPU) max concurrent in-flight.
    #[arg(
        long,
        env = "GATEWAY_ADMISSION_RANGE_MAX_INFLIGHT",
        default_value_t = 1
    )]
    pub admission_range_max_inflight: usize,

    /// Agg-pool (cpunode) max concurrent in-flight.
    #[arg(long, env = "GATEWAY_ADMISSION_AGG_MAX_INFLIGHT", default_value_t = 2)]
    pub admission_agg_max_inflight: usize,

    /// Optional cap across ALL pools combined. Unset = pools independent.
    #[arg(long, env = "GATEWAY_ADMISSION_GLOBAL_MAX_INFLIGHT")]
    pub admission_global_max_inflight: Option<usize>,

    /// Comma-separated 0x-hex vk_hashes classified as Range (optional; mode is
    /// the fallback when empty).
    #[arg(long, env = "GATEWAY_ADMISSION_RANGE_VK_HASHES", value_delimiter = ',')]
    pub admission_range_vk_hashes: Option<Vec<String>>,

    /// Comma-separated 0x-hex vk_hashes classified as Agg.
    #[arg(long, env = "GATEWAY_ADMISSION_AGG_VK_HASHES", value_delimiter = ',')]
    pub admission_agg_vk_hashes: Option<Vec<String>>,

    /// Reaper sweep period (seconds).
    #[arg(long, env = "GATEWAY_ADMISSION_REAP_PERIOD_SECS", default_value_t = 60)]
    pub admission_reap_period_secs: u64,

    /// Slot TTL (seconds): the reap BACKSTOP for a committed slot. A live proof
    /// refreshes it on every non-terminal status/details poll (`touch`) AND
    /// whenever a reconcile sees it still Pending, so in normal operation the
    /// reconciler frees finished/gone slots and this only fires when reconcile
    /// CANNOT confirm liveness (cluster query persistently failing/truncated) AND
    /// no client is polling. It must therefore exceed the maximum gap between a
    /// slot's liveness signals (a client poll OR a reconcile-present observation),
    /// NOT the proof duration. It also bounds a hung pre-commit upload (a RESERVED
    /// slot is reclaimed once past this). The default (3600) clears any realistic
    /// gap.
    #[arg(long, env = "GATEWAY_ADMISSION_SLOT_TTL_SECS", default_value_t = 3600)]
    pub admission_slot_ttl_secs: u64,

    /// Reconcile absence threshold (count): a COMMITTED slot is released only
    /// after the cluster's Pending set has shown it ABSENT for this many
    /// CONSECUTIVE successful reconciles. A present observation or a client poll
    /// resets the streak to 0; a SKIPPED reconcile (truncated / timeout / query
    /// error / unimplemented) leaves the streak unchanged (it is an observation
    /// count, not a wall clock, so a gap in observation neither advances nor
    /// resets it). The wall-clock debounce window is therefore ≈ this count ×
    /// reap period. Must be >= 2 (a single anomalous empty reply must never
    /// release a live slot), and `count × reap_period` must stay below the slot
    /// TTL (else the TTL backstop fires before reconcile can). Default 3.
    #[arg(
        long,
        env = "GATEWAY_ADMISSION_RECONCILE_ABSENT_OBSERVATIONS",
        default_value_t = 3
    )]
    pub admission_reconcile_absent_observations: u32,

    /// Grace period (seconds) after a slot is COMMITTED before the reconciler may
    /// begin counting it absent. `request_proof` commits the slot just BEFORE the
    /// cluster `create_proof_request`, so during that create call the proof is not
    /// in the cluster's Pending set yet and would otherwise look "absent" to
    /// reconcile. This grace should meet or exceed the maximum create-leg latency
    /// (bounded by the cluster client's request timeout; the default 60 matches
    /// it, and the downstream absent-observation debounce adds further margin) so
    /// an in-flight create can't be reconciled away into an over-admit. It is
    /// independent of the reap cadence,
    /// so it protects even an aggressively-fast reconcile config (small
    /// `reap_period` × `absent_observations`). A committed slot is spared —
    /// exactly like a RESERVED one — until it has been committed for this long.
    /// Default 60 (matches the cluster client's channel timeout). 0 disables the
    /// grace (only safe when `absent_observations × reap_period` already exceeds
    /// the create-leg latency).
    #[arg(
        long,
        env = "GATEWAY_ADMISSION_RECONCILE_COMMIT_GRACE_SECS",
        default_value_t = 60
    )]
    pub admission_reconcile_commit_grace_secs: u64,

    /// Per-tick timeout (seconds) for the reconcile Pending-set query, so a
    /// slow/half-open cluster can't stretch the reaper cadence. Must stay within
    /// one reap period (the fetch is awaited inline in the reaper loop). Default
    /// 10.
    #[arg(
        long,
        env = "GATEWAY_ADMISSION_RECONCILE_FETCH_TIMEOUT_SECS",
        default_value_t = 10
    )]
    pub admission_reconcile_fetch_timeout_secs: u64,

    /// Enable priority-aware slot allocation (requires ENFORCE to actually
    /// hold; in dry-run it only emits `would_yield`). Default off.
    #[arg(
        long,
        env = "GATEWAY_ADMISSION_PRIORITY_ENABLE",
        default_value_t = false
    )]
    pub admission_priority_enable: bool,

    /// Per-proposer priority ranks: comma-separated `0xADDR:RANK` pairs. Lower
    /// rank = higher priority; ties allowed (equal rank → first-come-first-served
    /// among fresh demanders). Requesters not listed get the lowest priority.
    #[arg(long, env = "GATEWAY_ADMISSION_PRIORITY_ORDER", value_delimiter = ',')]
    pub admission_priority_order: Option<Vec<String>>,

    /// Priority demand freshness / max hold-open idle (seconds). A demander is
    /// "still waiting" while it retried within this window; a held slot idles at
    /// most this long waiting for a higher-priority retry. Default 90.
    #[arg(
        long,
        env = "GATEWAY_ADMISSION_PRIORITY_TTL_SECS",
        default_value_t = 90
    )]
    pub admission_priority_ttl_secs: u64,
}
