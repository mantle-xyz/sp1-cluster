use anyhow::{bail, Context, Result};
use clap::Parser;
use jemallocator::Jemalloc;
use opentelemetry_sdk::Resource;
use sp1_cluster_artifact::{
    redis::RedisArtifactClient,
    s3::{S3ArtifactClient, S3DownloadMode},
};
use sp1_cluster_common::auth::token_from_env;
use sp1_cluster_common::logger;
use sp1_cluster_network_gateway::{config::Config, run};
use tracing::info;

/// Same allocator as the coordinator and the workers. Under glibc malloc the gateway kept the
/// freed buffers of past uploads in its arenas and never returned them; jemalloc hands freed
/// pages back to the OS after its decay delay.
#[global_allocator]
static ALLOCATOR: Jemalloc = Jemalloc;

/// Purge from jemalloc's background threads. Without them an arena only decays when it is used
/// again, and the arenas of tokio workers that served a burst of uploads and then went idle
/// would keep their pages. Only enabled for linux-gnu: jemalloc-sys does not support background
/// threads on musl. Operators can override it at runtime with `_RJEM_MALLOC_CONF` (this build is
/// prefixed, so a plain `MALLOC_CONF` is ignored).
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[export_name = "_rjem_malloc_conf"]
static MALLOC_CONF: MallocConf = MallocConf(c"background_thread:true".as_ptr());

/// A C string pointer, which jemalloc reads at startup as `malloc_conf`.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[repr(transparent)]
struct MallocConf(*const std::ffi::c_char);

// SAFETY: points at a static, immutable C string literal.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
unsafe impl Sync for MallocConf {}

#[tokio::main]
async fn main() -> Result<()> {
    if let Err(e) = dotenv::dotenv() {
        eprintln!("not loading .env file: {e}");
    }
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();
    logger::init(Resource::empty());

    let mut cfg = Config::parse();
    cfg.cluster_access_token = token_from_env(Some("GATEWAY"));
    info!(
        grpc_addr = %cfg.grpc_addr,
        http_addr = %cfg.http_addr,
        cluster_rpc = %cfg.cluster_rpc,
        artifact_store = %cfg.artifact_store,
        "starting network-gateway"
    );

    match cfg.artifact_store.as_str() {
        "s3" => {
            let region = cfg
                .s3_region
                .clone()
                .context("GATEWAY_S3_REGION is required for s3 backend")?;
            let bucket = cfg
                .s3_bucket
                .clone()
                .context("GATEWAY_S3_BUCKET is required for s3 backend")?;
            let client = S3ArtifactClient::new(
                region.clone(),
                bucket,
                cfg.s3_concurrency,
                S3DownloadMode::AwsSDK(
                    S3ArtifactClient::create_s3_sdk_download_client(region).await,
                ),
            )
            .await;
            run(cfg, client).await
        }
        "redis" => {
            let nodes = cfg
                .redis_nodes
                .clone()
                .context("GATEWAY_REDIS_NODES is required for redis backend")?;
            let client = RedisArtifactClient::new(nodes, cfg.redis_pool_max_size);
            run(cfg, client).await
        }
        other => bail!("unknown GATEWAY_ARTIFACT_STORE={other} (expected s3 or redis)"),
    }
}
