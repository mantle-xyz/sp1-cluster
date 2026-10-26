use chrono::prelude::*;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    compile_mantle_cluster_service();

    println!("cargo:rerun-if-env-changed=BUILD_GIT_SHA");

    let git_sha = match env::var("BUILD_GIT_SHA") {
        Ok(git_sha) => git_sha,
        Err(_) => "unknown".to_string(),
    };

    let pretty_time = Utc::now().format("%Y-%m-%dT%H:%M:%S");
    let version_string = format!("{git_sha} {pretty_time}");
    println!("cargo:rustc-env=BUILD_VERSION={version_string}");
    Ok(())
}

/// Mantle-only extension of the cluster API (`crate::mantle`).
///
/// The upstream `ClusterService` proto ships inside the published
/// `sp1-prover-types` crate, so it cannot gain fields here. The extension is a
/// second service on the same API port. It is defined in Rust rather than in a
/// `.proto` file, so it can reuse the upstream response message without
/// importing `cluster.proto` from the crates.io source tree.
fn compile_mantle_cluster_service() {
    let service = tonic_build::manual::Service::builder()
        .name("MantleClusterService")
        .package("mantle.cluster")
        .method(
            tonic_build::manual::Method::builder()
                .name("query_proof_requests")
                .route_name("QueryProofRequests")
                .input_type("crate::mantle::ProofRequestQueryRequest")
                .output_type("crate::proto::ProofRequestListResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .build();
    tonic_build::manual::Builder::new().compile(&[service]);
}
