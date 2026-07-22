//! End-to-end test that drives the gateway's gRPC + HTTP surface exactly the way
//! sp1-sdk's `NetworkProver` would. Stops short of running `client.prove(...)`
//! (which requires a real ELF + heavy setup) — instead exercises the proto
//! contract directly with the SDK's generated tonic clients. That's enough to
//! cover: `create_artifact` → PUT → `create_program` → `create_artifact` → PUT
//! → `request_proof` → `get_proof_request_status` polling → proof download.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sp1_cluster_artifact::{ArtifactClient, ArtifactType, InMemoryArtifactClient};
use sp1_cluster_common::{
    client::ClusterServiceClient,
    proto as cluster_pb,
    proto::{
        cluster_service_client::ClusterServiceClient as InnerClusterClient,
        cluster_service_server::{ClusterService, ClusterServiceServer},
    },
};
use sp1_cluster_network_gateway::{
    auth::AuthMode,
    build_auth,
    config::Config,
    program_store::{InMemoryProgramStore, ProgramStore},
    serve,
};
use sp1_sdk::network::proto::{
    artifact::{
        artifact_store_client::ArtifactStoreClient, ArtifactType as SdkArtifactType,
        CreateArtifactRequest,
    },
    base::{
        network::prover_network_client::ProverNetworkClient,
        types::{
            CreateProgramRequest, CreateProgramRequestBody, FulfillmentStatus,
            GetProofRequestDetailsRequest, GetProofRequestStatusRequest, MessageFormat, ProofMode,
            RequestProofRequest, RequestProofRequestBody,
        },
    },
};
use tokio::sync::oneshot;
use tonic::transport::{Channel, Endpoint};

/// In-memory fake of the cluster's API + coordinator + node. When a proof
/// request lands it pre-populates the shared artifact store with a canned
/// proof blob, then on the next `proof_request_get` reports Completed so the
/// SDK-style polling loop terminates.
#[derive(Clone)]
struct FakeCluster {
    artifacts: InMemoryArtifactClient,
    proof_bytes: Vec<u8>, // raw bincode bytes to serve as the proof (pre-zstd is the store's job)
    requests: Arc<dashmap::DashMap<String, cluster_pb::ProofRequest>>,
    // When set, `proof_request_create` fails immediately (simulates a backend
    // outage) so tests can verify the gateway releases the admission slot it
    // reserved instead of leaking it.
    fail_create: Arc<AtomicBool>,
    // When set, `proof_request_create` records the proof as Pending (a
    // non-terminal cluster status) instead of Completed, and `proof_request_get`
    // keeps reporting Pending — i.e. the proof never reaches a terminal
    // verdict on its own. Used to exercise the admission reaper/touch paths,
    // which only matter for slots that outlive a single poll. Default false
    // (Completed immediately) leaves existing tests unaffected.
    pending: Arc<AtomicBool>,
    // When set, `proof_request_list` returns an error, simulating a cluster whose
    // Pending query is unavailable — so the reconciler can't run and the TTL
    // reaper is the only path that can reclaim a committed slot.
    list_fails: Arc<AtomicBool>,
}

impl FakeCluster {
    fn new(artifacts: InMemoryArtifactClient, proof_bytes: Vec<u8>) -> Self {
        Self {
            artifacts,
            proof_bytes,
            requests: Arc::new(dashmap::DashMap::new()),
            fail_create: Arc::new(AtomicBool::new(false)),
            pending: Arc::new(AtomicBool::new(false)),
            list_fails: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[tonic::async_trait]
impl ClusterService for FakeCluster {
    async fn proof_request_create(
        &self,
        request: tonic::Request<cluster_pb::ProofRequestCreateRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        if self.fail_create.load(Ordering::SeqCst) {
            return Err(tonic::Status::internal("simulated cluster failure"));
        }
        let req = request.into_inner();

        // Pre-populate the proof artifact so a later GET on the proof_uri serves
        // what we canned. The gateway's HTTP `PUT` path zstd-wraps via
        // `upload_raw_compressed`; here we need the STORE-LEVEL bytes, which
        // `download_raw` will zstd-decode back to raw bincode on GET. For the
        // in-memory backend the two layers are identity, so just upload the
        // already-bincoded proof bytes.
        let proof_artifact_id = req
            .proof_artifact_id
            .clone()
            .expect("proof_artifact_id set");
        self.artifacts
            .upload_raw(
                &proof_artifact_id,
                ArtifactType::Proof,
                self.proof_bytes.clone(),
            )
            .await
            .expect("upload canned proof");

        let proof_status = if self.pending.load(Ordering::SeqCst) {
            cluster_pb::ProofRequestStatus::Pending
        } else {
            cluster_pb::ProofRequestStatus::Completed
        };
        self.requests.insert(
            req.proof_id.clone(),
            cluster_pb::ProofRequest {
                id: req.proof_id,
                proof_status: proof_status as i32,
                requester: req.requester,
                execution_result: Some(cluster_pb::ExecutionResult {
                    status: cluster_pb::ExecutionStatus::Executed as i32,
                    failure_cause: 0,
                    cycles: 0,
                    gas: 0,
                    public_values_hash: vec![],
                }),
                stdin_artifact_id: req.stdin_artifact_id,
                program_artifact_id: req.program_artifact_id,
                proof_artifact_id: Some(proof_artifact_id),
                options_artifact_id: req.options_artifact_id,
                cycle_limit: Some(req.cycle_limit),
                gas_limit: Some(req.gas_limit),
                deadline: req.deadline,
                handled: true,
                metadata: String::new(),
                created_at: 0,
                updated_at: 0,
                extra_data: None,
                scheduled_by: req.scheduled_by,
            },
        );
        Ok(tonic::Response::new(()))
    }

    async fn proof_request_cancel(
        &self,
        _request: tonic::Request<cluster_pb::ProofRequestCancelRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Err(tonic::Status::unimplemented("cancel"))
    }

    async fn proof_request_update(
        &self,
        _request: tonic::Request<cluster_pb::ProofRequestUpdateRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Err(tonic::Status::unimplemented("update"))
    }

    async fn proof_request_get(
        &self,
        request: tonic::Request<cluster_pb::ProofRequestGetRequest>,
    ) -> Result<tonic::Response<cluster_pb::ProofRequestGetResponse>, tonic::Status> {
        let id = request.into_inner().proof_id;
        let resp = cluster_pb::ProofRequestGetResponse {
            proof_request: self.requests.get(&id).map(|r| r.clone()),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn proof_request_list(
        &self,
        request: tonic::Request<cluster_pb::ProofRequestListRequest>,
    ) -> Result<tonic::Response<cluster_pb::ProofRequestListResponse>, tonic::Status> {
        if self.list_fails.load(Ordering::SeqCst) {
            return Err(tonic::Status::unavailable(
                "simulated proof_request_list outage",
            ));
        }
        // Honor the proof_status filter like the real cluster api, so the
        // gateway's reconciler sees only the requested states (an empty filter
        // means "no status filter"). Without this the reconciler would always
        // see every proof as still-pending and never reconcile.
        let req = request.into_inner();
        let proof_requests = self
            .requests
            .iter()
            .map(|r| r.clone())
            .filter(|r| req.proof_status.is_empty() || req.proof_status.contains(&r.proof_status))
            .collect();
        Ok(tonic::Response::new(cluster_pb::ProofRequestListResponse {
            proof_requests,
        }))
    }

    async fn healthcheck(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Ok(tonic::Response::new(()))
    }
}

/// Pick a free localhost port by briefly binding to 0.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// A running gateway + fake cluster, wired together, with everything a test
/// needs to drive the SDK-facing surface and then tear it down.
struct GatewayStack {
    network_rpc: ProverNetworkClient<Channel>,
    artifact_rpc: ArtifactStoreClient<Channel>,
    http: reqwest::Client,
    public_http_url: String,
    /// `host:port` of the dedicated `/metrics` listener (separate from
    /// `public_http_url`, see `GATEWAY_METRICS_ADDR`).
    metrics_addr: String,
    gw_grpc_shutdown_tx: oneshot::Sender<()>,
    gw_http_shutdown_tx: oneshot::Sender<()>,
    cluster_shutdown_tx: oneshot::Sender<()>,
    gateway: tokio::task::JoinHandle<()>,
    cluster_server: tokio::task::JoinHandle<()>,
    // Shared with the `FakeCluster` instance backing this stack; toggling it
    // flips whether `proof_request_create` succeeds or fails.
    fail_create: Arc<AtomicBool>,
    // Shared with the `FakeCluster` instance backing this stack; toggling it
    // flips whether newly-created proofs are recorded Pending (non-terminal,
    // never auto-completes) instead of Completed.
    pending: Arc<AtomicBool>,
    // Shared store of created proofs, so a test can drive the reconciler by
    // dropping a proof from the cluster's Pending set (remove it, or flip its
    // status to a terminal one).
    requests: Arc<dashmap::DashMap<String, cluster_pb::ProofRequest>>,
    // Shared with `FakeCluster`; toggling it makes proof_request_list error, so a
    // test can exercise the TTL reaper backstop with the reconciler unavailable.
    list_fails: Arc<AtomicBool>,
}

impl GatewayStack {
    /// Trigger both graceful-shutdown channels and wait (with a timeout) for
    /// both server tasks to exit.
    async fn shutdown(self) {
        self.gw_grpc_shutdown_tx.send(()).ok();
        self.gw_http_shutdown_tx.send(()).ok();
        self.cluster_shutdown_tx.send(()).ok();
        let _ = tokio::time::timeout(Duration::from_secs(5), self.gateway).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), self.cluster_server).await;
    }
}

/// Bring up a `FakeCluster` on one ephemeral port and a real gateway on two
/// more, wired together, mirroring the bring-up every e2e test needs. Callers
/// get back connected SDK-style clients plus shutdown handles.
async fn spawn_gateway_stack(
    proof_bytes: Vec<u8>,
    admission_overrides: impl FnOnce(&mut Config),
) -> GatewayStack {
    // ---- shared in-memory artifact store ----
    let artifacts = InMemoryArtifactClient::new();

    // ---- start the fake ClusterService on an ephemeral port ----
    let cluster_port = free_port();
    let cluster_addr: SocketAddr = format!("127.0.0.1:{cluster_port}").parse().unwrap();
    let fake = FakeCluster::new(artifacts.clone(), proof_bytes);
    let fail_create = fake.fail_create.clone();
    let pending = fake.pending.clone();
    let requests = fake.requests.clone();
    let list_fails = fake.list_fails.clone();
    let (cluster_shutdown_tx, cluster_shutdown_rx) = oneshot::channel::<()>();
    let cluster_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ClusterServiceServer::new(fake))
            .serve_with_shutdown(cluster_addr, async move {
                cluster_shutdown_rx.await.ok();
            })
            .await
            .unwrap();
    });

    // ---- connect a real `ClusterServiceClient` to the fake ----
    // Use a lazy channel so we don't race the server's readiness.
    let cluster_rpc = format!("http://{cluster_addr}");
    let channel = Endpoint::from_shared(cluster_rpc.clone())
        .unwrap()
        .connect_lazy();
    // `ClusterServiceClient` retries Internal/Unavailable/etc. with backoff;
    // the crate default's `max_elapsed_time` is 15 minutes, which would make
    // any test that exercises a failing cluster call (e.g. the admission
    // release-on-error test) hang for the length of the test suite. Keep the
    // retry loop itself (some tests rely on eventual success against the
    // fake) but cap it at a test-scale duration.
    let backoff = backoff::ExponentialBackoffBuilder::default()
        .with_initial_interval(Duration::from_millis(5))
        .with_max_interval(Duration::from_millis(20))
        .with_max_elapsed_time(Some(Duration::from_millis(200)))
        .build();
    let cluster = ClusterServiceClient {
        rpc: InnerClusterClient::new(channel),
        backoff,
    };

    // ---- start the gateway ----
    let grpc_port = free_port();
    let http_port = free_port();
    let metrics_port = free_port();
    let public_http_url = format!("http://127.0.0.1:{http_port}");
    let mut cfg = Config {
        grpc_addr: format!("127.0.0.1:{grpc_port}"),
        http_addr: format!("127.0.0.1:{http_port}"),
        metrics_addr: format!("127.0.0.1:{metrics_port}"),
        public_http_url: public_http_url.clone(),
        cluster_rpc: cluster_rpc.clone(),
        artifact_store: "unused".into(),
        s3_bucket: None,
        s3_region: None,
        s3_concurrency: 0,
        redis_nodes: None,
        redis_pool_max_size: 0,
        balance_amount: None,
        auth_mode: sp1_cluster_network_gateway::auth::AuthMode::None,
        auth_allowlist: None,
        program_store: "memory".into(),
        program_store_dir: None,
        admission_enforce: false,
        admission_range_max_inflight: 1,
        admission_agg_max_inflight: 2,
        admission_global_max_inflight: None,
        admission_range_vk_hashes: None,
        admission_agg_vk_hashes: None,
        admission_reap_period_secs: 60,
        admission_slot_ttl_secs: 3600,
        admission_reconcile_absent_observations: 3,
        // 0 = no post-commit grace: the FakeCluster create is instant, so there's
        // no create-window race to guard against, and the reconcile tests want
        // prompt release. Production defaults this to 60s.
        admission_reconcile_commit_grace_secs: 0,
        admission_reconcile_fetch_timeout_secs: 5,
        admission_priority_enable: false,
        admission_priority_order: None,
        admission_priority_ttl_secs: 90,
    };
    admission_overrides(&mut cfg);
    // Build `Auth` from the (possibly-overridden) config so tests can exercise
    // `AuthMode::Verify` end-to-end. With the default `AuthMode::None` this is
    // identical to the previous `Auth::default()`, leaving existing tests
    // unaffected.
    let auth = build_auth(&cfg).expect("build_auth");
    let program_store: Arc<dyn ProgramStore> = Arc::new(InMemoryProgramStore::new());
    let (gw_grpc_shutdown_tx, gw_grpc_shutdown_rx) = oneshot::channel::<()>();
    let (gw_http_shutdown_tx, gw_http_shutdown_rx) = oneshot::channel::<()>();
    let gateway_artifacts = artifacts.clone();
    let gateway = tokio::spawn(async move {
        serve(
            cfg,
            gateway_artifacts,
            cluster,
            auth,
            program_store,
            async move {
                gw_grpc_shutdown_rx.await.ok();
            },
            async move {
                gw_http_shutdown_rx.await.ok();
            },
        )
        .await
        .unwrap();
    });

    // Give the gateway a moment to bind.
    wait_for_port(http_port).await;
    wait_for_port(grpc_port).await;
    wait_for_port(metrics_port).await;

    // ---- connect the SDK-side clients ----
    let gw_channel = Endpoint::from_shared(format!("http://127.0.0.1:{grpc_port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let artifact_rpc = ArtifactStoreClient::new(gw_channel.clone());
    let network_rpc = ProverNetworkClient::new(gw_channel);
    let http = reqwest::Client::new();

    GatewayStack {
        network_rpc,
        artifact_rpc,
        http,
        public_http_url,
        metrics_addr: format!("127.0.0.1:{metrics_port}"),
        gw_grpc_shutdown_tx,
        gw_http_shutdown_tx,
        cluster_shutdown_tx,
        gateway,
        cluster_server,
        fail_create,
        pending,
        requests,
        list_fails,
    }
}

#[tokio::test]
async fn e2e_register_program_request_proof_download() {
    // ---- canned proof bytes (raw bincode, per SDK wire format) ----
    let proof_bytes: Vec<u8> = (0..1024u16).flat_map(|x| x.to_le_bytes()).collect();

    let mut stack = spawn_gateway_stack(proof_bytes.clone(), |_cfg| {}).await;
    let public_http_url = stack.public_http_url.clone();

    // 1) upload the ELF ("program")
    let elf_bytes = b"fake-elf-bytes".to_vec();
    let program_uri = create_artifact_put(
        &mut stack.artifact_rpc,
        &stack.http,
        SdkArtifactType::Program,
        &elf_bytes,
    )
    .await;

    // 2) register_program: record vk_hash → program_artifact_id sidecar
    let vk_hash = vec![0xaa; 32];
    let vk_bytes = b"fake-vk".to_vec();
    let body = CreateProgramRequestBody {
        nonce: 0,
        vk_hash: vk_hash.clone(),
        vk: vk_bytes.clone(),
        program_uri,
    };
    stack
        .network_rpc
        .create_program(CreateProgramRequest {
            format: MessageFormat::Binary as i32,
            signature: vec![],
            body: Some(body),
        })
        .await
        .unwrap();

    // 3) upload stdin
    let stdin_bytes = b"fake-stdin".to_vec();
    let stdin_uri = create_artifact_put(
        &mut stack.artifact_rpc,
        &stack.http,
        SdkArtifactType::Stdin,
        &stdin_bytes,
    )
    .await;

    // 4) request_proof
    let body = RequestProofRequestBody {
        nonce: 0,
        vk_hash: vk_hash.clone(),
        version: "test".into(),
        mode: ProofMode::Core as i32,
        strategy: 2, // Reserved
        stdin_uri,
        deadline: u64::MAX,
        cycle_limit: 0,
        gas_limit: 0,
        min_auction_period: 0,
        whitelist: vec![],
    };
    let resp = stack
        .network_rpc
        .request_proof(RequestProofRequest {
            format: MessageFormat::Binary as i32,
            signature: vec![],
            body: Some(body),
        })
        .await
        .unwrap()
        .into_inner();
    let request_id = resp.body.expect("body").request_id;
    assert!(!request_id.is_empty());

    // 5) poll get_proof_request_status (fake reports Completed immediately)
    let status = stack
        .network_rpc
        .get_proof_request_status(GetProofRequestStatusRequest {
            request_id: request_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        status.fulfillment_status,
        FulfillmentStatus::Fulfilled as i32
    );
    let proof_uri = status.proof_uri.expect("proof_uri on Fulfilled");
    assert!(
        proof_uri.starts_with(&public_http_url),
        "expected gateway URL, got {proof_uri}"
    );

    // 6) GET proof_uri — gateway `download_raw` zstd-decodes; InMemoryArtifactClient
    // is identity, so bytes come back as what we uploaded (raw bincode of the "proof").
    let got = stack.http.get(&proof_uri).send().await.unwrap();
    assert!(got.status().is_success());
    let got_bytes = got.bytes().await.unwrap().to_vec();
    assert_eq!(
        got_bytes, proof_bytes,
        "proof bytes must round-trip byte-for-byte"
    );

    // ---- shutdown ----
    stack.shutdown().await;
}

/// The admission gate sheds a second Compressed (Range-pool) request once the
/// pool cap is exhausted, then re-admits after the first request's terminal
/// status poll releases its slot.
#[tokio::test]
async fn e2e_admission_sheds_over_cap_then_readmits() {
    let proof_bytes: Vec<u8> = (0..256u16).flat_map(|x| x.to_le_bytes()).collect();

    let mut stack = spawn_gateway_stack(proof_bytes, |cfg| {
        cfg.admission_enforce = true;
        cfg.admission_range_max_inflight = 1;
    })
    .await;

    // ---- register a program ----
    let elf_bytes = b"fake-elf-bytes".to_vec();
    let program_uri = create_artifact_put(
        &mut stack.artifact_rpc,
        &stack.http,
        SdkArtifactType::Program,
        &elf_bytes,
    )
    .await;

    let vk_hash = vec![0xbb; 32];
    let vk_bytes = b"fake-vk".to_vec();
    let body = CreateProgramRequestBody {
        nonce: 0,
        vk_hash: vk_hash.clone(),
        vk: vk_bytes.clone(),
        program_uri,
    };
    stack
        .network_rpc
        .create_program(CreateProgramRequest {
            format: MessageFormat::Binary as i32,
            signature: vec![],
            body: Some(body),
        })
        .await
        .unwrap();

    // Helper to build a fresh Compressed request_proof body (each call mints
    // its own request_id/proof_id, i.e. its own admission slot).
    async fn compressed_request_body(
        stack: &mut GatewayStack,
        vk_hash: &[u8],
    ) -> RequestProofRequestBody {
        let stdin_bytes = b"fake-stdin".to_vec();
        let stdin_uri = create_artifact_put(
            &mut stack.artifact_rpc,
            &stack.http,
            SdkArtifactType::Stdin,
            &stdin_bytes,
        )
        .await;
        RequestProofRequestBody {
            nonce: 0,
            vk_hash: vk_hash.to_vec(),
            version: "test".into(),
            mode: ProofMode::Compressed as i32,
            strategy: 2, // Reserved
            stdin_uri,
            deadline: u64::MAX,
            cycle_limit: 0,
            gas_limit: 0,
            min_auction_period: 0,
            whitelist: vec![],
        }
    }

    // 1) request_proof #1 (Compressed) — holds the single Range slot.
    let body1 = compressed_request_body(&mut stack, &vk_hash).await;
    let resp1 = stack
        .network_rpc
        .request_proof(RequestProofRequest {
            format: MessageFormat::Binary as i32,
            signature: vec![],
            body: Some(body1),
        })
        .await
        .expect("request #1 should be admitted (Range pool empty)")
        .into_inner();
    let request_id_1 = resp1.body.expect("body").request_id;
    assert!(!request_id_1.is_empty());

    // 2) request_proof #2 (Compressed) — Range pool cap (1) already held by
    // #1, so this must be shed with Unavailable. Note: do NOT poll status for
    // #1 before this — the fake marks proofs Completed on create, but the
    // gate only releases the slot on a terminal status poll.
    let body2 = compressed_request_body(&mut stack, &vk_hash).await;
    let err = stack
        .network_rpc
        .request_proof(RequestProofRequest {
            format: MessageFormat::Binary as i32,
            signature: vec![],
            body: Some(body2),
        })
        .await
        .expect_err("request #2 should be shed: Range pool is at cap");
    assert_eq!(
        err.code(),
        tonic::Code::Unavailable,
        "expected Unavailable, got {err:?}"
    );
    // The shed must carry the admission marker (metadata trailer + message
    // token) so the proof-router settles it neutrally instead of tripping its
    // circuit breaker and failing over to Succinct.
    assert!(
        err.metadata().contains_key("x-sp1-admission-shed"),
        "shed must carry the x-sp1-admission-shed metadata trailer, got {err:?}"
    );
    assert!(
        err.message().contains("x-sp1-admission-shed"),
        "shed message must carry the marker token as a proxy-robust fallback, got {err:?}"
    );

    // 3) poll get_proof_request_status for #1 — fake reports Completed, so
    // this observes a terminal Fulfilled verdict and releases #1's slot.
    let status1 = stack
        .network_rpc
        .get_proof_request_status(GetProofRequestStatusRequest {
            request_id: request_id_1.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        status1.fulfillment_status,
        FulfillmentStatus::Fulfilled as i32
    );

    // 4) request_proof #3 (Compressed) — the Range slot freed by #1's release
    // means this is admitted again.
    let body3 = compressed_request_body(&mut stack, &vk_hash).await;
    let resp3 = stack
        .network_rpc
        .request_proof(RequestProofRequest {
            format: MessageFormat::Binary as i32,
            signature: vec![],
            body: Some(body3),
        })
        .await
        .expect("request #3 should be re-admitted after #1's slot was released")
        .into_inner();
    let request_id_3 = resp3.body.expect("body").request_id;
    assert!(!request_id_3.is_empty());
    assert_ne!(request_id_3, request_id_1, "each request mints its own id");

    // ---- shutdown ----
    stack.shutdown().await;
}

/// A `request_proof` whose cluster `create_proof_request` errors keeps its
/// admission slot COMMITTED (the create may have registered the proof and only
/// lost the response — releasing here would over-admit). The slot is then held
/// until the RECONCILER observes the proof absent from the cluster's Pending set
/// and releases it. This exercises both the commit-before-create behavior (a
/// follow-up request is shed while the slot is held) AND the full reconcile
/// wiring (fetch Pending → reconcile → release), which then re-admits.
#[tokio::test]
async fn e2e_admission_create_failure_holds_slot_until_reconciled() {
    let proof_bytes: Vec<u8> = (0..256u16).flat_map(|x| x.to_le_bytes()).collect();

    let mut stack = spawn_gateway_stack(proof_bytes, |cfg| {
        cfg.admission_enforce = true;
        cfg.admission_range_max_inflight = 1;
        // Fast reconcile so the test doesn't wait the production cadence: reap
        // every 1s, release a committed-absent slot after 2 consecutive absent
        // observations (≈2s at a 1s reap period).
        cfg.admission_reap_period_secs = 1;
        cfg.admission_reconcile_fetch_timeout_secs = 1;
        cfg.admission_reconcile_absent_observations = 2;
    })
    .await;

    // ---- register a program ----
    let elf_bytes = b"fake-elf-bytes".to_vec();
    let program_uri = create_artifact_put(
        &mut stack.artifact_rpc,
        &stack.http,
        SdkArtifactType::Program,
        &elf_bytes,
    )
    .await;

    let vk_hash = vec![0xcc; 32];
    let vk_bytes = b"fake-vk".to_vec();
    let body = CreateProgramRequestBody {
        nonce: 0,
        vk_hash: vk_hash.clone(),
        vk: vk_bytes.clone(),
        program_uri,
    };
    stack
        .network_rpc
        .create_program(CreateProgramRequest {
            format: MessageFormat::Binary as i32,
            signature: vec![],
            body: Some(body),
        })
        .await
        .unwrap();

    async fn compressed_request_body(
        stack: &mut GatewayStack,
        vk_hash: &[u8],
    ) -> RequestProofRequestBody {
        let stdin_bytes = b"fake-stdin".to_vec();
        let stdin_uri = create_artifact_put(
            &mut stack.artifact_rpc,
            &stack.http,
            SdkArtifactType::Stdin,
            &stdin_bytes,
        )
        .await;
        RequestProofRequestBody {
            nonce: 0,
            vk_hash: vk_hash.to_vec(),
            version: "test".into(),
            mode: ProofMode::Compressed as i32,
            strategy: 2, // Reserved
            stdin_uri,
            deadline: u64::MAX,
            cycle_limit: 0,
            gas_limit: 0,
            min_auction_period: 0,
            whitelist: vec![],
        }
    }

    // 1) make the cluster's create_proof_request fail, then send a Compressed
    // request — it reserves the sole Range slot, commits it (just before the
    // create), the cluster call errors, and request_proof surfaces Internal. The
    // slot stays COMMITTED (the create's fate is ambiguous).
    stack.fail_create.store(true, Ordering::SeqCst);
    let body1 = compressed_request_body(&mut stack, &vk_hash).await;
    let err = stack
        .network_rpc
        .request_proof(RequestProofRequest {
            format: MessageFormat::Binary as i32,
            signature: vec![],
            body: Some(body1),
        })
        .await
        .expect_err("cluster create_proof_request failure must surface as an error");
    assert_eq!(
        err.code(),
        tonic::Code::Internal,
        "expected Internal, got {err:?}"
    );

    // 2) the slot is HELD (committed), so an immediate follow-up (cap 1) is shed
    // — this is the deliberate no-over-admit behavior, not a leak.
    stack.fail_create.store(false, Ordering::SeqCst);
    let body2 = compressed_request_body(&mut stack, &vk_hash).await;
    let shed = stack
        .network_rpc
        .request_proof(RequestProofRequest {
            format: MessageFormat::Binary as i32,
            signature: vec![],
            body: Some(body2),
        })
        .await
        .expect_err("while the failed request's slot is still committed, a 2nd request is shed");
    assert_eq!(
        shed.code(),
        tonic::Code::Unavailable,
        "expected an admission shed (Unavailable), got {shed:?}"
    );

    // 3) the failed create never registered the proof in the cluster, so the
    // reconciler (fetch Pending → reconcile) sees the committed slot as absent
    // and releases it after the absence-observation threshold. A follow-up then
    // admits.
    // Poll until it succeeds (bounded), rather than a fixed sleep.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut admitted = None;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let body = compressed_request_body(&mut stack, &vk_hash).await;
        match stack
            .network_rpc
            .request_proof(RequestProofRequest {
                format: MessageFormat::Binary as i32,
                signature: vec![],
                body: Some(body),
            })
            .await
        {
            Ok(resp) => {
                admitted = Some(resp.into_inner().body.expect("body").request_id);
                break;
            }
            Err(e) => assert_eq!(
                e.code(),
                tonic::Code::Unavailable,
                "pre-reconcile requests are shed; got {e:?}"
            ),
        }
    }
    let request_id = admitted.expect("the reconciler must release the phantom slot and re-admit");
    assert!(!request_id.is_empty());

    // ---- shutdown ----
    stack.shutdown().await;
}

/// The reconciler releases a COMMITTED slot once its proof leaves the cluster's
/// Pending set (completed). Admit a Pending proof (holds the sole Range slot),
/// flip its stored status to Completed so `proof_request_list` (Pending filter)
/// no longer returns it, and confirm the reconciler frees the slot and re-admits.
#[tokio::test]
async fn e2e_admission_reconcile_releases_completed_proof() {
    let proof_bytes: Vec<u8> = (0..256u16).flat_map(|x| x.to_le_bytes()).collect();

    let mut stack = spawn_gateway_stack(proof_bytes, |cfg| {
        cfg.admission_enforce = true;
        cfg.admission_range_max_inflight = 1;
        cfg.admission_reap_period_secs = 1;
        cfg.admission_reconcile_fetch_timeout_secs = 1;
        cfg.admission_reconcile_absent_observations = 2;
    })
    .await;

    let vk_hash = vec![0xdd; 32];
    register_program(&mut stack, &vk_hash).await;

    // Newly-created proofs stay Pending (never auto-complete), so the first
    // request commits the sole Range slot and holds it.
    stack.pending.store(true, Ordering::SeqCst);
    let body1 = compressed_body(&mut stack, &vk_hash).await;
    send_request_proof(&mut stack, body1)
        .await
        .expect("first request admitted");

    // Cap 1 is now full: a 2nd request is shed.
    let body2 = compressed_body(&mut stack, &vk_hash).await;
    let shed = send_request_proof(&mut stack, body2)
        .await
        .expect_err("pool at capacity → shed");
    assert_eq!(shed.code(), tonic::Code::Unavailable, "got {shed:?}");

    // The proof completes in the cluster → drops out of the Pending set.
    for mut r in stack.requests.iter_mut() {
        r.value_mut().proof_status = cluster_pb::ProofRequestStatus::Completed as i32;
    }

    // The reconciler observes the committed slot absent from Pending and frees
    // it; a follow-up then admits. Poll (bounded) rather than sleep a fixed time.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut admitted = false;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let body = compressed_body(&mut stack, &vk_hash).await;
        match send_request_proof(&mut stack, body).await {
            Ok(_) => {
                admitted = true;
                break;
            }
            Err(e) => assert_eq!(e.code(), tonic::Code::Unavailable, "got {e:?}"),
        }
    }
    assert!(
        admitted,
        "the reconciler must release the completed proof's slot and re-admit"
    );

    stack.shutdown().await;
}

/// Register a program under `vk_hash` (uploads a canned ELF, then
/// `create_program`). Shared setup for the admission e2e tests below.
async fn register_program(stack: &mut GatewayStack, vk_hash: &[u8]) {
    let elf_bytes = b"fake-elf-bytes".to_vec();
    let program_uri = create_artifact_put(
        &mut stack.artifact_rpc,
        &stack.http,
        SdkArtifactType::Program,
        &elf_bytes,
    )
    .await;
    let body = CreateProgramRequestBody {
        nonce: 0,
        vk_hash: vk_hash.to_vec(),
        vk: b"fake-vk".to_vec(),
        program_uri,
    };
    stack
        .network_rpc
        .create_program(CreateProgramRequest {
            format: MessageFormat::Binary as i32,
            signature: vec![],
            body: Some(body),
        })
        .await
        .unwrap();
}

/// Build a fresh Compressed (Range-pool) `request_proof` body against an
/// already-registered `vk_hash`. Each call uploads its own stdin artifact, so
/// distinct calls mint distinct proof_ids (i.e. distinct admission slots).
async fn compressed_body(stack: &mut GatewayStack, vk_hash: &[u8]) -> RequestProofRequestBody {
    let stdin_bytes = b"fake-stdin".to_vec();
    let stdin_uri = create_artifact_put(
        &mut stack.artifact_rpc,
        &stack.http,
        SdkArtifactType::Stdin,
        &stdin_bytes,
    )
    .await;
    RequestProofRequestBody {
        nonce: 0,
        vk_hash: vk_hash.to_vec(),
        version: "test".into(),
        mode: ProofMode::Compressed as i32,
        strategy: 2, // Reserved
        stdin_uri,
        deadline: u64::MAX,
        cycle_limit: 0,
        gas_limit: 0,
        min_auction_period: 0,
        whitelist: vec![],
    }
}

/// Send `request_proof`, returning the minted `request_id` on success. Kept
/// as a `Result` (rather than unwrapping) so callers can assert on the
/// rejection's gRPC status code too.
async fn send_request_proof(
    stack: &mut GatewayStack,
    body: RequestProofRequestBody,
) -> Result<Vec<u8>, tonic::Status> {
    let resp = stack
        .network_rpc
        .request_proof(RequestProofRequest {
            format: MessageFormat::Binary as i32,
            signature: vec![],
            body: Some(body),
        })
        .await?;
    Ok(resp.into_inner().body.expect("body").request_id)
}

/// Dry-run mode (`admission_enforce=false`) must count and expose metrics but
/// never reject — even once a pool is at (or past) its cap. This is the
/// contract that lets operators observe admission pressure before flipping
/// enforcement on.
#[tokio::test]
async fn e2e_admission_dry_run_admits_over_cap() {
    let proof_bytes: Vec<u8> = (0..256u16).flat_map(|x| x.to_le_bytes()).collect();
    let mut stack = spawn_gateway_stack(proof_bytes, |cfg| {
        cfg.admission_enforce = false;
        cfg.admission_range_max_inflight = 1;
    })
    .await;

    let vk_hash = vec![0xd0; 32];
    register_program(&mut stack, &vk_hash).await;

    // #1 holds the sole Range slot. Deliberately not polled — dry-run must
    // not need a release to admit the next request anyway.
    let body1 = compressed_body(&mut stack, &vk_hash).await;
    let request_id_1 = send_request_proof(&mut stack, body1)
        .await
        .expect("request #1 admitted (pool empty)");
    assert!(!request_id_1.is_empty());

    // #2 would be shed in enforce mode (pool at cap 1), but dry-run must
    // admit it anyway — the whole point of enforce=false is observe, never shed.
    let body2 = compressed_body(&mut stack, &vk_hash).await;
    let request_id_2 = send_request_proof(&mut stack, body2)
        .await
        .expect("dry-run must admit past cap, not reject");
    assert!(!request_id_2.is_empty());
    assert_ne!(request_id_1, request_id_2, "each request mints its own id");

    stack.shutdown().await;
}

/// `get_proof_request_details` (not just `get_proof_request_status`) must
/// also observe a terminal verdict and release the admission slot — the two
/// RPCs are separate code paths in `ProverNetworkImpl` and both need the
/// release wired in.
#[tokio::test]
async fn e2e_admission_releases_on_details_terminal() {
    let proof_bytes: Vec<u8> = (0..256u16).flat_map(|x| x.to_le_bytes()).collect();
    let mut stack = spawn_gateway_stack(proof_bytes, |cfg| {
        cfg.admission_enforce = true;
        cfg.admission_range_max_inflight = 1;
    })
    .await;

    let vk_hash = vec![0xd1; 32];
    register_program(&mut stack, &vk_hash).await;

    // #1 holds the sole Range slot.
    let body1 = compressed_body(&mut stack, &vk_hash).await;
    let request_id_1 = send_request_proof(&mut stack, body1)
        .await
        .expect("request #1 admitted (pool empty)");

    // #2 is shed: the pool is at cap and #1's slot hasn't been released yet.
    let body2 = compressed_body(&mut stack, &vk_hash).await;
    let err = send_request_proof(&mut stack, body2)
        .await
        .expect_err("request #2 should be shed: Range pool is at cap");
    assert_eq!(
        err.code(),
        tonic::Code::Unavailable,
        "expected Unavailable, got {err:?}"
    );

    // get_proof_request_details for #1 (NOT get_proof_request_status) — the
    // fake reports Completed, so this observes a terminal Fulfilled verdict
    // via the details path specifically, and must release #1's slot.
    let details = stack
        .network_rpc
        .get_proof_request_details(GetProofRequestDetailsRequest {
            request_id: request_id_1.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let fulfillment = details.request.expect("request present").fulfillment_status;
    assert_eq!(fulfillment, FulfillmentStatus::Fulfilled as i32);

    // #3 is admitted: #1's slot was freed by the details-path release.
    let body3 = compressed_body(&mut stack, &vk_hash).await;
    let request_id_3 = send_request_proof(&mut stack, body3)
        .await
        .expect("request #3 re-admitted after details released #1's slot");
    assert_ne!(request_id_3, request_id_1, "each request mints its own id");

    stack.shutdown().await;
}

/// The dedicated `/metrics` listener (`GATEWAY_METRICS_ADDR`, off the public
/// artifact HTTP surface) must actually serve the admission counters, with
/// pool labels, once requests have flowed through the gate.
#[tokio::test]
async fn e2e_admission_metrics_endpoint() {
    let proof_bytes: Vec<u8> = (0..256u16).flat_map(|x| x.to_le_bytes()).collect();
    let mut stack = spawn_gateway_stack(proof_bytes, |cfg| {
        cfg.admission_enforce = true;
        cfg.admission_range_max_inflight = 1;
    })
    .await;

    let vk_hash = vec![0xd2; 32];
    register_program(&mut stack, &vk_hash).await;

    // One admitted request (holds the sole Range slot)...
    let body1 = compressed_body(&mut stack, &vk_hash).await;
    send_request_proof(&mut stack, body1)
        .await
        .expect("request #1 admitted (pool empty)");

    // ...and one rejected request (pool at cap).
    let body2 = compressed_body(&mut stack, &vk_hash).await;
    let err = send_request_proof(&mut stack, body2)
        .await
        .expect_err("request #2 should be shed: Range pool is at cap");
    assert_eq!(err.code(), tonic::Code::Unavailable);

    let body = stack
        .http
        .get(format!("http://{}/metrics", stack.metrics_addr))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    // Assert the SAMPLE lines with values (label-set present ⇒ the counter
    // actually incremented), not just the always-present `# TYPE` name — the
    // latter would pass even if the gate never counted anything.
    assert!(
        body.contains("gateway_admission_admitted_total{pool=\"range\"} 1"),
        "admitted_total{{range}} should be 1 in:\n{body}"
    );
    assert!(
        body.contains("gateway_admission_rejected_total{pool=\"range\"} 1"),
        "rejected_total{{range}} should be 1 in:\n{body}"
    );

    stack.shutdown().await;
}

/// A slot whose release was lost (or whose proof was simply abandoned by the
/// client) must eventually be reclaimed by the background reaper — otherwise
/// it leaks capacity forever. Uses `pending` mode so the fake cluster never
/// reports a terminal status on its own; only the reaper can free the slot.
#[tokio::test]
async fn e2e_admission_reaper_reclaims_abandoned() {
    let proof_bytes: Vec<u8> = (0..256u16).flat_map(|x| x.to_le_bytes()).collect();
    let mut stack = spawn_gateway_stack(proof_bytes, |cfg| {
        cfg.admission_enforce = true;
        cfg.admission_range_max_inflight = 1;
        // Reap sweeps every 1s; a committed slot's TTL backstop is 3s — the
        // smallest legal ttl here, since the absence debounce (2 observations) ×
        // reap (1s) = 2s must stay strictly below it. The reaper's first sweep
        // fires after ~1s and every period thereafter; sleeping ~4.7s below
        // guarantees a sweep observes the slot past its 3s ttl without being so
        // tight that jitter flakes it.
        cfg.admission_reap_period_secs = 1;
        cfg.admission_slot_ttl_secs = 3;
        cfg.admission_reconcile_absent_observations = 2;
        // Fetch timeout must be <= the reap period (it is awaited inline in the
        // reaper loop).
        cfg.admission_reconcile_fetch_timeout_secs = 1;
    })
    .await;
    // Proof stays Pending (never terminal) AND the Pending query fails, so the
    // reconciler can't confirm/refresh the slot — the TTL reaper is the ONLY path
    // that can reclaim it (which is exactly what this test covers). Without the
    // list outage the reconciler would keep refreshing the still-Pending slot and
    // the reaper would correctly never fire.
    stack.pending.store(true, Ordering::SeqCst);
    stack.list_fails.store(true, Ordering::SeqCst);

    let vk_hash = vec![0xd3; 32];
    register_program(&mut stack, &vk_hash).await;

    // #1 holds the sole Range slot. It is never polled, so nothing but the
    // reaper can ever touch or release it.
    let body1 = compressed_body(&mut stack, &vk_hash).await;
    let request_id_1 = send_request_proof(&mut stack, body1)
        .await
        .expect("request #1 admitted (pool empty)");
    assert!(!request_id_1.is_empty());

    // Let the reaper run past ttl (3s) plus at least one full reap period (1s).
    tokio::time::sleep(Duration::from_millis(4700)).await;

    // #2 is admitted: the reaper must have reclaimed #1's abandoned slot.
    let body2 = compressed_body(&mut stack, &vk_hash).await;
    send_request_proof(&mut stack, body2)
        .await
        .expect("request #2 admitted: reaper must have reclaimed #1's abandoned slot");

    stack.shutdown().await;
}

/// A slot that IS being actively polled must survive the reaper indefinitely:
/// every non-terminal poll calls `touch`, refreshing the slot's age. Uses
/// `pending` mode (fake cluster never resolves on its own) so the only thing
/// keeping the slot alive is the polling loop itself.
#[tokio::test]
async fn e2e_admission_touch_keeps_polled_slot() {
    let proof_bytes: Vec<u8> = (0..256u16).flat_map(|x| x.to_le_bytes()).collect();
    let mut stack = spawn_gateway_stack(proof_bytes, |cfg| {
        cfg.admission_enforce = true;
        cfg.admission_range_max_inflight = 1;
        // reap every 1s against a 3s ttl — the smallest legal ttl, since the
        // absence debounce (2 observations) × reap (1s) = 2s must stay strictly
        // below it. The ~300ms poll loop below refreshes the slot well within the
        // 3s ttl on every sweep, while an unpolled slot would be reaped after 3s.
        cfg.admission_reap_period_secs = 1;
        cfg.admission_slot_ttl_secs = 3;
        cfg.admission_reconcile_absent_observations = 2;
        // Fetch timeout must be <= the reap period (it is awaited inline in the
        // reaper loop).
        cfg.admission_reconcile_fetch_timeout_secs = 1;
    })
    .await;
    stack.pending.store(true, Ordering::SeqCst); // never reaches a terminal status on its own
                                                 // Disable the cluster Pending query so the reconciler stays inert: the reap
                                                 // deadline is then refreshed ONLY by touch-on-poll, so this test proves the
                                                 // polling loop (not a reconcile-present refresh) is what keeps the slot alive.
    stack.list_fails.store(true, Ordering::SeqCst);

    let vk_hash = vec![0xd4; 32];
    register_program(&mut stack, &vk_hash).await;

    let body1 = compressed_body(&mut stack, &vk_hash).await;
    let request_id_1 = send_request_proof(&mut stack, body1)
        .await
        .expect("request #1 admitted (pool empty)");

    // Poll get_proof_request_status every ~300ms for ~4.5s (15 iterations),
    // comfortably past the 3s ttl so an unpolled slot WOULD be reaped. Each
    // non-terminal poll (Pending → Requested, not terminal) calls `touch`,
    // refreshing the slot's age so the reaper — sweeping every 1s against a 3s
    // ttl — never observes it stale.
    for _ in 0..15 {
        let status = stack
            .network_rpc
            .get_proof_request_status(GetProofRequestStatusRequest {
                request_id: request_id_1.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            status.fulfillment_status,
            FulfillmentStatus::Requested as i32,
            "fake cluster is in pending mode; status must stay non-terminal for this test to be meaningful"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // #2 is still shed: the actively-polled slot was never reaped.
    let body2 = compressed_body(&mut stack, &vk_hash).await;
    let err = send_request_proof(&mut stack, body2)
        .await
        .expect_err("request #2 must be shed: #1's slot was kept alive by touch-on-poll");
    assert_eq!(
        err.code(),
        tonic::Code::Unavailable,
        "expected Unavailable, got {err:?}"
    );

    stack.shutdown().await;
}

/// Sign the encoded proto `body` (the exact bytes the gateway feeds to
/// `Auth::authorize`) with `signer`, yielding a 65-byte `[r||s||v]` EIP-191
/// signature. Mirrors sp1-sdk's `NetworkProver` signing of `request_proof` /
/// `create_program` bodies.
fn sign_body(body: &impl prost::Message, signer: &alloy_signer_local::PrivateKeySigner) -> Vec<u8> {
    use alloy_signer::SignerSync;
    signer
        .sign_message_sync(&body.encode_to_vec())
        .unwrap()
        .as_bytes()
        .to_vec()
}

/// The fixed message `create_artifact` authenticates over (see
/// `ArtifactStoreImpl`'s `CREATE_ARTIFACT_MESSAGE`). Unlike the proto-body RPCs,
/// this signs a constant string, not the request body.
fn sign_create_artifact(signer: &alloy_signer_local::PrivateKeySigner) -> Vec<u8> {
    use alloy_signer::SignerSync;
    signer
        .sign_message_sync(b"create_artifact")
        .unwrap()
        .as_bytes()
        .to_vec()
}

/// `create_artifact` + HTTP PUT, signed for `AuthMode::Verify`. Same as
/// [`create_artifact_put`] but carries the fixed-message signature so the
/// gateway can recover a requester instead of rejecting with Unauthenticated.
async fn create_artifact_put_signed(
    artifact_rpc: &mut ArtifactStoreClient<Channel>,
    http: &reqwest::Client,
    artifact_type: SdkArtifactType,
    raw: &[u8],
    signer: &alloy_signer_local::PrivateKeySigner,
) -> String {
    let req = CreateArtifactRequest {
        artifact_type: artifact_type as i32,
        signature: sign_create_artifact(signer),
    };
    let resp = artifact_rpc
        .create_artifact(req)
        .await
        .unwrap()
        .into_inner();
    let bincoded = bincode::serialize(raw).unwrap();
    let compressed = zstd::encode_all(bincoded.as_slice(), 3).unwrap();
    let put = http
        .put(&resp.artifact_presigned_url)
        .body(compressed)
        .send()
        .await
        .unwrap();
    assert!(put.status().is_success(), "PUT failed: {}", put.status());
    resp.artifact_uri
}

/// Register a program under `vk_hash` with all RPCs signed by `signer` (needed
/// under `AuthMode::Verify`). Program bytes aren't priority-gated, so any valid
/// signer works.
async fn register_program_signed(
    stack: &mut GatewayStack,
    vk_hash: &[u8],
    signer: &alloy_signer_local::PrivateKeySigner,
) {
    let elf_bytes = b"fake-elf-bytes".to_vec();
    let program_uri = create_artifact_put_signed(
        &mut stack.artifact_rpc,
        &stack.http,
        SdkArtifactType::Program,
        &elf_bytes,
        signer,
    )
    .await;
    let body = CreateProgramRequestBody {
        nonce: 0,
        vk_hash: vk_hash.to_vec(),
        vk: b"fake-vk".to_vec(),
        program_uri,
    };
    let signature = sign_body(&body, signer);
    stack
        .network_rpc
        .create_program(CreateProgramRequest {
            format: MessageFormat::Binary as i32,
            signature,
            body: Some(body),
        })
        .await
        .unwrap();
}

/// Build a fresh Compressed (Range-pool) `request_proof` body reusing a shared
/// `stdin_uri`. Each server-side call still mints its own random request_id
/// (i.e. its own admission slot), so the stdin artifact can be shared.
fn compressed_body_with_stdin(vk_hash: &[u8], stdin_uri: &str) -> RequestProofRequestBody {
    RequestProofRequestBody {
        nonce: 0,
        vk_hash: vk_hash.to_vec(),
        version: "test".into(),
        mode: ProofMode::Compressed as i32,
        strategy: 2, // Reserved
        stdin_uri: stdin_uri.to_string(),
        deadline: u64::MAX,
        cycle_limit: 0,
        gas_limit: 0,
        min_auction_period: 0,
        whitelist: vec![],
    }
}

/// Send a `request_proof` signed by `signer` (so the gateway recovers that
/// proposer's address under `AuthMode::Verify`). Returns the minted request_id
/// on success, or the raw `tonic::Status` so callers can assert on the shed
/// code/metadata/message.
async fn send_request_proof_signed(
    stack: &mut GatewayStack,
    body: RequestProofRequestBody,
    signer: &alloy_signer_local::PrivateKeySigner,
) -> Result<Vec<u8>, tonic::Status> {
    let signature = sign_body(&body, signer);
    let resp = stack
        .network_rpc
        .request_proof(RequestProofRequest {
            format: MessageFormat::Binary as i32,
            signature,
            body: Some(body),
        })
        .await?;
    Ok(resp.into_inner().body.expect("body").request_id)
}

/// Priority-aware admission, end-to-end over gRPC under `AuthMode::Verify`:
/// when the sole Range slot frees and a higher-ranked proposer has fresh
/// demand, a lower-ranked proposer is YIELDED (shed as Unavailable) and the
/// higher-ranked one wins the slot. This is the e2e mirror of the unit test
/// `higher_rank_fresh_demand_makes_lower_yield_even_with_free_slot`.
#[tokio::test]
async fn e2e_admission_priority_admits_higher_rank() {
    use alloy_signer_local::PrivateKeySigner;

    let proof_bytes: Vec<u8> = (0..256u16).flat_map(|x| x.to_le_bytes()).collect();

    // `hi` is rank 0 (highest priority), `lo` is rank 1.
    let hi = PrivateKeySigner::random();
    let lo = PrivateKeySigner::random();
    let hi_addr = hi.address();
    let lo_addr = lo.address();

    let mut stack = spawn_gateway_stack(proof_bytes, |cfg| {
        cfg.auth_mode = AuthMode::Verify;
        cfg.admission_enforce = true;
        cfg.admission_range_max_inflight = 1;
        cfg.admission_priority_enable = true;
        // `{:x}` renders the 20-byte address as 40 lowercase hex chars (no 0x);
        // the parser hex-decodes it and compares against the recovered signer.
        cfg.admission_priority_order =
            Some(vec![format!("{hi_addr:x}:0"), format!("{lo_addr:x}:1")]);
        cfg.admission_priority_ttl_secs = 3600;
    })
    .await;

    // ---- register a program + one shared stdin artifact (signed by `hi`) ----
    let vk_hash = vec![0xd5; 32];
    register_program_signed(&mut stack, &vk_hash, &hi).await;
    let stdin_uri = create_artifact_put_signed(
        &mut stack.artifact_rpc,
        &stack.http,
        SdkArtifactType::Stdin,
        b"fake-stdin",
        &hi,
    )
    .await;

    // 1) `lo` requests → admitted, holding the sole Range slot. Not polled, so
    // the slot stays reserved (the fake marks it Completed on create, but the
    // gate only releases on a terminal status poll).
    let lo_body_1 = compressed_body_with_stdin(&vk_hash, &stdin_uri);
    let lo_req_1 = send_request_proof_signed(&mut stack, lo_body_1, &lo)
        .await
        .expect("lo admitted: Range pool empty");
    assert!(!lo_req_1.is_empty());

    // 2) `hi` requests → shed on PoolCap (slot held by `lo`). This records
    // fresh demand for `hi`, which is what makes the later yield fire.
    let hi_body_1 = compressed_body_with_stdin(&vk_hash, &stdin_uri);
    let err = send_request_proof_signed(&mut stack, hi_body_1, &hi)
        .await
        .expect_err("hi shed: Range pool at cap");
    assert_eq!(
        err.code(),
        tonic::Code::Unavailable,
        "expected Unavailable (PoolCap), got {err:?}"
    );

    // 3) poll status for `lo`'s request → terminal Fulfilled → releases the slot.
    let status = stack
        .network_rpc
        .get_proof_request_status(GetProofRequestStatusRequest {
            request_id: lo_req_1.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        status.fulfillment_status,
        FulfillmentStatus::Fulfilled as i32
    );

    // 4) `lo` requests again → the slot is now FREE, but `hi` (rank 0) has fresh
    // demand and out-ranks `lo` (rank 1), so `lo` must YIELD. The shed must
    // carry the admission marker (so the router stays neutral) and name the
    // higher-priority hold in its message.
    let lo_body_2 = compressed_body_with_stdin(&vk_hash, &stdin_uri);
    let yield_err = send_request_proof_signed(&mut stack, lo_body_2, &lo)
        .await
        .expect_err("lo must yield the freed slot to higher-ranked hi");
    assert_eq!(
        yield_err.code(),
        tonic::Code::Unavailable,
        "expected Unavailable (PriorityYield), got {yield_err:?}"
    );
    assert!(
        yield_err.metadata().contains_key("x-sp1-admission-shed"),
        "yield must carry the x-sp1-admission-shed metadata trailer, got {yield_err:?}"
    );
    assert!(
        yield_err.message().contains("higher-priority"),
        "yield message must name the higher-priority hold, got {yield_err:?}"
    );

    // 5) `hi` requests → wins the freed slot it was holding out for.
    let hi_body_2 = compressed_body_with_stdin(&vk_hash, &stdin_uri);
    let hi_req_2 = send_request_proof_signed(&mut stack, hi_body_2, &hi)
        .await
        .expect("hi wins the freed slot");
    assert!(!hi_req_2.is_empty());

    stack.shutdown().await;
}

/// create_artifact → HTTP PUT with the SDK's zstd(bincode(...)) shape. Returns
/// the gateway-emitted artifact_uri.
async fn create_artifact_put(
    artifact_rpc: &mut ArtifactStoreClient<Channel>,
    http: &reqwest::Client,
    artifact_type: SdkArtifactType,
    raw: &[u8],
) -> String {
    let req = CreateArtifactRequest {
        artifact_type: artifact_type as i32,
        ..Default::default()
    };
    let resp = artifact_rpc
        .create_artifact(req)
        .await
        .unwrap()
        .into_inner();
    // Mirror SDK's encoding: bincode then zstd level 3.
    let bincoded = bincode::serialize(raw).unwrap();
    let compressed = zstd::encode_all(bincoded.as_slice(), 3).unwrap();
    let put = http
        .put(&resp.artifact_presigned_url)
        .body(compressed)
        .send()
        .await
        .unwrap();
    assert!(put.status().is_success(), "PUT failed: {}", put.status());
    resp.artifact_uri
}

async fn wait_for_port(port: u16) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("port {port} never became ready");
}
