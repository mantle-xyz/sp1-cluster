# Mantle sp1-cluster Changes

This file is the registry of every Mantle modification on top of the upstream
`succinctlabs/sp1-cluster` baseline. Read it before syncing upstream, and use it to find why a
Mantle-only line exists.

**Whenever a Mantle change is added, modified, or removed, update this file in the same PR.**

## 1. Current baseline

| Item | Value |
|---|---|
| Upstream tracking point | `succinctlabs/sp1-cluster` tag `v2.9.0` @ `3249344` (merged in PR #13, `98bd0ae`) |
| Upstream ahead, not synced | git tag `v2.9.1` @ `8c284e6` (2026-11-10, no GitHub Release yet): `d310bdf` page cancel queries and skip repeat cancels (#213), plus docs. |
| Mantle branch | `mantle_develop` (`origin` = `mantle-xyz/sp1-cluster`). Feature branches are cut from it and merged back by PR. `main` mirrors upstream (currently at `v2.9.0`) and carries no Mantle code. |
| SP1 | `6.10.0` (comes with upstream `v2.9.0`; circuits `v6.1.0`) |
| Workers | Run the **official** images `ghcr.io/succinctlabs/sp1-cluster:{node-gpu,base}-v2.9.0`. No Mantle code runs in a worker, so worker-side changes here (e.g. tests in `crates/worker`) never need a worker image. |

### 1.1 Release tags and images

Tags are cut on this repo and built by `.github/workflows/release-ghcr.yml` into
`ghcr.io/mantle-xyz/sp1-cluster:<component>-<tag>` for the three components this fork changes:
`gateway`, `coordinator`, `api`.

- Tag scheme: `mantle-v<upstream>` for a release on an upstream baseline,
  `mantle-v<upstream>-fix.<n>` for Mantle fixes on the same baseline, `-rc.<n>` for pre-releases.
- `<component>-latest` moves only on tags without `-rc` / `-alpha` / `-beta`.
- **Deploy api and gateway from the same tag** (the gateway needs the api's
  `QueryProofRequests`, §3.3). The coordinator may run a different tag.
- What each environment runs is decided in `mantlenetworkio/mantle-config`
  (`cicd/zan-gpu-prover/{mainnet,testnet}/compose/`), not here.

#### Version map

| Tag | Commit | Upstream baseline | Contents |
|---|---|---|---|
| `mantle-v2.9.0-fix.1-rc.4` | _to be tagged_ | `v2.9.0` | rc.3 + execution shards as a fixed-bucket histogram, counters exported at 0, cycles and shards recorded for completed proofs (§3.8d). Coordinator only. |
| `mantle-v2.9.0-fix.1-rc.3` | `d5a3c34` | `v2.9.0` | rc.2 + per-execution shard limit and failed-proof cleanup (§3.8c). Branch `fix/proof-expiry-and-unexecutable`, PR #15. |
| `mantle-v2.9.0-fix.1-rc.2` | `6b45f5d` | `v2.9.0` | rc.1 + gateway jemalloc and one less upload copy (§3.8b). |
| `mantle-v2.9.0-fix.1-rc.1` | `73a82d6` | `v2.9.0` | Expired proofs → Failed, cycle-limit stop → Unexecutable (§3.8a). |
| `mantle-v2.9.0` (= `-rc.1`) | `6fff81a` | `v2.9.0` | First release on the `mantle-` tag line. Everything in §3.1–§3.7. |
| `v2.8.4-admission-gate-2-rc.1` | `ccadc077` | `v2.8.4` | Old naming scheme (`v<upstream>-admission-gate-<n>`). |
| `v2.7.2-admission-gate-1` | `965df34d` | `v2.7.2` | Old naming scheme. |
| `v2.2.0-admission-gate[-2..5]` | `aceea29`… | `v2.2.0` | Old naming scheme; admission gate development. |

## 2. Design rules

1. **Add, don't edit.** New behaviour goes into new files (`admission.rs`, `mantle.rs`,
   `shards.rs`, `cleanup.rs`, …) with a small hook in the upstream file. Each hook is listed in §3
   so a sync can find it.
2. **Never change upstream's wire protocol.** The cluster proto ships inside the published
   `sp1-prover-types` crate. A Mantle API is a separate gRPC service on the same port (§3.3), and
   a Mantle payload in an existing field uses its own JSON envelope (`{"shard_limit": …}`,
   `{"expired": …}`) so it cannot be mistaken for an upstream one.
3. **Nothing Mantle-specific in the worker.** Workers run upstream images (§1). A fix that
   needs worker code is a fork of the worker image and needs its own decision.
4. **Environment variables use the component prefix** (`GATEWAY_*`, `COORDINATOR_*`); the
   config loaders only read prefixed names.

## 3. Mantle changes registry

### 3.1 Gateway: concurrency admission gate

Caps how many range / aggregation proofs the gateway admits at once, because every proof-router
environment fans in to this one gateway. Over the cap it sheds with a marked `Unavailable`,
which the SDK retries in place, so op-succinct neither records a failure nor bisects.

| File | Change |
|---|---|
| `bin/network-gateway/src/admission.rs` | **Mantle-only.** Pools (`range`, `agg`, global cap), slot lifecycle (acquire → commit → release / touch), reaper, reconcile against the cluster's runnable set, per-requester priority yield, Prometheus metrics. |
| `bin/network-gateway/src/config.rs` | `GATEWAY_METRICS_ADDR` and the `GATEWAY_ADMISSION_*` options (`ENFORCE`, `{RANGE,AGG,GLOBAL}_MAX_INFLIGHT`, `{RANGE,AGG}_VK_HASHES`, `REAP_PERIOD_SECS`, `SLOT_TTL_SECS`, `RECONCILE_*`, `PRIORITY_{ENABLE,ORDER,TTL_SECS}`). |
| `bin/network-gateway/src/lib.rs` | `build_admission`; spawns the reaper, the dedicated `/metrics` server and the restart seed; passes the controller to the service. |
| `bin/network-gateway/src/service/prover_network.rs` | `request_proof` acquires a slot before submitting and releases it on every error path; status polls `settle_slot` (release on terminal, otherwise touch). `admission_shed_status` is the one constructor of the shed `Status`. |
| `bin/network-gateway/src/status.rs` | `is_terminal`, the single terminal predicate used by `settle_slot`. |
| `bin/network-gateway/Cargo.toml` | `prometheus-client`, `eyre`; dev `backoff`. |
| `bin/network-gateway/tests/e2e.rs` | Admission e2e tests (reject over cap, readmit after release, reaper, priority, metrics). |
| `bin/test-cluster/src/cluster.rs` | Fills `metrics_addr` and `admission: Default::default()` (dry-run) for the test cluster. |

Contracts with other repos:
- The shed marker `x-sp1-admission-shed` (trailer **and** message prefix) must match
  `prove-network` `crates/proof-router/src/backend.rs` `ADMISSION_SHED_MARKER`; the router keys on
  it to not trip its circuit breaker.
- A slot is held for "Pending and deadline ≥ now", the same runnable set the coordinator and
  fulfiller use. Counting bare `Pending` let expired rows hold the cap forever.
- Single instance only: the counter is in memory, so a second gateway replica doubles the cap.

### 3.2 Gateway: recover the real `create_artifact` signer

sp1-sdk's `network::utils::sign_message` (still in 6.10.0) adds 27 to a v that is already 27/28,
so `create_artifact` signatures arrive with v = 54/55. alloy reads those as EIP-155 and recovers
a wrong address, so `Allowlist` rejects the client and `Verify` records the wrong requester.

| File | Change |
|---|---|
| `bin/network-gateway/src/auth.rs` | `Auth::authorize_create_artifact` maps v 54/55 back to 27/28, other values pass through. Tests pin the SDK's signature shape. |
| `bin/network-gateway/src/service/artifact_store.rs` | `create_artifact` calls `authorize_create_artifact`. |

**On every SDK bump** re-check `sign_message`. If the SDK fixes the double +27, this remap is
harmless, but the tests that build the old shape should then be dropped.

### 3.3 API + gateway: `MantleClusterService/QueryProofRequests`

Upstream `ProofRequestList` cannot filter by requester and returns oldest first, so the gateway's
`get_filtered_proof_requests` showed the oldest rows of the whole cluster. The extension adds a
requester filter and newest-first order.

| File | Change |
|---|---|
| `crates/common/src/mantle.rs` | **Mantle-only.** `ProofRequestQueryRequest` (prost-derived; field numbers 1, 2, 3, 5, 6 match upstream `ProofRequestListRequest`, Mantle fields start at 100) and the client helper. |
| `crates/common/build.rs` | `compile_mantle_cluster_service()` generates the service with `tonic_build::manual` (no `.proto`, reuses upstream's response message). |
| `crates/common/src/client.rs` | `ClusterServiceClient` gains a `mantle` client on the same channel and token; `from_channel` / `with_auth` constructors; `retry_call` is `pub(crate)`. |
| `crates/common/src/lib.rs`, `crates/common/Cargo.toml`, `Cargo.toml` | `pub mod mantle`; `prost`, `tonic-prost`, build-dep `tonic-build` (workspace `0.14.6`). |
| `bin/api/src/mantle.rs` | **Mantle-only.** `MantleClusterServiceImpl`, the SQL query. |
| `bin/api/src/lib.rs` | Serves the extension behind the same auth / grpc-web layers as the upstream service. |
| `bin/api/src/service.rs` | `DbProofRequest` made `pub(crate)` for reuse. |
| `bin/api/tests/mantle_query.rs` | **Mantle-only.** Postgres-backed tests, `#[ignore]`d by default (`DATABASE_URL=… cargo test --release -p sp1-cluster-api --tests -- --ignored --test-threads=1`). |
| `bin/network-gateway/src/service/prover_network.rs` | `get_filtered_proof_requests` uses `query_proof_requests`; an api without it yields `FailedPrecondition` ("deploy the api image from the same release"). |
| `bin/network-gateway/README.md` | Documents which filters round-trip. |

### 3.4 Coordinator: subscription gauges

| File | Change |
|---|---|
| `bin/coordinator/src/metrics.rs` | `set_subscription_counts` → gauges `coordinator_sub_handlers`, `coordinator_subscribers` (exported as `spn_coordinator_*`). |
| `bin/coordinator/src/server.rs` | `spawn_subscription_gauge_task`, sampled every 10s. |

The leak these gauges watched was fixed in our `68721e0` and **upstream since `v2.8.5`**; only
the gauges remain Mantle. Drop them if upstream adds equivalent metrics.

### 3.5 Gateway: artifact TTL comments

`bin/network-gateway/src/ids.rs`, `bin/network-gateway/src/program_store.rs`: comments only.
Redis artifacts expire after `ARTIFACT_TIMEOUT_SECONDS` (default 6h) and `Program` artifacts have
no TTL. Take upstream's text if it now says the same.

### 3.6 CI and image release

| File | Change |
|---|---|
| `.github/workflows/mantle-ci.yml` | **Mantle-only.** Lint (fmt, clippy, test, cargo sort) and integration (artifact, api) for `mantle_develop` on GitHub-hosted runners. Falls back to `goproxy.cn` because a Go module gnark-ffi needs has disappeared from GitHub. |
| `.github/workflows/release-ghcr.yml` | **Mantle-only.** Builds `gateway` / `coordinator` / `api` images on any tag (§1.1). |
| `.github/workflows/docker.yml` | **Deleted** (upstream's ECR + GHCR build; needs Succinct's runners and registry). Delete it again if a sync brings it back. |
| `infra/Dockerfile.{api,coordinator,network-gateway}` | **Mantle-only.** Per-component images. `sp1up` version must match the SP1 version (§1). |

The upstream workflows are kept as is. `lint.yml`, `integration.yml` and `e2e*.yml` only run
for `main` / `staging`, so `mantle-ci.yml` is the gate for `mantle_develop`. `deny.yml` still runs
on any PR touching `Cargo.toml` / `Cargo.lock` and reports new RustSec advisories; treat it as a
signal, not a gate.

### 3.7 Test cluster

`bin/test-cluster/src/cluster.rs`: new gateway / coordinator config fields filled with defaults.
Every new config field in §3 must be added here or the test cluster stops compiling.

### 3.8 Pending: on `fix/proof-expiry-and-unexecutable` (PR #15), not yet in `mantle_develop`

Move these into the sections above when PR #15 merges.

#### 3.8a Expired proofs → Failed; cycle-limit stop → Unexecutable (`73a82d6`)

| File | Change |
|---|---|
| `bin/coordinator/src/cluster.rs` | `expired_update`: a tracked proof whose row expired while still Pending gets a terminal Failed write (`{"expired":{"deadline":…}}`); previously the row stayed Pending forever. `execution_failure`: a cycle-limit stop (and, from rc.3, the shard limit) is recorded as `ExecutionStatus::Failed`, which the gateway reports as `Unexecutable`, so op-succinct splits the range on the first failure. |
| `crates/common/src/failure.rs` | `ProvingFailure::exceeded_cycle_limit` (matches the prover's two error texts), `expired_extra_data`. |
| `crates/worker/src/lib.rs` | Tests only: pin the cycle-limit error texts to the real `ExecutionError`. |
| `bin/test-cluster/src/{assert.rs,scenarios/fatal_failure.rs}` | Assertions for the new statuses. |

**On every SP1 bump** run the worker tests: `exceeded_cycle_limit` matches the `Debug` text of
`ExecutionError::ExceededCycleLimit` and the `"cycle limit exceeded:"` message.

#### 3.8b Gateway memory (`6b45f5d`)

| File | Change |
|---|---|
| `bin/network-gateway/src/main.rs`, `Cargo.toml` | jemalloc global allocator with background purge threads (linux-gnu). glibc kept freed upload buffers in its arenas. |
| `bin/network-gateway/src/artifact_http.rs` | `upload_bytes`: `Vec::from(Bytes)` instead of `to_vec()`, one copy less per PUT. |

#### 3.8c Per-execution shard limit and failed-proof cleanup (`56337aa`)

| File | Change |
|---|---|
| `bin/coordinator/src/shards.rs` | **Mantle-only.** Counts shards per execution (stream by `common_input`, bucket by submitting worker × its failure reports × cancels delivered to it). |
| `bin/coordinator/src/cleanup.rs` | **Mantle-only.** After a proof fails / expires / is rejected, deletes the `artifact_*` ids its non-root tasks name, minus the root task's (program, stdin, final proof); 30s delay, batches of 200. |
| `bin/coordinator/src/shard_cleanup_tests.rs` | **Mantle-only.** Integration tests over the real coordinator paths. |
| `bin/coordinator/src/lib.rs` | Hooks: `Proof.shard_streams` / `completed`, `create_task_from(worker_id, …)`, over-limit branch in task creation, `note_failed` in `fail_task` (before the holder checks), `note_cancelled` in heartbeat reconcile (only when delivered), cleanup in `fail_proof_internal`, `complete_proof` takes the write lock and records the histogram once. |
| `bin/coordinator/src/server.rs` | Passes the submitting worker id; builds the Redis cleaner with `RedisArtifactClient::try_new` (cleanup off on error). |
| `bin/coordinator/src/config.rs` | `COORDINATOR_MAX_SHARDS_PER_PROOF` (0 = off), `COORDINATOR_REDIS_NODES` (unset = no cleanup), `COORDINATOR_REDIS_POOL_MAX_SIZE` (default 4). |
| `bin/coordinator/src/metrics.rs` | `coordinator_proofs_over_shard_limit_total`, `coordinator_unmatched_shards_total`, `coordinator_duplicate_executions_total`, `coordinator_proof_execution_shards{outcome}` (a histogram from rc.4, §3.8d), `coordinator_failed_proof_cleanup_{ids,errors}_total`. |
| `bin/coordinator/src/cluster.rs` | `execution_failure` maps `{"shard_limit":…}` to Failed / Unspecified. |
| `crates/common/src/failure.rs` | `shard_limit_extra_data`, `is_shard_limit`. |
| `crates/artifact/src/redis.rs` | `RedisArtifactClient::try_new` (fallible constructor; `new` unchanged). |

**On every SP1 bump** check the task input layout `shards.rs` reads (`CoreExecute` `inputs[2]`,
`ProveShard` `inputs[1]` = `common_input`); `the_shard_layout_matches_sp1_prover` pins it to
sp1-prover's own `ProveShardTaskRequest::into_raw`, and `coordinator_unmatched_shards_total`
going non-zero in production means the layout moved.

#### 3.8d Execution shards histogram, zero-exported counters, cycles and shards of completed proofs (rc.4)

| File | Change |
|---|---|
| `bin/coordinator/src/metrics.rs` | `coordinator_proof_execution_shards` is exported as a Prometheus histogram built from counters: `_bucket{outcome,le}` (1k … 100k, `+Inf`), `_sum`, `_count`; query it with `histogram_quantile`. `register()` exports the alerted counters and every bucket at 0 at startup. |
| `bin/coordinator/src/store.rs` | **Mantle-only.** `ArtifactStore` (was `cleanup::ArtifactCleaner`): `delete` plus `execution_cycles`, which reads the `ExecutionOutput` a `CoreExecute` task uploaded to its only output. |
| `bin/coordinator/src/lib.rs` | `Proof.execution_cycles`; `complete_task` reads a finished `CoreExecute`'s cycles in the background (the controller deletes that output when the proof completes); `complete_proof` passes them on and, when the worker sent no `extra_data`, writes `{"execution_shards": N}`. Field and setter renamed to `artifact_store` / `set_artifact_store`. |
| `bin/coordinator/src/cluster.rs` | A completed proof with known cycles is written with `ExecutionResult { Executed, cycles, gas: 0 }`, so `execution_cycles` is filled and the gateway returns `cycles` to the SDK. |
| `crates/common/src/failure.rs` | `execution_shards_extra_data`. |
| `bin/coordinator/Cargo.toml` | `sp1-core-machine` (for `ExecutionOutput`), dev `metrics-exporter-prometheus` (render test). |

Why not a `histogram!`: spn-metrics installs `metrics-exporter-prometheus` with default settings,
which renders histograms as summaries over a rolling window of about a minute. Proofs finish
minutes apart, so the quantiles read 0. The bucket counters accumulate for the process lifetime.

`execution_gas` is written as **0, meaning "not computed"** (the API writes status, cycles and gas
together, so it cannot stay NULL once cycles are written): the proving path runs the plain
executor, which does not estimate gas, and a second gas-estimating execution per proof is not
worth its cost. Cycles need `COORDINATOR_REDIS_NODES`, as the cleanup does; the read is bounded by
30s, and a completion without cycles logs `completed without its execution cycles`.

The histogram's top quantile is a bucket bound, not the exact maximum: a proof failed by the shard
limit (limit + 1 shards) lands in the next bucket up, e.g. 70000 for a 52000 limit.

**On every SP1 bump** check that the worker image and this build agree on `ExecutionOutput`
(`store::tests` only pins the type and read path, both ends use this build). The output id is read
as the task's only output, as `CoreExecuteTaskRequest::from_raw` (`let [execution_output] =
outputs`) has it; recheck that if the layout changes. A wrong id shows as a
`could not read the execution cycles` warning per proof and empty `execution_cycles`.

## 4. Removed or upstreamed

| What | When | Why |
|---|---|---|
| Coordinator subscriber-handler leak fix (`68721e0`) | sync to `v2.9.0` | Upstream fixed it in `v2.8.5`; upstream's version kept. |
| `infra/Dockerfile.cpu_node` (`c0fb127`) | sync to `v2.9.0` (`b145f48`) | CPU workers run the official `base-v2.9.0` image. |

## 5. Sync workflow

### 5.1 Dry run

```bash
git fetch upstream --tags
git switch -c chore/sync-upstream-v<X.Y.Z> origin/mantle_develop
git merge --no-commit --no-ff v<X.Y.Z>
git diff --name-only --diff-filter=U   # conflicts; compare with the files in §3
git merge --abort
```

### 5.2 Merge

1. Resolve conflicts with §3 open. Mantle-only files rarely conflict; the hooks in upstream files
   (`prover_network.rs`, `gateway lib.rs`, `coordinator lib.rs` / `server.rs`, `common client.rs`,
   `test-cluster cluster.rs`) do.
2. Take upstream where it now does what a Mantle change did, and log the removal in §4.
3. `Cargo.lock`: take upstream's and let cargo add our extra dependencies, so the lock stays as
   close to upstream as possible.
4. New upstream config fields: add them to `bin/test-cluster/src/cluster.rs` (§3.7).
5. Check `.github/workflows/docker.yml` did not come back (§3.6).

### 5.3 Verify (same commands as `mantle-ci.yml`)

```bash
cargo +1.91.1 fmt --all -- --check
cargo clippy --manifest-path Cargo.toml --all-targets -- -D warnings -A incomplete-features
cargo test --manifest-path Cargo.toml
```

Plus the checks marked **On every SP1 / SDK bump** in §3.2, §3.8a, §3.8c, and the `sp1up`
version in `infra/Dockerfile.*`.

### 5.4 Land

PR into `mantle_develop`, then update §1 (baseline, version map) in the same PR or right after
tagging.

## 6. Maintaining this file

When you add, change, or remove a Mantle change:

1. Add or update its entry in §3: files, what changed, why, and what a sync must check.
2. If it adds a config field, list the env var and add it to the test cluster (§3.7).
3. If you remove a Mantle change because upstream now covers it, move it to §4 so the next sync
   does not bring it back from history.
4. When you cut a tag, add it to the version map (§1.1).
