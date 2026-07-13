//! Per-proof-type concurrency admission gate (mantle-xyz addition).
//!
//! The gateway is the single fan-in to the shared sp1-cluster: every
//! proof-router environment forwards here, so one in-memory counter is an
//! authoritative aggregate cap (no distributed coordination). Overflow is shed
//! with gRPC `Unavailable`, which the SP1 SDK retries in place — so op-succinct
//! neither records a failure nor bisects the range. Single-instance only: this
//! is a per-process in-memory counter, so running more than one gateway
//! replica multiplies the aggregate cap rather than sharing it.
//!
//! Unclassified requests (Core / unspecified mode with no vk match) are
//! passed through **ungated by design** — this is a protective throttle on
//! known proof shapes, not an allowlist, and it must never silently block an
//! unconfigured request type.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use sp1_sdk::network::proto::base::types::ProofMode;

use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

/// The two concurrency pools, one per backend-bound proof type.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum PoolId {
    /// Range proofs — GPU-bound (`.compressed()` in op-succinct).
    Range,
    /// Aggregation proofs — cpunode-bound (Plonk / Groth16).
    Agg,
}

impl PoolId {
    pub fn label(self) -> &'static str {
        match self {
            PoolId::Range => "range",
            PoolId::Agg => "agg",
        }
    }
}

/// Why a request was (or would be) shed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// Pool concurrency cap reached.
    PoolCap,
    /// Global cross-pool cap reached.
    GlobalCap,
    /// A free slot was held for a higher-priority (or earlier equal-rank) proposer.
    PriorityYield,
}

/// Why `try_acquire` rejected in enforce mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rejection {
    pub pool: PoolId,
    pub reason: RejectReason,
}

/// A proposer's outstanding demand for a pool slot (drives priority + FCFS).
struct Demand {
    rank: u32,
    /// when this waiting-spell began — FCFS tie-break
    first_seen: Instant,
    /// most recent shed — freshness
    last_seen: Instant,
}

/// Maps a request to a pool by `vk_hash` (primary) then proof `mode` (fallback).
/// `None` = unclassified → passed through ungated.
pub struct Classifier {
    range_vks: HashSet<Vec<u8>>,
    agg_vks: HashSet<Vec<u8>>,
}

impl Classifier {
    pub fn new(range_vks: HashSet<Vec<u8>>, agg_vks: HashSet<Vec<u8>>) -> Self {
        Self { range_vks, agg_vks }
    }

    /// vk_hash (authoritative) first, then `mode`:
    /// Compressed → Range; Plonk/Groth16 → Agg; else → None (ungated).
    ///
    /// `mode` is matched against the proto `ProofMode` enum (not raw i32
    /// literals) so the mapping tracks the proto definition if it's ever
    /// renumbered.
    ///
    /// ⚠️ Mode path assumes range == Compressed and agg != Compressed. If a
    /// deployment ever makes agg Compressed, it MUST set `agg_vks`.
    pub fn classify(&self, mode: i32, vk_hash: &[u8]) -> Option<PoolId> {
        if self.range_vks.contains(vk_hash) {
            return Some(PoolId::Range);
        }
        if self.agg_vks.contains(vk_hash) {
            return Some(PoolId::Agg);
        }
        match ProofMode::try_from(mode) {
            Ok(ProofMode::Compressed) => Some(PoolId::Range),
            Ok(ProofMode::Plonk | ProofMode::Groth16) => Some(PoolId::Agg),
            _ => None, // Core / Unspecified / unknown → ungated
        }
    }
}

/// A `usize` held per pool — total-function access, no HashMap.
#[derive(Default, Clone, Copy)]
struct PerPool {
    range: usize,
    agg: usize,
}

impl PerPool {
    fn get(&self, pool: PoolId) -> usize {
        match pool {
            PoolId::Range => self.range,
            PoolId::Agg => self.agg,
        }
    }
    fn get_mut(&mut self, pool: PoolId) -> &mut usize {
        match pool {
            PoolId::Range => &mut self.range,
            PoolId::Agg => &mut self.agg,
        }
    }
    fn total(&self) -> usize {
        self.range + self.agg
    }
}

/// Global per-proof-type concurrency gate. Single-instance, in-memory.
pub struct AdmissionController {
    caps: PerPool,
    /// `None` = pools independent; `Some(n)` = at most `n` in-flight total.
    global_cap: Option<usize>,
    counts: Mutex<PerPool>,
    /// proof_id → (pool, last_polled_at). Owns committed-slot accounting; the
    /// idempotent-release latch is `DashMap::remove` (one winner). The
    /// timestamp is refreshed by [`touch`](Self::touch) on every non-terminal
    /// poll, so it tracks recency-of-poll rather than acquire time.
    slots: DashMap<String, (PoolId, Instant)>,
    /// `false` = dry-run (count + metrics, never reject); `true` = enforce.
    enforce: bool,
    /// Reaper TTL. The single invariant: **TTL must exceed the maximum gap
    /// between a live proof's consecutive status/details polls.** A live proof
    /// is `touch`ed on every non-terminal poll, so the timestamp tracks
    /// recency-of-poll, not acquire time — a slot unpolled for longer than
    /// this is assumed abandoned (or its release was lost) and is reclaimed.
    /// Set it comfortably above the SDK's longest poll backoff; the default
    /// (3600s) clears any realistic gap.
    ttl: Duration,
    classifier: Classifier,
    metrics: Option<Arc<AdmissionMetrics>>,
    priorities: std::collections::HashMap<Vec<u8>, u32>,
    demand: DashMap<(PoolId, Vec<u8>), Demand>,
    priority_enable: bool,
    priority_ttl: Duration,
}

impl AdmissionController {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        classifier: Classifier,
        range_cap: usize,
        agg_cap: usize,
        global_cap: Option<usize>,
        enforce: bool,
        ttl: Duration,
        priorities: HashMap<Vec<u8>, u32>,
        priority_enable: bool,
        priority_ttl: Duration,
    ) -> Self {
        Self {
            caps: PerPool {
                range: range_cap,
                agg: agg_cap,
            },
            global_cap,
            counts: Mutex::new(PerPool::default()),
            slots: DashMap::new(),
            enforce,
            ttl,
            classifier,
            metrics: None,
            priorities,
            demand: DashMap::new(),
            priority_enable,
            priority_ttl,
        }
    }

    fn rank_of(&self, requester: &[u8]) -> u32 {
        self.priorities.get(requester).copied().unwrap_or(u32::MAX)
    }

    /// Is there a fresh demander for `pool` that out-ranks `requester`?
    /// (strictly higher rank, or equal rank with an earlier first_seen).
    fn out_ranked(&self, pool: PoolId, requester: &[u8], rank_r: u32) -> bool {
        let r_first = self
            .demand
            .get(&(pool, requester.to_vec()))
            .filter(|d| d.last_seen.elapsed() <= self.priority_ttl)
            .map(|d| d.first_seen)
            .unwrap_or_else(Instant::now);
        self.demand.iter().any(|e| {
            let ((p, addr), d) = (e.key(), e.value());
            *p == pool
                && addr.as_slice() != requester
                && d.last_seen.elapsed() <= self.priority_ttl
                && (d.rank < rank_r || (d.rank == rank_r && d.first_seen < r_first))
        })
    }

    fn record_demand(&self, pool: PoolId, requester: &[u8], rank: u32) {
        let now = Instant::now();
        let mut e = self
            .demand
            .entry((pool, requester.to_vec()))
            .or_insert(Demand {
                rank,
                first_seen: now,
                last_seen: now,
            });
        if e.last_seen.elapsed() > self.priority_ttl {
            e.first_seen = now; // returning after stale → restart FCFS seniority
        }
        e.rank = rank;
        e.last_seen = now;
        drop(e);
        self.metric_demand_gauge(pool);
    }

    fn clear_demand(&self, pool: PoolId, requester: &[u8]) {
        self.demand.remove(&(pool, requester.to_vec()));
        self.metric_demand_gauge(pool);
    }

    #[cfg(test)]
    pub fn demand_is_empty(&self) -> bool {
        self.demand.is_empty()
    }

    /// Classify + cap-check + reserve a slot keyed by `proof_id`.
    /// - `Ok(())` when admitted (a slot was reserved) OR ungated (no slot).
    /// - `Err(Rejection)` only in enforce mode when over a cap.
    ///
    /// The caller MUST call [`release`](Self::release) if the subsequent cluster
    /// create fails, and again (idempotently) on the terminal status poll.
    /// Releasing an unknown `proof_id` (ungated request) is a no-op.
    pub fn try_acquire(
        &self,
        proof_id: &str,
        mode: i32,
        vk_hash: &[u8],
        requester: &[u8],
    ) -> Result<(), Rejection> {
        let Some(pool) = self.classifier.classify(mode, vk_hash) else {
            self.metric_unclassified();
            return Ok(()); // ungated
        };
        let rank_r = self.rank_of(requester);

        // Under the counts lock: decide over-cap / would-yield and, if we admit,
        // increment. `demand` is only *read* here (via `out_ranked`, O(#proposers));
        // demand *writes* happen after the guard is dropped, below.
        enum Outcome {
            /// Admitted but contended (dry-run over-cap/yield): keep demand.
            AdmitContended,
            /// Clean admit: clear demand.
            AdmitClean,
        }
        let outcome = {
            let mut c = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            let pool_over = c.get(pool) >= self.caps.get(pool);
            let global_over = self.global_cap.is_some_and(|g| c.total() >= g);
            let over = pool_over || global_over;
            let would_yield =
                self.priority_enable && !over && self.out_ranked(pool, requester, rank_r);
            let reason = if over {
                Some(if global_over && !pool_over {
                    RejectReason::GlobalCap
                } else {
                    RejectReason::PoolCap
                })
            } else if would_yield {
                Some(RejectReason::PriorityYield)
            } else {
                None
            };
            match reason {
                Some(reason) if self.enforce => {
                    drop(c);
                    self.record_demand(pool, requester, rank_r);
                    self.metric_reject(pool, reason);
                    return Err(Rejection { pool, reason });
                }
                Some(reason) => {
                    // dry-run: admit but count + flag the contention.
                    self.metric_would(pool, reason);
                    *c.get_mut(pool) += 1;
                    self.set_gauges(pool, c.get(pool), c.total());
                    Outcome::AdmitContended
                }
                None => {
                    *c.get_mut(pool) += 1;
                    self.set_gauges(pool, c.get(pool), c.total());
                    Outcome::AdmitClean
                }
            }
        };
        // Reserve the slot (dry-run reserves too, so release stays symmetric).
        self.slots
            .insert(proof_id.to_string(), (pool, Instant::now()));
        match outcome {
            Outcome::AdmitClean => self.clear_demand(pool, requester),
            Outcome::AdmitContended => self.record_demand(pool, requester, rank_r),
        }
        self.metric_admitted(pool);
        Ok(())
    }

    /// Classify a `(mode, vk_hash)` pair without acquiring a slot. Exposed so
    /// the restart-seed path (best-effort, see `seed`) can classify proofs
    /// that already existed in the cluster before this process started.
    pub fn classify(&self, mode: i32, vk_hash: &[u8]) -> Option<PoolId> {
        self.classifier.classify(mode, vk_hash)
    }

    /// Best-effort restart seed: insert a slot for `proof_id` in `pool` and
    /// increment its count, bypassing the cap check entirely. Used at boot to
    /// reflect proofs that were already in flight in the cluster before this
    /// gateway process started (the in-memory counters otherwise reset to 0
    /// on restart while the cluster keeps proving). Idempotent — a `proof_id`
    /// already tracked is left untouched rather than double-counted.
    pub fn seed(&self, proof_id: &str, pool: PoolId) {
        if self.slots.contains_key(proof_id) {
            return;
        }
        {
            let mut c = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            *c.get_mut(pool) += 1;
            self.set_gauges(pool, c.get(pool), c.total());
        }
        self.slots
            .insert(proof_id.to_string(), (pool, Instant::now()));
    }

    /// Release the slot held by `proof_id`, if any. Idempotent.
    pub fn release(&self, proof_id: &str) {
        if let Some((_, (pool, _))) = self.slots.remove(proof_id) {
            self.dec_pool(pool);
        }
    }

    /// RAII handle over a just-acquired slot: on drop it releases the slot
    /// unless [`commit`](SlotGuard::commit) was called. Use it on the
    /// `request_proof` path so any early return between `try_acquire` and the
    /// cluster accepting the request can't leak the reservation — the release
    /// no longer depends on remembering to call it at every `?`.
    pub fn guard<'a>(&'a self, proof_id: &'a str) -> SlotGuard<'a> {
        SlotGuard {
            controller: self,
            proof_id,
            committed: false,
        }
    }

    /// Refresh a slot's age so the reaper won't reclaim it while its proof is
    /// still being polled (i.e. still alive). No-op if the slot isn't tracked
    /// (ungated request, or already released). Called on every non-terminal
    /// status/details poll.
    pub fn touch(&self, proof_id: &str) {
        if let Some(mut e) = self.slots.get_mut(proof_id) {
            e.value_mut().1 = Instant::now();
        }
    }

    /// Reclaim slots not polled within `ttl` (a lost release, or an abandoned
    /// proof the client stopped polling). A live proof is polled continuously
    /// and `touch`ed, so it is never reclaimed.
    pub fn reap(&self) {
        let candidates: Vec<String> = self
            .slots
            .iter()
            .filter(|e| e.value().1.elapsed() > self.ttl)
            .map(|e| e.key().clone())
            .collect();
        let mut reclaimed = 0usize;
        for id in candidates {
            // Re-check age atomically at removal: if a concurrent `touch`
            // refreshed it (the proof is still being polled), spare it.
            if let Some((_, (pool, _))) = self.slots.remove_if(&id, |_, v| v.1.elapsed() > self.ttl)
            {
                self.dec_pool(pool);
                self.metric_reaped(pool);
                reclaimed += 1;
            }
        }
        if reclaimed > 0 {
            tracing::warn!(
                reclaimed,
                "admission reaper reclaimed leaked slots (a release was likely missed or a proof was abandoned)"
            );
        }
        self.demand
            .retain(|_, d| d.last_seen.elapsed() <= self.priority_ttl);
    }

    /// In-flight for a pool (test/observability accessor).
    #[cfg(test)]
    pub fn in_flight(&self, pool: PoolId) -> usize {
        self.counts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(pool)
    }

    fn dec_pool(&self, pool: PoolId) {
        let mut c = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let e = c.get_mut(pool);
        *e = e.saturating_sub(1);
        self.set_gauges(pool, c.get(pool), c.total());
    }

    pub fn with_metrics(mut self, metrics: Arc<AdmissionMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn set_gauges(&self, pool: PoolId, pool_val: usize, total: usize) {
        if let Some(m) = &self.metrics {
            m.inflight
                .get_or_create(&PoolLabel {
                    pool: pool.label().into(),
                })
                .set(pool_val as i64);
            m.global_inflight.set(total as i64);
        }
    }
    fn metric_admitted(&self, pool: PoolId) {
        if let Some(m) = &self.metrics {
            m.admitted
                .get_or_create(&PoolLabel {
                    pool: pool.label().into(),
                })
                .inc();
        }
    }
    fn metric_would_reject(&self, pool: PoolId) {
        if let Some(m) = &self.metrics {
            m.would_reject
                .get_or_create(&PoolLabel {
                    pool: pool.label().into(),
                })
                .inc();
        }
    }
    fn metric_reaped(&self, pool: PoolId) {
        if let Some(m) = &self.metrics {
            m.reaped
                .get_or_create(&PoolLabel {
                    pool: pool.label().into(),
                })
                .inc();
        }
    }
    fn metric_rejected(&self, pool: PoolId, global: bool) {
        if let Some(m) = &self.metrics {
            if global {
                m.rejected_global.inc();
            } else {
                m.rejected
                    .get_or_create(&PoolLabel {
                        pool: pool.label().into(),
                    })
                    .inc();
            }
        }
    }
    fn metric_unclassified(&self) {
        if let Some(m) = &self.metrics {
            m.unclassified.inc();
        }
    }
    /// Reason-aware reject metric. Cap rejects map onto the existing counters;
    /// a PriorityYield bumps both the yield-shed counter and the hold-open
    /// counter (v1: hold_open counts yield events).
    fn metric_reject(&self, pool: PoolId, reason: RejectReason) {
        match reason {
            RejectReason::PoolCap => self.metric_rejected(pool, false),
            RejectReason::GlobalCap => self.metric_rejected(pool, true),
            RejectReason::PriorityYield => {
                if let Some(m) = &self.metrics {
                    m.priority_yielded
                        .get_or_create(&PoolLabel {
                            pool: pool.label().into(),
                        })
                        .inc();
                    m.priority_hold_open
                        .get_or_create(&PoolLabel {
                            pool: pool.label().into(),
                        })
                        .inc();
                }
            }
        }
    }
    /// Dry-run "would reject/yield" metric. Cap contention maps onto the
    /// would_reject counter; a would-yield bumps the priority would-yield
    /// counter.
    fn metric_would(&self, pool: PoolId, reason: RejectReason) {
        match reason {
            RejectReason::PoolCap | RejectReason::GlobalCap => self.metric_would_reject(pool),
            RejectReason::PriorityYield => {
                if let Some(m) = &self.metrics {
                    m.priority_would_yield
                        .get_or_create(&PoolLabel {
                            pool: pool.label().into(),
                        })
                        .inc();
                }
            }
        }
    }
    fn metric_demand_gauge(&self, pool: PoolId) {
        if let Some(m) = &self.metrics {
            let n = self
                .demand
                .iter()
                .filter(|e| e.key().0 == pool && e.value().last_seen.elapsed() <= self.priority_ttl)
                .count();
            m.priority_demand
                .get_or_create(&PoolLabel {
                    pool: pool.label().into(),
                })
                .set(n as i64);
        }
    }
}

/// RAII slot reservation (see [`AdmissionController::guard`]). Releases on drop
/// unless committed, so a new fallible step added to `request_proof` can't
/// silently leak the slot until the reaper TTL.
pub struct SlotGuard<'a> {
    controller: &'a AdmissionController,
    proof_id: &'a str,
    committed: bool,
}

impl SlotGuard<'_> {
    /// Hand the slot off to the terminal-poll release path — the reservation
    /// outlives this scope. Call only once the cluster has accepted the proof.
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for SlotGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.controller.release(self.proof_id);
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PoolLabel {
    pub pool: String,
}

/// Admission metrics + their registry. `render()` returns the OpenMetrics text
/// body for the `/metrics` endpoint.
///
/// NOTE: this is the gateway's only metrics surface (there was none before the
/// admission gate). It deliberately uses a self-contained `prometheus-client`
/// registry rather than the workspace's `spn_metrics` MetricServer used by the
/// cluster binaries (coordinator/bidder/worker/fulfiller): `spn_metrics` is a
/// process-global recorder facade, whereas an owned registry keeps these
/// metrics injectable and unit-testable (see `metrics_render_reflects_state`
/// and the e2e `/metrics` test). Revisit only if the gateway needs to emit
/// metrics that must share the fleet's recorder.
pub struct AdmissionMetrics {
    registry: Registry,
    inflight: Family<PoolLabel, Gauge>,
    global_inflight: Gauge,
    admitted: Family<PoolLabel, Counter>,
    rejected: Family<PoolLabel, Counter>,
    rejected_global: Counter,
    would_reject: Family<PoolLabel, Counter>,
    reaped: Family<PoolLabel, Counter>,
    unclassified: Counter,
    priority_yielded: Family<PoolLabel, Counter>,
    priority_would_yield: Family<PoolLabel, Counter>,
    priority_demand: Family<PoolLabel, Gauge>,
    priority_hold_open: Family<PoolLabel, Counter>,
}

impl AdmissionMetrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();
        let inflight = Family::<PoolLabel, Gauge>::default();
        let global_inflight = Gauge::default();
        let admitted = Family::<PoolLabel, Counter>::default();
        let rejected = Family::<PoolLabel, Counter>::default();
        let rejected_global = Counter::default();
        let would_reject = Family::<PoolLabel, Counter>::default();
        let reaped = Family::<PoolLabel, Counter>::default();
        let unclassified = Counter::default();
        let priority_yielded = Family::<PoolLabel, Counter>::default();
        let priority_would_yield = Family::<PoolLabel, Counter>::default();
        let priority_demand = Family::<PoolLabel, Gauge>::default();
        let priority_hold_open = Family::<PoolLabel, Counter>::default();
        registry.register(
            "gateway_admission_inflight",
            "In-flight per pool",
            inflight.clone(),
        );
        registry.register(
            "gateway_admission_global_inflight",
            "In-flight across all pools",
            global_inflight.clone(),
        );
        registry.register(
            "gateway_admission_admitted",
            "Admitted, per pool",
            admitted.clone(),
        );
        registry.register(
            "gateway_admission_rejected",
            "Rejected (pool cap), per pool",
            rejected.clone(),
        );
        registry.register(
            "gateway_admission_rejected_global",
            "Rejected (global cap)",
            rejected_global.clone(),
        );
        registry.register(
            "gateway_admission_would_reject",
            "Dry-run over-cap admits, per pool",
            would_reject.clone(),
        );
        registry.register(
            "gateway_admission_reaped",
            "Slots reclaimed by the reaper, per pool",
            reaped.clone(),
        );
        registry.register(
            "gateway_admission_unclassified",
            "Requests passed through ungated",
            unclassified.clone(),
        );
        registry.register(
            "gateway_admission_priority_yielded",
            "Requests shed by yielding a free slot to a higher-priority proposer, per pool",
            priority_yielded.clone(),
        );
        registry.register(
            "gateway_admission_priority_would_yield",
            "Dry-run: requests that WOULD have yielded, per pool",
            priority_would_yield.clone(),
        );
        registry.register(
            "gateway_admission_priority_demand",
            "Current fresh priority demanders, per pool",
            priority_demand.clone(),
        );
        registry.register(
            "gateway_admission_priority_hold_open",
            "Yield events (a free slot held for a higher-priority proposer), per pool",
            priority_hold_open.clone(),
        );
        Self {
            registry,
            inflight,
            global_inflight,
            admitted,
            rejected,
            rejected_global,
            would_reject,
            reaped,
            unclassified,
            priority_yielded,
            priority_would_yield,
            priority_demand,
            priority_hold_open,
        }
    }

    /// OpenMetrics text body. Never panics: on the (practically-unreachable)
    /// encode error it logs and returns an empty body rather than killing the
    /// scrape task.
    pub fn render(&self) -> String {
        let mut buf = String::new();
        if let Err(e) = encode(&mut buf, &self.registry) {
            tracing::error!(error = %e, "failed to encode admission metrics");
            buf.clear();
        }
        buf
    }

    /// axum response with the correct OpenMetrics content-type.
    pub fn render_response(&self) -> axum::response::Response {
        use axum::response::IntoResponse;
        (
            [(
                axum::http::header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            self.render(),
        )
            .into_response()
    }
}

impl Default for AdmissionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const RANGE_VK: &[u8] = &[0x11; 4];
    const AGG_VK: &[u8] = &[0x22; 4];

    fn classifier() -> Classifier {
        Classifier::new(
            HashSet::from([RANGE_VK.to_vec()]),
            HashSet::from([AGG_VK.to_vec()]),
        )
    }

    #[test]
    fn classify_vk_primary_then_mode() {
        let c = classifier();
        assert_eq!(c.classify(3, RANGE_VK), Some(PoolId::Range)); // vk wins over mode
        assert_eq!(c.classify(2, AGG_VK), Some(PoolId::Agg));
        assert_eq!(c.classify(2, &[0x99; 4]), Some(PoolId::Range)); // Compressed
        assert_eq!(c.classify(3, &[0x99; 4]), Some(PoolId::Agg)); // Plonk
        assert_eq!(c.classify(4, &[0x99; 4]), Some(PoolId::Agg)); // Groth16
        assert_eq!(c.classify(1, &[0x99; 4]), None); // Core → ungated
    }

    fn enforce_ctrl() -> AdmissionController {
        AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        )
    }

    #[test]
    fn rank_of_uses_config_then_default() {
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::from([(vec![0xAAu8], 0u32)]),
            false,
            Duration::from_secs(90),
        );
        assert_eq!(c.rank_of(&[0xAA]), 0);
        assert_eq!(c.rank_of(&[0xBB]), u32::MAX);
    }

    #[test]
    fn acquire_up_to_cap_then_reject() {
        let c = enforce_ctrl();
        assert!(c.try_acquire("p1", 2, RANGE_VK, &[0x01]).is_ok()); // Range cap 1
        assert_eq!(c.in_flight(PoolId::Range), 1);
        assert_eq!(
            c.try_acquire("p2", 2, RANGE_VK, &[0x01]).unwrap_err(),
            Rejection {
                pool: PoolId::Range,
                reason: RejectReason::PoolCap
            }
        );
    }

    #[test]
    fn release_frees_slot_idempotently() {
        let c = enforce_ctrl();
        c.try_acquire("p1", 2, RANGE_VK, &[0x01]).unwrap();
        c.release("p1");
        c.release("p1"); // idempotent
        assert_eq!(c.in_flight(PoolId::Range), 0);
        assert!(c.try_acquire("p3", 2, RANGE_VK, &[0x01]).is_ok());
    }

    #[test]
    fn dry_run_admits_over_cap() {
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            false,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("p1", 2, RANGE_VK, &[0x01]).unwrap();
        assert!(c.try_acquire("p2", 2, RANGE_VK, &[0x01]).is_ok()); // over cap, dry-run admits
        assert_eq!(c.in_flight(PoolId::Range), 2);
    }

    #[test]
    fn ungated_takes_no_slot() {
        let c = enforce_ctrl();
        c.try_acquire("p1", 1, &[0x99; 4], &[0x01]).unwrap(); // Core → ungated
        assert_eq!(c.in_flight(PoolId::Range), 0);
        c.release("p1"); // no-op, no panic
    }

    #[test]
    fn global_cap_serialises_pools() {
        let c = AdmissionController::new(
            classifier(),
            2,
            2,
            Some(1),
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("p1", 2, RANGE_VK, &[0x01]).unwrap(); // takes the 1 global slot
        assert_eq!(
            c.try_acquire("p2", 3, AGG_VK, &[0x01]).unwrap_err(),
            Rejection {
                pool: PoolId::Agg,
                reason: RejectReason::GlobalCap
            } // agg pool had room; global full
        );
    }

    #[test]
    fn reaper_reclaims_expired() {
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_millis(30),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("p1", 2, RANGE_VK, &[0x01]).unwrap();
        c.reap();
        assert_eq!(c.in_flight(PoolId::Range), 1); // not yet expired
        std::thread::sleep(Duration::from_millis(45));
        c.reap();
        assert_eq!(c.in_flight(PoolId::Range), 0);
    }

    #[test]
    fn touch_prevents_reap_of_polled_slot() {
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_millis(30),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("p1", 2, RANGE_VK, &[0x01]).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        c.touch("p1"); // still being polled → refresh
        std::thread::sleep(Duration::from_millis(20)); // 40ms since acquire, but 20ms since touch
        c.reap();
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "recently-touched slot must not be reaped"
        );
        std::thread::sleep(Duration::from_millis(40)); // now >30ms since last touch
        c.reap();
        assert_eq!(
            c.in_flight(PoolId::Range),
            0,
            "unpolled slot ages out and is reaped"
        );
    }

    #[test]
    fn touch_absent_slot_is_noop() {
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.touch("nope"); // no panic, no effect
        assert_eq!(c.in_flight(PoolId::Range), 0);
    }

    #[test]
    fn seed_bypasses_cap_and_is_idempotent() {
        let c = enforce_ctrl(); // Range cap = 1
        c.seed("restart-1", PoolId::Range);
        c.seed("restart-2", PoolId::Range); // over cap, but seed doesn't check it
        assert_eq!(c.in_flight(PoolId::Range), 2);
        c.seed("restart-1", PoolId::Range); // already tracked → no double-count
        assert_eq!(c.in_flight(PoolId::Range), 2);
        // A normal acquire still enforces the cap against the seeded count.
        assert!(c.try_acquire("p1", 2, RANGE_VK, &[0x01]).is_err());
        // Releasing a seeded slot behaves like any other slot.
        c.release("restart-1");
        assert_eq!(c.in_flight(PoolId::Range), 1);
    }

    #[test]
    fn metrics_render_reflects_state() {
        let m = Arc::new(AdmissionMetrics::new());
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        )
        .with_metrics(m.clone());
        c.try_acquire("p1", 2, RANGE_VK, &[0x01]).unwrap();
        let _ = c.try_acquire("p2", 2, RANGE_VK, &[0x01]); // rejected
        let out = m.render();
        assert!(out.contains("gateway_admission_admitted"));
        assert!(out.contains("gateway_admission_rejected"));
        assert!(out.contains("pool=\"range\""));
    }

    const AA: &[u8] = &[0xAA]; // rank 0 (high)
    const BB: &[u8] = &[0xBB]; // rank 1 (low)

    fn prio_ctrl() -> AdmissionController {
        AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_secs(3600),
            HashMap::from([(AA.to_vec(), 0u32), (BB.to_vec(), 1u32)]),
            true,
            Duration::from_secs(90),
        )
    }

    #[test]
    fn higher_rank_fresh_demand_makes_lower_yield_even_with_free_slot() {
        let c = prio_ctrl();
        c.try_acquire("occupier", 2, RANGE_VK, &[0x01]).unwrap(); // fills Range cap=1
        let e = c.try_acquire("a1", 2, RANGE_VK, AA).unwrap_err();
        assert_eq!(e.reason, RejectReason::PoolCap); // A shed on cap → demand[A]
        c.release("occupier");
        let e2 = c.try_acquire("b1", 2, RANGE_VK, BB).unwrap_err();
        assert_eq!(e2.reason, RejectReason::PriorityYield); // slot free but A out-ranks B
        assert!(c.try_acquire("a2", 2, RANGE_VK, AA).is_ok()); // A wins
        assert_eq!(c.in_flight(PoolId::Range), 1);
    }

    #[test]
    fn stale_higher_demand_does_not_block_lower() {
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_secs(3600),
            HashMap::from([(AA.to_vec(), 0u32), (BB.to_vec(), 1u32)]),
            true,
            Duration::from_millis(20),
        );
        c.try_acquire("occ", 2, RANGE_VK, &[0x01]).unwrap();
        c.try_acquire("a1", 2, RANGE_VK, AA).unwrap_err();
        c.release("occ");
        std::thread::sleep(Duration::from_millis(35));
        assert!(c.try_acquire("b1", 2, RANGE_VK, BB).is_ok());
    }

    #[test]
    fn equal_rank_first_seen_wins() {
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_secs(3600),
            HashMap::from([(AA.to_vec(), 5), (BB.to_vec(), 5)]),
            true,
            Duration::from_secs(90),
        );
        c.try_acquire("occ", 2, RANGE_VK, &[0x01]).unwrap();
        c.try_acquire("a1", 2, RANGE_VK, AA).unwrap_err(); // A waits first
        std::thread::sleep(Duration::from_millis(5));
        c.try_acquire("b1", 2, RANGE_VK, BB).unwrap_err(); // B waits second
        c.release("occ");
        let eb = c.try_acquire("b2", 2, RANGE_VK, BB).unwrap_err();
        assert_eq!(eb.reason, RejectReason::PriorityYield); // B yields to earlier A
        assert!(c.try_acquire("a2", 2, RANGE_VK, AA).is_ok());
    }

    #[test]
    fn priority_disabled_behaves_as_today() {
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_secs(3600),
            HashMap::from([(AA.to_vec(), 0), (BB.to_vec(), 1)]),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("occ", 2, RANGE_VK, &[0x01]).unwrap();
        c.try_acquire("a1", 2, RANGE_VK, AA).unwrap_err();
        c.release("occ");
        assert!(c.try_acquire("b1", 2, RANGE_VK, BB).is_ok()); // no priority → first wins
    }

    #[test]
    fn priority_metrics_render() {
        let m = Arc::new(AdmissionMetrics::new());
        let c = prio_ctrl().with_metrics(m.clone());
        c.try_acquire("occ", 2, RANGE_VK, &[0x01]).unwrap();
        c.try_acquire("a1", 2, RANGE_VK, AA).unwrap_err(); // A shed (cap) → demand[A]
        c.release("occ");
        c.try_acquire("b1", 2, RANGE_VK, BB).unwrap_err(); // yield
        let out = m.render();
        // Assert the SAMPLE lines (label-set present ⇒ get_or_create+inc/set
        // actually ran), not just the registered `# TYPE` name.
        assert!(
            out.contains("gateway_admission_priority_yielded_total{pool=\"range\"} 1"),
            "yield must have incremented priority_yielded for range:\n{out}"
        );
        // Both A (shed on cap) and B (yielded) are fresh Range demanders → 2.
        assert!(
            out.contains("gateway_admission_priority_demand{pool=\"range\"} 2"),
            "demand gauge must reflect the two fresh Range demanders:\n{out}"
        );
    }

    #[test]
    fn dry_run_emits_would_yield_not_yielded() {
        let m = Arc::new(AdmissionMetrics::new());
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            false,
            Duration::from_secs(3600), // enforce OFF
            std::collections::HashMap::from([(AA.to_vec(), 0u32), (BB.to_vec(), 1u32)]),
            true,
            Duration::from_secs(90),
        )
        .with_metrics(m.clone());
        // Range cap = 1. Fill it (occ), push A over → dry-run admits but records
        // demand[A]. Then release BOTH reservations so the pool is back under
        // cap when B arrives — `release` frees the slot but does NOT clear
        // demand, so demand[A] persists. B then sees a free slot with a
        // higher-ranked fresh demander → would_yield (dry-run admits anyway).
        c.try_acquire("occ", 2, RANGE_VK, &[0x01]).unwrap(); // count 1 (cap 1)
        c.try_acquire("a1", 2, RANGE_VK, AA).unwrap(); // over cap → dry-run admit; demand[A]
        c.release("occ");
        c.release("a1"); // pool back to 0; demand[A] persists (release ≠ clear_demand)
        c.try_acquire("b1", 2, RANGE_VK, BB).unwrap(); // slot free + A out-ranks B → would_yield
        let out = m.render();
        // Dry-run must emit would_yield (the SAMPLE line, label present)...
        assert!(
            out.contains("gateway_admission_priority_would_yield_total{pool=\"range\"} 1"),
            "dry-run yield must increment priority_would_yield for range:\n{out}"
        );
        // ...and must NOT emit an actual yielded sample (enforce is OFF).
        assert!(
            !out.contains("gateway_admission_priority_yielded_total{pool=\"range\"}"),
            "dry-run must not shed: priority_yielded must have no range sample:\n{out}"
        );
    }

    #[test]
    fn reap_drops_stale_demand() {
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::from([(AA.to_vec(), 0u32)]),
            true,
            Duration::from_millis(20),
        );
        c.try_acquire("occ", 2, RANGE_VK, &[0x01]).unwrap();
        c.try_acquire("a1", 2, RANGE_VK, AA).unwrap_err(); // records demand[AA]
        assert!(!c.demand_is_empty());
        std::thread::sleep(Duration::from_millis(35));
        c.reap();
        assert!(c.demand_is_empty(), "stale demand must be reaped");
    }

    #[test]
    fn admit_clears_demand() {
        let c = prio_ctrl(); // Range cap = 1
                             // AA gets shed first (cap full) → records demand[AA].
        c.try_acquire("occ", 2, RANGE_VK, &[0x01]).unwrap();
        c.try_acquire("a1", 2, RANGE_VK, AA).unwrap_err();
        assert!(!c.demand_is_empty(), "shed must have recorded demand[AA]");
        // Free the slot and let AA admit cleanly → its demand must be cleared.
        c.release("occ");
        c.try_acquire("a2", 2, RANGE_VK, AA).unwrap();
        assert!(
            c.demand_is_empty(),
            "clean admit must clear the proposer's demand"
        );
    }
}
