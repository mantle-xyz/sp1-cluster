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

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
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

/// Why `try_acquire` rejected in enforce mode. `global` is true when the GLOBAL
/// cap (not the pool cap) was the binding constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rejection {
    pub pool: PoolId,
    pub global: bool,
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
    /// Compressed(2) → Range; Plonk(3)/Groth16(4) → Agg; else → None (ungated).
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
        match mode {
            2 => Some(PoolId::Range),   // ProofMode::Compressed
            3 | 4 => Some(PoolId::Agg), // Plonk | Groth16
            _ => None,                  // Core / unspecified
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
    /// Reaper TTL: max time since a slot's *last poll* (not since acquire) —
    /// a live proof is `touch`ed on every non-terminal status/details poll,
    /// so TTL only needs to exceed the client's poll interval, not the
    /// longest legitimate proof duration. A slot that goes unpolled for
    /// longer than this is assumed abandoned (or its release was lost) and
    /// is reclaimed.
    ttl: Duration,
    classifier: Classifier,
    metrics: Option<Arc<AdmissionMetrics>>,
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
        }
    }

    /// Classify + cap-check + reserve a slot keyed by `proof_id`.
    /// - `Ok(())` when admitted (a slot was reserved) OR ungated (no slot).
    /// - `Err(Rejection)` only in enforce mode when over a cap.
    ///
    /// The caller MUST call [`release`](Self::release) if the subsequent cluster
    /// create fails, and again (idempotently) on the terminal status poll.
    /// Releasing an unknown `proof_id` (ungated request) is a no-op.
    pub fn try_acquire(&self, proof_id: &str, mode: i32, vk_hash: &[u8]) -> Result<(), Rejection> {
        let Some(pool) = self.classifier.classify(mode, vk_hash) else {
            self.metric_unclassified();
            return Ok(()); // ungated
        };
        let over = {
            let mut c = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            let pool_over = c.get(pool) >= self.caps.get(pool);
            let global_over = self.global_cap.is_some_and(|g| c.total() >= g);
            let over = pool_over || global_over;
            if over && self.enforce {
                let global = global_over && !pool_over;
                drop(c);
                self.metric_rejected(pool, global);
                return Err(Rejection { pool, global });
            }
            *c.get_mut(pool) += 1;
            self.set_gauges(pool, c.get(pool), c.total());
            over
        };
        // Reserve the slot (dry-run reserves too, so release stays symmetric).
        self.slots
            .insert(proof_id.to_string(), (pool, Instant::now()));
        if over {
            self.metric_would_reject(pool);
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
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PoolLabel {
    pub pool: String,
}

/// Admission metrics + their registry. `render()` returns the OpenMetrics text
/// body for the `/metrics` endpoint.
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
        AdmissionController::new(classifier(), 1, 2, None, true, Duration::from_secs(3600))
    }

    #[test]
    fn acquire_up_to_cap_then_reject() {
        let c = enforce_ctrl();
        assert!(c.try_acquire("p1", 2, RANGE_VK).is_ok()); // Range cap 1
        assert_eq!(c.in_flight(PoolId::Range), 1);
        assert_eq!(
            c.try_acquire("p2", 2, RANGE_VK).unwrap_err(),
            Rejection {
                pool: PoolId::Range,
                global: false
            }
        );
    }

    #[test]
    fn release_frees_slot_idempotently() {
        let c = enforce_ctrl();
        c.try_acquire("p1", 2, RANGE_VK).unwrap();
        c.release("p1");
        c.release("p1"); // idempotent
        assert_eq!(c.in_flight(PoolId::Range), 0);
        assert!(c.try_acquire("p3", 2, RANGE_VK).is_ok());
    }

    #[test]
    fn dry_run_admits_over_cap() {
        let c =
            AdmissionController::new(classifier(), 1, 2, None, false, Duration::from_secs(3600));
        c.try_acquire("p1", 2, RANGE_VK).unwrap();
        assert!(c.try_acquire("p2", 2, RANGE_VK).is_ok()); // over cap, dry-run admits
        assert_eq!(c.in_flight(PoolId::Range), 2);
    }

    #[test]
    fn ungated_takes_no_slot() {
        let c = enforce_ctrl();
        c.try_acquire("p1", 1, &[0x99; 4]).unwrap(); // Core → ungated
        assert_eq!(c.in_flight(PoolId::Range), 0);
        c.release("p1"); // no-op, no panic
    }

    #[test]
    fn global_cap_serialises_pools() {
        let c =
            AdmissionController::new(classifier(), 2, 2, Some(1), true, Duration::from_secs(3600));
        c.try_acquire("p1", 2, RANGE_VK).unwrap(); // takes the 1 global slot
        assert_eq!(
            c.try_acquire("p2", 3, AGG_VK).unwrap_err(),
            Rejection {
                pool: PoolId::Agg,
                global: true
            } // agg pool had room; global full
        );
    }

    #[test]
    fn reaper_reclaims_expired() {
        let c = AdmissionController::new(classifier(), 1, 2, None, true, Duration::from_millis(30));
        c.try_acquire("p1", 2, RANGE_VK).unwrap();
        c.reap();
        assert_eq!(c.in_flight(PoolId::Range), 1); // not yet expired
        std::thread::sleep(Duration::from_millis(45));
        c.reap();
        assert_eq!(c.in_flight(PoolId::Range), 0);
    }

    #[test]
    fn touch_prevents_reap_of_polled_slot() {
        let c = AdmissionController::new(classifier(), 1, 2, None, true, Duration::from_millis(30));
        c.try_acquire("p1", 2, RANGE_VK).unwrap();
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
        let c = AdmissionController::new(classifier(), 1, 2, None, true, Duration::from_secs(3600));
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
        assert!(c.try_acquire("p1", 2, RANGE_VK).is_err());
        // Releasing a seeded slot behaves like any other slot.
        c.release("restart-1");
        assert_eq!(c.in_flight(PoolId::Range), 1);
    }

    #[test]
    fn metrics_render_reflects_state() {
        let m = Arc::new(AdmissionMetrics::new());
        let c = AdmissionController::new(classifier(), 1, 2, None, true, Duration::from_secs(3600))
            .with_metrics(m.clone());
        c.try_acquire("p1", 2, RANGE_VK).unwrap();
        let _ = c.try_acquire("p2", 2, RANGE_VK); // rejected
        let out = m.render();
        assert!(out.contains("gateway_admission_admitted"));
        assert!(out.contains("gateway_admission_rejected"));
        assert!(out.contains("pool=\"range\""));
    }
}
