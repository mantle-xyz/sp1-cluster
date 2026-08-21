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

/// What a single reconcile tick observed about the cluster's Pending set. This
/// is the SOLE input to [`AdmissionController::reconcile`], so the per-tick
/// "how should I treat this observation" decision lives in one place instead of
/// being spread across the reaper loop's match arms.
pub enum PendingObservation {
    /// A COMPLETE Pending snapshot (reply under the server page limit) — the
    /// authoritative live set. Present committed slots are refreshed to live;
    /// absent ones advance their absence streak and are released once it reaches
    /// the threshold.
    Complete(HashSet<String>),
    /// A TRUNCATED Pending snapshot (hit the server page limit) — an INCOMPLETE
    /// view. A present sighting is still trustworthy, so present committed slots
    /// are refreshed; but a slot missing from a truncated page may just be on an
    /// unseen page, so absence is NOT concluded and its streak is left untouched.
    Partial(HashSet<String>),
    /// NO usable snapshot this tick (query error / timeout). A gap in
    /// observation: every streak is left unchanged (neither advanced nor reset).
    /// Because the streak is an observation count rather than a wall clock, a gap
    /// simply doesn't count as an observation — so absence still accrues only
    /// across genuine consecutive absent replies, and a flaky query can neither
    /// zero a real streak nor forge absence.
    None,
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

/// A tracked slot reservation.
///
/// Lifecycle: RESERVED (`committed_at == None`, set by `try_acquire`) → COMMITTED
/// (`committed_at == Some`, set by `mark_committed` just before the cluster
/// create). A RESERVED slot is owned by the in-flight `request_proof` handler's
/// [`SlotGuard`] (released on its Drop — including on cancellation/panic-unwind)
/// and is touched by NEITHER the reaper NOR the reconciler; only COMMITTED slots
/// are subject to cluster-truth reconciliation and the backstop TTL reaper. This
/// is what prevents a slow upload / mid-create cancellation from being reaped or
/// reconciled out from under a live-but-not-yet-registered proof.
struct Slot {
    pool: PoolId,
    /// Who holds this slot, for per-requester occupancy reporting. `None` only
    /// when genuinely unknown — a seeded proof whose cluster record carries no
    /// requester — which reports as `unknown`.
    requester: Option<Vec<u8>>,
    /// When this slot was acquired. Distinct from every other instant here on
    /// purpose: `deadline` is refreshed by liveness signals and `committed_at`
    /// marks the hand-off to the cluster, so neither can answer "how long has
    /// this been held". Never refreshed.
    ///
    /// For a SEEDED slot this is back-dated from the cluster's `created_at` so
    /// occupancy survives a gateway restart. That matters more than it sounds:
    /// restarting the gateway is a common first move during an incident, and
    /// without back-dating every stuck proof's age would reset to zero exactly
    /// when someone is trying to find it.
    admitted_at: Instant,
    /// Absolute backstop reap deadline (`now + ttl`), refreshed on commit/touch.
    /// Only consulted for COMMITTED slots and only as a backstop for when the
    /// reconciler can't reach the cluster; a live proof is `touch`ed so it never
    /// hits this. Stored as the deadline (not a last-polled instant) to avoid
    /// `Instant`-underflow on a freshly-booted host.
    deadline: Instant,
    /// `Some(t)` once handed to the cluster at `t` (create leg or seed), else
    /// `None` while still RESERVED in the handler. The reconciler and the reaper
    /// act ONLY on COMMITTED slots — a reserved slot's proof isn't in the
    /// cluster's Pending set yet, so treating its absence as "gone" would wrongly
    /// release a proof that is still uploading / about to be created → over-admit.
    committed_at: Option<Instant>,
    /// For a COMMITTED slot: number of CONSECUTIVE successful reconciles that
    /// observed this proof ABSENT from the cluster's Pending set. Any liveness
    /// signal — a present reconcile observation, a client poll, or commit —
    /// resets it to 0; each absent reconcile observation increments it. The
    /// reconciler releases a committed slot once this reaches the configured
    /// threshold, so a reappearance (a transient/anomalous empty reply, or a
    /// create still landing in the Pending set) resets it and a single bad
    /// snapshot can't mass-release live slots. It is an OBSERVATION COUNT, not a
    /// wall clock: a SKIPPED reconcile (truncated / timeout / query error /
    /// unimplemented) leaves it unchanged rather than resetting it, so absence
    /// accrues across intermittent query failures instead of being repeatedly
    /// zeroed — a genuinely-gone proof is still reclaimed by cluster truth
    /// (not left to the TTL backstop) even when the query is flaky.
    absent_streak: u32,
}

impl Slot {
    /// Apply a liveness signal: clear the absence streak and refresh the backstop
    /// reap deadline. The single place these two "still alive" effects happen, so
    /// commit / client-poll / reconcile-saw-it-pending can't drift. `now` is
    /// passed in so a caller already holding one reuses it.
    fn observe_live(&mut self, now: Instant, ttl: Duration) {
        self.absent_streak = 0;
        self.deadline = now + ttl;
    }

    /// Record one absent reconcile observation and return the new streak.
    fn observe_absent(&mut self) -> u32 {
        self.absent_streak += 1;
        self.absent_streak
    }

    fn is_committed(&self) -> bool {
        self.committed_at.is_some()
    }
}

/// Per-tick summary of a [`reconcile`](AdmissionController::reconcile) pass,
/// returned so the reaper loop can log — at INFO, once per reap period — exactly
/// what the reconcile observed and did. This closes a real observability blind
/// spot: previously a reconcile that saw every slot still Pending logged NOTHING
/// (present-kept is silent) and a skipped query logged only at debug, so "slots
/// held but nothing being released" was indistinguishable from "reconcile task
/// dead" without recompiling. `kind` is `"complete"` | `"partial"` | `"skip"`.
#[derive(Debug, Default, Clone)]
pub struct ReconcileReport {
    /// Which observation this tick processed.
    pub kind: &'static str,
    /// Size of the cluster Pending set seen this tick (0 for a skip).
    pub live: usize,
    /// COMMITTED slots examined (reserved slots are not the reconciler's business).
    pub committed_tracked: usize,
    /// Committed slots confirmed present in the cluster Pending set.
    pub present: usize,
    /// Committed slots absent from it (spared by partial/commit-grace, or counted).
    pub absent: usize,
    /// Slots released this tick (sustained-absence threshold reached).
    pub released: usize,
    /// Committed slot ids confirmed present in the Pending set (bounded, for logs).
    pub present_ids: Vec<String>,
    /// Committed slot ids absent from the Pending set (bounded, for logs) — the
    /// ones the reconciler is (or would be) releasing. Seeing the seeded/wedged
    /// ids here vs in the raw Pending set is the id-match diagnostic.
    pub absent_ids: Vec<String>,
}

/// Global per-proof-type concurrency gate. Single-instance, in-memory.
pub struct AdmissionController {
    caps: PerPool,
    /// `None` = pools independent; `Some(n)` = at most `n` in-flight total.
    global_cap: Option<usize>,
    counts: Mutex<PerPool>,
    /// proof_id → [`Slot`]. Owns per-proof reservation accounting; the
    /// idempotent-release latch is the atomic `DashMap::remove_if` inside
    /// [`remove_and_dec`](Self::remove_and_dec) (one winner). See [`Slot`] for the
    /// deadline (reap) and committed_at (reconcile) semantics. All remove+decrement
    /// go through `remove_and_dec` so the counter and the map can't drift.
    slots: DashMap<String, Slot>,
    /// `false` = dry-run (count + metrics, never reject); `true` = enforce.
    enforce: bool,
    /// Reaper TTL: the BACKSTOP deadline for a COMMITTED slot. Both a client poll
    /// (`touch`) and a reconcile that sees the proof still Pending refresh it (via
    /// [`Slot::observe_live`]), so in normal operation the reconciler frees
    /// finished/gone slots and this only fires when reconcile CANNOT confirm
    /// liveness (cluster query persistently failing/truncated) AND no client is
    /// polling. It must therefore exceed the maximum gap between a slot's liveness
    /// signals — a client poll OR a reconcile-present observation — NOT the proof
    /// duration. It also bounds a hung pre-commit upload: a RESERVED slot past this
    /// deadline is reclaimed as well. The default (3600s) clears any realistic gap.
    /// (Related invariants — `reap_period < ttl`, `absent_observations ×
    /// reap_period < ttl`, `commit_grace < ttl` — are enforced in
    /// `build_admission`.)
    ttl: Duration,
    /// Grace after a slot is COMMITTED before the reconciler may count it absent.
    /// `commit` happens just before the cluster create, so during the create leg
    /// the proof isn't in the Pending set yet and looks "absent" — this spares it
    /// (like a RESERVED slot) until it has been committed for `commit_grace`,
    /// preventing an in-flight create from being reconciled into an over-admit.
    /// Independent of the reap cadence, so it protects even an aggressively-fast
    /// reconcile config. `Duration::ZERO` = no grace (set via
    /// [`with_commit_grace`](Self::with_commit_grace); `new` defaults to ZERO).
    commit_grace: Duration,
    classifier: Classifier,
    metrics: Option<Arc<AdmissionMetrics>>,
    priorities: std::collections::HashMap<Vec<u8>, u32>,
    /// Outstanding demand for a pool slot, keyed per `(pool, requester)` — i.e.
    /// ONE priority identity per proposer. A proposer pipelining multiple
    /// requests shares a single demand entry, and a clean admit of any one of
    /// them clears it (it is re-recorded on the next shed). This is intentional:
    /// a proposer is a single priority actor, not one actor per in-flight
    /// request.
    ///
    /// Lock discipline: demand writes happen under the `counts` lock (order
    /// counts→demand), making the counter decision and the demand update atomic.
    /// No path may take `counts` while holding a `demand` guard.
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
            commit_grace: Duration::ZERO,
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
                // u32::MAX = unlisted/default rank: no FCFS among unconfigured proposers
                && (d.rank < rank_r
                    || (d.rank == rank_r && rank_r != u32::MAX && d.first_seen < r_first))
        })
    }

    fn record_demand(&self, pool: PoolId, requester: &[u8], rank: u32) {
        if !self.priority_enable {
            return; // priority OFF → no demand tracking, no demand gauge
        }
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
        if !self.priority_enable {
            return; // priority OFF → no demand tracking, no demand gauge
        }
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

        // Under the counts lock: decide over-cap / would-yield, increment if we
        // admit, AND write demand — all atomically. `demand` is read here (via
        // `out_ranked`, O(#proposers)) and written here (record/clear_demand),
        // both under the `counts` lock so the counter decision and the demand
        // update can't be reordered against a concurrent try_acquire from the
        // same proposer (which would otherwise cause phantom demand or lost FCFS
        // seniority). Lock order is always counts→demand; no path takes `counts`
        // while holding a `demand` guard, so this stays deadlock-free.
        {
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
                    // Shed: record demand while still under `counts` so the
                    // counter decision and the demand write are atomic.
                    self.record_demand(pool, requester, rank_r);
                    drop(c);
                    self.metric_reject(pool, reason);
                    return Err(Rejection { pool, reason });
                }
                Some(reason) => {
                    // dry-run: admit but count + flag the contention, keeping
                    // (recording) demand atomically under `counts`.
                    self.metric_would(pool, reason);
                    *c.get_mut(pool) += 1;
                    self.set_gauges(pool, c.get(pool), c.total());
                    self.record_demand(pool, requester, rank_r);
                }
                None => {
                    // Clean admit: clear demand atomically under `counts`.
                    *c.get_mut(pool) += 1;
                    self.set_gauges(pool, c.get(pool), c.total());
                    self.clear_demand(pool, requester);
                }
            }
            // Reserve the slot UNDER the same `counts` hold as the increment, so
            // a panic can't leave a counted-but-slotless phantom (the count and
            // the slot map stay consistent; a poison-recovered lock would
            // otherwise inflate the pool forever). `committed_at: None` marks it
            // RESERVED so the reaper/reconciler leave it alone until `commit()`.
            //
            // TRADEOFF (do not flip without reading this): holding `counts`
            // across the DashMap insert means an admit can briefly block behind a
            // shard write-guard held by reconcile's `iter_mut` sweep, stalling
            // other admits (all serialize on `counts`). At the intended pool caps
            // (single digits) the slot map is tiny; even after a restart `seed`
            // transiently pushes the map above the caps it stays bounded by the
            // cluster Pending page limit (≤ ~1000, see PENDING_QUERY_LIMIT) and
            // the sweep is microseconds, so the stall is negligible. The
            // alternative (insert outside the lock) reintroduces the panic-window
            // phantom-count, which is unreclaimable. Keep it under the lock unless
            // the caps grow large enough that the sweep stall becomes measurable —
            // then switch reconcile to short per-id `get_mut`s instead of a long
            // `iter_mut`.
            let now = Instant::now();
            self.slots.insert(
                proof_id.to_string(),
                Slot {
                    pool,
                    requester: Some(requester.to_vec()),
                    admitted_at: now,
                    deadline: now + self.ttl,
                    committed_at: None,
                    absent_streak: 0,
                },
            );
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
    ///
    /// A seeded slot is COMMITTED (it reflects a proof the cluster is already
    /// running, i.e. present in its Pending set), so the reconciler is its
    /// authority: it is kept while the cluster still reports the proof Pending
    /// and released once it goes terminal/absent — no special grace needed.
    /// `requester` / `age` come from the cluster's own record of the proof (its
    /// `requester` and `created_at` fields), so a restart preserves both the
    /// attribution and the true age. Pass `None` for either when the record does
    /// not carry it.
    pub fn seed(
        &self,
        proof_id: &str,
        pool: PoolId,
        requester: Option<Vec<u8>>,
        age: Option<Duration>,
    ) {
        if self.slots.contains_key(proof_id) {
            return;
        }
        {
            let mut c = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            *c.get_mut(pool) += 1;
            self.set_gauges(pool, c.get(pool), c.total());
            // Insert the slot UNDER the same `counts` hold as the increment, so a
            // panic between the two can't leave a counted-but-slotless phantom that
            // no path could reclaim — the same invariant `try_acquire` documents at
            // its reserve site. A seeded slot is COMMITTED (it reflects a proof the
            // cluster is already running).
            let now = Instant::now();
            self.slots.insert(
                proof_id.to_string(),
                Slot {
                    pool,
                    requester,
                    // Back-date to the proof's real admission, so a restart does
                    // not reset a wedged proof's age to zero.
                    //
                    // On unix `Instant::checked_sub` does NOT clamp at the
                    // monotonic origin — subtracting decades still returns Some
                    // (measured) — so the back-date is exact and `unwrap_or(now)`
                    // is only reachable on platforms where `Instant` is a bare
                    // `Duration`. That also means nothing here bounds a bogus
                    // `age`; the caller is responsible for rejecting implausible
                    // ones, and does.
                    admitted_at: age.and_then(|a| now.checked_sub(a)).unwrap_or(now),
                    deadline: now + self.ttl,
                    committed_at: Some(now),
                    absent_streak: 0,
                },
            );
        }
        self.metric_seeded(pool);
    }

    /// Remove `proof_id` from `slots` (only if `still_removable`) and decrement
    /// its pool counter — the SINGLE decrement path, so release / reap /
    /// reconcile can't drift on locking. The remove + decrement happen under one
    /// `counts` hold so a concurrent [`try_acquire`](Self::try_acquire) for a
    /// DIFFERENT proof can't observe the slot gone but the count not yet
    /// decremented (which on a cap-full pool would spuriously shed it). Returns
    /// the freed pool, or `None` if nothing was removed.
    fn remove_and_dec(
        &self,
        proof_id: &str,
        still_removable: impl Fn(&Slot) -> bool,
    ) -> Option<PoolId> {
        let mut c = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let (_, slot) = self.slots.remove_if(proof_id, |_, v| still_removable(v))?;
        let e = c.get_mut(slot.pool);
        *e = e.saturating_sub(1);
        self.set_gauges(slot.pool, c.get(slot.pool), c.total());
        Some(slot.pool)
    }

    /// Mark a reserved slot as handed off to the cluster (see
    /// [`Slot::committed_at`]). Called by [`SlotGuard::commit`]. Also (re)starts
    /// the reap deadline from now, so the committed backstop TTL runs from
    /// commit time, and clears any absence clock (a fresh commit is a liveness
    /// signal). No-op if the slot is already gone.
    fn mark_committed(&self, proof_id: &str) {
        if let Some(mut e) = self.slots.get_mut(proof_id) {
            let now = Instant::now();
            let s = e.value_mut();
            s.committed_at = Some(now);
            s.observe_live(now, self.ttl); // fresh commit is a liveness signal
        }
    }

    /// Whether any slot is currently tracked. The reaper uses this to skip the
    /// per-tick cluster reconcile query when there is nothing to reconcile.
    pub fn has_tracked_slots(&self) -> bool {
        !self.slots.is_empty()
    }

    /// Release the slot held by `proof_id`, if any. Idempotent.
    pub fn release(&self, proof_id: &str) {
        // Cheap pre-check so a terminal poll of an ungated / already-released
        // proof (which holds no slot) skips the `counts` lock entirely — the id
        // is the caller's own and nothing inserts it after its terminal poll.
        if !self.slots.contains_key(proof_id) {
            return;
        }
        self.remove_and_dec(proof_id, |_| true);
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
    /// still being polled (i.e. still alive), and clear its absence clock — a
    /// non-terminal client poll is a liveness signal, so it must also stop the
    /// reconciler from releasing the slot even if the cluster's Pending list
    /// transiently doesn't show it. No-op if the slot isn't tracked (ungated
    /// request, or already released). Called on every non-terminal status/details
    /// poll.
    pub fn touch(&self, proof_id: &str) {
        if let Some(mut e) = self.slots.get_mut(proof_id) {
            e.value_mut().observe_live(Instant::now(), self.ttl);
        }
    }

    /// Backstop reclaim for COMMITTED slots whose reap deadline has passed. This
    /// is a TRUE backstop: the deadline is refreshed both by `touch` (a client
    /// poll) AND by `reconcile` seeing the proof still Pending, so a committed
    /// slot only ages out — and is only reaped — when the reconciler CANNOT
    /// confirm liveness (the cluster query is persistently failing / truncated /
    /// unimplemented) AND no client is polling. In normal operation the
    /// reconciler releases finished/gone proofs; this only catches slots stranded
    /// by a prolonged cluster-query outage.
    ///
    /// Consequence: while reconcile is working, a proof that stays Pending AND is
    /// still within its deadline (a genuinely slow proof, or a cluster-side
    /// deadlock the cluster still considers live) keeps its deadline refreshed and
    /// is therefore NEVER reaped — its slot is held until the proof resolves, its
    /// cluster `deadline` passes, or the gateway restarts. That is intentional:
    /// such a proof still occupies cluster proving capacity, so freeing the slot
    /// would over-admit against it; the `GatewaySlotWedged` alert (fed by
    /// `report_slot_ages`) is the human-facing signal instead. Note the reconcile's "live" set is the
    /// cluster's RUNNABLE set (`Pending ∧ deadline ≥ now`, see `fetch_pending_proofs`),
    /// NOT every `Pending` row: a past-deadline zombie the cluster will never run
    /// drops out of the live set and is released by reconcile (not left to this
    /// backstop), because it consumes no capacity.
    ///
    /// RESERVED slots are deliberately NOT reaped, even past their deadline: they
    /// are owned by the in-flight handler's [`SlotGuard`], and their proof may
    /// still be uploading / about to be created. Reaping one is UNSAFE, not just
    /// conservative — the guard outlives the reap, so a later `commit()` +
    /// successful `create` would leave a live proof running with no tracked slot
    /// (the reaper already decremented the count), i.e. an over-admit. A stranded
    /// RESERVED slot (e.g. a pre-commit `upload_raw` that hangs) is instead
    /// bounded by the request future being cancelled — a client disconnect or the
    /// transport/keepalive timeout drops the handler future, running
    /// `SlotGuard::Drop` → `release`. The only unbounded case is a half-open
    /// connection that never errors AND a client that never disconnects; that is
    /// left to the transport layer rather than reaped here, precisely to preserve
    /// the no-over-admit guarantee above.
    pub fn reap(&self) {
        // A committed slot is expired once its stored reap deadline has passed.
        // Capture `now` once; a concurrent `touch` pushes the deadline to a
        // far-future `now2 + ttl`, so the removal re-check below still spares it.
        let now = Instant::now();
        let expired = |s: &Slot| s.committed_at.is_some() && s.deadline <= now;
        let candidates: Vec<String> = self
            .slots
            .iter()
            .filter(|e| expired(e.value()))
            .map(|e| e.key().clone())
            .collect();
        let mut reclaimed = 0usize;
        for id in candidates {
            // remove_and_dec re-checks the predicate atomically under the counts
            // lock: a concurrent `touch` that refreshed the deadline (proof still
            // polled) spares it, and remove+decrement stay atomic (no
            // spurious-shed window).
            if let Some(pool) = self.remove_and_dec(&id, expired) {
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
        // Prune stale demand under the `counts` lock so this write honours the
        // "all demand writes happen under counts" invariant that `out_ranked`
        // relies on (otherwise a priority decision could be made against a demand
        // map being torn down concurrently). Held separately from the per-slot
        // `remove_and_dec` calls above — never nested — so there is no re-entrant
        // lock.
        {
            let _c = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            self.demand
                .retain(|_, d| d.last_seen.elapsed() <= self.priority_ttl);
        }
        // Refresh the demand gauge for both pools so a demander that just went
        // stale is reflected. `metric_demand_gauge` is a no-op when metrics is
        // None, and takes no `counts` lock — safe from the reaper task.
        self.metric_demand_gauge(PoolId::Range);
        self.metric_demand_gauge(PoolId::Agg);
    }

    /// Reconcile tracked slots against a cluster Pending-set [`PendingObservation`]:
    /// release any COMMITTED slot whose proof is no longer pending (Completed /
    /// Failed / Cancelled, or gone). This bounds the "slot held for the full TTL
    /// after the work is already done" case — most often op-succinct abandoning a
    /// request and re-requesting under a fresh request_id, so the old proof_id is
    /// never polled to a terminal status. `Pending` is the cluster's ONLY
    /// non-terminal status (a proof stays `Pending` while it is being proved), so
    /// absence from the live set means terminal-or-gone.
    ///
    /// Only COMMITTED slots are candidates (a reserved slot's proof isn't in the
    /// cluster's Pending set yet, so its absence is meaningless). A committed slot
    /// is released only once it has been absent for `absent_observations`
    /// CONSECUTIVE [`Complete`](PendingObservation::Complete) reconciles: a
    /// present sighting resets the streak (and refreshes the backstop deadline), an
    /// absent one advances it. A [`Partial`](PendingObservation::Partial)
    /// (truncated) view refreshes present slots but never advances absence (a
    /// missing slot may be on an unseen page); a [`None`](PendingObservation::None)
    /// observation (query error/timeout) leaves every streak untouched. This makes
    /// a single anomalous reply harmless, and — because the streak counts
    /// observations, not wall-clock time — makes absence robust to intermittent
    /// query failures. `absent_observations` therefore just sets how many clean
    /// absent snapshots confirm a proof is gone; the effective debounce window is
    /// `absent_observations × reap_period`.
    pub fn reconcile(&self, obs: PendingObservation, absent_observations: u32) -> ReconcileReport {
        let (live, partial, kind) = match &obs {
            // A gap in observation: leave every streak unchanged. Nothing to do.
            PendingObservation::None => {
                return ReconcileReport {
                    kind: "skip",
                    ..Default::default()
                }
            }
            PendingObservation::Complete(live) => (live, false, "complete"),
            PendingObservation::Partial(live) => (live, true, "partial"),
        };
        let now = Instant::now();
        // Pass 1: update each committed slot's absence streak (reserved slots are
        // ignored). Collect the ids whose streak reached the release threshold, and
        // tally present/absent among committed slots for the per-tick report.
        let mut stale: Vec<String> = Vec::new();
        let mut committed_tracked = 0usize;
        let mut present_ct = 0usize;
        let mut absent_ct = 0usize;
        // Bounded id samples for the per-tick log (committed slots only, so at most
        // ~pool caps + seeds — already tiny; cap anyway to stay log-safe).
        const ID_SAMPLE_CAP: usize = 64;
        let mut present_ids: Vec<String> = Vec::new();
        let mut absent_ids: Vec<String> = Vec::new();
        for mut e in self.slots.iter_mut() {
            // Clone the key up front: `e.key()` and `e.value_mut()` can't be held
            // together, and the arms below both mutate the slot and record the id.
            let key = e.key().clone();
            let present = live.contains(&key);
            let s = e.value_mut();
            if !s.is_committed() {
                continue; // RESERVED — not the reconciler's business
            }
            committed_tracked += 1;
            if present {
                // Confirmed live in the cluster → liveness signal (reset streak +
                // refresh the backstop reap deadline). This keeps the TTL reaper a
                // TRUE backstop: it only reclaims a committed slot when reconcile
                // CAN'T confirm liveness, never one reconcile just saw Pending — so
                // a genuinely-running-but-unpolled proof isn't reaped out from
                // under itself.
                present_ct += 1;
                if present_ids.len() < ID_SAMPLE_CAP {
                    present_ids.push(key.clone());
                }
                s.observe_live(now, self.ttl);
            } else if partial {
                // Truncated (incomplete) view: a slot missing from a capped page
                // may simply be on an unseen page, so do NOT advance its absence
                // streak — that would risk releasing a live slot we didn't see.
                // Present slots above were still refreshed on the trustworthy
                // positive sightings.
                absent_ct += 1;
                if absent_ids.len() < ID_SAMPLE_CAP {
                    absent_ids.push(key.clone());
                }
            } else if s
                .committed_at
                .is_some_and(|t| now.saturating_duration_since(t) < self.commit_grace)
            {
                // Within the post-commit grace: `commit` runs just before the
                // cluster create, so during the create leg this proof isn't in the
                // Pending set yet and its absence is not yet meaningful. Spare it
                // (exactly like a RESERVED slot) until the grace elapses, so an
                // in-flight create can't be reconciled away into an over-admit. The
                // grace is wall-clock (independent of the reap cadence), so this
                // holds even under an aggressively-fast `absent_observations ×
                // reap_period` window.
                absent_ct += 1;
                if absent_ids.len() < ID_SAMPLE_CAP {
                    absent_ids.push(key.clone());
                }
            } else {
                absent_ct += 1;
                if absent_ids.len() < ID_SAMPLE_CAP {
                    absent_ids.push(key.clone());
                }
                if s.observe_absent() >= absent_observations {
                    stale.push(key);
                }
            }
        }
        // Pass 2: release the slots that reached the threshold. remove_and_dec
        // re-checks the predicate atomically under the counts lock, so a
        // concurrent `touch` (a client poll — the liveness signal that resets the
        // streak via observe_live) landing between pass 1 and here spares the slot.
        // (mark_committed can't apply here: a `stale` slot is already committed,
        // and reserved slots never enter `stale`.) The iter_mut guards from pass 1
        // are dropped before we lock counts, preserving the counts→slots order.
        let sustained_absent =
            |s: &Slot| s.is_committed() && s.absent_streak >= absent_observations;
        let mut released = 0usize;
        for id in stale {
            if let Some(pool) = self.remove_and_dec(&id, sustained_absent) {
                self.metric_reconciled(pool);
                released += 1;
            }
        }
        if released > 0 {
            tracing::info!(
                released,
                "admission reconciler released slots continuously absent from the cluster Pending set"
            );
        }
        ReconcileReport {
            kind,
            live: live.len(),
            committed_tracked,
            present: present_ct,
            absent: absent_ct,
            released,
            present_ids,
            absent_ids,
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

    pub fn with_metrics(mut self, metrics: Arc<AdmissionMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Set the post-commit grace (see [`commit_grace`](Self::commit_grace)). The
    /// production path sets this from `GATEWAY_ADMISSION_RECONCILE_COMMIT_GRACE_SECS`;
    /// `new` leaves it `Duration::ZERO` (no grace) so unit tests exercise the raw
    /// streak logic unless they opt in.
    pub fn with_commit_grace(mut self, grace: Duration) -> Self {
        self.commit_grace = grace;
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
    fn metric_reconciled(&self, pool: PoolId) {
        if let Some(m) = &self.metrics {
            m.reconciled
                .get_or_create(&PoolLabel {
                    pool: pool.label().into(),
                })
                .inc();
        }
    }
    /// Publish, per `(pool, requester)`, how long that requester's OLDEST slot
    /// has been held. Call it on the reconcile tick.
    ///
    /// Answers "who is holding the pool right now, and for how long" — the
    /// question a completed-proof latency metric cannot answer while the proof is
    /// still running. It deliberately does not separate the reasons a slot is
    /// held: a genuinely slow proof, a slot whose terminal release was lost, and
    /// a RESERVED slot still uploading its stdin (a real cost here — the
    /// gateway↔on-prem link is ~30 Mbit, so multi-MB inputs take a while) all
    /// look the same. They should: each one means this requester is occupying
    /// scarce capacity, which is what the operator acts on.
    ///
    /// The family is cleared first so a requester that released everything stops
    /// exporting a series, rather than freezing at its last value forever. That
    /// also bounds the `requester` label set to whoever currently holds a slot.
    ///
    /// Two things this deliberately does NOT expose, so they are not re-litigated:
    /// a per-requester slot COUNT (the pool total is already
    /// `gateway_admission_inflight`, and "how many" has not been the question
    /// during an incident — "how long" has), and any historical distribution such
    /// as a weekly p90 per requester (a gauge cannot answer it; the analytics
    /// Postgres has per-proof rows and an ad-hoc query is the right tool at ~6
    /// proofs/hour).
    pub fn report_slot_ages(&self) {
        let Some(m) = &self.metrics else { return };
        let now = Instant::now();
        // Oldest per (pool, requester): max age, since that is the one that would
        // trip a "held too long" threshold.
        let mut oldest: std::collections::HashMap<(PoolId, String), u64> =
            std::collections::HashMap::new();
        for entry in self.slots.iter() {
            let slot = entry.value();
            let requester = requester_label(slot.requester.as_deref());
            let age = now.saturating_duration_since(slot.admitted_at).as_secs();
            oldest
                .entry((slot.pool, requester))
                .and_modify(|a| *a = (*a).max(age))
                .or_insert(age);
        }
        m.slot_held_seconds.clear();
        for ((pool, requester), age) in oldest {
            m.slot_held_seconds
                .get_or_create(&PoolRequesterLabel {
                    pool: pool.label().into(),
                    requester,
                })
                .set(age as i64);
        }
    }

    fn metric_seeded(&self, pool: PoolId) {
        if let Some(m) = &self.metrics {
            m.seeded
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
    /// outlives this scope. Marks the slot COMMITTED so the reconciler may
    /// resolve it against cluster state. Call once the cluster leg has run
    /// (create returned OR errored — an errored create may still have registered
    /// the proof, so keeping it committed lets the reconciler release it only if
    /// it is truly absent, rather than under-counting a live proof).
    pub fn commit(mut self) {
        self.committed = true;
        self.controller.mark_committed(self.proof_id);
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

/// Render a slot's holder as a metric label value.
///
/// Three cases, deliberately distinguishable — an operator reading an alert has
/// to be able to tell them apart:
/// - `0x`-prefixed lowercase hex: a real address. The prefix is not cosmetic:
///   this value gets pasted into the explorer, whose requester filter is an
///   exact string match against `0x`-prefixed values written by the router. A
///   bare hex string silently returns zero rows, which reads as "this requester
///   has no proofs" and sends triage down the wrong path.
/// - `unauthenticated`: the zero address, which is what `AuthMode::None`
///   (the DEFAULT for `GATEWAY_AUTH_MODE`) hands every caller. Note what this
///   does and does not buy: every proposer still collapses into ONE series, so
///   attribution is genuinely unavailable until `GATEWAY_AUTH_MODE=verify` —
///   naming it only stops the reader from chasing an address that does not
///   exist. It is also, deliberately, the one label value that cannot be pasted
///   into the explorer (which stores the zero address as `0x0000…0`); a reader
///   who needs to search should fix auth first.
/// - `unknown`: no requester recorded at all (a seeded proof whose cluster
///   record carried none).
fn requester_label(requester: Option<&[u8]>) -> String {
    match requester {
        None => "unknown".to_string(),
        Some([]) => "unknown".to_string(),
        Some(r) if r.iter().all(|b| *b == 0) => "unauthenticated".to_string(),
        Some(r) => format!("0x{}", hex::encode(r)),
    }
}

/// Labels for per-requester slot occupancy. See [`requester_label`] for the
/// `requester` value's three forms.
///
/// Cardinality is the number of proposers holding slots (a handful, and bounded
/// above by the pool caps at any instant). If the gateway is ever opened to
/// arbitrary signers, this becomes an unbounded label and would need to fold to
/// the configured `PRIORITY_ORDER` set.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PoolRequesterLabel {
    pub pool: String,
    pub requester: String,
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
    reconciled: Family<PoolLabel, Counter>,
    seeded: Family<PoolLabel, Counter>,
    unclassified: Counter,
    priority_yielded: Family<PoolLabel, Counter>,
    priority_would_yield: Family<PoolLabel, Counter>,
    priority_demand: Family<PoolLabel, Gauge>,
    priority_hold_open: Family<PoolLabel, Counter>,
    /// Age of the OLDEST slot each requester currently holds in each pool.
    ///
    /// The in-flight counterpart to everything else here, which only moves on
    /// state transitions: while a proof is stuck this number just keeps growing,
    /// so a threshold on it is a direct read on pressure. A completed-proof duration
    /// metric has the opposite behaviour — it goes quiet exactly when things are
    /// worst, because a stuck proof never produces a sample.
    slot_held_seconds: Family<PoolRequesterLabel, Gauge>,
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
        let reconciled = Family::<PoolLabel, Counter>::default();
        let seeded = Family::<PoolLabel, Counter>::default();
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
            "gateway_admission_reconciled",
            "Slots released by the cluster-truth reconciler (proof no longer pending), per pool",
            reconciled.clone(),
        );
        registry.register(
            "gateway_admission_seeded",
            "Slots seeded from cluster in-flight proofs at restart, per pool",
            seeded.clone(),
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
        let slot_held_seconds = Family::<PoolRequesterLabel, Gauge>::default();
        registry.register(
            "gateway_admission_slot_held_seconds",
            "Age of the oldest slot each requester currently holds, per pool",
            slot_held_seconds.clone(),
        );
        Self {
            registry,
            slot_held_seconds,
            inflight,
            global_inflight,
            admitted,
            rejected,
            rejected_global,
            would_reject,
            reaped,
            reconciled,
            seeded,
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
        // Wide ttl (150ms) so commit→first-reap can't exceed it under CI
        // scheduling jitter and flake the "not yet expired" assert (matches the
        // margins in `touch_prevents_reap_of_polled_slot`).
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_millis(150),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("p1", 2, RANGE_VK, &[0x01]).unwrap();
        c.guard("p1").commit(); // reap is committed-only; simulate the cluster handoff
        c.reap();
        assert_eq!(c.in_flight(PoolId::Range), 1); // not yet expired
        std::thread::sleep(Duration::from_millis(220));
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
            Duration::from_millis(150), // ttl (wide margins so scheduling jitter can't flake it)
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("p1", 2, RANGE_VK, &[0x01]).unwrap();
        c.guard("p1").commit(); // reap is committed-only; simulate the cluster handoff
        std::thread::sleep(Duration::from_millis(60));
        c.touch("p1"); // still being polled → refresh deadline to now+150ms
        std::thread::sleep(Duration::from_millis(60)); // 120ms since acquire, but 60ms since touch
        c.reap();
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "recently-touched slot must not be reaped"
        );
        std::thread::sleep(Duration::from_millis(220)); // now well past ttl since last touch
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
        c.seed("restart-1", PoolId::Range, None, None);
        c.seed("restart-2", PoolId::Range, None, None); // over cap, but seed doesn't check it
        assert_eq!(c.in_flight(PoolId::Range), 2);
        c.seed("restart-1", PoolId::Range, None, None); // already tracked → no double-count
        assert_eq!(c.in_flight(PoolId::Range), 2);
        // A normal acquire still enforces the cap against the seeded count.
        assert!(c.try_acquire("p1", 2, RANGE_VK, &[0x01]).is_err());
        // Releasing a seeded slot behaves like any other slot.
        c.release("restart-1");
        assert_eq!(c.in_flight(PoolId::Range), 1);
    }

    #[test]
    fn seeded_slot_is_committed_and_reconciled_by_cluster_truth() {
        // A restart-seeded slot is COMMITTED: the reconciler is its authority —
        // kept while the cluster still reports it Pending, released once absent.
        // No seed_grace; no reliance on the TTL for the normal case.
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
        c.seed("seeded-proof", PoolId::Range, None, None);
        assert_eq!(c.in_flight(PoolId::Range), 1);

        // Cluster still reports it Pending → reconcile keeps it.
        let live: HashSet<String> = ["seeded-proof".to_string()].into_iter().collect();
        c.reconcile(PendingObservation::Complete(live), 1);
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "a seeded proof still Pending in the cluster is kept (it's genuinely running)"
        );

        // Cluster no longer reports it → reconcile releases it (threshold 1).
        c.reconcile(PendingObservation::Complete(HashSet::new()), 1);
        assert_eq!(
            c.in_flight(PoolId::Range),
            0,
            "a seeded proof absent from the cluster is reconciled away"
        );
    }

    #[test]
    fn seeded_slot_reaped_by_backstop_ttl_when_reconcile_unavailable() {
        // If the reconciler can't reach the cluster, the committed (seeded) slot
        // is still bounded by the backstop TTL reaper.
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_millis(150), // ttl backstop (wide margin vs CI jitter)
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.seed("seeded-proof", PoolId::Range, None, None);
        c.reap();
        assert_eq!(c.in_flight(PoolId::Range), 1, "within ttl, kept");
        std::thread::sleep(Duration::from_millis(220));
        c.reap();
        assert_eq!(
            c.in_flight(PoolId::Range),
            0,
            "an unpolled committed slot is reclaimed by the backstop TTL"
        );
    }

    #[test]
    fn reap_never_reclaims_a_reserved_slot() {
        // A RESERVED (uncommitted, in-flight handler) slot must NOT be reaped even
        // past the TTL — it is owned by the request handler's guard. Reaping it
        // would leave a live, about-to-be-created proof untracked → over-admit.
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_millis(30), // tiny ttl
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("reserved", 2, RANGE_VK, &[0x01]).unwrap(); // committed_at = None
        std::thread::sleep(Duration::from_millis(45)); // well past ttl
        c.reap();
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "a reserved slot is guard-owned and must never be reaped"
        );
        assert!(c.slots.contains_key("reserved"));
    }

    #[test]
    fn reconcile_releases_absent_proof_but_keeps_pending() {
        // Range cap 2 so both admit. Threshold 1 → one absent observation releases.
        let c = AdmissionController::new(
            classifier(),
            2,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("p_live", 2, RANGE_VK, &[0x01]).unwrap();
        c.try_acquire("p_gone", 2, RANGE_VK, &[0x01]).unwrap();
        // Both are handed off to the cluster (committed); reconcile only ever
        // touches committed slots.
        c.guard("p_live").commit();
        c.guard("p_gone").commit();
        assert_eq!(c.in_flight(PoolId::Range), 2);

        // Cluster now reports only p_live as Pending; p_gone finished/vanished.
        let live: HashSet<String> = ["p_live".to_string()].into_iter().collect();
        c.reconcile(PendingObservation::Complete(live), 1);
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "the absent proof's slot is released; the pending one is kept"
        );
        assert!(c.slots.contains_key("p_live"));
        assert!(!c.slots.contains_key("p_gone"));
    }

    #[test]
    fn reconcile_never_touches_a_reserved_uncommitted_slot() {
        // A slot still RESERVED in the request_proof handler (uploading / about
        // to create, not yet committed) has no cluster proof yet, so its absence
        // from the live set must NOT reconcile it away — even at threshold 1.
        // This is the regression guard for the upload-under-guard over-admit.
        let c = AdmissionController::new(
            classifier(),
            2,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("reserved", 2, RANGE_VK, &[0x01]).unwrap(); // committed_at = None
        c.reconcile(PendingObservation::Complete(HashSet::new()), 1); // absent everywhere
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "a reserved (uncommitted) slot must never be reconciled away"
        );
        assert!(c.slots.contains_key("reserved"));
    }

    #[test]
    fn reconcile_spares_recently_committed_slot_even_if_absent() {
        // A just-committed proof may not be visible in the cluster Pending list
        // yet; with a threshold above 1, a single absent observation must spare it
        // so reconcile can't race the create→registration window into an
        // over-admit.
        let c = AdmissionController::new(
            classifier(),
            2,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("just_committed", 2, RANGE_VK, &[0x01])
            .unwrap();
        c.guard("just_committed").commit(); // committed_at = now
        c.reconcile(PendingObservation::Complete(HashSet::new()), 2); // 1 absent < threshold 2 → spared
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "a slot absent for fewer than the threshold observations must not be reconciled away"
        );
    }

    #[test]
    fn reconcile_requires_sustained_absence() {
        // A committed slot is released only after being absent for
        // `absent_observations` CONSECUTIVE Complete reconciles — a single absent
        // observation isn't enough, and a reappearance resets the streak (so an
        // anomalous/empty reply can't mass-release live slots).
        let c = AdmissionController::new(
            classifier(),
            2,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("p", 2, RANGE_VK, &[0x01]).unwrap();
        c.guard("p").commit();
        let empty: HashSet<String> = HashSet::new();
        let n = 2; // release after 2 consecutive absent observations

        // First absent observation: streak 1 < 2, does NOT release.
        c.reconcile(PendingObservation::Complete(empty.clone()), n);
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "one absent observation must not release"
        );

        // Reappears in the cluster set: streak resets to 0.
        let live: HashSet<String> = ["p".to_string()].into_iter().collect();
        c.reconcile(PendingObservation::Complete(live), n);
        // A single absent observation after the reset is streak 1 < 2 → kept.
        c.reconcile(PendingObservation::Complete(empty.clone()), n);
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "a reappearance resets the streak; a later single absent observation must not release"
        );

        // Second consecutive absent observation: streak 2 >= 2 → released.
        c.reconcile(PendingObservation::Complete(empty), n);
        assert_eq!(
            c.in_flight(PoolId::Range),
            0,
            "a slot absent for the full threshold of consecutive observations is released"
        );
    }

    #[test]
    fn skip_neither_resets_nor_advances_absent_streak() {
        // A SKIPPED reconcile (query error/timeout/unimplemented) is a
        // `PendingObservation::None`: a gap in observation. Because the streak is
        // an OBSERVATION COUNT, a skip must leave it exactly as-is — neither reset
        // (so a genuinely-gone proof still gets reclaimed across a flaky query,
        // round-6 #3) nor advanced (so a gap can't forge absence).
        let c = AdmissionController::new(
            classifier(),
            2,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("p", 2, RANGE_VK, &[0x01]).unwrap();
        c.guard("p").commit();
        let empty: HashSet<String> = HashSet::new();
        let n = 3;

        // Absent #1 → streak 1.
        c.reconcile(PendingObservation::Complete(empty.clone()), n);
        assert_eq!(c.in_flight(PoolId::Range), 1);
        // A skip must not release and must not touch the streak.
        c.reconcile(PendingObservation::None, n);
        assert_eq!(c.in_flight(PoolId::Range), 1, "a skip must not release");
        // Absent #2 → streak 2 (< 3). If the skip had ADVANCED the streak, this
        // would already be 3 and release — it must not.
        c.reconcile(PendingObservation::Complete(empty.clone()), n);
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "a skip must not advance the streak"
        );
        // Absent #3 → streak 3 >= 3 → released. If the skip had RESET the streak,
        // this would be only the 2nd post-skip absent and would not release — so
        // reaching the threshold here proves absence accrued across the skip.
        c.reconcile(PendingObservation::Complete(empty), n);
        assert_eq!(
            c.in_flight(PoolId::Range),
            0,
            "absence accrues across the skip: 3 absent observations reach the threshold"
        );
    }

    #[test]
    fn reconcile_present_refreshes_reap_deadline() {
        // A committed proof still in the cluster Pending set keeps the TTL reaper a
        // TRUE backstop: reconcile seeing it Pending refreshes the reap deadline, so
        // a genuinely-running-but-unpolled proof is NOT reaped out from under itself
        // once its original TTL elapses.
        let ttl = Duration::from_millis(80);
        let c = AdmissionController::new(
            classifier(),
            2,
            2,
            None,
            true,
            ttl,
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("p", 2, RANGE_VK, &[0x01]).unwrap();
        c.guard("p").commit();
        std::thread::sleep(Duration::from_millis(120)); // original deadline now passed
        let live: HashSet<String> = ["p".to_string()].into_iter().collect();
        c.reconcile(PendingObservation::Complete(live), 2); // present → deadline refreshed
        c.reap(); // would reclaim on the stale deadline; refreshed one spares it
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "a proof seen Pending by reconcile must not be reaped on its old deadline"
        );
    }

    #[test]
    fn seed_ghost_slot_released_by_reconcile_absence_not_ttl() {
        // A restart-seeded slot for a proof that is NOT actually in the cluster (a
        // stale/ghost seed) must be released by reconcile after the absence
        // threshold — WITHOUT waiting for the (long) backstop TTL. This is the
        // core "over-admit heals in minutes, not an hour" guarantee.
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_secs(3600), // long TTL: reconcile must win, not this
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.seed("ghost", PoolId::Range, None, None);
        assert_eq!(c.in_flight(PoolId::Range), 1);
        let empty: HashSet<String> = HashSet::new();
        // Absent #1 → streak 1 < 2, kept.
        c.reconcile(PendingObservation::Complete(empty.clone()), 2);
        assert_eq!(c.in_flight(PoolId::Range), 1);
        // Absent #2 → streak 2 >= 2 → released, long before the 3600s TTL.
        c.reconcile(PendingObservation::Complete(empty), 2);
        assert_eq!(
            c.in_flight(PoolId::Range),
            0,
            "a ghost seed is reconciled away by cluster truth, not left to the TTL backstop"
        );
    }

    #[test]
    fn seed_over_cap_converges_via_reconcile() {
        // Restart seed bypasses the cap (it reflects real backend in-flight), so
        // counts can briefly exceed the cap. Reconcile against cluster truth
        // converges them back down — the "#6 seed bypasses cap" behaviour is
        // intentional and self-healing.
        let c = enforce_ctrl(); // Range cap 1
        c.seed("s1", PoolId::Range, None, None);
        c.seed("s2", PoolId::Range, None, None);
        c.seed("s3", PoolId::Range, None, None);
        assert_eq!(c.in_flight(PoolId::Range), 3, "seed bypasses the cap");
        // Cluster only still runs s2; s1/s3 are gone.
        let live: HashSet<String> = ["s2".to_string()].into_iter().collect();
        c.reconcile(PendingObservation::Complete(live), 1);
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "reconcile converges over-cap seeds back down to cluster truth"
        );
        assert!(c.slots.contains_key("s2"));
    }

    #[test]
    fn partial_view_refreshes_present_but_never_releases_absent() {
        // A truncated (Partial) Pending view is incomplete: it must NEVER release a
        // slot merely absent from it (that slot may be on an unseen page), even at
        // threshold 1 and across repeated Partials.
        let c = AdmissionController::new(
            classifier(),
            2,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("p_seen", 2, RANGE_VK, &[0x01]).unwrap();
        c.try_acquire("p_unseen", 2, RANGE_VK, &[0x01]).unwrap();
        c.guard("p_seen").commit();
        c.guard("p_unseen").commit();
        let partial: HashSet<String> = ["p_seen".to_string()].into_iter().collect();
        c.reconcile(PendingObservation::Partial(partial.clone()), 1);
        c.reconcile(PendingObservation::Partial(partial), 1);
        assert_eq!(
            c.in_flight(PoolId::Range),
            2,
            "a Partial (truncated) view must never release an absent slot"
        );
    }

    #[test]
    fn partial_view_refreshes_present_slot_deadline() {
        // The slots a Partial view CAN see are trustworthy positive sightings, so
        // their backstop reap deadline is refreshed (a genuinely-running proof
        // visible on the page isn't reaped just because the page was truncated).
        let ttl = Duration::from_millis(80);
        let c = AdmissionController::new(
            classifier(),
            2,
            2,
            None,
            true,
            ttl,
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("p", 2, RANGE_VK, &[0x01]).unwrap();
        c.guard("p").commit();
        std::thread::sleep(Duration::from_millis(120)); // original deadline passed
        let partial: HashSet<String> = ["p".to_string()].into_iter().collect();
        c.reconcile(PendingObservation::Partial(partial), 2); // present in partial → refreshed
        c.reap();
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "a slot present in a Partial view must have its reap deadline refreshed"
        );
    }

    #[test]
    fn reconcile_within_commit_grace_spares_absent_committed_slot() {
        // A1: `commit` runs just before the cluster create, so during the create
        // leg an absent committed slot must be SPARED until the commit grace
        // elapses — otherwise an in-flight create is reconciled into an over-admit,
        // even under an aggressive threshold (here 1). After the grace, absence
        // counts normally.
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
        .with_commit_grace(Duration::from_millis(120));
        c.try_acquire("p", 2, RANGE_VK, &[0x01]).unwrap();
        c.guard("p").commit(); // committed_at = now
        let empty: HashSet<String> = HashSet::new();
        // Absent while within the grace: NOT counted, even at threshold 1 across
        // repeated reconciles.
        c.reconcile(PendingObservation::Complete(empty.clone()), 1);
        c.reconcile(PendingObservation::Complete(empty.clone()), 1);
        assert_eq!(
            c.in_flight(PoolId::Range),
            1,
            "a just-committed slot must be spared by the commit grace while its create leg runs"
        );
        // Past the grace: sustained absence now counts and (threshold 1) releases.
        std::thread::sleep(Duration::from_millis(140));
        c.reconcile(PendingObservation::Complete(empty), 1);
        assert_eq!(
            c.in_flight(PoolId::Range),
            0,
            "once past the commit grace, absence releases the slot"
        );
    }

    #[test]
    fn commit_grace_defaults_off_so_new_controllers_count_absence_immediately() {
        // `new` leaves commit_grace = ZERO (opt-in via with_commit_grace), so a
        // controller built without it counts absence from the first reconcile —
        // the behaviour every other reconcile test here relies on.
        let c = enforce_ctrl();
        c.try_acquire("p", 2, RANGE_VK, &[0x01]).unwrap();
        c.guard("p").commit();
        c.reconcile(PendingObservation::Complete(HashSet::new()), 1);
        assert_eq!(
            c.in_flight(PoolId::Range),
            0,
            "with no commit grace, one absent Complete reconcile at threshold 1 releases"
        );
    }

    #[test]
    fn guard_drop_without_commit_releases_the_slot() {
        // Every pre-commit early return in request_proof relies on the SlotGuard's
        // Drop releasing the reserved slot.
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
        c.try_acquire("x", 2, RANGE_VK, &[0x01]).unwrap();
        assert_eq!(c.in_flight(PoolId::Range), 1);
        {
            let _g = c.guard("x"); // dropped WITHOUT commit at end of scope
        }
        assert_eq!(
            c.in_flight(PoolId::Range),
            0,
            "dropping an uncommitted guard must release the reserved slot"
        );
        assert!(!c.slots.contains_key("x"));
    }

    /// Per-requester occupancy is reported per pool, attributed to the holder,
    /// and STOPS being reported once the slot is released.
    ///
    /// The release half is the part worth pinning: without the `clear()` in
    /// `report_slot_ages` the gauge would freeze at its last value, so a
    /// requester that finished long ago would keep looking like it was holding a
    /// slot — and a "held too long" alert would latch on forever.
    #[test]
    fn slot_ages_are_reported_per_requester_and_cleared_on_release() {
        let m = Arc::new(AdmissionMetrics::new());
        let c = AdmissionController::new(
            classifier(),
            2,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        )
        .with_metrics(m.clone());

        c.try_acquire("p1", 2, RANGE_VK, &[0xaa]).unwrap(); // range, requester aa
        c.try_acquire("p2", 3, AGG_VK, &[0xbb]).unwrap(); // agg, requester bb
        c.report_slot_ages();
        let out = m.render();
        assert!(
            out.contains("gateway_admission_slot_held_seconds{pool=\"range\",requester=\"0xaa\"}"),
            "range slot must be attributed to its holder:\n{out}"
        );
        assert!(
            out.contains("gateway_admission_slot_held_seconds{pool=\"agg\",requester=\"0xbb\"}"),
            "agg slot must be attributed to its holder:\n{out}"
        );

        c.release("p1");
        c.report_slot_ages();
        let out = m.render();
        assert!(
            !out.contains("requester=\"0xaa\""),
            "a released slot must stop exporting a series, not freeze:\n{out}"
        );
        assert!(
            out.contains("gateway_admission_slot_held_seconds{pool=\"agg\",requester=\"0xbb\"}"),
            "the still-held slot must remain:\n{out}"
        );
    }

    /// A seeded slot whose cluster record carried no requester still has to be
    /// reported rather than silently dropped — it occupies real capacity.
    #[test]
    fn seeded_slots_report_as_unknown_requester() {
        let m = Arc::new(AdmissionMetrics::new());
        let c = AdmissionController::new(
            classifier(),
            2,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        )
        .with_metrics(m.clone());
        c.seed("recovered", PoolId::Range, None, None);
        c.report_slot_ages();
        let out = m.render();
        assert!(
            out.contains(
                "gateway_admission_slot_held_seconds{pool=\"range\",requester=\"unknown\"}"
            ),
            "a seeded slot must be visible as unknown-requester occupancy:\n{out}"
        );
    }

    /// A restart must not lose attribution or age.
    ///
    /// The cluster's own record carries both `requester` and `created_at`, so a
    /// seeded slot can be restored exactly. Without this, restarting the gateway
    /// — a common first move during an incident — resets every stuck proof's age
    /// to zero and drops its owner, blinding the alert for another ~15 minutes
    /// precisely when someone is hunting for the stuck one.
    #[test]
    fn seeded_slots_keep_their_requester_and_backdated_age() {
        let m = Arc::new(AdmissionMetrics::new());
        let c = AdmissionController::new(
            classifier(),
            2,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        )
        .with_metrics(m.clone());
        // Recovered from the cluster: owned by 0xaa, already running for 20 min.
        c.seed(
            "recovered",
            PoolId::Range,
            Some(vec![0xaa]),
            Some(Duration::from_secs(1200)),
        );
        c.report_slot_ages();
        let out = m.render();
        assert!(
            out.contains(
                "gateway_admission_slot_held_seconds{pool=\"range\",requester=\"0xaa\"} 1200\n"
            ),
            "a seeded slot must keep its owner and report its pre-restart age as \
             exactly 1200 seconds. Matching only a leading '12' would also accept \
             120, 1299, and — the one that matters — 1200000 from an as_millis() \
             mix-up:\n{out}"
        );
    }

    /// The zero address means `AuthMode::None` (the DEFAULT), not a real
    /// proposer. Rendering it as an address would collapse every requester into
    /// one all-zeroes series and silently kill attribution — so it gets a name
    /// that tells the operator which of the two it is.
    #[test]
    fn zero_address_is_labelled_unauthenticated() {
        assert_eq!(requester_label(Some(&[0u8; 20])), "unauthenticated");
        assert_eq!(requester_label(None), "unknown");
        assert_eq!(requester_label(Some(&[])), "unknown");
        // Real addresses keep the 0x prefix the explorer's exact-match filter
        // (and the rest of this system) expects.
        assert_eq!(requester_label(Some(&[0xab, 0xcd])), "0xabcd");
    }

    /// The reported age is the OLDEST slot a requester holds, not the newest, so
    /// one stuck proof cannot be masked by fresher ones from the same proposer.
    ///
    /// Ages are injected via `seed()` rather than produced by sleeping: an exact
    /// expected value distinguishes max (100) from min (10), from
    /// last-writer-wins (either), and from sum (110) — a sleep-based version can
    /// only tell "not zero", and pays a second of wall time to do it.
    #[test]
    fn slot_age_reports_the_oldest_of_a_requesters_slots() {
        let m = Arc::new(AdmissionMetrics::new());
        let c = AdmissionController::new(
            classifier(),
            4,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(),
            false,
            Duration::from_secs(90),
        )
        .with_metrics(m.clone());
        let who = Some(vec![0xaa]);
        c.seed(
            "old",
            PoolId::Range,
            who.clone(),
            Some(Duration::from_secs(100)),
        );
        c.seed("new", PoolId::Range, who, Some(Duration::from_secs(10)));
        c.report_slot_ages();
        let out = m.render();
        assert!(
            out.contains(
                "gateway_admission_slot_held_seconds{pool=\"range\",requester=\"0xaa\"} 100\n"
            ),
            "must report the oldest slot's age (100), not the newest (10) or the \
             sum (110):\n{out}"
        );
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
        // Assert the SAMPLE lines (label-set present ⇒ the counter actually
        // incremented), not just the always-present `# TYPE` name — the latter
        // would pass even if admit/reject accounting were dead.
        assert!(
            out.contains("gateway_admission_admitted_total{pool=\"range\"} 1"),
            "admit must increment admitted_total for range:\n{out}"
        );
        assert!(
            out.contains("gateway_admission_rejected_total{pool=\"range\"} 1"),
            "over-cap shed must increment rejected_total for range:\n{out}"
        );
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
    fn priority_disabled_records_no_demand() {
        // enforce on, priority OFF: cap sheds must NOT populate the demand map.
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::from([(AA.to_vec(), 0u32), (BB.to_vec(), 1u32)]),
            false,
            Duration::from_secs(90),
        );
        c.try_acquire("occ", 2, RANGE_VK, &[0x01]).unwrap();
        c.try_acquire("a1", 2, RANGE_VK, AA).unwrap_err(); // cap shed
        assert!(c.demand_is_empty(), "priority-off must not record demand");
    }

    #[test]
    fn unconfigured_equal_default_ranks_do_not_yield() {
        // priority ENABLED but NO ranks configured → two distinct unlisted
        // proposers must NOT yield a free slot to each other (empty order = no-op).
        let c = AdmissionController::new(
            classifier(),
            1,
            2,
            None,
            true,
            Duration::from_secs(3600),
            std::collections::HashMap::new(), // empty PRIORITY_ORDER
            true,
            Duration::from_secs(90),
        );
        let x: &[u8] = &[0x01];
        let y: &[u8] = &[0x02];
        c.try_acquire("occ", 2, RANGE_VK, x).unwrap(); // fills cap
        c.try_acquire("x1", 2, RANGE_VK, x).unwrap_err(); // x shed → demand[x]
        c.release("occ");
        assert!(
            c.try_acquire("y1", 2, RANGE_VK, y).is_ok(),
            "unconfigured proposer must not yield to another unconfigured one"
        );
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
        // A yield bumps hold_open in lockstep with yielded (v1: hold_open counts
        // yield events), so its sample line must be present for the yielded pool.
        assert!(
            out.contains("gateway_admission_priority_hold_open_total{pool=\"range\"}"),
            "yield must have incremented priority_hold_open for range:\n{out}"
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
