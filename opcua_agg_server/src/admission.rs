// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

//! Bounded admission bookkeeping. Network and shared integration state belong to callers.
//!
//! A successful full discovery snapshot may establish absence. Partial snapshots
//! provide positive sightings only. Exactly one ticket can be running, and an
//! expired running identity remains reserved until its worker reports termination.
//! Retry limits apply to one generation. Exhausted identities occupy a bounded
//! quarantine until a fixed cooldown or confirmed absence, after which a later
//! sighting can create another generation. No unseen overflow backlog is retained.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Source {
    pub name: String,
    pub address: String,
}

impl Source {
    pub fn new(name: impl Into<String>, address: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            address: address.into(),
        }
    }

    pub fn key(&self) -> SourceKey {
        SourceKey {
            application_uri: self.name.clone(),
            canonical_endpoint: self.address.trim_end_matches('/').to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct SourceKey {
    pub application_uri: String,
    pub canonical_endpoint: String,
}

#[derive(Clone, Debug)]
pub struct Ticket {
    pub source: Source,
    pub key: SourceKey,
    pub generation: u64,
    pub attempt: u32,
    /// First accepted observation of this generation. Sources deferred before
    /// admission have no retained first-sighting timestamp in this scheduler.
    pub first_observed_at: Instant,
    pub enqueued_at: Instant,
    pub started_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinishDisposition {
    Completed,
    RetryScheduled,
    Exhausted,
    Retired,
    IgnoredStale,
}

#[derive(Clone, Debug)]
pub struct FinishOutcome {
    pub disposition: FinishDisposition,
    pub ticket: Ticket,
    pub ended_at: Instant,
    pub next_ready_at: Option<Instant>,
    pub queue: QueueStats,
}

#[derive(Clone, Debug)]
pub struct RetiredRecord {
    pub key: SourceKey,
    pub generation: u64,
    pub attempts: u32,
    /// First accepted observation of this generation. Sources deferred before
    /// admission have no retained first-sighting timestamp in this scheduler.
    pub first_observed_at: Instant,
    pub enqueued_at: Instant,
    pub ended_at: Instant,
}

#[derive(Clone, Debug)]
pub struct AdmittedRecord {
    pub source: Source,
    pub key: SourceKey,
    pub generation: u64,
    /// First accepted observation of this generation. Sources deferred before
    /// admission have no retained first-sighting timestamp in this scheduler.
    pub first_observed_at: Instant,
    pub enqueued_at: Instant,
}

#[derive(Clone, Debug, Default)]
pub struct ObservationSummary {
    pub queued: usize,
    pub admitted: Vec<AdmittedRecord>,
    pub deduplicated: usize,
    pub quarantined: usize,
    pub deferred_full: usize,
    pub deferred_conflict: usize,
    /// Direct renewals admitted in the observation that releases quarantine.
    /// Later readmissions after a deferral require correlation by SourceKey in
    /// the external event log. No unbounded per-source history is retained.
    pub renewed_generations: usize,
    pub retired: Vec<RetiredRecord>,
    pub removed_active: usize,
    pub stale_running: usize,
}

#[derive(Clone, Debug, Default)]
pub struct QueueStats {
    pub capacity: usize,
    pub pending: usize,
    pub retrying: usize,
    pub running: usize,
    pub active: usize,
    pub quarantined: usize,
    pub pending_removals: usize,
    pub admitted_total: u64,
    pub completed_total: u64,
    pub failed_attempts_total: u64,
    pub exhausted_total: u64,
    pub retired_total: u64,
    /// Counts snapshot deferrals, not distinct sources.
    pub deferred_full_total: u64,
    pub deferred_conflict_total: u64,
    pub renewed_generations_total: u64,
    pub stale_finish_total: u64,
    pub rebuild_deferred_total: u64,
}

#[derive(Clone, Debug, Default)]
pub struct RebuildSummary {
    pub requeued: usize,
    pub deferred: usize,
}

#[derive(Clone, Debug)]
struct Presence {
    missing_since: Option<Instant>,
    // A restarted registry may be empty while an existing source session is
    // still healthy. Only a fresh positive sighting restores absence evidence.
    require_positive_confirmation: bool,
}

impl Presence {
    fn new() -> Self {
        Self {
            missing_since: None,
            require_positive_confirmation: false,
        }
    }

    fn mark_uncertain(&mut self) {
        self.missing_since = None;
        self.require_positive_confirmation = true;
    }

    fn observe(&mut self, seen: bool, authoritative: bool, now: Instant, grace: Duration) -> bool {
        if seen {
            self.missing_since = None;
            self.require_positive_confirmation = false;
            false
        } else if authoritative && !self.require_positive_confirmation {
            let missing = *self.missing_since.get_or_insert(now);
            now.saturating_duration_since(missing) >= grace
        } else {
            false
        }
    }
}

#[derive(Clone, Debug)]
enum WorkState {
    Pending { ready_at: Instant },
    Running { ticket: Ticket, stale: bool },
}

#[derive(Clone, Debug)]
struct Work {
    source: Source,
    generation: u64,
    attempt: u32,
    first_observed_at: Instant,
    enqueued_at: Instant,
    presence: Presence,
    state: WorkState,
}

#[derive(Clone, Debug)]
struct Active {
    ticket: Ticket,
    presence: Presence,
}

#[derive(Clone, Debug)]
struct Quarantine {
    retry_after: Instant,
    presence: Presence,
}

pub struct Admission {
    capacity: usize,
    removal_grace: Duration,
    max_attempts: u32,
    retry_base: Duration,
    quarantine_duration: Duration,
    next_generation: u64,
    work: BTreeMap<SourceKey, Work>,
    order: VecDeque<SourceKey>,
    active: BTreeMap<SourceKey, Active>,
    quarantine: BTreeMap<SourceKey, Quarantine>,
    removals: BTreeSet<SourceKey>,
    totals: QueueStats,
}

impl Admission {
    pub fn new(
        capacity: usize,
        removal_grace: Duration,
        max_attempts: u32,
        retry_base: Duration,
        quarantine_duration: Duration,
        _now: Instant,
    ) -> Self {
        assert!(capacity > 0, "admission capacity must be positive");
        assert!(!removal_grace.is_zero(), "removal grace must be positive");
        assert!(
            max_attempts > 0 && max_attempts <= 100,
            "attempt limit must be 1 to 100"
        );
        assert!(!retry_base.is_zero(), "retry base must be positive");
        assert!(
            quarantine_duration >= Duration::from_secs(60),
            "quarantine must be at least 60 seconds"
        );
        Self {
            capacity,
            removal_grace,
            max_attempts,
            retry_base,
            quarantine_duration,
            next_generation: 1,
            work: BTreeMap::new(),
            order: VecDeque::new(),
            active: BTreeMap::new(),
            quarantine: BTreeMap::new(),
            removals: BTreeSet::new(),
            totals: QueueStats::default(),
        }
    }

    /// A failed discovery cycle invalidates ongoing absence evidence. Retained
    /// identities require their own next positive sighting before an otherwise
    /// complete snapshot can establish disappearance again. In particular, an
    /// empty replacement LDS is not proof that integrated sources disappeared.
    ///
    /// Work carries this protection into Active when its current attempt succeeds.
    /// This does not start a writer, undo an already confirmed removal, extend
    /// retry deadlines or quarantine, or prevent runtime failure invalidation.
    /// A source that never re-registers may remain active while its session stays
    /// healthy. This is deliberate uncertainty, not proof of registry presence.
    /// Call on an actual discovery failure, not merely on a partial snapshot.
    pub fn mark_discovery_uncertain(&mut self) {
        for work in self.work.values_mut() {
            work.presence.mark_uncertain();
        }
        for active in self.active.values_mut() {
            active.presence.mark_uncertain();
        }
        for quarantined in self.quarantine.values_mut() {
            quarantined.presence.mark_uncertain();
        }
    }

    /// Failed discovery calls must not call this method. Partial successful calls
    /// use `authoritative = false` and never advance absence retirement.
    pub fn observe(
        &mut self,
        sources: Vec<Source>,
        authoritative: bool,
        now: Instant,
    ) -> ObservationSummary {
        let mut summary = ObservationSummary::default();
        // Deterministic identity ordering within a single observation. Preserve
        // the exact endpoint for connection, including its original path suffix.
        let mut unique: BTreeMap<SourceKey, Source> = BTreeMap::new();
        for source in sources {
            let key = source.key();
            if key.application_uri.is_empty() || key.canonical_endpoint.is_empty() {
                continue;
            }
            match unique.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(source);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    summary.deduplicated += 1;
                    // Match the previous deterministic address tie-break without
                    // cloning both identity strings in every sort comparison.
                    if source.address < entry.get().address {
                        entry.insert(source);
                    }
                }
            }
        }
        // One ApplicationUri is one lower server. Multiple advertised URLs
        // are aliases. Keep its admitted URL when still visible, otherwise
        // choose a stable URL and let the old identity's grace period run.
        let mut by_application: BTreeMap<String, Vec<(SourceKey, Source)>> = BTreeMap::new();
        for (key, source) in unique {
            by_application
                .entry(key.application_uri.clone())
                .or_default()
                .push((key, source));
        }
        let mut seen = BTreeMap::new();
        for (_, mut aliases) in by_application {
            let preferred = aliases
                .iter()
                .position(|(key, _)| {
                    self.work.contains_key(key)
                        || self.active.contains_key(key)
                        || self.quarantine.contains_key(key)
                        || self.removals.contains(key)
                })
                .unwrap_or(0);
            summary.deduplicated += aliases.len().saturating_sub(1);
            let (key, source) = aliases.swap_remove(preferred);
            seen.insert(key, source);
        }

        let mut retired = Vec::new();
        for (key, work) in &mut self.work {
            let expired = work.presence.observe(
                seen.contains_key(key),
                authoritative,
                now,
                self.removal_grace,
            );
            if expired {
                match &mut work.state {
                    WorkState::Running { stale, .. } => {
                        if !*stale {
                            summary.stale_running += 1;
                        }
                        *stale = true;
                    }
                    WorkState::Pending { .. } => retired.push(key.clone()),
                }
            }
        }
        let pending_retired = !retired.is_empty();
        for key in retired {
            let work = self.work.remove(&key).expect("retired work exists");
            summary.retired.push(RetiredRecord {
                key,
                generation: work.generation,
                attempts: work.attempt,
                first_observed_at: work.first_observed_at,
                enqueued_at: work.enqueued_at,
                ended_at: now,
            });
            self.totals.retired_total += 1;
        }

        if pending_retired {
            // Filter once after the batch, rather than rescanning the FIFO for
            // each expired pending source while holding the scheduler mutex.
            self.order.retain(|key| self.work.contains_key(key));
        }

        let mut removed = Vec::new();
        for (key, active) in &mut self.active {
            if active.presence.observe(
                seen.contains_key(key),
                authoritative,
                now,
                self.removal_grace,
            ) {
                removed.push(key.clone());
            }
        }
        for key in removed {
            self.active.remove(&key);
            self.removals.insert(key);
            summary.removed_active += 1;
            self.totals.retired_total += 1;
        }

        let mut expired_quarantine = BTreeSet::new();
        self.quarantine.retain(|key, entry| {
            let absent = entry.presence.observe(
                seen.contains_key(key),
                authoritative,
                now,
                self.removal_grace,
            );
            let expired = now >= entry.retry_after || absent;
            if expired {
                expired_quarantine.insert(key.clone());
            }
            !expired
        });

        // Releasing quarantine capacity must give fresh healthy candidates a
        // chance before a repeatedly failing source renews its generation.
        let mut candidates: Vec<_> = seen.into_iter().collect();
        candidates.sort_by(|(a, _), (b, _)| {
            expired_quarantine
                .contains(a)
                .cmp(&expired_quarantine.contains(b))
                .then(a.cmp(b))
        });
        // A snapshot may contain many more candidates than the pending budget.
        // Index both conflict dimensions once, then extend the indices as this
        // snapshot admits entries. Scanning every active source per candidate
        // would make a burst quadratic while holding the scheduler mutex.
        // These are temporary indices, not an additional overflow backlog.
        let mut reserved_applications = BTreeSet::new();
        let mut reserved_endpoints = BTreeSet::new();
        for key in self
            .work
            .keys()
            .chain(self.active.keys())
            .chain(self.quarantine.keys())
            .chain(self.removals.iter())
        {
            reserved_applications.insert(key.application_uri.clone());
            reserved_endpoints.insert(key.canonical_endpoint.clone());
        }
        for (key, source) in candidates {
            if self.work.contains_key(&key) || self.active.contains_key(&key) {
                summary.deduplicated += 1;
                continue;
            }
            if self.quarantine.contains_key(&key) {
                summary.quarantined += 1;
                continue;
            }
            if reserved_applications.contains(&key.application_uri)
                || reserved_endpoints.contains(&key.canonical_endpoint)
            {
                summary.deferred_conflict += 1;
                self.totals.deferred_conflict_total += 1;
                continue;
            }
            if self.work.len() + self.quarantine.len() >= self.capacity {
                summary.deferred_full += 1;
                self.totals.deferred_full_total += 1;
                continue;
            }
            let admitted = self.enqueue(source, now);
            reserved_applications.insert(key.application_uri.clone());
            reserved_endpoints.insert(key.canonical_endpoint.clone());
            summary.admitted.push(admitted);
            summary.queued += 1;
            if expired_quarantine.contains(&key) {
                summary.renewed_generations += 1;
                self.totals.renewed_generations_total += 1;
            }
        }
        summary
    }

    fn enqueue(&mut self, source: Source, now: Instant) -> AdmittedRecord {
        let key = source.key();
        let generation = self.next_generation;
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .expect("admission generation exhausted");
        let admitted = AdmittedRecord {
            source: source.clone(),
            key: key.clone(),
            generation,
            first_observed_at: now,
            enqueued_at: now,
        };
        self.work.insert(
            key.clone(),
            Work {
                source,
                generation,
                attempt: 0,
                first_observed_at: now,
                enqueued_at: now,
                presence: Presence::new(),
                state: WorkState::Pending { ready_at: now },
            },
        );
        self.order.push_back(key);
        self.totals.admitted_total += 1;
        admitted
    }

    pub fn take_ready(&mut self, now: Instant) -> Option<Ticket> {
        if !self.removals.is_empty()
            || self
                .work
                .values()
                .any(|work| matches!(work.state, WorkState::Running { .. }))
        {
            return None;
        }
        // Selecting a ready entry must not rotate blocked older entries behind
        // younger work. A retry rejoins the tail only when it is scheduled.
        let ready_position = self.order.iter().position(|key| {
            let work = self.work.get(key).expect("queued work exists");
            matches!(work.state, WorkState::Pending { ready_at } if now >= ready_at)
                && work.presence.missing_since.is_none()
        })?;
        let key = self
            .order
            .remove(ready_position)
            .expect("ready position exists");
        let work = self.work.get_mut(&key).expect("queued work exists");
        work.attempt += 1;
        let ticket = Ticket {
            source: work.source.clone(),
            key,
            generation: work.generation,
            attempt: work.attempt,
            first_observed_at: work.first_observed_at,
            enqueued_at: work.enqueued_at,
            started_at: now,
        };
        work.state = WorkState::Running {
            ticket: ticket.clone(),
            stale: false,
        };
        Some(ticket)
    }

    /// Call only after the integration worker and any rollback have terminated.
    /// A stale/duplicate ticket cannot complete or release another running job.
    pub fn finish(&mut self, ticket: &Ticket, success: bool, now: Instant) -> FinishOutcome {
        let valid = self
            .work
            .get(&ticket.key)
            .map(|work| {
                matches!(&work.state, WorkState::Running { ticket: current, .. }
                if current.generation == ticket.generation && current.attempt == ticket.attempt)
            })
            .unwrap_or(false);
        if !valid {
            self.totals.stale_finish_total += 1;
            return FinishOutcome {
                disposition: FinishDisposition::IgnoredStale,
                ticket: ticket.clone(),
                ended_at: now,
                next_ready_at: None,
                queue: self.stats(),
            };
        }
        let mut work = self
            .work
            .remove(&ticket.key)
            .expect("running work validated");
        let stale = matches!(work.state, WorkState::Running { stale: true, .. });
        let mut next_ready_at = None;
        let disposition = if stale {
            self.removals.insert(ticket.key.clone());
            self.totals.retired_total += 1;
            if !success {
                self.totals.failed_attempts_total += 1;
            }
            FinishDisposition::Retired
        } else if success {
            self.active.insert(
                ticket.key.clone(),
                Active {
                    ticket: ticket.clone(),
                    presence: work.presence,
                },
            );
            self.totals.completed_total += 1;
            FinishDisposition::Completed
        } else {
            self.totals.failed_attempts_total += 1;
            if work.attempt >= self.max_attempts {
                let retry_after = now + self.quarantine_duration;
                self.quarantine.insert(
                    ticket.key.clone(),
                    Quarantine {
                        retry_after,
                        presence: work.presence,
                    },
                );
                self.totals.exhausted_total += 1;
                next_ready_at = Some(retry_after);
                FinishDisposition::Exhausted
            } else {
                let factor = 1u32 << work.attempt.saturating_sub(1).min(20);
                let delay = self
                    .retry_base
                    .checked_mul(factor)
                    .unwrap_or(self.quarantine_duration)
                    .min(self.quarantine_duration);
                let ready_at = now + delay;
                work.enqueued_at = now;
                work.state = WorkState::Pending { ready_at };
                self.work.insert(ticket.key.clone(), work);
                self.order.push_back(ticket.key.clone());
                next_ready_at = Some(ready_at);
                FinishDisposition::RetryScheduled
            }
        };
        FinishOutcome {
            disposition,
            ticket: ticket.clone(),
            ended_at: now,
            next_ready_at,
            queue: self.stats(),
        }
    }

    /// Compare these identities with the runtime lower-server inventory from the
    /// serial owner. Discovery advertisements alone do not establish live sessions.
    pub fn active_source_names(&self) -> Vec<String> {
        self.active
            .keys()
            .map(|key| key.application_uri.clone())
            .collect()
    }

    /// Runtime failure is positive evidence distinct from discovery absence.
    /// Retire only completed active identities and defer shared-state cleanup
    /// through the same removal barrier used for confirmed disappearance.
    pub fn invalidate_active(&mut self, names: &[String]) -> usize {
        let names: BTreeSet<_> = names.iter().collect();
        let invalid: Vec<_> = self
            .active
            .keys()
            .filter(|key| names.contains(&key.application_uri))
            .cloned()
            .collect();
        for key in &invalid {
            self.active.remove(key);
            self.removals.insert(key.clone());
        }
        self.totals.retired_total += invalid.len() as u64;
        invalid.len()
    }

    /// A running writer must finish before the caller can rebuild shared state.
    pub fn take_removals(&mut self) -> Vec<SourceKey> {
        if self
            .work
            .values()
            .any(|work| matches!(work.state, WorkState::Running { .. }))
        {
            return Vec::new();
        }
        std::mem::take(&mut self.removals).into_iter().collect()
    }

    /// The caller has completed a serial full rebuild. Previously active sources
    /// are now unintegrated. Requeue within the same bounded admission budget.
    /// Overflow is explicit and must be rediscovered in later observations.
    pub fn requeue_active_after_rebuild(&mut self, now: Instant) -> RebuildSummary {
        assert!(
            !self
                .work
                .values()
                .any(|work| matches!(work.state, WorkState::Running { .. })),
            "cannot rebuild with a running writer"
        );
        let active = std::mem::take(&mut self.active);
        let mut active: Vec<_> = active.into_values().collect();
        active.sort_by(|a, b| {
            a.ticket
                .first_observed_at
                .cmp(&b.ticket.first_observed_at)
                .then(a.ticket.key.cmp(&b.ticket.key))
        });
        let mut summary = RebuildSummary::default();
        for entry in active {
            if self.work.len() + self.quarantine.len() >= self.capacity {
                summary.deferred += 1;
            } else {
                let key = entry.ticket.key.clone();
                self.enqueue(entry.ticket.source, now);
                self.work
                    .get_mut(&key)
                    .expect("requeued work exists")
                    .presence = entry.presence;
                summary.requeued += 1;
            }
        }
        self.totals.rebuild_deferred_total += summary.deferred as u64;
        summary
    }

    pub fn stats(&self) -> QueueStats {
        let mut stats = self.totals.clone();
        stats.capacity = self.capacity;
        stats.active = self.active.len();
        stats.quarantined = self.quarantine.len();
        stats.pending_removals = self.removals.len();
        for work in self.work.values() {
            match work.state {
                WorkState::Running { .. } => stats.running += 1,
                WorkState::Pending { .. } if work.attempt > 0 => stats.retrying += 1,
                WorkState::Pending { .. } => stats.pending += 1,
            }
        }
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(id: &str) -> Source {
        Source::new(
            format!("urn:test:{id}"),
            format!("opc.tcp://localhost:4840/{id}/"),
        )
    }

    fn scheduler(capacity: usize, max_attempts: u32, now: Instant) -> Admission {
        Admission::new(
            capacity,
            Duration::from_secs(10),
            max_attempts,
            Duration::from_secs(2),
            Duration::from_secs(60),
            now,
        )
    }

    fn complete(scheduler: &mut Admission, now: Instant) -> Ticket {
        let ticket = scheduler.take_ready(now).expect("ready ticket");
        assert_eq!(
            scheduler.finish(&ticket, true, now).disposition,
            FinishDisposition::Completed
        );
        ticket
    }

    #[test]
    fn full_queue_defers_without_hidden_backlog_and_later_snapshot_reconsiders() {
        let now = Instant::now();
        let mut queue = scheduler(1, 2, now);
        let result = queue.observe(vec![source("b"), source("a")], true, now);
        assert_eq!(result.queued, 1);
        assert_eq!(result.deferred_full, 1);
        assert_eq!(result.admitted[0].key, source("a").key());
        complete(&mut queue, now);
        assert!(
            queue.take_ready(now).is_none(),
            "deferred sources are not an overflow queue"
        );
        assert_eq!(
            queue
                .observe(vec![source("a"), source("b")], true, now)
                .queued,
            1
        );
        assert_eq!(queue.take_ready(now).unwrap().key, source("b").key());
    }

    #[test]
    fn repeated_sightings_and_endpoint_aliases_do_not_duplicate_admission() {
        let now = Instant::now();
        let mut queue = scheduler(4, 2, now);
        let original = source("z");
        queue.observe(vec![original.clone()], true, now);
        let ticket = complete(&mut queue, now);
        assert_eq!(
            ticket.source.address, original.address,
            "keep exact connect endpoint"
        );
        let mut without_slash = original.clone();
        without_slash.address.pop();
        let alias = Source::new(original.name.clone(), "opc.tcp://localhost:4840/a/");
        let result = queue.observe(vec![alias, original.clone(), without_slash], true, now);
        assert_eq!(result.queued, 0);
        assert_eq!(queue.stats().active, 1);
        assert_eq!(queue.stats().pending_removals, 0);
        assert!(queue.take_ready(now).is_none());
    }

    #[test]
    fn new_arrivals_are_observed_while_running_but_never_create_second_writer() {
        let now = Instant::now();
        let mut queue = scheduler(3, 2, now);
        queue.observe(vec![source("a")], true, now);
        let running = queue.take_ready(now).unwrap();
        let later = now + Duration::from_secs(1);
        let result = queue.observe(vec![source("a"), source("b"), source("c")], true, later);
        assert_eq!(result.queued, 2);
        assert_eq!(queue.stats().running, 1);
        assert_eq!(queue.stats().pending, 2);
        assert!(queue.take_ready(later).is_none());
        queue.finish(&running, true, later);
        let next = queue.take_ready(later).unwrap();
        assert_eq!(next.key, source("b").key());
        assert_eq!(next.enqueued_at, later);
        assert_eq!(next.started_at, later);
    }

    #[test]
    fn fifo_uses_snapshot_time_then_sorted_identity_not_global_name_sort() {
        let now = Instant::now();
        let mut queue = scheduler(4, 2, now);
        queue.observe(vec![source("z")], true, now);
        queue.observe(
            vec![source("z"), source("b"), source("a")],
            true,
            now + Duration::from_secs(1),
        );
        assert_eq!(
            complete(&mut queue, now + Duration::from_secs(1)).key,
            source("z").key()
        );
        assert_eq!(
            complete(&mut queue, now + Duration::from_secs(1)).key,
            source("a").key()
        );
        assert_eq!(
            complete(&mut queue, now + Duration::from_secs(1)).key,
            source("b").key()
        );
    }

    #[test]
    fn partial_snapshots_never_remove_and_positive_sightings_clear_absence() {
        let now = Instant::now();
        let mut queue = scheduler(2, 2, now);
        queue.observe(vec![source("a")], true, now);
        complete(&mut queue, now);
        queue.observe(vec![], true, now + Duration::from_secs(1));
        queue.observe(vec![], false, now + Duration::from_secs(20));
        assert!(queue.take_removals().is_empty());
        queue.observe(vec![source("a")], false, now + Duration::from_secs(21));
        queue.observe(vec![], true, now + Duration::from_secs(22));
        assert!(queue.take_removals().is_empty());
        queue.observe(vec![], true, now + Duration::from_secs(32));
        assert_eq!(queue.take_removals(), vec![source("a").key()]);
    }

    #[test]
    fn disappeared_pending_source_retires_after_time_grace_without_state_removal() {
        let now = Instant::now();
        let mut queue = scheduler(1, 2, now);
        queue.observe(vec![source("a")], true, now);
        queue.observe(vec![], true, now + Duration::from_secs(1));
        assert!(queue.take_ready(now + Duration::from_secs(2)).is_none());
        let result = queue.observe(vec![], true, now + Duration::from_secs(11));
        assert_eq!(result.retired.len(), 1);
        assert_eq!(result.retired[0].attempts, 0);
        assert_eq!(queue.stats().pending, 0);
        assert!(queue.take_removals().is_empty());
    }

    #[test]
    fn stale_running_identity_is_retained_until_terminal_cleanup() {
        let now = Instant::now();
        let mut queue = scheduler(3, 2, now);
        let old = source("a");
        let replacement = Source::new("urn:test:new", old.address.clone());
        queue.observe(vec![old.clone()], true, now);
        let running = queue.take_ready(now).unwrap();
        queue.observe(
            vec![replacement.clone()],
            true,
            now + Duration::from_secs(1),
        );
        let result = queue.observe(
            vec![replacement.clone()],
            true,
            now + Duration::from_secs(11),
        );
        assert_eq!(result.stale_running, 1);
        assert_eq!(result.deferred_conflict, 1);
        assert!(queue.take_removals().is_empty());
        assert!(queue.take_ready(now + Duration::from_secs(11)).is_none());
        let ended = now + Duration::from_secs(12);
        let outcome = queue.finish(&running, true, ended);
        assert_eq!(outcome.disposition, FinishDisposition::Retired);
        assert_eq!(outcome.ended_at, ended);
        assert_eq!(queue.stats().active, 0);
        assert_eq!(queue.take_removals(), vec![old.key()]);
        assert_eq!(queue.observe(vec![replacement], true, ended).queued, 1);
        let next = queue.take_ready(ended).unwrap();
        assert_ne!(running.generation, next.generation);
        assert_eq!(
            queue.finish(&running, true, ended).disposition,
            FinishDisposition::IgnoredStale
        );
        assert_eq!(queue.stats().running, 1);
    }

    #[test]
    fn active_endpoint_replacement_waits_for_grace_and_removal_barrier() {
        let now = Instant::now();
        let mut queue = scheduler(3, 2, now);
        let old = source("a");
        let replacement = Source::new(old.name.clone(), "opc.tcp://localhost:5000/a/");
        queue.observe(vec![old.clone()], true, now);
        complete(&mut queue, now);
        queue.observe(
            vec![replacement.clone()],
            true,
            now + Duration::from_secs(1),
        );
        assert_eq!(queue.stats().active, 1);
        queue.observe(
            vec![replacement.clone()],
            true,
            now + Duration::from_secs(11),
        );
        assert_eq!(queue.stats().active, 0);
        assert!(queue.take_ready(now + Duration::from_secs(11)).is_none());
        assert_eq!(queue.take_removals(), vec![old.key()]);
        queue.requeue_active_after_rebuild(now + Duration::from_secs(12));
        assert_eq!(
            queue
                .observe(
                    vec![replacement.clone()],
                    true,
                    now + Duration::from_secs(12)
                )
                .queued,
            1
        );
        assert_eq!(
            queue
                .take_ready(now + Duration::from_secs(12))
                .unwrap()
                .source
                .address,
            replacement.address
        );
    }

    #[test]
    fn retry_backoff_allows_other_ready_sources_and_exhaustion_is_quarantined() {
        let now = Instant::now();
        let mut queue = scheduler(3, 2, now);
        queue.observe(vec![source("a"), source("b")], true, now);
        let first = queue.take_ready(now).unwrap();
        assert_eq!(
            queue.finish(&first, false, now).disposition,
            FinishDisposition::RetryScheduled
        );
        let second = queue.take_ready(now).unwrap();
        assert_eq!(second.key, source("b").key());
        queue.finish(&second, true, now);
        assert!(queue.take_ready(now + Duration::from_secs(1)).is_none());
        let retry = queue.take_ready(now + Duration::from_secs(2)).unwrap();
        assert_eq!(retry.generation, first.generation);
        assert_eq!(retry.attempt, 2);
        assert_eq!(
            queue.finish(&first, true, now).disposition,
            FinishDisposition::IgnoredStale
        );
        let exhausted = queue.finish(&retry, false, now + Duration::from_secs(2));
        assert_eq!(exhausted.disposition, FinishDisposition::Exhausted);
        assert_eq!(queue.stats().quarantined, 1);
        assert_eq!(
            queue.stats().pending + queue.stats().retrying + queue.stats().running,
            0
        );
        let result = queue.observe(
            vec![source("a"), source("b")],
            true,
            now + Duration::from_secs(3),
        );
        assert_eq!(result.quarantined, 1);
        assert!(queue.take_ready(now + Duration::from_secs(3)).is_none());
    }

    #[test]
    fn quarantine_is_bounded_and_cooldown_creates_explicit_new_generation() {
        let now = Instant::now();
        let mut queue = scheduler(1, 1, now);
        queue.observe(vec![source("a")], true, now);
        let first = queue.take_ready(now).unwrap();
        queue.finish(&first, false, now);
        let result = queue.observe(
            vec![source("a"), source("b")],
            true,
            now + Duration::from_secs(1),
        );
        assert_eq!(result.quarantined, 1);
        assert_eq!(result.deferred_full, 1);
        assert_eq!(queue.stats().quarantined + queue.stats().pending, 1);
        let later = now + Duration::from_secs(60);
        let renewed = queue.observe(vec![source("a")], true, later);
        assert_eq!(renewed.renewed_generations, 1);
        let next = queue.take_ready(later).unwrap();
        assert_ne!(next.generation, first.generation);
        assert_eq!(next.attempt, 1);
        assert_eq!(
            queue.finish(&first, true, later).disposition,
            FinishDisposition::IgnoredStale
        );
        assert_eq!(queue.stats().running, 1);
    }

    #[test]
    fn confirmed_quarantine_absence_releases_capacity_before_cooldown() {
        let now = Instant::now();
        let mut queue = scheduler(1, 1, now);
        queue.observe(vec![source("a")], true, now);
        let first = queue.take_ready(now).unwrap();
        queue.finish(&first, false, now);
        queue.observe(vec![source("b")], true, now + Duration::from_secs(1));
        let admitted = queue.observe(vec![source("b")], true, now + Duration::from_secs(11));
        assert_eq!(admitted.queued, 1);
        assert_eq!(queue.stats().quarantined, 0);
        assert_eq!(
            queue.take_ready(now + Duration::from_secs(11)).unwrap().key,
            source("b").key()
        );
    }

    #[test]
    fn rebuilding_more_active_sources_than_capacity_defers_without_hidden_queue() {
        let now = Instant::now();
        let mut queue = scheduler(1, 2, now);
        for id in ["a", "b", "c"] {
            queue.observe(vec![source(id)], false, now);
            complete(&mut queue, now);
        }
        assert_eq!(queue.stats().active, 3);
        let rebuilt = queue.requeue_active_after_rebuild(now);
        assert_eq!(rebuilt.requeued, 1);
        assert_eq!(rebuilt.deferred, 2);
        assert_eq!(queue.stats().active, 0);
        assert_eq!(queue.stats().pending, 1);
        complete(&mut queue, now);
        assert!(queue.take_ready(now).is_none());
        assert_eq!(
            queue
                .observe(vec![source("a"), source("b"), source("c")], true, now)
                .queued,
            1
        );
    }

    #[test]
    fn removal_of_other_active_source_waits_for_current_writer() {
        let now = Instant::now();
        let mut queue = scheduler(2, 2, now);
        queue.observe(vec![source("a"), source("b")], true, now);
        complete(&mut queue, now);
        let running = queue.take_ready(now).unwrap();
        queue.observe(vec![source("b")], true, now + Duration::from_secs(1));
        queue.observe(vec![source("b")], true, now + Duration::from_secs(11));
        assert_eq!(queue.stats().pending_removals, 1);
        assert!(queue.take_removals().is_empty());
        queue.finish(&running, true, now + Duration::from_secs(12));
        assert_eq!(queue.take_removals(), vec![source("a").key()]);
    }
    #[test]
    fn released_quarantine_capacity_prefers_fresh_candidate_before_failed_renewal() {
        let now = Instant::now();
        let mut queue = scheduler(1, 1, now);
        queue.observe(vec![source("a")], true, now);
        let failed = queue.take_ready(now).unwrap();
        queue.finish(&failed, false, now);
        let result = queue.observe(
            vec![source("a"), source("z")],
            true,
            now + Duration::from_secs(60),
        );
        assert_eq!(result.queued, 1);
        assert_eq!(result.deferred_full, 1);
        assert_eq!(
            queue.take_ready(now + Duration::from_secs(60)).unwrap().key,
            source("z").key()
        );
    }
    #[test]
    fn observer_thread_admits_and_deduplicates_while_single_writer_is_busy() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{mpsc, Arc, Mutex};
        use std::thread;

        let now = Instant::now();
        let mut initial = scheduler(3, 2, now);
        initial.observe(vec![source("a")], true, now);
        let queue = Arc::new(Mutex::new(initial));
        let writers = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (continue_tx, continue_rx) = mpsc::sync_channel(1);
        let writer_queue = queue.clone();
        let writer_count = writers.clone();
        let writer = thread::spawn(move || {
            let first = writer_queue.lock().unwrap().take_ready(now).unwrap();
            assert_eq!(writer_count.fetch_add(1, Ordering::SeqCst), 0);
            started_tx.send(()).unwrap();
            continue_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            assert_eq!(writer_count.fetch_sub(1, Ordering::SeqCst), 1);
            writer_queue.lock().unwrap().finish(&first, true, now);
            let mut completed = vec![first.key];
            loop {
                let ticket = writer_queue.lock().unwrap().take_ready(now);
                let Some(ticket) = ticket else {
                    break;
                };
                assert_eq!(writer_count.fetch_add(1, Ordering::SeqCst), 0);
                assert_eq!(writer_count.fetch_sub(1, Ordering::SeqCst), 1);
                writer_queue.lock().unwrap().finish(&ticket, true, now);
                completed.push(ticket.key);
            }
            completed
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let observer_queue = queue.clone();
        let observer_writers = writers.clone();
        let observer = thread::spawn(move || {
            for scan in 0..20 {
                let mut state = observer_queue.lock().unwrap();
                let observation =
                    state.observe(vec![source("c"), source("a"), source("b")], true, now);
                assert_eq!(observation.queued, if scan == 0 { 2 } else { 0 });
                assert_eq!(state.stats().running, 1);
                assert_eq!(state.stats().pending, 2);
                assert_eq!(observer_writers.load(Ordering::SeqCst), 1);
                assert!(
                    state.take_ready(now).is_none(),
                    "concurrent admission cannot issue another writer ticket"
                );
            }
        });
        observer.join().unwrap();
        continue_tx.send(()).unwrap();
        assert_eq!(
            writer.join().unwrap(),
            vec![source("a").key(), source("b").key(), source("c").key()]
        );
        let state = queue.lock().unwrap();
        assert_eq!(state.stats().active, 3);
        assert_eq!(state.stats().admitted_total, 3);
        assert_eq!(state.stats().running, 0);
        assert_eq!(writers.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn runtime_invalidation_only_retires_completed_active_sources() {
        let now = Instant::now();
        let mut queue = scheduler(3, 2, now);
        queue.observe(vec![source("a"), source("b"), source("c")], true, now);
        complete(&mut queue, now);
        let running = queue.take_ready(now).unwrap();
        assert_eq!(queue.active_source_names(), vec![source("a").name]);
        assert_eq!(
            queue.invalidate_active(&[
                source("a").name,
                source("b").name,
                source("c").name,
                "urn:unknown".to_string()
            ]),
            1
        );
        assert!(queue.active_source_names().is_empty());
        assert_eq!(queue.stats().running, 1);
        assert_eq!(queue.stats().pending, 1);
        assert!(queue.take_removals().is_empty());
        queue.finish(&running, true, now);
        assert_eq!(queue.take_removals(), vec![source("a").key()]);
        assert_eq!(queue.active_source_names(), vec![source("b").name]);
    }

    #[test]
    fn failed_runtime_source_still_advertised_can_be_admitted_after_cleanup() {
        let now = Instant::now();
        let mut queue = scheduler(1, 2, now);
        queue.observe(vec![source("a")], true, now);
        let old = complete(&mut queue, now);
        assert_eq!(queue.invalidate_active(&[source("a").name]), 1);
        assert_eq!(
            queue
                .observe(vec![source("a")], true, now)
                .deferred_conflict,
            1
        );
        assert!(queue.take_ready(now).is_none());
        assert_eq!(queue.take_removals(), vec![source("a").key()]);
        queue.requeue_active_after_rebuild(now);
        assert_eq!(queue.observe(vec![source("a")], true, now).queued, 1);
        let new = queue.take_ready(now).unwrap();
        assert_ne!(old.generation, new.generation);
        assert_eq!(
            queue.finish(&old, true, now).disposition,
            FinishDisposition::IgnoredStale
        );
        assert_eq!(queue.stats().running, 1);
    }
    #[test]
    fn selecting_ready_work_preserves_blocked_entries_in_admission_order() {
        let now = Instant::now();
        let mut queue = scheduler(4, 3, now);
        queue.observe(vec![source("a"), source("b")], true, now);
        let a = queue.take_ready(now).unwrap();
        queue.finish(&a, false, now);
        let b = queue.take_ready(now).unwrap();
        queue.finish(&b, false, now);
        queue.observe(
            vec![source("a"), source("b"), source("c"), source("d")],
            true,
            now,
        );
        // A and B are in retry backoff. D is temporarily absent, while C is
        // ready. Selecting C must retain the original blocked order A, B, D.
        queue.observe(
            vec![source("a"), source("b"), source("c")],
            true,
            now + Duration::from_millis(500),
        );
        let c = queue.take_ready(now + Duration::from_secs(1)).unwrap();
        assert_eq!(c.key, source("c").key());
        queue.finish(&c, true, now + Duration::from_secs(1));
        let later = now + Duration::from_secs(2);
        queue.observe(
            vec![source("a"), source("b"), source("c"), source("d")],
            true,
            later,
        );
        assert_eq!(complete(&mut queue, later).key, source("a").key());
        assert_eq!(complete(&mut queue, later).key, source("b").key());
        assert_eq!(complete(&mut queue, later).key, source("d").key());
    }

    #[test]
    fn conflict_indices_cover_all_reserved_states_and_new_admissions() {
        let now = Instant::now();
        let mut queue = scheduler(8, 1, now);
        let originals = vec![source("a"), source("b"), source("c"), source("d")];
        queue.observe(originals.clone(), true, now);
        complete(&mut queue, now); // A remains active.
        let failed = queue.take_ready(now).unwrap();
        queue.finish(&failed, false, now); // B remains quarantined.
        let running = queue.take_ready(now).unwrap(); // C remains running.
                                                      // D remains pending. All four endpoints must remain reserved.
        let mut snapshot = originals.clone();
        for original in &originals {
            snapshot.push(Source::new(
                format!("urn:conflict:{}", original.name),
                original.address.clone(),
            ));
        }
        let observation = queue.observe(snapshot.clone(), true, now);
        assert_eq!(observation.queued, 0);
        assert_eq!(observation.deferred_conflict, 4);
        assert_eq!(queue.stats().running, 1);
        assert_eq!(queue.stats().pending, 1);
        assert_eq!(queue.stats().quarantined, 1);
        assert_eq!(queue.stats().active, 1);
        assert_eq!(queue.invalidate_active(&[source("a").name]), 1);
        let observation = queue.observe(snapshot, true, now);
        assert_eq!(observation.queued, 0);
        assert_eq!(observation.deferred_conflict, 5);
        assert_eq!(queue.stats().pending_removals, 1);
        assert!(queue.take_removals().is_empty());
        queue.finish(&running, true, now);
        assert_eq!(queue.take_removals(), vec![source("a").key()]);

        let mut fresh = scheduler(8, 1, now);
        let first = source("a");
        let second = Source::new("urn:test:z", first.address.clone());
        let observation = fresh.observe(vec![second, first.clone()], true, now);
        assert_eq!(observation.queued, 1);
        assert_eq!(observation.deferred_conflict, 1);
        assert_eq!(fresh.take_ready(now).unwrap().key, first.key());
    }

    #[test]
    fn duplicate_endpoint_spelling_is_deterministic_without_sort_key_cloning() {
        let now = Instant::now();
        let with_slash = source("a");
        let mut without_slash = with_slash.clone();
        without_slash.address.pop();
        for snapshot in [
            vec![with_slash.clone(), without_slash.clone()],
            vec![without_slash.clone(), with_slash.clone()],
        ] {
            let mut queue = scheduler(2, 1, now);
            let result = queue.observe(snapshot, true, now);
            assert_eq!(result.deduplicated, 1);
            assert_eq!(result.admitted.len(), 1);
            assert_eq!(result.admitted[0].source.address, without_slash.address);
        }
    }

    #[test]
    fn repeated_large_burst_drains_once_with_bounded_pending_state() {
        let now = Instant::now();
        let mut queue = scheduler(32, 2, now);
        let sources: Vec<_> = (0..1024).map(|i| source(&format!("{i:04}"))).collect();
        let mut completed = BTreeSet::new();
        for batch in 0..32 {
            let result = queue.observe(sources.clone(), true, now);
            assert_eq!(result.queued, 32);
            assert_eq!(result.deferred_full, 1024 - (batch + 1) * 32);
            assert_eq!(queue.stats().pending, 32);
            assert_eq!(queue.order.len(), 32);
            for _ in 0..32 {
                let ticket = queue.take_ready(now).unwrap();
                assert!(queue.take_ready(now).is_none());
                assert!(
                    completed.insert(ticket.key.clone()),
                    "duplicate writer ticket"
                );
                queue.finish(&ticket, true, now);
            }
            assert_eq!(queue.work.len(), 0);
            assert!(queue.order.is_empty());
        }
        let repeated = queue.observe(sources, true, now);
        assert_eq!(repeated.queued, 0);
        assert_eq!(repeated.deduplicated, 1024);
        assert_eq!(queue.stats().active, 1024);
        assert_eq!(queue.stats().admitted_total, 1024);
        assert_eq!(queue.stats().completed_total, 1024);
        assert!(queue.take_ready(now).is_none());
    }

    #[test]
    fn registry_outage_does_not_retire_active_or_inflight_success_on_empty_snapshots() {
        let now = Instant::now();
        let mut queue = scheduler(2, 2, now);
        queue.observe(vec![source("a"), source("b")], true, now);
        complete(&mut queue, now);
        let running = queue.take_ready(now).unwrap();
        // A previously started absence timer must not survive the outage.
        queue.observe(vec![], true, now + Duration::from_secs(1));
        queue.mark_discovery_uncertain();
        assert_eq!(
            queue
                .finish(&running, true, now + Duration::from_secs(2))
                .disposition,
            FinishDisposition::Completed
        );
        for seconds in [20, 100, 1000] {
            let result = queue.observe(vec![], true, now + Duration::from_secs(seconds));
            assert_eq!(result.removed_active, 0);
            assert_eq!(queue.stats().active, 2);
            assert!(queue.take_removals().is_empty());
        }
        // Only A has re-confirmed its registry presence. Its next actual absence
        // has a fresh grace window, while B still has no reliable negative evidence.
        queue.observe(vec![source("a")], true, now + Duration::from_secs(1001));
        queue.observe(vec![], true, now + Duration::from_secs(1002));
        assert!(queue.take_removals().is_empty());
        queue.observe(vec![], true, now + Duration::from_secs(1012));
        assert_eq!(queue.take_removals(), vec![source("a").key()]);
        assert_eq!(queue.active_source_names(), vec![source("b").name]);
    }

    #[test]
    fn runtime_failure_still_retires_a_registry_protected_active_source() {
        let now = Instant::now();
        let mut queue = scheduler(1, 2, now);
        queue.observe(vec![source("a")], true, now);
        complete(&mut queue, now);
        queue.mark_discovery_uncertain();
        queue.observe(vec![], true, now + Duration::from_secs(100));
        assert_eq!(queue.stats().active, 1);
        assert_eq!(queue.invalidate_active(&[source("a").name]), 1);
        assert_eq!(queue.take_removals(), vec![source("a").key()]);
        assert_eq!(queue.stats().active, 0);
    }

    #[test]
    fn registry_uncertainty_does_not_extend_attempts_backoff_or_quarantine() {
        let now = Instant::now();
        let mut queue = scheduler(1, 2, now);
        queue.observe(vec![source("a")], true, now);
        queue.mark_discovery_uncertain();
        let first = queue.take_ready(now).unwrap();
        assert!(queue.take_ready(now).is_none());
        let failure = queue.finish(&first, false, now);
        assert_eq!(failure.disposition, FinishDisposition::RetryScheduled);
        queue.mark_discovery_uncertain();
        assert!(queue.take_ready(now + Duration::from_secs(1)).is_none());
        let second = queue.take_ready(now + Duration::from_secs(2)).unwrap();
        assert_eq!(second.attempt, 2);
        assert_eq!(second.generation, first.generation);
        let exhausted = queue.finish(&second, false, now + Duration::from_secs(2));
        assert_eq!(exhausted.disposition, FinishDisposition::Exhausted);
        queue.mark_discovery_uncertain();
        queue.observe(vec![], true, now + Duration::from_secs(3));
        queue.observe(vec![], true, now + Duration::from_secs(13));
        assert_eq!(queue.stats().quarantined, 1);
        queue.observe(vec![], true, now + Duration::from_secs(62));
        assert_eq!(queue.stats().quarantined, 0);
        assert_eq!(queue.stats().failed_attempts_total, 2);
    }

    #[test]
    fn uncertainty_does_not_undo_already_confirmed_running_retirement() {
        let now = Instant::now();
        let mut queue = scheduler(1, 2, now);
        queue.observe(vec![source("a")], true, now);
        let running = queue.take_ready(now).unwrap();
        queue.observe(vec![], true, now + Duration::from_secs(1));
        queue.observe(vec![], true, now + Duration::from_secs(11));
        queue.mark_discovery_uncertain();
        assert_eq!(
            queue
                .finish(&running, true, now + Duration::from_secs(12))
                .disposition,
            FinishDisposition::Retired
        );
        assert_eq!(queue.take_removals(), vec![source("a").key()]);
    }

    #[test]
    fn mass_pending_retirement_filters_fifo_once_and_preserves_live_order() {
        let now = Instant::now();
        let mut queue = scheduler(1024, 2, now);
        let sources: Vec<_> = (0..1024).map(|i| source(&format!("{i:04}"))).collect();
        queue.observe(sources.clone(), true, now);
        let survivors: Vec<_> = sources.into_iter().step_by(100).collect();
        queue.observe(survivors.clone(), true, now + Duration::from_secs(1));
        let result = queue.observe(survivors.clone(), true, now + Duration::from_secs(11));
        assert_eq!(result.retired.len(), 1024 - survivors.len());
        assert_eq!(queue.order.len(), survivors.len());
        assert_eq!(queue.stats().pending, survivors.len());
        for expected in survivors {
            assert_eq!(
                complete(&mut queue, now + Duration::from_secs(11)).key,
                expected.key()
            );
        }
        assert!(queue.order.is_empty());
        assert!(queue.take_ready(now + Duration::from_secs(11)).is_none());
    }
}
