// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

//! Back off failed discovery observations without removing admitted identities.
use std::time::Duration;

pub(crate) struct DiscoveryBackoff {
    interval: Duration,
    maximum: Duration,
    next_failure: Duration,
}

impl DiscoveryBackoff {
    pub(crate) fn new(interval: Duration, maximum: Duration) -> Self {
        Self {
            interval,
            maximum: maximum.max(interval),
            next_failure: interval,
        }
    }

    pub(crate) fn after_observation(&mut self, success: bool) -> Duration {
        if success {
            self.next_failure = self.interval;
            return self.interval;
        }
        let delay = self.next_failure;
        self.next_failure = delay.saturating_mul(2).min(self.maximum);
        delay
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn repeated_outage_is_bounded_and_success_resets_backoff() {
        let mut policy = DiscoveryBackoff::new(Duration::from_secs(1), Duration::from_secs(30));
        let delays: Vec<_> = (0..8)
            .map(|_| policy.after_observation(false).as_secs())
            .collect();
        assert_eq!(delays, vec![1, 2, 4, 8, 16, 30, 30, 30]);
        assert_eq!(policy.after_observation(true), Duration::from_secs(1));
        assert_eq!(policy.after_observation(false), Duration::from_secs(1));
    }
    #[test]
    fn configured_interval_is_never_shortened_by_retry_cap() {
        let mut policy = DiscoveryBackoff::new(Duration::from_secs(90), Duration::from_secs(30));
        for _ in 0..1000 {
            assert_eq!(policy.after_observation(false), Duration::from_secs(90));
        }
    }
}
