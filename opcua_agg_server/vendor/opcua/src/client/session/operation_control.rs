// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: MPL-2.0

//! Cooperative cancellation for a bounded group of OPC UA operations.

use std::{
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use crate::types::StatusCode;

#[derive(Debug)]
struct OperationState {
    deadline: Instant,
    // 0 = armed, 1 = successfully disarmed, 2 = cancelled/expired.
    state: AtomicU8,
}

/// A monotonic deadline shared by discovery, connection and onboarding calls.
///
/// Call `disarm` after successful onboarding so an established session and its
/// subscriptions may outlive the onboarding budget. Explicit cancellation remains
/// effective after disarming. This is cooperative: OS name resolution, filesystem
/// access and application callbacks require an outer supervisor if they can stall.
#[derive(Clone, Debug)]
pub struct SessionOperationControl(Arc<OperationState>);

impl SessionOperationControl {
    pub fn new(timeout: Duration) -> Self {
        Self(Arc::new(OperationState {
            deadline: Instant::now() + timeout,
            state: AtomicU8::new(0),
        }))
    }

    pub fn cancel(&self) {
        self.0.state.store(2, Ordering::Release);
    }

    pub fn disarm(&self) {
        // A late success cannot revive an expired/cancelled onboarding operation.
        if self.check().is_ok() {
            let _ = self
                .0
                .state
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire);
        }
    }

    /// Remaining budget, or `None` when the deadline has been disarmed.
    /// Explicit cancellation always returns a zero budget.
    pub fn remaining(&self) -> Option<Duration> {
        match self.0.state.load(Ordering::Acquire) {
            2 => Some(Duration::ZERO),
            1 => None,
            _ => {
                let remaining = self.0.deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    // Do not expire a control another thread has just disarmed.
                    match self
                        .0
                        .state
                        .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
                    {
                        Err(1) => return None,
                        _ => {}
                    }
                }
                Some(remaining)
            }
        }
    }

    pub fn is_cancelled_or_expired(&self) -> bool {
        self.remaining()
            .map_or(false, |remaining| remaining.is_zero())
    }

    pub fn check(&self) -> Result<(), StatusCode> {
        if self.is_cancelled_or_expired() {
            Err(StatusCode::BadTimeout)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_is_shared_and_survives_disarm() {
        let control = SessionOperationControl::new(Duration::from_secs(60));
        let clone = control.clone();
        control.disarm();
        assert_eq!(clone.remaining(), None);
        clone.cancel();
        control.disarm();
        assert_eq!(control.check(), Err(StatusCode::BadTimeout));
    }

    #[test]
    fn expired_budget_is_rejected() {
        let control = SessionOperationControl::new(Duration::ZERO);
        assert!(control.is_cancelled_or_expired());
        assert_eq!(control.check(), Err(StatusCode::BadTimeout));
        control.disarm();
        assert_eq!(control.check(), Err(StatusCode::BadTimeout));
    }

    #[test]
    fn disarmed_success_is_not_limited_by_onboarding_deadline() {
        let control = SessionOperationControl::new(Duration::from_millis(50));
        control.disarm();
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(control.check(), Ok(()));
    }
}
