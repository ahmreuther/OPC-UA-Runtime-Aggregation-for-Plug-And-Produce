// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

//! Serialized onboarding with a lock-independent deadline and rollback acknowledgement.
//!
//! A worker that does not acknowledge cancellation is never replaced by another
//! writer in this process. The application must terminate on `UnsafeToContinue`.
use opcua::client::session::SessionOperationControl;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Completed,
    Failed(String),
    TimedOut,
    UnsafeToContinue(String),
}

pub struct SerialOnboarding {
    timeout: Duration,
    cleanup_grace: Duration,
    poisoned: bool,
}

impl SerialOnboarding {
    pub fn new(timeout: Duration, cleanup_grace: Duration) -> Self {
        assert!(!timeout.is_zero() && !cleanup_grace.is_zero());
        Self {
            timeout,
            cleanup_grace,
            poisoned: false,
        }
    }

    /// `rollback` runs in the same worker, after `work` stops. A successful work
    /// result is provisional until the supervisor accepts it before the deadline.
    pub fn run<W, R>(&mut self, work: W, rollback: R) -> Outcome
    where
        W: FnOnce(SessionOperationControl) -> Result<(), String> + Send + 'static,
        R: FnOnce() -> Result<(), String> + Send + 'static,
    {
        self.run_with_commit(work, rollback, || Ok(()))
    }

    /// Finalize accepted work in its worker before acknowledging completion.
    /// A late result is rolled back and never committed. Commit failures poison
    /// the queue because the accepted state can no longer be safely discarded.
    pub fn run_with_commit<W, R, C>(&mut self, work: W, rollback: R, commit: C) -> Outcome
    where
        W: FnOnce(SessionOperationControl) -> Result<(), String> + Send + 'static,
        R: FnOnce() -> Result<(), String> + Send + 'static,
        C: FnOnce() -> Result<(), String> + Send + 'static,
    {
        if self.poisoned {
            return Outcome::UnsafeToContinue("previous worker did not stop safely".into());
        }
        let deadline = Instant::now() + self.timeout;
        let control = SessionOperationControl::new(self.timeout);
        let worker_control = control.clone();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (decision_tx, decision_rx) = mpsc::sync_channel(1);
        let (stopped_tx, stopped_rx) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| work(worker_control.clone())))
                .unwrap_or_else(|_| Err("onboarding worker panicked".into()));
            let _ = ready_tx.send(result);
            let accepted = decision_rx.recv().unwrap_or(false);
            let finalization = if accepted {
                catch_unwind(AssertUnwindSafe(commit))
                    .unwrap_or_else(|_| Err("commit panicked".into()))
                    .map_err(|error| format!("commit finalization failed: {error}"))
            } else {
                worker_control.cancel();
                catch_unwind(AssertUnwindSafe(rollback))
                    .unwrap_or_else(|_| Err("rollback panicked".into()))
                    .map_err(|error| format!("rollback failed: {error}"))
            };
            let _ = stopped_tx.send(finalization);
        });

        let ready = ready_rx.recv_timeout(deadline.saturating_duration_since(Instant::now()));
        let outcome = match ready {
            Ok(Ok(())) if Instant::now() < deadline => Outcome::Completed,
            Ok(Err(error)) if Instant::now() < deadline => Outcome::Failed(error),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Outcome::Failed("worker disconnected before reporting its result".into())
            }
            _ => Outcome::TimedOut,
        };
        if outcome != Outcome::Completed {
            control.cancel();
        }
        let _ = decision_tx.send(outcome == Outcome::Completed);
        let cleanup_deadline = Instant::now() + self.cleanup_grace;
        let stopped =
            stopped_rx.recv_timeout(cleanup_deadline.saturating_duration_since(Instant::now()));
        // Acknowledge only after commit or rollback and retain the thread until
        // it has exited. Never use an unbounded join as a timeout mechanism.
        while !worker.is_finished() && Instant::now() < cleanup_deadline {
            thread::sleep(Duration::from_millis(1));
        }
        if !worker.is_finished() {
            self.poisoned = true;
            return Outcome::UnsafeToContinue(
                "worker/commit/rollback exceeded finalization grace".into(),
            );
        }
        if worker.join().is_err() {
            self.poisoned = true;
            return Outcome::UnsafeToContinue(
                "worker exited without a safe acknowledgement".into(),
            );
        }
        match stopped {
            Ok(Ok(())) => outcome,
            Ok(Err(error)) => {
                self.poisoned = true;
                Outcome::UnsafeToContinue(error)
            }
            Err(_) => {
                self.poisoned = true;
                Outcome::UnsafeToContinue("missing finalization/exit acknowledgement".into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    };

    #[test]
    fn successful_job_is_committed_without_rollback() {
        let rolled_back = Arc::new(AtomicBool::new(false));
        let flag = rolled_back.clone();
        let mut supervisor = SerialOnboarding::new(Duration::from_secs(1), Duration::from_secs(1));
        assert_eq!(
            supervisor.run(
                |_| Ok(()),
                move || {
                    flag.store(true, Ordering::SeqCst);
                    Ok(())
                }
            ),
            Outcome::Completed
        );
        assert!(!rolled_back.load(Ordering::SeqCst));
    }

    #[test]
    fn accepted_commit_finishes_in_worker_before_completed_is_observable() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let work_events = events.clone();
        let commit_events = events.clone();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let supervisor_thread = thread::spawn(move || {
            let mut supervisor =
                SerialOnboarding::new(Duration::from_secs(5), Duration::from_secs(5));
            let result = supervisor.run_with_commit(
                move |_| {
                    work_events
                        .lock()
                        .unwrap()
                        .push(("work", thread::current().id()));
                    Ok(())
                },
                || panic!("accepted work must not roll back"),
                move || {
                    commit_events
                        .lock()
                        .unwrap()
                        .push(("commit", thread::current().id()));
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
                    commit_events
                        .lock()
                        .unwrap()
                        .push(("committed", thread::current().id()));
                    Ok(())
                },
            );
            done_tx.send(result).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        release_tx.send(()).unwrap();
        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(3)).unwrap(),
            Outcome::Completed
        );
        supervisor_thread.join().unwrap();
        let events = events.lock().unwrap();
        assert_eq!(
            events.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
            vec!["work", "commit", "committed"]
        );
        assert!(events.iter().all(|(_, id)| *id == events[0].1));
    }

    fn assert_queue_rejects_all_following_callbacks(mut supervisor: SerialOnboarding) {
        let invoked = Arc::new(AtomicUsize::new(0));
        let work_count = invoked.clone();
        let rollback_count = invoked.clone();
        let commit_count = invoked.clone();
        assert!(matches!(
            supervisor.run_with_commit(
                move |_| {
                    work_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                move || {
                    rollback_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                move || {
                    commit_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            ),
            Outcome::UnsafeToContinue(_)
        ));
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn commit_error_is_explicit_and_poisoned_queue_rejects_next_writer() {
        let mut supervisor = SerialOnboarding::new(Duration::from_secs(1), Duration::from_secs(1));
        assert_eq!(
            supervisor.run_with_commit(
                |_| Ok(()),
                || panic!("accepted commit failure must fail closed"),
                || Err("checkpoint persistence failed".into()),
            ),
            Outcome::UnsafeToContinue(
                "commit finalization failed: checkpoint persistence failed".into()
            )
        );
        assert_queue_rejects_all_following_callbacks(supervisor);
    }

    #[test]
    fn commit_panic_is_explicit_and_poisoned_queue_rejects_next_writer() {
        let mut supervisor = SerialOnboarding::new(Duration::from_secs(1), Duration::from_secs(1));
        assert_eq!(
            supervisor.run_with_commit(
                |_| Ok(()),
                || panic!("accepted commit failure must fail closed"),
                || panic!("injected commit panic"),
            ),
            Outcome::UnsafeToContinue("commit finalization failed: commit panicked".into())
        );
        assert_queue_rejects_all_following_callbacks(supervisor);
    }

    #[test]
    fn timed_out_worker_finishes_rollback_before_next_source() {
        let written = Arc::new(AtomicBool::new(false));
        let work_flag = written.clone();
        let cleanup_flag = written.clone();
        let committed = Arc::new(AtomicBool::new(false));
        let commit_flag = committed.clone();
        let mut supervisor =
            SerialOnboarding::new(Duration::from_millis(40), Duration::from_secs(1));
        let result = supervisor.run_with_commit(
            move |control| {
                work_flag.store(true, Ordering::SeqCst);
                while !control.is_cancelled_or_expired() {
                    thread::sleep(Duration::from_millis(1));
                }
                // A late successful return must not become a committed success.
                Ok(())
            },
            move || {
                cleanup_flag.store(false, Ordering::SeqCst);
                Ok(())
            },
            move || {
                commit_flag.store(true, Ordering::SeqCst);
                Ok(())
            },
        );
        assert_eq!(result, Outcome::TimedOut);
        assert!(!written.load(Ordering::SeqCst));
        assert!(!committed.load(Ordering::SeqCst));
        assert_eq!(
            supervisor.run(
                move |_| {
                    assert!(!written.load(Ordering::SeqCst));
                    Ok(())
                },
                || Ok(())
            ),
            Outcome::Completed
        );
    }

    #[test]
    fn held_mutex_cannot_block_deadline_or_allow_a_second_writer() {
        let mutex = Arc::new(Mutex::new(()));
        let lock = mutex.lock().unwrap();
        let worker_mutex = mutex.clone();
        let started = Instant::now();
        let mut supervisor =
            SerialOnboarding::new(Duration::from_millis(30), Duration::from_millis(40));
        let (exited_tx, exited_rx) = mpsc::channel();
        assert!(matches!(
            supervisor.run(
                move |_| {
                    let _guard = worker_mutex.lock().unwrap();
                    Ok(())
                },
                move || {
                    exited_tx.send(()).unwrap();
                    Ok(())
                }
            ),
            Outcome::UnsafeToContinue(_)
        ));
        assert!(started.elapsed() < Duration::from_secs(2));
        let second = Arc::new(AtomicBool::new(false));
        let flag = second.clone();
        assert!(matches!(
            supervisor.run(
                move |_| {
                    flag.store(true, Ordering::SeqCst);
                    Ok(())
                },
                || Ok(())
            ),
            Outcome::UnsafeToContinue(_)
        ));
        assert!(!second.load(Ordering::SeqCst));
        drop(lock);
        exited_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn failed_rollback_poisoned_queue_rejects_following_sources() {
        let mut supervisor = SerialOnboarding::new(Duration::from_secs(1), Duration::from_secs(1));
        assert!(matches!(
            supervisor.run(
                |_| Err("source failed".into()),
                || Err("database still dirty".into())
            ),
            Outcome::UnsafeToContinue(_)
        ));
        assert!(matches!(
            supervisor.run(|_| panic!("must not start"), || Ok(())),
            Outcome::UnsafeToContinue(_)
        ));
    }

    #[test]
    fn burst_is_processed_once_each_with_one_writer() {
        let active = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let mut supervisor = SerialOnboarding::new(Duration::from_secs(1), Duration::from_secs(1));
        for _ in 0..20 {
            let active = active.clone();
            let completed = completed.clone();
            assert_eq!(
                supervisor.run(
                    move |_| {
                        assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                        thread::sleep(Duration::from_millis(1));
                        active.fetch_sub(1, Ordering::SeqCst);
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    },
                    || Ok(())
                ),
                Outcome::Completed
            );
        }
        assert_eq!(completed.load(Ordering::SeqCst), 20);
    }

    #[test]
    fn panic_runs_rollback_and_is_never_reported_as_success() {
        let mut supervisor = SerialOnboarding::new(Duration::from_secs(1), Duration::from_secs(1));
        let rollback = Arc::new(AtomicBool::new(false));
        let flag = rollback.clone();
        assert!(matches!(
            supervisor.run(
                |_| panic!("injected source panic"),
                move || {
                    flag.store(true, Ordering::SeqCst);
                    Ok(())
                }
            ),
            Outcome::Failed(_)
        ));
        assert!(rollback.load(Ordering::SeqCst));
    }
}
