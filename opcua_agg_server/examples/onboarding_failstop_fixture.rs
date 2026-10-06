// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

//! Process-level fail-stop check using the application's actual supervisor.
//! No networking, database, or runtime configuration is touched.
#[path = "../src/onboarding.rs"]
mod onboarding;

use onboarding::{Outcome, SerialOnboarding};
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn main() {
    let shared = Arc::new(Mutex::new(()));
    let _held = shared.lock().unwrap();
    let worker_lock = shared.clone();
    let started = Instant::now();
    let mut supervisor =
        SerialOnboarding::new(Duration::from_millis(100), Duration::from_millis(100));
    let outcome = supervisor.run(
        move |_| {
            println!("WORKER_WAITING_ON_HELD_MUTEX");
            io::stdout().flush().unwrap();
            let _guard = worker_lock.lock().unwrap();
            Ok(())
        },
        || Ok(()),
    );
    assert!(matches!(outcome, Outcome::UnsafeToContinue(_)));
    let next = supervisor.run(
        |_| {
            println!("FOLLOWING_SOURCE_STARTED");
            io::stdout().flush().unwrap();
            Ok(())
        },
        || Ok(()),
    );
    assert!(matches!(next, Outcome::UnsafeToContinue(_)));
    println!(
        "FAILSTOP_RESULT {}",
        serde_json::json!({"outcome":"UnsafeToContinue", "following_source_rejected":true,
            "elapsed_ms":started.elapsed().as_secs_f64()*1000.0,
            "timeout_ms":100, "cleanup_grace_ms":100, "exit_code":70})
    );
    io::stdout().flush().unwrap();
    // This is the same exit policy used by the real application. The held
    // mutex is deliberately never released before the whole process ends.
    std::process::exit(70);
}
