// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

//! Isolated lower-server lifecycle fixture; never starts discovery or HTTP.
//! Driven by scripts/test-onboarding-robustness.py with a temporary cwd.
use std::fs::File;
use std::io::{self, BufRead, Write};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use opcua::client::prelude::SessionOperationControl;
use opcua::server::aggregation_server::aggregation_server::AggregationServer;
use opcua::server::builder::ServerBuilder;
use opcua::server::server::Server;
use opcua::sync::RwLock;
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
struct Source {
    name: String,
    port: u16,
    kind: String,
    batch: String,
}

#[derive(Deserialize)]
struct Plan {
    aggregation_port: u16,
    healthy_timeout_ms: u64,
    fault_timeout_ms: u64,
    cleanup_timeout_ms: u64,
    late_observation_ms: u64,
    sources: Vec<Source>,
}

fn emit(event: Value) {
    println!("ROBUSTNESS_EVENT {}", event);
    io::stdout().flush().unwrap();
}

fn snapshot(aggregation: &AggregationServer, name: &str) -> Option<(bool, Option<String>)> {
    aggregation
        .lower_servers_info_p
        .try_read()
        .and_then(|infos| {
            infos
                .get(name)
                .map(|info| (info.aggregation_finished, info.aggregation_error.clone()))
        })
}

fn clean(aggregation: &AggregationServer, name: &str, budget: Duration) -> Value {
    let started = Instant::now();
    let _ = aggregation.remove_lower_server(name);
    while !aggregation.lower_server_cleanup_complete(name) {
        assert!(
            started.elapsed() < budget,
            "cleanup deadline exceeded for {name}"
        );
        thread::sleep(Duration::from_millis(10));
    }
    let counts = aggregation
        .lower_server_debug_counts(name)
        .expect("cleanup diagnostics unavailable");
    let counts = serde_json::to_value(counts).expect("cleanup diagnostics not serializable");
    let info_exists = aggregation.lower_servers_info_p.read().contains_key(name);
    emit(json!({"event":"cleanup", "source":name,
        "elapsed_ms":started.elapsed().as_secs_f64()*1000.0,
        "complete":true, "info_exists":info_exists, "counts":counts}));
    assert!(!info_exists, "terminal state was not reaped for {name}");
    counts
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 2, "usage: robustness_fixture <plan.json>");
    let plan: Plan = serde_json::from_reader(File::open(&args[1]).unwrap()).unwrap();
    assert!(!plan.sources.is_empty());
    assert!(plan.aggregation_port != 0);
    tracing_subscriber::fmt()
        .with_env_filter("error")
        .with_writer(io::stderr)
        .init();
    let server = Arc::new(RwLock::new(
        ServerBuilder::new_anonymous("OnboardingRobustness")
            .application_uri("urn:ojies:onboarding-robustness:aggregator")
            .host_and_port("127.0.0.1", plan.aggregation_port)
            .discovery_urls(vec!["/".to_string()])
            .create_sample_keypair(true)
            .is_aggregation_server()
            .server()
            .expect("failed to create isolated aggregation fixture"),
    ));
    let aggregation = server.read().aggregation_server().unwrap();
    let running = server.clone();
    thread::spawn(move || Server::run_server(running));
    emit(json!({"event":"ready",
        "endpoint":format!("opc.tcp://127.0.0.1:{}/", plan.aggregation_port)}));
    let mut last_batch = String::new();
    for source in &plan.sources {
        if last_batch != source.batch {
            // A batch represents sources offered in the same discovery cycle.
            // Mutation stays serial, as in the application's production queue.
            for queued in plan
                .sources
                .iter()
                .filter(|candidate| candidate.batch == source.batch)
            {
                emit(json!({"event":"queued", "source":queued.name, "batch":queued.batch}));
            }
            last_batch.clone_from(&source.batch);
        }
        assert!(source.port != 0 && source.port != plan.aggregation_port);
        let fault = source.kind != "healthy";
        let budget = Duration::from_millis(if fault {
            plan.fault_timeout_ms
        } else {
            plan.healthy_timeout_ms
        });
        let control = SessionOperationControl::new(budget);
        let started = Instant::now();
        emit(
            json!({"event":"started", "source":source.name, "kind":source.kind,
            "budget_ms":budget.as_millis()}),
        );
        aggregation
            .add_lower_server_with_control(
                &server,
                &format!("opc.tcp://127.0.0.1:{}/", source.port),
                &source.name,
                control.clone(),
            )
            .expect("could not enqueue source");
        let mut error = None;
        let outcome = loop {
            if let Some((finished, failure)) = snapshot(&aggregation, &source.name) {
                if finished {
                    break "completed";
                }
                if failure.is_some() {
                    error = failure;
                    break "failed";
                }
            }
            if started.elapsed() >= budget {
                control.cancel();
                break "deadline";
            }
            thread::sleep(Duration::from_millis(10));
        };
        emit(
            json!({"event":"terminal", "source":source.name, "kind":source.kind,
            "outcome":outcome, "error":error,
            "elapsed_ms":started.elapsed().as_secs_f64()*1000.0}),
        );
        if fault {
            assert_ne!(outcome, "completed", "fault source unexpectedly completed");
            control.cancel();
            clean(
                &aggregation,
                &source.name,
                Duration::from_millis(plan.cleanup_timeout_ms),
            );
            if source.kind == "namespace_stall" {
                // Python releases the delayed response before acknowledging here.
                let mut acknowledgement = String::new();
                io::stdin().lock().read_line(&mut acknowledgement).unwrap();
                assert_eq!(acknowledgement.trim(), format!("released {}", source.name));
                thread::sleep(Duration::from_millis(plan.late_observation_ms));
                let counts = aggregation.lower_server_debug_counts(&source.name).unwrap();
                let info_exists = aggregation
                    .lower_servers_info_p
                    .read()
                    .contains_key(&source.name);
                emit(json!({"event":"late_observation", "source":source.name,
                    "info_exists":info_exists, "counts":counts}));
                assert!(!info_exists, "late worker recreated terminal state");
            }
        } else {
            assert_eq!(outcome, "completed", "healthy source failed: {:?}", error);
        }
    }
    emit(json!({"event":"all_sources_processed"}));
    // Keep the healthy endpoints available until Python finishes external checks.
    let mut acknowledgement = String::new();
    io::stdin().lock().read_line(&mut acknowledgement).unwrap();
    assert_eq!(acknowledgement.trim(), "verified");
    for source in plan
        .sources
        .iter()
        .filter(|source| source.kind == "healthy")
    {
        clean(
            &aggregation,
            &source.name,
            Duration::from_millis(plan.cleanup_timeout_ms),
        );
    }
    emit(json!({"event":"finished"}));
}
