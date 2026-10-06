// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

//! Loopback-only aggregation fixture for scripts/test-aggregation-subscriptions.py.
//! Build with `cargo build --example subscription_fixture`; the Python harness
//! supplies a temporary working directory for certificates and mapping state.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use opcua::server::builder::ServerBuilder;
use opcua::server::server::Server;
use opcua::sync::RwLock;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(
        args.len(),
        3,
        "usage: subscription_fixture <source-port> <aggregation-port>"
    );
    let source_port: u16 = args[1].parse().expect("invalid source port");
    let aggregation_port: u16 = args[2].parse().expect("invalid aggregation port");
    assert!(source_port != 0 && aggregation_port != 0 && source_port != aggregation_port);
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let server = Arc::new(RwLock::new(
        ServerBuilder::new_anonymous("SubscriptionRegression")
            .application_uri("urn:ojies:subscription-regression:aggregator")
            .host_and_port("127.0.0.1", aggregation_port)
            .discovery_urls(vec!["/".to_string()])
            .create_sample_keypair(true)
            .is_aggregation_server()
            .server()
            .expect("failed to create aggregation fixture"),
    ));
    let aggregation = server.read().aggregation_server().unwrap();
    let running_server = server.clone();
    let worker = thread::spawn(move || Server::run_server(running_server));
    let source_url = format!("opc.tcp://127.0.0.1:{source_port}/");
    let source_name = "Regression___local";
    aggregation
        .add_lower_server(&server, &source_url, source_name)
        .expect("failed to add loopback source");

    let started = Instant::now();
    loop {
        {
            let lower_servers = aggregation.lower_servers_info_p.read();
            let lower = lower_servers.get(source_name).expect("source disappeared");
            assert!(
                lower.aggregation_error.is_none(),
                "aggregation failed: {:?}",
                lower.aggregation_error
            );
            if lower.aggregation_finished {
                break;
            }
        }
        assert!(
            started.elapsed() < Duration::from_secs(90),
            "aggregation timed out"
        );
        thread::sleep(Duration::from_millis(50));
    }
    println!("FIXTURE_READY opc.tcp://127.0.0.1:{aggregation_port}/");
    worker.join().expect("fixture server thread failed");
}
