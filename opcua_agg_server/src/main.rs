// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

use std::collections::HashSet;
use std::io::{self, BufRead, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};
use std::{
    panic,
    path::Path,
    sync::{Arc, Mutex},
};

use chrono::Local;
use ctrlc;
use opcua::server::address_space::AddressSpace;
use opcua::server::builder::ServerBuilder;
use opcua::server::server::Server;
use opcua::sync::RwLock;
use opcua::types::NodeId;

use tracing::{error, info, warn};
use tracing_subscriber::{
    filter::EnvFilter,
    fmt::{self, format::FmtSpan},
};

use opcua::client::prelude::*;
mod admission;
mod onboarding;
pub mod server_discovery;

use server_discovery::incremental_rules::{ExecutorUpdate, RuleStore};

static ONBOARDING_LOG_PATH: OnceLock<String> = OnceLock::new();
static ADMISSION_LOG_PATH: OnceLock<String> = OnceLock::new();
static ADMISSION_EPOCH: OnceLock<Instant> = OnceLock::new();
static ADMISSION_LOG_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static ATTEMPT_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
static COMPLETION_LOG_PATH: OnceLock<String> = OnceLock::new();
static LDS_PROCESS: OnceLock<Mutex<Option<Child>>> = OnceLock::new();
static LDS_STOPPING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static RULE_STORE: OnceLock<Mutex<RuleStore>> = OnceLock::new();
static PHASE_TIMING_ENABLED: OnceLock<bool> = OnceLock::new();
const URDF_MAX_BYTE_STRING_LENGTH: usize = 16 * 1024 * 1024;
const URDF_MAX_MESSAGE_SIZE: usize = 20 * 1024 * 1024;

// Onboarding and full rebuilds are serialized by the main lifecycle owner.
// This cache is the only writer of the paired canonical rule files.
fn rule_store() -> Result<std::sync::MutexGuard<'static, RuleStore>, String> {
    RULE_STORE
        .get()
        .ok_or_else(|| "rule store is not initialized".to_string())?
        .lock()
        .map_err(|error| error.to_string())
}

fn apply_rule_update(
    aggregation: &opcua::server::aggregation_server::aggregation_server::AggregationServer,
    update: ExecutorUpdate,
) -> Result<(), String> {
    match update {
        ExecutorUpdate::Append(rules) => aggregation.append_mapping_rules(
            rules
                .iter()
                .map(|rule| rule.to_instance_mapping_rule())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?,
        ),
        ExecutorUpdate::Replace(rules) => aggregation.replace_mapping_rules(
            rules
                .iter()
                .map(|rule| rule.to_instance_mapping_rule())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?,
        ),
        ExecutorUpdate::Truncate(len) => aggregation.truncate_mapping_rules(len),
    }
    .map_err(|error| error.to_string())
}

// Phase timing is opt-in to keep normal operation logs concise.
fn phase_timing_enabled() -> bool {
    *PHASE_TIMING_ENABLED.get_or_init(|| {
        std::env::var("OJIES_PHASE_TIMING")
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

fn log_phase_timing(server_name: &str, phase: &str, elapsed: Duration) {
    if phase_timing_enabled() {
        warn!(
            "OJIES_PHASE_TIMING server={} phase={} elapsed_ms={:.3}",
            server_name,
            phase,
            elapsed.as_secs_f64() * 1000.0
        );
    }
}

fn lds_is_reachable() -> bool {
    let address = SocketAddr::from(([127, 0, 0, 1], 4840));
    TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok()
}

fn find_lds_executable() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("OJIES_LDS_EXECUTABLE").map(PathBuf::from) {
        if path.is_file() {
            return Some(path);
        }
    }

    let relative = Path::new("open62541")
        .join("build-lds")
        .join("bin")
        .join("examples");
    let mut candidates = Vec::new();

    if let Ok(current_dir) = std::env::current_dir() {
        candidates.push(current_dir.join(&relative));
        if let Some(parent) = current_dir.parent() {
            candidates.push(parent.join(&relative));
        }
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    candidates.push(manifest_dir.join(&relative));
    if let Some(parent) = manifest_dir.parent() {
        candidates.push(parent.join(&relative));
    }

    candidates.into_iter().find_map(|directory| {
        ["discovery_server_lds", "discovery_server_lds.exe"]
            .into_iter()
            .map(|name| directory.join(name))
            .find(|path| path.is_file())
    })
}

fn start_local_discovery_server() -> Result<bool, String> {
    // Serialize the whole spawn/readiness transition with shutdown. A spawned
    // child is always owned before any fallible readiness check can return.
    let mut owner = LDS_PROCESS
        .get_or_init(|| Mutex::new(None))
        .lock()
        .map_err(|_| "LDS process lock poisoned".to_string())?;
    if LDS_STOPPING.load(std::sync::atomic::Ordering::Acquire) {
        return Err("LDS shutdown is in progress".into());
    }
    if lds_is_reachable() {
        // Another supervisor may already have replaced our exited child. Do
        // not claim ownership of its listener or keep reporting fake restarts.
        if let Some(child) = owner.as_mut() {
            if child
                .try_wait()
                .map_err(|error| error.to_string())?
                .is_some()
            {
                *owner = None;
            }
        }
        info!("LDS laeuft bereits auf opc.tcp://localhost:4840");
        return Ok(false);
    }
    if let Some(child) = owner.as_mut() {
        if child
            .try_wait()
            .map_err(|error| error.to_string())?
            .is_none()
        {
            return Err("Owned LDS is alive but its listener is unavailable".into());
        }
    }
    let executable = find_lds_executable()
        .ok_or_else(|| "discovery_server_lds wurde im Projekt nicht gefunden".to_string())?;
    let working_dir = executable
        .parent()
        .ok_or_else(|| "LDS working directory is unavailable".to_string())?;
    // Preserve crash diagnostics across owned restarts in the runtime folder.
    let diagnostic = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("lds_runtime.log")
        .map_err(|error| format!("LDS diagnostic log could not be opened: {error}"))?;
    let diagnostic_err = diagnostic
        .try_clone()
        .map_err(|error| format!("LDS diagnostic log could not be cloned: {error}"))?;
    let child = Command::new(&executable)
        .current_dir(working_dir)
        .stdout(Stdio::from(diagnostic))
        .stderr(Stdio::from(diagnostic_err))
        .spawn()
        .map_err(|error| format!("LDS could not start: {error}"))?;
    *owner = Some(child);
    let readiness = (|| {
        for _ in 0..50 {
            if LDS_STOPPING.load(std::sync::atomic::Ordering::Acquire) {
                return Err("LDS shutdown requested during startup".to_string());
            }
            let child = owner.as_mut().expect("spawned LDS is owned");
            if let Some(status) = child
                .try_wait()
                .map_err(|error| format!("LDS status: {error}"))?
            {
                return Err(format!("LDS exited during startup: {status}"));
            }
            if lds_is_reachable() {
                info!("LDS gestartet: opc.tcp://localhost:4840");
                return Ok(true);
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err("LDS listener was not ready after 5 seconds".to_string())
    })();
    if readiness.is_err() {
        if let Some(child) = owner.as_mut() {
            // Keep the handle until exit is acknowledged. If kill/wait fails,
            // the next recovery cycle must not create another live child.
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    readiness
}

/// Only repair a child started by this process. An external LDS remains under
/// its own operator or experiment coordinator, including its failure outcome.
fn recover_owned_discovery_server() -> Result<bool, String> {
    let Some(process) = LDS_PROCESS.get() else {
        return Ok(false);
    };
    let mut guard = process
        .lock()
        .map_err(|_| "LDS process lock poisoned".to_string())?;
    let Some(child) = guard.as_mut() else {
        return Ok(false);
    };
    let Some(status) = child
        .try_wait()
        .map_err(|error| format!("LDS status: {error}"))?
    else {
        return Ok(false);
    };
    // Retain the reaped handle until a replacement starts successfully. Failed
    // starts remain eligible for the next backed-off observation cycle.
    warn!("Owned LDS exited with {status}. Attempting a supervised restart");
    drop(guard);
    start_local_discovery_server()
}

fn stop_local_discovery_server() {
    LDS_STOPPING.store(true, std::sync::atomic::Ordering::Release);
    let Some(process) = LDS_PROCESS.get() else {
        return;
    };
    let Ok(mut guard) = process.lock() else {
        return;
    };
    let Some(mut child) = guard.take() else {
        return;
    };

    let _ = child.kill();
    let _ = child.wait();
}

fn tcp_port_is_available(host: &str, port: u16) -> bool {
    TcpListener::bind((host, port)).is_ok()
}

// Tracks bereits in den Address Space importierte Namespace-URLs.
// Verhindert doppelten NodeSet-Import wenn mehrere Geräte dieselben Companion Specs teilen.
static IMPORTED_NAMESPACES: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn get_imported_namespaces() -> &'static Mutex<HashSet<String>> {
    IMPORTED_NAMESPACES.get_or_init(|| Mutex::new(HashSet::new()))
}

// Exakte NodeIds aller Knoten, die aus dynamisch entdeckten NodeSets
// erfolgreich in den Address Space eingefügt wurden.
static IMPORTED_NODE_IDS: OnceLock<Mutex<HashSet<NodeId>>> = OnceLock::new();

fn get_imported_node_ids() -> &'static Mutex<HashSet<NodeId>> {
    IMPORTED_NODE_IDS.get_or_init(|| Mutex::new(HashSet::new()))
}

// Entry-Points haengen nur von der Menge der importierten NodeSets ab.
// Der Cache wird nach jedem erfolgreichen neuen NodeSet-Import ersetzt.
static ENTRY_POINTS_CACHE: OnceLock<Mutex<Option<Vec<server_discovery::NodeSetEntryPoints>>>> =
    OnceLock::new();

fn get_entry_points_cache() -> &'static Mutex<Option<Vec<server_discovery::NodeSetEntryPoints>>> {
    ENTRY_POINTS_CACHE.get_or_init(|| Mutex::new(None))
}

// Rule-Generator-Knoten werden lokal in den Address Space geschrieben und
// deshalb nicht vom Lower-Server-Cleanup der OPC-UA-Bibliothek erfasst.
static GENERATED_RULE_NODES: OnceLock<Mutex<HashSet<NodeId>>> = OnceLock::new();

fn get_generated_rule_nodes() -> &'static Mutex<HashSet<NodeId>> {
    GENERATED_RULE_NODES.get_or_init(|| Mutex::new(HashSet::new()))
}

// Globale Schreibsperre: verhindert Race Conditions beim gleichzeitigen Schreiben
// in rules.json, entry_points.json und namespaces.json durch parallele Server-Threads.
static JSON_WRITE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn get_json_write_lock() -> &'static Mutex<()> {
    JSON_WRITE_LOCK.get_or_init(|| Mutex::new(()))
}

fn wait_for_lower_server_cleanup(
    aggregation_server: &Arc<
        Mutex<opcua::server::aggregation_server::aggregation_server::AggregationServer>,
    >,
    server_names: &[String],
    deadline: Instant,
) -> bool {
    while Instant::now() < deadline {
        // The old loop's lock() could block before it ever counted a poll.
        // Probe the actual worker acknowledgement; the info entry alone does
        // not establish that a writer or its rollback has stopped.
        let lifecycle = match aggregation_server.try_lock() {
            Ok(server) => Some(server.clone()),
            Err(std::sync::TryLockError::WouldBlock) => None,
            Err(std::sync::TryLockError::Poisoned(_)) => return false,
        };
        if let Some(lifecycle) = lifecycle {
            if server_names
                .iter()
                .all(|name| lifecycle.lower_server_cleanup_complete(name))
            {
                return true;
            }
        }
        thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(10)),
        );
    }

    warn!(
        "Lower-Server-Cleanup bis zur Frist nicht abgeschlossen: {:?}",
        server_names
    );
    false
}

fn wait_for_lower_server_aggregation(
    aggregation_server: &opcua::server::aggregation_server::aggregation_server::AggregationServer,
    server_name: &str,
    control: &SessionOperationControl,
) -> Result<(), String> {
    loop {
        control.check().map_err(|e| e.to_string())?;
        // Never wait on the application-wide AggregationServer mutex or on a
        // status writer. The supervisor also retains its independent deadline.
        if let Some(infos) = aggregation_server.lower_servers_info_p.try_read() {
            match infos.get(server_name) {
                Some(info) if info.aggregation_error.is_some() => {
                    return Err(info.aggregation_error.clone().unwrap())
                }
                Some(info) if info.removal_in_progress => {
                    return Err("source removed during onboarding".into())
                }
                Some(info) if info.aggregation_finished => return Ok(()),
                None => return Err("source disappeared before completion".into()),
                _ => {}
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(test)]
fn append_missing_configured_servers<F>(
    hosts: &[server_discovery::Host],
    active_server_names: &HashSet<String>,
    new_servers: &mut Vec<(String, String)>,
    mut endpoint_is_reachable: F,
) -> usize
where
    F: FnMut(&str) -> bool,
{
    let mut queued_names: HashSet<String> =
        new_servers.iter().map(|(name, _)| name.clone()).collect();
    let mut added = 0;

    for host in hosts {
        if !active_server_names.contains(&host.name)
            && !queued_names.contains(&host.name)
            && endpoint_is_reachable(&host.address)
        {
            queued_names.insert(host.name.clone());
            new_servers.push((host.name.clone(), host.address.clone()));
            added += 1;
        }
    }

    added
}

fn delete_hierarchical_subtree(address_space: &mut AddressSpace, node_id: &NodeId) -> usize {
    // Hierarchical references form a graph, not necessarily a tree. Iterate to
    // avoid stack overflow on deep models and revisit neither cycles nor aliases.
    let mut pending = vec![node_id.clone()];
    let mut seen = HashSet::new();
    let mut ordered = Vec::new();
    while let Some(node) = pending.pop() {
        // A source reference must never delete standard server infrastructure.
        if node.namespace == 0 || !seen.insert(node.clone()) {
            continue;
        }
        pending.extend(
            address_space
                .find_hierarchical_references(&node)
                .unwrap_or_default(),
        );
        ordered.push(node);
    }
    ordered
        .iter()
        .rev()
        .filter(|node| address_space.delete(*node, true))
        .count()
}

fn delete_lower_server_roots(address_space: &mut AddressSpace, server_names: &[String]) -> usize {
    let server_names: HashSet<&str> = server_names.iter().map(String::as_str).collect();
    let root_node_ids = address_space
        .find_hierarchical_references(&NodeId::objects_folder_id())
        .unwrap_or_default()
        .into_iter()
        .filter(|node_id| {
            address_space.find_node(node_id).is_some_and(|node| {
                node.as_node()
                    .browse_name()
                    .name
                    .value()
                    .as_deref()
                    .is_some_and(|name| server_names.contains(name))
            })
        })
        .collect::<Vec<_>>();

    root_node_ids
        .iter()
        .map(|node_id| delete_hierarchical_subtree(address_space, node_id))
        .sum()
}

fn clear_dynamic_aggregation_state(
    server_p: &Arc<RwLock<Server>>,
    aggregation_server: &Arc<
        Mutex<opcua::server::aggregation_server::aggregation_server::AggregationServer>,
    >,
    lower_server_names: &[String],
) -> Result<(), String> {
    let mut node_ids: HashSet<NodeId> = {
        let mut generated_nodes = get_generated_rule_nodes()
            .lock()
            .expect("Failed to lock generated rule nodes");
        generated_nodes.drain().collect()
    };

    {
        let mut imported_nodes = get_imported_node_ids()
            .lock()
            .expect("Failed to lock imported NodeIds");
        node_ids.extend(imported_nodes.drain());
    }

    let (deleted_nodes, deleted_lower_server_nodes) = {
        let server = server_p.read();
        let address_space_p = server.address_space();
        let mut address_space = address_space_p.write();

        let deleted_lower_server_nodes =
            delete_lower_server_roots(&mut address_space, lower_server_names);
        let deleted_nodes = node_ids
            .iter()
            .filter(|node_id| address_space.delete(*node_id, true))
            .count();

        (deleted_nodes, deleted_lower_server_nodes)
    };

    get_imported_namespaces()
        .lock()
        .expect("Failed to lock imported namespaces")
        .clear();
    *get_entry_points_cache()
        .lock()
        .expect("Failed to lock entry-point cache") = None;

    let update = {
        let _json_lock = get_json_write_lock()
            .lock()
            .map_err(|error| error.to_string())?;
        let mut store = rule_store()?;
        store
            .begin_transaction()
            .map_err(|error| error.to_string())?;
        let update = store.clear().map_err(|error| error.to_string())?;
        let base_namespaces = serde_json::json!([
            { "url": "http://opcfoundation.org/UA/", "nsid": 0 }
        ]);
        std::fs::write("entry_points.json", "[]\n").map_err(|error| error.to_string())?;
        std::fs::write(
            "namespaces.json",
            serde_json::to_vec_pretty(&base_namespaces).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        update
    };

    let mut agg_server = aggregation_server
        .lock()
        .map_err(|error| error.to_string())?;
    agg_server.incomplete_mappings_p.write().clear();
    apply_rule_update(&agg_server, update)?;
    agg_server
        .load_standardized_namespaces(Path::new("namespaces.json"))
        .map_err(|error| error.to_string())?;
    rule_store()?.commit().map_err(|error| error.to_string())?;

    info!(
        "Dynamischer Aggregationszustand bereinigt: {} lokale/importierte und {} Lower-Server-Knoten entfernt",
        deleted_nodes, deleted_lower_server_nodes
    );
    Ok(())
}

fn ask_for_urdf_export<R: BufRead, W: Write>(input: &mut R, output: &mut W) -> io::Result<bool> {
    loop {
        write!(output, "URDF-Export aktivieren? [j/n]: ")?;
        output.flush()?;
        let mut answer = String::new();
        if input.read_line(&mut answer)? == 0 {
            // Detached services and redirected stdin may reach EOF immediately.
            // Do not spin or flood their logs with an unanswerable prompt.
            writeln!(output, "Keine Eingabe; URDF-Export bleibt deaktiviert")?;
            return Ok(false);
        }
        match answer.trim().to_lowercase().as_str() {
            "j" | "ja" | "y" | "yes" => return Ok(true),
            "n" | "nein" | "no" => return Ok(false),
            _ => writeln!(output, "Bitte 'j' oder 'n' eingeben.")?,
        }
    }
}

fn main() {
    // Do not overwrite evidence of an interrupted rule transaction at startup.
    if let Err(error) = RuleStore::ensure_no_pending_transaction(Path::new("rules.json")) {
        eprintln!("Rule state requires inspection before restart: {error}");
        std::process::exit(74);
    }
    // Dienste können die interaktive Abfrage über OJIES_EXPORT_URDF umgehen.
    let configured_export = std::env::var("OJIES_EXPORT_URDF").ok().and_then(|value| {
        match value.trim().to_lowercase().as_str() {
            "1" | "true" | "j" | "ja" | "y" | "yes" => Some(true),
            "0" | "false" | "n" | "nein" | "no" => Some(false),
            _ => None,
        }
    });
    let export_urdf = configured_export.unwrap_or_else(|| {
        ask_for_urdf_export(&mut io::stdin().lock(), &mut io::stdout().lock()).unwrap_or_else(
            |error| {
                eprintln!("URDF-Abfrage fehlgeschlagen: {error}; Export bleibt deaktiviert");
                false
            },
        )
    });

    ctrlc::set_handler(|| {
        println!("Received SIGTERM - shutting down gracefully...");
        stop_local_discovery_server();
        std::process::exit(0);
    })
    .expect("Failed to set SIGTERM handler");

    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    fmt::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .with_timer(fmt::time::uptime())
        .with_level(true)
        .with_thread_names(true)
        .with_span_events(FmtSpan::ENTER)
        .init();

    panic::set_hook(Box::new(|p| error!(err = ?p)));

    // An acquisition orchestrator can bind the completion log to its immutable
    // run directory. Interactive/manual starts retain the historical default.
    let log_path = std::env::var("OJIES_COMPLETION_LOG_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            format!(
                "logs/device_completions_{}.log",
                Local::now().format("%Y-%m-%d_%H-%M-%S")
            )
        });
    if let Some(log_dir) = Path::new(&log_path).parent() {
        if let Err(e) = std::fs::create_dir_all(log_dir) {
            error!(
                "Konnte Completion-Log-Ordner '{}' nicht erstellen: {}",
                log_dir.display(),
                e
            );
        }
    }
    COMPLETION_LOG_PATH
        .set(log_path.clone())
        .expect("OnceLock bereits gesetzt");
    info!("Completion-Log dieser Session: {}", log_path);
    let outcome_path = std::env::var("OJIES_ONBOARDING_LOG_PATH").unwrap_or_else(|_| {
        Path::new(&log_path)
            .with_extension("jsonl")
            .to_string_lossy()
            .into_owned()
    });
    if let Some(parent) = Path::new(&outcome_path).parent() {
        std::fs::create_dir_all(parent).expect("cannot create onboarding event directory");
    }
    let admission_path = std::env::var("OJIES_ADMISSION_LOG_PATH").unwrap_or_else(|_| {
        Path::new(&outcome_path)
            .with_extension("admission.jsonl")
            .to_string_lossy()
            .into_owned()
    });
    if let Some(parent) = Path::new(&admission_path).parent() {
        std::fs::create_dir_all(parent).expect("cannot create admission event directory");
    }
    ADMISSION_LOG_PATH
        .set(admission_path)
        .expect("admission log already set");
    ONBOARDING_LOG_PATH
        .set(outcome_path)
        .expect("onboarding log already set");

    let (endpoint_connect_timeout, endpoint_failure_threshold) =
        server_discovery::endpoint_liveness_configuration();
    warn!(
        "OJIES_ENDPOINT_LIVENESS connect_timeout_ms={} failure_threshold={}",
        endpoint_connect_timeout.as_millis(),
        endpoint_failure_threshold
    );

    info!("OPC UA Aggregation Server startet...");

    if let Err(error) = start_local_discovery_server() {
        error!("LDS-Start fehlgeschlagen: {}", error);
        std::process::exit(1);
    }

    if !tcp_port_is_available("0.0.0.0", 8080) {
        error!(
            "HTTP-Server Port 8080 ist bereits belegt. Beende Prozess statt in Thread-Panic zu laufen."
        );
        std::process::exit(1);
    }

    thread::spawn(|| {
        println!("HTTP Thread gestartet");
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(error) => {
                error!("Tokio Runtime fehlgeschlagen: {}", error);
                return;
            }
        };
        rt.block_on(async {
            let app = server_discovery::create_router();
            let listener = match tokio::net::TcpListener::bind("0.0.0.0:8080").await {
                Ok(listener) => listener,
                Err(error) => {
                    error!("HTTP-Server Bind fehlgeschlagen: {}", error);
                    return;
                }
            };
            info!("HTTP-Server laeuft auf http://0.0.0.0:8080");
            if let Err(error) = axum::serve(listener, app).await {
                error!("HTTP-Server fehlgeschlagen: {}", error);
            }
        });
    });

    info!("Initialisiere Python-Runtime...");

    let root = std::env::var_os("OJIES_RUNTIME_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")));
    let venv = root.join("python_service").join(".venv");
    let scripts = root.join("python_service").join("scripts");

    if let Err(e) = server_discovery::init_python(&venv, &scripts) {
        error!("Python-Runtime-Initialisierung fehlgeschlagen: {}", e);
        std::process::exit(1);
    }

    info!("Lade URDF-Exporter...");
    let urdf_exporter = match server_discovery::UrdfExporter::new() {
        Ok(exp) => {
            info!("URDF-Exporter bereit");
            Arc::new(exp)
        }
        Err(e) => {
            error!("URDF-Exporter konnte nicht geladen werden: {}", e);
            std::process::exit(1);
        }
    };

    info!("Leere JSON-Dateien...");
    if let Err(e) = server_discovery::reset_json_files() {
        error!("Fehler beim Leeren der JSON-Dateien: {}", e);
        std::process::exit(74);
    }
    let store = RuleStore::load(Path::new("rules.json")).unwrap_or_else(|error| {
        eprintln!("Rule store initialization failed: {error}");
        std::process::exit(74);
    });
    if RULE_STORE.set(Mutex::new(store)).is_err() {
        panic!("rule store already initialized");
    }

    info!("Aktualisiere Companion Specs Liste...");
    server_discovery::update_companion_specs_list("config.json");

    info!("Lade initiale Konfiguration...");
    let config = match server_discovery::load_config("config.json") {
        Ok(cfg) => {
            info!(
                "Konfiguration geladen (Endpoint: opc.tcp://{}:{})",
                cfg.host, cfg.port
            );
            cfg
        }
        Err(e) => {
            error!("Fehler beim Laden der Konfiguration: {}", e);
            std::process::exit(1);
        }
    };

    let aggregation_host = std::env::var("OJIES_OPCUA_HOST")
        .ok()
        .map(|host| host.trim().to_string())
        .filter(|host| !host.is_empty())
        .unwrap_or_else(|| config.host.clone());

    if matches!(aggregation_host.as_str(), "0.0.0.0" | "::" | "[::]" | "*") {
        error!(
            "OPC-UA-Host '{}' ist keine erreichbare Client-Adresse. Bitte eine konkrete IP oder einen Hostnamen konfigurieren.",
            aggregation_host
        );
        std::process::exit(1);
    }

    if !tcp_port_is_available(&aggregation_host, config.port) {
        error!(
            "OPC UA Endpoint opc.tcp://{}:{} kann nicht gebunden werden. Die Adresse muss diesem Rechner zugewiesen und der Port frei sein.",
            aggregation_host, config.port
        );
        std::process::exit(1);
    }

    info!(
        "Erstelle OPC UA Aggregation Server unter opc.tcp://{}:{}...",
        aggregation_host, config.port
    );
    let server_p: Arc<RwLock<Server>> = Arc::new(RwLock::new(
        ServerBuilder::new_anonymous("AggregationServer")
            .application_uri("roboteach.plcm.tu-darmstadt.de/agg-server")
            .product_uri("roboteach.plcm.tu-darmstadt.de")
            .host_and_port(aggregation_host.clone(), config.port)
            .max_byte_string_length(URDF_MAX_BYTE_STRING_LENGTH)
            .max_message_size(URDF_MAX_MESSAGE_SIZE)
            .max_chunk_count(0)
            // new_anonymous() materializes its discovery URL immediately with
            // the builder defaults (127.0.0.1:4855). Rebuild it after changing
            // host and port so FindServers and GetEndpoints advertise the same URL.
            .discovery_urls(vec!["/".to_string()])
            .create_sample_keypair(true)
            .is_aggregation_server()
            .server()
            .expect("Failed to create OPC UA server"),
    ));

    let aggregation_server = {
        let server = server_p.read();
        let agg = server
            .aggregation_server()
            .expect("Server is not set as aggregation server");
        Arc::new(Mutex::new(agg))
    };

    let t_server_p = server_p.clone();
    let server_task = thread::spawn(move || {
        info!("Server Thread laeuft");
        Server::run_server(t_server_p);
    });

    {
        let mut agg_server = aggregation_server
            .lock()
            .expect("Failed to lock aggregation server");

        let initial_rules = rule_store().and_then(|store| {
            store
                .executor_rules()
                .iter()
                .map(|rule| rule.to_instance_mapping_rule())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())
        });
        if let Err(error) = initial_rules.and_then(|rules| {
            agg_server
                .replace_mapping_rules(rules)
                .map_err(|error| error.to_string())
        }) {
            eprintln!("Initial mapping state could not be installed: {error}");
            std::process::exit(74);
        }

        if let Err(e) = agg_server.load_standardized_namespaces(Path::new("namespaces.json")) {
            error!("Fehler beim Laden der Namespaces: {:?}", e);
        } else {
            info!("Standardized Namespaces geladen");
        }
    }

    let mut onboarding_supervisor = onboarding::SerialOnboarding::new(
        onboarding_duration("OJIES_ONBOARDING_TIMEOUT_MS", 600_000),
        onboarding_duration("OJIES_ONBOARDING_CLEANUP_MS", 15_000),
    );
    let capacity = admission_limit("OJIES_ADMISSION_CAPACITY", 256, 1, 65_536) as usize;
    let grace = onboarding_duration("OJIES_SOURCE_ABSENCE_GRACE_MS", 30_000);
    let attempts = admission_limit("OJIES_ADMISSION_MAX_ATTEMPTS", 3, 1, 100) as u32;
    let retry = onboarding_duration("OJIES_ADMISSION_RETRY_BASE_MS", 1_000);
    let quarantine = Duration::from_millis(admission_limit(
        "OJIES_ADMISSION_QUARANTINE_MS",
        300_000,
        60_000,
        86_400_000,
    ));
    let epoch = Instant::now();
    let _ = ADMISSION_EPOCH.set(epoch);
    let queue = Arc::new(Mutex::new(admission::Admission::new(
        capacity,
        grace,
        attempts,
        retry,
        quarantine,
        Instant::now(),
    )));
    // Configured startup candidates use the same queue and writer as discovery.
    let initial = config
        .hosts
        .iter()
        .map(|host| admission::Source::new(host.name.clone(), host.address.clone()))
        .collect();
    let initial_summary =
        queue
            .lock()
            .expect("admission queue poisoned")
            .observe(initial, false, Instant::now());
    emit_observation_events(
        &initial_summary,
        &queue.lock().expect("admission queue poisoned").stats(),
        false,
    );
    emit_admission_event(
        "policy",
        serde_json::json!({
            "capacity":capacity, "absence_grace_ms":grace.as_millis(),
            "max_attempts_per_generation":attempts,"retry_base_ms":retry.as_millis(),
            "quarantine_ms":quarantine.as_millis(),"integration_writers":1,
        }),
    );
    let observer_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let current_observation = Arc::new(Mutex::new(None::<SessionOperationControl>));
    let observer = start_admission_observer(
        queue.clone(),
        observer_stop.clone(),
        current_observation.clone(),
    );
    // A removal keeps the legacy whole-layer rebuild policy. Only this owner
    // performs config mutation, worker cleanup, rule reset and re-admission.
    let mut pending_cleanup: Option<(Vec<String>, Instant)> = None;
    loop {
        if server_task.is_finished() {
            break;
        }
        if observer.is_finished() {
            error!("Discovery observer stopped unexpectedly; refusing new integration");
            std::process::exit(70);
        }
        if pending_cleanup.is_none() {
            // Registry presence is not proof that a previously integrated source
            // still has a healthy runtime session. Read without blocking discovery.
            let expected = queue
                .lock()
                .expect("admission queue poisoned")
                .active_source_names();
            let unhealthy = aggregation_server
                .try_lock()
                .ok()
                .and_then(|state| {
                    state.lower_servers_info_p.try_read().map(|infos| {
                        expected
                            .into_iter()
                            .filter(|name| {
                                infos
                                    .get(name)
                                    .map(|info| {
                                        info.removal_in_progress
                                            || info.aggregation_error.is_some()
                                            || !info.aggregation_finished
                                    })
                                    .unwrap_or(true)
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .unwrap_or_default();
            if !unhealthy.is_empty() {
                let invalidated = queue
                    .lock()
                    .expect("admission queue poisoned")
                    .invalidate_active(&unhealthy);
                emit_admission_event(
                    "runtime_invalidated",
                    serde_json::json!({"sources":unhealthy,"invalidated":invalidated}),
                );
            }
            let retired = queue
                .lock()
                .expect("admission queue poisoned")
                .take_removals();
            if !retired.is_empty() {
                let active = {
                    let agg = aggregation_server
                        .lock()
                        .expect("aggregation state poisoned");
                    let names = agg
                        .lower_servers_info_p
                        .read()
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>();
                    names
                };
                let cleanup_names = active
                    .iter()
                    .cloned()
                    .chain(retired.iter().map(|key| key.application_uri.clone()))
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                for name in &cleanup_names {
                    let agg = aggregation_server
                        .lock()
                        .expect("aggregation state poisoned");
                    if let Err(error) = agg.remove_lower_server(name) {
                        warn!("Cleanup for {}: {:?}", name, error);
                    }
                }
                pending_cleanup = Some((
                    cleanup_names,
                    Instant::now() + onboarding_duration("OJIES_ONBOARDING_CLEANUP_MS", 15_000),
                ));
            }
        }
        if let Some((cleanup_names, deadline)) = &pending_cleanup {
            if !wait_for_lower_server_cleanup(&aggregation_server, cleanup_names, *deadline) {
                emit_admission_event(
                    "rebuild_failed",
                    serde_json::json!({"sources":cleanup_names,"detail":"worker cleanup was not acknowledged before its deadline"}),
                );
                error!("Admission rebuild cleanup expired; refusing any following writer");
                std::process::exit(74);
            }
            if let Err(error) =
                clear_dynamic_aggregation_state(&server_p, &aggregation_server, cleanup_names)
                    .and_then(|_| {
                        server_discovery::config::forget_all_admitted_sources("config.json")
                    })
            {
                error!(
                    "Admission rebuild failed; restart requires inspection: {}",
                    error
                );
                std::process::exit(74);
            }
            let rebuilt = queue
                .lock()
                .expect("admission queue poisoned")
                .requeue_active_after_rebuild(Instant::now());
            emit_admission_event(
                "rebuild",
                serde_json::json!({"requeued":rebuilt.requeued,"deferred":rebuilt.deferred}),
            );
            pending_cleanup = None;
            continue;
        }
        let ticket = queue
            .lock()
            .expect("admission queue poisoned")
            .take_ready(Instant::now());
        if let Some(ticket) = ticket {
            emit_admission_event(
                "started",
                serde_json::json!({
                    "source":ticket.source.name,"endpoint":ticket.source.address,
                    "generation":ticket.generation,"attempt":ticket.attempt,
                    "admitted_at_ms":admission_elapsed_at(ticket.first_observed_at),
                    "enqueued_at_ms":admission_elapsed_at(ticket.enqueued_at),
                    "started_at_ms":admission_elapsed_at(ticket.started_at),
                    "queue_wait_ms":ticket.started_at.saturating_duration_since(ticket.enqueued_at).as_millis(),
                    "admitted_to_start_ms":ticket.started_at.saturating_duration_since(ticket.first_observed_at).as_millis(),
                }),
            );
            let success = handle_new_server(
                &mut onboarding_supervisor,
                &ticket.source.name,
                &ticket.source.address,
                &server_p,
                &aggregation_server,
                &urdf_exporter,
                export_urdf,
            );
            let outcome = queue.lock().expect("admission queue poisoned").finish(
                &ticket,
                success,
                Instant::now(),
            );
            emit_admission_event(
                "finished",
                serde_json::json!({
                    "source":ticket.source.name,"endpoint":ticket.source.address,
                    "generation":ticket.generation,"attempt":ticket.attempt,
                    "admitted_at_ms":admission_elapsed_at(ticket.first_observed_at),
                    "enqueued_at_ms":admission_elapsed_at(ticket.enqueued_at),
                    "started_at_ms":admission_elapsed_at(ticket.started_at),
                    "outcome":format!("{:?}",outcome.disposition),"integration_success":success,
                    "ended_at_ms":admission_elapsed_at(outcome.ended_at),
                    "attempt_elapsed_ms":outcome.ended_at.saturating_duration_since(ticket.started_at).as_millis(),
                    "admitted_to_attempt_end_ms":outcome.ended_at.saturating_duration_since(ticket.first_observed_at).as_millis(),
                    "retry_after_ms":outcome.next_ready_at.map(|at|at.saturating_duration_since(outcome.ended_at).as_millis()),
                    "queue":admission_stats_json(&outcome.queue),
                }),
            );
        } else {
            thread::sleep(Duration::from_millis(100));
        }
    }
    observer_stop.store(true, std::sync::atomic::Ordering::Release);
    if let Some(control) = current_observation
        .lock()
        .expect("observer control poisoned")
        .as_ref()
    {
        control.cancel();
    }
    // Never block shutdown on native DNS/transport calls or create a replacement
    // observer while the first one remains alive. Process exit ends this reader.
    if observer.is_finished() {
        let _ = observer.join();
    }
}

#[cfg(test)]
mod lower_server_retry_tests {
    use super::*;
    use opcua::core::{
        comms::{
            message_writer::MessageWriter,
            secure_channel::{Role, SecureChannel},
        },
        supported_message::SupportedMessage,
    };
    use opcua::crypto::CertificateStore;
    use opcua::types::{ByteString, DataValue, DecodingOptions, ReadResponse, ResponseHeader};

    #[test]
    fn large_read_response_is_split_at_the_negotiated_chunk_size() {
        const CHUNK_SIZE: usize = 65_535;
        const MESH_SIZE: usize = 120_561;

        let response: SupportedMessage = ReadResponse {
            response_header: ResponseHeader::null(),
            results: Some(vec![DataValue::new_now(ByteString::from(vec![
                0u8;
                MESH_SIZE
            ]))]),
            diagnostic_infos: None,
        }
        .into();
        let certificate_store = Arc::new(RwLock::new(CertificateStore::new(&std::env::temp_dir())));
        let secure_channel =
            SecureChannel::new(certificate_store, Role::Server, DecodingOptions::default());
        let mut writer = MessageWriter::new(CHUNK_SIZE, 0, 0);

        writer.write(1, response, &secure_channel).unwrap();
        let encoded = writer.bytes_to_write();
        let first_chunk_size = u32::from_le_bytes(encoded[4..8].try_into().unwrap()) as usize;

        assert!(first_chunk_size <= CHUNK_SIZE);
        assert!(encoded.len() > first_chunk_size);
        assert_eq!(&encoded[first_chunk_size..first_chunk_size + 3], b"MSG");
    }

    #[test]
    fn configured_endpoint_is_also_advertised_by_find_servers() {
        let config = ServerBuilder::new_anonymous("AggregationServer")
            .host_and_port("192.0.2.27", 48400)
            .discovery_urls(vec!["/".to_string()])
            .config();

        assert_eq!(
            config.discovery_urls,
            vec!["opc.tcp://192.0.2.27:48400/".to_string()]
        );
        assert_eq!(config.base_endpoint_url(), "opc.tcp://192.0.2.27:48400");
    }

    #[test]
    fn production_address_space_excludes_generated_test_nodeset() {
        let address_space = AddressSpace::new();

        assert!(address_space.find_node(&NodeId::new(0, 398u32)).is_none());
        assert!(address_space.find_node(&NodeId::new(0, 11886u32)).is_none());
    }

    fn host(name: &str, address: &str) -> server_discovery::Host {
        server_discovery::Host {
            name: name.to_string(),
            address: address.to_string(),
        }
    }

    #[test]
    fn queues_only_reachable_configured_sources_missing_from_active_lower_servers_once() {
        let hosts = vec![
            host("urn:test:a", "opc.tcp://127.0.0.1:4860"),
            host("urn:test:b", "opc.tcp://127.0.0.1:4861"),
            host("urn:test:c", "opc.tcp://127.0.0.1:4862"),
            host("urn:test:d", "opc.tcp://127.0.0.1:4863"),
        ];
        let active = HashSet::from(["urn:test:a".to_string()]);
        let mut new_servers = vec![(
            "urn:test:b".to_string(),
            "opc.tcp://127.0.0.1:4861".to_string(),
        )];

        let mut probed = Vec::new();
        let added =
            append_missing_configured_servers(&hosts, &active, &mut new_servers, |address| {
                probed.push(address.to_string());
                address.ends_with(":4863")
            });

        assert_eq!(added, 1);
        assert_eq!(new_servers.len(), 2);
        assert!(new_servers.iter().any(|(name, _)| name == "urn:test:b"));
        assert!(!new_servers.iter().any(|(name, _)| name == "urn:test:c"));
        assert!(new_servers.iter().any(|(name, _)| name == "urn:test:d"));
        assert_eq!(
            probed,
            vec![
                "opc.tcp://127.0.0.1:4862".to_string(),
                "opc.tcp://127.0.0.1:4863".to_string()
            ]
        );
    }

    #[test]
    fn replaces_planned_namespace_ids_with_address_space_ids() {
        let mut address_space = AddressSpace::new();
        let uri = "urn:ojies:test:actual-namespace";
        let actual_nsid = address_space.register_namespace(uri).unwrap();
        let mut namespaces = vec![server_discovery::NamespaceEntry {
            url: uri.to_string(),
            nsid: 99,
        }];

        synchronize_namespace_ids(&address_space, &mut namespaces).unwrap();

        assert_eq!(namespaces[0].nsid, actual_nsid);
    }
}

fn log_device_start(name: &str) {
    let path = COMPLETION_LOG_PATH
        .get()
        .expect("Completion-Log-Pfad nicht initialisiert");
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
    let line = format!("[{}] START {}\n", timestamp, name);

    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        Ok(mut file) => {
            if let Err(e) = file.write_all(line.as_bytes()) {
                error!("Fehler beim Schreiben des Start-Logs: {}", e);
            } else {
                info!("Start-Timestamp gespeichert fuer '{}': {}", name, timestamp);
            }
        }
        Err(e) => error!("Konnte '{}' nicht oeffnen: {}", path, e),
    }
}

fn log_device_aggregation(name: &str, finished_at: chrono::DateTime<Local>) {
    let path = COMPLETION_LOG_PATH
        .get()
        .expect("Completion-Log-Pfad nicht initialisiert");
    let timestamp = finished_at.format("%Y-%m-%d %H:%M:%S%.3f");
    let line = format!("[{}] DONE AGGREGATED  {}\n", timestamp, name);

    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        Ok(mut file) => {
            if let Err(e) = file.write_all(line.as_bytes()) {
                error!("Fehler beim Schreiben des Completion-Logs: {}", e);
            } else {
                info!(
                    "Completion-Timestamp gespeichert fuer '{}': {}",
                    name, timestamp
                );
            }
        }
        Err(e) => error!("Konnte '{}' nicht oeffnen: {}", path, e),
    }
}

fn log_device_completion(name: &str) {
    let path = COMPLETION_LOG_PATH
        .get()
        .expect("Completion-Log-Pfad nicht initialisiert");
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
    let line = format!("[{}] DONE WITH URDF  {}\n", timestamp, name);

    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        Ok(mut file) => {
            if let Err(e) = file.write_all(line.as_bytes()) {
                error!("Fehler beim Schreiben des Completion-Logs: {}", e);
            } else {
                info!(
                    "Completion-Timestamp gespeichert fuer '{}': {}",
                    name, timestamp
                );
            }
        }
        Err(e) => error!("Konnte '{}' nicht oeffnen: {}", path, e),
    }
}

fn synchronize_namespace_ids(
    address_space: &AddressSpace,
    namespaces: &mut [server_discovery::NamespaceEntry],
) -> Result<(), String> {
    for namespace in namespaces.iter_mut().filter(|entry| entry.nsid > 1) {
        namespace.nsid = server_discovery::nodeset_import::namespace_index_for_uri(
            address_space,
            &namespace.url,
        )
        .ok_or_else(|| {
            format!(
                "Namespace {} wurde nicht im Address Space registriert",
                namespace.url
            )
        })?;
    }
    Ok(())
}

fn onboarding_duration(variable: &str, default_ms: u64) -> Duration {
    let milliseconds = match std::env::var(variable) {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0 && *value <= 86_400_000)
            .unwrap_or_else(|| panic!("{variable} must be between 1 and 86400000 milliseconds")),
        Err(_) => default_ms,
    };
    Duration::from_millis(milliseconds)
}

fn admission_limit(name: &str, default: u64, min: u64, max: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|value| (*value >= min) && (*value <= max))
            .unwrap_or_else(|| panic!("{name} must be between {min} and {max}")),
        Err(_) => default,
    }
}

fn admission_stats_json(stats: &admission::QueueStats) -> serde_json::Value {
    serde_json::json!({
        "capacity":stats.capacity,"pending":stats.pending,"retrying":stats.retrying,
        "running":stats.running,"active":stats.active,"quarantined":stats.quarantined,
        "pending_removals":stats.pending_removals,"admitted_total":stats.admitted_total,
        "completed_total":stats.completed_total,"failed_attempts_total":stats.failed_attempts_total,
        "exhausted_total":stats.exhausted_total,"deferred_full_total":stats.deferred_full_total,
        "retired_total":stats.retired_total,"deferred_conflict_total":stats.deferred_conflict_total,
        "renewed_generations_total":stats.renewed_generations_total,
        "stale_finish_total":stats.stale_finish_total,"rebuild_deferred_total":stats.rebuild_deferred_total,
    })
}

fn admission_elapsed_at(at: Instant) -> u128 {
    at.saturating_duration_since(
        *ADMISSION_EPOCH
            .get()
            .expect("admission clock not initialized"),
    )
    .as_millis()
}

fn emit_admission_event(event: &str, mut fields: serde_json::Value) {
    fields["schema"] = serde_json::json!("ojies.admission/v1");
    fields["event"] = serde_json::json!(event);
    fields["timestamp_utc"] = serde_json::json!(chrono::Utc::now().to_rfc3339());
    let line = serde_json::to_string(&fields).expect("admission event serialization failed");
    info!("OJIES_ADMISSION_EVENT {}", line);
    if let Some(path) = ADMISSION_LOG_PATH.get() {
        let _guard = ADMISSION_LOG_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("admission log poisoned");
        if let Err(error) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .and_then(|mut file| writeln!(file, "{}", line))
        {
            error!("Admission evidence could not be written: {}", error);
        }
    }
}

fn emit_observation_events(
    summary: &admission::ObservationSummary,
    stats: &admission::QueueStats,
    authoritative: bool,
) {
    for item in &summary.admitted {
        emit_admission_event(
            "queued",
            serde_json::json!({
                "source":item.source.name,"endpoint":item.source.address,"generation":item.generation,
                "admitted_at_ms":admission_elapsed_at(item.first_observed_at),
                "enqueued_at_ms":admission_elapsed_at(item.enqueued_at),
                "queue":admission_stats_json(stats),
            }),
        );
    }
    for item in &summary.retired {
        emit_admission_event(
            "retired",
            serde_json::json!({
                "source":item.key.application_uri,"endpoint":item.key.canonical_endpoint,
                "generation":item.generation,"attempts":item.attempts,
                "admitted_at_ms":admission_elapsed_at(item.first_observed_at),
                "retired_at_ms":admission_elapsed_at(item.ended_at),
                "admitted_to_retirement_ms":item.ended_at.saturating_duration_since(item.first_observed_at).as_millis(),
            }),
        );
    }
    if summary.queued > 0
        || summary.deferred_full > 0
        || summary.deferred_conflict > 0
        || summary.removed_active > 0
        || summary.stale_running > 0
        || !summary.retired.is_empty()
    {
        emit_admission_event(
            "observation",
            serde_json::json!({
                "authoritative":authoritative,"queued":summary.queued,
                "deferred_full":summary.deferred_full,"deferred_conflict":summary.deferred_conflict,
                "removed_active":summary.removed_active,"stale_running":summary.stale_running,
                "renewed_generations":summary.renewed_generations,"deduplicated":summary.deduplicated,
                "quarantined":summary.quarantined,
                "queue":admission_stats_json(stats),
            }),
        );
    }
}

fn start_admission_observer(
    queue: Arc<Mutex<admission::Admission>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    current: Arc<Mutex<Option<SessionOperationControl>>>,
) -> thread::JoinHandle<()> {
    let budget = onboarding_duration("OJIES_DISCOVERY_BUDGET_MS", 15_000);
    let interval = onboarding_duration("OJIES_DISCOVERY_INTERVAL_MS", 1_000);
    let retry_maximum = onboarding_duration("OJIES_DISCOVERY_RETRY_MAX_MS", 30_000);
    thread::spawn(move || {
        let mut discovery = server_discovery::discovery::DiscoveryObserver::new();
        let mut backoff =
            server_discovery::recovery::DiscoveryBackoff::new(interval, retry_maximum);
        while !stop.load(std::sync::atomic::Ordering::Acquire) {
            let control = SessionOperationControl::new(budget);
            *current.lock().expect("observer control poisoned") = Some(control.clone());
            let observation = discovery.observe_resilient(control);
            *current.lock().expect("observer control poisoned") = None;
            if stop.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            let delay = backoff.after_observation(observation.is_ok());
            match observation {
                Ok(observation) => {
                    let sources = observation
                        .sources
                        .into_iter()
                        .map(|(name, address)| admission::Source::new(name, address))
                        .collect();
                    let (summary, stats) = {
                        let mut state = queue.lock().expect("admission queue poisoned");
                        let summary =
                            state.observe(sources, observation.authoritative, Instant::now());
                        (summary, state.stats())
                    };
                    emit_observation_events(&summary, &stats, observation.authoritative);
                }
                Err(error) => {
                    // A recovered LDS can have an empty registry until source
                    // renewals arrive. Unknown discovery must not evict healthy
                    // mappings merely because those renewals are delayed.
                    queue
                        .lock()
                        .expect("admission queue poisoned")
                        .mark_discovery_uncertain();
                    emit_admission_event(
                        "discovery_failed",
                        serde_json::json!({
                            "detail":error, "retry_after_ms":delay.as_millis(),
                        }),
                    );
                    match recover_owned_discovery_server() {
                        Ok(true) => emit_admission_event(
                            "discovery_restarted",
                            serde_json::json!({
                                "owner":"aggregation_process",
                            }),
                        ),
                        Ok(false) => {}
                        Err(error) => emit_admission_event(
                            "discovery_restart_failed",
                            serde_json::json!({
                                "detail":error, "retry_after_ms":delay.as_millis(),
                            }),
                        ),
                    }
                }
            }
            let until = Instant::now() + delay;
            while Instant::now() < until && !stop.load(std::sync::atomic::Ordering::Acquire) {
                thread::sleep(
                    Duration::from_millis(50).min(until.saturating_duration_since(Instant::now())),
                );
            }
        }
    })
}

struct OnboardingSnapshot {
    nodes: HashSet<NodeId>,
    imported_namespaces: HashSet<String>,
    imported_nodes: HashSet<NodeId>,
    generated_nodes: HashSet<NodeId>,
    entry_points: Option<Vec<server_discovery::NodeSetEntryPoints>>,
    files: Vec<(String, Option<Vec<u8>>)>,
    types: Vec<(u64, NodeId)>,
    standard_namespaces: Vec<opcua::server::aggregation_server::util_types::StandardizedNamespace>,
    incomplete: Vec<opcua::server::aggregation_server::util_types::IncompleteMapping>,
}

impl OnboardingSnapshot {
    fn capture(
        server: &Arc<RwLock<Server>>,
        aggregation: &Arc<
            Mutex<opcua::server::aggregation_server::aggregation_server::AggregationServer>,
        >,
    ) -> Result<Self, String> {
        let lifecycle = aggregation.lock().map_err(|e| e.to_string())?.clone();
        let files = ["config.json", "entry_points.json", "namespaces.json"]
            .into_iter()
            .map(|path| {
                let data = match std::fs::read(path) {
                    Ok(data) => Some(data),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                    Err(error) => return Err(error.to_string()),
                };
                Ok((path.to_string(), data))
            })
            .collect::<Result<_, String>>()?;
        let nodes = server
            .read()
            .address_space()
            .read()
            .aggregation_node_ids()
            .into_iter()
            .collect();
        let types = lifecycle
            .global_type_hashmap_p
            .read()
            .iter()
            .map(|(hash, id)| (*hash, id.clone()))
            .collect();
        let standard_namespaces = lifecycle.standard_namespaces_p.read().clone();
        let incomplete = lifecycle.incomplete_mappings_p.read().clone();
        Ok(Self {
            nodes,
            imported_namespaces: get_imported_namespaces()
                .lock()
                .map_err(|e| e.to_string())?
                .clone(),
            imported_nodes: get_imported_node_ids()
                .lock()
                .map_err(|e| e.to_string())?
                .clone(),
            generated_nodes: get_generated_rule_nodes()
                .lock()
                .map_err(|e| e.to_string())?
                .clone(),
            entry_points: get_entry_points_cache()
                .lock()
                .map_err(|e| e.to_string())?
                .clone(),
            files,
            types,
            standard_namespaces,
            incomplete,
        })
    }

    fn rollback(
        self,
        name: &str,
        server: &Arc<RwLock<Server>>,
        aggregation: &Arc<
            Mutex<opcua::server::aggregation_server::aggregation_server::AggregationServer>,
        >,
        source_added: bool,
    ) -> Result<(), String> {
        let lifecycle = aggregation.lock().map_err(|e| e.to_string())?.clone();
        // remove is a cancellation request; completion requires acknowledgement
        // from the actual writer, including its rollback and thread termination.
        if source_added {
            let _ = lifecycle.remove_lower_server(name);
            while !lifecycle.lower_server_cleanup_complete(name) {
                thread::sleep(Duration::from_millis(10));
            }
            let _ = lifecycle.remove_lower_server(name);
        }
        {
            let address_space = server.read().address_space();
            let mut space = address_space.write();
            let added: Vec<_> = space
                .aggregation_node_ids()
                .into_iter()
                .filter(|id| !self.nodes.contains(id))
                .collect();
            space.remove_aggregation_nodes_exact(&added);
        }
        {
            let mut types = lifecycle.global_type_hashmap_p.write();
            types.clear();
            for (hash, id) in self.types {
                types.insert(hash, id);
            }
        }
        *lifecycle.standard_namespaces_p.write() = self.standard_namespaces;
        *get_imported_namespaces()
            .lock()
            .map_err(|e| e.to_string())? = self.imported_namespaces;
        *get_imported_node_ids().lock().map_err(|e| e.to_string())? = self.imported_nodes;
        *get_generated_rule_nodes()
            .lock()
            .map_err(|e| e.to_string())? = self.generated_nodes;
        *get_entry_points_cache().lock().map_err(|e| e.to_string())? = self.entry_points;
        for (path, data) in self.files {
            match data {
                Some(bytes) => std::fs::write(&path, bytes).map_err(|e| e.to_string())?,
                None => {
                    if Path::new(&path).exists() {
                        std::fs::remove_file(&path).map_err(|e| e.to_string())?;
                    }
                }
            }
        }
        // Append rollback keeps old IDs stable. A replacement changes IDs, so
        // restore its canonical vector before publishing the saved pending IDs.
        let update = rule_store()?
            .rollback()
            .map_err(|error| error.to_string())?;
        match update {
            ExecutorUpdate::Truncate(len) => {
                lifecycle
                    .restore_pending_mappings_for_active_sources(self.incomplete)
                    .map_err(|error| error.to_string())?;
                apply_rule_update(&lifecycle, ExecutorUpdate::Truncate(len))?;
            }
            update => {
                apply_rule_update(&lifecycle, update)?;
                lifecycle
                    .restore_pending_mappings_for_active_sources(self.incomplete)
                    .map_err(|error| error.to_string())?;
            }
        }
        // NamespaceArray IDs are append-only and deliberately never reused.
        Ok(())
    }
}

fn onboarding_event(
    attempt: &str,
    name: &str,
    address: &str,
    event: &str,
    elapsed: Duration,
    detail: &str,
) {
    let path = ONBOARDING_LOG_PATH
        .get()
        .expect("onboarding log not configured");
    let record = serde_json::json!({
        "schema_version": 1, "attempt_id": attempt, "source": name, "endpoint": address,
        "event": event, "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
        "timestamp": Local::now().to_rfc3339(), "detail": detail,
    });
    let result = (|| -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        writeln!(file, "{record}")?;
        file.sync_data()
    })();
    if let Err(error) = result {
        eprintln!("Cannot preserve onboarding outcome: {error}");
        std::process::exit(74);
    }
}

fn handle_new_server(
    supervisor: &mut onboarding::SerialOnboarding,
    name: &str,
    address: &str,
    server: &Arc<RwLock<Server>>,
    aggregation: &Arc<
        Mutex<opcua::server::aggregation_server::aggregation_server::AggregationServer>,
    >,
    exporter: &Arc<server_discovery::UrdfExporter>,
    export_urdf: bool,
) -> bool {
    let attempt = format!(
        "{}-{}",
        Local::now().format("%Y%m%dT%H%M%S%.9f"),
        ATTEMPT_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let started = Instant::now();
    onboarding_event(
        &attempt,
        name,
        address,
        "started",
        Duration::ZERO,
        "serialized onboarding",
    );
    log_device_start(name);
    let snapshot = Arc::new(Mutex::new(None::<OnboardingSnapshot>));
    let saved = snapshot.clone();
    let source_added = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let worker_added = source_added.clone();
    let aggregation_finished_at = Arc::new(Mutex::new(None::<chrono::DateTime<Local>>));
    let worker_finished_at = aggregation_finished_at.clone();
    let worker_server = server.clone();
    let worker_aggregation = aggregation.clone();
    let worker_exporter = exporter.clone();
    let worker_name = name.to_string();
    let worker_address = address.to_string();
    let cleanup_server = server.clone();
    let cleanup_aggregation = aggregation.clone();
    let cleanup_name = name.to_string();
    let commit_name = name.to_string();
    let outcome = supervisor.run_with_commit(
        move |control| {
            // Reject duplicate registrations before mutating preparation state.
            let lifecycle = worker_aggregation
                .lock()
                .map_err(|e| e.to_string())?
                .clone();
            // Keep each read guard in its own statement: the lifecycle reaper
            // acquires related -> infos, so never hold infos while reading related.
            let source_registered = lifecycle
                .lower_servers_info_p
                .read()
                .contains_key(&worker_name);
            let worker_exists = lifecycle
                .lower_servers_related_p
                .read()
                .contains_key(&worker_name);
            if source_registered || worker_exists {
                return Err(format!("source '{}' is already registered", worker_name));
            }
            let before = OnboardingSnapshot::capture(&worker_server, &worker_aggregation)?;
            // Keep a cleanup owner even when opening the transaction fails.
            // An uncertain persistence state must stop the queue rather than
            // permit the next source to attempt another write.
            *saved.lock().map_err(|error| error.to_string())? = Some(before);
            rule_store()?
                .begin_transaction()
                .map_err(|error| error.to_string())?;
            let admission_preparation_started = Instant::now();
            server_discovery::config::prepare_admitted_source(
                "config.json",
                "namespaces.json",
                &worker_name,
                &worker_address,
                control.clone(),
            )?;
            log_phase_timing(
                &worker_name,
                "main_admission_prepare",
                admission_preparation_started.elapsed(),
            );
            if handle_new_server_work(
                &worker_name,
                &worker_address,
                &worker_server,
                &worker_aggregation,
                &worker_exporter,
                export_urdf,
                &control,
                &worker_added,
                &worker_finished_at,
            ) {
                Ok(())
            } else {
                Err("onboarding failed; see phase error in server log".into())
            }
        },
        move || {
            if let Some(before) = snapshot.lock().map_err(|e| e.to_string())?.take() {
                before.rollback(
                    &cleanup_name,
                    &cleanup_server,
                    &cleanup_aggregation,
                    source_added.load(std::sync::atomic::Ordering::Acquire),
                )?;
            }
            Ok(())
        },
        move || {
            let started = Instant::now();
            rule_store()?.commit().map_err(|error| error.to_string())?;
            log_phase_timing(&commit_name, "main_rule_commit", started.elapsed());
            Ok(())
        },
    );
    let (event, detail) = match &outcome {
        onboarding::Outcome::Completed => (
            "completed",
            "accepted before deadline and rule files committed",
        ),
        onboarding::Outcome::Failed(reason) => ("failed", reason.as_str()),
        onboarding::Outcome::TimedOut => ("timed_out", "worker stopped and rollback acknowledged"),
        onboarding::Outcome::UnsafeToContinue(reason) => ("fatal", reason.as_str()),
    };
    onboarding_event(&attempt, name, address, event, started.elapsed(), detail);
    match outcome {
        onboarding::Outcome::Completed => {
            // Publish the actual aggregation timestamp only after the parent
            // accepted the entire attempt; optional export time stays separate.
            let finished_at = aggregation_finished_at
                .lock()
                .expect("aggregation timestamp mutex poisoned")
                .take()
                .expect("completed attempt must have an aggregation timestamp");
            log_device_aggregation(name, finished_at);
            if export_urdf {
                log_device_completion(name);
            }
            true
        }
        onboarding::Outcome::UnsafeToContinue(reason) => {
            // A Rust thread cannot safely be killed while it owns shared state.
            // Do not acquire any of its locks during this fail-stop path.
            eprintln!("ONBOARDING FAIL-STOP: {reason}; supervisor restart required");
            std::process::exit(70);
        }
        _ => false,
    }
}

fn handle_new_server_work(
    name: &str,
    address: &str,
    server_p: &Arc<RwLock<Server>>,
    aggregation_server: &Arc<
        Mutex<opcua::server::aggregation_server::aggregation_server::AggregationServer>,
    >,
    urdf_exporter: &Arc<server_discovery::UrdfExporter>,
    export_urdf: bool,
    control: &SessionOperationControl,
    source_added: &std::sync::atomic::AtomicBool,
    aggregation_finished_at: &Mutex<Option<chrono::DateTime<Local>>>,
) -> bool {
    let total_started = Instant::now();
    let namespace_load_started = Instant::now();
    if control.check().is_err() {
        return false;
    }
    info!("Lade Namespaces fuer {}...", name);
    let mut namespaces = match server_discovery::load_namespaces("namespaces.json") {
        Ok(ns) => {
            info!("{} Namespaces geladen", ns.len());
            ns
        }
        Err(e) => {
            error!("Fehler beim Laden der Namespaces: {}", e);
            return false;
        }
    };
    log_phase_timing(
        name,
        "main_namespace_load",
        namespace_load_started.elapsed(),
    );

    let preparation_started = Instant::now();
    let executor_update;
    {
        let server_guard = server_p.read();
        let address_space_p = server_guard.address_space();
        let mut address_space = address_space_p.write();

        info!("Registriere Namespaces im Address Space...");
        let mut registered_count = 0;
        for ns_entry in &namespaces {
            if ns_entry.nsid <= 1 {
                continue;
            }
            if server_discovery::nodeset_import::namespace_index_for_uri(
                &address_space,
                &ns_entry.url,
            )
            .is_some()
            {
                info!("Namespace bereits vorhanden: {}", ns_entry.url);
                continue;
            }
            match address_space.register_namespace(&ns_entry.url) {
                Ok(registered_nsid) => {
                    info!(
                        "Namespace registriert: {} -> nsid:{}",
                        ns_entry.url, registered_nsid
                    );
                    registered_count += 1;
                }
                Err(_) => {
                    info!("Namespace bereits vorhanden: {}", ns_entry.url);
                }
            }
        }
        info!("{} neue Namespaces registriert", registered_count);

        if let Err(error) = synchronize_namespace_ids(&address_space, &mut namespaces) {
            error!(
                "Namespace-IDs konnten nicht synchronisiert werden: {}",
                error
            );
            return false;
        }

        // Schreibsperre: verhindert gleichzeitiges Schreiben in JSON-Dateien durch parallele Server-Threads
        let _json_lock = get_json_write_lock()
            .lock()
            .expect("Failed to lock JSON write mutex");
        if let Err(error) =
            server_discovery::config::save_namespaces("namespaces.json", &namespaces)
        {
            error!(
                "Tatsaechliche Namespace-IDs konnten nicht gespeichert werden: {}",
                error
            );
            return false;
        }

        // Nur Namespaces importieren, die noch NICHT in den Address Space importiert wurden
        let new_namespaces: Vec<_> = {
            let imported = get_imported_namespaces()
                .lock()
                .expect("Failed to lock imported namespaces");
            namespaces
                .iter()
                .filter(|ns| {
                    ns.nsid > 1
                        && !imported.contains(
                            &server_discovery::companion_specs::normalize_namespace_uri(&ns.url),
                        )
                })
                .cloned()
                .collect()
        };

        if new_namespaces.is_empty() {
            info!("Alle Namespaces bereits importiert – NodeSet-Import und Entry-Point-Suche uebersprungen");
        } else {
            info!("Importiere {} neue NodeSet(s)...", new_namespaces.len());

            let imported_node_ids = match server_discovery::import_nodesets_to_address_space(
                &mut address_space,
                &new_namespaces,
            ) {
                Ok(node_ids) => node_ids,
                Err(e) => {
                    error!("NodeSet-Import fehlgeschlagen: {}", e);
                    return false;
                }
            };
            get_imported_node_ids()
                .lock()
                .expect("Failed to lock imported NodeIds")
                .extend(imported_node_ids);

            // Erfolgreiche Imports in der globalen Menge vermerken
            {
                let mut imported = get_imported_namespaces()
                    .lock()
                    .expect("Failed to lock imported namespaces");
                for ns in &new_namespaces {
                    imported.insert(server_discovery::companion_specs::normalize_namespace_uri(
                        &ns.url,
                    ));
                }
            }
            info!("NodeSets erfolgreich importiert");

            info!("Suche Entry-Points...");
            let entry_points_for_save = server_discovery::find_all_entry_points(&namespaces);
            if let Err(e) = server_discovery::save_entry_points_to_json(
                &entry_points_for_save,
                "entry_points.json",
            ) {
                error!("Entry-Points konnten nicht gespeichert werden: {}", e);
                return false;
            } else {
                let total: usize = entry_points_for_save
                    .iter()
                    .map(|ep| ep.entry_points.len())
                    .sum();
                info!("{} Entry-Points gespeichert", total);
            }
            *get_entry_points_cache()
                .lock()
                .expect("Failed to lock entry-point cache") = Some(entry_points_for_save);
        }

        // Aggregationsregeln werden fuer JEDEN neuen Server generiert
        log_phase_timing(name, "main_preparation", preparation_started.elapsed());
        let rule_generation_started = Instant::now();
        info!(
            "Generiere Aggregationsregeln durch Browsen von {}...",
            address
        );
        let mut entry_points = {
            let mut cache = get_entry_points_cache()
                .lock()
                .expect("Failed to lock entry-point cache");
            if cache.is_none() {
                *cache = Some(server_discovery::find_all_entry_points(&namespaces));
            }
            cache
                .as_ref()
                .expect("entry-point cache was initialized above")
                .clone()
        };
        match server_discovery::rule_generator::generate_rules_delta_for_server_with_control(
            address,
            &mut entry_points,
            &mut address_space,
            control.clone(),
        ) {
            Err(e) => {
                error!("Regelgenerierung fehlgeschlagen: {}", e);
                return false;
            }
            Ok(generated) => {
                let persist_started = Instant::now();
                executor_update = match rule_store().and_then(|mut store| {
                    let update = store.apply(&generated.rules, &generated.shadowed_rules)
                        .map_err(|error| error.to_string())?;
                    if phase_timing_enabled() {
                        let mode = match &update {
                            ExecutorUpdate::Append(_) => "append",
                            ExecutorUpdate::Replace(_) => "replace",
                            ExecutorUpdate::Truncate(_) => "truncate",
                        };
                        warn!(
                            "OJIES_RULE_UPDATE source={} mode={} incoming={} retained_rich={} retained_executor={} cumulative_full_rebuilds={}",
                            address, mode, generated.rules.len(), store.rich_rules().len(),
                            store.executor_rules().len(), store.operation_counts().full_rebuilds,
                        );
                    }
                    Ok(update)
                }) {
                    Ok(update) => update,
                    Err(error) => {
                        error!("Incremental rule publication failed: {}", error);
                        return false;
                    }
                };
                log_phase_timing(address, "rule_merge_persist", persist_started.elapsed());
                log_phase_timing(address, "rule_total", rule_generation_started.elapsed());
                let mut generated_nodes = get_generated_rule_nodes()
                    .lock()
                    .expect("Failed to lock generated rule nodes");
                let old_count = generated_nodes.len();
                generated_nodes.extend(generated.created_node_ids);
                info!(
                    "{} Rule-Generator-Knoten registriert (gesamt: {})",
                    generated_nodes.len() - old_count,
                    generated_nodes.len()
                );
                info!("Regelgenerierung abgeschlossen");
            }
        }
        log_phase_timing(
            name,
            "main_rule_generation",
            rule_generation_started.elapsed(),
        );
        // _json_lock wird hier automatisch freigegeben
    }

    let rule_reload_started = Instant::now();
    {
        // Do not hold an address-space guard while acquiring the onboarding gate.
        let agg_server = aggregation_server
            .lock()
            .expect("Failed to lock aggregation server");
        if let Err(error) = apply_rule_update(&agg_server, executor_update) {
            error!("Incremental runtime rule update failed: {}", error);
            return false;
        }
    }

    {
        let mut agg_server = aggregation_server
            .lock()
            .expect("Failed to lock aggregation server");
        if let Err(e) = agg_server.load_standardized_namespaces(Path::new("namespaces.json")) {
            error!(
                "Fehler beim Aktualisieren der Namespaces im AggServer: {}",
                e
            );
            return false;
        } else {
            info!("Namespaces im Aggregation Server aktualisiert");
        }
    }
    log_phase_timing(name, "main_rule_reload", rule_reload_started.elapsed());

    let connection_test_started = Instant::now();
    info!("Teste Verbindung zu {}...", address);
    let mut test_client = match ClientBuilder::new()
        .operation_control(control.clone())
        .request_timeout(5_000)
        .application_name("Endpoint Tester")
        .application_uri("urn:test")
        .trust_server_certs(true)
        .session_retry_limit(1)
        .client()
    {
        Some(c) => c,
        None => {
            error!("Konnte Test-Client nicht erstellen");
            return false;
        }
    };

    let discovery_endpoint: EndpointDescription = (
        address,
        "None",
        MessageSecurityMode::None,
        UserTokenPolicy::anonymous(),
    )
        .into();

    match test_client.connect_to_endpoint(discovery_endpoint, IdentityToken::Anonymous) {
        Ok(session) => {
            info!("Verbindung erfolgreich");
            drop(session);
            drop(test_client);
            log_phase_timing(
                name,
                "main_connection_test",
                connection_test_started.elapsed(),
            );

            info!("Fuege '{}' zum Aggregation Server hinzu...", name);
            let lower_aggregation_started = Instant::now();
            {
                let agg_server = aggregation_server
                    .lock()
                    .expect("Failed to lock aggregation server");
                match agg_server.add_lower_server_with_control(
                    server_p,
                    address,
                    name,
                    control.clone(),
                ) {
                    Ok(_) => {
                        source_added.store(true, std::sync::atomic::Ordering::Release);
                        info!("Server hinzugefuegt: {}", name);
                    }
                    Err(e) => {
                        error!("Fehler beim Hinzufügen von {}: {:?}", name, e);
                        return false;
                    }
                }
            } // ← agg_server Lock wird hier freigegeben

            info!("Warte auf echten Aggregationsabschluss fuer '{}'...", name);
            let lifecycle = aggregation_server
                .lock()
                .expect("aggregation mutex poisoned")
                .clone();
            if let Err(error) = wait_for_lower_server_aggregation(&lifecycle, name, control) {
                error!("Aggregation fuer '{}' fehlgeschlagen: {}", name, error);
                return false;
            }

            *aggregation_finished_at
                .lock()
                .expect("aggregation timestamp mutex poisoned") = Some(Local::now());

            log_phase_timing(
                name,
                "main_lower_aggregation_wait",
                lower_aggregation_started.elapsed(),
            );

            // URDF-Export läuft NACH dem Lock – andere Threads können jetzt add_lower_server aufrufen
            if export_urdf {
                info!("Starte URDF-Export fuer '{}'...", name);
                if let Err(e) = urdf_exporter.export_and_move(address, name) {
                    error!("URDF-Export fuer '{}' fehlgeschlagen: {}", name, e);
                    return false;
                }
            } else {
                info!("URDF-Export fuer '{}' uebersprungen", name);
            }

            log_phase_timing(name, "main_total", total_started.elapsed());
            true
        }
        Err(e) => {
            error!("Verbindung zu '{}' fehlgeschlagen: {:?}", name, e);
            false
        }
    }
}

#[cfg(test)]
mod dynamic_cleanup_tests {
    use super::*;
    use opcua::types::{QualifiedName, ReferenceTypeId};

    #[test]
    fn cyclic_cleanup_deletes_each_dynamic_node_once_and_preserves_standard_nodes() {
        let mut address_space = AddressSpace::new();
        let ns = address_space
            .register_namespace("urn:test:cyclic-cleanup")
            .unwrap();
        let a = NodeId::new(ns, "a");
        let b = NodeId::new(ns, "b");
        let objects = NodeId::objects_folder_id();
        assert!(address_space.add_folder_with_id(&a, QualifiedName::new(ns, "a"), "a", &objects));
        assert!(address_space.add_folder_with_id(&b, QualifiedName::new(ns, "b"), "b", &a));
        address_space.insert_reference(&b, &a, ReferenceTypeId::Organizes);
        address_space.insert_reference(&b, &objects, ReferenceTypeId::Organizes);
        assert_eq!(delete_hierarchical_subtree(&mut address_space, &a), 2);
        assert!(address_space.find_node(&a).is_none());
        assert!(address_space.find_node(&b).is_none());
        assert!(address_space.find_node(&objects).is_some());
    }
}

#[cfg(test)]
mod startup_input_tests {
    use super::*;

    #[test]
    fn detached_startup_disables_export_at_eof_without_repeating_prompt() {
        let mut output = Vec::new();
        assert!(!ask_for_urdf_export(&mut io::Cursor::new(b""), &mut output).unwrap());
        assert_eq!(
            String::from_utf8(output).unwrap().matches("[j/n]").count(),
            1
        );
    }

    #[test]
    fn interactive_export_choice_retries_invalid_input() {
        let mut output = Vec::new();
        assert!(ask_for_urdf_export(&mut io::Cursor::new(b"invalid\nJa\n"), &mut output).unwrap());
    }
}
