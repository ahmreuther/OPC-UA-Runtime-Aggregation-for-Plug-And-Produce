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
pub mod server_discovery;

static COMPLETION_LOG_PATH: OnceLock<String> = OnceLock::new();
static LDS_PROCESS: OnceLock<Mutex<Option<Child>>> = OnceLock::new();
static PHASE_TIMING_ENABLED: OnceLock<bool> = OnceLock::new();
const URDF_MAX_BYTE_STRING_LENGTH: usize = 16 * 1024 * 1024;
const URDF_MAX_MESSAGE_SIZE: usize = 20 * 1024 * 1024;

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

fn start_local_discovery_server() -> Result<(), String> {
    if lds_is_reachable() {
        info!("LDS laeuft bereits auf opc.tcp://localhost:4840");
        return Ok(());
    }

    let executable = find_lds_executable()
        .ok_or_else(|| "discovery_server_lds wurde im Projekt nicht gefunden".to_string())?;
    let working_dir = executable
        .parent()
        .ok_or_else(|| "LDS-Arbeitsverzeichnis konnte nicht bestimmt werden".to_string())?;

    let mut child = Command::new(&executable)
        .current_dir(working_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| format!("LDS konnte nicht gestartet werden: {}", error))?;

    for _ in 0..50 {
        if lds_is_reachable() {
            info!("LDS gestartet: opc.tcp://localhost:4840");
            *LDS_PROCESS
                .get_or_init(|| Mutex::new(None))
                .lock()
                .map_err(|_| "LDS-Prozesssperre ist vergiftet".to_string())? = Some(child);
            return Ok(());
        }

        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("LDS-Status konnte nicht gelesen werden: {}", error))?
        {
            return Err(format!("LDS wurde vorzeitig beendet: {}", status));
        }

        thread::sleep(Duration::from_millis(100));
    }

    let _ = child.kill();
    let _ = child.wait();
    Err("LDS war nach 5 Sekunden nicht auf Port 4840 erreichbar".to_string())
}

fn stop_local_discovery_server() {
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
) {
    for _ in 0..40 {
        let pending = {
            let agg_server = aggregation_server
                .lock()
                .expect("Failed to lock aggregation server");
            let infos = agg_server.lower_servers_info_p.read();
            server_names
                .iter()
                .filter(|name| infos.contains_key(*name))
                .cloned()
                .collect::<Vec<_>>()
        };

        if pending.is_empty() {
            return;
        }

        thread::sleep(Duration::from_millis(250));
    }

    warn!(
        "Lower-Server-Cleanup nach 10 Sekunden noch nicht abgeschlossen: {:?}",
        server_names
    );
}

fn wait_for_lower_server_aggregation(
    aggregation_server: &Arc<
        Mutex<opcua::server::aggregation_server::aggregation_server::AggregationServer>,
    >,
    server_name: &str,
) -> Result<(), String> {
    const MAX_POLLS: usize = 2400;

    for _ in 0..MAX_POLLS {
        let state = {
            let agg_server = aggregation_server
                .lock()
                .expect("Failed to lock aggregation server");
            let infos = agg_server.lower_servers_info_p.read();
            infos
                .get(server_name)
                .map(|info| (info.aggregation_finished, info.removal_in_progress))
        };

        match state {
            Some((true, false)) => return Ok(()),
            Some((_, true)) => {
                return Err(format!(
                    "Lower-Server '{}' wurde waehrend der Aggregation entfernt",
                    server_name
                ));
            }
            Some((false, false)) => thread::sleep(Duration::from_millis(250)),
            None => {
                return Err(format!(
                    "Lower-Server '{}' ist vor Abschluss der Aggregation verschwunden",
                    server_name
                ));
            }
        }
    }

    Err(format!(
        "Aggregation fuer '{}' war nach 600 Sekunden nicht abgeschlossen",
        server_name
    ))
}

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
    let children = address_space
        .find_hierarchical_references(node_id)
        .unwrap_or_default();
    let mut deleted_nodes = children
        .iter()
        .map(|child| delete_hierarchical_subtree(address_space, child))
        .sum();

    if address_space.delete(node_id, true) {
        deleted_nodes += 1;
    }

    deleted_nodes
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
    rules_path: &str,
    lower_server_names: &[String],
) {
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

    {
        let _json_lock = get_json_write_lock()
            .lock()
            .expect("Failed to lock JSON write mutex");
        let base_namespaces = serde_json::json!([
            {
                "url": "http://opcfoundation.org/UA/",
                "nsid": 0
            }
        ]);

        if let Err(error) = std::fs::write(
            rules_path, "[]
",
        ) {
            error!(
                "rules.json konnte beim Cleanup nicht geleert werden: {}",
                error
            );
        }
        if let Err(error) = std::fs::write(
            "rules_executor.json",
            "[]
",
        ) {
            error!(
                "rules_executor.json konnte beim Cleanup nicht geleert werden: {}",
                error
            );
        }
        if let Err(error) = std::fs::write(
            "entry_points.json",
            "[]
",
        ) {
            error!(
                "entry_points.json konnte beim Cleanup nicht geleert werden: {}",
                error
            );
        }
        if let Err(error) = std::fs::write(
            "namespaces.json",
            serde_json::to_string_pretty(&base_namespaces)
                .expect("Basis-Namespaces konnten nicht serialisiert werden"),
        ) {
            error!(
                "namespaces.json konnte beim Cleanup nicht geleert werden: {}",
                error
            );
        }
    }

    let mut agg_server = aggregation_server
        .lock()
        .expect("Failed to lock aggregation server");
    agg_server.incomplete_mappings_p.write().clear();

    if let Err(error) = agg_server.load_mapping_rules(Path::new("rules_executor.json")) {
        error!(
            "Leere Mapping Rules konnten nicht geladen werden: {}",
            error
        );
    }
    if let Err(error) = agg_server.load_standardized_namespaces(Path::new("namespaces.json")) {
        error!("Basis-Namespaces konnten nicht geladen werden: {}", error);
    }

    info!(
        "Dynamischer Aggregationszustand bereinigt: {} lokale/importierte und {} Lower-Server-Knoten entfernt",
        deleted_nodes, deleted_lower_server_nodes
    );
}

fn rebuild_namespace_state(hosts: &[server_discovery::Host]) {
    for host in hosts {
        match server_discovery::get_namespaces(&host.address) {
            Ok(namespaces) => {
                if let Err(error) =
                    server_discovery::update_namespaces_config("namespaces.json", namespaces)
                {
                    error!(
                        "Namespaces von {} konnten nicht neu aufgebaut werden: {}",
                        host.name, error
                    );
                }
            }
            Err(error) => {
                error!(
                    "Namespaces von verbleibendem Server {} konnten nicht gelesen werden: {}",
                    host.name, error
                );
            }
        }
    }
}
fn main() {
    // Dienste können die interaktive Abfrage über OJIES_EXPORT_URDF umgehen.
    let configured_export = std::env::var("OJIES_EXPORT_URDF").ok().and_then(|value| {
        match value.trim().to_lowercase().as_str() {
            "1" | "true" | "j" | "ja" | "y" | "yes" => Some(true),
            "0" | "false" | "n" | "nein" | "no" => Some(false),
            _ => None,
        }
    });
    let export_urdf: bool = configured_export.unwrap_or_else(|| loop {
        print!("URDF-Export aktivieren? [j/n]: ");
        io::stdout().flush().expect("stdout flush fehlgeschlagen");
        let mut input = String::new();
        io::stdin()
            .lock()
            .read_line(&mut input)
            .expect("Fehler beim Lesen der Eingabe");
        match input.trim().to_lowercase().as_str() {
            "j" | "ja" | "y" | "yes" => {
                println!("URDF-Export: AKTIVIERT");
                break true;
            }
            "n" | "nein" | "no" => {
                println!("URDF-Export: DEAKTIVIERT");
                break false;
            }
            _ => println!("Bitte 'j' oder 'n' eingeben."),
        }
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

        if let Err(e) = agg_server.load_mapping_rules(Path::new("rules_executor.json")) {
            warn!("Fehler beim Laden der Mapping Rules: {:?}", e);
        } else {
            info!("Mapping Rules geladen");
        }

        if let Err(e) = agg_server.load_standardized_namespaces(Path::new("namespaces.json")) {
            error!("Fehler beim Laden der Namespaces: {:?}", e);
        } else {
            info!("Standardized Namespaces geladen");
        }
    }

    info!("Fuege initiale Server aus Config hinzu...");
    for host in &config.hosts {
        info!("Pruefe Server: {} ({})", host.name, host.address);
        handle_new_server(
            &host.name,
            &host.address,
            &server_p,
            &aggregation_server,
            &urdf_exporter,
            export_urdf,
        );
    }

    info!("Server laeuft - starte Discovery-Loop...");

    const DISCOVERY_INTERVAL_SECS: u64 = 1;

    loop {
        if server_task.is_finished() {
            warn!("Server Thread wurde beendet");
            break;
        }

        info!("Starte Discovery-Zyklus...");

        match server_discovery::run_discovery_cycle("config.json", "namespaces.json") {
            Err(e) => {
                error!("Discovery-Zyklus fehlgeschlagen: {}", e);
            }
            Ok((removed_servers, mut new_servers)) => {
                if !removed_servers.is_empty() {
                    info!(
                        "{} Server entfernt; dynamische Aggregationsschicht wird neu aufgebaut",
                        removed_servers.len()
                    );

                    let active_server_names = {
                        let agg_server = aggregation_server
                            .lock()
                            .expect("Failed to lock aggregation server");
                        let infos = agg_server.lower_servers_info_p.read();
                        infos.keys().cloned().collect::<Vec<_>>()
                    };
                    let cleanup_server_names = removed_servers
                        .iter()
                        .chain(active_server_names.iter())
                        .cloned()
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>();

                    for server_name in &active_server_names {
                        let agg_server = aggregation_server
                            .lock()
                            .expect("Failed to lock aggregation server");
                        match agg_server.remove_lower_server(server_name) {
                            Ok(()) => info!("Cleanup fuer {} angefordert", server_name),
                            Err(error) => warn!(
                                "Lower Server {} war beim Cleanup bereits entfernt: {:?}",
                                server_name, error
                            ),
                        }
                    }

                    wait_for_lower_server_cleanup(&aggregation_server, &active_server_names);
                    clear_dynamic_aggregation_state(
                        &server_p,
                        &aggregation_server,
                        "rules.json",
                        &cleanup_server_names,
                    );

                    match server_discovery::load_config("config.json") {
                        Ok(current_config) => {
                            rebuild_namespace_state(&current_config.hosts);
                            new_servers = current_config
                                .hosts
                                .into_iter()
                                .map(|host| (host.name, host.address))
                                .collect();
                        }
                        Err(error) => {
                            error!(
                                "Config konnte nach Server-Entfernung nicht geladen werden: {}",
                                error
                            );
                            new_servers.clear();
                        }
                    }
                }

                match server_discovery::load_config("config.json") {
                    Ok(current_config) => {
                        let active_server_names = {
                            let agg_server = aggregation_server
                                .lock()
                                .expect("Failed to lock aggregation server");
                            let names = agg_server
                                .lower_servers_info_p
                                .read()
                                .keys()
                                .cloned()
                                .collect::<HashSet<_>>();
                            names
                        };
                        let retry_count = append_missing_configured_servers(
                            &current_config.hosts,
                            &active_server_names,
                            &mut new_servers,
                            server_discovery::endpoint_is_reachable,
                        );
                        if retry_count > 0 {
                            warn!(
                                "{} konfigurierte Quelle(n) ohne aktiven Lower-Server werden erneut aufgenommen",
                                retry_count
                            );
                        }
                    }
                    Err(error) => warn!(
                        "Aktive Lower-Server konnten nicht mit config.json abgeglichen werden: {}",
                        error
                    ),
                }

                if new_servers.is_empty() {
                    info!("Keine neuen Server gefunden.");
                } else {
                    info!(
                        "{} neuer/neue Server gefunden - verarbeite sequenziell...",
                        new_servers.len()
                    );
                    // Jeder Lower-Server-Thread schreibt in dieselbe SQLite-Mapping-Datenbank.
                    // Erst nach aggregation_finished darf der naechste Writer starten.
                    for (name, address) in new_servers {
                        info!("NEUER SERVER: {} ({})", name, address);
                        if handle_new_server(
                            &name,
                            &address,
                            &server_p,
                            &aggregation_server,
                            &urdf_exporter,
                            export_urdf,
                        ) {
                            info!("Server vollstaendig verarbeitet: {}", name);
                        } else {
                            error!("Serververarbeitung fehlgeschlagen: {}", name);
                        }
                    }
                }
            }
        }

        info!(
            "Naechster Discovery-Zyklus in {}s...",
            DISCOVERY_INTERVAL_SECS
        );
        thread::sleep(Duration::from_secs(DISCOVERY_INTERVAL_SECS));
    }

    stop_local_discovery_server();
    info!("Aggregation Server wurde beendet");
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
            .host_and_port("127.0.0.1", 48400)
            .discovery_urls(vec!["/".to_string()])
            .config();

        assert_eq!(
            config.discovery_urls,
            vec!["opc.tcp://127.0.0.1:48400/".to_string()]
        );
        assert_eq!(config.base_endpoint_url(), "opc.tcp://127.0.0.1:48400");
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

fn log_device_aggregation(name: &str) {
    let path = COMPLETION_LOG_PATH
        .get()
        .expect("Completion-Log-Pfad nicht initialisiert");
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
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

fn handle_new_server(
    name: &str,
    address: &str,
    server_p: &Arc<RwLock<Server>>,
    aggregation_server: &Arc<
        Mutex<opcua::server::aggregation_server::aggregation_server::AggregationServer>,
    >,
    urdf_exporter: &Arc<server_discovery::UrdfExporter>,
    export_urdf: bool,
) -> bool {
    let total_started = Instant::now();
    let namespace_load_started = Instant::now();
    log_device_start(name);
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
        match server_discovery::generate_rules_for_server(
            address,
            &mut entry_points,
            &mut address_space,
            "rules.json",
        ) {
            Err(e) => error!("Regelgenerierung fehlgeschlagen: {}", e),
            Ok(created_node_ids) => {
                let mut generated_nodes = get_generated_rule_nodes()
                    .lock()
                    .expect("Failed to lock generated rule nodes");
                let old_count = generated_nodes.len();
                generated_nodes.extend(created_node_ids);
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
        let mut agg_server = aggregation_server
            .lock()
            .expect("Failed to lock aggregation server");
        if let Err(e) = agg_server.load_mapping_rules(Path::new("rules_executor.json")) {
            warn!("Fehler beim Laden der Mapping Rules: {:?}", e);
        } else {
            info!("Mapping Rules geladen");
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
        } else {
            info!("Namespaces im Aggregation Server aktualisiert");
        }
    }
    log_phase_timing(name, "main_rule_reload", rule_reload_started.elapsed());

    let connection_test_started = Instant::now();
    info!("Teste Verbindung zu {}...", address);
    let mut test_client = match ClientBuilder::new()
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
                match agg_server.add_lower_server(server_p, address, name) {
                    Ok(_) => {
                        info!("Server hinzugefuegt: {}", name);
                    }
                    Err(e) => {
                        error!("Fehler beim Hinzufügen von {}: {:?}", name, e);
                        return false;
                    }
                }
            } // ← agg_server Lock wird hier freigegeben

            info!("Warte auf echten Aggregationsabschluss fuer '{}'...", name);
            if let Err(error) = wait_for_lower_server_aggregation(aggregation_server, name) {
                error!("Aggregation fuer '{}' fehlgeschlagen: {}", name, error);
                return false;
            }
            log_device_aggregation(name);
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
                }
            } else {
                info!("URDF-Export fuer '{}' uebersprungen", name);
            }

            log_device_completion(name);
            log_phase_timing(name, "main_total", total_started.elapsed());
            true
        }
        Err(e) => {
            error!("Verbindung zu '{}' fehlgeschlagen: {:?}", name, e);
            false
        }
    }
}
