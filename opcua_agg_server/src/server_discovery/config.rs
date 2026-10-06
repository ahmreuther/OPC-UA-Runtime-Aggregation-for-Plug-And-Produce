// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

use lazy_static::lazy_static;
use opcua::types::service_types::FindServersResponse;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use crate::server_discovery::companion_specs::{
    download_nodeset_for_spec, extract_spec_name_from_url, get_sorted_namespaces_with_requirements,
    is_companion_spec_namespace, normalize_namespace_uri,
};
use crate::server_discovery::discovery::get_namespaces;

// FILE LOCKS für Thread-sichere JSON-Operationen
lazy_static! {
    pub(crate) static ref CONFIG_FILE_LOCK: Mutex<()> = Mutex::new(());
    pub(crate) static ref NAMESPACES_FILE_LOCK: Mutex<()> = Mutex::new(());
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    pub port: u16,
    #[serde(default = "default_aggregation_host")]
    pub host: String,
    #[serde(default = "default_github_branch")]
    pub opcua_github_branch: String,
    pub hosts: Vec<Host>,
}

fn default_aggregation_host() -> String {
    "127.0.0.1".to_string()
}

fn default_github_branch() -> String {
    "v1.04".to_string()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Host {
    pub name: String,
    pub address: String,
}

fn reconcile_configured_hosts(
    hosts: &mut Vec<Host>,
    discovered_servers: &std::collections::HashMap<String, (String, String)>,
) -> Vec<String> {
    let mut removed_servers = Vec::new();

    hosts.retain_mut(|host| {
        let Some((discovered_name, _)) = discovered_servers.get(&host.address) else {
            println!(
                "Server nicht mehr verfuegbar: {} ({})",
                host.name, host.address
            );
            removed_servers.push(host.name.clone());
            return false;
        };

        if host.name != *discovered_name {
            let previous_name = std::mem::replace(&mut host.name, discovered_name.clone());
            println!(
                "Serveridentitaet aktualisiert: {} -> {} ({})",
                previous_name, host.name, host.address
            );
            removed_servers.push(previous_name);
        }

        true
    });

    removed_servers
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NamespaceEntry {
    pub url: String,
    pub nsid: u16,
}

fn get_namespaces_with_retry(address: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut last_error: Option<Box<dyn std::error::Error>> = None;

    for attempt in 1..=4 {
        match get_namespaces(address) {
            Ok(namespaces) => return Ok(namespaces),
            Err(error) => {
                if attempt == 1 || attempt == 4 {
                    eprintln!(
                        "Namespace-Abfrage fehlgeschlagen (Versuch {}/4) fuer {}: {}",
                        attempt, address, error
                    );
                }
                last_error = Some(error);

                if attempt < 4 {
                    thread::sleep(Duration::from_millis(400 * attempt as u64));
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| "Namespace-Abfrage fehlgeschlagen".into()))
}

/// Prepare one admitted source under the serial onboarding supervisor.
/// Observation never calls this function. The caller snapshots config and
/// namespace state before entry and restores it if the attempt is rejected.
pub fn prepare_admitted_source(
    config_path: &str,
    namespaces_path: &str,
    name: &str,
    address: &str,
    control: opcua::client::session::SessionOperationControl,
) -> Result<(), String> {
    if name.is_empty() || address.is_empty() {
        return Err("source identity and endpoint must not be empty".into());
    }
    control.check().map_err(|error| error.to_string())?;
    let namespaces =
        crate::server_discovery::discovery::get_namespaces_controlled(address, control.clone())
            .map_err(|error| error.to_string())?;
    persist_admitted_source(
        config_path,
        namespaces_path,
        name,
        address,
        namespaces,
        control,
    )
}

fn persist_admitted_source(
    config_path: &str,
    namespaces_path: &str,
    name: &str,
    address: &str,
    namespaces: Vec<String>,
    control: opcua::client::session::SessionOperationControl,
) -> Result<(), String> {
    control.check().map_err(|error| error.to_string())?;
    let _lock = CONFIG_FILE_LOCK.lock().map_err(|error| error.to_string())?;
    let mut config = load_config_unlocked(config_path).map_err(|error| error.to_string())?;
    check_admission_identity(&config.hosts, name, address)?;
    update_namespaces_config(namespaces_path, namespaces).map_err(|error| error.to_string())?;
    control.check().map_err(|error| error.to_string())?;
    if !config
        .hosts
        .iter()
        .any(|host| host.name == name && same_endpoint(&host.address, address))
    {
        config.hosts.push(Host {
            name: name.into(),
            address: address.into(),
        });
    }
    let data = serde_json::to_string_pretty(&config).map_err(|error| error.to_string())?;
    fs::write(config_path, data).map_err(|error| error.to_string())
}

fn same_endpoint(left: &str, right: &str) -> bool {
    left.trim_end_matches('/') == right.trim_end_matches('/')
}

fn check_admission_identity(hosts: &[Host], name: &str, address: &str) -> Result<(), String> {
    if hosts.iter().any(|host| {
        (host.name == name && !same_endpoint(&host.address, address))
            || (host.name != name && same_endpoint(&host.address, address))
    }) {
        return Err("source identity changed before previous generation was removed".into());
    }
    Ok(())
}

/// A complete layer rebuild dismantles every lower source. Clear all source
/// descriptors, including survivors deferred by the bounded admission queue,
/// so an orphan descriptor cannot block a later identity or endpoint change.
pub fn forget_all_admitted_sources(config_path: &str) -> Result<(), String> {
    let _lock = CONFIG_FILE_LOCK.lock().map_err(|error| error.to_string())?;
    let mut config = load_config_unlocked(config_path).map_err(|error| error.to_string())?;
    config.hosts.clear();
    let data = serde_json::to_string_pretty(&config).map_err(|error| error.to_string())?;
    fs::write(config_path, data).map_err(|error| error.to_string())
}

// Leert/Resettet die JSON-Dateien beim Serverstart
pub fn reset_json_files() -> Result<(), Box<dyn std::error::Error>> {
    println!("🗑️  Leere JSON-Dateien...");

    // config.json: Nur hosts leeren, Port und Branch behalten
    let mut config = if Path::new("config.json").exists() {
        let config_str = fs::read_to_string("config.json")?;
        serde_json::from_str(&config_str).unwrap_or_else(|_| ServerConfig {
            port: 48400,
            host: default_aggregation_host(),
            opcua_github_branch: "v1.04".to_string(),
            hosts: Vec::new(),
        })
    } else {
        ServerConfig {
            port: 48400,
            host: default_aggregation_host(),
            opcua_github_branch: "v1.04".to_string(),
            hosts: Vec::new(),
        }
    };

    config.hosts.clear();
    let config_json = serde_json::to_string_pretty(&config)?;
    fs::write("config.json", config_json)?;
    println!(
        "  ✓ config.json: hosts geleert (Port: {}, Branch: {})",
        config.port, config.opcua_github_branch
    );

    // namespaces.json leeren (nur OPC UA Basis-Namespace)
    let empty_namespaces = vec![NamespaceEntry {
        url: "http://opcfoundation.org/UA/".to_string(),
        nsid: 0,
    }];
    let namespaces_json = serde_json::to_string_pretty(&empty_namespaces)?;
    fs::write("namespaces.json", namespaces_json)?;
    println!("  ✓ namespaces.json geleert");

    fs::write("rules.json", "[]")?;
    println!("  OK rules.json geleert");
    fs::write("rules_executor.json", "[]")?;
    println!("  OK rules_executor.json geleert");

    fs::write("entry_points.json", "[]")?;
    println!("  OK entry_points.json geleert");
    // nodeset_mapping.json wird NICHT geleert - bleibt erhalten!
    println!("  ℹ nodeset_mapping.json bleibt erhalten");

    println!("✅ JSON-Dateien zurückgesetzt\n");
    Ok(())
}

// Öffentliche Funktion zum Thread-sicheren Laden der Config
pub fn load_config(config_path: &str) -> Result<ServerConfig, Box<dyn std::error::Error>> {
    let _lock = CONFIG_FILE_LOCK.lock().unwrap();
    load_config_unlocked(config_path)
}

// Caller owns CONFIG_FILE_LOCK. Never acquire this non-reentrant lock twice.
fn load_config_unlocked(config_path: &str) -> Result<ServerConfig, Box<dyn std::error::Error>> {
    let config_str = fs::read_to_string(config_path)?;
    let config: ServerConfig = serde_json::from_str(&config_str)?;
    Ok(config)
}

// Öffentliche Funktion zum Thread-sicheren Laden der Namespaces
pub fn load_namespaces(
    namespaces_path: &str,
) -> Result<Vec<NamespaceEntry>, Box<dyn std::error::Error>> {
    let _lock = NAMESPACES_FILE_LOCK.lock().unwrap();
    let namespaces_str = fs::read_to_string(namespaces_path)?;
    let namespaces: Vec<NamespaceEntry> = serde_json::from_str(&namespaces_str)?;
    Ok(namespaces)
}

/// Schreibt die Namespace-Tabelle thread-sicher zurück. Das wird benötigt,
/// nachdem der Address Space die tatsächlich vergebenen Namespace-Indizes
/// festgelegt hat.
pub fn save_namespaces(
    namespaces_path: &str,
    namespaces: &[NamespaceEntry],
) -> Result<(), Box<dyn std::error::Error>> {
    let _lock = NAMESPACES_FILE_LOCK.lock().unwrap();
    let namespaces_json = serde_json::to_string_pretty(namespaces)?;
    fs::write(namespaces_path, namespaces_json)?;
    Ok(())
}

// Aktualisiert die namespaces.json basierend auf der Companion Specs Liste.
// Filtert NUR Companion Specs, lädt NodeSets herunter und sortiert nach Requirements.
pub fn update_namespaces_config(
    namespaces_path: &str,
    new_namespaces: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let _lock = NAMESPACES_FILE_LOCK.lock().unwrap();

    if let Some(parent) = Path::new(namespaces_path).parent() {
        fs::create_dir_all(parent)?;
    }

    // Bestehende Namespaces laden oder Standard erstellen
    let mut namespace_entries: Vec<NamespaceEntry> = if Path::new(namespaces_path).exists() {
        let config_str = fs::read_to_string(namespaces_path)?;
        serde_json::from_str(&config_str)?
    } else {
        vec![NamespaceEntry {
            url: "http://opcfoundation.org/UA/".to_string(),
            nsid: 0,
        }]
    };

    let mut max_nsid = namespace_entries.iter().map(|e| e.nsid).max().unwrap_or(0);

    if max_nsid == 0 {
        max_nsid = 1;
    }

    // Neue Companion Spec Namespaces hinzufügen (noch nicht sortiert)
    for (index, namespace_url) in new_namespaces.iter().enumerate() {
        if index == 0 || index == 1 {
            continue;
        }

        if !is_companion_spec_namespace(namespace_url) {
            println!(
                "⊗ Namespace übersprungen (keine Companion Spec): {}",
                namespace_url
            );
            continue;
        }

        let normalized_url = normalize_namespace_uri(namespace_url);
        let already_exists = namespace_entries
            .iter()
            .any(|entry| normalize_namespace_uri(&entry.url) == normalized_url);

        if !already_exists {
            max_nsid += 1;
            let new_entry = NamespaceEntry {
                url: namespace_url.clone(),
                nsid: max_nsid,
            };
            println!(
                "✓ Füge Companion Spec Namespace hinzu: {} -> nsid: {}",
                new_entry.url, new_entry.nsid
            );
            namespace_entries.push(new_entry);

            // NodeSet herunterladen
            if let Some(spec_name) = extract_spec_name_from_url(namespace_url) {
                if let Err(e) = download_nodeset_for_spec(&spec_name, namespace_url) {
                    eprintln!(
                        "  ⚠ Fehler beim Herunterladen des NodeSets für {}: {}",
                        spec_name, e
                    );
                }
            }
        } else {
            println!("○ Namespace bereits vorhanden: {}", namespace_url);
        }
    }

    // Nach Requirements sortieren und nsids neu vergeben
    println!("📋 Sortiere Namespaces nach Requirements...");
    let sorted = get_sorted_namespaces_with_requirements(&namespace_entries);

    // nsids in sortierter Reihenfolge neu vergeben
    // nsid 0 = UA, nsid 1 = Server-Namespace (übersprungen), ab 2 aufsteigend
    let mut final_entries: Vec<NamespaceEntry> = Vec::new();
    let mut next_nsid: u16 = 2;

    for entry in &sorted {
        if entry.nsid <= 1 {
            // Standard-Namespaces behalten ihre nsid
            final_entries.push(entry.clone());
        } else {
            final_entries.push(NamespaceEntry {
                url: entry.url.clone(),
                nsid: next_nsid,
            });
            next_nsid += 1;
        }
    }

    let json = serde_json::to_string_pretty(&final_entries)?;
    fs::write(namespaces_path, json)?;
    println!("✓ Namespaces sortiert und gespeichert: {}", namespaces_path);

    Ok(())
}

// Gibt (entfernte, neue) Server zurück
pub fn update_config_with_discovered_servers(
    config_path: &str,
    namespaces_path: &str,
    discovered_servers: &FindServersResponse,
    lds_uri: &str,
) -> Result<(Vec<String>, Vec<(String, String)>), Box<dyn std::error::Error>> {
    let _lock = CONFIG_FILE_LOCK.lock().unwrap();

    if let Some(parent) = Path::new(config_path).parent() {
        fs::create_dir_all(parent)?;
    }

    let mut config: ServerConfig = if Path::new(config_path).exists() {
        let config_str = fs::read_to_string(config_path)?;
        serde_json::from_str(&config_str)?
    } else {
        ServerConfig {
            port: 48400,
            host: default_aggregation_host(),
            opcua_github_branch: "v1.04".to_string(),
            hosts: Vec::new(),
        }
    };

    // Sammle alle aktuell entdeckten Server
    let mut discovered_servers_map = std::collections::HashMap::new();

    if let Some(servers) = &discovered_servers.servers {
        for server in servers {
            let app_uri = server.application_uri.as_ref();

            if app_uri == lds_uri {
                continue;
            }

            if let Some(discovery_urls) = &server.discovery_urls {
                for discovery_url in discovery_urls {
                    let url_str = discovery_url.as_ref().to_string();
                    discovered_servers_map.insert(url_str.clone(), (app_uri.to_string(), url_str));
                }
            }
        }
    }

    // Entferne nicht mehr sichtbare Server und aktualisiere die ApplicationUri,
    // falls unter derselben Endpoint-Adresse eine neue Serveridentitaet laeuft.
    // Der alte Name wird als entfernt gemeldet, damit die dynamische
    // Aggregationsschicht ihre Lower-Server-Zuordnung kontrolliert neu aufbaut.
    let removed_servers = reconcile_configured_hosts(&mut config.hosts, &discovered_servers_map);

    // Finde neue Server
    let mut new_servers = Vec::new();

    for (address, (name, _)) in discovered_servers_map {
        let already_exists = config.hosts.iter().any(|h| h.address == address);

        if !already_exists {
            println!("✓ Neuer Server gefunden: {} -> {}", name, address);

            // Namespaces zuerst lesen. Nicht erreichbare LDS-Eintraege werden nicht
            // in config.json uebernommen und nicht an die Aggregation weitergegeben.
            println!("\nFrage Namespaces ab von: {}", address);
            match get_namespaces_with_retry(&address) {
                Ok(namespaces) => {
                    if let Err(e) = update_namespaces_config(namespaces_path, namespaces) {
                        eprintln!(
                            "Fehler beim Aktualisieren der Namespaces fuer {}: {}",
                            address, e
                        );
                        continue;
                    }

                    config.hosts.push(Host {
                        name: name.clone(),
                        address: address.clone(),
                    });

                    new_servers.push((name, address.clone()));
                }
                Err(e) => {
                    eprintln!(
                        "Ueberspringe nicht erreichbaren Discovery-Server {} ({}): {}",
                        name, address, e
                    );
                }
            }
        }
    }

    // Config speichern
    let config_json = serde_json::to_string_pretty(&config)?;
    fs::write(config_path, config_json)?;
    println!("\n✓ Config aktualisiert: {}", config_path);

    Ok((removed_servers, new_servers))
}

#[cfg(test)]
mod tests {
    use super::{reconcile_configured_hosts, Host, ServerConfig};
    use std::collections::HashMap;

    #[test]
    fn legacy_config_uses_laboratory_aggregation_host() {
        let config: ServerConfig = serde_json::from_str(
            r#"{
                "port": 48400,
                "opcua_github_branch": "v1.04",
                "hosts": []
            }"#,
        )
        .expect("legacy config should remain readable");

        assert_eq!(config.host, "127.0.0.1");
    }

    #[test]
    fn existing_endpoint_updates_changed_application_uri() {
        let address = "opc.tcp://192.0.2.41:4840".to_string();
        let old_name = "urn:example:legacy:card-reader:001";
        let new_name = "urn:example:card-reader:001";
        let mut hosts = vec![Host {
            name: old_name.to_string(),
            address: address.clone(),
        }];
        let discovered =
            HashMap::from([(address.clone(), (new_name.to_string(), address.clone()))]);

        let removed = reconcile_configured_hosts(&mut hosts, &discovered);

        assert_eq!(removed, vec![old_name.to_string()]);
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0].name, new_name);
        assert_eq!(hosts[0].address, address);
    }
}

#[cfg(test)]
mod admission_identity_tests {
    use super::*;
    #[test]
    fn rejects_reused_uri_or_endpoint_until_old_generation_is_retired() {
        let hosts = vec![Host {
            name: "urn:a".into(),
            address: "opc.tcp://localhost:4860/a/".into(),
        }];
        assert!(check_admission_identity(&hosts, "urn:a", "opc.tcp://localhost:4861/a/").is_err());
        assert!(check_admission_identity(&hosts, "urn:b", "opc.tcp://localhost:4860/a").is_err());
        assert!(check_admission_identity(&hosts, "urn:a", "opc.tcp://localhost:4860/a").is_ok());
        assert!(check_admission_identity(&hosts, "urn:b", "opc.tcp://localhost:4861/b/").is_ok());
    }
    #[test]
    fn preparation_and_full_reset_preserve_server_settings_without_orphan_hosts() {
        let root = std::env::temp_dir().join(format!(
            "ojies-admission-config-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let config_path = root.join("config.json");
        let namespace_path = root.join("namespaces.json");
        fs::write(
            &config_path,
            r#"{"host":"127.0.0.1","port":48400,"opcua_github_branch":"v1.04","hosts":[]}"#,
        )
        .unwrap();
        let config_path_str = config_path.to_str().unwrap();
        let namespace_path_str = namespace_path.to_str().unwrap();
        let control =
            || opcua::client::session::SessionOperationControl::new(Duration::from_secs(5));
        let namespaces = || vec!["http://opcfoundation.org/UA/".into(), "urn:a".into()];
        persist_admitted_source(
            config_path_str,
            namespace_path_str,
            "urn:a",
            "opc.tcp://127.0.0.1:4860/",
            namespaces(),
            control(),
        )
        .unwrap();
        persist_admitted_source(
            config_path_str,
            namespace_path_str,
            "urn:a",
            "opc.tcp://127.0.0.1:4860",
            namespaces(),
            control(),
        )
        .unwrap();
        assert_eq!(load_config(config_path_str).unwrap().hosts.len(), 1);
        assert!(persist_admitted_source(
            config_path_str,
            namespace_path_str,
            "urn:a",
            "opc.tcp://127.0.0.1:4861",
            namespaces(),
            control()
        )
        .is_err());
        forget_all_admitted_sources(config_path_str).unwrap();
        let empty = load_config(config_path_str).unwrap();
        assert!(empty.hosts.is_empty());
        assert_eq!(empty.host, "127.0.0.1");
        assert_eq!(empty.port, 48400);
        assert_eq!(empty.opcua_github_branch, "v1.04");
        persist_admitted_source(
            config_path_str,
            namespace_path_str,
            "urn:a",
            "opc.tcp://127.0.0.1:4861",
            namespaces(),
            control(),
        )
        .unwrap();
        let before_config = fs::read(&config_path).unwrap();
        let before_namespaces = fs::read(&namespace_path).unwrap();
        let cancelled = control();
        cancelled.cancel();
        assert!(persist_admitted_source(
            config_path_str,
            namespace_path_str,
            "urn:b",
            "opc.tcp://127.0.0.1:4862",
            namespaces(),
            cancelled
        )
        .is_err());
        assert_eq!(fs::read(&config_path).unwrap(), before_config);
        assert_eq!(fs::read(&namespace_path).unwrap(), before_namespaces);
        fs::remove_file(&config_path).unwrap();
        fs::remove_file(&namespace_path).unwrap();
        fs::remove_dir(&root).unwrap();
    }
}
