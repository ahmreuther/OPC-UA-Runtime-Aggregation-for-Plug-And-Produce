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

    // Finde entfernte Server
    let mut removed_servers = Vec::new();
    config.hosts.retain(|host| {
        if discovered_servers_map.contains_key(&host.address) {
            true
        } else {
            println!(
                "⊗ Server nicht mehr verfügbar: {} ({})",
                host.name, host.address
            );
            removed_servers.push(host.name.clone());
            false
        }
    });

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
    use super::ServerConfig;

    #[test]
    fn legacy_config_uses_loopback_aggregation_host() {
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
}
