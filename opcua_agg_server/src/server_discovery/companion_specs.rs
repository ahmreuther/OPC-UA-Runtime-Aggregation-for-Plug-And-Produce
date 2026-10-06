// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use crate::server_discovery::NODESETS_DIR;

const NODESET_VERSION_FILE: &str = "./nodesets/nodeset_version.json";

#[derive(Debug, Serialize, Deserialize)]
struct NodeSetVersion {
    github_branch: String,
    last_updated: String,
}

const GITHUB_API_BASE: &str = "https://api.github.com/repos/OPCFoundation/UA-Nodeset/contents";
const GITHUB_RAW_BASE: &str = "https://raw.githubusercontent.com/OPCFoundation/UA-Nodeset";
const COMPANION_SPECS_FILE: &str = "./companion_specs.json";
const NODESET_MAPPING_FILE: &str = "./nodeset_mapping.json";

// Basis OPC UA Namespace - wird bei Requirements immer ausgelassen
const BASE_OPC_UA_NAMESPACE: &str = "http://opcfoundation.org/UA/";

lazy_static! {
    static ref COMPANION_SPECS_CACHE: Mutex<Vec<String>> = Mutex::new(Vec::new());
    static ref GITHUB_BRANCH_CACHE: Mutex<String> = Mutex::new(String::from("v1.04"));
    static ref COMPANION_SPECS_FILE_LOCK: Mutex<()> = Mutex::new(());
    pub(crate) static ref NODESET_MAPPING_LOCK: Mutex<()> = Mutex::new(());
}

#[derive(Debug, Deserialize)]
struct GitHubContent {
    name: String,
    #[serde(rename = "type")]
    item_type: String,
    #[serde(default)]
    download_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CompanionSpecsList {
    last_updated: String,
    specs: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeSetMapping {
    pub namespace_uri: String,
    pub nodeset_file: String,
    pub model_uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct NodeSetMappingList {
    last_updated: String,
    mappings: Vec<NodeSetMapping>,
}

fn load_companion_specs_from_file() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let _lock = COMPANION_SPECS_FILE_LOCK.lock().unwrap();

    if !Path::new(COMPANION_SPECS_FILE).exists() {
        println!("⚠ Keine lokale Companion Specs Datei gefunden");
        return Ok(get_default_companion_specs());
    }

    let content = fs::read_to_string(COMPANION_SPECS_FILE)?;
    let list: CompanionSpecsList = serde_json::from_str(&content)?;

    println!(
        "✓ {} Companion Specs von lokaler Datei geladen (Stand: {})",
        list.specs.len(),
        list.last_updated
    );

    Ok(list.specs)
}

fn save_companion_specs_to_file(specs: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let _lock = COMPANION_SPECS_FILE_LOCK.lock().unwrap();

    if let Some(parent) = Path::new(COMPANION_SPECS_FILE).parent() {
        fs::create_dir_all(parent)?;
    }

    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let list = CompanionSpecsList {
        last_updated: now,
        specs: specs.to_vec(),
    };

    let json = serde_json::to_string_pretty(&list)?;
    fs::write(COMPANION_SPECS_FILE, json)?;

    println!(
        "✓ Companion Specs Liste gespeichert: {}",
        COMPANION_SPECS_FILE
    );
    Ok(())
}

fn get_default_companion_specs() -> Vec<String> {
    vec![
        "http://opcfoundation.org/UA/".to_string(),
        "http://opcfoundation.org/UA/DI/".to_string(),
        "http://opcfoundation.org/UA/Machinery/".to_string(),
        "http://opcfoundation.org/UA/Robotics/".to_string(),
    ]
}

fn fetch_companion_specs_from_github(
    branch: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    println!("Lade Companion Spec Liste von GitHub ({})...", branch);

    let client = reqwest::blocking::Client::builder()
        .user_agent("OPC-UA-Discovery-Client")
        .timeout(Duration::from_secs(10))
        .build()?;

    let api_url = format!("{}?ref={}", GITHUB_API_BASE, branch);
    println!("  API URL: {}", api_url);

    let response = client.get(&api_url).send()?;

    if !response.status().is_success() {
        return Err(format!(
            "GitHub API HTTP Fehler: {} - {}",
            response.status(),
            response
                .text()
                .unwrap_or_else(|_| "Keine Details".to_string())
        )
        .into());
    }

    let response_text = response.text()?;
    let contents: Vec<GitHubContent> = serde_json::from_str(&response_text).map_err(|e| {
        format!(
            "JSON Parse Fehler: {} - Response: {}",
            e,
            &response_text[..response_text.len().min(200)]
        )
    })?;

    let mut specs = Vec::new();
    specs.push(BASE_OPC_UA_NAMESPACE.to_string());

    for item in contents {
        if item.item_type == "dir" {
            if item.name.starts_with('.')
                || item.name == "Schema"
                || item.name == "XML"
                || item.name == "DotNet"
                || item.name == "AnsiC"
            {
                continue;
            }

            let namespace_url = format!("http://opcfoundation.org/UA/{}/", item.name);
            specs.push(namespace_url);
        }
    }

    println!(
        "✓ {} Companion Specs von GitHub {} geladen",
        specs.len(),
        branch
    );
    Ok(specs)
}

fn load_nodeset_version() -> Option<NodeSetVersion> {
    if !Path::new(NODESET_VERSION_FILE).exists() {
        return None;
    }

    match fs::read_to_string(NODESET_VERSION_FILE) {
        Ok(content) => serde_json::from_str(&content).ok(),
        Err(_) => None,
    }
}

fn save_nodeset_version(branch: &str) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(NODESETS_DIR)?;

    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let version = NodeSetVersion {
        github_branch: branch.to_string(),
        last_updated: now,
    };

    let json = serde_json::to_string_pretty(&version)?;
    fs::write(NODESET_VERSION_FILE, json)?;

    Ok(())
}

fn clear_nodesets_directory() -> Result<(), Box<dyn std::error::Error>> {
    println!("🗑️  Lösche alle NodeSet XML-Dateien...");

    if !Path::new(NODESETS_DIR).exists() {
        println!("  ℹ NodeSets Verzeichnis existiert nicht");
        return Ok(());
    }

    let mut deleted_count = 0;

    for entry in fs::read_dir(NODESETS_DIR)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "xml" {
                    fs::remove_file(&path)?;
                    println!("  ✗ Gelöscht: {}", path.display());
                    deleted_count += 1;
                }
            }
        }
    }

    println!("  ✓ {} NodeSet-Dateien gelöscht", deleted_count);

    if Path::new(NODESET_MAPPING_FILE).exists() {
        let empty_mapping = serde_json::json!({
            "last_updated": "",
            "mappings": []
        });
        let mapping_json = serde_json::to_string_pretty(&empty_mapping)?;
        fs::write(NODESET_MAPPING_FILE, mapping_json)?;
        println!("  🗑️ Geleert: nodeset_mapping.json (alte Mappings ungültig)");
    }

    if Path::new(COMPANION_SPECS_FILE).exists() {
        let empty_specs = CompanionSpecsList {
            last_updated: String::new(),
            specs: Vec::new(),
        };
        let specs_json = serde_json::to_string_pretty(&empty_specs)?;
        fs::write(COMPANION_SPECS_FILE, specs_json)?;
        println!("  🗑️ Geleert: companion_specs.json (alter Branch)");
    }

    Ok(())
}

pub fn check_and_update_nodeset_version(
    current_branch: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Prüfe NodeSet-Version ===");

    match load_nodeset_version() {
        Some(saved_version) => {
            println!(
                "  Gespeicherte Version: {} ({})",
                saved_version.github_branch, saved_version.last_updated
            );
            println!("  Aktuelle Config:      {}", current_branch);

            if saved_version.github_branch != current_branch {
                println!("  ⚠️  Branch hat sich geändert!");
                clear_nodesets_directory()?;
                save_nodeset_version(current_branch)?;
                println!("  ✓ NodeSet-Version aktualisiert auf '{}'", current_branch);
            } else {
                println!("  ✓ Branch unverändert - NodeSets bleiben erhalten");
            }
        }
        None => {
            println!("  ℹ Keine gespeicherte Version gefunden");
            save_nodeset_version(current_branch)?;
        }
    }

    println!("===========================\n");
    Ok(())
}

pub fn update_companion_specs_list(config_path: &str) {
    println!("\n=== Aktualisiere Companion Specs Liste ===");

    let branch = if let Ok(config_file) = fs::File::open(Path::new(config_path)) {
        let config_reader = std::io::BufReader::new(config_file);
        if let Ok(config) = serde_json::from_reader::<
            _,
            crate::server_discovery::config::ServerConfig,
        >(config_reader)
        {
            println!("✓ GitHub Branch aus Config: {}", config.opcua_github_branch);
            config.opcua_github_branch
        } else {
            println!("⚠ Konnte Config nicht lesen, nutze Standard: v1.04");
            "v1.04".to_string()
        }
    } else {
        println!("⚠ Config nicht gefunden, nutze Standard: v1.04");
        "v1.04".to_string()
    };

    {
        let mut branch_cache = GITHUB_BRANCH_CACHE.lock().unwrap();
        *branch_cache = branch.clone();
    }

    if let Err(e) = check_and_update_nodeset_version(&branch) {
        eprintln!("⚠ Fehler beim Prüfen der NodeSet-Version: {}", e);
    }

    if let Err(e) = scan_and_register_existing_nodesets() {
        eprintln!(
            "WARN Lokale NodeSet-Mappings konnten nicht aktualisiert werden: {}",
            e
        );
    }

    let mut local_specs = match load_companion_specs_from_file() {
        Ok(specs) => specs,
        Err(e) => {
            eprintln!("⚠ Fehler beim Laden der lokalen Liste: {}", e);
            get_default_companion_specs()
        }
    };

    match fetch_companion_specs_from_github(&branch) {
        Ok(github_specs) => {
            let old_count = local_specs.len();

            for spec in github_specs {
                if !local_specs.contains(&spec) {
                    println!("➕ Neue Companion Spec hinzugefügt: {}", spec);
                    local_specs.push(spec);
                }
            }

            let new_count = local_specs.len();
            if new_count > old_count {
                println!(
                    "✓ {} neue Companion Specs hinzugefügt",
                    new_count - old_count
                );
            } else {
                println!("✓ Liste ist bereits aktuell");
            }

            if let Err(e) = save_companion_specs_to_file(&local_specs) {
                eprintln!("⚠ Fehler beim Speichern der Liste: {}", e);
            }
        }
        Err(e) => {
            eprintln!("⚠ GitHub nicht erreichbar: {}", e);
            println!(
                "ℹ Verwende gespeicherte lokale Liste mit {} Specs",
                local_specs.len()
            );
        }
    }

    let mut cache = COMPANION_SPECS_CACHE.lock().unwrap();
    *cache = local_specs;

    println!(
        "=== Companion Specs Liste bereit ({} Specs, Branch: {}) ===\n",
        cache.len(),
        branch
    );
}

pub fn is_companion_spec_namespace(namespace_url: &str) -> bool {
    let cache = COMPANION_SPECS_CACHE.lock().unwrap();
    cache.iter().any(|spec| namespace_url.starts_with(spec))
}

pub fn extract_spec_name_from_url(namespace_url: &str) -> Option<String> {
    const PREFIX: &str = "http://opcfoundation.org/UA/";

    if !namespace_url.starts_with(PREFIX) {
        println!(
            "      ⚠ Namespace beginnt nicht mit {}: {}",
            PREFIX, namespace_url
        );
        return None;
    }

    let remaining = &namespace_url[PREFIX.len()..];
    let spec_name = remaining.trim_end_matches('/');

    if spec_name.is_empty() {
        println!(
            "      ⚠ Spec-Name ist leer nach Verarbeitung von: {}",
            namespace_url
        );
        None
    } else {
        println!(
            "      ✓ Spec-Name extrahiert: '{}' aus '{}'",
            spec_name, namespace_url
        );
        Some(spec_name.to_string())
    }
}

fn extract_model_uri_from_xml(xml_content: &str) -> Option<String> {
    if let Some(model_start) = xml_content.find("<Model ") {
        let after_model = &xml_content[model_start..];

        if let Some(uri_start) = after_model.find("ModelUri=\"") {
            let after_uri = &after_model[uri_start + 10..];
            if let Some(uri_end) = after_uri.find("\"") {
                let uri = after_uri[..uri_end].trim();
                return Some(uri.to_string());
            }
        }
    }

    None
}

/// Liest RequiredModel-URIs aus einem NodeSet XML.
/// Überspringt den Basis-OPC-UA-Namespace.
pub fn get_required_models_from_xml(xml_path: &str) -> Vec<String> {
    let content = match fs::read_to_string(xml_path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let doc = match roxmltree::Document::parse(&content) {
        Ok(d) => d,
        Err(_) => return Vec::new(),
    };

    let mut requirements = Vec::new();

    for node in doc.descendants() {
        if node.tag_name().name() == "RequiredModel" {
            if let Some(uri) = node.attribute("ModelUri") {
                // Basis OPC UA Namespace immer auslassen
                if uri != BASE_OPC_UA_NAMESPACE
                    && !uri
                        .trim_end_matches('/')
                        .eq(BASE_OPC_UA_NAMESPACE.trim_end_matches('/'))
                {
                    requirements.push(uri.to_string());
                }
            }
        }
    }

    requirements
}

/// Gibt Namespaces topologisch sortiert zurück (Requirements zuerst).
/// Lädt fehlende NodeSets automatisch von GitHub nach.
/// Basis-OPC-UA-Namespace (ns=0) wird übersprungen / immer zuerst belassen.
pub fn get_sorted_namespaces_with_requirements(
    namespaces: &[crate::server_discovery::config::NamespaceEntry],
) -> Vec<crate::server_discovery::config::NamespaceEntry> {
    use crate::server_discovery::config::NamespaceEntry;
    use std::collections::{HashMap, HashSet, VecDeque};

    println!("\n  📋 Topologische Sortierung der Namespaces...");

    // Standard-Namespaces (nsid <= 1) immer am Anfang
    let standard: Vec<NamespaceEntry> = namespaces
        .iter()
        .filter(|ns| ns.nsid <= 1)
        .cloned()
        .collect();

    // Nur Companion Specs sortieren (nsid > 1)
    let companion_specs: Vec<&NamespaceEntry> =
        namespaces.iter().filter(|ns| ns.nsid > 1).collect();

    // Baue URL → Abhängigkeiten Map auf
    // url_normalized → Vec<required_url_normalized>
    let mut deps: HashMap<String, Vec<String>> = HashMap::new();
    let mut known_urls: HashSet<String> = HashSet::new();

    // Alle bekannten URLs sammeln
    for ns in &companion_specs {
        known_urls.insert(normalize_namespace_uri(&ns.url));
    }

    // Requirements für jeden Namespace ermitteln
    for ns in &companion_specs {
        let normalized = normalize_namespace_uri(&ns.url);

        let xml_path = match find_nodeset_file_for_namespace(&ns.url) {
            Some(p) => p,
            None => {
                // Versuche von GitHub nachzuladen
                if let Some(spec_name) = extract_spec_name_from_url(&ns.url) {
                    if let Err(e) = download_nodeset_for_spec(&spec_name, &ns.url) {
                        eprintln!("    ⚠ Download fehlgeschlagen für {}: {}", ns.url, e);
                    }
                }
                deps.entry(normalized).or_default();
                continue;
            }
        };

        let reqs = get_required_models_from_xml(&xml_path);
        println!("    Requirements für {}: {:?}", ns.url, reqs);

        let mut req_normalized = Vec::new();
        for req_url in &reqs {
            let req_norm = normalize_namespace_uri(req_url);

            // Fehlende Requirements nachladen
            if !known_urls.contains(&req_norm) {
                println!("    ➕ Fehlende Dependency erkannt: {}", req_url);
                if let Some(spec_name) = extract_spec_name_from_url(req_url) {
                    if let Err(e) = download_nodeset_for_spec(&spec_name, req_url) {
                        eprintln!("    ⚠ Download fehlgeschlagen für {}: {}", req_url, e);
                    } else {
                        known_urls.insert(req_norm.clone());
                    }
                }
            }

            req_normalized.push(req_norm);
        }

        deps.insert(normalized, req_normalized);
    }

    // Kahn's Algorithm für topologische Sortierung
    // in_degree: wie viele Requirements hat dieser Namespace noch?
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    // dependents: welche Namespaces hängen von diesem ab?
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

    for (url, reqs) in &deps {
        in_degree.entry(url.clone()).or_insert(0);

        for req in reqs {
            if deps.contains_key(req.as_str()) {
                *in_degree.entry(url.clone()).or_insert(0) += 1;
                dependents.entry(req.clone()).or_default().push(url.clone());
            }
            // Requirements die selbst nicht in deps sind (z.B. neu geladene),
            // werden ignoriert - sie haben keine eigenen Abhängigkeiten in unserer Liste
        }
    }

    // Starte mit allen Nodes ohne Abhängigkeiten
    let mut queue: VecDeque<String> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(url, _)| url.clone())
        .collect();

    let mut sorted_normalized: Vec<String> = Vec::new();

    while let Some(url) = queue.pop_front() {
        sorted_normalized.push(url.clone());

        if let Some(deps_list) = dependents.get(&url) {
            for dep in deps_list {
                let deg = in_degree.entry(dep.clone()).or_insert(0);
                if *deg > 0 {
                    *deg -= 1;
                }
                if *deg == 0 {
                    queue.push_back(dep.clone());
                }
            }
        }
    }

    // Zyklenerkennung: falls nicht alle verarbeitet wurden
    if sorted_normalized.len() < deps.len() {
        eprintln!("    ⚠ Zyklische Abhängigkeiten erkannt! Verarbeite verbleibende Namespaces unsortiert.");
        for url in deps.keys() {
            if !sorted_normalized.contains(url) {
                sorted_normalized.push(url.clone());
            }
        }
    }

    // Sortierte URLs zurück in NamespaceEntry konvertieren
    let mut result: Vec<NamespaceEntry> = standard;

    for norm_url in &sorted_normalized {
        // Suche in originalen companion_specs
        if let Some(ns) = companion_specs
            .iter()
            .find(|n| normalize_namespace_uri(&n.url) == *norm_url)
        {
            result.push((*ns).clone());
        } else {
            // Implizit geladene Dependency (war nicht in ursprünglicher Liste)
            // Weise neue nsid zu
            let new_nsid = result.iter().map(|n| n.nsid).max().unwrap_or(1) + 1;
            // Versuche originale URL (mit Slash) zu rekonstruieren
            let url = if norm_url.ends_with('/') {
                norm_url.clone()
            } else {
                format!("{}/", norm_url)
            };
            println!(
                "    ➕ Implizite Dependency als Namespace hinzugefügt: {} -> nsid:{}",
                url, new_nsid
            );
            result.push(NamespaceEntry {
                url,
                nsid: new_nsid,
            });
        }
    }

    println!("  ✓ Sortierte Reihenfolge:");
    for ns in &result {
        println!("    nsid:{} = {}", ns.nsid, ns.url);
    }

    result
}

/// Normalisiert eine Namespace-URI (trailing slash entfernen für Vergleiche)
pub fn normalize_namespace_uri(uri: &str) -> String {
    uri.trim_end_matches('/').to_string()
}

fn model_uri_matches_namespace(model_uri: &str, expected_namespace: &str) -> bool {
    let model_normalized = normalize_namespace_uri(model_uri);
    let expected_normalized = normalize_namespace_uri(expected_namespace);
    model_normalized == expected_normalized
}

pub fn download_nodeset_for_spec(
    spec_name: &str,
    expected_namespace_uri: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(NODESETS_DIR)?;

    let branch = GITHUB_BRANCH_CACHE.lock().unwrap().clone();

    println!(
        "  🔍 Suche NodeSet Dateien für Spec: {} ({})",
        spec_name, branch
    );

    let api_url = format!("{}/{}?ref={}", GITHUB_API_BASE, spec_name, branch);

    let client = reqwest::blocking::Client::builder()
        .user_agent("OPC-UA-Discovery-Client")
        .timeout(Duration::from_secs(30))
        .build()?;

    let response = client.get(&api_url).send()?;

    if !response.status().is_success() {
        return Err(format!("GitHub API Fehler: {} für {}", response.status(), api_url).into());
    }

    let contents: Vec<GitHubContent> = response.json()?;

    let nodeset2_files: Vec<_> = contents
        .iter()
        .filter(|item| item.item_type == "file" && item.name.ends_with(".NodeSet2.xml"))
        .collect();

    let nodeset_files: Vec<_> = contents
        .iter()
        .filter(|item| item.item_type == "file" && item.name.ends_with(".NodeSet.xml"))
        .collect();

    if nodeset2_files.is_empty() && nodeset_files.is_empty() {
        println!("  ⚠ Keine NodeSet Dateien gefunden für {}", spec_name);
        return Ok(());
    }

    let files_to_check = if !nodeset2_files.is_empty() {
        println!(
            "  ✓ {} NodeSet2.xml Datei(en) gefunden",
            nodeset2_files.len()
        );
        nodeset2_files
    } else {
        println!("  ℹ Nutze {} NodeSet.xml Datei(en)", nodeset_files.len());
        nodeset_files
    };

    let mut downloaded_count = 0;

    for nodeset_file in files_to_check {
        let filename = &nodeset_file.name;
        let local_path = format!("{}/{}", NODESETS_DIR, filename);

        println!("    📋 Verarbeite Datei: {}", filename);

        if Path::new(&local_path).exists() {
            println!("    ℹ Bereits vorhanden: {}", filename);
            // Mapping sicherstellen auch wenn Datei schon da
            let xml_content = fs::read_to_string(&local_path)?;
            if let Some(model_uri) = extract_model_uri_from_xml(&xml_content) {
                if model_uri_matches_namespace(&model_uri, expected_namespace_uri) {
                    let _ =
                        add_or_update_nodeset_mapping(expected_namespace_uri, filename, &model_uri);
                }
            }
            continue;
        }

        let download_url = if let Some(url) = &nodeset_file.download_url {
            url.clone()
        } else {
            format!("{}/{}/{}/{}", GITHUB_RAW_BASE, branch, spec_name, filename)
        };

        println!("    ⬇ Lade herunter: {}", filename);

        let file_response = client.get(&download_url).send()?;

        if !file_response.status().is_success() {
            eprintln!("    ✗ Fehler beim Download: {}", file_response.status());
            continue;
        }

        let content = file_response.text()?;

        if let Some(model_uri) = extract_model_uri_from_xml(&content) {
            println!("    📋 ModelUri gefunden: {}", model_uri);

            if model_uri_matches_namespace(&model_uri, expected_namespace_uri) {
                println!("    ✓ ModelUri passt zur Namespace URI");
                fs::write(&local_path, content)?;
                println!("    ✓ Gespeichert: {}", local_path);

                if let Err(e) =
                    add_or_update_nodeset_mapping(expected_namespace_uri, filename, &model_uri)
                {
                    eprintln!("    ⚠ Fehler beim Speichern des Mappings: {}", e);
                }

                downloaded_count += 1;
            } else {
                println!(
                    "    ⊗ ModelUri passt NICHT (erwartet: {})",
                    expected_namespace_uri
                );
            }
        } else {
            eprintln!("    ⚠ Konnte ModelUri nicht extrahieren - speichere trotzdem");
            fs::write(&local_path, content)?;
            println!("    ✓ Gespeichert: {}", local_path);

            if let Err(e) = add_or_update_nodeset_mapping(expected_namespace_uri, filename, "") {
                eprintln!("    ⚠ Fehler beim Speichern des Mappings: {}", e);
            }

            downloaded_count += 1;
        }
    }

    if downloaded_count > 0 {
        println!(
            "  ✓ {} Datei(en) erfolgreich heruntergeladen",
            downloaded_count
        );
    }

    Ok(())
}

pub fn load_nodeset_mapping() -> Result<Vec<NodeSetMapping>, Box<dyn std::error::Error>> {
    let _lock = NODESET_MAPPING_LOCK.lock().unwrap();

    if !Path::new(NODESET_MAPPING_FILE).exists() {
        return Ok(Vec::new());
    }

    let content = fs::read_to_string(NODESET_MAPPING_FILE)?;
    let list: NodeSetMappingList = serde_json::from_str(&content)?;

    Ok(list.mappings)
}

fn save_nodeset_mapping(mappings: &[NodeSetMapping]) -> Result<(), Box<dyn std::error::Error>> {
    let _lock = NODESET_MAPPING_LOCK.lock().unwrap();

    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let list = NodeSetMappingList {
        last_updated: now,
        mappings: mappings.to_vec(),
    };

    let json = serde_json::to_string_pretty(&list)?;
    fs::write(NODESET_MAPPING_FILE, json)?;

    Ok(())
}

fn add_or_update_nodeset_mapping(
    namespace_uri: &str,
    nodeset_file: &str,
    model_uri: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut mappings = load_nodeset_mapping().unwrap_or_default();
    let normalized_uri = normalize_namespace_uri(namespace_uri);

    if let Some(existing) = mappings
        .iter_mut()
        .find(|mapping| normalize_namespace_uri(&mapping.namespace_uri) == normalized_uri)
    {
        existing.namespace_uri = namespace_uri.to_string();
        existing.nodeset_file = nodeset_file.to_string();
        existing.model_uri = model_uri.to_string();
        let mut retained_match = false;
        mappings.retain(|mapping| {
            if normalize_namespace_uri(&mapping.namespace_uri) != normalized_uri {
                return true;
            }
            if retained_match {
                false
            } else {
                retained_match = true;
                true
            }
        });
        println!(
            "    ✓ Mapping aktualisiert: {} → {}",
            namespace_uri, nodeset_file
        );
    } else {
        mappings.push(NodeSetMapping {
            namespace_uri: namespace_uri.to_string(),
            nodeset_file: nodeset_file.to_string(),
            model_uri: model_uri.to_string(),
        });
        println!(
            "    ✓ Mapping hinzugefügt: {} → {}",
            namespace_uri, nodeset_file
        );
    }

    save_nodeset_mapping(&mappings)?;
    Ok(())
}

pub fn find_nodeset_file_for_namespace(namespace_uri: &str) -> Option<String> {
    println!("  🔍 Suche NodeSet für Namespace: {}", namespace_uri);

    let mappings = match load_nodeset_mapping() {
        Ok(m) => m,
        Err(e) => {
            println!("    ✗ Fehler beim Laden des Mappings: {}", e);
            return None;
        }
    };

    let normalized_uri = normalize_namespace_uri(namespace_uri);
    let mapping = mappings
        .iter()
        .find(|mapping| normalize_namespace_uri(&mapping.namespace_uri) == normalized_uri);

    if let Some(mapping) = mapping {
        let full_path = format!("{}/{}", NODESETS_DIR, mapping.nodeset_file);

        if Path::new(&full_path).exists() {
            println!("    ✓ Gefunden via Mapping: {}", full_path);
            return Some(full_path);
        } else {
            println!(
                "    ⚠ Mapping gefunden, aber Datei existiert nicht: {}",
                full_path
            );
        }
    }

    println!("    ✗ Kein Mapping gefunden für: {}", namespace_uri);
    None
}

pub fn scan_and_register_existing_nodesets() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Scanne vorhandene NodeSet-Dateien ===");

    if !Path::new(NODESETS_DIR).exists() {
        println!("  ℹ NodeSets Verzeichnis existiert nicht");
        return Ok(());
    }

    let mut scanned_count = 0;
    let mut registered_count = 0;

    for entry in fs::read_dir(NODESETS_DIR)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "xml" {
                    scanned_count += 1;
                    let filename = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("unknown");

                    println!("  📋 Gefunden: {}", filename);

                    match fs::read_to_string(&path) {
                        Ok(xml_content) => {
                            if let Some(model_uri) = extract_model_uri_from_xml(&xml_content) {
                                println!("    ✓ ModelUri: {}", model_uri);

                                if let Err(e) =
                                    add_or_update_nodeset_mapping(&model_uri, filename, &model_uri)
                                {
                                    eprintln!("    ⚠ Fehler beim Registrieren: {}", e);
                                } else {
                                    println!("    ✓ Mapping registriert");
                                    registered_count += 1;
                                }
                            } else {
                                println!("    ⚠ Konnte ModelUri nicht extrahieren");
                            }
                        }
                        Err(e) => {
                            eprintln!("    ✗ Fehler beim Lesen: {}", e);
                        }
                    }
                }
            }
        }
    }

    println!("\n  ✓ {} NodeSet-Dateien gescannt", scanned_count);
    println!("  ✓ {} Mappings registriert", registered_count);
    println!("=====================================\n");

    Ok(())
}
