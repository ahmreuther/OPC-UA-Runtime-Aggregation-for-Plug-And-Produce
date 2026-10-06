// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

use serde::{Deserialize, Serialize};
use std::fs;

use crate::server_discovery::companion_specs::find_nodeset_file_for_namespace;
use crate::server_discovery::config::NamespaceEntry;

/// Ein Entry-Point in einem NodeSet
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryPoint {
    pub node_id: String,
    pub browse_name: String,
    pub display_name: String,
    pub browse_path: Vec<String>, // z.B. ["Objects", "DeviceSet"]
    pub namespace_uri: String,
    pub namespace_index: u16,
}

/// Ergebnis der Entry-Point Detection für einen NodeSet
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSetEntryPoints {
    pub namespace_uri: String,
    pub namespace_index: u16,
    pub entry_points: Vec<EntryPoint>,
}

/// Findet alle Entry-Points durch Parsen der XML-Dateien
pub fn find_all_entry_points(namespaces: &[NamespaceEntry]) -> Vec<NodeSetEntryPoints> {
    println!("\n=== Suche Entry-Points in NodeSet XMLs ===");

    let mut results = Vec::new();

    // Objects immer als erster Entry-Point
    results.push(NodeSetEntryPoints {
        namespace_uri: "http://opcfoundation.org/UA/".to_string(),
        namespace_index: 0,
        entry_points: vec![EntryPoint {
            node_id: "i=85".to_string(),
            browse_name: "Objects".to_string(),
            display_name: "Objects".to_string(),
            browse_path: vec!["Objects".to_string()],
            namespace_uri: "http://opcfoundation.org/UA/".to_string(),
            namespace_index: 0,
        }],
    });

    println!("📦 Objects immer als Entry-Point");

    // Für jeden Namespace
    for ns_entry in namespaces {
        if ns_entry.nsid <= 1 {
            continue;
        }

        println!(
            "\n📦 Analysiere NodeSet: {} (nsid: {})",
            ns_entry.url, ns_entry.nsid
        );

        let entry_points =
            find_entry_points_for_namespace(&ns_entry.url, ns_entry.nsid, namespaces);

        println!("  ✓ {} Entry-Point(s) gefunden", entry_points.len());

        for ep in &entry_points {
            println!("    → {} ({})", ep.display_name, ep.browse_path.join("/"));
        }

        results.push(NodeSetEntryPoints {
            namespace_uri: ns_entry.url.clone(),
            namespace_index: ns_entry.nsid,
            entry_points,
        });
    }

    println!("\n=== Entry-Point Suche abgeschlossen ===");
    let total_count: usize = results.iter().map(|r| r.entry_points.len()).sum();
    println!(
        "Total: {} Entry-Points in {} NodeSets\n",
        total_count,
        results.len()
    );

    results
}

fn find_entry_points_for_namespace(
    namespace_uri: &str,
    namespace_index: u16,
    all_namespaces: &[NamespaceEntry],
) -> Vec<EntryPoint> {
    let xml_path = match find_nodeset_file_for_namespace(namespace_uri) {
        Some(path) => path,
        None => {
            println!("  ⚠ Keine XML-Datei gefunden");
            return Vec::new();
        }
    };

    let xml_content = match fs::read_to_string(&xml_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("  ✗ Fehler beim Lesen: {}", e);
            return Vec::new();
        }
    };

    let doc = match roxmltree::Document::parse(&xml_content) {
        Ok(doc) => doc,
        Err(e) => {
            eprintln!("  ✗ Fehler beim Parsen: {}", e);
            return Vec::new();
        }
    };

    let xml_ns_index = 1; // Modell-Namespace
    let model_uri = get_model_uri(&doc);
    let mut entry_points = Vec::new();

    for node_elem in doc.descendants() {
        if node_elem.tag_name().name() != "UAObject" {
            continue;
        }

        let node_id = match node_elem.attribute("NodeId") {
            Some(id) => id,
            None => continue,
        };

        // Muss im Modell-Namespace sein
        if !is_node_in_namespace(node_id, xml_ns_index) {
            continue;
        }

        // Parent muss in ANDEREM Namespace sein
        if !has_parent_in_different_namespace(&node_elem, xml_ns_index) {
            continue;
        }

        // Filter
        if is_model_object(&node_elem, &model_uri)
            || is_placeholder(&node_elem)
            || is_default_encoding(&node_elem)
        {
            continue;
        }

        // Baue Browse-Pfad rekursiv
        let browse_path = build_browse_path(&node_elem, &doc, all_namespaces);

        let browse_name = node_elem
            .attribute("BrowseName")
            .unwrap_or("")
            .split(':')
            .last()
            .unwrap_or("")
            .to_string();

        let display_name = node_elem
            .children()
            .find(|n| n.tag_name().name() == "DisplayName")
            .and_then(|n| n.text())
            .unwrap_or(&browse_name)
            .to_string();

        entry_points.push(EntryPoint {
            node_id: node_id.to_string(),
            browse_name,
            display_name,
            browse_path,
            namespace_uri: namespace_uri.to_string(),
            namespace_index,
        });
    }

    entry_points
}

/// Baut den Browse-Pfad rekursiv auf, auch über NodeSet-Grenzen hinweg
fn build_browse_path(
    node_elem: &roxmltree::Node,
    doc: &roxmltree::Document,
    all_namespaces: &[NamespaceEntry],
) -> Vec<String> {
    let mut path = Vec::new();

    // Aktueller BrowseName
    if let Some(bn) = node_elem.attribute("BrowseName") {
        path.push(bn.split(':').last().unwrap_or(bn).to_string());
    }

    // Finde Parent
    if let Some(parent_id) = get_parent_id(node_elem) {
        // Stoppe bei Objects
        if parent_id == "i=85" {
            path.push("Objects".to_string());
            path.reverse();
            return path;
        }

        // Suche Parent im aktuellen XML
        if let Some(parent_node) = find_node_by_id(doc, &parent_id) {
            // Parent im gleichen XML gefunden
            let parent_path = build_browse_path(&parent_node, doc, all_namespaces);
            path.extend(parent_path);
            path.reverse();
            return path;
        }

        // Parent nicht im aktuellen XML → Suche in anderen NodeSets
        let parent_path = find_node_in_other_nodesets(&parent_id, all_namespaces);
        path.extend(parent_path);
    }

    path.reverse();
    path
}

/// Findet einen Node in anderen NodeSets
fn find_node_in_other_nodesets(node_id: &str, all_namespaces: &[NamespaceEntry]) -> Vec<String> {
    // Bestimme Namespace aus node_id
    let ns_idx = extract_namespace_index(node_id).unwrap_or(0);

    // Finde entsprechendes NodeSet
    for ns_entry in all_namespaces {
        if ns_entry.nsid != ns_idx {
            continue;
        }

        let xml_path = match find_nodeset_file_for_namespace(&ns_entry.url) {
            Some(p) => p,
            None => continue,
        };

        let xml_content = match fs::read_to_string(&xml_path) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let doc = match roxmltree::Document::parse(&xml_content) {
            Ok(d) => d,
            Err(_) => continue,
        };

        if let Some(node) = find_node_by_id(&doc, node_id) {
            return build_browse_path(&node, &doc, all_namespaces);
        }
    }

    // Fallback
    if node_id == "i=85" {
        vec!["Objects".to_string()]
    } else {
        vec![]
    }
}

fn get_parent_id(node_elem: &roxmltree::Node) -> Option<String> {
    for child in node_elem.children() {
        if child.tag_name().name() != "References" {
            continue;
        }

        for ref_elem in child.children() {
            if ref_elem.tag_name().name() != "Reference" {
                continue;
            }

            let ref_type = ref_elem.attribute("ReferenceType")?;
            if !is_hierarchical_ref(ref_type) {
                continue;
            }

            let is_forward = ref_elem
                .attribute("IsForward")
                .map(|v| v == "true")
                .unwrap_or(true);

            if !is_forward {
                return ref_elem.text().map(|s| s.trim().to_string());
            }
        }
    }
    None
}

fn find_node_by_id<'a>(
    doc: &'a roxmltree::Document,
    node_id: &str,
) -> Option<roxmltree::Node<'a, 'a>> {
    doc.descendants()
        .find(|n| n.attribute("NodeId") == Some(node_id))
}

fn extract_namespace_index(node_id: &str) -> Option<u16> {
    node_id.split(';').next()?.strip_prefix("ns=")?.parse().ok()
}

fn is_node_in_namespace(node_id: &str, ns_idx: u16) -> bool {
    extract_namespace_index(node_id) == Some(ns_idx)
}

fn has_parent_in_different_namespace(node_elem: &roxmltree::Node, ns_idx: u16) -> bool {
    if let Some(parent_id) = get_parent_id(node_elem) {
        !is_node_in_namespace(&parent_id, ns_idx)
    } else {
        false
    }
}

fn is_hierarchical_ref(ref_type: &str) -> bool {
    matches!(
        ref_type,
        "Organizes" | "HasComponent" | "HasOrderedComponent" | "HasEventSource" | "HasNotifier"
    )
}

fn get_model_uri(doc: &roxmltree::Document) -> Option<String> {
    doc.descendants()
        .find(|n| n.tag_name().name() == "Model")
        .and_then(|n| n.attribute("ModelUri"))
        .map(String::from)
}

fn is_model_object(node_elem: &roxmltree::Node, model_uri: &Option<String>) -> bool {
    let uri = match model_uri {
        Some(u) => u,
        None => return false,
    };

    if let Some(bn) = node_elem.attribute("BrowseName") {
        if bn.split(':').last().unwrap_or(bn) == uri {
            return true;
        }
    }

    for child in node_elem.children() {
        if child.tag_name().name() == "DisplayName" {
            if let Some(text) = child.text() {
                if text.trim() == uri {
                    return true;
                }
            }
        }
    }

    false
}

fn is_placeholder(node_elem: &roxmltree::Node) -> bool {
    if let Some(bn) = node_elem.attribute("BrowseName") {
        let name = bn.split(':').last().unwrap_or(bn);
        if name.starts_with('<') && name.ends_with("Identifier>") {
            return true;
        }
    }
    false
}

fn is_default_encoding(node_elem: &roxmltree::Node) -> bool {
    const ENCODINGS: &[&str] = &["Default Binary", "Default XML", "Default JSON"];

    if let Some(bn) = node_elem.attribute("BrowseName") {
        let name = bn.split(':').last().unwrap_or(bn);
        if ENCODINGS.contains(&name) {
            return true;
        }
    }
    false
}

pub fn save_entry_points_to_json(
    entry_points: &[NodeSetEntryPoints],
    filepath: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string_pretty(entry_points)?;
    fs::write(filepath, json)?;
    println!("💾 Entry-Points gespeichert: {}", filepath);
    Ok(())
}
