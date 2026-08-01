// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

use opcua::types::NodeId;
use std::fs;
use std::str::FromStr;

use crate::server_discovery::companion_specs::{
    find_nodeset_file_for_namespace, normalize_namespace_uri,
};
use crate::server_discovery::config::NamespaceEntry;

// Import-Statistiken für bessere Fehlerdiagnose
#[derive(Debug, Default)]
pub struct ImportStatistics {
    pub total_nodes_attempted: usize,
    pub successful_inserts: usize,
    pub failed_inserts: usize,
    pub skipped_references: usize,
    pub imported_node_ids: Vec<NodeId>,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ImportStatistics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn print_summary(&self) {
        println!("\n=== Import Statistics ===");
        println!("  Total nodes attempted: {}", self.total_nodes_attempted);
        println!("  ✓ Successful inserts: {}", self.successful_inserts);
        println!("  ✗ Failed inserts: {}", self.failed_inserts);
        println!("  ⚠ Skipped references: {}", self.skipped_references);

        if !self.warnings.is_empty() {
            println!("\n  Warnings ({}):", self.warnings.len());
            for warning in self.warnings.iter().take(10) {
                println!("    ⚠ {}", warning);
            }
            if self.warnings.len() > 10 {
                println!("    ... and {} more warnings", self.warnings.len() - 10);
            }
        }

        if !self.errors.is_empty() {
            println!("\n  Errors ({}):", self.errors.len());
            for error in self.errors.iter().take(10) {
                println!("    ✗ {}", error);
            }
            if self.errors.len() > 10 {
                println!("    ... and {} more errors", self.errors.len() - 10);
            }
        }
        println!("=========================\n");
    }
}

// Importiert alle NodeSets für die registrierten Namespaces in den Address Space
// WICHTIG: Muss NACH dem Registrieren der Namespaces aufgerufen werden
pub fn import_nodesets_to_address_space(
    address_space: &mut opcua::server::address_space::AddressSpace,
    namespaces: &[NamespaceEntry],
) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
    println!("\n=== Importiere NodeSets in Address Space ===");

    let mut total_stats = ImportStatistics::new();
    total_stats
        .imported_node_ids
        .extend(ensure_required_standard_types(address_space)?);

    // Überspringe Standard-Namespaces (0=UA, 1=Server)
    for ns_entry in namespaces {
        if ns_entry.nsid <= 1 {
            continue;
        }

        println!(
            "\n📦 Verarbeite Namespace: {} (nsid: {})",
            ns_entry.url, ns_entry.nsid
        );

        let import_result = (|| -> Result<ImportStatistics, Box<dyn std::error::Error>> {
            let xml_path = find_nodeset_file_for_namespace(&ns_entry.url)
                .ok_or_else(|| format!("Keine NodeSet XML gefunden für {}", ns_entry.url))?;
            println!("  ✓ NodeSet gefunden: {}", xml_path);

            let xml_content = fs::read_to_string(&xml_path)
                .map_err(|error| format!("Fehler beim Lesen von {}: {}", xml_path, error))?;

            import_xml_nodeset(address_space, &xml_content, &ns_entry.url, ns_entry.nsid)
        })();

        match import_result {
            Ok(stats) => {
                println!("  ✓ Import abgeschlossen");
                stats.print_summary();

                // Akkumuliere Statistiken
                total_stats.total_nodes_attempted += stats.total_nodes_attempted;
                total_stats.successful_inserts += stats.successful_inserts;
                total_stats.failed_inserts += stats.failed_inserts;
                total_stats.skipped_references += stats.skipped_references;
                total_stats
                    .imported_node_ids
                    .extend(stats.imported_node_ids);
                total_stats.errors.extend(stats.errors);
                total_stats.warnings.extend(stats.warnings);
            }
            Err(e) => {
                rollback_imported_nodes(address_space, &total_stats.imported_node_ids);
                return Err(format!(
                    "NodeSet-Import für {} fehlgeschlagen; Batch zurückgerollt: {}",
                    ns_entry.url, e
                )
                .into());
            }
        }
    }

    if let Err(error) =
        validate_imported_type_definitions(address_space, &total_stats.imported_node_ids)
    {
        rollback_imported_nodes(address_space, &total_stats.imported_node_ids);
        return Err(format!(
            "NodeSet-Import enthält ungültige HasTypeDefinition-Referenzen; Batch zurückgerollt: {}",
            error
        )
        .into());
    }

    println!("\n=== Gesamt-Import-Statistik ===");
    total_stats.print_summary();
    println!("=== NodeSet Import abgeschlossen ===\n");

    Ok(total_stats.imported_node_ids)
}

fn rollback_imported_nodes(
    address_space: &mut opcua::server::address_space::AddressSpace,
    imported_node_ids: &[NodeId],
) {
    for node_id in imported_node_ids.iter().rev() {
        address_space.delete(node_id, true);
    }
}

fn ensure_required_standard_types(
    address_space: &mut opcua::server::address_space::AddressSpace,
) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
    use opcua::server::prelude::{ObjectType, ReferenceDirection, VariableType};
    use opcua::types::QualifiedName;

    const MULTI_STATE_DICTIONARY_ENTRY_DISCRETE_BASE_TYPE: u32 = 19077;
    const MULTI_STATE_DICTIONARY_ENTRY_DISCRETE_TYPE: u32 = 19084;
    const MULTI_STATE_DISCRETE_TYPE: u32 = 11238;
    const NUMBER_DATA_TYPE: u32 = 26;
    const HAS_SUBTYPE: u32 = 45;
    const FOLDER_TYPE: u32 = 61;
    const ALIAS_NAME_CATEGORY_TYPE: u32 = 23456;

    let base_type_id = NodeId::new(0, MULTI_STATE_DICTIONARY_ENTRY_DISCRETE_BASE_TYPE);
    let concrete_type_id = NodeId::new(0, MULTI_STATE_DICTIONARY_ENTRY_DISCRETE_TYPE);
    let number_data_type = NodeId::new(0, NUMBER_DATA_TYPE);
    let has_subtype = NodeId::new(0, HAS_SUBTYPE);
    let mut inserted = Vec::new();

    for (node_id, browse_name, super_type_id) in [
        (
            base_type_id.clone(),
            "MultiStateDictionaryEntryDiscreteBaseType",
            NodeId::new(0, MULTI_STATE_DISCRETE_TYPE),
        ),
        (
            concrete_type_id,
            "MultiStateDictionaryEntryDiscreteType",
            base_type_id,
        ),
    ] {
        if address_space.find_node(&node_id).is_some() {
            continue;
        }
        if address_space.find_node(&super_type_id).is_none() {
            rollback_imported_nodes(address_space, &inserted);
            return Err(format!(
                "Standard-Supertyp {} für {} fehlt im Basis-Address-Space",
                super_type_id, node_id
            )
            .into());
        }

        let variable_type = VariableType::new(
            &node_id,
            QualifiedName::new(0, browse_name),
            browse_name,
            number_data_type.clone(),
            false,
            -1,
        );
        let references = [(&super_type_id, &has_subtype, ReferenceDirection::Inverse)];
        if !address_space.insert::<VariableType, NodeId>(variable_type, Some(&references)) {
            rollback_imported_nodes(address_space, &inserted);
            return Err(format!("Standardtyp {} konnte nicht ergänzt werden", node_id).into());
        }
        inserted.push(node_id);
    }

    let alias_type_id = NodeId::new(0, ALIAS_NAME_CATEGORY_TYPE);
    let folder_type_id = NodeId::new(0, FOLDER_TYPE);
    if address_space.find_node(&alias_type_id).is_none() {
        if address_space.find_node(&folder_type_id).is_none() {
            rollback_imported_nodes(address_space, &inserted);
            return Err("Standard-Supertyp i=61 fuer i=23456 fehlt im Basis-Address-Space".into());
        }

        let object_type = ObjectType::new(
            &alias_type_id,
            QualifiedName::new(0, "AliasNameCategoryType"),
            "AliasNameCategoryType",
            false,
        );
        let references = [(&folder_type_id, &has_subtype, ReferenceDirection::Inverse)];
        if !address_space.insert::<ObjectType, NodeId>(object_type, Some(&references)) {
            rollback_imported_nodes(address_space, &inserted);
            return Err("Standardtyp i=23456 konnte nicht ergaenzt werden".into());
        }
        inserted.push(alias_type_id);
    }

    Ok(inserted)
}

fn validate_imported_type_definitions(
    address_space: &opcua::server::address_space::AddressSpace,
    imported_node_ids: &[NodeId],
) -> Result<(), String> {
    let has_type_definition = NodeId::new(0, 40u32);
    let mut dangling = Vec::new();

    for source_node_id in imported_node_ids {
        if let Some(references) = address_space
            .find_references(source_node_id, Some((has_type_definition.clone(), false)))
        {
            for reference in references {
                if address_space.find_node(&reference.target_node).is_none() {
                    dangling.push(format!("{} -> {}", source_node_id, reference.target_node));
                }
            }
        }
    }

    if dangling.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} dangling Referenz(en), Beispiele: {}",
            dangling.len(),
            dangling
                .iter()
                .take(10)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }
}

// Importiert ein einzelnes NodeSet XML in den Address Space
// Gibt Import-Statistiken zurück
fn import_xml_nodeset(
    address_space: &mut opcua::server::address_space::AddressSpace,
    xml_content: &str,
    namespace_uri: &str,
    target_nsid: u16,
) -> Result<ImportStatistics, Box<dyn std::error::Error>> {
    println!("  🔄 Parse XML...");

    let doc = roxmltree::Document::parse(xml_content)?;
    let mut stats = ImportStatistics::new();

    // Parse Aliases (wichtig für ReferenceTypes!)
    let aliases = parse_aliases(&doc);
    println!("  📋 XML Aliases gefunden: {}", aliases.len());

    // Namespace-Index im XML finden
    let xml_namespace_map = parse_namespace_uris(&doc)?;
    println!("  📋 XML Namespaces: {:?}", xml_namespace_map);

    // Finde den Namespace-Index im XML der zu unserem target_nsid gemappt werden soll
    let xml_nsid = xml_namespace_map
        .iter()
        .find(|(_, uri)| normalize_namespace_uri(uri) == normalize_namespace_uri(namespace_uri))
        .map(|(idx, _)| *idx)
        .ok_or("Namespace nicht in XML gefunden")?;

    let registered_target_nsid =
        namespace_index_for_uri(address_space, namespace_uri).ok_or_else(|| {
            format!(
                "Namespace {} ist nicht im Server registriert",
                namespace_uri
            )
        })?;

    println!(
        "  🔄 Mappe XML nsid:{} → Aggregationsserver nsid:{} (Konfiguration: {})",
        xml_nsid, registered_target_nsid, target_nsid
    );

    // Erstelle Namespace-Mapping für alle Namespaces im XML
    let ns_mapping = create_namespace_mapping(&xml_namespace_map, address_space, &mut stats)?;
    if ns_mapping.get(&xml_nsid) != Some(&registered_target_nsid) {
        return Err(format!(
            "Namespace {} wurde nicht eindeutig auf Server-nsid {} gemappt",
            namespace_uri, registered_target_nsid
        )
        .into());
    }

    println!("  🔄 Importiere Nodes in 2 Phasen...");

    // Phase 1: ObjectTypes, VariableTypes, DataTypes, ReferenceTypes
    println!("  📦 Phase 1: Type Definitions");
    for node_elem in doc.descendants() {
        match node_elem.tag_name().name() {
            "UAObjectType" => {
                stats.total_nodes_attempted += 1;
                match import_object_type(
                    address_space,
                    &node_elem,
                    &ns_mapping,
                    &aliases,
                    &mut stats,
                ) {
                    Ok(node_id) => {
                        stats.successful_inserts += 1;
                        stats.imported_node_ids.push(node_id);
                    }
                    Err(e) => {
                        stats.failed_inserts += 1;
                        stats
                            .errors
                            .push(format!("ObjectType import failed: {}", e));
                    }
                }
            }
            "UAVariableType" => {
                stats.total_nodes_attempted += 1;
                match import_variable_type(
                    address_space,
                    &node_elem,
                    &ns_mapping,
                    &aliases,
                    &mut stats,
                ) {
                    Ok(node_id) => {
                        stats.successful_inserts += 1;
                        stats.imported_node_ids.push(node_id);
                    }
                    Err(e) => {
                        stats.failed_inserts += 1;
                        stats
                            .errors
                            .push(format!("VariableType import failed: {}", e));
                    }
                }
            }
            "UADataType" => {
                stats.total_nodes_attempted += 1;
                match import_data_type(address_space, &node_elem, &ns_mapping, &aliases, &mut stats)
                {
                    Ok(node_id) => {
                        stats.successful_inserts += 1;
                        stats.imported_node_ids.push(node_id);
                    }
                    Err(e) => {
                        stats.failed_inserts += 1;
                        stats.errors.push(format!("DataType import failed: {}", e));
                    }
                }
            }
            "UAReferenceType" => {
                stats.total_nodes_attempted += 1;
                match import_reference_type(
                    address_space,
                    &node_elem,
                    &ns_mapping,
                    &aliases,
                    &mut stats,
                ) {
                    Ok(node_id) => {
                        stats.successful_inserts += 1;
                        stats.imported_node_ids.push(node_id);
                    }
                    Err(e) => {
                        stats.failed_inserts += 1;
                        stats
                            .errors
                            .push(format!("ReferenceType import failed: {}", e));
                    }
                }
            }
            _ => {}
        }
    }

    // Phase 2: Objects, Variables, Methods
    println!("  📦 Phase 2: Instances");
    for node_elem in doc.descendants() {
        match node_elem.tag_name().name() {
            "UAObject" => {
                stats.total_nodes_attempted += 1;
                match import_object(address_space, &node_elem, &ns_mapping, &aliases, &mut stats) {
                    Ok(node_id) => {
                        stats.successful_inserts += 1;
                        stats.imported_node_ids.push(node_id);
                    }
                    Err(e) => {
                        stats.failed_inserts += 1;
                        stats.errors.push(format!("Object import failed: {}", e));
                    }
                }
            }
            "UAVariable" => {
                stats.total_nodes_attempted += 1;
                match import_variable(address_space, &node_elem, &ns_mapping, &aliases, &mut stats)
                {
                    Ok(node_id) => {
                        stats.successful_inserts += 1;
                        stats.imported_node_ids.push(node_id);
                    }
                    Err(e) => {
                        stats.failed_inserts += 1;
                        stats.errors.push(format!("Variable import failed: {}", e));
                    }
                }
            }
            "UAMethod" => {
                stats.total_nodes_attempted += 1;
                match import_method(address_space, &node_elem, &ns_mapping, &aliases, &mut stats) {
                    Ok(node_id) => {
                        stats.successful_inserts += 1;
                        stats.imported_node_ids.push(node_id);
                    }
                    Err(e) => {
                        stats.failed_inserts += 1;
                        stats.errors.push(format!("Method import failed: {}", e));
                    }
                }
            }
            _ => {}
        }
    }

    if stats.failed_inserts > 0 || stats.skipped_references > 0 || !stats.errors.is_empty() {
        let summary = format!(
            "{} fehlgeschlagene Node-Inserts, {} übersprungene Referenzen: {}",
            stats.failed_inserts,
            stats.skipped_references,
            stats
                .errors
                .iter()
                .take(10)
                .cloned()
                .collect::<Vec<_>>()
                .join("; ")
        );
        rollback_imported_nodes(address_space, &stats.imported_node_ids);
        return Err(summary.into());
    }

    if let Err(error) = validate_imported_type_definitions(address_space, &stats.imported_node_ids)
    {
        rollback_imported_nodes(address_space, &stats.imported_node_ids);
        return Err(error.into());
    }

    Ok(stats)
}

// Parst die Aliases aus dem XML (z.B. HasComponent → i=47)
fn parse_aliases(doc: &roxmltree::Document) -> std::collections::HashMap<String, String> {
    let mut aliases = std::collections::HashMap::new();

    // Standard OPC UA Aliases die IMMER existieren
    aliases.insert("Boolean".to_string(), "i=1".to_string());
    aliases.insert("SByte".to_string(), "i=2".to_string());
    aliases.insert("Byte".to_string(), "i=3".to_string());
    aliases.insert("Int16".to_string(), "i=4".to_string());
    aliases.insert("UInt16".to_string(), "i=5".to_string());
    aliases.insert("Int32".to_string(), "i=6".to_string());
    aliases.insert("UInt32".to_string(), "i=7".to_string());
    aliases.insert("Int64".to_string(), "i=8".to_string());
    aliases.insert("UInt64".to_string(), "i=9".to_string());
    aliases.insert("Float".to_string(), "i=10".to_string());
    aliases.insert("Double".to_string(), "i=11".to_string());
    aliases.insert("String".to_string(), "i=12".to_string());
    aliases.insert("DateTime".to_string(), "i=13".to_string());
    aliases.insert("Guid".to_string(), "i=14".to_string());
    aliases.insert("ByteString".to_string(), "i=15".to_string());
    aliases.insert("XmlElement".to_string(), "i=16".to_string());
    aliases.insert("NodeId".to_string(), "i=17".to_string());
    aliases.insert("ExpandedNodeId".to_string(), "i=18".to_string());
    aliases.insert("StatusCode".to_string(), "i=19".to_string());
    aliases.insert("QualifiedName".to_string(), "i=20".to_string());
    aliases.insert("LocalizedText".to_string(), "i=21".to_string());
    aliases.insert("Structure".to_string(), "i=22".to_string());
    aliases.insert("DataValue".to_string(), "i=23".to_string());
    aliases.insert("BaseDataType".to_string(), "i=24".to_string());
    aliases.insert("DiagnosticInfo".to_string(), "i=25".to_string());
    aliases.insert("Number".to_string(), "i=26".to_string());
    aliases.insert("Integer".to_string(), "i=27".to_string());
    aliases.insert("UInteger".to_string(), "i=28".to_string());
    aliases.insert("Enumeration".to_string(), "i=29".to_string());

    // Standard ReferenceTypes
    aliases.insert("Organizes".to_string(), "i=35".to_string());
    aliases.insert("HasComponent".to_string(), "i=47".to_string());
    aliases.insert("HasProperty".to_string(), "i=46".to_string());
    aliases.insert("HasTypeDefinition".to_string(), "i=40".to_string());
    aliases.insert("HasSubtype".to_string(), "i=45".to_string());
    aliases.insert("HasModellingRule".to_string(), "i=37".to_string());
    aliases.insert("HasEncoding".to_string(), "i=38".to_string());
    aliases.insert("HasDescription".to_string(), "i=39".to_string());
    aliases.insert("GeneratesEvent".to_string(), "i=41".to_string());
    aliases.insert("AlwaysGeneratesEvent".to_string(), "i=3065".to_string());
    aliases.insert("HasEventSource".to_string(), "i=36".to_string());
    aliases.insert("HasNotifier".to_string(), "i=48".to_string());
    aliases.insert("HasOrderedComponent".to_string(), "i=49".to_string());
    aliases.insert("FromState".to_string(), "i=51".to_string());
    aliases.insert("ToState".to_string(), "i=52".to_string());
    aliases.insert("HasCause".to_string(), "i=53".to_string());
    aliases.insert("HasEffect".to_string(), "i=54".to_string());
    aliases.insert("HasHistoricalConfiguration".to_string(), "i=56".to_string());
    aliases.insert("HasSubStateMachine".to_string(), "i=117".to_string());
    aliases.insert("HasArgumentDescription".to_string(), "i=15110".to_string());
    aliases.insert(
        "HasOptionalInputArgumentDescription".to_string(),
        "i=15114".to_string(),
    );
    aliases.insert("HasInterface".to_string(), "i=17603".to_string());
    aliases.insert("HasAddIn".to_string(), "i=17604".to_string());

    // Suche <Aliases> Element im XML
    for node in doc.descendants() {
        if node.tag_name().name() == "Aliases" {
            for alias_node in node.children() {
                if alias_node.tag_name().name() == "Alias" {
                    if let (Some(alias_name), Some(node_id)) =
                        (alias_node.attribute("Alias"), alias_node.text())
                    {
                        aliases.insert(alias_name.to_string(), node_id.to_string());
                    }
                }
            }
            break;
        }
    }

    aliases
}

// Parst die NamespaceUris aus dem XML
fn parse_namespace_uris(
    doc: &roxmltree::Document,
) -> Result<Vec<(u16, String)>, Box<dyn std::error::Error>> {
    let mut namespaces = vec![
        (0, "http://opcfoundation.org/UA/".to_string()), // Standard OPC UA
    ];

    // Suche <NamespaceUris> Element
    for node in doc.descendants() {
        if node.tag_name().name() == "NamespaceUris" {
            let mut idx = 1;
            for uri_node in node.children() {
                if uri_node.tag_name().name() == "Uri" {
                    if let Some(uri) = uri_node.text() {
                        namespaces.push((idx, uri.to_string()));
                        idx += 1;
                    }
                }
            }
            break;
        }
    }

    Ok(namespaces)
}

pub(crate) fn namespace_index_for_uri(
    address_space: &opcua::server::address_space::AddressSpace,
    namespace_uri: &str,
) -> Option<u16> {
    address_space.namespace_index(namespace_uri).or_else(|| {
        let alternate = if namespace_uri.ends_with('/') {
            namespace_uri.trim_end_matches('/').to_string()
        } else {
            format!("{}/", namespace_uri)
        };
        address_space.namespace_index(&alternate)
    })
}

// Erstellt Namespace-Mapping zwischen XML und Aggregationsserver
fn create_namespace_mapping(
    xml_namespaces: &[(u16, String)],
    address_space: &opcua::server::address_space::AddressSpace,
    stats: &mut ImportStatistics,
) -> Result<std::collections::HashMap<u16, u16>, Box<dyn std::error::Error>> {
    let mut mapping = std::collections::HashMap::new();
    mapping.insert(0, 0);
    for (xml_nsid, namespace_uri) in xml_namespaces {
        if let Some(server_nsid) = namespace_index_for_uri(address_space, namespace_uri) {
            mapping.insert(*xml_nsid, server_nsid);
            println!(
                "    ✓ Mappe XML ns:{} → Server ns:{} ({})",
                xml_nsid, server_nsid, namespace_uri
            );
        } else {
            let message = format!(
                "XML namespace ns:{} ({}) ist nicht im Server registriert",
                xml_nsid, namespace_uri
            );
            stats.errors.push(message.clone());
            return Err(message.into());
        }
    }

    Ok(mapping)
}

// Helper: Remappt eine NodeId vom XML-Namespace zum Server-Namespace
// VERBESSERT: Gibt klare Fehler bei fehlenden Mappings
// UNTERSTÜTZT: Alias-Resolution (z.B. "HasComponent" → "i=47")
fn remap_node_id(
    node_id_str: &str,
    ns_mapping: &std::collections::HashMap<u16, u16>,
) -> Result<opcua::types::NodeId, Box<dyn std::error::Error>> {
    remap_node_id_with_aliases(node_id_str, ns_mapping, &std::collections::HashMap::new())
}

fn remap_node_id_with_aliases(
    node_id_str: &str,
    ns_mapping: &std::collections::HashMap<u16, u16>,
    aliases: &std::collections::HashMap<String, String>,
) -> Result<opcua::types::NodeId, Box<dyn std::error::Error>> {
    use opcua::types::NodeId;

    // Prüfe ob es ein Alias ist (kein "ns=" oder "i=" Präfix)
    let actual_node_id_str = if !node_id_str.contains('=') {
        // Es ist ein Alias - suche in der Alias-Map
        aliases
            .get(node_id_str)
            .map(|s| s.as_str())
            .unwrap_or(node_id_str)
    } else {
        node_id_str
    };

    // Parse NodeId Format: "ns=X;i=Y" oder "ns=X;s=ABC"
    let parsed = NodeId::from_str(actual_node_id_str)
        .map_err(|_| format!("Ungültige NodeId: {}", node_id_str))?;

    // WICHTIG: Wenn Namespace nicht gemappt ist, FEHLER werfen statt falschen Namespace zu nutzen
    let new_ns = ns_mapping.get(&parsed.namespace).copied().ok_or_else(|| {
        format!(
            "Namespace {} für NodeId {} nicht gemappt",
            parsed.namespace, node_id_str
        )
    })?;

    // Erstelle neue NodeId mit gemapptem Namespace
    let remapped = match &parsed.identifier {
        opcua::types::Identifier::Numeric(id) => NodeId::new(new_ns, *id),
        opcua::types::Identifier::String(s) => NodeId::new(new_ns, s.as_ref().to_string()),
        opcua::types::Identifier::Guid(g) => NodeId::new(new_ns, g.clone()),
        opcua::types::Identifier::ByteString(b) => NodeId::new(new_ns, b.clone()),
    };

    Ok(remapped)
}

fn remap_browse_name(
    browse_name: &str,
    ns_mapping: &std::collections::HashMap<u16, u16>,
) -> Result<opcua::types::QualifiedName, Box<dyn std::error::Error>> {
    use opcua::types::{QualifiedName, UAString};

    let (xml_namespace, name) = match browse_name.split_once(':') {
        Some((prefix, name)) => match prefix.parse::<u16>() {
            Ok(namespace) => (namespace, name),
            Err(_) => (0, browse_name),
        },
        None => (0, browse_name),
    };
    let server_namespace = ns_mapping.get(&xml_namespace).copied().ok_or_else(|| {
        format!(
            "BrowseName {} verwendet nicht gemappten XML-Namespace {}",
            browse_name, xml_namespace
        )
    })?;

    Ok(QualifiedName {
        namespace_index: server_namespace,
        name: UAString::from(name),
    })
}

fn browse_name_local_part(browse_name: &str) -> &str {
    match browse_name.split_once(':') {
        Some((prefix, name)) if prefix.parse::<u16>().is_ok() => name,
        _ => browse_name,
    }
}

// Helper-Funktion: Parst Referenzen aus einem XML-Element
fn parse_references(
    elem: &roxmltree::Node,
    ns_mapping: &std::collections::HashMap<u16, u16>,
    aliases: &std::collections::HashMap<String, String>,
    node_id_str: &str,
    stats: &mut ImportStatistics,
) -> Result<Vec<(opcua::types::NodeId, opcua::types::NodeId, bool)>, Box<dyn std::error::Error>> {
    let mut references = Vec::new();

    for ref_elem in elem.children().filter(|n| n.has_tag_name("References")) {
        for ref_node in ref_elem.children().filter(|n| n.has_tag_name("Reference")) {
            let ref_type = ref_node.attribute("ReferenceType").ok_or_else(|| {
                stats.skipped_references += 1;
                format!("Reference in {} hat keinen ReferenceType", node_id_str)
            })?;
            let target_id_str = ref_node
                .text()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    stats.skipped_references += 1;
                    format!("Reference in {} hat kein Ziel", node_id_str)
                })?;
            let is_forward = ref_node
                .attribute("IsForward")
                .map(|v| v != "false" && v != "0")
                .unwrap_or(true);

            let target_id = remap_node_id_with_aliases(target_id_str, ns_mapping, aliases)
                .map_err(|error| {
                    stats.skipped_references += 1;
                    format!(
                        "Reference in {} kann Ziel {} nicht mappen: {}",
                        node_id_str, target_id_str, error
                    )
                })?;
            let ref_type_id =
                remap_node_id_with_aliases(ref_type, ns_mapping, aliases).map_err(|error| {
                    stats.skipped_references += 1;
                    format!(
                        "Reference in {} kann Typ {} nicht mappen: {}",
                        node_id_str, ref_type, error
                    )
                })?;
            references.push((target_id, ref_type_id, is_forward));
        }
    }

    Ok(references)
}

// KORRIGIERT: Akzeptiert ALLE Referenztypen (nicht nur numerische)
fn build_reference_tuples(
    references: &[(opcua::types::NodeId, opcua::types::NodeId, bool)],
    _stats: &mut ImportStatistics,
) -> Result<
    Vec<(
        opcua::types::NodeId,
        opcua::types::NodeId,
        opcua::server::prelude::ReferenceDirection,
    )>,
    Box<dyn std::error::Error>,
> {
    use opcua::server::prelude::ReferenceDirection;

    let tuples: Vec<_> = references
        .iter()
        .map(|(target, ref_type, is_forward)| {
            let dir = if *is_forward {
                ReferenceDirection::Forward
            } else {
                ReferenceDirection::Inverse
            };
            (target.clone(), ref_type.clone(), dir)
        })
        .collect();

    Ok(tuples)
}

// Importiert ein ObjectType aus XML in den Address Space
// KORRIGIERT: Nutzt ObjectType statt Object, korrekte Fehlerbehandlung
fn import_object_type(
    address_space: &mut opcua::server::address_space::AddressSpace,
    elem: &roxmltree::Node,
    ns_mapping: &std::collections::HashMap<u16, u16>,
    aliases: &std::collections::HashMap<String, String>,
    stats: &mut ImportStatistics,
) -> Result<NodeId, Box<dyn std::error::Error>> {
    use opcua::server::prelude::ObjectType;

    // NodeId auslesen und remappen
    let node_id_str = elem.attribute("NodeId").ok_or("UAObjectType ohne NodeId")?;
    let node_id = remap_node_id(node_id_str, ns_mapping)
        .map_err(|e| format!("NodeId remap failed for {}: {}", node_id_str, e))?;

    // BrowseName auslesen (Format: "ns:name")
    let browse_name_str = elem
        .attribute("BrowseName")
        .ok_or("UAObjectType ohne BrowseName")?;
    let name = browse_name_local_part(browse_name_str);
    let browse_name = remap_browse_name(browse_name_str, ns_mapping)?;

    let display_name = elem
        .children()
        .find(|n| n.has_tag_name("DisplayName"))
        .and_then(|n| n.text())
        .unwrap_or(name);

    // IsAbstract Attribut lesen
    let is_abstract = elem
        .attribute("IsAbstract")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    // KORRIGIERT: ObjectType statt Object verwenden!
    let node = ObjectType::new(&node_id, browse_name, display_name, is_abstract);

    // Referenzen parsen
    let references = parse_references(elem, ns_mapping, aliases, node_id_str, stats)?;

    // Node einfügen mit Fehlerbehandlung
    if references.is_empty() {
        let success = address_space.insert::<ObjectType, opcua::types::NodeId>(node, None);
        if !success {
            return Err(format!("Failed to insert ObjectType {}", node_id_str).into());
        }
    } else {
        let ref_tuples = build_reference_tuples(&references, stats)?;
        let ref_tuples_refs: Vec<_> = ref_tuples.iter().map(|(a, b, c)| (a, b, *c)).collect();
        let success =
            address_space.insert::<ObjectType, opcua::types::NodeId>(node, Some(&ref_tuples_refs));
        if !success {
            return Err(format!("Failed to insert ObjectType {} with refs", node_id_str).into());
        }
    }

    Ok(node_id)
}

// Importiert ein Object aus XML in den Address Space
// KORRIGIERT: Bessere Fehlerbehandlung
fn import_object(
    address_space: &mut opcua::server::address_space::AddressSpace,
    elem: &roxmltree::Node,
    ns_mapping: &std::collections::HashMap<u16, u16>,
    aliases: &std::collections::HashMap<String, String>,
    stats: &mut ImportStatistics,
) -> Result<NodeId, Box<dyn std::error::Error>> {
    use opcua::server::prelude::Object;

    // NodeId auslesen und remappen
    let node_id_str = elem.attribute("NodeId").ok_or("UAObject ohne NodeId")?;
    let node_id = remap_node_id(node_id_str, ns_mapping)
        .map_err(|e| format!("NodeId remap failed for {}: {}", node_id_str, e))?;

    // BrowseName auslesen (Format: "ns:name")
    let browse_name_str = elem
        .attribute("BrowseName")
        .ok_or("UAObject ohne BrowseName")?;
    let name = browse_name_local_part(browse_name_str);
    let browse_name = remap_browse_name(browse_name_str, ns_mapping)?;

    let display_name = elem
        .children()
        .find(|n| n.has_tag_name("DisplayName"))
        .and_then(|n| n.text())
        .unwrap_or(name);

    // EventNotifier (optional)
    let event_notifier = opcua::server::prelude::EventNotifier::empty();

    // Object erzeugen
    let node = Object::new(&node_id, browse_name, display_name, event_notifier);

    // Referenzen parsen
    let references = parse_references(elem, ns_mapping, aliases, node_id_str, stats)?;

    // Node einfügen mit Fehlerbehandlung
    if references.is_empty() {
        let success = address_space.insert::<Object, opcua::types::NodeId>(node, None);
        if !success {
            return Err(format!("Failed to insert Object {}", node_id_str).into());
        }
    } else {
        let ref_tuples = build_reference_tuples(&references, stats)?;
        let ref_tuples_refs: Vec<_> = ref_tuples.iter().map(|(a, b, c)| (a, b, *c)).collect();
        let success =
            address_space.insert::<Object, opcua::types::NodeId>(node, Some(&ref_tuples_refs));
        if !success {
            return Err(format!("Failed to insert Object {} with refs", node_id_str).into());
        }
    }

    Ok(node_id)
}

fn parse_localized_text(node: &roxmltree::Node<'_, '_>) -> opcua::types::LocalizedText {
    let locale = node
        .children()
        .find(|child| child.has_tag_name("Locale"))
        .and_then(|child| child.text())
        .unwrap_or("");
    let text = node
        .children()
        .find(|child| child.has_tag_name("Text"))
        .and_then(|child| child.text())
        .unwrap_or("");
    opcua::types::LocalizedText::new(locale, text)
}

fn parse_variable_value(
    elem: &roxmltree::Node<'_, '_>,
) -> Result<opcua::types::Variant, Box<dyn std::error::Error>> {
    use opcua::types::{service_types::EnumValueType, ExtensionObject, Variant, VariantTypeId};

    let Some(value_element) = elem.children().find(|child| child.has_tag_name("Value")) else {
        return Ok(Variant::Empty);
    };
    let Some(value_root) = value_element.children().find(|child| child.is_element()) else {
        return Ok(Variant::Empty);
    };

    match value_root.tag_name().name() {
        "ListOfLocalizedText" => {
            let values = value_root
                .children()
                .filter(|child| child.has_tag_name("LocalizedText"))
                .map(|child| Variant::from(parse_localized_text(&child)))
                .collect::<Vec<_>>();
            Ok(Variant::from((VariantTypeId::LocalizedText, values)))
        }
        "ListOfExtensionObject" => {
            let enum_values = value_root
                .descendants()
                .filter(|child| child.has_tag_name("EnumValueType"))
                .map(|enum_node| {
                    let value = enum_node
                        .children()
                        .find(|child| child.has_tag_name("Value"))
                        .and_then(|child| child.text())
                        .ok_or("EnumValueType ohne Value")?
                        .trim()
                        .parse::<i64>()?;
                    let display_name = enum_node
                        .children()
                        .find(|child| child.has_tag_name("DisplayName"))
                        .map(|child| parse_localized_text(&child))
                        .unwrap_or_default();
                    let description = enum_node
                        .children()
                        .find(|child| child.has_tag_name("Description"))
                        .map(|child| parse_localized_text(&child))
                        .unwrap_or_default();
                    let enum_value = EnumValueType {
                        value,
                        display_name,
                        description,
                    };
                    Ok::<Variant, Box<dyn std::error::Error>>(Variant::from(
                        ExtensionObject::from_encodable(NodeId::new(0, 8251u32), &enum_value),
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;

            if enum_values.is_empty() {
                Ok(Variant::String(opcua::types::UAString::from(
                    value_element.text().unwrap_or(""),
                )))
            } else {
                Ok(Variant::from((VariantTypeId::ExtensionObject, enum_values)))
            }
        }
        "LocalizedText" => Ok(Variant::from(parse_localized_text(&value_root))),
        "Boolean" => Ok(Variant::Boolean(
            value_root.text().unwrap_or("false").trim() == "true",
        )),
        "Int32" => Ok(Variant::Int32(
            value_root.text().unwrap_or("0").trim().parse()?,
        )),
        "UInt32" => Ok(Variant::UInt32(
            value_root.text().unwrap_or("0").trim().parse()?,
        )),
        "Int64" => Ok(Variant::Int64(
            value_root.text().unwrap_or("0").trim().parse()?,
        )),
        "UInt64" => Ok(Variant::UInt64(
            value_root.text().unwrap_or("0").trim().parse()?,
        )),
        "Float" => Ok(Variant::Float(
            value_root.text().unwrap_or("0").trim().parse()?,
        )),
        "Double" => Ok(Variant::Double(
            value_root.text().unwrap_or("0").trim().parse()?,
        )),
        _ => Ok(Variant::String(opcua::types::UAString::from(
            value_root.text().unwrap_or(""),
        ))),
    }
}

// Importiert ein Variable aus XML in den Address Space
// KORRIGIERT: JETZT MIT REFERENZEN!
fn import_variable(
    address_space: &mut opcua::server::address_space::AddressSpace,
    elem: &roxmltree::Node,
    ns_mapping: &std::collections::HashMap<u16, u16>,
    aliases: &std::collections::HashMap<String, String>,
    stats: &mut ImportStatistics,
) -> Result<NodeId, Box<dyn std::error::Error>> {
    use opcua::server::prelude::Variable;

    // NodeId auslesen und remappen
    let node_id_str = elem.attribute("NodeId").ok_or("UAVariable ohne NodeId")?;
    let node_id = remap_node_id(node_id_str, ns_mapping)
        .map_err(|e| format!("NodeId remap failed for {}: {}", node_id_str, e))?;

    let browse_name_str = elem
        .attribute("BrowseName")
        .ok_or("UAVariable ohne BrowseName")?;
    let name = browse_name_local_part(browse_name_str);
    let browse_name = remap_browse_name(browse_name_str, ns_mapping)?;

    let display_name = elem
        .children()
        .find(|n| n.has_tag_name("DisplayName"))
        .and_then(|n| n.text())
        .unwrap_or(name);

    let data_type = match elem.attribute("DataType") {
        Some(data_type) => {
            remap_node_id_with_aliases(data_type, ns_mapping, aliases).map_err(|error| {
                format!(
                    "DataType {} kann nicht gemappt werden: {}",
                    data_type, error
                )
            })?
        }
        None => NodeId::new(0, 24),
    };

    let value = parse_variable_value(elem)?;
    let value_rank = elem
        .attribute("ValueRank")
        .and_then(|rank| rank.parse::<i32>().ok());
    let array_dimensions = elem
        .attribute("ArrayDimensions")
        .and_then(|dimensions| dimensions.split(',').next())
        .and_then(|dimension| dimension.trim().parse::<u32>().ok());

    let node: Variable = Variable::new_data_value(
        &node_id,
        browse_name,
        display_name,
        data_type,
        value_rank,
        array_dimensions,
        value,
    );

    // KORRIGIERT: Referenzen parsen (wie bei Object)!
    let references = parse_references(elem, ns_mapping, aliases, node_id_str, stats)?;

    // Node einfügen mit Fehlerbehandlung
    if references.is_empty() {
        let success = address_space.insert::<Variable, opcua::types::NodeId>(node, None);
        if !success {
            return Err(format!("Failed to insert Variable {}", node_id_str).into());
        }
    } else {
        let ref_tuples = build_reference_tuples(&references, stats)?;
        let ref_tuples_refs: Vec<_> = ref_tuples.iter().map(|(a, b, c)| (a, b, *c)).collect();
        let success =
            address_space.insert::<Variable, opcua::types::NodeId>(node, Some(&ref_tuples_refs));
        if !success {
            return Err(format!("Failed to insert Variable {} with refs", node_id_str).into());
        }
    }

    Ok(node_id)
}

// Importiert ein VariableType aus XML in den Address Space
// KORRIGIERT: Bessere Fehlerbehandlung
fn import_variable_type(
    address_space: &mut opcua::server::address_space::AddressSpace,
    elem: &roxmltree::Node,
    ns_mapping: &std::collections::HashMap<u16, u16>,
    aliases: &std::collections::HashMap<String, String>,
    stats: &mut ImportStatistics,
) -> Result<NodeId, Box<dyn std::error::Error>> {
    use opcua::server::prelude::VariableType;

    // NodeId auslesen und remappen
    let node_id_str = elem
        .attribute("NodeId")
        .ok_or("UAVariableType ohne NodeId")?;
    let node_id = remap_node_id(node_id_str, ns_mapping)
        .map_err(|e| format!("NodeId remap failed for {}: {}", node_id_str, e))?;

    // BrowseName auslesen (Format: "ns:name")
    let browse_name_str = elem
        .attribute("BrowseName")
        .ok_or("UAVariableType ohne BrowseName")?;
    let name = browse_name_local_part(browse_name_str);
    let browse_name = remap_browse_name(browse_name_str, ns_mapping)?;

    let display_name = elem
        .children()
        .find(|n| n.has_tag_name("DisplayName"))
        .and_then(|n| n.text())
        .unwrap_or(name);

    let data_type = match elem.attribute("DataType") {
        Some(data_type) => {
            remap_node_id_with_aliases(data_type, ns_mapping, aliases).map_err(|error| {
                format!(
                    "DataType {} kann nicht gemappt werden: {}",
                    data_type, error
                )
            })?
        }
        None => NodeId::new(0, 24),
    };

    let value_rank = elem
        .attribute("ValueRank")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(-1);
    let is_abstract = elem
        .attribute("IsAbstract")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    let node: VariableType = VariableType::new(
        &node_id,
        browse_name,
        display_name,
        data_type,
        is_abstract,
        value_rank,
    );

    // Referenzen parsen
    let references = parse_references(elem, ns_mapping, aliases, node_id_str, stats)?;

    // Node einfügen mit Fehlerbehandlung
    if references.is_empty() {
        let success = address_space.insert::<VariableType, opcua::types::NodeId>(node, None);
        if !success {
            return Err(format!("Failed to insert VariableType {}", node_id_str).into());
        }
    } else {
        let ref_tuples = build_reference_tuples(&references, stats)?;
        let ref_tuples_refs: Vec<_> = ref_tuples.iter().map(|(a, b, c)| (a, b, *c)).collect();
        let success = address_space
            .insert::<VariableType, opcua::types::NodeId>(node, Some(&ref_tuples_refs));
        if !success {
            return Err(format!("Failed to insert VariableType {} with refs", node_id_str).into());
        }
    }

    Ok(node_id)
}

// Importiert ein ReferenceType aus XML in den Address Space
// KORRIGIERT: Bessere Fehlerbehandlung
fn import_reference_type(
    address_space: &mut opcua::server::address_space::AddressSpace,
    elem: &roxmltree::Node,
    ns_mapping: &std::collections::HashMap<u16, u16>,
    aliases: &std::collections::HashMap<String, String>,
    stats: &mut ImportStatistics,
) -> Result<NodeId, Box<dyn std::error::Error>> {
    use opcua::server::prelude::ReferenceType;
    use opcua::types::LocalizedText;

    // NodeId auslesen und remappen
    let node_id_str = elem
        .attribute("NodeId")
        .ok_or("UAReferenceType ohne NodeId")?;
    let node_id = remap_node_id(node_id_str, ns_mapping)
        .map_err(|e| format!("NodeId remap failed for {}: {}", node_id_str, e))?;

    // BrowseName auslesen (Format: "ns:name")
    let browse_name_str = elem
        .attribute("BrowseName")
        .ok_or("UAReferenceType ohne BrowseName")?;
    let name = browse_name_local_part(browse_name_str);
    let browse_name = remap_browse_name(browse_name_str, ns_mapping)?;

    let display_name = elem
        .children()
        .find(|n| n.has_tag_name("DisplayName"))
        .and_then(|n| n.text())
        .map(LocalizedText::from)
        .unwrap_or_else(|| LocalizedText::from(name));

    let is_abstract = elem
        .attribute("IsAbstract")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let symmetric = elem
        .attribute("Symmetric")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    let node: ReferenceType = ReferenceType::new(
        &node_id,
        browse_name,
        display_name,
        Some(LocalizedText::from(is_abstract.to_string())),
        symmetric,
        is_abstract,
    );

    // Referenzen parsen
    let references = parse_references(elem, ns_mapping, aliases, node_id_str, stats)?;

    // Node einfügen mit Fehlerbehandlung
    if references.is_empty() {
        let success = address_space.insert::<ReferenceType, opcua::types::NodeId>(node, None);
        if !success {
            return Err(format!("Failed to insert ReferenceType {}", node_id_str).into());
        }
    } else {
        let ref_tuples = build_reference_tuples(&references, stats)?;
        let ref_tuples_refs: Vec<_> = ref_tuples.iter().map(|(a, b, c)| (a, b, *c)).collect();
        let success = address_space
            .insert::<ReferenceType, opcua::types::NodeId>(node, Some(&ref_tuples_refs));
        if !success {
            return Err(format!("Failed to insert ReferenceType {} with refs", node_id_str).into());
        }
    }

    Ok(node_id)
}

// Importiert ein DataType aus XML in den Address Space
// KORRIGIERT: Bessere Fehlerbehandlung
fn import_data_type(
    address_space: &mut opcua::server::address_space::AddressSpace,
    elem: &roxmltree::Node,
    ns_mapping: &std::collections::HashMap<u16, u16>,
    aliases: &std::collections::HashMap<String, String>,
    stats: &mut ImportStatistics,
) -> Result<NodeId, Box<dyn std::error::Error>> {
    use opcua::server::prelude::DataType;

    // NodeId auslesen und remappen
    let node_id_str = elem.attribute("NodeId").ok_or("UADataType ohne NodeId")?;
    let node_id = remap_node_id(node_id_str, ns_mapping)
        .map_err(|e| format!("NodeId remap failed for {}: {}", node_id_str, e))?;

    // BrowseName auslesen (Format: "ns:name")
    let browse_name_str = elem
        .attribute("BrowseName")
        .ok_or("UADataType ohne BrowseName")?;
    let name = browse_name_local_part(browse_name_str);
    let browse_name = remap_browse_name(browse_name_str, ns_mapping)?;

    let display_name = elem
        .children()
        .find(|n| n.has_tag_name("DisplayName"))
        .and_then(|n| n.text())
        .unwrap_or(name);

    let is_abstract = elem
        .attribute("IsAbstract")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    let node: DataType = DataType::new(&node_id, browse_name, display_name, is_abstract);

    // Referenzen parsen
    let references = parse_references(elem, ns_mapping, aliases, node_id_str, stats)?;

    // Node einfügen mit Fehlerbehandlung
    if references.is_empty() {
        let success = address_space.insert::<DataType, opcua::types::NodeId>(node, None);
        if !success {
            return Err(format!("Failed to insert DataType {}", node_id_str).into());
        }
    } else {
        let ref_tuples = build_reference_tuples(&references, stats)?;
        let ref_tuples_refs: Vec<_> = ref_tuples.iter().map(|(a, b, c)| (a, b, *c)).collect();
        let success =
            address_space.insert::<DataType, opcua::types::NodeId>(node, Some(&ref_tuples_refs));
        if !success {
            return Err(format!("Failed to insert DataType {} with refs", node_id_str).into());
        }
    }

    Ok(node_id)
}

// Importiert ein Method aus XML in den Address Space
// HINWEIS: In opcua-rust werden Methods als Objects mit speziellen Attributen behandelt
fn import_method(
    address_space: &mut opcua::server::address_space::AddressSpace,
    elem: &roxmltree::Node,
    ns_mapping: &std::collections::HashMap<u16, u16>,
    aliases: &std::collections::HashMap<String, String>,
    stats: &mut ImportStatistics,
) -> Result<NodeId, Box<dyn std::error::Error>> {
    use opcua::server::prelude::Object;

    let node_id_str = elem.attribute("NodeId").ok_or("UAMethod ohne NodeId")?;
    let node_id = remap_node_id(node_id_str, ns_mapping)
        .map_err(|e| format!("NodeId remap failed for {}: {}", node_id_str, e))?;

    let browse_name_str = elem
        .attribute("BrowseName")
        .ok_or("UAMethod ohne BrowseName")?;
    let name = browse_name_local_part(browse_name_str);
    let browse_name = remap_browse_name(browse_name_str, ns_mapping)?;

    let display_name = elem
        .children()
        .find(|n| n.has_tag_name("DisplayName"))
        .and_then(|n| n.text())
        .unwrap_or(name);

    // Methods werden als Objects behandelt
    // Executable und UserExecutable Attribute werden über Properties gesetzt
    let node = Object::new(
        &node_id,
        browse_name,
        display_name,
        opcua::server::prelude::EventNotifier::empty(),
    );

    // Referenzen parsen
    let references = parse_references(elem, ns_mapping, aliases, node_id_str, stats)?;

    // Node einfügen mit Fehlerbehandlung
    if references.is_empty() {
        let success = address_space.insert::<Object, opcua::types::NodeId>(node, None);
        if !success {
            return Err(format!("Failed to insert Method {} as Object", node_id_str).into());
        }
    } else {
        let ref_tuples = build_reference_tuples(&references, stats)?;
        let ref_tuples_refs: Vec<_> = ref_tuples.iter().map(|(a, b, c)| (a, b, *c)).collect();
        let success =
            address_space.insert::<Object, opcua::types::NodeId>(node, Some(&ref_tuples_refs));
        if !success {
            return Err(format!(
                "Failed to insert Method {} as Object with refs",
                node_id_str
            )
            .into());
        }
    }

    Ok(node_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opcua::server::address_space::AddressSpace;
    use std::collections::{BTreeMap, HashSet};
    use std::path::PathBuf;

    const NS: &str = "urn:ojies:test:types";

    fn nodeset(body: &str, namespace_uri: &str) -> String {
        format!(
            r#"<UANodeSet>
  <NamespaceUris><Uri>{namespace_uri}</Uri></NamespaceUris>
  {body}
</UANodeSet>"#
        )
    }

    #[test]
    fn namespace_lookup_accepts_both_trailing_slash_variants() {
        let mut without_slash = AddressSpace::new();
        let ns_without_slash = without_slash.register_namespace(NS).unwrap();
        assert_eq!(
            namespace_index_for_uri(&without_slash, &format!("{}/", NS)),
            Some(ns_without_slash)
        );

        let mut with_slash = AddressSpace::new();
        let ns_with_slash = with_slash.register_namespace(&format!("{}/", NS)).unwrap();
        assert_eq!(
            namespace_index_for_uri(&with_slash, NS),
            Some(ns_with_slash)
        );
    }

    #[test]
    fn required_standard_types_include_alias_name_category_type() {
        let mut address_space = AddressSpace::new();

        let inserted = ensure_required_standard_types(&mut address_space).unwrap();
        let alias_type_id = NodeId::new(0, 23456u32);

        assert!(inserted.contains(&alias_type_id));
        assert!(address_space.find_node(&alias_type_id).is_some());
    }

    #[test]
    fn parses_enum_strings_as_localized_text_array() {
        let document = roxmltree::Document::parse(
            r#"<UAVariable>
  <Value>
    <ListOfLocalizedText>
      <LocalizedText><Text>NORMAL</Text></LocalizedText>
      <LocalizedText><Locale>en</Locale><Text>FAILURE</Text></LocalizedText>
    </ListOfLocalizedText>
  </Value>
</UAVariable>"#,
        )
        .unwrap();

        let value = parse_variable_value(&document.root_element()).unwrap();
        let opcua::types::Variant::Array(array) = value else {
            panic!("EnumStrings must be imported as an array");
        };
        assert_eq!(array.value_type, opcua::types::VariantTypeId::LocalizedText);
        assert_eq!(array.values.len(), 2);
        let opcua::types::Variant::LocalizedText(second) = &array.values[1] else {
            panic!("EnumStrings entries must be LocalizedText values");
        };
        assert_eq!(second.locale.to_string(), "en");
        assert_eq!(second.text.to_string(), "FAILURE");
    }

    #[test]
    fn parses_enum_values_as_binary_enum_value_type_array() {
        let document = roxmltree::Document::parse(
            r#"<UAVariable>
  <Value>
    <ListOfExtensionObject>
      <ExtensionObject>
        <Body>
          <EnumValueType>
            <Value>2</Value>
            <DisplayName><Text>Other</Text></DisplayName>
            <Description><Locale>en</Locale><Text>Fallback value.</Text></Description>
          </EnumValueType>
        </Body>
      </ExtensionObject>
    </ListOfExtensionObject>
  </Value>
</UAVariable>"#,
        )
        .unwrap();

        let value = parse_variable_value(&document.root_element()).unwrap();
        let opcua::types::Variant::Array(array) = value else {
            panic!("EnumValues must be imported as an array");
        };
        assert_eq!(
            array.value_type,
            opcua::types::VariantTypeId::ExtensionObject
        );
        let opcua::types::Variant::ExtensionObject(encoded) = &array.values[0] else {
            panic!("EnumValues entries must be ExtensionObjects");
        };
        let decoded: opcua::types::service_types::EnumValueType = encoded
            .decode_inner(&opcua::types::DecodingOptions::minimal())
            .unwrap();
        assert_eq!(decoded.value, 2);
        assert_eq!(decoded.display_name.text.to_string(), "Other");
        assert_eq!(decoded.description.locale.to_string(), "en");
        assert_eq!(decoded.description.text.to_string(), "Fallback value.");
    }

    #[test]
    fn slash_variant_imports_into_the_registered_namespace() {
        let mut address_space = AddressSpace::new();
        let target_nsid = address_space.register_namespace(NS).unwrap();
        let xml = nodeset(
            r#"<UADataType NodeId="ns=1;i=5001" BrowseName="1:FixtureType">
  <DisplayName>FixtureType</DisplayName>
</UADataType>"#,
            &format!("{}/", NS),
        );

        let stats = import_xml_nodeset(&mut address_space, &xml, NS, target_nsid).unwrap();

        assert_eq!(stats.failed_inserts, 0);
        assert_eq!(stats.successful_inserts, 1);
        assert!(address_space
            .find_node(&NodeId::new(target_nsid, 5001u32))
            .is_some());
    }

    #[test]
    fn unregistered_xml_namespace_fails_before_inserting_nodes() {
        let mut address_space = AddressSpace::new();
        let target_nsid = address_space.register_namespace(NS).unwrap();
        let xml = format!(
            r#"<UANodeSet>
  <NamespaceUris>
    <Uri>{NS}</Uri>
    <Uri>urn:ojies:test:missing</Uri>
  </NamespaceUris>
  <UADataType NodeId="ns=1;i=5002" BrowseName="1:MustNotRemain">
    <DisplayName>MustNotRemain</DisplayName>
  </UADataType>
</UANodeSet>"#
        );

        let error = import_xml_nodeset(&mut address_space, &xml, NS, target_nsid)
            .expect_err("an unregistered XML namespace must fail closed");

        assert!(error.to_string().contains("nicht im Server registriert"));
        assert!(address_space
            .find_node(&NodeId::new(target_nsid, 5002u32))
            .is_none());
    }

    #[test]
    fn failed_node_insert_rolls_back_the_complete_nodeset() {
        let mut address_space = AddressSpace::new();
        let target_nsid = address_space.register_namespace(NS).unwrap();
        let xml = nodeset(
            r#"<UADataType NodeId="ns=1;i=5003" BrowseName="1:First">
  <DisplayName>First</DisplayName>
</UADataType>
<UADataType NodeId="ns=1;i=5003" BrowseName="1:Duplicate">
  <DisplayName>Duplicate</DisplayName>
</UADataType>"#,
            NS,
        );

        let error = import_xml_nodeset(&mut address_space, &xml, NS, target_nsid)
            .expect_err("a partial NodeSet insert must fail");

        assert!(error.to_string().contains("fehlgeschlagene Node-Inserts"));
        assert!(address_space
            .find_node(&NodeId::new(target_nsid, 5003u32))
            .is_none());
    }

    #[test]
    fn malformed_reference_is_not_silently_skipped() {
        let mut address_space = AddressSpace::new();
        let target_nsid = address_space.register_namespace(NS).unwrap();
        let xml = nodeset(
            r#"<UADataType NodeId="ns=1;i=5004" BrowseName="1:BrokenReference">
  <DisplayName>BrokenReference</DisplayName>
  <References><Reference>i=24</Reference></References>
</UADataType>"#,
            NS,
        );

        let error = import_xml_nodeset(&mut address_space, &xml, NS, target_nsid)
            .expect_err("a malformed reference must fail closed");

        assert!(error.to_string().contains("keinen ReferenceType"));
        assert!(address_space
            .find_node(&NodeId::new(target_nsid, 5004u32))
            .is_none());
    }

    #[test]
    fn dangling_type_definition_rolls_back_the_nodeset() {
        let mut address_space = AddressSpace::new();
        let target_nsid = address_space.register_namespace(NS).unwrap();
        let xml = nodeset(
            r#"<UAObject NodeId="ns=1;i=5005" BrowseName="1:BrokenInstance">
  <DisplayName>BrokenInstance</DisplayName>
  <References>
    <Reference ReferenceType="HasTypeDefinition">ns=1;i=5999</Reference>
  </References>
</UAObject>"#,
            NS,
        );

        let error = import_xml_nodeset(&mut address_space, &xml, NS, target_nsid)
            .expect_err("a dangling HasTypeDefinition target must fail closed");

        assert!(error.to_string().contains("dangling Referenz"));
        assert!(address_space
            .find_node(&NodeId::new(target_nsid, 5005u32))
            .is_none());
    }

    #[test]
    fn repository_nodesets_import_without_partial_or_dangling_types() {
        #[derive(Debug)]
        struct RequiredModel {
            model_uri: String,
            publication_date: String,
        }

        #[derive(Debug)]
        struct ModelFile {
            path: PathBuf,
            model_uri: String,
            publication_date: String,
            required_models: Vec<RequiredModel>,
        }

        let nodeset_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("nodesets");
        let mut models = BTreeMap::new();
        for entry in std::fs::read_dir(&nodeset_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("xml") {
                continue;
            }

            let xml = std::fs::read_to_string(&path).unwrap();
            let document = roxmltree::Document::parse(&xml).unwrap();
            let model = document
                .descendants()
                .find(|node| node.has_tag_name("Model"))
                .unwrap_or_else(|| panic!("{} has no Model element", path.display()));
            let model_uri = model.attribute("ModelUri").unwrap().to_string();
            let publication_date = model.attribute("PublicationDate").unwrap().to_string();
            let required_models = model
                .children()
                .filter(|node| node.has_tag_name("RequiredModel"))
                .map(|node| RequiredModel {
                    model_uri: node.attribute("ModelUri").unwrap().to_string(),
                    publication_date: node.attribute("PublicationDate").unwrap().to_string(),
                })
                .collect::<Vec<_>>();
            let canonical_uri = normalize_namespace_uri(&model_uri);
            assert!(
                models
                    .insert(
                        canonical_uri.clone(),
                        ModelFile {
                            path,
                            model_uri,
                            publication_date,
                            required_models,
                        },
                    )
                    .is_none(),
                "duplicate model URI {}",
                canonical_uri
            );
        }

        let base_uri = normalize_namespace_uri("http://opcfoundation.org/UA/");
        for model in models.values() {
            for required in &model.required_models {
                let required_uri = normalize_namespace_uri(&required.model_uri);
                assert!(
                    required_uri == base_uri || models.contains_key(&required_uri),
                    "{} requires missing model {}",
                    model.path.display(),
                    required_uri
                );
                if required_uri != base_uri {
                    let dependency = &models[&required_uri];
                    assert!(
                        dependency.publication_date >= required.publication_date,
                        "{} requires {} from {}, but repository has {}",
                        model.path.display(),
                        required_uri,
                        required.publication_date,
                        dependency.publication_date
                    );
                }
            }
        }

        let mut address_space = AddressSpace::new();
        let mut imported_node_ids = ensure_required_standard_types(&mut address_space).unwrap();
        let mut server_namespaces = BTreeMap::new();
        for (canonical_uri, model) in &models {
            let nsid = address_space.register_namespace(&model.model_uri).unwrap();
            server_namespaces.insert(canonical_uri.clone(), nsid);
        }

        let mut imported_models = HashSet::from([base_uri]);
        let mut pending = models.keys().cloned().collect::<Vec<_>>();
        while !pending.is_empty() {
            let before = pending.len();
            let mut still_pending = Vec::new();
            for canonical_uri in pending {
                let model = &models[&canonical_uri];
                if !model.required_models.iter().all(|required| {
                    imported_models.contains(&normalize_namespace_uri(&required.model_uri))
                }) {
                    still_pending.push(canonical_uri);
                    continue;
                }

                let xml = std::fs::read_to_string(&model.path).unwrap();
                let nsid = server_namespaces[&canonical_uri];
                let stats = import_xml_nodeset(&mut address_space, &xml, &model.model_uri, nsid)
                    .unwrap_or_else(|error| panic!("{} failed: {}", model.path.display(), error));
                imported_node_ids.extend(stats.imported_node_ids);
                imported_models.insert(canonical_uri);
            }
            assert!(
                still_pending.len() < before,
                "cyclic or unresolved RequiredModel graph: {:?}",
                still_pending
            );
            pending = still_pending;
        }

        assert!(!imported_node_ids.is_empty());
        validate_imported_type_definitions(&address_space, &imported_node_ids).unwrap();
    }
}
