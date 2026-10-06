// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

use opcua::client::prelude::*;
use opcua::server::address_space::AddressSpace;
use opcua::sync::RwLock;
use serde::ser::{SerializeSeq, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::server_discovery::entry_point_generator::{EntryPoint, NodeSetEntryPoints};

const FOLDER_TYPE_ID: u32 = 61;
const OBJECTS_NODE_ID: u32 = 85;
const HIERARCHICAL_REFERENCES_ID: u32 = 33;
const ORGANIZES_REFERENCE_ID: u32 = 35;
const HAS_PROPERTY_REFERENCE_ID: u32 = 46;
const HAS_COMPONENT_REFERENCE_ID: u32 = 47;
const HAS_ORDERED_COMPONENT_REFERENCE_ID: u32 = 49;
const REFERENCE_POLICY: &str = "normalize_tree_reference_to_forward_organizes";

const ORGANIZES_REF_PATH: &[&str] = &[
    "Types",
    "ReferenceTypes",
    "References",
    "HierarchicalReferences",
    "Organizes",
];

const MOTION_DEVICE_SYSTEM_COLLECTIONS: &[&str] = &["MotionDevices", "Controllers", "SafetyStates"];
const DUAL_CAMERA_VISION_SYSTEM_COLLECTIONS: &[&str] = &[
    "Configuration",
    "Recipes",
    "RecipeManagement",
    "Products",
    "SafetyState",
    "SafetyStateManagement",
    "ResultManagement",
    "Results",
    "ConfigurationManagement",
    "Configurations",
    "Cameras",
    "VisionStateMachine",
    "AutomaticMode",
];
const VISION_SYSTEM_ASSET_COLLECTIONS: &[&str] = &[
    "Identification",
    "ComputingDevices",
    "SoftwareAndLicenses",
    "ImageSensors",
    "Lenses",
    "Lamps",
];
static NODE_ID_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(50000);
static PHASE_TIMING_ENABLED: OnceLock<bool> = OnceLock::new();

fn phase_timing_enabled() -> bool {
    *PHASE_TIMING_ENABLED.get_or_init(|| {
        std::env::var("OJIES_PHASE_TIMING")
            .map(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

fn log_rule_timing(server: &str, phase: &str, elapsed: Duration) {
    if phase_timing_enabled() {
        warn!(
            "OJIES_RULE_TIMING server={} phase={} elapsed_ms={:.3}",
            server,
            phase,
            elapsed.as_secs_f64() * 1000.0
        );
    }
}

fn next_node_id(ns_idx: u16) -> NodeId {
    let id = NODE_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    NodeId::new(ns_idx, id)
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedPathElement {
    pub namespace_uri: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identifier: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedNodeIdentity {
    pub namespace_uri: String,
    pub identifier: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct AggregationRule {
    pub target_node: Vec<String>,
    pub source_node: Vec<String>,
    pub ref_type: Vec<String>,
    pub is_forward: bool,
    #[serde(default)]
    pub target_node_qualified: Vec<QualifiedPathElement>,
    #[serde(default)]
    pub source_node_qualified: Vec<QualifiedPathElement>,
    #[serde(default)]
    pub source_id: String,
    #[serde(default)]
    pub source_node_id: Option<QualifiedNodeIdentity>,
    #[serde(default)]
    pub reference_type: Option<QualifiedNodeIdentity>,
    #[serde(default)]
    pub source_reference_type: Option<QualifiedNodeIdentity>,
    #[serde(default)]
    pub source_reference_is_forward: Option<bool>,
    #[serde(default)]
    pub reference_policy: String,
    #[serde(default)]
    pub merge_policy: String,
    #[serde(default)]
    pub merge_key: Vec<QualifiedPathElement>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ExecutorRule {
    target_node: Vec<QualifiedPathElement>,
    source_node: Vec<QualifiedPathElement>,
    ref_type: Vec<String>,
    is_forward: bool,
    source_id: String,
    source_node_id: Option<QualifiedNodeIdentity>,
    reference_type: Option<QualifiedNodeIdentity>,
    merge_policy: String,
    merge_key: Vec<QualifiedPathElement>,
}

impl ExecutorRule {
    /// Convert only this new projection, never the accumulated collection.
    pub(crate) fn to_instance_mapping_rule(
        &self,
    ) -> Result<opcua::server::aggregation_server::util_types::InstanceMappingRule, serde_json::Error>
    {
        serde_json::from_value(serde_json::to_value(self)?)
    }
}

#[derive(Debug)]
pub struct GeneratedRules {
    pub created_node_ids: Vec<NodeId>,
    pub rules: Vec<AggregationRule>,
    pub shadowed_rules: ShadowedRulePaths,
}

#[derive(Debug, Clone)]
struct ChildNode {
    node_id: NodeId,
    browse_namespace: u16,
    browse_name: String,
    display_name: String,
    node_class: NodeClass,
    type_definition: Option<NodeId>,
    reference_type_id: NodeId,
    is_forward: bool,
}

#[derive(Debug, Clone)]
struct StructuralChildren {
    containers: Vec<(ChildNode, Vec<ChildNode>)>,
    direct_children: Vec<ChildNode>,
}

#[derive(Default)]
struct OrderedRuleSet {
    ordered: Vec<AggregationRule>,
    seen: HashSet<AggregationRule>,
}

impl OrderedRuleSet {
    fn insert(&mut self, rule: AggregationRule) -> bool {
        if !self.seen.insert(rule.clone()) {
            return false;
        }
        self.ordered.push(rule);
        true
    }

    fn is_empty(&self) -> bool {
        self.ordered.is_empty()
    }

    fn len(&self) -> usize {
        self.ordered.len()
    }

    fn last(&self) -> Option<&AggregationRule> {
        self.ordered.last()
    }

    fn as_slice(&self) -> &[AggregationRule] {
        &self.ordered
    }
}

pub type ShadowedRulePaths = HashMap<Vec<String>, HashSet<Vec<String>>>;

fn is_objects_entry_point(entry_point: &EntryPoint) -> bool {
    entry_point.browse_path.len() == 1 && entry_point.browse_path[0] == "Objects"
}

fn is_top_level_shared_root(name: &str) -> bool {
    matches!(name, "MyVisionSystem_Assets")
}

fn is_shared_companion_singleton(parent_path: &[String], child_name: &str) -> bool {
    parent_path.last().map(String::as_str) == Some("DeviceSet") && child_name == "DeviceFeatures"
}

fn namespace_uri_for_index(
    namespaces: &[String],
    namespace_index: u16,
) -> Result<String, Box<dyn std::error::Error>> {
    namespaces
        .get(namespace_index as usize)
        .filter(|uri| !uri.trim().is_empty())
        .cloned()
        .ok_or_else(|| format!("Namespace-URI fuer Source-Index {} fehlt", namespace_index).into())
}

fn qualified_path_element(
    child: &ChildNode,
    source_namespaces: &[String],
) -> Result<QualifiedPathElement, Box<dyn std::error::Error>> {
    Ok(QualifiedPathElement {
        namespace_uri: namespace_uri_for_index(source_namespaces, child.browse_namespace)?,
        name: child.browse_name.clone(),
        identifier: None,
    })
}

fn qualified_node_identity(
    node_id: &NodeId,
    source_namespaces: &[String],
) -> Result<QualifiedNodeIdentity, Box<dyn std::error::Error>> {
    let serialized = node_id.to_string();
    let identifier = serialized
        .split_once(';')
        .map(|(_, identifier)| identifier.to_string())
        .unwrap_or(serialized);
    Ok(QualifiedNodeIdentity {
        namespace_uri: namespace_uri_for_index(source_namespaces, node_id.namespace)?,
        identifier,
    })
}

fn extend_qualified_path(
    path: &[QualifiedPathElement],
    child: &ChildNode,
    source_namespaces: &[String],
) -> Result<Vec<QualifiedPathElement>, Box<dyn std::error::Error>> {
    let mut qualified = path.to_vec();
    qualified.push(qualified_path_element(child, source_namespaces)?);
    Ok(qualified)
}

fn source_node_is_standard_model_node(
    address_space: &AddressSpace,
    source_namespaces: &[String],
    source_node: &ChildNode,
) -> bool {
    remap_source_node_id_namespace(Some(&source_node.node_id), address_space, source_namespaces)
        .is_some_and(|node_id| address_space.find_node(&node_id).is_some())
}

fn build_mapping_rule(
    server_address: &str,
    source_namespaces: &[String],
    target_node: Vec<String>,
    source_node_path: Vec<String>,
    target_node_qualified: Vec<QualifiedPathElement>,
    source_node_qualified: Vec<QualifiedPathElement>,
    source_node: &ChildNode,
) -> Result<AggregationRule, Box<dyn std::error::Error>> {
    let organizes = NodeId::new(0, ORGANIZES_REFERENCE_ID);
    Ok(AggregationRule {
        target_node,
        source_node: source_node_path,
        ref_type: ORGANIZES_REF_PATH
            .iter()
            .map(|segment| segment.to_string())
            .collect(),
        is_forward: true,
        target_node_qualified: target_node_qualified.clone(),
        source_node_qualified,
        source_id: server_address.trim_end_matches('/').to_string(),
        source_node_id: Some(qualified_node_identity(
            &source_node.node_id,
            source_namespaces,
        )?),
        reference_type: Some(qualified_node_identity(&organizes, source_namespaces)?),
        source_reference_type: Some(qualified_node_identity(
            &source_node.reference_type_id,
            source_namespaces,
        )?),
        source_reference_is_forward: Some(source_node.is_forward),
        reference_policy: REFERENCE_POLICY.to_string(),
        merge_policy: "merge_at_qualified_target".to_string(),
        merge_key: target_node_qualified,
    })
}

pub fn generate_rules_for_server(
    server_address: &str,
    all_entry_points: &mut Vec<NodeSetEntryPoints>,
    address_space: &mut AddressSpace,
    rules_path: &str,
) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
    generate_rules_for_server_with_control(
        server_address,
        all_entry_points,
        address_space,
        rules_path,
        SessionOperationControl::new(Duration::from_secs(600)),
    )
}

/// Compatibility path for callers that still request full JSON persistence.
pub fn generate_rules_for_server_with_control(
    server_address: &str,
    all_entry_points: &mut Vec<NodeSetEntryPoints>,
    address_space: &mut AddressSpace,
    rules_path: &str,
    control: SessionOperationControl,
) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
    let total_started = Instant::now();
    let generated = generate_rules_delta_for_server_with_control(
        server_address,
        all_entry_points,
        address_space,
        control.clone(),
    )?;
    control.check()?;
    let persist_started = Instant::now();
    if !generated.rules.is_empty() || !generated.shadowed_rules.is_empty() {
        merge_and_save_rules(rules_path, &generated.rules, &generated.shadowed_rules)?;
    }
    log_rule_timing(
        server_address,
        "rule_merge_persist",
        persist_started.elapsed(),
    );
    log_rule_timing(server_address, "rule_total", total_started.elapsed());
    Ok(generated.created_node_ids)
}

/// Derive only the incoming source's rules. The caller owns persistence and rollback.
pub fn generate_rules_delta_for_server_with_control(
    server_address: &str,
    all_entry_points: &mut Vec<NodeSetEntryPoints>,
    address_space: &mut AddressSpace,
    control: SessionOperationControl,
) -> Result<GeneratedRules, Box<dyn std::error::Error>> {
    control.check()?;
    let total_started = Instant::now();
    let mut created_node_ids = Vec::new();
    let mut new_rules = OrderedRuleSet::default();
    let mut shadowed_rules = ShadowedRulePaths::new();
    debug!("\n=== Regelgenerierung fuer Server: {} ===", server_address);

    let mut client = ClientBuilder::new()
        .operation_control(control.clone())
        .request_timeout(5_000)
        .application_name("Rule Generator")
        .application_uri("urn:RuleGenerator")
        .trust_server_certs(true)
        .session_retry_limit(3)
        .client()
        .ok_or("Failed to create client")?;

    let endpoint: EndpointDescription = (
        server_address,
        SecurityPolicy::None.to_str(),
        MessageSecurityMode::None,
        UserTokenPolicy::anonymous(),
    )
        .into();

    let connect_started = Instant::now();
    let session = client.connect_to_endpoint(endpoint, IdentityToken::Anonymous)?;
    log_rule_timing(server_address, "rule_connect", connect_started.elapsed());

    {
        let traversal_started = Instant::now();
        let source_namespaces = read_namespace_array(&session)?;
        register_rule_target_namespaces(address_space, &source_namespaces)?;

        let existing_paths: Vec<Vec<String>> = all_entry_points
            .iter()
            .flat_map(|ns| ns.entry_points.iter().map(|ep| ep.browse_path.clone()))
            .collect();

        let entry_points_to_process: Vec<(String, EntryPoint)> = all_entry_points
            .iter()
            .filter(|ns| {
                ns.namespace_index > 0 || ns.entry_points.iter().any(is_objects_entry_point)
            })
            .flat_map(|ns| {
                let ns_uri = ns.namespace_uri.clone();
                ns.entry_points
                    .iter()
                    .map(move |ep| (ns_uri.clone(), ep.clone()))
            })
            .collect();

        for (ns_uri, entry_point) in &entry_points_to_process {
            debug!(
                "\nEntry-Point: {} -> {:?}",
                entry_point.display_name, entry_point.browse_path
            );

            let objects_entry_point = is_objects_entry_point(entry_point);
            let local_ns_idx = if objects_entry_point {
                0
            } else {
                namespace_index_by_uri(address_space, ns_uri).unwrap_or_else(|| {
                    warn!("Namespace {} nicht registriert", ns_uri);
                    0
                })
            };

            if local_ns_idx == 0 && !objects_entry_point {
                continue;
            }

            // A companion entry point may legitimately be absent on this source.
            // A failed Browse, timeout or ambiguous path is not an absent path and
            // must abort the attempt instead of publishing incomplete rules.
            let Some((ep_node_id, entry_point_qualified)) =
                resolve_path(&session, &entry_point.browse_path, &source_namespaces)?
            else {
                debug!(
                    "Entry-Point auf dieser Quelle nicht vorhanden: {:?}",
                    entry_point.browse_path
                );
                continue;
            };

            let children = browse_children(&session, &ep_node_id)?;

            debug!("  {} Kinder gefunden", children.len());

            for child in &children {
                if is_shared_companion_singleton(&entry_point.browse_path, &child.browse_name) {
                    debug!(
                        "  Gemeinsamer Companion-Spec-Knoten wird nicht delegiert: {}",
                        child.browse_name
                    );
                    continue;
                }

                let child_path: Vec<String> = {
                    let mut p = entry_point.browse_path.clone();
                    p.push(child.browse_name.clone());
                    p
                };
                let child_qualified =
                    extend_qualified_path(&entry_point_qualified, child, &source_namespaces)?;

                let grandchildren = browse_children(&session, &child.node_id)?;
                if objects_entry_point && !should_process_objects_child(child, &grandchildren) {
                    continue;
                }

                let already_ep = existing_paths.iter().any(|ep| *ep == child_path);
                if already_ep {
                    debug!("  Bereits Entry-Point: {}", child.browse_name);
                    continue;
                }

                if let Some(structural_plan) =
                    collect_structural_children(&session, &child.browse_name, &grandchildren)?
                {
                    debug!("  Fall 1 (Companion-Spec-Struktur): {}", child.browse_name);
                    remember_shadowed_container_rule(&mut shadowed_rules, &child_path);

                    let child_ns_idx = local_namespace_index_for_source(
                        address_space,
                        &source_namespaces,
                        child.browse_namespace,
                        local_ns_idx,
                    );
                    let child_type_definition = remap_source_node_id_namespace(
                        child.type_definition.as_ref(),
                        address_space,
                        &source_namespaces,
                    );

                    if !node_exists_in_address_space(address_space, &child_path) {
                        if let Some(node_id) = create_object_node_in_address_space(
                            address_space,
                            &child.browse_name,
                            &child.display_name,
                            &child_path,
                            child_ns_idx,
                            child_type_definition,
                        ) {
                            created_node_ids.push(node_id);
                        }
                    }

                    for direct_child in &structural_plan.direct_children {
                        let direct_path: Vec<String> = {
                            let mut p = child_path.clone();
                            p.push(direct_child.browse_name.clone());
                            p
                        };

                        if source_node_is_standard_model_node(
                            address_space,
                            &source_namespaces,
                            direct_child,
                        ) {
                            continue;
                        }
                        let direct_qualified = extend_qualified_path(
                            &child_qualified,
                            direct_child,
                            &source_namespaces,
                        )?;
                        let rule = build_mapping_rule(
                            server_address,
                            &source_namespaces,
                            child_path.clone(),
                            direct_path,
                            child_qualified.clone(),
                            direct_qualified,
                            direct_child,
                        )?;
                        if new_rules.insert(rule) {
                            let rule = new_rules
                                .last()
                                .expect("inserted rule must be the last ordered rule");
                            debug!(
                                "    Regel (F1-Blatt): {:?} -> {:?}",
                                rule.source_node.last(),
                                rule.target_node.last()
                            );
                        }
                    }

                    for (folder_child, folder_grandchildren) in &structural_plan.containers {
                        let folder_path: Vec<String> = {
                            let mut p = child_path.clone();
                            p.push(folder_child.browse_name.clone());
                            p
                        };

                        let folder_exists =
                            node_exists_in_address_space(address_space, &folder_path);
                        let folder_qualified = extend_qualified_path(
                            &child_qualified,
                            folder_child,
                            &source_namespaces,
                        )?;
                        let folder_ns_idx = local_namespace_index_for_source(
                            address_space,
                            &source_namespaces,
                            folder_child.browse_namespace,
                            child_ns_idx,
                        );
                        let folder_type_definition = remap_source_node_id_namespace(
                            folder_child.type_definition.as_ref(),
                            address_space,
                            &source_namespaces,
                        );

                        if !folder_exists {
                            if let Some(node_id) = create_object_node_in_address_space(
                                address_space,
                                &folder_child.browse_name,
                                &folder_child.display_name,
                                &folder_path,
                                folder_ns_idx,
                                folder_type_definition,
                            ) {
                                created_node_ids.push(node_id);
                            }
                        }

                        for fg_child in folder_grandchildren {
                            let fg_path: Vec<String> = {
                                let mut p = folder_path.clone();
                                p.push(fg_child.browse_name.clone());
                                p
                            };

                            let mut active_path = HashSet::from([
                                child.node_id.clone(),
                                folder_child.node_id.clone(),
                            ]);
                            let fg_qualified = extend_qualified_path(
                                &folder_qualified,
                                fg_child,
                                &source_namespaces,
                            )?;
                            create_or_map_subtree(
                                &session,
                                address_space,
                                server_address,
                                &source_namespaces,
                                &mut new_rules,
                                &folder_path,
                                &folder_qualified,
                                fg_path,
                                fg_qualified.clone(),
                                fg_qualified,
                                fg_child,
                                folder_ns_idx,
                                &mut created_node_ids,
                                &mut active_path,
                            )?;
                        }
                    }
                } else {
                    debug!("  Fall 2: {}", child.browse_name);

                    if source_node_is_standard_model_node(address_space, &source_namespaces, child)
                    {
                        continue;
                    }
                    let rule = build_mapping_rule(
                        server_address,
                        &source_namespaces,
                        entry_point.browse_path.clone(),
                        child_path,
                        entry_point_qualified.clone(),
                        child_qualified,
                        child,
                    )?;
                    if new_rules.insert(rule) {
                        let rule = new_rules
                            .last()
                            .expect("inserted rule must be the last ordered rule");
                        debug!(
                            "    Regel (F2): {:?} -> {:?}",
                            rule.source_node.last(),
                            rule.target_node.last()
                        );
                    }
                }
            }
        }

        log_rule_timing(
            server_address,
            "rule_traversal",
            traversal_started.elapsed(),
        );
        control.check()?;
    }

    log_rule_timing(
        server_address,
        "rule_generate_delta",
        total_started.elapsed(),
    );
    debug!("=== Regelgenerierung abgeschlossen ===\n");
    Ok(GeneratedRules {
        created_node_ids,
        rules: new_rules.ordered,
        shadowed_rules,
    })
}

fn resolve_path(
    session: &Arc<RwLock<Session>>,
    path: &[String],
    source_namespaces: &[String],
) -> Result<Option<(NodeId, Vec<QualifiedPathElement>)>, Box<dyn std::error::Error>> {
    resolve_path_with_browse(path, source_namespaces, |node_id| {
        browse_children(session, node_id)
    })
}

fn resolve_path_with_browse<F>(
    path: &[String],
    source_namespaces: &[String],
    mut browse: F,
) -> Result<Option<(NodeId, Vec<QualifiedPathElement>)>, Box<dyn std::error::Error>>
where
    F: FnMut(&NodeId) -> Result<Vec<ChildNode>, Box<dyn std::error::Error>>,
{
    let mut current_id = NodeId::new(0, OBJECTS_NODE_ID);
    let mut qualified_path = vec![QualifiedPathElement {
        namespace_uri: namespace_uri_for_index(source_namespaces, 0)?,
        name: "Objects".to_string(),
        identifier: None,
    }];
    let steps = if path.first().map(|s| s.as_str()) == Some("Objects") {
        &path[1..]
    } else {
        path
    };

    for step in steps {
        let children = browse(&current_id)?;
        let matches = children
            .iter()
            .filter(|child| child.browse_name == *step)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [child] => {
                current_id = child.node_id.clone();
                qualified_path.push(qualified_path_element(child, source_namespaces)?);
            }
            [] => return Ok(None),
            _ => {
                return Err(format!(
                    "Schritt '{}' ist ohne Namespace-Identitaet mehrdeutig",
                    step
                )
                .into())
            }
        }
    }
    Ok(Some((current_id, qualified_path)))
}

fn browse_children(
    session: &Arc<RwLock<Session>>,
    node_id: &NodeId,
) -> Result<Vec<ChildNode>, Box<dyn std::error::Error>> {
    use opcua::types::service_types::{BrowseDescription, BrowseDirection};

    let session_guard = session.read();

    let browse_desc = BrowseDescription {
        node_id: node_id.clone(),
        browse_direction: BrowseDirection::Forward,
        reference_type_id: NodeId::new(0, HIERARCHICAL_REFERENCES_ID),
        include_subtypes: true,
        node_class_mask: 0xFF,
        result_mask: 0x3F,
    };

    let mut initial_results = session_guard
        .browse(&[browse_desc])
        .map_err(|e| format!("BrowseRequest fehlgeschlagen: {:?}", e))?
        .ok_or("BrowseResponse enthaelt keine Ergebnisse")?;
    if initial_results.len() != 1 {
        return Err(format!(
            "BrowseResponse enthaelt {} statt genau eines Ergebnisses",
            initial_results.len()
        )
        .into());
    }

    let first_result = initial_results.remove(0);
    let references = collect_browse_references(first_result, |continuation_point| {
        let mut next_results = session_guard
            .browse_next(false, &[continuation_point.clone()])
            .map_err(|e| format!("BrowseNext fehlgeschlagen: {:?}", e))?
            .ok_or("BrowseNext enthaelt keine Ergebnisse")?;
        if next_results.len() != 1 {
            return Err(format!(
                "BrowseNext enthaelt {} statt genau eines Ergebnisses",
                next_results.len()
            )
            .into());
        }
        Ok(next_results.remove(0))
    })?;

    let mut children = Vec::new();
    for ref_desc in references {
        let browse_name = ref_desc
            .browse_name
            .name
            .value()
            .as_deref()
            .unwrap_or("")
            .to_string();
        if is_placeholder_browse_name(&browse_name)
            || !is_tree_reference_type(&ref_desc.reference_type_id)
        {
            continue;
        }
        let display_name = {
            let dn = ref_desc
                .display_name
                .text
                .value()
                .as_deref()
                .unwrap_or("")
                .to_string();
            if dn.is_empty() {
                browse_name.clone()
            } else {
                dn
            }
        };
        let type_definition = {
            let td = ref_desc.type_definition.node_id.clone();
            if td.is_null() {
                None
            } else {
                Some(td)
            }
        };
        children.push(ChildNode {
            node_id: ref_desc.node_id.node_id,
            browse_namespace: ref_desc.browse_name.namespace_index,
            browse_name,
            display_name,
            node_class: ref_desc.node_class,
            type_definition,
            reference_type_id: ref_desc.reference_type_id,
            is_forward: ref_desc.is_forward,
        });
    }
    sort_children(&mut children);
    Ok(children)
}

fn sort_children(children: &mut [ChildNode]) {
    children.sort_by(|left, right| {
        (
            left.browse_namespace,
            left.browse_name.as_str(),
            left.node_id.to_string(),
        )
            .cmp(&(
                right.browse_namespace,
                right.browse_name.as_str(),
                right.node_id.to_string(),
            ))
    });
}

fn collect_browse_references<F>(
    mut result: opcua::types::service_types::BrowseResult,
    mut browse_next: F,
) -> Result<Vec<opcua::types::service_types::ReferenceDescription>, Box<dyn std::error::Error>>
where
    F: FnMut(
        &ByteString,
    ) -> Result<opcua::types::service_types::BrowseResult, Box<dyn std::error::Error>>,
{
    let mut references = Vec::new();
    let mut seen_continuation_points = HashSet::new();

    loop {
        if result.status_code.is_bad() {
            return Err(format!("Browse lieferte StatusCode {}", result.status_code.name()).into());
        }
        references.extend(result.references.take().unwrap_or_default());

        let continuation_point = result.continuation_point;
        if continuation_point.is_null() {
            break;
        }
        if !seen_continuation_points.insert(continuation_point.clone()) {
            return Err("BrowseNext lieferte einen wiederholten ContinuationPoint".into());
        }
        result = browse_next(&continuation_point)?;
    }

    Ok(references)
}

fn read_namespace_array(
    session: &Arc<RwLock<Session>>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    use opcua::types::service_types::{ReadRequest, ReadValueId};

    let session_guard = session.read();
    let read_value_id = ReadValueId {
        node_id: NodeId::new(0, 2255u32),
        attribute_id: AttributeId::Value as u32,
        index_range: UAString::null(),
        data_encoding: QualifiedName::null(),
    };

    let request = ReadRequest {
        request_header: session_guard.make_request_header(),
        max_age: 0.0,
        timestamps_to_return: TimestampsToReturn::Neither,
        nodes_to_read: Some(vec![read_value_id]),
    };

    match session_guard.send_request(request) {
        Ok(response) => {
            if let SupportedMessage::ReadResponse(read_response) = response {
                if let Some(results) = read_response.results {
                    if let Some(result) = results.first() {
                        if let Some(Variant::Array(arr)) = &result.value {
                            let namespaces = arr
                                .values
                                .iter()
                                .filter_map(|v| {
                                    if let Variant::String(s) = v {
                                        s.value().as_ref().cloned()
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            return Ok(namespaces);
                        }
                    }
                }
            }
            Err("NamespaceArray konnte nicht gelesen werden".into())
        }
        Err(e) => Err(format!("ReadRequest fuer NamespaceArray fehlgeschlagen: {:?}", e).into()),
    }
}
fn is_folder_type(type_definition: &Option<NodeId>) -> bool {
    match type_definition {
        Some(id) => id.namespace == 0 && id.identifier == Identifier::Numeric(FOLDER_TYPE_ID),
        None => false,
    }
}

fn is_tree_reference_type(reference_type_id: &NodeId) -> bool {
    reference_type_id.namespace == 0
        && matches!(
            &reference_type_id.identifier,
            Identifier::Numeric(id)
                if matches!(
                    *id,
                    ORGANIZES_REFERENCE_ID
                        | HAS_PROPERTY_REFERENCE_ID
                        | HAS_COMPONENT_REFERENCE_ID
                        | HAS_ORDERED_COMPONENT_REFERENCE_ID
                )
        )
}

fn is_placeholder_browse_name(name: &str) -> bool {
    let name = name.trim();
    name.starts_with('<') && name.ends_with('>')
}

fn companion_collection_names(parent_name: &str) -> Option<&'static [&'static str]> {
    match parent_name {
        "MotionDeviceSystem" => Some(MOTION_DEVICE_SYSTEM_COLLECTIONS),
        "DualCameraVisionSystem" => Some(DUAL_CAMERA_VISION_SYSTEM_COLLECTIONS),
        "MyVisionSystem_Assets" => Some(VISION_SYSTEM_ASSET_COLLECTIONS),
        "Configuration" => Some(&["Recipes", "Products", "Configurations"]),
        "ResultManagement" => Some(&["Results", "ResultResultFolder"]),
        "ComputingDevices" => Some(&["SoftwareAndLicenses"]),
        _ => None,
    }
}

fn is_companion_structural_child(parent_name: &str, child_name: &str) -> bool {
    companion_collection_names(parent_name)
        .map(|names| names.iter().any(|name| *name == child_name))
        .unwrap_or(false)
        || (parent_name == "DualCameraVisionSystem" && child_name.starts_with("Control_"))
}

fn is_standard_objects_child(name: &str) -> bool {
    matches!(name, "Server" | "Aliases" | "Locations")
}

fn should_process_objects_child(child: &ChildNode, _child_children: &[ChildNode]) -> bool {
    is_top_level_shared_root(&child.browse_name)
        || (child.browse_namespace > 0 && !is_standard_objects_child(&child.browse_name))
}

fn is_object_like(child: &ChildNode) -> bool {
    matches!(child.node_class, NodeClass::Object | NodeClass::View)
}

fn is_structural_container(
    parent_name: &str,
    child: &ChildNode,
    child_children: &[ChildNode],
) -> bool {
    is_object_like(child)
        && (is_folder_type(&child.type_definition)
            || is_companion_structural_child(parent_name, &child.browse_name)
            || !child_children.is_empty())
}

fn collect_structural_children(
    session: &Arc<RwLock<Session>>,
    parent_name: &str,
    children: &[ChildNode],
) -> Result<Option<StructuralChildren>, Box<dyn std::error::Error>> {
    if children.is_empty() {
        return Ok(None);
    }

    let mut structural_children = StructuralChildren {
        containers: Vec::new(),
        direct_children: Vec::new(),
    };

    for child in children {
        let child_children = browse_children(session, &child.node_id)?;

        if is_structural_container(parent_name, child, &child_children) {
            structural_children
                .containers
                .push((child.clone(), child_children));
        } else {
            structural_children.direct_children.push(child.clone());
        }
    }

    if structural_children.containers.is_empty() {
        Ok(None)
    } else {
        Ok(Some(structural_children))
    }
}

fn add_mapping_rule(
    address_space: &AddressSpace,
    server_address: &str,
    source_namespaces: &[String],
    new_rules: &mut OrderedRuleSet,
    target_node: Vec<String>,
    source_node: Vec<String>,
    target_node_qualified: Vec<QualifiedPathElement>,
    source_node_qualified: Vec<QualifiedPathElement>,
    source_leaf: &ChildNode,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if source_node_is_standard_model_node(address_space, source_namespaces, source_leaf) {
        return Ok(());
    }
    let rule = build_mapping_rule(
        server_address,
        source_namespaces,
        target_node,
        source_node,
        target_node_qualified,
        source_node_qualified,
        source_leaf,
    )?;

    if new_rules.insert(rule) {
        let rule = new_rules
            .last()
            .expect("inserted rule must be the last ordered rule");
        debug!(
            "    Regel ({}): {:?} -> {:?}",
            label,
            rule.source_node.last(),
            rule.target_node.last()
        );
    }
    Ok(())
}

fn create_or_map_subtree(
    session: &Arc<RwLock<Session>>,
    address_space: &mut AddressSpace,
    server_address: &str,
    source_namespaces: &[String],
    new_rules: &mut OrderedRuleSet,
    parent_target_path: &[String],
    parent_target_qualified: &[QualifiedPathElement],
    source_path: Vec<String>,
    source_qualified: Vec<QualifiedPathElement>,
    target_qualified: Vec<QualifiedPathElement>,
    source_node: &ChildNode,
    fallback_ns_idx: u16,
    created_node_ids: &mut Vec<NodeId>,
    active_path: &mut HashSet<NodeId>,
) -> Result<(), Box<dyn std::error::Error>> {
    if is_placeholder_browse_name(&source_node.browse_name) {
        return Ok(());
    }

    if !enter_active_path(active_path, &source_node.node_id) {
        return Ok(());
    }

    let result = create_or_map_subtree_inner(
        session,
        address_space,
        server_address,
        source_namespaces,
        new_rules,
        parent_target_path,
        parent_target_qualified,
        source_path,
        source_qualified,
        target_qualified,
        source_node,
        fallback_ns_idx,
        created_node_ids,
        active_path,
    );
    leave_active_path(active_path, &source_node.node_id);
    result
}

fn enter_active_path(active_path: &mut HashSet<NodeId>, node_id: &NodeId) -> bool {
    active_path.insert(node_id.clone())
}

fn leave_active_path(active_path: &mut HashSet<NodeId>, node_id: &NodeId) {
    active_path.remove(node_id);
}

fn create_or_map_subtree_inner(
    session: &Arc<RwLock<Session>>,
    address_space: &mut AddressSpace,
    server_address: &str,
    source_namespaces: &[String],
    new_rules: &mut OrderedRuleSet,
    parent_target_path: &[String],
    parent_target_qualified: &[QualifiedPathElement],
    source_path: Vec<String>,
    source_qualified: Vec<QualifiedPathElement>,
    target_qualified: Vec<QualifiedPathElement>,
    source_node: &ChildNode,
    fallback_ns_idx: u16,
    created_node_ids: &mut Vec<NodeId>,
    active_path: &mut HashSet<NodeId>,
) -> Result<(), Box<dyn std::error::Error>> {
    let children = browse_children(session, &source_node.node_id)?;

    let source_parent_name = parent_name(source_path.as_slice());
    if is_structural_container(&source_parent_name, source_node, &children) {
        let node_ns_idx = local_namespace_index_for_source(
            address_space,
            source_namespaces,
            source_node.browse_namespace,
            fallback_ns_idx,
        );
        let node_type_definition = remap_source_node_id_namespace(
            source_node.type_definition.as_ref(),
            address_space,
            source_namespaces,
        );

        let force_distinct_instance = is_source_scoped_instance_parent(&source_parent_name);
        let mut current_target_qualified = target_qualified;
        let existing_target = (!force_distinct_instance)
            .then(|| find_node_by_qualified_path(address_space, &current_target_qualified))
            .flatten();
        let target_node_id = if let Some(node_id) = existing_target {
            node_id
        } else {
            let parent_node_id =
                find_node_by_qualified_path(address_space, parent_target_qualified).ok_or_else(
                    || {
                        format!(
                            "Ziel-Parent fuer '{}' konnte nicht qualifiziert aufgeloest werden",
                            source_node.browse_name
                        )
                    },
                )?;
            let node_id = insert_object_node_under_parent(
                address_space,
                &source_node.browse_name,
                &source_node.display_name,
                parent_node_id,
                node_ns_idx,
                node_type_definition,
            );
            created_node_ids.push(node_id.clone());
            node_id
        };
        set_target_identifier(&mut current_target_qualified, &target_node_id);

        for child in &children {
            let child_path: Vec<String> = {
                let mut p = source_path.clone();
                p.push(child.browse_name.clone());
                p
            };
            let child_source_qualified =
                extend_qualified_path(&source_qualified, child, source_namespaces)?;
            let child_target_qualified =
                extend_qualified_path(&current_target_qualified, child, source_namespaces)?;
            create_or_map_subtree(
                session,
                address_space,
                server_address,
                source_namespaces,
                new_rules,
                &source_path,
                &current_target_qualified,
                child_path,
                child_source_qualified,
                child_target_qualified,
                child,
                node_ns_idx,
                created_node_ids,
                active_path,
            )?;
        }
    } else {
        add_mapping_rule(
            address_space,
            server_address,
            source_namespaces,
            new_rules,
            parent_target_path.to_vec(),
            source_path,
            parent_target_qualified.to_vec(),
            source_qualified,
            source_node,
            "F1-Blatt",
        )?;
    }
    Ok(())
}

fn parent_name(path: &[String]) -> String {
    path.get(path.len().saturating_sub(2))
        .cloned()
        .unwrap_or_default()
}

fn is_source_scoped_instance_parent(parent_name: &str) -> bool {
    matches!(
        parent_name,
        "MotionDevices" | "Controllers" | "SafetyStates"
    )
}

fn namespace_index_by_uri(address_space: &AddressSpace, ns_uri: &str) -> Option<u16> {
    let uri_with_slash = if ns_uri.ends_with('/') {
        ns_uri.to_string()
    } else {
        format!("{}/", ns_uri)
    };
    let uri_without_slash = ns_uri.trim_end_matches('/').to_string();

    address_space
        .namespace_index(&uri_with_slash)
        .or_else(|| address_space.namespace_index(&uri_without_slash))
}

fn register_rule_target_namespaces(
    address_space: &mut AddressSpace,
    source_namespaces: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    for namespace_uri in source_namespaces.iter().skip(1) {
        if namespace_uri.trim().is_empty()
            || namespace_index_by_uri(address_space, namespace_uri).is_some()
        {
            continue;
        }
        address_space
            .register_namespace(namespace_uri)
            .map_err(|_| {
                format!(
                    "Namespace '{}' konnte fuer qualifizierte Regelziele nicht registriert werden",
                    namespace_uri
                )
            })?;
    }
    Ok(())
}

fn local_namespace_index_for_source(
    address_space: &AddressSpace,
    source_namespaces: &[String],
    source_ns_idx: u16,
    fallback: u16,
) -> u16 {
    if source_ns_idx == 0 {
        return 0;
    }

    source_namespaces
        .get(source_ns_idx as usize)
        .and_then(|uri| namespace_index_by_uri(address_space, uri))
        .unwrap_or(fallback)
}

fn remap_source_node_id_namespace(
    node_id: Option<&NodeId>,
    address_space: &AddressSpace,
    source_namespaces: &[String],
) -> Option<NodeId> {
    let node_id = node_id?;
    if node_id.namespace == 0 {
        return Some(node_id.clone());
    }

    let local_ns_idx = source_namespaces
        .get(node_id.namespace as usize)
        .and_then(|uri| namespace_index_by_uri(address_space, uri))?;

    Some(rebuild_node_id_with_namespace(node_id, local_ns_idx))
}

fn rebuild_node_id_with_namespace(node_id: &NodeId, namespace: u16) -> NodeId {
    match &node_id.identifier {
        Identifier::Numeric(id) => NodeId::new(namespace, *id),
        Identifier::String(s) => NodeId::new(namespace, s.as_ref().to_string()),
        Identifier::Guid(g) => NodeId::new(namespace, g.clone()),
        Identifier::ByteString(b) => NodeId::new(namespace, b.clone()),
    }
}

fn remember_shadowed_container_rule(
    shadowed_rules: &mut ShadowedRulePaths,
    container_path: &[String],
) {
    if container_path.len() < 2 {
        return;
    }

    shadowed_rules
        .entry(container_path.to_vec())
        .or_default()
        .insert(container_path[..container_path.len() - 1].to_vec());
}

fn is_shadowed_rule(rule: &AggregationRule, shadowed_rules: &ShadowedRulePaths) -> bool {
    shadowed_rules
        .get(rule.source_node.as_slice())
        .is_some_and(|targets| targets.contains(rule.target_node.as_slice()))
}
fn find_node_by_browse_path(
    address_space: &AddressSpace,
    browse_path: &[String],
) -> Option<NodeId> {
    let hierarchical = NodeId::new(0, HIERARCHICAL_REFERENCES_ID);
    let steps = if browse_path.first().map(|s| s.as_str()) == Some("Objects") {
        &browse_path[1..]
    } else {
        browse_path
    };

    let mut current = NodeId::new(0, OBJECTS_NODE_ID);

    for step in steps {
        let refs = address_space
            .find_references(&current, Some((&hierarchical, true)))
            .unwrap_or_default();

        let found = refs.iter().find_map(|r| {
            let target_id = &r.target_node;
            address_space.find_node(target_id).and_then(|n| {
                let bn = n.as_node().browse_name();
                if bn
                    .name
                    .value()
                    .as_deref()
                    .map(|s| s == step.as_str())
                    .unwrap_or(false)
                {
                    Some(target_id.clone())
                } else {
                    None
                }
            })
        });

        match found {
            Some(id) => current = id,
            None => return None,
        }
    }

    Some(current)
}

fn node_exists_in_address_space(address_space: &AddressSpace, browse_path: &[String]) -> bool {
    find_node_by_browse_path(address_space, browse_path).is_some()
}

pub(crate) fn find_node_by_qualified_path(
    address_space: &AddressSpace,
    qualified_path: &[QualifiedPathElement],
) -> Option<NodeId> {
    let hierarchical = NodeId::new(0, HIERARCHICAL_REFERENCES_ID);
    let steps = if qualified_path.first().map(|step| step.name.as_str()) == Some("Objects") {
        &qualified_path[1..]
    } else {
        qualified_path
    };
    let mut current = NodeId::new(0, OBJECTS_NODE_ID);

    for step in steps {
        let namespace = namespace_index_by_uri(address_space, &step.namespace_uri)?;
        let references = address_space
            .find_references(&current, Some((&hierarchical, true)))
            .unwrap_or_default();
        let expected_id = step.identifier.as_deref().and_then(|identifier| {
            let mut node_id = NodeId::from_str(identifier).ok()?;
            if node_id.namespace != 0 {
                return None;
            }
            node_id.namespace = namespace;
            Some(node_id)
        });

        let found = references.iter().find_map(|reference| {
            let target_id = &reference.target_node;
            if expected_id
                .as_ref()
                .is_some_and(|expected| target_id != expected)
            {
                return None;
            }
            address_space.find_node(target_id).and_then(|node| {
                let browse_name = node.as_node().browse_name();
                (browse_name.namespace_index == namespace
                    && browse_name.name.value().as_deref() == Some(step.name.as_str()))
                .then(|| target_id.clone())
            })
        })?;
        current = found;
    }

    Some(current)
}

fn set_target_identifier(path: &mut [QualifiedPathElement], node_id: &NodeId) {
    let serialized = node_id.to_string();
    let identifier = serialized
        .split_once(';')
        .map(|(_, identifier)| identifier.to_string())
        .unwrap_or(serialized);
    if let Some(last) = path.last_mut() {
        last.identifier = Some(identifier);
    }
}

fn create_object_node_in_address_space(
    address_space: &mut AddressSpace,
    browse_name: &str,
    display_name: &str,
    browse_path: &[String],
    ns_idx: u16,
    type_def: Option<NodeId>,
) -> Option<NodeId> {
    let parent_path = &browse_path[..browse_path.len() - 1];
    let parent_node_id =
        if parent_path.is_empty() || (parent_path.len() == 1 && parent_path[0] == "Objects") {
            NodeId::new(0, OBJECTS_NODE_ID)
        } else {
            match find_node_by_browse_path(address_space, parent_path) {
                Some(id) => id,
                None => {
                    warn!(
                        "    ⚠ Parent '{}' nicht gefunden – Knoten wird nicht erstellt",
                        parent_path.last().unwrap_or(&String::new())
                    );
                    return None;
                }
            }
        };

    Some(insert_object_node_under_parent(
        address_space,
        browse_name,
        display_name,
        parent_node_id,
        ns_idx,
        type_def,
    ))
}

fn insert_object_node_under_parent(
    address_space: &mut AddressSpace,
    browse_name: &str,
    display_name: &str,
    parent_node_id: NodeId,
    ns_idx: u16,
    type_def: Option<NodeId>,
) -> NodeId {
    use opcua::server::prelude::ObjectBuilder;
    use opcua::types::QualifiedName;

    let node_id = next_node_id(ns_idx);
    let actual_type_def = type_def.unwrap_or_else(|| NodeId::new(0, FOLDER_TYPE_ID));
    ObjectBuilder::new(
        &node_id,
        QualifiedName::new(ns_idx, browse_name),
        display_name,
    )
    .has_type_definition(actual_type_def)
    .organized_by(parent_node_id)
    .insert(address_space);
    debug!("    ✓ Knoten erstellt: {} (ns={})", browse_name, ns_idx);
    node_id
}

fn load_rules(rules_path: &str) -> Result<Vec<AggregationRule>, Box<dyn std::error::Error>> {
    match File::open(rules_path) {
        Ok(file) => Ok(serde_json::from_reader(BufReader::new(file))?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

fn executor_rules_path(rules_path: &str) -> PathBuf {
    Path::new(rules_path).with_file_name("rules_executor.json")
}

pub(crate) fn executor_rule(rule: &AggregationRule) -> ExecutorRule {
    ExecutorRule {
        target_node: rule.target_node_qualified.clone(),
        source_node: rule.source_node_qualified.clone(),
        ref_type: rule.ref_type.clone(),
        is_forward: rule.is_forward,
        source_id: rule.source_id.clone(),
        source_node_id: rule.source_node_id.clone(),
        reference_type: rule.reference_type.clone(),
        merge_policy: rule.merge_policy.clone(),
        merge_key: rule.merge_key.clone(),
    }
}

pub(crate) fn executor_projection(rules: &[AggregationRule]) -> Vec<ExecutorRule> {
    let mut projected = Vec::new();
    let mut seen = HashSet::new();
    for rule in rules {
        let executor_rule = executor_rule(rule);
        if seen.insert(executor_rule.clone()) {
            projected.push(executor_rule);
        }
    }
    projected
}

fn write_executor_rules<W: Write>(
    writer: W,
    rules: &[AggregationRule],
) -> Result<(), serde_json::Error> {
    let mut serializer = serde_json::Serializer::pretty(writer);
    let mut sequence = serializer.serialize_seq(None)?;
    let mut seen = HashSet::new();
    for rule in rules {
        let executor_rule = executor_rule(rule);
        if seen.insert(executor_rule.clone()) {
            sequence.serialize_element(&executor_rule)?;
        }
    }
    sequence.end()
}

fn write_pretty_json<T: Serialize + ?Sized>(
    path: impl AsRef<Path>,
    value: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)?;
    writer.flush()?;
    Ok(())
}

fn write_rule_files(
    rules_path: &str,
    rules: &[AggregationRule],
) -> Result<(), Box<dyn std::error::Error>> {
    write_pretty_json(rules_path, rules)?;
    let file = File::create(executor_rules_path(rules_path))?;
    let mut writer = BufWriter::new(file);
    write_executor_rules(&mut writer, rules)?;
    writer.flush()?;
    Ok(())
}

pub(crate) fn merge_rules(
    mut existing: Vec<AggregationRule>,
    new_rules: &[AggregationRule],
    shadowed_rules: &ShadowedRulePaths,
) -> (Vec<AggregationRule>, usize, usize) {
    let before = existing.len();
    existing.retain(|rule| !is_shadowed_rule(rule, shadowed_rules));
    let removed = before - existing.len();

    let existing_index: HashSet<&AggregationRule> = existing.iter().collect();
    let mut incoming_index: HashSet<&AggregationRule> = HashSet::new();
    let mut additions = Vec::new();
    for rule in new_rules {
        if !existing_index.contains(rule) && incoming_index.insert(rule) {
            additions.push(rule.clone());
        }
    }
    drop(existing_index);
    let added = additions.len();
    existing.extend(additions);
    (existing, added, removed)
}

fn merge_and_save_rules(
    rules_path: &str,
    new_rules: &[AggregationRule],
    shadowed_rules: &ShadowedRulePaths,
) -> Result<(), Box<dyn std::error::Error>> {
    let (existing, added, removed) =
        merge_rules(load_rules(rules_path)?, new_rules, shadowed_rules);
    write_rule_files(rules_path, &existing)?;
    if removed > 0 {
        debug!("  Entferne {} veraltete Direktregel(n)", removed);
    }
    debug!("  💾 {} neue Regeln (gesamt: {})", added, existing.len());
    Ok(())
}

#[cfg(test)]
mod recursion_tests {
    use super::*;
    use opcua::server::prelude::ReferenceTypeId;
    use opcua::types::service_types::{BrowseResult, ReferenceDescription};
    use opcua::types::{ByteString, LocalizedText, QualifiedName, StatusCode};
    use std::collections::{HashMap, HashSet};

    fn reference(node_id: u32, namespace: u16, name: &str) -> ReferenceDescription {
        ReferenceDescription {
            reference_type_id: ReferenceTypeId::HasComponent.into(),
            is_forward: true,
            node_id: NodeId::new(namespace, node_id).into(),
            browse_name: QualifiedName::new(namespace, name),
            display_name: LocalizedText::from(name),
            node_class: NodeClass::Variable,
            type_definition: NodeId::new(0, 63u32).into(),
        }
    }

    fn browse_page(
        status_code: StatusCode,
        continuation_point: ByteString,
        references: Vec<ReferenceDescription>,
    ) -> BrowseResult {
        BrowseResult {
            status_code,
            continuation_point,
            references: Some(references),
        }
    }

    #[test]
    fn invalid_existing_rule_file_is_not_overwritten_as_an_empty_rule_set() {
        let temp = std::env::temp_dir().join(format!(
            "ojies-rule-input-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&temp).unwrap();
        let path = temp.join("rules.json");
        let rules_path = path.to_str().unwrap();
        // First use is allowed; a malformed existing file is an input failure.
        assert!(load_rules(rules_path).unwrap().is_empty());
        std::fs::write(&path, b"{ malformed existing rules").unwrap();
        assert!(merge_and_save_rules(rules_path, &[], &ShadowedRulePaths::new()).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"{ malformed existing rules");
        assert!(!temp.join("rules_executor.json").exists());
        std::fs::remove_file(&path).unwrap();
        // A path that exists but cannot be read as a file must not mean empty.
        std::fs::create_dir(&path).unwrap();
        assert!(load_rules(rules_path).is_err());
        std::fs::remove_dir(&path).unwrap();
        std::fs::remove_dir(&temp).unwrap();
    }

    #[test]
    fn entry_point_resolution_distinguishes_absence_from_failed_browse() {
        let path = vec!["Objects".into(), "Device".into()];
        let namespaces = vec!["http://opcfoundation.org/UA/".into()];
        let absent = resolve_path_with_browse(&path, &namespaces, |_| Ok(Vec::new())).unwrap();
        assert!(absent.is_none());
        let failed =
            resolve_path_with_browse(&path, &namespaces, |_| Err(StatusCode::BadTimeout.into()));
        assert_eq!(
            failed.unwrap_err().downcast_ref::<StatusCode>(),
            Some(&StatusCode::BadTimeout)
        );
    }

    #[test]
    fn entry_point_resolution_rejects_ambiguous_names() {
        let path = vec!["Objects".into(), "Device".into()];
        let namespaces = vec!["http://opcfoundation.org/UA/".into(), "urn:test".into()];
        let result = resolve_path_with_browse(&path, &namespaces, |_| {
            Ok(vec![
                test_child(1, 10, "Device"),
                test_child(1, 11, "Device"),
            ])
        });
        assert!(result.unwrap_err().to_string().contains("mehrdeutig"));
    }

    #[test]
    fn drains_forced_continuation_point_and_propagates_browse_errors() {
        let continuation_point = ByteString::from(vec![0x01, 0x02]);
        let first = browse_page(
            StatusCode::Good,
            continuation_point.clone(),
            vec![reference(1, 1, "PageOneLeaf")],
        );
        let mut browse_next_calls = 0;

        let references = collect_browse_references(first, |received| {
            browse_next_calls += 1;
            assert_eq!(received, &continuation_point);
            Ok(browse_page(
                StatusCode::Good,
                ByteString::null(),
                vec![reference(2, 1, "PageTwoLeaf")],
            ))
        })
        .expect("all browse pages must be collected");

        assert_eq!(browse_next_calls, 1);
        assert_eq!(references.len(), 2);

        let bad = browse_page(StatusCode::BadNodeIdUnknown, ByteString::null(), Vec::new());
        let error = collect_browse_references(bad, |_| {
            panic!("BrowseNext must not run after a bad initial result")
        })
        .expect_err("bad BrowseResult status must propagate");
        assert!(error.to_string().contains("StatusCode"));

        let repeated = ByteString::from(vec![0xAA]);
        let error = collect_browse_references(
            browse_page(StatusCode::Good, repeated.clone(), Vec::new()),
            |_| Ok(browse_page(StatusCode::Good, repeated.clone(), Vec::new())),
        )
        .expect_err("a repeated continuation point must terminate");
        assert!(error.to_string().contains("ContinuationPoint"));
    }

    #[test]
    fn source_namespace_leaf_below_objects_is_not_silently_skipped() {
        let leaf = ChildNode {
            node_id: NodeId::new(2, 100u32),
            browse_namespace: 2,
            browse_name: "TopLevelLeaf".to_string(),
            display_name: "TopLevelLeaf".to_string(),
            node_class: NodeClass::Object,
            type_definition: None,
            reference_type_id: NodeId::new(0, ORGANIZES_REFERENCE_ID),
            is_forward: true,
        };
        assert!(should_process_objects_child(&leaf, &[]));

        let standard = ChildNode {
            node_id: NodeId::new(0, 2253u32),
            browse_namespace: 0,
            browse_name: "Server".to_string(),
            display_name: "Server".to_string(),
            node_class: NodeClass::Object,
            type_definition: None,
            reference_type_id: NodeId::new(0, ORGANIZES_REFERENCE_ID),
            is_forward: true,
        };
        assert!(!should_process_objects_child(&standard, &[]));
    }

    #[test]
    fn active_path_stops_cycles_but_retains_all_shared_node_paths() {
        fn walk(
            node: u32,
            graph: &HashMap<u32, Vec<u32>>,
            active_path: &mut HashSet<NodeId>,
            current: &mut Vec<u32>,
            reached: &mut Vec<Vec<u32>>,
        ) {
            let node_id = NodeId::new(1, node);
            if !enter_active_path(active_path, &node_id) {
                return;
            }
            current.push(node);
            reached.push(current.clone());
            for child in graph.get(&node).into_iter().flatten() {
                walk(*child, graph, active_path, current, reached);
            }
            current.pop();
            leave_active_path(active_path, &node_id);
        }

        // Three nested containers, a shared leaf reachable through three
        // valid paths, and a back edge from the third container to the root.
        let graph = HashMap::from([
            (1, vec![2, 4]),
            (2, vec![3, 9]),
            (3, vec![9, 1]),
            (4, vec![9]),
        ]);
        let mut reached = Vec::new();
        walk(
            1,
            &graph,
            &mut HashSet::new(),
            &mut Vec::new(),
            &mut reached,
        );

        assert!(reached.contains(&vec![1, 2, 3]));
        assert!(reached.contains(&vec![1, 2, 3, 9]));
        assert!(reached.contains(&vec![1, 2, 9]));
        assert!(reached.contains(&vec![1, 4, 9]));
        assert!(!reached.contains(&vec![1, 2, 3, 1]));
    }

    #[test]
    fn child_order_is_stable_by_namespace_name_and_node_id() {
        let mut children = vec![
            test_child(2, 3, "Status"),
            test_child(1, 2, "Status"),
            test_child(1, 1, "Alpha"),
        ];

        sort_children(&mut children);
        let order = children
            .iter()
            .map(|child| (child.browse_namespace, child.browse_name.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(order, vec![(1, "Alpha"), (1, "Status"), (2, "Status")]);
    }

    #[test]
    fn paper_tree_forming_reference_set_is_exact() {
        for reference_id in [
            ORGANIZES_REFERENCE_ID,
            HAS_COMPONENT_REFERENCE_ID,
            HAS_PROPERTY_REFERENCE_ID,
            HAS_ORDERED_COMPONENT_REFERENCE_ID,
        ] {
            assert!(is_tree_reference_type(&NodeId::new(0, reference_id)));
        }
        assert!(!is_tree_reference_type(&NodeId::new(
            0,
            HIERARCHICAL_REFERENCES_ID
        )));
        assert!(!is_tree_reference_type(&NodeId::new(
            2,
            ORGANIZES_REFERENCE_ID
        )));
    }

    #[test]
    fn source_only_namespace_is_registered_for_qualified_rule_targets() {
        let mut address_space = AddressSpace::new();
        let namespace_uri = "urn:plcm:validation:machine-vision";
        register_rule_target_namespaces(
            &mut address_space,
            &[
                "http://opcfoundation.org/UA/".to_string(),
                namespace_uri.to_string(),
            ],
        )
        .unwrap();
        assert!(namespace_index_by_uri(&address_space, namespace_uri).is_some());
    }

    #[test]
    fn qualified_target_identifier_distinguishes_equal_robotics_instances() {
        let mut address_space = AddressSpace::new();
        let namespace_uri = "urn:test:robotics";
        let namespace = address_space.register_namespace(namespace_uri).unwrap();
        let parent = insert_object_node_under_parent(
            &mut address_space,
            "MotionDevices",
            "MotionDevices",
            NodeId::objects_folder_id(),
            namespace,
            None,
        );
        let first = insert_object_node_under_parent(
            &mut address_space,
            "MotionDevice_Generic",
            "MotionDevice_Generic",
            parent.clone(),
            namespace,
            None,
        );
        let second = insert_object_node_under_parent(
            &mut address_space,
            "MotionDevice_Generic",
            "MotionDevice_Generic",
            parent,
            namespace,
            None,
        );

        let element = |name: &str| QualifiedPathElement {
            namespace_uri: namespace_uri.to_string(),
            name: name.to_string(),
            identifier: None,
        };
        let mut first_path = vec![
            QualifiedPathElement {
                namespace_uri: "http://opcfoundation.org/UA/".to_string(),
                name: "Objects".to_string(),
                identifier: None,
            },
            element("MotionDevices"),
            element("MotionDevice_Generic"),
        ];
        let mut second_path = first_path.clone();
        set_target_identifier(&mut first_path, &first);
        set_target_identifier(&mut second_path, &second);

        assert_eq!(
            find_node_by_qualified_path(&address_space, &first_path),
            Some(first)
        );
        assert_eq!(
            find_node_by_qualified_path(&address_space, &second_path),
            Some(second)
        );
    }

    #[test]
    fn q10_metadata_and_executor_projection_remain_source_bound() {
        let namespaces = vec![
            "http://opcfoundation.org/UA/".to_string(),
            "urn:internal".to_string(),
            "urn:fixture:machine-vision".to_string(),
        ];
        let source_leaf = test_child(2, 42, "TopCam01");
        let target_qualified = vec![
            QualifiedPathElement {
                namespace_uri: namespaces[0].clone(),
                name: "Objects".to_string(),
                identifier: None,
            },
            QualifiedPathElement {
                namespace_uri: namespaces[2].clone(),
                name: "DualCameraVisionSystem_001".to_string(),
                identifier: None,
            },
        ];
        let mut source_qualified = target_qualified.clone();
        source_qualified.push(QualifiedPathElement {
            namespace_uri: namespaces[2].clone(),
            name: source_leaf.browse_name.clone(),
            identifier: None,
        });

        let first = build_mapping_rule(
            "opc.tcp://127.0.0.1:4864/vision/1/",
            &namespaces,
            vec![
                "Objects".to_string(),
                "DualCameraVisionSystem_001".to_string(),
            ],
            vec![
                "Objects".to_string(),
                "DualCameraVisionSystem_001".to_string(),
                "TopCam01".to_string(),
            ],
            target_qualified,
            source_qualified,
            &source_leaf,
        )
        .unwrap();
        let mut second = first.clone();
        second.source_id = "opc.tcp://127.0.0.1:4865/vision/2".to_string();

        assert_eq!(first.source_id, "opc.tcp://127.0.0.1:4864/vision/1");
        assert_eq!(first.reference_policy, REFERENCE_POLICY);
        assert_eq!(first.merge_key, first.target_node_qualified);
        assert_eq!(first.source_node_id.as_ref().unwrap().identifier, "i=42");
        let projected = executor_projection(&[first, second]);
        assert_eq!(projected.len(), 2);
        assert_ne!(projected[0].source_id, projected[1].source_id);
    }

    #[test]
    fn indexed_rule_merge_preserves_order_and_shadow_removal() {
        fn rule(source_id: &str, source_node: &[&str], target_node: &[&str]) -> AggregationRule {
            AggregationRule {
                target_node: target_node
                    .iter()
                    .map(|segment| segment.to_string())
                    .collect(),
                source_node: source_node
                    .iter()
                    .map(|segment| segment.to_string())
                    .collect(),
                ref_type: ORGANIZES_REF_PATH
                    .iter()
                    .map(|segment| segment.to_string())
                    .collect(),
                is_forward: true,
                target_node_qualified: Vec::new(),
                source_node_qualified: Vec::new(),
                source_id: source_id.to_string(),
                source_node_id: None,
                reference_type: None,
                source_reference_type: None,
                source_reference_is_forward: None,
                reference_policy: REFERENCE_POLICY.to_string(),
                merge_policy: "merge_at_qualified_target".to_string(),
                merge_key: Vec::new(),
            }
        }

        let shadowed = rule(
            "opc.tcp://127.0.0.1:4860",
            &["Objects", "Robot"],
            &["Objects"],
        );
        let retained = rule(
            "opc.tcp://127.0.0.1:4860",
            &["Objects", "Robot", "Status"],
            &["Objects", "Robot"],
        );
        let added = rule(
            "opc.tcp://127.0.0.1:4861",
            &["Objects", "Robot", "Status"],
            &["Objects", "Robot"],
        );

        let mut shadowed_rules = ShadowedRulePaths::new();
        remember_shadowed_container_rule(
            &mut shadowed_rules,
            &["Objects".to_string(), "Robot".to_string()],
        );

        let (merged, added_count, removed_count) = merge_rules(
            vec![shadowed, retained.clone()],
            &[retained.clone(), added.clone(), added.clone()],
            &shadowed_rules,
        );

        assert_eq!(removed_count, 1);
        assert_eq!(added_count, 1);
        assert_eq!(merged, vec![retained, added]);

        let expected_executor_json =
            serde_json::to_vec_pretty(&executor_projection(&merged)).unwrap();
        let mut streamed_executor_json = Vec::new();
        write_executor_rules(&mut streamed_executor_json, &merged).unwrap();
        assert_eq!(streamed_executor_json, expected_executor_json);
    }

    #[test]
    fn ordered_rule_set_keeps_first_occurrence_only() {
        let namespaces = vec![
            "http://opcfoundation.org/UA/".to_string(),
            "urn:test:fixture".to_string(),
        ];
        let source_leaf = test_child(1, 42, "Status");
        let rule = build_mapping_rule(
            "opc.tcp://127.0.0.1:4860",
            &namespaces,
            vec!["Objects".to_string()],
            vec!["Objects".to_string(), "Status".to_string()],
            Vec::new(),
            Vec::new(),
            &source_leaf,
        )
        .unwrap();

        let mut rules = OrderedRuleSet::default();
        assert!(rules.insert(rule.clone()));
        assert!(!rules.insert(rule.clone()));
        assert_eq!(rules.as_slice(), &[rule]);
    }

    fn test_child(namespace: u16, node_id: u32, name: &str) -> ChildNode {
        ChildNode {
            node_id: NodeId::new(namespace, node_id),
            browse_namespace: namespace,
            browse_name: name.to_string(),
            display_name: name.to_string(),
            node_class: NodeClass::Variable,
            type_definition: None,
            reference_type_id: NodeId::new(0, HAS_COMPONENT_REFERENCE_ID),
            is_forward: true,
        }
    }
}
