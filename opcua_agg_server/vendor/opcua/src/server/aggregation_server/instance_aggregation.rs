use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use tracing::warn;

use crate::client::prelude::{
    BrowseDirection, QualifiedName, ReferenceDescription, ReferenceTypeId,
};
use crate::prelude::AddressSpace;
use crate::server::aggregation_server::{
    error_types::MappingError,
    map_db::MapDatabaseConnection,
    util_traits::ViewServiceAdditions as _,
    util_types::{
        IncompleteMapping, InstanceMappingRule, LowerServer, RuleBrowsePathElement,
        RuleMergePolicy, RuleNodeIdentity,
    },
};
use crate::types::{NodeClass, NodeId};
use crate::{client::prelude::Session, sync::RwLock};

fn namespace_uri_eq(left: &str, right: &str) -> bool {
    left.trim_end_matches('/') == right.trim_end_matches('/')
}

fn endpoint_eq(left: &str, right: &str) -> bool {
    normalized_endpoint(left) == normalized_endpoint(right)
}

fn normalized_endpoint(endpoint: &str) -> &str {
    endpoint.trim_end_matches('/')
}

fn local_namespace_index(address_space: &AddressSpace, uri: &str) -> Option<u16> {
    address_space.namespace_index(uri).or_else(|| {
        let alternate = if uri.ends_with('/') {
            uri.trim_end_matches('/').to_string()
        } else {
            format!("{}/", uri)
        };
        address_space.namespace_index(&alternate)
    })
}

fn path_element_matches_source(
    element: &RuleBrowsePathElement,
    browse_name: &QualifiedName,
    namespace_array: &[String],
) -> bool {
    if browse_name.name.value().as_deref() != Some(element.name()) {
        return false;
    }
    match element.namespace_uri() {
        None => true,
        Some(uri) => namespace_array
            .get(browse_name.namespace_index as usize)
            .map(|candidate| namespace_uri_eq(candidate, uri))
            .unwrap_or(false),
    }
}

fn path_element_matches_target(
    element: &RuleBrowsePathElement,
    browse_name: &QualifiedName,
    address_space: &AddressSpace,
) -> bool {
    if browse_name.name.value().as_deref() != Some(element.name()) {
        return false;
    }
    match element.namespace_uri() {
        None => true,
        Some(uri) => local_namespace_index(address_space, uri)
            .map(|index| browse_name.namespace_index == index)
            .unwrap_or(false),
    }
}

#[derive(Default)]
struct SourceBrowseCache {
    children_by_parent: HashMap<NodeId, Vec<ReferenceDescription>>,
}

impl SourceBrowseCache {
    fn children<'a>(
        &'a mut self,
        session: &Session,
        parent: &NodeId,
    ) -> Result<&'a [ReferenceDescription], MappingError> {
        if !self.children_by_parent.contains_key(parent) {
            let children = session.get_children(parent).map_err(|error| {
                MappingError::InvalidRule(format!("source browse failed: {error}"))
            })?;
            self.children_by_parent.insert(parent.clone(), children);
        }

        Ok(self
            .children_by_parent
            .get(parent)
            .expect("source children were inserted above"))
    }
}

fn target_node_id_from_element(
    element: &RuleBrowsePathElement,
    address_space: &AddressSpace,
) -> Result<Option<NodeId>, MappingError> {
    let Some(identifier) = element.identifier() else {
        return Ok(None);
    };
    let namespace_uri = element.namespace_uri().ok_or_else(|| {
        MappingError::InvalidRule(
            "a target path identifier requires a namespace-qualified segment".to_string(),
        )
    })?;
    let namespace = local_namespace_index(address_space, namespace_uri).ok_or_else(|| {
        MappingError::InvalidRule(format!(
            "target namespace URI '{namespace_uri}' is absent from the address space"
        ))
    })?;
    let mut node_id = NodeId::from_str(identifier).map_err(|error| {
        MappingError::InvalidRule(format!(
            "invalid namespace-independent target NodeId '{identifier}': {error:?}"
        ))
    })?;
    if node_id.namespace != 0 {
        return Err(MappingError::InvalidRule(format!(
            "target identifier '{identifier}' must not contain a namespace index"
        )));
    }
    node_id.namespace = namespace;
    Ok(Some(node_id))
}

fn unique_source_child(
    session: &Session,
    parent: &NodeId,
    element: &RuleBrowsePathElement,
    namespace_array: &[String],
    browse_cache: &mut SourceBrowseCache,
) -> Result<Option<ReferenceDescription>, MappingError> {
    let children = browse_cache.children(session, parent)?;
    let mut matches = children
        .iter()
        .filter(|child| path_element_matches_source(element, &child.browse_name, namespace_array));
    let first = matches.next();
    if matches.next().is_some() {
        return Err(MappingError::InvalidRule(format!(
            "source segment '{}' is not unique",
            element.name()
        )));
    }
    Ok(first.cloned())
}

fn unique_target_child(
    address_space: &AddressSpace,
    parent: &NodeId,
    element: &RuleBrowsePathElement,
) -> Result<Option<NodeId>, MappingError> {
    let children = address_space
        .find_hierarchical_references(parent)
        .unwrap_or_default();

    if let Some(expected) = target_node_id_from_element(element, address_space)? {
        if !children.iter().any(|child_id| *child_id == expected) {
            return Ok(None);
        }
        let matches_metadata = address_space
            .find(&expected)
            .map(|child| {
                path_element_matches_target(element, &child.as_node().browse_name(), address_space)
            })
            .unwrap_or(false);
        return if matches_metadata {
            Ok(Some(expected))
        } else {
            Err(MappingError::InvalidRule(format!(
                "target identifier '{}' does not match segment '{}'",
                element.identifier().unwrap_or_default(),
                element.name()
            )))
        };
    }

    let mut matches = children.into_iter().filter(|child_id| {
        address_space
            .find(child_id)
            .map(|child| {
                path_element_matches_target(element, &child.as_node().browse_name(), address_space)
            })
            .unwrap_or(false)
    });
    let first = matches.next();
    if matches.next().is_some() {
        return Err(MappingError::InvalidRule(format!(
            "target segment '{}' is not unique",
            element.name()
        )));
    }
    Ok(first)
}

fn resolve_source_path(
    session: &Session,
    path: &[RuleBrowsePathElement],
    namespace_array: &[String],
    browse_cache: &mut SourceBrowseCache,
) -> Result<Option<(NodeId, ReferenceDescription)>, MappingError> {
    let mut current = NodeId::root_folder_id();
    let mut final_reference = None;
    for element in path {
        let Some(reference) =
            unique_source_child(session, &current, element, namespace_array, browse_cache)?
        else {
            return Ok(None);
        };
        current = reference.node_id.node_id.clone();
        final_reference = Some(reference);
    }
    Ok(final_reference.map(|reference| (current, reference)))
}

fn resolve_target_path(
    address_space: &AddressSpace,
    path: &[RuleBrowsePathElement],
) -> Result<Option<NodeId>, MappingError> {
    let mut current = NodeId::root_folder_id();
    for element in path {
        let Some(child) = unique_target_child(address_space, &current, element)? else {
            return Ok(None);
        };
        current = child;
    }
    Ok(Some(current))
}

fn node_id_from_identity(
    identity: &RuleNodeIdentity,
    namespace_array: &[String],
) -> Result<NodeId, MappingError> {
    let namespace = namespace_array
        .iter()
        .position(|uri| namespace_uri_eq(uri, &identity.namespace_uri))
        .ok_or_else(|| {
            MappingError::InvalidRule(format!(
                "namespace URI '{}' is absent from the source NamespaceArray",
                identity.namespace_uri
            ))
        })? as u16;
    let mut node_id = NodeId::from_str(&identity.identifier).map_err(|error| {
        MappingError::InvalidRule(format!(
            "invalid namespace-independent NodeId '{}': {:?}",
            identity.identifier, error
        ))
    })?;
    if node_id.namespace != 0 {
        return Err(MappingError::InvalidRule(format!(
            "rule identifier '{}' must not contain a namespace index",
            identity.identifier
        )));
    }
    node_id.namespace = namespace;
    Ok(node_id)
}

fn validate_rule(rule: &InstanceMappingRule) -> Result<(), MappingError> {
    if rule.target_node.is_empty() || rule.source_node.is_empty() {
        return Err(MappingError::InvalidRule(
            "source_node and target_node must not be empty".to_string(),
        ));
    }

    if rule.merge_policy == RuleMergePolicy::MergeAtQualifiedTarget {
        if rule.source_id.is_none()
            || rule.source_node_id.is_none()
            || rule.reference_type.is_none()
        {
            return Err(MappingError::InvalidRule(
                "qualified rule lacks source_id, source_node_id, or reference_type".to_string(),
            ));
        }
        if rule.merge_key != rule.target_node {
            return Err(MappingError::InvalidRule(
                "merge_key must equal the exact qualified target path".to_string(),
            ));
        }
        if rule
            .source_node
            .iter()
            .chain(rule.target_node.iter())
            .chain(rule.merge_key.iter())
            .any(|element| element.namespace_uri().is_none())
        {
            return Err(MappingError::InvalidRule(
                "qualified merge policy contains a legacy local-name segment".to_string(),
            ));
        }
    }
    Ok(())
}

fn rule_applies_to_source(
    rule: &InstanceMappingRule,
    lower_server: &LowerServer,
) -> Result<bool, MappingError> {
    validate_rule(rule)?;
    Ok(rule
        .source_id
        .as_deref()
        .map(|source_id| endpoint_eq(source_id, &lower_server.url))
        .unwrap_or(true))
}

/// Exact stored rule identity for preserving pending references across replacement.
/// Borrowing every field avoids serializing or cloning the complete rule collection.
#[derive(PartialEq, Eq, Hash)]
pub(crate) struct InstanceMappingRuleKey<'a> {
    target_node: &'a [RuleBrowsePathElement],
    source_node: &'a [RuleBrowsePathElement],
    ref_type: &'a [RuleBrowsePathElement],
    is_forward: bool,
    source_id: Option<&'a str>,
    source_node_id: Option<(&'a str, &'a str)>,
    reference_type: Option<(&'a str, &'a str)>,
    merge_policy: std::mem::Discriminant<RuleMergePolicy>,
    merge_key: &'a [RuleBrowsePathElement],
}

pub(crate) fn mapping_rule_key(rule: &InstanceMappingRule) -> InstanceMappingRuleKey<'_> {
    InstanceMappingRuleKey {
        target_node: &rule.target_node,
        source_node: &rule.source_node,
        ref_type: &rule.ref_type,
        is_forward: rule.is_forward,
        source_id: rule.source_id.as_deref(),
        source_node_id: rule
            .source_node_id
            .as_ref()
            .map(|value| (value.namespace_uri.as_str(), value.identifier.as_str())),
        reference_type: rule
            .reference_type
            .as_ref()
            .map(|value| (value.namespace_uri.as_str(), value.identifier.as_str())),
        merge_policy: std::mem::discriminant(&rule.merge_policy),
        merge_key: &rule.merge_key,
    }
}

/// Indexes the canonical rule vector without changing its stable positions.
///
/// `IncompleteMapping::rule_id` continues to address the public canonical
/// vector directly. The index only narrows the current-source iteration and
/// never renumbers rules.
#[derive(Debug, Default)]
pub(crate) struct InstanceMappingRuleIndex {
    indices_by_source: HashMap<String, Vec<usize>>,
    unscoped_indices: Vec<usize>,
    rule_count: usize,
}

impl InstanceMappingRuleIndex {
    pub(crate) fn new(rules: &[InstanceMappingRule]) -> Result<Self, MappingError> {
        let mut indices_by_source: HashMap<String, Vec<usize>> = HashMap::new();
        let mut unscoped_indices = Vec::new();

        for (rule_index, rule) in rules.iter().enumerate() {
            validate_rule(rule)?;
            if let Some(source_id) = rule.source_id.as_deref() {
                indices_by_source
                    .entry(normalized_endpoint(source_id).to_string())
                    .or_default()
                    .push(rule_index);
            } else {
                unscoped_indices.push(rule_index);
            }
        }

        Ok(Self {
            indices_by_source,
            unscoped_indices,
            rule_count: rules.len(),
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.rule_count
    }

    /// Append an independently validated delta without rescanning old rules.
    pub(crate) fn append(&mut self, delta: Self) -> Result<(), MappingError> {
        let new_count = self
            .rule_count
            .checked_add(delta.rule_count)
            .ok_or_else(|| MappingError::InvalidRule("mapping rule count overflow".to_string()))?;
        let offset = self.rule_count;
        for (source, indices) in delta.indices_by_source {
            self.indices_by_source
                .entry(source)
                .or_default()
                .extend(indices.into_iter().map(|index| index + offset));
        }
        self.unscoped_indices.extend(
            delta
                .unscoped_indices
                .into_iter()
                .map(|index| index + offset),
        );
        self.rule_count = new_count;
        Ok(())
    }

    /// Remove only the appended suffix and the source buckets it touches.
    /// Check the complete suffix before mutating the index.
    pub(crate) fn truncate(
        &mut self,
        rules: &[InstanceMappingRule],
        len: usize,
    ) -> Result<(), MappingError> {
        if self.rule_count != rules.len() || len > rules.len() {
            return Err(MappingError::InvalidRule(
                "rule vector and source index disagree on truncation bounds".to_string(),
            ));
        }
        let mut removed_by_source: HashMap<&str, usize> = HashMap::new();
        let mut removed_unscoped = 0;
        for (rule_id, rule) in rules.iter().enumerate().skip(len).rev() {
            let (indices, removed) = if let Some(source) = rule.source_id.as_deref() {
                let source = normalized_endpoint(source);
                let indices = self.indices_by_source.get(source).ok_or_else(|| {
                    MappingError::InvalidRule("missing source index on truncation".to_string())
                })?;
                (indices, removed_by_source.entry(source).or_default())
            } else {
                (&self.unscoped_indices, &mut removed_unscoped)
            };
            let position = indices.len().checked_sub(*removed + 1);
            if position.and_then(|position| indices.get(position)) != Some(&rule_id) {
                return Err(MappingError::InvalidRule(
                    "source index suffix does not match canonical rules".to_string(),
                ));
            }
            *removed += 1;
        }
        for (source, count) in removed_by_source {
            let indices = self
                .indices_by_source
                .get_mut(source)
                .expect("checked source index");
            indices.truncate(indices.len() - count);
            if indices.is_empty() {
                self.indices_by_source.remove(source);
            }
        }
        self.unscoped_indices
            .truncate(self.unscoped_indices.len() - removed_unscoped);
        self.rule_count = len;
        Ok(())
    }

    pub(crate) fn indices_for_source(&self, source_url: &str) -> Vec<usize> {
        let scoped_indices = self
            .indices_by_source
            .get(normalized_endpoint(source_url))
            .map(Vec::as_slice)
            .unwrap_or_default();

        if self.unscoped_indices.is_empty() {
            return scoped_indices.to_vec();
        }
        if scoped_indices.is_empty() {
            return self.unscoped_indices.clone();
        }

        let mut merged = Vec::with_capacity(scoped_indices.len() + self.unscoped_indices.len());
        let mut scoped = scoped_indices.iter().copied().peekable();
        let mut unscoped = self.unscoped_indices.iter().copied().peekable();
        loop {
            match (scoped.peek(), unscoped.peek()) {
                (Some(left), Some(right)) if left <= right => {
                    merged.push(scoped.next().expect("peeked scoped rule index"));
                }
                (Some(_), Some(_)) => {
                    merged.push(unscoped.next().expect("peeked unscoped rule index"));
                }
                (Some(_), None) => {
                    merged.extend(scoped);
                    break;
                }
                (None, Some(_)) => {
                    merged.extend(unscoped);
                    break;
                }
                (None, None) => break,
            }
        }
        merged
    }
}

fn resolve_reference_type(
    rule: &InstanceMappingRule,
    address_space: &AddressSpace,
    map_db: &MapDatabaseConnection,
    lower_server: &LowerServer,
) -> Result<NodeId, MappingError> {
    let reference_type = if let Some(identity) = &rule.reference_type {
        let source_type = node_id_from_identity(identity, &lower_server.namespace_array)?;
        if source_type.namespace == 0 {
            source_type
        } else {
            map_db
                .get_aggserver_type(lower_server.id, &source_type)?
                .ok_or_else(|| {
                    MappingError::InvalidRule(format!(
                        "reference type {source_type} has no aggregation-server mapping"
                    ))
                })?
        }
    } else {
        resolve_target_path(address_space, &rule.ref_type)?.ok_or_else(|| {
            MappingError::InvalidRule("legacy reference-type path was not found".to_string())
        })?
    };

    let node = address_space.find(&reference_type).ok_or_else(|| {
        MappingError::InvalidRule(format!(
            "resolved reference type {reference_type} is absent from the address space"
        ))
    })?;
    if node.as_node().node_class() != NodeClass::ReferenceType {
        return Err(MappingError::InvalidRule(format!(
            "resolved reference type {reference_type} has class {:?}",
            node.as_node().node_class()
        )));
    }
    Ok(reference_type)
}

fn target_metadata(
    address_space: &AddressSpace,
    target: &NodeId,
) -> Result<
    (
        QualifiedName,
        crate::types::LocalizedText,
        NodeClass,
        NodeId,
    ),
    MappingError,
> {
    let node = address_space
        .find(target)
        .ok_or_else(|| MappingError::InvalidRule(format!("target node {target} disappeared")))?;
    let browse_name = node.as_node().browse_name();
    let display_name = node.as_node().display_name();
    let node_class = node.as_node().node_class();
    let (type_references, _) = address_space.find_references_by_direction(
        target,
        BrowseDirection::Forward,
        Some((ReferenceTypeId::HasTypeDefinition, true)),
    );
    let type_definition = type_references
        .into_iter()
        .next()
        .map(|reference| reference.target_node)
        .ok_or_else(|| MappingError::InvalidRule(format!("target node {target} has no type")))?;
    Ok((browse_name, display_name, node_class, type_definition))
}

pub fn aggregate_instances(
    session_p: &Arc<RwLock<Session>>,
    address_space_p: &Arc<RwLock<AddressSpace>>,
    instance_mapping_rules: Arc<RwLock<Vec<InstanceMappingRule>>>,
    map_db: &MapDatabaseConnection,
    lower_server: &mut LowerServer,
    incomplete_mappings_p: Arc<RwLock<Vec<IncompleteMapping>>>,
) -> Result<(), MappingError> {
    let rule_index = {
        lower_server.check_operation()?;
        let rules = instance_mapping_rules.read();
        InstanceMappingRuleIndex::new(&rules)?
    };
    aggregate_instances_indexed(
        session_p,
        address_space_p,
        instance_mapping_rules,
        Arc::new(RwLock::new(rule_index)),
        map_db,
        lower_server,
        incomplete_mappings_p,
    )
}

pub(crate) fn aggregate_instances_indexed(
    session_p: &Arc<RwLock<Session>>,
    address_space_p: &Arc<RwLock<AddressSpace>>,
    instance_mapping_rules: Arc<RwLock<Vec<InstanceMappingRule>>>,
    instance_mapping_rule_index: Arc<RwLock<InstanceMappingRuleIndex>>,
    map_db: &MapDatabaseConnection,
    lower_server: &mut LowerServer,
    incomplete_mappings_p: Arc<RwLock<Vec<IncompleteMapping>>>,
) -> Result<(), MappingError> {
    lower_server.check_operation()?;
    let rules = instance_mapping_rules.read();
    let rule_indices = instance_mapping_rule_index.read();
    let mut incomplete_mappings = incomplete_mappings_p.write();
    let pending = std::mem::take(&mut *incomplete_mappings);
    let mut source_browse_cache = SourceBrowseCache::default();

    for mapping in pending {
        lower_server.check_operation()?;
        let rule = rules.get(mapping.rule_id).ok_or_else(|| {
            MappingError::InvalidRule(format!("missing rule {}", mapping.rule_id))
        })?;
        if !rule_applies_to_source(rule, lower_server)? {
            continue;
        }

        let address_space = address_space_p.read();
        let Some(target) = resolve_target_path(&address_space, &rule.target_node)? else {
            incomplete_mappings.push(mapping);
            continue;
        };
        let (target_browse_name, target_display_name, target_class, target_type) =
            target_metadata(&address_space, &target)?;
        let reference_type = resolve_reference_type(rule, &address_space, map_db, lower_server)?;
        drop(address_space);

        let forward = ReferenceDescription {
            reference_type_id: reference_type.clone(),
            is_forward: rule.is_forward,
            node_id: mapping.source_nid.clone().into(),
            browse_name: mapping.source_bname.clone(),
            display_name: mapping.source_dname.clone(),
            node_class: mapping.source_class,
            type_definition: mapping.source_type.clone().into(),
        };
        let inverse = ReferenceDescription {
            reference_type_id: reference_type,
            is_forward: !rule.is_forward,
            node_id: target.clone().into(),
            browse_name: target_browse_name,
            display_name: target_display_name,
            node_class: target_class,
            type_definition: target_type.into(),
        };
        map_db.insert_reference(&target, lower_server.id, &forward)?;
        map_db.insert_reference(&mapping.source_nid, lower_server.id, &inverse)?;
    }

    'rules: for rule_index in rule_indices.indices_for_source(&lower_server.url) {
        lower_server.check_operation()?;
        let rule = rules.get(rule_index).ok_or_else(|| {
            MappingError::InvalidRule(format!("missing indexed rule {rule_index}"))
        })?;
        debug_assert!(rule_applies_to_source(rule, lower_server)?);

        let source = {
            let session = session_p.read();
            resolve_source_path(
                &session,
                &rule.source_node,
                &lower_server.namespace_array,
                &mut source_browse_cache,
            )?
        };
        let Some((source_node, source_reference)) = source else {
            warn!(rule_index, "Mapping rule source path was not found");
            continue;
        };
        if let Some(identity) = &rule.source_node_id {
            let expected = node_id_from_identity(identity, &lower_server.namespace_array)?;
            if source_node != expected {
                return Err(MappingError::InvalidRule(format!(
                    "source path resolved to {source_node}, expected {expected}"
                )));
            }
        }

        let transformed_namespace =
            map_db.get_aggserver_nsid_inst(lower_server.id, source_node.namespace)?;
        let aggregated_source = NodeId::new(transformed_namespace, source_node.identifier);
        let source_browse_name =
            QualifiedName::new(transformed_namespace, source_reference.browse_name.name);
        let source_display_name = source_reference.display_name;
        let source_class = source_reference.node_class;
        let source_type = map_db
            .get_aggserver_type(lower_server.id, &source_reference.type_definition.node_id)?
            .unwrap_or(source_reference.type_definition.node_id);

        let address_space = address_space_p.read();
        let Some(target) = resolve_target_path(&address_space, &rule.target_node)? else {
            incomplete_mappings.push(IncompleteMapping {
                source_server_id: lower_server.id,
                source_server_name: lower_server.name.clone(),
                rule_id: rule_index,
                source_nid: aggregated_source,
                source_bname: source_browse_name,
                source_dname: source_display_name,
                source_class,
                source_type,
            });
            continue 'rules;
        };
        let (target_browse_name, target_display_name, target_class, target_type) =
            target_metadata(&address_space, &target)?;
        let reference_type = resolve_reference_type(rule, &address_space, map_db, lower_server)?;
        drop(address_space);

        let forward = ReferenceDescription {
            reference_type_id: reference_type.clone(),
            is_forward: rule.is_forward,
            node_id: aggregated_source.clone().into(),
            browse_name: source_browse_name,
            display_name: source_display_name,
            node_class: source_class,
            type_definition: source_type.into(),
        };
        let inverse = ReferenceDescription {
            reference_type_id: reference_type,
            is_forward: !rule.is_forward,
            node_id: target.clone().into(),
            browse_name: target_browse_name,
            display_name: target_display_name,
            node_class: target_class,
            type_definition: target_type.into(),
        };
        map_db.insert_reference(&target, lower_server.id, &forward)?;
        map_db.insert_reference(&aggregated_source, lower_server.id, &inverse)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::prelude::ObjectBuilder;

    fn indexed_rule(source_id: Option<&str>) -> InstanceMappingRule {
        InstanceMappingRule {
            target_node: vec![RuleBrowsePathElement::Legacy("Target".to_string())],
            source_node: vec![RuleBrowsePathElement::Legacy("Source".to_string())],
            ref_type: vec![RuleBrowsePathElement::Legacy("Organizes".to_string())],
            is_forward: true,
            source_id: source_id.map(str::to_string),
            source_node_id: None,
            reference_type: None,
            merge_policy: RuleMergePolicy::LegacyUniquePath,
            merge_key: Vec::new(),
        }
    }

    #[test]
    fn source_index_preserves_global_order_and_includes_unscoped_rules() {
        let rules = vec![
            indexed_rule(Some("opc.tcp://localhost:4860")),
            indexed_rule(None),
            indexed_rule(Some("opc.tcp://localhost:4861")),
            indexed_rule(Some("opc.tcp://localhost:4860/")),
        ];
        let index = InstanceMappingRuleIndex::new(&rules).unwrap();

        assert_eq!(
            index.indices_for_source("opc.tcp://localhost:4860/"),
            vec![0, 1, 3]
        );
        assert_eq!(
            index.indices_for_source("opc.tcp://localhost:4861"),
            vec![1, 2]
        );
        assert_eq!(
            index.indices_for_source("opc.tcp://localhost:4999"),
            vec![1]
        );
    }

    #[test]
    fn source_index_rejects_invalid_rules_before_aggregation() {
        let mut invalid = indexed_rule(Some("opc.tcp://localhost:4860"));
        invalid.source_node.clear();

        assert!(InstanceMappingRuleIndex::new(&[invalid]).is_err());
    }

    #[test]
    fn delta_index_and_suffix_rollback_preserve_scoped_order() {
        let mut rules = vec![indexed_rule(Some("opc.tcp://a")), indexed_rule(None)];
        let mut index = InstanceMappingRuleIndex::new(&rules).unwrap();
        let delta = vec![
            indexed_rule(Some("opc.tcp://b")),
            indexed_rule(None),
            indexed_rule(Some("opc.tcp://a/")),
            indexed_rule(Some("opc.tcp://b/")),
        ];
        index
            .append(InstanceMappingRuleIndex::new(&delta).unwrap())
            .unwrap();
        rules.extend(delta);
        assert_eq!(index.indices_for_source("opc.tcp://a/"), vec![0, 1, 3, 4]);
        assert_eq!(index.indices_for_source("opc.tcp://b"), vec![1, 2, 3, 5]);
        assert_eq!(index.indices_for_source("opc.tcp://other"), vec![1, 3]);
        index.truncate(&rules, 3).unwrap();
        rules.truncate(3);
        assert_eq!(index.indices_for_source("opc.tcp://a"), vec![0, 1]);
        assert_eq!(index.indices_for_source("opc.tcp://b"), vec![1, 2]);
        index.truncate(&rules, 2).unwrap();
        rules.truncate(2);
        assert!(!index.indices_by_source.contains_key("opc.tcp://b"));
        index.truncate(&rules, 0).unwrap();
        assert_eq!(index.len(), 0);
        assert!(index.indices_by_source.is_empty());
        assert!(index.unscoped_indices.is_empty());
    }

    #[test]
    fn invalid_index_suffix_is_rejected_before_any_bucket_changes() {
        let rules = vec![
            indexed_rule(Some("opc.tcp://a")),
            indexed_rule(Some("opc.tcp://b")),
            indexed_rule(Some("opc.tcp://a")),
        ];
        let mut index = InstanceMappingRuleIndex::new(&rules).unwrap();
        index.indices_by_source.get_mut("opc.tcp://b").unwrap()[0] = 0;
        assert!(index.truncate(&rules, 1).is_err());
        assert_eq!(index.len(), 3);
        assert_eq!(index.indices_for_source("opc.tcp://a"), vec![0, 2]);
        assert_eq!(index.indices_for_source("opc.tcp://b"), vec![0]);
        assert!(index.truncate(&rules, 4).is_err());
    }

    #[test]
    fn qualified_segments_distinguish_duplicate_local_names() {
        let namespaces = vec![
            "http://opcfoundation.org/UA/".to_string(),
            "urn:test:a".to_string(),
            "urn:test:b".to_string(),
        ];
        let a = RuleBrowsePathElement::Qualified {
            namespace_uri: "urn:test:a".to_string(),
            name: "Duplicate".to_string(),
            identifier: None,
        };
        let b = RuleBrowsePathElement::Qualified {
            namespace_uri: "urn:test:b".to_string(),
            name: "Duplicate".to_string(),
            identifier: None,
        };
        let browse_a = QualifiedName::new(1, "Duplicate");
        let browse_b = QualifiedName::new(2, "Duplicate");

        assert!(path_element_matches_source(&a, &browse_a, &namespaces));
        assert!(!path_element_matches_source(&a, &browse_b, &namespaces));
        assert!(path_element_matches_source(&b, &browse_b, &namespaces));
    }

    #[test]
    fn exact_merge_policy_requires_machine_verifiable_identity() {
        let qualified = |name: &str| RuleBrowsePathElement::Qualified {
            namespace_uri: "urn:test".to_string(),
            name: name.to_string(),
            identifier: None,
        };
        let rule = InstanceMappingRule {
            target_node: vec![qualified("Target")],
            source_node: vec![qualified("Source")],
            ref_type: Vec::new(),
            is_forward: true,
            source_id: Some("opc.tcp://localhost:4841".to_string()),
            source_node_id: Some(RuleNodeIdentity {
                namespace_uri: "urn:test".to_string(),
                identifier: "i=1".to_string(),
            }),
            reference_type: Some(RuleNodeIdentity {
                namespace_uri: "http://opcfoundation.org/UA/".to_string(),
                identifier: "i=47".to_string(),
            }),
            merge_policy: RuleMergePolicy::MergeAtQualifiedTarget,
            merge_key: vec![qualified("Target")],
        };
        assert!(validate_rule(&rule).is_ok());
    }

    #[test]
    fn target_identifier_distinguishes_equal_sibling_browse_names() {
        let mut address_space = AddressSpace::new();
        let namespace_uri = "urn:test:robotics";
        let namespace = address_space.register_namespace(namespace_uri).unwrap();
        let first = NodeId::new(namespace, 100u32);
        let second = NodeId::new(namespace, 101u32);

        for node_id in [&first, &second] {
            assert!(ObjectBuilder::new(
                node_id,
                QualifiedName::new(namespace, "MotionDevice_Generic"),
                "MotionDevice_Generic",
            )
            .has_type_definition(NodeId::new(0, 61u32))
            .organized_by(NodeId::objects_folder_id())
            .insert(&mut address_space));
        }

        let selected = RuleBrowsePathElement::Qualified {
            namespace_uri: namespace_uri.to_string(),
            name: "MotionDevice_Generic".to_string(),
            identifier: Some("i=101".to_string()),
        };
        assert_eq!(
            unique_target_child(&address_space, &NodeId::objects_folder_id(), &selected).unwrap(),
            Some(second)
        );
    }
}
