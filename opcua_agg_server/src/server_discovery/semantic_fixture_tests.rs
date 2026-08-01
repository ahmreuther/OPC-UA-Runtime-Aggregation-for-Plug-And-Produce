// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

use super::*;

use opcua::server::prelude::{ObjectTypeBuilder, ReferenceTypeId};
use opcua::types::service_types::{BrowseResult, ReferenceDescription};
use opcua::types::{ByteString, LocalizedText, QualifiedName, StatusCode};
use std::collections::{HashMap, HashSet};

const NS_A: &str = "urn:ojies:test:a";
const NS_B: &str = "urn:ojies:test:b";
const NS_TYPES: &str = "urn:ojies:test:types";
const SOURCE_A: &str = "opc.tcp://127.0.0.1:4860";
const SOURCE_B: &str = "opc.tcp://127.0.0.1:4861";

fn q(namespace_uri: &str, name: &str) -> RuleBrowsePathElement {
    RuleBrowsePathElement::qualified(namespace_uri, name)
}

fn child(
    node_id: NodeId,
    browse_namespace: u16,
    browse_name: &str,
    node_class: NodeClass,
    type_definition: Option<NodeId>,
    reference_type: NodeId,
    is_forward: bool,
) -> ChildNode {
    ChildNode {
        node_id,
        browse_namespace,
        browse_name: browse_name.to_string(),
        display_name: browse_name.to_string(),
        node_class,
        type_definition,
        reference_type,
        is_forward,
    }
}

fn reference(node_id: u32, name: &str) -> ReferenceDescription {
    ReferenceDescription {
        reference_type_id: ReferenceTypeId::HasComponent.into(),
        is_forward: true,
        node_id: NodeId::new(1, node_id).into(),
        browse_name: QualifiedName::new(1, name),
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
fn forced_continuation_point_is_drained_and_browse_errors_are_propagated() {
    let forced_continuation_point = ByteString::from(vec![0x01, 0x02]);
    let first = browse_page(
        StatusCode::Good,
        forced_continuation_point.clone(),
        vec![reference(1, "PageOneLeaf")],
    );
    let mut browse_next_calls = 0;

    let references = collect_browse_references(first, |continuation_point| {
        browse_next_calls += 1;
        assert_eq!(continuation_point, forced_continuation_point);
        Ok(browse_page(
            StatusCode::Good,
            ByteString::null(),
            vec![reference(2, "PageTwoLeaf")],
        ))
    })
    .expect("all forced browse pages must be collected");

    assert_eq!(browse_next_calls, 1);
    assert_eq!(references.len(), 2);
    assert_eq!(references[0].node_id.node_id, NodeId::new(1, 1u32));
    assert_eq!(references[1].node_id.node_id, NodeId::new(1, 2u32));

    let bad_result = browse_page(StatusCode::BadNodeIdUnknown, ByteString::null(), Vec::new());
    let error = collect_browse_references(bad_result, |_| {
        panic!("BrowseNext must not run after a bad initial BrowseResult")
    })
    .expect_err("bad Browse status must not become an empty child list");
    assert!(error.to_string().contains("Browse lieferte StatusCode"));

    let repeated = ByteString::from(vec![0xAA]);
    let repeated_error = collect_browse_references(
        browse_page(StatusCode::Good, repeated.clone(), Vec::new()),
        |_| Ok(browse_page(StatusCode::Good, repeated.clone(), Vec::new())),
    )
    .expect_err("a repeated continuation point must terminate with an error");
    assert!(repeated_error.to_string().contains("ContinuationPoint"));
}

#[test]
fn recursive_semantic_fixture_preserves_identity_types_references_and_merge_policy() {
    // The source NamespaceArray deliberately differs from the local registration
    // order below. This makes the TypeDefinition assertion a real URI remap.
    let source_namespaces = vec![
        OPC_UA_NAMESPACE_URI.to_string(),
        NS_A.to_string(),
        NS_B.to_string(),
        NS_TYPES.to_string(),
    ];

    let container_1 = child(
        NodeId::new(1, 100u32),
        1,
        "Container1",
        NodeClass::Object,
        Some(NodeId::new(0, FOLDER_TYPE_ID)),
        ReferenceTypeId::HasComponent.into(),
        true,
    );
    let container_2 = child(
        NodeId::new(1, 101u32),
        1,
        "Container2",
        NodeClass::Object,
        Some(NodeId::new(0, FOLDER_TYPE_ID)),
        ReferenceTypeId::HasComponent.into(),
        true,
    );
    let container_3 = child(
        NodeId::new(1, 102u32),
        1,
        "Container3",
        NodeClass::Object,
        Some(NodeId::new(0, FOLDER_TYPE_ID)),
        ReferenceTypeId::HasComponent.into(),
        true,
    );
    let duplicate_a = child(
        NodeId::new(1, 110u32),
        1,
        "Status",
        NodeClass::Variable,
        Some(NodeId::new(3, 500u32)),
        ReferenceTypeId::HasComponent.into(),
        true,
    );
    let duplicate_b = child(
        NodeId::new(2, 210u32),
        2,
        "Status",
        NodeClass::Variable,
        Some(NodeId::new(3, 500u32)),
        ReferenceTypeId::HasProperty.into(),
        true,
    );
    let shared = child(
        NodeId::new(1, 120u32),
        1,
        "SharedLeaf",
        NodeClass::Variable,
        Some(NodeId::new(3, 500u32)),
        ReferenceTypeId::HasComponent.into(),
        true,
    );
    let unsupported = child(
        NodeId::new(1, 130u32),
        1,
        "UnsupportedObjectType",
        NodeClass::ObjectType,
        None,
        ReferenceTypeId::HasComponent.into(),
        true,
    );

    assert!(is_supported_instance_node(&container_1));
    assert!(is_supported_instance_node(&duplicate_a));
    assert!(!is_supported_instance_node(&unsupported));

    // Three nested containers; duplicate local names in two namespaces; a
    // shared node; an unsupported NodeClass; and Container3 -> Container1.
    let fixture_edges = HashMap::from([
        (
            container_1.node_id.clone(),
            vec![container_2.node_id.clone()],
        ),
        (
            container_2.node_id.clone(),
            vec![container_3.node_id.clone(), shared.node_id.clone()],
        ),
        (
            container_3.node_id.clone(),
            vec![
                duplicate_a.node_id.clone(),
                duplicate_b.node_id.clone(),
                shared.node_id.clone(),
                unsupported.node_id.clone(),
                container_1.node_id.clone(),
            ],
        ),
    ]);
    assert_eq!(fixture_edges.len(), 3);
    assert!(fixture_edges[&container_3.node_id].contains(&container_1.node_id));

    // Same two invariants as create_or_map_subtree: active_path rejects a back
    // edge; the per-source visited set makes the first qualified path to a
    // shared node win deterministically and suppresses every later DAG edge.
    let mut active_path = HashSet::new();
    assert!(active_path.insert(container_1.node_id.clone()));
    assert!(active_path.insert(container_2.node_id.clone()));
    assert!(active_path.insert(container_3.node_id.clone()));
    assert!(!active_path.insert(container_1.node_id.clone()));
    let mut visited_source_nodes = HashSet::new();
    assert!(first_qualified_path_wins(
        &mut visited_source_nodes,
        &shared.node_id
    ));
    assert!(!first_qualified_path_wins(
        &mut visited_source_nodes,
        &shared.node_id
    ));

    let objects = q(OPC_UA_NAMESPACE_URI, "Objects");
    let container_path = vec![
        objects.clone(),
        q(NS_A, "Container1"),
        q(NS_A, "Container2"),
        q(NS_A, "Container3"),
    ];
    let mut duplicate_a_path = container_path.clone();
    duplicate_a_path.push(q(NS_A, "Status"));
    let mut duplicate_b_path = container_path.clone();
    duplicate_b_path.push(q(NS_B, "Status"));

    let shared_via_container_2 = vec![
        objects.clone(),
        q(NS_A, "Container1"),
        q(NS_A, "Container2"),
        q(NS_A, "SharedLeaf"),
    ];
    let mut shared_via_container_3 = container_path.clone();
    shared_via_container_3.push(q(NS_A, "SharedLeaf"));

    let rule_a = make_mapping_rule(
        SOURCE_A,
        &source_namespaces,
        container_path.clone(),
        duplicate_a_path.clone(),
        &duplicate_a,
    )
    .unwrap();
    let rule_b = make_mapping_rule(
        SOURCE_A,
        &source_namespaces,
        container_path.clone(),
        duplicate_b_path.clone(),
        &duplicate_b,
    )
    .unwrap();
    let shared_rule_1 = make_mapping_rule(
        SOURCE_A,
        &source_namespaces,
        container_path.clone(),
        shared_via_container_2.clone(),
        &shared,
    )
    .unwrap();
    // Source and target coverage includes both colliding leaves and exactly the
    // first qualified path to the shared node; every segment is qualified.
    let rules = [&rule_a, &rule_b, &shared_rule_1];
    let covered_paths: HashSet<Vec<RuleBrowsePathElement>> =
        rules.iter().map(|rule| rule.source_node.clone()).collect();
    assert_eq!(covered_paths.len(), 3);
    assert!(covered_paths.contains(&duplicate_a_path));
    assert!(covered_paths.contains(&duplicate_b_path));
    assert!(covered_paths.contains(&shared_via_container_2));
    assert!(!covered_paths.contains(&shared_via_container_3));
    assert!(rules
        .iter()
        .all(|rule| rule.source_node_id.as_ref().unwrap().identifier != "i=130"));
    let covered_targets: HashSet<Vec<RuleBrowsePathElement>> =
        rules.iter().map(|rule| rule.target_node.clone()).collect();
    assert_eq!(covered_targets, HashSet::from([container_path.clone()]));
    assert!(rules.iter().all(|rule| {
        rule.source_node
            .iter()
            .chain(rule.target_node.iter())
            .all(|element| element.namespace_uri().is_some())
    }));

    assert_ne!(rule_a.source_node, rule_b.source_node);
    assert_ne!(rule_a.source_node_id, rule_b.source_node_id);
    assert_eq!(rule_a.source_node_id.as_ref().unwrap().namespace_uri, NS_A);
    assert_eq!(rule_b.source_node_id.as_ref().unwrap().namespace_uri, NS_B);
    assert_eq!(rule_a.source_node_id.as_ref().unwrap().identifier, "i=110");
    assert_eq!(rule_b.source_node_id.as_ref().unwrap().identifier, "i=210");
    assert_eq!(rule_a.reference_type.as_ref().unwrap().identifier, "i=47");
    assert_eq!(rule_b.reference_type.as_ref().unwrap().identifier, "i=46");
    assert!(rule_a.is_forward);
    assert!(rule_b.is_forward);

    // The one retained shared rule still carries the stable source identity;
    // the suppressed path is an intentional, explicit consolidation decision.
    assert_eq!(
        shared_rule_1.source_node_id.as_ref().unwrap().identifier,
        "i=120"
    );

    // Intended consolidation is explicit: different source endpoints may share
    // the exact qualified target merge key. A namespace-distinct target with
    // the same local name remains a different merge key.
    let rule_from_second_source = make_mapping_rule(
        SOURCE_B,
        &source_namespaces,
        container_path.clone(),
        duplicate_a_path,
        &duplicate_a,
    )
    .unwrap();
    assert_ne!(rule_a.source_id, rule_from_second_source.source_id);
    assert_eq!(rule_a.merge_key, rule_from_second_source.merge_key);
    assert_eq!(rule_a.merge_key, rule_a.target_node);

    let mut namespace_distinct_target = container_path.clone();
    namespace_distinct_target.pop();
    namespace_distinct_target.push(q(NS_B, "Container3"));
    let namespace_distinct_rule = make_mapping_rule(
        SOURCE_A,
        &source_namespaces,
        namespace_distinct_target,
        duplicate_b_path,
        &duplicate_b,
    )
    .unwrap();
    assert_ne!(rule_a.merge_key, namespace_distinct_rule.merge_key);

    let mut address_space = AddressSpace::new();
    let local_b = address_space.register_namespace(NS_B).unwrap();
    let local_types = address_space.register_namespace(NS_TYPES).unwrap();
    let local_a = address_space.register_namespace(NS_A).unwrap();
    assert_eq!((local_b, local_types, local_a), (1, 2, 3));

    let imported_type = NodeId::new(local_types, 500u32);
    assert!(ObjectTypeBuilder::new(
        &imported_type,
        QualifiedName::new(local_types, "FixtureLeafType"),
        "FixtureLeafType",
    )
    .subtype_of(NodeId::new(0, 58u32))
    .insert(&mut address_space));
    assert!(address_space.find_node(&imported_type).is_some());

    let remapped_type = remap_source_node_id_namespace(
        duplicate_a.type_definition.as_ref(),
        &address_space,
        &source_namespaces,
    )
    .expect("source TypeDefinition URI must map to the imported local type namespace");
    assert_eq!(remapped_type, imported_type);

    let structural_ref: NodeId = ReferenceTypeId::HasComponent.into();
    let mut current_path = vec![objects];
    let mut parent_id = NodeId::new(0, OBJECTS_NODE_ID);
    for (name, source_container) in [
        ("Container1", &container_1),
        ("Container2", &container_2),
        ("Container3", &container_3),
    ] {
        current_path.push(q(NS_A, name));
        let created = create_object_node_in_address_space(
            &mut address_space,
            name,
            name,
            &current_path,
            local_a,
            Some(NodeId::new(0, FOLDER_TYPE_ID)),
            structural_ref.clone(),
            source_container.is_forward,
        )
        .unwrap();
        assert!(address_space.has_reference(&parent_id, &created, structural_ref.clone()));
        parent_id = created;
    }

    let nested_target = find_node_by_browse_path(&address_space, &current_path)
        .unwrap()
        .expect("three-level qualified target path must resolve");
    assert_eq!(nested_target, parent_id);

    let mut status_a_target = current_path.clone();
    status_a_target.push(q(NS_A, "Status"));
    let status_a = create_object_node_in_address_space(
        &mut address_space,
        "Status",
        "Status A",
        &status_a_target,
        local_a,
        Some(remapped_type.clone()),
        ReferenceTypeId::HasComponent.into(),
        true,
    )
    .unwrap();

    let mut status_b_target = current_path.clone();
    status_b_target.push(q(NS_B, "Status"));
    let status_b = create_object_node_in_address_space(
        &mut address_space,
        "Status",
        "Status B",
        &status_b_target,
        local_b,
        Some(remapped_type.clone()),
        ReferenceTypeId::HasProperty.into(),
        true,
    )
    .unwrap();

    assert_ne!(status_a, status_b);
    assert_eq!(
        find_node_by_browse_path(&address_space, &status_a_target).unwrap(),
        Some(status_a.clone())
    );
    assert_eq!(
        find_node_by_browse_path(&address_space, &status_b_target).unwrap(),
        Some(status_b.clone())
    );
    assert!(address_space.has_reference(&nested_target, &status_a, ReferenceTypeId::HasComponent));
    assert!(address_space.has_reference(&nested_target, &status_b, ReferenceTypeId::HasProperty));
    assert!(address_space.has_reference(
        &status_a,
        &remapped_type,
        ReferenceTypeId::HasTypeDefinition
    ));
    assert!(address_space.has_reference(
        &status_b,
        &remapped_type,
        ReferenceTypeId::HasTypeDefinition
    ));

    let mut ambiguous_legacy_path = current_path;
    ambiguous_legacy_path.push(RuleBrowsePathElement::Legacy("Status".to_string()));
    assert!(find_node_by_browse_path(&address_space, &ambiguous_legacy_path).is_err());

    // Direction is asserted independently with an inverse source reference.
    let mut inverse_target = status_a_target;
    inverse_target.pop();
    inverse_target.push(q(NS_A, "InverseLeaf"));
    let inverse_leaf = create_object_node_in_address_space(
        &mut address_space,
        "InverseLeaf",
        "InverseLeaf",
        &inverse_target,
        local_a,
        Some(remapped_type),
        ReferenceTypeId::HasNotifier.into(),
        false,
    )
    .unwrap();
    assert!(address_space.has_reference(
        &inverse_leaf,
        &nested_target,
        ReferenceTypeId::HasNotifier
    ));
    assert!(!address_space.has_reference(
        &nested_target,
        &inverse_leaf,
        ReferenceTypeId::HasNotifier
    ));
}
