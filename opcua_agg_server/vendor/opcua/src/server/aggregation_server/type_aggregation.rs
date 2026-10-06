use std::collections::hash_map::DefaultHasher;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::client::prelude::{AttributeService, Session};
use crate::server::prelude::AddressSpace;
use crate::sync::RwLock;
use crate::types::node_ids::ReferenceTypeId;
use crate::types::{
    AttributeId, Identifier, NodeId, ObjectId, QualifiedName, ReadValueId, ReferenceDescription,
    UAString,
};
use bimap::BiMap;
use tracing::{debug, instrument, warn};

use crate::server::aggregation_server::error_types::{
    NodeCopyError, OpcuaResponseError, TypeAggregationError,
};
use crate::server::aggregation_server::map_db::MapDatabaseConnection;
use crate::server::aggregation_server::node_copy::copy_node;
use crate::server::aggregation_server::util_traits::{
    AttributeServiceAdditions, ViewServiceAdditions,
};
use crate::server::aggregation_server::util_types::{LowerServer, VariantWrapper};
use crate::server::aggregation_server::utils::is_type_node;

use super::util_types::StandardizedNamespace;

const IDENTIFYING_ATTRIBUTES: [AttributeId; 8] = [
    AttributeId::AccessLevel,
    // AttributeId::ArrayDimensions,
    AttributeId::ContainsNoLoops,
    // AttributeId::DataType,
    // AttributeId::DataTypeDefinition,
    AttributeId::EventNotifier,
    AttributeId::Executable,
    // AttributeId::Historizing,
    AttributeId::IsAbstract,
    AttributeId::NodeClass,
    AttributeId::Symmetric,
    // AttributeId::UserAccessLevel,
    // AttributeId::UserExecutable,
    // AttributeId::UserWriteMask,
    // AttributeId::Value,
    AttributeId::ValueRank,
    // AttributeId::WriteMask,
];

#[instrument(level = "info", err, ret, skip_all, fields(name = lserver.name))]
pub fn aggregate_types(
    address_space_p: &Arc<RwLock<AddressSpace>>,
    session_p: &Arc<RwLock<Session>>,
    global_type_hashmap_p: &Arc<RwLock<BiMap<u64, NodeId>>>,
    map_db: &MapDatabaseConnection,
    standard_namespaces: &mut Vec<StandardizedNamespace>,
    lserver: &mut LowerServer,
) -> Result<(), TypeAggregationError> {
    lserver.check_operation()?;
    // Type Aggregation Process:
    // 1. Traverse all ChildNodes (HasSubType) starting at the Type Folder.
    // 2. When reching a type node that is not namespace 0 hash it (see 'Hashing').
    // 3. Copy nodes with unique hash into the aggregation server

    let mut queue: VecDeque<(NodeId, ReferenceDescription)> = VecDeque::with_capacity(50);

    let types_folder_nid: NodeId = ObjectId::TypesFolder.into();
    let rdescs = session_p.read().get_children(&types_folder_nid)?;
    for rdesc in rdescs {
        if rdesc.reference_type_id == ReferenceTypeId::HasSubtype.into()
            || rdesc.reference_type_id == ReferenceTypeId::Organizes.into()
        {
            if rdesc.node_id.node_id.identifier == Identifier::from(3048)
                || rdesc.node_id.node_id.identifier == Identifier::from(17708)
            {
                // Disregard EventTypes and InterfaceTypes since they are ObjectTypes
                // and thus already reachable through ObjectTypes
                continue;
            }
            queue.push_back((types_folder_nid.clone(), rdesc));
        }
    }
    // Hash only one type at a time to avoid race conditions
    let mut global_type_hashmap = global_type_hashmap_p.write();
    let mut visited = HashSet::new();
    while let Some((og_parent, rdesc)) = queue.pop_front() {
        lserver.check_operation()?;
        let og_node = rdesc.node_id.node_id.clone();
        if !visited.insert(og_node.clone()) {
            continue;
        }
        debug!(message = "Aggregating type", source_node = ?og_node);
        let ns_url = lserver
            .namespace_array
            .get(og_node.namespace as usize)
            .ok_or_else(|| TypeAggregationError::InvalidTypeGraph(og_node.clone()))?;
        let aggserver_type = if let Some(ns) = standard_namespaces
            .iter()
            .find(|n| (&n.url == ns_url) && n.nsid.is_some())
        {
            let mut aggserver_type = og_node.clone();
            aggserver_type.namespace = ns.nsid.ok_or(TypeAggregationError::ProgrammingError)?; // We already check that its there
            aggserver_type
        } else if og_node.namespace != 0 {
            let hash = hash_node(
                session_p,
                map_db,
                &mut global_type_hashmap,
                lserver,
                Some(&og_parent),
                &og_node,
                true,
            )?;
            let aggserver_type = if global_type_hashmap.contains_left(&hash) {
                let aggserver_type = global_type_hashmap
                    .get_by_left(&hash)
                    .ok_or(TypeAggregationError::ProgrammingError)?;
                aggserver_type.clone()
            } else {
                let aggserver_type = copy_type(
                    address_space_p,
                    session_p,
                    map_db,
                    lserver,
                    og_parent,
                    rdesc,
                )?;
                global_type_hashmap.insert(hash, aggserver_type.clone());
                aggserver_type
            };
            aggserver_type
        } else {
            og_node.clone()
        };

        if og_node.namespace != 0 {
            if let Err(e) = map_db.insert_type(lserver.id, &og_node, &aggserver_type) {
                warn!(
                    "Error {:?} while iterating over: {:?} -> {:?}",
                    &e, &og_node, &aggserver_type
                );
            }
        }

        let new_rdescs = session_p.read().get_children(&og_node)?;
        for new_rdesc in new_rdescs {
            if new_rdesc.reference_type_id == ReferenceTypeId::HasSubtype.into()
                || new_rdesc.reference_type_id == ReferenceTypeId::Organizes.into()
            {
                queue.push_back((og_node.clone(), new_rdesc));
            }
        }
    }
    drop(global_type_hashmap);
    for (nsid, url) in lserver.namespace_array.iter().enumerate() {
        lserver.check_operation()?;
        for stand_ns in &mut *standard_namespaces {
            if &stand_ns.url == url {
                if let Some(nsid_aggs) = stand_ns.nsid {
                    map_db.add_aggserver_nsid_types(lserver.id, nsid as u16, nsid_aggs)?;
                } else {
                    let nsid_aggs = map_db.get_aggserver_nsid_inst(lserver.id, nsid as u16)?;
                    map_db.add_aggserver_nsid_types(lserver.id, nsid as u16, nsid_aggs)?;
                    stand_ns.nsid = Some(nsid_aggs);
                }
            }
        }
    }
    return Ok(());
}

#[instrument(
    level = "debug",
    err,
    ret,
    skip(session_p, global_type_hashmap, lserver, map_db)
)]
pub fn hash_node(
    session_p: &Arc<RwLock<Session>>,
    map_db: &MapDatabaseConnection,
    global_type_hashmap: &mut BiMap<u64, NodeId>,
    lserver: &mut LowerServer,
    og_parent_opt: Option<&NodeId>,
    og_node: &NodeId,
    is_type: bool,
) -> Result<u64, TypeAggregationError> {
    hash_node_inner(
        session_p,
        map_db,
        global_type_hashmap,
        lserver,
        og_parent_opt,
        og_node,
        is_type,
        &mut HashSet::new(),
    )
}

fn hash_node_inner(
    session_p: &Arc<RwLock<Session>>,
    map_db: &MapDatabaseConnection,
    global_type_hashmap: &mut BiMap<u64, NodeId>,
    lserver: &mut LowerServer,
    og_parent_opt: Option<&NodeId>,
    og_node: &NodeId,
    is_type: bool,
    active: &mut HashSet<NodeId>,
) -> Result<u64, TypeAggregationError> {
    lserver.check_operation()?;
    if active.len() >= 128 || !active.insert(og_node.clone()) {
        return Err(TypeAggregationError::InvalidTypeGraph(og_node.clone()));
    }
    // Hashing rules:
    // Characteristics of a type that are used to calculate the hash are the following:
    // - The supertype
    // - Selected Attributes of the type
    // - InstanceDeclarations of the type, meaning the child nodes that define this type
    //   that are not subtypes.

    let session = session_p.read();
    let browsename = session.read_browsename(og_node)?;
    let attrs_to_read = IDENTIFYING_ATTRIBUTES.map(|attr| ReadValueId {
        node_id: og_node.clone(),
        attribute_id: attr as u32,
        index_range: UAString::null(),
        data_encoding: QualifiedName::null(),
    });
    let identifying_attributes = session
        .read(
            &attrs_to_read,
            crate::types::TimestampsToReturn::Neither,
            0.0,
        )
        .map_err(OpcuaResponseError::from)?;
    drop(session);

    // Hash node signature
    let mut hasher = DefaultHasher::new();

    // 1. Hash supertype
    if is_type {
        let og_parent = {
            if let Some(unwrapped) = og_parent_opt {
                unwrapped.clone()
            } else {
                let Some(ref_t_parent) = session_p
                    .read()
                    .get_parents(og_node)?
                    .iter()
                    .filter(|rd| rd.reference_type_id == ReferenceTypeId::HasSubtype.into())
                    .next()
                    .cloned()
                else {
                    return Err(TypeAggregationError::NoSupertype {
                        nodeid: og_node.clone(),
                    });
                };
                ref_t_parent.node_id.node_id
            }
        };

        if og_parent.namespace == 0 {
            og_parent.hash(&mut hasher)
        } else {
            let lserver_parent_opt = map_db.get_aggserver_type(lserver.id, &og_parent)?;
            if let Some(lserver_parent) = lserver_parent_opt {
                if let Some(parent_hash) = global_type_hashmap.get_by_right(&lserver_parent) {
                    parent_hash.hash(&mut hasher);
                } else {
                    lserver_parent.hash(&mut hasher);
                }
            } else {
                info!("No parent of type found.");
            }
        }
    }

    // 2. Hash identifying attributes
    // Hash BrowseName separately to disregard its namespace id
    browsename.name.hash(&mut hasher);
    for attr in identifying_attributes.into_iter() {
        VariantWrapper(attr.value.unwrap_or_default(), 0).hash(&mut hasher);
    }

    // 3. Hash hierarchical references
    let mut hrefs = session_p.read().get_children(og_node)?;
    // Sort references by child browsename
    hrefs.sort_by_key(|r| r.browse_name.name.value().clone().unwrap_or_default());

    for rdesc in hrefs {
        lserver.check_operation()?;
        // 3a. Hash referenced node
        if rdesc.reference_type_id == ReferenceTypeId::HasSubtype.into() {
            // Ignore subtypes because they do not characterize the type itself
            continue;
        }
        if rdesc.node_id.node_id.namespace == 0 {
            // Hash of ns0 nodes are just their nodeid, since they are unique
            rdesc.node_id.node_id.hash(&mut hasher);
        } else if is_type_node(&rdesc.node_class) {
            hash_node_inner(
                session_p,
                map_db,
                global_type_hashmap,
                lserver,
                None,
                &rdesc.node_id.node_id,
                true,
                active,
            )?
            .hash(&mut hasher);
        } else {
            hash_node_inner(
                session_p,
                map_db,
                global_type_hashmap,
                lserver,
                None,
                &rdesc.node_id.node_id,
                false,
                active,
            )?
            .hash(&mut hasher);
        }

        // 3b. Hash referencetype
        if rdesc.reference_type_id.namespace == 0 {
            // Again, hash nodeid if in ns0
            rdesc.reference_type_id.hash(&mut hasher)
        } else {
            rdesc.browse_name.name.hash(&mut hasher);
        }
    }

    // 3. Hash non-hierarchical references
    let mut nrefs = session_p.read().get_references(
        og_node,
        crate::types::BrowseDirection::Forward,
        ReferenceTypeId::NonHierarchicalReferences.into(),
    )?;
    // Sort references by child browsename
    nrefs.sort_by_key(|r| r.browse_name.name.value().clone().unwrap_or_default());

    for rdesc in nrefs {
        lserver.check_operation()?;
        // 3a. Hash referenced node
        if rdesc.node_id.node_id.namespace == 0 {
            // Hash of ns0 nodes are just their nodeid, since they are unique
            rdesc.node_id.node_id.hash(&mut hasher);
        } else {
            rdesc.browse_name.name.hash(&mut hasher);
        }

        // 3b. Hash referencetype
        if rdesc.reference_type_id.namespace == 0 {
            // Again, hash nodeid if in ns0
            rdesc.reference_type_id.hash(&mut hasher)
        } else {
            rdesc.browse_name.name.hash(&mut hasher);
        }
    }

    let node_hash = hasher.finish();
    debug!(message = "Hash calculated.", hash = node_hash);
    active.remove(og_node);
    return Ok(node_hash);
}

#[instrument(
    level = "debug",
    err,
    ret,
    skip(address_space_p, session_p, lserver, map_db)
)]
fn copy_type(
    address_space_p: &Arc<RwLock<AddressSpace>>,
    session_p: &Arc<RwLock<Session>>,
    map_db: &MapDatabaseConnection,
    lserver: &mut LowerServer,
    og_root_parent: NodeId,
    root_ref: ReferenceDescription,
) -> Result<NodeId, TypeAggregationError> {
    let dest_root_parent = determine_dest_nid(map_db, lserver, &og_root_parent)?;

    let mut queue: VecDeque<(NodeId, ReferenceDescription)> = VecDeque::new();
    queue.push_back((dest_root_parent, root_ref));

    let mut new_nodeid: Option<NodeId> = None;
    let mut visited = HashSet::new();

    while let Some((dest_parent, rdesc)) = queue.pop_front() {
        lserver.check_operation()?;
        let og_node = &rdesc.node_id.node_id;
        if !visited.insert(og_node.clone()) {
            continue;
        }
        let og_browsename = session_p.read().read_browsename(og_node)?;

        let dest_refid = determine_dest_nid(map_db, lserver, &rdesc.reference_type_id)?;
        let dest_node = determine_dest_nid(map_db, lserver, og_node)?;

        if new_nodeid.is_none() {
            new_nodeid = Some(dest_node.clone());
        }

        let dest_browsename = QualifiedName {
            namespace_index: dest_node.namespace.clone(),
            name: og_browsename.name,
        };

        let dest_typedef = determine_dest_nid(map_db, lserver, &rdesc.type_definition.node_id)?;

        let dest_ref = ReferenceDescription {
            reference_type_id: dest_refid,
            is_forward: rdesc.is_forward,
            node_id: dest_node.clone().into(),
            browse_name: dest_browsename.clone(),
            display_name: rdesc.display_name,
            node_class: rdesc.node_class,
            type_definition: dest_typedef.into(),
        };

        lserver.check_operation()?;
        if !address_space_p.read().node_exists(&dest_node) {
            lserver.created_nodes.push(dest_node.clone());
        }
        copy_node(
            session_p,
            address_space_p,
            &dest_ref,
            og_node,
            &dest_parent,
            &dest_node,
            dest_browsename,
        )?;

        debug!(message = "Type copied", from = ?og_node, to = ?dest_node);

        let new_refs = session_p.read().get_children(og_node)?;
        for r in new_refs {
            // Copy all children that are not subtypes
            if r.reference_type_id != ReferenceTypeId::HasSubtype.into() {
                // Don´t copy subtypes with this type, they get copied separately
                queue.push_back((dest_node.clone(), r));
            }
        }
    }

    let Some(ret) = new_nodeid else {
        return Err(NodeCopyError::NotCopied.into());
    };
    return Ok(ret);
}

#[instrument(level = "trace", err, ret, skip(lserver, map_db), fields(name = lserver.name))]
fn determine_dest_nid(
    map_db: &MapDatabaseConnection,
    lserver: &LowerServer,
    og_node: &NodeId,
) -> Result<NodeId, TypeAggregationError> {
    if og_node.namespace == 0 {
        return Ok(og_node.clone());
    }
    if let Some(dest_node) = map_db.get_aggserver_type(lserver.id, og_node)? {
        // If it is not ns0 it should be in the type map
        return Ok(dest_node.clone());
    } else {
        let nsid = map_db.get_aggserver_nsid_inst(lserver.id, og_node.namespace)?;
        let new_nodeid = NodeId::new(nsid, og_node.identifier.clone());
        return Ok(new_nodeid);
    }
}
