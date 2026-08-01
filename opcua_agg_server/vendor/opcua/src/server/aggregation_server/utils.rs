use std::sync::Arc;

use crate::server::address_space::node::{NodeBase, NodeType};
use crate::server::prelude::{AddressSpace, ReferenceDirection};
use crate::sync::RwLock;
use crate::types::NodeClass;
use crate::types::NodeId;
use crate::types::QualifiedName;
use tracing::{debug, instrument};

use crate::server::aggregation_server::error_types::InvalidNodeClass;

/// PANICS!
#[instrument(skip(address_space_p), level = "debug")]
pub fn insert_reference(
    address_space_p: &Arc<RwLock<AddressSpace>>,
    dest_parent: &NodeId,
    dest_node: &NodeId,
    dest_refid: &NodeId,
    is_forward: bool,
) {
    // PANICS!
    if is_forward {
        address_space_p
            .write()
            .insert_reference(&dest_parent, &dest_node, dest_refid);
    } else {
        address_space_p
            .write()
            .insert_reference(&dest_node, &dest_parent, dest_refid);
    }
    debug!(message = "Aggregated reference.", dest_parent = ?dest_parent, dest_node = ?dest_node);
}

#[instrument(level = "trace", err, ret)]
pub fn int_to_nodeclass(i: i32) -> Result<NodeClass, InvalidNodeClass> {
    match i {
        0 => Ok(NodeClass::Unspecified),
        1 => Ok(NodeClass::Object),
        2 => Ok(NodeClass::Variable),
        4 => Ok(NodeClass::Method),
        8 => Ok(NodeClass::ObjectType),
        16 => Ok(NodeClass::VariableType),
        32 => Ok(NodeClass::ReferenceType),
        64 => Ok(NodeClass::DataType),
        128 => Ok(NodeClass::View),
        _ => Err(InvalidNodeClass(i)),
    }
}

pub fn ref_dir(is_forward: bool) -> ReferenceDirection {
    if is_forward {
        ReferenceDirection::Forward
    } else {
        ReferenceDirection::Inverse
    }
}

pub fn ref_dir_reverse(refdir: ReferenceDirection) -> ReferenceDirection {
    match refdir {
        ReferenceDirection::Forward => ReferenceDirection::Inverse,
        ReferenceDirection::Inverse => ReferenceDirection::Forward,
    }
}

pub fn is_type_node(node_class: &NodeClass) -> bool {
    if [
        NodeClass::ObjectType,
        NodeClass::VariableType,
        NodeClass::ReferenceType,
        NodeClass::DataType,
    ]
    .contains(node_class)
    {
        return true;
    }
    return false;
}

#[instrument(level = "trace", ret)]
pub fn append_postfix_to_uri(uri: &String, postfix: &String) -> String {
    if uri.starts_with("urn:") {
        if uri.ends_with(":") {
            uri.clone() + postfix
        } else {
            uri.clone() + ":" + postfix
        }
    } else {
        if uri.ends_with("/") {
            uri.clone() + postfix
        } else {
            uri.clone() + "/" + postfix
        }
    }
}

pub fn get_browsename(nt: &NodeType) -> QualifiedName {
    match nt {
        NodeType::Object(n) => n.browse_name(),
        NodeType::ObjectType(n) => n.browse_name(),
        NodeType::ReferenceType(n) => n.browse_name(),
        NodeType::Variable(n) => n.browse_name(),
        NodeType::VariableType(n) => n.browse_name(),
        NodeType::View(n) => n.browse_name(),
        NodeType::DataType(n) => n.browse_name(),
        NodeType::Method(n) => n.browse_name(),
    }
}
