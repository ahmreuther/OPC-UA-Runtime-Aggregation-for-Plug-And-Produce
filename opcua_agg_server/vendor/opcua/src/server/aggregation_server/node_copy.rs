use std::{array, sync::Arc};

use crate::{
    client::prelude::{AttributeService, Session},
    server::{
        address_space::{node::Node, types::Method},
        prelude::{
            AddressSpace, DataType, EventNotifier, Object, ObjectType, ReferenceType, Variable,
            VariableType, View,
        },
    },
    sync::RwLock,
    types::{
        AttributeId, DataValue, NodeClass, NodeId, QualifiedName, ReadValueId,
        ReferenceDescription, UAString,
    },
};
use tracing::{error, instrument, warn};

use crate::server::aggregation_server::{
    error_types::{NodeCopyError, OpcuaResponseError},
    utils::{ref_dir, ref_dir_reverse},
};

#[instrument(level = "debug", err, ret, skip(session_p, address_space_p))]
pub fn copy_node(
    session_p: &Arc<RwLock<Session>>,
    address_space_p: &Arc<RwLock<AddressSpace>>,
    rdesc: &ReferenceDescription,
    og_node: &NodeId,
    dest_parent: &NodeId,
    dest_node: &NodeId,
    dest_browsename: QualifiedName,
) -> Result<(), NodeCopyError> {
    if og_node.namespace == 0 {
        error!(msg = "Trying to copy ns0 node");
        return Err(NodeCopyError::NSZeroCopy);
    }

    // requests for all attributeids from 4 to 27
    let reqs: [ReadValueId; 24] = array::from_fn(|i| i + 4).map(|id| ReadValueId {
        node_id: og_node.clone(),
        attribute_id: id as u32,
        index_range: UAString::null(),
        data_encoding: QualifiedName::null(),
    });

    let datavalues = session_p
        .read()
        .read(&reqs, crate::types::TimestampsToReturn::Neither, 0.0)
        .map_err(OpcuaResponseError::from)?;

    let refdir = ref_dir(rdesc.is_forward);
    let refdir_inv = ref_dir_reverse(refdir);
    let parent_ref = (dest_parent, &rdesc.reference_type_id, refdir_inv);

    let success: bool;
    match rdesc.node_class {
        NodeClass::Unspecified => {
            warn!(
                message = "Trying to copy a node with unspecifiend nodeclass. Skipping it, \
                             but the aggregated AddressSpace may be corrupted or not complete \
                             as a result. Please investigate root cause."
            );
            return Ok(());
        }
        NodeClass::Object => {
            let mut obj = Object::new(
                &dest_node,
                dest_browsename,
                "",
                EventNotifier::SUBSCRIBE_TO_EVENTS,
            );

            init_node_from_attr(datavalues, &mut obj)?;
            success = address_space_p.write().insert(obj, Some(&[parent_ref]));
        }
        NodeClass::Variable => {
            let mut var = Variable::new(&dest_node, dest_browsename, "", 0);

            init_node_from_attr(datavalues, &mut var)?;
            success = address_space_p.write().insert(var, Some(&[parent_ref]));
        }
        NodeClass::Method => {
            let mut meth = Method::new(&dest_node, dest_browsename, "", false, false);

            init_node_from_attr(datavalues, &mut meth)?;
            success = address_space_p.write().insert(meth, Some(&[parent_ref]));
        }
        NodeClass::ObjectType => {
            let mut objt = ObjectType::new(&dest_node, dest_browsename, "", true);

            init_node_from_attr(datavalues, &mut objt)?;
            success = address_space_p.write().insert(objt, Some(&[parent_ref]));
        }
        NodeClass::VariableType => {
            let mut vart =
                VariableType::new(&dest_node, dest_browsename, "", NodeId::null(), true, 0);

            init_node_from_attr(datavalues, &mut vart)?;
            success = address_space_p.write().insert(vart, Some(&[parent_ref]));
        }
        NodeClass::ReferenceType => {
            let mut reft = ReferenceType::new(&dest_node, dest_browsename, "", None, true, true);

            init_node_from_attr(datavalues, &mut reft)?;
            success = address_space_p.write().insert(reft, Some(&[parent_ref]));
        }
        NodeClass::DataType => {
            let mut datat = DataType::new(&dest_node, dest_browsename, "", true);

            init_node_from_attr(datavalues, &mut datat)?;
            success = address_space_p.write().insert(datat, Some(&[parent_ref]));
        }
        NodeClass::View => {
            let mut view = View::new(
                &dest_node,
                dest_browsename,
                "",
                EventNotifier::SUBSCRIBE_TO_EVENTS,
                false,
            );

            init_node_from_attr(datavalues, &mut view)?;
            success = address_space_p.write().insert(view, Some(&[parent_ref]));
        }
    }
    if success {
        Ok(())
    } else {
        warn!(
            "Failed to copy node. Continuing, but may result in corrupted or incomplete \
               AddressSpace of aggregated server or other problems."
        );
        Ok(())
    }
}

#[instrument(level = "trace", err, ret, skip_all)]
fn init_node_from_attr<T>(attrs: Vec<DataValue>, node: &mut T) -> Result<(), NodeCopyError>
where
    T: Node,
{
    for (i, dv) in attrs.iter().enumerate() {
        if !dv.is_valid() {
            continue;
        }
        let Some(var) = &dv.value else {
            continue;
        };
        let attr_id = i + 4;
        if let Err(err) = node.set_attribute(AttributeId::from_u32(attr_id as u32)?, var.clone()) {
            warn!(msg = "Error setting node attribute: ", attr_id=attr_id, node_id=?node.node_id(), err=?err);
        }
    }
    return Ok(());
}
