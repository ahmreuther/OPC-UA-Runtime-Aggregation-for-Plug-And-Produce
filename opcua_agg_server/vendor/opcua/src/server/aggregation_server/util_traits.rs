use std::collections::HashSet;

use crate::{
    client::prelude::{AttributeService, Session, ViewService},
    server::prelude::AddressSpace,
    types::{
        AttributeId, BrowseDescription, BrowseDescriptionResultMask, BrowseDirection,
        NodeClassMask, NodeId, QualifiedName, ReadValueId, ReferenceDescription, ReferenceTypeId,
        StatusCode, UAString, Variant,
    },
};
use tracing::instrument;

use crate::server::aggregation_server::error_types::OpcuaResponseError;

pub trait AddressSpaceAdditions {
    fn delete_rec_hierarchical_refs(
        &mut self,
        node_id: &NodeId,
        delete_target_references: bool,
    ) -> bool;
}

impl AddressSpaceAdditions for AddressSpace {
    #[instrument(level = "trace", ret, skip(self))]
    fn delete_rec_hierarchical_refs(
        &mut self,
        node_id: &NodeId,
        delete_target_references: bool,
    ) -> bool {
        // Delete any children recursively
        if let Some(child_nodes) = self.find_hierarchical_references(node_id) {
            child_nodes.into_iter().for_each(|node_id| {
                let _ = self.delete_rec_hierarchical_refs(&node_id, delete_target_references);
            });
        }
        return self.delete(node_id, delete_target_references);
    }
}

pub trait AttributeServiceAdditions {
    fn read_val(&self, node_id: &NodeId) -> Result<Variant, OpcuaResponseError>;
    fn read_browsename(&self, node_id: &NodeId) -> Result<QualifiedName, OpcuaResponseError>;
    fn read_attr(&self, node_id: &NodeId, attr: AttributeId)
        -> Result<Variant, OpcuaResponseError>;
}

impl AttributeServiceAdditions for Session {
    #[instrument(level = "trace", skip(self), err, ret)]
    fn read_val(&self, node_id: &NodeId) -> Result<Variant, OpcuaResponseError> {
        self.read_attr(node_id, AttributeId::Value)
    }
    #[instrument(level = "trace", skip(self), err, ret)]
    fn read_browsename(&self, node_id: &NodeId) -> Result<QualifiedName, OpcuaResponseError> {
        let datavalues = self.read(
            &[ReadValueId {
                node_id: node_id.clone(),
                attribute_id: AttributeId::BrowseName as u32,
                index_range: UAString::null(),
                data_encoding: QualifiedName::null(),
            }],
            crate::types::TimestampsToReturn::Neither,
            0.0,
        )?;
        if datavalues.len() != 1 {
            let resp_str = format!("{:?}", datavalues);
            return Err(OpcuaResponseError::UnexpectedResponseContent(resp_str));
        };
        let Some(Variant::QualifiedName(browsename)) = datavalues[0].value.clone() else {
            let resp_str = format!("{:?}", datavalues);
            return Err(OpcuaResponseError::UnexpectedResponseContent(resp_str));
        };
        return Ok(QualifiedName::from(*browsename));
    }

    #[instrument(level = "trace", skip(self), err, ret)]
    fn read_attr(
        &self,
        node_id: &NodeId,
        attr: AttributeId,
    ) -> Result<Variant, OpcuaResponseError> {
        let datavalues = self.read(
            &[ReadValueId {
                node_id: node_id.clone(),
                attribute_id: attr as u32,
                index_range: UAString::null(),
                data_encoding: QualifiedName::null(),
            }],
            crate::types::TimestampsToReturn::Neither,
            0.0,
        )?;
        if datavalues.len() == 0 {
            return Ok(Variant::Empty);
        }
        let Some(val) = datavalues[0].value.clone() else {
            let resp_str = format!("{:?}", datavalues);
            return Err(OpcuaResponseError::UnexpectedResponseContent(resp_str));
        };
        return Ok(val);
    }
}

pub trait ViewServiceAdditions {
    fn get_children(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<ReferenceDescription>, OpcuaResponseError>;
    fn get_parents(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<ReferenceDescription>, OpcuaResponseError>;
    fn get_forward_references(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<ReferenceDescription>, OpcuaResponseError>;
    fn get_references(
        &self,
        node_id: &NodeId,
        browse_direction: BrowseDirection,
        reference_type_id: NodeId,
    ) -> Result<Vec<ReferenceDescription>, OpcuaResponseError>;
}

impl ViewServiceAdditions for Session {
    #[instrument(level = "trace", skip(self), err, ret)]
    fn get_children(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<ReferenceDescription>, OpcuaResponseError> {
        self.get_references(
            node_id,
            BrowseDirection::Forward,
            ReferenceTypeId::HierarchicalReferences.into(),
        )
    }

    #[instrument(level = "trace", skip(self), err, ret)]
    fn get_parents(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<ReferenceDescription>, OpcuaResponseError> {
        self.get_references(
            node_id,
            BrowseDirection::Inverse,
            ReferenceTypeId::HierarchicalReferences.into(),
        )
    }

    #[instrument(level = "trace", skip(self), err, ret)]
    fn get_forward_references(
        &self,
        node_id: &NodeId,
    ) -> Result<Vec<ReferenceDescription>, OpcuaResponseError> {
        self.get_references(
            node_id,
            BrowseDirection::Forward,
            ReferenceTypeId::References.into(),
        )
    }

    #[instrument(level = "trace", skip(self), err, ret)]
    fn get_references(
        &self,
        node_id: &NodeId,
        browse_direction: BrowseDirection,
        reference_type_id: NodeId,
    ) -> Result<Vec<ReferenceDescription>, OpcuaResponseError> {
        let bd = BrowseDescription {
            node_id: node_id.clone(),
            browse_direction,
            reference_type_id,
            include_subtypes: true,
            node_class_mask: NodeClassMask::all().bits(),
            result_mask: BrowseDescriptionResultMask::all().bits(),
        };
        let res = self.browse(&[bd])?;
        let Some(br_vec) = res else {
            let resp_str = format!("{:?}", res);
            return Err(OpcuaResponseError::UnexpectedResponseContent(resp_str));
        };
        if br_vec.len() != 1 {
            // Should not happen since only one request has been made
            let resp_str = format!("{:?}", br_vec);
            return Err(OpcuaResponseError::UnexpectedResponseContent(resp_str));
        }
        let mut browse_result = br_vec.into_iter().next().ok_or_else(|| {
            OpcuaResponseError::UnexpectedResponseContent("empty Browse result".to_string())
        })?;
        let mut references = Vec::new();
        let mut seen_continuation_points = HashSet::new();

        loop {
            if !browse_result.status_code.is_good() {
                return Err(browse_result.status_code.into());
            }
            references.extend(browse_result.references.take().unwrap_or_default());

            let continuation_point = browse_result.continuation_point;
            if continuation_point.is_null() {
                break;
            }
            if !seen_continuation_points.insert(continuation_point.clone()) {
                return Err(OpcuaResponseError::UnexpectedResponseContent(
                    "BrowseNext repeated a continuation point".to_string(),
                ));
            }

            let next = self.browse_next(false, &[continuation_point])?;
            let Some(mut next_results) = next else {
                return Err(OpcuaResponseError::UnexpectedResponseContent(
                    "BrowseNext returned no results".to_string(),
                ));
            };
            if next_results.len() != 1 {
                return Err(OpcuaResponseError::UnexpectedResponseContent(format!(
                    "BrowseNext returned {} results for one continuation point",
                    next_results.len()
                )));
            }
            browse_result = next_results.remove(0);
        }

        Ok(references)
    }
}
