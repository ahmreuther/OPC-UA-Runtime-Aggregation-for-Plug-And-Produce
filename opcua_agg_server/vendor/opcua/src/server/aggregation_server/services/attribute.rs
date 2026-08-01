use std::collections::HashMap;
use std::sync::Arc;
use tracing::{instrument, trace, warn};

use crate::{
    client::prelude::{
        AttributeService as _, DataValue, NodeId, QualifiedName, ReadRequest, ReadResponse,
        ReadValueId, ResponseHeader, Session as ClientSession, StatusCode, Variant, WriteRequest,
        WriteResponse, WriteValue,
    },
    sync::RwLock,
};
use crate::{
    core::supported_message::SupportedMessage,
    server::aggregation_server::aggregation_server::AggregationServer,
};

use crate::server::{
    address_space::AddressSpace, services::attribute::AttributeService,
    session::Session as ServerSession,
};

use super::service_delegation::delegate_service_call;
use super::service_delegation::DecidingField;
use super::service_delegation::TransformableItem;

pub(crate) trait AggServerAttributeService {
    fn read_lower_servers(
        &self,
        session_p: Arc<RwLock<ServerSession>>,
        address_space_p: Arc<RwLock<AddressSpace>>,
        aggregation_server: AggregationServer,
        read_request: &ReadRequest,
        nodes_to_read: &Vec<ReadValueId>,
    ) -> SupportedMessage;
    fn write_lower_servers(
        &self,
        session_p: Arc<RwLock<ServerSession>>,
        address_space_p: Arc<RwLock<AddressSpace>>,
        aggregation_server: AggregationServer,
        read_request: &WriteRequest,
        nodes_to_read: &Vec<WriteValue>,
    ) -> SupportedMessage;
}

impl TransformableItem for ReadValueId {
    fn node_ids(&mut self) -> Vec<&mut NodeId> {
        vec![&mut self.node_id]
    }
    fn deciding_field(&self) -> Option<DecidingField> {
        Some(DecidingField::NodeId(&self.node_id))
    }
}

impl TransformableItem for WriteValue {
    fn node_ids(&mut self) -> Vec<&mut NodeId> {
        vec![&mut self.node_id]
    }
    fn deciding_field(&self) -> Option<DecidingField> {
        Some(DecidingField::NodeId(&self.node_id))
    }
}

#[derive(Debug)]
struct AttributeDataValue {
    dv: DataValue,
    attribute_id: u32,
}

impl TransformableItem for AttributeDataValue {
    fn node_ids(&mut self) -> Vec<&mut NodeId> {
        // Don't transform NodeIds that are Variable values
        if self.attribute_id != 13 {
            if let Some(Variant::NodeId(nid)) = &mut self.dv.value {
                return vec![nid.as_mut()];
            }
        }
        Vec::new()
    }
    fn browse_names(&mut self) -> Vec<&mut QualifiedName> {
        if let Some(Variant::QualifiedName(qname)) = &mut self.dv.value {
            vec![qname.as_mut()]
        } else {
            vec![]
        }
    }
}

impl AggServerAttributeService for AttributeService {
    #[instrument(level = "trace", skip_all)]
    fn read_lower_servers(
        &self,
        session_p: Arc<RwLock<ServerSession>>,
        address_space_p: Arc<RwLock<AddressSpace>>,
        aggregation_server: AggregationServer,
        read_request: &ReadRequest,
        nodes_to_read: &Vec<ReadValueId>,
    ) -> SupportedMessage {
        let mut node_map = HashMap::new();
        for read in nodes_to_read {
            // Count all attribute read requests for each node
            *node_map.entry(&read.node_id).or_insert(0) += 1;
        }
        for (node_id, count) in node_map.iter() {
            trace!(
                req_type = "read",
                handle = read_request.request_header.request_handle,
                ?node_id,
                num_of_attributes_to_read = count
            );
        }

        let aggserver_read_call = |reads: &Vec<ReadValueId>| {
            let session = session_p.read();
            let address_space = address_space_p.read();

            let attribute_id_iter = reads.iter().map(|r| r.attribute_id);

            let timestamps_to_return = read_request.timestamps_to_return;
            let res = reads
                .into_iter()
                .map(|node_to_read| {
                    Self::read_node_value(
                        &session,
                        &address_space,
                        node_to_read,
                        read_request.max_age,
                        timestamps_to_return,
                    )
                })
                .zip(attribute_id_iter)
                .map(|(dv, attribute_id)| AttributeDataValue { dv, attribute_id })
                .collect();

            return res;
        };
        let lserver_read_call = |_lserver_id: &u16,
                                 client_session_p: Arc<RwLock<ClientSession>>,
                                 reads: &Vec<ReadValueId>| {
            let attribute_id_iter = reads.iter().map(|r| r.attribute_id);

            let resp = client_session_p.read().read(
                reads,
                read_request.timestamps_to_return,
                read_request.max_age,
            );

            let resp_mapped = resp.map(|v| {
                let zipped = v.into_iter().zip(attribute_id_iter);
                zipped
                    .map(|(mut dv, attribute_id)| {
                        // For compatability with python client: Never return a None value for ArrayDimensions Attribute
                        // Instead, return BadAttributeIdInvalid
                        if attribute_id == 16 {
                            if let Some(Variant::Array(a_box)) = &dv.value {
                                if a_box.values.len() == 0 {
                                    dv.value = None;
                                    dv.status = Some(StatusCode::BadAttributeIdInvalid);
                                }
                            }
                        }
                        Some(AttributeDataValue { dv, attribute_id })
                    })
                    .collect()
            });
            resp_mapped
        };

        let map_db = match aggregation_server.map_db_p.read().connect() {
            Ok(map_db) => map_db,
            Err(e) => {
                warn!("Could not connect to Mapping Database! {:?}", e);
                return ReadResponse {
                    response_header: ResponseHeader::new_service_result(
                        &read_request.request_header,
                        StatusCode::BadUnexpectedError,
                    ),
                    results: None,
                    diagnostic_infos: None,
                }
                .into();
            }
        };

        let res = delegate_service_call(
            &map_db,
            &aggregation_server,
            nodes_to_read,
            false,
            aggserver_read_call,
            lserver_read_call,
        )
        .map(|v| v.into_iter().map(|adv| adv.dv).collect::<Vec<DataValue>>());

        return match res {
            Ok(r) => {
                let good_count = r.iter().filter(|dv| dv.status().is_good()).count();
                let bad_count = r.len() - good_count;
                trace!(
                    req_type = "read",
                    handle = read_request.request_header.request_handle,
                    good_responses = good_count,
                    bad_responses = bad_count,
                );
                ReadResponse {
                    response_header: ResponseHeader::new_good(&read_request.request_header),
                    results: Some(r),
                    diagnostic_infos: None,
                }
                .into()
            }
            Err(sc) => {
                let node_ids: Vec<&NodeId> = node_map.into_keys().collect();
                warn!(
                    req_type = "read",
                    handle = read_request.request_header.request_handle,
                    ?node_ids,
                    "Failed with StatusCode {}",
                    sc.name()
                );
                ReadResponse {
                    response_header: ResponseHeader::new_service_result(
                        &read_request.request_header,
                        sc,
                    ),
                    results: None,
                    diagnostic_infos: None,
                }
                .into()
            }
        };
    }

    #[instrument(level = "trace", skip_all)]
    fn write_lower_servers(
        &self,
        session_p: Arc<RwLock<ServerSession>>,
        address_space_p: Arc<RwLock<AddressSpace>>,
        aggregation_server: AggregationServer,
        write_request: &WriteRequest,
        nodes_to_write: &Vec<WriteValue>,
    ) -> SupportedMessage {
        trace!(
            req_type = "write",
            handle = write_request.request_header.request_handle,
            ?nodes_to_write,
        );

        let aggserver_write_call = |writes: &Vec<WriteValue>| {
            let session = trace_read_lock!(session_p);
            let mut address_space = trace_write_lock!(address_space_p);
            let results = writes
                .iter()
                .map(|write| Self::write_node_value(&session, &mut address_space, write))
                .collect();
            return results;
        };
        let lserver_write_call = |_lserver_id: &u16,
                                  client_session_p: Arc<RwLock<ClientSession>>,
                                  writes: &Vec<WriteValue>| {
            client_session_p.read().write(writes).map(|v| Some(v))
        };

        let map_db = match aggregation_server.map_db_p.read().connect() {
            Ok(map_db) => map_db,
            Err(e) => {
                warn!("Could not connect to Mapping Database! {:?}", e);
                return WriteResponse {
                    response_header: ResponseHeader::new_service_result(
                        &write_request.request_header,
                        StatusCode::BadUnexpectedError,
                    ),
                    results: None,
                    diagnostic_infos: None,
                }
                .into();
            }
        };

        let res = delegate_service_call(
            &map_db,
            &aggregation_server,
            nodes_to_write,
            false,
            aggserver_write_call,
            lserver_write_call,
        );
        return match res {
            Ok(r) => {
                let good_count = r.iter().filter(|dv| dv.status().is_good()).count();
                let bad_count = r.len() - good_count;
                trace!(
                    req_type = "write",
                    handle = write_request.request_header.request_handle,
                    good_responses = good_count,
                    bad_responses = bad_count,
                );
                WriteResponse {
                    response_header: ResponseHeader::new_good(&write_request.request_header),
                    results: Some(r),
                    diagnostic_infos: None,
                }
                .into()
            }
            Err(sc) => {
                warn!(
                    req_type = "write",
                    handle = write_request.request_header.request_handle,
                    ?nodes_to_write,
                    "Failed with StatusCode {}",
                    sc.name()
                );
                WriteResponse {
                    response_header: ResponseHeader::new_service_result(
                        &write_request.request_header,
                        sc,
                    ),
                    results: None,
                    diagnostic_infos: None,
                }
                .into()
            }
        };
    }
}
