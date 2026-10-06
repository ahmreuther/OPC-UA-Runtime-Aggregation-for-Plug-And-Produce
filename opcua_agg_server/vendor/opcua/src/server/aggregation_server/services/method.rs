use std::sync::Arc;

use crate::{
    client::prelude::{
        CallMethodRequest, CallMethodResult, CallRequest, CallResponse, MethodService as _, NodeId,
        ResponseHeader, Session as ClientSession, StatusCode, SupportedMessage,
    },
    prelude::AddressSpace,
    server::{
        aggregation_server::aggregation_server::AggregationServer, services::method::MethodService,
        session::SessionManager, state::ServerState,
    },
    sync::RwLock,
};

use super::service_delegation::{
    delegate_service_call, DecidingField, ServiceResultItem, TransformableItem,
};

impl TransformableItem for CallMethodRequest {
    fn node_ids(&mut self) -> Vec<&mut NodeId> {
        vec![&mut self.object_id, &mut self.method_id]
    }
    fn deciding_field(&self) -> Option<DecidingField> {
        Some(DecidingField::NodeId(&self.method_id))
    }
}

impl TransformableItem for CallMethodResult {}
impl ServiceResultItem for CallMethodResult {
    fn from_status_code(status_code: StatusCode) -> Self {
        Self {
            status_code,
            input_argument_results: None,
            input_argument_diagnostic_infos: None,
            output_arguments: None,
        }
    }
}

pub(crate) trait AggServerMethodService {
    fn call_lower_servers(
        &self,
        session_id: &NodeId,
        session_manager: Arc<RwLock<SessionManager>>,
        aggregation_server: AggregationServer,
        server_state: &ServerState,
        address_space_p: Arc<RwLock<AddressSpace>>,
        request: &CallRequest,
        calls: &Vec<CallMethodRequest>,
    ) -> SupportedMessage;
}

impl AggServerMethodService for MethodService {
    fn call_lower_servers(
        &self,
        session_id: &NodeId,
        session_manager: Arc<RwLock<SessionManager>>,
        aggregation_server: AggregationServer,
        server_state: &ServerState,
        address_space_p: Arc<RwLock<AddressSpace>>,
        request: &CallRequest,
        calls: &Vec<CallMethodRequest>,
    ) -> SupportedMessage {
        let aggserver_service_call = |calls: &Vec<CallMethodRequest>| {
            let mut address_space = address_space_p.write();

            let results: Vec<CallMethodResult> = calls
                .iter()
                .map(|request| {
                    trace!(
                        "Calling to {:?} on {:?}",
                        request.method_id,
                        request.object_id
                    );

                    // Note: Method invocations that modify the address space, write a value, or modify the
                    // state of the system (acknowledge, batch sequencing or other system changes) must
                    // generate an AuditUpdateMethodEventType or a subtype of it.

                    // Call the method via whatever is registered in the address space
                    match address_space.call_method(
                        &server_state,
                        session_id,
                        session_manager.clone(),
                        request,
                    ) {
                        Ok(response) => response,
                        Err(status_code) => {
                            // Call didn't work for some reason
                            error!(
                                "Call to {:?} on {:?} failed with status code {}",
                                request.method_id, request.object_id, status_code
                            );
                            CallMethodResult {
                                status_code,
                                input_argument_results: None,
                                input_argument_diagnostic_infos: None,
                                output_arguments: None,
                            }
                        }
                    }
                })
                .collect();

            drop(address_space);

            results
        };

        let lserver_service_call =
            |_lserver_id: &u16,
             client_session_p: Arc<RwLock<ClientSession>>,
             calls: &Vec<CallMethodRequest>| {
                if calls.is_empty() {
                    return Ok(None);
                }
                let client = client_session_p.read();
                let mut responses = Vec::new();
                for call in calls {
                    let resp = client.call(call.clone());
                    match resp {
                        Ok(val) => responses.push(val),
                        Err(sc) => responses.push(CallMethodResult::from_status_code(sc)),
                    }
                }
                return Ok(Some(responses));
            };

        let map_db = match aggregation_server.map_db_p.read().connect() {
            Ok(map_db) => map_db,
            Err(e) => {
                warn!("Could not connect to Mapping Database! {:?}", e);
                return CallResponse {
                    response_header: ResponseHeader::new_service_result(
                        &request.request_header,
                        StatusCode::BadInternalError,
                    ),
                    results: None,
                    diagnostic_infos: None,
                }
                .into();
            }
        };

        let results = delegate_service_call(
            &map_db,
            &aggregation_server,
            calls,
            true,
            aggserver_service_call,
            lserver_service_call,
        );

        return match results {
            Ok(r) => CallResponse {
                response_header: ResponseHeader::new_good(&request.request_header),
                results: Some(r),
                diagnostic_infos: None,
            }
            .into(),
            Err(sc) => CallResponse {
                response_header: ResponseHeader::new_service_result(&request.request_header, sc),
                results: None,
                diagnostic_infos: None,
            }
            .into(),
        };
    }
}
