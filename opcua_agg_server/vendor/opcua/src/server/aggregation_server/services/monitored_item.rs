use crate::client::prelude::{DeleteMonitoredItemsRequest, DeleteMonitoredItemsResponse};
use crate::prelude::{
    AddressSpace, CreateMonitoredItemsResponse, MonitoredItemService as _, NodeId, ResponseHeader,
    Session as ClientSession, SupportedMessage,
};
use crate::server::aggregation_server::aggregation_server::AggregationServer;
use crate::server::aggregation_server::map_db::MapDatabaseConnection;
use crate::server::aggregation_server::services::service_delegation::{
    delegate_service_call, DecidingField, MonitoredItem, ServiceResultItem, TransformableItem,
};
use crate::server::services::monitored_item::MonitoredItemService;
use crate::server::session::Session as ServerSession;
use crate::server::state::ServerState;
use crate::server::subscriptions::subscription::Subscription;
use crate::sync::RwLock;
use crate::types::{
    CreateMonitoredItemsRequest, ExtensionObject, ModifyMonitoredItemsRequest,
    ModifyMonitoredItemsResponse, MonitoredItemCreateRequest, MonitoredItemCreateResult,
    MonitoredItemModifyRequest, MonitoredItemModifyResult, MonitoringParameters,
    SetMonitoringModeRequest, SetMonitoringModeResponse, StatusCode,
};
use std::sync::Arc;
use tracing::instrument;

impl TransformableItem for MonitoredItemCreateRequest {
    fn node_ids(&mut self) -> Vec<&mut NodeId> {
        vec![&mut self.item_to_monitor.node_id]
    }
    fn deciding_field(&self) -> Option<DecidingField> {
        Some(DecidingField::NodeId(&self.item_to_monitor.node_id))
    }
}

impl TransformableItem for MonitoredItemCreateResult {}

impl TransformableItem for MonitoredItem {
    fn monitored_items(&mut self) -> Vec<&mut MonitoredItem> {
        vec![self]
    }
    fn deciding_field(&self) -> Option<DecidingField> {
        Some(DecidingField::MonitoredItem(self))
    }
}

impl TransformableItem for StatusCode {}

impl ServiceResultItem for MonitoredItemCreateResult {
    fn from_status_code(status_code: StatusCode) -> Self {
        Self {
            status_code,
            monitored_item_id: 0,
            revised_sampling_interval: 0.0,
            revised_queue_size: 0,
            filter_result: ExtensionObject::null(),
        }
    }
}

impl TransformableItem for MonitoredItemModifyResult {}

impl ServiceResultItem for MonitoredItemModifyResult {
    fn from_status_code(status_code: StatusCode) -> Self {
        Self {
            status_code,
            revised_sampling_interval: 0.0,
            revised_queue_size: 0,
            filter_result: ExtensionObject::null(),
        }
    }
}

impl ServiceResultItem for StatusCode {
    fn from_status_code(status_code: StatusCode) -> Self {
        status_code
    }
}

#[derive(Clone, Debug)]
struct MonitoredItemOperation<T> {
    item: MonitoredItem,
    parameters: T,
}

impl<T> TransformableItem for MonitoredItemOperation<T> {
    fn deciding_field(&self) -> Option<DecidingField> {
        Some(DecidingField::MonitoredItem(&self.item))
    }

    // Keep upper IDs during partitioning. Resolve source IDs only after the
    // source session is locked so reconnect cannot invalidate them before use.
}

fn call_source_monitored_items<T, R, F>(
    map_db: &MapDatabaseConnection,
    source_id: u16,
    subscription_id: u32,
    items: &[MonitoredItemOperation<T>],
    call: F,
) -> Vec<R>
where
    T: Clone,
    R: ServiceResultItem,
    F: FnOnce(u32, &[(u32, T)]) -> Result<Vec<R>, StatusCode>,
{
    let source_sub = match map_db.get_lserver_sub_id(source_id, subscription_id) {
        Ok(source_sub) => source_sub,
        Err(error) => {
            warn!("Cannot resolve source subscription: {:?}", error);
            return items
                .iter()
                .map(|_| R::from_status_code(StatusCode::BadSubscriptionIdInvalid))
                .collect();
        }
    };
    let mut results: Vec<Option<R>> = items.iter().map(|_| None).collect();
    let mut source_items = Vec::new();
    let mut indices = Vec::new();
    for (index, item) in items.iter().enumerate() {
        match map_db.get_lserver_monitored_item(subscription_id, item.item.mitem_id) {
            Ok(Some(source_item_id)) => {
                indices.push(index);
                source_items.push((source_item_id, item.parameters.clone()));
            }
            Ok(None) => {
                results[index] = Some(R::from_status_code(StatusCode::BadMonitoredItemIdInvalid))
            }
            Err(error) => {
                warn!("Cannot resolve source monitored item: {:?}", error);
                results[index] = Some(R::from_status_code(StatusCode::BadInternalError));
            }
        }
    }
    if !source_items.is_empty() {
        let source_results = match call(source_sub.lserver_sub_id, &source_items) {
            Ok(values) if values.len() == source_items.len() => values,
            Ok(_) => source_items
                .iter()
                .map(|_| R::from_status_code(StatusCode::BadUnexpectedError))
                .collect(),
            Err(status) => source_items
                .iter()
                .map(|_| R::from_status_code(status))
                .collect(),
        };
        for (index, result) in indices.into_iter().zip(source_results) {
            results[index] = Some(result);
        }
    }
    results
        .into_iter()
        .map(|value| value.unwrap_or_else(|| R::from_status_code(StatusCode::BadUnexpectedError)))
        .collect()
}

fn map_created_monitored_items(
    map_db: &MapDatabaseConnection,
    internal_sub_id: i64,
    subscription: &mut Subscription,
    results: &mut [MonitoredItemCreateResult],
) -> Vec<u32> {
    let mut unmapped_source_items = Vec::new();
    for result in results {
        // Failed source operations have no item to map; their zero IDs must not
        // consume aggregation IDs or create phantom mappings.
        if !result.status_code.is_good() {
            continue;
        }
        let source_id = result.monitored_item_id;
        let aggregation_id = subscription.take_next_monitored_item_id();
        match map_db.insert_monitored_item(internal_sub_id, source_id, aggregation_id) {
            Ok(()) => result.monitored_item_id = aggregation_id,
            Err(error) => {
                warn!("Cannot insert into monitored item map: {:?}", error);
                result.status_code = StatusCode::BadInternalError;
                result.monitored_item_id = 0;
                unmapped_source_items.push(source_id);
            }
        }
    }
    unmapped_source_items
}

pub(crate) trait AggServerMonitoredItemsService {
    fn create_monitored_items_lservers(
        &self,
        request: &CreateMonitoredItemsRequest,
        aggregation_server: &AggregationServer,
        server_session_p: Arc<RwLock<ServerSession>>,
        aggserver_sub_id: u32,
        server_state: &ServerState,
        address_space: &AddressSpace,
        items_to_create: &Vec<MonitoredItemCreateRequest>,
    ) -> SupportedMessage;
    fn modify_monitored_items_lservers(
        &self,
        request: &ModifyMonitoredItemsRequest,
        aggregation_server: &AggregationServer,
        server_session_p: Arc<RwLock<ServerSession>>,
        server_state: Arc<RwLock<ServerState>>,
        address_space: Arc<RwLock<AddressSpace>>,
    ) -> SupportedMessage;
    fn set_monitoring_mode_lservers(
        &self,
        request: &SetMonitoringModeRequest,
        aggregation_server: &AggregationServer,
        server_session_p: Arc<RwLock<ServerSession>>,
    ) -> SupportedMessage;
    fn delete_monitored_items_lservers(
        &self,
        request: &DeleteMonitoredItemsRequest,
        aggregation_server: &AggregationServer,
        server_session_p: Arc<RwLock<ServerSession>>,
        aggserver_sub_id: u32,
    ) -> SupportedMessage;
}

impl AggServerMonitoredItemsService for MonitoredItemService {
    #[instrument(
        level = "debug",
        skip(
            self,
            server_session_p,
            address_space,
            server_state,
            aggregation_server
        )
    )]
    fn create_monitored_items_lservers(
        &self,
        request: &CreateMonitoredItemsRequest,
        aggregation_server: &AggregationServer,
        server_session_p: Arc<RwLock<ServerSession>>,
        aggserver_sub_id: u32,
        server_state: &ServerState,
        address_space: &AddressSpace,
        items_to_create: &Vec<MonitoredItemCreateRequest>,
    ) -> SupportedMessage {
        let aggserver_service_call = |items: &Vec<MonitoredItemCreateRequest>| {
            let now = chrono::Utc::now();
            let mut session = server_session_p.write();
            let Some(subscription) = session.subscriptions_mut().get_mut(request.subscription_id)
            else {
                error!("Could not get subscription to create monitored item.");
                return Vec::new();
            };
            let results = Some(subscription.create_monitored_items(
                server_state,
                address_space,
                &now,
                request.timestamps_to_return,
                items,
            ));
            drop(session);
            let Some(r) = results else {
                return Vec::new();
            };
            return r;
        };

        let map_db = match aggregation_server.map_db_p.read().connect() {
            Ok(map_db) => map_db,
            Err(e) => {
                tracing::warn!("Could not connect to Mapping Database! {:?}", e);
                return CreateMonitoredItemsResponse {
                    response_header: ResponseHeader::new_service_result(
                        &request.request_header,
                        StatusCode::BadUnexpectedError,
                    ),
                    results: None,
                    diagnostic_infos: None,
                }
                .into();
            }
        };

        let lserver_service_call =
            |lserver_id: &u16,
             client_session_p: Arc<RwLock<ClientSession>>,
             items: &Vec<MonitoredItemCreateRequest>| {
                let sub = map_db
                    .get_lserver_sub_id(lserver_id.clone(), aggserver_sub_id)
                    .map_err(|e| {
                        warn!("Subscription not found: {:?}", e);
                        StatusCode::BadNotFound
                    })?;

                let res = client_session_p.read().create_monitored_items(
                    sub.lserver_sub_id,
                    request.timestamps_to_return,
                    items,
                );

                let mut session = server_session_p.write();
                let Some(subscription) =
                    session.subscriptions_mut().get_mut(request.subscription_id)
                else {
                    warn!("Could not get subscription to create monitored item.");
                    return Err(StatusCode::BadNotFound);
                };

                let mut results = res?;
                let unmapped_source_items = map_created_monitored_items(
                    &map_db,
                    sub.internal_sub_id,
                    subscription,
                    &mut results,
                );
                drop(session);
                if !unmapped_source_items.is_empty() {
                    if let Err(error) = client_session_p
                        .read()
                        .delete_monitored_items(sub.lserver_sub_id, &unmapped_source_items)
                    {
                        warn!(
                            "Cannot clean up unmapped source monitored items: {:?}",
                            error
                        );
                    }
                }
                Ok(Some(results))
            };

        let results = delegate_service_call(
            &map_db,
            aggregation_server,
            items_to_create,
            true,
            aggserver_service_call,
            lserver_service_call,
        );
        match results {
            Ok(r) => CreateMonitoredItemsResponse {
                response_header: ResponseHeader::new_good(&request.request_header),
                results: Some(r),
                diagnostic_infos: None,
            }
            .into(),
            Err(sc) => CreateMonitoredItemsResponse {
                response_header: ResponseHeader::new_service_result(&request.request_header, sc),
                results: None,
                diagnostic_infos: None,
            }
            .into(),
        }
    }

    fn modify_monitored_items_lservers(
        &self,
        request: &ModifyMonitoredItemsRequest,
        aggregation_server: &AggregationServer,
        server_session_p: Arc<RwLock<ServerSession>>,
        server_state: Arc<RwLock<ServerState>>,
        address_space: Arc<RwLock<AddressSpace>>,
    ) -> SupportedMessage {
        let results = (|| {
            let map_db = aggregation_server
                .map_db_p
                .read()
                .connect()
                .map_err(|error| {
                    warn!(
                        "Cannot connect to monitored-item mapping database: {:?}",
                        error
                    );
                    StatusCode::BadInternalError
                })?;
            let items: Vec<_> = request
                .items_to_modify
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|item| MonitoredItemOperation {
                    item: MonitoredItem {
                        sub_id: request.subscription_id,
                        mitem_id: item.monitored_item_id,
                    },
                    parameters: item.requested_parameters.clone(),
                })
                .collect();
            delegate_service_call(
                &map_db,
                aggregation_server,
                &items,
                false,
                |items| {
                    let state = server_state.read();
                    let mut session = server_session_p.write();
                    let address_space = address_space.read();
                    let Some(subscription) =
                        session.subscriptions_mut().get_mut(request.subscription_id)
                    else {
                        return items
                            .iter()
                            .map(|_| {
                                MonitoredItemModifyResult::from_status_code(
                                    StatusCode::BadSubscriptionIdInvalid,
                                )
                            })
                            .collect();
                    };
                    let local_items: Vec<_> = items
                        .iter()
                        .map(|item| MonitoredItemModifyRequest {
                            monitored_item_id: item.item.mitem_id,
                            requested_parameters: item.parameters.clone(),
                        })
                        .collect();
                    subscription.modify_monitored_items(
                        &state,
                        &address_space,
                        request.timestamps_to_return,
                        &local_items,
                    )
                },
                |source_id, source_session, items| {
                    let source_session = source_session.read();
                    Ok(Some(call_source_monitored_items(
                        &map_db,
                        *source_id,
                        request.subscription_id,
                        items,
                        |source_sub, source_items| {
                            let source_items: Vec<_> = source_items
                                .iter()
                                .map(|(id, parameters)| MonitoredItemModifyRequest {
                                    monitored_item_id: *id,
                                    requested_parameters: parameters.clone(),
                                })
                                .collect();
                            source_session.modify_monitored_items(
                                source_sub,
                                request.timestamps_to_return,
                                &source_items,
                            )
                        },
                    )))
                },
            )
        })();
        let (status, results) = match results {
            Ok(results) => (StatusCode::Good, Some(results)),
            Err(status) => (status, None),
        };
        ModifyMonitoredItemsResponse {
            response_header: ResponseHeader::new_service_result(&request.request_header, status),
            results,
            diagnostic_infos: None,
        }
        .into()
    }

    fn set_monitoring_mode_lservers(
        &self,
        request: &SetMonitoringModeRequest,
        aggregation_server: &AggregationServer,
        server_session_p: Arc<RwLock<ServerSession>>,
    ) -> SupportedMessage {
        let results = (|| {
            let map_db = aggregation_server
                .map_db_p
                .read()
                .connect()
                .map_err(|error| {
                    warn!(
                        "Cannot connect to monitored-item mapping database: {:?}",
                        error
                    );
                    StatusCode::BadInternalError
                })?;
            let items: Vec<_> = request
                .monitored_item_ids
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|id| MonitoredItemOperation {
                    item: MonitoredItem {
                        sub_id: request.subscription_id,
                        mitem_id: *id,
                    },
                    parameters: (),
                })
                .collect();
            delegate_service_call(
                &map_db,
                aggregation_server,
                &items,
                false,
                |items| {
                    let mut session = server_session_p.write();
                    let Some(subscription) =
                        session.subscriptions_mut().get_mut(request.subscription_id)
                    else {
                        return items
                            .iter()
                            .map(|_| StatusCode::BadSubscriptionIdInvalid)
                            .collect();
                    };
                    items
                        .iter()
                        .map(|item| {
                            subscription
                                .set_monitoring_mode(item.item.mitem_id, request.monitoring_mode)
                        })
                        .collect()
                },
                |source_id, source_session, items| {
                    let source_session = source_session.read();
                    Ok(Some(call_source_monitored_items(
                        &map_db,
                        *source_id,
                        request.subscription_id,
                        items,
                        |source_sub, source_items| {
                            let source_ids: Vec<_> =
                                source_items.iter().map(|(id, ())| *id).collect();
                            source_session.set_monitoring_mode(
                                source_sub,
                                request.monitoring_mode,
                                &source_ids,
                            )
                        },
                    )))
                },
            )
        })();
        let (status, results) = match results {
            Ok(results) => (StatusCode::Good, Some(results)),
            Err(status) => (status, None),
        };
        SetMonitoringModeResponse {
            response_header: ResponseHeader::new_service_result(&request.request_header, status),
            results,
            diagnostic_infos: None,
        }
        .into()
    }

    #[instrument(level = "debug", skip(self, server_session_p, aggregation_server))]
    fn delete_monitored_items_lservers(
        &self,
        request: &DeleteMonitoredItemsRequest,
        aggregation_server: &AggregationServer,
        server_session_p: Arc<RwLock<ServerSession>>,
        aggserver_sub_id: u32,
    ) -> SupportedMessage {
        let Some(mitem_ids) = &request.monitored_item_ids else {
            return DeleteMonitoredItemsResponse {
                response_header: ResponseHeader::new_service_result(
                    &request.request_header,
                    StatusCode::BadNothingToDo,
                ),
                results: None,
                diagnostic_infos: None,
            }
            .into();
        };

        let aggserver_service_call = |items: &Vec<MonitoredItem>| {
            let mut server_session = server_session_p.write();
            let Some(subscription) = server_session.subscriptions_mut().get_mut(aggserver_sub_id)
            else {
                error!("Could not get subscription from server session!");
                return std::iter::repeat(StatusCode::BadInternalError)
                    .take(items.len())
                    .collect();
            };
            let local_ids: Vec<u32> = items.iter().map(|item| item.mitem_id).collect();
            subscription.delete_monitored_items(&local_ids)
        };

        let lserver_service_call = |_lserver_id: &u16,
                                    client_session_p: Arc<RwLock<ClientSession>>,
                                    items: &Vec<MonitoredItem>| {
            let Some(first_item) = items.get(0) else {
                return Ok(None);
            };
            // Since all items should belong to the same subscription, we just take the sub id from the first element
            let sub_id = first_item.sub_id;
            let mitem_id_slice: Vec<u32> = items.iter().map(|mitem| mitem.mitem_id).collect();
            let res = client_session_p
                .read()
                .delete_monitored_items(sub_id, &mitem_id_slice)
                .map(|v| Some(v));
            res
        };

        let items: Vec<MonitoredItem> = mitem_ids
            .into_iter()
            .map(|mitem_id| MonitoredItem {
                sub_id: aggserver_sub_id,
                mitem_id: *mitem_id,
            })
            .collect();

        let map_db = match aggregation_server.map_db_p.read().connect() {
            Ok(map_db) => map_db,
            Err(e) => {
                tracing::warn!("Could not connect to Mapping Database! {:?}", e);
                return DeleteMonitoredItemsResponse {
                    response_header: ResponseHeader::new_service_result(
                        &request.request_header,
                        StatusCode::BadUnexpectedError,
                    ),
                    results: None,
                    diagnostic_infos: None,
                }
                .into();
            }
        };

        let results = delegate_service_call(
            &map_db,
            aggregation_server,
            &items,
            true,
            aggserver_service_call,
            lserver_service_call,
        );

        if let Ok(statuses) = &results {
            for (item, status) in items.iter().zip(statuses) {
                if status.is_good() {
                    if let Err(error) =
                        map_db.delete_monitored_item_aggserver(item.sub_id, item.mitem_id)
                    {
                        warn!("Error deleting monitored item: {:?}", error);
                    }
                }
            }
        }

        match results {
            Ok(r) => DeleteMonitoredItemsResponse {
                response_header: ResponseHeader::new_good(&request.request_header),
                results: Some(r),
                diagnostic_infos: None,
            }
            .into(),
            Err(sc) => DeleteMonitoredItemsResponse {
                response_header: ResponseHeader::new_service_result(&request.request_header, sc),
                results: None,
                diagnostic_infos: None,
            }
            .into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::aggregation_server::map_db::MapDatabasePool;
    use crate::server::builder::ServerBuilder;
    use crate::server::diagnostics::ServerDiagnostics;
    use crate::server::prelude::{DataTypeId, VariableBuilder};
    use crate::types::{
        ExtensionObject, MonitoringMode, MonitoringParameters, RequestHeader, TimestampsToReturn,
    };

    fn subscription() -> Subscription {
        Subscription::new(
            Arc::new(RwLock::new(ServerDiagnostics::default())),
            7,
            true,
            100.0,
            300,
            100,
            0,
            None,
        )
    }

    fn result(status_code: StatusCode, monitored_item_id: u32) -> MonitoredItemCreateResult {
        MonitoredItemCreateResult {
            status_code,
            monitored_item_id,
            revised_sampling_interval: 100.0,
            revised_queue_size: 1,
            filter_result: ExtensionObject::null(),
        }
    }

    #[test]
    fn failed_source_items_do_not_create_mappings_or_consume_ids() {
        let pool = MapDatabasePool::new().unwrap();
        let db = pool.connect().unwrap();
        let source_id = db
            .insert_lserver(&"source".to_string(), &NodeId::new(2, "source"))
            .unwrap();
        db.insert_subscription(source_id, 13, 7).unwrap();
        let source_sub = db.get_lserver_sub_id(source_id, 7).unwrap();
        let mut subscription = subscription();
        let failed = result(StatusCode::BadNodeIdUnknown, 0);
        let mut results = vec![failed.clone(), result(StatusCode::Good, 42), failed.clone()];

        assert!(map_created_monitored_items(
            &db,
            source_sub.internal_sub_id,
            &mut subscription,
            &mut results
        )
        .is_empty());

        assert_eq!(results[0], failed);
        assert_eq!(results[1].monitored_item_id, 1);
        assert_eq!(results[2], failed);
        assert_eq!(db.get_lserver_monitored_item(7, 1).unwrap(), Some(42));
        assert_eq!(db.get_aggserver_monitored_item(7, 0).unwrap(), None);
        assert_eq!(subscription.take_next_monitored_item_id(), 2);
    }

    #[test]
    fn failed_source_deletion_keeps_mapping_for_retry() {
        let address_space = Arc::new(RwLock::new(AddressSpace::new()));
        let aggregation = AggregationServer::new(&address_space).unwrap();
        let db = aggregation.map_db_p.read().connect().unwrap();
        let source_id = db
            .insert_lserver(&"source".to_string(), &NodeId::new(2, "source"))
            .unwrap();
        db.insert_subscription(source_id, 13, 7).unwrap();
        let source_sub = db.get_lserver_sub_id(source_id, 7).unwrap();
        db.insert_monitored_item(source_sub.internal_sub_id, 42, 1)
            .unwrap();
        let session = Arc::new(RwLock::new(ServerSession::new_no_certificate_store()));
        session
            .write()
            .subscriptions_mut()
            .insert(7, subscription());
        let request = DeleteMonitoredItemsRequest {
            request_header: RequestHeader::dummy(),
            subscription_id: 7,
            monitored_item_ids: Some(vec![1]),
        };

        // The source session is unavailable, so deletion cannot be confirmed.
        let response = MonitoredItemService::new().delete_monitored_items_lservers(
            &request,
            &aggregation,
            session,
            7,
        );
        let SupportedMessage::DeleteMonitoredItemsResponse(response) = response else {
            panic!("expected DeleteMonitoredItemsResponse");
        };
        assert_eq!(response.response_header.service_result, StatusCode::Good);
        assert_eq!(
            response.results.unwrap(),
            vec![StatusCode::BadServerNotConnected]
        );
        assert_eq!(db.get_lserver_monitored_item(7, 1).unwrap(), Some(42));
    }

    #[test]
    fn mixed_create_does_not_sample_source_nodes_locally() {
        let temp = tempdir::TempDir::new("aggregation-monitored-items").unwrap();
        let server = ServerBuilder::new_sample()
            .pki_dir(temp.path())
            .server()
            .unwrap();
        let server_state = server.server_state();
        let address_space = server.address_space();
        let aggregation = AggregationServer::new(&address_space).unwrap();
        let db = aggregation.map_db_p.read().connect().unwrap();
        let (local_namespace, source_namespace) = {
            let mut address_space = address_space.write();
            (
                address_space
                    .register_namespace("urn:test:aggregation:local")
                    .unwrap(),
                address_space
                    .register_namespace("urn:test:aggregation:source")
                    .unwrap(),
            )
        };
        let source_id = db
            .insert_lserver(
                &"source".to_string(),
                &NodeId::new(source_namespace, "source"),
            )
            .unwrap();
        db.insert_namespace(source_id, source_namespace, 1).unwrap();
        let local_node = NodeId::new(local_namespace, "local-test-value");
        let source_node = NodeId::new(source_namespace, "source-test-value");
        {
            let mut address_space = address_space.write();
            for node in [&local_node, &source_node] {
                VariableBuilder::new(node, "value", "value")
                    .data_type(DataTypeId::UInt32)
                    .value(0u32)
                    .insert(&mut address_space);
            }
        }
        let session = Arc::new(RwLock::new(ServerSession::new_no_certificate_store()));
        session
            .write()
            .subscriptions_mut()
            .insert(7, subscription());
        let items: Vec<_> = [(source_node, 11), (local_node, 22)]
            .into_iter()
            .map(|(node_id, client_handle)| MonitoredItemCreateRequest {
                item_to_monitor: node_id.into(),
                monitoring_mode: MonitoringMode::Reporting,
                requested_parameters: MonitoringParameters {
                    client_handle,
                    sampling_interval: 100.0,
                    filter: ExtensionObject::null(),
                    queue_size: 1,
                    discard_oldest: true,
                },
            })
            .collect();
        let request = CreateMonitoredItemsRequest {
            request_header: RequestHeader::dummy(),
            subscription_id: 7,
            timestamps_to_return: TimestampsToReturn::Both,
            items_to_create: Some(items.clone()),
        };
        MonitoredItemService::new().create_monitored_items_lservers(
            &request,
            &aggregation,
            session.clone(),
            7,
            &server_state.read(),
            &address_space.read(),
            &items,
        );

        let mut session = session.write();
        let subscription = session.subscriptions_mut().get_mut(7).unwrap();
        assert_eq!(subscription.monitored_items_len(), 1);
        assert_eq!(subscription.get_handles().1, vec![22]);
    }

    #[test]
    fn source_modify_resolves_current_ids_and_preserves_unmapped_errors() {
        let pool = MapDatabasePool::new().unwrap();
        let db = pool.connect().unwrap();
        let source = db
            .insert_lserver(&"source".to_string(), &NodeId::new(2, "source"))
            .unwrap();
        db.insert_subscription(source, 13, 7).unwrap();
        let sub = db.get_lserver_sub_id(source, 7).unwrap();
        db.insert_monitored_item(sub.internal_sub_id, 42, 1)
            .unwrap();
        let parameters = MonitoringParameters {
            client_handle: 77,
            sampling_interval: 250.0,
            filter: ExtensionObject::null(),
            queue_size: 5,
            discard_oldest: false,
        };
        let items: Vec<_> = [1, 999]
            .into_iter()
            .map(|id| MonitoredItemOperation {
                item: MonitoredItem {
                    sub_id: 7,
                    mitem_id: id,
                },
                parameters: parameters.clone(),
            })
            .collect();
        // Source IDs may be replaced by reconnect after the request is routed.
        db.delete_monitored_item(sub.internal_sub_id, 42).unwrap();
        db.insert_monitored_item(sub.internal_sub_id, 84, 1)
            .unwrap();
        let results: Vec<MonitoredItemModifyResult> =
            call_source_monitored_items(&db, source, 7, &items, |source_sub, source_items| {
                assert_eq!(source_sub, 13);
                assert_eq!(source_items, &[(84, parameters)]);
                Ok(vec![MonitoredItemModifyResult {
                    status_code: StatusCode::Good,
                    revised_sampling_interval: 300.0,
                    revised_queue_size: 5,
                    filter_result: ExtensionObject::null(),
                }])
            });
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].status_code, StatusCode::Good);
        assert_eq!(results[0].revised_sampling_interval, 300.0);
        assert_eq!(
            results[1].status_code,
            StatusCode::BadMonitoredItemIdInvalid
        );
    }

    #[test]
    fn malformed_source_mode_response_preserves_every_result_slot() {
        let pool = MapDatabasePool::new().unwrap();
        let db = pool.connect().unwrap();
        let source = db
            .insert_lserver(&"source".to_string(), &NodeId::new(2, "source"))
            .unwrap();
        db.insert_subscription(source, 13, 7).unwrap();
        let sub = db.get_lserver_sub_id(source, 7).unwrap();
        db.insert_monitored_item(sub.internal_sub_id, 42, 1)
            .unwrap();
        db.insert_monitored_item(sub.internal_sub_id, 43, 2)
            .unwrap();
        let items: Vec<_> = [1, 999, 2]
            .into_iter()
            .map(|id| MonitoredItemOperation {
                item: MonitoredItem {
                    sub_id: 7,
                    mitem_id: id,
                },
                parameters: (),
            })
            .collect();
        let results: Vec<StatusCode> =
            call_source_monitored_items(&db, source, 7, &items, |_, ids| {
                assert_eq!(ids, &[(42, ()), (43, ())]);
                Ok(vec![StatusCode::Good])
            });
        assert_eq!(
            results,
            vec![
                StatusCode::BadUnexpectedError,
                StatusCode::BadMonitoredItemIdInvalid,
                StatusCode::BadUnexpectedError
            ]
        );
    }

    #[test]
    fn mixed_modify_and_mode_keep_local_results_when_source_is_unavailable() {
        let temp = tempdir::TempDir::new("aggregation-modify-mode").unwrap();
        let server = ServerBuilder::new_sample()
            .pki_dir(temp.path())
            .server()
            .unwrap();
        let state = server.server_state();
        let address_space = server.address_space();
        let aggregation = AggregationServer::new(&address_space).unwrap();
        let db = aggregation.map_db_p.read().connect().unwrap();
        let source = db
            .insert_lserver(&"source".to_string(), &NodeId::new(1, "source"))
            .unwrap();
        db.insert_subscription(source, 13, 7).unwrap();
        let source_sub = db.get_lserver_sub_id(source, 7).unwrap();
        db.insert_monitored_item(source_sub.internal_sub_id, 42, 100)
            .unwrap();
        state.write().aggregation_server = Some(aggregation.clone());
        let local_node = {
            let mut address_space = address_space.write();
            let ns = address_space
                .register_namespace("urn:test:local-modify")
                .unwrap();
            let node = NodeId::new(ns, "value");
            VariableBuilder::new(&node, "value", "value")
                .data_type(DataTypeId::UInt32)
                .value(0u32)
                .insert(&mut address_space);
            node
        };
        let parameters = MonitoringParameters {
            client_handle: 10,
            sampling_interval: 100.0,
            filter: ExtensionObject::null(),
            queue_size: 2,
            discard_oldest: true,
        };
        let mut sub = Subscription::new(
            Arc::new(RwLock::new(ServerDiagnostics::default())),
            7,
            true,
            100.0,
            300,
            100,
            0,
            Some(aggregation.clone()),
        );
        let created = sub.create_monitored_items(
            &state.read(),
            &address_space.read(),
            &chrono::Utc::now(),
            TimestampsToReturn::Both,
            &[MonitoredItemCreateRequest {
                item_to_monitor: local_node.into(),
                monitoring_mode: MonitoringMode::Reporting,
                requested_parameters: parameters.clone(),
            }],
        );
        assert_eq!(created[0].status_code, StatusCode::Good);
        let local_id = created[0].monitored_item_id;
        let session = Arc::new(RwLock::new(ServerSession::new_no_certificate_store()));
        session.write().subscriptions_mut().insert(7, sub);
        let ids = vec![100, local_id, 999];
        let modify = ModifyMonitoredItemsRequest {
            request_header: RequestHeader::dummy(),
            subscription_id: 7,
            timestamps_to_return: TimestampsToReturn::Both,
            items_to_modify: Some(
                ids.iter()
                    .map(|id| MonitoredItemModifyRequest {
                        monitored_item_id: *id,
                        requested_parameters: MonitoringParameters {
                            client_handle: 20,
                            ..parameters.clone()
                        },
                    })
                    .collect(),
            ),
        };
        let response = MonitoredItemService::new().modify_monitored_items(
            state.clone(),
            session.clone(),
            address_space,
            &modify,
        );
        let SupportedMessage::ModifyMonitoredItemsResponse(response) = response else {
            panic!("expected modify response")
        };
        assert_eq!(response.response_header.service_result, StatusCode::Good);
        let statuses: Vec<_> = response
            .results
            .unwrap()
            .into_iter()
            .map(|result| result.status_code)
            .collect();
        let expected = vec![
            StatusCode::BadServerNotConnected,
            StatusCode::Good,
            StatusCode::BadMonitoredItemIdInvalid,
        ];
        assert_eq!(statuses, expected);
        assert_eq!(
            session
                .write()
                .subscriptions_mut()
                .get_mut(7)
                .unwrap()
                .get_handles()
                .1,
            vec![20]
        );
        let mode = SetMonitoringModeRequest {
            request_header: RequestHeader::dummy(),
            subscription_id: 7,
            monitoring_mode: MonitoringMode::Disabled,
            monitored_item_ids: Some(ids),
        };
        let response = MonitoredItemService::new().set_monitoring_mode(session, &mode);
        let SupportedMessage::SetMonitoringModeResponse(response) = response else {
            panic!("expected monitoring-mode response")
        };
        assert_eq!(response.response_header.service_result, StatusCode::Good);
        assert_eq!(response.results.unwrap(), expected);
    }
}
