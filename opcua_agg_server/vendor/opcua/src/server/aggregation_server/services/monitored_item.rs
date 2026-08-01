use crate::client::prelude::{DeleteMonitoredItemsRequest, DeleteMonitoredItemsResponse};
use crate::prelude::{
    AddressSpace, CreateMonitoredItemsResponse, MonitoredItemService as _, NodeId, ResponseHeader,
    Session as ClientSession, SupportedMessage,
};
use crate::server::aggregation_server::aggregation_server::AggregationServer;
use crate::server::aggregation_server::services::service_delegation::{
    delegate_service_call, DecidingField, MonitoredItem, TransformableItem,
};
use crate::server::services::monitored_item::MonitoredItemService;
use crate::server::session::Session as ServerSession;
use crate::server::state::ServerState;
use crate::sync::RwLock;
use crate::types::{
    CreateMonitoredItemsRequest, MonitoredItemCreateRequest, MonitoredItemCreateResult, StatusCode,
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
        let aggserver_service_call = |_items: &Vec<MonitoredItemCreateRequest>| {
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
                items_to_create,
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

                let map_res = res.map(|mut v| {
                    for mitem_res in &mut v {
                        let lserver_mitem_id = mitem_res.monitored_item_id;
                        let aggserver_mitem_id = subscription.take_next_monitored_item_id();
                        if let Err(e) = map_db.insert_monitored_item(
                            sub.internal_sub_id,
                            lserver_mitem_id,
                            aggserver_mitem_id,
                        ) {
                            warn!("Cannot insert into monitored item map: {:?}", e);
                        };
                        mitem_res.monitored_item_id = aggserver_mitem_id;
                    }

                    Some(v)
                });
                drop(session);
                return map_res;
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
            return subscription.delete_monitored_items(&mitem_ids);
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

        for item in items {
            let res = map_db.delete_monitored_item_aggserver(item.sub_id, item.mitem_id);
            if let Err(e) = res {
                warn!("Error deleting monitored item: {:?}", e);
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
