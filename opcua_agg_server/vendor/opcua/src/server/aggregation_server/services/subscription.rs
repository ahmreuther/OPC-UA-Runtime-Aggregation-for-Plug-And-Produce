use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use tracing::{instrument, warn};

use crate::client::prelude::{MonitoredItem as ClientMonitoredItem, MonitoredItemNotification};
use crate::prelude::OnSubscriptionNotification;
use crate::prelude::SubscriptionService as ClientSubscriptionService;
use crate::prelude::{
    CreateSubscriptionRequest, CreateSubscriptionResponse, DateTime, EventNotificationList,
    ResponseHeader, SupportedMessage,
};
use crate::server::aggregation_server::aggregation_server::AggregationServer;
use crate::server::services::subscription::SubscriptionService as ServerSubscriptionService;
use crate::server::session::Session;
use crate::sync::RwLock;
use crate::types::{Duration, NotificationMessage, StatusCode};

pub(crate) trait AggServerSubscriptionService {
    fn create_subscriptions_aggserver(
        &self,
        aggregation_server: AggregationServer,
        session_p: Arc<RwLock<Session>>,
        request: &CreateSubscriptionRequest,
        revised_publishing_interval: Duration,
        revised_lifetime_count: u32,
        revised_max_keep_alive_count: u32,
        agg_server_sub_id: u32,
    ) -> Result<(), SupportedMessage>;
}

pub(crate) struct AggregationSubscriptionNotification {
    pub(crate) session_p: Arc<RwLock<Session>>,
    pub(crate) lserver_id: u16,
    pub(crate) aggserver_sub_id: u32,
}

impl Debug for AggregationSubscriptionNotification {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregationSubscriptionNotification")
            .field("lserver_id", &format!("{:p}", &self.lserver_id))
            .field("aggserver_sub_id", &format!("{:p}", &self.aggserver_sub_id))
            .finish()
    }
}

impl OnSubscriptionNotification for AggregationSubscriptionNotification {
    #[instrument(level = "trace")]
    fn on_event(&mut self, _events: &EventNotificationList) {
        let Some(events) = &_events.events else {
            return;
        };

        let mut session = self.session_p.write();
        let subs = session.subscriptions_mut();
        let Some(sub) = subs.get_mut(self.aggserver_sub_id) else {
            warn!("Subscription ID not found on aggregation server.");
            return;
        };
        let next_sequence_number = sub.next_sequence_number();
        sub.enqueue_notification(NotificationMessage::data_change(
            next_sequence_number,
            DateTime::now(),
            Vec::new(),
            events.clone(),
        ));
        drop(session);
    }

    #[instrument(level = "trace")]
    fn on_data_change(&mut self, _data_change_items: &[&ClientMonitoredItem]) {
        let mut session = self.session_p.write();
        let subs = session.subscriptions_mut();
        let Some(sub) = subs.get_mut(self.aggserver_sub_id) else {
            warn!("Subscription ID not found on aggregation server.");
            return;
        };
        let next_sequence_number = sub.next_sequence_number();
        let mut data_change_notification: Vec<MonitoredItemNotification> = Vec::new();
        for item in _data_change_items {
            for value in item.values() {
                data_change_notification.push(MonitoredItemNotification {
                    client_handle: item.client_handle(),
                    value: value.clone(),
                });
            }
        }
        sub.enqueue_notification(NotificationMessage::data_change(
            next_sequence_number,
            DateTime::now(),
            data_change_notification,
            Vec::new(),
        ));
        drop(session);
    }
}

impl AggServerSubscriptionService for ServerSubscriptionService {
    #[instrument(level = "debug", skip(self, server_session_p, aggregation_server))]
    fn create_subscriptions_aggserver(
        &self,
        aggregation_server: AggregationServer,
        server_session_p: Arc<RwLock<Session>>,
        request: &CreateSubscriptionRequest,
        revised_publishing_interval: Duration,
        revised_lifetime_count: u32,
        revised_max_keep_alive_count: u32,
        aggserver_sub_id: u32,
    ) -> Result<(), SupportedMessage> {
        let lower_server_sessions = aggregation_server.lower_server_sessions_p.read();
        let map_db = match aggregation_server.map_db_p.read().connect() {
            Ok(map_db) => map_db,
            Err(e) => {
                tracing::warn!("Could not connect to Mapping Database! {:?}", e);
                return Err(CreateSubscriptionResponse {
                    response_header: ResponseHeader::new_service_result(
                        &request.request_header,
                        StatusCode::BadUnexpectedError,
                    ),
                    subscription_id: aggserver_sub_id,
                    revised_publishing_interval,
                    revised_lifetime_count,
                    revised_max_keep_alive_count,
                }
                .into());
            }
        };

        for (lserver_id, client_session_p) in lower_server_sessions.iter() {
            let session = client_session_p.read();
            match session.create_subscription(
                revised_publishing_interval,
                revised_lifetime_count,
                revised_max_keep_alive_count,
                request.max_notifications_per_publish,
                request.priority,
                request.publishing_enabled,
                AggregationSubscriptionNotification {
                    session_p: server_session_p.clone(),
                    lserver_id: lserver_id.clone(),
                    aggserver_sub_id: aggserver_sub_id.clone(),
                },
            ) {
                Ok(lserver_sub_id) => {
                    if let Err(e) = map_db.insert_subscription(
                        lserver_id.clone(),
                        lserver_sub_id,
                        aggserver_sub_id,
                    ) {
                        warn!("Could not insert subscription. {:?}", e);
                    }
                }
                Err(status_code) => {
                    warn!(
                        msg = "Could not create Subscription on lower server.",
                        lower_server_id = ?lserver_id,
                        status_code = ?status_code.name(),
                    );
                }
            };
        }
        return Ok(());
    }
}
