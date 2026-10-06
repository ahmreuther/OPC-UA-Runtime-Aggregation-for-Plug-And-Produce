use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use tracing::{instrument, warn};

use crate::client::prelude::{MonitoredItem as ClientMonitoredItem, MonitoredItemNotification};
use crate::prelude::OnSubscriptionNotification;
use crate::prelude::SubscriptionService as ClientSubscriptionService;
use crate::prelude::{
    CreateSubscriptionRequest, CreateSubscriptionResponse, EventNotificationList, ResponseHeader,
    SupportedMessage,
};
use crate::server::aggregation_server::aggregation_server::AggregationServer;
use crate::server::aggregation_server::map_db::MapDatabasePool;
use crate::server::services::subscription::SubscriptionService as ServerSubscriptionService;
use crate::server::subscriptions::monitored_item::Notification;
use crate::sync::{Mutex, RwLock};
use crate::types::{Duration, StatusCode};

pub(crate) type AggregationNotificationQueue = Arc<Mutex<VecDeque<Notification>>>;

pub(crate) trait AggServerSubscriptionService {
    fn create_subscriptions_aggserver(
        &self,
        aggregation_server: AggregationServer,
        notifications: AggregationNotificationQueue,
        request: &CreateSubscriptionRequest,
        revised_publishing_interval: Duration,
        revised_lifetime_count: u32,
        revised_max_keep_alive_count: u32,
        agg_server_sub_id: u32,
    ) -> Result<(), SupportedMessage>;
}

pub(crate) struct AggregationSubscriptionNotification {
    pub(crate) notifications: AggregationNotificationQueue,
    pub(crate) map_db_p: Arc<RwLock<MapDatabasePool>>,
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
    fn subscription_context_id(&self) -> Option<u32> {
        Some(self.aggserver_sub_id)
    }

    fn on_subscription_recreation_started(
        &mut self,
        _old_subscription_id: u32,
    ) -> Result<(), StatusCode> {
        let database = self
            .map_db_p
            .read()
            .connect()
            .map_err(|_| StatusCode::BadInternalError)?;
        database
            .begin_subscription_recreation(self.lserver_id, self.aggserver_sub_id)
            .map_err(|error| {
                warn!("Cannot invalidate source subscription mapping: {:?}", error);
                StatusCode::BadInternalError
            })
    }

    fn on_subscription_recreated(
        &mut self,
        _old_subscription_id: u32,
        new_subscription_id: u32,
        monitored_items: &[(u32, u32, crate::types::MonitoredItemCreateResult)],
    ) -> Result<(), StatusCode> {
        let database = self
            .map_db_p
            .read()
            .connect()
            .map_err(|_| StatusCode::BadInternalError)?;
        let mappings: Vec<_> = monitored_items
            .iter()
            .map(|(old_id, _, result)| (*old_id, result.clone()))
            .collect();
        database
            .remap_recreated_subscription(
                self.lserver_id,
                self.aggserver_sub_id,
                new_subscription_id,
                &mappings,
            )
            .map_err(|error| {
                warn!("Cannot restore source subscription mappings: {:?}", error);
                StatusCode::BadInternalError
            })?;
        let mut notifications = self.notifications.lock();
        for (_, client_handle, result) in monitored_items {
            if result.status_code.is_bad() {
                notifications.push_back(
                    MonitoredItemNotification {
                        client_handle: *client_handle,
                        value: crate::types::DataValue {
                            status: Some(result.status_code),
                            ..crate::types::DataValue::null()
                        },
                    }
                    .into(),
                );
            }
        }
        Ok(())
    }

    #[instrument(level = "trace")]
    fn on_event(&mut self, _events: &EventNotificationList) {
        let Some(events) = &_events.events else {
            return;
        };

        self.notifications
            .lock()
            .extend(events.iter().cloned().map(Notification::Event));
    }

    #[instrument(level = "trace")]
    fn on_data_change(&mut self, _data_change_items: &[&ClientMonitoredItem]) {
        // The source client invokes callbacks while holding its session locks.
        // Never acquire the upper session here: its service handlers can be
        // waiting for that same source client. The upper publish tick drains
        // this independent queue and assigns notification sequence numbers.
        let mut notifications = self.notifications.lock();
        for item in _data_change_items {
            for value in item.values() {
                notifications.push_back(Notification::MonitoredItemNotification(
                    MonitoredItemNotification {
                        client_handle: item.client_handle(),
                        value: value.clone(),
                    },
                ));
            }
        }
    }
}

impl AggServerSubscriptionService for ServerSubscriptionService {
    #[instrument(level = "debug", skip(self, notifications, aggregation_server))]
    fn create_subscriptions_aggserver(
        &self,
        aggregation_server: AggregationServer,
        notifications: AggregationNotificationQueue,
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
                    notifications: notifications.clone(),
                    map_db_p: aggregation_server.map_db_p.clone(),
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

/// Resolve mappings only while the source session is locked. A reconnect owns
/// its write lock while replacing source-side IDs and publishing the new maps.
pub(crate) fn change_source_subscriptions<F>(
    aggregation: &AggregationServer,
    upper_subscription_id: u32,
    change: F,
) -> Result<(), StatusCode>
where
    F: Fn(&crate::client::prelude::Session, u32) -> Result<(), StatusCode>,
{
    let sources: Vec<_> = aggregation
        .lower_server_sessions_p
        .read()
        .iter()
        .map(|(id, session)| (*id, session.clone()))
        .collect();
    let database = aggregation
        .map_db_p
        .read()
        .connect()
        .map_err(|_| StatusCode::BadInternalError)?;
    let mut first_error = None;
    for (source_id, source) in sources {
        let session = source.read();
        let result = database
            .get_lserver_sub_id(source_id, upper_subscription_id)
            .map_err(|_| StatusCode::BadSubscriptionIdInvalid)
            .and_then(|mapping| change(&session, mapping.lserver_sub_id));
        if let Err(status) = result {
            first_error.get_or_insert(status);
        }
    }
    first_error.map_or(Ok(()), Err)
}
