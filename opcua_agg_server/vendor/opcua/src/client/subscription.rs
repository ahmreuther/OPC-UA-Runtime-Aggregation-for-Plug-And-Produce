// OPCUA for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2017-2022 Adam Lock

//! Provides subscription and monitored item tracking.
//!
//! The structs and functions in this file allow the client to maintain a shadow copy of the
//! subscription and monitored item state on the server. If the server goes down and the session
//! needs to be recreated, the client API can reconstruct the subscriptions and monitored item from
//! its shadow version.
//!
//! None of this is for public consumption. The client is expected to recreate state automatically
//! on a reconnect if necessary.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    marker::Sync,
    sync::Arc,
};

use crate::sync::*;
use crate::types::{
    service_types::{DataChangeNotification, ReadValueId},
    *,
};

use super::callbacks::OnSubscriptionNotification;

pub(crate) struct CreateMonitoredItem {
    pub id: u32,
    pub client_handle: u32,
    pub item_to_monitor: ReadValueId,
    pub monitoring_mode: MonitoringMode,
    pub queue_size: u32,
    pub discard_oldest: bool,
    pub sampling_interval: f64,
    pub filter: ExtensionObject,
    pub timestamps_to_return: TimestampsToReturn,
}

pub(crate) struct ModifyMonitoredItem {
    pub id: u32,
    pub client_handle: u32,
    pub sampling_interval: f64,
    pub queue_size: u32,
    pub discard_oldest: bool,
    pub filter: ExtensionObject,
    pub timestamps_to_return: TimestampsToReturn,
}

#[derive(Debug)]
pub struct MonitoredItem {
    /// This is the monitored item's id within the subscription
    id: u32,
    /// Monitored item's handle. Used internally - not modifiable
    client_handle: u32,
    // The thing that is actually being monitored - the node id, attribute, index, encoding.
    item_to_monitor: ReadValueId,
    /// Queue size
    queue_size: usize,
    /// Discard oldest
    discard_oldest: bool,
    /// Monitoring mode
    monitoring_mode: MonitoringMode,
    /// Sampling interval
    sampling_interval: f64,
    /// Creation metadata retained for reconnection.
    filter: ExtensionObject,
    timestamps_to_return: TimestampsToReturn,
    /// Last value of the item
    last_value: DataValue,
    /// A list of all values received in the last data change notification. This list is cleared immediately
    /// after the data change notification.
    values: Vec<DataValue>,
    /// Triggered items
    triggered_items: BTreeSet<u32>,
}

impl MonitoredItem {
    pub fn new(client_handle: u32) -> MonitoredItem {
        MonitoredItem {
            id: 0,
            queue_size: 1,
            sampling_interval: 0.0,
            item_to_monitor: ReadValueId {
                node_id: NodeId::null(),
                attribute_id: 0,
                index_range: UAString::null(),
                data_encoding: QualifiedName::null(),
            },
            monitoring_mode: MonitoringMode::Reporting,
            discard_oldest: false,
            filter: ExtensionObject::null(),
            timestamps_to_return: TimestampsToReturn::Both,
            last_value: DataValue::null(),
            values: Vec::with_capacity(1),
            client_handle,
            triggered_items: BTreeSet::new(),
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn client_handle(&self) -> u32 {
        self.client_handle
    }

    pub fn item_to_monitor(&self) -> &ReadValueId {
        &self.item_to_monitor
    }

    pub fn sampling_interval(&self) -> f64 {
        self.sampling_interval
    }

    pub fn queue_size(&self) -> usize {
        self.queue_size
    }

    pub fn last_value(&self) -> &DataValue {
        &self.last_value
    }

    pub fn values(&self) -> &Vec<DataValue> {
        &self.values
    }

    pub fn clear_values(&mut self) {
        self.values.clear();
    }

    pub fn append_new_value(&mut self, value: DataValue) {
        if self.values.len() == self.queue_size {
            let _ = self.values.pop();
            self.values.push(value);
        }
    }

    pub fn monitoring_mode(&self) -> MonitoringMode {
        self.monitoring_mode
    }

    pub fn discard_oldest(&self) -> bool {
        self.discard_oldest
    }

    pub(crate) fn timestamps_to_return(&self) -> TimestampsToReturn {
        self.timestamps_to_return
    }

    pub(crate) fn recreation_request(&self) -> MonitoredItemCreateRequest {
        MonitoredItemCreateRequest {
            item_to_monitor: self.item_to_monitor.clone(),
            monitoring_mode: self.monitoring_mode,
            requested_parameters: MonitoringParameters {
                client_handle: self.client_handle,
                sampling_interval: self.sampling_interval,
                filter: self.filter.clone(),
                queue_size: self.queue_size as u32,
                discard_oldest: self.discard_oldest,
            },
        }
    }

    pub(crate) fn set_id(&mut self, value: u32) {
        self.id = value;
    }

    pub(crate) fn set_item_to_monitor(&mut self, item_to_monitor: ReadValueId) {
        self.item_to_monitor = item_to_monitor;
    }

    pub(crate) fn set_sampling_interval(&mut self, value: f64) {
        self.sampling_interval = value;
    }

    pub(crate) fn set_queue_size(&mut self, value: usize) {
        self.queue_size = value;
        if self.queue_size > self.values.capacity() {
            self.values
                .reserve(self.queue_size - self.values.capacity());
        }
    }

    pub(crate) fn set_monitoring_mode(&mut self, monitoring_mode: MonitoringMode) {
        self.monitoring_mode = monitoring_mode;
    }

    pub(crate) fn set_discard_oldest(&mut self, discard_oldest: bool) {
        self.discard_oldest = discard_oldest;
    }

    pub(crate) fn set_triggering(&mut self, links_to_add: &[u32], links_to_remove: &[u32]) {
        links_to_remove.iter().for_each(|i| {
            self.triggered_items.remove(i);
        });
        links_to_add.iter().for_each(|i| {
            self.triggered_items.insert(*i);
        });
    }

    pub(crate) fn triggered_items(&self) -> &BTreeSet<u32> {
        &self.triggered_items
    }
}

pub struct Subscription {
    /// Subscription id, supplied by server
    subscription_id: u32,
    /// Publishing interval in seconds
    publishing_interval: f64,
    /// Lifetime count, revised by server
    lifetime_count: u32,
    /// Max keep alive count, revised by server
    max_keep_alive_count: u32,
    /// Max notifications per publish, revised by server
    max_notifications_per_publish: u32,
    /// Publishing enabled
    publishing_enabled: bool,
    /// Priority
    priority: u8,
    /// The change callback will be what is called if any monitored item changes within a cycle.
    /// The monitored item is referenced by its id
    notification_callback: Arc<Mutex<dyn OnSubscriptionNotification + Send + Sync>>,
    /// A map of monitored items associated with the subscription (key = monitored_item_id)
    monitored_items: HashMap<u32, MonitoredItem>,
    /// A map of client handle to monitored item id
    client_handles: HashMap<u32, u32>,
}

impl Subscription {
    /// Creates a new subscription using the supplied parameters and the supplied data change callback.
    pub fn new(
        subscription_id: u32,
        publishing_interval: f64,
        lifetime_count: u32,
        max_keep_alive_count: u32,
        max_notifications_per_publish: u32,
        publishing_enabled: bool,
        priority: u8,
        notification_callback: Arc<Mutex<dyn OnSubscriptionNotification + Send + Sync>>,
    ) -> Subscription {
        Subscription {
            subscription_id,
            publishing_interval,
            lifetime_count,
            max_keep_alive_count,
            max_notifications_per_publish,
            publishing_enabled,
            priority,
            notification_callback,
            monitored_items: HashMap::new(),
            client_handles: HashMap::new(),
        }
    }

    pub fn monitored_items(&self) -> &HashMap<u32, MonitoredItem> {
        &self.monitored_items
    }

    pub fn subscription_id(&self) -> u32 {
        self.subscription_id
    }

    pub fn publishing_interval(&self) -> f64 {
        self.publishing_interval
    }

    pub fn lifetime_count(&self) -> u32 {
        self.lifetime_count
    }

    pub fn max_keep_alive_count(&self) -> u32 {
        self.max_keep_alive_count
    }

    pub fn max_notifications_per_publish(&self) -> u32 {
        self.max_notifications_per_publish
    }

    pub fn publishing_enabled(&self) -> bool {
        self.publishing_enabled
    }

    pub fn priority(&self) -> u8 {
        self.priority
    }

    pub fn notification_callback(
        &self,
    ) -> Arc<Mutex<dyn OnSubscriptionNotification + Send + Sync>> {
        self.notification_callback.clone()
    }

    pub(crate) fn set_publishing_interval(&mut self, publishing_interval: f64) {
        self.publishing_interval = publishing_interval;
    }

    pub(crate) fn set_lifetime_count(&mut self, lifetime_count: u32) {
        self.lifetime_count = lifetime_count;
    }

    pub(crate) fn set_max_keep_alive_count(&mut self, max_keep_alive_count: u32) {
        self.max_keep_alive_count = max_keep_alive_count;
    }

    pub(crate) fn set_max_notifications_per_publish(&mut self, max_notifications_per_publish: u32) {
        self.max_notifications_per_publish = max_notifications_per_publish;
    }

    pub(crate) fn set_priority(&mut self, priority: u8) {
        self.priority = priority;
    }

    pub(crate) fn set_publishing_enabled(&mut self, publishing_enabled: bool) {
        self.publishing_enabled = publishing_enabled;
    }

    pub(crate) fn insert_monitored_items(&mut self, items_to_create: &[CreateMonitoredItem]) {
        items_to_create.iter().for_each(|i| {
            let mut monitored_item = MonitoredItem::new(i.client_handle);
            monitored_item.set_id(i.id);
            monitored_item.set_monitoring_mode(i.monitoring_mode);
            monitored_item.set_discard_oldest(i.discard_oldest);
            monitored_item.set_sampling_interval(i.sampling_interval);
            monitored_item.set_queue_size(i.queue_size as usize);
            monitored_item.set_item_to_monitor(i.item_to_monitor.clone());
            monitored_item.filter = i.filter.clone();
            monitored_item.timestamps_to_return = i.timestamps_to_return;

            let client_handle = monitored_item.client_handle();
            let monitored_item_id = monitored_item.id();
            self.monitored_items
                .insert(monitored_item_id, monitored_item);
            self.client_handles.insert(client_handle, monitored_item_id);
        });
    }

    pub(crate) fn modify_monitored_items(&mut self, items_to_modify: &[ModifyMonitoredItem]) {
        items_to_modify.iter().for_each(|i| {
            if let Some(ref mut monitored_item) = self.monitored_items.get_mut(&i.id) {
                self.client_handles.remove(&monitored_item.client_handle);
                monitored_item.client_handle = i.client_handle;
                self.client_handles.insert(i.client_handle, i.id);
                monitored_item.set_sampling_interval(i.sampling_interval);
                monitored_item.set_queue_size(i.queue_size as usize);
                monitored_item.set_discard_oldest(i.discard_oldest);
                monitored_item.filter = i.filter.clone();
                monitored_item.timestamps_to_return = i.timestamps_to_return;
            }
        });
    }

    pub(crate) fn set_monitoring_mode(&mut self, item_ids: &[u32], mode: MonitoringMode) {
        for id in item_ids {
            if let Some(item) = self.monitored_items.get_mut(id) {
                item.set_monitoring_mode(mode);
            }
        }
    }

    pub(crate) fn delete_monitored_items(&mut self, items_to_delete: &[u32]) {
        items_to_delete.iter().for_each(|id| {
            // Remove the monitored item and the client handle / id entry
            if let Some(monitored_item) = self.monitored_items.remove(id) {
                let _ = self.client_handles.remove(&monitored_item.client_handle());
            }
        })
    }

    pub(crate) fn set_triggering(
        &mut self,
        triggering_item_id: u32,
        links_to_add: &[u32],
        links_to_remove: &[u32],
    ) {
        if let Some(ref mut monitored_item) = self.monitored_items.get_mut(&triggering_item_id) {
            monitored_item.set_triggering(links_to_add, links_to_remove);
        }
    }

    fn monitored_item_id_from_handle(&self, client_handle: u32) -> Option<u32> {
        self.client_handles.get(&client_handle).copied()
    }

    pub(crate) fn on_event(&mut self, events: &[EventNotificationList]) {
        let mut cb = trace_lock!(self.notification_callback);
        events.iter().for_each(|event| {
            cb.on_event(event);
        });
    }

    pub(crate) fn on_data_change(&mut self, data_change_notifications: &[DataChangeNotification]) {
        let mut monitored_item_ids = HashSet::new();
        data_change_notifications.iter().for_each(|n| {
            if let Some(ref monitored_items) = n.monitored_items {
                monitored_item_ids.clear();
                for i in monitored_items {
                    let monitored_item_id = {
                        let monitored_item_id = self.monitored_item_id_from_handle(i.client_handle);
                        if monitored_item_id.is_none() {
                            continue;
                        }
                        *monitored_item_id.as_ref().unwrap()
                    };
                    let monitored_item = self.monitored_items.get_mut(&monitored_item_id).unwrap();
                    monitored_item.last_value = i.value.clone();
                    monitored_item.values.push(i.value.clone());
                    monitored_item_ids.insert(monitored_item_id);
                }
                if !monitored_item_ids.is_empty() {
                    let data_change_items: Vec<&MonitoredItem> = monitored_item_ids
                        .iter()
                        .map(|id| self.monitored_items.get(id).unwrap())
                        .collect();

                    {
                        // Call the call back with the changes we collected
                        let mut cb = trace_lock!(self.notification_callback);
                        cb.on_data_change(&data_change_items);
                    }

                    // Clear the values
                    monitored_item_ids.iter().for_each(|id| {
                        let m = self.monitored_items.get_mut(id).unwrap();
                        m.clear_values();
                    });
                }
            }
        });
    }
}

#[cfg(all(test, feature = "server"))]
mod aggregation_notification_tests {
    use super::*;
    use crate::server::aggregation_server::services::subscription::AggregationSubscriptionNotification;
    use crate::server::diagnostics::ServerDiagnostics;
    use crate::server::session::Session;
    use crate::server::subscriptions::monitored_item::Notification;
    use crate::server::subscriptions::subscription::Subscription as ServerSubscription;
    use std::sync::mpsc;
    use std::time::Duration as StdDuration;

    #[test]
    fn source_callbacks_complete_while_upper_session_is_locked() {
        let session = Arc::new(RwLock::new(Session::new_no_certificate_store()));
        let sub = ServerSubscription::new(
            Arc::new(RwLock::new(ServerDiagnostics::default())),
            7,
            true,
            100.0,
            300,
            100,
            0,
            None,
        );
        let queue = sub.aggregation_notifications();
        session.write().subscriptions_mut().insert(7, sub);
        let callback = AggregationSubscriptionNotification {
            map_db_p: Arc::new(RwLock::new(
                crate::server::aggregation_server::map_db::MapDatabasePool::new().unwrap(),
            )),
            notifications: queue.clone(),
            lserver_id: 1,
            aggserver_sub_id: 7,
        };
        let mut source = Subscription::new(
            11,
            100.0,
            300,
            100,
            0,
            true,
            0,
            Arc::new(Mutex::new(callback)),
        );
        source.insert_monitored_items(&[CreateMonitoredItem {
            id: 12,
            client_handle: 456,
            item_to_monitor: NodeId::new(2, "value").into(),
            monitoring_mode: MonitoringMode::Reporting,
            queue_size: 10,
            discard_oldest: true,
            sampling_interval: 100.0,
            filter: ExtensionObject::null(),
            timestamps_to_return: TimestampsToReturn::Both,
        }]);
        let expected = vec![
            MonitoredItemNotification {
                client_handle: 456,
                value: DataValue::new_now(41u32),
            },
            MonitoredItemNotification {
                client_handle: 456,
                value: DataValue::new_now(42u32),
            },
        ];
        let changes = expected.clone();
        let events = EventNotificationList {
            events: Some(vec![EventFieldList {
                client_handle: 789,
                event_fields: Some(vec![Variant::UInt32(43)]),
            }]),
        };
        let expected_events = events.events.clone().unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let guard = session.write();
        let worker = std::thread::spawn(move || {
            source.on_data_change(&[DataChangeNotification {
                monitored_items: Some(changes),
                diagnostic_infos: None,
            }]);
            source.on_event(&[events]);
            done_tx.send(()).unwrap();
        });
        let result = done_rx.recv_timeout(StdDuration::from_secs(2));
        drop(guard);
        worker.join().unwrap();
        assert!(
            result.is_ok(),
            "source callback waited for the locked upper session"
        );
        let actual: Vec<_> = queue.lock().drain(..).collect();
        let expected: Vec<_> = expected
            .into_iter()
            .map(Notification::MonitoredItemNotification)
            .chain(expected_events.into_iter().map(Notification::Event))
            .collect();
        assert_eq!(actual, expected);
    }
}

#[cfg(test)]
mod reconnect_metadata_tests {
    use super::*;
    use crate::client::callbacks::DataChangeCallback;

    #[test]
    fn modified_item_metadata_and_handle_survive_recreation() {
        let mut sub = Subscription::new(
            10,
            100.0,
            300,
            100,
            0,
            true,
            0,
            Arc::new(Mutex::new(DataChangeCallback::new(|_| {}))),
        );
        let filter = ExtensionObject::from_encodable(
            NodeId::new(0, 724),
            &DataChangeFilter {
                trigger: DataChangeTrigger::StatusValueTimestamp,
                deadband_type: 1,
                deadband_value: 2.5,
            },
        );
        sub.insert_monitored_items(&[CreateMonitoredItem {
            id: 1,
            client_handle: 2,
            item_to_monitor: NodeId::new(2, "value").into(),
            monitoring_mode: MonitoringMode::Reporting,
            queue_size: 5,
            discard_oldest: false,
            sampling_interval: 25.0,
            filter: filter.clone(),
            timestamps_to_return: TimestampsToReturn::Source,
        }]);
        let request = sub.monitored_items()[&1].recreation_request();
        assert_eq!(request.requested_parameters.filter, filter);
        assert!(!request.requested_parameters.discard_oldest);
        assert_eq!(
            sub.monitored_items()[&1].timestamps_to_return(),
            TimestampsToReturn::Source
        );

        sub.modify_monitored_items(&[ModifyMonitoredItem {
            id: 1,
            client_handle: 20,
            queue_size: 9,
            sampling_interval: 75.0,
            discard_oldest: true,
            filter: ExtensionObject::null(),
            timestamps_to_return: TimestampsToReturn::Neither,
        }]);
        sub.set_monitoring_mode(&[1], MonitoringMode::Sampling);
        assert_eq!(sub.monitored_item_id_from_handle(2), None);
        assert_eq!(sub.monitored_item_id_from_handle(20), Some(1));
        let item = &sub.monitored_items()[&1];
        let request = item.recreation_request();
        assert_eq!(request.requested_parameters.client_handle, 20);
        assert_eq!(request.requested_parameters.queue_size, 9);
        assert_eq!(request.requested_parameters.sampling_interval, 75.0);
        assert!(request.requested_parameters.discard_oldest);
        assert_eq!(request.requested_parameters.filter, ExtensionObject::null());
        assert_eq!(request.monitoring_mode, MonitoringMode::Sampling);
        assert_eq!(item.timestamps_to_return(), TimestampsToReturn::Neither);
    }
}
