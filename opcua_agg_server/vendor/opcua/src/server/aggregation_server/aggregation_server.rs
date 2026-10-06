use crate::client::session::SessionOperationControl;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, OnceLock,
};
use std::thread;
use std::time::{Duration, Instant};

use crate::client::prelude::{ClientBuilder, IdentityToken, Session, SessionCommand};
use crate::server::address_space::AddressSpace;
use crate::server::aggregation_server::instance_aggregation::{
    aggregate_instances_indexed, mapping_rule_key, InstanceMappingRuleIndex,
};
use crate::server::aggregation_server::services::subscription::AggregationSubscriptionNotification;
use crate::sync::{Mutex, RwLock};
use crate::types::{EndpointDescription, NodeId, QualifiedName};
use bimap::BiMap;
use serde::Deserialize;
use serde_json;
use tokio::sync::oneshot;
use tracing::{error, info, instrument, warn};

use crate::prelude::{Server, SubscriptionService as ClientSubscriptionService};
use crate::server::aggregation_server::error_types::{
    AddServerError, LowerServerError, MappingError, ReadStateError, RemoveLowerServerError,
};
use crate::server::aggregation_server::map_db::MapDatabasePool;
use crate::server::aggregation_server::namespace_aggregation::aggregate_namespaces;
use crate::server::aggregation_server::type_aggregation::aggregate_types;
use crate::server::aggregation_server::util_types::{
    InstanceMappingRule, LowerServer, LowerServerInfo, LowerServerThreading, StandardizedNamespace,
};

use super::util_types::IncompleteMapping;

// Keep a failed embedded source from holding an upstream OPC UA client long
// enough for that client to abandon its own session.
const LOWER_SERVER_REQUEST_TIMEOUT_MS: u32 = 5_000;
const URDF_MAX_BYTE_STRING_LENGTH: usize = 16 * 1024 * 1024;
const URDF_MAX_MESSAGE_SIZE: usize = 20 * 1024 * 1024;
static PHASE_TIMING_ENABLED: OnceLock<bool> = OnceLock::new();

fn phase_timing_enabled() -> bool {
    *PHASE_TIMING_ENABLED.get_or_init(|| {
        std::env::var("OJIES_PHASE_TIMING")
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

fn log_phase_timing(server_name: &str, phase: &str, elapsed: Duration) {
    if phase_timing_enabled() {
        warn!(
            "OJIES_PHASE_TIMING server={} phase={} elapsed_ms={:.3}",
            server_name,
            phase,
            elapsed.as_secs_f64() * 1000.0
        );
    }
}

#[derive(Clone)]
pub struct AggregationServer {
    /// References to server objects
    pub address_space_p: Arc<RwLock<AddressSpace>>,
    /// References to own new objects
    pub(crate) map_db_p: Arc<RwLock<MapDatabasePool>>,
    /// Hashes of types mapped to their NodeId on the aggregation server
    pub global_type_hashmap_p: Arc<RwLock<BiMap<u64, NodeId>>>,
    /// Client sessions of each lower server (mapped to their lower server id)
    pub lower_server_sessions_p: Arc<RwLock<HashMap<u16, Arc<RwLock<Session>>>>>,
    pub lower_servers_info_p: Arc<RwLock<HashMap<String, LowerServerInfo>>>,
    pub lower_servers_related_p: Arc<RwLock<HashMap<String, LowerServerThreading>>>,
    pub instance_mapping_rules_p: Arc<RwLock<Vec<InstanceMappingRule>>>,
    pub(crate) instance_mapping_rule_index_p: Arc<RwLock<InstanceMappingRuleIndex>>,
    pub standard_namespaces_p: Arc<RwLock<Vec<StandardizedNamespace>>>,
    /// If one node in a mapping is found but not its counterpart, it is
    /// saved as an incomplete mapping. Every time a new server is aggregated,
    /// we check if the counterpart can be found in this new server.
    pub incomplete_mappings_p: Arc<RwLock<Vec<IncompleteMapping>>>,
    /// Serializes only onboarding transactions; status/cancel never need this lock.
    onboarding_gate: Arc<Mutex<()>>,
}

impl std::fmt::Debug for AggregationServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregationServer")
            .field("address_space_p", &format!("{:p}", &self.address_space_p))
            .field(
                "global_type_hashmap_p",
                &format!("{:p}", &self.global_type_hashmap_p),
            )
            .field(
                "lower_servers_p",
                &format!("{:p}", &self.lower_server_sessions_p),
            )
            .field(
                "lower_servers_info_p",
                &format!("{:p}", &self.lower_servers_info_p),
            )
            .field(
                "lower_servers_related_p",
                &format!("{:p}", &self.lower_servers_related_p),
            )
            .finish()
    }
}

impl AggregationServer {
    #[instrument(level = "info", err, ret, skip_all)]
    pub fn new(address_space_p: &Arc<RwLock<AddressSpace>>) -> Result<Self, MappingError> {
        let ns0_namespace: StandardizedNamespace = StandardizedNamespace {
            url: String::from("http://opcfoundation.org/UA/"),
            nsid: Some(0),
        };
        let server = Self {
            address_space_p: address_space_p.clone(),
            map_db_p: Arc::new(RwLock::new(MapDatabasePool::new()?)),
            global_type_hashmap_p: Arc::new(RwLock::new(BiMap::new())),
            lower_server_sessions_p: Arc::new(RwLock::new(HashMap::new())),
            lower_servers_info_p: Arc::new(RwLock::new(HashMap::new())),
            lower_servers_related_p: Arc::new(RwLock::new(HashMap::new())),
            instance_mapping_rules_p: Arc::new(RwLock::new(Vec::new())),
            instance_mapping_rule_index_p: Arc::new(RwLock::new(
                InstanceMappingRuleIndex::default(),
            )),
            standard_namespaces_p: Arc::new(RwLock::new(vec![ns0_namespace])),
            incomplete_mappings_p: Arc::new(RwLock::new(Vec::new())),
            onboarding_gate: Arc::new(Mutex::new(())),
        };
        return Ok(server);
    }

    pub fn load_standardized_namespaces(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut standard_namespaces: Vec<StandardizedNamespace> = serde_json::from_reader(reader)?;
        if !standard_namespaces
            .iter()
            .any(|n| n.url == "http://opcfoundation.org/UA/")
        {
            let ns0_namespace: StandardizedNamespace = StandardizedNamespace {
                url: String::from("http://opcfoundation.org/UA/"),
                nsid: Some(0),
            };
            standard_namespaces.push(ns0_namespace);
        }
        *self.standard_namespaces_p.write() = standard_namespaces;
        info!(
            "Mapping rules read: {:?}",
            *self.standard_namespaces_p.read()
        );
        return Ok(());
    }

    pub fn load_mapping_rules(&mut self, path: &Path) -> Result<(), Box<dyn Error>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let instance_mapping_rules: Vec<InstanceMappingRule> = serde_json::from_reader(reader)?;
        self.replace_mapping_rules(instance_mapping_rules)?;
        Ok(())
    }

    /// Validate and atomically replace rules and their derived index.
    /// Pending mappings follow the same surviving rule, not its old vector position.
    /// Callers restoring a snapshot must restore its pending mappings after this call.
    pub fn replace_mapping_rules(
        &self,
        instance_mapping_rules: Vec<InstanceMappingRule>,
    ) -> Result<(), MappingError> {
        let indexed_rules = InstanceMappingRuleIndex::new(&instance_mapping_rules)?;
        let rule_count = instance_mapping_rules.len();
        // The gate prevents a source worker from publishing an older staged pending set.
        let _gate = self.onboarding_gate.lock();
        let mut rules = self.instance_mapping_rules_p.write();
        let mut rule_index = self.instance_mapping_rule_index_p.write();
        let mut pending = self.incomplete_mappings_p.write();
        if !pending.is_empty() {
            let remapped = {
                let mut replacements = HashMap::new();
                for (index, rule) in instance_mapping_rules.iter().enumerate() {
                    replacements.entry(mapping_rule_key(rule)).or_insert(index);
                }
                pending
                    .iter()
                    .map(|mapping| {
                        let old_rule = rules.get(mapping.rule_id).ok_or_else(|| {
                            MappingError::InvalidRule(format!(
                                "missing pending rule {}",
                                mapping.rule_id
                            ))
                        })?;
                        Ok(replacements.get(&mapping_rule_key(old_rule)).copied())
                    })
                    .collect::<Result<Vec<_>, MappingError>>()?
            };
            let mut remapped = remapped.into_iter();
            pending.retain_mut(|mapping| {
                if let Some(new_id) = remapped.next().expect("one result per pending rule") {
                    mapping.rule_id = new_id;
                    true
                } else {
                    false
                }
            });
        }
        *rules = instance_mapping_rules;
        *rule_index = indexed_rules;
        info!("Mapping rules read: {} rule(s)", rule_count);
        Ok(())
    }

    /// Validate the entire delta, then append rules and index entries together.
    /// Existing rule IDs and pending mappings are unchanged.
    pub fn append_mapping_rules(
        &self,
        additions: Vec<InstanceMappingRule>,
    ) -> Result<(), MappingError> {
        let delta_index = InstanceMappingRuleIndex::new(&additions)?;
        let _gate = self.onboarding_gate.lock();
        let mut rules = self.instance_mapping_rules_p.write();
        let mut rule_index = self.instance_mapping_rule_index_p.write();
        if rule_index.len() != rules.len() {
            return Err(MappingError::InvalidRule(
                "rule vector and source index disagree before append".to_string(),
            ));
        }
        rules.try_reserve(additions.len()).map_err(|error| {
            MappingError::InvalidRule(format!("cannot reserve mapping rule delta: {error}"))
        })?;
        rule_index.append(delta_index)?;
        rules.extend(additions);
        Ok(())
    }

    /// Undo an appended suffix without rebuilding the remaining source index.
    /// Restore pending mappings first. A live reference into the suffix is an error.
    pub fn truncate_mapping_rules(&self, len: usize) -> Result<(), MappingError> {
        let _gate = self.onboarding_gate.lock();
        let mut rules = self.instance_mapping_rules_p.write();
        let mut rule_index = self.instance_mapping_rule_index_p.write();
        let pending = self.incomplete_mappings_p.read();
        if len > rules.len() {
            return Err(MappingError::InvalidRule(format!(
                "cannot truncate {} mapping rules to {len}",
                rules.len(),
            )));
        }
        if pending.iter().any(|mapping| mapping.rule_id >= len) {
            return Err(MappingError::InvalidRule(
                "pending mapping still refers to the truncated suffix".to_string(),
            ));
        }
        rule_index.truncate(&rules, len)?;
        rules.truncate(len);
        Ok(())
    }

    /// Restore pending rules without resurrecting a source removed while the
    /// application prepared another source. The gate orders this with lifecycle
    /// cleanup; matching both ID and name also rejects recycled SQLite IDs.
    pub fn restore_pending_mappings_for_active_sources(
        &self,
        snapshot: Vec<IncompleteMapping>,
    ) -> Result<(), MappingError> {
        let _gate = self.onboarding_gate.lock();
        let active = self
            .map_db_p
            .read()
            .connect()?
            .active_lower_server_names()?;
        *self.incomplete_mappings_p.write() = snapshot
            .into_iter()
            .filter(|mapping| {
                active.get(&mapping.source_server_id) == Some(&mapping.source_server_name)
            })
            .collect();
        Ok(())
    }

    pub fn add_lower_server(
        &self,
        server_p: &Arc<RwLock<Server>>,
        url: &str,
        name: &str,
    ) -> Result<(), AddServerError> {
        self.add_lower_server_with_timeout(server_p, url, name, Duration::from_secs(180))
    }

    pub fn add_lower_server_with_timeout(
        &self,
        server_p: &Arc<RwLock<Server>>,
        url: &str,
        name: &str,
        timeout: Duration,
    ) -> Result<(), AddServerError> {
        self.add_lower_server_with_control(
            server_p,
            url,
            name,
            SessionOperationControl::new(timeout),
        )
    }

    /// The control belongs to the complete onboarding operation, including any
    /// caller-side preparation. Cancellation never acquires the session lock.
    pub fn add_lower_server_with_control(
        &self,
        server_p: &Arc<RwLock<Server>>,
        url: &str,
        name: &str,
        operation_control: SessionOperationControl,
    ) -> Result<(), AddServerError> {
        // Keep the lock ordering consistent with removal/reaping. A new worker
        // cannot overwrite the bookkeeping of a still-running predecessor.
        let mut related = self.lower_servers_related_p.write();
        let mut infos = self.lower_servers_info_p.write();
        if related.contains_key(name) || infos.contains_key(name) {
            return Err(AddServerError::ServerAlreadyAdded);
        }
        let postfix = name
            .rsplit("___")
            .next()
            .ok_or_else(|| AddServerError::InvalidName {
                name: name.to_string(),
            })?
            .to_string();
        infos.insert(
            name.to_string(),
            LowerServerInfo {
                aggregation_finished: false,
                removal_in_progress: false,
                aggregation_error: None,
                cleanup_complete: false,
            },
        );
        let removal_requested = Arc::new(AtomicBool::new(false));
        let cleanup_succeeded = Arc::new(AtomicBool::new(false));
        let worker_aggregation = self.clone();
        let worker_server = server_p.clone();
        let worker_name = name.to_string();
        let worker_url = url.to_string();
        let worker_control = operation_control.clone();
        let worker_cleanup = cleanup_succeeded.clone();
        let thread = thread::spawn(move || {
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                lower_server_thread(
                    worker_aggregation.clone(),
                    worker_server,
                    worker_control,
                    worker_cleanup,
                    worker_url,
                    worker_name.clone(),
                    postfix,
                )
            }))
            .unwrap_or(Err(LowerServerError::WorkerPanicked));
            let mut infos = worker_aggregation.lower_servers_info_p.write();
            if let Some(info) = infos.get_mut(&worker_name) {
                info.aggregation_finished = false;
                info.aggregation_error = outcome.as_ref().err().map(ToString::to_string);
                // The reaper acknowledges completion only after joining this worker.
                info.cleanup_complete = false;
            }
            outcome
        });
        related.insert(
            name.to_string(),
            LowerServerThreading {
                thread_handle: Some(thread),
                operation_control,
                removal_requested,
                cleanup_succeeded,
            },
        );
        Ok(())
    }

    /// Request cancellation. Cleanup and worker termination are acknowledged by
    /// lower_server_cleanup_complete; dropping a JoinHandle is not cancellation.
    pub fn remove_lower_server(&self, name: &str) -> Result<(), RemoveLowerServerError> {
        {
            let related = self.lower_servers_related_p.read();
            if let Some(worker) = related.get(name) {
                worker.removal_requested.store(true, Ordering::Release);
                worker.operation_control.cancel();
                if let Some(info) = self.lower_servers_info_p.write().get_mut(name) {
                    info.removal_in_progress = true;
                    info.aggregation_finished = false;
                }
                return Ok(());
            }
        }
        if self.lower_servers_info_p.write().remove(name).is_some() {
            Ok(())
        } else {
            Err(RemoveLowerServerError::NameNotFound(name.to_string()))
        }
    }

    /// Bounded, non-blocking status probe. True means the worker has terminated,
    /// been joined, and all source-local cleanup succeeded. Failed cleanup is
    /// deliberately retained as an unfinished entry so callers must fail closed.
    pub fn lower_server_cleanup_complete(&self, name: &str) -> bool {
        let Some(mut related) = self.lower_servers_related_p.try_write() else {
            return false;
        };
        let Some(worker) = related.get_mut(name) else {
            return true;
        };
        if let Some(handle) = worker.thread_handle.as_ref() {
            if !handle.is_finished() {
                return false;
            }
        }
        let Some(mut infos) = self.lower_servers_info_p.try_write() else {
            return false;
        };
        if let Some(handle) = worker.thread_handle.take() {
            if handle.join().is_err() {
                return false;
            }
        }
        if !worker.cleanup_succeeded.load(Ordering::Acquire) {
            return false;
        }
        if worker.removal_requested.load(Ordering::Acquire) {
            infos.remove(name);
        } else if let Some(info) = infos.get_mut(name) {
            info.cleanup_complete = true;
        }
        related.remove(name);
        true
    }

    /// Source-scoped read-only diagnostics for isolated fault-injection fixtures.
    pub fn lower_server_debug_counts(
        &self,
        name: &str,
    ) -> Result<LowerServerDebugCounts, MappingError> {
        let database = self.map_db_p.read().connect()?;
        let mut counts = database.lower_server_debug_counts(name)?;
        let address_space = self.address_space_p.read();
        counts.root_folders = address_space
            .find_hierarchical_references(&NodeId::objects_folder_id())
            .unwrap_or_default()
            .iter()
            .filter(|id| {
                address_space
                    .find_node(id)
                    .map(|node| node.as_node().browse_name().name.value().as_deref() == Some(name))
                    .unwrap_or(false)
            })
            .count();
        counts.active_session = database
            .lower_server_id_by_name(name)?
            .map(|id| self.lower_server_sessions_p.read().contains_key(&id))
            .unwrap_or(false);
        counts.worker_present = self.lower_servers_related_p.read().contains_key(name);
        Ok(counts)
    }

    #[instrument(level = "trace", skip(self), err, ret)]
    pub fn read_state(&self, name: &str) -> Result<String, ReadStateError> {
        let lower_servers_info = self.lower_servers_info_p.read();
        let Some(lsr) = lower_servers_info.get(name) else {
            return Err(ReadStateError::NameNotFound(name.to_string()));
        };
        let ret_val = serde_json::to_string(&(name, lsr))?;
        return Ok(ret_val);
    }

    #[instrument(level = "trace", skip_all, err, ret)]
    pub fn read_state_all(&self) -> Result<String, ReadStateError> {
        let lower_servers_info = self.lower_servers_info_p.read();
        let infos: Vec<(&String, &LowerServerInfo)> = lower_servers_info.iter().collect();
        let ret_val = serde_json::to_string(&infos)?;
        return Ok(ret_val);
    }
}

/// Counts are scoped through the source row, including cascading child tables.
#[derive(Debug, Default, serde_derive::Serialize)]
pub struct LowerServerDebugCounts {
    pub lower_servers: usize,
    pub namespace_mappings: usize,
    pub type_mappings: usize,
    pub references: usize,
    pub subscriptions: usize,
    pub monitored_items: usize,
    pub root_folders: usize,
    pub active_session: bool,
    pub worker_present: bool,
}

struct SourceSessionRunner {
    session: Arc<RwLock<Session>>,
    stop: Option<oneshot::Sender<SessionCommand>>,
    thread: Option<thread::JoinHandle<()>>,
    control: SessionOperationControl,
}

impl SourceSessionRunner {
    fn new(session: Arc<RwLock<Session>>, control: SessionOperationControl) -> Self {
        let (tx, rx) = oneshot::channel();
        let running_session = session.clone();
        let thread = thread::spawn(move || Session::run_loop(running_session, 10, rx));
        Self {
            session,
            stop: Some(tx),
            thread: Some(thread),
            control,
        }
    }

    fn stopped(&self) -> bool {
        self.thread
            .as_ref()
            .map(|thread| thread.is_finished())
            .unwrap_or(true)
    }

    fn stop_and_join(&mut self) -> Result<(), LowerServerError> {
        self.control.cancel();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(SessionCommand::Stop);
        }
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| LowerServerError::SessionWorkerPanicked)?;
        }
        // Explicitly join transport even when a service still owns another Arc.
        self.session.read().disconnect();
        Ok(())
    }
}

impl Drop for SourceSessionRunner {
    fn drop(&mut self) {
        let _ = self.stop_and_join();
    }
}

#[instrument(err, skip_all)]
fn lower_server_thread(
    aggregation: AggregationServer,
    server_p: Arc<RwLock<Server>>,
    control: SessionOperationControl,
    cleanup_succeeded: Arc<AtomicBool>,
    url: String,
    name: String,
    postfix: String,
) -> Result<(), LowerServerError> {
    let total_started = Instant::now();
    // A failed or cancelled queued source must not wait for the active source's
    // mutex before observing its own monotonic deadline.
    let gate = loop {
        if let Err(error) = control.check() {
            cleanup_succeeded.store(true, Ordering::Release);
            return Err(error.into());
        }
        if let Some(gate) = aggregation.onboarding_gate.try_lock() {
            break gate;
        }
        thread::sleep(Duration::from_millis(10));
    };
    let database = match aggregation.map_db_p.read().connect() {
        Ok(database) => database,
        Err(error) => {
            cleanup_succeeded.store(true, Ordering::Release);
            return Err(error.into());
        }
    };
    let root_folder = NodeId::next_numeric(1);
    let mut source = LowerServer::new(&name, 0, &url, &postfix, &root_folder);
    source.operation_control = Some(control.clone());
    let mut source_inserted = false;
    let mut runner: Option<SourceSessionRunner> = None;
    // Shared type and pending-rule state is published only after every phase
    // succeeds. Network calls never hold the shared global-type/namespace locks.
    let original_types = aggregation.global_type_hashmap_p.read().clone();
    let original_namespaces = aggregation.standard_namespaces_p.read().clone();
    let original_incomplete = aggregation.incomplete_mappings_p.read().clone();
    let staged_types = Arc::new(RwLock::new(original_types.clone()));
    let mut staged_namespaces = original_namespaces.clone();
    let staged_incomplete = Arc::new(RwLock::new(original_incomplete.clone()));
    let onboarding = catch_unwind(AssertUnwindSafe(|| -> Result<(), LowerServerError> {
        control.check()?;
        if !aggregation.address_space_p.write().add_folder_with_id(
            &root_folder,
            QualifiedName::new(1, &name),
            &name,
            &NodeId::objects_folder_id(),
        ) {
            return Err(LowerServerError::AddressSpaceError);
        }
        source.id = database.insert_lserver(&name, &root_folder)?;
        source_inserted = true;
        let setup_started = Instant::now();
        let mut client = ClientBuilder::new()
            .application_name("ACOR DT Aggregation Server")
            .application_uri("http://acor.plcm.tu-darmstadt.de/aggregation-server-instance/")
            .session_retry_limit(5)
            .session_retry_interval(1000)
            .session_timeout(60_000)
            .request_timeout(LOWER_SERVER_REQUEST_TIMEOUT_MS)
            .operation_control(control.clone())
            .max_byte_string_length(URDF_MAX_BYTE_STRING_LENGTH)
            .max_message_size(URDF_MAX_MESSAGE_SIZE)
            .max_chunk_count(0)
            .ignore_clock_skew()
            .client()
            .ok_or(LowerServerError::CreateClientError)?;
        let session = client
            .connect_to_endpoint(
                EndpointDescription::from(url.as_str()),
                IdentityToken::Anonymous,
            )
            .map_err(|status_code| LowerServerError::ConnectionError { status_code })?;
        runner = Some(SourceSessionRunner::new(session.clone(), control.clone()));
        log_phase_timing(&name, "vendor_setup", setup_started.elapsed());
        control.check()?;
        let started = Instant::now();
        aggregate_namespaces(
            &aggregation.address_space_p,
            &session,
            &database,
            &mut source,
        )?;
        log_phase_timing(&name, "vendor_namespace", started.elapsed());
        control.check()?;
        let started = Instant::now();
        aggregate_types(
            &aggregation.address_space_p,
            &session,
            &staged_types,
            &database,
            &mut staged_namespaces,
            &mut source,
        )?;
        log_phase_timing(&name, "vendor_type", started.elapsed());
        control.check()?;
        let started = Instant::now();
        aggregate_instances_indexed(
            &session,
            &aggregation.address_space_p,
            aggregation.instance_mapping_rules_p.clone(),
            aggregation.instance_mapping_rule_index_p.clone(),
            &database,
            &mut source,
            staged_incomplete.clone(),
        )?;
        log_phase_timing(&name, "vendor_instance", started.elapsed());

        // Snapshot subscription parameters before making any remote request;
        // no server/session-manager lock may be held while the source stalls.
        let manager = server_p.read().session_manager().clone();
        let subscriptions: Vec<_> = {
            let manager = manager.read();
            manager
                .sessions
                .values()
                .flat_map(|session| {
                    let session = session.read();
                    session
                        .subscriptions()
                        .subscriptions()
                        .values()
                        .map(|sub| {
                            (
                                sub.publishing_interval(),
                                sub.max_lifetime_count(),
                                sub.max_keep_alive_count(),
                                sub.priority(),
                                sub.publishing_enabled(),
                                sub.subscription_id(),
                                sub.aggregation_notifications(),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        };
        for (interval, lifetime, keepalive, priority, enabled, upper_id, notifications) in
            subscriptions
        {
            control.check()?;
            let lower_id = session
                .read()
                .create_subscription(
                    interval,
                    lifetime,
                    keepalive,
                    0,
                    priority,
                    enabled,
                    AggregationSubscriptionNotification {
                        notifications,
                        map_db_p: aggregation.map_db_p.clone(),
                        lserver_id: source.id,
                        aggserver_sub_id: upper_id,
                    },
                )
                .map_err(|status_code| LowerServerError::ConnectionError { status_code })?;
            database.insert_subscription(source.id, lower_id, upper_id)?;
        }
        control.check()?;
        *aggregation.global_type_hashmap_p.write() = staged_types.read().clone();
        *aggregation.standard_namespaces_p.write() = staged_namespaces.clone();
        *aggregation.incomplete_mappings_p.write() = staged_incomplete.read().clone();
        aggregation
            .lower_server_sessions_p
            .write()
            .insert(source.id, session);
        // The parent still enforces its own end-to-end deadline. Disarming
        // merely allows the established session to remain connected afterwards.
        control.disarm();
        control.check()?;
        let mut infos = aggregation.lower_servers_info_p.write();
        let info = infos
            .get_mut(&name)
            .ok_or_else(|| LowerServerError::NameNotFoundError { name: name.clone() })?;
        info.aggregation_finished = true;
        info.aggregation_error = None;
        log_phase_timing(&name, "vendor_total", total_started.elapsed());
        Ok(())
    }))
    .unwrap_or(Err(LowerServerError::WorkerPanicked));
    let committed = onboarding.is_ok();
    if !committed {
        *aggregation.global_type_hashmap_p.write() = original_types;
        *aggregation.standard_namespaces_p.write() = original_namespaces;
        *aggregation.incomplete_mappings_p.write() = original_incomplete;
    } else {
        drop((original_types, original_namespaces, original_incomplete));
    }
    drop((staged_types, staged_namespaces, staged_incomplete));
    let mut outcome = onboarding;
    let mut gate = Some(gate);
    if committed {
        drop(gate.take());
        loop {
            if control.is_cancelled_or_expired() {
                break;
            }
            if runner
                .as_ref()
                .map(|runner| runner.stopped())
                .unwrap_or(true)
            {
                outcome = Err(LowerServerError::ConnectionLost { name: name.clone() });
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
    // Stop and JOIN the session before deleting any mapping. The old source's
    // subscription callbacks cannot write into a subsequent source generation.
    if let Some(mut runner) = runner.take() {
        if let Err(error) = runner.stop_and_join() {
            outcome = Err(error);
        }
        drop(runner);
    } else {
        control.cancel();
    }
    let _cleanup_gate = gate.unwrap_or_else(|| aggregation.onboarding_gate.lock());
    if let Some(info) = aggregation.lower_servers_info_p.write().get_mut(&name) {
        info.aggregation_finished = false;
        info.removal_in_progress = true;
    }
    if source_inserted {
        aggregation
            .lower_server_sessions_p
            .write()
            .remove(&source.id);
    }
    {
        let mut address_space = aggregation.address_space_p.write();
        if !committed {
            address_space.remove_aggregation_nodes_exact(&source.created_nodes);
        }
        address_space.remove_aggregation_nodes_exact(&[root_folder]);
        // NamespaceArray remains append-only; exposed namespace indices are
        // never reused or renumbered after a failed attempt.
    }
    if source_inserted {
        aggregation
            .incomplete_mappings_p
            .write()
            .retain(|mapping| mapping.source_server_id != source.id);
        database.delete_lserver(source.id)?;
    }
    cleanup_succeeded.store(true, Ordering::Release);
    outcome
}

#[cfg(test)]
mod subscription_lifecycle_tests {
    use super::*;
    use crate::server::diagnostics::ServerDiagnostics;
    use crate::server::subscriptions::subscription::Subscription;

    #[test]
    fn subscription_cleanup_continues_after_missing_source_mapping() {
        let address_space = Arc::new(RwLock::new(AddressSpace::default()));
        let aggregation_server = AggregationServer::new(&address_space).unwrap();
        let map_db = aggregation_server.map_db_p.read().connect().unwrap();
        map_db
            .insert_lserver(&"without_subscription".to_string(), &NodeId::new(1, 100))
            .unwrap();
        let mapped_source = map_db
            .insert_lserver(&"with_subscription".to_string(), &NodeId::new(1, 101))
            .unwrap();
        map_db.insert_subscription(mapped_source, 10, 20).unwrap();
        let mapping = map_db.get_lserver_sub_id(mapped_source, 20).unwrap();
        map_db
            .insert_monitored_item(mapping.internal_sub_id, 30, 40)
            .unwrap();
        let subscription = Subscription::new(
            Arc::new(RwLock::new(ServerDiagnostics::default())),
            20,
            true,
            1000.0,
            300,
            100,
            0,
            Some(aggregation_server),
        );

        // The earlier source has no matching subscription and the mapped source
        // has no connected client session. Neither condition may prevent local
        // mapping cleanup for the subscription being removed.
        drop(subscription);

        assert!(map_db.get_lserver_sub_id(mapped_source, 20).is_err());
        assert!(map_db
            .get_aggserver_mitem_id(mapping.internal_sub_id, 30)
            .is_err());
    }
}

#[cfg(test)]
mod onboarding_lifecycle_tests {
    use super::*;
    use std::sync::mpsc;

    fn test_worker(aggregation: &AggregationServer, name: &str, cleanup: bool) -> mpsc::Sender<()> {
        let (release, wait) = mpsc::channel();
        let thread = thread::spawn(move || {
            let _ = wait.recv();
            Ok(())
        });
        aggregation.lower_servers_info_p.write().insert(
            name.into(),
            LowerServerInfo {
                aggregation_finished: false,
                removal_in_progress: false,
                aggregation_error: None,
                cleanup_complete: false,
            },
        );
        aggregation.lower_servers_related_p.write().insert(
            name.into(),
            LowerServerThreading {
                thread_handle: Some(thread),
                operation_control: SessionOperationControl::new(Duration::from_secs(30)),
                removal_requested: Arc::new(AtomicBool::new(false)),
                cleanup_succeeded: Arc::new(AtomicBool::new(cleanup)),
            },
        );
        release
    }

    #[test]
    fn cancellation_does_not_acknowledge_or_detach_a_live_worker() {
        let aggregation =
            AggregationServer::new(&Arc::new(RwLock::new(AddressSpace::default()))).unwrap();
        let release = test_worker(&aggregation, "stalled", true);
        aggregation.remove_lower_server("stalled").unwrap();
        assert!(!aggregation.lower_server_cleanup_complete("stalled"));
        {
            let related = aggregation.lower_servers_related_p.read();
            let worker = related.get("stalled").unwrap();
            assert!(worker.operation_control.is_cancelled_or_expired());
            assert!(worker.thread_handle.is_some());
        }
        release.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !aggregation.lower_server_cleanup_complete("stalled") {
            assert!(Instant::now() < deadline, "worker was not joined");
            thread::yield_now();
        }
        assert!(!aggregation
            .lower_servers_info_p
            .read()
            .contains_key("stalled"));
        assert!(!aggregation
            .lower_servers_related_p
            .read()
            .contains_key("stalled"));
    }

    #[test]
    fn failed_cleanup_remains_quarantined_after_worker_exit() {
        let aggregation =
            AggregationServer::new(&Arc::new(RwLock::new(AddressSpace::default()))).unwrap();
        let release = test_worker(&aggregation, "failed-cleanup", false);
        release.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            assert!(!aggregation.lower_server_cleanup_complete("failed-cleanup"));
            if aggregation
                .lower_servers_related_p
                .read()
                .get("failed-cleanup")
                .unwrap()
                .thread_handle
                .is_none()
            {
                break;
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        assert!(aggregation
            .lower_servers_related_p
            .read()
            .contains_key("failed-cleanup"));
    }

    #[test]
    fn cleanup_probe_does_not_wait_for_busy_status_lock() {
        let aggregation =
            AggregationServer::new(&Arc::new(RwLock::new(AddressSpace::default()))).unwrap();
        let release = test_worker(&aggregation, "busy-status", true);
        release.send(()).unwrap();
        let _status_lock = aggregation.lower_servers_info_p.write();
        assert!(!aggregation.lower_server_cleanup_complete("busy-status"));
    }

    #[test]
    fn rule_restore_and_validation_keep_rules_and_index_consistent() {
        let aggregation =
            AggregationServer::new(&Arc::new(RwLock::new(AddressSpace::default()))).unwrap();
        let rule = |source: &str| {
            serde_json::from_value::<InstanceMappingRule>(serde_json::json!({
                "source_node": ["Source"], "target_node": ["Target"], "ref_type": ["Organizes"],
                "is_forward": true, "source_id": source,
            }))
            .unwrap()
        };
        aggregation
            .replace_mapping_rules(vec![rule("opc.tcp://source-a")])
            .unwrap();
        let original = aggregation.instance_mapping_rules_p.read().clone();
        aggregation
            .replace_mapping_rules(vec![rule("opc.tcp://source-b")])
            .unwrap();
        aggregation.replace_mapping_rules(original).unwrap();
        assert_eq!(
            aggregation.instance_mapping_rules_p.read()[0]
                .source_id
                .as_deref(),
            Some("opc.tcp://source-a")
        );
        assert_eq!(
            aggregation
                .instance_mapping_rule_index_p
                .read()
                .indices_for_source("opc.tcp://source-a"),
            vec![0]
        );
        assert!(aggregation
            .instance_mapping_rule_index_p
            .read()
            .indices_for_source("opc.tcp://source-b")
            .is_empty());
        let mut invalid = rule("opc.tcp://invalid");
        invalid.source_node.clear();
        assert!(aggregation.replace_mapping_rules(vec![invalid]).is_err());
        assert_eq!(
            aggregation.instance_mapping_rules_p.read()[0]
                .source_id
                .as_deref(),
            Some("opc.tcp://source-a")
        );
        assert_eq!(
            aggregation
                .instance_mapping_rule_index_p
                .read()
                .indices_for_source("opc.tcp://source-a"),
            vec![0]
        );
    }

    fn incremental_test_rule(source: Option<&str>, target: &str) -> InstanceMappingRule {
        serde_json::from_value(serde_json::json!({
            "source_node": ["Source"], "target_node": [target], "ref_type": ["Organizes"],
            "is_forward": true, "source_id": source,
        }))
        .unwrap()
    }

    fn incremental_test_pending(rule_id: usize) -> IncompleteMapping {
        IncompleteMapping {
            source_server_id: 1,
            source_server_name: "source".into(),
            rule_id,
            source_nid: NodeId::new(2, 1),
            source_bname: QualifiedName::null(),
            source_dname: crate::types::LocalizedText::null(),
            source_class: crate::types::NodeClass::Object,
            source_type: NodeId::null(),
        }
    }

    #[test]
    fn incremental_append_validates_entire_delta_without_partial_publication() {
        let aggregation =
            AggregationServer::new(&Arc::new(RwLock::new(AddressSpace::default()))).unwrap();
        aggregation
            .append_mapping_rules(vec![incremental_test_rule(Some("opc.tcp://a"), "A")])
            .unwrap();
        aggregation
            .incomplete_mappings_p
            .write()
            .push(incremental_test_pending(0));
        let valid = incremental_test_rule(Some("opc.tcp://b"), "B");
        let mut invalid = incremental_test_rule(Some("opc.tcp://c"), "C");
        invalid.source_node.clear();
        assert!(aggregation
            .append_mapping_rules(vec![valid, invalid])
            .is_err());
        assert_eq!(aggregation.instance_mapping_rules_p.read().len(), 1);
        assert_eq!(
            aggregation
                .instance_mapping_rule_index_p
                .read()
                .indices_for_source("opc.tcp://a"),
            vec![0]
        );
        assert!(aggregation
            .instance_mapping_rule_index_p
            .read()
            .indices_for_source("opc.tcp://b")
            .is_empty());
        assert_eq!(aggregation.incomplete_mappings_p.read()[0].rule_id, 0);
    }

    #[test]
    fn incremental_append_and_truncate_preserve_existing_pending_rule_ids() {
        let aggregation =
            AggregationServer::new(&Arc::new(RwLock::new(AddressSpace::default()))).unwrap();
        aggregation
            .append_mapping_rules(vec![incremental_test_rule(Some("opc.tcp://a"), "A")])
            .unwrap();
        aggregation
            .incomplete_mappings_p
            .write()
            .push(incremental_test_pending(0));
        aggregation
            .append_mapping_rules(vec![
                incremental_test_rule(None, "Shared"),
                incremental_test_rule(Some("opc.tcp://b"), "B"),
                incremental_test_rule(Some("opc.tcp://a/"), "A2"),
            ])
            .unwrap();
        assert_eq!(
            aggregation
                .instance_mapping_rule_index_p
                .read()
                .indices_for_source("opc.tcp://a/"),
            vec![0, 1, 3]
        );
        assert_eq!(
            aggregation
                .instance_mapping_rule_index_p
                .read()
                .indices_for_source("opc.tcp://b/"),
            vec![1, 2]
        );
        assert_eq!(aggregation.incomplete_mappings_p.read()[0].rule_id, 0);
        aggregation.truncate_mapping_rules(1).unwrap();
        assert_eq!(aggregation.instance_mapping_rules_p.read().len(), 1);
        assert_eq!(
            aggregation
                .instance_mapping_rule_index_p
                .read()
                .indices_for_source("opc.tcp://a"),
            vec![0]
        );
        assert!(aggregation
            .instance_mapping_rule_index_p
            .read()
            .indices_for_source("opc.tcp://b")
            .is_empty());
        assert_eq!(aggregation.incomplete_mappings_p.read()[0].rule_id, 0);
        aggregation
            .append_mapping_rules(vec![incremental_test_rule(Some("opc.tcp://c"), "C")])
            .unwrap();
        assert_eq!(
            aggregation
                .instance_mapping_rule_index_p
                .read()
                .indices_for_source("opc.tcp://c"),
            vec![1]
        );
    }

    #[test]
    fn incremental_truncate_rejects_live_suffix_and_invalid_bounds_atomically() {
        let aggregation =
            AggregationServer::new(&Arc::new(RwLock::new(AddressSpace::default()))).unwrap();
        aggregation
            .append_mapping_rules(vec![
                incremental_test_rule(Some("opc.tcp://a"), "A"),
                incremental_test_rule(Some("opc.tcp://b"), "B"),
            ])
            .unwrap();
        aggregation
            .incomplete_mappings_p
            .write()
            .push(incremental_test_pending(1));
        assert!(aggregation.truncate_mapping_rules(3).is_err());
        assert!(aggregation.truncate_mapping_rules(1).is_err());
        assert_eq!(aggregation.instance_mapping_rules_p.read().len(), 2);
        assert_eq!(
            aggregation
                .instance_mapping_rule_index_p
                .read()
                .indices_for_source("opc.tcp://b"),
            vec![1]
        );
        assert_eq!(aggregation.incomplete_mappings_p.read()[0].rule_id, 1);
        aggregation.incomplete_mappings_p.write().clear();
        aggregation.truncate_mapping_rules(0).unwrap();
        assert!(aggregation.instance_mapping_rules_p.read().is_empty());
        assert_eq!(aggregation.instance_mapping_rule_index_p.read().len(), 0);
    }

    #[test]
    fn replacement_remaps_surviving_pending_rules_and_discards_only_removed_rules() {
        let aggregation =
            AggregationServer::new(&Arc::new(RwLock::new(AddressSpace::default()))).unwrap();
        let a = incremental_test_rule(Some("opc.tcp://a"), "A");
        let b = incremental_test_rule(Some("opc.tcp://b"), "B");
        let c = incremental_test_rule(Some("opc.tcp://c"), "C");
        aggregation
            .replace_mapping_rules(vec![a.clone(), b, c.clone()])
            .unwrap();
        *aggregation.incomplete_mappings_p.write() = (0..3).map(incremental_test_pending).collect();
        aggregation.replace_mapping_rules(vec![c, a]).unwrap();
        let pending = aggregation.incomplete_mappings_p.read();
        assert_eq!(
            pending
                .iter()
                .map(|mapping| mapping.rule_id)
                .collect::<Vec<_>>(),
            vec![1, 0]
        );
        let rules = aggregation.instance_mapping_rules_p.read();
        assert_eq!(
            rules[pending[0].rule_id].source_id.as_deref(),
            Some("opc.tcp://a")
        );
        assert_eq!(
            rules[pending[1].rule_id].source_id.as_deref(),
            Some("opc.tcp://c")
        );
    }

    #[test]
    fn replacement_rejects_missing_pending_identity_without_mutation() {
        let aggregation =
            AggregationServer::new(&Arc::new(RwLock::new(AddressSpace::default()))).unwrap();
        aggregation
            .replace_mapping_rules(vec![incremental_test_rule(Some("opc.tcp://a"), "A")])
            .unwrap();
        aggregation
            .incomplete_mappings_p
            .write()
            .push(incremental_test_pending(2));
        assert!(aggregation
            .replace_mapping_rules(vec![incremental_test_rule(Some("opc.tcp://b"), "B")])
            .is_err());
        assert_eq!(
            aggregation.instance_mapping_rules_p.read()[0]
                .source_id
                .as_deref(),
            Some("opc.tcp://a")
        );
        assert_eq!(
            aggregation
                .instance_mapping_rule_index_p
                .read()
                .indices_for_source("opc.tcp://a"),
            vec![0]
        );
        assert!(aggregation
            .instance_mapping_rule_index_p
            .read()
            .indices_for_source("opc.tcp://b")
            .is_empty());
        assert_eq!(aggregation.incomplete_mappings_p.read()[0].rule_id, 2);
    }

    #[test]
    fn restoring_pending_rules_cannot_resurrect_removed_or_recycled_source() {
        let aggregation =
            AggregationServer::new(&Arc::new(RwLock::new(AddressSpace::default()))).unwrap();
        let db = aggregation.map_db_p.read().connect().unwrap();
        let a = db
            .insert_lserver(&"A".into(), &NodeId::new(1, 501))
            .unwrap();
        let b = db
            .insert_lserver(&"B".into(), &NodeId::new(1, 502))
            .unwrap();
        let pending = |id, name: &str| IncompleteMapping {
            source_server_id: id,
            source_server_name: name.into(),
            rule_id: 0,
            source_nid: NodeId::new(2, 1),
            source_bname: QualifiedName::null(),
            source_dname: crate::types::LocalizedText::null(),
            source_class: crate::types::NodeClass::Object,
            source_type: NodeId::null(),
        };
        let snapshot = vec![pending(a, "A"), pending(b, "B")];
        db.delete_lserver(a).unwrap();
        aggregation
            .restore_pending_mappings_for_active_sources(snapshot.clone())
            .unwrap();
        assert_eq!(aggregation.incomplete_mappings_p.read().len(), 1);
        assert_eq!(
            aggregation.incomplete_mappings_p.read()[0].source_server_name,
            "B"
        );
        db.delete_lserver(b).unwrap();
        let recycled = db
            .insert_lserver(&"C".into(), &NodeId::new(1, 503))
            .unwrap();
        assert_eq!(recycled, a, "fixture must exercise SQLite row-id reuse");
        aggregation
            .restore_pending_mappings_for_active_sources(snapshot)
            .unwrap();
        assert!(aggregation.incomplete_mappings_p.read().is_empty());
    }

    #[test]
    fn exact_node_rollback_preserves_preexisting_shared_children() {
        let mut address = AddressSpace::default();
        let own = NodeId::new(1, 800_001);
        let shared = NodeId::new(1, 800_002);
        assert!(address.add_folder_with_id(&own, "own", "own", &NodeId::objects_folder_id()));
        assert!(address.add_folder_with_id(&shared, "shared", "shared", &own));
        address.remove_aggregation_nodes_exact(&[own.clone()]);
        assert!(!address.node_exists(&own));
        assert!(address.node_exists(&shared));
        assert!(address
            .find_hierarchical_references(&own)
            .unwrap_or_default()
            .is_empty());
    }
}
