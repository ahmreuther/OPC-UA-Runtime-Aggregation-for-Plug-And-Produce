use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use crate::client::prelude::{ClientBuilder, IdentityToken, Session, SessionCommand};
use crate::server::address_space::AddressSpace;
use crate::server::aggregation_server::instance_aggregation::{
    aggregate_instances_indexed, InstanceMappingRuleIndex,
};
use crate::server::aggregation_server::services::subscription::AggregationSubscriptionNotification;
use crate::sync::RwLock;
use crate::types::{EndpointDescription, NodeId, QualifiedName};
use bimap::BiMap;
use serde::Deserialize;
use serde_json;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::{error, info, instrument, warn};

use crate::prelude::{Server, SubscriptionService as ClientSubscriptionService};
use crate::server::aggregation_server::error_types::{
    AddServerError, LowerServerError, MappingError, ReadStateError, RemoveLowerServerError,
};
use crate::server::aggregation_server::map_db::MapDatabasePool;
use crate::server::aggregation_server::namespace_aggregation::aggregate_namespaces;
use crate::server::aggregation_server::type_aggregation::aggregate_types;
use crate::server::aggregation_server::util_traits::AddressSpaceAdditions;
use crate::server::aggregation_server::util_types::{
    InstanceMappingRule, LowerServer, LowerServerInfo, LowerServerThreading, StandardizedNamespace,
};

use super::util_types::IncompleteMapping;

const URDF_LOWER_SERVER_REQUEST_TIMEOUT_MS: u32 = 60_000;
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
        let indexed_rules = InstanceMappingRuleIndex::new(&instance_mapping_rules)?;
        let rule_count = instance_mapping_rules.len();
        let mut rules = self.instance_mapping_rules_p.write();
        let mut rule_index = self.instance_mapping_rule_index_p.write();
        *rules = instance_mapping_rules;
        *rule_index = indexed_rules;
        info!("Mapping rules read: {} rule(s)", rule_count);
        return Ok(());
    }

    #[instrument(level = "info", skip(self, server_p), err, ret)]
    pub fn add_lower_server(
        &self,
        server_p: &Arc<RwLock<Server>>,
        url: &str,
        name: &str,
    ) -> Result<(), AddServerError> {
        let mut lower_server_info = self.lower_servers_info_p.write();
        if lower_server_info.contains_key(name) {
            return Err(AddServerError::ServerAlreadyAdded);
        }

        let mut lower_server_related = self.lower_servers_related_p.write();
        if lower_server_related.contains_key(name) {
            warn!(
                "LowerServerRelated HashMap entry still exists although the LowerServerInfo \
                HashMap entry doesn't. This means something went wrong in the \
                Lower Server Thread. Removing it now."
            );
            if let Some(lsr) = lower_server_related.remove(name) {
                if lsr.remove_lserver_tx.send(()).is_err() {
                    warn!(
                        "Lower server thread message receiver was dropped early. \
                            This could mean that the lower server thread crashed."
                    );
                };
            };
        }
        drop(lower_server_related);

        info!(message = "Adding lower server.", url = ?url, name = ?name);

        let Some(postfix) = name.rsplit("___").next() else {
            return Err(AddServerError::InvalidName {
                name: name.to_string(),
            });
        };

        lower_server_info.insert(
            name.to_string(),
            LowerServerInfo {
                aggregation_finished: false,
                removal_in_progress: false,
                aggregation_error: None,
            },
        );
        drop(lower_server_info);

        let (tx, rx) = oneshot::channel();

        let t_aggregation_server = self.clone();
        let t_server_session_p = server_p.clone();
        let t_url = url.to_string();
        let t_name = name.to_string();
        let t_postfix = postfix.to_string();
        let status_info = self.lower_servers_info_p.clone();
        let status_name = name.to_string();

        let thread = thread::spawn(move || {
            let result = lower_server_thread(
                t_aggregation_server,
                t_server_session_p,
                rx,
                t_url,
                t_name,
                t_postfix,
            );
            if let Err(error) = &result {
                let mut infos = status_info.write();
                let info = infos.entry(status_name).or_insert(LowerServerInfo {
                    aggregation_finished: false,
                    removal_in_progress: false,
                    aggregation_error: None,
                });
                info.aggregation_finished = false;
                info.removal_in_progress = false;
                info.aggregation_error = Some(error.to_string());
            }
            result
        });

        self.lower_servers_related_p.write().insert(
            name.to_string(),
            LowerServerThreading {
                thread_handle: thread,
                remove_lserver_tx: tx,
            },
        );

        Ok(())
    }

    #[instrument(level = "info", skip(self), err, ret)]
    pub fn remove_lower_server(&self, name: &str) -> Result<(), RemoveLowerServerError> {
        let Some(lsr) = self.lower_servers_related_p.write().remove(name) else {
            let failed_status_exists = self
                .lower_servers_info_p
                .read()
                .get(name)
                .map(|info| info.aggregation_error.is_some())
                .unwrap_or(false);
            return if failed_status_exists {
                self.lower_servers_info_p.write().remove(name);
                Ok(())
            } else {
                Err(RemoveLowerServerError::NameNotFound(name.to_string()))
            };
        };

        let LowerServerThreading {
            thread_handle,
            remove_lserver_tx,
        } = lsr;
        if remove_lserver_tx.send(()).is_err() {
            warn!(
                "Lower server thread message receiver was dropped early. \
                This could mean that the lower server thread crashed."
            );
            // The wrapper writes terminal failure only after the thread exits.
            // Join first, then explicitly acknowledge/remove that status.
            let _ = thread_handle.join();
            self.lower_servers_info_p.write().remove(name);
        };

        Ok(())
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

#[instrument(err, skip_all)]
fn lower_server_thread(
    aggregation_server: AggregationServer,
    server_p: Arc<RwLock<Server>>,
    mut remove_rx: oneshot::Receiver<()>,
    url: String,
    name: String,
    postfix: String,
) -> Result<(), LowerServerError> {
    let total_started = Instant::now();
    let setup_started = Instant::now();
    let address_space_p = aggregation_server.address_space_p;
    let global_type_hashmap_p = aggregation_server.global_type_hashmap_p;
    let lower_servers_info_p = aggregation_server.lower_servers_info_p;
    let lower_server_sessions_p = aggregation_server.lower_server_sessions_p;

    let map_db = aggregation_server.map_db_p.read().connect()?;

    let mut address_space = address_space_p.write();
    // Look if root folder with this name already exists and delete it if that is the case.
    if let Some(lserver_root_folders) =
        address_space.find_hierarchical_references(&NodeId::objects_folder_id())
    {
        for lrf_nid in lserver_root_folders {
            if let Some(root_folder) = address_space.find_node(&lrf_nid) {
                if root_folder.as_node().browse_name().name.value() == &Some(name.clone()) {
                    warn!(
                        "Root folder for lower server being aggregated already exists! \
                        Deleting that folder."
                    );
                    let deleted = address_space.delete_rec_hierarchical_refs(&lrf_nid, true);
                    if !deleted {
                        warn!("Lower server root folder could not be deleted.");
                    }
                }
            }
        }
    }
    let root_folder = NodeId::next_numeric(1);
    if !address_space.add_folder_with_id(
        &root_folder,
        QualifiedName::new(1, &name),
        &name,
        &NodeId::objects_folder_id(),
    ) {
        let remove_lsi = lower_servers_info_p.write().remove(&name);
        if remove_lsi.is_none() {
            error!("Name of lower server could not be found in lower server info hashmap.")
        };
        return Err(LowerServerError::AddressSpaceError);
    };
    drop(address_space);

    let lserver_id = match map_db.insert_lserver(&name, &root_folder) {
        Ok(lserver_id) => lserver_id,
        Err(e) => {
            error!("Could not insert lserver into database: {:?}", e);

            let deleted = address_space_p
                .write()
                .delete_rec_hierarchical_refs(&root_folder, true);
            if !deleted {
                warn!("Lower server root folder could not be deleted.");
            }

            let remove_lsi = lower_servers_info_p.write().remove(&name);
            if remove_lsi.is_none() {
                error!("Name of lower server could not be found in lower server info hashmap.")
            };
            return Err(e.into());
        }
    };

    let cleanup = || {
        info!("Starting cleanup!");
        let mut lower_server_info = lower_servers_info_p.write();
        if let Some(lsi) = lower_server_info.get_mut(&name) {
            lsi.removal_in_progress = true;
        } else {
            error!("Name of lower server could not be found in lower server info hashmap.")
        };
        drop(lower_server_info);

        lower_server_sessions_p.write().remove(&lserver_id);
        let mut address_space = address_space_p.write();
        let deleted = address_space.delete_rec_hierarchical_refs(&root_folder, true);
        if !deleted {
            warn!("Lower server root folder could not be deleted.");
        }
        drop(address_space);
        if let Err(e) = map_db.delete_lserver(lserver_id) {
            error!("Cannot delete lower server: {:?}", e)
        };

        let remove_lsi = lower_servers_info_p.write().remove(&name);
        if remove_lsi.is_none() {
            error!("Name of lower server could not be found in lower server info hashmap.")
        };
        info!("Cleanup finished!");
    };

    let Some(mut client) = ClientBuilder::new()
        .application_name("ACOR DT Aggregation Server")
        .application_uri("http://acor.plcm.tu-darmstadt.de/aggregation-server-instance/")
        .session_retry_limit(5)
        .session_retry_interval(1000)
        .session_timeout(60_000)
        .request_timeout(URDF_LOWER_SERVER_REQUEST_TIMEOUT_MS)
        .max_byte_string_length(URDF_MAX_BYTE_STRING_LENGTH)
        .max_message_size(URDF_MAX_MESSAGE_SIZE)
        .max_chunk_count(0)
        .ignore_clock_skew()
        .client()
    else {
        cleanup();
        return Err(LowerServerError::CreateClientError);
    };

    // No security considered yet
    let session_p = client
        .connect_to_endpoint(
            EndpointDescription::from(url.as_str()),
            IdentityToken::Anonymous,
        )
        .or_else(|e| {
            cleanup();
            Err(LowerServerError::ConnectionError { status_code: e })
        })?;
    let session_tx = Session::run_async(session_p.clone());

    let mut lserver = LowerServer::new(&name, lserver_id, &url, &postfix, &root_folder);
    log_phase_timing(&name, "vendor_setup", setup_started.elapsed());

    let namespace_started = Instant::now();
    info!("Starting namespace aggregation.");
    aggregate_namespaces(&address_space_p, &session_p, &map_db, &mut lserver).or_else(|e| {
        cleanup();
        Err(e)
    })?;
    log_phase_timing(&name, "vendor_namespace", namespace_started.elapsed());

    // TODO: This write call will block service delegation during type aggregation
    let mut standard_namespaces = aggregation_server.standard_namespaces_p.write();
    let type_started = Instant::now();
    info!("Starting type aggregation.");
    aggregate_types(
        &address_space_p,
        &session_p,
        &global_type_hashmap_p,
        &map_db,
        &mut standard_namespaces,
        &mut lserver,
    )
    .or_else(|e| {
        cleanup();
        Err(e)
    })?;
    drop(standard_namespaces);
    log_phase_timing(&name, "vendor_type", type_started.elapsed());

    let instance_started = Instant::now();
    info!("Starting instance aggregation.");
    aggregate_instances_indexed(
        &session_p,
        &address_space_p,
        aggregation_server.instance_mapping_rules_p,
        aggregation_server.instance_mapping_rule_index_p,
        &map_db,
        &mut lserver,
        aggregation_server.incomplete_mappings_p,
    )
    .or_else(|e| {
        cleanup();
        Err(e)
    })?;
    log_phase_timing(&name, "vendor_instance", instance_started.elapsed());
    log_phase_timing(&name, "vendor_total", total_started.elapsed());
    info!("Aggregation complete.");

    lower_server_sessions_p
        .write()
        .insert(lserver_id, session_p.clone());

    // Writing in the lower server info object that the aggregation has finished
    let mut lower_server_info = lower_servers_info_p.write();
    let Some(lsi) = lower_server_info.get_mut(&name) else {
        cleanup();
        return Err(LowerServerError::NameNotFoundError { name: name });
    };
    lsi.aggregation_finished = true;
    lsi.aggregation_error = None;
    drop(lower_server_info);

    // Apply existing subscriptions
    let server_session_p = server_p.read().session_manager().clone();
    let server_session_manager = server_session_p.read();
    let client_session = session_p.read();
    for server_session_p in server_session_manager.sessions.values() {
        let server_session = server_session_p.read();
        let subscriptions = server_session.subscriptions();
        for sub in subscriptions.subscriptions().values() {
            match client_session.create_subscription(
                sub.publishing_interval(),
                sub.max_lifetime_count(),
                sub.max_keep_alive_count(),
                0,
                sub.priority(),
                sub.publishing_enabled(),
                AggregationSubscriptionNotification {
                    session_p: server_session_p.clone(),
                    lserver_id: lserver_id.clone(),
                    aggserver_sub_id: sub.subscription_id().clone(),
                },
            ) {
                Ok(lserver_sub_id) => {
                    if let Err(e) = map_db.insert_subscription(
                        lserver_id.clone(),
                        lserver_sub_id,
                        sub.subscription_id(),
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
            }
        }
    }
    drop(server_session_manager);
    drop(client_session); // important or else it will block forever

    let mut connection_lost = false;
    loop {
        let client_session = session_p.read();
        if !client_session.is_connected() {
            connection_lost = true;
            break;
        }
        drop(client_session);
        match remove_rx.try_recv() {
            Ok(_) => break,
            Err(TryRecvError::Closed) => break,
            Err(TryRecvError::Empty) => {}
        }
        thread::sleep(Duration::from_millis(250));
    }
    if let Err(_) = session_tx.send(SessionCommand::Stop) {
        warn!("Session oneshot receiver dropped early.");
    };
    cleanup();

    if connection_lost {
        Err(LowerServerError::ConnectionLost { name })
    } else {
        Ok(())
    }
}
