// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

use opcua::client::prelude::*;
use opcua::sync::RwLock;
use opcua::types::service_types::{
    FindServersOnNetworkRequest, FindServersOnNetworkResponse, FindServersRequest,
    FindServersResponse, ServerOnNetwork,
};
use opcua::types::service_types::{ReadRequest, ReadValueId};
use opcua::types::status_code::StatusCode;
use opcua::types::string::UAString;
use opcua::types::{AttributeId, NodeId, TimestampsToReturn};
use std::collections::{HashMap, HashSet};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::server_discovery::config::update_config_with_discovered_servers;

static ENDPOINT_FAILURES: OnceLock<Mutex<HashMap<String, u8>>> = OnceLock::new();
static LDS_SESSION: OnceLock<Mutex<Option<Arc<RwLock<Session>>>>> = OnceLock::new();
static ENDPOINT_FAILURE_THRESHOLD: OnceLock<u8> = OnceLock::new();
static ENDPOINT_CONNECT_TIMEOUT: OnceLock<Duration> = OnceLock::new();
const DEFAULT_ENDPOINT_FAILURE_THRESHOLD: u8 = 5;
const DEFAULT_ENDPOINT_CONNECT_TIMEOUT_MS: u64 = 2_000;
const MAX_ENDPOINT_FAILURE_THRESHOLD: u64 = 100;
const MAX_ENDPOINT_CONNECT_TIMEOUT_MS: u64 = 60_000;

const OBSERVATION_BUDGET: Duration = Duration::from_secs(15);
const OBSERVATION_REQUEST_TIMEOUT_MS: u32 = 2_000;
const MAX_NETWORK_IDENTITY_LOOKUPS: usize = 64;
const NETWORK_IDENTITY_MAX_AGE: Duration = Duration::from_secs(300);
const LOCAL_LDS_URI: &str = "urn:open62541.example.local_discovery_server";

/// Discovery advertisements, not proof that every advertised endpoint is live.
/// A partial observation may admit positive identities but must not remove an
/// identity absent from the result. Removal grace belongs to the admission owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryObservation {
    pub sources: Vec<(String, String)>,
    pub authoritative: bool,
}

/// Owns even a partially connected session and disconnects it on every exit.
struct OwnedDiscoverySession(Arc<RwLock<Session>>);

impl Drop for OwnedDiscoverySession {
    fn drop(&mut self) {
        self.0.read().disconnect();
    }
}

fn observation_client(control: SessionOperationControl) -> Result<Client, String> {
    control.check().map_err(|error| error.to_string())?;
    ClientBuilder::new()
        .application_name("Aggregation Discovery Observer")
        .application_uri("urn:AggregationDiscoveryObserver")
        .operation_control(control)
        .request_timeout(OBSERVATION_REQUEST_TIMEOUT_MS)
        .session_retry_limit(0)
        .trust_server_certs(true)
        .create_sample_keypair(false)
        .client()
        .ok_or_else(|| "Failed to create discovery observer client".into())
}

fn open_discovery_session(
    url: &str,
    control: SessionOperationControl,
) -> Result<OwnedDiscoverySession, String> {
    let mut client = observation_client(control)?;
    let endpoint: EndpointDescription = (
        url,
        SecurityPolicy::None.to_str(),
        MessageSecurityMode::None,
        UserTokenPolicy::anonymous(),
    )
        .into();
    // Discovery services require an open channel, not an activated session.
    // Avoid GetEndpoints and its strict endpoint-path comparison for mDNS URLs.
    let session = OwnedDiscoverySession(client.new_session_from_info(endpoint)?);
    session
        .0
        .read()
        .connect()
        .map_err(|error| error.to_string())?;
    Ok(session)
}

fn check_discovery_response(response: &FindServersResponse) -> Result<(), String> {
    let status = response.response_header.service_result;
    if status.is_good() {
        Ok(())
    } else {
        Err(format!("FindServers returned {status}"))
    }
}

fn observe_network_identity(
    url: &str,
    control: SessionOperationControl,
) -> Result<FindServersResponse, String> {
    let session = open_discovery_session(url, control)?;
    let response =
        find_servers(Arc::clone(&session.0), url, None, None).map_err(|error| error.to_string())?;
    check_discovery_response(&response)?;
    Ok(response)
}

fn advertised_sources(response: FindServersResponse) -> (Vec<(String, String)>, bool) {
    let mut sources = Vec::new();
    let mut complete = true;
    for application in response.servers.unwrap_or_default() {
        let name = application.application_uri.as_ref();
        if name == LOCAL_LDS_URI || application.application_type == ApplicationType::DiscoveryServer
        {
            continue;
        }
        if name.is_empty() {
            complete = false;
            continue;
        }
        let urls = application.discovery_urls.unwrap_or_default();
        if urls.is_empty() {
            complete = false;
        }
        for url in urls {
            // Do not probe reachability or turn a transient TCP failure into an
            // absent registration. Preparation validates connectivity later.
            if parse_opc_tcp_endpoint(url.as_ref()).is_none() {
                complete = false;
                continue;
            }
            sources.push((name.to_string(), url.as_ref().to_string()));
        }
    }
    (sources, complete)
}

#[derive(Clone, Debug)]
struct CachedNetworkIdentity {
    sources: Vec<(String, String)>,
    resolved_at: Instant,
    refresh_failed: bool,
}

/// One observer owns its rotating mDNS identity cache. A complete current mDNS
/// listing prunes vanished advertisements. Known identities are refreshed within
/// the shared cycle budget rather than repeatedly resolving only the first page.
/// Cached identities are advertisements, not ongoing connectivity guarantees.
/// Their maximum permitted age is 300 seconds. Beyond that age, or after any
/// failed refresh, observations are partial until resolution succeeds again.
#[derive(Default)]
pub struct DiscoveryObserver {
    network_identities: HashMap<String, CachedNetworkIdentity>,
    scan_cursor: usize,
}

impl DiscoveryObserver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Isolate recoverable Rust panics in the read-only protocol observer. It
    /// owns no aggregation mutations. Discard a possibly incomplete cache and
    /// let the next cycle open a fresh channel. Native faults are not caught.
    pub fn observe_resilient(
        &mut self,
        control: SessionOperationControl,
    ) -> Result<DiscoveryObservation, String> {
        self.protected_observation(|observer| observer.observe(control))
    }

    fn protected_observation<F>(&mut self, observe: F) -> Result<DiscoveryObservation, String>
    where
        F: FnOnce(&mut Self) -> Result<DiscoveryObservation, String>,
    {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observe(self))) {
            Ok(result) => result,
            Err(_) => {
                *self = Self::new();
                Err("discovery observer panicked, discarded its cache and will retry".into())
            }
        }
    }

    /// No config, namespace, NodeSet, rule or aggregation state is written here.
    /// Protocol calls share the provided cooperative budget. Native DNS and local
    /// filesystem work still require ownership of an unresponsive outer worker.
    pub fn observe(
        &mut self,
        control: SessionOperationControl,
    ) -> Result<DiscoveryObservation, String> {
        let session = open_discovery_session("opc.tcp://127.0.0.1:4840", control.clone())?;
        let response = find_servers(Arc::clone(&session.0), "", None, None)
            .map_err(|error| format!("LDS FindServers failed: {error}"))?;
        check_discovery_response(&response)?;
        let network = find_servers_on_network(Arc::clone(&session.0))
            .map_err(|error| format!("FindServersOnNetwork failed: {error}"));
        drop(session);
        Ok(self
            .merge(
                response,
                network,
                &control,
                MAX_NETWORK_IDENTITY_LOOKUPS,
                Instant::now(),
                observe_network_identity,
            )
            .0)
    }

    fn merge<F>(
        &mut self,
        response: FindServersResponse,
        network: Result<FindServersOnNetworkResponse, String>,
        control: &SessionOperationControl,
        max_lookups: usize,
        now: Instant,
        mut lookup: F,
    ) -> (DiscoveryObservation, usize)
    where
        F: FnMut(&str, SessionOperationControl) -> Result<FindServersResponse, String>,
    {
        let started = Instant::now();
        let (mut sources, mut authoritative) = advertised_sources(response);
        let primary_urls: HashSet<String> = sources
            .iter()
            .map(|(_, url)| discovery_url_key(url).to_string())
            .collect();
        let primary_names: HashSet<String> = sources.iter().map(|(name, _)| name.clone()).collect();
        let network = match network {
            Ok(network) if network.response_header.service_result.is_good() => network,
            _ => {
                // A missing network listing cannot confirm disappearance. Retain
                // only still-fresh positive cached identities and prohibit removal.
                for cached in self.network_identities.values() {
                    if now.saturating_duration_since(cached.resolved_at) <= NETWORK_IDENTITY_MAX_AGE
                    {
                        sources.extend(
                            cached
                                .sources
                                .iter()
                                .filter(|(name, url)| {
                                    !primary_urls.contains(discovery_url_key(url))
                                        && !primary_names.contains(name)
                                })
                                .cloned(),
                        );
                    }
                }
                sources.sort();
                sources.dedup();
                return (
                    DiscoveryObservation {
                        sources,
                        authoritative: false,
                    },
                    0,
                );
            }
        };
        let mut advertised = std::collections::BTreeMap::new();
        for server in network.servers.unwrap_or_default() {
            if server_has_capability(&server, "LDS") {
                continue;
            }
            let url = server.discovery_url.as_ref();
            if parse_opc_tcp_endpoint(url).is_none() {
                authoritative = false;
                continue;
            }
            advertised
                .entry(discovery_url_key(url).to_string())
                .or_insert_with(|| url.to_string());
        }
        // Pruning follows the complete mDNS record listing, never a failed TCP
        // identity lookup. A failed refresh is explicitly incomplete information.
        self.network_identities
            .retain(|key, _| advertised.contains_key(key));
        let mut candidates: Vec<_> = advertised
            .into_iter()
            .filter(|(key, _)| !primary_urls.contains(key))
            .collect();
        if !candidates.is_empty() {
            let offset = self.scan_cursor % candidates.len();
            candidates.rotate_left(offset);
        }
        let mut attempted = 0;
        for (key, url) in &candidates {
            if attempted >= max_lookups || control.check().is_err() {
                break;
            }
            attempted += 1;
            match lookup(url, control.clone()) {
                Ok(direct) if check_discovery_response(&direct).is_ok() => {
                    let (discovered, complete) = advertised_sources(direct);
                    if discovered.is_empty() {
                        if let Some(cached) = self.network_identities.get_mut(key) {
                            cached.refresh_failed = true;
                        }
                    } else {
                        self.network_identities.insert(
                            key.clone(),
                            CachedNetworkIdentity {
                                sources: discovered,
                                resolved_at: now + started.elapsed(),
                                refresh_failed: !complete,
                            },
                        );
                    }
                }
                Err(_) | Ok(_) => {
                    if let Some(cached) = self.network_identities.get_mut(key) {
                        cached.refresh_failed = true;
                    }
                }
            }
        }
        self.scan_cursor = if candidates.is_empty() {
            0
        } else {
            (self.scan_cursor % candidates.len() + attempted.max(1)) % candidates.len()
        };
        let completed_at = now + started.elapsed();
        // Coverage may be complete after cache warmup even if this refresh scans
        // fewer records than are advertised. Every unrefreshed identity must
        // still be fresh and its most recent attempted refresh successful.
        for (key, _) in candidates {
            match self.network_identities.get(&key) {
                Some(cached)
                    if completed_at.saturating_duration_since(cached.resolved_at)
                        <= NETWORK_IDENTITY_MAX_AGE =>
                {
                    authoritative &= !cached.refresh_failed;
                    sources.extend(cached.sources.iter().cloned());
                }
                _ => authoritative = false,
            }
        }
        sources.sort();
        sources.dedup();
        (
            DiscoveryObservation {
                sources,
                authoritative,
            },
            attempted,
        )
    }
}

// One-shot compatibility helper. The long-running application must retain one
// DiscoveryObserver so mDNS-only populations can complete cache warmup.
pub fn discover_observation() -> Result<DiscoveryObservation, String> {
    discover_observation_with_control(SessionOperationControl::new(OBSERVATION_BUDGET))
}

pub fn discover_observation_with_control(
    control: SessionOperationControl,
) -> Result<DiscoveryObservation, String> {
    DiscoveryObserver::new().observe(control)
}

#[cfg(test)]
fn merge_observation<F>(
    response: FindServersResponse,
    network: Result<FindServersOnNetworkResponse, String>,
    control: &SessionOperationControl,
    scan_start: usize,
    max_lookups: usize,
    lookup: F,
) -> (DiscoveryObservation, usize)
where
    F: FnMut(&str, SessionOperationControl) -> Result<FindServersResponse, String>,
{
    let mut observer = DiscoveryObserver::new();
    observer.scan_cursor = scan_start;
    observer.merge(
        response,
        network,
        control,
        max_lookups,
        Instant::now(),
        lookup,
    )
}

// Notification-Typen für Server-Events (behalten für Kompatibilität)
#[derive(Debug, Clone)]
pub enum ServerEvent {
    NewServerDiscovered { name: String, address: String },
    ServerRemoved { name: String },
}

fn parse_opc_tcp_endpoint(endpoint_url: &str) -> Option<(String, u16)> {
    let authority = endpoint_url.strip_prefix("opc.tcp://")?.split('/').next()?;

    let (host, port) = if let Some(ipv6) = authority.strip_prefix('[') {
        let closing_bracket = ipv6.find(']')?;
        let host = &ipv6[..closing_bracket];
        let port = ipv6[closing_bracket + 1..].strip_prefix(':')?;
        (host, port)
    } else {
        authority.rsplit_once(':')?
    };

    Some((host.to_string(), port.parse().ok()?))
}

fn parse_bounded_u64(raw: Option<&str>, default: u64, minimum: u64, maximum: u64) -> u64 {
    raw.and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| (minimum..=maximum).contains(value))
        .unwrap_or(default)
}

pub fn endpoint_liveness_configuration() -> (Duration, u8) {
    let timeout = *ENDPOINT_CONNECT_TIMEOUT.get_or_init(|| {
        let milliseconds = parse_bounded_u64(
            std::env::var("OJIES_ENDPOINT_CONNECT_TIMEOUT_MS")
                .ok()
                .as_deref(),
            DEFAULT_ENDPOINT_CONNECT_TIMEOUT_MS,
            1,
            MAX_ENDPOINT_CONNECT_TIMEOUT_MS,
        );
        Duration::from_millis(milliseconds)
    });
    let failure_threshold = *ENDPOINT_FAILURE_THRESHOLD.get_or_init(|| {
        parse_bounded_u64(
            std::env::var("OJIES_ENDPOINT_FAILURE_THRESHOLD")
                .ok()
                .as_deref(),
            u64::from(DEFAULT_ENDPOINT_FAILURE_THRESHOLD),
            1,
            MAX_ENDPOINT_FAILURE_THRESHOLD,
        ) as u8
    });
    (timeout, failure_threshold)
}

pub fn endpoint_is_reachable(endpoint_url: &str) -> bool {
    let Some((host, port)) = parse_opc_tcp_endpoint(endpoint_url) else {
        return false;
    };
    let Ok(addresses) = (host.as_str(), port).to_socket_addrs() else {
        return false;
    };

    let (connect_timeout, _) = endpoint_liveness_configuration();
    addresses
        .into_iter()
        .any(|address| TcpStream::connect_timeout(&address, connect_timeout).is_ok())
}

fn filter_unreachable_servers(response: &mut FindServersResponse, lds_app_uri: &str) {
    let Some(servers) = response.servers.as_mut() else {
        return;
    };
    let mut failures = ENDPOINT_FAILURES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("Endpoint failure tracker lock poisoned");

    servers.retain(|server| {
        let application_uri = server.application_uri.as_ref();

        if application_uri == lds_app_uri {
            return true;
        }

        let reachable = server
            .discovery_urls
            .as_ref()
            .map(|urls| urls.iter().any(|url| endpoint_is_reachable(url.as_ref())))
            .unwrap_or(false);

        if reachable {
            failures.remove(application_uri);
            return true;
        }

        let failure_count = failures
            .entry(application_uri.to_string())
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);

        let (_, failure_threshold) = endpoint_liveness_configuration();
        if *failure_count >= failure_threshold {
            println!(
                "⊗ Server-Endpunkt nicht erreichbar: {} ({} Prüfungen)",
                application_uri, failure_count
            );
            false
        } else {
            println!(
                "⚠ Server-Endpunkt vorübergehend nicht erreichbar: {}",
                application_uri
            );
            true
        }
    });
}

// FindServers Funktion mit Rückgabe der Response
pub fn find_servers(
    session: Arc<RwLock<Session>>,
    endpoint_url: &str,
    locale_ids: Option<Vec<UAString>>,
    server_uris: Option<Vec<UAString>>,
) -> Result<FindServersResponse, StatusCode> {
    let session_guard = session.read();

    let request = FindServersRequest {
        request_header: session_guard.make_request_header(),
        endpoint_url: UAString::from(endpoint_url),
        locale_ids,
        server_uris,
    };

    match session_guard.send_request(request) {
        Ok(response) => {
            if let SupportedMessage::FindServersResponse(find_servers_response) = response {
                Ok(*find_servers_response)
            } else {
                eprintln!("Unerwarteter Response-Typ");
                Err(StatusCode::BadUnexpectedError)
            }
        }
        Err(e) => {
            eprintln!("Fehler bei FindServers: {:?}", e);
            Err(e)
        }
    }
}

pub fn find_servers_on_network(
    session: Arc<RwLock<Session>>,
) -> Result<FindServersOnNetworkResponse, StatusCode> {
    let session_guard = session.read();
    let request = FindServersOnNetworkRequest {
        request_header: session_guard.make_request_header(),
        starting_record_id: 0,
        max_records_to_return: 0,
        server_capability_filter: None,
    };

    match session_guard.send_request(request) {
        Ok(response) => {
            if let SupportedMessage::FindServersOnNetworkResponse(network_response) = response {
                Ok(*network_response)
            } else {
                eprintln!("Unerwarteter Response-Typ fuer FindServersOnNetwork");
                Err(StatusCode::BadUnexpectedError)
            }
        }
        Err(error) => {
            eprintln!("Fehler bei FindServersOnNetwork: {:?}", error);
            Err(error)
        }
    }
}

fn server_has_capability(server: &ServerOnNetwork, capability: &str) -> bool {
    server
        .server_capabilities
        .as_ref()
        .map(|capabilities| {
            capabilities
                .iter()
                .any(|item| item.as_ref().eq_ignore_ascii_case(capability))
        })
        .unwrap_or(false)
}

fn discovery_url_key(url: &str) -> &str {
    url.trim_end_matches('/')
}

fn select_anonymous_none_endpoint(
    endpoints: Vec<EndpointDescription>,
) -> Option<EndpointDescription> {
    endpoints.into_iter().find(|endpoint| {
        endpoint.security_mode == MessageSecurityMode::None
            && SecurityPolicy::from_uri(endpoint.security_policy_uri.as_ref())
                == SecurityPolicy::None
    })
}

fn persistent_lds_session(
    lds_url: &str,
) -> Result<Arc<RwLock<Session>>, Box<dyn std::error::Error>> {
    let mut cached_session = LDS_SESSION
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("LDS session cache lock poisoned");

    if let Some(session) = cached_session.as_ref() {
        if session.read().is_connected() {
            return Ok(Arc::clone(session));
        }
    }
    *cached_session = None;

    let mut client = ClientBuilder::new()
        .application_name("FindServers Client")
        .application_uri("urn:FindServersClient")
        .trust_server_certs(true)
        .create_sample_keypair(true)
        .session_retry_limit(3)
        .max_message_size(0)
        .max_chunk_count(0)
        .client()
        .ok_or("Failed to create OPC UA client")?;

    let endpoint: EndpointDescription = (
        lds_url,
        SecurityPolicy::None.to_str(),
        MessageSecurityMode::None,
        UserTokenPolicy::anonymous(),
    )
        .into();
    let session = client
        .connect_to_endpoint(endpoint, IdentityToken::Anonymous)
        .map_err(|error| format!("LDS Verbindung fehlgeschlagen: {:?}", error))?;
    *cached_session = Some(Arc::clone(&session));
    Ok(session)
}

fn find_servers_at_url(
    discovery_url: &str,
) -> Result<FindServersResponse, Box<dyn std::error::Error>> {
    let mut client = ClientBuilder::new()
        .application_name("Network Discovery Client")
        .application_uri("urn:NetworkDiscoveryClient")
        .trust_server_certs(true)
        .create_sample_keypair(true)
        .session_retry_limit(1)
        .max_message_size(0)
        .max_chunk_count(0)
        .client()
        .ok_or("Failed to create OPC UA network-discovery client")?;

    // FindServersOnNetwork may omit a trailing slash which is present in the
    // endpoint returned by GetEndpoints. The Rust client intentionally
    // compares endpoint paths strictly, so use the server's exact anonymous
    // endpoint instead of reconstructing one from the mDNS discovery URL.
    let endpoint = select_anonymous_none_endpoint(
        client.get_server_endpoints_from_url(discovery_url.to_string())?,
    )
    .ok_or_else(|| {
        format!(
            "Kein anonymer SecurityPolicy-None-Endpoint bei {} gefunden",
            discovery_url
        )
    })?;
    let session = client
        .connect_to_endpoint(endpoint, IdentityToken::Anonymous)
        .map_err(|error| {
            format!(
                "Verbindung zu {} fehlgeschlagen: {:?}",
                discovery_url, error
            )
        })?;

    let response = find_servers(Arc::clone(&session), "", None, None).map_err(|error| {
        format!(
            "FindServers bei {} fehlgeschlagen: {:?}",
            discovery_url, error
        )
        .into()
    });
    session.read().disconnect();
    response
}

fn merge_network_discovered_servers(
    response: &mut FindServersResponse,
    network_response: FindServersOnNetworkResponse,
) {
    let target = response.servers.get_or_insert_with(Vec::new);
    let mut known_applications: HashSet<String> = target
        .iter()
        .map(|server| server.application_uri.as_ref().to_string())
        .filter(|application_uri| !application_uri.is_empty())
        .collect();
    let mut known_discovery_urls: HashSet<String> = target
        .iter()
        .flat_map(|server| server.discovery_urls.iter().flatten())
        .map(|url| discovery_url_key(url.as_ref()).to_string())
        .collect();
    let mut queried_urls = HashSet::new();

    for network_server in network_response.servers.unwrap_or_default() {
        if server_has_capability(&network_server, "LDS") {
            continue;
        }

        let discovery_url = network_server.discovery_url.as_ref().to_string();
        if discovery_url.is_empty() || !queried_urls.insert(discovery_url.clone()) {
            continue;
        }
        let network_url_key = discovery_url_key(&discovery_url).to_string();
        if known_discovery_urls.contains(&network_url_key) {
            continue;
        }
        if !endpoint_is_reachable(&discovery_url) {
            println!(
                "Netzwerk-Discovery ignoriert nicht erreichbaren Endpoint: {}",
                discovery_url
            );
            continue;
        }

        match find_servers_at_url(&discovery_url) {
            Ok(direct_response) => {
                for application in direct_response.servers.unwrap_or_default() {
                    known_discovery_urls.extend(
                        application
                            .discovery_urls
                            .iter()
                            .flatten()
                            .map(|url| discovery_url_key(url.as_ref()).to_string()),
                    );
                    let application_uri = application.application_uri.as_ref().to_string();
                    if application_uri.is_empty()
                        || known_applications.insert(application_uri.clone())
                    {
                        println!(
                            "Netzwerk-Discovery gefunden: {} -> {}",
                            application_uri, discovery_url
                        );
                        target.push(application);
                    }
                }
                known_discovery_urls.insert(network_url_key);
            }
            Err(error) => eprintln!(
                "Netzwerk-Discovery konnte {} nicht aufloesen: {}",
                discovery_url, error
            ),
        }
    }
}

// Legacy callers retain this entry point. The supervised application passes
// its own shared control through get_namespaces_controlled instead.
pub fn get_namespaces(server_url: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    get_namespaces_controlled(
        server_url,
        SessionOperationControl::new(Duration::from_secs(60)),
    )
}

fn namespace_values(response: SupportedMessage) -> Result<Vec<String>, String> {
    let SupportedMessage::ReadResponse(response) = response else {
        return Err("Unexpected response to NamespaceArray Read".into());
    };
    if !response.response_header.service_result.is_good() {
        return Err(format!(
            "NamespaceArray Read failed: {}",
            response.response_header.service_result
        ));
    }
    let results = response
        .results
        .ok_or("NamespaceArray Read has no results")?;
    if results.len() != 1 {
        return Err("NamespaceArray Read must contain exactly one result".into());
    }
    let result = results.into_iter().next().unwrap();
    if let Some(status) = result.status {
        if !status.is_good() {
            return Err(format!("NamespaceArray has status {status}"));
        }
    }
    let Some(opcua::types::Variant::Array(array)) = result.value else {
        return Err("NamespaceArray value is not an array".into());
    };
    if array.values.is_empty() {
        return Err("NamespaceArray is empty".into());
    }
    array
        .values
        .into_iter()
        .map(|value| match value {
            opcua::types::Variant::String(value) => value
                .value()
                .as_ref()
                .cloned()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "NamespaceArray contains an empty URI".to_string()),
            _ => Err("NamespaceArray contains a non-string element".into()),
        })
        .collect()
}

pub fn get_namespaces_controlled(
    server_url: &str,
    control: SessionOperationControl,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut client = observation_client(control.clone())?;
    let endpoint: EndpointDescription = (
        server_url,
        SecurityPolicy::None.to_str(),
        MessageSecurityMode::None,
        UserTokenPolicy::anonymous(),
    )
        .into();
    let session =
        OwnedDiscoverySession(client.connect_to_endpoint(endpoint, IdentityToken::Anonymous)?);
    control.check()?;
    let response = {
        let session_guard = session.0.read();
        session_guard.send_request(ReadRequest {
            request_header: session_guard.make_request_header(),
            max_age: 0.0,
            timestamps_to_return: TimestampsToReturn::Neither,
            nodes_to_read: Some(vec![ReadValueId {
                node_id: NodeId::new(0, 2255u32),
                attribute_id: AttributeId::Value as u32,
                index_range: UAString::null(),
                data_encoding: opcua::types::QualifiedName::null(),
            }]),
        })?
    };
    namespace_values(response).map_err(Into::into)
}

// Einmaliger Discovery-Zyklus.
// Gibt (entfernte_server, neue_server) zurück.
// Wird direkt aus der main-Loop aufgerufen
pub fn run_discovery_cycle(
    config_path: &str,
    namespaces_path: &str,
) -> Result<(Vec<String>, Vec<(String, String)>), Box<dyn std::error::Error>> {
    println!("\n=== FindServers Discovery Zyklus ===");

    let lds_url = "opc.tcp://127.0.0.1:4840";
    let lds_app_uri = "urn:open62541.example.local_discovery_server";

    println!("Verbinde zum LDS: {}", lds_url);
    let session = persistent_lds_session(lds_url)?;

    println!("✓ Verbunden zum LDS");

    let mut response = find_servers(Arc::clone(&session), "", None, None)
        .map_err(|e| format!("FindServers fehlgeschlagen: {:?}", e))?;

    println!("✓ FindServers erfolgreich");

    match find_servers_on_network(Arc::clone(&session)) {
        Ok(network_response) => {
            println!("✓ FindServersOnNetwork erfolgreich");
            merge_network_discovered_servers(&mut response, network_response);
        }
        Err(error) => {
            eprintln!(
                "FindServersOnNetwork nicht verfuegbar; verwende registrierte LDS-Server: {:?}",
                error
            );
        }
    }
    filter_unreachable_servers(&mut response, lds_app_uri);

    update_config_with_discovered_servers(config_path, namespaces_path, &response, lds_app_uri)
}

#[cfg(test)]
mod tests {
    use super::{
        discovery_url_key, parse_bounded_u64, parse_opc_tcp_endpoint,
        select_anonymous_none_endpoint, server_has_capability,
    };
    use opcua::client::prelude::{MessageSecurityMode, SecurityPolicy, UserTokenPolicy};
    use opcua::types::service_types::{EndpointDescription, ServerOnNetwork};
    use opcua::types::string::UAString;

    #[test]
    fn parses_ipv4_endpoint_with_path() {
        assert_eq!(
            parse_opc_tcp_endpoint("opc.tcp://192.0.2.20:4860/plcm/robotics/1/"),
            Some(("192.0.2.20".to_string(), 4860))
        );
    }

    #[test]
    fn parses_ipv6_endpoint() {
        assert_eq!(
            parse_opc_tcp_endpoint("opc.tcp://[::1]:4840"),
            Some(("::1".to_string(), 4840))
        );
    }

    #[test]
    fn rejects_non_opc_tcp_endpoint() {
        assert_eq!(parse_opc_tcp_endpoint("http://localhost:4840"), None);
    }

    #[test]
    fn accepts_only_bounded_positive_liveness_values() {
        assert_eq!(parse_bounded_u64(Some("2000"), 250, 1, 60_000), 2_000);
        assert_eq!(parse_bounded_u64(Some("0"), 250, 1, 60_000), 250);
        assert_eq!(parse_bounded_u64(Some("60001"), 250, 1, 60_000), 250);
        assert_eq!(parse_bounded_u64(Some("invalid"), 5, 1, 100), 5);
        assert_eq!(parse_bounded_u64(None, 5, 1, 100), 5);
    }

    #[test]
    fn distinguishes_lds_from_source_server_mdns_records() {
        let lds = ServerOnNetwork {
            record_id: 1,
            server_name: UAString::from("Local Discovery Server"),
            discovery_url: UAString::from("opc.tcp://192.0.2.10:4840"),
            server_capabilities: Some(vec![UAString::from("LDS")]),
        };
        let source = ServerOnNetwork {
            record_id: 2,
            server_name: UAString::from("EVA OPCUA Server"),
            discovery_url: UAString::from("opc.tcp://192.0.2.26:4840/freeopcua/server/"),
            server_capabilities: Some(vec![UAString::from("NA")]),
        };

        assert!(server_has_capability(&lds, "lds"));
        assert!(!server_has_capability(&source, "LDS"));
    }

    #[test]
    fn uses_exact_endpoint_url_returned_by_get_endpoints() {
        let advertised_without_slash = "opc.tcp://127.0.0.1:4860/plcm/robotics/1";
        let returned_with_slash = "opc.tcp://127.0.0.1:4860/plcm/robotics/1/";
        let endpoint: EndpointDescription = (
            returned_with_slash,
            SecurityPolicy::None.to_uri(),
            MessageSecurityMode::None,
            UserTokenPolicy::anonymous(),
        )
            .into();

        assert_ne!(advertised_without_slash, endpoint.endpoint_url.as_ref());
        assert_eq!(
            discovery_url_key(advertised_without_slash),
            discovery_url_key(returned_with_slash)
        );
        assert_eq!(
            select_anonymous_none_endpoint(vec![endpoint])
                .unwrap()
                .endpoint_url
                .as_ref(),
            returned_with_slash
        );
    }

    fn registrations(entries: &[(&str, &str)]) -> super::FindServersResponse {
        super::FindServersResponse {
            response_header: opcua::types::ResponseHeader::new_good(
                &opcua::types::RequestHeader::default(),
            ),
            servers: Some(
                entries
                    .iter()
                    .map(|(name, url)| opcua::types::ApplicationDescription {
                        application_uri: UAString::from(*name),
                        application_type: opcua::types::ApplicationType::Server,
                        discovery_urls: Some(vec![UAString::from(*url)]),
                        ..Default::default()
                    })
                    .collect(),
            ),
        }
    }

    fn network_records(urls: &[&str]) -> super::FindServersOnNetworkResponse {
        super::FindServersOnNetworkResponse {
            response_header: opcua::types::ResponseHeader::new_good(
                &opcua::types::RequestHeader::default(),
            ),
            last_counter_reset_time: opcua::types::DateTime::now(),
            servers: Some(
                urls.iter()
                    .enumerate()
                    .map(|(index, url)| ServerOnNetwork {
                        record_id: index as u32,
                        server_name: UAString::from("fixture"),
                        discovery_url: UAString::from(*url),
                        server_capabilities: None,
                    })
                    .collect(),
            ),
        }
    }

    #[test]
    fn incomplete_mdns_retains_positive_lds_registrations_without_authorizing_removal() {
        let (observed, attempts) = super::merge_observation(
            registrations(&[("urn:registered", "opc.tcp://192.0.2.1:4860/")]),
            Err("mDNS service unavailable".into()),
            &super::SessionOperationControl::new(std::time::Duration::from_secs(1)),
            0,
            64,
            |_, _| panic!("no direct lookup should run"),
        );
        assert_eq!(attempts, 0);
        assert!(!observed.authoritative);
        assert_eq!(
            observed.sources,
            vec![("urn:registered".into(), "opc.tcp://192.0.2.1:4860/".into())]
        );
    }

    #[test]
    fn advertised_endpoint_needs_no_reachability_probe_and_duplicate_records_are_collapsed() {
        let (observed, attempts) = super::merge_observation(
            registrations(&[
                ("urn:registered", "opc.tcp://192.0.2.1:4860/"),
                ("urn:registered", "opc.tcp://192.0.2.1:4860/"),
                (super::LOCAL_LDS_URI, "opc.tcp://127.0.0.1:4840"),
            ]),
            Ok(network_records(&["opc.tcp://192.0.2.1:4860"])),
            &super::SessionOperationControl::new(std::time::Duration::from_secs(1)),
            0,
            64,
            |_, _| panic!("known advertisement must not trigger a probe"),
        );
        assert!(observed.authoritative);
        assert_eq!(observed.sources.len(), 1);
        assert_eq!(attempts, 0);
    }

    #[test]
    fn failed_mdns_identity_does_not_discard_other_positive_sources() {
        let (observed, attempts) = super::merge_observation(
            registrations(&[("urn:registered", "opc.tcp://192.0.2.1:4860/")]),
            Ok(network_records(&[
                "opc.tcp://192.0.2.2:4860",
                "opc.tcp://192.0.2.3:4860",
            ])),
            &super::SessionOperationControl::new(std::time::Duration::from_secs(1)),
            0,
            64,
            |url, _| {
                if url.contains(".2:") {
                    Err("offline".into())
                } else {
                    Ok(registrations(&[("urn:network", url)]))
                }
            },
        );
        assert_eq!(attempts, 2);
        assert!(!observed.authoritative);
        assert_eq!(observed.sources.len(), 2);
        assert!(observed
            .sources
            .iter()
            .any(|(name, _)| name == "urn:network"));
    }

    #[test]
    fn finite_network_scan_rotates_and_reports_its_incomplete_coverage() {
        let mut visited = Vec::new();
        for start in 0..3 {
            let (observed, attempts) = super::merge_observation(
                registrations(&[]),
                Ok(network_records(&[
                    "opc.tcp://192.0.2.1:4860",
                    "opc.tcp://192.0.2.2:4860",
                    "opc.tcp://192.0.2.3:4860",
                ])),
                &super::SessionOperationControl::new(std::time::Duration::from_secs(1)),
                start,
                1,
                |url, _| {
                    visited.push(url.to_string());
                    Err("offline".into())
                },
            );
            assert_eq!(attempts, 1);
            assert!(!observed.authoritative);
        }
        visited.sort();
        visited.dedup();
        assert_eq!(visited.len(), 3);
    }

    #[test]
    fn exhausted_cycle_budget_starts_no_additional_lookup() {
        let (observed, attempts) = super::merge_observation(
            registrations(&[("urn:registered", "opc.tcp://192.0.2.1:4860/")]),
            Ok(network_records(&["opc.tcp://192.0.2.2:4860"])),
            &super::SessionOperationControl::new(std::time::Duration::ZERO),
            0,
            64,
            |_, _| panic!("expired observation must not start another operation"),
        );
        assert_eq!(attempts, 0);
        assert!(!observed.authoritative);
        assert_eq!(observed.sources.len(), 1);
    }

    fn namespace_response(
        value: opcua::types::Variant,
        status: Option<super::StatusCode>,
    ) -> super::SupportedMessage {
        super::SupportedMessage::ReadResponse(Box::new(opcua::types::ReadResponse {
            response_header: opcua::types::ResponseHeader::new_good(
                &opcua::types::RequestHeader::default(),
            ),
            results: Some(vec![opcua::types::DataValue {
                value: Some(value),
                status,
                ..Default::default()
            }]),
            diagnostic_infos: None,
        }))
    }

    fn string_array(values: Vec<UAString>) -> opcua::types::Variant {
        opcua::types::Variant::Array(Box::new(
            opcua::types::Array::new(
                opcua::types::VariantTypeId::String,
                values
                    .into_iter()
                    .map(opcua::types::Variant::String)
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        ))
    }

    #[test]
    fn namespace_read_rejects_bad_status_instead_of_accepting_its_value() {
        let response = namespace_response(
            string_array(vec![UAString::from("http://opcfoundation.org/UA/")]),
            Some(super::StatusCode::BadNotReadable),
        );
        assert!(super::namespace_values(response)
            .unwrap_err()
            .contains("BadNotReadable"));
    }

    #[test]
    fn namespace_read_preserves_indices_and_rejects_malformed_entries() {
        let values = vec![
            UAString::from("http://opcfoundation.org/UA/"),
            UAString::from("urn:fixture"),
        ];
        assert_eq!(
            super::namespace_values(namespace_response(string_array(values), None)).unwrap(),
            vec!["http://opcfoundation.org/UA/", "urn:fixture"],
        );
        let values = vec![
            UAString::from("http://opcfoundation.org/UA/"),
            UAString::null(),
        ];
        assert!(super::namespace_values(namespace_response(string_array(values), None)).is_err());
        assert!(super::namespace_values(namespace_response(
            opcua::types::Variant::from(vec![1_i32]),
            None
        ))
        .is_err());
    }

    #[test]
    fn complete_direct_mdns_resolution_and_empty_snapshot_are_authoritative() {
        let control = super::SessionOperationControl::new(std::time::Duration::from_secs(1));
        let (observed, attempts) = super::merge_observation(
            registrations(&[]),
            Ok(network_records(&["opc.tcp://192.0.2.2:4860"])),
            &control,
            0,
            64,
            |url, _| Ok(registrations(&[("urn:network", url)])),
        );
        assert!(observed.authoritative);
        assert_eq!(attempts, 1);
        assert_eq!(observed.sources.len(), 1);
        let (empty, attempts) = super::merge_observation(
            registrations(&[]),
            Ok(network_records(&[])),
            &control,
            0,
            64,
            |_, _| panic!("empty discovery must not probe"),
        );
        assert!(empty.authoritative);
        assert!(empty.sources.is_empty());
        assert_eq!(attempts, 0);
    }

    #[test]
    fn malformed_advertisements_cannot_authorize_absence() {
        let (observed, _) = super::merge_observation(
            registrations(&[
                ("", "opc.tcp://192.0.2.1:4860"),
                ("urn:bad", "http://192.0.2.2/"),
            ]),
            Ok(network_records(&[])),
            &super::SessionOperationControl::new(std::time::Duration::from_secs(1)),
            0,
            64,
            |_, _| panic!("no network candidates"),
        );
        assert!(!observed.authoritative);
        assert!(observed.sources.is_empty());
    }

    fn cached_observation(
        observer: &mut super::DiscoveryObserver,
        urls: &[String],
        now: super::Instant,
        lookup: impl FnMut(
            &str,
            super::SessionOperationControl,
        ) -> Result<super::FindServersResponse, String>,
    ) -> super::DiscoveryObservation {
        let refs = urls.iter().map(String::as_str).collect::<Vec<_>>();
        observer
            .merge(
                registrations(&[]),
                Ok(network_records(&refs)),
                &super::SessionOperationControl::new(std::time::Duration::from_secs(10)),
                64,
                now,
                lookup,
            )
            .0
    }

    fn identity_for_url(
        url: &str,
        _: super::SessionOperationControl,
    ) -> Result<super::FindServersResponse, String> {
        Ok(registrations(&[(&format!("urn:fixture:{url}"), url)]))
    }

    #[test]
    fn hundred_mdns_only_sources_become_complete_after_bounded_cache_warmup() {
        let mut observer = super::DiscoveryObserver::new();
        let now = super::Instant::now();
        let urls = (0..100)
            .map(|index| format!("opc.tcp://192.0.2.1:{}/", 4860 + index))
            .collect::<Vec<_>>();
        let first = cached_observation(&mut observer, &urls, now, identity_for_url);
        assert!(!first.authoritative);
        assert_eq!(first.sources.len(), 64);
        let second = cached_observation(
            &mut observer,
            &urls,
            now + std::time::Duration::from_secs(1),
            identity_for_url,
        );
        assert!(second.authoritative);
        assert_eq!(second.sources.len(), 100);
        let removed_url = urls[0].clone();
        let remaining = urls[1..].to_vec();
        let after_removal = cached_observation(
            &mut observer,
            &remaining,
            now + std::time::Duration::from_secs(2),
            identity_for_url,
        );
        assert!(after_removal.authoritative);
        assert_eq!(after_removal.sources.len(), 99);
        assert!(!after_removal
            .sources
            .iter()
            .any(|(_, endpoint)| endpoint == &removed_url));
        assert_eq!(observer.network_identities.len(), 99);
    }

    #[test]
    fn unchanged_mdns_url_is_refreshed_and_replaces_its_previous_application_identity() {
        let mut observer = super::DiscoveryObserver::new();
        let now = super::Instant::now();
        let urls = vec!["opc.tcp://192.0.2.1:4860/".to_string()];
        let first = cached_observation(&mut observer, &urls, now, |url, _| {
            Ok(registrations(&[("urn:old", url)]))
        });
        assert_eq!(first.sources[0].0, "urn:old");
        let replacement = cached_observation(
            &mut observer,
            &urls,
            now + std::time::Duration::from_secs(1),
            |url, _| Ok(registrations(&[("urn:new", url)])),
        );
        assert!(replacement.authoritative);
        assert_eq!(
            replacement.sources,
            vec![("urn:new".into(), urls[0].clone())]
        );
    }

    #[test]
    fn failed_cached_refresh_is_partial_and_cannot_fabricate_absence() {
        let mut observer = super::DiscoveryObserver::new();
        let now = super::Instant::now();
        let urls = vec!["opc.tcp://192.0.2.1:4860/".to_string()];
        let initial = cached_observation(&mut observer, &urls, now, identity_for_url);
        let failed = cached_observation(
            &mut observer,
            &urls,
            now + std::time::Duration::from_secs(1),
            |_, _| Err("offline".into()),
        );
        assert!(!failed.authoritative);
        assert_eq!(failed.sources, initial.sources);
        let refs = urls.iter().map(String::as_str).collect::<Vec<_>>();
        let unrefreshed = observer
            .merge(
                registrations(&[]),
                Ok(network_records(&refs)),
                &super::SessionOperationControl::new(std::time::Duration::ZERO),
                64,
                now + std::time::Duration::from_secs(2),
                |_, _| panic!("expired cycle must retain the previous failed-refresh state"),
            )
            .0;
        assert!(!unrefreshed.authoritative);
        assert_eq!(unrefreshed.sources, initial.sources);
        let recovered = cached_observation(
            &mut observer,
            &urls,
            now + std::time::Duration::from_secs(3),
            identity_for_url,
        );
        assert!(recovered.authoritative);
    }

    #[test]
    fn network_listing_failure_does_not_prune_cached_identities() {
        let mut observer = super::DiscoveryObserver::new();
        let now = super::Instant::now();
        let urls = vec!["opc.tcp://192.0.2.1:4860/".to_string()];
        let initial = cached_observation(&mut observer, &urls, now, identity_for_url);
        let partial = observer
            .merge(
                registrations(&[]),
                Err("network listing failed".into()),
                &super::SessionOperationControl::new(std::time::Duration::from_secs(1)),
                64,
                now + std::time::Duration::from_secs(1),
                |_, _| panic!("no complete network listing"),
            )
            .0;
        assert!(!partial.authoritative);
        assert_eq!(partial.sources, initial.sources);
        assert_eq!(observer.network_identities.len(), 1);
        let absent = cached_observation(
            &mut observer,
            &[],
            now + std::time::Duration::from_secs(2),
            identity_for_url,
        );
        assert!(absent.authoritative);
        assert!(absent.sources.is_empty());
        assert!(observer.network_identities.is_empty());
    }

    #[test]
    fn expired_identity_without_successful_refresh_is_partial_not_authoritative_absence() {
        let mut observer = super::DiscoveryObserver::new();
        let now = super::Instant::now();
        let urls = vec!["opc.tcp://192.0.2.1:4860/".to_string()];
        cached_observation(&mut observer, &urls, now, identity_for_url);
        let expired = cached_observation(
            &mut observer,
            &urls,
            now + super::NETWORK_IDENTITY_MAX_AGE + std::time::Duration::from_secs(1),
            |_, _| Err("offline".into()),
        );
        assert!(!expired.authoritative);
        assert!(expired.sources.is_empty());
        assert_eq!(observer.network_identities.len(), 1);
    }

    #[test]
    fn fresh_primary_identity_supersedes_cache_during_network_listing_failure() {
        let mut observer = super::DiscoveryObserver::new();
        let now = super::Instant::now();
        let url = "opc.tcp://192.0.2.1:4860/";
        cached_observation(&mut observer, &[url.to_string()], now, |url, _| {
            Ok(registrations(&[("urn:old", url)]))
        });
        let partial = observer
            .merge(
                registrations(&[("urn:new", url)]),
                Err("mDNS unavailable".into()),
                &super::SessionOperationControl::new(std::time::Duration::from_secs(1)),
                64,
                now + std::time::Duration::from_secs(1),
                |_, _| panic!("no network listing"),
            )
            .0;
        assert!(!partial.authoritative);
        assert_eq!(partial.sources, vec![("urn:new".into(), url.to_string())]);
    }
}

#[cfg(test)]
mod observer_recovery_tests {
    use super::*;

    fn seeded() -> DiscoveryObserver {
        let mut observer = DiscoveryObserver::new();
        observer.scan_cursor = 10;
        observer.network_identities.insert(
            "old".into(),
            CachedNetworkIdentity {
                sources: vec![("urn:old".into(), "opc.tcp://localhost:51001".into())],
                resolved_at: Instant::now(),
                refresh_failed: false,
            },
        );
        observer
    }

    #[test]
    fn observer_panic_discards_partial_cache_and_next_cycle_can_succeed() {
        let mut observer = seeded();
        assert!(observer
            .protected_observation(|_| panic!("simulated protocol panic"))
            .is_err());
        assert!(observer.network_identities.is_empty());
        assert_eq!(observer.scan_cursor, 0);
        let result = observer
            .protected_observation(|_| {
                Ok(DiscoveryObservation {
                    sources: vec![("urn:new".into(), "opc.tcp://localhost:51002".into())],
                    authoritative: true,
                })
            })
            .unwrap();
        assert_eq!(result.sources.len(), 1);
        assert!(result.authoritative);
    }

    #[test]
    fn disconnected_observation_is_an_error_not_an_authoritative_empty_snapshot() {
        let mut observer = seeded();
        for _ in 0..100 {
            assert!(observer
                .protected_observation(|_| Err("BadNotConnected".into()))
                .is_err());
        }
        assert_eq!(observer.network_identities.len(), 1);
        assert_eq!(observer.scan_cursor, 10);
    }
}
