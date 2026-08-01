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
use std::time::Duration;

use crate::server_discovery::config::update_config_with_discovered_servers;

static ENDPOINT_FAILURES: OnceLock<Mutex<HashMap<String, u8>>> = OnceLock::new();
static LDS_SESSION: OnceLock<Mutex<Option<Arc<RwLock<Session>>>>> = OnceLock::new();
static ENDPOINT_FAILURE_THRESHOLD: OnceLock<u8> = OnceLock::new();
static ENDPOINT_CONNECT_TIMEOUT: OnceLock<Duration> = OnceLock::new();
const DEFAULT_ENDPOINT_FAILURE_THRESHOLD: u8 = 5;
const DEFAULT_ENDPOINT_CONNECT_TIMEOUT_MS: u64 = 2_000;
const MAX_ENDPOINT_FAILURE_THRESHOLD: u64 = 100;
const MAX_ENDPOINT_CONNECT_TIMEOUT_MS: u64 = 60_000;

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

// Namespaces von einem Server abfragen
pub fn get_namespaces(server_url: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    println!("\n=== Lese Namespaces von {} ===", server_url);

    let mut client = ClientBuilder::new()
        .application_name("Namespace Reader")
        .application_uri("urn:NamespaceReader")
        .trust_server_certs(true)
        .create_sample_keypair(true)
        .session_retry_limit(3)
        .client()
        .ok_or("Failed to create OPC UA client")?;

    let endpoint: EndpointDescription = (
        server_url,
        SecurityPolicy::None.to_str(),
        MessageSecurityMode::None,
        UserTokenPolicy::anonymous(),
    )
        .into();

    println!("Verbinde zu Endpoint: {}", server_url);

    let session = client.connect_to_endpoint(endpoint, IdentityToken::Anonymous)?;
    let session_guard = session.read();

    let namespace_array_node = NodeId::new(0, 2255u32);

    let read_value_id = ReadValueId {
        node_id: namespace_array_node,
        attribute_id: AttributeId::Value as u32,
        index_range: UAString::null(),
        data_encoding: opcua::types::QualifiedName::null(),
    };

    let request = ReadRequest {
        request_header: session_guard.make_request_header(),
        max_age: 0.0,
        timestamps_to_return: TimestampsToReturn::Neither,
        nodes_to_read: Some(vec![read_value_id]),
    };

    match session_guard.send_request(request) {
        Ok(response) => {
            if let SupportedMessage::ReadResponse(read_response) = response {
                if let Some(results) = read_response.results {
                    if let Some(result) = results.first() {
                        if let Some(value) = &result.value {
                            if let opcua::types::Variant::Array(arr) = value {
                                let namespaces: Vec<String> = arr
                                    .values
                                    .iter()
                                    .filter_map(|v| {
                                        if let opcua::types::Variant::String(s) = v {
                                            s.value().as_ref().cloned()
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();

                                println!("✓ Gefundene Namespaces: {:?}", namespaces);
                                return Ok(namespaces);
                            }
                        }
                    }
                }
            }
            Err("Keine Namespaces gefunden".into())
        }
        Err(e) => {
            eprintln!("Fehler beim Lesen der Namespaces: {:?}", e);
            Err(e.into())
        }
    }
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
}
