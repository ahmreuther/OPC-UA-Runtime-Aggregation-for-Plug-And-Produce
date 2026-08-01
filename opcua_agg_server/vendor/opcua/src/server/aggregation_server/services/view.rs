use std::sync::Arc;
use tracing::instrument;

use crate::client::prelude::BrowseDescription;
use crate::client::prelude::BrowsePath;
use crate::client::prelude::BrowsePathResult;
use crate::client::prelude::BrowsePathTarget;
use crate::client::prelude::BrowseRequest;
use crate::client::prelude::BrowseResponse;
use crate::client::prelude::BrowseResult;
use crate::client::prelude::ExpandedNodeId;
use crate::client::prelude::NodeId;
use crate::client::prelude::QualifiedName;
use crate::client::prelude::ResponseHeader;
use crate::client::prelude::Session as ClientSession;
use crate::client::prelude::TranslateBrowsePathsToNodeIdsRequest;
use crate::client::prelude::TranslateBrowsePathsToNodeIdsResponse;
use crate::client::prelude::ViewService as _;
use crate::core::supported_message::SupportedMessage;
use crate::prelude::StatusCode;
use crate::server::address_space::relative_path;
use crate::server::aggregation_server::aggregation_server::AggregationServer;
use crate::server::session::Session as ServerSession;
use crate::server::{address_space::AddressSpace, services::view::ViewService};
use crate::sync::RwLock;
use crate::types::BrowseDirection;
use crate::types::ReferenceDescription;

use super::service_delegation::delegate_service_call;
use super::service_delegation::DecidingField;
use super::service_delegation::TransformableItem;

impl TransformableItem for BrowseDescription {
    fn node_ids(&mut self) -> Vec<&mut NodeId> {
        vec![&mut self.node_id, &mut self.reference_type_id]
    }
    fn deciding_field(&self) -> Option<DecidingField> {
        Some(DecidingField::NodeId(&self.node_id))
    }
}

impl TransformableItem for BrowseResult {
    fn node_ids(&mut self) -> Vec<&mut NodeId> {
        let mut transformable_nodes: Vec<&mut NodeId> = Vec::new();
        let Some(refs) = &mut self.references else {
            return transformable_nodes;
        };
        for r in refs {
            transformable_nodes.push(&mut r.node_id.node_id);
            transformable_nodes.push(&mut r.reference_type_id);
            transformable_nodes.push(&mut r.type_definition.node_id);
        }
        return transformable_nodes;
    }
    fn browse_names(&mut self) -> Vec<&mut QualifiedName> {
        let mut qnames: Vec<&mut QualifiedName> = Vec::new();
        let Some(refs) = &mut self.references else {
            return qnames;
        };
        for r in refs {
            qnames.push(&mut r.browse_name);
        }
        return qnames;
    }
}

impl TransformableItem for BrowsePath {
    fn node_ids(&mut self) -> Vec<&mut NodeId> {
        let mut node_ids = vec![&mut self.starting_node];
        if let Some(path) = &mut self.relative_path.elements {
            for el in path {
                node_ids.push(&mut el.reference_type_id);
            }
        }
        node_ids
    }
    fn browse_names(&mut self) -> Vec<&mut QualifiedName> {
        let mut qnames = Vec::new();
        if let Some(path) = &mut self.relative_path.elements {
            for el in path {
                qnames.push(&mut el.target_name);
            }
        }
        qnames
    }
    fn deciding_field(&self) -> Option<DecidingField> {
        Some(DecidingField::NodeId(&self.starting_node))
    }
}

impl TransformableItem for BrowsePathResult {
    fn node_ids(&mut self) -> Vec<&mut NodeId> {
        let mut node_ids = Vec::new();
        if let Some(targets) = &mut self.targets {
            for target in targets {
                node_ids.push(&mut target.target_id.node_id)
            }
        }
        node_ids
    }
}

pub(crate) trait AggServerViewService {
    fn browse_lower_servers(
        &self,
        session_p: Arc<RwLock<ServerSession>>,
        address_space_p: Arc<RwLock<AddressSpace>>,
        aggregation_server: AggregationServer,
        browse_request: &BrowseRequest,
        nodes_to_browse: &Vec<BrowseDescription>,
        max_references_per_node: u32,
    ) -> SupportedMessage;
    fn translate_browse_paths_lservers(
        &self,
        aggregation_server: AggregationServer,
        address_space_p: Arc<RwLock<AddressSpace>>,
        request: &TranslateBrowsePathsToNodeIdsRequest,
        browse_paths: &Vec<BrowsePath>,
    ) -> SupportedMessage;
}

impl AggServerViewService for ViewService {
    #[instrument(
        level = "trace",
        ret,
        skip(self, session_p, address_space_p, aggregation_server)
    )]
    fn browse_lower_servers(
        &self,
        session_p: Arc<RwLock<ServerSession>>,
        address_space_p: Arc<RwLock<AddressSpace>>,
        aggregation_server: AggregationServer,
        browse_request: &BrowseRequest,
        nodes_to_browse: &Vec<BrowseDescription>,
        max_references_per_node: u32,
    ) -> SupportedMessage {
        let aggserver_browse_call = |browses: &Vec<BrowseDescription>| {
            let mut session = session_p.write();
            let address_space = address_space_p.read();
            let results = Self::browse_nodes(
                &mut session,
                &address_space,
                browses,
                max_references_per_node as usize,
            );
            return results;
        };
        let lserver_browse_call =
            |_lserver_id: &u16,
             client_session_p: Arc<RwLock<ClientSession>>,
             browses: &Vec<BrowseDescription>| {
                return client_session_p.read().browse(browses.as_ref());
            };

        let map_db = match aggregation_server.map_db_p.read().connect() {
            Ok(map_db) => map_db,
            Err(e) => {
                tracing::warn!("Could not connect to Mapping Database! {:?}", e);
                return BrowseResponse {
                    response_header: ResponseHeader::new_service_result(
                        &browse_request.request_header,
                        StatusCode::BadUnexpectedError,
                    ),
                    results: None,
                    diagnostic_infos: None,
                }
                .into();
            }
        };

        let res = delegate_service_call(
            &map_db,
            &aggregation_server,
            nodes_to_browse,
            true,
            aggserver_browse_call,
            lserver_browse_call,
        );
        return match res {
            Ok(mut r) => {
                for br in &r {
                    if br.status_code.is_bad() {
                        warn!("Aggregation Server service browse_lower_servers returned bad BrowseResult: {:?}", br.status_code.name());
                    }
                }

                // Inject additional browse references from instance aggregation
                if let Ok(map_db) = aggregation_server.map_db_p.read().connect() {
                    for (index, bd) in nodes_to_browse.iter().enumerate() {
                        let Ok(new_refs) = map_db.get_references(&bd.node_id) else {
                            warn!("Error getting additional browse references.");
                            continue;
                        };
                        let new_refs_filtered: Vec<ReferenceDescription> = new_refs
                            .into_iter()
                            .filter(|x| {
                                if bd.browse_direction == BrowseDirection::Forward {
                                    x.is_forward
                                } else if bd.browse_direction == BrowseDirection::Inverse {
                                    !x.is_forward
                                } else if bd.browse_direction == BrowseDirection::Both {
                                    true
                                } else {
                                    false
                                }
                            })
                            .collect();
                        let Some(br) = r.get_mut(index) else { continue };
                        if let Some(refs) = br.references.as_mut() {
                            refs.extend(new_refs_filtered);
                        } else {
                            br.references = Some(new_refs_filtered);
                        }
                    }
                } else {
                    warn!("Error connecting to mapping database for additional browse references.");
                }

                BrowseResponse {
                    response_header: ResponseHeader::new_good(&browse_request.request_header),
                    results: Some(r),
                    diagnostic_infos: None,
                }
                .into()
            }
            Err(sc) => {
                warn!(
                    "Aggregation Server service browse_lower_servers returned bad statuscode: {:?}",
                    sc.name()
                );
                BrowseResponse {
                    response_header: ResponseHeader::new_service_result(
                        &browse_request.request_header,
                        sc,
                    ),
                    results: None,
                    diagnostic_infos: None,
                }
                .into()
            }
        };
    }

    fn translate_browse_paths_lservers(
        &self,
        aggregation_server: AggregationServer,
        address_space_p: Arc<RwLock<AddressSpace>>,
        request: &TranslateBrowsePathsToNodeIdsRequest,
        browse_paths: &Vec<BrowsePath>,
    ) -> SupportedMessage {
        let aggserver_browse_call = |browse_paths: &Vec<BrowsePath>| {
            let address_space = address_space_p.read();
            let results = browse_paths
                .iter()
                .enumerate()
                .map(|(i, browse_path)| {
                    trace!("Processing browse path {}", i);
                    let node_id = browse_path.starting_node.clone();
                    if browse_path.relative_path.elements.is_none() {
                        BrowsePathResult {
                            status_code: StatusCode::BadNothingToDo,
                            targets: None,
                        }
                    } else {
                        // Starting from the node_id, find paths
                        match relative_path::find_nodes_relative_path(
                            &address_space,
                            &node_id,
                            &browse_path.relative_path,
                        ) {
                            Err(err) => {
                                trace!(
                                    "Browse path result for find nodes returned in error {}",
                                    err.name()
                                );
                                BrowsePathResult {
                                    status_code: err,
                                    targets: None,
                                }
                            }
                            Ok(result) => {
                                let targets = if !result.is_empty() {
                                    use std::u32;
                                    let targets = result
                                        .iter()
                                        .map(|node_id| BrowsePathTarget {
                                            target_id: ExpandedNodeId::new(node_id.clone()),
                                            remaining_path_index: u32::MAX,
                                        })
                                        .collect();
                                    Some(targets)
                                } else {
                                    None
                                };
                                BrowsePathResult {
                                    status_code: StatusCode::Good,
                                    targets,
                                }
                            }
                        }
                    }
                })
                .collect();
            return results;
        };
        let lserver_browse_call =
            |_lserver_id: &u16,
             client_session_p: Arc<RwLock<ClientSession>>,
             browse_paths: &Vec<BrowsePath>| {
                return client_session_p
                    .read()
                    .translate_browse_paths_to_node_ids(browse_paths)
                    .map(|r| Some(r));
            };

        let map_db = match aggregation_server.map_db_p.read().connect() {
            Ok(map_db) => map_db,
            Err(e) => {
                tracing::warn!("Could not connect to Mapping Database! {:?}", e);
                return BrowseResponse {
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

        let res = delegate_service_call(
            &map_db,
            &aggregation_server,
            browse_paths,
            true,
            aggserver_browse_call,
            lserver_browse_call,
        );

        return match res {
            Ok(r) => {
                for br in &r {
                    if br.status_code.is_bad() {
                        warn!("Aggregation Server service translate_browse_paths_lservers returned bad statuscode: {:?}", br.status_code.name());
                    }
                }
                TranslateBrowsePathsToNodeIdsResponse {
                    response_header: ResponseHeader::new_good(&request.request_header),
                    results: Some(r),
                    diagnostic_infos: None,
                }
                .into()
            }
            Err(sc) => {
                warn!("Aggregation Server service translate_browse_paths_lservers returned bad statuscode: {:?}", sc.name());
                TranslateBrowsePathsToNodeIdsResponse {
                    response_header: ResponseHeader::new_service_result(
                        &request.request_header,
                        sc,
                    ),
                    results: None,
                    diagnostic_infos: None,
                }
                .into()
            }
        };
    }
}
