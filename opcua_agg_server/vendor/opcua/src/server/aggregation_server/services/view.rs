use std::{collections::HashSet, sync::Arc};
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
use crate::types::ByteString;
use crate::types::ReferenceDescription;

use super::service_delegation::delegate_service_call;
use super::service_delegation::DecidingField;
use super::service_delegation::{ServiceResultItem, TransformableItem};

// A small batch remains compatible with constrained embedded servers and keeps
// their simultaneous ContinuationPoint count bounded.
const LOWER_SERVER_BROWSE_BATCH_SIZE: usize = 4;

impl TransformableItem for BrowseDescription {
    fn node_ids(&mut self) -> Vec<&mut NodeId> {
        vec![&mut self.node_id, &mut self.reference_type_id]
    }
    fn deciding_field(&self) -> Option<DecidingField> {
        Some(DecidingField::NodeId(&self.node_id))
    }
}

impl ServiceResultItem for BrowseResult {
    fn from_status_code(status_code: StatusCode) -> Self {
        Self {
            status_code,
            continuation_point: ByteString::null(),
            references: None,
        }
    }
}
impl ServiceResultItem for BrowsePathResult {
    fn from_status_code(status_code: StatusCode) -> Self {
        Self {
            status_code,
            targets: None,
        }
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

fn normalize_source_browse_error(status_code: StatusCode) -> StatusCode {
    match status_code {
        StatusCode::BadCommunicationError
        | StatusCode::BadConnectionClosed
        | StatusCode::BadNotConnected => StatusCode::BadServerNotConnected,
        _ => status_code,
    }
}

fn browse_error_results(count: usize, status_code: StatusCode) -> Vec<BrowseResult> {
    let status_code = normalize_source_browse_error(status_code);
    (0..count)
        .map(|_| BrowseResult {
            status_code,
            continuation_point: ByteString::null(),
            references: None,
        })
        .collect()
}

fn collect_source_browse_pages<F>(
    mut result: BrowseResult,
    mut browse_next: F,
) -> Result<BrowseResult, StatusCode>
where
    F: FnMut(&ByteString) -> Result<Option<Vec<BrowseResult>>, StatusCode>,
{
    if result.status_code.is_bad() {
        result.continuation_point = ByteString::null();
        return Ok(result);
    }

    let mut references = result.references.take().unwrap_or_default();
    let mut continuation_point =
        std::mem::replace(&mut result.continuation_point, ByteString::null());
    let mut seen_continuation_points = HashSet::new();

    while !continuation_point.is_null() {
        if !seen_continuation_points.insert(continuation_point.clone()) {
            return Err(StatusCode::BadUnexpectedError);
        }

        let Some(mut next_results) = browse_next(&continuation_point)? else {
            return Err(StatusCode::BadUnknownResponse);
        };
        if next_results.len() != 1 {
            return Err(StatusCode::BadUnexpectedError);
        }

        let mut next = next_results.remove(0);
        if next.status_code.is_bad() {
            next.continuation_point = ByteString::null();
            next.references = None;
            return Ok(next);
        }
        references.extend(next.references.take().unwrap_or_default());
        continuation_point = std::mem::replace(&mut next.continuation_point, ByteString::null());
    }

    result.references = Some(references);
    Ok(result)
}

fn browse_source_all_pages(
    client_session_p: Arc<RwLock<ClientSession>>,
    browses: &[BrowseDescription],
) -> Vec<BrowseResult> {
    let session = client_session_p.read();
    let mut merged_results = Vec::with_capacity(browses.len());

    for browse_batch in browses.chunks(LOWER_SERVER_BROWSE_BATCH_SIZE) {
        let initial_results = match session.browse(browse_batch) {
            Ok(Some(results)) if results.len() == browse_batch.len() => results,
            Ok(Some(results)) => {
                warn!(
                    "Lower server returned {} BrowseResults for {} requests",
                    results.len(),
                    browse_batch.len()
                );
                let remaining = browses.len() - merged_results.len();
                merged_results.extend(browse_error_results(
                    remaining,
                    StatusCode::BadUnexpectedError,
                ));
                return merged_results;
            }
            Ok(None) => {
                let remaining = browses.len() - merged_results.len();
                merged_results.extend(browse_error_results(
                    remaining,
                    StatusCode::BadUnknownResponse,
                ));
                return merged_results;
            }
            Err(status_code) => {
                warn!("Lower server Browse failed: {}", status_code.name());
                let remaining = browses.len() - merged_results.len();
                merged_results.extend(browse_error_results(remaining, status_code));
                return merged_results;
            }
        };

        for result in initial_results {
            match collect_source_browse_pages(result, |continuation_point| {
                session.browse_next(false, &[continuation_point.clone()])
            }) {
                Ok(result) => merged_results.push(result),
                Err(status_code) => {
                    warn!("Lower server BrowseNext failed: {}", status_code.name());
                    merged_results.extend(browse_error_results(1, status_code));
                    if normalize_source_browse_error(status_code)
                        == StatusCode::BadServerNotConnected
                    {
                        let remaining = browses.len() - merged_results.len();
                        merged_results.extend(browse_error_results(remaining, status_code));
                        return merged_results;
                    }
                }
            }
        }
    }

    merged_results
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
                Ok(Some(browse_source_all_pages(client_session_p, browses)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LocalizedText, NodeClass, QualifiedName, ReferenceTypeId};

    fn reference(node_id: u32, name: &str) -> ReferenceDescription {
        ReferenceDescription {
            reference_type_id: ReferenceTypeId::HasComponent.into(),
            is_forward: true,
            node_id: NodeId::new(1, node_id).into(),
            browse_name: QualifiedName::new(1, name),
            display_name: LocalizedText::from(name),
            node_class: NodeClass::Variable,
            type_definition: NodeId::new(0, 63u32).into(),
        }
    }

    fn browse_page(
        status_code: StatusCode,
        continuation_point: ByteString,
        references: Vec<ReferenceDescription>,
    ) -> BrowseResult {
        BrowseResult {
            status_code,
            continuation_point,
            references: Some(references),
        }
    }

    #[test]
    fn source_continuation_points_are_drained_before_returning() {
        let continuation_point = ByteString::from(vec![1, 2, 3]);
        let first = browse_page(
            StatusCode::Good,
            continuation_point.clone(),
            vec![reference(1, "First")],
        );
        let mut browse_next_calls = 0;

        let merged = collect_source_browse_pages(first, |received| {
            browse_next_calls += 1;
            assert_eq!(received, &continuation_point);
            Ok(Some(vec![browse_page(
                StatusCode::Good,
                ByteString::null(),
                vec![reference(2, "Second")],
            )]))
        })
        .expect("all source pages should be merged");

        assert_eq!(browse_next_calls, 1);
        assert!(merged.continuation_point.is_null());
        let references = merged.references.expect("merged references");
        assert_eq!(references.len(), 2);
        assert_eq!(references[0].node_id.node_id, NodeId::new(1, 1u32));
        assert_eq!(references[1].node_id.node_id, NodeId::new(1, 2u32));
    }

    #[test]
    fn repeated_source_continuation_point_fails_closed() {
        let continuation_point = ByteString::from(vec![4, 5, 6]);
        let first = browse_page(StatusCode::Good, continuation_point.clone(), Vec::new());

        let error = collect_source_browse_pages(first, |_| {
            Ok(Some(vec![browse_page(
                StatusCode::Good,
                continuation_point.clone(),
                Vec::new(),
            )]))
        })
        .expect_err("a repeated source continuation point must not loop");

        assert_eq!(error, StatusCode::BadUnexpectedError);
    }

    #[test]
    fn disconnected_source_returns_one_result_per_requested_node() {
        let results = browse_error_results(3, StatusCode::BadConnectionClosed);

        assert_eq!(results.len(), 3);
        assert!(results
            .iter()
            .all(|result| result.status_code == StatusCode::BadServerNotConnected));
    }
}
