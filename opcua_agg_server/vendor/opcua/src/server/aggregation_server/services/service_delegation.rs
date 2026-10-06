use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use tracing::instrument;

use crate::client::prelude::{Identifier, NodeId, ObjectId, QualifiedName, Session, StatusCode};
use crate::server::aggregation_server::aggregation_server::AggregationServer;
use crate::server::aggregation_server::error_types::ServiceDelegationError;
use crate::server::aggregation_server::map_db::MapDatabaseConnection;
use crate::sync::RwLock;

// These are the NodeId identifiers for the Server Object of all OPCUA servers
static SERVER_OBJECT_NODE_IDS: [u32; 328] = [
    2253, 2254, 2255, 2256, 2267, 2994, 12885, 17634, 2268, 2274, 2295, 2296, 11715, 11492, 12873,
    12749, 12886, 17594, 14443, 2257, 2258, 2259, 2260, 2992, 2993, 2269, 2271, 2272, 2735, 2736,
    2737, 3704, 11702, 11703, 12911, 11704, 2996, 2997, 15606, 11192, 2275, 2289, 2290, 3706, 2294,
    3709, 15957, 11493, 11494, 12874, 12750, 12751, 12887, 15215, 15440, 15443, 17366, 17369,
    17371, 17405, 17409, 17481, 2262, 2263, 2261, 2264, 2265, 2266, 11705, 12165, 12166, 11707,
    12167, 12168, 11709, 11710, 11711, 11712, 11713, 11714, 16301, 16304, 15644, 15656, 15668,
    15680, 16036, 15692, 15716, 15704, 11193, 11242, 11273, 11274, 11196, 11197, 11198, 11199,
    11200, 11281, 11282, 11283, 11502, 11275, 11201, 19091, 2276, 2277, 2278, 2279, 3705, 2281,
    2282, 2284, 2285, 2286, 2287, 2288, 3707, 3708, 15958, 15959, 15960, 15961, 15962, 15963,
    15964, 16134, 16135, 16136, 15216, 15217, 15441, 15442, 15444, 15447, 17367, 17368, 17370,
    17406, 17410, 17411, 17416, 17421, 17422, 17423, 17457, 16302, 16303, 16305, 16192, 16193,
    15412, 16194, 15413, 15648, 15650, 16195, 16197, 16199, 16201, 16203, 16204, 15414, 16205,
    15415, 15660, 15662, 16206, 16208, 16210, 16212, 16214, 16215, 15416, 16216, 15417, 15672,
    15674, 16217, 16219, 16221, 16223, 16225, 16226, 15418, 16227, 15423, 15684, 15686, 16228,
    16230, 16232, 16234, 16236, 16237, 15424, 16238, 15425, 16041, 16043, 16239, 16241, 16243,
    16245, 16247, 16248, 15426, 16249, 15427, 15696, 15698, 16250, 16252, 16254, 16256, 16269,
    16270, 15428, 16271, 15429, 15720, 15722, 16272, 16274, 16276, 16278, 16258, 16259, 15430,
    16260, 15527, 15708, 15710, 16261, 16263, 16265, 16267, 15445, 15446, 15448, 17412, 17413,
    17414, 17417, 17418, 17419, 17424, 17431, 17436, 17441, 17446, 17451, 17458, 17460, 17462,
    17464, 15649, 15651, 16196, 16198, 16200, 16202, 15661, 15663, 16207, 16209, 16211, 16213,
    15673, 15675, 16218, 16220, 16222, 16224, 15685, 15687, 16229, 16231, 16233, 16235, 16042,
    16044, 16240, 16242, 16244, 16246, 15697, 15699, 16251, 16253, 16255, 16257, 15721, 15723,
    16273, 16275, 16277, 16279, 15709, 15711, 16262, 16264, 16266, 16268, 17425, 17426, 17429,
    17432, 17433, 17434, 17437, 17438, 17439, 17442, 17443, 17444, 17447, 17448, 17449, 17452,
    17453, 17454, 17459, 17461, 17463, 17466, 23470,
];

#[derive(Debug)]
pub struct IndexedItem<T> {
    /// The index of the BD in the browse request
    pub index: usize,
    pub item: T,
}

#[derive(Debug)]
pub struct SeparatedItems<T> {
    pub aggserver_items: Vec<IndexedItem<T>>,
    /// Items sorted by lower server id
    pub lserver_items: HashMap<u16, Vec<IndexedItem<T>>>,
}
impl<T> SeparatedItems<T> {
    fn new() -> Self {
        Self {
            aggserver_items: Vec::new(),
            lserver_items: HashMap::new(),
        }
    }
    fn add_item_to_aggserver_list(&mut self, index: usize, item: T) {
        let indexed_item = IndexedItem { index, item };
        self.aggserver_items.push(indexed_item);
    }
    fn add_item_to_lserver_list(&mut self, lserver_id: u16, index: usize, item: T) {
        let indexed_item = IndexedItem { index, item };
        if let Some(vec) = self.lserver_items.get_mut(&lserver_id) {
            vec.push(indexed_item);
        } else {
            self.lserver_items
                .insert(lserver_id.clone(), vec![indexed_item]);
        }
    }
}

#[derive(Clone, Debug)]
pub struct MonitoredItem {
    pub sub_id: u32,
    pub mitem_id: u32,
}

#[derive(Debug)]
pub enum DecidingField<'a> {
    NodeId(&'a NodeId),
    MonitoredItem(&'a MonitoredItem),
}

pub trait TransformableItem {
    fn node_ids(&mut self) -> Vec<&mut NodeId> {
        Vec::new()
    }
    fn browse_names(&mut self) -> Vec<&mut QualifiedName> {
        Vec::new()
    }
    fn monitored_items(&mut self) -> Vec<&mut MonitoredItem> {
        Vec::new()
    }
    fn deciding_field(&self) -> Option<DecidingField> {
        None
    }
}

/// A service result keeps one response slot for every requested operation.
pub trait ServiceResultItem: TransformableItem {
    fn from_status_code(status_code: StatusCode) -> Self;
}

#[instrument(level = "trace", err, ret, skip(map_db))]
pub fn retransform_item<T>(
    item: &mut T,
    lserver_id: u16,
    map_db: &MapDatabaseConnection,
) -> Result<(), String>
where
    T: TransformableItem + Debug,
{
    for node_id in item.node_ids() {
        if node_id.namespace == 0 {
            let Identifier::Numeric(identifier) = node_id.identifier else {
                continue;
            };
            if !SERVER_OBJECT_NODE_IDS.contains(&identifier) {
                continue;
            }
        }
        // Process the node depending on if its a type or not
        if let Some(retransformed_node_id_bin) = map_db
            .get_aggserver_type(lserver_id, node_id)
            .map_err(|e| e.to_string())?
        {
            *node_id = retransformed_node_id_bin;
            continue;
        };
        if let Ok(retransformed_namespace) =
            map_db.get_aggserver_nsid_inst(lserver_id, node_id.namespace)
        {
            node_id.namespace = retransformed_namespace;
        } else {
            return Err("Could not retransform instance. Namespace not aggregated.".into());
        }
    }
    for qname in item.browse_names() {
        if qname.namespace_index == 0 {
            continue;
        }
        if let Ok(retransformed_namespace) =
            map_db.get_aggserver_nsid_inst(lserver_id, qname.namespace_index)
        {
            qname.namespace_index = retransformed_namespace;
        } else {
            return Err("Could not retransform instance. Namespace not aggregated.".into());
        }
    }
    for mitem in item.monitored_items() {
        if let Some(retransformed_mitem) = map_db
            .get_aggserver_monitored_item(mitem.sub_id, mitem.mitem_id)
            .map_err(|e| e.to_string())?
        {
            mitem.mitem_id = retransformed_mitem;
        };
    }
    Ok(())
}

/// Return None if it should go to the aggregation server itself
#[instrument(level = "trace", ret, skip(map_db))]
fn determine_lower_server(
    map_db: &MapDatabaseConnection,
    deciding_field: Option<DecidingField>,
    transform_root_folder: bool,
) -> Result<Option<u16>, ServiceDelegationError> {
    Ok(match deciding_field {
        None => None,
        Some(DecidingField::NodeId(node_id)) => {
            if transform_root_folder {
                if let Some(source) = map_db.get_lserver_by_root_folder(node_id)? {
                    return Ok(Some(source));
                }
            }
            map_db.get_lserver_by_namespace(&node_id.namespace)?
        }
        Some(DecidingField::MonitoredItem(item)) => {
            map_db.get_lserver_by_monitored_item(item.sub_id, item.mitem_id)?
        }
    })
}

#[instrument(level = "trace", err, ret, skip(map_db))]
pub fn separate_and_transform_items<T>(
    aggregation_server: &AggregationServer,
    map_db: &MapDatabaseConnection,
    items: &Vec<T>,
    transform_root_folder: bool,
) -> Result<SeparatedItems<T>, ServiceDelegationError>
where
    T: TransformableItem + Clone + Debug,
{
    let mut separated_items: SeparatedItems<T> = SeparatedItems::new();

    let global_type_hashmap = aggregation_server.global_type_hashmap_p.read();

    for (idx, item) in items.iter().enumerate() {
        let lserver_id_opt =
            determine_lower_server(map_db, item.deciding_field(), transform_root_folder)?;
        if let Some(lserver_id) = lserver_id_opt {
            // Transform item to lower server namespace
            let mut item_transformed = item.clone();
            let mut type_nodes: Vec<&mut NodeId> = Vec::new();
            for node_id in item_transformed.node_ids() {
                if node_id.namespace == 0 {
                    let Identifier::Numeric(identifier) = node_id.identifier else {
                        continue;
                    };
                    if !SERVER_OBJECT_NODE_IDS.contains(&identifier) {
                        continue;
                    }
                }
                if global_type_hashmap.contains_right(node_id) {
                    type_nodes.push(node_id);
                } else if transform_root_folder && map_db.contains_root_folder(node_id)? {
                    if map_db.get_lserver_by_root_folder(node_id)? != Some(lserver_id) {
                        return Err(crate::server::aggregation_server::error_types::MappingError::InvalidRule(
                            "Request combines root folders from different source servers".into(),
                        ).into());
                    }
                    *node_id = ObjectId::ObjectsFolder.into();
                } else {
                    let namespace = map_db.get_lserver_nsid(node_id.namespace)?;
                    if namespace.id != lserver_id {
                        return Err(crate::server::aggregation_server::error_types::MappingError::InvalidRule(
                            "Request combines nodes from different source servers".into(),
                        ).into());
                    }
                    node_id.namespace = namespace.namespace;
                }
            }
            // Transfrom types
            for type_node in type_nodes {
                *type_node = map_db.get_lserver_type(lserver_id, type_node)?;
            }
            // Transform qualified names
            for qname in item_transformed.browse_names() {
                if qname.namespace_index == 0 {
                    continue;
                } else {
                    let namespace = map_db.get_lserver_nsid(qname.namespace_index)?;
                    if namespace.id != lserver_id {
                        return Err(crate::server::aggregation_server::error_types::MappingError::InvalidRule(
                            "Request combines browse names from different source servers".into(),
                        ).into());
                    }
                    qname.namespace_index = namespace.namespace;
                }
            }
            // Transform monitored item ids
            for mitem in item_transformed.monitored_items() {
                let aggserver_sub_id = mitem.sub_id;
                mitem.mitem_id = map_db
                    .get_lserver_monitored_item(aggserver_sub_id, mitem.mitem_id)?
                    .ok_or_else(|| {
                        crate::server::aggregation_server::error_types::MappingError::InvalidRule(
                            "Monitored item mapping disappeared".into(),
                        )
                    })?;
                mitem.sub_id = map_db
                    .get_lserver_sub_id(lserver_id, aggserver_sub_id)?
                    .lserver_sub_id;
            }

            separated_items.add_item_to_lserver_list(lserver_id, idx, item_transformed);
        } else {
            // dont transform item
            separated_items.add_item_to_aggserver_list(idx, item.clone());
        }
    }
    return Ok(separated_items);
}

#[instrument(level = "trace", ret, skip(service_call))]
pub fn call_agg_server<I, R, F>(
    aggserver_items: Vec<IndexedItem<I>>,
    service_call: F,
) -> Vec<IndexedItem<R>>
where
    I: Debug,
    R: ServiceResultItem + Debug,
    F: FnOnce(&Vec<I>) -> Vec<R>,
{
    let mut results: Vec<IndexedItem<R>> = Vec::new();

    if aggserver_items.len() != 0 {
        let (items, indices): (Vec<I>, Vec<usize>) = aggserver_items
            .into_iter()
            .map(|b| (b.item, b.index))
            .unzip();

        // Browse the nodes
        let response = service_call(&items);
        if response.len() != indices.len() {
            results.extend(indices.into_iter().map(|index| IndexedItem {
                index,
                item: R::from_status_code(StatusCode::BadUnexpectedError),
            }));
        } else {
            results.extend(
                indices
                    .into_iter()
                    .zip(response)
                    .map(|(index, item)| IndexedItem { index, item }),
            );
        }
    }

    return results;
}

#[instrument(
    level = "trace",
    err,
    ret,
    skip(service_call, map_db, lower_server_sessions_p)
)]
pub fn call_lower_server<I, R, F>(
    map_db: &MapDatabaseConnection,
    lserver_items: HashMap<u16, Vec<IndexedItem<I>>>,
    service_call: F,
    lower_server_sessions_p: Arc<RwLock<HashMap<u16, Arc<RwLock<Session>>>>>,
) -> Result<Vec<IndexedItem<R>>, StatusCode>
where
    I: Debug,
    R: ServiceResultItem + Debug,
    F: Fn(&u16, Arc<RwLock<Session>>, &Vec<I>) -> Result<Option<Vec<R>>, StatusCode>,
{
    let mut lserver_results = Vec::new();
    for (lserver_id, group) in lserver_items {
        let (items, indices): (Vec<I>, Vec<usize>) = group
            .into_iter()
            .map(|item| (item.item, item.index))
            .unzip();
        let session = lower_server_sessions_p.read().get(&lserver_id).cloned();
        let response = match session {
            Some(session) => service_call(&lserver_id, session, &items),
            None => Err(StatusCode::BadServerNotConnected),
        };
        let results = match response {
            Ok(Some(results)) if results.len() == indices.len() => results,
            response => {
                let status = match response {
                    Err(status) => status,
                    _ => StatusCode::BadUnexpectedError,
                };
                lserver_results.extend(indices.into_iter().map(|index| IndexedItem {
                    index,
                    item: R::from_status_code(status),
                }));
                continue;
            }
        };
        for (index, mut item) in indices.into_iter().zip(results) {
            if let Err(error) = retransform_item(&mut item, lserver_id, map_db) {
                warn!(
                    "Cannot transform result from source {}: {}",
                    lserver_id, error
                );
                item = R::from_status_code(StatusCode::BadUnexpectedError);
            }
            lserver_results.push(IndexedItem { index, item });
        }
    }
    Ok(lserver_results)
}

pub fn merge_and_order_results<R>(
    mut aggserver_results: Vec<IndexedItem<R>>,
    mut lserver_results: Vec<IndexedItem<R>>,
) -> Vec<R> {
    let mut results_complete: Vec<IndexedItem<R>> = Vec::new();
    results_complete.append(&mut aggserver_results);
    results_complete.append(&mut lserver_results);
    // step 3: assemble answer
    results_complete.sort_by_key(|b| b.index);
    let results_formatted = results_complete.into_iter().map(|b| b.item).collect();
    return results_formatted;
}

#[instrument(
    level = "trace",
    err,
    ret,
    skip(aggserver_service_call, lserver_service_call, map_db)
)]
pub fn delegate_service_call<I, R, F1, F2>(
    map_db: &MapDatabaseConnection,
    aggregation_server: &AggregationServer,
    items: &Vec<I>,
    transform_root_folder: bool,
    aggserver_service_call: F1,
    lserver_service_call: F2,
) -> Result<Vec<R>, StatusCode>
where
    I: TransformableItem + Debug + Clone,
    R: ServiceResultItem + Debug,
    F1: FnOnce(&Vec<I>) -> Vec<R>,
    F2: Fn(&u16, Arc<RwLock<Session>>, &Vec<I>) -> Result<Option<Vec<R>>, StatusCode>,
{
    // Isolate mapping errors as well as source errors to their operation slot.
    // Never forward an unchanged aggregation NodeId or silently reroute on error.
    let mut separated_items = SeparatedItems::new();
    let mut failed_results = Vec::new();
    for (index, item) in items.iter().enumerate() {
        match separate_and_transform_items(
            aggregation_server,
            map_db,
            &vec![item.clone()],
            transform_root_folder,
        ) {
            Ok(separated) => {
                for local in separated.aggserver_items {
                    separated_items.add_item_to_aggserver_list(index, local.item);
                }
                for (source, group) in separated.lserver_items {
                    for remote in group {
                        separated_items.add_item_to_lserver_list(source, index, remote.item);
                    }
                }
            }
            Err(error) => {
                warn!("Cannot route operation {}: {}", index, error);
                failed_results.push(IndexedItem {
                    index,
                    item: R::from_status_code(StatusCode::BadUnexpectedError),
                });
            }
        }
    }

    // Step 2a: send read requests to own server
    let mut aggserver_results =
        call_agg_server(separated_items.aggserver_items, aggserver_service_call);
    aggserver_results.extend(failed_results);

    // Step 2b: send normal browse requests to lower servers

    let lserver_results: Vec<IndexedItem<R>> = match call_lower_server(
        map_db,
        separated_items.lserver_items,
        lserver_service_call,
        aggregation_server.lower_server_sessions_p.clone(),
    ) {
        Ok(res) => res,
        Err(error_code) => {
            return Err(error_code);
        }
    };

    let results_formatted = merge_and_order_results(aggserver_results, lserver_results);
    return Ok(results_formatted);
}

#[cfg(test)]
mod cross_source_tests {
    use super::*;
    use crate::prelude::AddressSpace;
    use crate::types::ReadValueId;

    #[derive(Clone, Debug)]
    struct RelatedNodes {
        target: NodeId,
        related: NodeId,
    }

    impl TransformableItem for RelatedNodes {
        fn node_ids(&mut self) -> Vec<&mut NodeId> {
            vec![&mut self.target, &mut self.related]
        }
        fn deciding_field(&self) -> Option<DecidingField> {
            Some(DecidingField::NodeId(&self.target))
        }
    }

    #[test]
    fn source_root_transformation_rejects_another_sources_root() {
        let address_space = Arc::new(RwLock::new(AddressSpace::new()));
        let aggregation = AggregationServer::new(&address_space).unwrap();
        let db = aggregation.map_db_p.read().connect().unwrap();
        let first_root = NodeId::new(1, "source-a");
        let second_root = NodeId::new(1, "source-b");
        let first = db.insert_lserver(&"source-a".into(), &first_root).unwrap();
        db.insert_lserver(&"source-b".into(), &second_root).unwrap();
        db.insert_namespace(first, 2, 1).unwrap();
        let request = RelatedNodes {
            target: NodeId::new(2, "value"),
            related: second_root,
        };
        assert!(
            separate_and_transform_items(&aggregation, &db, &vec![request.clone()], true).is_err()
        );
        let valid = RelatedNodes {
            related: first_root,
            ..request
        };
        let mut separated =
            separate_and_transform_items(&aggregation, &db, &vec![valid], true).unwrap();
        let mapped = separated
            .lserver_items
            .remove(&first)
            .unwrap()
            .remove(0)
            .item;
        assert_eq!(mapped.target, NodeId::new(1, "value"));
        assert_eq!(mapped.related, ObjectId::ObjectsFolder.into());
    }

    #[test]
    fn malformed_local_response_keeps_each_original_result_index() {
        let items = vec![
            IndexedItem { index: 1, item: () },
            IndexedItem { index: 4, item: () },
        ];
        let results = call_agg_server(items, |_| vec![StatusCode::Good]);
        assert_eq!(results.len(), 2);
        assert_eq!(
            (results[0].index, results[0].item),
            (1, StatusCode::BadUnexpectedError)
        );
        assert_eq!(
            (results[1].index, results[1].item),
            (4, StatusCode::BadUnexpectedError)
        );
    }

    #[test]
    fn disconnected_source_preserves_order_and_local_successes() {
        let address_space = Arc::new(RwLock::new(AddressSpace::new()));
        let aggregation = AggregationServer::new(&address_space).unwrap();
        let db = aggregation.map_db_p.read().connect().unwrap();
        let source = db
            .insert_lserver(&"source".into(), &NodeId::new(1, "source"))
            .unwrap();
        db.insert_namespace(source, 2, 1).unwrap();
        let items: Vec<ReadValueId> = [
            NodeId::new(2, "first"),
            NodeId::new(1, "local"),
            NodeId::new(2, "second"),
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        let results: Vec<StatusCode> = delegate_service_call(
            &db,
            &aggregation,
            &items,
            false,
            |local| {
                assert_eq!(local.len(), 1);
                assert_eq!(local[0].node_id, NodeId::new(1, "local"));
                vec![StatusCode::Good]
            },
            |_, _, _| panic!("an unavailable source must not be called"),
        )
        .unwrap();
        assert_eq!(
            results,
            vec![
                StatusCode::BadServerNotConnected,
                StatusCode::Good,
                StatusCode::BadServerNotConnected
            ]
        );
    }
}
