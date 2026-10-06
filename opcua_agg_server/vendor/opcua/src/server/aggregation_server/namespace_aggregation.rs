use std::sync::Arc;

use crate::{
    client::prelude::Session,
    server::prelude::AddressSpace,
    sync::RwLock,
    types::{VariableId, Variant},
};
use tracing::{info, instrument};

use crate::server::aggregation_server::map_db::MapDatabaseConnection;
use crate::server::aggregation_server::{
    error_types::{NamespaceAggregationError, OpcuaResponseError},
    util_traits::AttributeServiceAdditions,
    util_types::LowerServer,
    utils::append_postfix_to_uri,
};

#[instrument(err, ret, skip_all, fields(name = lserver.name))]
pub fn aggregate_namespaces(
    address_space_p: &Arc<RwLock<AddressSpace>>,
    session_p: &Arc<RwLock<Session>>,
    map_db: &MapDatabaseConnection,
    lserver: &mut LowerServer,
) -> Result<(), NamespaceAggregationError> {
    lserver.check_operation()?;
    // Aggregates namespaces by adding a unique postfix to each namespace url.

    let origin_ns_array = {
        let session = session_p.read();
        let val = session.read_val(&VariableId::Server_NamespaceArray.into())?;
        let Variant::Array(arr) = val else {
            let resp_str = format!("{:?}", val);
            return Err(OpcuaResponseError::UnexpectedResponseContent(resp_str).into());
        };
        arr.values
    };

    for (origin_nsid, origin_nsvar) in origin_ns_array.iter().enumerate() {
        lserver.check_operation()?;
        let Variant::String(origin_ns_uastr) = origin_nsvar else {
            continue;
        };
        let Some(origin_ns_uri) = origin_ns_uastr.value() else {
            let resp_str = format!("{:?}", origin_ns_array);
            return Err(OpcuaResponseError::UnexpectedResponseContent(resp_str).into());
        };
        lserver.namespace_array.push(origin_ns_uri.clone());

        let mut address_space = address_space_p.write();
        let namespace_postfixed = append_postfix_to_uri(origin_ns_uri, &lserver.postfix);
        let ns = namespace_postfixed.clone();
        let Ok(dest_nsid) = address_space.register_namespace(&ns) else {
            return Err(NamespaceAggregationError::RegisterNamespaceError(ns));
        };
        drop(address_space);

        info!(
            message = "Aggregated namespace.",
            from = ?origin_ns_uri,
            to = ?namespace_postfixed,
            id = ?dest_nsid,
        );
        map_db.insert_namespace(lserver.id, dest_nsid, origin_nsid as u16)?;
    }

    Ok(())
}
