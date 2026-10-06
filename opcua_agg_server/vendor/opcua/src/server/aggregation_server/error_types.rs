use crate::types::{AttributeIdError, NodeId, StatusCode};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MappingError {
    #[error("Aggregation operation stopped: {0}")]
    OperationStopped(#[from] StatusCode),
    #[error("Error with the mapping database connection pooling. {0}")]
    R2D2Error(#[from] r2d2::Error),
    #[error("Error with the mapping database. {0}")]
    SQLiteError(#[from] rusqlite::Error),
    #[error("Invalid or ambiguous instance mapping rule: {0}")]
    InvalidRule(String),
}

#[derive(Error, Debug)]
pub enum RequestError {
    #[error("Request is not a valid JSON string: < {req} >.")]
    RequestNotJSON { req: String },
    #[error("Request is not formatted as a JSON array: < {req} >.")]
    RequestNotAnArray { req: String },
    #[error("Command not recognized: < {command} >; In request: < {req} >.")]
    CommandNotFound { req: String, command: String },
    #[error("Error with add server request. {0}")]
    AddServerError(#[from] AddServerError),
    #[error("Error with read request. {0}")]
    ReadStateError(#[from] ReadStateError),
    #[error("Error with remove request. {0}")]
    RemoveLowerServerError(#[from] RemoveLowerServerError),
}

#[derive(Error, Debug)]
pub enum AddServerError {
    #[error("OPCUA Server is not in Aggregation Server mode.")]
    NotAggregationServer,
    #[error("Lower Server is either already aggregated, being aggregated or being removed.")]
    ServerAlreadyAdded,
    #[error(
        "Name is invalid. It requires a separator ('___') to distinguish the app name. \
    Name: {name}."
    )]
    InvalidName { name: String },
}

#[derive(Error, Debug)]
pub enum ReadStateError {
    #[error("OPCUA Server is not in Aggregation Server mode.")]
    NotAggregationServer,
    #[error("Name not found: < {0} >.")]
    NameNotFound(String),
    #[error("Error serializing read response. {0}")]
    ResponseJSONSerializeError(#[from] serde_json::Error),
}

#[derive(Error, Debug)]
pub enum RemoveLowerServerError {
    #[error("OPCUA Server is not in Aggregation Server mode.")]
    NotAggregationServer,
    #[error("Name not found: < {0} >.")]
    NameNotFound(String),
}

#[derive(Error, Debug)]
pub enum LowerServerError {
    #[error("Aggregation operation stopped: {0}")]
    OperationStopped(#[from] StatusCode),
    #[error("Lower-server worker panicked; partial onboarding was rolled back.")]
    WorkerPanicked,
    #[error("Lower-server session worker panicked while stopping.")]
    SessionWorkerPanicked,
    #[error("Could not add root lserver folder to address space.")]
    AddressSpaceError,
    #[error("Could not create OPCUA client. ClientBuilder was in an invalid state.")]
    CreateClientError,
    #[error("Could not connect to lower server. StatusCode: {status_code}.")]
    ConnectionError {
        #[source]
        status_code: StatusCode,
    },
    #[error("Connection to lower server '{name}' was lost after onboarding.")]
    ConnectionLost { name: String },
    #[error(
        "Although it should exist, the name of the lower server {name} could not \
             be found in the local (lsi/lsr) hashmap."
    )]
    NameNotFoundError { name: String },
    #[error("Error while aggregating OPC-UA types. {0}")]
    TypeAggregationError(#[from] TypeAggregationError),
    #[error("Error while aggregating OPC-UA namespaces. {0}")]
    NamespaceAggregationError(#[from] NamespaceAggregationError),
    #[error(transparent)]
    MappingError(#[from] MappingError),
}

#[derive(Error, Debug)]
pub enum TypeAggregationError {
    #[error("Aggregation operation stopped: {0}")]
    OperationStopped(#[from] StatusCode),
    #[error("Cyclic or excessively deep type graph at {0}")]
    InvalidTypeGraph(NodeId),
    #[error("Error with an opcua service response from lower server: {0}")]
    OpcuaResponseError(#[from] OpcuaResponseError),
    #[error("Type hash not found: {0}.")]
    NotInHashmap(u64),
    #[error("Node {nodeid} in lower server {lserver_name} was not yet hashed.")]
    NoHash {
        lserver_name: String,
        nodeid: NodeId,
    },
    #[error("Cannot find supertype of type {nodeid}.")]
    NoSupertype { nodeid: NodeId },
    #[error("Really should not happen.")]
    ProgrammingError,
    #[error(transparent)]
    NamespaceAggregationError(#[from] NamespaceAggregationError),
    #[error(transparent)]
    NodeCopyError(#[from] NodeCopyError),
    #[error(transparent)]
    MappingError(#[from] MappingError),
}

#[derive(Error, Debug)]
pub enum NamespaceAggregationError {
    #[error("Aggregation operation stopped: {0}")]
    OperationStopped(#[from] StatusCode),
    #[error("Error with an opcua service response from lower server: {0}")]
    OpcuaResponseError(#[from] OpcuaResponseError),
    #[error("Error registering namespace: {0}.")]
    RegisterNamespaceError(String),
    #[error("Namespace {nsid} of lower server {lserver_name} has not yet been aggregated.")]
    NamespaceNotAggregated { lserver_name: String, nsid: u16 },
    #[error(transparent)]
    MappingError(#[from] MappingError),
}

#[derive(Error, Debug)]
pub enum NodeCopyError {
    #[error("Error with an opcua service response from lower server: {0}")]
    OpcuaResponseError(#[from] OpcuaResponseError),
    #[error("Namespace Zero nodes cannot be copied.")]
    NSZeroCopy,
    #[error("Invalid AttributeId. {0}")]
    InvalidAttributeId(#[from] AttributeIdError),
    #[error("Did not copy node.")]
    NotCopied,
}

#[derive(Error, Debug)]
pub enum OpcuaResponseError {
    #[error("The OPC-UA request to the lower server resulted in a bad StatusCode: {0}")]
    BadStatusCode(#[from] StatusCode),
    #[error(
        "The aggregation server received a request response from a lower server \
             whose structure it did not expect: < {0} >."
    )]
    UnexpectedResponseContent(String),
}

#[derive(Error, Debug)]
#[error("The integer is not a valid NodeClass identifier: {0}")]
pub struct InvalidNodeClass(pub i32);

#[derive(Error, Debug)]
pub enum ServiceDelegationError {
    #[error(transparent)]
    MappingError(#[from] MappingError),
}
