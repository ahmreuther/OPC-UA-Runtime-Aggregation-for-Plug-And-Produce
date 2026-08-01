use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::thread::JoinHandle;

use crate::types::{
    DiagnosticInfo, ExtensionObjectEncoding, LocalizedText, NodeClass, NodeId, QualifiedName,
    Variant,
};
use serde::Serialize;
use tokio::sync::oneshot;
use tracing::{instrument, warn};

use crate::server::aggregation_server::error_types::{LowerServerError, TypeAggregationError};

#[derive(Debug)]
pub struct LowerServerThreading {
    pub thread_handle: JoinHandle<Result<(), LowerServerError>>,
    pub remove_lserver_tx: oneshot::Sender<()>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LowerServerInfo {
    pub aggregation_finished: bool,
    pub removal_in_progress: bool,
    /// Terminal error reported by the lower-server background thread. An
    /// explicit remove keeps this field empty and must not be interpreted as
    /// successful aggregation.
    pub aggregation_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StandardizedNamespace {
    pub url: String,
    pub nsid: Option<u16>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum RuleBrowsePathElement {
    Qualified {
        namespace_uri: String,
        name: String,
        #[serde(default)]
        identifier: Option<String>,
    },
    Legacy(String),
}

impl RuleBrowsePathElement {
    pub fn name(&self) -> &str {
        match self {
            Self::Qualified { name, .. } => name,
            Self::Legacy(name) => name,
        }
    }

    pub fn namespace_uri(&self) -> Option<&str> {
        match self {
            Self::Qualified { namespace_uri, .. } => Some(namespace_uri),
            Self::Legacy(_) => None,
        }
    }

    pub fn identifier(&self) -> Option<&str> {
        match self {
            Self::Qualified { identifier, .. } => identifier.as_deref(),
            Self::Legacy(_) => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RuleNodeIdentity {
    pub namespace_uri: String,
    /// Namespace-independent OPC UA identifier, for example `i=47` or `s=Robot1`.
    pub identifier: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RuleMergePolicy {
    /// Backwards-compatible policy for old rule files. Every local-name segment
    /// must resolve to exactly one child; ambiguity is an error.
    #[default]
    LegacyUniquePath,
    /// Source nodes keep their lower-server identity and are attached to the
    /// one target selected by the complete namespace-qualified merge key.
    MergeAtQualifiedTarget,
}

#[derive(Debug, Deserialize)]
/// Mapping rule format. Qualified rules are bound to one source endpoint and
/// carry stable source/reference identities. Legacy string-only paths remain
/// readable, but their resolver rejects ambiguous sibling names.
pub struct InstanceMappingRule {
    pub target_node: Vec<RuleBrowsePathElement>,
    pub source_node: Vec<RuleBrowsePathElement>,
    #[serde(default)]
    pub ref_type: Vec<RuleBrowsePathElement>,
    pub is_forward: bool,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub source_node_id: Option<RuleNodeIdentity>,
    #[serde(default)]
    pub reference_type: Option<RuleNodeIdentity>,
    #[serde(default)]
    pub merge_policy: RuleMergePolicy,
    #[serde(default)]
    pub merge_key: Vec<RuleBrowsePathElement>,
}

#[derive(Debug)]
pub struct IncompleteMapping {
    pub rule_id: usize,
    pub source_nid: NodeId,
    pub source_bname: QualifiedName,
    pub source_dname: LocalizedText,
    pub source_class: NodeClass,
    pub source_type: NodeId,
}

#[derive(Clone, Debug)]
pub struct LowerServer {
    pub name: String,
    pub id: u16,
    pub url: String,
    pub postfix: String,
    pub namespace_array: Vec<String>,
    pub folder_nodeid: NodeId,
}

impl LowerServer {
    #[instrument(level = "info", ret)]
    pub fn new(
        name: &str,
        lserver_id: u16,
        url: &str,
        postfix: &str,
        folder_nodeid: &NodeId,
    ) -> Self {
        return Self {
            name: name.into(),
            id: lserver_id,
            url: url.into(),
            postfix: postfix.into(),
            namespace_array: Vec::new(),
            folder_nodeid: folder_nodeid.clone(),
        };
    }
}

// Second field is recursion count
#[derive(Debug)]
pub struct VariantWrapper(pub Variant, pub u16);

impl Hash for VariantWrapper {
    #[instrument(level = "trace", skip(state))]
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.1 > 10 {
            // recursion stop
            warn!(msg = "Recusion stop for Variant hashing. Hash will not be correct!", var = ?self);
            return;
        }
        match &self.0 {
            Variant::Empty => "".hash(state),
            Variant::Boolean(v) => v.hash(state),
            Variant::ExpandedNodeId(v) => {
                v.node_id.hash(state);
                v.namespace_uri.hash(state);
                v.server_index.hash(state);
            }
            Variant::SByte(v) => v.hash(state),
            Variant::Int16(v) => v.hash(state),
            Variant::UInt16(v) => v.hash(state),
            Variant::Int32(v) => v.hash(state),
            Variant::UInt32(v) => v.hash(state),
            Variant::Int64(v) => v.hash(state),
            Variant::UInt64(v) => v.hash(state),
            Variant::Float(v) => v.to_bits().hash(state),
            Variant::Double(v) => v.to_bits().hash(state),
            Variant::String(v) => v.hash(state),
            Variant::DateTime(v) => v.ticks().hash(state),
            Variant::Guid(v) => v.hash(state),
            Variant::StatusCode(v) => v.hash(state),
            Variant::ByteString(v) => v.hash(state),
            Variant::XmlElement(v) => v.hash(state),
            Variant::QualifiedName(v) => {
                v.namespace_index.hash(state);
                v.name.hash(state);
            }
            Variant::LocalizedText(v) => v.to_string().hash(state),
            Variant::ExtensionObject(v) => {
                v.node_id.hash(state);
                match &v.body {
                    ExtensionObjectEncoding::None => "".hash(state),
                    ExtensionObjectEncoding::ByteString(v) => v.hash(state),
                    ExtensionObjectEncoding::XmlElement(v) => v.hash(state),
                }
            }
            Variant::DataValue(v) => {
                VariantWrapper(v.value.clone().unwrap_or_default(), self.1 + 1).hash(state);
                v.status.hash(state);
                v.source_timestamp.unwrap_or_default().ticks().hash(state);
                v.source_picoseconds.hash(state);
                v.server_timestamp.unwrap_or_default().ticks().hash(state);
                v.server_picoseconds.hash(state);
            }
            Variant::DiagnosticInfo(v) => {
                v.symbolic_id.hash(state);
                v.namespace_uri.hash(state);
                v.locale.hash(state);
                v.localized_text.hash(state);
                v.additional_info.hash(state);
                v.inner_status_code.hash(state);
                DiagnosticsInfoWrapper(*v.inner_diagnostic_info.clone().unwrap_or_default(), 0)
                    .hash(state);
            }
            Variant::Array(a) => {
                for val in a.values.clone() {
                    VariantWrapper(val, self.1 + 1).hash(state);
                }
                a.dimensions.hash(state);
            }
            Variant::NodeId(v) => v.hash(state),
            Variant::Byte(v) => v.hash(state),
            Variant::Variant(v) => {
                VariantWrapper((**v).clone(), self.1 + 1).hash(state);
            }
        }
    }
}

// Second field is recursion count
#[derive(Debug)]
struct DiagnosticsInfoWrapper(DiagnosticInfo, u16);

impl Hash for DiagnosticsInfoWrapper {
    #[instrument(level = "trace", skip(state))]
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.1 > 10 {
            // Recursion stop
            warn!(msg = "Recusion stop for DiagnosticsInfo hashing. Hash will not be correct!", var = ?self);
            return;
        }
        self.0.symbolic_id.hash(state);
        self.0.namespace_uri.hash(state);
        self.0.locale.hash(state);
        self.0.localized_text.hash(state);
        self.0.additional_info.hash(state);
        self.0.inner_status_code.hash(state);
        DiagnosticsInfoWrapper(
            *self.0.inner_diagnostic_info.clone().unwrap_or_default(),
            self.1 + 1,
        )
        .hash(state);
    }
}
