// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

// Module declarations
pub mod companion_specs;
pub mod config;
pub mod discovery;
pub mod entry_point_generator;
pub mod http_server;
pub mod nodeset_import;
pub mod python_runtime;
pub mod rule_generator;
pub mod urdf_exporter;

// Re-exports der öffentlichen API
pub use discovery::{
    endpoint_is_reachable, endpoint_liveness_configuration, find_servers, find_servers_on_network,
    get_namespaces, run_discovery_cycle, ServerEvent,
};

pub use config::{
    load_config, load_namespaces, reset_json_files, update_config_with_discovered_servers,
    update_namespaces_config, Host, NamespaceEntry, ServerConfig,
};

pub use companion_specs::{
    get_sorted_namespaces_with_requirements, scan_and_register_existing_nodesets,
    update_companion_specs_list,
};

pub use nodeset_import::{import_nodesets_to_address_space, ImportStatistics};

pub use entry_point_generator::{
    find_all_entry_points, save_entry_points_to_json, EntryPoint, NodeSetEntryPoints,
};

pub use rule_generator::{generate_rules_for_server, AggregationRule};

pub use urdf_exporter::UrdfExporter;

pub use python_runtime::init_python;

pub use http_server::{create_router, sanitize_folder_name};

// Gemeinsame Konstanten
pub const NODESETS_DIR: &str = "./nodesets";
