// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

use anyhow::{Context, Result};
use pyo3::prelude::*;
use tracing::info;

use super::http_server::{runtime_root, sanitize_folder_name};

fn export_paths(root: &std::path::Path, name: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let name = sanitize_folder_name(name);
    (
        root.join(format!("urdf_tmp_{name}")),
        root.join("urdfs").join(name),
    )
}

pub struct UrdfExporter {
    callable: Py<PyAny>,
}

impl UrdfExporter {
    pub fn new() -> Result<Self> {
        let callable = Python::attach(|py| -> PyResult<Py<PyAny>> {
            let module = py.import("OPCUA_2_URDF")?;
            let func = module.getattr("export_urdf")?;
            info!("URDF-Exporter: Modul geladen und Funktion gecacht");
            Ok(func.into())
        })
        .map_err(|e| anyhow::anyhow!("Modul 'OPCUA_2_URDF' konnte nicht geladen werden: {}", e))?;

        Ok(Self { callable })
    }

    pub fn export(&self, endpoint: &str, output_path: &str) -> Result<()> {
        info!("URDF-Export startet: endpoint={endpoint}, output={output_path}");

        std::fs::create_dir_all(output_path)
            .with_context(|| format!("Konnte Verzeichnis nicht erstellen: {output_path}"))?;

        info!("Rufe Python-Funktion auf...");

        let result = std::thread::spawn({
            let endpoint = endpoint.to_string();
            let output_path = output_path.to_string();
            let callable = Python::attach(|py| self.callable.clone_ref(py));
            move || {
                Python::attach(|py| -> PyResult<()> {
                    callable.call1(py, (endpoint.as_str(), output_path.as_str()))?;
                    Ok(())
                })
            }
        })
        .join()
        .map_err(|_| anyhow::anyhow!("Python-Thread ist gepanicked"))?;

        info!("Python-Funktion zurückgekehrt");

        result.map_err(|e| {
            let traceback = Python::attach(|py| {
                e.traceback(py)
                    .and_then(|tb| tb.format().ok())
                    .unwrap_or_else(|| "<kein Traceback>".to_string())
            });
            anyhow::anyhow!(
                "Python-Exception:\nNachricht: {}\nTraceback:\n{}",
                e,
                traceback
            )
        })?;

        info!("URDF-Export erfolgreich: {output_path}");
        Ok(())
    }

    pub fn export_and_move(&self, address: &str, name: &str) -> anyhow::Result<()> {
        let (urdf_tmp, app_uri_dir) = export_paths(&runtime_root(), name);
        let temp_path = urdf_tmp.join("_export");
        std::fs::create_dir_all(&temp_path)?; // Erstellt urdf_tmp_{name}/_export

        if let Err(e) = self.export(address, &temp_path.to_string_lossy()) {
            std::fs::remove_dir_all(&urdf_tmp).ok(); // Cleanup bei Export-Fehler
            return Err(e);
        }

        if app_uri_dir.exists() {
            std::fs::remove_dir_all(&app_uri_dir)?;
        }
        std::fs::create_dir_all(&app_uri_dir)?;

        let entries: Vec<_> = std::fs::read_dir(&urdf_tmp)?
            .flatten()
            .filter(|e| e.file_name() != "_export")
            .collect();

        if entries.is_empty() {
            std::fs::remove_dir_all(&urdf_tmp).ok();
            return Err(anyhow::anyhow!(
                "Script hat nichts erstellt in: {:?}",
                urdf_tmp
            ));
        }

        for entry in entries {
            let target_path = app_uri_dir.join(entry.file_name());
            std::fs::rename(entry.path(), &target_path).map_err(|e| {
                anyhow::anyhow!("Fehler beim Verschieben von {:?}: {}", entry.path(), e)
            })?;
            info!("Verschoben: {:?} -> {:?}", entry.path(), target_path);
        }

        std::fs::remove_dir_all(&urdf_tmp).ok(); // Ganzen tmp-Ordner löschen nach erfolgreichem Verschieben

        info!("URDF-Export abgeschlossen nach: {:?}", app_uri_dir);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::export_paths;
    use std::path::Path;

    #[test]
    fn urdf_export_uses_selected_runtime_root_for_temp_and_http_bundle() {
        let root = Path::new("configured-runtime");
        let (temporary, bundle) = export_paths(root, "urn:robot:1");
        assert_eq!(temporary, root.join("urdf_tmp_urn_robot_1"));
        assert_eq!(bundle, root.join("urdfs").join("urn_robot_1"));
    }

    #[test]
    fn source_name_cannot_choose_runtime_parent_for_bundle_replacement() {
        let root = Path::new("configured-runtime");
        for name in [".", "..", "...", "../private", "a/b", "a\\b"] {
            let (temporary, bundle) = export_paths(root, name);
            assert_eq!(temporary.parent(), Some(root));
            assert_eq!(bundle.parent(), Some(root.join("urdfs").as_path()));
            assert_ne!(bundle, root.join("urdfs"));
        }
    }
}
