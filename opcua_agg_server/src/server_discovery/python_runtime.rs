// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

use anyhow::{bail, Context, Result};
use pyo3::prelude::*;
use std::path::{Path, PathBuf};
use tracing::info;

fn find_site_packages(venv_path: &Path) -> Result<PathBuf> {
    // ---------- Windows ----------
    let windows_site = venv_path.join("Lib").join("site-packages");
    if windows_site.is_dir() {
        return Ok(windows_site);
    }

    // ---------- Linux / macOS ----------
    for lib_name in ["lib", "lib64"] {
        let lib_dir = venv_path.join(lib_name);

        if let Ok(entries) = std::fs::read_dir(&lib_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();

                if name_str.starts_with("python")
                    && entry.file_type().map(|t| t.is_dir()).unwrap_or(false)
                {
                    let site_packages = entry.path().join("site-packages");

                    if site_packages.is_dir() {
                        return Ok(site_packages);
                    }
                }
            }
        }
    }

    bail!(
        "Kein site-packages-Verzeichnis in {} gefunden",
        venv_path.display()
    )
}

pub fn init_python(venv_path: &Path, scripts_path: &Path) -> Result<()> {
    if !venv_path.is_dir() {
        bail!("venv-Pfad existiert nicht: {}", venv_path.display());
    }

    if !scripts_path.is_dir() {
        bail!("scripts-Pfad existiert nicht: {}", scripts_path.display());
    }

    let site_packages = find_site_packages(venv_path).with_context(|| {
        format!(
            "site-packages fuer '{}' nicht gefunden",
            venv_path.display()
        )
    })?;

    // PyO3 erwartet Strings (UTF-8). Auf Linux ist das praktisch immer ok.
    let site_packages_str = site_packages
        .to_str()
        .context("site-packages Pfad enthaelt ungueltige UTF-8-Zeichen")?
        .to_owned();

    let scripts_str = scripts_path
        .to_str()
        .context("scripts Pfad enthaelt ungueltige UTF-8-Zeichen")?
        .to_owned();

    Python::attach(|py| -> PyResult<()> {
        let site = py.import("site")?;
        site.call_method1("addsitedir", (&site_packages_str,))?;

        let sys = py.import("sys")?;
        let path = sys.getattr("path")?;
        path.call_method1("insert", (0, &scripts_str))?;

        let len = path.len()?;
        info!(
            "Python-Runtime initialisiert:\n  venv:    {}\n  scripts: {}\n  sys.path hat {} Eintraege",
            site_packages_str, scripts_str, len
        );
        Ok(())
    })
        .map_err(|e| anyhow::anyhow!("Python-Initialisierung fehlgeschlagen: {}", e))
}
