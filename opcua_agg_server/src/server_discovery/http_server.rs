// SPDX-FileCopyrightText: 2026 Adrian Reuther
// SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use std::io::Write;
use std::path::PathBuf;
use zip::write::FileOptions;

pub fn create_router() -> Router {
    Router::new()
        .route("/urdfs/{name}", get(get_urdf_bundle)) // ← {name} statt :name
        .route("/urdfs", get(list_urdfs))
}

async fn list_urdfs() -> impl IntoResponse {
    let urdfs_dir = runtime_root().join("urdfs");

    let entries = match std::fs::read_dir(&urdfs_dir) {
        Ok(e) => e,
        Err(_) => {
            return (StatusCode::NOT_FOUND, "urdfs-Verzeichnis nicht gefunden").into_response()
        }
    };

    let names: Vec<String> = entries
        .flatten()
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    axum::Json(names).into_response()
}

async fn get_urdf_bundle(Path(name): Path<String>) -> impl IntoResponse {
    let bundle_path = runtime_root().join("urdfs").join(&name);

    if !bundle_path.is_dir() {
        return (
            StatusCode::NOT_FOUND,
            format!("Kein URDF-Bundle fuer '{name}' gefunden"),
        )
            .into_response();
    }

    match zip_directory(&bundle_path.to_string_lossy()) {
        // ← kein bundle_name mehr
        Ok(zip_bytes) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/zip"),
                (
                    header::CONTENT_DISPOSITION,
                    &format!("attachment; filename=\"{name}.zip\""),
                ),
            ],
            zip_bytes,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    }
}

fn runtime_root() -> PathBuf {
    std::env::var_os("OJIES_RUNTIME_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")))
}

fn zip_directory(dir_path: &str) -> anyhow::Result<Vec<u8>> {
    let buf = Vec::new();
    let cursor = std::io::Cursor::new(buf);
    let mut zip = zip::ZipWriter::new(cursor);
    let options = FileOptions::<()>::default().compression_method(zip::CompressionMethod::Deflated);

    // Ordnername direkt aus dem Pfad nehmen
    let folder_name = std::path::Path::new(dir_path)
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    add_dir_to_zip(&mut zip, dir_path, dir_path, &folder_name, &options)?;

    let cursor = zip.finish()?;
    Ok(cursor.into_inner())
}
fn add_dir_to_zip(
    zip: &mut zip::ZipWriter<std::io::Cursor<Vec<u8>>>,
    base_path: &str,
    current_path: &str,
    bundle_name: &str,
    options: &FileOptions<()>,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(current_path)?.flatten() {
        let path = entry.path();
        let file_type = entry.file_type()?;

        let relative = path.strip_prefix(base_path)?;
        let zip_path = format!(
            "{}/{}",
            bundle_name,
            relative.to_string_lossy().replace('\\', "/")
        );

        if file_type.is_dir() {
            zip.add_directory(&zip_path, *options)?;
            add_dir_to_zip(zip, base_path, path.to_str().unwrap(), bundle_name, options)?;
        } else if file_type.is_file() {
            zip.start_file(&zip_path, *options)?;
            let data = std::fs::read(&path)?;
            zip.write_all(&data)?;
        }
    }
    Ok(())
}

pub fn sanitize_folder_name(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            // Ungültige Windows-Zeichen ersetzen
            ':' | '/' | '\\' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c => c,
        })
        .collect()
}
