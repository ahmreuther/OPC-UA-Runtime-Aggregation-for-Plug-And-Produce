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
use std::path::{Component, Path as FsPath, PathBuf};
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
    let bundle_path = match resolve_bundle_path(&runtime_root().join("urdfs"), &name) {
        Ok(path) => path,
        Err(status) => return (status, "URDF-Bundle nicht verfuegbar").into_response(),
    };

    // ZIP compression and filesystem reads must not block an async HTTP worker.
    let bundle_name = name.clone();
    match tokio::task::spawn_blocking(move || zip_directory(&bundle_path, &bundle_name)).await {
        Ok(Ok(zip_bytes)) => (
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
        Ok(Err(error)) => {
            tracing::warn!("URDF bundle could not be archived: {}", error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "URDF-Bundle konnte nicht erstellt werden",
            )
                .into_response()
        }
        Err(error) => {
            tracing::warn!("URDF archive worker failed: {}", error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "URDF-Bundle konnte nicht erstellt werden",
            )
                .into_response()
        }
    }
}

pub(crate) fn runtime_root() -> PathBuf {
    std::env::var_os("OJIES_RUNTIME_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")))
}

fn valid_bundle_name(name: &str) -> bool {
    // Axum has already percent-decoded this route parameter. Reject separators
    // from both platforms and header control characters before joining paths.
    !name.is_empty()
        && !name
            .chars()
            .any(|c| c.is_control() || matches!(c, '/' | '\\' | ':' | '"'))
        && {
            let mut components = FsPath::new(name).components();
            matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
        }
}

fn resolve_bundle_path(root: &FsPath, name: &str) -> Result<PathBuf, StatusCode> {
    if !valid_bundle_name(name) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let root = root.canonicalize().map_err(|_| StatusCode::NOT_FOUND)?;
    let bundle = root
        .join(name)
        .canonicalize()
        .map_err(|_| StatusCode::NOT_FOUND)?;
    // Canonicalization also catches a bundle symlink/junction pointing outside
    // the export directory, including one pointing back to the directory itself.
    if bundle == root || !bundle.starts_with(&root) {
        return Err(StatusCode::BAD_REQUEST);
    }
    if !bundle.is_dir() {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(bundle)
}

fn zip_directory(dir_path: &FsPath, bundle_name: &str) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(valid_bundle_name(bundle_name), "invalid archive root name");
    // The handler supplies the already checked canonical path. Do not adopt
    // a different root if a symlink/junction replaces it before the worker runs.
    anyhow::ensure!(
        dir_path.canonicalize()? == dir_path,
        "bundle target changed before archiving"
    );
    let cursor = std::io::Cursor::new(Vec::new());
    let mut zip = zip::ZipWriter::new(cursor);
    let options = FileOptions::<()>::default().compression_method(zip::CompressionMethod::Deflated);
    add_dir_to_zip(&mut zip, &dir_path, &dir_path, bundle_name, &options)?;
    Ok(zip.finish()?.into_inner())
}

fn add_dir_to_zip(
    zip: &mut zip::ZipWriter<std::io::Cursor<Vec<u8>>>,
    base_path: &FsPath,
    current_path: &FsPath,
    bundle_name: &str,
    options: &FileOptions<()>,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(current_path)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        // Do not dereference file or directory links while building downloads.
        if file_type.is_symlink() {
            continue;
        }
        let canonical = path.canonicalize()?;
        // The second check also excludes junctions and aliases inside a bundle,
        // preventing recursive cycles on platforms with other reparse types.
        anyhow::ensure!(
            canonical.starts_with(base_path),
            "archive entry escaped bundle"
        );
        if canonical != path {
            continue;
        }
        let relative = path.strip_prefix(base_path)?;
        anyhow::ensure!(
            relative.components().all(|component| {
                matches!(component, Component::Normal(name) if !name.to_string_lossy().contains('\\'))
            }),
            "invalid archive entry name"
        );
        let zip_path = format!(
            "{}/{}",
            bundle_name,
            relative.to_string_lossy().replace('\\', "/")
        );
        if file_type.is_dir() {
            zip.add_directory(&zip_path, *options)?;
            add_dir_to_zip(zip, base_path, &path, bundle_name, options)?;
        } else if file_type.is_file() {
            zip.start_file(&zip_path, *options)?;
            zip.write_all(&std::fs::read(&canonical)?)?;
        }
    }
    Ok(())
}

pub fn sanitize_folder_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| match c {
            ':' | '/' | '\\' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    // Windows strips trailing spaces/dots. A source name must never resolve
    // to the export directory or its parent before replacing an old bundle.
    let sanitized = sanitized.trim_end_matches([' ', '.']);
    if sanitized.is_empty() {
        "_".to_string()
    } else {
        sanitized.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);
    struct TestDirectory(PathBuf);
    impl TestDirectory {
        fn new() -> Self {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "ojies-http-{}-{now}-{}",
                std::process::id(),
                NEXT_DIR.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn bundle_download_rejects_decoded_traversal_and_header_controls() {
        for name in [
            "", ".", "..", "../pki", "a/b", "a\\b", "C:\\pki", "/etc", "a\r\nb", "a\"b",
        ] {
            assert!(!valid_bundle_name(name), "accepted {name:?}");
            assert_eq!(
                resolve_bundle_path(FsPath::new("unused"), name),
                Err(StatusCode::BAD_REQUEST)
            );
        }
        assert!(valid_bundle_name("urn_ojies_robot-1"));
    }

    #[test]
    fn bundle_archive_contains_only_selected_export_tree() {
        let test = TestDirectory::new();
        let root = test.0.join("urdfs");
        let bundle = root.join("robot");
        std::fs::create_dir_all(bundle.join("meshes")).unwrap();
        std::fs::write(bundle.join("robot.urdf"), "robot content").unwrap();
        std::fs::write(bundle.join("meshes/part.stl"), "mesh content").unwrap();
        std::fs::write(test.0.join("secret.pem"), "private content").unwrap();
        let resolved = resolve_bundle_path(&root, "robot").unwrap();
        let data = zip_directory(&resolved, "robot").unwrap();
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(data)).unwrap();
        let mut content = String::new();
        archive
            .by_name("robot/meshes/part.stl")
            .unwrap()
            .read_to_string(&mut content)
            .unwrap();
        assert_eq!(content, "mesh content");
        assert!(archive.by_name("secret.pem").is_err());
        assert_eq!(archive.len(), 3);
        assert_eq!(
            resolve_bundle_path(&root, "missing"),
            Err(StatusCode::NOT_FOUND)
        );
    }

    #[cfg(unix)]
    #[test]
    fn bundle_links_cannot_expose_external_directories_or_files() {
        use std::os::unix::fs::symlink;
        let test = TestDirectory::new();
        let root = test.0.join("urdfs");
        let bundle = root.join("robot");
        let outside = test.0.join("private");
        std::fs::create_dir_all(&bundle).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("key.pem"), "private content").unwrap();
        std::fs::write(bundle.join("robot.urdf"), "robot content").unwrap();
        symlink(&outside, root.join("escape")).unwrap();
        assert_eq!(
            resolve_bundle_path(&root, "escape"),
            Err(StatusCode::BAD_REQUEST)
        );
        symlink(outside.join("key.pem"), bundle.join("key.pem")).unwrap();
        symlink(&outside, bundle.join("private")).unwrap();
        symlink(&bundle, bundle.join("cycle")).unwrap();
        let data = zip_directory(&bundle, "robot").unwrap();
        let archive = zip::ZipArchive::new(std::io::Cursor::new(data)).unwrap();
        assert_eq!(archive.len(), 1);
        assert_eq!(
            archive.file_names().collect::<Vec<_>>(),
            ["robot/robot.urdf"]
        );
    }

    #[test]
    fn sanitized_source_names_never_select_export_root_or_parent() {
        for name in ["", ".", "..", "...", "  ", ".. "] {
            let sanitized = sanitize_folder_name(name);
            assert_eq!(sanitized, "_");
            assert!(valid_bundle_name(&sanitized));
        }
        assert_eq!(sanitize_folder_name("urn:robot/a\\b"), "urn_robot_a_b");
    }
}
