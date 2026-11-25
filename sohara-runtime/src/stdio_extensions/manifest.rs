//! Manifest path discovery, parsing, and command resolution.

use super::*;

pub(crate) fn manifest_paths(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut manifests = Vec::new();
    for path in paths {
        if path.is_dir() {
            manifests.extend(manifest_paths_in_dir(path)?);
        } else if path.is_file() {
            if supported_manifest(path) {
                manifests.push(path.clone());
            } else {
                return Err(Error::Config(format!(
                    "unsupported extension manifest '{}'; use .json/.yaml/.yml",
                    path.display()
                )));
            }
        } else {
            return Err(Error::Config(format!(
                "extension path '{}' does not exist",
                path.display()
            )));
        }
    }
    manifests.sort();
    Ok(manifests)
}

fn manifest_paths_in_dir(path: &Path) -> Result<Vec<PathBuf>> {
    let mut entries = read_manifest_dir(path)?;
    entries.sort();
    Ok(entries
        .into_iter()
        .filter(|entry| entry.is_file() && supported_manifest(entry))
        .collect())
}

fn read_manifest_dir(path: &Path) -> Result<Vec<PathBuf>> {
    std::fs::read_dir(path)
        .map_err(|error| {
            Error::Config(format!("read extension dir '{}': {error}", path.display()))
        })?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Error::Config(format!("read extension dir '{}': {error}", path.display())))
}

pub(crate) fn supported_manifest(path: &Path) -> bool {
    matches!(
        path.extension().and_then(OsStr::to_str),
        Some("json" | "yaml" | "yml")
    )
}

pub(crate) fn parse_manifest(path: &Path) -> Result<StdioExtensionManifest> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        Error::Config(format!(
            "read extension manifest '{}': {error}",
            path.display()
        ))
    })?;
    match path.extension().and_then(OsStr::to_str) {
        Some("json") => serde_json::from_str(&text).map_err(|error| {
            Error::Config(format!(
                "parse extension manifest '{}': {error}",
                path.display()
            ))
        }),
        Some("yaml" | "yml") => serde_yaml::from_str(&text).map_err(|error| {
            Error::Config(format!(
                "parse extension manifest '{}': {error}",
                path.display()
            ))
        }),
        _ => Err(Error::Config(format!(
            "unsupported extension manifest '{}'; use .json/.yaml/.yml",
            path.display()
        ))),
    }
}

pub(crate) fn resolve_command(manifest_path: &Path, command: &str) -> String {
    let command_path = Path::new(command);
    if command_path.is_absolute() || command_path.components().count() == 1 {
        return command.to_owned();
    }
    manifest_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(command_path)
        .to_string_lossy()
        .into_owned()
}

pub(crate) fn default_protocol() -> String {
    PROTOCOL.to_owned()
}
