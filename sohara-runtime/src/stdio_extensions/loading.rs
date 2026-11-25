//! Public extension-loading entry points.

use super::*;

/// Load stdio extension manifests from the given files or directories and
/// register their step factories into the provided registry.
pub fn load_stdio_extensions(
    registry: &mut ComponentRegistry,
    paths: &[PathBuf],
) -> Result<StdioExtensionHost> {
    load_stdio_extensions_with_trusted(registry, &[], paths)
}

/// Load trusted builtin manifests first, then user-provided manifests.
pub fn load_stdio_extensions_with_trusted(
    registry: &mut ComponentRegistry,
    trusted_paths: &[PathBuf],
    paths: &[PathBuf],
) -> Result<StdioExtensionHost> {
    let mut host = StdioExtensionHost::default();
    for manifest_path in manifest_paths(trusted_paths)? {
        let manifest = parse_manifest(&manifest_path)?;
        validate_manifest(&manifest, registry, &host, true)?;
        register_manifest(registry, &mut host, &manifest, &manifest_path)?;
    }
    for manifest_path in manifest_paths(paths)? {
        let manifest = parse_manifest(&manifest_path)?;
        validate_manifest(&manifest, registry, &host, false)?;
        register_manifest(registry, &mut host, &manifest, &manifest_path)?;
    }
    Ok(host)
}
