//! Per-step environment for script contexts (S6 host API)

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value};
use sohara_core::{EventBus, StepMeta};

/// Permissions grantable to a script via the `allow` config (see §10).
pub const PERM_FILE_WRITE: &str = "file.write";
pub const PERM_DB: &str = "db";
pub const PERM_HTTP: &str = "http";
pub const PERM_NOTIFY: &str = "notify";
pub const PERM_ALL: &str = "all";

/// `RefUnwindSafe` wrapper around the event bus: QuickJS callbacks must be
/// `RefUnwindSafe`, and `dyn EventBus` may contain interior mutability.
#[derive(Clone)]
pub struct BusHandle(pub Arc<dyn EventBus>);

impl std::panic::RefUnwindSafe for BusHandle {}

/// Per-step script environment, cloned into every context invocation.
#[derive(Clone)]
pub struct StepEnv {
    /// Step name (for diagnostics).
    pub name: String,
    /// Directory of the script file (module resolution; `None` for inline).
    pub script_dir: Option<PathBuf>,
    /// Flow-level variables.
    pub vars: Map<String, Value>,
    /// Flow name.
    pub flow: String,
    /// Step identity (`ctx.step`).
    pub step: StepMeta,
    /// Event bus (serve mode); `None` in run mode.
    pub bus: Option<BusHandle>,
    /// Granted permissions.
    pub permissions: Vec<String>,
    /// SQLite path for `sohara.db.query`.
    pub db: Option<String>,
    /// Shared `ctx.state` holder (per step, per run).
    pub state: Arc<Mutex<Value>>,
    /// Per-invocation `ctx.emit` buffer.
    pub emit: Arc<Mutex<Vec<Value>>>,
    /// Correlation id for the current invocation.
    pub correlation_id: String,
}

impl StepEnv {
    /// Check a permission; file reads are always allowed.
    pub fn require(&self, permission: &str) -> Result<(), String> {
        if self
            .permissions
            .iter()
            .any(|p| p == permission || p == PERM_ALL)
        {
            Ok(())
        } else {
            Err(format!(
                "permission '{permission}' not granted; add it to the script step's 'allow' config"
            ))
        }
    }

    /// Resolve a module path for `require` (relative to the script dir).
    pub fn resolve_module(&self, path: &str) -> Result<PathBuf, String> {
        let candidate = if Path::new(path).is_absolute() {
            PathBuf::from(path)
        } else if let Some(dir) = &self.script_dir {
            dir.join(path)
        } else {
            PathBuf::from(path)
        };
        if candidate.is_file() {
            return Ok(candidate);
        }
        let with_extension = if candidate.extension().is_none() {
            candidate.with_extension("js")
        } else {
            candidate
        };
        if with_extension.is_file() {
            Ok(with_extension)
        } else {
            Err(format!("require: module '{path}' not found"))
        }
    }
}
