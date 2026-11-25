//! Helpers shared by the stdio extension integration tests.

use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

pub fn write_extension(tag: &str, name: &str, registrations_yaml: &str) -> PathBuf {
    let dir = temp_dir(tag);
    let script = dir.join("plugin.py");
    let manifest = dir.join("extension.yaml");
    std::fs::write(&script, plugin_script()).unwrap();
    std::fs::write(
        &manifest,
        format!(
            r#"
name: {name}
version: "0.1.0"
command: python3
args:
  - "{}"
{registrations_yaml}
"#,
            script.display()
        ),
    )
    .unwrap();
    dir
}

pub fn assert_python3() {
    let status = Command::new("python3")
        .arg("--version")
        .status()
        .unwrap_or_else(|error| panic!("python3 is required for stdio extension tests: {error}"));
    assert!(status.success(), "python3 --version failed");
}

pub fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("sohara-runtime-{tag}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

pub fn builtin_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../extensions/builtin")
}

pub fn ensure_builtin_binaries() {
    static BUILTINS: OnceLock<()> = OnceLock::new();
    BUILTINS.get_or_init(|| {
        let status = Command::new("cargo")
            .args(["build", "-q", "-p", "sohara-builtin-extensions", "--bins"])
            .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".."))
            .status()
            .unwrap();
        assert!(
            status.success(),
            "failed to build builtin extension binaries"
        );
    });
}

pub fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub fn plugin_script() -> &'static str {
    include_str!("plugin.py")
}
