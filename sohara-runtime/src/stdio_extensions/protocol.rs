//! JSON-RPC validation and error construction helpers.

use super::*;

pub(crate) fn validate_jsonrpc<S>(
    spec: &S,
    method: &str,
    id: u64,
    response: &JsonRpcResponse<impl Sized>,
) -> Result<()>
where
    S: SpecError,
{
    if response.jsonrpc != "2.0" {
        return Err(spec_error(
            spec,
            format!("invalid {method} JSON-RPC version: {}", response.jsonrpc),
        ));
    }
    if response.id != id {
        return Err(spec_error(
            spec,
            format!(
                "mismatched {method} response id: expected {id}, got {}",
                response.id
            ),
        ));
    }
    Ok(())
}

pub(crate) fn take_async_result<T>(
    spec: &AsyncSpec,
    response: JsonRpcResponse<T>,
    method: &str,
    name: &str,
) -> Result<T> {
    if let Some(error) = response.error {
        return Err(async_error(
            spec,
            format!(
                "extension '{name}' {method} failed: {} ({})",
                error.message, error.code
            ),
        ));
    }
    response.result.ok_or_else(|| {
        async_error(
            spec,
            format!("extension '{name}' returned no result for {method}"),
        )
    })
}

pub(crate) fn take_blocking_result<T>(
    spec: &BlockingSpec,
    response: JsonRpcResponse<T>,
    method: &str,
    name: &str,
) -> Result<T> {
    if let Some(error) = response.error {
        return Err(blocking_error(
            spec,
            format!(
                "extension '{name}' {method} failed: {} ({})",
                error.message, error.code
            ),
        ));
    }
    response.result.ok_or_else(|| {
        blocking_error(
            spec,
            format!("extension '{name}' returned no result for {method}"),
        )
    })
}

pub(crate) trait SpecError {
    fn to_error(&self, message: String) -> Error;
}

impl SpecError for AsyncSpec {
    fn to_error(&self, message: String) -> Error {
        async_error(self, message)
    }
}

impl SpecError for BlockingSpec {
    fn to_error(&self, message: String) -> Error {
        blocking_error(self, message)
    }
}

pub(crate) fn spec_error(spec: &impl SpecError, message: String) -> Error {
    spec.to_error(message)
}

pub(crate) fn async_error(spec: &AsyncSpec, message: String) -> Error {
    match spec.kind {
        AsyncKind::Source | AsyncKind::Trigger => Error::Source(message),
        AsyncKind::Transform => Error::Transform(message),
        AsyncKind::Sink => Error::Sink(message),
    }
}

pub(crate) fn blocking_error(_spec: &BlockingSpec, message: String) -> Error {
    Error::Runtime(message)
}

pub(crate) fn spawn_async_error(
    spec: &AsyncSpec,
    command: &Command,
    error: std::io::Error,
) -> String {
    format!(
        "failed to spawn extension '{}' ({:?}): {error}",
        spec.extension_name,
        command.as_std().get_args().collect::<Vec<&OsStr>>()
    )
}

pub(crate) fn spawn_blocking_error(
    spec: &BlockingSpec,
    command: &StdCommand,
    error: std::io::Error,
) -> String {
    format!(
        "failed to spawn extension '{}' ({:?}): {error}",
        spec.extension_name,
        command.get_args().collect::<Vec<&OsStr>>()
    )
}

pub(crate) fn write_error(method: &str, error: std::io::Error) -> String {
    format!("failed to write {method} request to extension: {error}")
}

pub(crate) fn flush_error(method: &str, error: std::io::Error) -> String {
    format!("failed to flush {method} request to extension: {error}")
}

pub(crate) fn read_error(method: &str, error: std::io::Error) -> String {
    format!("failed to read {method} response from extension: {error}")
}

pub(crate) fn async_kind_name(kind: AsyncKind) -> &'static str {
    match kind {
        AsyncKind::Source => "source",
        AsyncKind::Transform => "transform",
        AsyncKind::Sink => "sink",
        AsyncKind::Trigger => "trigger",
    }
}

pub(crate) fn blocking_kind_name(kind: BlockingKind) -> &'static str {
    match kind {
        BlockingKind::StateStore => "state_store",
        BlockingKind::EventBus => "event_bus",
    }
}
