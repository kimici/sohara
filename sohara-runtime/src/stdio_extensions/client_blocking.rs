//! Blocking stdio client: spawn, initialize, and blocking JSON-RPC calls.

use super::*;

impl Drop for BlockingStdioClient {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

impl BlockingStdioClient {
    pub(crate) fn spawn(spec: &BlockingSpec) -> Result<Self> {
        let (child, stdin, stdout) = spawn_command(spec)?;
        let mut client = Self {
            child,
            stdin,
            stdout: StdBufReader::new(stdout),
            next_id: 1,
            capabilities: ExtensionCapabilities::default(),
        };
        client.initialize(spec)?;
        Ok(client)
    }

    fn initialize(&mut self, spec: &BlockingSpec) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "initialize",
            params: InitializeParams {
                protocol: PROTOCOL,
                extension: HostExtensionInfo {
                    name: &spec.extension_name,
                    version: &spec.extension_version,
                },
            },
        };
        let response: JsonRpcResponse<InitializeResult> = self.request(spec, request)?;
        let result = take_blocking_result(spec, response, "initialize", &spec.extension_name)?;
        if result.protocol != spec.protocol {
            return Err(blocking_error(
                spec,
                format!(
                    "extension '{}' protocol mismatch: host={} extension={}",
                    spec.extension_name, spec.protocol, result.protocol
                ),
            ));
        }
        self.capabilities = result.capabilities;
        self.ensure_capability(spec)
    }

    pub(crate) fn state_load(
        &mut self,
        spec: &BlockingSpec,
        config: &Value,
        key: &str,
    ) -> Result<StateLoadResult> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "state.load",
            params: StoreKeyParams {
                store: TypeName {
                    type_name: &spec.type_name,
                },
                config,
                key,
            },
        };
        let response: JsonRpcResponse<StateLoadResult> = self.request(spec, request)?;
        take_blocking_result(spec, response, "state.load", &spec.type_name)
    }

    pub(crate) fn state_save(
        &mut self,
        spec: &BlockingSpec,
        config: &Value,
        key: &str,
        value: &Value,
    ) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "state.save",
            params: StoreSaveParams {
                store: TypeName {
                    type_name: &spec.type_name,
                },
                config,
                key,
                value,
            },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request)?;
        let _ = take_blocking_result(spec, response, "state.save", &spec.type_name)?;
        Ok(())
    }

    pub(crate) fn state_delete(
        &mut self,
        spec: &BlockingSpec,
        config: &Value,
        key: &str,
    ) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "state.delete",
            params: StoreKeyParams {
                store: TypeName {
                    type_name: &spec.type_name,
                },
                config,
                key,
            },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request)?;
        let _ = take_blocking_result(spec, response, "state.delete", &spec.type_name)?;
        Ok(())
    }

    pub(crate) fn bus_publish(
        &mut self,
        spec: &BlockingSpec,
        config: &Value,
        topic: &str,
        payload: &Value,
    ) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "bus.publish",
            params: EventBusPublishParams {
                bus: TypeName {
                    type_name: &spec.type_name,
                },
                config,
                topic,
                payload,
            },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request)?;
        let _ = take_blocking_result(spec, response, "bus.publish", &spec.type_name)?;
        Ok(())
    }

    fn request<T, R>(
        &mut self,
        spec: &BlockingSpec,
        request: JsonRpcRequest<'_, T>,
    ) -> Result<JsonRpcResponse<R>>
    where
        T: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let mut line = serde_json::to_vec(&request)?;
        line.push(b'\n');
        self.stdin
            .write_all(&line)
            .map_err(|error| blocking_error(spec, write_error(request.method, error)))?;
        self.stdin
            .flush()
            .map_err(|error| blocking_error(spec, flush_error(request.method, error)))?;

        let mut response = String::new();
        let read = self
            .stdout
            .read_line(&mut response)
            .map_err(|error| blocking_error(spec, read_error(request.method, error)))?;
        if read == 0 {
            return Err(blocking_error(
                spec,
                format!("extension closed stdout during {}", request.method),
            ));
        }
        let parsed: JsonRpcResponse<R> = serde_json::from_str(&response).map_err(|error| {
            blocking_error(
                spec,
                format!(
                    "invalid {} JSON-RPC response: {error}; raw={}",
                    request.method,
                    response.trim()
                ),
            )
        })?;
        validate_jsonrpc(spec, request.method, request.id, &parsed)?;
        Ok(parsed)
    }

    fn ensure_capability(&self, spec: &BlockingSpec) -> Result<()> {
        if self.capabilities.supports_blocking(spec.kind) {
            return Ok(());
        }
        Err(blocking_error(
            spec,
            format!(
                "extension '{}' does not advertise {} capability",
                spec.extension_name,
                blocking_kind_name(spec.kind)
            ),
        ))
    }

    fn take_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

fn spawn_command(spec: &BlockingSpec) -> Result<(StdChild, StdChildStdin, StdChildStdout)> {
    let mut command = StdCommand::new(&spec.command);
    command
        .args(&spec.args)
        .stdin(StdStdio::piped())
        .stdout(StdStdio::piped())
        .stderr(StdStdio::inherit());
    if let Some(cwd) = &spec.cwd {
        command.current_dir(cwd);
    }
    let mut child = command
        .spawn()
        .map_err(|error| blocking_error(spec, spawn_blocking_error(spec, &command, error)))?;
    let stdin = child.stdin.take().ok_or_else(|| {
        blocking_error(
            spec,
            format!("extension '{}' did not expose stdin", spec.extension_name),
        )
    })?;
    let stdout = child.stdout.take().ok_or_else(|| {
        blocking_error(
            spec,
            format!("extension '{}' did not expose stdout", spec.extension_name),
        )
    })?;
    Ok((child, stdin, stdout))
}
