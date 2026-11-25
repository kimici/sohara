//! Async stdio client: spawn, initialize, and async JSON-RPC calls.

use super::*;

impl Drop for AsyncStdioClient {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

impl AsyncStdioClient {
    pub(crate) async fn spawn(spec: &AsyncSpec) -> Result<Self> {
        let (child, stdin, stdout) = spawn_command(spec)?;
        let mut client = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
            capabilities: ExtensionCapabilities::default(),
        };
        client.initialize(spec).await?;
        Ok(client)
    }

    async fn initialize(&mut self, spec: &AsyncSpec) -> Result<()> {
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
        let response: JsonRpcResponse<InitializeResult> = self.request(spec, request).await?;
        let result = take_async_result(spec, response, "initialize", &spec.extension_name)?;
        if result.protocol != spec.protocol {
            return Err(async_error(
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

    pub(crate) async fn source_pull(
        &mut self,
        spec: &AsyncSpec,
        step: &StepMeta,
        config: &Value,
    ) -> Result<PullResult> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "source.pull",
            params: StepParams { step, config },
        };
        let response: JsonRpcResponse<PullResult> = self.request(spec, request).await?;
        take_async_result(spec, response, "source.pull", &step.step_type)
    }

    pub(crate) async fn transform(
        &mut self,
        spec: &AsyncSpec,
        step: &StepMeta,
        config: &Value,
        record: &Record,
    ) -> Result<TransformOutcome> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "transform",
            params: TransformParams {
                step,
                config,
                record,
            },
        };
        let response: JsonRpcResponse<TransformResult> = self.request(spec, request).await?;
        let result = take_async_result(spec, response, "transform", &step.step_type)?;
        Ok(match result {
            TransformResult::Pass { record } => TransformOutcome::Pass(record),
            TransformResult::Filtered => TransformOutcome::Filtered,
            TransformResult::Expand { records } => TransformOutcome::Expand(records),
        })
    }

    pub(crate) async fn sink_send(
        &mut self,
        spec: &AsyncSpec,
        step: &StepMeta,
        config: &Value,
        record: &Record,
    ) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "sink.send",
            params: SinkParams {
                step,
                config,
                record,
            },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request).await?;
        let _ = take_async_result(spec, response, "sink.send", &step.step_type)?;
        Ok(())
    }

    pub(crate) async fn sink_flush(
        &mut self,
        spec: &AsyncSpec,
        step: &StepMeta,
        config: &Value,
    ) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "sink.flush",
            params: StepParams { step, config },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request).await?;
        let _ = take_async_result(spec, response, "sink.flush", &step.step_type)?;
        Ok(())
    }

    pub(crate) async fn trigger_start(
        &mut self,
        spec: &AsyncSpec,
        trigger: &TriggerMeta,
        config: &Value,
    ) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "trigger.start",
            params: TriggerParams { trigger, config },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request).await?;
        let _ = take_async_result(spec, response, "trigger.start", &trigger.trigger_type)?;
        Ok(())
    }

    pub(crate) async fn trigger_pull(
        &mut self,
        spec: &AsyncSpec,
        trigger: &TriggerMeta,
        config: &Value,
    ) -> Result<PullResult> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "trigger.pull",
            params: TriggerParams { trigger, config },
        };
        let response: JsonRpcResponse<PullResult> = self.request(spec, request).await?;
        take_async_result(spec, response, "trigger.pull", &trigger.trigger_type)
    }

    pub(crate) async fn trigger_stop(
        &mut self,
        spec: &AsyncSpec,
        trigger: &TriggerMeta,
        config: &Value,
    ) -> Result<()> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.take_id(),
            method: "trigger.stop",
            params: TriggerParams { trigger, config },
        };
        let response: JsonRpcResponse<EmptyResult> = self.request(spec, request).await?;
        let _ = take_async_result(spec, response, "trigger.stop", &trigger.trigger_type)?;
        Ok(())
    }

    async fn request<T, R>(
        &mut self,
        spec: &AsyncSpec,
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
            .await
            .map_err(|error| async_error(spec, write_error(request.method, error)))?;
        self.stdin
            .flush()
            .await
            .map_err(|error| async_error(spec, flush_error(request.method, error)))?;

        let mut response = String::new();
        let read = self
            .stdout
            .read_line(&mut response)
            .await
            .map_err(|error| async_error(spec, read_error(request.method, error)))?;
        if read == 0 {
            return Err(async_error(
                spec,
                format!("extension closed stdout during {}", request.method),
            ));
        }
        let parsed: JsonRpcResponse<R> = serde_json::from_str(&response).map_err(|error| {
            async_error(
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

    fn ensure_capability(&self, spec: &AsyncSpec) -> Result<()> {
        if self.capabilities.supports_async(spec.kind) {
            return Ok(());
        }
        Err(async_error(
            spec,
            format!(
                "extension '{}' does not advertise {} capability",
                spec.extension_name,
                async_kind_name(spec.kind)
            ),
        ))
    }

    fn take_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

fn spawn_command(spec: &AsyncSpec) -> Result<(Child, ChildStdin, ChildStdout)> {
    let mut command = Command::new(&spec.command);
    command
        .args(&spec.args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit());
    if let Some(cwd) = &spec.cwd {
        command.current_dir(cwd);
    }
    let mut child = command
        .spawn()
        .map_err(|error| async_error(spec, spawn_async_error(spec, &command, error)))?;
    let stdin = child.stdin.take().ok_or_else(|| {
        async_error(
            spec,
            format!("extension '{}' did not expose stdin", spec.extension_name),
        )
    })?;
    let stdout = child.stdout.take().ok_or_else(|| {
        async_error(
            spec,
            format!("extension '{}' did not expose stdout", spec.extension_name),
        )
    })?;
    Ok((child, stdin, stdout))
}
