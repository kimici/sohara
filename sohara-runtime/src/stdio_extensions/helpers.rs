//! Capability checks and async client lifecycle helpers.

use super::*;

impl ExtensionCapabilities {
    pub(crate) const fn supports_async(&self, kind: AsyncKind) -> bool {
        match kind {
            AsyncKind::Source => self.source,
            AsyncKind::Transform => self.transform,
            AsyncKind::Sink => self.sink,
            AsyncKind::Trigger => self.trigger,
        }
    }

    pub(crate) const fn supports_blocking(&self, kind: BlockingKind) -> bool {
        match kind {
            BlockingKind::StateStore => self.state_store,
            BlockingKind::EventBus => self.event_bus,
        }
    }
}

async fn pull_source(state: &mut SourceState) -> Result<PullResult> {
    let mut guard = state.client.lock().await;
    let client = ensure_async_client(&mut guard, &state.spec).await?;
    let result = client
        .source_pull(&state.spec, &state.step, &state.config)
        .await;
    match result {
        Ok(pulled) if pulled.records.is_empty() && !pulled.done => {
            reset_async_client(&mut guard);
            Err(async_error(
                &state.spec,
                format!(
                    "extension '{}' returned no records and done=false for source.pull",
                    state.spec.extension_name
                ),
            ))
        }
        Ok(pulled) => Ok(pulled),
        Err(error) => {
            reset_async_client(&mut guard);
            Err(error)
        }
    }
}

pub(crate) async fn next_source_record(
    mut state: SourceState,
) -> Result<Option<(Record, SourceState)>> {
    loop {
        if let Some(record) = state.buffered.pop_front() {
            return Ok(Some((record, state)));
        }
        if state.finished {
            return Ok(None);
        }
        let pulled = pull_source(&mut state).await?;
        state.finished = pulled.done;
        state.buffered = pulled.records.into();
    }
}

pub(crate) async fn next_trigger_record(
    mut state: TriggerState,
) -> Result<Option<(Record, TriggerState)>> {
    loop {
        if let Some(record) = state.buffered.pop_front() {
            return Ok(Some((record, state)));
        }
        if state.finished {
            return Ok(None);
        }
        let mut guard = state.client.lock().await;
        let client = ensure_async_client(&mut guard, &state.spec).await?;
        let response = client
            .trigger_pull(&state.spec, &state.trigger, &state.config)
            .await;
        let pulled = match response {
            Ok(result) => result,
            Err(error) => {
                reset_async_client(&mut guard);
                return Err(error);
            }
        };
        if !pulled.done && pulled.records.is_empty() {
            drop(guard);
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        state.finished = pulled.done;
        state.buffered = pulled.records.into();
    }
}

pub(crate) async fn ensure_async_client<'a>(
    guard: &'a mut Option<AsyncStdioClient>,
    spec: &AsyncSpec,
) -> Result<&'a mut AsyncStdioClient> {
    if guard.is_none() {
        *guard = Some(AsyncStdioClient::spawn(spec).await?);
    }
    guard.as_mut().ok_or_else(|| {
        async_error(
            spec,
            format!("extension '{}' failed to initialize", spec.extension_name),
        )
    })
}

pub(crate) fn ensure_blocking_client<'a>(
    guard: &'a mut Option<BlockingStdioClient>,
    spec: &BlockingSpec,
) -> Result<&'a mut BlockingStdioClient> {
    if guard.is_none() {
        *guard = Some(BlockingStdioClient::spawn(spec)?);
    }
    guard.as_mut().ok_or_else(|| {
        blocking_error(
            spec,
            format!("extension '{}' failed to initialize", spec.extension_name),
        )
    })
}

pub(crate) fn reset_async_client(client: &mut Option<AsyncStdioClient>) {
    if let Some(client) = client.as_mut() {
        let _ = client.child.start_kill();
    }
    *client = None;
}

pub(crate) fn reset_blocking_client(client: &mut Option<BlockingStdioClient>) {
    if let Some(client) = client.as_mut() {
        let _ = client.child.kill();
    }
    *client = None;
}
