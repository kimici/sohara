//! Step/trigger trait implementations backed by a stdio client.

use super::*;

#[async_trait::async_trait]
impl StepFactory for StdioStepFactory {
    fn build(&self, config: &Value, ctx: &BuildContext) -> Result<BuiltStep> {
        let Some(step) = ctx.step.clone() else {
            return Err(Error::Config(format!(
                "stdio extension '{}' requires step metadata during build",
                self.spec.type_name
            )));
        };
        if self.spec.kind == AsyncKind::Trigger {
            return Err(Error::Config(format!(
                "trigger '{}' must be declared under flow.triggers, not steps",
                self.spec.type_name
            )));
        }
        build_step(&self.spec, config, step)
    }
}

fn build_step(spec: &AsyncSpec, config: &Value, step: StepMeta) -> Result<BuiltStep> {
    let client = Arc::new(Mutex::new(None));
    let name = format!("{}::{}", spec.extension_name, spec.type_name);
    let spec = spec.clone();
    let config = config.clone();
    Ok(match spec.kind {
        AsyncKind::Source => BuiltStep::Source(Box::new(StdioSourceStep {
            name,
            spec,
            config,
            step,
            client,
        })),
        AsyncKind::Transform => BuiltStep::Transform(Box::new(StdioTransformStep {
            name,
            spec,
            config,
            step,
            client,
        })),
        AsyncKind::Sink => BuiltStep::Sink(Box::new(StdioSinkStep {
            name,
            spec,
            config,
            step,
            client,
        })),
        AsyncKind::Trigger => unreachable!(),
    })
}

#[async_trait::async_trait]
impl Source for StdioSourceStep {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        let state = SourceState {
            spec: self.spec.clone(),
            config: self.config.clone(),
            step: self.step.clone(),
            client: self.client.clone(),
            buffered: VecDeque::new(),
            finished: false,
        };
        Ok(Box::pin(stream::try_unfold(state, next_source_record)))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl Transform for StdioTransformStep {
    async fn transform(&self, record: Record) -> Result<TransformOutcome> {
        let mut guard = self.client.lock().await;
        let client = ensure_async_client(&mut guard, &self.spec).await?;
        match client
            .transform(&self.spec, &self.step, &self.config, &record)
            .await
        {
            Ok(result) => Ok(result),
            Err(error) => {
                reset_async_client(&mut guard);
                Err(error)
            }
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl Sink for StdioSinkStep {
    async fn send(&self, record: Record) -> Result<()> {
        let mut guard = self.client.lock().await;
        let client = ensure_async_client(&mut guard, &self.spec).await?;
        if let Err(error) = client
            .sink_send(&self.spec, &self.step, &self.config, &record)
            .await
        {
            reset_async_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let mut guard = self.client.lock().await;
        let client = ensure_async_client(&mut guard, &self.spec).await?;
        if let Err(error) = client
            .sink_flush(&self.spec, &self.step, &self.config)
            .await
        {
            reset_async_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl Source for StdioTriggerSource {
    async fn stream(&self) -> Result<BoxStream<'static, Result<Record>>> {
        let state = TriggerState {
            spec: self.spec.clone(),
            config: self.config.clone(),
            trigger: self.trigger.clone(),
            client: self.client.clone(),
            buffered: VecDeque::new(),
            finished: false,
        };
        Ok(Box::pin(stream::try_unfold(state, next_trigger_record)))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl Trigger for StdioTriggerSource {
    async fn start(&self) -> Result<()> {
        let mut guard = self.client.lock().await;
        let client = ensure_async_client(&mut guard, &self.spec).await?;
        if let Err(error) = client
            .trigger_start(&self.spec, &self.trigger, &self.config)
            .await
        {
            reset_async_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let mut guard = self.client.lock().await;
        let Some(client) = guard.as_mut() else {
            return Ok(());
        };
        if let Err(error) = client
            .trigger_stop(&self.spec, &self.trigger, &self.config)
            .await
        {
            reset_async_client(&mut guard);
            return Err(error);
        }
        Ok(())
    }
}
