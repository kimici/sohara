//! Manifest registration and validation.

use super::*;

pub(crate) fn register_manifest(
    registry: &mut ComponentRegistry,
    host: &mut StdioExtensionHost,
    manifest: &StdioExtensionManifest,
    manifest_path: &Path,
) -> Result<()> {
    let command = resolve_command(manifest_path, &manifest.command);
    let cwd = manifest
        .cwd
        .as_deref()
        .map(|cwd| resolve_command(manifest_path, cwd));
    for step in &manifest.steps {
        if step.kind == StepKind::Control {
            return Err(Error::Config(format!(
                "extension '{}' step '{}': control steps are not supported in the stdio host",
                manifest.name, step.step_type
            )));
        }
        let spec = AsyncSpec {
            extension_name: manifest.name.clone(),
            extension_version: manifest.version.clone(),
            protocol: manifest.protocol.clone(),
            command: command.clone(),
            cwd: cwd.clone(),
            args: manifest.args.clone(),
            kind: match step.kind {
                StepKind::Source => AsyncKind::Source,
                StepKind::Transform => AsyncKind::Transform,
                StepKind::Sink => AsyncKind::Sink,
                StepKind::Control => unreachable!(),
            },
            type_name: step.step_type.clone(),
        };
        registry.register(
            step.kind,
            &step.step_type,
            Arc::new(StdioStepFactory { spec }),
        );
    }
    for trigger in &manifest.triggers {
        host.triggers.insert(
            trigger.type_name.clone(),
            AsyncSpec {
                extension_name: manifest.name.clone(),
                extension_version: manifest.version.clone(),
                protocol: manifest.protocol.clone(),
                command: command.clone(),
                cwd: cwd.clone(),
                args: manifest.args.clone(),
                kind: AsyncKind::Trigger,
                type_name: trigger.type_name.clone(),
            },
        );
    }
    for store in &manifest.state_stores {
        host.state_stores.insert(
            store.type_name.clone(),
            BlockingSpec {
                extension_name: manifest.name.clone(),
                extension_version: manifest.version.clone(),
                protocol: manifest.protocol.clone(),
                command: command.clone(),
                cwd: cwd.clone(),
                args: manifest.args.clone(),
                kind: BlockingKind::StateStore,
                type_name: store.type_name.clone(),
            },
        );
    }
    for bus in &manifest.event_buses {
        host.event_buses.insert(
            bus.type_name.clone(),
            BlockingSpec {
                extension_name: manifest.name.clone(),
                extension_version: manifest.version.clone(),
                protocol: manifest.protocol.clone(),
                command: command.clone(),
                cwd: cwd.clone(),
                args: manifest.args.clone(),
                kind: BlockingKind::EventBus,
                type_name: bus.type_name.clone(),
            },
        );
    }
    host.loaded.push(LoadedExtension {
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        registrations: manifest_registrations(manifest),
    });
    Ok(())
}

pub(crate) fn validate_manifest(
    manifest: &StdioExtensionManifest,
    registry: &ComponentRegistry,
    host: &StdioExtensionHost,
    trusted: bool,
) -> Result<()> {
    if manifest.builtin && !trusted {
        return Err(Error::Config(format!(
            "extension '{}' declares builtin=true outside Sohara's trusted builtin path",
            manifest.name
        )));
    }
    if manifest.protocol != PROTOCOL {
        return Err(Error::Config(format!(
            "extension '{}' uses unsupported protocol '{}'; expected '{}'",
            manifest.name, manifest.protocol, PROTOCOL
        )));
    }
    if manifest.steps.is_empty()
        && manifest.triggers.is_empty()
        && manifest.state_stores.is_empty()
        && manifest.event_buses.is_empty()
    {
        return Err(Error::Config(format!(
            "extension '{}' must declare at least one registration",
            manifest.name
        )));
    }
    validate_builtin_prefixes(manifest, trusted)?;
    let mut seen_steps = HashSet::new();
    for step in &manifest.steps {
        if !seen_steps.insert((step.kind, step.step_type.clone())) {
            return Err(Error::Config(format!(
                "extension '{}' declares duplicate kind='{}' type='{}'",
                manifest.name,
                step.kind.as_str(),
                step.step_type
            )));
        }
        if registry.contains(step.kind, &step.step_type) {
            return Err(Error::Config(format!(
                "extension '{}' duplicates existing registration for kind='{}' type='{}'",
                manifest.name,
                step.kind.as_str(),
                step.step_type
            )));
        }
    }
    validate_named_duplicates(manifest, "trigger", &manifest.triggers, |type_name| {
        host.triggers.contains_key(type_name)
    })?;
    validate_named_duplicates(
        manifest,
        "state_store",
        &manifest.state_stores,
        |type_name| host.state_stores.contains_key(type_name),
    )?;
    validate_named_duplicates(manifest, "event_bus", &manifest.event_buses, |type_name| {
        host.event_buses.contains_key(type_name)
    })?;
    Ok(())
}

pub(crate) fn validate_builtin_prefixes(
    manifest: &StdioExtensionManifest,
    trusted: bool,
) -> Result<()> {
    let registrations = manifest_registrations(manifest);
    let reserved = registrations.iter().find(|registration| {
        registration
            .rsplit(':')
            .next()
            .is_some_and(|name| name.starts_with("builtin-"))
    });
    let Some(reserved) = reserved else {
        return Ok(());
    };
    if !trusted || !manifest.builtin {
        return Err(Error::Config(format!(
            "registration '{}' is reserved for Sohara builtin extensions",
            reserved
        )));
    }
    Ok(())
}

pub(crate) fn validate_named_duplicates(
    manifest: &StdioExtensionManifest,
    kind: &str,
    registrations: &[NamedRegistration],
    exists: impl Fn(&str) -> bool,
) -> Result<()> {
    let mut seen = HashSet::new();
    for registration in registrations {
        if !seen.insert(registration.type_name.clone()) {
            return Err(Error::Config(format!(
                "extension '{}' declares duplicate {kind} type='{}'",
                manifest.name, registration.type_name
            )));
        }
        if exists(&registration.type_name) {
            return Err(Error::Config(format!(
                "extension '{}' duplicates existing {kind} type='{}'",
                manifest.name, registration.type_name
            )));
        }
    }
    Ok(())
}

pub(crate) fn manifest_registrations(manifest: &StdioExtensionManifest) -> Vec<String> {
    let mut result = Vec::new();
    result.extend(
        manifest
            .steps
            .iter()
            .map(|step| format!("{}:{}", step.kind.as_str(), step.step_type)),
    );
    result.extend(
        manifest
            .triggers
            .iter()
            .map(|trigger| format!("trigger:{}", trigger.type_name)),
    );
    result.extend(
        manifest
            .state_stores
            .iter()
            .map(|store| format!("state_store:{}", store.type_name)),
    );
    result.extend(
        manifest
            .event_buses
            .iter()
            .map(|bus| format!("event_bus:{}", bus.type_name)),
    );
    result
}
