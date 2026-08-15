//! Desired/actual reconciliation: command decisions (D3)

use sohara_agent::{Command, InstanceReport, InstanceState};

use crate::registry::Inner;
use crate::types::{Desired, InstanceDecl};

/// The declared instances of one node.
pub(crate) fn declared_for(inner: &Inner, node: &str) -> Vec<InstanceDecl> {
    inner
        .instances
        .values()
        .filter(|decl| decl.node == node)
        .cloned()
        .collect()
}

/// Enqueue commands moving each declared instance toward its desired state.
pub(crate) fn enqueue_reconcile(
    inner: &mut Inner,
    node: &str,
    declared: &[InstanceDecl],
    reports: &[InstanceReport],
) {
    for decl in declared {
        let actual = reports.iter().find(|report| report.id == decl.id);
        let pending = inner.pending.get(node).map(Vec::as_slice).unwrap_or(&[]);
        let Some(mut command) = desired_command(decl, actual, pending) else {
            continue;
        };
        let seq = inner.seq.entry(node.to_owned()).or_insert(0);
        *seq += 1;
        command.seq = *seq;
        inner
            .pending
            .entry(node.to_owned())
            .or_default()
            .push(command);
    }
}

/// Human-readable state transitions in one heartbeat batch (D6 events).
pub(crate) fn state_transitions(
    inner: &Inner,
    node: &str,
    reports: &[InstanceReport],
) -> Vec<String> {
    reports
        .iter()
        .filter_map(|report| {
            let previous = actual_of(inner, node, &report.id).map(|r| r.state);
            (previous != Some(report.state)).then(|| {
                format!(
                    "instance '{}' state {} -> {}",
                    report.id,
                    previous.map_or("unknown", |s| s.as_str()),
                    report.state.as_str()
                )
            })
        })
        .collect()
}

/// The actual report for an instance, if the node reported one.
pub(crate) fn actual_of<'a>(
    inner: &'a Inner,
    node: &str,
    instance: &str,
) -> Option<&'a InstanceReport> {
    inner
        .actual
        .get(node)
        .and_then(|reports| reports.iter().find(|report| report.id == instance))
}

/// The command that moves an instance toward its desired state, if any.
pub(crate) fn desired_command(
    decl: &InstanceDecl,
    actual: Option<&InstanceReport>,
    pending: &[Command],
) -> Option<Command> {
    let actual = actual?;
    let op = match (decl.desired, actual.state) {
        (Desired::Running, InstanceState::Stopped | InstanceState::Failed) => "start",
        (Desired::Running, InstanceState::Paused) => "resume",
        (Desired::Paused, InstanceState::Running) => "pause",
        (Desired::Paused, InstanceState::Stopped | InstanceState::Failed) => "start",
        (
            Desired::Stopped,
            InstanceState::Running
            | InstanceState::Paused
            | InstanceState::Starting
            | InstanceState::Restarting,
        ) => "stop",
        _ => return None,
    };
    if pending
        .iter()
        .any(|command| command.instance == decl.id && command.op == op)
    {
        return None;
    }
    Some(Command {
        seq: 0,
        op: op.to_owned(),
        instance: decl.id.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decl(desired: Desired) -> InstanceDecl {
        InstanceDecl {
            id: "i1".to_owned(),
            node: "n1".to_owned(),
            flow_id: None,
            desired,
            spec: sohara_agent::InstanceSpec {
                id: "i1".to_owned(),
                ..sohara_agent::InstanceSpec::default()
            },
        }
    }

    fn report(state: InstanceState) -> InstanceReport {
        InstanceReport {
            id: "i1".to_owned(),
            state,
            paused: state == InstanceState::Paused,
            healthy: true,
            restarts: 0,
            admin: None,
            trigger: None,
        }
    }

    #[test]
    fn running_desired_against_stopped_actual_starts() {
        let command = desired_command(
            &decl(Desired::Running),
            Some(&report(InstanceState::Stopped)),
            &[],
        );
        assert_eq!(command.map(|c| c.op), Some("start".to_owned()));
    }

    #[test]
    fn stopped_desired_against_running_actual_stops() {
        let command = desired_command(
            &decl(Desired::Stopped),
            Some(&report(InstanceState::Running)),
            &[],
        );
        assert_eq!(command.map(|c| c.op), Some("stop".to_owned()));
    }

    #[test]
    fn paused_desired_against_running_actual_pauses() {
        let command = desired_command(
            &decl(Desired::Paused),
            Some(&report(InstanceState::Running)),
            &[],
        );
        assert_eq!(command.map(|c| c.op), Some("pause".to_owned()));
    }

    #[test]
    fn converged_states_and_unknown_reports_yield_nothing() {
        assert_eq!(
            desired_command(
                &decl(Desired::Running),
                Some(&report(InstanceState::Running)),
                &[]
            ),
            None
        );
        assert_eq!(
            desired_command(
                &decl(Desired::Stopped),
                Some(&report(InstanceState::Stopped)),
                &[]
            ),
            None
        );
        assert_eq!(desired_command(&decl(Desired::Running), None, &[]), None);
    }

    #[test]
    fn pending_duplicate_is_not_reenqueued() {
        let pending = vec![Command {
            seq: 1,
            op: "start".to_owned(),
            instance: "i1".to_owned(),
        }];
        assert_eq!(
            desired_command(
                &decl(Desired::Running),
                Some(&report(InstanceState::Stopped)),
                &pending
            ),
            None
        );
    }
}
