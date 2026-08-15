//! Pure restart-policy decisions (D2)

use std::time::Duration;

use crate::config::Policy;

/// How long to wait before the next restart attempt, if one is allowed.
///
/// Returns `None` when restarts are disabled or the attempt budget is spent.
/// The backoff doubles per attempt and is capped at 60 seconds.
pub fn restart_backoff(policy: &Policy, attempts: u32) -> Option<Duration> {
    if !policy.restart || attempts >= policy.max_restarts {
        return None;
    }
    let factor = 1u64 << attempts.min(10);
    let millis = policy.backoff_ms.saturating_mul(factor).min(60_000);
    Some(Duration::from_millis(millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(restart: bool, max: u32, backoff_ms: u64) -> Policy {
        Policy {
            restart,
            max_restarts: max,
            backoff_ms,
            health_failures: 3,
        }
    }

    #[test]
    fn disabled_restart_yields_none() {
        assert_eq!(restart_backoff(&policy(false, 5, 100), 0), None);
    }

    #[test]
    fn attempts_past_budget_yield_none() {
        let policy = policy(true, 2, 100);
        assert!(restart_backoff(&policy, 0).is_some());
        assert!(restart_backoff(&policy, 1).is_some());
        assert_eq!(restart_backoff(&policy, 2), None);
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let policy = policy(true, 20, 1000);
        assert_eq!(
            restart_backoff(&policy, 0),
            Some(Duration::from_millis(1000))
        );
        assert_eq!(
            restart_backoff(&policy, 1),
            Some(Duration::from_millis(2000))
        );
        assert_eq!(
            restart_backoff(&policy, 3),
            Some(Duration::from_millis(8000))
        );
        assert_eq!(
            restart_backoff(&policy, 10),
            Some(Duration::from_secs(60)),
            "backoff must be capped at 60s"
        );
    }
}
