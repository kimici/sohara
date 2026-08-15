//! Small shared utilities

use std::time::Duration;

/// Parse a duration string with a unit suffix: `500ms`, `2s`, `5m`, `1h`.
///
/// # Errors
/// Returns a message when the string has no valid unit or number.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let (digits, unit) = split_at_unit(text)?;
    let amount: f64 = digits
        .parse()
        .map_err(|_| format!("invalid duration '{text}'"))?;
    let millis = match unit {
        "ms" => amount,
        "s" => amount * 1_000.0,
        "m" => amount * 60_000.0,
        "h" => amount * 3_600_000.0,
        other => return Err(format!("invalid duration unit '{other}' (use ms|s|m|h)")),
    };
    if !millis.is_finite() || millis < 0.0 {
        return Err(format!("invalid duration '{text}'"));
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Ok(Duration::from_millis(millis.round() as u64))
}

fn split_at_unit(text: &str) -> Result<(&str, &str), String> {
    let index = text
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .ok_or_else(|| format!("duration '{text}' needs a unit (ms|s|m|h)"))?;
    if index == 0 {
        return Err(format!("invalid duration '{text}'"));
    }
    Ok((&text[..index], &text[index..]))
}
