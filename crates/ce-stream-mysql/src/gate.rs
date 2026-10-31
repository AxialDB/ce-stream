//! MySQL capture preflight: verify server settings required for safe ROW CDC.

use std::collections::HashMap;

use ce_stream_core::error::{Error, Result};
use ce_stream_core::gate::GateReport;
use mysql_binlog_connector_rust::command::authenticator::Authenticator;
use mysql_binlog_connector_rust::command::command_util::CommandUtil;

const GATE_VARIABLES: &[&str] = &[
    "binlog_format",
    "binlog_row_image",
    "binlog_row_metadata",
    "gtid_mode",
    "enforce_gtid_consistency",
];

/// Evaluate gate variables already fetched from the server.
pub fn evaluate_gate_variables(vars: &HashMap<String, String>) -> GateReport {
    let mut report = GateReport::default();

    require_value(
        &mut report,
        vars,
        "binlog_format",
        &["ROW"],
        "SET GLOBAL binlog_format = 'ROW';",
    );
    require_value(
        &mut report,
        vars,
        "binlog_row_image",
        &["FULL"],
        "SET GLOBAL binlog_row_image = 'FULL';",
    );
    require_value(
        &mut report,
        vars,
        "binlog_row_metadata",
        &["FULL"],
        "SET PERSIST binlog_row_metadata = 'FULL';",
    );
    require_value(
        &mut report,
        vars,
        "gtid_mode",
        &["ON"],
        "SET GLOBAL gtid_mode = ON;",
    );

    match vars.get("enforce_gtid_consistency").map(|s| s.as_str()) {
        Some("ON") => {}
        Some(value) => report.warn(
            "enforce_gtid_consistency",
            format!("{value} (recommended ON for safe GTID capture)"),
            None,
        ),
        None => report.warn(
            "enforce_gtid_consistency",
            "not reported (recommended ON)",
            None,
        ),
    }

    report
}

fn require_value(
    report: &mut GateReport,
    vars: &HashMap<String, String>,
    name: &str,
    allowed: &[&str],
    remediation: &str,
) {
    match vars.get(name) {
        None => report.fail(name, "missing server variable", Some(remediation)),
        Some(value) if allowed.iter().any(|a| value.eq_ignore_ascii_case(a)) => {}
        Some(value) => report.fail(
            name,
            format!("{value} (required {})", allowed.join("|")),
            Some(remediation),
        ),
    }
}

/// Read a global GTID-related server variable (`gtid_purged`, `gtid_executed`, ...).
pub async fn fetch_global_gtid(url: &str, variable: &str) -> Result<String> {
    let allowed = ["gtid_purged", "gtid_executed"];
    if !allowed.contains(&variable) {
        return Err(Error::Source(format!(
            "fetch_global_gtid: unsupported variable {variable}"
        )));
    }

    let mut authenticator = Authenticator::new(url, 30, None)
        .map_err(|e| Error::Source(format!("gtid variable connect: {e}")))?;
    let mut channel = authenticator
        .connect()
        .await
        .map_err(|e| Error::Source(format!("gtid variable connect: {e}")))?;

    let sql = format!("SELECT @@GLOBAL.{variable}");
    let rows = CommandUtil::execute_query(&mut channel, &sql)
        .await
        .map_err(|e| Error::Source(format!("gtid variable query: {e}")))?;

    rows.into_iter()
        .next()
        .and_then(|row| row.values.into_iter().next())
        .ok_or_else(|| Error::Source(format!("gtid variable {variable}: empty result")))
}

/// Connect and validate MySQL server settings for ROW binlog capture.
pub async fn validate_capture_gates(url: &str) -> Result<GateReport> {
    let mut authenticator = Authenticator::new(url, 30, None)
        .map_err(|e| Error::Source(format!("capture gate connect: {e}")))?;
    let mut channel = authenticator
        .connect()
        .await
        .map_err(|e| Error::Source(format!("capture gate connect: {e}")))?;

    let in_list = GATE_VARIABLES
        .iter()
        .map(|v| format!("'{v}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SHOW VARIABLES WHERE Variable_name IN ({in_list})");

    let rows = CommandUtil::execute_query(&mut channel, &sql)
        .await
        .map_err(|e| Error::Source(format!("capture gate query: {e}")))?;

    let mut vars = HashMap::new();
    for row in rows {
        if row.values.len() >= 2 {
            vars.insert(row.values[0].clone(), row.values[1].clone());
        }
    }

    Ok(evaluate_gate_variables(&vars))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn passes_when_all_required_gates_ok() {
        let report = evaluate_gate_variables(&vars(&[
            ("binlog_format", "ROW"),
            ("binlog_row_image", "FULL"),
            ("binlog_row_metadata", "FULL"),
            ("gtid_mode", "ON"),
            ("enforce_gtid_consistency", "ON"),
        ]));
        assert!(report.passed());
        assert!(report.warnings.is_empty());
    }

    #[test]
    fn warns_when_enforce_gtid_consistency_off() {
        let report = evaluate_gate_variables(&vars(&[
            ("binlog_format", "ROW"),
            ("binlog_row_image", "FULL"),
            ("binlog_row_metadata", "FULL"),
            ("gtid_mode", "ON"),
            ("enforce_gtid_consistency", "OFF"),
        ]));
        assert!(report.passed());
        assert_eq!(report.warnings.len(), 1);
    }

    #[test]
    fn fails_when_binlog_row_metadata_not_full() {
        let report = evaluate_gate_variables(&vars(&[
            ("binlog_format", "ROW"),
            ("binlog_row_image", "FULL"),
            ("binlog_row_metadata", "MINIMAL"),
            ("gtid_mode", "ON"),
            ("enforce_gtid_consistency", "ON"),
        ]));
        assert_eq!(report.failures.len(), 1);
        let err = report.into_result().unwrap_err();
        assert!(err.to_string().contains("binlog_row_metadata"));
    }

    #[test]
    fn reports_every_failure_at_once() {
        let report = evaluate_gate_variables(&vars(&[("binlog_format", "MIXED")]));
        // MIXED, and the three variables the server did not report.
        assert_eq!(report.failures.len(), 4);
    }
}
