//! What a source checks before it captures (issue #19). Server checks run at
//! connect, table checks when a table enters the include list.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// One finding: the object, what is wrong with it, and the fix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateItem {
    pub object: String,
    pub problem: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fix: Option<String>,
}

impl std::fmt::Display for GateItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.object, self.problem)?;
        if let Some(fix) = &self.fix {
            write!(f, ". Remediation: {fix}")?;
        }
        Ok(())
    }
}

/// The outcome of a gate. A failure stops capture unless the gate is skipped;
/// a warning does not.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateReport {
    pub failures: Vec<GateItem>,
    pub warnings: Vec<GateItem>,
}

impl GateReport {
    pub fn fail(
        &mut self,
        object: impl Into<String>,
        problem: impl Into<String>,
        fix: Option<&str>,
    ) {
        self.failures.push(GateItem {
            object: object.into(),
            problem: problem.into(),
            fix: fix.map(str::to_string),
        });
    }

    pub fn warn(
        &mut self,
        object: impl Into<String>,
        problem: impl Into<String>,
        fix: Option<&str>,
    ) {
        self.warnings.push(GateItem {
            object: object.into(),
            problem: problem.into(),
            fix: fix.map(str::to_string),
        });
    }

    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }

    /// `Err` naming every failure, for a source about to start.
    pub fn into_result(self) -> Result<Self> {
        if self.passed() {
            return Ok(self);
        }
        let text = self
            .failures
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        Err(Error::Source(format!("capture gate failed: {text}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_report_with_only_warnings_passes() {
        let mut report = GateReport::default();
        report.warn("server", "slow", None);
        assert!(report.passed());
        assert_eq!(report.into_result().unwrap().warnings.len(), 1);
    }

    #[test]
    fn the_error_names_every_failure_and_its_fix() {
        let mut report = GateReport::default();
        report.fail("binlog_format", "MIXED (required ROW)", Some("SET ..."));
        report.fail("db.t", "no primary key", None);
        let text = report.into_result().unwrap_err().to_string();
        assert!(text.contains("binlog_format: MIXED (required ROW). Remediation: SET ..."));
        assert!(text.contains("db.t: no primary key"));
    }
}
