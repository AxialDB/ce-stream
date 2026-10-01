use serde::{Deserialize, Serialize};

use crate::event::{CloudEvent, TableRef};

/// One DDL statement observed in the binlog within a transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DdlStatement {
    pub schema: String,
    pub query: String,
}

/// Where a commit sits in the source change log.
///
/// `after` is the checkpoint payload that resumes capture after this commit
/// (same shape the adapter stores in [`crate::Checkpoint::payload`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourcePosition {
    /// Adapter id, e.g. `mysql` or `mongo`.
    pub adapter: String,
    /// Position of this commit.
    pub at: serde_json::Value,
    /// Resume position once this commit is applied.
    pub after: serde_json::Value,
}

/// Lifecycle change to a source table or collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlKind {
    Dropped,
    Renamed,
    DatabaseDropped,
    /// The source can no longer deliver changes for `subject` on this session.
    Invalidated,
}

impl ControlKind {
    pub fn as_ce_type(&self) -> &'static str {
        match self {
            Self::Dropped => "io.ce-stream.control.dropped",
            Self::Renamed => "io.ce-stream.control.renamed",
            Self::DatabaseDropped => "io.ce-stream.control.database_dropped",
            Self::Invalidated => "io.ce-stream.control.invalidated",
        }
    }
}

/// Typed lifecycle event for sources without SQL DDL text. MySQL reports DDL
/// through [`DdlStatement`] instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlEvent {
    pub kind: ControlKind,
    /// Affected object. `table` is empty for [`ControlKind::DatabaseDropped`].
    pub subject: TableRef,
    /// New name for [`ControlKind::Renamed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renamed_to: Option<TableRef>,
}

impl ControlEvent {
    /// Row-mode CloudEvent for this control event.
    pub fn to_cloud_event(
        &self,
        source: impl Into<String>,
        extensions: serde_json::Map<String, serde_json::Value>,
    ) -> CloudEvent {
        let subject = if self.subject.table.is_empty() {
            self.subject.database.clone()
        } else {
            self.subject.as_subject()
        };
        CloudEvent {
            specversion: "1.0".into(),
            id: uuid::Uuid::new_v4().to_string(),
            ty: self.kind.as_ce_type().into(),
            source: source.into(),
            subject,
            time: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            extensions,
            data: serde_json::to_value(self).unwrap_or(serde_json::Value::Null),
        }
    }
}

/// One committed source transaction — primary delivery unit when `delivery_unit = transaction`.
///
/// MySQL fills `gtid` / `gtid_set_after` and leaves `position` and `control`
/// empty, so its JSON and Avro output match v0.3. Other adapters leave the GTID
/// fields empty and set `position`. Use [`Self::source_position`] for either.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommittedTransaction {
    /// MySQL: GTID of this commit, e.g. `3e11fa47-71ca-11e1-9e33-c080020c9a66:1`.
    #[serde(default)]
    pub gtid: String,
    /// MySQL: executed GTID set **after** this transaction commits (for checkpoint resume).
    #[serde(default)]
    pub gtid_set_after: String,
    /// Non-MySQL adapters: position of this commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<SourcePosition>,
    /// DDL in binlog order, before row events.
    pub ddl: Vec<DdlStatement>,
    /// Lifecycle events, after row events. Empty for MySQL.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub control: Vec<ControlEvent>,
    /// Row change events in source order.
    pub events: Vec<CloudEvent>,
}

impl CommittedTransaction {
    /// Adapter-neutral position. MySQL commits derive it from the GTID fields:
    /// `at = {"gtid": gtid}`, `after = {"gtid": gtid_set_after}`.
    pub fn source_position(&self) -> SourcePosition {
        self.position.clone().unwrap_or_else(|| SourcePosition {
            adapter: "mysql".into(),
            at: serde_json::json!({ "gtid": self.gtid }),
            after: serde_json::json!({ "gtid": self.gtid_set_after }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mysql_txn() -> CommittedTransaction {
        CommittedTransaction {
            gtid: "sid:5".into(),
            gtid_set_after: "sid:1-5".into(),
            ..Default::default()
        }
    }

    #[test]
    fn mysql_json_matches_v03() {
        assert_eq!(
            serde_json::to_string(&mysql_txn()).unwrap(),
            r#"{"gtid":"sid:5","gtid_set_after":"sid:1-5","ddl":[],"events":[]}"#
        );
    }

    #[test]
    fn mysql_position_is_derived_from_gtid() {
        let pos = mysql_txn().source_position();
        assert_eq!(pos.adapter, "mysql");
        assert_eq!(pos.at["gtid"], "sid:5");
        assert_eq!(pos.after["gtid"], "sid:1-5");
    }

    #[test]
    fn v03_json_still_decodes() {
        let txn: CommittedTransaction = serde_json::from_str(
            r#"{"gtid":"sid:2","gtid_set_after":"sid:1-2","ddl":[],"events":[]}"#,
        )
        .unwrap();
        assert!(txn.position.is_none());
        assert!(txn.control.is_empty());
        assert_eq!(txn.source_position().after["gtid"], "sid:1-2");
    }

    #[test]
    fn adapter_position_roundtrips() {
        let txn = CommittedTransaction {
            position: Some(SourcePosition {
                adapter: "mongo".into(),
                at: serde_json::json!({ "cluster_time": [1, 2] }),
                after: serde_json::json!({ "resume_token": "82AB" }),
            }),
            control: vec![ControlEvent {
                kind: ControlKind::Renamed,
                subject: TableRef::new("app", "orders"),
                renamed_to: Some(TableRef::new("app", "orders_old")),
            }],
            ..Default::default()
        };
        let back: CommittedTransaction =
            serde_json::from_str(&serde_json::to_string(&txn).unwrap()).unwrap();
        assert_eq!(back.source_position(), txn.source_position());
        assert_eq!(back.control, txn.control);
        assert!(back.gtid.is_empty());
    }

    #[test]
    fn control_cloud_event_uses_database_subject_for_database_drop() {
        let ev = ControlEvent {
            kind: ControlKind::DatabaseDropped,
            subject: TableRef::new("app", ""),
            renamed_to: None,
        }
        .to_cloud_event("mongo://test", serde_json::Map::new());
        assert_eq!(ev.ty, "io.ce-stream.control.database_dropped");
        assert_eq!(ev.subject, "app");
        assert_eq!(ev.data["kind"], "database_dropped");
    }
}
