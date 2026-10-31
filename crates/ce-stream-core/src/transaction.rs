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

/// Lifecycle change to a source table or collection. The consumer decides what
/// stops its own work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ControlKind {
    Dropped,
    Renamed {
        /// v0.5 wrote this as `renamed_to`, beside the kind.
        #[serde(alias = "renamed_to")]
        to: TableRef,
    },
    /// All rows of the subject are gone as of this commit.
    Truncated,
    /// The subject's columns differ from its last [`crate::TableSchema`].
    DefinitionChanged,
    DatabaseDropped,
    /// The source can no longer deliver changes for `subject` on this session.
    Invalidated,
    /// A message the consumer wrote into the source's log inside a
    /// transaction, delivered at its position under its prefix.
    Message {
        prefix: String,
        #[serde(with = "base64_bytes")]
        content: Vec<u8>,
    },
}

mod base64_bytes {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(text)
            .map_err(serde::de::Error::custom)
    }
}

impl ControlKind {
    pub fn as_ce_type(&self) -> &'static str {
        match self {
            Self::Dropped => "io.ce-stream.control.dropped",
            Self::Renamed { .. } => "io.ce-stream.control.renamed",
            Self::Truncated => "io.ce-stream.control.truncated",
            Self::DefinitionChanged => "io.ce-stream.control.definition_changed",
            Self::DatabaseDropped => "io.ce-stream.control.database_dropped",
            Self::Invalidated => "io.ce-stream.control.invalidated",
            Self::Message { .. } => "io.ce-stream.control.message",
        }
    }
}

/// Typed lifecycle event, on every adapter. A host with SQL DDL text also has
/// it in [`DdlStatement`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlEvent {
    #[serde(flatten)]
    pub kind: ControlKind,
    /// Affected object. `table` is empty for [`ControlKind::DatabaseDropped`].
    pub subject: TableRef,
    /// The table's host identity, where it has one that survives a rename.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
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

/// Where a delivery sits in a transaction that is delivered in parts.
///
/// A transaction larger than one part (see [`crate::SourceConfig::part_max_rows`]
/// and [`crate::SourceConfig::part_max_bytes`]) is delivered as several
/// [`CommittedTransaction`] values with the same `at`, numbered from 1, in
/// order. Only a [`crate::TableSchema`] may come between them. Only the last
/// one ends the transaction: it alone carries the position after it and the
/// transaction's control events, and moves the checkpoint. A part that is not
/// the last is not a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionPart {
    /// 1 for the first part.
    pub number: u32,
    /// The last part of the transaction.
    pub last: bool,
}

/// One committed source transaction — primary delivery unit when `delivery_unit = transaction`.
///
/// A transaction's rows are delivered when it commits, on every adapter.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommittedTransaction {
    /// Where this commit sits. Every adapter sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<SourcePosition>,
    /// DDL in binlog order, before row events.
    #[serde(default)]
    pub ddl: Vec<DdlStatement>,
    /// Lifecycle events; a consumer reads them all before it applies the rows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub control: Vec<ControlEvent>,
    /// Row change events in source order.
    pub events: Vec<CloudEvent>,
    /// Set when the transaction is delivered in parts; `None` for a
    /// transaction delivered whole. See [`TransactionPart`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub part: Option<TransactionPart>,
}

impl CommittedTransaction {
    /// Does this delivery end its transaction: a whole transaction, or the
    /// last part of one? Only such a delivery carries the position after the
    /// transaction and may move a checkpoint.
    pub fn ends_transaction(&self) -> bool {
        self.part.map_or(true, |part| part.last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapter_position_roundtrips() {
        let txn = CommittedTransaction {
            part: None,
            position: Some(SourcePosition {
                adapter: "mongo".into(),
                at: serde_json::json!({ "cluster_time": [1, 2] }),
                after: serde_json::json!({ "resume_token": "82AB" }),
            }),
            control: vec![ControlEvent {
                kind: ControlKind::Renamed {
                    to: TableRef::new("app", "orders_old"),
                },
                subject: TableRef::new("app", "orders"),
                identity: None,
            }],
            ..Default::default()
        };
        let back: CommittedTransaction =
            serde_json::from_str(&serde_json::to_string(&txn).unwrap()).unwrap();
        assert_eq!(back.position, txn.position);
        assert_eq!(back.control, txn.control);
    }

    #[test]
    fn a_v05_control_event_still_decodes() {
        let back: ControlEvent = serde_json::from_str(
            r#"{"kind":"renamed","subject":{"database":"a","table":"t"},"renamed_to":{"database":"a","table":"u"}}"#,
        )
        .unwrap();
        assert_eq!(
            back.kind,
            ControlKind::Renamed {
                to: TableRef::new("a", "u")
            }
        );
        let back: ControlEvent =
            serde_json::from_str(r#"{"kind":"dropped","subject":{"database":"a","table":"t"}}"#)
                .unwrap();
        assert_eq!(back.kind, ControlKind::Dropped);
    }

    #[test]
    fn a_message_control_keeps_its_bytes() {
        let event = ControlEvent {
            kind: ControlKind::Message {
                prefix: "axialdb_ddl".into(),
                content: vec![0, 255, 7],
            },
            subject: TableRef::new("", ""),
            identity: None,
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["kind"], "message");
        assert_eq!(json["content"], "AP8H");
        let back: ControlEvent = serde_json::from_value(json).unwrap();
        assert_eq!(back, event);
    }

    #[test]
    fn control_cloud_event_uses_database_subject_for_database_drop() {
        let ev = ControlEvent {
            kind: ControlKind::DatabaseDropped,
            subject: TableRef::new("app", ""),
            identity: None,
        }
        .to_cloud_event("mongo://test", serde_json::Map::new());
        assert_eq!(ev.ty, "io.ce-stream.control.database_dropped");
        assert_eq!(ev.subject, "app");
        assert_eq!(ev.data["kind"], "database_dropped");
    }
}
