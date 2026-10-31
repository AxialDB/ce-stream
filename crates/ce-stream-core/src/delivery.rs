//! What a source hands its consumer (issue #19): commits, progress, and table
//! definitions, and how the consumer says what it has made durable.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::error::{Error, Result};
use crate::event::{CloudEvent, TableRef};
use crate::transaction::{CommittedTransaction, SourcePosition};

/// CloudEvent type of a [`Delivery::Schema`] in row mode.
pub const SCHEMA_CE_TYPE: &str = "io.ce-stream.schema";

/// One thing a source delivers. Commits of one stream arrive in commit order;
/// `Progress` and `Schema` are in order with them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Delivery {
    /// One committed source transaction, or one part of it (#16).
    Commit(CommittedTransaction),
    /// The stream moved to `position` and nothing in it was for this consumer.
    /// `after` is the resume payload, the same shape as a commit's.
    Progress { position: SourcePosition },
    /// The definition of a table. May come between two parts of a transaction;
    /// it carries no position and moves nothing.
    Schema(TableSchema),
}

/// One column of a [`TableSchema`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    /// The adapter's rendering of the host's type, such as `numeric(10,2)`.
    pub host_type: String,
    /// `Some` only where the host says it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    /// Part of the key the host sends old rows by.
    pub key: bool,
}

/// The definition of a table as the host announces it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub subject: TableRef,
    /// An identity that survives a rename, where the host has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    /// Row-mode CloudEvent for this definition.
    pub fn to_cloud_event(&self, source: impl Into<String>) -> CloudEvent {
        CloudEvent {
            specversion: "1.0".into(),
            id: uuid::Uuid::new_v4().to_string(),
            ty: SCHEMA_CE_TYPE.into(),
            source: source.into(),
            subject: self.subject.as_subject(),
            time: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            extensions: serde_json::Map::new(),
            data: serde_json::to_value(self).unwrap_or(serde_json::Value::Null),
        }
    }
}

/// When the checkpoint moves.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointMode {
    /// When the callback returns `Ok`: the callback's `Ok` is the confirmation.
    #[default]
    OnOk,
    /// When the consumer calls [`Confirm::confirm`], for a consumer that makes
    /// a delivery durable after its callback has returned.
    OnConfirm,
}

/// Does a position with nothing after it end a transaction? A part that is not
/// the last carries an empty `after`.
pub fn has_resume_payload(position: &SourcePosition) -> bool {
    match &position.after {
        serde_json::Value::Null => false,
        serde_json::Value::Object(map) => !map.is_empty(),
        _ => true,
    }
}

type CoversFn = dyn Fn(&SourcePosition, &SourcePosition) -> Result<bool> + Send + Sync;

/// The consumer's word on what is durable, under [`CheckpointMode::OnConfirm`].
/// Clone it freely; every clone moves the same position.
#[derive(Clone)]
pub struct Confirm {
    adapter: String,
    latest: Arc<watch::Sender<Option<SourcePosition>>>,
    covers: Arc<CoversFn>,
}

impl std::fmt::Debug for Confirm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Confirm")
            .field("adapter", &self.adapter)
            .field("latest", &*self.latest.borrow())
            .finish()
    }
}

impl Confirm {
    /// For adapters. `covers` is the adapter's [`crate::ChangeSource::covers`].
    pub fn new(
        adapter: impl Into<String>,
        covers: impl Fn(&SourcePosition, &SourcePosition) -> Result<bool> + Send + Sync + 'static,
    ) -> Self {
        let (latest, _) = watch::channel(None);
        Self {
            adapter: adapter.into(),
            latest: Arc::new(latest),
            covers: Arc::new(covers),
        }
    }

    /// Move the checkpoint, and whatever the server retains, to this position.
    /// A position behind the last confirmed one is a no-op. A part that is not
    /// the last cannot be confirmed: it has no position after it.
    pub fn confirm(&self, position: &SourcePosition) -> Result<()> {
        if position.adapter != self.adapter {
            return Err(Error::Source(format!(
                "confirm: a {} position on a {} source",
                position.adapter, self.adapter
            )));
        }
        if !has_resume_payload(position) {
            return Err(Error::Source(
                "confirm: a part that is not the last has no position after it".into(),
            ));
        }
        let mut failed = None;
        self.latest.send_if_modified(|latest| {
            let behind = match latest.as_ref() {
                Some(current) => match (self.covers)(current, position) {
                    Ok(covered) => covered,
                    Err(err) => {
                        failed = Some(err);
                        return false;
                    }
                },
                None => false,
            };
            if behind {
                return false;
            }
            *latest = Some(position.clone());
            true
        });
        match failed {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// The last position confirmed.
    pub fn latest(&self) -> Option<SourcePosition> {
        self.latest.borrow().clone()
    }

    /// For adapters: wakes on each position that moves the checkpoint.
    pub fn subscribe(&self) -> watch::Receiver<Option<SourcePosition>> {
        self.latest.subscribe()
    }
}

/// The cut for a seed, on a connection the consumer supplies to the primary,
/// under the consumer's own account: never the stream's connection or account.
/// The copy itself is the consumer's; ce-stream never reads source rows.
#[async_trait]
pub trait SeedCut: Send + Sync {
    /// A position such that every transaction committed before the call is at
    /// or before it.
    async fn mark(&self) -> Result<SourcePosition>;

    /// Returns when every transaction in flight at `mark` has ended, so that a
    /// snapshot taken afterwards contains all of them.
    async fn wait_settled(&self, mark: &SourcePosition) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn at(n: u64) -> SourcePosition {
        SourcePosition {
            adapter: "t".into(),
            at: json!({ "n": n }),
            after: json!({ "n": n }),
        }
    }

    fn confirm() -> Confirm {
        Confirm::new("t", |current, target| {
            Ok(target.at["n"].as_u64() <= current.after["n"].as_u64())
        })
    }

    #[test]
    fn confirm_moves_forward_and_ignores_what_is_behind() {
        let handle = confirm();
        let other = handle.clone();
        handle.confirm(&at(5)).unwrap();
        other.confirm(&at(3)).unwrap();
        assert_eq!(handle.latest().unwrap().after["n"], 5);
        other.confirm(&at(6)).unwrap();
        assert_eq!(handle.latest().unwrap().after["n"], 6);
    }

    #[test]
    fn a_part_that_is_not_the_last_cannot_be_confirmed() {
        let mut part = at(5);
        part.after = json!({});
        let err = confirm().confirm(&part).unwrap_err();
        assert!(err.to_string().contains("part"));
    }

    #[test]
    fn another_adapters_position_is_refused() {
        let mut other = at(5);
        other.adapter = "mysql".into();
        assert!(confirm().confirm(&other).is_err());
    }

    #[tokio::test]
    async fn a_subscriber_wakes_only_when_the_position_moves() {
        let handle = confirm();
        let mut rx = handle.subscribe();
        handle.confirm(&at(5)).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(rx.borrow_and_update().as_ref().unwrap().after["n"], 5);
        handle.confirm(&at(4)).unwrap();
        assert!(!rx.has_changed().unwrap());
    }

    #[test]
    fn a_schema_has_a_cloud_event_of_its_own() {
        let schema = TableSchema {
            subject: TableRef::new("db", "t"),
            identity: Some("16420".into()),
            columns: vec![ColumnSchema {
                name: "id".into(),
                host_type: "integer".into(),
                nullable: None,
                key: true,
            }],
        };
        let ev = schema.to_cloud_event("s");
        assert_eq!(ev.ty, SCHEMA_CE_TYPE);
        assert_eq!(ev.subject, "db.t");
        assert_eq!(ev.data["columns"][0]["host_type"], "integer");
    }

    #[test]
    fn a_delivery_roundtrips_json() {
        let json = serde_json::to_string(&Delivery::Progress { position: at(2) }).unwrap();
        let Delivery::Progress { position } = serde_json::from_str(&json).unwrap() else {
            panic!("expected progress");
        };
        assert_eq!(position, at(2));
    }
}
