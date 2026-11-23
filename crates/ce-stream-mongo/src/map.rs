//! Change-stream events to row CloudEvents or control events.
//!
//! The stream is opened on the whole database. A finite include list is also a server-side
//! `$match` (see `source.rs`), which is widened by reopening the stream when the list gains a
//! collection. The filter in [`crate::group`] stays: it pins the list for one transaction and
//! drops what a list that has shrunk since the stream was opened still lets through.

use ce_stream_core::event::{ChangeOp, CloudEvent, PayloadMode, TableRef};
use ce_stream_core::transaction::{ControlEvent, ControlKind};
use serde_json::{json, Value};

use crate::bson_json::Image;
use crate::checkpoint::ClusterTime;

#[derive(Clone, Debug, PartialEq)]
pub struct RawChange {
    pub op: ChangeOpKind,
    pub db: String,
    pub coll: Option<String>,
    pub to_db: Option<String>,
    pub to_coll: Option<String>,
    pub document_key: Option<Image>,
    pub full_document: Option<Image>,
    pub txn_key: Option<String>,
    pub resume_token: Value,
    pub cluster_time: Option<ClusterTime>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeOpKind {
    Insert,
    Update,
    Replace,
    Delete,
    Drop,
    Rename,
    DropDatabase,
    Invalidate,
    /// create, and event types we do not apply.
    Other,
}

#[derive(Clone, Debug)]
pub struct Incoming {
    pub txn_key: Option<String>,
    pub resume_token: Value,
    pub cluster_time: Option<ClusterTime>,
    pub effect: Effect,
}

#[derive(Clone, Debug)]
pub enum Effect {
    Row(CloudEvent),
    Control(ControlEvent),
    /// Not applied. The grouper still advances the resume token.
    Filtered,
}

pub fn map_change(raw: RawChange, source_id: &str, mode: PayloadMode) -> Incoming {
    let effect = match raw.op {
        ChangeOpKind::Insert | ChangeOpKind::Update | ChangeOpKind::Replace => {
            row_effect(&raw, source_id, mode)
        }
        ChangeOpKind::Delete => delete_effect(&raw, source_id, mode),
        ChangeOpKind::Drop => control_effect(&raw, ControlKind::Dropped),
        ChangeOpKind::Rename => {
            let to = TableRef::new(
                raw.to_db.clone().unwrap_or_else(|| raw.db.clone()),
                raw.to_coll.clone().unwrap_or_default(),
            );
            control_effect(&raw, ControlKind::Renamed { to })
        }
        ChangeOpKind::DropDatabase => Effect::Control(ControlEvent {
            kind: ControlKind::DatabaseDropped,
            subject: TableRef::new(&raw.db, ""),
            identity: None,
        }),
        ChangeOpKind::Invalidate => Effect::Control(ControlEvent {
            kind: ControlKind::Invalidated,
            subject: TableRef::new(&raw.db, raw.coll.clone().unwrap_or_default()),
            identity: None,
        }),
        ChangeOpKind::Other => Effect::Filtered,
    };
    Incoming {
        txn_key: raw.txn_key,
        resume_token: raw.resume_token,
        cluster_time: raw.cluster_time,
        effect,
    }
}

fn row_effect(raw: &RawChange, source_id: &str, mode: PayloadMode) -> Effect {
    let Some(coll) = raw.coll.as_deref() else {
        return Effect::Filtered;
    };
    if coll.starts_with("system.") {
        return Effect::Filtered;
    }
    let table = TableRef::new(&raw.db, coll);
    let op = match raw.op {
        ChangeOpKind::Insert => ChangeOp::Insert,
        ChangeOpKind::Update | ChangeOpKind::Replace => ChangeOp::Update,
        _ => return Effect::Filtered,
    };
    let data = match mode {
        PayloadMode::Signal => json!({"op": op.as_str(), "signal": true}),
        PayloadMode::Full => {
            let Some(after) = raw.full_document.as_ref() else {
                // updateLookup can miss. A later delete converges. Required mode errors on the server.
                return Effect::Filtered;
            };
            after.row_data(op.as_str(), "after")
        }
    };
    Effect::Row(CloudEvent::row_change(
        source_id,
        &table,
        op,
        data,
        extensions(raw),
    ))
}

fn delete_effect(raw: &RawChange, source_id: &str, mode: PayloadMode) -> Effect {
    let Some(coll) = raw.coll.as_deref() else {
        return Effect::Filtered;
    };
    let table = TableRef::new(&raw.db, coll);
    let data = match mode {
        PayloadMode::Signal => json!({"op": "delete", "signal": true}),
        PayloadMode::Full => {
            let Some(key) = raw.document_key.as_ref() else {
                return Effect::Filtered;
            };
            key.row_data("delete", "before")
        }
    };
    Effect::Row(CloudEvent::row_change(
        source_id,
        &table,
        ChangeOp::Delete,
        data,
        extensions(raw),
    ))
}

fn control_effect(raw: &RawChange, kind: ControlKind) -> Effect {
    let Some(coll) = raw.coll.clone() else {
        return Effect::Filtered;
    };
    Effect::Control(ControlEvent {
        kind,
        subject: TableRef::new(&raw.db, coll),
        identity: None,
    })
}

fn extensions(raw: &RawChange) -> serde_json::Map<String, Value> {
    let mut ext = serde_json::Map::new();
    ext.insert("resumeToken".into(), raw.resume_token.clone());
    if let Some(ts) = raw.cluster_time {
        ext.insert("clusterTime".into(), json!({"t": ts.t, "i": ts.i}));
    }
    ext
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(op: ChangeOpKind) -> RawChange {
        RawChange {
            op,
            db: "app".into(),
            coll: Some("orders".into()),
            to_db: None,
            to_coll: None,
            document_key: Some(Image {
                value: json!({"_id": "aabbccddeeff001122334455"}),
                types: Some(json!({"_id": "objectId"})),
            }),
            full_document: Some(Image {
                value: json!({"_id": "aabbccddeeff001122334455", "n": 1}),
                types: Some(json!({"_id": "objectId"})),
            }),
            txn_key: None,
            resume_token: json!({"_data": "t1"}),
            cluster_time: Some(ClusterTime { t: 9, i: 1 }),
        }
    }

    #[test]
    fn insert_update_replace_and_delete() {
        let insert = map_change(base(ChangeOpKind::Insert), "mongo://t", PayloadMode::Full);
        let Effect::Row(ev) = insert.effect else {
            panic!("row")
        };
        assert_eq!(ev.ty, "io.ce-stream.row.inserted");
        assert_eq!(ev.subject, "app.orders");
        assert_eq!(ev.data["after"]["n"], 1);
        assert_eq!(ev.data["types"]["_id"], "objectId");

        let replace = map_change(base(ChangeOpKind::Replace), "mongo://t", PayloadMode::Full);
        let Effect::Row(ev) = replace.effect else {
            panic!("row")
        };
        assert_eq!(ev.ty, "io.ce-stream.row.updated");

        let mut update = base(ChangeOpKind::Update);
        update.full_document = None;
        assert!(matches!(
            map_change(update, "mongo://t", PayloadMode::Full).effect,
            Effect::Filtered
        ));

        let del = map_change(base(ChangeOpKind::Delete), "mongo://t", PayloadMode::Full);
        let Effect::Row(ev) = del.effect else {
            panic!("row")
        };
        assert_eq!(ev.ty, "io.ce-stream.row.deleted");
        assert!(ev.data.get("after").is_none());
        assert_eq!(ev.data["before"]["_id"], "aabbccddeeff001122334455");
        assert_eq!(ev.data["types"]["_id"], "objectId");
    }

    #[test]
    fn rename_and_database_drop() {
        let mut raw = base(ChangeOpKind::Rename);
        raw.to_db = Some("app".into());
        raw.to_coll = Some("orders_old".into());
        let mapped = map_change(raw, "mongo://t", PayloadMode::Full);
        let Effect::Control(c) = mapped.effect else {
            panic!("control")
        };
        assert_eq!(
            c.kind,
            ControlKind::Renamed {
                to: TableRef::new("app", "orders_old")
            }
        );

        let mut drop_db = base(ChangeOpKind::DropDatabase);
        drop_db.coll = None;
        let mapped = map_change(drop_db, "mongo://t", PayloadMode::Full);
        let Effect::Control(c) = mapped.effect else {
            panic!("control")
        };
        assert_eq!(c.kind, ControlKind::DatabaseDropped);
        assert!(c.subject.table.is_empty());
    }

    #[test]
    fn system_collections_are_ignored() {
        let mut raw = base(ChangeOpKind::Insert);
        raw.coll = Some("system.views".into());
        assert!(matches!(
            map_change(raw, "mongo://t", PayloadMode::Full).effect,
            Effect::Filtered
        ));
    }
}
