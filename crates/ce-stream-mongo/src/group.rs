//! Group change-stream events into commits.
//!
//! A multi-document transaction has no commit marker. Consecutive events with the same
//! `(lsid, txnNumber)` are one commit. The group closes when the next event is a different
//! transaction, or when an empty batch reports a resume token (the batch has ended).
//! A non-transactional write is its own commit.

use ce_stream_core::event::CloudEvent;
use ce_stream_core::include::IncludeFilter;
use ce_stream_core::transaction::{
    CommittedTransaction, ControlEvent, ControlKind, SourcePosition,
};
use serde_json::Value;

use crate::checkpoint::{ClusterTime, MongoCheckpoint};
use crate::map::{Effect, Incoming};

#[derive(Clone, Debug)]
pub struct Emit {
    pub txn: CommittedTransaction,
    /// drop / rename of a watched collection, dropDatabase, or invalidate.
    pub stop: bool,
}

pub struct TxnGrouper {
    seed_cluster_time: Option<ClusterTime>,
    open: Option<OpenGroup>,
    /// The last position handed out, so an idle stream reports a new one only when it moved.
    last_after: Option<Value>,
}

struct OpenGroup {
    key: String,
    filter: IncludeFilter,
    events: Vec<CloudEvent>,
    control: Vec<ControlEvent>,
    first_token: Value,
    first_time: Option<ClusterTime>,
    last_token: Value,
    last_time: Option<ClusterTime>,
    stop: bool,
}

impl TxnGrouper {
    pub fn new(seed_cluster_time: Option<ClusterTime>) -> Self {
        Self {
            seed_cluster_time,
            open: None,
            last_after: None,
        }
    }

    pub fn push(&mut self, incoming: Incoming, filter: &IncludeFilter) -> Vec<Emit> {
        let Some(key) = incoming.txn_key.clone() else {
            let mut out = self.flush(None);
            out.push(self.single(incoming, filter));
            return out;
        };
        if self.open.as_ref().is_some_and(|g| g.key != key) {
            let out = self.flush(None);
            self.start(key, incoming, filter);
            out
        } else {
            if self.open.is_none() {
                self.start(key, incoming, filter);
            } else {
                self.append(incoming);
            }
            Vec::new()
        }
    }

    /// An empty `next_if_any` batch. Closes an open transaction. With none open, a token that
    /// has moved is handed out as a commit with no events: the server-side filter leaves out
    /// the writes of other collections, and the position has to pass them all the same, or a
    /// restart would read from a place the oplog may no longer hold.
    pub fn on_batch_boundary(&mut self, post_batch_token: Option<Value>) -> Vec<Emit> {
        if self.open.is_some() {
            return self.flush(post_batch_token);
        }
        let Some(token) = post_batch_token else {
            return Vec::new();
        };
        if self.last_after.as_ref() == Some(&token) {
            return Vec::new();
        }
        let time = ClusterTime::of_resume_token(&token);
        let group = OpenGroup {
            key: String::new(),
            filter: IncludeFilter::All,
            events: Vec::new(),
            control: Vec::new(),
            first_token: token.clone(),
            first_time: time,
            last_token: token,
            last_time: time,
            stop: false,
        };
        vec![self.finish(group, None)]
    }

    fn start(&mut self, key: String, incoming: Incoming, filter: &IncludeFilter) {
        let token = incoming.resume_token.clone();
        let time = incoming.cluster_time;
        let mut group = OpenGroup {
            key,
            filter: filter.clone(),
            events: Vec::new(),
            control: Vec::new(),
            first_token: token.clone(),
            first_time: time,
            last_token: token,
            last_time: time,
            stop: false,
        };
        group.apply(incoming);
        self.open = Some(group);
    }

    fn append(&mut self, incoming: Incoming) {
        let Some(group) = self.open.as_mut() else {
            return;
        };
        group.last_token = incoming.resume_token.clone();
        group.last_time = incoming.cluster_time;
        group.apply(incoming);
    }

    fn single(&mut self, incoming: Incoming, filter: &IncludeFilter) -> Emit {
        let mut group = OpenGroup {
            key: String::new(),
            filter: filter.clone(),
            events: Vec::new(),
            control: Vec::new(),
            first_token: incoming.resume_token.clone(),
            first_time: incoming.cluster_time,
            last_token: incoming.resume_token.clone(),
            last_time: incoming.cluster_time,
            stop: false,
        };
        group.apply(incoming);
        self.finish(group, None)
    }

    fn flush(&mut self, after_token: Option<Value>) -> Vec<Emit> {
        match self.open.take() {
            Some(group) => vec![self.finish(group, after_token)],
            None => Vec::new(),
        }
    }

    fn finish(&mut self, group: OpenGroup, after_token: Option<Value>) -> Emit {
        let after_token = after_token.unwrap_or(group.last_token.clone());
        let after_time = group.last_time;
        self.last_after = Some(after_token.clone());
        let position = SourcePosition {
            adapter: crate::checkpoint::ADAPTER.into(),
            at: MongoCheckpoint {
                resume_token: group.first_token,
                cluster_time: group.first_time,
                seed_cluster_time: self.seed_cluster_time,
            }
            .to_checkpoint()
            .payload,
            after: MongoCheckpoint {
                resume_token: after_token,
                cluster_time: after_time,
                seed_cluster_time: self.seed_cluster_time,
            }
            .to_checkpoint()
            .payload,
        };
        Emit {
            stop: group.stop,
            txn: CommittedTransaction {
                position: Some(position),
                events: group.events,
                control: group.control,
                ..Default::default()
            },
        }
    }
}

impl OpenGroup {
    fn apply(&mut self, incoming: Incoming) {
        match filter_effect(incoming.effect, &self.filter) {
            Effect::Row(ev) => self.events.push(ev),
            Effect::Control(ev) => {
                self.stop = true;
                self.control.push(ev);
            }
            Effect::Filtered => {}
        }
    }
}

fn filter_effect(effect: Effect, filter: &IncludeFilter) -> Effect {
    match effect {
        Effect::Row(ev) => {
            if filter.row_allowed(&ev.subject) {
                Effect::Row(ev)
            } else {
                Effect::Filtered
            }
        }
        Effect::Control(ev) => match ev.kind {
            ControlKind::Dropped | ControlKind::Renamed => {
                if ev.subject.table.is_empty() || filter.row_allowed(&ev.subject.as_subject()) {
                    Effect::Control(ev)
                } else {
                    Effect::Filtered
                }
            }
            ControlKind::DatabaseDropped | ControlKind::Invalidated => Effect::Control(ev),
        },
        Effect::Filtered => Effect::Filtered,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::map::{map_change, ChangeOpKind, RawChange};
    use ce_stream_core::event::{PayloadMode, TableRef};
    use ce_stream_core::include::IncludeList;
    use serde_json::json;

    fn raw(op: ChangeOpKind, token: &str, txn: Option<&str>, coll: &str) -> Incoming {
        map_change(
            RawChange {
                op,
                db: "app".into(),
                coll: Some(coll.into()),
                to_db: None,
                to_coll: None,
                document_key: Some(json!({"_id": {"$oid": "aabbccddeeff001122334455"}})),
                full_document: Some(json!({"_id": 1})),
                txn_key: txn.map(str::to_string),
                resume_token: json!({"_data": token}),
                cluster_time: Some(ClusterTime { t: 1, i: 1 }),
            },
            "mongo://t",
            PayloadMode::Full,
        )
    }

    fn orders() -> IncludeFilter {
        IncludeList::from_tables([TableRef::new("app", "orders")]).snapshot_filter()
    }

    #[test]
    fn transaction_waits_for_a_different_key() {
        let mut g = TxnGrouper::new(None);
        let filter = orders();
        assert!(g
            .push(
                raw(ChangeOpKind::Insert, "a", Some("s:1"), "orders"),
                &filter
            )
            .is_empty());
        assert!(g
            .push(
                raw(ChangeOpKind::Update, "b", Some("s:1"), "orders"),
                &filter
            )
            .is_empty());
        let out = g.push(
            raw(ChangeOpKind::Insert, "c", Some("s:2"), "orders"),
            &filter,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].txn.events.len(), 2);
        assert_eq!(
            out[0].txn.position.as_ref().unwrap().at["resume_token"]["_data"],
            "a"
        );
        assert_eq!(
            out[0].txn.position.as_ref().unwrap().after["resume_token"]["_data"],
            "b"
        );
        let tail = g.on_batch_boundary(Some(json!({"_data": "post"})));
        assert_eq!(tail[0].txn.events.len(), 1);
        assert_eq!(
            tail[0].txn.position.as_ref().unwrap().after["resume_token"]["_data"],
            "post"
        );
    }

    #[test]
    fn empty_batch_closes_the_open_group() {
        let mut g = TxnGrouper::new(Some(ClusterTime { t: 4, i: 0 }));
        let filter = orders();
        g.push(
            raw(ChangeOpKind::Insert, "a", Some("s:1"), "orders"),
            &filter,
        );
        let out = g.on_batch_boundary(Some(json!({"_data": "post"})));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].txn.events.len(), 1);
        assert_eq!(
            out[0].txn.position.as_ref().unwrap().after["seed_cluster_time"],
            json!({"t": 4, "i": 0})
        );
        assert!(g.on_batch_boundary(None).is_empty());
    }

    #[test]
    fn an_idle_batch_hands_out_a_position_that_moved_and_only_once() {
        let mut g = TxnGrouper::new(Some(ClusterTime { t: 4, i: 0 }));
        g.push(raw(ChangeOpKind::Insert, "a", None, "orders"), &orders());
        // The same place as the last commit: nothing new to say.
        assert!(g.on_batch_boundary(Some(json!({"_data": "a"}))).is_empty());
        let moved = json!({"_data": "826AC91BAF00000004"});
        let out = g.on_batch_boundary(Some(moved.clone()));
        assert_eq!(out.len(), 1);
        assert!(out[0].txn.events.is_empty());
        assert!(!out[0].stop);
        let position = out[0].txn.position.as_ref().unwrap();
        assert_eq!(position.after["resume_token"], moved);
        assert_eq!(
            position.after["cluster_time"],
            json!({"t": 0x6AC9_1BAFu32, "i": 4})
        );
        assert_eq!(position.after["seed_cluster_time"], json!({"t": 4, "i": 0}));
        assert!(g.on_batch_boundary(Some(moved)).is_empty());
        assert!(g.on_batch_boundary(None).is_empty());
    }

    #[test]
    fn non_transactional_write_is_its_own_commit() {
        let mut g = TxnGrouper::new(None);
        let out = g.push(raw(ChangeOpKind::Insert, "a", None, "orders"), &orders());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].txn.events.len(), 1);
        assert!(out[0].txn.gtid.is_empty());
    }

    #[test]
    fn unwatched_collection_advances_token_without_rows() {
        let mut g = TxnGrouper::new(None);
        let out = g.push(raw(ChangeOpKind::Insert, "a", None, "other"), &orders());
        assert!(out[0].txn.events.is_empty());
        assert!(!out[0].stop);
        assert_eq!(
            out[0].txn.position.as_ref().unwrap().after["resume_token"]["_data"],
            "a"
        );
    }

    #[test]
    fn watched_drop_stops_unwatched_drop_does_not() {
        let mut g = TxnGrouper::new(None);
        let skip = g.push(raw(ChangeOpKind::Drop, "a", None, "other"), &orders());
        assert!(!skip[0].stop);
        assert!(skip[0].txn.control.is_empty());
        let stop = g.push(raw(ChangeOpKind::Drop, "b", None, "orders"), &orders());
        assert!(stop[0].stop);
        assert_eq!(stop[0].txn.control.len(), 1);
    }

    #[test]
    fn include_is_pinned_at_group_start() {
        let mut g = TxnGrouper::new(None);
        let orders = orders();
        let all = IncludeFilter::All;
        g.push(
            raw(ChangeOpKind::Insert, "a", Some("s:1"), "orders"),
            &orders,
        );
        g.push(raw(ChangeOpKind::Insert, "b", Some("s:1"), "noise"), &all);
        let out = g.on_batch_boundary(None);
        assert_eq!(out[0].txn.events.len(), 1);
        assert_eq!(out[0].txn.events[0].subject, "app.orders");
    }
}
