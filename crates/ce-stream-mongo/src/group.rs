//! Group change-stream events into commits.
//!
//! A multi-document transaction has no commit marker. Consecutive events with the same
//! `(lsid, txnNumber)` are one commit. The group closes when the next event is a different
//! transaction, or when an empty batch reports a resume token (the batch has ended).
//! A non-transactional write is its own commit.
//!
//! A transaction larger than one part (`part_max_rows` rows or `part_max_bytes` bytes of
//! row data) is handed out in parts as it is read, so that no more than one part is held.
//! A full part goes out when the next event of its transaction comes: a transaction of
//! exactly one part is handed out whole.
//! Every part carries the position of the transaction's first event; only the last part
//! carries the position after the transaction, and only it is a commit.

use ce_stream_core::event::{approx_json_bytes, CloudEvent};
use ce_stream_core::include::IncludeFilter;
use ce_stream_core::source::{DEFAULT_PART_MAX_BYTES, DEFAULT_PART_MAX_ROWS};
use ce_stream_core::transaction::{
    CommittedTransaction, ControlEvent, ControlKind, SourcePosition, TransactionPart,
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
    part_max_rows: usize,
    part_max_bytes: usize,
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
    /// Rows and bytes of row data held since the last part was handed out.
    part_rows: usize,
    part_bytes: usize,
    parts_sent: u32,
}

impl TxnGrouper {
    pub fn new(seed_cluster_time: Option<ClusterTime>) -> Self {
        Self {
            seed_cluster_time,
            open: None,
            last_after: None,
            part_max_rows: DEFAULT_PART_MAX_ROWS,
            part_max_bytes: DEFAULT_PART_MAX_BYTES,
        }
    }

    /// The size of a part. 0 keeps the default of that limit.
    pub fn with_part_limits(mut self, max_rows: usize, max_bytes: usize) -> Self {
        if max_rows > 0 {
            self.part_max_rows = max_rows;
        }
        if max_bytes > 0 {
            self.part_max_bytes = max_bytes;
        }
        self
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
        } else if self.open.is_none() {
            self.start(key, incoming, filter);
            Vec::new()
        } else {
            let out: Vec<Emit> = self.take_part_if_full().into_iter().collect();
            self.append(incoming);
            out
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
        let group = OpenGroup::new(String::new(), IncludeFilter::All, token, time);
        vec![self.finish(group, None)]
    }

    fn start(&mut self, key: String, incoming: Incoming, filter: &IncludeFilter) {
        let mut group = OpenGroup::new(
            key,
            filter.clone(),
            incoming.resume_token.clone(),
            incoming.cluster_time,
        );
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
        let mut group = OpenGroup::new(
            String::new(),
            filter.clone(),
            incoming.resume_token.clone(),
            incoming.cluster_time,
        );
        group.apply(incoming);
        self.finish(group, None)
    }

    fn flush(&mut self, after_token: Option<Value>) -> Vec<Emit> {
        match self.open.take() {
            Some(group) => vec![self.finish(group, after_token)],
            None => Vec::new(),
        }
    }

    /// The rows of the open transaction fill a part: hand them out before its next event.
    /// A part that is not the last moves nothing: its position after is empty, and the
    /// position an idle stream compares against stays where it was. Control events stay
    /// with the group and go out with the last part, which carries the stop.
    fn take_part_if_full(&mut self) -> Option<Emit> {
        let (max_rows, max_bytes) = (self.part_max_rows, self.part_max_bytes);
        let seed_cluster_time = self.seed_cluster_time;
        let group = self.open.as_mut()?;
        let full =
            group.part_rows >= max_rows || (group.part_rows > 0 && group.part_bytes >= max_bytes);
        if !full {
            return None;
        }
        group.parts_sent += 1;
        group.part_rows = 0;
        group.part_bytes = 0;
        let position = SourcePosition {
            adapter: crate::checkpoint::ADAPTER.into(),
            at: MongoCheckpoint {
                resume_token: group.first_token.clone(),
                cluster_time: group.first_time,
                seed_cluster_time,
            }
            .to_checkpoint()
            .payload,
            after: Value::Object(serde_json::Map::new()),
        };
        Some(Emit {
            stop: false,
            txn: CommittedTransaction {
                part: Some(TransactionPart {
                    number: group.parts_sent,
                    last: false,
                }),
                position: Some(position),
                events: std::mem::take(&mut group.events),
                ..Default::default()
            },
        })
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
        let part = (group.parts_sent > 0).then_some(TransactionPart {
            number: group.parts_sent + 1,
            last: true,
        });
        Emit {
            stop: group.stop,
            txn: CommittedTransaction {
                part,
                position: Some(position),
                events: group.events,
                control: group.control,
                ..Default::default()
            },
        }
    }
}

impl OpenGroup {
    fn new(key: String, filter: IncludeFilter, token: Value, time: Option<ClusterTime>) -> Self {
        Self {
            key,
            filter,
            events: Vec::new(),
            control: Vec::new(),
            first_token: token.clone(),
            first_time: time,
            last_token: token,
            last_time: time,
            stop: false,
            part_rows: 0,
            part_bytes: 0,
            parts_sent: 0,
        }
    }

    fn apply(&mut self, incoming: Incoming) {
        match filter_effect(incoming.effect, &self.filter) {
            Effect::Row(ev) => {
                self.part_rows += 1;
                self.part_bytes = self.part_bytes.saturating_add(approx_json_bytes(&ev.data));
                self.events.push(ev);
            }
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
            ControlKind::Dropped | ControlKind::Renamed { .. } => {
                if ev.subject.table.is_empty() || filter.row_allowed(&ev.subject.as_subject()) {
                    Effect::Control(ev)
                } else {
                    Effect::Filtered
                }
            }
            _ => Effect::Control(ev),
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

    fn after_token(emit: &Emit) -> &Value {
        &emit.txn.position.as_ref().unwrap().after["resume_token"]["_data"]
    }

    #[test]
    fn a_long_transaction_is_cut_into_parts_by_rows() {
        let mut g = TxnGrouper::new(None).with_part_limits(2, 1_000_000);
        let filter = orders();
        let mut parts = Vec::new();
        for token in ["a", "b", "c", "d", "e"] {
            parts.extend(g.push(
                raw(ChangeOpKind::Update, token, Some("s:1"), "orders"),
                &filter,
            ));
        }
        assert_eq!(parts.len(), 2, "two full parts while reading");
        for (i, part) in parts.iter().enumerate() {
            assert_eq!(
                part.txn.part,
                Some(TransactionPart {
                    number: i as u32 + 1,
                    last: false
                })
            );
            assert!(!part.txn.ends_transaction());
            assert_eq!(part.txn.events.len(), 2);
            let position = part.txn.position.as_ref().unwrap();
            assert_eq!(position.at["resume_token"]["_data"], "a");
            assert_eq!(position.after, json!({}), "a part has no position after");
            assert!(!part.stop);
        }
        // The batch ends: the last part, with the position after the transaction.
        let last = g.on_batch_boundary(Some(json!({"_data": "post"})));
        assert_eq!(last.len(), 1);
        assert_eq!(
            last[0].txn.part,
            Some(TransactionPart {
                number: 3,
                last: true
            })
        );
        assert!(last[0].txn.ends_transaction());
        assert_eq!(last[0].txn.events.len(), 1);
        assert_eq!(after_token(&last[0]), "post");
        assert_eq!(
            last[0].txn.position.as_ref().unwrap().at["resume_token"]["_data"],
            "a"
        );
    }

    #[test]
    fn a_part_also_ends_at_its_size_in_bytes() {
        // Every row is larger than a part of one byte: a part of its own.
        let mut g = TxnGrouper::new(None).with_part_limits(1_000, 1);
        let filter = orders();
        let first = g.push(
            raw(ChangeOpKind::Insert, "a", Some("s:1"), "orders"),
            &filter,
        );
        assert!(first.is_empty(), "a part goes out when the next row comes");
        let second = g.push(
            raw(ChangeOpKind::Insert, "b", Some("s:1"), "orders"),
            &filter,
        );
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].txn.events.len(), 1);
        assert_eq!(second[0].txn.part.unwrap().number, 1);
        // The transaction ends on the next one: its last part holds the second row.
        let out = g.push(
            raw(ChangeOpKind::Insert, "c", Some("s:2"), "orders"),
            &filter,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].txn.part,
            Some(TransactionPart {
                number: 2,
                last: true
            })
        );
        assert_eq!(out[0].txn.events.len(), 1);
        assert_eq!(after_token(&out[0]), "b");
        // The next transaction is open and whole so far.
        let tail = g.on_batch_boundary(None);
        assert!(tail[0].txn.part.is_none());
        assert_eq!(
            tail[0].txn.position.as_ref().unwrap().at["resume_token"]["_data"],
            "c"
        );
    }

    #[test]
    fn a_transaction_of_exactly_one_part_is_whole_and_one_more_row_makes_two() {
        let filter = orders();
        let mut exact = TxnGrouper::new(None).with_part_limits(3, 1_000_000);
        for token in ["a", "b", "c"] {
            assert!(exact
                .push(
                    raw(ChangeOpKind::Insert, token, Some("s:1"), "orders"),
                    &filter
                )
                .is_empty());
        }
        let whole = exact.on_batch_boundary(None);
        assert_eq!(whole.len(), 1);
        assert_eq!(whole[0].txn.events.len(), 3);
        assert!(whole[0].txn.part.is_none());

        let mut more = TxnGrouper::new(None).with_part_limits(3, 1_000_000);
        let mut out = Vec::new();
        for token in ["a", "b", "c", "d"] {
            out.extend(more.push(
                raw(ChangeOpKind::Insert, token, Some("s:1"), "orders"),
                &filter,
            ));
        }
        out.extend(more.on_batch_boundary(None));
        let shape: Vec<(usize, Option<TransactionPart>)> = out
            .iter()
            .map(|e| (e.txn.events.len(), e.txn.part))
            .collect();
        assert_eq!(
            shape,
            [
                (
                    3,
                    Some(TransactionPart {
                        number: 1,
                        last: false
                    })
                ),
                (
                    1,
                    Some(TransactionPart {
                        number: 2,
                        last: true
                    })
                ),
            ]
        );
    }

    #[test]
    fn a_short_transaction_and_a_single_write_stay_whole() {
        let mut g = TxnGrouper::new(None).with_part_limits(3, 1_000_000);
        let filter = orders();
        for token in ["a", "b"] {
            assert!(g
                .push(
                    raw(ChangeOpKind::Insert, token, Some("s:1"), "orders"),
                    &filter
                )
                .is_empty());
        }
        let out = g.on_batch_boundary(None);
        assert_eq!(out[0].txn.events.len(), 2);
        assert!(out[0].txn.part.is_none());
        // A write outside a transaction is never cut, whatever the limits.
        let mut tiny = TxnGrouper::new(None).with_part_limits(1, 1);
        let single = tiny.push(raw(ChangeOpKind::Insert, "z", None, "orders"), &filter);
        assert_eq!(single.len(), 1);
        assert!(single[0].txn.part.is_none());
    }

    #[test]
    fn rows_left_out_by_the_include_list_do_not_fill_a_part() {
        let mut g = TxnGrouper::new(None).with_part_limits(2, 1_000_000);
        let filter = orders();
        for token in ["a", "b", "c"] {
            assert!(g
                .push(
                    raw(ChangeOpKind::Insert, token, Some("s:1"), "noise"),
                    &filter
                )
                .is_empty());
        }
        let out = g.on_batch_boundary(None);
        assert!(out[0].txn.events.is_empty());
        assert!(out[0].txn.part.is_none());
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
