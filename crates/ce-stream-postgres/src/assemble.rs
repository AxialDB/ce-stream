//! From `pgoutput` messages to deliveries. No connection here: the reader feeds
//! it messages and hands on what comes out.

use std::collections::HashMap;

use ce_stream_core::delivery::{ColumnSchema, Delivery, TableSchema};
use ce_stream_core::error::{Error, Result};
use ce_stream_core::event::{approx_json_bytes, ChangeOp, CloudEvent, PayloadMode, TableRef};
use ce_stream_core::include::{IncludeFilter, IncludeList};
use ce_stream_core::source::{DEFAULT_PART_MAX_BYTES, DEFAULT_PART_MAX_ROWS};
use ce_stream_core::transaction::{
    CommittedTransaction, ControlEvent, ControlKind, TransactionPart,
};
use serde_json::{json, Map, Value};

use crate::lsn::Lsn;
use crate::pgoutput::{Message, RelationColumn, Tuple, TupleValue};
use crate::position::{commit_position, message_position, passed};
use crate::values::{host_type, text_to_json};

/// The top of a partition tree a leaf belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionRoot {
    pub(crate) oid: u32,
    pub(crate) table: TableRef,
}

#[derive(Debug)]
struct Relation {
    subject: TableRef,
    replica_identity: u8,
    columns: Vec<RelationColumn>,
    /// `None` until asked. Asked again after each Relation message: `ATTACH
    /// PARTITION` makes the server announce the table again.
    root: Option<Option<PartitionRoot>>,
    /// Its current definition has been delivered on this session.
    announced: bool,
    /// It differs from the definition before it.
    changed: bool,
}

#[derive(Debug)]
struct OpenTxn {
    final_lsn: Lsn,
    xid: u32,
    /// Pinned at Begin: a newer include list starts at the next transaction.
    filter: IncludeFilter,
    events: Vec<CloudEvent>,
    control: Vec<ControlEvent>,
    part_rows: usize,
    part_bytes: usize,
    parts_sent: u32,
}

pub(crate) struct Assembler {
    source_id: String,
    payload_mode: PayloadMode,
    include: IncludeList,
    part_max_rows: usize,
    part_max_bytes: usize,
    message_prefixes: Vec<String>,
    /// A commit that starts before this was delivered before the restart.
    resume: Lsn,
    relations: HashMap<u32, Relation>,
    types: HashMap<u32, String>,
    txn: Option<OpenTxn>,
}

impl Assembler {
    pub(crate) fn new(
        source_id: String,
        payload_mode: PayloadMode,
        include: IncludeList,
        (part_max_rows, part_max_bytes): (usize, usize),
        message_prefixes: Vec<String>,
        resume: Lsn,
    ) -> Self {
        Self {
            source_id,
            payload_mode,
            include,
            part_max_rows: if part_max_rows == 0 {
                DEFAULT_PART_MAX_ROWS
            } else {
                part_max_rows
            },
            part_max_bytes: if part_max_bytes == 0 {
                DEFAULT_PART_MAX_BYTES
            } else {
                part_max_bytes
            },
            message_prefixes,
            resume,
            relations: HashMap::new(),
            types: HashMap::new(),
            txn: None,
        }
    }

    pub(crate) fn in_transaction(&self) -> bool {
        self.txn.is_some()
    }

    /// Relations this message touches whose partition root must be known first.
    /// Asked only under a finite list, for a table not on it by its own name.
    pub(crate) fn roots_needed(&self, message: &Message) -> Vec<u32> {
        let oids: Vec<u32> = match message {
            Message::Insert { relation, .. }
            | Message::Update { relation, .. }
            | Message::Delete { relation, .. } => vec![*relation],
            Message::Truncate { relations } => relations.clone(),
            _ => return Vec::new(),
        };
        let Some(txn) = self.txn.as_ref() else {
            return Vec::new();
        };
        oids.into_iter()
            .filter(|oid| {
                self.relations.get(oid).is_some_and(|rel| {
                    rel.root.is_none() && !txn.filter.row_allowed(&rel.subject.as_subject())
                })
            })
            .collect()
    }

    pub(crate) fn set_root(&mut self, oid: u32, root: Option<PartitionRoot>) {
        if let Some(rel) = self.relations.get_mut(&oid) {
            rel.root = Some(root);
        }
    }

    pub(crate) fn push(&mut self, message: Message) -> Result<Vec<Delivery>> {
        let mut out = Vec::new();
        match message {
            Message::Begin { final_lsn, xid } => {
                if self.txn.is_some() {
                    return Err(Error::Source("pgoutput: Begin inside a transaction".into()));
                }
                self.txn = Some(OpenTxn {
                    final_lsn,
                    xid,
                    filter: self.include.snapshot_filter(),
                    events: Vec::new(),
                    control: Vec::new(),
                    part_rows: 0,
                    part_bytes: 0,
                    parts_sent: 0,
                });
            }
            Message::Commit { lsn, end_lsn } => self.commit(lsn, end_lsn, &mut out)?,
            Message::Origin => {}
            Message::Type {
                oid,
                namespace,
                name,
            } => {
                self.types.insert(oid, format!("{namespace}.{name}"));
            }
            Message::Relation {
                oid,
                namespace,
                name,
                replica_identity,
                columns,
            } => {
                let changed = self.relations.get(&oid).is_some_and(|last| {
                    last.columns != columns || last.replica_identity != replica_identity
                });
                self.relations.insert(
                    oid,
                    Relation {
                        subject: TableRef::new(namespace, name),
                        replica_identity,
                        columns,
                        root: None,
                        announced: false,
                        changed,
                    },
                );
            }
            Message::Insert { relation, new } => {
                self.row(relation, ChangeOp::Insert, None, Some(new), &mut out)?
            }
            Message::Update {
                relation, old, new, ..
            } => self.row(relation, ChangeOp::Update, old, Some(new), &mut out)?,
            Message::Delete { relation, old, .. } => {
                self.row(relation, ChangeOp::Delete, old, None, &mut out)?
            }
            Message::Truncate { relations } => {
                for oid in relations {
                    if let Some(subject) = self.admit(oid, &mut out)? {
                        self.open()?.control.push(ControlEvent {
                            kind: ControlKind::Truncated,
                            subject,
                            identity: Some(oid.to_string()),
                        });
                    }
                }
            }
            Message::Logical {
                transactional,
                lsn,
                prefix,
                content,
            } => {
                if !self.message_prefixes.contains(&prefix) {
                    return Ok(out);
                }
                if transactional {
                    self.open()?.control.push(ControlEvent {
                        kind: ControlKind::Message { prefix, content },
                        subject: TableRef::new("", ""),
                        identity: None,
                    });
                } else {
                    if self.txn.is_some() {
                        return Err(Error::Source(
                            "pgoutput: a non-transactional message inside a transaction".into(),
                        ));
                    }
                    out.push(Delivery::Progress {
                        position: message_position(lsn, &prefix, &content),
                    });
                }
            }
        }
        Ok(out)
    }

    fn open(&mut self) -> Result<&mut OpenTxn> {
        self.txn
            .as_mut()
            .ok_or_else(|| Error::Source("pgoutput: a change outside a transaction".into()))
    }

    /// Is this relation in the transaction's list? If so its definition is out,
    /// and any change of it is among the transaction's controls.
    fn admit(&mut self, oid: u32, out: &mut Vec<Delivery>) -> Result<Option<TableRef>> {
        let filter = &self
            .txn
            .as_ref()
            .ok_or_else(|| Error::Source("pgoutput: a change outside a transaction".into()))?
            .filter;
        let types = &self.types;
        let rel = self.relations.get_mut(&oid).ok_or_else(|| {
            Error::Source(format!("pgoutput: a change for unknown relation {oid}"))
        })?;
        let allowed = filter.row_allowed(&rel.subject.as_subject())
            || rel
                .root
                .as_ref()
                .and_then(Option::as_ref)
                .is_some_and(|root| filter.row_allowed(&root.table.as_subject()));
        if !allowed {
            rel.changed = false;
            return Ok(None);
        }
        let subject = rel.subject.clone();
        if !rel.announced {
            rel.announced = true;
            out.push(Delivery::Schema(TableSchema {
                subject: subject.clone(),
                identity: Some(oid.to_string()),
                columns: rel
                    .columns
                    .iter()
                    .map(|column| ColumnSchema {
                        name: column.name.clone(),
                        host_type: host_type(
                            column.type_oid,
                            column.typmod,
                            types.get(&column.type_oid).map(String::as_str),
                        ),
                        nullable: None,
                        key: column.key,
                    })
                    .collect(),
            }));
            if std::mem::take(&mut rel.changed) {
                self.open()?.control.push(ControlEvent {
                    kind: ControlKind::DefinitionChanged,
                    subject: subject.clone(),
                    identity: Some(oid.to_string()),
                });
            }
        }
        Ok(Some(subject))
    }

    fn row(
        &mut self,
        oid: u32,
        op: ChangeOp,
        old: Option<Tuple>,
        new: Option<Tuple>,
        out: &mut Vec<Delivery>,
    ) -> Result<()> {
        let Some(subject) = self.admit(oid, out)? else {
            return Ok(());
        };
        if op != ChangeOp::Insert && old.is_none() {
            // The table left REPLICA IDENTITY FULL: the old row is not in the WAL.
            // The stream goes on for the other tables.
            let txn = self.open()?;
            let seen = txn.control.iter().any(|control| {
                control.kind == ControlKind::Invalidated
                    && control.identity.as_deref() == Some(oid.to_string().as_str())
            });
            if !seen {
                txn.control.push(ControlEvent {
                    kind: ControlKind::Invalidated,
                    subject,
                    identity: Some(oid.to_string()),
                });
            }
            return Ok(());
        }
        let rel = &self.relations[&oid];
        let data = match self.payload_mode {
            PayloadMode::Signal => json!({ "op": op.as_str(), "signal": true }),
            PayloadMode::Full => {
                let mut data = Map::new();
                data.insert("op".into(), op.as_str().into());
                if let Some(old) = &old {
                    data.insert("before".into(), row_image(rel, old, None)?.into());
                }
                if let Some(new) = &new {
                    data.insert("after".into(), row_image(rel, new, old.as_ref())?.into());
                }
                Value::Object(data)
            }
        };
        let txn = self.txn.as_ref().expect("admit checked it");
        let mut extensions = Map::new();
        extensions.insert("lsn".into(), txn.final_lsn.to_string().into());
        extensions.insert("xid".into(), txn.xid.into());
        extensions.insert("relation_oid".into(), oid.into());
        if let Some(Some(root)) = &rel.root {
            extensions.insert("partition_root".into(), root.table.as_subject().into());
            extensions.insert("partition_root_oid".into(), root.oid.into());
        }
        let bytes = approx_json_bytes(&data);
        let event = CloudEvent::row_change(&self.source_id, &subject, op, data, extensions);
        let (max_rows, max_bytes) = (self.part_max_rows, self.part_max_bytes);
        let txn = self.open()?;
        txn.events.push(event);
        txn.part_rows += 1;
        txn.part_bytes = txn.part_bytes.saturating_add(bytes);
        if txn.part_rows >= max_rows || txn.part_bytes >= max_bytes {
            txn.parts_sent += 1;
            txn.part_rows = 0;
            txn.part_bytes = 0;
            out.push(Delivery::Commit(CommittedTransaction {
                position: Some(commit_position(txn.final_lsn, txn.xid, None)),
                ddl: Vec::new(),
                control: Vec::new(),
                events: std::mem::take(&mut txn.events),
                part: Some(TransactionPart {
                    number: txn.parts_sent,
                    last: false,
                }),
            }));
        }
        Ok(())
    }

    fn commit(&mut self, lsn: Lsn, end_lsn: Lsn, out: &mut Vec<Delivery>) -> Result<()> {
        let txn = self
            .txn
            .take()
            .ok_or_else(|| Error::Source("pgoutput: Commit without Begin".into()))?;
        if txn.final_lsn != lsn {
            return Err(Error::Source(format!(
                "pgoutput: Begin said the commit is at {}, Commit says {lsn}",
                txn.final_lsn
            )));
        }
        if lsn < self.resume {
            return Ok(());
        }
        let position = commit_position(lsn, txn.xid, Some(end_lsn));
        if txn.parts_sent == 0 && txn.events.is_empty() && txn.control.is_empty() {
            // A filtered commit keeps its commit's position.
            out.push(Delivery::Progress { position });
            return Ok(());
        }
        out.push(Delivery::Commit(CommittedTransaction {
            part: (txn.parts_sent > 0).then_some(TransactionPart {
                number: txn.parts_sent + 1,
                last: true,
            }),
            position: Some(position),
            ddl: Vec::new(),
            control: txn.control,
            events: txn.events,
        }));
        Ok(())
    }

    /// The keepalive's position, as a delivery. The reader decides when.
    pub(crate) fn progress_at(&self, wal_end: Lsn) -> Delivery {
        Delivery::Progress {
            position: passed(wal_end),
        }
    }
}

/// One row as the contract says. A `u` (a TOASTed value the update left alone)
/// is filled from the old row; with no old value it is an error, not a partial row.
fn row_image(rel: &Relation, tuple: &Tuple, old: Option<&Tuple>) -> Result<Map<String, Value>> {
    let name = |column: &RelationColumn| format!("{}.{}", rel.subject.as_subject(), column.name);
    if tuple.len() != rel.columns.len() {
        return Err(Error::Source(format!(
            "pgoutput: {} columns for {}, whose Relation has {}",
            tuple.len(),
            rel.subject.as_subject(),
            rel.columns.len()
        )));
    }
    let mut row = Map::with_capacity(tuple.len());
    for (i, (column, value)) in rel.columns.iter().zip(tuple).enumerate() {
        let value = match value {
            TupleValue::Unchanged => old.and_then(|old| old.get(i)).ok_or_else(|| {
                Error::Source(format!(
                    "column {}: an unchanged value and no old row to fill it from",
                    name(column)
                ))
            })?,
            other => other,
        };
        let json = match value {
            TupleValue::Null => Value::Null,
            TupleValue::Unchanged => {
                return Err(Error::Source(format!(
                    "column {}: the old row has no value to fill an unchanged one",
                    name(column)
                )))
            }
            TupleValue::Text(bytes) => {
                let text = std::str::from_utf8(bytes).map_err(|_| {
                    Error::Source(format!(
                        "column {}: a value that is not UTF-8 ({} bytes)",
                        name(column),
                        bytes.len()
                    ))
                })?;
                text_to_json(column.type_oid, text)
            }
        };
        row.insert(column.name.clone(), json);
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::TupleValue::{Null, Text, Unchanged};

    fn include(subjects: &[&str]) -> IncludeList {
        IncludeList::from_subjects(subjects.iter().map(|s| s.to_string()))
    }

    fn assembler(list: IncludeList) -> Assembler {
        Assembler::new(
            "postgres://t/db".into(),
            PayloadMode::Full,
            list,
            (1000, 1 << 20),
            vec!["mine".into()],
            Lsn(0),
        )
    }

    fn column(name: &str, oid: u32) -> RelationColumn {
        RelationColumn {
            key: true,
            name: name.into(),
            type_oid: oid,
            typmod: -1,
        }
    }

    fn relation(oid: u32, name: &str, columns: Vec<RelationColumn>) -> Message {
        Message::Relation {
            oid,
            namespace: "public".into(),
            name: name.into(),
            replica_identity: b'f',
            columns,
        }
    }

    fn text(value: &str) -> TupleValue {
        Text(value.as_bytes().to_vec())
    }

    fn begin(lsn: u64) -> Message {
        Message::Begin {
            final_lsn: Lsn(lsn),
            xid: 7,
        }
    }

    fn commit(lsn: u64) -> Message {
        Message::Commit {
            lsn: Lsn(lsn),
            end_lsn: Lsn(lsn + 8),
        }
    }

    fn insert(relation: u32, id: &str) -> Message {
        Message::Insert {
            relation,
            new: vec![text(id), text("x")],
        }
    }

    fn orders() -> Message {
        relation(1, "orders", vec![column("id", 23), column("note", 25)])
    }

    fn feed(asm: &mut Assembler, messages: Vec<Message>) -> Vec<Delivery> {
        let mut out = Vec::new();
        for message in messages {
            out.extend(asm.push(message).unwrap());
        }
        out
    }

    fn the_commit(deliveries: &[Delivery]) -> &CommittedTransaction {
        deliveries
            .iter()
            .find_map(|d| match d {
                Delivery::Commit(txn) => Some(txn),
                _ => None,
            })
            .expect("a commit")
    }

    #[test]
    fn a_commit_has_its_schema_first_and_its_position() {
        let mut asm = assembler(include(&["public.orders"]));
        let out = feed(
            &mut asm,
            vec![begin(16), orders(), insert(1, "5"), commit(16)],
        );
        let Delivery::Schema(schema) = &out[0] else {
            panic!("expected the definition first");
        };
        assert_eq!(schema.identity.as_deref(), Some("1"));
        assert_eq!(schema.columns[0].host_type, "integer");
        assert_eq!(schema.columns[1].nullable, None);
        let txn = the_commit(&out);
        let position = txn.position.as_ref().unwrap();
        assert_eq!(position.at, json!({ "lsn": "0/10", "xid": 7 }));
        assert_eq!(position.after, json!({ "lsn": "0/18" }));
        assert_eq!(txn.events[0].subject, "public.orders");
        assert_eq!(txn.events[0].data["after"]["id"], 5);
        assert_eq!(txn.events[0].extensions["relation_oid"], 1);
    }

    #[test]
    fn an_unchanged_value_is_filled_from_the_old_row() {
        let mut asm = assembler(include(&["public.orders"]));
        let out = feed(
            &mut asm,
            vec![
                begin(16),
                orders(),
                Message::Update {
                    relation: 1,
                    old: Some(vec![text("5"), text("big")]),
                    key: None,
                    new: vec![text("6"), Unchanged],
                },
                commit(16),
            ],
        );
        let data = &the_commit(&out).events[0].data;
        assert_eq!(data["before"]["id"], 5);
        assert_eq!(data["after"]["id"], 6);
        assert_eq!(data["after"]["note"], "big");
    }

    #[test]
    fn an_unchanged_value_with_no_old_value_is_an_error_naming_the_column() {
        let mut asm = assembler(include(&["public.orders"]));
        feed(&mut asm, vec![begin(16), orders()]);
        let err = asm
            .push(Message::Insert {
                relation: 1,
                new: vec![text("6"), Unchanged],
            })
            .unwrap_err();
        assert!(err.to_string().contains("public.orders.note"), "{err}");
        let err = asm
            .push(Message::Update {
                relation: 1,
                old: Some(vec![text("6"), Unchanged]),
                key: None,
                new: vec![text("6"), Unchanged],
            })
            .unwrap_err();
        assert!(err.to_string().contains("public.orders.note"), "{err}");
    }

    #[test]
    fn a_change_without_the_old_row_invalidates_the_table_and_the_stream_goes_on() {
        let mut asm = assembler(include(&["public.orders", "public.items"]));
        let out = feed(
            &mut asm,
            vec![
                begin(16),
                orders(),
                relation(2, "items", vec![column("id", 23)]),
                Message::Update {
                    relation: 1,
                    old: None,
                    key: Some(vec![text("5"), Null]),
                    new: vec![text("5"), text("x")],
                },
                Message::Delete {
                    relation: 1,
                    old: None,
                    key: Some(vec![text("5"), Null]),
                },
                Message::Insert {
                    relation: 2,
                    new: vec![text("1")],
                },
                commit(16),
            ],
        );
        let txn = the_commit(&out);
        assert_eq!(txn.events.len(), 1, "only the other table's row");
        assert_eq!(txn.events[0].subject, "public.items");
        assert_eq!(txn.control.len(), 1, "one control per table");
        assert_eq!(txn.control[0].kind, ControlKind::Invalidated);
        assert_eq!(txn.control[0].subject.table, "orders");
        assert_eq!(txn.control[0].identity.as_deref(), Some("1"));
    }

    #[test]
    fn every_announcement_is_a_schema_and_only_a_difference_is_a_control() {
        let mut asm = assembler(include(&["public.orders"]));
        feed(
            &mut asm,
            vec![begin(16), orders(), insert(1, "1"), commit(16)],
        );
        // The same columns again: a Schema, no control.
        let out = feed(
            &mut asm,
            vec![begin(32), orders(), insert(1, "2"), commit(32)],
        );
        assert!(matches!(out[0], Delivery::Schema(_)));
        assert!(the_commit(&out).control.is_empty());
        // No announcement: no Schema.
        let out = feed(&mut asm, vec![begin(48), insert(1, "3"), commit(48)]);
        assert!(!out.iter().any(|d| matches!(d, Delivery::Schema(_))));
        // Other columns: a Schema, and the control with the commit.
        let out = feed(
            &mut asm,
            vec![
                begin(64),
                relation(1, "orders", vec![column("id", 20), column("note", 25)]),
                insert(1, "4"),
                commit(64),
            ],
        );
        let Delivery::Schema(schema) = &out[0] else {
            panic!("expected a definition");
        };
        assert_eq!(schema.columns[0].host_type, "bigint");
        assert_eq!(
            the_commit(&out).control[0].kind,
            ControlKind::DefinitionChanged
        );
    }

    #[test]
    fn a_table_added_to_the_list_later_gets_its_definition_at_its_first_row() {
        let list = include(&[]);
        let mut asm = assembler(list.clone());
        let out = feed(
            &mut asm,
            vec![begin(16), orders(), insert(1, "1"), commit(16)],
        );
        assert!(
            matches!(out[..], [Delivery::Progress { .. }]),
            "nothing for the consumer"
        );
        list.insert(TableRef::new("public", "orders"));
        let out = feed(&mut asm, vec![begin(32), insert(1, "2"), commit(32)]);
        assert!(matches!(out[0], Delivery::Schema(_)));
        assert_eq!(the_commit(&out).events.len(), 1);
    }

    #[test]
    fn the_list_is_pinned_at_begin() {
        let list = include(&["public.orders"]);
        let mut asm = assembler(list.clone());
        feed(&mut asm, vec![begin(16), orders()]);
        list.remove(&TableRef::new("public", "orders"));
        let out = feed(&mut asm, vec![insert(1, "1"), commit(16)]);
        assert_eq!(the_commit(&out).events.len(), 1);
    }

    #[test]
    fn a_leaf_of_a_listed_parent_keeps_its_name_and_says_its_root() {
        let mut asm = assembler(include(&["public.parent"]));
        let message = insert(1, "1");
        feed(&mut asm, vec![begin(16), orders()]);
        assert_eq!(asm.roots_needed(&message), vec![1]);
        asm.set_root(
            1,
            Some(PartitionRoot {
                oid: 9,
                table: TableRef::new("public", "parent"),
            }),
        );
        assert!(asm.roots_needed(&message).is_empty());
        let out = feed(&mut asm, vec![message, commit(16)]);
        let event = &the_commit(&out).events[0];
        assert_eq!(event.subject, "public.orders");
        assert_eq!(event.extensions["partition_root"], "public.parent");
        assert_eq!(event.extensions["partition_root_oid"], 9);
    }

    #[test]
    fn a_table_on_the_list_by_its_own_name_needs_no_lookup_and_a_new_relation_asks_again() {
        let mut asm = assembler(include(&["public.orders"]));
        feed(&mut asm, vec![begin(16), orders()]);
        assert!(asm.roots_needed(&insert(1, "1")).is_empty());
        let mut asm = assembler(include(&["public.other"]));
        feed(&mut asm, vec![begin(16), orders()]);
        asm.set_root(1, None);
        assert!(asm.roots_needed(&insert(1, "1")).is_empty());
        feed(&mut asm, vec![orders()]);
        assert_eq!(asm.roots_needed(&insert(1, "1")), vec![1]);
    }

    #[test]
    fn truncate_is_a_control_per_listed_table() {
        let mut asm = assembler(include(&["public.orders"]));
        let out = feed(
            &mut asm,
            vec![
                begin(16),
                orders(),
                relation(2, "noise", vec![column("id", 23)]),
                Message::Truncate {
                    relations: vec![1, 2],
                },
                commit(16),
            ],
        );
        let txn = the_commit(&out);
        assert_eq!(txn.control.len(), 1);
        assert_eq!(txn.control[0].kind, ControlKind::Truncated);
        assert_eq!(txn.control[0].subject.table, "orders");
    }

    #[test]
    fn messages_under_a_configured_prefix_only() {
        let mut asm = assembler(include(&["public.orders"]));
        let other = Message::Logical {
            transactional: false,
            lsn: Lsn(5),
            prefix: "other".into(),
            content: vec![1],
        };
        assert!(asm.push(other).unwrap().is_empty());
        let mine = Message::Logical {
            transactional: false,
            lsn: Lsn(5),
            prefix: "mine".into(),
            content: vec![0, 255, 7],
        };
        let out = asm.push(mine).unwrap();
        let Delivery::Progress { position } = &out[0] else {
            panic!("a mark is a position passed");
        };
        assert_eq!(
            position.at,
            json!({ "lsn": "0/5", "prefix": "mine", "content": "AP8H" })
        );
        assert_eq!(position.after, json!({ "lsn": "0/5" }));
        let out = feed(
            &mut asm,
            vec![
                begin(16),
                Message::Logical {
                    transactional: true,
                    lsn: Lsn(10),
                    prefix: "mine".into(),
                    content: vec![9],
                },
                commit(16),
            ],
        );
        assert_eq!(
            the_commit(&out).control[0].kind,
            ControlKind::Message {
                prefix: "mine".into(),
                content: vec![9]
            }
        );
    }

    #[test]
    fn a_commit_with_nothing_for_the_consumer_is_progress() {
        let mut asm = assembler(include(&["public.other"]));
        let mut out = feed(&mut asm, vec![begin(16), orders()]);
        asm.set_root(1, None);
        out.extend(feed(&mut asm, vec![insert(1, "1"), commit(16)]));
        assert_eq!(out.len(), 1);
        let Delivery::Progress { position } = &out[0] else {
            panic!("expected progress");
        };
        assert_eq!(position.after, json!({ "lsn": "0/18" }));
    }

    #[test]
    fn a_long_transaction_comes_in_parts_with_the_controls_on_the_last() {
        let mut asm = Assembler::new(
            "s".into(),
            PayloadMode::Full,
            include(&["public.orders"]),
            (2, 1 << 20),
            Vec::new(),
            Lsn(0),
        );
        let out = feed(
            &mut asm,
            vec![
                begin(16),
                orders(),
                insert(1, "1"),
                Message::Truncate { relations: vec![1] },
                insert(1, "2"),
                insert(1, "3"),
                commit(16),
            ],
        );
        let commits: Vec<&CommittedTransaction> = out
            .iter()
            .filter_map(|d| match d {
                Delivery::Commit(txn) => Some(txn),
                _ => None,
            })
            .collect();
        assert_eq!(commits.len(), 2);
        assert_eq!(
            commits[0].part,
            Some(TransactionPart {
                number: 1,
                last: false
            })
        );
        assert_eq!(commits[0].events.len(), 2);
        assert!(commits[0].control.is_empty());
        assert_eq!(commits[0].position.as_ref().unwrap().after, json!({}));
        assert_eq!(
            commits[0].position.as_ref().unwrap().at,
            commits[1].position.as_ref().unwrap().at,
            "every part has the same at"
        );
        assert_eq!(
            commits[1].part,
            Some(TransactionPart {
                number: 2,
                last: true
            })
        );
        assert_eq!(commits[1].control.len(), 1);
        assert_eq!(
            commits[1].position.as_ref().unwrap().after,
            json!({ "lsn": "0/18" })
        );
    }

    #[test]
    fn on_resume_only_a_commit_that_starts_before_the_checkpoint_is_dropped() {
        let mut asm = Assembler::new(
            "s".into(),
            PayloadMode::Full,
            include(&["public.orders"]),
            (1000, 1 << 20),
            Vec::new(),
            Lsn(16),
        );
        let out = feed(
            &mut asm,
            vec![begin(8), orders(), insert(1, "1"), commit(8)],
        );
        assert!(!out.iter().any(|d| matches!(d, Delivery::Commit(_))));
        // A commit record can start exactly where the checkpoint's commit ended.
        let out = feed(&mut asm, vec![begin(16), insert(1, "2"), commit(16)]);
        assert_eq!(the_commit(&out).events.len(), 1);
    }

    #[test]
    fn signal_mode_has_no_row_images() {
        let mut asm = Assembler::new(
            "s".into(),
            PayloadMode::Signal,
            include(&["public.orders"]),
            (1000, 1 << 20),
            Vec::new(),
            Lsn(0),
        );
        let out = feed(
            &mut asm,
            vec![begin(16), orders(), insert(1, "1"), commit(16)],
        );
        assert_eq!(
            the_commit(&out).events[0].data,
            json!({ "op": "insert", "signal": true })
        );
    }

    #[test]
    fn a_type_message_names_what_is_not_built_in() {
        let mut asm = assembler(include(&["public.orders"]));
        let out = feed(
            &mut asm,
            vec![
                begin(16),
                Message::Type {
                    oid: 99_999,
                    namespace: "public".into(),
                    name: "mood".into(),
                },
                relation(1, "orders", vec![column("m", 99_999)]),
                Message::Insert {
                    relation: 1,
                    new: vec![text("happy")],
                },
                commit(16),
            ],
        );
        let Delivery::Schema(schema) = &out[0] else {
            panic!("expected a definition");
        };
        assert_eq!(schema.columns[0].host_type, "public.mood");
        assert_eq!(the_commit(&out).events[0].data["after"]["m"], "happy");
    }

    #[test]
    fn broken_framing_is_an_error() {
        let mut asm = assembler(include(&["public.orders"]));
        assert!(asm.push(commit(16)).is_err());
        asm.push(begin(16)).unwrap();
        assert!(asm.push(begin(32)).is_err());
        assert!(asm.push(commit(32)).is_err());
        let mut asm = assembler(include(&["public.orders"]));
        asm.push(begin(16)).unwrap();
        assert!(asm.push(insert(404, "1")).is_err());
    }
}
