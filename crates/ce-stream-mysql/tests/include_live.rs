//! Live include-list updates take effect at the next GTID, never mid-envelope.

use std::collections::HashMap;
use std::sync::Arc;

use ce_stream_core::event::{PayloadMode, TableRef};
use ce_stream_mysql::test_support::{dispatch, BinlogDispatchCtx};
use ce_stream_mysql::{ExecutedSet, IncludeList, TxnBuffer};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::gtid_event::GtidEvent;
use mysql_binlog_connector_rust::event::query_event::QueryEvent;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use mysql_binlog_connector_rust::event::write_rows_event::WriteRowsEvent;
use mysql_binlog_connector_rust::event::xid_event::XidEvent;
use tokio::sync::{mpsc, Mutex};

fn table_map(table_id: u64, table: &str) -> TableMapEvent {
    TableMapEvent {
        table_id,
        database_name: "demo".into(),
        table_name: table.into(),
        column_types: vec![3],
        column_metas: vec![0],
        null_bits: vec![true],
        table_metadata: None,
    }
}

fn write_rows(table_id: u64, n: usize) -> WriteRowsEvent {
    WriteRowsEvent {
        table_id,
        included_columns: vec![true],
        rows: (0..n)
            .map(|i| RowEvent {
                column_values: vec![ColumnValue::Long(i as i32 + 1)],
            })
            .collect(),
    }
}

fn gtid(id: &str) -> EventData {
    EventData::Gtid(GtidEvent {
        flags: 0,
        gtid: id.into(),
    })
}

#[test]
fn add_takes_effect_on_next_gtid() {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(4);
    let executed = Arc::new(Mutex::new(ExecutedSet::default()));
    let mut tables = HashMap::new();
    let mut txn = TxnBuffer::default();

    let mut ctx = BinlogDispatchCtx {
        txn: &mut txn,
        executed: &executed,
        tables: &mut tables,
        source_id: "mysql://test",
        include: &include,
        payload_mode: PayloadMode::Full,
        tx: &tx,
    };

    dispatch(&mut ctx, EventData::TableMap(table_map(1, "orders"))).unwrap();
    dispatch(&mut ctx, EventData::TableMap(table_map(2, "noise"))).unwrap();

    dispatch(&mut ctx, gtid("uuid:1")).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(1, 1))).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(2, 1))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 1 })).unwrap();
    let first = rx.try_recv().unwrap().unwrap();
    assert_eq!(first.events.len(), 1);
    assert_eq!(first.events[0].subject, "demo.orders");

    include.insert(TableRef::new("demo", "noise"));

    dispatch(&mut ctx, gtid("uuid:2")).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(1, 1))).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(2, 1))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 2 })).unwrap();
    let second = rx.try_recv().unwrap().unwrap();
    let mut subjects: Vec<_> = second.events.iter().map(|e| e.subject.as_str()).collect();
    subjects.sort();
    assert_eq!(subjects, vec!["demo.noise", "demo.orders"]);
}

#[test]
fn remove_takes_effect_on_next_gtid() {
    let include = IncludeList::from_subjects(["demo.orders", "demo.noise"]);
    let (tx, mut rx) = mpsc::channel(4);
    let executed = Arc::new(Mutex::new(ExecutedSet::default()));
    let mut tables = HashMap::new();
    let mut txn = TxnBuffer::default();

    let mut ctx = BinlogDispatchCtx {
        txn: &mut txn,
        executed: &executed,
        tables: &mut tables,
        source_id: "mysql://test",
        include: &include,
        payload_mode: PayloadMode::Full,
        tx: &tx,
    };

    dispatch(&mut ctx, EventData::TableMap(table_map(1, "orders"))).unwrap();
    dispatch(&mut ctx, EventData::TableMap(table_map(2, "noise"))).unwrap();

    dispatch(&mut ctx, gtid("uuid:1")).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(1, 1))).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(2, 1))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 1 })).unwrap();
    assert_eq!(rx.try_recv().unwrap().unwrap().events.len(), 2);

    include.remove(&TableRef::new("demo", "orders"));

    dispatch(&mut ctx, gtid("uuid:2")).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(1, 1))).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(2, 1))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 2 })).unwrap();
    let second = rx.try_recv().unwrap().unwrap();
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].subject, "demo.noise");
}

#[test]
fn mid_transaction_update_does_not_split_envelope() {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(4);
    let executed = Arc::new(Mutex::new(ExecutedSet::default()));
    let mut tables = HashMap::new();
    let mut txn = TxnBuffer::default();

    let mut ctx = BinlogDispatchCtx {
        txn: &mut txn,
        executed: &executed,
        tables: &mut tables,
        source_id: "mysql://test",
        include: &include,
        payload_mode: PayloadMode::Full,
        tx: &tx,
    };

    dispatch(&mut ctx, EventData::TableMap(table_map(1, "orders"))).unwrap();
    dispatch(&mut ctx, EventData::TableMap(table_map(2, "noise"))).unwrap();

    dispatch(&mut ctx, gtid("uuid:1")).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(1, 1))).unwrap();
    include.insert(TableRef::new("demo", "noise"));
    dispatch(&mut ctx, EventData::WriteRows(write_rows(2, 1))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 1 })).unwrap();

    let commit = rx.try_recv().unwrap().unwrap();
    assert_eq!(commit.events.len(), 1);
    assert_eq!(commit.events[0].subject, "demo.orders");
}

#[test]
fn remove_all_still_delivers_empty_commit() {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(4);
    let executed = Arc::new(Mutex::new(ExecutedSet::default()));
    let mut tables = HashMap::new();
    let mut txn = TxnBuffer::default();

    let mut ctx = BinlogDispatchCtx {
        txn: &mut txn,
        executed: &executed,
        tables: &mut tables,
        source_id: "mysql://test",
        include: &include,
        payload_mode: PayloadMode::Full,
        tx: &tx,
    };

    dispatch(&mut ctx, EventData::TableMap(table_map(1, "orders"))).unwrap();
    include.remove(&TableRef::new("demo", "orders"));

    dispatch(&mut ctx, gtid("uuid:1")).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(1, 2))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 1 })).unwrap();

    let commit = rx.try_recv().unwrap().unwrap();
    assert!(commit.events.is_empty());
    assert!(commit.ddl.is_empty());
    assert_eq!(commit.gtid, "uuid:1");
}

#[test]
fn replace_takes_effect_on_next_gtid() {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(4);
    let executed = Arc::new(Mutex::new(ExecutedSet::default()));
    let mut tables = HashMap::new();
    let mut txn = TxnBuffer::default();

    let mut ctx = BinlogDispatchCtx {
        txn: &mut txn,
        executed: &executed,
        tables: &mut tables,
        source_id: "mysql://test",
        include: &include,
        payload_mode: PayloadMode::Full,
        tx: &tx,
    };

    dispatch(&mut ctx, EventData::TableMap(table_map(1, "orders"))).unwrap();
    dispatch(&mut ctx, EventData::TableMap(table_map(2, "noise"))).unwrap();

    dispatch(&mut ctx, gtid("uuid:1")).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(1, 1))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 1 })).unwrap();
    assert_eq!(
        rx.try_recv().unwrap().unwrap().events[0].subject,
        "demo.orders"
    );

    include.replace([TableRef::new("demo", "noise")]);

    dispatch(&mut ctx, gtid("uuid:2")).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(1, 1))).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(2, 1))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 2 })).unwrap();
    let second = rx.try_recv().unwrap().unwrap();
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].subject, "demo.noise");
}

#[test]
fn ddl_is_not_gated_by_include_list() {
    let include = IncludeList::from_subjects(["other.table"]);
    let (tx, mut rx) = mpsc::channel(4);
    let executed = Arc::new(Mutex::new(ExecutedSet::default()));
    let mut tables = HashMap::new();
    let mut txn = TxnBuffer::default();

    let mut ctx = BinlogDispatchCtx {
        txn: &mut txn,
        executed: &executed,
        tables: &mut tables,
        source_id: "mysql://test",
        include: &include,
        payload_mode: PayloadMode::Full,
        tx: &tx,
    };

    dispatch(&mut ctx, gtid("uuid:1")).unwrap();
    dispatch(
        &mut ctx,
        EventData::Query(QueryEvent {
            thread_id: 1,
            exec_time: 0,
            error_code: 0,
            schema: "demo".into(),
            query: "ALTER TABLE orders ADD COLUMN x INT".into(),
        }),
    )
    .unwrap();
    dispatch(&mut ctx, EventData::TableMap(table_map(1, "orders"))).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(1, 1))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 1 })).unwrap();

    let commit = rx.try_recv().unwrap().unwrap();
    assert_eq!(commit.ddl.len(), 1);
    assert!(commit.events.is_empty());
}
