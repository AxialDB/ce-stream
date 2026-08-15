//! DDL buffering and row-mode / transaction-mode delivery tests.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ce_stream_core::event::PayloadMode;
use ce_stream_core::DeliveryUnit;
use ce_stream_mysql::test_support::{dispatch, BinlogDispatchCtx};
use ce_stream_mysql::DDL_CE_TYPE;
use ce_stream_mysql::{deliver_committed, DeliverCtx, ExecutedSet, TxnBuffer};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::gtid_event::GtidEvent;
use mysql_binlog_connector_rust::event::query_event::QueryEvent;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use mysql_binlog_connector_rust::event::write_rows_event::WriteRowsEvent;
use mysql_binlog_connector_rust::event::xid_event::XidEvent;
use tokio::sync::{mpsc, Mutex};

fn table_map() -> TableMapEvent {
    TableMapEvent {
        table_id: 1,
        database_name: "demo".into(),
        table_name: "orders".into(),
        column_types: vec![3],
        column_metas: vec![0],
        null_bits: vec![true],
        table_metadata: None,
    }
}

fn write_rows(n: usize) -> WriteRowsEvent {
    WriteRowsEvent {
        table_id: 1,
        included_columns: vec![true],
        rows: (0..n)
            .map(|i| RowEvent {
                column_values: vec![ColumnValue::Long(i as i32 + 1)],
            })
            .collect(),
    }
}

#[test]
fn ddl_buffered_then_commit_includes_ddl_and_rows() {
    let include = HashSet::from(["demo.orders".into()]);
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

    dispatch(
        &mut ctx,
        EventData::Gtid(GtidEvent {
            flags: 0,
            gtid: "uuid:1".into(),
        }),
    )
    .unwrap();
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
    dispatch(&mut ctx, EventData::TableMap(table_map())).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(3))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 1 })).unwrap();

    let commit = rx.try_recv().unwrap().unwrap();
    assert_eq!(commit.ddl.len(), 1);
    assert_eq!(commit.ddl[0].schema, "demo");
    assert_eq!(commit.events.len(), 3);
}

#[test]
fn row_mode_one_ddl_ce_then_three_row_ces() {
    let include = HashSet::from(["demo.orders".into()]);
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

    dispatch(
        &mut ctx,
        EventData::Gtid(GtidEvent {
            flags: 0,
            gtid: "uuid:1".into(),
        }),
    )
    .unwrap();
    dispatch(
        &mut ctx,
        EventData::Query(QueryEvent {
            thread_id: 1,
            exec_time: 0,
            error_code: 0,
            schema: "demo".into(),
            query: "CREATE TABLE orders (id INT)".into(),
        }),
    )
    .unwrap();
    dispatch(&mut ctx, EventData::TableMap(table_map())).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(3))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 1 })).unwrap();

    let commit = rx.try_recv().unwrap().unwrap();
    let mut types = Vec::new();
    let mut executed = ExecutedSet::default();
    let mut checkpoint_store = None;
    let mut checkpoint = None;
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async {
            deliver_committed(
                commit,
                &mut DeliverCtx {
                    source_id: "mysql://test",
                    delivery_unit: DeliveryUnit::Row,
                    executed: &mut executed,
                    checkpoint_store: &mut checkpoint_store,
                    checkpoint: &mut checkpoint,
                },
                &mut |ev| {
                    types.push(ev.ty.clone());
                    Ok(())
                },
                &mut |_| Ok(()),
            )
            .await
        })
        .unwrap();

    assert_eq!(types.len(), 4);
    assert_eq!(types[0], DDL_CE_TYPE);
}

#[test]
fn begin_query_is_ignored() {
    let include = HashSet::from(["demo.orders".into()]);
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

    dispatch(
        &mut ctx,
        EventData::Gtid(GtidEvent {
            flags: 0,
            gtid: "uuid:1".into(),
        }),
    )
    .unwrap();
    dispatch(
        &mut ctx,
        EventData::Query(QueryEvent {
            thread_id: 1,
            exec_time: 0,
            error_code: 0,
            schema: "demo".into(),
            query: "BEGIN".into(),
        }),
    )
    .unwrap();
    dispatch(&mut ctx, EventData::TableMap(table_map())).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(write_rows(1))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 1 })).unwrap();

    let commit = rx.try_recv().unwrap().unwrap();
    assert!(commit.ddl.is_empty());
}
