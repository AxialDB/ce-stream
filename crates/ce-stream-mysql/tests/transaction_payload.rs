//! Compressed transaction (`TransactionPayload`) dispatch tests.

use std::collections::HashMap;
use std::sync::Arc;

use ce_stream_core::event::PayloadMode;
use ce_stream_mysql::test_support::{dispatch, BinlogDispatchCtx, TableMap};
use ce_stream_mysql::{ExecutedSet, IncludeList, TxnBuffer};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::event_header::EventHeader;
use mysql_binlog_connector_rust::event::gtid_event::GtidEvent;
use mysql_binlog_connector_rust::event::query_event::QueryEvent;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use mysql_binlog_connector_rust::event::transaction_payload_event::TransactionPayloadEvent;
use mysql_binlog_connector_rust::event::write_rows_event::WriteRowsEvent;
use mysql_binlog_connector_rust::event::xid_event::XidEvent;
use tokio::sync::{mpsc, Mutex};

fn sample_table_map() -> TableMapEvent {
    TableMapEvent {
        table_id: 90,
        database_name: "demo".into(),
        table_name: "orders".into(),
        column_types: vec![3, 3],
        column_metas: vec![0, 0],
        null_bits: vec![true, true],
        table_metadata: None,
    }
}

fn sample_write_rows(table_id: u64, n: usize) -> WriteRowsEvent {
    WriteRowsEvent {
        table_id,
        included_columns: vec![true, true],
        rows: (0..n)
            .map(|i| RowEvent {
                column_values: vec![ColumnValue::Long(i as i32 + 1), ColumnValue::Long(i as i32)],
            })
            .collect(),
    }
}

fn compressed_txn_payload(row_count: usize) -> EventData {
    let header = EventHeader {
        timestamp: 0,
        event_type: 0,
        server_id: 1,
        event_length: 0,
        next_event_position: 0,
        event_flags: 0,
    };
    EventData::TransactionPayload(TransactionPayloadEvent {
        uncompressed_size: 0,
        uncompressed_events: vec![
            (header.clone(), EventData::TableMap(sample_table_map())),
            (
                header.clone(),
                EventData::WriteRows(sample_write_rows(90, row_count)),
            ),
            (header, EventData::Xid(XidEvent { xid: 42 })),
        ],
    })
}

#[test]
fn transaction_payload_unpack_produces_one_commit() {
    let include = IncludeList::from_subjects(["demo.orders"]);

    let (tx, mut rx) = mpsc::channel(4);
    let executed = Arc::new(Mutex::new(ExecutedSet::default()));
    let mut tables: HashMap<u64, TableMap> = HashMap::new();
    let mut txn = TxnBuffer::default();
    let source_id = "mysql://test";

    let mut ctx = BinlogDispatchCtx {
        txn: &mut txn,
        executed: &executed,
        tables: &mut tables,
        source_id,
        include: &include,
        payload_mode: PayloadMode::Full,
        tx: &tx,
    };

    dispatch(
        &mut ctx,
        EventData::Gtid(GtidEvent {
            flags: 0,
            gtid: "36682cf3-a048-11ed-b4b3-0242ac110004:1".into(),
        }),
    )
    .expect("gtid");

    assert!(rx.try_recv().is_err(), "no emit before payload unpack");

    dispatch(&mut ctx, compressed_txn_payload(3)).expect("transaction payload");

    let commit = rx
        .try_recv()
        .expect("one commit at inner XID")
        .expect("commit ok");
    assert_eq!(commit.gtid, "36682cf3-a048-11ed-b4b3-0242ac110004:1");
    assert_eq!(commit.events.len(), 3);
    assert!(rx.try_recv().is_err());
}

#[test]
fn transaction_payload_includes_ddl_query() {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(4);
    let executed = Arc::new(Mutex::new(ExecutedSet::default()));
    let mut tables = HashMap::new();
    let mut txn = TxnBuffer::default();

    let header = EventHeader {
        timestamp: 0,
        event_type: 0,
        server_id: 1,
        event_length: 0,
        next_event_position: 0,
        event_flags: 0,
    };

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
        EventData::TransactionPayload(TransactionPayloadEvent {
            uncompressed_size: 0,
            uncompressed_events: vec![
                (
                    header.clone(),
                    EventData::Query(QueryEvent {
                        thread_id: 1,
                        exec_time: 0,
                        error_code: 0,
                        schema: "demo".into(),
                        query: "ALTER TABLE orders ADD COLUMN x INT".into(),
                    }),
                ),
                (header.clone(), EventData::TableMap(sample_table_map())),
                (
                    header.clone(),
                    EventData::WriteRows(sample_write_rows(90, 2)),
                ),
                (header, EventData::Xid(XidEvent { xid: 1 })),
            ],
        }),
    )
    .unwrap();

    let commit = rx.try_recv().unwrap().unwrap();
    assert_eq!(commit.ddl.len(), 1);
    assert_eq!(commit.events.len(), 2);
}

#[test]
fn empty_transaction_payload_fails() {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, _rx) = mpsc::channel(1);
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

    let err = dispatch(
        &mut ctx,
        EventData::TransactionPayload(TransactionPayloadEvent {
            uncompressed_size: 0,
            uncompressed_events: vec![],
        }),
    )
    .expect_err("empty payload");

    assert!(
        err.contains("binlog transaction compression"),
        "unexpected error: {err}"
    );
}
