//! The partition of a row (WL#12168): a rows event of a partitioned table says
//! which partition its rows are in, and an update which one they left. The
//! adapter passes both on as extensions; a table that is not partitioned has
//! neither.

use std::collections::HashMap;
use std::sync::Arc;

use ce_stream_core::delivery::Delivery;
use ce_stream_core::event::PayloadMode;
use ce_stream_core::IncludeList;
use ce_stream_mysql::test_support::{dispatch, BinlogDispatchCtx};
use ce_stream_mysql::{ExecutedSet, TxnBuffer};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::event::delete_rows_event::DeleteRowsEvent;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::gtid_event::GtidEvent;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use mysql_binlog_connector_rust::event::update_rows_event::UpdateRowsEvent;
use mysql_binlog_connector_rust::event::write_rows_event::WriteRowsEvent;
use mysql_binlog_connector_rust::event::xid_event::XidEvent;
use tokio::sync::{mpsc, Mutex};

fn gtid() -> EventData {
    EventData::Gtid(GtidEvent {
        flags: 0,
        gtid: "uuid:1".into(),
    })
}

fn table_map() -> EventData {
    EventData::TableMap(TableMapEvent {
        table_id: 1,
        database_name: "demo".into(),
        table_name: "orders".into(),
        column_types: vec![3],
        column_metas: vec![0],
        null_bits: vec![true],
        table_metadata: None,
    })
}

fn row(n: i32) -> RowEvent {
    RowEvent {
        column_values: vec![ColumnValue::Long(n)],
    }
}

fn write(partition_id: Option<u16>) -> EventData {
    EventData::WriteRows(WriteRowsEvent {
        table_id: 1,
        included_columns: vec![true],
        rows: vec![row(1), row(2)],
        partition_id,
    })
}

fn update(partition_id: Option<u16>, source_partition_id: Option<u16>) -> EventData {
    EventData::UpdateRows(UpdateRowsEvent {
        table_id: 1,
        included_columns_before: vec![true],
        included_columns_after: vec![true],
        rows: vec![(row(1), row(3))],
        partition_id,
        source_partition_id,
    })
}

fn delete(partition_id: Option<u16>) -> EventData {
    EventData::DeleteRows(DeleteRowsEvent {
        table_id: 1,
        included_columns: vec![true],
        rows: vec![row(3)],
        partition_id,
    })
}

fn xid() -> EventData {
    EventData::Xid(XidEvent { xid: 1 })
}

/// The row events of one committed transaction holding `events`.
fn rows_of(events: Vec<EventData>) -> Vec<ce_stream_core::event::CloudEvent> {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(64);
    let executed = Arc::new(Mutex::new(ExecutedSet::default()));
    let mut tables = HashMap::new();
    let mut txn = TxnBuffer::default();
    let mut all = vec![gtid(), table_map()];
    all.extend(events);
    all.push(xid());
    for event in all {
        let mut ctx = BinlogDispatchCtx {
            txn: &mut txn,
            executed: &executed,
            tables: &mut tables,
            source_id: "mysql://test",
            include: &include,
            payload_mode: PayloadMode::Full,
            tx: &tx,
        };
        dispatch(&mut ctx, event).expect("dispatch");
    }
    let mut rows = Vec::new();
    while let Ok(delivery) = rx.try_recv() {
        if let Delivery::Commit(commit) = delivery.expect("delivery") {
            rows.extend(commit.events);
        }
    }
    rows
}

fn partition(event: &ce_stream_core::event::CloudEvent, key: &str) -> Option<u64> {
    event
        .extensions
        .get(key)
        .map(|v| v.as_u64().expect("a number"))
}

#[test]
fn a_partitioned_table_says_which_partition_each_row_is_in() {
    let rows = rows_of(vec![write(Some(3)), delete(Some(3))]);
    assert_eq!(rows.len(), 3);
    for event in &rows {
        assert_eq!(partition(event, "partition_id"), Some(3), "{event:?}");
        assert_eq!(partition(event, "source_partition_id"), None);
    }
}

#[test]
fn an_update_carries_the_partition_the_row_left() {
    let rows = rows_of(vec![update(Some(3), Some(1))]);
    assert_eq!(rows.len(), 1);
    assert_eq!(partition(&rows[0], "partition_id"), Some(3));
    assert_eq!(partition(&rows[0], "source_partition_id"), Some(1));
}

#[test]
fn an_update_inside_one_partition_names_it_twice() {
    let rows = rows_of(vec![update(Some(2), Some(2))]);
    assert_eq!(partition(&rows[0], "partition_id"), Some(2));
    assert_eq!(partition(&rows[0], "source_partition_id"), Some(2));
}

#[test]
fn a_plain_table_has_no_partition_extensions() {
    let rows = rows_of(vec![write(None), update(None, None), delete(None)]);
    assert_eq!(rows.len(), 4);
    for event in &rows {
        assert!(!event.extensions.contains_key("partition_id"), "{event:?}");
        assert!(!event.extensions.contains_key("source_partition_id"));
        assert!(event.extensions.contains_key("gtid"));
    }
}
