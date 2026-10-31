//! What the reader hands over under the contract of issue #19: a commit, or the
//! position passed when a transaction held nothing for the consumer.

use std::collections::HashMap;
use std::sync::Arc;

use ce_stream_core::delivery::Delivery;
use ce_stream_core::event::PayloadMode;
use ce_stream_core::IncludeList;
use ce_stream_core::{Checkpoint, DeliveryUnit};
use ce_stream_mysql::test_support::{dispatch, BinlogDispatchCtx};
use ce_stream_mysql::{
    commit_position, deliver_progress, new_confirm, DeliverCtx, ExecutedSet, TxnBuffer,
};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::gtid_event::GtidEvent;
use mysql_binlog_connector_rust::event::query_event::QueryEvent;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use mysql_binlog_connector_rust::event::write_rows_event::WriteRowsEvent;
use mysql_binlog_connector_rust::event::xa_prepare_event::XaPrepareEvent;
use mysql_binlog_connector_rust::event::xid_event::XidEvent;
use tokio::sync::{mpsc, Mutex};

fn gtid(n: u64) -> EventData {
    EventData::Gtid(GtidEvent {
        flags: 0,
        gtid: format!("uuid:{n}"),
    })
}

fn query(text: &str) -> EventData {
    EventData::Query(QueryEvent {
        thread_id: 1,
        exec_time: 0,
        error_code: 0,
        schema: "demo".into(),
        query: text.into(),
    })
}

fn table_map(table: &str) -> EventData {
    EventData::TableMap(TableMapEvent {
        table_id: 1,
        database_name: "demo".into(),
        table_name: table.into(),
        column_types: vec![3],
        column_metas: vec![0],
        null_bits: vec![true],
        table_metadata: None,
    })
}

fn write_row() -> EventData {
    EventData::WriteRows(WriteRowsEvent {
        table_id: 1,
        included_columns: vec![true],
        rows: vec![RowEvent {
            column_values: vec![ColumnValue::Long(1)],
        }],
    })
}

fn xid() -> EventData {
    EventData::Xid(XidEvent { xid: 1 })
}

fn deliveries(events: Vec<EventData>) -> Vec<Delivery> {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(64);
    let executed = Arc::new(Mutex::new(ExecutedSet::default()));
    let mut tables = HashMap::new();
    let mut txn = TxnBuffer::default();
    let mut out = Vec::new();
    for event in events {
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
        while let Ok(delivery) = rx.try_recv() {
            out.push(delivery.expect("delivery"));
        }
    }
    out
}

fn xa_prepare(one_phase: bool) -> EventData {
    EventData::XaPrepare(XaPrepareEvent {
        one_phase,
        format_id: 1,
        gtrid: "two".into(),
        bqual: String::new(),
    })
}

const XA_ID: &str = "X'74776f',X'',1";

/// An XA transaction on `demo.orders` up to its `XA PREPARE`, as MySQL 9.7.2
/// writes it.
fn xa_prepared(n: u64, rows: usize) -> Vec<EventData> {
    let mut events = vec![
        gtid(n),
        query(&format!("XA START {XA_ID}")),
        table_map("orders"),
    ];
    events.extend((0..rows).map(|_| write_row()));
    events.push(query(&format!("XA END {XA_ID}")));
    events.push(xa_prepare(false));
    events
}

#[test]
fn an_xa_transaction_is_delivered_at_its_commit() {
    let mut events = xa_prepared(1, 2);
    assert!(deliveries(events.clone()).is_empty(), "nothing at prepare");
    events.push(gtid(2));
    events.push(query(&format!("XA COMMIT {XA_ID}")));
    let out = deliveries(events);
    assert_eq!(out.len(), 1);
    let Delivery::Commit(commit) = &out[0] else {
        panic!("expected a commit, got {:?}", out[0]);
    };
    assert_eq!(commit.events.len(), 2);
    let position = commit.position.as_ref().unwrap();
    assert_eq!(position.at["gtid"], "uuid:2");
    assert_eq!(position.at["prepared_gtid"], "uuid:1");
    assert_eq!(position.after["gtid"], "uuid:1-2");
    assert_eq!(commit.events[0].extensions["gtid"], "uuid:2");
    assert_eq!(commit.events[0].extensions["gtidset"], "uuid:1-2");
}

#[test]
fn an_xa_rollback_drops_the_rows_and_ends_the_prepared_gtid() {
    let mut events = xa_prepared(1, 2);
    events.push(gtid(2));
    events.push(query(&format!("XA ROLLBACK {XA_ID}")));
    let out = deliveries(events);
    assert_eq!(out.len(), 1);
    let Delivery::Progress { position } = &out[0] else {
        panic!("expected progress, got {:?}", out[0]);
    };
    assert_eq!(position.at["prepared_gtid"], "uuid:1");
    assert_eq!(position.after["gtid"], "uuid:1-2");
}

#[test]
fn a_prepared_gtid_stays_out_of_the_set_until_it_ends() {
    let mut events = xa_prepared(1, 1);
    events.extend([
        gtid(2),
        query("BEGIN"),
        table_map("orders"),
        write_row(),
        xid(),
        gtid(3),
        query(&format!("XA COMMIT {XA_ID}")),
    ]);
    let out = deliveries(events);
    assert_eq!(out.len(), 2);
    let after = |d: &Delivery| match d {
        Delivery::Commit(c) => c.position.as_ref().unwrap().after["gtid"].clone(),
        other => panic!("expected a commit, got {other:?}"),
    };
    // A restart here reads the prepared transaction again.
    assert_eq!(after(&out[0]), "uuid:2-2");
    assert_eq!(after(&out[1]), "uuid:1-3");
}

#[test]
fn a_prepared_transaction_read_again_after_a_restart_is_delivered_once() {
    // The checkpoint has what ran after the prepare, and not the prepare.
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(64);
    let executed = Arc::new(Mutex::new(
        ExecutedSet::from_set_string("uuid:2-2").unwrap(),
    ));
    let mut tables = HashMap::new();
    let mut txn = TxnBuffer::default();
    let mut events = xa_prepared(1, 1);
    events.extend([gtid(3), query(&format!("XA COMMIT {XA_ID}"))]);
    for event in events {
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
    let Delivery::Commit(commit) = rx.try_recv().unwrap().unwrap() else {
        panic!("expected a commit");
    };
    assert_eq!(commit.events.len(), 1);
    assert_eq!(commit.position.unwrap().after["gtid"], "uuid:1-3");
    assert!(rx.try_recv().is_err());
}

#[test]
fn a_one_phase_xa_commit_delivers_at_once() {
    let out = deliveries(vec![
        gtid(1),
        query(&format!("XA START {XA_ID}")),
        table_map("orders"),
        write_row(),
        query(&format!("XA END {XA_ID}")),
        xa_prepare(true),
    ]);
    assert_eq!(out.len(), 1);
    let Delivery::Commit(commit) = &out[0] else {
        panic!("expected a commit, got {:?}", out[0]);
    };
    assert_eq!(commit.events.len(), 1);
    let position = commit.position.as_ref().unwrap();
    assert_eq!(position.at["gtid"], "uuid:1");
    assert_eq!(position.after["gtid"], "uuid:1-1");
    assert!(position.at.get("prepared_gtid").is_none());
}

#[test]
fn an_xa_transaction_is_held_whole_and_cut_into_parts_at_its_commit() {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(64);
    let executed = Arc::new(Mutex::new(ExecutedSet::default()));
    let mut tables = HashMap::new();
    let mut txn = TxnBuffer::with_part_limits(2, 0);
    let mut events = xa_prepared(1, 5);
    let prepare_at = events.len();
    events.extend([gtid(2), query(&format!("XA COMMIT {XA_ID}"))]);
    for (at, event) in events.into_iter().enumerate() {
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
        if at < prepare_at {
            assert!(rx.try_recv().is_err(), "no part before the commit");
        }
    }
    let mut sizes = Vec::new();
    let mut ended = 0;
    while let Ok(delivery) = rx.try_recv() {
        let Delivery::Commit(part) = delivery.unwrap() else {
            panic!("expected a commit");
        };
        assert_eq!(part.position.as_ref().unwrap().at["gtid"], "uuid:2");
        sizes.push(part.events.len());
        ended += usize::from(part.ends_transaction());
    }
    assert_eq!(sizes, [2, 2, 1]);
    assert_eq!(ended, 1);
}

#[test]
fn an_xa_commit_of_rows_never_read_stops_the_stream() {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, _rx) = mpsc::channel(64);
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
    dispatch(&mut ctx, gtid(5)).unwrap();
    let err = dispatch(&mut ctx, query(&format!("XA COMMIT {XA_ID}"))).unwrap_err();
    assert!(err.contains("before capture started"), "{err}");
}

#[test]
fn a_transaction_of_other_tables_is_a_position_passed() {
    let out = deliveries(vec![
        gtid(1),
        query("BEGIN"),
        table_map("other"),
        write_row(),
        xid(),
    ]);
    assert_eq!(out.len(), 1);
    let Delivery::Progress { position } = &out[0] else {
        panic!("expected progress, got {:?}", out[0]);
    };
    assert_eq!(position.adapter, "mysql");
    assert_eq!(position.at["gtid"], "uuid:1");
    assert_eq!(position.after["gtid"], "uuid:1-1");
}

#[test]
fn a_statement_with_nothing_to_deliver_is_a_position_passed() {
    let out = deliveries(vec![gtid(1), query("GRANT SELECT ON demo.* TO 'x'")]);
    assert!(matches!(out[0], Delivery::Progress { .. }));
}

#[test]
fn a_transaction_with_rows_or_ddl_is_a_commit() {
    let out = deliveries(vec![
        gtid(1),
        query("BEGIN"),
        table_map("orders"),
        write_row(),
        xid(),
        gtid(2),
        query("TRUNCATE TABLE demo.orders"),
    ]);
    assert_eq!(out.len(), 2);
    let Delivery::Commit(rows) = &out[0] else {
        panic!("expected a commit");
    };
    assert_eq!(rows.events.len(), 1);
    assert_eq!(rows.position.as_ref().unwrap().after["gtid"], "uuid:1-1");
    let Delivery::Commit(ddl) = &out[1] else {
        panic!("expected a commit");
    };
    assert_eq!(ddl.ddl.len(), 1);
}

#[tokio::test]
async fn a_position_passed_moves_the_checkpoint_in_either_mode() {
    for unit in [DeliveryUnit::Row, DeliveryUnit::Transaction] {
        let mut executed = ExecutedSet::from_set_string("abc:1-4").unwrap();
        let mut store = None;
        let mut checkpoint: Option<Checkpoint> = None;
        let mut delivered: Vec<Delivery> = Vec::new();
        deliver_progress(
            commit_position("abc:5", "abc:1-5"),
            &mut DeliverCtx {
                source_id: "mysql://test",
                delivery_unit: unit,
                executed: &mut executed,
                checkpoint_store: &mut store,
                checkpoint: &mut checkpoint,
            },
            &mut |delivery| {
                delivered.push(delivery);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(checkpoint.unwrap().payload["gtid"], "abc:1-5");
        // Row mode has no use for a position, and a sink would get one per
        // unrelated transaction.
        assert_eq!(
            delivered.len(),
            usize::from(unit == DeliveryUnit::Transaction)
        );
    }
}

#[test]
fn confirm_orders_positions_by_the_executed_set() {
    let confirm = new_confirm();
    confirm
        .confirm(&commit_position("abc:5", "abc:1-5"))
        .unwrap();
    // Behind the last one: nothing moves.
    confirm
        .confirm(&commit_position("abc:3", "abc:1-3"))
        .unwrap();
    assert_eq!(confirm.latest().unwrap().after["gtid"], "abc:1-5");
    confirm
        .confirm(&commit_position("abc:6", "abc:1-6"))
        .unwrap();
    assert_eq!(confirm.latest().unwrap().after["gtid"], "abc:1-6");
    // A part that is not the last has no position after it.
    assert!(confirm.confirm(&commit_position("abc:7", "")).is_err());
}
