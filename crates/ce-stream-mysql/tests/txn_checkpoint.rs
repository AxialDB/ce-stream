// Integration-test crate: same async_trait / clippy 1.99 `double_must_use` as the lib.
#![allow(clippy::double_must_use)]

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ce_stream_core::avro_encode;
use ce_stream_core::error::Result;
use ce_stream_core::event::{ChangeOp, PayloadMode, TableRef};
use ce_stream_core::transaction::{CommittedTransaction, DdlStatement};
use ce_stream_core::{Checkpoint, CheckpointStore, CloudEvent, DeliveryUnit};
use ce_stream_mysql::test_support::{dispatch, BinlogDispatchCtx};
use ce_stream_mysql::{deliver_committed, DeliverCtx, ExecutedSet, IncludeList, TxnBuffer};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::gtid_event::GtidEvent;
use mysql_binlog_connector_rust::event::query_event::QueryEvent;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use mysql_binlog_connector_rust::event::write_rows_event::WriteRowsEvent;
use mysql_binlog_connector_rust::event::xid_event::XidEvent;
use tokio::sync::{mpsc, Mutex as AsyncMutex};

struct MemStore {
    saves: Arc<Mutex<u32>>,
}

#[async_trait::async_trait]
impl CheckpointStore for MemStore {
    async fn load(&self) -> Result<Option<Checkpoint>> {
        Ok(None)
    }

    async fn save(&self, _checkpoint: &Checkpoint) -> Result<()> {
        *self.saves.lock().unwrap() += 1;
        Ok(())
    }
}

fn row_event() -> CloudEvent {
    CloudEvent::row_change(
        "mysql://test",
        &TableRef::new("db", "t"),
        ChangeOp::Insert,
        serde_json::json!({"op": "insert"}),
        serde_json::Map::new(),
    )
}

fn make_txn(gtid: &str, rows: usize) -> CommittedTransaction {
    CommittedTransaction {
        gtid: gtid.into(),
        gtid_set_after: format!("{gtid}"),
        ddl: vec![],
        events: (0..rows).map(|_| row_event()).collect(),
        ..Default::default()
    }
}

fn demo_table_map() -> TableMapEvent {
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

fn demo_write_rows(n: usize) -> WriteRowsEvent {
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
fn txn_buffer_no_emit_before_xid() {
    let mut buf = TxnBuffer::default();
    buf.on_gtid("uuid:1".into());
    buf.push_row(row_event());
    buf.push_row(row_event());
    assert_eq!(buf.pending_gtid(), Some("uuid:1"));

    let txn = buf.take_commit("uuid:1-1".into()).expect("xid");
    assert_eq!(txn.events.len(), 2);
    assert!(buf.pending_gtid().is_none());
}

#[test]
fn dispatch_no_commit_before_xid() {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(4);
    let executed = Arc::new(AsyncMutex::new(ExecutedSet::default()));
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
    assert!(rx.try_recv().is_err(), "no commit on GTID");

    dispatch(&mut ctx, EventData::TableMap(demo_table_map())).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(demo_write_rows(2))).unwrap();
    assert!(rx.try_recv().is_err(), "no commit after rows before XID");

    dispatch(&mut ctx, EventData::WriteRows(demo_write_rows(1))).unwrap();
    assert!(rx.try_recv().is_err(), "still no commit before XID");

    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 1 })).unwrap();
    let commit = rx.try_recv().unwrap().expect("commit at XID");
    assert_eq!(commit.gtid, "uuid:1");
    assert_eq!(commit.events.len(), 3);
}

#[test]
fn dispatch_three_row_callbacks_after_xid_in_row_mode() {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(4);
    let executed = Arc::new(AsyncMutex::new(ExecutedSet::default()));
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
            gtid: "uuid:2".into(),
        }),
    )
    .unwrap();
    dispatch(&mut ctx, EventData::TableMap(demo_table_map())).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(demo_write_rows(3))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 2 })).unwrap();

    let commit = rx.try_recv().unwrap().unwrap();
    let delivered = Cell::new(0u32);
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
                &mut |_| {
                    delivered.set(delivered.get() + 1);
                    Ok(())
                },
                &mut |_| Ok(()),
            )
            .await
        })
        .unwrap();

    assert_eq!(delivered.get(), 3);
}

#[test]
fn all_filtered_txn_produces_empty_commit_at_xid() {
    let include = IncludeList::from_subjects(["other.table"]);
    let (tx, mut rx) = mpsc::channel(4);
    let executed = Arc::new(AsyncMutex::new(ExecutedSet::default()));
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
            gtid: "uuid:3".into(),
        }),
    )
    .unwrap();
    dispatch(&mut ctx, EventData::TableMap(demo_table_map())).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(demo_write_rows(2))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 3 })).unwrap();

    let commit = rx.try_recv().unwrap().unwrap();
    assert!(commit.events.is_empty());
    assert!(commit.ddl.is_empty());
    assert_eq!(commit.gtid, "uuid:3");
}

#[test]
fn row_event_without_gtid_hard_fails() {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, _rx) = mpsc::channel(4);
    let executed = Arc::new(AsyncMutex::new(ExecutedSet::default()));
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

    dispatch(&mut ctx, EventData::TableMap(demo_table_map())).unwrap();
    let err = dispatch(&mut ctx, EventData::WriteRows(demo_write_rows(1))).unwrap_err();
    assert!(
        err.contains("row event without pending GTID"),
        "unexpected: {err}"
    );
}

#[test]
fn dispatch_ddl_populated_in_commit() {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(4);
    let executed = Arc::new(AsyncMutex::new(ExecutedSet::default()));
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
            gtid: "uuid:5".into(),
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
    dispatch(&mut ctx, EventData::TableMap(demo_table_map())).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(demo_write_rows(1))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 5 })).unwrap();

    let commit = rx.try_recv().unwrap().unwrap();
    assert_eq!(commit.ddl.len(), 1);
    assert_eq!(commit.ddl[0].schema, "demo");
    assert_eq!(commit.events.len(), 1);
}

#[tokio::test]
async fn one_checkpoint_after_full_txn_ack_row_mode() {
    let saves = Arc::new(Mutex::new(0u32));
    let store = MemStore {
        saves: Arc::clone(&saves),
    };
    let mut executed = ExecutedSet::default();
    let mut checkpoint_store: Option<Box<dyn CheckpointStore>> = Some(Box::new(store));
    let mut checkpoint = None;
    let delivered = Cell::new(0u32);

    deliver_committed(
        make_txn("abc:1", 3),
        &mut DeliverCtx {
            source_id: "mysql://test",
            delivery_unit: DeliveryUnit::Row,
            executed: &mut executed,
            checkpoint_store: &mut checkpoint_store,
            checkpoint: &mut checkpoint,
        },
        &mut |_| {
            delivered.set(delivered.get() + 1);
            Ok(())
        },
        &mut |_| Ok(()),
    )
    .await
    .unwrap();

    assert_eq!(delivered.get(), 3);
    assert_eq!(executed.to_set_string(), "abc:1-1");
    assert_eq!(*saves.lock().unwrap(), 1);
}

#[tokio::test]
async fn row_mode_avro_after_commit_one_checkpoint() {
    let saves = Arc::new(Mutex::new(0u32));
    let store = MemStore {
        saves: Arc::clone(&saves),
    };
    let mut executed = ExecutedSet::default();
    let mut checkpoint_store: Option<Box<dyn CheckpointStore>> = Some(Box::new(store));
    let mut checkpoint = None;
    let encoded = Cell::new(0u32);

    deliver_committed(
        make_txn("abc:1", 3),
        &mut DeliverCtx {
            source_id: "mysql://test",
            delivery_unit: DeliveryUnit::Row,
            executed: &mut executed,
            checkpoint_store: &mut checkpoint_store,
            checkpoint: &mut checkpoint,
        },
        &mut |ev| {
            let bytes = avro_encode::encode_cloudevent(&ev)?;
            assert!(!bytes.is_empty());
            encoded.set(encoded.get() + 1);
            Ok(())
        },
        &mut |_| Ok(()),
    )
    .await
    .unwrap();

    assert_eq!(encoded.get(), 3, "one Avro encode per row after commit");
    assert_eq!(
        *saves.lock().unwrap(),
        1,
        "single checkpoint after all encodes"
    );
}

#[tokio::test]
async fn transaction_mode_one_envelope_per_commit() {
    let mut received: Option<CommittedTransaction> = None;
    let mut executed = ExecutedSet::default();
    let mut checkpoint_store = None;
    let mut checkpoint = None;

    deliver_committed(
        make_txn("abc:1", 3),
        &mut DeliverCtx {
            source_id: "mysql://test",
            delivery_unit: DeliveryUnit::Transaction,
            executed: &mut executed,
            checkpoint_store: &mut checkpoint_store,
            checkpoint: &mut checkpoint,
        },
        &mut |_| panic!("row callback must not run in transaction mode"),
        &mut |txn| {
            received = Some(txn);
            Ok(())
        },
    )
    .await
    .unwrap();

    let txn = received.expect("one envelope");
    assert_eq!(txn.gtid, "abc:1");
    assert_eq!(txn.events.len(), 3);
}

#[tokio::test]
async fn ddl_in_commit_row_mode() {
    let saves = Arc::new(Mutex::new(0u32));
    let store = MemStore {
        saves: Arc::clone(&saves),
    };
    let mut executed = ExecutedSet::default();
    let mut checkpoint_store: Option<Box<dyn CheckpointStore>> = Some(Box::new(store));
    let mut checkpoint = None;
    let mut types: Vec<String> = Vec::new();

    deliver_committed(
        CommittedTransaction {
            gtid: "abc:1".into(),
            gtid_set_after: "abc:1-1".into(),
            ddl: vec![DdlStatement {
                schema: "app".into(),
                query: "ALTER TABLE t ADD COLUMN x INT".into(),
            }],
            events: make_txn("abc:1", 3).events,
            ..Default::default()
        },
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
    .unwrap();

    assert_eq!(types.len(), 4);
    assert_eq!(types[0], ce_stream_mysql::DDL_CE_TYPE);
    assert_eq!(*saves.lock().unwrap(), 1);
}

#[tokio::test]
async fn crash_mid_fanout_does_not_checkpoint() {
    let saves = Arc::new(Mutex::new(0u32));
    let store = MemStore {
        saves: Arc::clone(&saves),
    };
    let mut executed = ExecutedSet::default();
    let mut checkpoint_store: Option<Box<dyn CheckpointStore>> = Some(Box::new(store));
    let mut checkpoint = None;
    let delivered = Cell::new(0u32);

    let err = deliver_committed(
        make_txn("abc:1", 3),
        &mut DeliverCtx {
            source_id: "mysql://test",
            delivery_unit: DeliveryUnit::Row,
            executed: &mut executed,
            checkpoint_store: &mut checkpoint_store,
            checkpoint: &mut checkpoint,
        },
        &mut |_| {
            delivered.set(delivered.get() + 1);
            if delivered.get() == 1 {
                return Err(ce_stream_core::Error::Sink("simulated crash".into()));
            }
            Ok(())
        },
        &mut |_| Ok(()),
    )
    .await;

    assert!(err.is_err());
    assert_eq!(delivered.get(), 1);
    assert!(executed.to_set_string().is_empty());
    assert!(checkpoint.is_none());
    assert_eq!(*saves.lock().unwrap(), 0);
}

#[tokio::test]
async fn empty_commit_still_advances_watermark() {
    let saves = Arc::new(Mutex::new(0u32));
    let store = MemStore {
        saves: Arc::clone(&saves),
    };
    let mut executed = ExecutedSet::default();
    let mut checkpoint_store: Option<Box<dyn CheckpointStore>> = Some(Box::new(store));
    let mut checkpoint = None;

    deliver_committed(
        CommittedTransaction {
            gtid: "abc:1".into(),
            gtid_set_after: "abc:1-1".into(),
            ddl: vec![],
            events: vec![],
            ..Default::default()
        },
        &mut DeliverCtx {
            source_id: "mysql://test",
            delivery_unit: DeliveryUnit::Row,
            executed: &mut executed,
            checkpoint_store: &mut checkpoint_store,
            checkpoint: &mut checkpoint,
        },
        &mut |_| Ok(()),
        &mut |_| Ok(()),
    )
    .await
    .unwrap();

    assert_eq!(executed.to_set_string(), "abc:1-1");
    assert_eq!(*saves.lock().unwrap(), 1);
}

#[test]
fn all_filtered_commit_still_checkpoints() {
    let include = IncludeList::from_subjects(["other.table"]);
    let (tx, mut rx) = mpsc::channel(4);
    let executed = Arc::new(AsyncMutex::new(ExecutedSet::default()));
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
            gtid: "uuid:4".into(),
        }),
    )
    .unwrap();
    dispatch(&mut ctx, EventData::TableMap(demo_table_map())).unwrap();
    dispatch(&mut ctx, EventData::WriteRows(demo_write_rows(2))).unwrap();
    dispatch(&mut ctx, EventData::Xid(XidEvent { xid: 4 })).unwrap();

    let commit = rx.try_recv().unwrap().unwrap();

    let saves = Arc::new(Mutex::new(0u32));
    let store = MemStore {
        saves: Arc::clone(&saves),
    };
    let mut executed = ExecutedSet::default();
    let mut checkpoint_store: Option<Box<dyn CheckpointStore>> = Some(Box::new(store));
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
                &mut |_| Ok(()),
                &mut |_| Ok(()),
            )
            .await
        })
        .unwrap();

    assert_eq!(executed.to_set_string(), "uuid:4-4");
    assert_eq!(*saves.lock().unwrap(), 1);
}
