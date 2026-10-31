//! Where a transaction ends, for the event sequences MySQL 9.7 writes.
//!
//! The sequences are those of a real binlog (`mysqlbinlog`, `binlog_format=ROW`,
//! `gtid_mode=ON`). A transaction is either in brackets, `BEGIN` ... `Xid` or
//! `BEGIN` ... `COMMIT`, or it is one `Query` event with nothing after it.

use std::collections::HashMap;
use std::sync::Arc;

use ce_stream_core::event::PayloadMode;
use ce_stream_core::transaction::CommittedTransaction;
use ce_stream_core::IncludeList;
use ce_stream_mysql::test_support::{commit_of, dispatch, BinlogDispatchCtx};
use ce_stream_mysql::{commit_gtid, commit_gtid_set_after};
use ce_stream_mysql::{ExecutedSet, TxnBuffer};
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

fn write_rows(n: usize) -> EventData {
    EventData::WriteRows(WriteRowsEvent {
        table_id: 1,
        included_columns: vec![true],
        rows: (0..n)
            .map(|i| RowEvent {
                column_values: vec![ColumnValue::Long(i as i32 + 1)],
            })
            .collect(),
    })
}

fn xid() -> EventData {
    EventData::Xid(XidEvent { xid: 1 })
}

/// Every transaction the events deliver, in order. After each event the
/// transactions it delivered are taken, so the test also says when.
fn run(events: Vec<EventData>) -> Vec<(usize, CommittedTransaction)> {
    expect_no_error(run_with(events, TxnBuffer::default()))
}

/// [`run`] with parts of at most `rows` rows.
fn run_in_parts(events: Vec<EventData>, rows: usize) -> Vec<(usize, CommittedTransaction)> {
    expect_no_error(run_with(events, TxnBuffer::with_part_limits(rows, 0)))
}

type Run = (Vec<(usize, CommittedTransaction)>, Option<(usize, String)>);

fn expect_no_error((delivered, error): Run) -> Vec<(usize, CommittedTransaction)> {
    if let Some((at, err)) = error {
        panic!("event {at}: {err}");
    }
    delivered
}

/// What was delivered until the first error, and the error with its event.
fn run_with(events: Vec<EventData>, mut txn: TxnBuffer) -> Run {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(64);
    let executed = Arc::new(Mutex::new(ExecutedSet::default()));
    let mut tables = HashMap::new();
    let mut delivered = Vec::new();
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
        let result = dispatch(&mut ctx, event);
        while let Ok(commit) = rx.try_recv() {
            let commit = commit_of(commit.expect("commit"));
            // As the reader does: a part that is not the last is not a commit.
            if commit.ends_transaction() {
                executed
                    .try_lock()
                    .expect("executed")
                    .add_committed(commit_gtid(&commit))
                    .expect("add");
            }
            delivered.push((at, commit));
        }
        if let Err(err) = result {
            return (delivered, Some((at, err)));
        }
    }
    (delivered, None)
}

/// `TRUNCATE TABLE demo.orders`: a `Gtid` and one `Query`, and no `Xid`.
#[test]
fn a_ddl_statement_is_delivered_at_its_query_event() {
    let delivered = run(vec![gtid(1), query("TRUNCATE TABLE demo.orders")]);
    assert_eq!(delivered.len(), 1);
    let (at, commit) = &delivered[0];
    assert_eq!(*at, 1, "delivered when the statement is read, not later");
    assert_eq!(commit_gtid(commit), "uuid:1");
    assert_eq!(commit_gtid_set_after(commit), "uuid:1-1");
    assert_eq!(commit.ddl.len(), 1);
    assert_eq!(commit.ddl[0].schema, "demo");
    assert_eq!(commit.ddl[0].query, "TRUNCATE TABLE demo.orders");
    assert!(commit.events.is_empty());
}

/// DDL statements between row transactions: each is its own transaction,
/// and the executed set has no hole.
#[test]
fn ddl_statements_between_row_transactions_leave_no_hole() {
    let delivered = run(vec![
        gtid(1),
        query("BEGIN"),
        table_map(),
        write_rows(2),
        xid(),
        gtid(2),
        query("CREATE DATABASE demo2"),
        gtid(3),
        query("ALTER TABLE demo.orders ADD COLUMN extra INT"),
        gtid(4),
        query("BEGIN"),
        table_map(),
        write_rows(1),
        xid(),
        gtid(5),
        query("DROP TABLE `demo`.`orders` /* generated by server */"),
    ]);
    let gtids: Vec<&str> = delivered.iter().map(|(_, c)| commit_gtid(c)).collect();
    assert_eq!(gtids, ["uuid:1", "uuid:2", "uuid:3", "uuid:4", "uuid:5"]);
    let shape: Vec<(usize, usize)> = delivered
        .iter()
        .map(|(_, c)| (c.ddl.len(), c.events.len()))
        .collect();
    assert_eq!(shape, [(0, 2), (1, 0), (1, 0), (0, 1), (1, 0)]);
    assert_eq!(commit_gtid_set_after(&delivered[4].1), "uuid:1-5");
}

/// `CREATE TABLE ... SELECT` is one transaction: `BEGIN`, the statement with
/// `START TRANSACTION` at its end, the rows, `Xid`.
#[test]
fn create_table_select_is_one_transaction_with_its_rows() {
    let ctas = "CREATE TABLE `demo`.`orders` (\n  `id` int NOT NULL\n) START TRANSACTION";
    let delivered = run(vec![
        gtid(1),
        query("BEGIN"),
        query(ctas),
        table_map(),
        write_rows(3),
        xid(),
    ]);
    assert_eq!(delivered.len(), 1);
    let (at, commit) = &delivered[0];
    assert_eq!(*at, 5, "delivered at the Xid");
    assert_eq!(commit.ddl.len(), 1);
    assert_eq!(commit.events.len(), 3);

    // The same statement without a `BEGIN` in front still waits for its rows.
    let delivered = run(vec![
        gtid(1),
        query(ctas),
        table_map(),
        write_rows(3),
        xid(),
    ]);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].0, 4);
    assert_eq!(delivered[0].1.events.len(), 3);
}

/// The shape of each delivery: rows, DDL statements, part.
fn shapes(delivered: &[(usize, CommittedTransaction)]) -> Vec<(usize, usize, Option<(u32, bool)>)> {
    delivered
        .iter()
        .map(|(_, c)| {
            (
                c.events.len(),
                c.ddl.len(),
                c.part.map(|p| (p.number, p.last)),
            )
        })
        .collect()
}

/// A transaction of exactly one part is delivered whole: no part fields and
/// no empty last part.
#[test]
fn a_transaction_of_exactly_one_part_is_whole() {
    let delivered = run_in_parts(
        vec![gtid(1), query("BEGIN"), table_map(), write_rows(3), xid()],
        3,
    );
    assert_eq!(shapes(&delivered), [(3, 0, None)]);
    assert_eq!(commit_gtid_set_after(&delivered[0].1), "uuid:1-1");
}

/// One row more than a part: the part goes out when the extra row is read,
/// the last part at the `Xid`. Only the last part carries the executed set,
/// on the delivery and on its rows.
#[test]
fn one_row_more_than_a_part_is_two_parts() {
    let delivered = run_in_parts(
        vec![gtid(1), query("BEGIN"), table_map(), write_rows(4), xid()],
        3,
    );
    assert_eq!(
        shapes(&delivered),
        [(3, 0, Some((1, false))), (1, 0, Some((2, true)))]
    );
    let (at, first) = &delivered[0];
    assert_eq!(*at, 3, "the part goes out while the rows are read");
    assert_eq!(commit_gtid(first), "uuid:1");
    assert_eq!(commit_gtid_set_after(first), "");
    assert!(first
        .events
        .iter()
        .all(|e| !e.extensions.contains_key("gtidset")));
    let (at, last) = &delivered[1];
    assert_eq!(*at, 4, "the last part at the Xid");
    assert_eq!(commit_gtid(last), "uuid:1");
    assert_eq!(commit_gtid_set_after(last), "uuid:1-1");
}

/// Rows of one transaction in several row events fill parts across them.
#[test]
fn parts_are_cut_across_row_events() {
    let delivered = run_in_parts(
        vec![
            gtid(1),
            query("BEGIN"),
            table_map(),
            write_rows(2),
            table_map(),
            write_rows(2),
            table_map(),
            write_rows(3),
            xid(),
        ],
        3,
    );
    assert_eq!(
        shapes(&delivered),
        [
            (3, 0, Some((1, false))),
            (3, 0, Some((2, false))),
            (1, 0, Some((3, true)))
        ]
    );
    let at: Vec<usize> = delivered.iter().map(|(at, _)| *at).collect();
    assert_eq!(at, [5, 7, 8]);
}

/// `CREATE TABLE ... SELECT` in parts: its statement stays in front of its
/// rows, in the first part, and the later parts carry rows only.
#[test]
fn create_table_select_in_parts_keeps_its_statement_first() {
    let ctas = "CREATE TABLE `demo`.`orders` (\n  `id` int NOT NULL\n) START TRANSACTION";
    let delivered = run_in_parts(
        vec![
            gtid(1),
            query("BEGIN"),
            query(ctas),
            table_map(),
            write_rows(5),
            xid(),
        ],
        2,
    );
    assert_eq!(
        shapes(&delivered),
        [
            (2, 1, Some((1, false))),
            (2, 0, Some((2, false))),
            (1, 0, Some((3, true)))
        ]
    );
    assert_eq!(delivered[0].1.ddl[0].query, ctas);
}

/// A `ROLLBACK` after parts of the transaction were delivered stops the
/// stream: MySQL with `gtid_mode=ON` does not write one, and the consumer
/// could not be told to drop the parts it holds. Before the first part the
/// transaction is not delivered, as before parts existed.
#[test]
fn a_rollback_after_a_delivered_part_stops_the_stream() {
    let (delivered, error) = run_with(
        vec![
            gtid(1),
            query("BEGIN"),
            table_map(),
            write_rows(4),
            query("ROLLBACK"),
            gtid(2),
            query("TRUNCATE TABLE demo.orders"),
        ],
        TxnBuffer::with_part_limits(3, 0),
    );
    assert_eq!(shapes(&delivered), [(3, 0, Some((1, false)))]);
    let (at, err) = error.expect("the stream stops");
    assert_eq!(at, 4, "at the ROLLBACK");
    assert!(err.contains("uuid:1") && err.contains("ROLLBACK"), "{err}");

    let delivered = run_in_parts(
        vec![
            gtid(1),
            query("BEGIN"),
            table_map(),
            write_rows(3),
            query("ROLLBACK"),
            gtid(2),
            query("TRUNCATE TABLE demo.orders"),
        ],
        3,
    );
    let gtids: Vec<&str> = delivered.iter().map(|(_, c)| commit_gtid(c)).collect();
    assert_eq!(
        gtids,
        ["uuid:2"],
        "no part went out: not delivered, as before"
    );
}

/// The next transaction after one in parts starts again at part 1, or whole.
#[test]
fn the_transaction_after_one_in_parts_is_whole_again() {
    let delivered = run_in_parts(
        vec![
            gtid(1),
            query("BEGIN"),
            table_map(),
            write_rows(4),
            xid(),
            gtid(2),
            query("BEGIN"),
            table_map(),
            write_rows(2),
            xid(),
        ],
        3,
    );
    assert_eq!(
        shapes(&delivered),
        [
            (3, 0, Some((1, false))),
            (1, 0, Some((2, true))),
            (2, 0, None)
        ]
    );
    assert_eq!(commit_gtid_set_after(&delivered[2].1), "uuid:1-2");
}

/// A table without transactions (MyISAM): `BEGIN`, rows, and a `COMMIT`
/// query where InnoDB has an `Xid`.
#[test]
fn a_transaction_that_ends_with_a_commit_query_is_delivered() {
    let delivered = run(vec![
        gtid(1),
        query("BEGIN"),
        table_map(),
        write_rows(2),
        query("COMMIT"),
        gtid(2),
        query("BEGIN"),
        table_map(),
        write_rows(1),
        xid(),
    ]);
    assert_eq!(delivered.len(), 2);
    assert_eq!(delivered[0].0, 4, "delivered at the COMMIT");
    assert_eq!(delivered[0].1.events.len(), 2);
    assert_eq!(commit_gtid_set_after(&delivered[1].1), "uuid:1-2");
}

/// `GRANT`, `ANALYZE TABLE`: nothing to deliver but the transaction, so
/// that its GTID is executed.
#[test]
fn a_statement_that_is_not_ddl_is_an_empty_transaction() {
    let delivered = run(vec![
        gtid(1),
        query("GRANT SELECT ON `demo`.* TO 'u'@'localhost'"),
        gtid(2),
        query("ANALYZE TABLE demo.orders"),
        gtid(3),
        query("CREATE USER IF NOT EXISTS 'u'@'localhost' IDENTIFIED WITH 'caching_sha2_password'"),
    ]);
    assert_eq!(delivered.len(), 3);
    assert!(delivered[0].1.ddl.is_empty() && delivered[0].1.events.is_empty());
    assert!(delivered[1].1.ddl.is_empty() && delivered[1].1.events.is_empty());
    // `CREATE USER` has a DDL verb and is passed on as it is.
    assert_eq!(delivered[2].1.ddl.len(), 1);
    assert_eq!(commit_gtid_set_after(&delivered[2].1), "uuid:1-3");
}

/// A client's comment in front of the statement, as the server logs it.
#[test]
fn a_ddl_statement_with_a_comment_in_front_is_delivered() {
    let text = "/* ApplicationName=DBeaver 26.2.1 - SQLEditor <Script-9.sql> */ ALTER TABLE orders ADD COLUMN x INT";
    let delivered = run(vec![gtid(1), query(text)]);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].1.ddl.len(), 1);
    assert_eq!(delivered[0].1.ddl[0].query, text);
}

/// Statements inside a transaction in brackets do not end it.
#[test]
fn a_savepoint_inside_a_transaction_does_not_end_it() {
    let delivered = run(vec![
        gtid(1),
        query("BEGIN"),
        table_map(),
        write_rows(1),
        query("SAVEPOINT `sp`"),
        write_rows(1),
        query("ROLLBACK TO `sp`"),
        xid(),
    ]);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].0, 7);
    assert_eq!(delivered[0].1.events.len(), 2);
}

/// An XA transaction is delivered at its `XA COMMIT`, whose position it takes,
/// and the stream goes on. The rest is in `tests/deliveries.rs`.
#[test]
fn an_xa_transaction_is_delivered_at_its_commit() {
    let delivered = run(vec![
        gtid(1),
        query("XA START X'61',X'',1"),
        table_map(),
        write_rows(2),
        query("XA END X'61',X'',1"),
        EventData::XaPrepare(XaPrepareEvent {
            one_phase: false,
            format_id: 1,
            gtrid: "a".into(),
            bqual: String::new(),
        }),
        gtid(2),
        query("XA COMMIT X'61',X'',1"),
        gtid(3),
        query("BEGIN"),
        table_map(),
        write_rows(1),
        xid(),
    ]);
    let gtids: Vec<&str> = delivered.iter().map(|(_, c)| commit_gtid(c)).collect();
    assert_eq!(gtids, ["uuid:2", "uuid:3"]);
    assert_eq!(delivered[0].1.events.len(), 2);
    assert_eq!(delivered[1].1.events.len(), 1);
}

/// A transaction that is rolled back in the binlog is not delivered.
#[test]
fn a_rolled_back_transaction_is_not_delivered() {
    let delivered = run(vec![
        gtid(1),
        query("BEGIN"),
        table_map(),
        write_rows(2),
        query("ROLLBACK"),
        gtid(2),
        query("TRUNCATE TABLE demo.orders"),
    ]);
    assert_eq!(delivered.len(), 1);
    assert_eq!(commit_gtid(&delivered[0].1), "uuid:2");
    assert_eq!(commit_gtid_set_after(&delivered[0].1), "uuid:2-2");
}

/// The consumer is behind and commits wait in the queue. The set after each
/// still names every commit before it: the reader counts what it has passed
/// on, not what the consumer has taken.
#[test]
fn the_set_after_a_commit_names_the_commits_that_wait_in_the_queue() {
    let include = IncludeList::from_subjects(["demo.orders"]);
    let (tx, mut rx) = mpsc::channel(64);
    // What a checkpoint holds at the start. Nothing is taken from the queue
    // while the events are read, so this set does not move.
    let executed = Arc::new(Mutex::new(
        ExecutedSet::from_set_string("uuid:1-5").expect("set"),
    ));
    let mut tables = HashMap::new();
    let mut txn = TxnBuffer::default();
    let events = vec![
        gtid(6),
        query("CREATE DATABASE demo2"),
        gtid(7),
        query("TRUNCATE TABLE demo.orders"),
        gtid(8),
        query("BEGIN"),
        table_map(),
        write_rows(1),
        xid(),
        // Rolled back: not passed on, so it is in no set.
        gtid(9),
        query("BEGIN"),
        table_map(),
        write_rows(1),
        query("ROLLBACK"),
        gtid(10),
        query("ANALYZE TABLE demo.orders"),
    ];
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
    let mut commits = Vec::new();
    while let Ok(commit) = rx.try_recv() {
        commits.push(commit_of(commit.expect("commit")));
    }
    let sets: Vec<&str> = commits.iter().map(commit_gtid_set_after).collect();
    assert_eq!(sets, ["uuid:1-6", "uuid:1-7", "uuid:1-8", "uuid:1-8:10-10"]);
    // A row event carries the same set as its commit.
    assert_eq!(
        commits[2].events[0].extensions["gtidset"],
        serde_json::json!("uuid:1-8")
    );
}
