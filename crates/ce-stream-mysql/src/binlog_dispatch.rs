//! Binlog event routing, including compressed [`EventData::TransactionPayload`] unpack.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ce_stream_core::event::{ChangeOp, CloudEvent, PayloadMode, TableRef};
use ce_stream_core::transaction::CommittedTransaction;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, warn};

use crate::ddl;
use crate::gtid::ExecutedSet;
use crate::map;
use crate::txn_buffer::TxnBuffer;

#[doc(hidden)]
pub struct TableMap {
    pub table: TableRef,
    pub col_names: Vec<String>,
}

#[doc(hidden)]
pub struct BinlogDispatchCtx<'a> {
    pub txn: &'a mut TxnBuffer,
    pub executed: &'a Arc<Mutex<ExecutedSet>>,
    pub tables: &'a mut HashMap<u64, TableMap>,
    pub source_id: &'a str,
    pub include: &'a HashSet<String>,
    pub payload_mode: PayloadMode,
    pub tx: &'a mpsc::Sender<std::result::Result<CommittedTransaction, String>>,
}

/// Route one binlog event through the commit-boundary state machine.
pub(crate) fn dispatch_binlog_event(
    ctx: &mut BinlogDispatchCtx<'_>,
    data: EventData,
) -> std::result::Result<(), String> {
    match data {
        EventData::Gtid(g) => {
            ctx.txn.on_gtid(g.gtid);
        }
        EventData::TableMap(tm) => apply_table_map(ctx.tables, tm),
        EventData::WriteRows(e) => buffer_rows(
            ctx,
            e.table_id,
            ChangeOp::Insert,
            e.rows.iter().map(|r| (None, Some(r))),
        )?,
        EventData::UpdateRows(e) => buffer_rows(
            ctx,
            e.table_id,
            ChangeOp::Update,
            e.rows.iter().map(|(b, a)| (Some(b), Some(a))),
        )?,
        EventData::DeleteRows(e) => buffer_rows(
            ctx,
            e.table_id,
            ChangeOp::Delete,
            e.rows.iter().map(|r| (Some(r), None)),
        )?,
        EventData::Xid(_) => flush_commit(ctx)?,
        EventData::Query(q) => buffer_query(ctx, q)?,
        EventData::TransactionPayload(tp) => {
            if tp.uncompressed_events.is_empty() {
                return Err("binlog transaction compression: empty uncompressed payload".into());
            }
            for (_header, inner) in tp.uncompressed_events {
                dispatch_binlog_event(ctx, inner)?;
            }
        }
        EventData::HeartBeat => debug!("heartbeat"),
        other => debug!(?other, "ignored binlog event"),
    }
    Ok(())
}

fn apply_table_map(tables: &mut HashMap<u64, TableMap>, tm: TableMapEvent) {
    let col_names = column_names_from_table_map(&tm);
    tables.insert(
        tm.table_id,
        TableMap {
            table: TableRef::new(tm.database_name, tm.table_name),
            col_names,
        },
    );
}

fn buffer_query(
    ctx: &mut BinlogDispatchCtx<'_>,
    q: mysql_binlog_connector_rust::event::query_event::QueryEvent,
) -> std::result::Result<(), String> {
    if ddl::is_ignored_transaction_query(&q.query) {
        return Ok(());
    }
    if !ddl::is_ddl_query(&q.query) {
        debug!(schema = %q.schema, query = %q.query, "ignored non-DDL query");
        return Ok(());
    }

    ctx.txn
        .pending_gtid()
        .ok_or_else(|| "DDL query without pending GTID".to_string())?;

    ctx.txn.push_ddl(ce_stream_core::transaction::DdlStatement {
        schema: q.schema,
        query: q.query,
    });
    Ok(())
}

fn flush_commit(ctx: &mut BinlogDispatchCtx<'_>) -> std::result::Result<(), String> {
    let Some(pending) = ctx.txn.pending_gtid().map(str::to_string) else {
        return Err("XID without preceding GTID event".into());
    };

    let gtid_set_after = ctx
        .executed
        .blocking_lock()
        .gtid_set_after(&pending)
        .map_err(|e| e.to_string())?;

    let Some(commit) = ctx.txn.take_commit(gtid_set_after) else {
        return Err("XID without pending GTID".into());
    };

    ctx.tx
        .blocking_send(Ok(commit))
        .map_err(|_| "event channel closed".to_string())
}

fn column_names_from_table_map(tm: &TableMapEvent) -> Vec<String> {
    if let Some(meta) = &tm.table_metadata {
        let names: Vec<String> = meta
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| c.column_name.clone().unwrap_or_else(|| format!("col_{i}")))
            .collect();
        if names.iter().any(|n| !n.starts_with("col_")) {
            return names;
        }
        if !names.is_empty() {
            return names;
        }
    }
    (0..tm.column_types.len())
        .map(|i| format!("col_{i}"))
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn buffer_rows<'a, I>(
    ctx: &mut BinlogDispatchCtx<'_>,
    table_id: u64,
    op: ChangeOp,
    rows: I,
) -> std::result::Result<(), String>
where
    I: Iterator<
        Item = (
            Option<&'a mysql_binlog_connector_rust::event::row_event::RowEvent>,
            Option<&'a mysql_binlog_connector_rust::event::row_event::RowEvent>,
        ),
    >,
{
    let Some(tm) = ctx.tables.get(&table_id) else {
        warn!(table_id, "row event without table_map; skipping");
        return Ok(());
    };

    let subject = tm.table.as_subject();
    if !ctx.include.is_empty() && !ctx.include.contains(&subject) {
        return Ok(());
    }

    let pending_gtid = ctx
        .txn
        .pending_gtid()
        .ok_or_else(|| "row event without pending GTID".to_string())?
        .to_string();

    let gtid_set = ctx
        .executed
        .blocking_lock()
        .gtid_set_after(&pending_gtid)
        .map_err(|e| e.to_string())?;

    let table = tm.table.clone();
    let col_names = tm.col_names.clone();

    for (before, after) in rows {
        let data = match ctx.payload_mode {
            PayloadMode::Signal => serde_json::json!({
                "op": op.as_str(),
                "signal": true,
            }),
            PayloadMode::Full => match op {
                ChangeOp::Insert => {
                    let after = after.ok_or_else(|| "insert missing after".to_string())?;
                    serde_json::json!({
                        "op": "insert",
                        "after": map::row_to_object(&col_names, after),
                    })
                }
                ChangeOp::Update => {
                    let before = before.ok_or_else(|| "update missing before".to_string())?;
                    let after = after.ok_or_else(|| "update missing after".to_string())?;
                    serde_json::json!({
                        "op": "update",
                        "before": map::row_to_object(&col_names, before),
                        "after": map::row_to_object(&col_names, after),
                    })
                }
                ChangeOp::Delete => {
                    let before = before.ok_or_else(|| "delete missing before".to_string())?;
                    serde_json::json!({
                        "op": "delete",
                        "before": map::row_to_object(&col_names, before),
                    })
                }
            },
        };

        let mut extensions = serde_json::Map::new();
        extensions.insert(
            "gtid".into(),
            serde_json::Value::String(pending_gtid.clone()),
        );
        if !gtid_set.is_empty() {
            extensions.insert(
                "gtidset".into(),
                serde_json::Value::String(gtid_set.clone()),
            );
        }

        let event = CloudEvent::row_change(ctx.source_id, &table, op, data, extensions);
        ctx.txn.push_row(event);
    }

    Ok(())
}

#[doc(hidden)]
pub fn dispatch_binlog_event_for_test(
    ctx: &mut BinlogDispatchCtx<'_>,
    data: EventData,
) -> std::result::Result<(), String> {
    dispatch_binlog_event(ctx, data)
}
