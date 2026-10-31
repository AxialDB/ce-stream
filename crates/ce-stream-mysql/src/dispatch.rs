//! Commit-boundary delivery and checkpoint (shared by `run` / `run_deliveries`).

use ce_stream_core::delivery::Delivery;
use ce_stream_core::error::Result;
use ce_stream_core::transaction::{CommittedTransaction, SourcePosition};
use ce_stream_core::{Checkpoint, CheckpointStore, CloudEvent, DeliveryUnit};

use crate::ddl;
use crate::gtid::{commit_gtid, commit_gtid_set_after, ExecutedSet};

/// Mutable capture state used while delivering one committed transaction.
pub struct DeliverCtx<'a> {
    pub source_id: &'a str,
    pub delivery_unit: DeliveryUnit,
    pub executed: &'a mut ExecutedSet,
    pub checkpoint_store: &'a mut Option<Box<dyn CheckpointStore>>,
    pub checkpoint: &'a mut Option<Checkpoint>,
}

/// The GTID of the `XA PREPARE` this position's `XA COMMIT` or `XA ROLLBACK`
/// ends: it becomes executed with it.
fn prepared_gtid(position: &SourcePosition) -> Option<String> {
    position
        .at
        .get("prepared_gtid")
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Deliver one committed transaction and persist checkpoint on full success.
pub async fn deliver_committed<F, G>(
    txn: CommittedTransaction,
    ctx: &mut DeliverCtx<'_>,
    on_row: &mut F,
    on_txn: &mut G,
) -> Result<()>
where
    F: FnMut(CloudEvent) -> Result<()>,
    G: FnMut(CommittedTransaction) -> Result<()>,
{
    let gtid = commit_gtid(&txn).to_string();
    let gtid_set_after = commit_gtid_set_after(&txn).to_string();
    let prepared = txn.position.as_ref().and_then(prepared_gtid);
    // A part that is not the last is not a commit: the transaction is not
    // executed until its last part, and a restart before it reads the
    // transaction again from its start.
    let ends = txn.ends_transaction();
    match ctx.delivery_unit {
        DeliveryUnit::Row => {
            for stmt in &txn.ddl {
                on_row(ddl::ddl_cloud_event(
                    ctx.source_id,
                    stmt,
                    &gtid,
                    &gtid_set_after,
                ))?;
            }
            for ev in txn.events {
                on_row(ev)?;
            }
        }
        DeliveryUnit::Transaction => on_txn(txn)?,
    }

    if !ends {
        return Ok(());
    }
    if let Some(prepared) = prepared {
        ctx.executed.add_committed(&prepared)?;
    }
    ctx.executed.add_committed(&gtid)?;
    persist_gtid_set(
        ctx.executed.to_set_string(),
        ctx.checkpoint_store,
        ctx.checkpoint,
    )
    .await
}

/// Hand a position with nothing for the consumer over, in transaction mode,
/// and persist it on success: the transaction it stands for is executed all
/// the same. Row mode has no use for it, and a sink would get one call per
/// unrelated transaction.
pub async fn deliver_progress<G>(
    position: SourcePosition,
    ctx: &mut DeliverCtx<'_>,
    on_delivery: &mut G,
) -> Result<()>
where
    G: FnMut(Delivery) -> Result<()>,
{
    let gtid = position
        .at
        .get("gtid")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let prepared = prepared_gtid(&position);
    if ctx.delivery_unit == DeliveryUnit::Transaction {
        on_delivery(Delivery::Progress { position })?;
    }
    if let Some(prepared) = prepared {
        ctx.executed.add_committed(&prepared)?;
    }
    ctx.executed.add_committed(&gtid)?;
    persist_gtid_set(
        ctx.executed.to_set_string(),
        ctx.checkpoint_store,
        ctx.checkpoint,
    )
    .await
}

pub(crate) async fn persist_gtid_set(
    gtid_set: String,
    checkpoint_store: &mut Option<Box<dyn CheckpointStore>>,
    checkpoint: &mut Option<Checkpoint>,
) -> Result<()> {
    if gtid_set.is_empty() {
        return Ok(());
    }
    let mut payload = checkpoint
        .as_ref()
        .map(|cp| cp.payload.clone())
        .unwrap_or_else(|| serde_json::json!({}));
    if let Some(obj) = payload.as_object_mut() {
        obj.insert("gtid".into(), serde_json::Value::String(gtid_set));
    }
    let cp = Checkpoint {
        adapter: "mysql".into(),
        payload,
    };
    if let Some(store) = checkpoint_store {
        store.save(&cp).await?;
    }
    *checkpoint = Some(cp);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gtid::commit_position;
    use ce_stream_core::error::Error;
    use ce_stream_core::event::{ChangeOp, TableRef};
    use std::cell::Cell;
    use std::sync::Mutex;

    struct MemStore {
        saved: Mutex<Option<Checkpoint>>,
    }

    #[async_trait::async_trait]
    impl CheckpointStore for MemStore {
        async fn load(&self) -> Result<Option<Checkpoint>> {
            Ok(self.saved.lock().unwrap().clone())
        }

        async fn save(&self, checkpoint: &Checkpoint) -> Result<()> {
            *self.saved.lock().unwrap() = Some(checkpoint.clone());
            Ok(())
        }
    }

    fn deliver_ctx<'a>(
        executed: &'a mut ExecutedSet,
        checkpoint_store: &'a mut Option<Box<dyn CheckpointStore>>,
        checkpoint: &'a mut Option<Checkpoint>,
        delivery_unit: DeliveryUnit,
    ) -> DeliverCtx<'a> {
        DeliverCtx {
            source_id: "mysql://test",
            delivery_unit,
            executed,
            checkpoint_store,
            checkpoint,
        }
    }

    fn sample_txn(gtid: &str, rows: u32) -> CommittedTransaction {
        let events: Vec<_> = (0..rows)
            .map(|_| {
                CloudEvent::row_change(
                    "mysql://test",
                    &TableRef::new("db", "t"),
                    ChangeOp::Insert,
                    serde_json::json!({"op": "insert"}),
                    serde_json::Map::new(),
                )
            })
            .collect();
        CommittedTransaction {
            part: None,
            position: Some(commit_position(gtid, gtid)),
            ddl: vec![],
            events,
            ..Default::default()
        }
    }

    /// A part that is not the last is delivered and moves nothing: neither
    /// the executed set nor the checkpoint. The last part moves both.
    #[tokio::test]
    async fn only_the_last_part_moves_the_checkpoint() {
        use ce_stream_core::transaction::TransactionPart;
        let mut executed = ExecutedSet::default();
        let store = MemStore {
            saved: Mutex::new(None),
        };
        let mut checkpoint_store: Option<Box<dyn CheckpointStore>> = Some(Box::new(store));
        let mut checkpoint = None;
        let delivered = Cell::new(0u32);
        let mut first = sample_txn("3e11fa47-71ca-11e1-9e33-c080020c9a66:1", 2);
        first.position = Some(crate::gtid::commit_position(
            "3e11fa47-71ca-11e1-9e33-c080020c9a66:1",
            "",
        ));
        first.part = Some(TransactionPart {
            number: 1,
            last: false,
        });
        deliver_committed(
            first,
            &mut deliver_ctx(
                &mut executed,
                &mut checkpoint_store,
                &mut checkpoint,
                DeliveryUnit::Transaction,
            ),
            &mut |_| Ok(()),
            &mut |_| {
                delivered.set(delivered.get() + 1);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(delivered.get(), 1);
        assert!(checkpoint.is_none(), "a part is not a commit");
        assert_eq!(executed.to_set_string(), "");
        let mut last = sample_txn("3e11fa47-71ca-11e1-9e33-c080020c9a66:1", 1);
        last.part = Some(TransactionPart {
            number: 2,
            last: true,
        });
        deliver_committed(
            last,
            &mut deliver_ctx(
                &mut executed,
                &mut checkpoint_store,
                &mut checkpoint,
                DeliveryUnit::Transaction,
            ),
            &mut |_| Ok(()),
            &mut |_| {
                delivered.set(delivered.get() + 1);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(delivered.get(), 2);
        assert!(checkpoint.is_some());
        assert_eq!(
            executed.to_set_string(),
            "3e11fa47-71ca-11e1-9e33-c080020c9a66:1-1"
        );
    }

    #[tokio::test]
    async fn checkpoint_after_full_row_fanout() {
        let mut executed = ExecutedSet::default();
        let store = MemStore {
            saved: Mutex::new(None),
        };
        let mut checkpoint_store: Option<Box<dyn CheckpointStore>> = Some(Box::new(store));
        let mut checkpoint = None;
        let calls = Cell::new(0u32);

        let txn = sample_txn("abc:1", 3);
        deliver_committed(
            txn,
            &mut deliver_ctx(
                &mut executed,
                &mut checkpoint_store,
                &mut checkpoint,
                DeliveryUnit::Row,
            ),
            &mut |_| {
                calls.set(calls.get() + 1);
                Ok(())
            },
            &mut |_| Ok(()),
        )
        .await
        .unwrap();

        assert_eq!(calls.get(), 3);
        assert_eq!(executed.to_set_string(), "abc:1-1");
        assert_eq!(
            checkpoint
                .as_ref()
                .and_then(|c| c.payload.get("gtid"))
                .and_then(|v| v.as_str()),
            Some("abc:1-1")
        );
    }

    #[tokio::test]
    async fn no_checkpoint_when_row_callback_fails() {
        let mut executed = ExecutedSet::default();
        let store = MemStore {
            saved: Mutex::new(None),
        };
        let mut checkpoint_store: Option<Box<dyn CheckpointStore>> = Some(Box::new(store));
        let mut checkpoint = None;
        let calls = Cell::new(0u32);

        let txn = sample_txn("abc:1", 3);
        let err = deliver_committed(
            txn,
            &mut deliver_ctx(
                &mut executed,
                &mut checkpoint_store,
                &mut checkpoint,
                DeliveryUnit::Row,
            ),
            &mut |_| {
                calls.set(calls.get() + 1);
                if calls.get() == 2 {
                    return Err(Error::Sink("fail".into()));
                }
                Ok(())
            },
            &mut |_| Ok(()),
        )
        .await;

        assert!(err.is_err());
        assert!(executed.to_set_string().is_empty());
        assert!(checkpoint.is_none());
    }

    #[tokio::test]
    async fn row_mode_ddl_before_rows_in_fanout() {
        use ce_stream_core::transaction::DdlStatement;

        let mut executed = ExecutedSet::default();
        let mut checkpoint_store: Option<Box<dyn CheckpointStore>> = None;
        let mut checkpoint = None;
        let mut types: Vec<String> = Vec::new();

        let txn = CommittedTransaction {
            part: None,
            position: Some(commit_position("abc:1", "abc:1-1")),
            ddl: vec![DdlStatement {
                schema: "app".into(),
                query: "ALTER TABLE t ADD COLUMN x INT".into(),
            }],
            events: sample_txn("abc:1", 3).events,
            ..Default::default()
        };

        deliver_committed(
            txn,
            &mut deliver_ctx(
                &mut executed,
                &mut checkpoint_store,
                &mut checkpoint,
                DeliveryUnit::Row,
            ),
            &mut |ev| {
                types.push(ev.ty.clone());
                Ok(())
            },
            &mut |_| Ok(()),
        )
        .await
        .unwrap();

        assert_eq!(types.len(), 4);
        assert_eq!(types[0], crate::ddl::DDL_CE_TYPE);
        assert!(types[1..]
            .iter()
            .all(|t| t.starts_with("io.ce-stream.row.")));
    }

    #[tokio::test]
    async fn ddl_only_commit_row_mode() {
        use ce_stream_core::transaction::DdlStatement;

        let mut executed = ExecutedSet::default();
        let mut checkpoint_store: Option<Box<dyn CheckpointStore>> = None;
        let mut checkpoint = None;
        let mut count = 0u32;

        deliver_committed(
            CommittedTransaction {
                part: None,
                position: Some(commit_position("abc:2", "abc:1-2")),
                ddl: vec![DdlStatement {
                    schema: "app".into(),
                    query: "CREATE TABLE t (id INT)".into(),
                }],
                events: vec![],
                ..Default::default()
            },
            &mut deliver_ctx(
                &mut executed,
                &mut checkpoint_store,
                &mut checkpoint,
                DeliveryUnit::Row,
            ),
            &mut |ev| {
                count += 1;
                assert_eq!(ev.ty, crate::ddl::DDL_CE_TYPE);
                Ok(())
            },
            &mut |_| Ok(()),
        )
        .await
        .unwrap();

        assert_eq!(count, 1);
        assert_eq!(executed.to_set_string(), "abc:2-2");
    }

    #[tokio::test]
    async fn transaction_mode_envelope_includes_ddl() {
        use ce_stream_core::transaction::DdlStatement;

        let mut executed = ExecutedSet::default();
        let mut checkpoint_store: Option<Box<dyn CheckpointStore>> = None;
        let mut checkpoint = None;
        let mut received: Option<CommittedTransaction> = None;

        deliver_committed(
            CommittedTransaction {
                part: None,
                position: Some(commit_position("abc:1", "abc:1-1")),
                ddl: vec![DdlStatement {
                    schema: "app".into(),
                    query: "ALTER TABLE t ADD COLUMN x INT".into(),
                }],
                events: sample_txn("abc:1", 3).events,
                ..Default::default()
            },
            &mut deliver_ctx(
                &mut executed,
                &mut checkpoint_store,
                &mut checkpoint,
                DeliveryUnit::Transaction,
            ),
            &mut |_| Ok(()),
            &mut |txn| {
                received = Some(txn);
                Ok(())
            },
        )
        .await
        .unwrap();

        let txn = received.expect("envelope delivered");
        assert_eq!(txn.ddl.len(), 1);
        assert_eq!(txn.events.len(), 3);
    }
}
