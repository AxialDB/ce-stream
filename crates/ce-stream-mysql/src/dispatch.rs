//! Commit-boundary delivery and checkpoint (shared by `run` / `run_transactions`).

use ce_stream_core::error::Result;
use ce_stream_core::transaction::CommittedTransaction;
use ce_stream_core::{Checkpoint, CheckpointStore, CloudEvent, DeliveryUnit};

use crate::ddl;
use crate::gtid::ExecutedSet;

/// Deliver one committed transaction and persist checkpoint on full success.
pub async fn deliver_committed<F, G>(
    txn: CommittedTransaction,
    source_id: &str,
    delivery_unit: DeliveryUnit,
    executed: &mut ExecutedSet,
    checkpoint_store: &mut Option<Box<dyn CheckpointStore>>,
    checkpoint: &mut Option<Checkpoint>,
    on_row: &mut F,
    on_txn: &mut G,
) -> Result<()>
where
    F: FnMut(CloudEvent) -> Result<()>,
    G: FnMut(CommittedTransaction) -> Result<()>,
{
    let gtid = txn.gtid.clone();
    let gtid_set_after = txn.gtid_set_after.clone();
    match delivery_unit {
        DeliveryUnit::Row => {
            for stmt in &txn.ddl {
                on_row(ddl::ddl_cloud_event(
                    source_id,
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

    executed.add_committed(&gtid)?;
    persist_gtid_set(executed.to_set_string(), checkpoint_store, checkpoint).await
}

async fn persist_gtid_set(
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
            gtid: gtid.into(),
            gtid_set_after: format!("{gtid}"),
            ddl: vec![],
            events,
        }
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
            "mysql://test",
            DeliveryUnit::Row,
            &mut executed,
            &mut checkpoint_store,
            &mut checkpoint,
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
            "mysql://test",
            DeliveryUnit::Row,
            &mut executed,
            &mut checkpoint_store,
            &mut checkpoint,
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
            gtid: "abc:1".into(),
            gtid_set_after: "abc:1-1".into(),
            ddl: vec![DdlStatement {
                schema: "app".into(),
                query: "ALTER TABLE t ADD COLUMN x INT".into(),
            }],
            events: sample_txn("abc:1", 3).events,
        };

        deliver_committed(
            txn,
            "mysql://test",
            DeliveryUnit::Row,
            &mut executed,
            &mut checkpoint_store,
            &mut checkpoint,
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
                gtid: "abc:2".into(),
                gtid_set_after: "abc:1-2".into(),
                ddl: vec![DdlStatement {
                    schema: "app".into(),
                    query: "CREATE TABLE t (id INT)".into(),
                }],
                events: vec![],
            },
            "mysql://test",
            DeliveryUnit::Row,
            &mut executed,
            &mut checkpoint_store,
            &mut checkpoint,
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
                gtid: "abc:1".into(),
                gtid_set_after: "abc:1-1".into(),
                ddl: vec![DdlStatement {
                    schema: "app".into(),
                    query: "ALTER TABLE t ADD COLUMN x INT".into(),
                }],
                events: sample_txn("abc:1", 3).events,
            },
            "mysql://test",
            DeliveryUnit::Transaction,
            &mut executed,
            &mut checkpoint_store,
            &mut checkpoint,
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
