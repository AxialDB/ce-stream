//! MySQL 9.x ROW binlog → [`ce_stream_core::CloudEvent`].

// See the same allow in `ce-stream-core`: async_trait vs clippy 1.99 `double_must_use`.
#![allow(clippy::double_must_use)]

mod binlog_dispatch;
pub mod charset;
mod columns;
mod ddl;
mod dispatch;
mod gate;
mod gtid;
mod json_text;
mod map;
mod txn_buffer;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use binlog_dispatch::{dispatch_binlog_event, BinlogDispatchCtx, TableMap};
use ce_stream_core::{
    delivery::{CheckpointMode, Confirm, Delivery, SeedCut},
    error::{Error, Result},
    event::{CloudEvent, PayloadMode},
    gate::GateReport,
    include::IncludeList,
    queue::{self, QueueBytes},
    source::{ChangeSource, DeliveryUnit, SourceConfig},
    transaction::SourcePosition,
    Checkpoint, CheckpointStore,
};
use tracing::info;

pub use ddl::DDL_CE_TYPE;
pub use dispatch::{deliver_committed, deliver_progress, DeliverCtx};
pub use gate::validate_capture_gates;
pub use gtid::{commit_gtid, commit_gtid_set_after, commit_position, covers_position, ExecutedSet};
pub use map::column_value_to_json;
pub use txn_buffer::TxnBuffer;

use mysql_binlog_connector_rust::binlog_client::{BinlogClient, StartPosition};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

#[derive(Debug, Clone)]
pub struct MysqlSourceOptions {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    /// Unique replica server_id for this capture client.
    pub server_id: u64,
    /// Require TLS (`ssl-mode=required`).
    pub tls: bool,
}

impl MysqlSourceOptions {
    pub fn connection_url(&self) -> String {
        let user = utf8_percent_encode(&self.user, NON_ALPHANUMERIC);
        let pass = utf8_percent_encode(&self.password, NON_ALPHANUMERIC);
        let ssl = if self.tls {
            "ssl-mode=required"
        } else {
            "ssl-mode=disabled"
        };
        format!("mysql://{user}:{pass}@{}:{}/?{ssl}", self.host, self.port)
    }
}

pub struct MysqlBinlogSource {
    pub options: MysqlSourceOptions,
    pub config: SourceConfig,
    pub checkpoint: Option<Checkpoint>,
    pub checkpoint_store: Option<Box<dyn CheckpointStore>>,
    /// Skip [`validate_capture_gates`] (lab escape hatch only).
    pub skip_gate_check: bool,
    /// Live include list. Clone this handle before `run` / `run_transactions`
    /// to add or remove tables without ending the dump thread.
    ///
    /// If never mutated, capture start copies [`SourceConfig::include_tables`].
    /// Updates take effect at the next GTID (best-effort; no ack).
    pub include: IncludeList,
    /// The handle for [`CheckpointMode::OnConfirm`]; from [`new_confirm`].
    pub confirm: Confirm,
}

/// A [`Confirm`] that orders positions as this adapter does.
pub fn new_confirm() -> Confirm {
    Confirm::new(gtid::ADAPTER, covers_position)
}

/// The seed cut on MySQL, on the consumer's connection details and account: a
/// GTID set names finished transactions, so there is nothing to wait for.
pub struct MysqlSeedCut {
    pub options: MysqlSourceOptions,
}

#[async_trait]
impl SeedCut for MysqlSeedCut {
    async fn mark(&self) -> Result<SourcePosition> {
        let url = self.options.connection_url();
        let set = gate::fetch_global_gtid(&url, "gtid_executed").await?;
        Ok(SourcePosition {
            adapter: gtid::ADAPTER.into(),
            at: serde_json::json!({ "gtid": set }),
            after: serde_json::json!({ "gtid": set }),
        })
    }

    async fn wait_settled(&self, _mark: &SourcePosition) -> Result<()> {
        Ok(())
    }
}

impl MysqlBinlogSource {
    /// Cloneable handle to [`Self::include`]. Same Arc as the dump thread uses.
    pub fn include_handle(&self) -> IncludeList {
        self.include.clone()
    }

    fn initial_executed_set(&self) -> Result<ExecutedSet> {
        if let Some(cp) = &self.checkpoint {
            if let Some(gtid) = cp.payload.get("gtid").and_then(|v| v.as_str()) {
                return ExecutedSet::from_set_string(gtid);
            }
        }
        Ok(ExecutedSet::default())
    }

    fn seed_baseline_gtid(&mut self, baseline: &str) {
        if baseline.trim().is_empty() {
            return;
        }
        let mut cp = self.checkpoint.clone().unwrap_or_else(|| Checkpoint {
            adapter: "mysql".into(),
            payload: serde_json::json!({}),
        });
        let has_baseline = cp
            .payload
            .get("baseline_gtid")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty());
        if !has_baseline {
            cp.payload["baseline_gtid"] = serde_json::Value::String(baseline.to_string());
            self.checkpoint = Some(cp);
        }
    }

    async fn resolve_binlog_start(&self) -> Result<StartPosition> {
        let Some(cp) = &self.checkpoint else {
            return Ok(StartPosition::Latest);
        };
        if cp.adapter != "mysql" {
            return Ok(StartPosition::Latest);
        }
        let Some(checkpoint_gtid) = cp
            .payload
            .get("gtid")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        else {
            return Ok(StartPosition::Latest);
        };

        let baseline = cp.payload.get("baseline_gtid").and_then(|v| v.as_str());
        let url = self.options.connection_url();
        let purged = gate::fetch_global_gtid(&url, "gtid_purged").await?;
        let handshake = ExecutedSet::binlog_handshake_gtid_set(checkpoint_gtid, baseline, &purged)?;
        info!(handshake_gtid = %handshake, "resuming binlog from checkpoint");
        Ok(StartPosition::Gtid(handshake))
    }

    /// A confirmed position: the checkpoint moves to it, keeping what else the
    /// checkpoint holds.
    async fn save_confirmed(&mut self, position: &SourcePosition) -> Result<()> {
        let set = position
            .after
            .get("gtid")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        dispatch::persist_gtid_set(set, &mut self.checkpoint_store, &mut self.checkpoint).await
    }

    async fn run_capture<RowFn, DeliveryFn>(
        &mut self,
        mut on_row: RowFn,
        mut on_delivery: DeliveryFn,
    ) -> Result<()>
    where
        RowFn: FnMut(CloudEvent) -> Result<()> + Send,
        DeliveryFn: FnMut(Delivery) -> Result<()> + Send,
    {
        let url = self.options.connection_url();
        let server_id = self.options.server_id;

        if !self.skip_gate_check {
            let report = gate::validate_capture_gates(&url).await?.into_result()?;
            for warning in &report.warnings {
                tracing::warn!(gate_warning = %warning, "capture gate warning");
            }
            info!("capture gates passed");
        }

        let start = self.resolve_binlog_start().await?;
        if matches!(start, StartPosition::Latest) {
            let baseline = gate::fetch_global_gtid(&url, "gtid_executed").await?;
            self.seed_baseline_gtid(&baseline);
            info!(baseline_gtid = %baseline, "recorded connect baseline for GTID resume");
        }

        let source_id = self.config.source_id.clone();
        self.include
            .seed_if_uninitialized(&self.config.include_tables);
        let include = self.include.clone();
        let payload_mode = self.config.payload_mode;
        let delivery_unit = self.config.delivery_unit;
        let capacity = self.config.queue_capacity.max(1);
        let part_limits = (self.config.part_max_rows, self.config.part_max_bytes);
        let queue_max_bytes = if self.config.queue_max_bytes == 0 {
            ce_stream_core::source::DEFAULT_QUEUE_MAX_BYTES
        } else {
            self.config.queue_max_bytes
        };
        let queue_bytes = Arc::new(QueueBytes::new(queue_max_bytes as u64));
        let queue_bytes_bg = Arc::clone(&queue_bytes);

        let executed = Arc::new(tokio::sync::Mutex::new(self.initial_executed_set()?));
        let executed_bg = Arc::clone(&executed);

        info!(
            server_id,
            source = %source_id,
            include = ?include.subjects(),
            include_all = include.is_all(),
            ?payload_mode,
            ?delivery_unit,
            queue_capacity = capacity,
            part_max_rows = part_limits.0,
            part_max_bytes = part_limits.1,
            queue_max_bytes,
            "starting MySQL binlog source"
        );

        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<std::result::Result<Delivery, String>>(capacity);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_bg = Arc::clone(&stop);
        let source_id_delivery = source_id.clone();

        let join = tokio::task::spawn_blocking(move || {
            async_std::task::block_on(binlog_loop(
                url,
                server_id,
                start,
                source_id,
                include,
                executed_bg,
                payload_mode,
                part_limits,
                queue_bytes_bg,
                tx,
                stop_bg,
            ))
        });

        // Under OnConfirm a delivery moves nothing durable: the executed set
        // the reader counts from still moves, and the checkpoint on a confirm.
        let on_confirm = self.config.checkpoint == CheckpointMode::OnConfirm;
        let mut confirmed = self.confirm.subscribe();
        let mut no_store: Option<Box<dyn CheckpointStore>> = None;
        let mut unsaved: Option<Checkpoint> = None;

        let result = async {
            loop {
                let msg = tokio::select! {
                    msg = rx.recv() => msg,
                    changed = confirmed.changed(), if on_confirm => {
                        if changed.is_ok() {
                            let position = confirmed.borrow_and_update().clone();
                            if let Some(position) = position {
                                self.save_confirmed(&position).await?;
                            }
                        }
                        continue;
                    }
                };
                let Some(msg) = msg else {
                    return Ok(());
                };
                let delivery = msg.map_err(Error::Source)?;
                let mut shared = executed.lock().await;
                let (store, checkpoint) = if on_confirm {
                    (&mut no_store, &mut unsaved)
                } else {
                    (&mut self.checkpoint_store, &mut self.checkpoint)
                };
                let mut ctx = DeliverCtx {
                    source_id: &source_id_delivery,
                    delivery_unit,
                    executed: &mut shared,
                    checkpoint_store: store,
                    checkpoint,
                };
                match delivery {
                    Delivery::Commit(txn) => {
                        let bytes = queue::delivery_bytes(&txn);
                        deliver_committed(txn, &mut ctx, &mut on_row, &mut |txn| {
                            on_delivery(Delivery::Commit(txn))
                        })
                        .await?;
                        queue_bytes.release(bytes);
                    }
                    Delivery::Progress { position } => {
                        deliver_progress(position, &mut ctx, &mut on_delivery).await?;
                    }
                    Delivery::Schema(schema) => match delivery_unit {
                        DeliveryUnit::Row => on_row(schema.to_cloud_event(&source_id_delivery))?,
                        DeliveryUnit::Transaction => on_delivery(Delivery::Schema(schema))?,
                    },
                }
            }
        }
        .await;

        // The reader may be blocked handing the next transaction (or part) to
        // a queue nobody reads any more: closing the queue ends its wait.
        stop.store(true, Ordering::SeqCst);
        queue_bytes.close();
        drop(rx);
        if result.is_ok() {
            let _ = join.await;
        }
        // After an error the reader ends at its next event or send; the
        // caller gets the error now instead of after the server drops a dump
        // that is no longer read.

        result
    }
}

#[async_trait]
impl ChangeSource for MysqlBinlogSource {
    async fn run<F>(&mut self, on_event: F) -> Result<()>
    where
        F: FnMut(CloudEvent) -> Result<()> + Send,
    {
        if self.config.delivery_unit != DeliveryUnit::Row {
            return Err(Error::Source(
                "run() requires source.delivery_unit = row".into(),
            ));
        }
        self.run_capture(on_event, |_| Ok(())).await
    }

    async fn run_deliveries<F>(&mut self, on_delivery: F) -> Result<()>
    where
        F: FnMut(Delivery) -> Result<()> + Send,
    {
        if self.config.delivery_unit != DeliveryUnit::Transaction {
            return Err(Error::Source(
                "run_deliveries() requires source.delivery_unit = transaction".into(),
            ));
        }
        self.run_capture(|_| Ok(()), on_delivery).await
    }

    async fn gate(&self) -> Result<GateReport> {
        gate::validate_capture_gates(&self.options.connection_url()).await
    }

    fn confirm(&self) -> Confirm {
        self.confirm.clone()
    }

    fn covers(&self, at: &SourcePosition, target: &SourcePosition) -> Result<bool> {
        covers_position(at, target)
    }
}

fn compression_read_error(err: impl std::fmt::Display) -> String {
    let msg = err.to_string();
    let lower = msg.to_ascii_lowercase();
    if lower.contains("zstd") || lower.contains("decompress") || lower.contains("compression") {
        format!("binlog transaction compression: {msg}")
    } else {
        format!("binlog read: {msg}")
    }
}

#[allow(clippy::too_many_arguments)]
async fn binlog_loop(
    url: String,
    server_id: u64,
    start: StartPosition,
    source_id: String,
    include: IncludeList,
    executed: Arc<tokio::sync::Mutex<ExecutedSet>>,
    payload_mode: PayloadMode,
    part_limits: (usize, usize),
    queue_bytes: Arc<QueueBytes>,
    tx: tokio::sync::mpsc::Sender<std::result::Result<Delivery, String>>,
    stop: Arc<AtomicBool>,
) -> std::result::Result<(), String> {
    let mut client = BinlogClient::new(url.as_str(), server_id, start)
        .with_master_heartbeat(Duration::from_secs(5))
        .with_read_timeout(Duration::from_secs(3));

    let mut stream = client
        .connect()
        .await
        .map_err(|e| format!("binlog connect: {e}"))?;

    info!("binlog connected");

    let mut tables: HashMap<u64, TableMap> = HashMap::new();
    let mut txn = TxnBuffer::with_part_limits(part_limits.0, part_limits.1).with_queue(queue_bytes);

    while !stop.load(Ordering::SeqCst) {
        let read = stream.read().await;
        let (_header, data) = match read {
            Ok(v) => v,
            Err(e) => {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let msg = e.to_string();
                if msg.to_ascii_lowercase().contains("timeout") {
                    continue;
                }
                tracing::error!(error = %msg, "binlog read failed");
                return Err(compression_read_error(e));
            }
        };

        let mut ctx = BinlogDispatchCtx {
            txn: &mut txn,
            executed: &executed,
            tables: &mut tables,
            source_id: &source_id,
            include: &include,
            payload_mode,
            tx: &tx,
        };
        dispatch_binlog_event(&mut ctx, data)?;
    }

    Ok(())
}

#[doc(hidden)]
pub mod test_support {
    pub use crate::binlog_dispatch::{
        dispatch_binlog_event_for_test as dispatch, BinlogDispatchCtx, TableMap,
    };
    use ce_stream_core::delivery::Delivery;
    use ce_stream_core::transaction::CommittedTransaction;

    /// A delivery read off the queue, as the commit it stands for: a position
    /// passed is a commit with nothing in it.
    pub fn commit_of(delivery: Delivery) -> CommittedTransaction {
        match delivery {
            Delivery::Commit(txn) => txn,
            Delivery::Progress { position } => CommittedTransaction {
                position: Some(position),
                ..Default::default()
            },
            Delivery::Schema(schema) => panic!("expected a commit, got {schema:?}"),
        }
    }
}
