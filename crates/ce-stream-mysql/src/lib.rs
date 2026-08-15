//! MySQL 9.x ROW binlog → [`ce_stream_core::CloudEvent`].

mod binlog_dispatch;
mod ddl;
mod dispatch;
mod gate;
mod gtid;
mod map;
mod txn_buffer;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use binlog_dispatch::{dispatch_binlog_event, BinlogDispatchCtx, TableMap};
use ce_stream_core::{
    error::{Error, Result},
    event::{CloudEvent, PayloadMode},
    source::{ChangeSource, DeliveryUnit, SourceConfig},
    transaction::CommittedTransaction,
    Checkpoint, CheckpointStore,
};
use tracing::info;

pub use ddl::DDL_CE_TYPE;
pub use dispatch::{deliver_committed, DeliverCtx};
pub use gate::{validate_capture_gates, GateReport};
pub use gtid::ExecutedSet;
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
}

impl MysqlBinlogSource {
    fn include_set(&self) -> HashSet<String> {
        self.config
            .include_tables
            .iter()
            .map(|t| t.as_subject())
            .collect()
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

    async fn run_capture<RowFn, TxnFn>(
        &mut self,
        mut on_row: RowFn,
        mut on_txn: TxnFn,
    ) -> Result<()>
    where
        RowFn: FnMut(CloudEvent) -> Result<()> + Send,
        TxnFn: FnMut(CommittedTransaction) -> Result<()> + Send,
    {
        let url = self.options.connection_url();
        let server_id = self.options.server_id;

        if !self.skip_gate_check {
            let report = gate::validate_capture_gates(&url).await?;
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
        let include = self.include_set();
        let payload_mode = self.config.payload_mode;
        let delivery_unit = self.config.delivery_unit;
        let capacity = self.config.queue_capacity.max(1);

        let executed = Arc::new(tokio::sync::Mutex::new(self.initial_executed_set()?));
        let executed_bg = Arc::clone(&executed);

        info!(
            server_id,
            source = %source_id,
            include = ?include,
            ?payload_mode,
            ?delivery_unit,
            queue_capacity = capacity,
            "starting MySQL binlog source"
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel::<
            std::result::Result<CommittedTransaction, String>,
        >(capacity);
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
                tx,
                stop_bg,
            ))
        });

        let result = async {
            while let Some(msg) = rx.recv().await {
                match msg {
                    Ok(txn) => {
                        let mut shared = executed.lock().await;
                        deliver_committed(
                            txn,
                            &mut DeliverCtx {
                                source_id: &source_id_delivery,
                                delivery_unit,
                                executed: &mut shared,
                                checkpoint_store: &mut self.checkpoint_store,
                                checkpoint: &mut self.checkpoint,
                            },
                            &mut on_row,
                            &mut on_txn,
                        )
                        .await?;
                    }
                    Err(e) => return Err(Error::Source(e)),
                }
            }
            Ok(())
        }
        .await;

        stop.store(true, Ordering::SeqCst);
        let _ = join.await;

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

    async fn run_transactions<F>(&mut self, on_txn: F) -> Result<()>
    where
        F: FnMut(CommittedTransaction) -> Result<()> + Send,
    {
        if self.config.delivery_unit != DeliveryUnit::Transaction {
            return Err(Error::Source(
                "run_transactions() requires source.delivery_unit = transaction".into(),
            ));
        }
        self.run_capture(|_| Ok(()), on_txn).await
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
    include: HashSet<String>,
    executed: Arc<tokio::sync::Mutex<ExecutedSet>>,
    payload_mode: PayloadMode,
    tx: tokio::sync::mpsc::Sender<std::result::Result<CommittedTransaction, String>>,
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
    let mut txn = TxnBuffer::default();

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

/// File-backed checkpoint (GTID JSON in payload).
pub struct FileCheckpointStore {
    pub path: std::path::PathBuf,
}

#[async_trait]
impl CheckpointStore for FileCheckpointStore {
    async fn load(&self) -> Result<Option<Checkpoint>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let bytes = tokio::fs::read(&self.path).await?;
        let cp: Checkpoint = serde_json::from_slice(&bytes)?;
        Ok(Some(cp))
    }

    async fn save(&self, checkpoint: &Checkpoint) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let bytes = serde_json::to_vec_pretty(checkpoint)?;
        tokio::fs::write(&self.path, bytes).await?;
        Ok(())
    }
}

#[doc(hidden)]
pub mod test_support {
    pub use crate::binlog_dispatch::{
        dispatch_binlog_event_for_test as dispatch, BinlogDispatchCtx, TableMap,
    };
}
