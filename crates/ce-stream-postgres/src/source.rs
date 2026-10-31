//! The replication stream: a reader task that decodes, a task that keeps the
//! server alive on its own timer, and the consumer apart from both.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ce_stream_core::delivery::{has_resume_payload, CheckpointMode, Confirm, Delivery};
use ce_stream_core::error::{Error, Result};
use ce_stream_core::event::{CloudEvent, TableRef};
use ce_stream_core::gate::GateReport;
use ce_stream_core::include::IncludeList;
use ce_stream_core::queue::{delivery_bytes, QueueBytes};
use ce_stream_core::source::{ChangeSource, DeliveryUnit, SourceConfig, DEFAULT_QUEUE_MAX_BYTES};
use ce_stream_core::transaction::SourcePosition;
use ce_stream_core::{Checkpoint, CheckpointStore};
use serde_json::json;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};

use crate::assemble::{Assembler, PartitionRoot};
use crate::gate::validate_capture_gates;
use crate::lsn::Lsn;
use crate::options::{PostgresOptions, PostgresStreamOptions};
use crate::pgoutput::decode;
use crate::position::{after_lsn, covers_position, ADAPTER};
use crate::slots::read_figures;
use crate::wire::{
    map_server_error, parse_error, parse_stream_frame, quote_ident, quote_literal, read_message,
    status_update, Connection, StreamFrame,
};

/// The most the status updates are apart, whatever `wal_sender_timeout` is.
const MAX_STATUS_INTERVAL: Duration = Duration::from_secs(10);
const MIN_STATUS_INTERVAL: Duration = Duration::from_millis(100);
/// How often the checkpoint reaches the disk and the slot moves. An fsync per
/// commit would hold the stream to the disk's rate.
const SYNC_INTERVAL: Duration = Duration::from_secs(1);

pub struct PostgresSource {
    pub options: PostgresOptions,
    pub stream: PostgresStreamOptions,
    pub config: SourceConfig,
    pub checkpoint: Option<Checkpoint>,
    pub checkpoint_store: Option<Box<dyn CheckpointStore>>,
    pub skip_gate_check: bool,
    pub include: IncludeList,
    /// The handle for [`CheckpointMode::OnConfirm`]; from [`new_confirm`].
    pub confirm: Confirm,
}

/// A [`Confirm`] that orders positions as this adapter does.
pub fn new_confirm() -> Confirm {
    Confirm::new(ADAPTER, covers_position)
}

/// What a stored checkpoint says.
#[derive(Debug, PartialEq, Eq)]
struct Stored {
    lsn: Lsn,
    system_id: Option<String>,
    timeline: Option<String>,
}

fn stored_checkpoint(checkpoint: &Checkpoint) -> Result<Stored> {
    if checkpoint.adapter != ADAPTER {
        return Err(Error::Checkpoint(format!(
            "a {} checkpoint on a {ADAPTER} source",
            checkpoint.adapter
        )));
    }
    let text = checkpoint.payload["lsn"]
        .as_str()
        .ok_or_else(|| Error::Checkpoint(format!("no lsn in {}", checkpoint.payload)))?;
    let field = |name: &str| checkpoint.payload[name].as_str().map(str::to_string);
    Ok(Stored {
        lsn: Lsn::parse(text)?,
        system_id: field("system_id"),
        timeline: field("timeline"),
    })
}

/// A checkpoint from another server is its own error: the LSN means nothing there.
fn check_same_server(stored: &Stored, system_id: &str, timeline: &str) -> Result<()> {
    if let Some(id) = stored.system_id.as_deref().filter(|id| *id != system_id) {
        return Err(Error::Checkpoint(format!(
            "the checkpoint is of system {id} and this server is {system_id}; the position means nothing here"
        )));
    }
    if let Some(line) = stored.timeline.as_deref().filter(|line| *line != timeline) {
        return Err(Error::Checkpoint(format!(
            "the checkpoint is on timeline {line} and this server is on {timeline}; the position may not exist on this history"
        )));
    }
    Ok(())
}

/// `wal_sender_timeout` in milliseconds to the time between status updates.
fn status_interval(timeout_ms: u64) -> Duration {
    if timeout_ms == 0 {
        return MAX_STATUS_INTERVAL;
    }
    Duration::from_millis(timeout_ms / 4).clamp(MIN_STATUS_INTERVAL, MAX_STATUS_INTERVAL)
}

fn start_command(slot: &str, publication: &str, start: Lsn) -> String {
    format!(
        "START_REPLICATION SLOT {} LOGICAL {start} (proto_version '1', publication_names {}, messages 'true')",
        quote_ident(slot),
        quote_literal(&quote_ident(publication))
    )
}

/// What the consumer's loop has confirmed.
struct Confirmed {
    /// In the checkpoint: the consumer has it.
    saved: AtomicU64,
    /// On disk too: the writer sends this to the server, which forgets what lies
    /// behind it.
    durable: AtomicU64,
    wake: Notify,
}

impl Confirmed {
    fn starting_at(lsn: Lsn) -> Self {
        Self {
            saved: AtomicU64::new(lsn.0),
            durable: AtomicU64::new(lsn.0),
            wake: Notify::new(),
        }
    }

    fn note_saved(&self, lsn: Lsn) {
        self.saved.fetch_max(lsn.0, Ordering::SeqCst);
    }

    fn saved(&self) -> Lsn {
        Lsn(self.saved.load(Ordering::SeqCst))
    }

    fn make_durable(&self, lsn: Lsn) {
        self.durable.fetch_max(lsn.0, Ordering::SeqCst);
        self.wake.notify_one();
    }

    fn durable(&self) -> Lsn {
        Lsn(self.durable.load(Ordering::SeqCst))
    }
}

/// Keeps the server alive whatever the consumer does. Nothing short of durable
/// goes to the server: this is what lets the slot move on.
async fn write_status<W: AsyncWrite + Unpin>(
    mut writer: W,
    confirmed: Arc<Confirmed>,
    every: Duration,
) {
    let mut timer = tokio::time::interval(every);
    loop {
        tokio::select! {
            _ = timer.tick() => {}
            _ = confirmed.wake.notified() => {}
        }
        let frame = status_update(confirmed.durable());
        if writer.write_all(&frame).await.is_err() || writer.flush().await.is_err() {
            return;
        }
    }
}

async fn partition_root(options: &PostgresOptions, oid: u32) -> Result<Option<PartitionRoot>> {
    let mut conn = Connection::connect(options, false).await?;
    let rows = conn
        .query(&format!(
            "SELECT c.oid::text, n.nspname, c.relname
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.oid = pg_partition_root({oid}::oid::regclass) AND c.oid <> {oid}::oid"
        ))
        .await?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    match &row[..] {
        [Some(root), Some(schema), Some(table)] => Ok(Some(PartitionRoot {
            oid: root
                .parse()
                .map_err(|_| Error::Source(format!("bad oid {root:?}")))?,
            table: TableRef::new(schema.as_str(), table.as_str()),
        })),
        _ => Ok(None),
    }
}

struct Reader<R> {
    stream: R,
    assembler: Assembler,
    options: PostgresOptions,
    confirmed: Arc<Confirmed>,
    /// The furthest position handed to the consumer.
    handed: Lsn,
    queue: Arc<QueueBytes>,
    tx: mpsc::Sender<Result<Delivery>>,
}

impl<R: AsyncRead + Unpin + Send> Reader<R> {
    async fn run(mut self) {
        let tx = self.tx.clone();
        if let Err(err) = self.read().await {
            let _ = tx.send(Err(err)).await;
        }
    }

    async fn read(&mut self) -> Result<()> {
        loop {
            let (tag, payload) = read_message(&mut self.stream).await?;
            match tag {
                b'd' => match parse_stream_frame(&payload)? {
                    StreamFrame::XLog(data) => {
                        let message = decode(&data)?;
                        for oid in self.assembler.roots_needed(&message) {
                            let root = partition_root(&self.options, oid).await?;
                            self.assembler.set_root(oid, root);
                        }
                        for delivery in self.assembler.push(message)? {
                            if !self.hand_over(delivery).await? {
                                return Ok(());
                            }
                        }
                    }
                    StreamFrame::KeepAlive {
                        wal_end,
                        reply_requested,
                    } => {
                        if reply_requested {
                            self.confirmed.wake.notify_one();
                        }
                        if !self.assembler.in_transaction()
                            && self.confirmed.saved() >= self.handed
                            && wal_end > self.handed
                        {
                            let progress = self.assembler.progress_at(wal_end);
                            if !self.hand_over(progress).await? {
                                return Ok(());
                            }
                        }
                    }
                },
                b'E' => return Err(map_server_error("stream", parse_error(&payload))),
                b'c' | b'f' => return Err(Error::Source("the server ended the stream".into())),
                _ => {}
            }
        }
    }

    /// Wait for room in the queue and pass one delivery on. False when the
    /// consumer is gone.
    async fn hand_over(&mut self, delivery: Delivery) -> Result<bool> {
        if let Some(position) = resume_position(&delivery) {
            self.handed = self.handed.max(after_lsn(position)?);
        }
        let bytes = bytes_of(&delivery);
        let queue = Arc::clone(&self.queue);
        let room = tokio::task::spawn_blocking(move || queue.reserve(bytes)).await;
        if !matches!(room, Ok(Ok(()))) {
            return Ok(false);
        }
        Ok(self.tx.send(Ok(delivery)).await.is_ok())
    }
}

fn bytes_of(delivery: &Delivery) -> u64 {
    match delivery {
        Delivery::Commit(txn) => delivery_bytes(txn),
        _ => 0,
    }
}

/// The position a delivery moves the checkpoint to, if it moves it.
fn resume_position(delivery: &Delivery) -> Option<&SourcePosition> {
    let position = match delivery {
        Delivery::Commit(txn) => txn.position.as_ref(),
        Delivery::Progress { position } => Some(position),
        Delivery::Schema(_) => None,
    }?;
    has_resume_payload(position).then_some(position)
}

impl PostgresSource {
    pub fn include_handle(&self) -> IncludeList {
        self.include.clone()
    }

    fn resume_payload(lsn: Lsn, system_id: &str, timeline: &str) -> serde_json::Value {
        json!({ "lsn": lsn.to_string(), "system_id": system_id, "timeline": timeline })
    }

    /// The checkpoint moves here, on every commit. The server hears of it from
    /// [`Self::make_durable`].
    async fn save_position(
        &mut self,
        lsn: Lsn,
        (system_id, timeline): (&str, &str),
        confirmed: &Confirmed,
    ) -> Result<()> {
        if lsn < confirmed.saved() {
            return Ok(());
        }
        let checkpoint = Checkpoint {
            adapter: ADAPTER.into(),
            payload: Self::resume_payload(lsn, system_id, timeline),
        };
        if let Some(store) = self.checkpoint_store.as_mut() {
            store.save(&checkpoint).await?;
        }
        self.checkpoint = Some(checkpoint);
        confirmed.note_saved(lsn);
        Ok(())
    }

    /// The checkpoint reaches the disk, then the server may forget what lies
    /// behind it, so the slot never moves past a checkpoint a power cut could lose.
    async fn make_durable(&self, confirmed: &Confirmed) -> Result<()> {
        let saved = confirmed.saved();
        if saved > confirmed.durable() {
            if let Some(store) = self.checkpoint_store.as_ref() {
                store.sync().await?;
            }
            confirmed.make_durable(saved);
        }
        Ok(())
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
        self.include
            .seed_if_uninitialized(&self.config.include_tables);
        if !self.skip_gate_check {
            let report = self.gate().await?.into_result()?;
            for warning in &report.warnings {
                tracing::warn!(gate_warning = %warning, "capture gate warning");
            }
        }
        let mut conn = Connection::connect(&self.options, true).await?;
        let identity = conn.query("IDENTIFY_SYSTEM").await?;
        let (system_id, timeline) = match identity.first().map(Vec::as_slice) {
            Some([Some(id), Some(line), ..]) => (id.clone(), line.clone()),
            _ => return Err(Error::Source("IDENTIFY_SYSTEM returned no row".into())),
        };
        let stored = self
            .checkpoint
            .as_ref()
            .map(stored_checkpoint)
            .transpose()?;
        if let Some(stored) = &stored {
            check_same_server(stored, &system_id, &timeline)?;
        }

        let slot = &self.stream.slot;
        let figures = read_figures(&mut conn, slot).await?.ok_or_else(|| {
            Error::HistoryLost(format!("replication slot {slot:?} does not exist"))
        })?;
        if let Some(why) = figures.lost_because() {
            return Err(Error::HistoryLost(format!(
                "replication slot {slot:?}: {why}"
            )));
        }
        let slot_at = figures
            .confirmed_flush_lsn
            .as_deref()
            .map(Lsn::parse)
            .transpose()?
            .unwrap_or_default();
        // The server would start at the slot and say nothing of what lies between.
        let start = match &stored {
            Some(stored) if stored.lsn < slot_at => {
                return Err(Error::HistoryLost(format!(
                    "the checkpoint is at {} and slot {slot:?} has moved on to {slot_at}",
                    stored.lsn
                )))
            }
            Some(stored) => stored.lsn,
            None => slot_at,
        };

        let timeout: u64 = conn
            .query("SELECT setting FROM pg_settings WHERE name = 'wal_sender_timeout'")
            .await?
            .first()
            .and_then(|row| row.first().cloned().flatten())
            .and_then(|text| text.parse().ok())
            .unwrap_or(60_000);
        let command = start_command(slot, &self.stream.publication, start);
        match conn.try_query(&command).await? {
            Ok(None) => {}
            Ok(Some(_)) => return Err(Error::Source("START_REPLICATION did not stream".into())),
            Err(err) => return Err(map_server_error("START_REPLICATION", err)),
        }
        tracing::info!(%slot, %start, part_max_rows = self.config.part_max_rows, "starting PostgreSQL logical replication source");

        let confirmed = Arc::new(Confirmed::starting_at(start));
        let queue_max_bytes = match self.config.queue_max_bytes {
            0 => DEFAULT_QUEUE_MAX_BYTES,
            bytes => bytes,
        };
        let queue = Arc::new(QueueBytes::new(queue_max_bytes as u64));
        let (tx, mut rx) = mpsc::channel(self.config.queue_capacity.max(1));
        let (read_half, write_half) = tokio::io::split(conn.stream);
        let writer = tokio::spawn(write_status(
            write_half,
            Arc::clone(&confirmed),
            status_interval(timeout),
        ));
        let reader = tokio::spawn(
            Reader {
                stream: read_half,
                assembler: Assembler::new(
                    self.config.source_id.clone(),
                    self.config.payload_mode,
                    self.include.clone(),
                    (self.config.part_max_rows, self.config.part_max_bytes),
                    self.stream.message_prefixes.clone(),
                    start,
                ),
                options: self.options.clone(),
                confirmed: Arc::clone(&confirmed),
                handed: start,
                queue: Arc::clone(&queue),
                tx,
            }
            .run(),
        );

        let on_confirm = self.config.checkpoint == CheckpointMode::OnConfirm;
        let server = (system_id.as_str(), timeline.as_str());
        let mut wanted = self.confirm.subscribe();
        let mut sync_timer = tokio::time::interval(SYNC_INTERVAL);
        let result = loop {
            let delivery = tokio::select! {
                delivery = rx.recv() => delivery,
                _ = sync_timer.tick() => {
                    if let Err(err) = self.make_durable(&confirmed).await {
                        break Err(err);
                    }
                    continue;
                }
                changed = wanted.changed(), if on_confirm => {
                    if changed.is_ok() {
                        let position = wanted.borrow_and_update().clone();
                        if let Some(position) = position {
                            let saved = match after_lsn(&position) {
                                Ok(lsn) => self.save_position(lsn, server, &confirmed).await,
                                Err(err) => Err(err),
                            };
                            if let Err(err) = saved {
                                break Err(err);
                            }
                        }
                    }
                    continue;
                }
            };
            let delivery = match delivery {
                None => break Ok(()),
                Some(Err(err)) => break Err(err),
                Some(Ok(delivery)) => delivery,
            };
            let bytes = bytes_of(&delivery);
            let moved = match resume_position(&delivery).map(after_lsn).transpose() {
                Ok(moved) => moved,
                Err(err) => break Err(err),
            };
            let delivered = self.deliver(delivery, &mut on_row, &mut on_delivery);
            if let Err(err) = delivered {
                break Err(err);
            }
            queue.release(bytes);
            if let (false, Some(lsn)) = (on_confirm, moved) {
                if let Err(err) = self.save_position(lsn, server, &confirmed).await {
                    break Err(err);
                }
            }
        };
        queue.close();
        reader.abort();
        writer.abort();
        result
    }

    fn deliver<RowFn, DeliveryFn>(
        &self,
        delivery: Delivery,
        on_row: &mut RowFn,
        on_delivery: &mut DeliveryFn,
    ) -> Result<()>
    where
        RowFn: FnMut(CloudEvent) -> Result<()>,
        DeliveryFn: FnMut(Delivery) -> Result<()>,
    {
        if self.config.delivery_unit == DeliveryUnit::Transaction {
            return on_delivery(delivery);
        }
        match delivery {
            Delivery::Commit(txn) => {
                for control in &txn.control {
                    on_row(control.to_cloud_event(&self.config.source_id, serde_json::Map::new()))?;
                }
                for event in txn.events {
                    on_row(event)?;
                }
                Ok(())
            }
            Delivery::Schema(schema) => on_row(schema.to_cloud_event(&self.config.source_id)),
            // Not delivered in this mode; it still moves the checkpoint.
            Delivery::Progress { .. } => Ok(()),
        }
    }
}

#[async_trait]
impl ChangeSource for PostgresSource {
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
        validate_capture_gates(&self.options, &self.stream, &self.config.include_tables).await
    }

    fn confirm(&self) -> Confirm {
        self.confirm.clone()
    }

    fn covers(&self, at: &SourcePosition, target: &SourcePosition) -> Result<bool> {
        covers_position(at, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint(payload: serde_json::Value) -> Checkpoint {
        Checkpoint {
            adapter: ADAPTER.into(),
            payload,
        }
    }

    #[test]
    fn the_start_command_asks_for_messages_on_protocol_one() {
        assert_eq!(
            start_command("s", "my pub", Lsn(0x10)),
            "START_REPLICATION SLOT \"s\" LOGICAL 0/10 (proto_version '1', publication_names '\"my pub\"', messages 'true')"
        );
    }

    #[test]
    fn status_updates_come_at_a_quarter_of_the_timeout_within_limits() {
        assert_eq!(status_interval(10_000), Duration::from_millis(2_500));
        assert_eq!(status_interval(60_000), MAX_STATUS_INTERVAL);
        assert_eq!(status_interval(0), MAX_STATUS_INTERVAL);
        assert_eq!(status_interval(100), MIN_STATUS_INTERVAL);
    }

    #[test]
    fn a_checkpoint_of_another_server_is_its_own_error() {
        let stored = stored_checkpoint(&checkpoint(
            json!({ "lsn": "0/10", "system_id": "1", "timeline": "1" }),
        ))
        .unwrap();
        assert!(check_same_server(&stored, "1", "1").is_ok());
        let other = check_same_server(&stored, "2", "1").unwrap_err();
        assert!(matches!(other, Error::Checkpoint(_)));
        assert!(other.to_string().contains("system"));
        let other = check_same_server(&stored, "1", "2").unwrap_err();
        assert!(other.to_string().contains("timeline"));
    }

    #[test]
    fn a_checkpoint_from_a_position_alone_is_taken_as_it_is() {
        let stored = stored_checkpoint(&checkpoint(json!({ "lsn": "0/10" }))).unwrap();
        assert_eq!(stored.lsn, Lsn(0x10));
        assert!(check_same_server(&stored, "any", "any").is_ok());
    }

    #[test]
    fn a_checkpoint_that_is_not_ours_is_refused() {
        assert!(stored_checkpoint(&checkpoint(json!({}))).is_err());
        let mut mysql = checkpoint(json!({ "lsn": "0/10" }));
        mysql.adapter = "mysql".into();
        assert!(stored_checkpoint(&mysql).is_err());
    }

    #[test]
    fn a_part_and_a_definition_move_nothing() {
        let part = Delivery::Commit(ce_stream_core::transaction::CommittedTransaction {
            position: Some(crate::position::commit_position(Lsn(1), 1, None)),
            ..Default::default()
        });
        assert!(resume_position(&part).is_none());
        let progress = Delivery::Progress {
            position: crate::position::passed(Lsn(9)),
        };
        assert_eq!(
            after_lsn(resume_position(&progress).unwrap()).unwrap(),
            Lsn(9)
        );
    }
}
