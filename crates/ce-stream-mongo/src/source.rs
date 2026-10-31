//! Change stream loop. Grouping and mapping are tested without a server.

use async_trait::async_trait;
use ce_stream_core::delivery::{CheckpointMode, Confirm, Delivery};
use ce_stream_core::error::{Error, Result};
use ce_stream_core::event::{CloudEvent, PayloadMode};
use ce_stream_core::gate::GateReport;
use ce_stream_core::include::{IncludeFilter, IncludeList};
use ce_stream_core::queue::{delivery_bytes, QueueBytes};
use ce_stream_core::source::{ChangeSource, DeliveryUnit, SourceConfig, DEFAULT_QUEUE_MAX_BYTES};
use ce_stream_core::transaction::{ControlEvent, SourcePosition};
use ce_stream_core::{Checkpoint, CheckpointStore};
use std::sync::Arc;
use std::time::Duration;

use mongodb::bson::{doc, Document, Timestamp};
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken};
use mongodb::change_stream::ChangeStream;
use mongodb::options::FullDocumentType;
use mongodb::{Client, Database};
use serde_json::Value;

use crate::bson_json::{document_image, identity_text};
use crate::checkpoint::{ClusterTime, MongoCheckpoint};
use crate::gate::validate_capture_gates;
use crate::group::{Emit, TxnGrouper};
use crate::map::{map_change, ChangeOpKind, RawChange};
use crate::options::FullDocumentMode;
use crate::HISTORY_LOST_CODE;

#[derive(Clone, Debug)]
pub struct MongoSourceOptions {
    pub uri: String,
    pub database: String,
    pub full_document: FullDocumentMode,
}

/// How long one `getMore` waits for events. An include list that gained a collection is
/// noticed between two of them.
const AWAIT: Duration = Duration::from_millis(250);

/// Where a stream starts.
#[derive(Clone, Debug)]
enum Start {
    Now,
    After(ResumeToken),
    At(Timestamp),
}

/// The part of an include list that this database's stream can deliver.
fn in_this_database(database: &str, filter: &IncludeFilter) -> IncludeFilter {
    match filter {
        IncludeFilter::All => IncludeFilter::All,
        IncludeFilter::Only(subjects) => {
            let prefix = format!("{database}.");
            IncludeFilter::Only(
                subjects
                    .iter()
                    .filter(|subject| subject.starts_with(&prefix))
                    .cloned()
                    .collect(),
            )
        }
    }
}

/// The server-side filter for a finite include list: the collections on the list, and the
/// two events that have no collection and end a stream. `None` for "every table".
///
/// Without it the server prepares and sends every write of the database. With
/// `fullDocument: required` that also fails the stream on an update in any collection that
/// has no post-images, listed or not.
///
/// `resume_after` is the token the stream is opened after. The server has to find that
/// event in the stream, and it looks after the filter has run: a token of a collection that
/// has left the list (it was dropped, or taken off) would be "not found". Its one event is
/// let through by its id; it is the place to start and is never delivered.
fn server_filter(
    database: &str,
    filter: &IncludeFilter,
    resume_after: Option<&ResumeToken>,
) -> Option<Document> {
    let IncludeFilter::Only(subjects) = in_this_database(database, filter) else {
        return None;
    };
    let prefix = format!("{database}.");
    let mut collections: Vec<&str> = subjects
        .iter()
        .filter_map(|subject| subject.strip_prefix(&prefix))
        .collect();
    collections.sort_unstable();
    let mut any = vec![
        doc! { "ns.coll": { "$in": collections } },
        doc! { "operationType": { "$in": ["dropDatabase", "invalidate"] } },
    ];
    if let Some(token) = resume_after.and_then(|token| mongodb::bson::to_bson(token).ok()) {
        any.push(doc! { "_id": token });
    }
    Some(doc! { "$match": { "$or": any } })
}

/// Does a stream opened with `open` deliver everything `wanted` asks of this database?
/// A list that has shrunk is still covered: the filter in this process drops the rest.
fn covers(open: &IncludeFilter, wanted: &IncludeFilter) -> bool {
    match (open, wanted) {
        (IncludeFilter::All, _) => true,
        (IncludeFilter::Only(_), IncludeFilter::All) => false,
        (IncludeFilter::Only(open), IncludeFilter::Only(wanted)) => wanted.is_subset(open),
    }
}

/// Clears the reported list when capture ends, however it ends.
struct InEffect(IncludeList);

impl Drop for InEffect {
    fn drop(&mut self) {
        self.0.set_in_effect(None);
    }
}

pub struct MongoChangeStreamSource {
    pub options: MongoSourceOptions,
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
    Confirm::new(crate::checkpoint::ADAPTER, crate::covers_position)
}

impl MongoChangeStreamSource {
    pub fn include_handle(&self) -> IncludeList {
        self.include.clone()
    }

    /// The checkpoint moves to a position.
    async fn save_position(&mut self, position: &SourcePosition) -> Result<()> {
        let cp = Checkpoint {
            adapter: crate::checkpoint::ADAPTER.into(),
            payload: position.after.clone(),
        };
        if let Some(store) = self.checkpoint_store.as_mut() {
            store.save(&cp).await?;
        }
        self.checkpoint = Some(cp);
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
        let client = Client::with_uri_str(&self.options.uri)
            .await
            .map_err(map_mongo)?;
        self.include
            .seed_if_uninitialized(&self.config.include_tables);
        let watched = self.config.include_tables.clone();
        if !self.skip_gate_check {
            let report = validate_capture_gates(
                &client,
                &self.options.database,
                &watched,
                self.options.full_document,
            )
            .await?
            .into_result()?;
            for warning in &report.warnings {
                tracing::warn!(gate_warning = %warning, "capture gate warning");
            }
        }

        let stored = self
            .checkpoint
            .as_ref()
            .and_then(MongoCheckpoint::from_checkpoint);
        let seed = stored.as_ref().and_then(|s| s.seed_cluster_time);
        let start = match (stored.as_ref().and_then(resume_token), seed) {
            (Some(token), _) => Start::After(token),
            (None, Some(seed_time)) => Start::At(Timestamp {
                time: seed_time.t,
                increment: seed_time.i,
            }),
            (None, None) => Start::Now,
        };
        tracing::info!(
            database = %self.options.database,
            part_max_rows = self.config.part_max_rows,
            part_max_bytes = self.config.part_max_bytes,
            "starting MongoDB change stream source"
        );

        // The reader and the consumer are apart, as on every adapter with a
        // reader task: a slow consumer holds the reader at the queue's limits.
        let queue_max_bytes = match self.config.queue_max_bytes {
            0 => DEFAULT_QUEUE_MAX_BYTES,
            bytes => bytes,
        };
        let queue = Arc::new(QueueBytes::new(queue_max_bytes as u64));
        let (tx, mut rx) = tokio::sync::mpsc::channel(self.config.queue_capacity.max(1));
        let reader = Reader {
            database: client.database(&self.options.database),
            options: self.options.clone(),
            source_id: self.config.source_id.clone(),
            payload_mode: self.config.payload_mode,
            include: self.include.clone(),
            grouper: TxnGrouper::new(seed)
                .with_part_limits(self.config.part_max_rows, self.config.part_max_bytes),
            start,
            queue: Arc::clone(&queue),
            tx,
        };
        let task = tokio::spawn(reader.run());

        let on_confirm = self.config.checkpoint == CheckpointMode::OnConfirm;
        let mut confirmed = self.confirm.subscribe();
        let result = loop {
            let emit = tokio::select! {
                emit = rx.recv() => emit,
                changed = confirmed.changed(), if on_confirm => {
                    if changed.is_ok() {
                        let position = confirmed.borrow_and_update().clone();
                        if let Some(position) = position {
                            if let Err(err) = self.save_position(&position).await {
                                break Err(err);
                            }
                        }
                    }
                    continue;
                }
            };
            let emit = match emit {
                None => break Ok(()),
                Some(Err(err)) => break Err(err),
                Some(Ok(emit)) => emit,
            };
            let bytes = delivery_bytes(&emit.txn);
            match self.deliver(emit, &mut on_row, &mut on_delivery).await {
                Ok(true) => break Ok(()),
                Ok(false) => queue.release(bytes),
                Err(err) => break Err(err),
            }
        };
        // A reader blocked on a full queue, or waiting for the server, ends here.
        queue.close();
        task.abort();
        result
    }

    async fn deliver<RowFn, DeliveryFn>(
        &mut self,
        emit: Emit,
        on_row: &mut RowFn,
        on_delivery: &mut DeliveryFn,
    ) -> Result<bool>
    where
        RowFn: FnMut(CloudEvent) -> Result<()>,
        DeliveryFn: FnMut(Delivery) -> Result<()>,
    {
        let stop = emit.stop;
        let commit = emit.txn.ends_transaction();
        let source_id = self.config.source_id.clone();
        // A commit with nothing in it is the stream moving on: writes the
        // server-side filter left out, or an idle stream's new token.
        let nothing_in_it =
            emit.txn.part.is_none() && emit.txn.events.is_empty() && emit.txn.control.is_empty();
        match (self.config.delivery_unit, emit.txn.position.as_ref()) {
            (DeliveryUnit::Row, _) => {
                for event in emit.txn.events {
                    on_row(event)?;
                }
                for control in &emit.txn.control {
                    on_row(control_event(control, &source_id))?;
                }
            }
            (DeliveryUnit::Transaction, Some(position)) if nothing_in_it => {
                on_delivery(Delivery::Progress {
                    position: position.clone(),
                })?;
            }
            (DeliveryUnit::Transaction, _) => on_delivery(Delivery::Commit(emit.txn.clone()))?,
        }
        // A part that is not the last is not a commit: a restart reads its transaction again.
        if self.config.checkpoint == CheckpointMode::OnOk {
            if let Some(position) = emit.txn.position.as_ref().filter(|_| commit) {
                self.save_position(position).await?;
            }
        }
        if stop {
            let subject = emit
                .txn
                .control
                .first()
                .map(|c| c.subject.as_subject())
                .unwrap_or_default();
            tracing::warn!(%subject, "change stream ended on a control event");
        }
        Ok(stop)
    }
}

/// The change stream, read apart from the consumer.
struct Reader {
    database: Database,
    options: MongoSourceOptions,
    source_id: String,
    payload_mode: PayloadMode,
    include: IncludeList,
    grouper: TxnGrouper,
    start: Start,
    queue: Arc<QueueBytes>,
    tx: tokio::sync::mpsc::Sender<Result<Emit>>,
}

impl Reader {
    async fn run(mut self) {
        let tx = self.tx.clone();
        if let Err(err) = self.read().await {
            let _ = tx.send(Err(err)).await;
        }
    }

    /// Ends when the stream does, on a control event that stops it, or when
    /// the consumer is gone.
    async fn read(&mut self) -> Result<()> {
        let name = self.options.database.clone();
        let mut open = in_this_database(&name, &self.include.snapshot_filter());
        let mut stream = open_stream(&self.database, &self.options, &open, &self.start).await?;
        self.include.set_in_effect(Some(open.clone()));
        let _in_effect = InEffect(self.include.clone());
        // The last place read from the server: an event, or the end of an empty batch.
        let mut last_read: Option<ResumeToken> = None;

        loop {
            // TxnGrouper pins this filter when the group starts. Later events in the group ignore a newer list.
            let filter = self.include.snapshot_filter();
            let wanted = in_this_database(&name, &filter);
            if !covers(&open, &wanted) {
                // The list gained a collection the server leaves out. Open the stream again
                // where this one stopped reading, with the wider filter: what follows that
                // place is sent once, and nothing of it is skipped.
                if let Some(token) = last_read.clone().or_else(|| stream.resume_token()) {
                    self.start = Start::After(token);
                }
                stream = open_stream(&self.database, &self.options, &wanted, &self.start).await?;
                open = wanted;
                self.include.set_in_effect(Some(open.clone()));
                tracing::info!(
                    database = %name,
                    "change stream opened again with a wider include list"
                );
            }
            let emits = match stream.next_if_any().await.map_err(map_mongo)? {
                Some(event) => {
                    last_read = Some(event.id.clone());
                    let incoming =
                        map_change(raw_change(event)?, &self.source_id, self.payload_mode);
                    self.grouper.push(incoming, &filter)
                }
                None => {
                    if !stream.is_alive() {
                        return Ok(());
                    }
                    let token = stream.resume_token();
                    if token.is_some() {
                        last_read = token.clone();
                    }
                    let token = token.and_then(|t| token_json(&t).ok());
                    self.grouper.on_batch_boundary(token)
                }
            };
            for emit in emits {
                if !self.hand_over(emit).await {
                    return Ok(());
                }
            }
        }
    }

    /// Wait for room in the queue and pass one delivery on. False when reading
    /// is over: the consumer is gone, or the delivery ends the stream.
    async fn hand_over(&self, emit: Emit) -> bool {
        let stop = emit.stop;
        let bytes = delivery_bytes(&emit.txn);
        let queue = Arc::clone(&self.queue);
        let room = tokio::task::spawn_blocking(move || queue.reserve(bytes)).await;
        if !matches!(room, Ok(Ok(()))) {
            return false;
        }
        self.tx.send(Ok(emit)).await.is_ok() && !stop
    }
}

async fn open_stream(
    database: &Database,
    options: &MongoSourceOptions,
    filter: &IncludeFilter,
    start: &Start,
) -> Result<ChangeStream<ChangeStreamEvent<Document>>> {
    let mut watch =
        database
            .watch()
            .max_await_time(AWAIT)
            .full_document(match options.full_document {
                FullDocumentMode::Required => FullDocumentType::Required,
                FullDocumentMode::UpdateLookup => FullDocumentType::UpdateLookup,
            });
    let resume_after = match start {
        Start::After(token) => Some(token),
        _ => None,
    };
    if let Some(stage) = server_filter(&options.database, filter, resume_after) {
        watch = watch.pipeline([stage]);
    }
    watch = match start {
        Start::Now => watch,
        Start::After(token) => watch.resume_after(token.clone()),
        Start::At(time) => watch.start_at_operation_time(*time),
    };
    watch.await.map_err(map_mongo)
}

fn control_event(control: &ControlEvent, source_id: &str) -> CloudEvent {
    control.to_cloud_event(source_id, serde_json::Map::new())
}

fn raw_change(event: ChangeStreamEvent<Document>) -> Result<RawChange> {
    let resume_token = token_json(&event.id)?;
    let cluster_time = event.cluster_time.map(|ts| ClusterTime {
        t: ts.time,
        i: ts.increment,
    });
    let txn_key = match (&event.lsid, event.txn_number) {
        (Some(lsid), Some(n)) => Some(format!("{}:{n}", identity_text(lsid))),
        _ => None,
    };
    let (db, coll) = match &event.ns {
        Some(ns) => (ns.db.clone(), ns.coll.clone()),
        None => (String::new(), None),
    };
    let (to_db, to_coll) = match &event.to {
        Some(to) => (Some(to.db.clone()), to.coll.clone()),
        None => (None, None),
    };
    Ok(RawChange {
        op: match event.operation_type {
            OperationType::Insert => ChangeOpKind::Insert,
            OperationType::Update => ChangeOpKind::Update,
            OperationType::Replace => ChangeOpKind::Replace,
            OperationType::Delete => ChangeOpKind::Delete,
            OperationType::Drop => ChangeOpKind::Drop,
            OperationType::Rename => ChangeOpKind::Rename,
            OperationType::DropDatabase => ChangeOpKind::DropDatabase,
            OperationType::Invalidate => ChangeOpKind::Invalidate,
            OperationType::Other(_) | _ => ChangeOpKind::Other,
        },
        db,
        coll,
        to_db,
        to_coll,
        document_key: event.document_key.as_ref().map(document_image),
        full_document: event.full_document.as_ref().map(document_image),
        txn_key,
        resume_token,
        cluster_time,
    })
}

fn token_json(token: &ResumeToken) -> Result<Value> {
    serde_json::to_value(token).map_err(|e| Error::Source(format!("resume token: {e}")))
}

fn resume_token(stored: &MongoCheckpoint) -> Option<ResumeToken> {
    serde_json::from_value(stored.resume_token.clone()).ok()
}

pub(crate) fn map_mongo(err: mongodb::error::Error) -> Error {
    if history_lost(&err) {
        Error::HistoryLost(err.to_string())
    } else {
        Error::Source(err.to_string())
    }
}

/// Code 286 is `ChangeStreamHistoryLost`. A token the server cannot find is code 280
/// `ChangeStreamFatalError` with `NonResumableChangeStreamError`. Both mean reseed.
fn history_lost(err: &mongodb::error::Error) -> bool {
    if err.contains_label("NonResumableChangeStreamError") {
        return true;
    }
    let text = err.to_string();
    text.contains("ChangeStreamHistoryLost")
        || text.contains("NonResumableChangeStreamError")
        || text.contains("resume token was not found")
        || text.contains(&format!("code {HISTORY_LOST_CODE}"))
}

#[async_trait]
impl ChangeSource for MongoChangeStreamSource {
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
        let client = Client::with_uri_str(&self.options.uri)
            .await
            .map_err(map_mongo)?;
        validate_capture_gates(
            &client,
            &self.options.database,
            &self.config.include_tables,
            self.options.full_document,
        )
        .await
    }

    fn confirm(&self) -> Confirm {
        self.confirm.clone()
    }

    fn covers(&self, at: &SourcePosition, target: &SourcePosition) -> Result<bool> {
        crate::covers_position(at, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ce_stream_core::event::{ChangeOp, TableRef};
    use ce_stream_core::transaction::CommittedTransaction;
    use std::collections::HashSet;

    fn source(unit: DeliveryUnit, checkpoint: CheckpointMode) -> MongoChangeStreamSource {
        MongoChangeStreamSource {
            options: MongoSourceOptions {
                uri: String::new(),
                database: "app".into(),
                full_document: FullDocumentMode::Required,
            },
            config: SourceConfig {
                source_id: "mongo://t".into(),
                delivery_unit: unit,
                checkpoint,
                ..Default::default()
            },
            checkpoint: None,
            checkpoint_store: None,
            skip_gate_check: true,
            include: IncludeList::default(),
            confirm: new_confirm(),
        }
    }

    fn position(token: &str) -> SourcePosition {
        SourcePosition {
            adapter: crate::checkpoint::ADAPTER.into(),
            at: serde_json::json!({ "resume_token": { "_data": token } }),
            after: serde_json::json!({ "resume_token": { "_data": token } }),
        }
    }

    fn idle(token: &str) -> Emit {
        Emit {
            stop: false,
            txn: CommittedTransaction {
                position: Some(position(token)),
                ..Default::default()
            },
        }
    }

    fn with_a_row(token: &str) -> Emit {
        let mut emit = idle(token);
        emit.txn.events.push(CloudEvent::row_change(
            "mongo://t",
            &TableRef::new("app", "orders"),
            ChangeOp::Insert,
            serde_json::json!({ "op": "insert" }),
            serde_json::Map::new(),
        ));
        emit
    }

    #[tokio::test]
    async fn a_commit_with_nothing_in_it_is_a_position_passed() {
        let mut src = source(DeliveryUnit::Transaction, CheckpointMode::OnOk);
        let mut got = Vec::new();
        src.deliver(idle("a"), &mut |_| Ok(()), &mut |d| {
            got.push(d);
            Ok(())
        })
        .await
        .unwrap();
        src.deliver(with_a_row("b"), &mut |_| Ok(()), &mut |d| {
            got.push(d);
            Ok(())
        })
        .await
        .unwrap();
        assert!(matches!(got[0], Delivery::Progress { .. }));
        assert!(matches!(got[1], Delivery::Commit(_)));
        // Either way the checkpoint moved on the callback's Ok.
        assert_eq!(
            src.checkpoint.unwrap().payload["resume_token"]["_data"],
            "b"
        );
    }

    #[tokio::test]
    async fn row_mode_delivers_no_position_and_still_moves_the_checkpoint() {
        let mut src = source(DeliveryUnit::Row, CheckpointMode::OnOk);
        let mut rows: Vec<CloudEvent> = Vec::new();
        src.deliver(
            idle("a"),
            &mut |ev| {
                rows.push(ev);
                Ok(())
            },
            &mut |_| Ok(()),
        )
        .await
        .unwrap();
        assert!(rows.is_empty());
        assert!(src.checkpoint.is_some());
    }

    #[tokio::test]
    async fn on_confirm_a_delivery_moves_no_checkpoint() {
        let mut src = source(DeliveryUnit::Transaction, CheckpointMode::OnConfirm);
        src.deliver(with_a_row("a"), &mut |_| Ok(()), &mut |_| Ok(()))
            .await
            .unwrap();
        assert!(src.checkpoint.is_none());
        src.save_position(&position("a")).await.unwrap();
        assert_eq!(
            src.checkpoint.unwrap().payload["resume_token"]["_data"],
            "a"
        );
    }

    fn only(subjects: &[&str]) -> IncludeFilter {
        IncludeFilter::Only(
            subjects
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>(),
        )
    }

    #[test]
    fn a_finite_list_is_a_match_on_its_collections_and_the_ending_events() {
        let stage = server_filter(
            "app",
            &only(&["app.orders", "app.items", "other.orders"]),
            None,
        )
        .unwrap();
        assert_eq!(
            stage,
            doc! { "$match": { "$or": [
                { "ns.coll": { "$in": ["items", "orders"] } },
                { "operationType": { "$in": ["dropDatabase", "invalidate"] } },
            ] } }
        );
        // An empty list still lets the two ending events through.
        let nothing = server_filter("app", &only(&[]), None).unwrap();
        assert_eq!(
            nothing
                .get_document("$match")
                .unwrap()
                .get_array("$or")
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn the_event_a_stream_resumes_after_passes_the_filter_by_its_id() {
        let token: ResumeToken =
            serde_json::from_value(serde_json::json!({ "_data": "826AC93C3600000001" })).unwrap();
        let stage = server_filter("app", &only(&["app.orders"]), Some(&token)).unwrap();
        assert_eq!(
            stage,
            doc! { "$match": { "$or": [
                { "ns.coll": { "$in": ["orders"] } },
                { "operationType": { "$in": ["dropDatabase", "invalidate"] } },
                { "_id": { "_data": "826AC93C3600000001" } },
            ] } }
        );
    }

    #[test]
    fn every_table_is_an_unfiltered_stream() {
        assert!(server_filter("app", &IncludeFilter::All, None).is_none());
    }

    #[test]
    fn only_a_list_that_gained_a_collection_needs_a_new_stream() {
        let open = in_this_database("app", &only(&["app.orders", "app.items"]));
        assert!(covers(
            &open,
            &in_this_database("app", &only(&["app.orders"]))
        ));
        assert!(covers(&open, &in_this_database("app", &only(&[]))));
        assert!(covers(
            &open,
            &in_this_database("app", &only(&["app.items", "other.late"]))
        ));
        assert!(!covers(
            &open,
            &in_this_database("app", &only(&["app.orders", "app.late"]))
        ));
        assert!(!covers(&open, &IncludeFilter::All));
        assert!(covers(&IncludeFilter::All, &only(&["app.late"])));
    }

    #[test]
    fn the_reported_list_is_cleared_when_capture_ends() {
        let list = IncludeList::from_tables([TableRef::new("app", "orders")]);
        {
            list.set_in_effect(Some(list.snapshot_filter()));
            let _guard = InEffect(list.clone());
            assert!(list.in_effect_allows("app.orders"));
        }
        assert!(!list.in_effect_allows("app.orders"));
    }
}
