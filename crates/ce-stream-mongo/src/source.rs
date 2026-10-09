//! Change stream loop. Grouping and mapping are tested without a server.

use async_trait::async_trait;
use ce_stream_core::error::{Error, Result};
use ce_stream_core::event::CloudEvent;
use ce_stream_core::include::{IncludeFilter, IncludeList};
use ce_stream_core::source::{ChangeSource, DeliveryUnit, SourceConfig};
use ce_stream_core::transaction::{CommittedTransaction, ControlEvent};
use ce_stream_core::{Checkpoint, CheckpointStore};
use std::time::Duration;

use mongodb::bson::{doc, Document, Timestamp};
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken};
use mongodb::change_stream::ChangeStream;
use mongodb::options::FullDocumentType;
use mongodb::{Client, Database};
use serde_json::Value;

use crate::bson_json::{document_to_json, document_to_json_owned};
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
}

impl MongoChangeStreamSource {
    pub fn include_handle(&self) -> IncludeList {
        self.include.clone()
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
            .await?;
            for warning in &report.warnings {
                tracing::warn!(gate_warning = %warning, "capture gate warning");
            }
        }

        let stored = self
            .checkpoint
            .as_ref()
            .and_then(MongoCheckpoint::from_checkpoint);
        let seed = stored.as_ref().and_then(|s| s.seed_cluster_time);
        let mut start = match (stored.as_ref().and_then(resume_token), seed) {
            (Some(token), _) => Start::After(token),
            (None, Some(seed_time)) => Start::At(Timestamp {
                time: seed_time.t,
                increment: seed_time.i,
            }),
            (None, None) => Start::Now,
        };

        let database = client.database(&self.options.database);
        let mut open = in_this_database(&self.options.database, &self.include.snapshot_filter());
        let mut stream = self.open_stream(&database, &open, &start).await?;
        self.include.set_in_effect(Some(open.clone()));
        let _in_effect = InEffect(self.include.clone());
        // The last place read from the server: an event, or the end of an empty batch.
        let mut last_read: Option<ResumeToken> = None;
        let mut grouper = TxnGrouper::new(seed);

        loop {
            // TxnGrouper pins this filter when the group starts. Later events in the group ignore a newer list.
            let filter = self.include.snapshot_filter();
            let wanted = in_this_database(&self.options.database, &filter);
            if !covers(&open, &wanted) {
                // The list gained a collection the server leaves out. Open the stream again
                // where this one stopped reading, with the wider filter: what follows that
                // place is sent once, and nothing of it is skipped.
                if let Some(token) = last_read.clone().or_else(|| stream.resume_token()) {
                    start = Start::After(token);
                }
                stream = self.open_stream(&database, &wanted, &start).await?;
                open = wanted;
                self.include.set_in_effect(Some(open.clone()));
                tracing::info!(
                    database = %self.options.database,
                    "change stream opened again with a wider include list"
                );
            }
            match stream.next_if_any().await.map_err(map_mongo)? {
                Some(event) => {
                    last_read = Some(event.id.clone());
                    let incoming = map_change(
                        raw_change(event)?,
                        &self.config.source_id,
                        self.config.payload_mode,
                    );
                    for emit in grouper.push(incoming, &filter) {
                        let stop = self.deliver(emit, &mut on_row, &mut on_txn).await?;
                        if stop {
                            return Ok(());
                        }
                    }
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
                    for emit in grouper.on_batch_boundary(token) {
                        let stop = self.deliver(emit, &mut on_row, &mut on_txn).await?;
                        if stop {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn open_stream(
        &self,
        database: &Database,
        filter: &IncludeFilter,
        start: &Start,
    ) -> Result<ChangeStream<ChangeStreamEvent<Document>>> {
        let mut watch = database.watch().max_await_time(AWAIT).full_document(
            match self.options.full_document {
                FullDocumentMode::Required => FullDocumentType::Required,
                FullDocumentMode::UpdateLookup => FullDocumentType::UpdateLookup,
            },
        );
        let resume_after = match start {
            Start::After(token) => Some(token),
            _ => None,
        };
        if let Some(stage) = server_filter(&self.options.database, filter, resume_after) {
            watch = watch.pipeline([stage]);
        }
        watch = match start {
            Start::Now => watch,
            Start::After(token) => watch.resume_after(token.clone()),
            Start::At(time) => watch.start_at_operation_time(*time),
        };
        watch.await.map_err(map_mongo)
    }

    async fn deliver<RowFn, TxnFn>(
        &mut self,
        emit: Emit,
        on_row: &mut RowFn,
        on_txn: &mut TxnFn,
    ) -> Result<bool>
    where
        RowFn: FnMut(CloudEvent) -> Result<()>,
        TxnFn: FnMut(CommittedTransaction) -> Result<()>,
    {
        let stop = emit.stop;
        let source_id = self.config.source_id.clone();
        match self.config.delivery_unit {
            DeliveryUnit::Row => {
                for event in emit.txn.events {
                    on_row(event)?;
                }
                for control in &emit.txn.control {
                    on_row(control_event(control, &source_id))?;
                }
            }
            DeliveryUnit::Transaction => on_txn(emit.txn.clone())?,
        }
        if let Some(position) = emit.txn.position {
            let cp = Checkpoint {
                adapter: crate::checkpoint::ADAPTER.into(),
                payload: position.after,
            };
            if let Some(store) = self.checkpoint_store.as_mut() {
                store.save(&cp).await?;
            }
            self.checkpoint = Some(cp);
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
        (Some(lsid), Some(n)) => Some(format!("{}:{n}", document_to_json(lsid))),
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
        document_key: event.document_key.as_ref().map(document_to_json),
        full_document: event.full_document.map(document_to_json_owned),
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

#[cfg(test)]
mod tests {
    use super::*;
    use ce_stream_core::event::TableRef;
    use std::collections::HashSet;

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
