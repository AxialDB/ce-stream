//! Change stream loop. Grouping and mapping are tested without a server.

use async_trait::async_trait;
use ce_stream_core::error::{Error, Result};
use ce_stream_core::event::CloudEvent;
use ce_stream_core::include::IncludeList;
use ce_stream_core::source::{ChangeSource, DeliveryUnit, SourceConfig};
use ce_stream_core::transaction::{CommittedTransaction, ControlEvent};
use ce_stream_core::{Checkpoint, CheckpointStore};
use mongodb::bson::Document;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken};
use mongodb::options::FullDocumentType;
use mongodb::Client;
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
        let resume = stored.as_ref().and_then(resume_token);

        let database = client.database(&self.options.database);
        let mut watch = database
            .watch()
            .full_document(match self.options.full_document {
                FullDocumentMode::Required => FullDocumentType::Required,
                FullDocumentMode::UpdateLookup => FullDocumentType::UpdateLookup,
            });
        if let Some(token) = resume {
            watch = watch.resume_after(token);
        } else if let Some(seed_time) = seed {
            watch = watch.start_at_operation_time(mongodb::bson::Timestamp {
                time: seed_time.t,
                increment: seed_time.i,
            });
        }
        let mut stream = watch.await.map_err(map_mongo)?;
        let mut grouper = TxnGrouper::new(seed);

        loop {
            match stream.next_if_any().await.map_err(map_mongo)? {
                Some(event) => {
                    let incoming = map_change(
                        raw_change(event)?,
                        &self.config.source_id,
                        self.config.payload_mode,
                    );
                    // TxnGrouper pins this filter when the group starts. Later events in the group ignore a newer list.
                    let filter = self.include.snapshot_filter();
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
                    let token = stream.resume_token().and_then(|t| token_json(&t).ok());
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
    let lost = matches!(
        err.kind.as_ref(),
        mongodb::error::ErrorKind::Command(cmd) if cmd.code == HISTORY_LOST_CODE
    );
    if lost {
        Error::HistoryLost(err.to_string())
    } else {
        Error::Source(err.to_string())
    }
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
