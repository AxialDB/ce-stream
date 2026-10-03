//! Initial copy of a collection.
//!
//! Record cluster time T with one majority `ping`, then scan with a majority read.
//! The scan is a cursor in batches of [`SEED_BATCH`]. It is not loaded into one `Vec`.
//! The caller opens the change stream with `startAtOperationTime: T` and applies the
//! overlap as whole-document replaces.

use ce_stream_core::error::{Error, Result};
use ce_stream_core::event::{ChangeOp, CloudEvent, PayloadMode, TableRef};
use futures_util::StreamExt;
use mongodb::bson::{doc, Document};
use mongodb::options::ReadConcern;
use mongodb::{Client, Cursor};
use serde_json::json;

use crate::bson_json::document_to_json_owned;
use crate::checkpoint::ClusterTime;
use crate::source::map_mongo;

/// Documents per `getMore`. Large enough to cut round trips, small enough to bound memory.
pub const SEED_BATCH: u32 = 512;

pub async fn note_cluster_time(client: &Client) -> Result<ClusterTime> {
    let mut session = client.start_session().await.map_err(map_mongo)?;
    client
        .database("admin")
        .run_command(doc! { "ping": 1, "readConcern": { "level": "majority" } })
        .session(&mut session)
        .await
        .map_err(map_mongo)?;
    let ts = session
        .operation_time()
        .ok_or_else(|| Error::Source("majority ping did not return an operationTime".into()))?;
    Ok(ClusterTime {
        t: ts.time,
        i: ts.increment,
    })
}

pub async fn open_seed_cursor(
    client: &Client,
    database: &str,
    collection: &str,
) -> Result<Cursor<Document>> {
    client
        .database(database)
        .collection::<Document>(collection)
        .find(doc! {})
        .read_concern(ReadConcern::majority())
        .batch_size(SEED_BATCH)
        .await
        .map_err(map_mongo)
}

/// Next [`SEED_BATCH`] documents as row events. An empty vec means the cursor is done.
pub async fn read_seed_batch(
    cursor: &mut Cursor<Document>,
    source_id: &str,
    table: &TableRef,
    cluster_time: ClusterTime,
    mode: PayloadMode,
) -> Result<Vec<CloudEvent>> {
    let mut events = Vec::with_capacity(SEED_BATCH as usize);
    let mut ext = serde_json::Map::new();
    ext.insert(
        "clusterTime".into(),
        json!({"t": cluster_time.t, "i": cluster_time.i}),
    );
    while events.len() < SEED_BATCH as usize {
        let Some(doc) = cursor.next().await.transpose().map_err(map_mongo)? else {
            break;
        };
        let data = match mode {
            PayloadMode::Signal => json!({"op": "insert", "signal": true}),
            PayloadMode::Full => json!({"op": "insert", "after": document_to_json_owned(doc)}),
        };
        events.push(CloudEvent::row_change(
            source_id,
            table,
            ChangeOp::Insert,
            data,
            ext.clone(),
        ));
    }
    Ok(events)
}
