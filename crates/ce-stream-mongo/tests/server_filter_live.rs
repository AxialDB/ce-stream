//! Live test for the server-side include filter (issue #9). Not part of CI: it needs a
//! MongoDB 8.0+ replica set.
//!
//! ```text
//! CE_STREAM_MONGO_URI=mongodb://127.0.0.1:27017/?replicaSet=rs0 \
//!   cargo test -p ce-stream-mongo --test server_filter_live -- --ignored --test-threads=1
//! ```
//!
//! With `--ignored` and no URI the test says so and passes.

use std::time::Duration;

use ce_stream_core::event::TableRef;
use ce_stream_core::include::IncludeList;
use ce_stream_core::source::{ChangeSource, DeliveryUnit, SourceConfig};
use ce_stream_core::transaction::CommittedTransaction;
use ce_stream_core::Checkpoint;
use ce_stream_mongo::{
    ClusterTime, FullDocumentMode, MongoChangeStreamSource, MongoCheckpoint, MongoSourceOptions,
};
use mongodb::bson::{doc, Document};
use mongodb::{Client, Database};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::task::JoinHandle;

const DB: &str = "ce_stream_issue_9";
const WAIT: Duration = Duration::from_secs(20);

struct Capture {
    include: IncludeList,
    commits: UnboundedReceiver<CommittedTransaction>,
    task: JoinHandle<ce_stream_core::error::Result<()>>,
}

fn start(uri: &str, tables: &[&str], checkpoint: Option<Checkpoint>) -> Capture {
    let refs: Vec<TableRef> = tables.iter().map(|t| TableRef::new(DB, *t)).collect();
    let include = IncludeList::from_tables(refs.iter());
    let (tx, commits) = unbounded_channel();
    let mut source = MongoChangeStreamSource {
        options: MongoSourceOptions {
            uri: uri.to_string(),
            database: DB.to_string(),
            full_document: FullDocumentMode::Required,
        },
        config: SourceConfig {
            source_id: format!("mongo://{DB}"),
            include_tables: refs,
            delivery_unit: DeliveryUnit::Transaction,
            ..Default::default()
        },
        checkpoint,
        checkpoint_store: None,
        skip_gate_check: false,
        include: include.clone(),
    };
    let task = tokio::spawn(async move {
        source
            .run_transactions(move |txn| {
                let _ = tx.send(txn);
                Ok(())
            })
            .await
    });
    Capture {
        include,
        commits,
        task,
    }
}

impl Capture {
    async fn follows(&self, table: &str) {
        let subject = format!("{DB}.{table}");
        let deadline = tokio::time::Instant::now() + WAIT;
        while !self.include.in_effect_allows(&subject) {
            assert!(
                !self.task.is_finished(),
                "capture ended before it followed {subject}"
            );
            assert!(
                tokio::time::Instant::now() < deadline,
                "the stream did not start to deliver {subject}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The next commit, with or without events.
    async fn next(&mut self) -> CommittedTransaction {
        match tokio::time::timeout(WAIT, self.commits.recv()).await {
            Ok(Some(txn)) => txn,
            Ok(None) => panic!("capture ended: {:?}", (&mut self.task).await),
            Err(_) => panic!("no commit within {WAIT:?}"),
        }
    }

    /// The next commit that has row events, as `(collection, _id)` pairs, and its commit.
    async fn next_rows(&mut self) -> (Vec<(String, i64)>, CommittedTransaction) {
        loop {
            let txn = self.next().await;
            if txn.events.is_empty() && txn.control.is_empty() {
                continue;
            }
            let rows = txn
                .events
                .iter()
                .map(|event| {
                    let collection = event
                        .subject
                        .strip_prefix(&format!("{DB}."))
                        .unwrap_or(&event.subject)
                        .to_string();
                    let data = serde_json::to_value(&event.data).expect("event data");
                    let doc = data
                        .get("after")
                        .filter(|after| !after.is_null())
                        .or_else(|| data.get("before"))
                        .cloned()
                        .unwrap_or_default();
                    let id = &doc["_id"];
                    let id = id
                        .as_i64()
                        .or_else(|| id["$numberInt"].as_str().and_then(|n| n.parse().ok()))
                        .or_else(|| id["$numberLong"].as_str().and_then(|n| n.parse().ok()))
                        .unwrap_or(-1);
                    (collection, id)
                })
                .collect();
            return (rows, txn);
        }
    }

    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

fn after_time(txn: &CommittedTransaction) -> Option<ClusterTime> {
    let position = txn.position.as_ref()?;
    MongoCheckpoint::from_checkpoint(&Checkpoint {
        adapter: position.adapter.clone(),
        payload: position.after.clone(),
    })?
    .cluster_time
}

fn checkpoint_after(txn: &CommittedTransaction) -> Checkpoint {
    let position = txn
        .position
        .as_ref()
        .expect("a Mongo commit has a position");
    Checkpoint {
        adapter: position.adapter.clone(),
        payload: position.after.clone(),
    }
}

async fn prepare(uri: &str) -> Database {
    let client = Client::with_uri_str(uri).await.expect("connect");
    let db = client.database(DB);
    db.drop().await.expect("drop database");
    for name in ["items", "late", "plain"] {
        db.create_collection(name).await.expect("create collection");
    }
    // `plain` has no post-images: the collection that is not on the list.
    for name in ["items", "late"] {
        db.run_command(doc! {
            "collMod": name,
            "changeStreamPreAndPostImages": { "enabled": true },
        })
        .await
        .expect("enable post-images");
    }
    for name in ["items", "late", "plain"] {
        db.collection::<Document>(name)
            .insert_one(doc! { "_id": 1, "n": 1 })
            .await
            .expect("insert");
    }
    db
}

async fn scenario(uri: &str) {
    let db = prepare(uri).await;
    let items = db.collection::<Document>("items");
    let late = db.collection::<Document>("late");
    let plain = db.collection::<Document>("plain");

    let mut capture = start(uri, &["items"], None);
    capture.follows("items").await;
    assert!(!capture.include.in_effect_allows(&format!("{DB}.late")));

    // 1. An update in a collection that is not on the list and has no post-images. Without
    //    the server-side filter the server ends the stream here.
    plain
        .update_one(doc! { "_id": 1 }, doc! { "$set": { "n": 2 } })
        .await
        .expect("update plain");
    items
        .update_one(doc! { "_id": 1 }, doc! { "$set": { "n": 2 } })
        .await
        .expect("update items");
    let (rows, _) = capture.next_rows().await;
    assert_eq!(rows, vec![("items".to_string(), 1)]);

    // 2. The list gains a collection. Once the stream reports it, every change to it arrives,
    //    in order with the others, each once.
    late.insert_one(doc! { "_id": 2 })
        .await
        .expect("insert late");
    capture.include.insert(TableRef::new(DB, "late"));
    capture.follows("late").await;
    items.insert_one(doc! { "_id": 10 }).await.expect("insert");
    late.insert_one(doc! { "_id": 11 }).await.expect("insert");
    plain.insert_one(doc! { "_id": 12 }).await.expect("insert");
    late.update_one(doc! { "_id": 11 }, doc! { "$set": { "n": 1 } })
        .await
        .expect("update late");
    items.insert_one(doc! { "_id": 13 }).await.expect("insert");
    let mut seen = Vec::new();
    let mut last = None;
    while seen.last() != Some(&("items".to_string(), 13)) {
        let (rows, txn) = capture.next_rows().await;
        seen.extend(rows);
        last = Some(txn);
    }
    // `late` 2 was written before the stream followed the collection. Whether it arrives
    // depends on where the stream stood; an embedder copies the collection after `follows`.
    seen.retain(|row| row != &("late".to_string(), 2));
    assert_eq!(
        seen,
        vec![
            ("items".to_string(), 10),
            ("late".to_string(), 11),
            ("late".to_string(), 11),
            ("items".to_string(), 13),
        ]
    );
    let before_bulk = after_time(last.as_ref().expect("a commit")).expect("cluster time");

    // 3. Writes the filter leaves out still move the position: a commit with no events.
    let bulk: Vec<Document> = (100..2100).map(|i| doc! { "_id": i }).collect();
    plain.insert_many(bulk).await.expect("bulk insert");
    let moved = loop {
        let txn = capture.next().await;
        assert!(txn.events.is_empty(), "nothing on the list was written");
        if after_time(&txn).is_some_and(|at| (at.t, at.i) > (before_bulk.t, before_bulk.i)) {
            break txn;
        }
    };
    capture.stop().await;

    // 4. A restart from that position, with the wider list from the start: only what came
    //    after it.
    items.insert_one(doc! { "_id": 20 }).await.expect("insert");
    let mut capture = start(uri, &["items", "late"], Some(checkpoint_after(&moved)));
    capture.follows("late").await;
    let (rows, _) = capture.next_rows().await;
    assert_eq!(rows, vec![("items".to_string(), 20)]);

    // 5. Dropping a collection on the list still ends capture with a control event.
    items.drop().await.expect("drop items");
    let (_, txn) = capture.next_rows().await;
    assert_eq!(txn.control.len(), 1);
    let ended = tokio::time::timeout(WAIT, &mut capture.task)
        .await
        .expect("capture ends after the drop")
        .expect("join");
    assert!(ended.is_ok(), "capture ended with {ended:?}");
    assert!(!capture.include.in_effect_allows(&format!("{DB}.late")));

    db.drop().await.expect("drop database");
}

#[tokio::test]
#[ignore = "needs CE_STREAM_MONGO_URI (a MongoDB 8.0+ replica set)"]
async fn server_filter_on_each_configured_replica_set() {
    let Some(first) = std::env::var("CE_STREAM_MONGO_URI")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("SKIPPED: CE_STREAM_MONGO_URI is not set, no MongoDB was contacted");
        return;
    };
    scenario(&first).await;
    if let Some(second) = std::env::var("CE_STREAM_MONGO_URI_2")
        .ok()
        .filter(|s| !s.is_empty())
    {
        scenario(&second).await;
    }
}
