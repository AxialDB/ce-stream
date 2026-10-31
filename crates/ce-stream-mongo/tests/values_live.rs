//! One document of every kind, through a real change stream, against
//! `docs/values.md`. Ignored by default:
//!
//! ```text
//! CE_STREAM_MONGO_URI=mongodb://127.0.0.1:27017/?replicaSet=rs0 \
//!   cargo test -p ce-stream-mongo --test values_live -- --ignored
//! ```

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ce_stream_core::delivery::Delivery;
use ce_stream_core::error::Error;
use ce_stream_core::event::TableRef;
use ce_stream_core::source::{ChangeSource, DeliveryUnit, SourceConfig};
use ce_stream_mongo::{new_confirm, FullDocumentMode, MongoChangeStreamSource, MongoSourceOptions};
use mongodb::bson::oid::ObjectId;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{doc, Binary, Bson, DateTime, Decimal128, Document, Regex, Timestamp};
use mongodb::Client;
use serde_json::{json, Value};

const DB: &str = "ce_stream_values_live";

#[tokio::test]
#[ignore = "needs CE_STREAM_MONGO_URI (a MongoDB 8.0+ replica set)"]
async fn a_row_has_the_contracts_values_and_says_their_types() {
    let Ok(uri) = std::env::var("CE_STREAM_MONGO_URI") else {
        eprintln!("SKIPPED: CE_STREAM_MONGO_URI is not set, no MongoDB was reached");
        return;
    };
    let client = Client::with_uri_str(&uri).await.unwrap();
    let db = client.database(DB);
    db.drop().await.unwrap();
    db.run_command(doc! {
        "create": "t",
        "changeStreamPreAndPostImages": { "enabled": true },
    })
    .await
    .unwrap();

    let id = ObjectId::parse_str("656e8f1e2b3c4d5e6f708192").unwrap();
    let at = DateTime::from_millis(1_767_323_045_123);
    let since = DateTime::from_millis(1_704_067_200_000);
    let decimal: Decimal128 = "12.50".parse().unwrap();
    let document = doc! {
        "_id": id,
        "small": 7i32,
        "big": 9_007_199_254_740_993i64,
        "half": 1.5f64,
        "whole": 7.0f64,
        "price": decimal,
        "at": at,
        "blob": Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, 255] },
        "uuid": Binary { subtype: BinarySubtype::Uuid, bytes: vec![171; 16] },
        "note": "café",
        "ok": true,
        "gone": Bson::Null,
        "nan": f64::NAN,
        "customer": { "name": "a", "since": since },
        "items": [ "x", since ],
        "re": Regex { pattern: "a".into(), options: "i".into() },
        "ts": Timestamp { time: 10, increment: 2 },
    };

    let writer_client = client.clone();
    let writer = tokio::spawn(async move {
        // The source opens its stream at the current time: wait for it.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let collection = writer_client.database(DB).collection::<Document>("t");
        collection.insert_one(document).await.unwrap();
        collection.delete_one(doc! { "_id": id }).await.unwrap();
    });

    let mut source = MongoChangeStreamSource {
        options: MongoSourceOptions {
            uri,
            database: DB.into(),
            full_document: FullDocumentMode::Required,
        },
        config: SourceConfig {
            source_id: "mongo://lab".into(),
            include_tables: vec![TableRef::new(DB, "t")],
            delivery_unit: DeliveryUnit::Transaction,
            ..Default::default()
        },
        checkpoint: None,
        checkpoint_store: None,
        skip_gate_check: false,
        include: Default::default(),
        confirm: new_confirm(),
    };
    let rows = Arc::new(Mutex::new(Vec::<Value>::new()));
    let keep = Arc::clone(&rows);
    let run = source.run_deliveries(move |delivery| {
        if let Delivery::Commit(txn) = delivery {
            let mut rows = keep.lock().unwrap();
            rows.extend(txn.events.into_iter().map(|event| event.data));
            if rows.len() >= 2 {
                return Err(Error::Source("test is done".into()));
            }
        }
        Ok(())
    });
    let result = tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("both events arrived within 30 seconds");
    assert!(result.unwrap_err().to_string().contains("test is done"));
    writer.await.unwrap();

    let rows = rows.lock().unwrap().clone();
    let insert = &rows[0];
    let after = &insert["after"];
    let re = after["re"].as_str().unwrap().to_string();
    let ts = after["ts"].as_str().unwrap().to_string();
    assert!(re.contains("regularExpression"), "{re}");
    assert!(ts.contains("$timestamp"), "{ts}");
    assert_eq!(
        insert,
        &json!({
            "op": "insert",
            "after": {
                "_id": "656e8f1e2b3c4d5e6f708192",
                "small": 7,
                "big": 9_007_199_254_740_993i64,
                "half": 1.5,
                "whole": 7.0,
                "price": "12.50",
                "at": 1_767_323_045_123_000i64,
                "blob": "hex:0102ff",
                "uuid": format!("hex:{}", "ab".repeat(16)),
                "note": "café",
                "ok": true,
                "gone": null,
                "nan": "NaN",
                "customer": { "name": "a", "since": 1_704_067_200_000_000i64 },
                "items": [ "x", 1_704_067_200_000_000i64 ],
                "re": re,
                "ts": ts,
            },
            "types": {
                "_id": "objectId",
                "price": "decimal",
                "at": "date",
                "blob": "binary",
                "uuid": "binary:04",
                "nan": "double",
                "customer": { "since": "date" },
                "items": [ null, "date" ],
                "re": "regex",
                "ts": "timestamp",
            },
        })
    );
    assert!(after["whole"].is_f64(), "a whole double stays a double");
    assert_eq!(
        rows[1],
        json!({
            "op": "delete",
            "before": { "_id": "656e8f1e2b3c4d5e6f708192" },
            "types": { "_id": "objectId" },
        })
    );
    db.drop().await.unwrap();
}
