//! BSON to JSON for CloudEvent `data`.
//!
//! Values are [canonical extended JSON](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/).
//! `Int64`, `Decimal128`, `ObjectId`, and `Binary` stay exact. A JSON number is never used for a
//! 64-bit integer.

use mongodb::bson::{Bson, Document};

pub fn document_to_json(doc: &Document) -> serde_json::Value {
    document_to_json_owned(doc.clone())
}

pub fn document_to_json_owned(doc: Document) -> serde_json::Value {
    Bson::Document(doc).into_canonical_extjson()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::oid::ObjectId;
    use mongodb::bson::spec::BinarySubtype;
    use mongodb::bson::{Binary, DateTime, Decimal128, Timestamp};

    #[test]
    fn types_that_json_would_lose_stay_tagged() {
        let oid = ObjectId::parse_str("656e8f1e2b3c4d5e6f708192").unwrap();
        let decimal: Decimal128 = "12345678901234567890.12".parse().unwrap();
        let doc = mongodb::bson::doc! {
            "_id": oid,
            "n": 9_007_199_254_740_993i64,
            "small": 7i64,
            "money": decimal,
            "when": DateTime::from_millis(1_700_000_000_000),
            "blob": Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, 255] },
            "ts": Timestamp { time: 10, increment: 2 },
        };
        let json = document_to_json(&doc);
        assert_eq!(json["_id"]["$oid"], "656e8f1e2b3c4d5e6f708192");
        assert_eq!(json["n"]["$numberLong"], "9007199254740993");
        assert_eq!(json["small"]["$numberLong"], "7");
        assert_eq!(json["money"]["$numberDecimal"], "12345678901234567890.12");
        assert_eq!(json["when"]["$date"]["$numberLong"], "1700000000000");
        assert_eq!(json["blob"]["$binary"]["base64"], "AQL/");
        assert_eq!(json["blob"]["$binary"]["subType"], "00");
        assert_eq!(json["ts"]["$timestamp"]["t"], 10);
        assert_eq!(json["ts"]["$timestamp"]["i"], 2);
    }
}
