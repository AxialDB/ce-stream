//! BSON to row data, as `docs/values.md` says for every host.
//!
//! The values have the contract's forms. A collection has no schema to read them
//! by, so a row also says the type of each value whose form does not show it, in
//! `types`, which mirrors the shape of the document. A string, a boolean, a null,
//! and a number have no entry: `7` is whole and `7.0` is a double.

use mongodb::bson::{Bson, Document};
use serde_json::{json, Map, Value};

/// A document as a row image: its values, and the types its values do not show.
#[derive(Clone, Debug, PartialEq)]
pub struct Image {
    pub value: Value,
    pub types: Option<Value>,
}

impl Image {
    /// `data` of a row event. `side` is `after` or `before`.
    pub fn row_data(&self, op: &str, side: &str) -> Value {
        let mut data = Map::new();
        data.insert("op".into(), op.into());
        data.insert(side.into(), self.value.clone());
        if let Some(types) = &self.types {
            data.insert("types".into(), types.clone());
        }
        Value::Object(data)
    }
}

pub fn document_image(doc: &Document) -> Image {
    let (value, types) = document_parts(doc);
    Image { value, types }
}

fn document_parts(doc: &Document) -> (Value, Option<Value>) {
    let mut values = Map::with_capacity(doc.len());
    let mut types = Map::new();
    for (key, bson) in doc {
        let (value, of_value) = convert(bson);
        values.insert(key.clone(), value);
        if let Some(of_value) = of_value {
            types.insert(key.clone(), of_value);
        }
    }
    let types = (!types.is_empty()).then_some(Value::Object(types));
    (Value::Object(values), types)
}

/// As the host prints it, for a type the contract has no form for.
fn as_printed(bson: &Bson) -> Value {
    Value::String(bson.clone().into_canonical_extjson().to_string())
}

fn typed(value: Value, name: &str) -> (Value, Option<Value>) {
    (value, Some(Value::String(name.to_string())))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// JSON has no number for these. The same text as on PostgreSQL.
fn non_finite_text(n: f64) -> &'static str {
    if n.is_nan() {
        "NaN"
    } else if n > 0.0 {
        "Infinity"
    } else {
        "-Infinity"
    }
}

fn convert(bson: &Bson) -> (Value, Option<Value>) {
    match bson {
        Bson::Null => (Value::Null, None),
        Bson::Boolean(flag) => (Value::Bool(*flag), None),
        Bson::String(text) => (Value::String(text.clone()), None),
        Bson::Int32(n) => (json!(n), None),
        Bson::Int64(n) => (json!(n), None),
        Bson::Double(n) => match serde_json::Number::from_f64(*n) {
            Some(number) => (Value::Number(number), None),
            None => typed(Value::String(non_finite_text(*n).into()), "double"),
        },
        Bson::Document(doc) => document_parts(doc),
        Bson::Array(items) => {
            let parts: Vec<_> = items.iter().map(convert).collect();
            let types = parts.iter().any(|(_, of_item)| of_item.is_some()).then(|| {
                Value::Array(
                    parts
                        .iter()
                        .map(|(_, of_item)| of_item.clone().unwrap_or(Value::Null))
                        .collect(),
                )
            });
            let values = parts.into_iter().map(|(value, _)| value).collect();
            (Value::Array(values), types)
        }
        Bson::Decimal128(n) => typed(Value::String(n.to_string()), "decimal"),
        Bson::ObjectId(id) => typed(Value::String(id.to_hex()), "objectId"),
        Bson::DateTime(at) => {
            let millis = at.timestamp_millis();
            match millis.checked_mul(1000) {
                Some(micros) => typed(json!(micros), "date"),
                // A BSON date can be past what microseconds hold.
                None => typed(Value::String(format!("{millis} ms")), "date"),
            }
        }
        Bson::Binary(binary) => {
            let value = Value::String(format!("hex:{}", hex(&binary.bytes)));
            match u8::from(binary.subtype) {
                0 => typed(value, "binary"),
                subtype => typed(value, &format!("binary:{subtype:02x}")),
            }
        }
        Bson::Symbol(text) => typed(Value::String(text.clone()), "symbol"),
        Bson::RegularExpression(_) => typed(as_printed(bson), "regex"),
        Bson::JavaScriptCode(_) => typed(as_printed(bson), "javascript"),
        Bson::JavaScriptCodeWithScope(_) => typed(as_printed(bson), "javascriptWithScope"),
        Bson::Timestamp(_) => typed(as_printed(bson), "timestamp"),
        Bson::MinKey => typed(as_printed(bson), "minKey"),
        Bson::MaxKey => typed(as_printed(bson), "maxKey"),
        Bson::Undefined => typed(as_printed(bson), "undefined"),
        Bson::DbPointer(_) => typed(as_printed(bson), "dbPointer"),
    }
}

/// An exact text of a document, for an identity such as a session id. Not row data.
pub fn identity_text(doc: &Document) -> String {
    Bson::Document(doc.clone())
        .into_canonical_extjson()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::oid::ObjectId;
    use mongodb::bson::spec::BinarySubtype;
    use mongodb::bson::{doc, Binary, DateTime, Decimal128, Regex, Timestamp};

    #[test]
    fn values_have_the_contracts_forms_and_only_what_they_do_not_show_is_typed() {
        let decimal: Decimal128 = "12.50".parse().unwrap();
        let image = document_image(&doc! {
            "_id": ObjectId::parse_str("656e8f1e2b3c4d5e6f708192").unwrap(),
            "small": 7i32,
            "big": 9_007_199_254_740_993i64,
            "half": 1.5f64,
            "whole": 7.0f64,
            "price": decimal,
            "when": DateTime::from_millis(1_700_000_000_000i64),
            "blob": Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, 255] },
            "note": "café",
            "ok": true,
            "gone": Bson::Null,
        });
        assert_eq!(
            image.value,
            json!({
                "_id": "656e8f1e2b3c4d5e6f708192",
                "small": 7,
                "big": 9_007_199_254_740_993i64,
                "half": 1.5,
                "whole": 7.0,
                "price": "12.50",
                "when": 1_700_000_000_000_000i64,
                "blob": "hex:0102ff",
                "note": "café",
                "ok": true,
                "gone": null,
            })
        );
        assert_eq!(
            image.types,
            Some(json!({
                "_id": "objectId",
                "price": "decimal",
                "when": "date",
                "blob": "binary",
            }))
        );
        // A whole double stays apart from a whole number.
        assert!(image.value["small"].is_i64());
        assert!(image.value["whole"].is_f64());
    }

    #[test]
    fn a_row_of_plain_values_has_no_types() {
        let image = document_image(&doc! { "_id": 1, "note": "a", "tags": ["x", 2] });
        assert_eq!(image.types, None);
        assert_eq!(
            image.row_data("insert", "after"),
            json!({"op": "insert", "after": {"_id": 1, "note": "a", "tags": ["x", 2]}})
        );
    }

    #[test]
    fn types_mirror_the_shape_of_the_document() {
        let at = DateTime::from_millis(1000);
        let image = document_image(&doc! {
            "_id": 1,
            "customer": { "name": "a", "since": at },
            "plain": { "name": "b" },
            "items": [ "x", at, { "sku": "s", "made": at } ],
            "field.with.dots": at,
        });
        assert_eq!(
            image.types,
            Some(json!({
                "customer": { "since": "date" },
                "items": [ null, "date", { "made": "date" } ],
                "field.with.dots": "date",
            }))
        );
        assert_eq!(image.value["items"][1], 1_000_000);
        assert_eq!(
            image.row_data("delete", "before")["types"]["customer"]["since"],
            "date"
        );
    }

    #[test]
    fn a_date_and_the_same_number_can_be_told_apart() {
        let image = document_image(&doc! {
            "a": DateTime::from_millis(1000),
            "b": 1_000_000i64,
        });
        assert_eq!(image.value["a"], image.value["b"]);
        assert_eq!(image.types, Some(json!({ "a": "date" })));
    }

    #[test]
    fn a_date_past_what_microseconds_hold_says_its_milliseconds() {
        let image = document_image(&doc! { "a": DateTime::from_millis(i64::MAX) });
        assert_eq!(image.value["a"], format!("{} ms", i64::MAX));
        assert_eq!(image.types, Some(json!({ "a": "date" })));
    }

    #[test]
    fn what_json_has_no_number_for_is_text_with_its_type() {
        let image = document_image(&doc! {
            "a": f64::NAN,
            "b": f64::INFINITY,
            "c": f64::NEG_INFINITY,
        });
        assert_eq!(
            image.value,
            json!({ "a": "NaN", "b": "Infinity", "c": "-Infinity" })
        );
        assert_eq!(
            image.types,
            Some(json!({ "a": "double", "b": "double", "c": "double" }))
        );
    }

    #[test]
    fn a_binary_keeps_its_subtype_when_it_has_one() {
        let image = document_image(&doc! {
            "id": Binary { subtype: BinarySubtype::Uuid, bytes: vec![171] },
        });
        assert_eq!(image.value["id"], "hex:ab");
        assert_eq!(image.types, Some(json!({ "id": "binary:04" })));
    }

    #[test]
    fn a_type_with_no_form_is_as_the_host_prints_it_with_its_type() {
        let image = document_image(&doc! {
            "re": Regex { pattern: "a".into(), options: "i".into() },
            "ts": Timestamp { time: 10, increment: 2 },
            "low": Bson::MinKey,
            "sym": Bson::Symbol("s".into()),
        });
        assert_eq!(
            image.types,
            Some(json!({
                "re": "regex",
                "ts": "timestamp",
                "low": "minKey",
                "sym": "symbol",
            }))
        );
        assert!(image.value["re"]
            .as_str()
            .unwrap()
            .contains("regularExpression"));
        assert_eq!(image.value["sym"], "s");
    }
}
