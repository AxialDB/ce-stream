use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use serde_json::{Map, Value};

use crate::charset;
use crate::columns::{Column, ColumnKind};

/// A row as the contract says (`docs/values.md`). A value that cannot be read
/// is an error naming the column, never a `hex:` string in place of text.
pub fn row_to_object(columns: &[Column], row: &RowEvent) -> Result<Map<String, Value>, String> {
    let mut out = Map::new();
    for (i, val) in row.column_values.iter().enumerate() {
        match columns.get(i) {
            Some(column) if column.kind == ColumnKind::NoDecoder => {}
            Some(column) => {
                out.insert(column.name.clone(), column_to_json(column, val)?);
            }
            None => {
                out.insert(format!("col_{i}"), column_value_to_json(val));
            }
        }
    }
    Ok(out)
}

fn column_to_json(column: &Column, v: &ColumnValue) -> Result<Value, String> {
    match (&column.kind, v) {
        (ColumnKind::Text(cs), ColumnValue::String(bytes) | ColumnValue::Blob(bytes)) => {
            charset::decode(cs, bytes)
                .map(Value::String)
                .ok_or_else(|| {
                    format!(
                        "column {}: a value is not valid in its charset {cs} ({} bytes)",
                        column.name,
                        bytes.len()
                    )
                })
        }
        (ColumnKind::Binary, ColumnValue::String(bytes) | ColumnValue::Blob(bytes)) => {
            Ok(Value::String(format!("hex:{}", hex_encode(bytes))))
        }
        (ColumnKind::Enum(labels), ColumnValue::Enum(n)) => match *n {
            // 0 is MySQL's error value, which reads as ''.
            0 => Ok(Value::String(String::new())),
            n => labels
                .get(n as usize - 1)
                .map(|label| Value::String(label.clone()))
                .ok_or_else(|| {
                    format!(
                        "column {}: ENUM value {n} has no label in the table map",
                        column.name
                    )
                }),
        },
        (ColumnKind::Set(labels), ColumnValue::Set(mask)) => {
            let chosen: Vec<&str> = labels
                .iter()
                .enumerate()
                .filter(|(i, _)| *i < 64 && mask & (1u64 << i) != 0)
                .map(|(_, label)| label.as_str())
                .collect();
            Ok(Value::String(chosen.join(",")))
        }
        (_, ColumnValue::Json(bytes)) => {
            if bytes.is_empty() {
                return Ok(Value::String(String::new()));
            }
            crate::json_text::json_text(bytes)
                .map(Value::String)
                .map_err(|e| format!("column {}: JSON value: {e}", column.name))
        }
        _ => Ok(column_value_to_json(v)),
    }
}

pub fn column_value_to_json(v: &ColumnValue) -> Value {
    match v {
        ColumnValue::None => Value::Null,
        ColumnValue::Tiny(n) => Value::from(*n),
        ColumnValue::Short(n) => Value::from(*n),
        ColumnValue::Long(n) => Value::from(*n),
        ColumnValue::LongLong(n) => Value::from(*n),
        ColumnValue::Float(n) => Value::from(*n),
        ColumnValue::Double(n) => Value::from(*n),
        ColumnValue::Decimal(s)
        | ColumnValue::Time(s)
        | ColumnValue::Date(s)
        | ColumnValue::DateTime(s) => Value::String(s.clone()),
        ColumnValue::Timestamp(us) => Value::from(*us),
        ColumnValue::Year(y) => Value::from(*y),
        ColumnValue::String(bytes) => match String::from_utf8(bytes.clone()) {
            Ok(s) => Value::String(s),
            Err(_) => Value::String(format!("hex:{}", hex_encode(bytes))),
        },
        ColumnValue::Blob(bytes) | ColumnValue::Json(bytes) => {
            match String::from_utf8(bytes.clone()) {
                Ok(s) => Value::String(s),
                Err(_) => Value::String(format!("hex:{}", hex_encode(bytes))),
            }
        }
        ColumnValue::Bit(n) | ColumnValue::Set(n) => Value::from(*n),
        ColumnValue::Enum(n) => Value::from(*n),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(kind: ColumnKind) -> Column {
        Column {
            name: "c".into(),
            kind,
        }
    }

    fn read(kind: ColumnKind, v: ColumnValue) -> Result<Value, String> {
        column_to_json(&column(kind), &v)
    }

    #[test]
    fn text_is_read_in_its_charset_never_as_hex() {
        assert_eq!(
            read(
                ColumnKind::Text("latin1"),
                ColumnValue::String(b"th\xe9".to_vec())
            )
            .unwrap(),
            "th\u{e9}"
        );
        // Valid UTF-8 bytes in a latin1 column are two latin1 characters.
        assert_eq!(
            read(
                ColumnKind::Text("latin1"),
                ColumnValue::Blob(b"\xc3\xa9".to_vec())
            )
            .unwrap(),
            "\u{c3}\u{a9}"
        );
        let err = read(
            ColumnKind::Text("utf8mb4"),
            ColumnValue::String(b"\xe9".to_vec()),
        )
        .unwrap_err();
        assert!(err.contains("column c"), "{err}");
    }

    #[test]
    fn binary_is_always_hex() {
        assert_eq!(
            read(ColumnKind::Binary, ColumnValue::Blob(b"abc".to_vec())).unwrap(),
            "hex:616263"
        );
    }

    #[test]
    fn enum_and_set_are_their_labels() {
        let labels = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let e = ColumnKind::Enum(labels.clone());
        assert_eq!(read(e.clone(), ColumnValue::Enum(2)).unwrap(), "b");
        assert_eq!(read(e.clone(), ColumnValue::Enum(0)).unwrap(), "");
        assert!(read(e, ColumnValue::Enum(9)).is_err());
        let s = ColumnKind::Set(labels);
        assert_eq!(read(s.clone(), ColumnValue::Set(0b101)).unwrap(), "a,c");
        assert_eq!(read(s, ColumnValue::Set(0)).unwrap(), "");
    }

    #[test]
    fn a_column_with_no_decoder_is_left_out_of_the_row() {
        let columns = vec![
            column(ColumnKind::NoDecoder),
            Column {
                name: "id".into(),
                kind: ColumnKind::Plain,
            },
        ];
        let row = RowEvent {
            column_values: vec![ColumnValue::String(vec![0xe9]), ColumnValue::Long(1)],
        };
        let out = row_to_object(&columns, &row).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out["id"], 1);
    }
}
