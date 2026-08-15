use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use serde_json::{Map, Value};

pub fn row_to_object(col_names: &[String], row: &RowEvent) -> Map<String, Value> {
    let mut out = Map::new();
    for (i, val) in row.column_values.iter().enumerate() {
        let key = col_names
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("col_{i}"));
        out.insert(key, column_value_to_json(val));
    }
    out
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
