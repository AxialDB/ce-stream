//! What the table map says about each column: how to read its values, and the
//! table's definition (issue #19).

use ce_stream_core::delivery::{ColumnSchema, TableSchema};
use ce_stream_core::event::TableRef;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;

use crate::charset;

const CHARSET_BINARY: &str = "binary";

const TYPE_ENUM: u16 = 247;
const TYPE_SET: u16 = 248;
const TYPE_STRING: u8 = 254;

/// How a column's value is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnKind {
    /// Text in a charset with an admitted decoder.
    Text(&'static str),
    /// A charset with no admitted decoder. The column is left out of the row:
    /// a consumer that needs it refuses it, by the charset in its catalog.
    NoDecoder,
    /// Bytes: `BINARY`, `VARBINARY`, `BLOB`.
    Binary,
    Enum(Vec<String>),
    Set(Vec<String>),
    /// No metadata said (`binlog_row_metadata` is not `FULL`, which the gate
    /// refuses), or a type that needs none.
    Plain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub kind: ColumnKind,
}

/// The type an ENUM or SET really is: the table map writes both as `String`,
/// with the real type in the high byte of the metadata.
fn real_type(column_type: u8, meta: u16) -> u16 {
    let high = meta >> 8;
    if column_type == TYPE_STRING && (high == TYPE_ENUM || high == TYPE_SET) {
        high
    } else {
        u16::from(column_type)
    }
}

fn is_character(column_type: u8, meta: u16) -> bool {
    matches!(real_type(column_type, meta), 15 | 249..=254)
}

/// The collation of each character column, in table order. The metadata holds
/// a default with exceptions, or one collation for each character column.
fn collations(tm: &TableMapEvent) -> Vec<Option<u32>> {
    let mut out = vec![None; tm.column_types.len()];
    let Some(meta) = &tm.table_metadata else {
        return out;
    };
    let characters: Vec<usize> = (0..tm.column_types.len())
        .filter(|&i| is_character(tm.column_types[i], tm.column_metas[i]))
        .collect();
    if let Some(default) = &meta.default_charset {
        for &i in &characters {
            out[i] = Some(default.default_charset_collation);
        }
        for (nth, collation) in &default.charset_collations {
            if let Some(&i) = characters.get(*nth as usize) {
                out[i] = Some(*collation);
            }
        }
    } else {
        // The parser keeps these in the first columns, in the order they came:
        // one for each character column.
        for (nth, &i) in characters.iter().enumerate() {
            out[i] = meta.columns.get(nth).and_then(|c| c.charset_collation);
        }
    }
    out
}

pub fn columns_of(tm: &TableMapEvent) -> Vec<Column> {
    let collations = collations(tm);
    (0..tm.column_types.len())
        .map(|i| {
            let meta = tm.table_metadata.as_ref().and_then(|m| m.columns.get(i));
            let name = meta
                .and_then(|c| c.column_name.clone())
                .unwrap_or_else(|| format!("col_{i}"));
            let kind = match real_type(tm.column_types[i], tm.column_metas[i]) {
                TYPE_ENUM => match meta.and_then(|c| c.enum_string_values.clone()) {
                    Some(labels) => ColumnKind::Enum(labels),
                    None => ColumnKind::Plain,
                },
                TYPE_SET => match meta.and_then(|c| c.set_string_values.clone()) {
                    Some(labels) => ColumnKind::Set(labels),
                    None => ColumnKind::Plain,
                },
                _ => match collations[i] {
                    None => ColumnKind::Plain,
                    Some(id) => match charset::charset_of_collation(id) {
                        Some(CHARSET_BINARY) => ColumnKind::Binary,
                        Some(name) if charset::admitted(name) => ColumnKind::Text(name),
                        _ => ColumnKind::NoDecoder,
                    },
                },
            };
            Column { name, kind }
        })
        .collect()
}

/// MySQL's name for a column type, from its code and metadata. Sizes are given
/// where the table map holds them.
fn host_type(column_type: u8, meta: u16, signed: Option<bool>, collation: Option<u32>) -> String {
    // The parser's `is_signed` holds the metadata's bit, which is set for an
    // unsigned column.
    let unsigned = if signed == Some(true) {
        " unsigned"
    } else {
        ""
    };
    let binary = collation == Some(63);
    match real_type(column_type, meta) {
        0 | 246 => format!("decimal({},{})", meta & 0xff, meta >> 8),
        1 => format!("tinyint{unsigned}"),
        2 => format!("smallint{unsigned}"),
        3 => format!("int{unsigned}"),
        4 => "float".into(),
        5 => "double".into(),
        7 | 17 => "timestamp".into(),
        8 => format!("bigint{unsigned}"),
        9 => format!("mediumint{unsigned}"),
        10 | 14 => "date".into(),
        11 | 19 => "time".into(),
        12 | 18 => "datetime".into(),
        13 => "year".into(),
        15 | 253 if binary => "varbinary".into(),
        15 | 253 => "varchar".into(),
        16 => "bit".into(),
        245 => "json".into(),
        247 => "enum".into(),
        248 => "set".into(),
        249..=252 if binary => "blob".into(),
        249..=252 => "text".into(),
        254 if binary => "binary".into(),
        254 => "char".into(),
        255 => "geometry".into(),
        other => format!("type {other}"),
    }
}

/// The table's definition, when the table map carries metadata.
pub fn schema_of(tm: &TableMapEvent) -> Option<TableSchema> {
    let meta = tm.table_metadata.as_ref()?;
    let collations = collations(tm);
    let columns = (0..tm.column_types.len())
        .map(|i| {
            let column = meta.columns.get(i);
            ColumnSchema {
                name: column
                    .and_then(|c| c.column_name.clone())
                    .unwrap_or_else(|| format!("col_{i}")),
                host_type: host_type(
                    tm.column_types[i],
                    tm.column_metas[i],
                    column.and_then(|c| c.is_signed),
                    collations[i],
                ),
                nullable: tm.null_bits.get(i).copied(),
                key: column.is_some_and(|c| {
                    c.is_simple_primary_key == Some(true) || c.primary_key_prefix.is_some()
                }),
            }
        })
        .collect();
    Some(TableSchema {
        subject: TableRef::new(tm.database_name.clone(), tm.table_name.clone()),
        identity: None,
        columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_binlog_connector_rust::event::table_map::default_charset::DefaultCharset;
    use mysql_binlog_connector_rust::event::table_map::table_metadata::{
        ColumnMetadata, TableMetadata,
    };

    fn named(name: &str) -> ColumnMetadata {
        ColumnMetadata {
            column_name: Some(name.into()),
            ..Default::default()
        }
    }

    fn table(
        column_types: Vec<u8>,
        column_metas: Vec<u16>,
        columns: Vec<ColumnMetadata>,
        default_charset: Option<DefaultCharset>,
    ) -> TableMapEvent {
        let count = column_types.len();
        TableMapEvent {
            table_id: 1,
            database_name: "db".into(),
            table_name: "t".into(),
            column_types,
            column_metas,
            null_bits: vec![true; count],
            table_metadata: Some(TableMetadata {
                default_charset,
                enum_and_set_default_charset: None,
                columns,
            }),
        }
    }

    #[test]
    fn a_default_charset_has_exceptions_counted_among_the_character_columns() {
        // id INT, a VARCHAR latin1 (default), n INT, b TEXT utf8mb4, c BLOB.
        let tm = table(
            vec![3, 15, 3, 252, 252],
            vec![0, 200, 0, 2, 2],
            vec![named("id"), named("a"), named("n"), named("b"), named("c")],
            Some(DefaultCharset::new(8, vec![(1, 255), (2, 63)])),
        );
        let kinds: Vec<_> = columns_of(&tm).into_iter().map(|c| c.kind).collect();
        assert_eq!(
            kinds,
            [
                ColumnKind::Plain,
                ColumnKind::Text("latin1"),
                ColumnKind::Plain,
                ColumnKind::Text("utf8mb4"),
                ColumnKind::Binary,
            ]
        );
    }

    #[test]
    fn a_collation_per_character_column_is_kept_in_the_first_entries() {
        // The parser stores them from the first column on, whatever its type.
        let mut first = named("id");
        first.charset_collation = Some(8);
        let mut second = named("a");
        second.charset_collation = Some(14);
        let tm = table(
            vec![3, 15, 254],
            vec![0, 200, 0xfe0a],
            vec![first, second, named("b")],
            None,
        );
        let kinds: Vec<_> = columns_of(&tm).into_iter().map(|c| c.kind).collect();
        assert_eq!(
            kinds,
            [
                ColumnKind::Plain,
                ColumnKind::Text("latin1"),
                ColumnKind::Text("cp1251"),
            ]
        );
    }

    #[test]
    fn enum_and_set_are_known_by_the_high_byte_and_carry_their_labels() {
        let mut e = named("e");
        e.enum_string_values = Some(vec!["a".into(), "b".into()]);
        let mut s = named("s");
        s.set_string_values = Some(vec!["x".into()]);
        let tm = table(
            vec![254, 254],
            vec![(247 << 8) | 1, (248 << 8) | 1],
            vec![e, s],
            None,
        );
        let kinds: Vec<_> = columns_of(&tm).into_iter().map(|c| c.kind).collect();
        assert_eq!(kinds[0], ColumnKind::Enum(vec!["a".into(), "b".into()]));
        assert_eq!(kinds[1], ColumnKind::Set(vec!["x".into()]));
    }

    #[test]
    fn a_charset_with_no_decoder_is_said_so() {
        let tm = table(
            vec![15],
            vec![200],
            vec![named("a")],
            Some(DefaultCharset::new(13, vec![])),
        );
        assert_eq!(columns_of(&tm)[0].kind, ColumnKind::NoDecoder);
    }

    #[test]
    fn the_schema_says_the_key_and_the_host_types() {
        let mut id = named("id");
        id.is_simple_primary_key = Some(true);
        id.is_signed = Some(true);
        let tm = table(
            vec![3, 15, 246],
            vec![0, 200, (2 << 8) | 10],
            vec![id, named("a"), named("d")],
            Some(DefaultCharset::new(255, vec![])),
        );
        let schema = schema_of(&tm).unwrap();
        assert_eq!(schema.subject, TableRef::new("db", "t"));
        assert_eq!(schema.columns[0].host_type, "int unsigned");
        assert!(schema.columns[0].key);
        assert_eq!(schema.columns[1].host_type, "varchar");
        assert_eq!(schema.columns[1].nullable, Some(true));
        assert_eq!(schema.columns[2].host_type, "decimal(10,2)");
        assert!(!schema.columns[1].key);
    }
}
