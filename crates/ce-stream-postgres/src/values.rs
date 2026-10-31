//! Values and types: `pgoutput` text under the pinned settings, as the mapping in
//! `docs/values.md` says, and a type's name from its OID and typmod.

use chrono::{DateTime, FixedOffset};
use serde_json::Value;

const BOOL: u32 = 16;
const BYTEA: u32 = 17;
const INT8: u32 = 20;
const INT2: u32 = 21;
const INT4: u32 = 23;
const FLOAT4: u32 = 700;
const FLOAT8: u32 = 701;
const BPCHAR: u32 = 1042;
const VARCHAR: u32 = 1043;
const TIMESTAMPTZ: u32 = 1184;
const NUMERIC: u32 = 1700;

/// The value in the contract's form. Text that has no form there, such as
/// `infinity` in a `timestamptz` or `NaN`, stays text: the consumer decides.
pub fn text_to_json(type_oid: u32, text: &str) -> Value {
    match type_oid {
        BOOL => match text {
            "t" => Value::Bool(true),
            "f" => Value::Bool(false),
            _ => Value::String(text.to_string()),
        },
        INT2 | INT4 | INT8 => text
            .parse::<i64>()
            .map(Value::from)
            .unwrap_or_else(|_| Value::String(text.to_string())),
        FLOAT4 | FLOAT8 => text
            .parse::<f64>()
            .ok()
            .filter(|n| n.is_finite())
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(text.to_string())),
        TIMESTAMPTZ => timestamptz_micros(text)
            .map(Value::from)
            .unwrap_or_else(|| Value::String(text.to_string())),
        BYTEA => match text.strip_prefix("\\x") {
            Some(hex) => Value::String(format!("hex:{hex}")),
            None => Value::String(text.to_string()),
        },
        _ => Value::String(text.to_string()),
    }
}

/// `2026-01-02 03:04:05.5+00`, as `DateStyle=ISO`, `TimeZone=UTC` prints it.
fn timestamptz_micros(text: &str) -> Option<i64> {
    let with_minutes = format!("{text}:00");
    DateTime::<FixedOffset>::parse_from_str(&with_minutes, "%Y-%m-%d %H:%M:%S%.f%#z")
        .or_else(|_| DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z"))
        .ok()
        .map(|at| at.timestamp_micros())
}

/// The name of a type, as `format_type` gives it. `known` holds the names the
/// server sent in `Type` messages, for types that are not built in.
pub fn host_type(type_oid: u32, typmod: i32, known: Option<&str>) -> String {
    let name = match type_oid {
        BOOL => "boolean",
        BYTEA => "bytea",
        18 => "\"char\"",
        19 => "name",
        INT8 => "bigint",
        INT2 => "smallint",
        INT4 => "integer",
        25 => "text",
        26 => "oid",
        114 => "json",
        142 => "xml",
        FLOAT4 => "real",
        FLOAT8 => "double precision",
        790 => "money",
        869 => "inet",
        BPCHAR => "character",
        VARCHAR => "character varying",
        1082 => "date",
        1083 => "time without time zone",
        1114 => "timestamp without time zone",
        TIMESTAMPTZ => "timestamp with time zone",
        1186 => "interval",
        1266 => "time with time zone",
        1560 => "bit",
        1562 => "bit varying",
        NUMERIC => "numeric",
        2950 => "uuid",
        3802 => "jsonb",
        other => {
            return match known {
                Some(name) => name.to_string(),
                None => format!("oid {other}"),
            }
        }
    };
    match type_oid {
        NUMERIC if typmod >= 4 => {
            let packed = typmod - 4;
            let precision = (packed >> 16) & 0xffff;
            // The scale is eleven bits, signed: a negative scale exists since 15.
            let scale = ((packed & 0x7ff) ^ 1024) - 1024;
            format!("{name}({precision},{scale})")
        }
        BPCHAR | VARCHAR if typmod >= 4 => format!("{name}({})", typmod - 4),
        _ => name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_and_booleans_are_json_ones() {
        assert_eq!(text_to_json(INT4, "42"), Value::from(42));
        assert_eq!(
            text_to_json(INT8, "9223372036854775807"),
            Value::from(i64::MAX)
        );
        assert_eq!(text_to_json(FLOAT8, "1.5"), Value::from(1.5));
        assert_eq!(text_to_json(BOOL, "t"), Value::Bool(true));
        assert_eq!(text_to_json(BOOL, "f"), Value::Bool(false));
    }

    #[test]
    fn what_json_has_no_number_for_stays_text() {
        assert_eq!(text_to_json(FLOAT8, "NaN"), Value::from("NaN"));
        assert_eq!(text_to_json(FLOAT8, "Infinity"), Value::from("Infinity"));
        assert_eq!(text_to_json(NUMERIC, "NaN"), Value::from("NaN"));
    }

    #[test]
    fn decimals_stay_exact_text() {
        assert_eq!(
            text_to_json(NUMERIC, "12345678901234567890.123456789"),
            Value::from("12345678901234567890.123456789")
        );
    }

    #[test]
    fn an_instant_is_microseconds_and_a_time_without_a_zone_is_text() {
        assert_eq!(
            text_to_json(TIMESTAMPTZ, "2026-01-02 03:04:05+00"),
            Value::from(1_767_323_045_000_000i64)
        );
        assert_eq!(
            text_to_json(TIMESTAMPTZ, "2026-01-02 03:04:05.25+00"),
            Value::from(1_767_323_045_250_000i64)
        );
        assert_eq!(
            text_to_json(1114, "2026-01-02 03:04:05"),
            Value::from("2026-01-02 03:04:05")
        );
        assert_eq!(
            text_to_json(TIMESTAMPTZ, "infinity"),
            Value::from("infinity")
        );
    }

    #[test]
    fn bytes_are_hex_and_text_stays_text() {
        assert_eq!(text_to_json(BYTEA, "\\x00ff"), Value::from("hex:00ff"));
        assert_eq!(text_to_json(25, "caf\u{e9}"), Value::from("caf\u{e9}"));
        assert_eq!(text_to_json(2950, "a0ee"), Value::from("a0ee"));
        assert_eq!(text_to_json(3802, "{\"a\": 1}"), Value::from("{\"a\": 1}"));
        assert_eq!(text_to_json(1007, "{1,2}"), Value::from("{1,2}"));
    }

    #[test]
    fn type_names_with_their_sizes() {
        assert_eq!(host_type(INT4, -1, None), "integer");
        assert_eq!(host_type(NUMERIC, -1, None), "numeric");
        // numeric(10,2): ((10 << 16) | 2) + 4.
        assert_eq!(host_type(NUMERIC, 655_366, None), "numeric(10,2)");
        // numeric(3,-2): the scale is eleven bits, signed.
        assert_eq!(
            host_type(NUMERIC, ((3 << 16) | (-2i32 & 0x7ff)) + 4, None),
            "numeric(3,-2)"
        );
        assert_eq!(host_type(VARCHAR, 14, None), "character varying(10)");
        assert_eq!(host_type(BPCHAR, 7, None), "character(3)");
        assert_eq!(host_type(99_999, -1, Some("public.mood")), "public.mood");
        assert_eq!(host_type(99_999, -1, None), "oid 99999");
    }
}
